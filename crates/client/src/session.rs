use std::{
	collections::{
		HashMap,
		HashSet,
	},
	ffi::OsStr,
	io::SeekFrom,
	path::{
		Component,
		Path,
		PathBuf,
	},
	sync::Arc,
};

use anyhow::{
	Context,
	bail,
	ensure,
};
use bit_set::BitSet;
use bytes::{
	Bytes,
	BytesMut,
};
use parking_lot::Mutex;
use lib::{
	ClientMessage,
	Compression,
	FileManifestEntry,
	MAX_CONTROL_FRAME_SIZE,
	PacketHandler,
	ReceivedChunks,
	RejectedTransfer,
	ServerMessage,
	format_bytes,
	progress_bar,
};
use rayon::{
	iter::ParallelIterator,
	prelude::IntoParallelRefIterator,
};
use rkyv::rancor;
use tokio::{
	fs::File,
	io::{
		AsyncReadExt,
		AsyncSeekExt,
	},
	sync::mpsc,
	task::JoinSet,
};
use tracing::info;
use walkdir::WalkDir;

use crate::{
	ChunkJob,
	DuplicateStrategy,
	TransferCandidate,
};

#[derive(Debug, Clone)]
pub enum ClientState {
	Connecting,
	Idle,
	RegisteringTransfers,
	SendingFiles,
	Finishing,
}

pub const CHUNK_SIZE: u64 = 1024 * 1024;
pub const DATA_STREAM_COUNT: usize = 8;

pub struct ClientSession {
	conn: quinn::Connection,
	control: Option<(quinn::SendStream, quinn::RecvStream)>,
	state: ClientState,
	compression: Compression,
	files: HashMap<u32, TransferCandidate>,
}

impl PacketHandler for ClientSession {}

struct FileSendProgress {
	path: String,
	total_bytes: u64,
	completed_bytes: u64,
	completed: bool,
}

struct SendProgress {
	files: HashMap<u32, FileSendProgress>,
	total_bytes: u64,
	completed_bytes: u64,
	next_report_percent: u64,
}

impl SendProgress {
	fn new(files: &HashMap<u32, TransferCandidate>, accepted: &HashSet<u32>, resumed: &HashMap<u32, BitSet<u64>>) -> Self {
		let files = files
			.iter()
			.filter(|(id, _)| accepted.contains(id))
			.map(|(&id, transfer)| {
				let completed_bytes = resumed.get(&id).map_or(0, |chunks| ClientSession::received_bytes(chunks, transfer.uncompressed_size, CHUNK_SIZE));
				(
					id,
					FileSendProgress {
						path: transfer.rel_path.join("/"),
						total_bytes: transfer.uncompressed_size,
						completed_bytes,
						completed: completed_bytes >= transfer.uncompressed_size && transfer.uncompressed_size > 0,
					},
				)
			})
			.collect::<HashMap<_, _>>();
		let total_bytes = files.values().map(|file| file.total_bytes).sum();
		let completed_bytes = files.values().map(|file| file.completed_bytes).sum();

		Self {
			files,
			total_bytes,
			completed_bytes,
			next_report_percent: 5,
		}
	}

	fn record_sent(&mut self, file_id: u32, bytes: u64) -> Vec<String> {
		let Some(file) = self.files.get_mut(&file_id) else {
			return Vec::new();
		};

		let remaining = file.total_bytes.saturating_sub(file.completed_bytes);
		let bytes = bytes.min(remaining);
		file.completed_bytes += bytes;
		self.completed_bytes += bytes;

		let mut messages = Vec::new();
		if !file.completed && file.completed_bytes >= file.total_bytes {
			file.completed = true;
			messages.push(format!("Sent file {} ({})", file.path, format_bytes(file.total_bytes)));
		}

		let percent = self.percent_complete();
		if percent >= self.next_report_percent || self.completed_bytes >= self.total_bytes {
			messages.push(format!(
				"Send progress {} {}/{}",
				progress_bar(self.completed_bytes, self.total_bytes),
				format_bytes(self.completed_bytes),
				format_bytes(self.total_bytes)
			));
			while self.next_report_percent <= percent {
				self.next_report_percent += 5;
			}
		}

		messages
	}

	#[inline(always)]
	fn percent_complete(&self) -> u64 {
		if self.total_bytes == 0 {
			100
		} else {
			(self.completed_bytes.min(self.total_bytes) * 100) / self.total_bytes
		}
	}
}

fn record_sent(progress: &Arc<Mutex<SendProgress>>, file_id: u32, bytes: u64) {
	let messages = progress.lock().record_sent(file_id, bytes);
	for message in messages {
		info!("{message}");
	}
}

impl ClientSession {
	pub fn new(conn: quinn::Connection, files: Vec<PathBuf>, duplicate_strategy: DuplicateStrategy, compression: Compression) -> anyhow::Result<Self> {
		if files.is_empty() {
			bail!("No files selected");
		}
		let files = Self::build_transfers(files, duplicate_strategy)?;
		Ok(Self {
			conn,
			control: None,
			state: ClientState::Connecting,
			compression,
			files,
		})
	}

	pub async fn run(&mut self) -> anyhow::Result<()> {
		self.open_control_stream().await?;
		self.handshake().await?;
		let (accepted, rejected, received_chunks) = self.register_transfers().await?;
		if accepted.is_empty() {
			bail!("All files were rejected by the server: {:?}", rejected)
		}
		// Rejected are intentionally ignored
		self.send_accepted_files(accepted, received_chunks).await?;
		self.finish_and_reconcile().await?;
		Ok(())
	}

	async fn handshake(&mut self) -> anyhow::Result<()> {
		ensure!(matches!(self.state, ClientState::Connecting));

		let message = ClientMessage::Hello { version: lib::PROTOCOL_VERSION };
		self.write_control(message).await?;
		match self.read_control().await? {
			ServerMessage::HelloAck { version } => {
				ensure!(version == lib::PROTOCOL_VERSION);
				self.state = ClientState::Idle;
			}
			message => bail!("Unexpected message in handshake: {message:?}"),
		}
		Ok(())
	}

	async fn register_transfers(&mut self) -> anyhow::Result<(HashSet<u32>, Vec<RejectedTransfer>, Vec<ReceivedChunks>)> {
		ensure!(matches!(self.state, ClientState::Idle));
		self.state = ClientState::RegisteringTransfers;
		let files = Self::build_file_manifest(&self.files);
		let message = ClientMessage::RegisterTransfers {
			files,
			chunk_size: CHUNK_SIZE,
			compression: self.compression,
		};
		self.write_control(message).await?;
		match self.read_control().await? {
			ServerMessage::TransfersRegistered { accepted, rejected, received_chunks } => {
				self.state = ClientState::SendingFiles;
				Ok((accepted, rejected, received_chunks))
			}
			message => anyhow::bail!("Unexpected message in register transfers: {message:?}"),
		}
	}

	async fn send_accepted_files(&mut self, accepted: HashSet<u32>, received_chunks: Vec<ReceivedChunks>) -> anyhow::Result<()> {
		ensure!(matches!(self.state, ClientState::SendingFiles));

		let resumed = Self::received_chunks_by_file(received_chunks);
		let progress = Arc::new(Mutex::new(SendProgress::new(&self.files, &accepted, &resumed)));
		let (accepted_files, total_bytes) = {
			let progress = progress.lock();
			(progress.files.len(), progress.total_bytes)
		};
		let completed_bytes = progress.lock().completed_bytes;
		info!(
			"Sending {accepted_files} files ({}) with {:?} compression {}",
			format_bytes(total_bytes),
			self.compression,
			progress_bar(completed_bytes, total_bytes)
		);

		let mut senders = Vec::new();
		let mut join: JoinSet<anyhow::Result<_>> = JoinSet::new();

		for _ in 0..DATA_STREAM_COUNT {
			let (tx, mut rx) = mpsc::channel::<ChunkJob>(32);
			senders.push(tx);

			let conn = self.conn.clone();
			let compression = self.compression;
			let progress = progress.clone();

			join.spawn(async move {
				let mut stream = conn.open_uni().await.context("Failed to open uni stream")?;

				while let Some(job) = rx.recv().await {
					let bytes = compression.compress(Self::read_chunk(&job).await?);

					let message = ClientMessage::Chunk {
						file_id: job.id,
						offset: job.offset,
						bytes,
					};

					let encoded = rkyv::to_bytes::<rancor::Error>(&message).context("Failed to serialize chunk message")?;
					Self::write_frame(&mut stream, &encoded).await.context("Failed to write chunk message")?;
					record_sent(&progress, job.id, job.uncompressed_len);
				}

				stream.finish().context("Failed to finish data stream")?;

				Ok(())
			});
		}

		let mut next_stream = 0;

		for (&file_id, transfer) in &self.files {
			if !accepted.contains(&file_id) {
				continue;
			}

			let mut offset = 0;
			let mut chunk_index = 0usize;

			while offset < transfer.uncompressed_size {
				let remaining = transfer.uncompressed_size - offset;
				let len = remaining.min(CHUNK_SIZE);
				if resumed.get(&file_id).is_some_and(|chunks| chunks.contains(chunk_index)) {
					offset += len;
					chunk_index += 1;
					continue;
				}

				let job = ChunkJob {
					id: file_id,
					source_path: transfer.source_path.clone(),
					offset,
					uncompressed_len: len,
				};

				senders[next_stream].send(job).await.context("Failed to send chunk job")?;
				next_stream = (next_stream + 1) % DATA_STREAM_COUNT; // Rounrd robin
				offset += len;
				chunk_index += 1;
			}
		}

		drop(senders);

		while let Some(result) = join.join_next().await {
			result??;
		}

		Ok(())
	}

	async fn finish_and_reconcile(&mut self) -> anyhow::Result<()> {
		ensure!(matches!(self.state, ClientState::SendingFiles));
		self.state = ClientState::Finishing;
		let message = ClientMessage::TransferFinished;
		self.write_control(message).await?;
		match self.read_control().await? {
			ServerMessage::TransferComplete => {}
			message => anyhow::bail!("Unexpected message in finish and reconcile: {message:?}"),
		}
		Ok(())
	}

	async fn read_control(&mut self) -> anyhow::Result<ServerMessage> {
		let Some((_, ref mut recv)) = self.control else {
			anyhow::bail!("No control stream available");
		};

		let bytes = Self::read_frame(recv, MAX_CONTROL_FRAME_SIZE).await?;
		let message = rkyv::from_bytes::<ServerMessage, rancor::Error>(&bytes)?;
		Ok(message)
	}

	async fn write_control(&mut self, message: ClientMessage) -> anyhow::Result<()> {
		let Some((ref mut send, _)) = self.control else {
			anyhow::bail!("No control stream available");
		};

		let bytes = rkyv::to_bytes::<rancor::Error>(&message).context("Failed to serialize control message")?;
		Self::write_frame(send, &bytes).await.context("Failed to write control message")
	}

	async fn open_control_stream(&mut self) -> anyhow::Result<()> {
		let (send, recv) = self.conn.open_bi().await.context("Failed to open bidirectional stream")?;
		self.control = Some((send, recv));
		Ok(())
	}

	fn received_bytes(received: &BitSet<u64>, file_size: u64, chunk_size: u64) -> u64 {
		received
			.iter()
			.map(|chunk| {
				let offset = chunk as u64 * chunk_size;
				(file_size - offset).min(chunk_size)
			})
			.sum::<u64>()
			.min(file_size)
	}

	fn bitset_from_received_chunks(received_chunks: &ReceivedChunks) -> BitSet<u64> {
		let restored = BitSet::<u64>::from_bytes_general(&received_chunks.bitset_bytes);
		let mut received = BitSet::<u64>::default();
		received.reserve_len(received_chunks.chunk_count as usize);
		for chunk in restored.iter().filter(|&chunk| (chunk as u64) < received_chunks.chunk_count) {
			received.insert(chunk);
		}
		received
	}

	fn received_chunks_by_file(received_chunks: Vec<ReceivedChunks>) -> HashMap<u32, BitSet<u64>> {
		received_chunks.into_iter().map(|chunks| (chunks.file_id, Self::bitset_from_received_chunks(&chunks))).collect()
	}

	async fn read_chunk(job: &ChunkJob) -> anyhow::Result<Bytes> {
		let mut file = File::open(&job.source_path).await?;
		file.seek(SeekFrom::Start(job.offset)).await?;

		let mut buf = BytesMut::with_capacity(job.uncompressed_len as usize);
		let mut take = file.take(job.uncompressed_len);

		let expected = job.uncompressed_len as usize;
		while buf.len() < expected {
			let n = take.read_buf(&mut buf).await?;
			if n == 0 {
				bail!(
					"Unexpected EOF while reading {} at offset {}: expected {} bytes, got {}",
					job.source_path.display(),
					job.offset,
					expected,
					buf.len()
				);
			}
		}

		Ok(buf.freeze())
	}

	fn build_file_manifest(transfers: &HashMap<u32, TransferCandidate>) -> Vec<FileManifestEntry> {
		transfers
			.par_iter()
			.map(|(id, transfer)| FileManifestEntry {
				id: *id,
				rel_path: transfer.rel_path.clone(),
				uncompressed_size: transfer.uncompressed_size,
			})
			.collect()
	}

	fn build_transfers(files: Vec<PathBuf>, duplicate_strategy: DuplicateStrategy) -> anyhow::Result<HashMap<u32, TransferCandidate>> {
		let mut candidates = files
			.par_iter()
			.map(|selected| Self::collect_selected_path(selected))
			.collect::<anyhow::Result<Vec<Vec<TransferCandidate>>>>()?
			.into_iter()
			.flatten()
			.collect::<Vec<_>>();

		// Sort by path to make id assignment deterministic
		candidates.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));

		match duplicate_strategy {
			DuplicateStrategy::Reject => {
				for pair in candidates.windows(2) {
					let first = &pair[0];
					let second = &pair[1];
					if first.rel_path == second.rel_path {
						bail!(
							"The following files will result in the same resulting file after transfer: {}, {}",
							first.source_path.display(),
							second.source_path.display()
						);
					}
				}
			}
			DuplicateStrategy::Enumerate => {
				let mut seen: HashMap<Vec<String>, u32> = HashMap::new();

				for candidate in &mut candidates {
					let original = candidate.rel_path.clone();
					let count = seen.entry(original.clone()).or_insert(0);
					*count += 1;

					if *count > 1 {
						candidate.rel_path = Self::enumerate_rel_path(&original, *count)?;
					}
				}
			}
		}

		let mut next_id = 0u32;
		candidates
			.into_iter()
			.map(|candidate| {
				let id = next_id;
				next_id = next_id.checked_add(1u32).context("Too many files selected, transfer IDs are limited to u32")?;
				Ok((id, candidate))
			})
			.collect()
	}

	fn enumerate_rel_path(original: &[String], count: u32) -> anyhow::Result<Vec<String>> {
		let Some((file_name, parents)) = original.split_last() else {
			anyhow::bail!("Path has no file name");
		};

		let (stem, ext) = file_name.rsplit_once('.').map_or((file_name.as_str(), ""), |(stem, ext)| (stem, ext));

		let renamed = if ext.is_empty() { format!("{stem} ({count})") } else { format!("{stem} ({count}).{ext}") };

		let mut result = parents.to_owned();
		result.push(renamed);
		Ok(result)
	}

	fn collect_selected_path(selected: &Path) -> anyhow::Result<Vec<TransferCandidate>> {
		if selected.is_file() {
			let file_name = Self::path_component_to_string(selected.file_name().context("Path has no file name")?)?;

			let metadata = selected.metadata()?;

			return Ok(vec![TransferCandidate {
				source_path: selected.to_owned(),
				rel_path: vec![file_name],
				uncompressed_size: metadata.len(),
			}]);
		}

		if selected.is_dir() {
			let root_name = Self::path_component_to_string(selected.file_name().context("Path has no file name")?)?;
			return WalkDir::new(selected)
				.follow_links(false)
				.into_iter()
				.filter_map(|entry| match entry {
					Ok(entry) if entry.file_type().is_file() => Some(Ok(entry)),
					Ok(_) => None,
					Err(err) => Some(Err(err)),
				})
				.map(|entry| {
					let entry = entry?;
					let path = entry.path();

					let rel_inside_root = path.strip_prefix(selected).context("walked path was not under selected root")?;

					let mut rel_path = Vec::new();
					rel_path.push(root_name.clone());
					rel_path.extend(Self::path_to_rel_components(rel_inside_root)?);

					let metadata = entry.metadata()?;

					Ok(TransferCandidate {
						source_path: path.to_owned(),
						rel_path,
						uncompressed_size: metadata.len(),
					})
				})
				.collect();
		}

		anyhow::bail!("Selected path is neither a file nor a directory {}", selected.display())
	}

	fn path_component_to_string(part: &OsStr) -> anyhow::Result<String> {
		part.to_str().context("Path component is not valid UTF-8").map(str::to_owned)
	}

	fn path_to_rel_components(path: &Path) -> anyhow::Result<Vec<String>> {
		path.components()
			.map(|component| {
				let Component::Normal(part) = component else {
					anyhow::bail!("invalid relative path component");
				};

				Self::path_component_to_string(part)
			})
			.collect()
	}
}

#[cfg(test)]
mod tests {
	use std::{
		fs,
		time::{
			SystemTime,
			UNIX_EPOCH,
		},
	};

	use super::*;

	fn test_dir(name: &str) -> PathBuf {
		let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
		let path = std::env::temp_dir().join(format!("rqsync-client-{name}-{}-{nanos}", std::process::id()));
		fs::create_dir_all(&path).unwrap();
		path
	}

	#[test]
	fn collect_selected_file_uses_file_name_as_relative_path() {
		let root = test_dir("single-file");
		let file = root.join("sample.txt");
		fs::write(&file, b"hello").unwrap();

		let candidates = ClientSession::collect_selected_path(&file).unwrap();

		assert_eq!(candidates.len(), 1);
		assert_eq!(candidates[0].source_path, file);
		assert_eq!(candidates[0].rel_path, vec!["sample.txt"]);
		assert_eq!(candidates[0].uncompressed_size, 5);

		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn collect_selected_directory_keeps_root_directory_name() {
		let root = test_dir("directory");
		let selected = root.join("payload");
		fs::create_dir_all(selected.join("nested")).unwrap();
		fs::write(selected.join("nested").join("file.bin"), b"abc").unwrap();

		let candidates = ClientSession::collect_selected_path(&selected).unwrap();

		assert_eq!(candidates.len(), 1);
		assert_eq!(candidates[0].rel_path, vec!["payload", "nested", "file.bin"]);
		assert_eq!(candidates[0].uncompressed_size, 3);

		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn build_transfers_rejects_duplicate_output_paths() {
		let root = test_dir("reject-duplicates");
		let left = root.join("left");
		let right = root.join("right");
		fs::create_dir_all(&left).unwrap();
		fs::create_dir_all(&right).unwrap();
		fs::write(left.join("same.txt"), b"left").unwrap();
		fs::write(right.join("same.txt"), b"right").unwrap();

		let err = match ClientSession::build_transfers(vec![left.join("same.txt"), right.join("same.txt")], DuplicateStrategy::Reject) {
			Ok(_) => panic!("duplicate output paths should be rejected"),
			Err(err) => err,
		};

		assert!(err.to_string().contains("same resulting file"));

		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn build_transfers_enumerates_duplicate_output_paths() {
		let root = test_dir("enumerate-duplicates");
		let left = root.join("left");
		let right = root.join("right");
		fs::create_dir_all(&left).unwrap();
		fs::create_dir_all(&right).unwrap();
		fs::write(left.join("same.txt"), b"left").unwrap();
		fs::write(right.join("same.txt"), b"right").unwrap();

		let transfers = ClientSession::build_transfers(vec![left.join("same.txt"), right.join("same.txt")], DuplicateStrategy::Enumerate).unwrap();
		let mut rel_paths = transfers.into_values().map(|candidate| candidate.rel_path).collect::<Vec<_>>();
		rel_paths.sort();

		assert_eq!(rel_paths, vec![vec!["same (2).txt"], vec!["same.txt"]]);

		fs::remove_dir_all(root).unwrap();
	}

	#[tokio::test]
	async fn read_chunk_reads_requested_offset_and_length() {
		let root = test_dir("read-chunk");
		let file = root.join("data.bin");
		fs::write(&file, b"0123456789").unwrap();
		let job = ChunkJob {
			id: 7,
			source_path: file,
			offset: 3,
			uncompressed_len: 4,
		};

		let bytes = ClientSession::read_chunk(&job).await.unwrap();

		assert_eq!(&bytes[..], b"3456");

		fs::remove_dir_all(root).unwrap();
	}
}
