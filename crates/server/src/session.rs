use std::{
	collections::{
		HashMap,
		HashSet,
	},
	fmt::{
		Display,
		Formatter,
	},
	io::SeekFrom,
	path::PathBuf,
	sync::{
		Arc,
		Mutex as StdMutex,
	},
	time::{
		Duration,
		Instant,
	},
};

use anyhow::{
	Context,
	bail,
	ensure,
};
use bit_set::BitSet;
use bytes::Bytes;
use lib::{
	ArchivedClientMessage,
	ClientMessage,
	Compression,
	MAX_CONTROL_FRAME_SIZE,
	MAX_DATA_FRAME_SIZE,
	ManifestEntry,
	PacketError,
	PacketHandler,
	ReceivedChunks,
	RejectReason,
	RejectedTransfer,
	ServerMessage,
	format_bytes,
	progress_bar,
};
use quinn::{
	RecvStream,
	SendStream,
};
use rkyv::rancor;
use tokio::{
	fs::{
		self,
		OpenOptions,
	},
	io::{
		AsyncSeekExt,
		AsyncWriteExt,
	},
	sync::Mutex,
	task::JoinSet,
};
use tracing::info;

use crate::IncomingTransfer;

const RESUME_METADATA_FILE: &str = ".speedtest-resume.rkyv";
const RESUME_METADATA_TEMP_FILE: &str = ".speedtest-resume.rkyv.tmp";
const RESUME_METADATA_VERSION: u32 = 1;
const RESUME_CHECKPOINT_MIN_BYTES: u64 = 256 * 1024 * 1024;
const RESUME_CHECKPOINT_MIN_ELAPSED: Duration = Duration::from_secs(5);

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone)]
#[rkyv(derive(Debug))]
struct ResumeMetadata {
	version: u32,
	chunk_size: u64,
	files: Vec<ResumeFileMetadata>,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone)]
#[rkyv(derive(Debug))]
struct ResumeFileMetadata {
	rel_path: Vec<String>,
	uncompressed_size: u64,
	chunk_count: u64,
	bitset_bytes: Vec<u8>,
}

pub struct ServerSession {
	conn: quinn::Connection,
	control: Option<(SendStream, RecvStream)>,
	root_dir: PathBuf,
	state: ServerState,
}

impl PacketHandler for ServerSession {}

struct ReceiveProgress {
	total_bytes: u64,
	completed_bytes: u64,
	next_report_percent: u64,
}

pub struct ReceiveState {
	transfers: HashMap<u32, IncomingTransfer>,
	checkpoint: ResumeCheckpoint,
}

impl ReceiveState {
	fn new(transfers: HashMap<u32, IncomingTransfer>, resume_path: PathBuf) -> Self {
		Self {
			transfers,
			checkpoint: ResumeCheckpoint::new(resume_path),
		}
	}
}

struct ResumeCheckpoint {
	path: PathBuf,
	bytes_since_checkpoint: u64,
	last_checkpoint: Instant,
	dirty_file_ids: HashSet<u32>,
	min_bytes: u64,
	min_elapsed: Duration,
}

impl ResumeCheckpoint {
	fn new(path: PathBuf) -> Self {
		Self {
			path,
			bytes_since_checkpoint: 0,
			last_checkpoint: Instant::now(),
			dirty_file_ids: HashSet::new(),
			min_bytes: RESUME_CHECKPOINT_MIN_BYTES,
			min_elapsed: RESUME_CHECKPOINT_MIN_ELAPSED,
		}
	}

	fn record_written(&mut self, file_id: u32, bytes: u64) {
		self.dirty_file_ids.insert(file_id);
		self.bytes_since_checkpoint += bytes;
	}

	fn should_checkpoint(&self) -> bool {
		!self.dirty_file_ids.is_empty() && (self.bytes_since_checkpoint >= self.min_bytes || self.last_checkpoint.elapsed() >= self.min_elapsed)
	}

	fn reset(&mut self) {
		self.bytes_since_checkpoint = 0;
		self.last_checkpoint = Instant::now();
		self.dirty_file_ids.clear();
	}
}

impl ReceiveProgress {
	fn new(transfers: &HashMap<u32, IncomingTransfer>) -> Self {
		Self {
			total_bytes: transfers.values().map(|transfer| transfer.file_size).sum(),
			completed_bytes: transfers.values().map(|transfer| transfer.received_bytes).sum(),
			next_report_percent: 5,
		}
	}

	fn record_written(&mut self, bytes: u64) -> Option<String> {
		self.completed_bytes = (self.completed_bytes + bytes).min(self.total_bytes);
		let percent = self.percent_complete();
		if percent >= self.next_report_percent || self.completed_bytes >= self.total_bytes {
			while self.next_report_percent <= percent {
				self.next_report_percent += 5;
			}
			Some(format!(
				"Receive progress {} {}/{}",
				progress_bar(self.completed_bytes, self.total_bytes),
				format_bytes(self.completed_bytes),
				format_bytes(self.total_bytes)
			))
		} else {
			None
		}
	}

	fn percent_complete(&self) -> u64 {
		if self.total_bytes == 0 {
			100
		} else {
			(self.completed_bytes.min(self.total_bytes) * 100) / self.total_bytes
		}
	}
}

fn record_written(progress: &Arc<StdMutex<ReceiveProgress>>, bytes: u64) {
	let message = progress.lock().expect("receive progress lock poisoned").record_written(bytes);
	if let Some(message) = message {
		info!("{message}");
	}
}

impl ServerSession {
	pub fn new(conn: quinn::Connection, root_dir: PathBuf) -> Self {
		Self {
			conn,
			control: None,
			root_dir,
			state: ServerState::Handshaking,
		}
	}

	pub fn connection(&self) -> &quinn::Connection {
		&self.conn
	}

	pub async fn run(&mut self) -> anyhow::Result<()> {
		self.accept_control_stream().await?;
		self.handshake().await?;
		self.register_transfers().await?;
		self.finish_and_reconcile().await?;
		let _ = self.conn.closed().await;

		Ok(())
	}

	async fn handshake(&mut self) -> anyhow::Result<()> {
		ensure!(matches!(self.state, ServerState::Handshaking));

		match self.read_control().await? {
			ClientMessage::Hello { version } => {
				if version != lib::PROTOCOL_VERSION {
					bail!("Unsupported protocol version {version}, expected {}", lib::PROTOCOL_VERSION);
				}
				self.state = ServerState::Idle;
				self.write_control(ServerMessage::HelloAck { version }).await?;
				Ok(())
			}
			message => bail!("Unexpected message in handshake: {message:?}"),
		}
	}

	async fn register_transfers(&mut self) -> anyhow::Result<()> {
		ensure!(matches!(self.state, ServerState::Idle));

		let frame = self.read_control_frame().await?;
		let message = Self::access_message(&frame)?;

		match message {
			ArchivedClientMessage::RegisterTransfers { files, chunk_size, compression } => {
				let chunk_size = chunk_size.to_native();
				let (mut transfers, rejected) = Self::create_incoming_transfers(files.iter(), chunk_size, compression.into(), self.root_dir.clone());
				let resume_path = self.resume_metadata_path();
				let received_chunks = Self::apply_resume_metadata(&resume_path, &mut transfers, chunk_size).await?;

				let message = if transfers.is_empty() {
					self.state = ServerState::Idle;
					ServerMessage::AllTransfersRejected { rejected }
				} else {
					let accepted = transfers.keys().copied().collect::<HashSet<_>>();
					let receive_progress = Arc::new(StdMutex::new(ReceiveProgress::new(&transfers)));
					let total_bytes = receive_progress.lock().expect("receive progress lock poisoned").total_bytes;
					let completed_bytes = receive_progress.lock().expect("receive progress lock poisoned").completed_bytes;
					info!(
						"Receiving {} files ({}) with {:?} compression {}",
						transfers.len(),
						format_bytes(total_bytes),
						Compression::from(compression),
						progress_bar(completed_bytes, total_bytes)
					);
					Self::persist_resume_metadata(&resume_path, &transfers).await?;
					let receive_state = Arc::new(Mutex::new(ReceiveState::new(transfers, resume_path)));
					let mut streams = JoinSet::new();
					for _ in 0..lib::DATA_STREAM_COUNT {
						let conn = self.conn.clone();
						let receive_state = receive_state.clone();
						let receive_progress = receive_progress.clone();

						streams.spawn(async move {
							let recv = conn.accept_uni().await.context("Failed to accept uni stream")?;
							Self::receive_data_chunks(recv, receive_state, receive_progress).await
						});
					}

					self.state = ServerState::ReceivingFiles { streams, receive_state };
					ServerMessage::TransfersRegistered { accepted, rejected, received_chunks }
				};

				self.write_control(message).await?;

				Ok(())
			}
			message => bail!("Unexpected message in register transfers: {message:?}"),
		}
	}

	async fn finish_and_reconcile(&mut self) -> anyhow::Result<()> {
		let receive_state = {
			let ServerState::ReceivingFiles { ref mut streams, ref receive_state } = self.state else {
				bail!("Unexpected state in finish and reconcile: {}", self.state);
			};

			while let Some(result) = streams.join_next().await {
				result??;
			}

			receive_state.clone()
		};

		match self.read_control().await? {
			ClientMessage::TransferFinished => {}
			message => bail!("Unexpected message in finish and reconcile: {message:?}"),
		}

		Self::checkpoint_resume_metadata(&receive_state, true).await?;
		self.state = ServerState::Idle;
		let message = ServerMessage::TransferComplete;
		self.write_control(message).await?;
		self.finish_control().await?;
		Self::remove_resume_metadata(&self.resume_metadata_path()).await?;

		Ok(())
	}

	#[inline]
	fn access_message(bytes: &[u8]) -> Result<&ArchivedClientMessage, rancor::Error> {
		rkyv::access(bytes)
	}

	async fn read_control_frame(&mut self) -> anyhow::Result<Bytes> {
		let Some((_, ref mut recv)) = self.control else {
			return Err(anyhow::anyhow!("No control stream available"));
		};
		let frame = Self::read_frame(recv, MAX_CONTROL_FRAME_SIZE).await?;
		Ok(frame)
	}

	async fn read_control(&mut self) -> anyhow::Result<ClientMessage> {
		let Some((_, ref mut recv)) = self.control else { bail!("Control stream not opened yet") };
		let frame = Self::read_frame(recv, MAX_CONTROL_FRAME_SIZE).await?;
		rkyv::from_bytes::<ClientMessage, rancor::Error>(&frame).context("Failed to deserialize control message")
	}

	async fn write_control(&mut self, message: ServerMessage) -> anyhow::Result<()> {
		let Some((ref mut send, _)) = self.control else {
			return Err(anyhow::anyhow!("No control stream available"));
		};
		let message = rkyv::to_bytes::<rancor::Error>(&message).context("Failed to serialize control message")?;
		Self::write_frame(send, &message).await?;
		Ok(())
	}

	async fn finish_control(&mut self) -> anyhow::Result<()> {
		let Some((ref mut send, _)) = self.control else {
			return Err(anyhow::anyhow!("No control stream available"));
		};
		send.finish().context("Failed to finish control stream")?;
		Ok(())
	}

	async fn accept_control_stream(&mut self) -> anyhow::Result<()> {
		let (send, recv) = self.conn.accept_bi().await.context("Failed to accept bidirectional stream")?;
		self.control = Some((send, recv));
		Ok(())
	}

	async fn receive_data_chunks(mut recv: RecvStream, receive_state: Arc<Mutex<ReceiveState>>, progress: Arc<StdMutex<ReceiveProgress>>) -> anyhow::Result<()> {
		loop {
			let frame = match Self::read_frame(&mut recv, MAX_DATA_FRAME_SIZE).await {
				Ok(frame) => frame,
				Err(PacketError::ReadExactError(_)) => {
					return Ok(()); // EOF, probably
				}
				Err(err) => {
					bail!("Failed to read data frame: {err}");
				}
			};

			let message = rkyv::access::<ArchivedClientMessage, rancor::Error>(&frame)?;

			match message {
				ArchivedClientMessage::Chunk { file_id, offset, bytes } => {
					Self::write_chunk(receive_state.clone(), progress.clone(), file_id.to_native(), offset.to_native(), bytes).await?;
				}
				message => bail!("Unexpected message in data stream: {message:?}"),
			}
		}
	}

	async fn write_chunk(receive_state: Arc<Mutex<ReceiveState>>, progress: Arc<StdMutex<ReceiveProgress>>, file_id: u32, offset: u64, bytes: &[u8]) -> anyhow::Result<()> {
		let mut state = receive_state.lock().await;
		let Some(transfer) = state.transfers.get_mut(&file_id) else {
			bail!("Received chunk for unknown file id {file_id}");
		};

		if offset >= transfer.file_size {
			bail!("Received chunk with offset {offset} that is past the end of the file (size {})", transfer.file_size);
		}

		let div = offset / transfer.chunk_size;
		let rem = offset % transfer.chunk_size;

		if rem != 0 {
			bail!("Received chunk with offset {offset} not aligned to chunk size {}", transfer.chunk_size);
		}

		if transfer.received.contains(div as usize) {
			bail!("Received chunk with offset {offset} that was already received");
		}

		let expected_len = (transfer.file_size - offset).min(transfer.chunk_size) as usize;
		let bytes = transfer.compression.decompress(Bytes::copy_from_slice(bytes), expected_len).context("Failed to decompress chunk")?;

		let len = bytes.len() as u64;
		if len > transfer.chunk_size {
			bail!("Received chunk with length {len} that is larger than the chunk size {}", transfer.chunk_size);
		}

		if offset + len > transfer.file_size {
			bail!("Received chunk with offset {offset} and length {len} that is past the end of the file (size {})", transfer.file_size);
		}

		if transfer.file.is_none() {
			if let Some(parent) = transfer.path.parent() {
				tokio::fs::create_dir_all(parent).await?;
			}

			let file = OpenOptions::new().write(true).create(true).open(&transfer.path).await?;

			file.set_len(transfer.file_size).await?;
			transfer.file = Some(file);
		}

		{
			let file = transfer.file.as_mut().unwrap();
			file.seek(SeekFrom::Start(offset)).await?;
			file.write_all(&bytes).await?;
		}

		transfer.received.insert(div as usize);
		transfer.received_bytes += len;
		let wrote_file = if !transfer.completed && transfer.received_bytes >= transfer.file_size {
			transfer.file.as_mut().unwrap().sync_all().await?;
			transfer.completed = true;
			Some((transfer.path.clone(), transfer.file_size))
		} else {
			None
		};
		state.checkpoint.record_written(file_id, len);

		if let Some((path, file_size)) = wrote_file {
			info!("Wrote file {} ({})", path.display(), format_bytes(file_size));
		}
		drop(state);
		Self::checkpoint_resume_metadata(&receive_state, false).await?;
		record_written(&progress, len);

		Ok(())
	}

	async fn checkpoint_resume_metadata(receive_state: &Arc<Mutex<ReceiveState>>, force: bool) -> anyhow::Result<()> {
		let mut receive_state = receive_state.lock().await;
		if !force && !receive_state.checkpoint.should_checkpoint() {
			return Ok(());
		}

		if receive_state.checkpoint.dirty_file_ids.is_empty() {
			if force {
				Self::persist_resume_metadata(&receive_state.checkpoint.path, &receive_state.transfers).await?;
				receive_state.checkpoint.reset();
			}
			return Ok(());
		}

		let dirty_file_ids = receive_state.checkpoint.dirty_file_ids.iter().copied().collect::<Vec<_>>();
		for file_id in dirty_file_ids {
			if let Some(file) = receive_state.transfers.get_mut(&file_id).and_then(|transfer| transfer.file.as_mut()) {
				file.sync_all().await?;
			}
		}

		Self::persist_resume_metadata(&receive_state.checkpoint.path, &receive_state.transfers).await?;
		receive_state.checkpoint.reset();
		Ok(())
	}

	fn resume_metadata_path(&self) -> PathBuf {
		self.root_dir.join(RESUME_METADATA_FILE)
	}

	fn chunk_count(file_size: u64, chunk_size: u64) -> u64 {
		if file_size == 0 { 0 } else { file_size.div_ceil(chunk_size) }
	}

	fn chunk_len(file_size: u64, chunk_size: u64, chunk_index: u64) -> u64 {
		let offset = chunk_index * chunk_size;
		(file_size - offset).min(chunk_size)
	}

	fn bitset_from_bytes(bytes: &[u8], chunk_count: u64) -> BitSet<u64> {
		let restored = BitSet::<u64>::from_bytes_general(bytes);
		let mut received = BitSet::<u64>::default();
		received.reserve_len(chunk_count as usize);
		for chunk in restored.iter().filter(|&chunk| (chunk as u64) < chunk_count) {
			received.insert(chunk);
		}
		received
	}

	fn received_bytes(received: &BitSet<u64>, file_size: u64, chunk_size: u64) -> u64 {
		received.iter().map(|chunk| Self::chunk_len(file_size, chunk_size, chunk as u64)).sum::<u64>().min(file_size)
	}

	fn metadata_from_transfers(transfers: &HashMap<u32, IncomingTransfer>) -> ResumeMetadata {
		let chunk_size = transfers.values().next().map_or(0, |transfer| transfer.chunk_size);
		let mut files = transfers
			.values()
			.map(|transfer| ResumeFileMetadata {
				rel_path: transfer.rel_path.clone(),
				uncompressed_size: transfer.file_size,
				chunk_count: Self::chunk_count(transfer.file_size, transfer.chunk_size),
				bitset_bytes: transfer.received.get_ref().to_bytes(),
			})
			.collect::<Vec<_>>();
		files.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));

		ResumeMetadata {
			version: RESUME_METADATA_VERSION,
			chunk_size,
			files,
		}
	}

	fn metadata_matches(metadata: &ResumeMetadata, transfers: &HashMap<u32, IncomingTransfer>, chunk_size: u64) -> bool {
		if metadata.version != RESUME_METADATA_VERSION || metadata.chunk_size != chunk_size || metadata.files.len() != transfers.len() {
			return false;
		}

		let expected = transfers.values().map(|transfer| (transfer.rel_path.clone(), transfer.file_size)).collect::<HashSet<_>>();
		let actual = metadata.files.iter().map(|file| (file.rel_path.clone(), file.uncompressed_size)).collect::<HashSet<_>>();
		expected == actual
	}

	async fn load_resume_metadata(path: &std::path::Path) -> anyhow::Result<Option<ResumeMetadata>> {
		let bytes = match fs::read(path).await {
			Ok(bytes) => bytes,
			Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
			Err(err) => return Err(err).context("Failed to read resume metadata"),
		};

		let metadata = rkyv::from_bytes::<ResumeMetadata, rancor::Error>(&bytes).context("Failed to deserialize resume metadata")?;
		Ok(Some(metadata))
	}

	async fn apply_resume_metadata(path: &std::path::Path, transfers: &mut HashMap<u32, IncomingTransfer>, chunk_size: u64) -> anyhow::Result<Vec<ReceivedChunks>> {
		let Some(metadata) = Self::load_resume_metadata(path).await? else {
			return Ok(Vec::new());
		};

		if !Self::metadata_matches(&metadata, transfers, chunk_size) {
			return Ok(Vec::new());
		}

		let mut by_rel_path = metadata.files.into_iter().map(|file| (file.rel_path.clone(), file)).collect::<HashMap<_, _>>();
		let mut received_chunks = Vec::new();
		for (&file_id, transfer) in transfers.iter_mut() {
			let Some(file) = by_rel_path.remove(&transfer.rel_path) else {
				continue;
			};
			let chunk_count = Self::chunk_count(transfer.file_size, transfer.chunk_size);
			let received = Self::bitset_from_bytes(&file.bitset_bytes, chunk_count);
			transfer.received_bytes = Self::received_bytes(&received, transfer.file_size, transfer.chunk_size);
			transfer.completed = transfer.received_bytes >= transfer.file_size && transfer.file_size > 0;
			transfer.received = received;
			received_chunks.push(ReceivedChunks {
				file_id,
				chunk_count,
				bitset_bytes: transfer.received.get_ref().to_bytes(),
			});
		}

		Ok(received_chunks)
	}

	async fn persist_resume_metadata(path: &std::path::Path, transfers: &HashMap<u32, IncomingTransfer>) -> anyhow::Result<()> {
		let metadata = Self::metadata_from_transfers(transfers);
		let bytes = rkyv::to_bytes::<rancor::Error>(&metadata).context("Failed to serialize resume metadata")?;
		let temp_path = path.with_file_name(RESUME_METADATA_TEMP_FILE);
		fs::write(&temp_path, &bytes).await.context("Failed to write resume metadata")?;
		fs::rename(&temp_path, path).await.context("Failed to replace resume metadata")?;
		Ok(())
	}

	async fn remove_resume_metadata(path: &std::path::Path) -> anyhow::Result<()> {
		match fs::remove_file(path).await {
			Ok(()) => Ok(()),
			Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
			Err(err) => Err(err).context("Failed to remove resume metadata"),
		}
	}

	fn create_incoming_transfers<'a, I, E>(files: I, chunk_size: u64, compression: Compression, root_dir: PathBuf) -> (HashMap<u32, IncomingTransfer>, Vec<RejectedTransfer>)
	where
		I: ExactSizeIterator<Item = &'a E>,
		E: ManifestEntry + 'a,
	{
		let files = files.into_iter();
		let mut map = HashMap::with_capacity(files.len()); // Happy path is pre-allocated since this is what we expect to happen most of the time
		let mut seen_ids = HashSet::with_capacity(files.len());
		let mut seen_paths = HashSet::with_capacity(files.len());
		let mut rejected = HashMap::new();
		for entry in files {
			if !seen_ids.insert(entry.id()) {
				rejected.insert(entry.id(), RejectReason::DuplicateId);
				continue;
			}

			for i in 0..entry.rel_path_len() {
				if Self::is_path_banned(entry.rel_path_component(i)) {
					rejected.insert(entry.id(), RejectReason::InvalidPath);
					continue;
				}
			}

			let mut path = root_dir.clone();
			let mut rel_path = Vec::with_capacity(entry.rel_path_len());
			for i in 0..entry.rel_path_len() {
				let component = entry.rel_path_component(i);
				path.push(component);
				rel_path.push(component.to_owned());
			}

			if !seen_paths.insert(path.clone()) {
				rejected.insert(entry.id(), RejectReason::DuplicatePath);
				continue;
			}

			let transfer = IncomingTransfer {
				path,
				rel_path,
				file_size: entry.uncompressed_size(),
				chunk_size,
				compression,
				received_bytes: 0,
				completed: false,
				file: None,
				received: Default::default(),
			};
			map.insert(entry.id(), transfer);
		}
		// Sanitation
		// We want to go back and remove any transfer that were rejected for duplication, since a duplicate means it was already seen and might've been inserted to the map
		for (id, _) in rejected.iter() {
			map.remove(id);
		}

		// Having the rejected transfers as a map is more convenient for O(1) access
		(map, rejected.into_iter().map(RejectedTransfer::from).collect())
	}

	fn is_path_banned(path: &str) -> bool {
		path.is_empty() || path == ".." || path == "." || path.contains(['\\', '/', '\0']) || (cfg!(windows) && Self::is_path_banned_windows(path))
	}

	// This is a subset of the rules in https://docs.microsoft.com/en-us/windows/win32/fileio/naming-a-file
	fn is_path_banned_windows(path: &str) -> bool {
		path.contains([':', '<', '>', '"', '|', '?', '*']) // The drive patterns (e.g. C:\) are handled by the \\ check above and the : check here.
			|| path.ends_with(' ')
			|| path.ends_with('.')
			|| matches!(
				// Reserved device names
				path.split_once('.').map_or(path, |(stem, _)| stem).to_ascii_uppercase().as_str(),
			// The superscript ¹, ², and ³ are also recognized as part of the device name according to the doc link above.
				"CON" | "PRN" | "AUX" | "NUL" | "COM1" | "COM2" | "COM3" | "COM4" | "COM5" | "COM6" | "COM7" | "COM8" | "COM9" | "COM¹" | "COM²" |
				"COM³" | "LPT1" | "LPT2" | "LPT3" | "LPT4" | "LPT5" | "LPT6" | "LPT7" | "LPT8" | "LPT9" | "LPT¹" | "LPT²" | "LPT³"
			)
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

	use bit_set::BitSet;
	use lib::FileManifestEntry;

	use super::*;

	fn test_dir(name: &str) -> PathBuf {
		let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
		let path = std::env::temp_dir().join(format!("speedtest-server-{name}-{}-{nanos}", std::process::id()));
		fs::create_dir_all(&path).unwrap();
		path
	}

	fn manifest(id: u32, rel_path: &[&str], size: u64) -> FileManifestEntry {
		FileManifestEntry {
			id,
			uncompressed_size: size,
			rel_path: rel_path.iter().map(|part| (*part).to_owned()).collect(),
		}
	}

	fn rejected_reason(rejected: &[RejectedTransfer], id: u32) -> Option<RejectReason> {
		rejected.iter().find(|entry| entry.id == id).map(|entry| entry.reason.clone())
	}

	fn receive_progress(total_bytes: u64) -> Arc<StdMutex<ReceiveProgress>> {
		Arc::new(StdMutex::new(ReceiveProgress {
			total_bytes,
			completed_bytes: 0,
			next_report_percent: 5,
		}))
	}

	fn receive_state(transfers: HashMap<u32, IncomingTransfer>, resume_path: PathBuf) -> Arc<Mutex<ReceiveState>> {
		Arc::new(Mutex::new(ReceiveState::new(transfers, resume_path)))
	}

	fn incoming(root: &std::path::Path, rel_path: &[&str], size: u64, chunk_size: u64, compression: Compression, received: BitSet<u64>) -> IncomingTransfer {
		let mut path = root.to_path_buf();
		let rel_path = rel_path.iter().map(|part| (*part).to_owned()).collect::<Vec<_>>();
		for component in &rel_path {
			path.push(component);
		}
		let received_bytes = ServerSession::received_bytes(&received, size, chunk_size);
		IncomingTransfer {
			path,
			rel_path,
			file_size: size,
			chunk_size,
			compression,
			received_bytes,
			completed: false,
			file: None,
			received,
		}
	}

	#[test]
	fn create_incoming_transfers_accepts_valid_manifest_entries() {
		let root = test_dir("accepts-valid");
		let files = vec![manifest(10, &["nested", "file.txt"], 12)];

		let (transfers, rejected) = ServerSession::create_incoming_transfers(files.iter(), 4, Compression::None, root.clone());

		assert!(rejected.is_empty());
		assert_eq!(transfers.len(), 1);
		let transfer = transfers.get(&10).unwrap();
		assert_eq!(transfer.path, root.join("nested").join("file.txt"));
		assert_eq!(transfer.file_size, 12);
		assert_eq!(transfer.chunk_size, 4);
		assert_eq!(transfer.compression, Compression::None);

		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn create_incoming_transfers_rejects_invalid_and_duplicate_entries() {
		let root = test_dir("rejects-invalid");
		let files = vec![
			manifest(1, &["ok.txt"], 1),
			manifest(1, &["other.txt"], 1),
			manifest(2, &["..", "escape.txt"], 1),
			manifest(3, &["ok.txt"], 1),
		];

		let (transfers, rejected) = ServerSession::create_incoming_transfers(files.iter(), 4, Compression::LZ4, root.clone());

		assert!(transfers.is_empty());
		assert!(matches!(rejected_reason(&rejected, 1), Some(RejectReason::DuplicateId)));
		assert!(matches!(rejected_reason(&rejected, 2), Some(RejectReason::InvalidPath)));
		assert!(matches!(rejected_reason(&rejected, 3), Some(RejectReason::DuplicatePath)));

		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn resume_metadata_matches_paths_sizes_and_chunk_size_but_ignores_compression() {
		let root = test_dir("resume-match");
		let mut transfers = HashMap::new();
		transfers.insert(1, incoming(&root, &["file.bin"], 10, 4, Compression::LZ4, BitSet::default()));
		let metadata = ResumeMetadata {
			version: RESUME_METADATA_VERSION,
			chunk_size: 4,
			files: vec![ResumeFileMetadata {
				rel_path: vec!["file.bin".to_owned()],
				uncompressed_size: 10,
				chunk_count: 3,
				bitset_bytes: Vec::new(),
			}],
		};

		assert!(ServerSession::metadata_matches(&metadata, &transfers, 4));

		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn resume_metadata_mismatch_starts_fresh_for_path_size_or_chunk_size_change() {
		let root = test_dir("resume-mismatch");
		let mut transfers = HashMap::new();
		transfers.insert(1, incoming(&root, &["file.bin"], 10, 4, Compression::None, BitSet::default()));
		let mut metadata = ResumeMetadata {
			version: RESUME_METADATA_VERSION,
			chunk_size: 4,
			files: vec![ResumeFileMetadata {
				rel_path: vec!["file.bin".to_owned()],
				uncompressed_size: 10,
				chunk_count: 3,
				bitset_bytes: Vec::new(),
			}],
		};

		metadata.files[0].rel_path = vec!["other.bin".to_owned()];
		assert!(!ServerSession::metadata_matches(&metadata, &transfers, 4));

		metadata.files[0].rel_path = vec!["file.bin".to_owned()];
		metadata.files[0].uncompressed_size = 11;
		assert!(!ServerSession::metadata_matches(&metadata, &transfers, 4));

		metadata.files[0].uncompressed_size = 10;
		assert!(!ServerSession::metadata_matches(&metadata, &transfers, 8));

		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn bitset_round_trip_ignores_padded_bits_past_chunk_count() {
		let mut bitset = BitSet::<u64>::default();
		bitset.insert(0);
		bitset.insert(2);
		bitset.insert(7);
		let bytes = bitset.get_ref().to_bytes();

		let restored = ServerSession::bitset_from_bytes(&bytes, 5);

		assert!(restored.contains(0));
		assert!(restored.contains(2));
		assert!(!restored.contains(7));
	}

	#[tokio::test]
	async fn resumed_registration_seeds_received_and_received_bytes() {
		let root = test_dir("resume-seed");
		let resume_path = root.join(RESUME_METADATA_FILE);
		let mut bitset = BitSet::<u64>::default();
		bitset.insert(0);
		bitset.insert(2);
		let metadata = ResumeMetadata {
			version: RESUME_METADATA_VERSION,
			chunk_size: 4,
			files: vec![ResumeFileMetadata {
				rel_path: vec!["file.bin".to_owned()],
				uncompressed_size: 10,
				chunk_count: 3,
				bitset_bytes: bitset.get_ref().to_bytes(),
			}],
		};
		let bytes = rkyv::to_bytes::<rancor::Error>(&metadata).unwrap();
		fs::write(&resume_path, bytes).unwrap();
		let mut transfers = HashMap::new();
		transfers.insert(9, incoming(&root, &["file.bin"], 10, 4, Compression::LZ4, BitSet::default()));

		let received_chunks = ServerSession::apply_resume_metadata(&resume_path, &mut transfers, 4).await.unwrap();

		let transfer = transfers.get(&9).unwrap();
		assert!(transfer.received.contains(0));
		assert!(transfer.received.contains(2));
		assert_eq!(transfer.received_bytes, 6);
		assert_eq!(received_chunks.len(), 1);
		assert_eq!(received_chunks[0].file_id, 9);
		assert_eq!(received_chunks[0].chunk_count, 3);

		fs::remove_dir_all(root).unwrap();
	}

	#[tokio::test]
	async fn write_chunk_creates_file_and_records_received_chunk() {
		let root = test_dir("write-chunk");
		let path = root.join("nested").join("data.bin");
		let mut transfer = IncomingTransfer {
			path: path.clone(),
			rel_path: vec!["nested".to_owned(), "data.bin".to_owned()],
			file_size: 8,
			chunk_size: 4,
			compression: Compression::None,
			received_bytes: 0,
			completed: false,
			file: None,
			received: BitSet::default(),
		};
		transfer.received.reserve_len(2);
		let mut transfers = HashMap::new();
		transfers.insert(1, transfer);
		let receive_state = receive_state(transfers, root.join(RESUME_METADATA_FILE));
		let progress = receive_progress(8);

		ServerSession::write_chunk(receive_state.clone(), progress, 1, 4, b"test").await.unwrap();
		ServerSession::checkpoint_resume_metadata(&receive_state, true).await.unwrap();

		assert_eq!(fs::read(&path).unwrap(), b"\0\0\0\0test");
		assert!(receive_state.lock().await.transfers.get(&1).unwrap().received.contains(1));

		fs::remove_dir_all(root).unwrap();
	}

	#[tokio::test]
	async fn write_chunk_does_not_checkpoint_before_thresholds() {
		let root = test_dir("checkpoint-waits");
		let path = root.join("data.bin");
		let transfer = IncomingTransfer {
			path,
			rel_path: vec!["data.bin".to_owned()],
			file_size: 8,
			chunk_size: 4,
			compression: Compression::None,
			received_bytes: 0,
			completed: false,
			file: None,
			received: BitSet::default(),
		};
		let mut transfers = HashMap::new();
		transfers.insert(1, transfer);
		let resume_path = root.join(RESUME_METADATA_FILE);
		let receive_state = receive_state(transfers, resume_path.clone());
		let progress = receive_progress(8);

		ServerSession::write_chunk(receive_state, progress, 1, 0, b"test").await.unwrap();

		assert!(!resume_path.exists());

		fs::remove_dir_all(root).unwrap();
	}

	#[tokio::test]
	async fn forced_checkpoint_persists_metadata_and_clears_dirty_state() {
		let root = test_dir("checkpoint-force");
		let path = root.join("data.bin");
		let transfer = IncomingTransfer {
			path,
			rel_path: vec!["data.bin".to_owned()],
			file_size: 8,
			chunk_size: 4,
			compression: Compression::None,
			received_bytes: 0,
			completed: false,
			file: None,
			received: BitSet::default(),
		};
		let mut transfers = HashMap::new();
		transfers.insert(1, transfer);
		let resume_path = root.join(RESUME_METADATA_FILE);
		let receive_state = receive_state(transfers, resume_path.clone());
		let progress = receive_progress(8);

		ServerSession::write_chunk(receive_state.clone(), progress, 1, 0, b"test").await.unwrap();
		ServerSession::checkpoint_resume_metadata(&receive_state, true).await.unwrap();

		let metadata = ServerSession::load_resume_metadata(&resume_path).await.unwrap().unwrap();
		let received = ServerSession::bitset_from_bytes(&metadata.files[0].bitset_bytes, 2);
		assert!(received.contains(0));
		let state = receive_state.lock().await;
		assert_eq!(state.checkpoint.bytes_since_checkpoint, 0);
		assert!(state.checkpoint.dirty_file_ids.is_empty());

		fs::remove_dir_all(root).unwrap();
	}

	#[tokio::test]
	async fn completed_file_is_synced_marked_complete_and_readable() {
		let root = test_dir("complete-file");
		let path = root.join("data.bin");
		let transfer = IncomingTransfer {
			path: path.clone(),
			rel_path: vec!["data.bin".to_owned()],
			file_size: 4,
			chunk_size: 4,
			compression: Compression::None,
			received_bytes: 0,
			completed: false,
			file: None,
			received: BitSet::default(),
		};
		let mut transfers = HashMap::new();
		transfers.insert(1, transfer);
		let receive_state = receive_state(transfers, root.join(RESUME_METADATA_FILE));
		let progress = receive_progress(4);

		ServerSession::write_chunk(receive_state.clone(), progress, 1, 0, b"done").await.unwrap();

		assert_eq!(fs::read(&path).unwrap(), b"done");
		assert!(receive_state.lock().await.transfers.get(&1).unwrap().completed);

		fs::remove_dir_all(root).unwrap();
	}

	#[tokio::test]
	async fn write_chunk_rejects_duplicate_and_misaligned_chunks() {
		let root = test_dir("reject-chunks");
		let path = root.join("data.bin");
		let transfer = IncomingTransfer {
			path,
			rel_path: vec!["data.bin".to_owned()],
			file_size: 8,
			chunk_size: 4,
			compression: Compression::None,
			received_bytes: 0,
			completed: false,
			file: None,
			received: BitSet::default(),
		};
		let mut transfers = HashMap::new();
		transfers.insert(1, transfer);
		let receive_state = receive_state(transfers, root.join(RESUME_METADATA_FILE));
		let progress = receive_progress(8);

		ServerSession::write_chunk(receive_state.clone(), progress.clone(), 1, 0, b"abcd").await.unwrap();
		let duplicate = ServerSession::write_chunk(receive_state.clone(), progress.clone(), 1, 0, b"abcd").await.unwrap_err();
		let misaligned = ServerSession::write_chunk(receive_state, progress, 1, 2, b"ab").await.unwrap_err();

		assert!(duplicate.to_string().contains("already received"));
		assert!(misaligned.to_string().contains("not aligned"));

		fs::remove_dir_all(root).unwrap();
	}

	#[tokio::test]
	async fn write_chunk_decompresses_lz4_before_writing() {
		let root = test_dir("write-compressed-chunk");
		let path = root.join("data.bin");
		let transfer = IncomingTransfer {
			path: path.clone(),
			rel_path: vec!["data.bin".to_owned()],
			file_size: 16,
			chunk_size: 16,
			compression: Compression::LZ4,
			received_bytes: 0,
			completed: false,
			file: None,
			received: BitSet::default(),
		};
		let mut transfers = HashMap::new();
		transfers.insert(1, transfer);
		let receive_state = receive_state(transfers, root.join(RESUME_METADATA_FILE));
		let progress = receive_progress(16);
		let compressed = Compression::LZ4.compress(b"aaaaaaaaaaaaaaaa"[..].into());

		ServerSession::write_chunk(receive_state, progress, 1, 0, &compressed).await.unwrap();

		assert_eq!(fs::read(&path).unwrap(), b"aaaaaaaaaaaaaaaa");

		fs::remove_dir_all(root).unwrap();
	}
}

pub enum ServerState {
	Handshaking,
	Idle,
	RegisteringTransfers,
	ReceivingFiles {
		streams: JoinSet<anyhow::Result<()>>,
		receive_state: Arc<Mutex<ReceiveState>>,
	},
	ReportingMissingChunks,
}

impl Display for ServerState {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		match self {
			ServerState::Handshaking => write!(f, "Handshaking"),
			ServerState::Idle => write!(f, "Idle"),
			ServerState::RegisteringTransfers => write!(f, "RegisteringTransfers"),
			ServerState::ReceivingFiles { .. } => write!(f, "ReceivingFiles"),
			ServerState::ReportingMissingChunks => write!(f, "ReportingMissingChunks"),
		}
	}
}
