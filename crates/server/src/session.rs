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
	sync::Arc,
};

use anyhow::{
	bail,
	Context,
};
use bytes::Bytes;
use lib::{
	ArchivedClientMessage,
	ClientMessage,
	Compression,
	FileManifestEntry,
	PacketError,
	PacketHandler,
	RejectReason,
	RejectedTransfer,
	ServerMessage,
	MAX_CONTROL_FRAME_SIZE,
	MAX_DATA_FRAME_SIZE,
};
use quinn::{
	RecvStream,
	SendStream,
};
use rkyv::rancor;
use tokio::{
	fs::OpenOptions,
	io::{
		AsyncSeekExt,
		AsyncWriteExt,
	},
	sync::Mutex,
	task::JoinSet,
};

use crate::IncomingTransfer;

pub struct ServerSession {
	conn: quinn::Connection,
	control: Option<(SendStream, RecvStream)>,
	root_dir: PathBuf,
	state: ServerState,
}

impl PacketHandler for ServerSession {}

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
		loop {
			let msg = self.read_control().await?;
			self.handle_message(msg).await?;
		}
	}

	async fn read_control(&mut self) -> anyhow::Result<ClientMessage> {
		let Some((_, ref mut recv)) = self.control else {
			return Err(anyhow::anyhow!("No control stream available"));
		};
		let frame = Self::read_frame(recv, MAX_CONTROL_FRAME_SIZE).await?;
		let message = rkyv::from_bytes::<ClientMessage, rancor::Error>(&frame).context("Failed to deserialize control message")?;
		Ok(message)
	}

	async fn write_control(&mut self, message: ServerMessage) -> anyhow::Result<()> {
		let Some((ref mut send, _)) = self.control else {
			return Err(anyhow::anyhow!("No control stream available"));
		};
		let message = rkyv::to_bytes::<rancor::Error>(&message).context("Failed to serialize control message")?;
		Self::write_frame(send, &message).await?;
		Ok(())
	}

	async fn accept_control_stream(&mut self) -> anyhow::Result<()> {
		let (send, recv) = self.conn.accept_bi().await.context("Failed to accept bidirectional stream")?;
		self.control = Some((send, recv));
		Ok(())
	}

	async fn handle_message(&mut self, message: ClientMessage) -> anyhow::Result<()> {
		let state = std::mem::replace(&mut self.state, ServerState::RegisteringTransfers);
		match (state, message) {
			(ServerState::Handshaking, ClientMessage::Hello { version }) => {
				if version != lib::PROTOCOL_VERSION {
					bail!("Unsupported protocol version {version}, expected {}", lib::PROTOCOL_VERSION);
				}
				self.state = ServerState::Idle;
				self.write_control(ServerMessage::HelloAck { version }).await
			}
			(ServerState::Idle, ClientMessage::RegisterTransfers { files, chunk_size, compression }) => {
				let (transfers, rejected) = Self::create_incoming_transfers(files, chunk_size, compression, self.root_dir.clone());

				let message = if transfers.is_empty() {
					self.state = ServerState::Idle;
					ServerMessage::AllTransfersRejected { rejected }
				} else {
					let accepted = transfers.keys().copied().collect::<HashSet<_>>();
					let transfers = Arc::new(Mutex::new(transfers));
					let mut streams = JoinSet::new();
					for _ in 0..lib::DATA_STREAM_COUNT {
						let conn = self.conn.clone();
						let transfers = transfers.clone();

						streams.spawn(async move {
							let recv = conn.accept_uni().await.context("Failed to accept uni stream")?;
							Self::receive_data_chunks(recv, transfers).await
						});
					}

					self.state = ServerState::ReceivingFiles { streams };
					ServerMessage::TransfersRegistered { accepted, rejected }
				};

				self.write_control(message).await?;

				Ok(())
			}
			(ServerState::ReceivingFiles { mut streams }, ClientMessage::TransferFinished) => {
				while let Some(result) = streams.join_next().await {
					result??;
				}

				self.state = ServerState::Idle;
				let message = ServerMessage::TransferComplete;
				self.write_control(message).await?;

				Ok(())
			}

			(state, message) => {
				let error = anyhow::anyhow!("Unexpected message in {state} state: {message:?}");
				self.state = state;
				Err(error)
			}
		}
	}

	async fn receive_data_chunks(mut recv: RecvStream, transfers: Arc<Mutex<HashMap<u32, IncomingTransfer>>>) -> anyhow::Result<()> {
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
					Self::write_chunk(transfers.clone(), file_id.to_native(), offset.to_native(), bytes).await?;
				}
				message => bail!("Unexpected message in data stream: {message:?}"),
			}
		}
	}

	async fn write_chunk(transfers: Arc<Mutex<HashMap<u32, IncomingTransfer>>>, file_id: u32, offset: u64, bytes: &[u8]) -> anyhow::Result<()> {
		let mut transfers = transfers.lock().await;
		let Some(transfer) = transfers.get_mut(&file_id) else {
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

		let file = transfer.file.as_mut().unwrap();
		file.seek(SeekFrom::Start(offset)).await?;
		file.write_all(&bytes).await?;
		file.flush().await?;

		transfer.received.insert(div as usize);

		Ok(())
	}

	fn create_incoming_transfers(files: Vec<FileManifestEntry>, chunk_size: u64, compression: Compression, root_dir: PathBuf) -> (HashMap<u32, IncomingTransfer>, Vec<RejectedTransfer>) {
		let mut map = HashMap::with_capacity(files.len()); // Happy path is pre-allocated since this is what we expect to happen most of the time
		let mut seen_ids = HashSet::with_capacity(files.len());
		let mut seen_paths = HashSet::with_capacity(files.len());
		let mut rejected = HashMap::new();
		for entry in files {
			if !seen_ids.insert(entry.id) {
				rejected.insert(entry.id, RejectReason::DuplicateId);
				continue;
			}

			if entry.rel_path.iter().any(Self::is_path_banned) {
				rejected.insert(entry.id, RejectReason::InvalidPath);
				continue;
			}

			let mut path = root_dir.clone();
			path.extend(entry.rel_path);

			if !seen_paths.insert(path.clone()) {
				rejected.insert(entry.id, RejectReason::DuplicatePath);
				continue;
			}

			let transfer = IncomingTransfer {
				path,
				file_size: entry.uncompressed_size,
				chunk_size,
				compression,
				file: None,
				received: Default::default(),
			};
			map.insert(entry.id, transfer);
		}
		// Sanitation
		// We want to go back and remove any transfer that were rejected for duplication, since a duplicate means it was already seen and might've been inserted to the map
		for (id, _) in rejected.iter() {
			map.remove(id);
		}

		// Having the rejected transfers as a map is more convenient for O(1) access
		(map, rejected.into_iter().map(RejectedTransfer::from).collect())
	}

	fn is_path_banned(path: &String) -> bool {
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

	#[test]
	fn create_incoming_transfers_accepts_valid_manifest_entries() {
		let root = test_dir("accepts-valid");
		let files = vec![manifest(10, &["nested", "file.txt"], 12)];

		let (transfers, rejected) = ServerSession::create_incoming_transfers(files, 4, Compression::None, root.clone());

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

		let (transfers, rejected) = ServerSession::create_incoming_transfers(files, 4, Compression::LZ4, root.clone());

		assert!(transfers.is_empty());
		assert!(matches!(rejected_reason(&rejected, 1), Some(RejectReason::DuplicateId)));
		assert!(matches!(rejected_reason(&rejected, 2), Some(RejectReason::InvalidPath)));
		assert!(matches!(rejected_reason(&rejected, 3), Some(RejectReason::DuplicatePath)));

		fs::remove_dir_all(root).unwrap();
	}

	#[tokio::test]
	async fn write_chunk_creates_file_and_records_received_chunk() {
		let root = test_dir("write-chunk");
		let path = root.join("nested").join("data.bin");
		let mut transfer = IncomingTransfer {
			path: path.clone(),
			file_size: 8,
			chunk_size: 4,
			compression: Compression::None,
			file: None,
			received: BitSet::default(),
		};
		transfer.received.reserve_len(2);
		let mut transfers = HashMap::new();
		transfers.insert(1, transfer);
		let transfers = Arc::new(Mutex::new(transfers));

		ServerSession::write_chunk(transfers.clone(), 1, 4, b"test").await.unwrap();

		assert_eq!(fs::read(&path).unwrap(), b"\0\0\0\0test");
		assert!(transfers.lock().await.get(&1).unwrap().received.contains(1));

		fs::remove_dir_all(root).unwrap();
	}

	#[tokio::test]
	async fn write_chunk_rejects_duplicate_and_misaligned_chunks() {
		let root = test_dir("reject-chunks");
		let path = root.join("data.bin");
		let transfer = IncomingTransfer {
			path,
			file_size: 8,
			chunk_size: 4,
			compression: Compression::None,
			file: None,
			received: BitSet::default(),
		};
		let mut transfers = HashMap::new();
		transfers.insert(1, transfer);
		let transfers = Arc::new(Mutex::new(transfers));

		ServerSession::write_chunk(transfers.clone(), 1, 0, b"abcd").await.unwrap();
		let duplicate = ServerSession::write_chunk(transfers.clone(), 1, 0, b"abcd").await.unwrap_err();
		let misaligned = ServerSession::write_chunk(transfers, 1, 2, b"ab").await.unwrap_err();

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
			file_size: 16,
			chunk_size: 16,
			compression: Compression::LZ4,
			file: None,
			received: BitSet::default(),
		};
		let mut transfers = HashMap::new();
		transfers.insert(1, transfer);
		let transfers = Arc::new(Mutex::new(transfers));
		let compressed = Compression::LZ4.compress(b"aaaaaaaaaaaaaaaa"[..].into());

		ServerSession::write_chunk(transfers, 1, 0, &compressed).await.unwrap();

		assert_eq!(fs::read(&path).unwrap(), b"aaaaaaaaaaaaaaaa");

		fs::remove_dir_all(root).unwrap();
	}
}

pub enum ServerState {
	Handshaking,
	Idle,
	RegisteringTransfers,
	ReceivingFiles { streams: JoinSet<anyhow::Result<()>> },
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
