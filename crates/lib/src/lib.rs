pub mod cert;
pub mod compression;

use std::{
	collections::HashSet,
	time::Duration,
};

use bytes::{
	Bytes,
	BytesMut,
};
pub use compression::Compression;
use quinn::{
	TransportConfig,
	VarInt,
};
use thiserror::Error;

pub const PROTOCOL_VERSION: u32 = 2;
pub const DATA_STREAM_COUNT: usize = 8;
pub const MAX_CONTROL_FRAME_SIZE: usize = 1024 * 1024;
pub const MAX_DATA_FRAME_SIZE: usize = 16 * 1024 * 1024;

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone)]
#[rkyv(derive(Debug))]
pub enum ClientMessage {
	Hello {
		version: u32,
	},
	RegisterTransfers {
		files: Vec<FileManifestEntry>,
		chunk_size: u64,
		compression: Compression,
	},
	Chunk {
		file_id: u32,
		offset: u64,
		bytes: Bytes,
	},
	TransferFinished,
	ResendChunks {
		file_id: u32,
		offset: u64,
		bytes: Bytes,
	},
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone)]
#[rkyv(derive(Debug))]
pub enum ServerMessage {
	HelloAck {
		version: u32,
	},
	AllTransfersRejected {
		rejected: Vec<RejectedTransfer>,
	},
	TransfersRegistered {
		accepted: HashSet<u32>,
		rejected: Vec<RejectedTransfer>,
		received_chunks: Vec<ReceivedChunks>,
	},
	TransferComplete,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone)]
#[rkyv(derive(Debug))]
pub struct FileManifestEntry {
	pub id: u32,
	pub uncompressed_size: u64,
	// Last entry is the file name with extension included
	pub rel_path: Vec<String>,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone)]
#[rkyv(derive(Debug))]
pub struct ReceivedChunks {
	pub file_id: u32,
	pub chunk_count: u64,
	pub bitset_bytes: Vec<u8>,
}

pub trait ManifestEntry {
	fn id(&self) -> u32;
	fn uncompressed_size(&self) -> u64;
	fn rel_path_len(&self) -> usize;
	fn rel_path_component(&self, index: usize) -> &str;
}

impl ManifestEntry for FileManifestEntry {
	#[inline(always)]
	fn id(&self) -> u32 {
		self.id
	}

	#[inline(always)]
	fn uncompressed_size(&self) -> u64 {
		self.uncompressed_size
	}

	#[inline(always)]
	fn rel_path_len(&self) -> usize {
		self.rel_path.len()
	}

	#[inline(always)]
	fn rel_path_component(&self, index: usize) -> &str {
		&self.rel_path[index]
	}
}

impl ManifestEntry for ArchivedFileManifestEntry {
	#[inline(always)]
	fn id(&self) -> u32 {
		self.id.to_native()
	}

	#[inline(always)]
	fn uncompressed_size(&self) -> u64 {
		self.uncompressed_size.to_native()
	}

	#[inline(always)]
	fn rel_path_len(&self) -> usize {
		self.rel_path.len()
	}

	#[inline(always)]
	fn rel_path_component(&self, index: usize) -> &str {
		&self.rel_path[index]
	}
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone)]
#[rkyv(derive(Debug))]
pub struct RejectedTransfer {
	pub id: u32,
	pub reason: RejectReason,
}

impl From<(u32, RejectReason)> for RejectedTransfer {
	fn from((id, reason): (u32, RejectReason)) -> Self {
		Self { id, reason }
	}
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone)]
#[repr(u8)]
#[rkyv(derive(Debug))]
pub enum RejectReason {
	InvalidPath,
	DuplicateId,
	DuplicatePath,
	ParentDirFailed,
	FileTooLarge,
	UnsupportedFileType,
}

const FLOW_WINDOW: u64 = 512 * 1024 * 1024;

pub fn transport_config() -> TransportConfig {
	let mut transport = TransportConfig::default();
	transport
		.max_concurrent_uni_streams(VarInt::from_u32(1024))
		.max_concurrent_bidi_streams(VarInt::from_u32(16))
		.stream_receive_window(VarInt::from_u32(FLOW_WINDOW as u32))
		.receive_window(VarInt::from_u32(FLOW_WINDOW as u32))
		.send_window(FLOW_WINDOW)
		.send_fairness(false)
		.keep_alive_interval(Some(Duration::from_secs(1)));
	transport
}

pub fn format_bytes(bytes: u64) -> String {
	const KIB: f64 = 1024.0;
	const MIB: f64 = KIB * 1024.0;
	const GIB: f64 = MIB * 1024.0;

	let bytes = bytes as f64;
	if bytes >= GIB {
		format!("{:.2} GiB", bytes / GIB)
	} else if bytes >= MIB {
		format!("{:.2} MiB", bytes / MIB)
	} else if bytes >= KIB {
		format!("{:.2} KiB", bytes / KIB)
	} else {
		format!("{bytes:.0} B")
	}
}

pub fn progress_bar(completed: u64, total: u64) -> String {
	const WIDTH: usize = 24;

	let filled = if total == 0 {
		WIDTH
	} else {
		((completed.min(total) as u128 * WIDTH as u128) / total as u128) as usize
	};
	let percent = if total == 0 { 100.0 } else { (completed.min(total) as f64 / total as f64) * 100.0 };

	format!("[{}{}] {:>6.2}%", "#".repeat(filled), "-".repeat(WIDTH - filled), percent)
}

#[derive(Debug, Error)]
pub enum PacketError {
	#[error("Frame too large: expected {expected} bytes, got {actual}")]
	FrameTooLarge { expected: usize, actual: usize },
	#[error("Quinn read error: {0:?}")]
	ReadError(#[from] quinn::ReadExactError),
	#[error("Quinn write error: {0:?}")]
	WriteError(#[from] quinn::WriteError),
}

#[async_trait::async_trait]
pub trait PacketHandler {
	async fn read_frame(recv: &mut quinn::RecvStream, max_len: usize) -> Result<Bytes, PacketError> {
		let mut len_buf = [0u8; 4];
		recv.read_exact(&mut len_buf).await.map_err(PacketError::ReadError)?;
		let len = u32::from_be_bytes(len_buf) as usize;
		if len > max_len {
			return Err(PacketError::FrameTooLarge { expected: max_len, actual: len });
		}

		let mut buf = BytesMut::zeroed(len);
		recv.read_exact(&mut buf).await.map_err(PacketError::ReadError)?;
		Ok(buf.freeze())
	}

	async fn write_frame(send: &mut quinn::SendStream, data: &[u8]) -> Result<(), PacketError> {
		let len: u32 = data.len().try_into().map_err(|_| PacketError::FrameTooLarge {
			expected: u32::MAX as usize,
			actual: data.len(),
		})?;

		send.write_all(&len.to_be_bytes()).await.map_err(PacketError::WriteError)?;
		send.write_all(&data).await.map_err(PacketError::WriteError)?;
		Ok(())
	}
}

#[derive(Debug, Copy, Clone, clap::ValueEnum)]
pub enum DuplicateStrategy {
	Enumerate,
	Reject,
}
