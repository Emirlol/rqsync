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

pub const PROTOCOL_VERSION: u32 = 1;
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
	HelloAck { version: u32 },
	AllTransfersRejected { rejected: Vec<RejectedTransfer> },
	TransfersRegistered { accepted: HashSet<u32>, rejected: Vec<RejectedTransfer> },
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

#[derive(Debug, Error)]
pub enum PacketError {
	#[error("Packet too large: expected {expected} bytes, got {actual}")]
	FrameTooLarge { expected: usize, actual: usize },
	#[error("Quinn read error: {0:?}")]
	ReadExactError(#[from] quinn::ReadExactError),
	#[error("Quinn write error: {0:?}")]
	WriteError(#[from] quinn::WriteError),
}

#[async_trait::async_trait]
pub trait PacketHandler {
	async fn read_frame(recv: &mut quinn::RecvStream, max_len: usize) -> Result<Bytes, PacketError> {
		let mut len_buf = [0u8; 4];
		recv.read_exact(&mut len_buf).await.map_err(PacketError::ReadExactError)?;
		let len = u32::from_be_bytes(len_buf) as usize;
		if len > max_len {
			return Err(PacketError::FrameTooLarge { expected: max_len, actual: len });
		}

		let mut buf = BytesMut::zeroed(len);
		recv.read_exact(&mut buf).await.map_err(PacketError::ReadExactError)?;
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
