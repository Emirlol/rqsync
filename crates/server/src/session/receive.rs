use std::{
	path::PathBuf,
};
#[cfg(test)]
use std::{
	io::SeekFrom,
	sync::Arc,
};
use std::collections::HashMap;
use anyhow::{
	Context,
	bail,
};
use lib::{
	ArchivedClientMessage,
	MAX_DATA_FRAME_SIZE,
	PacketError,
	PacketHandler,
};
#[cfg(test)]
use lib::format_bytes;
use quinn::RecvStream;
use rkyv::rancor;
use tokio::sync::mpsc::Sender;
#[cfg(test)]
use tokio::{
	fs::OpenOptions,
	io::{
		AsyncSeekExt,
		AsyncWriteExt,
	},
	sync::Mutex,
};
#[cfg(test)]
use tracing::info;

use super::{
	write::WriteJob,
};
#[cfg(test)]
use super::{
	progress::{
		SharedReceiveProgress,
		record_written,
	},
	resume,
};
use crate::IncomingTransfer;

#[cfg(test)]
pub(super) type SharedReceiveState = Arc<Mutex<ReceiveState>>;

pub struct ReceiveState {
	pub(super) transfers: HashMap<u32, IncomingTransfer>,
	pub(super) resume_path: PathBuf,
	pub(super) dirty: bool,
}

impl ReceiveState {
	pub(super) fn new(transfers: HashMap<u32, IncomingTransfer>, resume_path: PathBuf) -> Self {
		Self { transfers, resume_path, dirty: false }
	}

	#[cfg(test)]
	fn mark_dirty(&mut self) {
		self.dirty = true;
	}
}

struct DataStreamHandler;

impl PacketHandler for DataStreamHandler {}

pub(super) async fn receive_data_chunks(mut recv: RecvStream, tx: Sender<WriteJob>) -> anyhow::Result<()> {
	loop {
		let frame = match DataStreamHandler::read_frame(&mut recv, MAX_DATA_FRAME_SIZE).await {
			Ok(frame) => frame,
			Err(PacketError::ReadError(_)) => {
				return Ok(()); // EOF, probably
			}
			Err(err) => {
				bail!("Failed to read data frame: {err}");
			}
		};

		let message = rkyv::access::<ArchivedClientMessage, rancor::Error>(&frame)?;

		match message {
			ArchivedClientMessage::Chunk { file_id, offset, bytes } => {
				let bytes = frame.slice_ref(bytes);
				tx.send(WriteJob {
					file_id: file_id.to_native(),
					offset: offset.to_native(),
					bytes,
				})
				.await
				.context("Failed to queue chunk for writer")?;
			}
			message => bail!("Unexpected message in data stream: {message:?}"),
		}
	}
}

#[cfg(test)]
pub(super) async fn write_chunk(receive_state: SharedReceiveState, progress: SharedReceiveProgress, file_id: u32, offset: u64, bytes: &[u8]) -> anyhow::Result<()> {
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
	let bytes = transfer.compression.decompress_slice(bytes, expected_len).context("Failed to decompress chunk")?;

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
		transfer.completed = true;
		transfer.file.as_mut().unwrap().flush().await?;
		Some((transfer.path.clone(), transfer.file_size))
	} else {
		None
	};
	state.mark_dirty();

	if let Some((path, file_size)) = wrote_file {
		info!("Wrote file {} ({})", path.display(), format_bytes(file_size));
	}
	drop(state);
	record_written(&progress, len);

	Ok(())
}

#[cfg(test)]
pub(super) async fn persist_resume_metadata_on_disconnect(receive_state: &SharedReceiveState) -> anyhow::Result<()> {
	let mut receive_state = receive_state.lock().await;
	if !receive_state.dirty {
		return Ok(());
	}

	let path = receive_state.resume_path.clone();
	let metadata = resume::metadata_from_transfers(&receive_state.transfers);
	receive_state.dirty = false;
	drop(receive_state);

	resume::persist_metadata(&path, metadata).await?;
	Ok(())
}
