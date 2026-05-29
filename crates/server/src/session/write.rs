use std::{
	collections::HashMap,
	io::SeekFrom,
	path::Path,
};

use anyhow::Context;
use bytes::Bytes;
use lib::format_bytes;
use thiserror::Error;
use tokio::{
	fs::{
		File,
		OpenOptions,
	},
	io::{
		AsyncSeekExt,
		AsyncWriteExt,
	},
	sync::mpsc::Receiver,
};
use tracing::info;

use super::progress::{
	SharedReceiveProgress,
	record_written,
};
use crate::session::{
	ReceiveState,
	resume,
};

pub(super) struct WriteJob {
	pub(super) file_id: u32,
	pub(super) offset: u64,
	pub(super) bytes: Bytes,
}

pub(super) struct Writer {
	pub(super) receive_state: ReceiveState,
	pub(super) open_files: HashMap<u32, File>,
	progress: SharedReceiveProgress,
}

pub(super) struct WriterResult {
	pub(super) receive_state: ReceiveState,
	pub(super) result: anyhow::Result<()>,
}

#[derive(Debug, Error)]
pub(super) enum WriteError {
	#[error("Unknown file id {0}")]
	UnknownFileId(u32),
	#[error("File id {file_id} at offset {offset} is past the end of the file")]
	OffsetPastEndOfFile { file_id: u32, offset: u64 },
	#[error("File id {file_id} at offset {offset} is not aligned to the chunk size")]
	OffsetNotAligned { file_id: u32, offset: u64 },
	#[error("File id {file_id} at offset {offset} was already received")]
	AlreadyReceived { file_id: u32, offset: u64 },
	#[error("File id {file_id} at offset {offset} has length {len} but the file is {file_size} bytes long")]
	ChunkTooLong { file_id: u32, offset: u64, len: usize, file_size: u64 },
	#[error("IO error: {0}")]
	IoError(#[from] std::io::Error),
}

impl Writer {
	pub(super) fn new(receive_state: ReceiveState, progress: SharedReceiveProgress) -> Self {
		Self {
			receive_state,
			open_files: HashMap::new(),
			progress,
		}
	}

	pub async fn run_worker(mut self, mut rx: Receiver<WriteJob>) -> WriterResult {
		while let Some(job) = rx.recv().await {
			if let Err(err) = self.write_job(job).await {
				return WriterResult {
					receive_state: self.receive_state,
					result: Err(err),
				};
			}
		}

		WriterResult {
			receive_state: self.receive_state,
			result: Ok(()),
		}
	}

	async fn write_job(&mut self, job: WriteJob) -> anyhow::Result<()> {
		let transfer = self.receive_state.transfers.get_mut(&job.file_id).ok_or_else(|| WriteError::UnknownFileId(job.file_id))?;
		if job.offset >= transfer.file_size {
			return Err(WriteError::OffsetPastEndOfFile {
				file_id: job.file_id,
				offset: job.offset,
			}
			.into());
		}

		let div = job.offset / transfer.chunk_size;
		let rem = job.offset % transfer.chunk_size;
		if rem != 0 {
			return Err(WriteError::OffsetNotAligned {
				file_id: job.file_id,
				offset: job.offset,
			}
			.into());
		}
		if transfer.received.contains(div as usize) {
			return Err(WriteError::AlreadyReceived {
				file_id: job.file_id,
				offset: job.offset,
			}
			.into());
		}

		let expected_len = (transfer.file_size - job.offset).min(transfer.chunk_size) as usize;
		let bytes = transfer
			.compression
			.decompress(job.bytes, expected_len)
			.with_context(|| format!("Failed to decompress chunk for file id {} at offset {}", job.file_id, job.offset))?;

		let path: &Path = transfer.path.as_path();
		let file_size = transfer.file_size;

		if !self.open_files.contains_key(&job.file_id) {
			if let Some(parent) = path.parent() {
				tokio::fs::create_dir_all(parent).await?;
			}

			let file = OpenOptions::new().write(true).create(true).open(path).await?;

			file.set_len(file_size).await?;
			self.open_files.insert(job.file_id, file);
		}

		let len = bytes.len() as u64;
		if job.offset + len > file_size {
			return Err(WriteError::ChunkTooLong {
				file_id: job.file_id,
				offset: job.offset,
				len: bytes.len(),
				file_size,
			}
			.into());
		}

		let file = self.open_files.get_mut(&job.file_id).expect("File should be open");
		file.seek(SeekFrom::Start(job.offset)).await?;
		file.write_all(&bytes).await?;

		let transfer = self.receive_state.transfers.get_mut(&job.file_id).expect("Transfer should exist");
		transfer.received.insert(div as usize);
		transfer.received_bytes += len;
		self.receive_state.dirty = true;

		if !transfer.completed && transfer.received_bytes >= transfer.file_size {
			transfer.completed = true;
			file.flush().await?;
			self.open_files.remove(&job.file_id);
			info!("Wrote file {} ({})", transfer.path.display(), format_bytes(transfer.file_size));
		}
		record_written(&self.progress, len);

		Ok(())
	}
}

pub(super) async fn persist_resume_metadata_on_disconnect(receive_state: &mut ReceiveState) -> anyhow::Result<()> {
	if !receive_state.dirty {
		return Ok(());
	}

	let path = receive_state.resume_path.clone();
	let metadata = resume::metadata_from_transfers(&receive_state.transfers);
	receive_state.dirty = false;

	resume::persist_metadata(&path, metadata).await?;
	Ok(())
}
