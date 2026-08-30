use std::{
	collections::HashMap,
	sync::Arc,
};

use lib::{
	Compression,
	format_bytes,
	progress_bar,
};
use parking_lot::Mutex;
use tracing::info;

use crate::IncomingTransfer;

pub(super) type SharedReceiveProgress = Arc<Mutex<ReceiveProgress>>;

pub(super) struct ReceiveProgress {
	total_bytes: u64,
	completed_bytes: u64,
	next_report_percent: u64,
}

impl ReceiveProgress {
	pub(super) fn new(transfers: &HashMap<u32, IncomingTransfer>) -> Self {
		Self {
			total_bytes: transfers.values().map(|transfer| transfer.file_size).sum(),
			completed_bytes: transfers.values().map(|transfer| transfer.received_bytes).sum(),
			next_report_percent: 5,
		}
	}

	pub(super) fn shared(transfers: &HashMap<u32, IncomingTransfer>) -> SharedReceiveProgress {
		Arc::new(Mutex::new(Self::new(transfers)))
	}

	#[cfg(test)]
	pub(super) fn empty(total_bytes: u64) -> SharedReceiveProgress {
		Arc::new(Mutex::new(Self {
			total_bytes,
			completed_bytes: 0,
			next_report_percent: 5,
		}))
	}

	pub(super) fn record_written(&mut self, bytes: u64) -> Option<String> {
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

pub(super) fn log_receiving_start(file_count: usize, compression: Compression, progress: &SharedReceiveProgress) {
	let progress = progress.lock();
	info!(
		"Receiving {} files ({}) with {:?} compression {}",
		file_count,
		format_bytes(progress.total_bytes),
		compression,
		progress_bar(progress.completed_bytes, progress.total_bytes)
	);
}

pub(super) fn record_written(progress: &SharedReceiveProgress, bytes: u64) {
	let message = progress.lock().record_written(bytes);
	if let Some(message) = message {
		info!("{message}");
	}
}
