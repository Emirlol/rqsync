use std::{
	collections::{
		HashMap,
		HashSet,
	},
	path::Path,
};

use anyhow::Context;
use bit_set::BitSet;
use lib::ReceivedChunks;
use rkyv::rancor;
use tokio::fs;

use crate::IncomingTransfer;

pub(super) const METADATA_FILE: &str = ".rqsync-resume.rkyv";
const METADATA_TEMP_FILE: &str = ".rqsync-resume.rkyv.tmp";
pub(super) const METADATA_VERSION: u32 = 1;

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone)]
#[rkyv(derive(Debug))]
pub(super) struct ResumeMetadata {
	pub(super) version: u32,
	pub(super) chunk_size: u64,
	pub(super) files: Vec<ResumeFileMetadata>,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone)]
#[rkyv(derive(Debug))]
pub(super) struct ResumeFileMetadata {
	pub(super) rel_path: Vec<String>,
	pub(super) uncompressed_size: u64,
	pub(super) chunk_count: u64,
	pub(super) bitset_bytes: Vec<u8>,
}

pub(super) fn chunk_count(file_size: u64, chunk_size: u64) -> u64 {
	if file_size == 0 { 0 } else { file_size.div_ceil(chunk_size) }
}

fn chunk_len(file_size: u64, chunk_size: u64, chunk_index: u64) -> u64 {
	let offset = chunk_index * chunk_size;
	(file_size - offset).min(chunk_size)
}

pub(super) fn bitset_from_bytes(bytes: &[u8], chunk_count: u64) -> BitSet<u64> {
	let restored = BitSet::<u64>::from_bytes_general(bytes);
	let mut received = BitSet::<u64>::default();
	received.reserve_len(chunk_count as usize);
	for chunk in restored.iter().filter(|&chunk| (chunk as u64) < chunk_count) {
		received.insert(chunk);
	}
	received
}

pub(super) fn received_bytes(received: &BitSet<u64>, file_size: u64, chunk_size: u64) -> u64 {
	received.iter().map(|chunk| chunk_len(file_size, chunk_size, chunk as u64)).sum::<u64>().min(file_size)
}

pub(super) fn metadata_from_transfers(transfers: &HashMap<u32, IncomingTransfer>) -> ResumeMetadata {
	let chunk_size = transfers.values().next().map_or(0, |transfer| transfer.chunk_size);
	let mut files = transfers
		.values()
		.map(|transfer| ResumeFileMetadata {
			rel_path: transfer.rel_path.clone(),
			uncompressed_size: transfer.file_size,
			chunk_count: chunk_count(transfer.file_size, transfer.chunk_size),
			bitset_bytes: transfer.received.get_ref().to_bytes(),
		})
		.collect::<Vec<_>>();
	files.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));

	ResumeMetadata {
		version: METADATA_VERSION,
		chunk_size,
		files,
	}
}

pub(super) fn metadata_matches(metadata: &ResumeMetadata, transfers: &HashMap<u32, IncomingTransfer>, chunk_size: u64) -> bool {
	if metadata.version != METADATA_VERSION || metadata.chunk_size != chunk_size || metadata.files.len() != transfers.len() {
		return false;
	}

	let expected = transfers.values().map(|transfer| (transfer.rel_path.clone(), transfer.file_size)).collect::<HashSet<_>>();
	let actual = metadata.files.iter().map(|file| (file.rel_path.clone(), file.uncompressed_size)).collect::<HashSet<_>>();
	expected == actual
}

pub(super) async fn load_metadata(path: &Path) -> anyhow::Result<Option<ResumeMetadata>> {
	let bytes = match fs::read(path).await {
		Ok(bytes) => bytes,
		Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
		Err(err) => return Err(err).context("Failed to read resume metadata"),
	};

	let metadata = rkyv::from_bytes::<ResumeMetadata, rancor::Error>(&bytes).context("Failed to deserialize resume metadata")?;
	Ok(Some(metadata))
}

pub(super) async fn apply_metadata(path: &Path, transfers: &mut HashMap<u32, IncomingTransfer>, chunk_size: u64) -> anyhow::Result<Vec<ReceivedChunks>> {
	let Some(metadata) = load_metadata(path).await? else {
		return Ok(Vec::new());
	};

	if !metadata_matches(&metadata, transfers, chunk_size) {
		return Ok(Vec::new());
	}

	let mut by_rel_path = metadata.files.into_iter().map(|file| (file.rel_path.clone(), file)).collect::<HashMap<_, _>>();
	let mut received_chunks = Vec::new();
	for (&file_id, transfer) in transfers.iter_mut() {
		let Some(file) = by_rel_path.remove(&transfer.rel_path) else {
			continue;
		};
		let chunk_count = chunk_count(transfer.file_size, transfer.chunk_size);
		let received = bitset_from_bytes(&file.bitset_bytes, chunk_count);
		transfer.received_bytes = received_bytes(&received, transfer.file_size, transfer.chunk_size);
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

pub(super) async fn persist_metadata(path: &Path, metadata: ResumeMetadata) -> anyhow::Result<()> {
	let bytes = rkyv::to_bytes::<rancor::Error>(&metadata).context("Failed to serialize resume metadata")?;
	let temp_path = path.with_file_name(METADATA_TEMP_FILE);
	fs::write(&temp_path, &bytes).await.context("Failed to write resume metadata")?;
	fs::rename(&temp_path, path).await.context("Failed to replace resume metadata")?;
	Ok(())
}

pub(super) async fn remove_metadata(path: &Path) -> anyhow::Result<()> {
	match fs::remove_file(path).await {
		Ok(()) => Ok(()),
		Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
		Err(err) => Err(err).context("Failed to remove resume metadata"),
	}
}
