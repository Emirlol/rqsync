use std::{
	collections::HashMap,
	fs,
	sync::Arc,
	time::{
		SystemTime,
		UNIX_EPOCH,
	},
};

use bit_set::BitSet;
use lib::{
	FileManifestEntry,
	RejectReason,
	RejectedTransfer,
};

use super::{
	progress::SharedReceiveProgress,
	resume::{
		METADATA_VERSION,
		ResumeFileMetadata,
		ResumeMetadata,
	},
	*,
};
use crate::IncomingTransfer;

fn test_dir(name: &str) -> PathBuf {
	let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
	let path = std::env::temp_dir().join(format!("rqsync-server-{name}-{}-{nanos}", std::process::id()));
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

fn receive_progress(total_bytes: u64) -> SharedReceiveProgress {
	ReceiveProgress::empty(total_bytes)
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
	let received_bytes = resume::received_bytes(&received, size, chunk_size);
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

	let (transfers, rejected) = manifest::create_incoming_transfers(files.iter(), 4, Compression::None, root.clone());

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

	let (transfers, rejected) = manifest::create_incoming_transfers(files.iter(), 4, Compression::LZ4, root.clone());

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
		version: METADATA_VERSION,
		chunk_size: 4,
		files: vec![ResumeFileMetadata {
			rel_path: vec!["file.bin".to_owned()],
			uncompressed_size: 10,
			chunk_count: 3,
			bitset_bytes: Vec::new(),
		}],
	};

	assert!(resume::metadata_matches(&metadata, &transfers, 4));

	fs::remove_dir_all(root).unwrap();
}

#[test]
fn resume_metadata_mismatch_starts_fresh_for_path_size_or_chunk_size_change() {
	let root = test_dir("resume-mismatch");
	let mut transfers = HashMap::new();
	transfers.insert(1, incoming(&root, &["file.bin"], 10, 4, Compression::None, BitSet::default()));
	let mut metadata = ResumeMetadata {
		version: METADATA_VERSION,
		chunk_size: 4,
		files: vec![ResumeFileMetadata {
			rel_path: vec!["file.bin".to_owned()],
			uncompressed_size: 10,
			chunk_count: 3,
			bitset_bytes: Vec::new(),
		}],
	};

	metadata.files[0].rel_path = vec!["other.bin".to_owned()];
	assert!(!resume::metadata_matches(&metadata, &transfers, 4));

	metadata.files[0].rel_path = vec!["file.bin".to_owned()];
	metadata.files[0].uncompressed_size = 11;
	assert!(!resume::metadata_matches(&metadata, &transfers, 4));

	metadata.files[0].uncompressed_size = 10;
	assert!(!resume::metadata_matches(&metadata, &transfers, 8));

	fs::remove_dir_all(root).unwrap();
}

#[test]
fn bitset_round_trip_ignores_padded_bits_past_chunk_count() {
	let mut bitset = BitSet::<u64>::default();
	bitset.insert(0);
	bitset.insert(2);
	bitset.insert(7);
	let bytes = bitset.get_ref().to_bytes();

	let restored = resume::bitset_from_bytes(&bytes, 5);

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
		version: METADATA_VERSION,
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

	let received_chunks = resume::apply_metadata(&resume_path, &mut transfers, 4).await.unwrap();

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

	receive::write_chunk(receive_state.clone(), progress, 1, 4, b"test").await.unwrap();
	receive::persist_resume_metadata_on_disconnect(&receive_state).await.unwrap();

	assert_eq!(fs::read(&path).unwrap(), b"\0\0\0\0test");
	assert!(receive_state.lock().await.transfers.get(&1).unwrap().received.contains(1));

	fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn write_chunk_does_not_write_resume_metadata_until_disconnect() {
	let root = test_dir("metadata-waits");
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

	receive::write_chunk(receive_state, progress, 1, 0, b"test").await.unwrap();

	assert!(!resume_path.exists());

	fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn disconnect_persists_metadata_and_clears_dirty_state() {
	let root = test_dir("metadata-disconnect");
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

	receive::write_chunk(receive_state.clone(), progress, 1, 0, b"test").await.unwrap();
	receive::persist_resume_metadata_on_disconnect(&receive_state).await.unwrap();

	let metadata = resume::load_metadata(&resume_path).await.unwrap().unwrap();
	let received = resume::bitset_from_bytes(&metadata.files[0].bitset_bytes, 2);
	assert!(received.contains(0));
	let state = receive_state.lock().await;
	assert!(!state.dirty);

	fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn completed_file_is_marked_complete_and_readable() {
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

	receive::write_chunk(receive_state.clone(), progress, 1, 0, b"done").await.unwrap();

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

	receive::write_chunk(receive_state.clone(), progress.clone(), 1, 0, b"abcd").await.unwrap();
	let duplicate = receive::write_chunk(receive_state.clone(), progress.clone(), 1, 0, b"abcd").await.unwrap_err();
	let misaligned = receive::write_chunk(receive_state, progress, 1, 2, b"ab").await.unwrap_err();

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

	receive::write_chunk(receive_state, progress, 1, 0, &compressed).await.unwrap();

	assert_eq!(fs::read(&path).unwrap(), b"aaaaaaaaaaaaaaaa");

	fs::remove_dir_all(root).unwrap();
}
