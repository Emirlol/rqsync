use std::{
	collections::{
		HashMap,
		HashSet,
	},
	path::PathBuf,
};

use lib::{
	Compression,
	ManifestEntry,
	RejectReason,
	RejectedTransfer,
};

use crate::IncomingTransfer;

pub(super) fn create_incoming_transfers<'a, I, E>(files: I, chunk_size: u64, compression: Compression, root_dir: PathBuf) -> (HashMap<u32, IncomingTransfer>, Vec<RejectedTransfer>)
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
			if is_path_banned(entry.rel_path_component(i)) {
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
			#[cfg(test)]
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
	path.is_empty() || path == ".." || path == "." || path.contains(['\\', '/', '\0']) || (cfg!(windows) && is_path_banned_windows(path))
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
