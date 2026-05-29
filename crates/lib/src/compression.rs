use bytes::Bytes;
use lz4_flex::block::DecompressError;
use thiserror::Error;

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Copy, Clone, Default, PartialEq, Eq, clap::ValueEnum)]
#[rkyv(derive(Debug))]
#[repr(u8)]
pub enum Compression {
	#[default]
	None,
	LZ4,
}

#[derive(Debug, Error)]
pub enum CompressionError {
	#[error("decompressed length mismatch: expected {expected}, got {actual}")]
	LengthMismatch { expected: usize, actual: usize },
	#[error("LZ4 decompression error: {0}")]
	LZ4(#[from] DecompressError),
}

impl Compression {
	pub fn compress(&self, bytes: Bytes) -> Bytes {
		match self {
			Compression::None => bytes,
			Compression::LZ4 => Bytes::from(lz4_flex::block::compress(bytes.as_ref())),
		}
	}

	pub fn decompress(&self, bytes: Bytes, expected_len: usize) -> Result<Bytes, CompressionError> {
		match self {
			Compression::None => {
				if bytes.len() == expected_len {
					Ok(bytes)
				} else {
					Err(CompressionError::LengthMismatch {
						expected: expected_len,
						actual: bytes.len(),
					})
				}
			}
			Compression::LZ4 => match lz4_flex::block::decompress(bytes.as_ref(), expected_len) {
				Ok(decompressed) => Ok(Bytes::from(decompressed)),
				Err(e) => Err(CompressionError::LZ4(e)),
			},
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn none_round_trips_when_length_matches() {
		let input = Bytes::from_static(b"raw bytes");

		let compressed = Compression::None.compress(input.clone());
		let decompressed = Compression::None.decompress(compressed, input.len()).unwrap();

		assert_eq!(decompressed, input);
	}

	#[test]
	fn none_rejects_unexpected_length() {
		let err = Compression::None.decompress(Bytes::from_static(b"short"), 10).unwrap_err();

		assert!(matches!(err, CompressionError::LengthMismatch { expected: 10, actual: 5 }));
	}

	#[test]
	fn lz4_round_trips() {
		let input = Bytes::from(vec![b'a'; 16 * 1024]);

		let compressed = Compression::LZ4.compress(input.clone());
		let decompressed = Compression::LZ4.decompress(compressed, input.len()).unwrap();

		assert_eq!(decompressed, input);
	}
}
