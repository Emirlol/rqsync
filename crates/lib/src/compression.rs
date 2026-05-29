use bytes::Bytes;
use lz4_flex::block::DecompressError;
use thiserror::Error;

#[derive(Debug, Copy, Clone, Default)]
#[repr(u8)]
pub enum Compression {
	#[default]
	None,
	LZ4,
}

#[derive(Debug, Error)]
pub enum CompressionError {
	#[error("LZ4 decompression error: $1")]
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
			Compression::None => Ok(bytes),
			Compression::LZ4 => match lz4_flex::block::decompress(bytes.as_ref(), expected_len) {
				Ok(decompressed) => Ok(Bytes::from(decompressed)),
				Err(e) => Err(CompressionError::LZ4(e)),
			},
		}
	}
}
