// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! A file of which only some byte ranges were fetched, for the Parquet
//! page reader. Reads outside the fetched ranges are a bug in the range
//! planning, reported as an error rather than fetched lazily, so that
//! decoding never does I/O (RS-4).

use std::io::Cursor;

use bytes::Bytes;
use parquet::errors::ParquetError;
use parquet::file::reader::{ChunkReader, Length};

/// The fetched ranges of a file.
#[derive(Clone, Debug)]
pub struct SparseFile {
    len: u64,
    chunks: Vec<(u64, Bytes)>,
}

impl SparseFile {
    /// A file of `len` bytes with the given `(offset, bytes)` chunks,
    /// which may be in any order.
    #[must_use]
    pub fn new(len: u64, mut chunks: Vec<(u64, Bytes)>) -> Self {
        chunks.sort_by_key(|(offset, _)| *offset);
        SparseFile { len, chunks }
    }

    /// The chunk holding `[start, start + length)`, as its offset and
    /// bytes.
    fn chunk(&self, start: u64, length: u64) -> Result<(u64, &Bytes), ParquetError> {
        self.chunks
            .iter()
            .find(|(offset, bytes)| {
                *offset <= start && start + length <= offset + bytes.len() as u64
            })
            .map(|(offset, bytes)| (*offset, bytes))
            .ok_or_else(|| {
                ParquetError::General(format!(
                    "bytes {start}..{} were not fetched",
                    start + length
                ))
            })
    }
}

impl Length for SparseFile {
    fn len(&self) -> u64 {
        self.len
    }
}

impl ChunkReader for SparseFile {
    type T = Cursor<Bytes>;

    fn get_read(&self, start: u64) -> Result<Self::T, ParquetError> {
        let (offset, bytes) = self.chunk(start, 0)?;
        let skip =
            usize::try_from(start - offset).map_err(|e| ParquetError::External(Box::new(e)))?;
        Ok(Cursor::new(bytes.slice(skip..)))
    }

    fn get_bytes(&self, start: u64, length: usize) -> Result<Bytes, ParquetError> {
        let (offset, bytes) = self.chunk(start, length as u64)?;
        let skip =
            usize::try_from(start - offset).map_err(|e| ParquetError::External(Box::new(e)))?;
        Ok(bytes.slice(skip..skip + length))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use super::*;

    #[test]
    fn reads_within_fetched_ranges_and_refuses_the_rest() {
        let file = SparseFile::new(
            100,
            vec![
                (50, Bytes::from_static(b"world")),
                (0, Bytes::from_static(b"hello")),
            ],
        );
        assert_eq!(file.len(), 100);
        assert_eq!(file.get_bytes(51, 3).unwrap().as_ref(), b"orl");
        let mut text = String::new();
        file.get_read(1).unwrap().read_to_string(&mut text).unwrap();
        assert_eq!(text, "ello");
        assert!(file.get_bytes(3, 4).is_err());
        assert!(file.get_read(20).is_err());
    }
}
