// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The data file format (spec section 6.4). Data files are Apache Parquet
//! (ST-11); everything else reaches them through the [`FileFormat`] trait
//! (ST-15), so that the format decision stays local to this crate.
//!
//! Files are written whole: L1 files are small by construction (section
//! 6.9). They are read whole where the caller wants every row, or through
//! a [`FileReader`] that fetches only the row groups and columns a query
//! needs ([`scan`]).

pub mod parquet;
mod prune;
pub mod reader;
pub mod scan;
pub mod sidecar;
mod sparse;
pub mod stats;
pub mod text;

use std::sync::Arc;

use arrow::array::RecordBatch;
use bytes::Bytes;
use tessera_object::{Key, ObjectStore};
use tessera_types::{ColumnStats, Timestamp};

pub use crate::parquet::ParquetFormat;
pub use crate::reader::ParquetReader;
pub use crate::scan::{Bound, Fetched, FileReader, RowPredicate, ScanRequest, ScanStats};
pub use crate::sidecar::{Bloom, IndexSpec, RowGroups, Sidecar};

/// Why a file could not be written or read.
#[derive(Debug, thiserror::Error)]
pub enum FormatError {
    /// Nothing to write.
    #[error("a data file needs at least one row")]
    Empty,
    /// The batches do not share one schema.
    #[error("all batches of a data file must have the same schema")]
    MixedSchemas,
    /// The batches lack the event time column.
    #[error("a data file needs a non-null `timestamp` column of nanoseconds")]
    NoTimestamp,
    /// The bytes are not a readable file.
    #[error("unreadable data file: {0}")]
    Unreadable(String),
    /// Arrow failed to build or sort a batch.
    #[error("arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
}

/// What a write produced: the bytes and what the manifest records about
/// them (ST-12).
#[derive(Clone, Debug)]
pub struct Written {
    /// The file body.
    pub bytes: Bytes,
    /// How many rows it holds.
    pub row_count: u64,
    /// The earliest event time.
    pub min_timestamp: Timestamp,
    /// The latest event time.
    pub max_timestamp: Timestamp,
    /// The columns the rows are sorted by.
    pub sort_key: Vec<String>,
    /// Statistics of every column.
    pub columns: Vec<ColumnStats>,
}

/// A data file format. Methods are CPU work and are called from a
/// blocking pool, never from the async runtime (RS-4).
pub trait FileFormat: Send + Sync {
    /// Encodes `batches`, which share one schema, sorted by the format's
    /// default sort key for that schema.
    ///
    /// # Errors
    ///
    /// `Empty`, `MixedSchemas`, `NoTimestamp`, or an encoding failure.
    fn write(&self, batches: &[RecordBatch]) -> Result<Written, FormatError>;

    /// Decodes a whole file.
    ///
    /// # Errors
    ///
    /// `Unreadable` if the bytes are not a file this format wrote.
    fn read(&self, bytes: Bytes) -> Result<Vec<RecordBatch>, FormatError>;

    /// Opens the `size`-byte file at `key` for partial reads.
    fn open(&self, store: Arc<dyn ObjectStore>, key: Key, size: u64) -> Box<dyn FileReader>;
}
