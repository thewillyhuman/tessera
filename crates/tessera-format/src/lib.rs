// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The data file format (spec section 6.4). Data files are Apache Parquet
//! (ST-11); everything else reaches them through the [`FileFormat`] trait
//! (ST-15), so that the format decision stays local to this crate.
//!
//! Files are written whole: L1 files are small by construction (section
//! 6.9). They are read whole where the caller wants every row, or through
//! a [`FileReader`] that fetches only the row groups and columns a query
//! needs ([`scan`]). A keyed tenant's files are encrypted under its key
//! and its sidecars sealed (SE-2); each names its key, and readers open
//! them with the keyring they were given.

pub mod footer;
pub mod layout;
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
use tessera_types::{ColumnStats, IndexRef, KeyError, NamedKey, Timestamp};

pub use crate::footer::FooterCache;
pub use crate::layout::{DEFAULT_ROW_GROUP_ROWS, Layout};
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
    /// The layout names a column the rows do not have.
    #[error("layout refers to unknown column {0}")]
    UnknownColumn(String),
    /// Arrow failed to build or sort a batch.
    #[error("arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    /// A tenant key is missing or does not open a sidecar (SE-2).
    #[error("tenant keys: {0}")]
    Keys(#[from] KeyError),
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
    /// How many row groups the file has.
    pub row_groups: usize,
    /// The sidecar, when the layout asked for indexes.
    pub sidecar: Option<Bytes>,
    /// The indexes the sidecar holds.
    pub indexes: Vec<IndexRef>,
}

/// A data file format. Methods are CPU work and are called from a
/// blocking pool, never from the async runtime (RS-4).
pub trait FileFormat: Send + Sync {
    /// Encodes `batches`, which share one schema, in `layout`: sorted by
    /// its key, split into its row groups, with its indexes in a sidecar.
    /// Under `key`, the file is encrypted and the sidecar sealed, each
    /// naming the key (SE-2).
    ///
    /// # Errors
    ///
    /// `Empty`, `MixedSchemas`, `NoTimestamp`, a sort key or index column
    /// the schema lacks, or an encoding failure.
    fn write(
        &self,
        batches: &[RecordBatch],
        layout: &Layout,
        key: Option<&NamedKey>,
    ) -> Result<Written, FormatError>;

    /// Decodes a whole file, with the format's keys if it is encrypted.
    ///
    /// # Errors
    ///
    /// `Unreadable` if the bytes are not a file this format wrote or its
    /// key is not held.
    fn read(&self, bytes: Bytes) -> Result<Vec<RecordBatch>, FormatError>;

    /// Opens the `size`-byte file at `key` for partial reads.
    fn open(&self, store: Arc<dyn ObjectStore>, key: Key, size: u64) -> Box<dyn FileReader>;
}
