// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Reading part of one data file (spec section 10.4, stages 6 and 7).
//!
//! The scan path above the page level is Tessera's own (QE-2): the
//! footer is read with a range request, row groups are pruned against
//! their statistics, the column chunks still needed are fetched as
//! coalesced ranges, and only then are pages decoded. Decoding is CPU
//! work: [`FileReader::fetch`] does the I/O and returns a [`Fetched`]
//! whose [`decode`](Fetched::decode) the caller runs on its CPU pool
//! (RS-4). A filter evaluates on its own columns first and the remaining
//! columns are decoded for matching rows only (late materialization).

use std::sync::Arc;

use arrow::array::{BooleanArray, RecordBatch};
use async_trait::async_trait;
use tessera_types::event::Scalar;

use crate::FormatError;

/// An inclusive range a column's values must fall in for a row to
/// qualify. Either end may be open.
#[derive(Clone, PartialEq, Debug)]
pub struct Bound {
    /// The column.
    pub column: String,
    /// The smallest qualifying value, if bounded below.
    pub min: Option<Scalar>,
    /// The largest qualifying value, if bounded above.
    pub max: Option<Scalar>,
}

/// The evaluation of a filter over a batch of its columns.
pub type Evaluate = dyn Fn(&RecordBatch) -> Result<BooleanArray, String> + Send + Sync;

/// A row filter: the columns it reads and how to evaluate it. Rows for
/// which the result is false or null are not materialized.
#[derive(Clone)]
pub struct RowPredicate {
    /// The columns the predicate reads.
    pub columns: Vec<String>,
    /// The predicate.
    pub evaluate: Arc<Evaluate>,
}

impl std::fmt::Debug for RowPredicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RowPredicate")
            .field("columns", &self.columns)
            .finish_non_exhaustive()
    }
}

/// What to read from a file.
#[derive(Clone, Debug, Default)]
pub struct ScanRequest {
    /// The columns to return, or every column.
    pub projection: Option<Vec<String>>,
    /// Bounds a row group must be able to satisfy to be read.
    pub bounds: Vec<Bound>,
    /// A filter applied before the projected columns are materialized.
    pub filter: Option<RowPredicate>,
    /// Only these row groups are read, when an index narrowed them.
    pub row_groups: Option<Vec<usize>>,
}

/// What a scan did, for `EXPLAIN ANALYZE` (QE-8).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct ScanStats {
    /// Row groups in the file.
    pub row_groups: usize,
    /// Row groups read after pruning.
    pub row_groups_read: usize,
    /// Bytes fetched, metadata included.
    pub bytes_read: u64,
    /// Range requests made.
    pub requests: usize,
    /// Whether the footer came from the cache instead of a request.
    pub footer_cached: bool,
}

/// Fetched bytes ready to decode.
pub struct Fetched {
    stats: ScanStats,
    decode: Box<dyn FnOnce() -> Result<Vec<RecordBatch>, FormatError> + Send>,
}

impl Fetched {
    /// Wraps a decoder.
    #[must_use]
    pub fn new(
        stats: ScanStats,
        decode: Box<dyn FnOnce() -> Result<Vec<RecordBatch>, FormatError> + Send>,
    ) -> Self {
        Fetched { stats, decode }
    }

    /// What the fetch did.
    #[must_use]
    pub fn stats(&self) -> ScanStats {
        self.stats
    }

    /// Decodes the rows. CPU work: run it on the CPU pool.
    ///
    /// # Errors
    ///
    /// If the bytes are not a readable file or the filter fails.
    pub fn decode(self) -> Result<Vec<RecordBatch>, FormatError> {
        (self.decode)()
    }
}

impl std::fmt::Debug for Fetched {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fetched")
            .field("stats", &self.stats)
            .finish_non_exhaustive()
    }
}

/// An open data file.
#[async_trait]
pub trait FileReader: Send + Sync {
    /// Reads the metadata, prunes row groups against `request.bounds` and
    /// fetches the column chunks the request needs.
    ///
    /// # Errors
    ///
    /// If the file cannot be read or is not a file this format wrote.
    async fn fetch(&self, request: ScanRequest) -> Result<Fetched, FormatError>;
}
