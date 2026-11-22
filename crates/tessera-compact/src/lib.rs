// SPDX-License-Identifier: Apache-2.0

//! Compaction (spec section 6.9) and garbage collection (section 6.10).
//!
//! Compaction never crosses partitions (CP-1): a job takes the L1 files
//! of one partition, merges them, removes duplicate events, writes the
//! result in the kind's current layout with its indexes (CP-3), and
//! replaces the inputs in one commit (CP-4). Garbage collection deletes
//! files that left a manifest longer ago than the snapshot-retention
//! window (GC-1) and segments every touched partition has adopted
//! (GC-2), each deletion preceded by the commit that made the object
//! unreachable (GC-3).
//!
//! The L1 files of a partition compact with the others ingested in the
//! same window (CP-2); an L3 file takes the whole partition.

pub mod gc;
pub mod patterns;
pub mod plan;
pub mod promote;
pub mod retention;
pub mod run;
pub mod spared;
pub mod tombstones;

use tessera_format::FormatError;
use tessera_manifest::ManifestError;
use tessera_object::{ObjectError, PoolError};
use tessera_wal::WalError;

pub use gc::{Checkpoints, LogCollection, collect_files, collect_log};
pub use plan::{Job, Policy, TRACE_COMPLETION_HORIZON, input_bytes, plan};
pub use promote::promote;
pub use retention::{Applied, Retain};
pub use run::{Compactor, DEFAULT_JOB_MEMORY, Outcome, stored_usage};
pub use spared::Spared;
pub use tombstones::remove_deleted;

/// Why compaction or collection failed.
#[derive(Debug, thiserror::Error)]
pub enum CompactError {
    /// A manifest could not be read or committed.
    #[error("{0}")]
    Manifest(#[from] ManifestError),
    /// A dataset's log could not be read.
    #[error("{0}")]
    Dataset(#[from] tessera_manifest::DatasetLogError),
    /// A file could not be read or written.
    #[error("{0}")]
    Format(#[from] FormatError),
    /// The object store failed.
    #[error("{0}")]
    Store(#[from] ObjectError),
    /// A file names a pool that is not registered.
    #[error("{0}")]
    Pool(#[from] PoolError),
    /// A writer log could not be read.
    #[error("{0}")]
    Wal(#[from] WalError),
    /// Arrow rejected an operation.
    #[error("arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    /// A tenant key is missing (SE-2).
    #[error("tenant keys: {0}")]
    Keys(#[from] tessera_types::KeyError),
    /// A kernel failed while promoting a column.
    #[error("kernel: {0}")]
    Kernel(#[from] tessera_kernels::KernelError),
    /// A CPU task did not complete.
    #[error("compaction task failed: {0}")]
    Task(String),
    /// A tombstone cannot be applied.
    #[error("tombstone: {0}")]
    Tombstone(String),
}
