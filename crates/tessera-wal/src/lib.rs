// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Writer logs (spec section 6.3): the durability mechanism, the source of
//! sub-second freshness and the stream that continuous queries consume.
//!
//! Each ingester owns one log, a strictly ordered sequence of entries at
//! `_wal/{writer_id}/{seq:012}`, each created with create-if-absent. No
//! other node appends while the writer is alive; recovery fences a dead
//! writer by creating the next entry itself (ST-9).

pub mod entry;

pub use entry::{BlockMeta, Entry, EntryBody, EntryId, FORMAT};

/// Why a writer-log operation failed.
#[derive(Debug, thiserror::Error)]
pub enum WalError {
    /// A segment needs at least one batch.
    #[error("a segment needs at least one batch")]
    EmptySegment,
    /// The bytes are not a readable entry.
    #[error("unreadable writer log entry: {0}")]
    Unreadable(String),
    /// The entry was written by a newer release.
    #[error("writer log entry format {format} is newer than the supported format {supported}")]
    UnsupportedFormat {
        /// The format found.
        format: u32,
        /// The newest format this release reads.
        supported: u32,
    },
    /// A block index is out of range.
    #[error("entry has no block {0}")]
    NoSuchBlock(usize),
    /// Arrow could not encode or decode a block.
    #[error("arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    /// A decoded block is not a valid batch.
    #[error("invalid block: {0}")]
    Batch(#[from] tessera_types::InvalidBatch),
}
