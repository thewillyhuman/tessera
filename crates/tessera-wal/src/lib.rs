// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Writer logs (spec section 6.3): the durability mechanism, the source of
//! sub-second freshness and the stream that continuous queries consume.
//!
//! Each ingester owns one log, a strictly ordered sequence of entries at
//! `_wal/{writer_id}/{seq:012}`, each created with create-if-absent. No
//! other node appends while the writer is alive; recovery fences a dead
//! writer by creating the next entry itself (ST-9).

pub mod entry;
pub mod flush;
pub mod reader;
pub mod writer;

pub use entry::{BlockMeta, Entry, EntryBody, EntryId, FORMAT};
pub use flush::{FlushTarget, write_l1_files};
pub use writer::WriterLog;

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
    /// Recovery sealed the log: the writer must stop (ST-9).
    #[error("writer {writer} is fenced at seq {seq}")]
    Fenced {
        /// The writer.
        writer: tessera_types::WriterId,
        /// The position the foreign entry was found at.
        seq: tessera_types::Seq,
    },
    /// The log was ended; nothing more can be appended.
    #[error("writer {writer} has ended its log")]
    Ended {
        /// The writer.
        writer: tessera_types::WriterId,
    },
    /// An entry object cannot be read.
    #[error("writer log object {key} is corrupt: {source}")]
    Corrupt {
        /// The offending object.
        key: tessera_object::Key,
        /// What is wrong with it.
        #[source]
        source: Box<WalError>,
    },
    /// The object store failed.
    #[error(transparent)]
    Store(#[from] tessera_object::ObjectError),
    /// An event time has no partition window.
    #[error("event time {timestamp} lies before the epoch and has no partition")]
    Unwindowed {
        /// The offending time.
        timestamp: tessera_types::Timestamp,
    },
    /// A file could not be encoded.
    #[error(transparent)]
    Format(#[from] tessera_format::FormatError),
    /// The flush failed for another reason.
    #[error("flush failed: {0}")]
    Flush(String),
}
