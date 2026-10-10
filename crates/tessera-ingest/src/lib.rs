// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Ingest (spec section 7): decode, normalize, buffer, append to the
//! writer log, acknowledge, and flush to L1 files.
//!
//! ```text
//! decode → normalize to Arrow batches → buffer
//!        → append segment to own writer log → acknowledge → memtable
//!        → L1 flush → flush record
//! ```

pub mod ingester;

pub use ingester::{Ack, IngestConfig, IngestError, Ingester};
