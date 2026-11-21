// SPDX-License-Identifier: Apache-2.0

//! Ingest (spec section 7): decode, normalize, buffer, append to the
//! writer log, acknowledge, and flush to L1 files.
//!
//! ```text
//! decode → normalize to Arrow batches → buffer
//!        → append segment to own writer log → acknowledge → memtable
//!        → L1 flush → flush record
//! ```

pub mod ingester;
pub mod loki_push;
pub mod otlp;
pub mod remote_write;
pub mod service;

pub use ingester::{Ack, IngestConfig, IngestError, Ingester};
pub use service::{IngestHandle, spawn, spawn_all};
