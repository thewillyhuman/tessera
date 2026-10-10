// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Core types shared by every Tessera crate: the event model, identifiers
//! and errors.
//!
//! This crate depends on nothing else in the workspace (spec section
//! 18.1), so any crate may use it without creating a cycle.

pub mod env;
pub mod event;
pub mod file;
pub mod hash;
pub mod id;
pub mod kind;
pub mod partition;
pub mod position;
pub mod random;
pub mod time;
pub mod work;

pub use env::Env;
pub use event::{Attributes, EventBatch, InvalidBatch, LogRow, Scalar};
pub use file::{ColumnStats, FileMeta, FileSource, Level};
pub use hash::stable_hash;
pub use id::{DatasetName, FileId, Id128, InvalidId, NodeId, PoolId, TenantId, WriterId};
pub use kind::Kind;
pub use partition::{InvalidWindow, PartitionId, Window};
pub use position::{Epoch, Seq, Version};
pub use random::{OsRandom, Random, SeededRandom, SharedRandom};
pub use time::{Clock, ManualClock, SystemClock, TimeRange, Timestamp};
pub use work::WorkKind;
