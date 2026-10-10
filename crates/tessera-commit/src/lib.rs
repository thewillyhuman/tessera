// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Commit logs (spec section 6.5): totally ordered, durable logs of
//! commits backed by create-if-absent writes. The cluster catalog, tenant
//! control stores, dataset logs, partition manifests and rule-state logs
//! all share this one implementation.

pub mod action;
mod checkpoint;
pub mod lease;
pub mod log;
pub mod record;
pub mod slot;
pub mod state;

pub use action::{Action, KvMutation};
pub use lease::{Lease, LeaseError, LeaseManager};
pub use log::{CommitError, CommitLog, UpdateError};
pub use record::{Commit, CommitId, DecodeError, FORMAT};
pub use slot::{InvalidSlotCount, Slot, SlotCount};
pub use state::{ApplyError, KvState, Snapshot, State};
