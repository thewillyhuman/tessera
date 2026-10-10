// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Core types shared by every Tessera crate: the event model, identifiers
//! and errors.
//!
//! This crate depends on nothing else in the workspace (spec section
//! 18.1), so any crate may use it without creating a cycle.

pub mod time;

pub use time::{Clock, ManualClock, SystemClock, Timestamp};
