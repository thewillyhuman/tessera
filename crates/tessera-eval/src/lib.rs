// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Continuous queries and alerting (spec section 11): alert rules kept
//! in the tenant's control store, the alert lifecycle of section 11.8,
//! and the state log of a rule group partition (CQ-12), which records
//! in one commit the instances, the input positions and the transitions
//! not yet notified.

pub mod lifecycle;
pub mod rule;
pub mod state;

pub use lifecycle::{Instance, Observation, Status, Transition, fingerprint, step};
pub use rule::{
    Acknowledgment, AlertRule, Language, NotificationRoute, RouteKind, Silence, UnknownLanguage,
};
pub use state::{EvalState, StateLog};
