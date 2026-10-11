// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Continuous queries and alerting (spec section 11): alert rules kept
//! in the tenant's control store, the alert lifecycle of section 11.8,
//! the state log of a rule group partition (CQ-12), which records in
//! one commit the instances, the input positions and the transitions
//! not yet notified, and the periodic evaluation of a rule group
//! through the engine (CQ-8) with its alert events (CQ-14).

pub mod evaluate;
pub mod group;
pub mod lifecycle;
pub mod rule;
pub mod sink;
pub mod state;

pub use evaluate::{EvalError, observe};
pub use group::{Evaluated, RuleGroup};
pub use lifecycle::{Instance, Observation, Status, Transition, fingerprint, step};
pub use rule::{
    Acknowledgment, AlertRule, Language, NotificationRoute, RouteKind, Silence, UnknownLanguage,
};
pub use sink::{AlertSink, MemorySink};
pub use state::{EvalState, StateLog};
