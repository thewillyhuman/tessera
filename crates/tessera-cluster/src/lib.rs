// SPDX-License-Identifier: Apache-2.0

//! The cluster (spec sections 5.2 and 14): the roles a node runs, the
//! node registry in the root pool, the membership view built from it,
//! admission control and the ledger that counts usage against budgets.
//!
//! Membership is a hint (P2): a wrong view slows the system down, it
//! cannot make it incorrect, because every effect of leased work passes
//! through a conditional write (OP-2). Nodes discover each other through
//! the registry alone in this release; gossip and DNS seeds (section
//! 14.1) can be added without changing what membership promises.

pub mod admission;
pub mod budget;
pub mod load;
pub mod membership;
pub mod registry;
pub mod role;

pub use admission::{Admission, IngestPermit, Limits, QueryPermit, Rejected};
pub use budget::{Account, Charge, Counts, Ledger, Phase};
pub use load::{Inflight, Load, ObjectCache};
pub use membership::Membership;
pub use registry::{NodeRecord, Registry, RegistryError, oldest_reader};
pub use role::{Role, UnknownRole};
