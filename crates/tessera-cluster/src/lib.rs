// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The cluster (spec sections 5.2 and 14): the roles a node runs, the
//! node registry in the root pool, the membership view built from it,
//! and admission control.
//!
//! Membership is a hint (P2): a wrong view slows the system down, it
//! cannot make it incorrect, because every effect of leased work passes
//! through a conditional write (OP-2). Nodes discover each other through
//! the registry alone in this release; gossip and DNS seeds (section
//! 14.1) can be added without changing what membership promises.

pub mod membership;
pub mod registry;
pub mod role;

pub use membership::Membership;
pub use registry::{NodeRecord, Registry, RegistryError};
pub use role::{Role, UnknownRole};
