// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The cluster catalog (spec section 12.1): the only cluster-wide mutable
//! state. It holds tenants, storage pools, slot counts and cluster-wide
//! settings, changes at human rate, and is a commit log in the root pool.
//!
//! The catalog is a library used by every role, not a process.

pub mod catalog;
pub mod control;
pub mod state;
pub mod tenant;

pub use catalog::{Catalog, CatalogError};
pub use control::{
    ControlError, ControlStore, ControlStores, Row, Table, Transaction, UnknownTable,
};
pub use state::{CatalogState, PoolConfig};
pub use tenant::{
    InvalidSettings, InvalidTenantName, Tenant, TenantName, TenantSettings, TenantStatus,
};
