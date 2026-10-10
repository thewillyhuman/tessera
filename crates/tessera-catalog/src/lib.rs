// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The cluster catalog (spec section 12.1): the only cluster-wide mutable
//! state. It holds tenants, storage pools, slot counts and cluster-wide
//! settings, changes at human rate, and is a commit log in the root pool.
//!
//! The catalog is a library used by every role, not a process.

pub mod catalog;
pub mod state;
pub mod tenant;

pub use catalog::{Catalog, CatalogError};
pub use state::{CatalogState, PoolConfig};
pub use tenant::{InvalidTenantName, Tenant, TenantName, TenantSettings, TenantStatus};
