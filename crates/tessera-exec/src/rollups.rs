// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Where the engine learns what rollups exist (section 8.7, AD-13): a
//! provider the node gives, answering per tenant with the rollups the
//! planner may read instead of the raw rows, each with the time its
//! output covers.

use futures::future::BoxFuture;
use tessera_ir::rollup::RollupEntry;
use tessera_types::TenantId;

/// Answers the rollups of a tenant.
pub trait RollupSource: Send + Sync {
    /// The enabled rollups of `tenant` with output to read; empty when
    /// there are none or they cannot be read right now, which only
    /// costs the rewrite.
    fn entries<'a>(&'a self, tenant: &'a TenantId) -> BoxFuture<'a, Vec<RollupEntry>>;
}
