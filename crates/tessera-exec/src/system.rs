// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The rows of the `system` tables (CS-3, CQ-13). The engine plans them
//! from their static schemas and asks a provider for the rows at run
//! time; the node provides them from the control stores and the rule
//! state logs.

use arrow::array::RecordBatch;
use futures::future::BoxFuture;
use tessera_types::TenantId;

/// Where the rows of the `system` tables come from.
pub trait SystemTables: Send + Sync {
    /// The rows of `system.<table>` of `tenant`, in the table's schema.
    fn rows<'a>(
        &'a self,
        tenant: &'a TenantId,
        table: &'a str,
    ) -> BoxFuture<'a, Result<Vec<RecordBatch>, String>>;
}
