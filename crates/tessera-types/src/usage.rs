// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Metering (section 13.1): what ingest received, compaction stored and
//! queries cost, per tenant, dataset, kind, service and team, and for
//! stored bytes per log pattern or metric name. The records are written
//! as events of `_system.usage` and read back as `system.usage` (CO-4).

use crate::{DatasetName, Kind, TenantId};

/// Bytes stored for one group of rows of a compacted file (CO-2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Stored {
    /// The tenant.
    pub tenant: TenantId,
    /// The dataset.
    pub dataset: DatasetName,
    /// The kind of event.
    pub kind: Kind,
    /// The service, when the rows carry one.
    pub service: Option<String>,
    /// The log pattern, for logs with one assigned.
    pub pattern_id: Option<i64>,
    /// The pattern's template, for logs with one assigned.
    pub pattern: Option<String>,
    /// The metric, span or event name, for those kinds.
    pub name: Option<String>,
    /// The rows of the group.
    pub rows: u64,
    /// The file bytes attributed to the group, by its share of rows.
    pub bytes: u64,
}

/// Where compaction reports what it stored.
pub trait Meter: Send + Sync {
    /// Records `stored`; must not block.
    fn stored(&self, stored: Vec<Stored>);
}
