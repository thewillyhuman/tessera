// SPDX-License-Identifier: Apache-2.0

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

/// Bytes a partition's files hold in the object store at one instant:
/// what the store bills for, recorded so that `system.usage` reconciles
/// with the bill (section 13.1, M9).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Held {
    /// The tenant.
    pub tenant: TenantId,
    /// The dataset.
    pub dataset: DatasetName,
    /// The kind of event.
    pub kind: Kind,
    /// The bytes of the adopted files.
    pub bytes: u64,
    /// The rows of the adopted files.
    pub rows: u64,
}

/// Where compaction reports what it stored and what is held.
pub trait Meter: Send + Sync {
    /// Records `stored`; must not block.
    fn stored(&self, stored: Vec<Stored>);

    /// Records what is held now; must not block. A meter that records
    /// what was stored only may leave it unimplemented.
    fn held(&self, held: Vec<Held>) {
        drop(held);
    }
}
