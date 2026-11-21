// SPDX-License-Identifier: Apache-2.0

//! The kinds of single-owner work that are assigned through slots and
//! leases (spec section 5.3).

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// A kind of work that one node at a time does for each item (AR-4).
/// Each kind has its own slot count in the cluster catalog and its own
/// leases under `_cluster/leases/{kind}/`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkKind {
    /// Moving a writer's L1 files into partition manifests (section 6.7).
    Intake,
    /// Committing to a partition manifest and planning its compaction
    /// (CL-4, CP-4).
    Partition,
    /// Evaluating a continuous-query rule group (section 11.3).
    RuleGroup,
    /// Sealing and flushing the log of a writer believed dead (ST-9).
    Recovery,
}

/// Why a string is not a work kind.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("unknown work kind {0:?}")]
pub struct UnknownWorkKind(String);

impl WorkKind {
    /// Every kind, in declaration order.
    pub const ALL: [WorkKind; 4] = [
        WorkKind::Intake,
        WorkKind::Partition,
        WorkKind::RuleGroup,
        WorkKind::Recovery,
    ];

    /// The name used in keys and in the catalog.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            WorkKind::Intake => "intake",
            WorkKind::Partition => "partition",
            WorkKind::RuleGroup => "rule-group",
            WorkKind::Recovery => "recovery",
        }
    }
}

impl fmt::Display for WorkKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for WorkKind {
    type Err = UnknownWorkKind;

    fn from_str(value: &str) -> Result<Self, UnknownWorkKind> {
        WorkKind::ALL
            .into_iter()
            .find(|kind| kind.as_str() == value)
            .ok_or_else(|| UnknownWorkKind(value.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn work_kind_round_trips_through_its_name() {
        for kind in WorkKind::ALL {
            assert_eq!(kind.as_str().parse::<WorkKind>().unwrap(), kind);
        }
    }

    #[test]
    fn names_are_valid_key_components() {
        for kind in WorkKind::ALL {
            assert!(
                kind.as_str()
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c == '-')
            );
        }
    }

    #[test]
    fn rejects_unknown_work_kind() {
        assert!("gardening".parse::<WorkKind>().is_err());
    }
}
