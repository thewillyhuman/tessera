// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Tenants (spec section 12.2): the isolation, accounting and
//! authorization boundary, managed while the system runs.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use tessera_types::{Budget, InvalidBudget, Redaction, TenantId, Timestamp};

const MAX_NAME_LEN: usize = 63;

/// Why a string is not a tenant name.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("invalid tenant name {value:?}: {reason}")]
pub struct InvalidTenantName {
    value: String,
    reason: &'static str,
}

/// A tenant's display name: what `CREATE TENANT acme` and the
/// `X-Scope-OrgID` header use. Unique among live tenants, and changeable
/// by `ALTER TENANT ... RENAME TO`. Names starting with `_` are reserved
/// for built-in tenants.
///
/// A name is an SQL identifier: a lowercase letter or `_`, then lowercase
/// letters, digits and `_`, at most 63 characters.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct TenantName(String);

impl TenantName {
    /// Validates `value` as a tenant name.
    ///
    /// # Errors
    ///
    /// If `value` is not an identifier as described on the type.
    pub fn new(value: impl Into<String>) -> Result<Self, InvalidTenantName> {
        let value = value.into();
        let invalid = |reason| InvalidTenantName {
            value: value.clone(),
            reason,
        };
        let Some(first) = value.chars().next() else {
            return Err(invalid("must not be empty"));
        };
        if value.len() > MAX_NAME_LEN {
            return Err(invalid("must be at most 63 characters"));
        }
        if !(first.is_ascii_lowercase() || first == '_') {
            return Err(invalid("must start with a lowercase letter or '_'"));
        }
        if !value
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        {
            return Err(invalid(
                "may only contain lowercase letters, digits and '_'",
            ));
        }
        Ok(TenantName(value))
    }

    /// The name as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Whether the name is reserved for built-in tenants.
    #[must_use]
    pub fn is_reserved(&self) -> bool {
        self.0.starts_with('_')
    }
}

impl fmt::Display for TenantName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Debug for TenantName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TenantName({:?})", self.0)
    }
}

impl FromStr for TenantName {
    type Err = InvalidTenantName;

    fn from_str(value: &str) -> Result<Self, InvalidTenantName> {
        TenantName::new(value)
    }
}

impl TryFrom<String> for TenantName {
    type Error = InvalidTenantName;

    fn try_from(value: String) -> Result<Self, InvalidTenantName> {
        TenantName::new(value)
    }
}

impl From<TenantName> for String {
    fn from(name: TenantName) -> String {
        name.0
    }
}

/// Whether a tenant may be used.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "lowercase")]
pub enum TenantStatus {
    /// Accepts writes and queries.
    Active,
    /// Refuses writes and queries; data and configuration are kept.
    Suspended,
    /// Unreachable; its data is deleted after a grace period, during which
    /// it can be restored (CT-8).
    Dropped {
        /// When it was dropped.
        at: Timestamp,
    },
}

/// Per-tenant settings, all changeable at runtime (CT-7). `None` means
/// the cluster default applies. Fields are added as the components that
/// consume them arrive.
#[derive(Clone, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TenantSettings {
    /// Sustained ingest rate the tenant may use.
    pub ingest_rate_bytes_per_second: Option<u64>,
    /// How long data is kept unless a retention policy says otherwise.
    pub retention_seconds: Option<u64>,
    /// How many queries may run at once.
    pub query_concurrency: Option<u32>,
    /// The most bytes one query may read from files and the tail
    /// (QE-18); unlimited by default.
    pub query_max_scan_bytes: Option<u64>,
    /// The most bytes one query's blocking operators may hold (QE-18);
    /// the node's default otherwise.
    pub query_max_memory_bytes: Option<u64>,
    /// The longest one query may run, in seconds (QE-18); unlimited by
    /// default.
    pub query_max_seconds: Option<u64>,
    /// Whether a log body that is a JSON object has its fields stored
    /// under `body.*` (DM-10); on by default.
    pub parse_json_bodies: Option<bool>,
    /// Whether a log body that is a logfmt line has its pairs stored
    /// under `body.*` (DM-10); off by default.
    pub parse_logfmt_bodies: Option<bool>,
    /// The attribute path that names the team a row belongs to, for
    /// metering by team (CO-1); none by default.
    pub team_attribute: Option<String>,
    /// Bytes the tenant may ingest per UTC day (section 13.4).
    pub ingest_budget: Option<Budget>,
    /// Bytes the tenant's queries may scan per UTC day (section 13.4);
    /// its action is alert or reject.
    pub query_budget: Option<Budget>,
    /// Ingest budgets per team, by the value of the team attribute.
    pub team_ingest_budgets: BTreeMap<String, Budget>,
    /// What is dropped, hashed or masked at ingest (SE-4); a patch
    /// with any rules replaces them all.
    pub redactions: Vec<Redaction>,
    /// The name of the key, in the nodes' keyring, the tenant's blocks
    /// and files are sealed under (SE-2); none stores them plain. A
    /// change seals what is written from then on under the new key;
    /// what the old key sealed is read with it while it stays in the
    /// keyring.
    pub encryption_key: Option<String>,
}

/// Why settings were refused.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum InvalidSettings {
    /// A budget is not valid.
    #[error("{setting}: {reason}")]
    Budget {
        /// Which setting: `ingest_budget`, `query_budget` or
        /// `team_ingest_budgets.<team>`.
        setting: String,
        /// Why.
        reason: InvalidBudget,
    },
    /// A redaction rule's pattern is not a regular expression.
    #[error("redactions.{path}: {reason}")]
    Redaction {
        /// The rule's path.
        path: String,
        /// Why.
        reason: String,
    },
    /// A limit is zero, which would refuse every query.
    #[error("{0} must be greater than zero")]
    Zero(String),
    /// A name is empty.
    #[error("{0} must not be empty")]
    Empty(String),
}

impl TenantSettings {
    /// These settings with every value set in `patch` replacing the one
    /// here: how `ALTER TENANT ... SET` applies.
    #[must_use]
    pub fn merged(&self, patch: &TenantSettings) -> TenantSettings {
        TenantSettings {
            ingest_rate_bytes_per_second: patch
                .ingest_rate_bytes_per_second
                .or(self.ingest_rate_bytes_per_second),
            retention_seconds: patch.retention_seconds.or(self.retention_seconds),
            query_concurrency: patch.query_concurrency.or(self.query_concurrency),
            query_max_scan_bytes: patch.query_max_scan_bytes.or(self.query_max_scan_bytes),
            query_max_memory_bytes: patch.query_max_memory_bytes.or(self.query_max_memory_bytes),
            query_max_seconds: patch.query_max_seconds.or(self.query_max_seconds),
            parse_json_bodies: patch.parse_json_bodies.or(self.parse_json_bodies),
            parse_logfmt_bodies: patch.parse_logfmt_bodies.or(self.parse_logfmt_bodies),
            team_attribute: patch
                .team_attribute
                .clone()
                .or_else(|| self.team_attribute.clone()),
            ingest_budget: patch.ingest_budget.or(self.ingest_budget),
            query_budget: patch.query_budget.or(self.query_budget),
            team_ingest_budgets: self
                .team_ingest_budgets
                .iter()
                .chain(&patch.team_ingest_budgets)
                .map(|(team, budget)| (team.clone(), *budget))
                .collect(),
            redactions: if patch.redactions.is_empty() {
                self.redactions.clone()
            } else {
                patch.redactions.clone()
            },
            encryption_key: patch
                .encryption_key
                .clone()
                .or_else(|| self.encryption_key.clone()),
        }
    }

    /// Checks the settings before they are stored.
    ///
    /// # Errors
    ///
    /// If a budget is not valid for what it limits.
    pub fn validate(&self) -> Result<(), InvalidSettings> {
        let budget = |setting: String, result: Result<(), InvalidBudget>| {
            result.map_err(|reason| InvalidSettings::Budget { setting, reason })
        };
        if let Some(ingest) = &self.ingest_budget {
            budget("ingest_budget".into(), ingest.validate_for_ingest())?;
        }
        if let Some(query) = &self.query_budget {
            budget("query_budget".into(), query.validate_for_queries())?;
        }
        for (team, team_budget) in &self.team_ingest_budgets {
            budget(
                format!("team_ingest_budgets.{team}"),
                team_budget.validate_for_ingest(),
            )?;
        }
        for rule in &self.redactions {
            rule.validate()
                .map_err(|reason| InvalidSettings::Redaction {
                    path: rule.path.clone(),
                    reason,
                })?;
        }
        let limits = [
            ("query_max_scan_bytes", self.query_max_scan_bytes),
            ("query_max_memory_bytes", self.query_max_memory_bytes),
            ("query_max_seconds", self.query_max_seconds),
        ];
        if let Some((setting, _)) = limits.iter().find(|(_, value)| *value == Some(0)) {
            return Err(InvalidSettings::Zero((*setting).to_owned()));
        }
        if self
            .encryption_key
            .as_deref()
            .is_some_and(|k| k.trim().is_empty())
        {
            return Err(InvalidSettings::Empty("encryption_key".into()));
        }
        Ok(())
    }
}

/// One tenant as recorded in the catalog.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Tenant {
    /// The immutable identifier, used as the tenant's object prefix.
    pub id: TenantId,
    /// The current display name.
    pub name: TenantName,
    /// Whether the tenant may be used.
    pub status: TenantStatus,
    /// The tenant's settings.
    pub settings: TenantSettings,
    /// When the tenant was created, by the creating node's clock.
    pub created_at: Timestamp,
}

impl Tenant {
    /// Whether this is one of the two tenants that always exist (CT-4).
    #[must_use]
    pub fn is_builtin(&self) -> bool {
        self.id == TenantId::DEFAULT || self.id == TenantId::SYSTEM
    }
}

#[cfg(test)]
mod tests {
    use tessera_types::BudgetAction;

    use super::*;

    #[test]
    fn query_limits_merge_and_must_be_positive() {
        let base = TenantSettings {
            query_max_scan_bytes: Some(1 << 30),
            ..TenantSettings::default()
        };
        let patch = TenantSettings {
            query_max_seconds: Some(30),
            ..TenantSettings::default()
        };
        let merged = base.merged(&patch);
        assert_eq!(merged.query_max_scan_bytes, Some(1 << 30));
        assert_eq!(merged.query_max_seconds, Some(30));
        assert_eq!(merged.query_max_memory_bytes, None);
        merged.validate().unwrap();
        let zero = TenantSettings {
            query_max_memory_bytes: Some(0),
            ..TenantSettings::default()
        };
        assert_eq!(
            zero.validate(),
            Err(InvalidSettings::Zero("query_max_memory_bytes".into()))
        );
    }

    /// SE-2, CT-7: the key name merges like any setting and cannot be
    /// blank.
    #[test]
    fn the_encryption_key_names_a_key_and_is_kept_until_changed() {
        let base = TenantSettings {
            encryption_key: Some("k2025".into()),
            ..TenantSettings::default()
        };
        let unrelated = TenantSettings {
            query_concurrency: Some(4),
            ..TenantSettings::default()
        };
        assert_eq!(
            base.merged(&unrelated).encryption_key.as_deref(),
            Some("k2025")
        );
        let rotated = TenantSettings {
            encryption_key: Some("k2026".into()),
            ..TenantSettings::default()
        };
        assert_eq!(
            base.merged(&rotated).encryption_key.as_deref(),
            Some("k2026")
        );
        let blank = TenantSettings {
            encryption_key: Some("  ".into()),
            ..TenantSettings::default()
        };
        assert_eq!(
            blank.validate(),
            Err(InvalidSettings::Empty("encryption_key".into()))
        );
    }

    #[test]
    fn accepts_sql_identifiers_as_names() {
        for valid in ["acme", "acme_corp", "_system", "a1", &"x".repeat(63)] {
            assert!(TenantName::new(valid).is_ok(), "{valid:?}");
        }
    }

    #[test]
    fn rejects_other_names() {
        for invalid in [
            "",
            "Acme",
            "1acme",
            "acme-corp",
            "acme corp",
            &"x".repeat(64),
        ] {
            assert!(TenantName::new(invalid).is_err(), "{invalid:?}");
        }
    }

    #[test]
    fn underscore_names_are_reserved() {
        assert!(TenantName::new("_system").unwrap().is_reserved());
        assert!(!TenantName::new("system").unwrap().is_reserved());
    }

    #[test]
    fn merging_settings_keeps_values_the_patch_leaves_unset() {
        let current = TenantSettings {
            retention_seconds: Some(10),
            ..TenantSettings::default()
        };
        let patch = TenantSettings {
            query_concurrency: Some(4),
            ..TenantSettings::default()
        };
        let merged = current.merged(&patch);
        assert_eq!(merged.retention_seconds, Some(10));
        assert_eq!(merged.query_concurrency, Some(4));
    }

    #[test]
    fn merging_settings_merges_team_budgets_by_team() {
        let budget = |bytes| Budget {
            bytes_per_day: bytes,
            action: BudgetAction::Alert,
        };
        let current = TenantSettings {
            team_ingest_budgets: BTreeMap::from([
                ("a".to_owned(), budget(1)),
                ("b".to_owned(), budget(2)),
            ]),
            ..TenantSettings::default()
        };
        let patch = TenantSettings {
            team_ingest_budgets: BTreeMap::from([("b".to_owned(), budget(3))]),
            ..TenantSettings::default()
        };
        let merged = current.merged(&patch);
        assert_eq!(merged.team_ingest_budgets["a"], budget(1));
        assert_eq!(merged.team_ingest_budgets["b"], budget(3));
    }

    #[test]
    fn validation_names_the_budget_that_is_wrong() {
        let settings = TenantSettings {
            query_budget: Some(Budget {
                bytes_per_day: 1,
                action: BudgetAction::Sample { rate: 0.5 },
            }),
            ..TenantSettings::default()
        };
        assert_eq!(
            settings.validate().unwrap_err().to_string(),
            "query_budget: a query budget cannot sample; use alert or reject"
        );
        let settings = TenantSettings {
            team_ingest_budgets: BTreeMap::from([(
                "ads".to_owned(),
                Budget {
                    bytes_per_day: 0,
                    action: BudgetAction::Reject,
                },
            )]),
            ..TenantSettings::default()
        };
        assert!(
            settings
                .validate()
                .unwrap_err()
                .to_string()
                .starts_with("team_ingest_budgets.ads:")
        );
        assert_eq!(TenantSettings::default().validate(), Ok(()));
    }

    #[test]
    fn settings_tolerate_missing_fields_when_decoded() {
        let settings: TenantSettings = serde_json::from_str("{}").unwrap();
        assert_eq!(settings, TenantSettings::default());
    }

    #[test]
    fn status_serializes_with_a_tag() {
        let dropped = TenantStatus::Dropped {
            at: Timestamp::from_nanos(5),
        };
        assert_eq!(
            serde_json::to_string(&dropped).unwrap(),
            r#"{"state":"dropped","at":5}"#
        );
        assert_eq!(
            serde_json::to_string(&TenantStatus::Active).unwrap(),
            r#"{"state":"active"}"#
        );
    }
}
