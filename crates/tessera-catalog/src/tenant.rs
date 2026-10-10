// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Tenants (spec section 12.2): the isolation, accounting and
//! authorization boundary, managed while the system runs.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use tessera_types::{TenantId, Timestamp};

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
        }
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
    use super::*;

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
