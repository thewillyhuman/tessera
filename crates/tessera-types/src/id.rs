// SPDX-License-Identifier: Apache-2.0

//! Identifiers of tenants, datasets, writers, files and nodes.
//!
//! Every identifier here appears in object keys (spec section 6.2), so each
//! is validated to be a safe key component: lowercase ASCII letters, digits,
//! `_` and `-`, at most 64 characters.

use std::borrow::Cow;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::Random;

const MAX_NAME_LEN: usize = 64;

/// Why a string is not a valid identifier.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("invalid {what} {value:?}: {reason}")]
pub struct InvalidId {
    what: &'static str,
    value: String,
    reason: &'static str,
}

impl InvalidId {
    fn new(what: &'static str, value: &str, reason: &'static str) -> Self {
        InvalidId {
            what,
            value: value.to_owned(),
            reason,
        }
    }
}

/// Checks the shared rules for human-readable identifiers used in keys.
fn validate_name(
    what: &'static str,
    value: &str,
    allow_leading_underscore: bool,
) -> Result<(), InvalidId> {
    let Some(first) = value.chars().next() else {
        return Err(InvalidId::new(what, value, "must not be empty"));
    };
    if value.len() > MAX_NAME_LEN {
        return Err(InvalidId::new(what, value, "must be at most 64 characters"));
    }
    if !value
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
    {
        return Err(InvalidId::new(
            what,
            value,
            "may only contain lowercase ASCII letters, digits, '_' and '-'",
        ));
    }
    if first == '-' || (first == '_' && !allow_leading_underscore) {
        return Err(InvalidId::new(
            what,
            value,
            "must start with a letter or digit",
        ));
    }
    Ok(())
}

/// Defines a validated string identifier with string conversions and
/// string-based serde.
macro_rules! name_id {
    ($(#[$doc:meta])* $name:ident, $what:literal, leading_underscore = $underscore:literal) => {
        $(#[$doc])*
        #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(Cow<'static, str>);

        impl $name {
            /// Validates `value` as an identifier of this type.
            ///
            /// # Errors
            ///
            /// If `value` breaks the identifier rules in the module documentation.
            pub fn new(value: impl Into<String>) -> Result<Self, InvalidId> {
                let value = value.into();
                validate_name($what, &value, $underscore)?;
                Ok($name(Cow::Owned(value)))
            }

            /// The identifier as text.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({:?})", stringify!($name), self.0)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl FromStr for $name {
            type Err = InvalidId;

            fn from_str(value: &str) -> Result<Self, InvalidId> {
                Self::new(value)
            }
        }

        impl TryFrom<String> for $name {
            type Error = InvalidId;

            fn try_from(value: String) -> Result<Self, InvalidId> {
                Self::new(value)
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> String {
                value.0.into_owned()
            }
        }
    };
}

name_id! {
    /// The immutable identifier of a tenant, used as its object prefix
    /// (spec section 6.2). Distinct from the tenant's display name, which
    /// can change.
    ///
    /// User tenants get a random identifier; the two built-in tenants
    /// (CT-4) have the well-known identifiers [`TenantId::DEFAULT`] and
    /// [`TenantId::SYSTEM`].
    TenantId, "tenant id", leading_underscore = true
}

impl TenantId {
    /// The tenant that a single-team installation uses without setup.
    pub const DEFAULT: TenantId = TenantId(Cow::Borrowed("default"));
    /// The tenant that receives Tessera's own telemetry (section 14.7).
    pub const SYSTEM: TenantId = TenantId(Cow::Borrowed("_system"));

    /// A fresh random identifier: 128 random bits as lowercase hex.
    pub fn random(random: &mut impl Random) -> Self {
        TenantId(Cow::Owned(Id128::random(random).to_string()))
    }
}

name_id! {
    /// The name of a dataset within a tenant. Names starting with `_` are
    /// reserved for Tessera's own prefixes under the tenant (`_control`,
    /// `_eval`).
    DatasetName, "dataset name", leading_underscore = false
}

name_id! {
    /// The name of a storage pool (ST-1, ST-2). Every object reference in
    /// metadata carries one.
    PoolId, "pool id", leading_underscore = false
}

/// A 128-bit identifier, formatted as 32 lowercase hex digits.
///
/// The building block of [`WriterId`], [`FileId`] and [`NodeId`]; other
/// crates wrap it for their own identifier types.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Id128(u128);

impl Id128 {
    /// Wraps an existing value.
    #[must_use]
    pub const fn from_u128(value: u128) -> Self {
        Id128(value)
    }

    /// The identifier as an integer.
    #[must_use]
    pub const fn as_u128(self) -> u128 {
        self.0
    }

    /// A fresh identifier from 128 random bits.
    pub fn random(random: &mut impl Random) -> Self {
        Id128(random.next_u128())
    }
}

impl fmt::Display for Id128 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:032x}", self.0)
    }
}

impl fmt::Debug for Id128 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Id128({self})")
    }
}

impl FromStr for Id128 {
    type Err = InvalidId;

    fn from_str(value: &str) -> Result<Self, InvalidId> {
        if value.len() != 32 {
            return Err(InvalidId::new("id", value, "must be 32 hex digits"));
        }
        u128::from_str_radix(value, 16)
            .map(Id128)
            .map_err(|_| InvalidId::new("id", value, "must be 32 hex digits"))
    }
}

impl TryFrom<String> for Id128 {
    type Error = InvalidId;

    fn try_from(value: String) -> Result<Self, InvalidId> {
        value.parse()
    }
}

impl From<Id128> for String {
    fn from(value: Id128) -> String {
        value.to_string()
    }
}

/// Defines a typed wrapper around [`Id128`].
macro_rules! id128 {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Id128);

        impl $name {
            /// Wraps an existing value.
            #[must_use]
            pub const fn from_u128(value: u128) -> Self {
                $name(Id128::from_u128(value))
            }

            /// A fresh identifier from 128 random bits.
            pub fn random(random: &mut impl Random) -> Self {
                $name(Id128::random(random))
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, f)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({})", stringify!($name), self.0)
            }
        }

        impl FromStr for $name {
            type Err = InvalidId;

            fn from_str(value: &str) -> Result<Self, InvalidId> {
                value.parse().map($name)
            }
        }
    };
}

id128! {
    /// The identity of one writer log, chosen at random when an ingester
    /// starts (ST-5).
    WriterId
}

id128! {
    /// The identity of one data file and its sidecar.
    FileId
}

id128! {
    /// The identity of one node process.
    NodeId
}

impl FileId {
    /// The `{hh}` key component that spreads files over the key space
    /// (ST-4, section 6.2): the first two hex digits of the id.
    #[must_use]
    pub fn hash_prefix(&self) -> String {
        self.to_string()[..2].to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SeededRandom;

    #[test]
    fn accepts_valid_tenant_ids() {
        for valid in ["default", "_system", "acme-corp", "a", &"f".repeat(64)] {
            assert!(TenantId::new(valid).is_ok(), "{valid:?} should be valid");
        }
    }

    #[test]
    fn rejects_invalid_tenant_ids() {
        for invalid in ["", "Acme", "a/b", "a.b", "-x", &"f".repeat(65), "a b"] {
            assert!(
                TenantId::new(invalid).is_err(),
                "{invalid:?} should be invalid"
            );
        }
    }

    #[test]
    fn builtin_tenant_ids_are_valid() {
        assert_eq!(TenantId::DEFAULT.as_str(), "default");
        assert_eq!(TenantId::SYSTEM.as_str(), "_system");
    }

    #[test]
    fn random_tenant_id_is_32_lowercase_hex_digits() {
        let id = TenantId::random(&mut SeededRandom::new(1));
        assert_eq!(id.as_str().len(), 32);
        assert!(
            id.as_str()
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_uppercase())
        );
    }

    #[test]
    fn dataset_name_rejects_reserved_prefixes() {
        assert!(DatasetName::new("prod").is_ok());
        assert!(DatasetName::new("_control").is_err());
        assert!(DatasetName::new("-x").is_err());
    }

    #[test]
    fn id128_round_trips_through_hex() {
        let id = Id128::from_u128(0x0123_4567_89ab_cdef_0011_2233_4455_6677);
        let text = id.to_string();
        assert_eq!(text, "0123456789abcdef0011223344556677");
        assert_eq!(text.parse::<Id128>().unwrap(), id);
    }

    #[test]
    fn id128_rejects_malformed_hex() {
        assert!("0123".parse::<Id128>().is_err());
        assert!("zz23456789abcdef0011223344556677".parse::<Id128>().is_err());
    }

    #[test]
    fn file_id_hash_prefix_is_its_first_two_hex_digits() {
        let id = FileId::from_u128(0xab00_0000_0000_0000_0000_0000_0000_0001);
        assert_eq!(id.hash_prefix(), "ab");
    }

    #[test]
    fn random_ids_differ() {
        let mut random = SeededRandom::new(5);
        assert_ne!(WriterId::random(&mut random), WriterId::random(&mut random));
    }

    #[test]
    fn identifiers_serialize_as_strings() {
        let tenant = TenantId::new("acme").unwrap();
        assert_eq!(serde_json::to_string(&tenant).unwrap(), "\"acme\"");
        let node = NodeId::from_u128(1);
        assert_eq!(
            serde_json::to_string(&node).unwrap(),
            "\"00000000000000000000000000000001\""
        );
        assert_eq!(
            serde_json::from_str::<TenantId>("\"acme\"").unwrap(),
            tenant
        );
        assert!(serde_json::from_str::<TenantId>("\"Not Valid\"").is_err());
    }
}
