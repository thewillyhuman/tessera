// SPDX-License-Identifier: Apache-2.0

//! Object keys.
//!
//! A key is a `/`-separated path of components. The rules below keep every
//! key usable verbatim on S3-compatible stores and as a relative path on a
//! local filesystem (AR-2), with no escaping anywhere:
//!
//! - at least one component, and at most 1024 bytes in total;
//! - each component is non-empty and made of ASCII letters, digits, `.`,
//!   `_` and `-`;
//! - no component starts with `.`, which rules out `.` and `..` and leaves
//!   dot-prefixed names free for implementation-private files.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

const MAX_KEY_LEN: usize = 1024;

/// Why a string is not a valid key or key component.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("invalid object key {value:?}: {reason}")]
pub struct InvalidKey {
    value: String,
    reason: &'static str,
}

impl InvalidKey {
    fn new(value: &str, reason: &'static str) -> Self {
        InvalidKey {
            value: value.to_owned(),
            reason,
        }
    }
}

/// A validated object key. See the module documentation for the rules.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Key(String);

impl Key {
    /// Validates `value` as a key.
    ///
    /// # Errors
    ///
    /// If `value` breaks the rules in the module documentation.
    pub fn new(value: impl Into<String>) -> Result<Self, InvalidKey> {
        let value = value.into();
        if value.len() > MAX_KEY_LEN {
            return Err(InvalidKey::new(&value, "longer than 1024 bytes"));
        }
        for component in value.split('/') {
            validate_component(component).map_err(|reason| InvalidKey::new(&value, reason))?;
        }
        Ok(Key(value))
    }

    /// The key with `component` appended as one more path component.
    ///
    /// # Errors
    ///
    /// If `component` is not a single valid component, or the result is
    /// too long.
    pub fn join(&self, component: &str) -> Result<Self, InvalidKey> {
        validate_component(component).map_err(|reason| InvalidKey::new(component, reason))?;
        Key::new(format!("{}/{component}", self.0))
    }

    /// The key as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The last component.
    #[must_use]
    pub fn name(&self) -> &str {
        self.0.rsplit('/').next().unwrap_or(&self.0)
    }

    /// Whether this key is `prefix` itself or lies below it, component-wise:
    /// `a/b` is under `a`, `ab` is not.
    #[must_use]
    pub fn is_under(&self, prefix: &Key) -> bool {
        self.0
            .strip_prefix(&prefix.0)
            .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
    }
}

fn validate_component(component: &str) -> Result<(), &'static str> {
    let Some(first) = component.chars().next() else {
        return Err("empty component");
    };
    if first == '.' {
        return Err("component starts with '.'");
    }
    if !component
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
    {
        return Err("component contains a character outside [A-Za-z0-9._-]");
    }
    Ok(())
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Debug for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Key({:?})", self.0)
    }
}

impl AsRef<str> for Key {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl FromStr for Key {
    type Err = InvalidKey;

    fn from_str(value: &str) -> Result<Self, InvalidKey> {
        Key::new(value)
    }
}

impl TryFrom<String> for Key {
    type Error = InvalidKey;

    fn try_from(value: String) -> Result<Self, InvalidKey> {
        Key::new(value)
    }
}

impl From<Key> for String {
    fn from(key: Key) -> String {
        key.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_well_formed_keys() {
        for valid in [
            "a",
            "a/b/c",
            "_cluster/catalog/_log/00000000000000000001",
            "x.parquet",
            "A-b_c.d",
        ] {
            assert!(Key::new(valid).is_ok(), "{valid:?} should be valid");
        }
    }

    #[test]
    fn rejects_malformed_keys() {
        let long = "a".repeat(1025);
        for invalid in [
            "",
            "/a",
            "a/",
            "a//b",
            "a/../b",
            "./a",
            "a/.hidden",
            "a b",
            "a\\b",
            "ä",
            &long,
        ] {
            assert!(Key::new(invalid).is_err(), "{invalid:?} should be invalid");
        }
    }

    #[test]
    fn join_appends_one_component() {
        let key = Key::new("a").unwrap().join("b").unwrap();
        assert_eq!(key.as_str(), "a/b");
        assert!(Key::new("a").unwrap().join("b/c").is_err());
        assert!(Key::new("a").unwrap().join("").is_err());
    }

    #[test]
    fn name_is_the_last_component() {
        assert_eq!(Key::new("a/b/c").unwrap().name(), "c");
        assert_eq!(Key::new("a").unwrap().name(), "a");
    }

    #[test]
    fn prefix_test_works_on_whole_components() {
        let prefix = Key::new("a").unwrap();
        assert!(Key::new("a/b").unwrap().is_under(&prefix));
        assert!(Key::new("a").unwrap().is_under(&prefix));
        assert!(!Key::new("ab").unwrap().is_under(&prefix));
        assert!(!Key::new("b/a").unwrap().is_under(&prefix));
    }

    #[test]
    fn keys_order_as_their_text() {
        assert!(Key::new("a/1").unwrap() < Key::new("a/2").unwrap());
        assert!(Key::new("a/2").unwrap() < Key::new("b").unwrap());
    }

    #[test]
    fn key_serializes_as_a_string() {
        let key = Key::new("a/b").unwrap();
        assert_eq!(serde_json::to_string(&key).unwrap(), "\"a/b\"");
        assert_eq!(serde_json::from_str::<Key>("\"a/b\"").unwrap(), key);
        assert!(serde_json::from_str::<Key>("\"/a\"").is_err());
    }
}
