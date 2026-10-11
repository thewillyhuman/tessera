// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Redaction rules (SE-4): what to do with an attribute before anything
//! is written. A rule names an attribute path, or every path under a
//! prefix with `prefix.*`, and drops the value, replaces it with the
//! hex SHA-256 of its text, or masks it. Rules are tenant settings and
//! apply at ingest, so a redacted value never reaches a writer log.

use serde::{Deserialize, Serialize};

/// What happens to a matching attribute.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RedactionAction {
    /// The attribute is removed.
    Drop,
    /// The value is replaced by the hex SHA-256 of its text, so equal
    /// values stay equal and can still be grouped and joined.
    Hash,
    /// The value is replaced by `***`.
    Mask,
}

/// One rule.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Redaction {
    /// The attribute path, such as `attr.user.email`, or `attr.user.*`
    /// for every path under `attr.user.`; `body` is a log's body.
    pub path: String,
    /// What happens to it.
    pub action: RedactionAction,
}

impl Redaction {
    /// Whether the rule applies to `path`.
    #[must_use]
    pub fn matches(&self, path: &str) -> bool {
        match self.path.strip_suffix(".*") {
            Some(prefix) => path
                .strip_prefix(prefix)
                .is_some_and(|rest| rest.starts_with('.')),
            None => self.path == path,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_rule_matches_its_path_or_everything_under_a_prefix() {
        let exact = Redaction {
            path: "attr.user.email".into(),
            action: RedactionAction::Hash,
        };
        assert!(exact.matches("attr.user.email"));
        assert!(!exact.matches("attr.user.email.domain"));
        assert!(!exact.matches("attr.user"));
        let under = Redaction {
            path: "attr.user.*".into(),
            action: RedactionAction::Drop,
        };
        assert!(under.matches("attr.user.email"));
        assert!(under.matches("attr.user.address.city"));
        assert!(!under.matches("attr.user"));
        assert!(!under.matches("attr.username"));
    }

    #[test]
    fn rules_read_from_json() {
        let rule: Redaction =
            serde_json::from_str(r#"{"path": "body", "action": "mask"}"#).unwrap();
        assert_eq!(rule.action, RedactionAction::Mask);
    }
}
