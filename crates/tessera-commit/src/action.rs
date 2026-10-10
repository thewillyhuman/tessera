// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! What a commit does to the state of its log (spec section 6.5).
//!
//! Only the actions that an implemented log needs exist yet; manifest and
//! dataset-log actions arrive with those logs.

use serde::{Deserialize, Serialize};

/// One action of a commit.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Action {
    /// A key-value mutation: the cluster catalog, control stores and rule
    /// state are key-value logs.
    Kv(KvMutation),
}

/// A mutation of one key in a key-value log.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum KvMutation {
    /// Sets `key` to `value`, replacing any previous value.
    Put {
        /// The key.
        key: String,
        /// The value, as text (configuration records are JSON documents).
        value: String,
    },
    /// Removes `key`; removing an absent key is a no-op.
    Delete {
        /// The key.
        key: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn actions_serialize_with_explicit_tags() {
        let action = Action::Kv(KvMutation::Put {
            key: "a".into(),
            value: "1".into(),
        });
        let json = serde_json::to_string(&action).unwrap();
        assert_eq!(json, r#"{"type":"kv","op":"put","key":"a","value":"1"}"#);
        assert_eq!(serde_json::from_str::<Action>(&json).unwrap(), action);
    }
}
