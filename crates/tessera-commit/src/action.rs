// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! What a commit does to the state of its log (spec section 6.5).
//!
//! Only the actions that an implemented log needs exist yet; manifest and
//! dataset-log actions arrive with those logs.

use serde::{Deserialize, Serialize};
use tessera_types::{FileId, FileMeta, Seq, WriterId};

/// One action of a commit.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Action {
    /// A key-value mutation: the cluster catalog, control stores and rule
    /// state are key-value logs.
    Kv(KvMutation),
    /// A partition manifest gains a file.
    AddFile(Box<FileMeta>),
    /// A partition manifest loses a file; the object is garbage-collected
    /// later (GC-1).
    RemoveFile {
        /// The file.
        file_id: FileId,
        /// Why it is removed.
        reason: RemoveReason,
    },
    /// Every row of this partition in `writer`'s segments up to `through`
    /// is represented by files in this manifest (IT-1).
    AdvanceWriter {
        /// The writer.
        writer: WriterId,
        /// The last adopted segment.
        through: Seq,
    },
    /// The partition is dropped (MF-6); its files are garbage-collected.
    DropPartition,
}

/// Why a file leaves a manifest.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoveReason {
    /// Replaced by the output of a compaction.
    Compacted,
    /// Removed by a retention policy.
    Expired,
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
    fn manifest_actions_serialize_with_explicit_tags() {
        let advance = Action::AdvanceWriter {
            writer: WriterId::from_u128(1),
            through: Seq::new(3),
        };
        let json = serde_json::to_string(&advance).unwrap();
        assert_eq!(
            json,
            r#"{"type":"advance_writer","writer":"00000000000000000000000000000001","through":3}"#
        );
        assert_eq!(serde_json::from_str::<Action>(&json).unwrap(), advance);
        let remove = Action::RemoveFile {
            file_id: FileId::from_u128(2),
            reason: RemoveReason::Compacted,
        };
        assert!(
            serde_json::to_string(&remove)
                .unwrap()
                .contains(r#""reason":"compacted""#)
        );
        assert_eq!(
            serde_json::to_string(&Action::DropPartition).unwrap(),
            r#"{"type":"drop_partition"}"#
        );
    }

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
