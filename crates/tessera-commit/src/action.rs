// SPDX-License-Identifier: Apache-2.0

//! What a commit does to the state of its log (spec section 6.5).
//!
//! Only the actions that an implemented log needs exist yet; manifest and
//! dataset-log actions arrive with those logs.

use serde::{Deserialize, Serialize};
use tessera_types::{FileId, FileMeta, Seq, Timestamp, WriterId};

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
    /// once no snapshot within the retention window can reference it
    /// (GC-1), which is measured from `at`.
    RemoveFile {
        /// The file.
        file_id: FileId,
        /// Why it is removed.
        reason: RemoveReason,
        /// When it was removed, by the committer's clock. Records written
        /// before this field carry the epoch and are collected at once.
        #[serde(default)]
        at: Timestamp,
    },
    /// The tombstones of removed files whose objects have been deleted
    /// are dropped from the manifest (GC-3).
    Forget {
        /// The files.
        file_ids: Vec<FileId>,
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
            at: Timestamp::from_nanos(5),
        };
        let json = serde_json::to_string(&remove).unwrap();
        assert!(json.contains(r#""reason":"compacted""#));
        assert_eq!(serde_json::from_str::<Action>(&json).unwrap(), remove);
        let old: Action = serde_json::from_str(
            r#"{"type":"remove_file","file_id":"00000000000000000000000000000002","reason":"expired"}"#,
        )
        .unwrap();
        assert!(matches!(old, Action::RemoveFile { at, .. } if at == Timestamp::UNIX_EPOCH));
        let forget = Action::Forget {
            file_ids: vec![FileId::from_u128(2)],
        };
        let json = serde_json::to_string(&forget).unwrap();
        assert!(json.starts_with(r#"{"type":"forget""#));
        assert_eq!(serde_json::from_str::<Action>(&json).unwrap(), forget);
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
