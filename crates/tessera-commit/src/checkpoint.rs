// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Checkpoints of a commit log's state (CL-3).
//!
//! A checkpoint at version *V* is the state folded through commit *V*,
//! written at `{prefix}/_checkpoint/{V:020}`. A cold handle loads the
//! newest checkpoint and replays only the commits after it. The object
//! `{prefix}/_hint` names the newest checkpoint; it is a hint (P2): if it is
//! missing or wrong, the checkpoints are listed instead.
//!
//! Checkpoints are whole-state objects, JSON unless a state encodes
//! itself otherwise through [`State::encode_checkpoint`]: a partition
//! manifest writes Parquet, which a reader scans in part by range
//! (MF-5). The tiering of CL-3 (a base plus incremental checkpoints)
//! is deferred until a log whose state is large enough to need it
//! exists; the format version leaves room for it.
//!
//! [`State::encode_checkpoint`]: crate::State::encode_checkpoint

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tessera_types::Version;

use crate::{DecodeError, FORMAT};

/// What is written at `_checkpoint/{version}`.
#[derive(Serialize, Deserialize)]
pub(crate) struct CheckpointRecord<S> {
    format: u32,
    version: Version,
    state: S,
}

/// A record over a borrowed state, for encoding without a clone.
#[derive(Serialize)]
struct BorrowedRecord<'a, S> {
    format: u32,
    version: Version,
    state: &'a S,
}

impl<S: Serialize + DeserializeOwned> CheckpointRecord<S> {
    fn new_borrowed(version: Version, state: &S) -> BorrowedRecord<'_, S> {
        BorrowedRecord {
            format: FORMAT,
            version,
            state,
        }
    }

    /// Parses a checkpoint object into its version and state.
    fn decode(bytes: &[u8]) -> Result<(Version, S), DecodeError> {
        let record: CheckpointRecord<S> = serde_json::from_slice(bytes)?;
        if record.format > FORMAT {
            return Err(DecodeError::UnsupportedFormat {
                format: record.format,
                supported: FORMAT,
            });
        }
        Ok((record.version, record.state))
    }
}

/// `state` at `version` as a JSON checkpoint object: what a state
/// encodes as unless it says otherwise.
pub fn encode_json<S: Serialize + DeserializeOwned>(version: Version, state: &S) -> Vec<u8> {
    serde_json::to_vec(&CheckpointRecord::<S>::new_borrowed(version, state))
        .expect("a checkpoint always serializes")
}

/// The state in a JSON checkpoint object written at `version`.
///
/// # Errors
///
/// If the object does not parse, was written by a newer release or
/// claims another version.
pub fn decode_json<S: Serialize + DeserializeOwned>(
    version: Version,
    bytes: &[u8],
) -> Result<S, DecodeError> {
    let (found, state) = CheckpointRecord::<S>::decode(bytes)?;
    if found != version {
        return Err(DecodeError::WrongVersion {
            claimed: found,
            expected: version,
        });
    }
    Ok(state)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use futures::TryStreamExt;
    use tessera_object::{Key, MemoryStore, ObjectStore};
    use tessera_types::{Env, Timestamp, Version};

    use crate::{Action, CommitError, CommitLog, KvMutation, KvState};

    fn prefix() -> Key {
        Key::new("t/_control").unwrap()
    }

    fn log(store: &MemoryStore, seed: u64) -> CommitLog<KvState> {
        CommitLog::new(
            Arc::new(store.clone()),
            prefix(),
            Env::simulated(seed, Timestamp::UNIX_EPOCH),
        )
    }

    fn put(key: &str, value: &str) -> Action {
        Action::Kv(KvMutation::Put {
            key: key.into(),
            value: value.into(),
        })
    }

    /// Commits `count` puts through `log` and returns the latest snapshot.
    async fn commit_puts(log: &CommitLog<KvState>, count: u64) -> Arc<crate::Snapshot<KvState>> {
        let mut base = log.snapshot().await.unwrap();
        for i in 1..=count {
            base = log
                .commit(&base, vec![put(&format!("k{i}"), "v")])
                .await
                .unwrap();
        }
        base
    }

    async fn delete_commits_through(store: &MemoryStore, version: u64) {
        for v in 1..=version {
            let key = prefix()
                .join("_log")
                .unwrap()
                .join(&Version::new(v).to_string())
                .unwrap();
            store.delete(&key).await.unwrap();
        }
    }

    #[tokio::test]
    async fn a_cold_handle_starts_from_the_checkpoint() {
        let store = MemoryStore::new();
        let writer = log(&store, 1);
        let at_three = commit_puts(&writer, 3).await;
        writer.checkpoint(&at_three).await.unwrap();
        delete_commits_through(&store, 3).await; // as garbage collection would
        let cold = log(&store, 2).snapshot().await.unwrap();
        assert_eq!(cold.version(), Version::new(3));
        assert_eq!(cold.state().len(), 3);
    }

    #[tokio::test]
    async fn commits_after_the_checkpoint_are_replayed() {
        let store = MemoryStore::new();
        let writer = log(&store, 1);
        let at_two = commit_puts(&writer, 2).await;
        writer.checkpoint(&at_two).await.unwrap();
        writer.commit(&at_two, vec![put("k3", "v")]).await.unwrap();
        let cold = log(&store, 2).snapshot().await.unwrap();
        assert_eq!(cold.version(), Version::new(3));
        assert_eq!(cold.state().get("k3"), Some("v"));
    }

    #[tokio::test]
    async fn a_wrong_hint_falls_back_to_listing_the_checkpoints() {
        let store = MemoryStore::new();
        let writer = log(&store, 1);
        let at_two = commit_puts(&writer, 2).await;
        writer.checkpoint(&at_two).await.unwrap();
        delete_commits_through(&store, 2).await;
        store
            .put(&prefix().join("_hint").unwrap(), Bytes::from_static(b"99"))
            .await
            .unwrap();
        let cold = log(&store, 2).snapshot().await.unwrap();
        assert_eq!(cold.version(), Version::new(2));
    }

    #[tokio::test]
    async fn a_missing_hint_falls_back_to_listing_the_checkpoints() {
        let store = MemoryStore::new();
        let writer = log(&store, 1);
        let at_two = commit_puts(&writer, 2).await;
        writer.checkpoint(&at_two).await.unwrap();
        delete_commits_through(&store, 2).await;
        store
            .delete(&prefix().join("_hint").unwrap())
            .await
            .unwrap();
        let cold = log(&store, 2).snapshot().await.unwrap();
        assert_eq!(cold.version(), Version::new(2));
    }

    #[tokio::test]
    async fn checkpointing_a_version_twice_is_harmless() {
        let store = MemoryStore::new();
        let writer = log(&store, 1);
        let at_one = commit_puts(&writer, 1).await;
        writer.checkpoint(&at_one).await.unwrap();
        writer.checkpoint(&at_one).await.unwrap();
    }

    #[tokio::test]
    async fn only_the_two_newest_checkpoints_are_kept() {
        let store = MemoryStore::new();
        let writer = log(&store, 1);
        let mut base = writer.snapshot().await.unwrap();
        for i in 1..=3 {
            base = writer
                .commit(&base, vec![put(&format!("k{i}"), "v")])
                .await
                .unwrap();
            writer.checkpoint(&base).await.unwrap();
        }
        let kept: Vec<String> = store
            .list(&prefix().join("_checkpoint").unwrap())
            .map_ok(|m| m.key.name().to_owned())
            .try_collect()
            .await
            .unwrap();
        assert_eq!(kept, ["00000000000000000002", "00000000000000000003"]);
    }

    #[tokio::test]
    async fn a_corrupt_checkpoint_is_reported() {
        let store = MemoryStore::new();
        let key = prefix()
            .join("_checkpoint")
            .unwrap()
            .join("00000000000000000005")
            .unwrap();
        store.put(&key, Bytes::from_static(b"{}")).await.unwrap();
        let error = log(&store, 1).snapshot().await.unwrap_err();
        assert!(
            matches!(error, CommitError::Corrupt { key: ref k, .. } if *k == key),
            "{error}"
        );
    }
}
