// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The state a commit log folds its actions into, and snapshots of it.

use std::collections::BTreeMap;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tessera_types::Version;

use crate::{Action, DecodeError, KvMutation};

/// Why an action could not be applied to a state. Since every action was
/// valid when committed, this means the log or the state is corrupt.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ApplyError(String);

impl ApplyError {
    /// An error with the given description.
    #[must_use]
    pub fn new(reason: impl Into<String>) -> Self {
        ApplyError(reason.into())
    }
}

/// The state a log's actions fold into. Each kind of log has one. States
/// serialize so that they can be checkpointed.
pub trait State: Clone + Default + Serialize + DeserializeOwned + Send + Sync + 'static {
    /// Applies one action, in commit order.
    ///
    /// # Errors
    ///
    /// If the action cannot apply to this state, which only a corrupt log
    /// can cause.
    fn apply(&mut self, action: &Action) -> Result<(), ApplyError>;

    /// The state as the checkpoint object at `version` (CL-3): JSON
    /// unless the state overrides it, as a manifest does with Parquet so
    /// that a reader scans its checkpoint in part (MF-5).
    fn encode_checkpoint(&self, version: Version) -> Vec<u8> {
        crate::checkpoint::encode_json(version, self)
    }

    /// The state in the checkpoint object filed under `version`.
    ///
    /// # Errors
    ///
    /// If the object does not decode or claims another version.
    fn decode_checkpoint(version: Version, bytes: &[u8]) -> Result<Self, DecodeError> {
        crate::checkpoint::decode_json(version, bytes)
    }
}

/// The state of a log at one version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot<S> {
    version: Version,
    state: S,
}

impl<S> Snapshot<S> {
    /// A snapshot at `version` with the given folded state.
    #[must_use]
    pub fn new(version: Version, state: S) -> Self {
        Snapshot { version, state }
    }

    /// The snapshot of an empty log: version zero of the default state.
    #[must_use]
    pub fn initial() -> Self
    where
        S: Default,
    {
        Snapshot::new(Version::ZERO, S::default())
    }

    /// The last version folded into this snapshot.
    #[must_use]
    pub fn version(&self) -> Version {
        self.version
    }

    /// The folded state.
    #[must_use]
    pub fn state(&self) -> &S {
        &self.state
    }
}

/// The state of a key-value log: an ordered map from key to text value.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct KvState {
    entries: BTreeMap<String, String>,
}

impl KvState {
    /// The value at `key`.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&str> {
        self.entries.get(key).map(String::as_str)
    }

    /// The entries whose key starts with `prefix`, in key order.
    pub fn range<'a>(&'a self, prefix: &'a str) -> impl Iterator<Item = (&'a str, &'a str)> + 'a {
        self.entries
            .range(prefix.to_owned()..)
            .take_while(move |(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.as_str(), value.as_str()))
    }

    /// The number of entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether there are no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl State for KvState {
    fn apply(&mut self, action: &Action) -> Result<(), ApplyError> {
        match action {
            Action::Kv(KvMutation::Put { key, value }) => {
                self.entries.insert(key.clone(), value.clone());
            }
            Action::Kv(KvMutation::Delete { key }) => {
                self.entries.remove(key);
            }
            other => {
                return Err(ApplyError::new(format!(
                    "not a key-value action: {other:?}"
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Action, KvMutation};

    fn put(key: &str, value: &str) -> Action {
        Action::Kv(KvMutation::Put {
            key: key.into(),
            value: value.into(),
        })
    }

    #[test]
    fn kv_state_applies_puts_and_deletes() {
        let mut state = KvState::default();
        state.apply(&put("a", "1")).unwrap();
        state.apply(&put("b", "2")).unwrap();
        state
            .apply(&Action::Kv(KvMutation::Delete { key: "a".into() }))
            .unwrap();
        assert_eq!(state.get("a"), None);
        assert_eq!(state.get("b"), Some("2"));
        assert_eq!(state.len(), 1);
    }

    #[test]
    fn a_kv_state_rejects_manifest_actions() {
        let mut state = KvState::default();
        assert!(state.apply(&Action::DropPartition).is_err());
    }

    #[test]
    fn deleting_an_absent_key_is_a_no_op() {
        let mut state = KvState::default();
        state
            .apply(&Action::Kv(KvMutation::Delete { key: "a".into() }))
            .unwrap();
        assert!(state.is_empty());
    }

    #[test]
    fn kv_state_lists_keys_under_a_prefix_in_order() {
        let mut state = KvState::default();
        for (k, v) in [("t/b", "2"), ("t/a", "1"), ("u/a", "3")] {
            state.apply(&put(k, v)).unwrap();
        }
        let under: Vec<(&str, &str)> = state.range("t/").collect();
        assert_eq!(under, [("t/a", "1"), ("t/b", "2")]);
    }

    #[test]
    fn the_initial_snapshot_is_version_zero_of_the_default_state() {
        let snapshot: Snapshot<KvState> = Snapshot::initial();
        assert_eq!(snapshot.version(), Version::ZERO);
        assert!(snapshot.state().is_empty());
    }
}
