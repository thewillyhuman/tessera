// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The node registry (section 14.1): one object per node under
//! `_cluster/nodes/` in the root pool, rewritten at every heartbeat and
//! deleted when the node stops. It is a hint: a node that fails to
//! announce itself is merely invisible, and a stale entry is ignored
//! once its heartbeat is old.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tessera_object::{Key, ObjectError, ObjectStore, layout};
use tessera_types::{Env, NodeId, Timestamp};

use crate::role::Role;

/// The format of registry entries (OP-7).
pub const FORMAT: u32 = 1;
/// How often a node announces itself.
pub const HEARTBEAT: Duration = Duration::from_secs(5);
/// How old a heartbeat may be before the node counts as gone: three
/// heartbeats, so that one lost write does not drop a node.
pub const TIMEOUT: Duration = Duration::from_secs(15);

/// What a node announces about itself.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRecord {
    /// The format this object was written in.
    pub format: u32,
    /// The node.
    pub node: NodeId,
    /// Its roles.
    pub roles: BTreeSet<Role>,
    /// The base URL of its HTTP API, for peers.
    pub http: String,
    /// When the process started, by its clock.
    pub started_at: Timestamp,
    /// When it last announced itself, by its clock.
    pub heartbeat_at: Timestamp,
}

impl NodeRecord {
    /// Whether the node has a role.
    #[must_use]
    pub fn has(&self, role: Role) -> bool {
        self.roles.contains(&role)
    }

    /// Whether the record is fresh at `now`: its heartbeat is less than
    /// [`TIMEOUT`] old.
    #[must_use]
    pub fn is_live_at(&self, now: Timestamp) -> bool {
        let age = now.as_nanos().saturating_sub(self.heartbeat_at.as_nanos());
        age < i64::try_from(TIMEOUT.as_nanos()).unwrap_or(i64::MAX)
    }
}

/// Why a registry operation failed.
#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    /// The store failed.
    #[error(transparent)]
    Store(#[from] ObjectError),
    /// An entry cannot be encoded or decoded.
    #[error("corrupt node registry entry {key}: {source}")]
    Corrupt {
        /// The entry.
        key: Key,
        /// The failure.
        #[source]
        source: serde_json::Error,
    },
}

/// Announces this node and lists the others.
pub struct Registry {
    store: Arc<dyn ObjectStore>,
    env: Env,
    roles: BTreeSet<Role>,
    http: String,
    started_at: Timestamp,
}

impl Registry {
    /// A registry for this node with `roles`, reachable at `http`.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, env: Env, roles: BTreeSet<Role>, http: String) -> Self {
        let started_at = env.clock.now();
        Registry {
            store,
            env,
            roles,
            http,
            started_at,
        }
    }

    /// This node's record at `now`.
    #[must_use]
    pub fn record(&self, now: Timestamp) -> NodeRecord {
        NodeRecord {
            format: FORMAT,
            node: self.env.node,
            roles: self.roles.clone(),
            http: self.http.clone(),
            started_at: self.started_at,
            heartbeat_at: now,
        }
    }

    /// Writes this node's record with the current time as heartbeat.
    ///
    /// # Errors
    ///
    /// `Store` if the write failed.
    pub async fn announce(&self) -> Result<NodeRecord, RegistryError> {
        let record = self.record(self.env.clock.now());
        let body = serde_json::to_vec(&record).map_err(|source| RegistryError::Corrupt {
            key: layout::node(self.env.node),
            source,
        })?;
        self.store
            .put(&layout::node(self.env.node), body.into())
            .await?;
        Ok(record)
    }

    /// Deletes this node's record, so that peers forget it at once.
    ///
    /// # Errors
    ///
    /// `Store` if the delete failed.
    pub async fn withdraw(&self) -> Result<(), RegistryError> {
        Ok(self.store.delete(&layout::node(self.env.node)).await?)
    }

    /// Every record in the registry, fresh or not.
    ///
    /// # Errors
    ///
    /// `Store` if the listing or a read failed; `Corrupt` for an entry
    /// that cannot be decoded.
    pub async fn list(&self) -> Result<Vec<NodeRecord>, RegistryError> {
        list(&*self.store).await
    }
}

/// Every record under `_cluster/nodes/` in `store`.
///
/// # Errors
///
/// `Store` if the listing or a read failed; `Corrupt` for an entry that
/// cannot be decoded.
pub async fn list(store: &dyn ObjectStore) -> Result<Vec<NodeRecord>, RegistryError> {
    let prefix = layout::nodes();
    let mut entries = store.list(&prefix);
    let mut records = Vec::new();
    while let Some(meta) = entries.next().await {
        let meta = meta?;
        let bytes = match store.get(&meta.key).await {
            Ok(bytes) => bytes,
            // Deleted between the listing and the read: the node left.
            Err(ObjectError::NotFound { .. }) => continue,
            Err(error) => return Err(error.into()),
        };
        let record: NodeRecord =
            serde_json::from_slice(&bytes).map_err(|source| RegistryError::Corrupt {
                key: meta.key.clone(),
                source,
            })?;
        records.push(record);
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use tessera_object::MemoryStore;
    use tessera_types::{Clock, ManualClock, SeededRandom, SharedRandom};

    use super::*;

    fn env(seed: u64, clock: Arc<ManualClock>) -> Env {
        let mut random = SeededRandom::new(seed);
        Env::new(
            NodeId::random(&mut random),
            clock,
            SharedRandom::new(random),
        )
    }

    #[tokio::test]
    async fn nodes_announce_list_and_withdraw() {
        let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
        let clock = Arc::new(ManualClock::new(Timestamp::from_nanos(0)));
        let a = Registry::new(
            Arc::clone(&store),
            env(1, Arc::clone(&clock)),
            BTreeSet::from([Role::Ingest, Role::Query]),
            "http://a".into(),
        );
        let b = Registry::new(
            Arc::clone(&store),
            env(2, Arc::clone(&clock)),
            BTreeSet::from([Role::Compact]),
            "http://b".into(),
        );
        a.announce().await.unwrap();
        clock.advance(Duration::from_secs(1));
        b.announce().await.unwrap();
        let mut records = a.list().await.unwrap();
        records.sort_by_key(|r| r.http.clone());
        assert_eq!(records.len(), 2);
        assert!(records[0].has(Role::Query) && !records[0].has(Role::Compact));
        assert_eq!(
            records[1].heartbeat_at,
            Timestamp::from_nanos(1_000_000_000)
        );
        assert_eq!(records[1].format, FORMAT);

        clock.advance(TIMEOUT);
        let now = clock.now();
        assert!(!records[0].is_live_at(now), "an old heartbeat means gone");
        let fresh = a.announce().await.unwrap();
        assert!(fresh.is_live_at(now));
        assert_eq!(
            fresh.started_at,
            Timestamp::from_nanos(0),
            "start time stays"
        );

        b.withdraw().await.unwrap();
        let records = a.list().await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].http, "http://a");
        b.withdraw().await.unwrap();
    }
}
