// SPDX-License-Identifier: Apache-2.0

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
use tessera_types::{Env, NodeId, TenantId, Timestamp};

use crate::role::Role;
use crate::woken::Woken;

/// The format of registry entries (OP-7).
pub const FORMAT: u32 = 1;
/// The format of the data this build writes: commit objects, writer-log
/// entries, files, sidecar indexes and checkpoints (OP-7). 2 since
/// writer-log blocks may be sealed under tenant keys (SE-2).
pub const DATA_FORMAT: u32 = 2;
/// The newest data format this build reads (OP-8).
pub const READS_THROUGH: u32 = DATA_FORMAT;

fn reads_through_before_the_field() -> u32 {
    1
}
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
    /// The newest data format the node reads (OP-7); nodes from before
    /// the field read the first.
    #[serde(default = "reads_through_before_the_field")]
    pub reads_through: u32,
    /// The tenants the node began to write to within the last minute
    /// ([`Woken`]): the nodes that compact go over them at once.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub woken: Vec<TenantId>,
}

/// The live record with the oldest reader (OP-7): a node must not write
/// a data format newer than what this node reads.
#[must_use]
pub fn oldest_reader(records: &[NodeRecord], now: Timestamp) -> Option<&NodeRecord> {
    records
        .iter()
        .filter(|r| r.is_live_at(now))
        .min_by_key(|r| r.reads_through)
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
    woken: Arc<Woken>,
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
            woken: Arc::default(),
        }
    }

    /// This registry naming in the node's record the tenants `woken`
    /// holds: those the node's writers began to write to.
    #[must_use]
    pub fn telling(mut self, woken: Arc<Woken>) -> Self {
        self.woken = woken;
        self
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
            reads_through: READS_THROUGH,
            woken: self.woken.lately(now),
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
    #[test]
    fn the_oldest_live_reader_bounds_what_may_be_written() {
        use super::*;

        let record = |node: u128, reads_through: u32, heartbeat: i64| NodeRecord {
            format: FORMAT,
            node: NodeId::from_u128(node),
            roles: BTreeSet::new(),
            http: String::new(),
            started_at: Timestamp::from_nanos(0),
            heartbeat_at: Timestamp::from_nanos(heartbeat),
            reads_through,
            woken: Vec::new(),
        };
        let now = Timestamp::from_nanos(100_000_000_000);
        let records = [
            record(1, 3, now.as_nanos()),
            record(2, 1, now.as_nanos()),
            record(3, 0, 0),
        ];
        assert_eq!(
            oldest_reader(&records, now).map(|r| r.node),
            Some(NodeId::from_u128(2)),
            "the dead node does not count"
        );
        assert!(oldest_reader(&[], now).is_none());
        let old: NodeRecord = serde_json::from_str(
            r#"{"format":1,"node":"00000000000000000000000000000001","roles":[],"http":"","started_at":0,"heartbeat_at":0}"#,
        )
        .unwrap();
        assert_eq!(old.reads_through, 1, "a record from before the field");
        assert!(old.woken.is_empty(), "nor does it name a tenant");
    }

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

        // A node names the tenants it began to write to, for a while.
        let woken = Arc::new(Woken::default());
        let acme = TenantId::new("acme").unwrap();
        let c = Registry::new(
            Arc::clone(&store),
            env(3, Arc::clone(&clock)),
            BTreeSet::from([Role::Ingest]),
            "http://c".into(),
        )
        .telling(Arc::clone(&woken));
        assert!(c.announce().await.unwrap().woken.is_empty());
        woken.note(&acme, clock.now());
        assert_eq!(
            c.announce().await.unwrap().woken,
            std::slice::from_ref(&acme)
        );
        let listed = c.list().await.unwrap();
        let named: Vec<&TenantId> = listed.iter().flat_map(|r| &r.woken).collect();
        assert_eq!(named, [&acme]);
        c.withdraw().await.unwrap();

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
