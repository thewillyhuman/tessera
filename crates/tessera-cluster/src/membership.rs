// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The membership view (section 14.1): the live nodes by role, refreshed
//! from the registry, and the rendezvous hashing that picks preferred
//! nodes for a key (IN-9, QE-15).
//!
//! The view is a hint: it may lag the registry by a refresh interval and
//! may name a node that just died. Every use of it falls back to doing
//! the work locally.

use std::collections::BTreeSet;
use std::sync::RwLock;

use tessera_types::{Env, NodeId, stable_hash};

use crate::registry::NodeRecord;
use crate::role::Role;

/// What this node believes about the cluster.
pub struct Membership {
    env: Env,
    records: RwLock<Vec<NodeRecord>>,
}

impl Membership {
    /// A view that knows only this node's own record.
    #[must_use]
    pub fn new(env: Env, own: NodeRecord) -> Self {
        Membership {
            env,
            records: RwLock::new(vec![own]),
        }
    }

    /// Replaces the view with `records`, as the registry listed them.
    pub fn replace(&self, records: Vec<NodeRecord>) {
        *self
            .records
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = records;
    }

    /// Every record the view holds, fresh or not.
    #[must_use]
    pub fn records(&self) -> Vec<NodeRecord> {
        self.records
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// The live nodes with `role`, this node included when it has the
    /// role, sorted by id.
    #[must_use]
    pub fn live(&self, role: Role) -> Vec<NodeRecord> {
        let now = self.env.clock.now();
        let mut live: Vec<NodeRecord> = self
            .records()
            .into_iter()
            .filter(|r| r.has(role) && (r.node == self.env.node || r.is_live_at(now)))
            .collect();
        live.sort_by_key(|r| r.node.to_string());
        live.dedup_by_key(|r| r.node);
        live
    }

    /// The live node ids with `role`.
    #[must_use]
    pub fn live_ids(&self, role: Role) -> BTreeSet<NodeId> {
        self.live(role).into_iter().map(|r| r.node).collect()
    }

    /// Whether `node` is live in any role.
    #[must_use]
    pub fn is_live(&self, node: NodeId) -> bool {
        let now = self.env.clock.now();
        node == self.env.node
            || self
                .records()
                .iter()
                .any(|r| r.node == node && r.is_live_at(now))
    }

    /// The `count` live nodes with `role` that rendezvous hashing prefers
    /// for `key`, best first: the same key always prefers the same
    /// nodes, and a node joining or leaving moves only the keys that
    /// were or become its own.
    #[must_use]
    pub fn preferred(&self, role: Role, key: &[u8], count: usize) -> Vec<NodeRecord> {
        let mut scored: Vec<(u64, NodeRecord)> = self
            .live(role)
            .into_iter()
            .map(|record| (rendezvous_score(key, record.node), record))
            .collect();
        scored.sort_by_key(|(score, _)| std::cmp::Reverse(*score));
        scored.into_iter().take(count).map(|(_, r)| r).collect()
    }

    /// This node.
    #[must_use]
    pub fn node(&self) -> NodeId {
        self.env.node
    }
}

/// The rendezvous score of `node` for `key`: the hash of both.
#[must_use]
pub fn rendezvous_score(key: &[u8], node: NodeId) -> u64 {
    let id = node.to_string();
    let mut bytes = Vec::with_capacity(key.len() + 1 + id.len());
    bytes.extend_from_slice(key);
    bytes.push(0);
    bytes.extend_from_slice(id.as_bytes());
    stable_hash(&bytes)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use tessera_types::{ManualClock, SeededRandom, SharedRandom, Timestamp};

    use super::*;
    use crate::registry::FORMAT;

    fn record(seed: u64, roles: &[Role], heartbeat: i64) -> NodeRecord {
        let mut random = SeededRandom::new(seed);
        NodeRecord {
            format: FORMAT,
            node: NodeId::random(&mut random),
            roles: roles.iter().copied().collect(),
            http: format!("http://n{seed}"),
            started_at: Timestamp::from_nanos(0),
            heartbeat_at: Timestamp::from_nanos(heartbeat),
        }
    }

    fn membership(own: NodeRecord, clock: Arc<ManualClock>) -> Membership {
        let env = Env::new(own.node, clock, SharedRandom::new(SeededRandom::new(99)));
        Membership::new(env, own)
    }

    #[test]
    fn live_nodes_are_fresh_or_oneself() {
        let clock = Arc::new(ManualClock::new(Timestamp::from_nanos(20_000_000_000)));
        let own = record(1, &[Role::Query], 0);
        let view = membership(own.clone(), Arc::clone(&clock));
        view.replace(vec![
            own.clone(),
            record(2, &[Role::Query, Role::Ingest], 19_000_000_000),
            record(3, &[Role::Query], 1_000_000_000),
            record(4, &[Role::Compact], 20_000_000_000),
        ]);
        let live = view.live(Role::Query);
        assert_eq!(
            live.len(),
            2,
            "oneself (old heartbeat) and node 2; node 3 is stale"
        );
        assert!(live.iter().any(|r| r.node == own.node));
        assert_eq!(view.live(Role::Ingest).len(), 1);
        assert!(view.live(Role::Eval).is_empty());
        assert!(view.is_live(own.node));
        assert!(!view.is_live(record(3, &[], 0).node));
        clock.advance(Duration::from_secs(20));
        assert_eq!(
            view.live(Role::Query).len(),
            1,
            "node 2 aged out; oneself stays"
        );
    }

    #[test]
    fn rendezvous_is_stable_and_minimal() {
        let clock = Arc::new(ManualClock::new(Timestamp::from_nanos(0)));
        let nodes: Vec<NodeRecord> = (1..=5).map(|i| record(i, &[Role::Ingest], 0)).collect();
        let view = membership(nodes[0].clone(), clock);
        view.replace(nodes.clone());
        let keys: Vec<Vec<u8>> = (0..200u32).map(|i| i.to_le_bytes().to_vec()).collect();
        let before: Vec<NodeId> = keys
            .iter()
            .map(|k| view.preferred(Role::Ingest, k, 1)[0].node)
            .collect();
        // The same answer regardless of record order.
        let mut shuffled = nodes.clone();
        shuffled.reverse();
        view.replace(shuffled);
        let again: Vec<NodeId> = keys
            .iter()
            .map(|k| view.preferred(Role::Ingest, k, 1)[0].node)
            .collect();
        assert_eq!(before, again);
        // Removing one node moves only its keys.
        let gone = nodes[2].node;
        view.replace(nodes.iter().filter(|r| r.node != gone).cloned().collect());
        let after: Vec<NodeId> = keys
            .iter()
            .map(|k| view.preferred(Role::Ingest, k, 1)[0].node)
            .collect();
        let moved = before.iter().zip(&after).filter(|(b, a)| b != a).count();
        let were_gone = before.iter().filter(|b| **b == gone).count();
        assert_eq!(moved, were_gone);
        assert!(were_gone > 0 && were_gone < 200);
        assert_eq!(
            view.preferred(Role::Ingest, b"k", 10).len(),
            4,
            "at most the live nodes"
        );
        assert!(view.preferred(Role::Eval, b"k", 2).is_empty());
    }
}
