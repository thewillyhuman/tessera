// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The membership view (section 14.1): the live nodes by role, refreshed
//! from the registry and from what the peers gossip, and the rendezvous
//! hashing that picks preferred nodes for a key (IN-9, QE-15).
//!
//! The view is a hint: it may lag the registry by a refresh interval and
//! may name a node that just died. Every use of it falls back to doing
//! the work locally. The registry says who is a member; gossip says how
//! recently each member was seen, a node's newest heartbeat winning
//! wherever it came from. A node gossip names that the registry does
//! not list is ignored: it withdrew, and its record would otherwise
//! circulate among the peers until it aged out.

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

    /// Replaces the view with `records`, as the registry listed them,
    /// keeping the fresher heartbeat gossip may know of a listed node.
    /// A node the listing lacks is gone.
    pub fn replace(&self, records: Vec<NodeRecord>) {
        let mut view = self
            .records
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut next = records;
        for known in view.drain(..) {
            if let Some(listed) = next.iter_mut().find(|r| r.node == known.node) {
                if known.heartbeat_at > listed.heartbeat_at {
                    *listed = known;
                }
            }
        }
        *view = next;
    }

    /// Folds `records`, as a peer gossiped them, into the view: a known
    /// node's newest heartbeat wins, this node's own record stays its
    /// own, and a node the view does not know is ignored, since the
    /// registry, not the peers, says who is a member.
    pub fn merge(&self, records: Vec<NodeRecord>) {
        let mut view = self
            .records
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for record in records {
            if record.node == self.env.node {
                continue;
            }
            if let Some(known) = view.iter_mut().find(|r| r.node == record.node) {
                if record.heartbeat_at > known.heartbeat_at {
                    *known = record;
                }
            }
        }
    }

    /// Up to `count` live peers to gossip with on `round`, this node
    /// left out: a different few each round, every node in turn.
    #[must_use]
    pub fn gossip_peers(&self, round: u64, count: usize) -> Vec<NodeRecord> {
        let now = self.env.clock.now();
        let mut peers: Vec<NodeRecord> = self
            .records()
            .into_iter()
            .filter(|r| r.node != self.env.node && r.is_live_at(now))
            .collect();
        peers.sort_by_key(|r| rendezvous_score(&round.to_le_bytes(), r.node));
        peers.dedup_by_key(|r| r.node);
        peers.truncate(count);
        peers
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
            reads_through: crate::registry::READS_THROUGH,
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

    /// Section 14.1: gossip folds into the view by freshness, and a
    /// listing keeps what gossip knew better.
    #[test]
    fn gossip_merges_by_freshness_and_listings_keep_fresher_records() {
        let clock = Arc::new(ManualClock::new(Timestamp::from_nanos(1_000_000_000_000)));
        let env = Env::new(
            NodeId::from_u128(1),
            clock.clone(),
            SharedRandom::new(SeededRandom::new(1)),
        );
        let at = |node: u128, seconds: i64| NodeRecord {
            format: 1,
            reads_through: 1,
            node: NodeId::from_u128(node),
            roles: BTreeSet::from([Role::Query]),
            http: format!("http://node-{node}"),
            started_at: Timestamp::from_nanos(0),
            heartbeat_at: Timestamp::from_nanos(seconds * 1_000_000_000),
        };
        let view = Membership::new(env, at(1, 1_000));
        view.merge(vec![at(2, 999), at(1, 0)]);
        assert_eq!(view.records().len(), 1, "an unlisted node is not a member");
        assert_eq!(
            view.records()[0].heartbeat_at,
            Timestamp::from_nanos(1_000_000_000_000),
            "the own record is not overwritten"
        );
        view.replace(vec![at(1, 1_000), at(2, 990)]);
        view.merge(vec![at(2, 999)]);
        let two = view
            .records()
            .into_iter()
            .find(|r| r.node == NodeId::from_u128(2))
            .unwrap();
        assert_eq!(
            two.heartbeat_at.as_nanos(),
            999 * 1_000_000_000,
            "gossip refreshes a member"
        );
        view.merge(vec![at(2, 998)]);
        let two = view
            .records()
            .into_iter()
            .find(|r| r.node == NodeId::from_u128(2))
            .unwrap();
        assert_eq!(
            two.heartbeat_at.as_nanos(),
            999 * 1_000_000_000,
            "older gossip is ignored"
        );
        view.replace(vec![at(1, 1_000), at(2, 997), at(3, 999)]);
        let two = view
            .records()
            .into_iter()
            .find(|r| r.node == NodeId::from_u128(2))
            .unwrap();
        assert_eq!(
            two.heartbeat_at.as_nanos(),
            999 * 1_000_000_000,
            "the fresher one stays"
        );
        assert_eq!(view.records().len(), 3);
        view.merge(vec![at(4, 999)]);
        assert_eq!(
            view.records().len(),
            3,
            "a withdrawn node gossiped back stays out"
        );
        assert_eq!(view.gossip_peers(0, 1).len(), 1);
        assert_eq!(
            view.gossip_peers(0, 5).len(),
            2,
            "the live peers, never itself"
        );
        view.replace(vec![at(1, 1_000)]);
        let left: BTreeSet<NodeId> = view.records().into_iter().map(|r| r.node).collect();
        assert_eq!(
            left,
            BTreeSet::from([NodeId::from_u128(1)]),
            "the listing says who is a member"
        );
    }
}
