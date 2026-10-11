// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Which slots this node owns (sections 5.3 and 14.2).
//!
//! Every kind of single-owner work has a slot count in the catalog
//! (AR-4); a node owns a slot by holding its lease (OP-1). The live
//! `compact` nodes share the slots by rendezvous hashing: each node
//! acquires the slots it is preferred for, renews them every renewal
//! interval and releases the ones a newer view prefers another node
//! for, so adding or removing a node moves its share and nothing else
//! (OP-4). A crashed node's slots come back when its leases expire.
//!
//! Ownership is a hint (AR-6): every effect of leased work goes through
//! a conditional write, so two nodes that both believe they own a slot
//! only cause retries.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tessera_catalog::Catalog;
use tessera_cluster::{Load, Membership, Role};
use tessera_commit::{Lease, LeaseManager, Slot, SlotCount};
use tessera_types::WorkKind;
use tokio::sync::oneshot;

/// The kinds of work a `compact` node owns slots of.
pub const COMPACT_KINDS: [WorkKind; 3] =
    [WorkKind::Intake, WorkKind::Recovery, WorkKind::Partition];
/// The kinds of work an `eval` node owns slots of.
pub const EVAL_KINDS: [WorkKind; 1] = [WorkKind::RuleGroup];

/// The role whose live nodes share the slots of `kind`.
#[must_use]
pub fn role_of(kind: WorkKind) -> Role {
    match kind {
        WorkKind::RuleGroup => Role::Eval,
        WorkKind::Intake | WorkKind::Recovery | WorkKind::Partition => Role::Compact,
    }
}

/// The kinds of work a node with `roles` owns slots of.
#[must_use]
pub fn kinds_of(roles: &std::collections::BTreeSet<Role>) -> Vec<WorkKind> {
    let mut kinds = Vec::new();
    if roles.contains(&Role::Compact) {
        kinds.extend(COMPACT_KINDS);
    }
    if roles.contains(&Role::Eval) {
        kinds.extend(EVAL_KINDS);
    }
    kinds
}

/// The slots held for one kind, under the slot count they were taken
/// with.
#[derive(Clone, Debug)]
struct Held {
    count: SlotCount,
    leases: BTreeMap<Slot, Lease>,
}

/// What this node owns right now: a view the background work consults
/// before every item.
#[derive(Clone, Debug, Default)]
pub struct Owned {
    held: Arc<RwLock<BTreeMap<WorkKind, Held>>>,
}

impl Owned {
    /// Whether the item with key `item` of `kind` is this node's.
    #[must_use]
    pub fn owns(&self, kind: WorkKind, item: &[u8]) -> bool {
        let held = self
            .held
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        held.get(&kind)
            .is_some_and(|held| held.leases.contains_key(&held.count.slot_of(item)))
    }

    /// The slots held for `kind`.
    #[must_use]
    pub fn slots(&self, kind: WorkKind) -> BTreeSet<Slot> {
        self.held
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&kind)
            .map(|held| held.leases.keys().copied().collect())
            .unwrap_or_default()
    }

    fn replace(&self, kind: WorkKind, count: SlotCount, leases: BTreeMap<Slot, Lease>) {
        self.held
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(kind, Held { count, leases });
    }

    fn take(&self, kind: WorkKind) -> BTreeMap<Slot, Lease> {
        self.held
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_mut(&kind)
            .map(|held| std::mem::take(&mut held.leases))
            .unwrap_or_default()
    }

    fn leases(&self, kind: WorkKind) -> BTreeMap<Slot, Lease> {
        self.held
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&kind)
            .map(|held| held.leases.clone())
            .unwrap_or_default()
    }
}

/// The lease loop's dependencies.
pub struct Leases {
    /// The lease objects in the root pool.
    pub manager: LeaseManager,
    /// The catalog, for the slot counts.
    pub catalog: Arc<Catalog>,
    /// The cluster, for who is preferred.
    pub membership: Arc<Membership>,
    /// Where the held slots are published.
    pub owned: Owned,
    /// The kinds this node takes slots of.
    pub kinds: Vec<WorkKind>,
    /// Where the slots held are reported (OP-5).
    pub load: Arc<Load>,
}

impl Leases {
    /// One pass over every slot of every kind: acquire the preferred
    /// ones, renew the held ones, release the rest. The owned view
    /// changes once per kind, at the end of the pass, so a reader never
    /// sees a slot missing while its renewal is in flight.
    pub async fn tick(&self) {
        for kind in self.kinds.iter().copied() {
            let count = self.catalog.cached().state().slot_count(kind);
            let mut held = self.owned.leases(kind);
            let mut next = BTreeMap::new();
            for slot in count.slots() {
                let preferred = self.is_preferred(kind, slot);
                match (held.remove(&slot), preferred) {
                    (Some(lease), true) => match self.manager.renew(&lease).await {
                        Ok(Some(renewed)) => {
                            next.insert(slot, renewed);
                        }
                        Ok(None) => tracing::info!(%kind, %slot, "slot taken over"),
                        Err(error) => {
                            tracing::warn!(%kind, %slot, error = %error, "renewal failed");
                        }
                    },
                    (Some(lease), false) => match self.manager.release(&lease).await {
                        Ok(()) => tracing::info!(%kind, %slot, "slot handed over"),
                        Err(error) => {
                            tracing::warn!(%kind, %slot, error = %error, "release failed");
                        }
                    },
                    (None, true) => match self.manager.acquire(kind, slot).await {
                        Ok(Some(lease)) => {
                            tracing::info!(%kind, %slot, epoch = %lease.epoch, "slot acquired");
                            next.insert(slot, lease);
                        }
                        Ok(None) => {}
                        Err(error) => {
                            tracing::warn!(%kind, %slot, error = %error, "acquisition failed");
                        }
                    },
                    (None, false) => {}
                }
            }
            self.load
                .set_slots(kind, u64::try_from(next.len()).unwrap_or(u64::MAX));
            self.owned.replace(kind, count, next);
        }
    }

    /// Gives every held slot up, for a clean stop.
    pub async fn release_all(&self) {
        for kind in self.kinds.iter().copied() {
            for lease in self.owned.take(kind).into_values() {
                if let Err(error) = self.manager.release(&lease).await {
                    tracing::warn!(%kind, slot = %lease.slot, error = %error, "release failed");
                }
            }
        }
    }

    fn is_preferred(&self, kind: WorkKind, slot: Slot) -> bool {
        let key = format!("{kind}/{slot}");
        let preferred = self.membership.preferred(role_of(kind), key.as_bytes(), 1);
        preferred
            .first()
            .is_none_or(|record| record.node == self.membership.node())
    }
}

/// Runs [`Leases::tick`] every `renewal` until `stop` fires, then gives
/// every held slot up. A pass is never cut short: a renewal that
/// completed in the store but not in the owned view would leave a lease
/// that no release supersedes, held until it expires.
pub async fn run(leases: Arc<Leases>, renewal: Duration, mut stop: oneshot::Receiver<()>) {
    let mut ticks = tokio::time::interval(renewal);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = &mut stop => break,
            _ = ticks.tick() => leases.tick().await,
        }
    }
    leases.release_all().await;
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use tessera_catalog::PoolConfig;
    use tessera_cluster::{NodeRecord, registry};
    use tessera_object::{MemoryStore, PoolState, layout};
    use tessera_types::{Env, ManualClock, NodeId, PoolId, SeededRandom, SharedRandom, Timestamp};

    use super::*;

    const DURATION: Duration = Duration::from_secs(30);

    struct Fixture {
        env: Env,
        clock: Arc<ManualClock>,
        store: Arc<MemoryStore>,
        catalog: Arc<Catalog>,
    }

    async fn fixture() -> Fixture {
        let clock = Arc::new(ManualClock::new(Timestamp::from_nanos(1_000_000_000_000)));
        let mut random = SeededRandom::new(3);
        let env = Env::new(
            NodeId::random(&mut random),
            Arc::clone(&clock) as _,
            SharedRandom::new(random),
        );
        let store = Arc::new(MemoryStore::new());
        let root = PoolConfig {
            id: PoolId::new("root").unwrap(),
            url: "memory://root".into(),
            weight: 1,
            state: PoolState::Active,
        };
        let catalog = Arc::new(
            Catalog::open(Arc::clone(&store) as _, root, env.clone())
                .await
                .unwrap(),
        );
        catalog.double_slots(WorkKind::Intake).await.unwrap();
        catalog.double_slots(WorkKind::Intake).await.unwrap();
        catalog.double_slots(WorkKind::Intake).await.unwrap();
        Fixture {
            env,
            clock,
            store,
            catalog,
        }
    }

    fn record(env: &Env, node: NodeId) -> NodeRecord {
        NodeRecord {
            format: registry::FORMAT,
            reads_through: registry::READS_THROUGH,
            node,
            roles: BTreeSet::from([Role::Compact]),
            http: String::new(),
            started_at: env.clock.now(),
            heartbeat_at: env.clock.now(),
        }
    }

    fn node(fixture: &Fixture, node: NodeId, sees: &[NodeId]) -> Leases {
        let env = fixture.env.as_node(node);
        let membership = Membership::new(env.clone(), record(&env, node));
        membership.replace(sees.iter().map(|n| record(&env, *n)).collect());
        Leases {
            manager: LeaseManager::new(
                Arc::clone(&fixture.store) as _,
                layout::leases(),
                env,
                DURATION,
            ),
            catalog: Arc::clone(&fixture.catalog),
            membership: Arc::new(membership),
            owned: Owned::default(),
            kinds: COMPACT_KINDS.to_vec(),
            load: Load::new(),
        }
    }

    #[tokio::test]
    async fn live_compact_nodes_share_the_slots() {
        let fixture = fixture().await;
        let (a, b) = (
            fixture.env.node,
            Env::simulated(4, Timestamp::from_nanos(0)).node,
        );
        let node_a = node(&fixture, a, &[a, b]);
        let node_b = node(&fixture, b, &[a, b]);
        node_a.tick().await;
        node_b.tick().await;
        let slots_a = node_a.owned.slots(WorkKind::Intake);
        let slots_b = node_b.owned.slots(WorkKind::Intake);
        assert_eq!(slots_a.len() + slots_b.len(), 8, "{slots_a:?} {slots_b:?}");
        assert!(slots_a.is_disjoint(&slots_b));
        assert!(!slots_a.is_empty() && !slots_b.is_empty());
        let item = b"writer-1";
        assert_ne!(
            node_a.owned.owns(WorkKind::Intake, item),
            node_b.owned.owns(WorkKind::Intake, item),
            "exactly one owns an item"
        );
        assert!(
            !node_a.owned.owns(WorkKind::RuleGroup, item),
            "a kind without leases is nobody's"
        );
    }

    #[tokio::test]
    async fn a_node_hands_over_what_a_newer_view_prefers_elsewhere() {
        let fixture = fixture().await;
        let (a, b) = (
            fixture.env.node,
            Env::simulated(4, Timestamp::from_nanos(0)).node,
        );
        let node_a = node(&fixture, a, &[a]);
        node_a.tick().await;
        assert_eq!(node_a.owned.slots(WorkKind::Intake).len(), 8, "alone, all");
        let node_b = node(&fixture, b, &[a, b]);
        node_b.tick().await;
        assert!(
            node_b.owned.slots(WorkKind::Intake).is_empty(),
            "a still holds them"
        );
        node_a
            .membership
            .replace(vec![record(&fixture.env, a), record(&fixture.env, b)]);
        node_a.tick().await;
        node_b.tick().await;
        let slots_a = node_a.owned.slots(WorkKind::Intake);
        let slots_b = node_b.owned.slots(WorkKind::Intake);
        assert_eq!(slots_a.len() + slots_b.len(), 8);
        assert!(!slots_b.is_empty(), "handed over at once");
    }

    #[tokio::test]
    async fn a_dead_nodes_slots_come_back_when_its_leases_expire() {
        let fixture = fixture().await;
        let (a, b) = (
            fixture.env.node,
            Env::simulated(4, Timestamp::from_nanos(0)).node,
        );
        let node_a = node(&fixture, a, &[a, b]);
        let node_b = node(&fixture, b, &[a, b]);
        node_a.tick().await;
        node_b.tick().await;
        let before = node_b.owned.slots(WorkKind::Intake);
        // a dies: b's view drops it, but a's leases are still fresh.
        node_b.membership.replace(vec![record(&fixture.env, b)]);
        node_b.tick().await;
        assert_eq!(node_b.owned.slots(WorkKind::Intake), before);
        fixture.clock.advance(DURATION + Duration::from_secs(1));
        node_b.tick().await;
        assert_eq!(node_b.owned.slots(WorkKind::Intake).len(), 8);
    }

    #[tokio::test]
    async fn release_all_frees_the_slots_for_the_next_node() {
        let fixture = fixture().await;
        let (a, b) = (
            fixture.env.node,
            Env::simulated(4, Timestamp::from_nanos(0)).node,
        );
        let node_a = node(&fixture, a, &[a]);
        node_a.tick().await;
        node_a.release_all().await;
        assert!(node_a.owned.slots(WorkKind::Intake).is_empty());
        let node_b = node(&fixture, b, &[b]);
        node_b.tick().await;
        assert_eq!(node_b.owned.slots(WorkKind::Intake).len(), 8);
    }
}
