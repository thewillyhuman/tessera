// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Slot leases (spec section 14.2).
//!
//! A lease is a sequence of epoch objects
//! `{prefix}/{kind}/{slot}/{epoch:020}`, each created with create-if-absent
//! and naming a holder and an expiry (OP-1). Renewal is the holder creating
//! the next epoch; takeover is another node creating it after the current
//! one has expired. Exactly one node creates each epoch, so two nodes never
//! both acquire the same epoch.
//!
//! Safety never depends on clocks (OP-2): a holder whose lease expired
//! without it noticing can still believe it owns the slot, and every effect
//! of leased work goes through a conditional write that then fails. Clock
//! skew only changes how quickly takeover happens.
//!
//! Only the two newest epochs of a slot are kept, so a lease costs a
//! bounded number of objects however long it is held.

use std::sync::Arc;
use std::time::Duration;

use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use tessera_object::{BoxError, Key, ObjectError, ObjectStore};
use tessera_types::{Env, Epoch, NodeId, Timestamp, WorkKind};

use crate::{DecodeError, FORMAT, Slot};

/// One epoch of a slot's lease: what is written at
/// `{prefix}/{kind}/{slot}/{epoch}`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    /// The format this object was written in (OP-7).
    pub format: u32,
    /// The kind of work the slot belongs to.
    pub kind: WorkKind,
    /// The slot.
    pub slot: Slot,
    /// The epoch of this acquisition or renewal.
    pub epoch: Epoch,
    /// The node that created this epoch.
    pub holder: NodeId,
    /// When the holder created it, by the holder's clock.
    pub acquired_at: Timestamp,
    /// When it may be taken over, by the holder's clock.
    pub expires_at: Timestamp,
}

impl Lease {
    /// Whether another node may take the slot at `now`.
    #[must_use]
    pub fn is_expired_at(&self, now: Timestamp) -> bool {
        now >= self.expires_at
    }

    fn encode(&self) -> bytes::Bytes {
        serde_json::to_vec(self)
            .expect("a lease always serializes")
            .into()
    }

    fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let lease: Lease = serde_json::from_slice(bytes)?;
        if lease.format > FORMAT {
            return Err(DecodeError::UnsupportedFormat {
                format: lease.format,
                supported: FORMAT,
            });
        }
        Ok(lease)
    }
}

/// Why a lease operation failed.
#[derive(Debug, thiserror::Error)]
pub enum LeaseError {
    /// Only the holder may renew or release a lease.
    #[error("lease for {kind} slot {slot} epoch {epoch} is held by {holder}, not this node")]
    NotHolder {
        /// The kind of work.
        kind: WorkKind,
        /// The slot.
        slot: Slot,
        /// The epoch.
        epoch: Epoch,
        /// The actual holder.
        holder: NodeId,
    },
    /// A lease object cannot be read.
    #[error("lease object {key} is corrupt: {source}")]
    Corrupt {
        /// The offending object.
        key: Key,
        /// What is wrong with it.
        #[source]
        source: BoxError,
    },
    /// The object store failed.
    #[error(transparent)]
    Store(#[from] ObjectError),
}

/// Acquires, renews and releases the leases of this node.
pub struct LeaseManager {
    store: Arc<dyn ObjectStore>,
    prefix: Key,
    env: Env,
    duration: Duration,
}

impl LeaseManager {
    /// The lease duration of OP-3.
    pub const DEFAULT_DURATION: Duration = Duration::from_secs(30);
    /// The renewal interval of OP-3.
    pub const DEFAULT_RENEWAL: Duration = Duration::from_secs(10);

    /// A manager for the leases under `prefix` (`_cluster/leases` in the
    /// root pool), granting them for `duration`.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, prefix: Key, env: Env, duration: Duration) -> Self {
        LeaseManager {
            store,
            prefix,
            env,
            duration,
        }
    }

    /// The newest epoch of a slot, whoever holds it.
    ///
    /// # Errors
    ///
    /// `Store` if the lease objects cannot be listed or read; `Corrupt` if
    /// the newest one cannot be decoded.
    pub async fn current(&self, kind: WorkKind, slot: Slot) -> Result<Option<Lease>, LeaseError> {
        // The newest epoch can vanish between listing and reading only if
        // the holder pruned it, which it does for all but the two newest;
        // one re-listing is then enough.
        for _ in 0..2 {
            let Some(epoch) = self.newest_epoch(kind, slot).await? else {
                return Ok(None);
            };
            match self.read(kind, slot, epoch).await {
                Ok(lease) => return Ok(Some(lease)),
                Err(LeaseError::Store(ObjectError::NotFound { .. })) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(None)
    }

    /// Tries to take a slot: succeeds if it is free, expired, or already
    /// held by this node.
    ///
    /// # Errors
    ///
    /// `Store` or `Corrupt` as for [`current`](Self::current).
    pub async fn acquire(&self, kind: WorkKind, slot: Slot) -> Result<Option<Lease>, LeaseError> {
        let now = self.env.clock.now();
        let next = match self.current(kind, slot).await? {
            None => Epoch::new(1),
            Some(current) if current.holder == self.env.node || current.is_expired_at(now) => {
                current.epoch.next()
            }
            Some(_) => return Ok(None),
        };
        self.claim(kind, slot, next, self.duration).await
    }

    /// Extends a lease this node holds by creating its next epoch. `None`
    /// means another node has taken the slot over.
    ///
    /// # Errors
    ///
    /// `NotHolder` if `lease` was granted to another node; `Store` or
    /// `Corrupt` otherwise.
    pub async fn renew(&self, lease: &Lease) -> Result<Option<Lease>, LeaseError> {
        self.check_holder(lease)?;
        let renewed = self
            .claim(lease.kind, lease.slot, lease.epoch.next(), self.duration)
            .await?;
        if renewed.is_some() {
            self.prune_before(lease.kind, lease.slot, lease.epoch).await;
        }
        Ok(renewed)
    }

    /// Gives a slot up so that another node can take it at once, by
    /// creating a next epoch that is already expired.
    ///
    /// # Errors
    ///
    /// `NotHolder` if `lease` was granted to another node; `Store` otherwise.
    /// Losing the race with a takeover is not an error.
    pub async fn release(&self, lease: &Lease) -> Result<(), LeaseError> {
        self.check_holder(lease)?;
        self.claim(lease.kind, lease.slot, lease.epoch.next(), Duration::ZERO)
            .await?;
        Ok(())
    }

    /// Creates `epoch` for this node. `None` means another node created it.
    async fn claim(
        &self,
        kind: WorkKind,
        slot: Slot,
        epoch: Epoch,
        duration: Duration,
    ) -> Result<Option<Lease>, LeaseError> {
        let now = self.env.clock.now();
        let lease = Lease {
            format: FORMAT,
            kind,
            slot,
            epoch,
            holder: self.env.node,
            acquired_at: now,
            expires_at: now.checked_add(duration).unwrap_or(now),
        };
        let key = self.key(kind, slot, epoch);
        match self.store.put_if_absent(&key, lease.encode()).await {
            Ok(()) => Ok(Some(lease)),
            // Either another node won, or our own earlier write got a lost
            // response; the object says which.
            Err(ObjectError::AlreadyExists { .. }) => self.mine_or_none(kind, slot, epoch).await,
            Err(error) if error.is_retryable() => match self.read(kind, slot, epoch).await {
                Ok(found) => Ok((found.holder == self.env.node).then_some(found)),
                Err(LeaseError::Store(ObjectError::NotFound { .. })) => {
                    Err(LeaseError::Store(error))
                }
                Err(other) => Err(other),
            },
            Err(error) => Err(LeaseError::Store(error)),
        }
    }

    async fn mine_or_none(
        &self,
        kind: WorkKind,
        slot: Slot,
        epoch: Epoch,
    ) -> Result<Option<Lease>, LeaseError> {
        let found = self.read(kind, slot, epoch).await?;
        Ok((found.holder == self.env.node).then_some(found))
    }

    fn check_holder(&self, lease: &Lease) -> Result<(), LeaseError> {
        if lease.holder == self.env.node {
            Ok(())
        } else {
            Err(LeaseError::NotHolder {
                kind: lease.kind,
                slot: lease.slot,
                epoch: lease.epoch,
                holder: lease.holder,
            })
        }
    }

    async fn read(&self, kind: WorkKind, slot: Slot, epoch: Epoch) -> Result<Lease, LeaseError> {
        let key = self.key(kind, slot, epoch);
        let bytes = self.store.get(&key).await?;
        Lease::decode(&bytes).map_err(|e| LeaseError::Corrupt {
            key,
            source: Box::new(e),
        })
    }

    async fn newest_epoch(&self, kind: WorkKind, slot: Slot) -> Result<Option<Epoch>, LeaseError> {
        let mut newest = None;
        let mut listing = self.store.list(&self.slot_prefix(kind, slot));
        while let Some(meta) = listing.try_next().await? {
            if let Ok(epoch) = meta.key.name().parse::<Epoch>() {
                newest = newest.max(Some(epoch));
            }
        }
        Ok(newest)
    }

    /// Deletes the epochs before `keep`, so that a slot holds at most the
    /// epoch being renewed and its successor. Failures only leave garbage.
    async fn prune_before(&self, kind: WorkKind, slot: Slot, keep: Epoch) {
        let mut listing = self.store.list(&self.slot_prefix(kind, slot));
        let mut stale = Vec::new();
        while let Ok(Some(meta)) = listing.try_next().await {
            if meta
                .key
                .name()
                .parse::<Epoch>()
                .is_ok_and(|epoch| epoch < keep)
            {
                stale.push(meta.key);
            }
        }
        drop(listing);
        for key in stale {
            let _ = self.store.delete(&key).await;
        }
    }

    fn slot_prefix(&self, kind: WorkKind, slot: Slot) -> Key {
        self.prefix
            .join(kind.as_str())
            .and_then(|k| k.join(&slot.to_string()))
            .expect("lease key components are valid")
    }

    fn key(&self, kind: WorkKind, slot: Slot, epoch: Epoch) -> Key {
        self.slot_prefix(kind, slot)
            .join(&epoch.to_string())
            .expect("lease key components are valid")
    }
}

impl std::fmt::Debug for LeaseManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaseManager")
            .field("prefix", &self.prefix)
            .field("node", &self.env.node)
            .field("duration", &self.duration)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use futures::TryStreamExt;
    use tessera_object::{FaultPolicy, FaultyStore, Key, MemoryStore, ObjectStore};
    use tessera_types::{
        Clock, Env, Epoch, ManualClock, NodeId, SeededRandom, SharedRandom, Timestamp, WorkKind,
    };

    use super::*;
    use crate::Slot;

    const DURATION: Duration = Duration::from_secs(30);

    struct Cluster {
        store: MemoryStore,
        clock: Arc<ManualClock>,
    }

    impl Cluster {
        fn new() -> Self {
            Cluster {
                store: MemoryStore::new(),
                clock: Arc::new(ManualClock::new(Timestamp::UNIX_EPOCH)),
            }
        }

        fn node(&self, id: u64) -> LeaseManager {
            self.node_on(id, Arc::new(self.store.clone()))
        }

        fn node_on(&self, id: u64, store: Arc<dyn ObjectStore>) -> LeaseManager {
            let env = Env::new(
                NodeId::from_u128(u128::from(id)),
                Arc::clone(&self.clock) as Arc<dyn Clock>,
                SharedRandom::new(SeededRandom::new(id)),
            );
            LeaseManager::new(store, Key::new("_cluster/leases").unwrap(), env, DURATION)
        }

        async fn epochs(&self, slot: Slot) -> Vec<String> {
            let prefix = Key::new(format!("_cluster/leases/intake/{slot}")).unwrap();
            self.store
                .list(&prefix)
                .map_ok(|m| m.key.name().to_owned())
                .try_collect()
                .await
                .unwrap()
        }
    }

    #[tokio::test]
    async fn a_free_slot_is_acquired_at_epoch_one() {
        let cluster = Cluster::new();
        let lease = cluster
            .node(1)
            .acquire(WorkKind::Intake, Slot::new(0))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lease.epoch, Epoch::new(1));
        assert_eq!(lease.holder, NodeId::from_u128(1));
        assert_eq!(
            lease.expires_at,
            Timestamp::UNIX_EPOCH.checked_add(DURATION).unwrap()
        );
    }

    #[tokio::test]
    async fn an_unexpired_lease_blocks_other_nodes() {
        let cluster = Cluster::new();
        cluster
            .node(1)
            .acquire(WorkKind::Intake, Slot::new(0))
            .await
            .unwrap()
            .unwrap();
        cluster.clock.advance(Duration::from_secs(29));
        assert!(
            cluster
                .node(2)
                .acquire(WorkKind::Intake, Slot::new(0))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn an_expired_lease_can_be_taken_over() {
        let cluster = Cluster::new();
        cluster
            .node(1)
            .acquire(WorkKind::Intake, Slot::new(0))
            .await
            .unwrap()
            .unwrap();
        cluster.clock.advance(DURATION);
        let taken = cluster
            .node(2)
            .acquire(WorkKind::Intake, Slot::new(0))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(taken.epoch, Epoch::new(2));
        assert_eq!(taken.holder, NodeId::from_u128(2));
    }

    #[tokio::test]
    async fn the_holder_renews_by_creating_the_next_epoch() {
        let cluster = Cluster::new();
        let node = cluster.node(1);
        let mut lease = node
            .acquire(WorkKind::Intake, Slot::new(0))
            .await
            .unwrap()
            .unwrap();
        for expected in 2..=4u64 {
            cluster.clock.advance(Duration::from_secs(10));
            lease = node.renew(&lease).await.unwrap().unwrap();
            assert_eq!(lease.epoch, Epoch::new(expected));
            assert_eq!(
                lease.expires_at,
                cluster.clock.now().checked_add(DURATION).unwrap()
            );
        }
        assert_eq!(
            cluster.epochs(Slot::new(0)).await,
            ["00000000000000000003", "00000000000000000004"]
        );
    }

    #[tokio::test]
    async fn a_renewal_after_takeover_fails() {
        let cluster = Cluster::new();
        let first = cluster.node(1);
        let lease = first
            .acquire(WorkKind::Intake, Slot::new(0))
            .await
            .unwrap()
            .unwrap();
        cluster.clock.advance(DURATION);
        cluster
            .node(2)
            .acquire(WorkKind::Intake, Slot::new(0))
            .await
            .unwrap()
            .unwrap();
        assert!(first.renew(&lease).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn releasing_lets_another_node_take_over_at_once() {
        let cluster = Cluster::new();
        let first = cluster.node(1);
        let lease = first
            .acquire(WorkKind::Intake, Slot::new(0))
            .await
            .unwrap()
            .unwrap();
        first.release(&lease).await.unwrap();
        let taken = cluster
            .node(2)
            .acquire(WorkKind::Intake, Slot::new(0))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(taken.epoch, Epoch::new(3));
    }

    #[tokio::test]
    async fn the_holder_can_reacquire_before_expiry() {
        let cluster = Cluster::new();
        let node = cluster.node(1);
        node.acquire(WorkKind::Intake, Slot::new(0))
            .await
            .unwrap()
            .unwrap();
        let again = node
            .acquire(WorkKind::Intake, Slot::new(0))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(again.epoch, Epoch::new(2));
    }

    #[tokio::test]
    async fn renewing_a_lease_held_by_another_node_is_refused() {
        let cluster = Cluster::new();
        let lease = cluster
            .node(1)
            .acquire(WorkKind::Intake, Slot::new(0))
            .await
            .unwrap()
            .unwrap();
        let error = cluster.node(2).renew(&lease).await.unwrap_err();
        assert!(matches!(error, LeaseError::NotHolder { .. }), "{error}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_lost_response_still_yields_the_lease() {
        let cluster = Cluster::new();
        let faulty = FaultyStore::new(
            Arc::new(cluster.store.clone()),
            SharedRandom::new(SeededRandom::new(9)),
            FaultPolicy {
                fail_before: 0.0,
                fail_after: 1.0,
            },
        );
        let node = cluster.node_on(1, Arc::new(faulty));
        let lease = node
            .acquire(WorkKind::Intake, Slot::new(0))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lease.epoch, Epoch::new(1));
        assert!(
            cluster
                .node(2)
                .acquire(WorkKind::Intake, Slot::new(0))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn slots_and_kinds_are_independent() {
        let cluster = Cluster::new();
        cluster
            .node(1)
            .acquire(WorkKind::Intake, Slot::new(0))
            .await
            .unwrap()
            .unwrap();
        assert!(
            cluster
                .node(2)
                .acquire(WorkKind::Intake, Slot::new(1))
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            cluster
                .node(2)
                .acquire(WorkKind::Partition, Slot::new(0))
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn a_corrupt_lease_object_is_reported() {
        let cluster = Cluster::new();
        let key = Key::new("_cluster/leases/intake/0/00000000000000000001").unwrap();
        cluster
            .store
            .put(&key, Bytes::from_static(b"nonsense"))
            .await
            .unwrap();
        let error = cluster
            .node(1)
            .acquire(WorkKind::Intake, Slot::new(0))
            .await
            .unwrap_err();
        assert!(
            matches!(error, LeaseError::Corrupt { key: ref k, .. } if *k == key),
            "{error}"
        );
    }
}
