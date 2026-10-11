// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Storage pools (spec section 6.1).
//!
//! A deployment starts with one root pool (ST-1) and may add, weight, drain
//! and remove further pools at runtime (ST-2). Every reference to an object
//! in metadata is a (pool, key) pair, [`ObjectRef`], so that no bucket's
//! limits bound the deployment.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tessera_types::{PoolId, Random};

use crate::{Key, ObjectStore};

/// A reference to one object: the pool that holds it and its key (ST-2).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct ObjectRef {
    /// The pool holding the object.
    pub pool: PoolId,
    /// The object's key within the pool.
    pub key: Key,
}

/// Whether a pool accepts new objects.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PoolState {
    /// New objects may be placed in the pool.
    Active,
    /// Existing objects stay readable; nothing new is placed (ST-2).
    Draining,
}

/// Why a pool operation failed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PoolError {
    /// A pool with this id is already registered.
    #[error("pool {0} already exists")]
    Duplicate(PoolId),
    /// No pool with this id is registered.
    #[error("unknown pool {0}")]
    Unknown(PoolId),
    /// The root pool holds the cluster catalog and cannot be removed.
    #[error("the root pool {0} cannot be removed")]
    RootRemoval(PoolId),
}

/// One registered pool and its open store.
#[derive(Clone)]
pub struct Pool {
    id: PoolId,
    store: Arc<dyn ObjectStore>,
    weight: u32,
    state: PoolState,
}

impl Pool {
    /// The pool's id.
    #[must_use]
    pub fn id(&self) -> &PoolId {
        &self.id
    }

    /// The store behind the pool.
    #[must_use]
    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    /// The pool's share of new objects relative to the other active pools.
    #[must_use]
    pub fn weight(&self) -> u32 {
        self.weight
    }

    /// Whether the pool accepts new objects.
    #[must_use]
    pub fn state(&self) -> PoolState {
        self.state
    }

    fn accepts_placements(&self) -> bool {
        self.state == PoolState::Active && self.weight > 0
    }
}

impl fmt::Debug for Pool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pool")
            .field("id", &self.id)
            .field("weight", &self.weight)
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

/// The pools of a deployment: the root pool plus any added at runtime.
///
/// This is the live registry of open stores. The catalog (section 12.1)
/// holds the configuration it is built from.
#[derive(Clone, Debug)]
pub struct Pools {
    root: PoolId,
    pools: BTreeMap<PoolId, Pool>,
}

impl Pools {
    /// A registry with only the root pool (ST-1), at weight 1.
    #[must_use]
    pub fn new(root: PoolId, store: Arc<dyn ObjectStore>) -> Self {
        let pool = Pool {
            id: root.clone(),
            store,
            weight: 1,
            state: PoolState::Active,
        };
        Pools {
            root: root.clone(),
            pools: BTreeMap::from([(root, pool)]),
        }
    }

    /// The root pool, which holds the cluster catalog.
    #[must_use]
    pub fn root(&self) -> &Pool {
        &self.pools[&self.root]
    }

    /// The pool with the given id.
    ///
    /// # Errors
    ///
    /// `Unknown` if no such pool is registered.
    pub fn get(&self, id: &PoolId) -> Result<&Pool, PoolError> {
        self.pools
            .get(id)
            .ok_or_else(|| PoolError::Unknown(id.clone()))
    }

    /// Every pool, in id order.
    pub fn iter(&self) -> impl Iterator<Item = &Pool> {
        self.pools.values()
    }

    /// Registers an active pool.
    ///
    /// # Errors
    ///
    /// `Duplicate` if the id is taken.
    pub fn add(
        &mut self,
        id: PoolId,
        store: Arc<dyn ObjectStore>,
        weight: u32,
    ) -> Result<(), PoolError> {
        if self.pools.contains_key(&id) {
            return Err(PoolError::Duplicate(id));
        }
        self.pools.insert(
            id.clone(),
            Pool {
                id,
                store,
                weight,
                state: PoolState::Active,
            },
        );
        Ok(())
    }

    /// Changes a pool's weight. Zero removes it from placement without
    /// draining it.
    ///
    /// # Errors
    ///
    /// `Unknown` if no such pool is registered.
    pub fn set_weight(&mut self, id: &PoolId, weight: u32) -> Result<(), PoolError> {
        self.get_mut(id)?.weight = weight;
        Ok(())
    }

    /// Stops placing new objects in a pool; existing objects stay readable.
    ///
    /// # Errors
    ///
    /// `Unknown` if no such pool is registered.
    pub fn drain(&mut self, id: &PoolId) -> Result<(), PoolError> {
        self.get_mut(id)?.state = PoolState::Draining;
        Ok(())
    }

    /// Unregisters a pool. The caller is responsible for having moved or
    /// dropped every object it held.
    ///
    /// # Errors
    ///
    /// `RootRemoval` for the root pool; `Unknown` if no such pool exists.
    pub fn remove(&mut self, id: &PoolId) -> Result<Pool, PoolError> {
        if *id == self.root {
            return Err(PoolError::RootRemoval(id.clone()));
        }
        self.pools
            .remove(id)
            .ok_or_else(|| PoolError::Unknown(id.clone()))
    }

    /// Chooses the pool for a new object of a tenant placed in
    /// `preferred` (CT-7): that pool while it accepts placements, by
    /// weight otherwise, as [`Pools::place`] does.
    pub fn place_in(&self, preferred: Option<&PoolId>, random: &mut impl Random) -> &Pool {
        preferred
            .and_then(|id| self.pools.get(id))
            .filter(|pool| pool.accepts_placements())
            .unwrap_or_else(|| self.place(random))
    }

    /// Chooses the pool for a new object, in proportion to the weights of
    /// the active pools. Falls back to the root pool when no pool is
    /// eligible, so that writes never stop for lack of placement.
    pub fn place(&self, random: &mut impl Random) -> &Pool {
        let eligible: Vec<&Pool> = self
            .pools
            .values()
            .filter(|p| p.accepts_placements())
            .collect();
        let total: u64 = eligible.iter().map(|p| u64::from(p.weight)).sum();
        if total == 0 {
            return self.root();
        }
        let mut draw = random.below(total);
        for pool in eligible {
            let weight = u64::from(pool.weight);
            if draw < weight {
                return pool;
            }
            draw -= weight;
        }
        self.root()
    }

    /// The store that holds the referenced object.
    ///
    /// # Errors
    ///
    /// `Unknown` if the reference names a pool that is not registered.
    pub fn resolve(&self, reference: &ObjectRef) -> Result<Arc<dyn ObjectStore>, PoolError> {
        self.get(&reference.pool)
            .map(|pool| Arc::clone(&pool.store))
    }

    fn get_mut(&mut self, id: &PoolId) -> Result<&mut Pool, PoolError> {
        self.pools
            .get_mut(id)
            .ok_or_else(|| PoolError::Unknown(id.clone()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tessera_types::SeededRandom;

    use super::*;
    use crate::MemoryStore;

    /// CT-7: a tenant's pool takes its objects while it accepts them.
    #[test]
    fn a_preferred_pool_is_used_while_it_accepts_placements() {
        let mut pools = Pools::new(pool("root"), Arc::new(MemoryStore::new()));
        pools
            .add(pool("cold"), Arc::new(MemoryStore::new()), 1)
            .unwrap();
        let mut random = SeededRandom::new(3);
        assert_eq!(
            pools.place_in(Some(&pool("cold")), &mut random).id(),
            &pool("cold")
        );
        pools.set_weight(&pool("cold"), 0).unwrap();
        assert_eq!(
            pools.place_in(Some(&pool("gone")), &mut random).id(),
            &pool("root"),
            "unknown: by weight"
        );
        assert_eq!(
            pools.place_in(Some(&pool("cold")), &mut random).id(),
            &pool("root"),
            "retired: by weight"
        );
    }

    fn pool(id: &str) -> PoolId {
        PoolId::new(id).unwrap()
    }

    fn store() -> Arc<dyn ObjectStore> {
        Arc::new(MemoryStore::new())
    }

    fn count_placements(pools: &Pools, draws: usize) -> std::collections::BTreeMap<PoolId, usize> {
        let mut random = SeededRandom::new(11);
        let mut counts = std::collections::BTreeMap::new();
        for _ in 0..draws {
            *counts
                .entry(pools.place(&mut random).id().clone())
                .or_insert(0) += 1;
        }
        counts
    }

    #[test]
    fn the_root_pool_is_the_only_target_at_start() {
        let pools = Pools::new(pool("root"), store());
        assert_eq!(pools.root().id(), &pool("root"));
        assert_eq!(count_placements(&pools, 10)[&pool("root")], 10);
    }

    #[test]
    fn placement_follows_weights() {
        let mut pools = Pools::new(pool("root"), store());
        pools.add(pool("big"), store(), 3).unwrap();
        let counts = count_placements(&pools, 4_000);
        let big = counts[&pool("big")];
        assert!(
            (2_800..=3_200).contains(&big),
            "big pool got {big} of 4000, expected about 3000"
        );
    }

    #[test]
    fn a_draining_pool_receives_no_placements() {
        let mut pools = Pools::new(pool("root"), store());
        pools.add(pool("old"), store(), 10).unwrap();
        pools.drain(&pool("old")).unwrap();
        assert!(!count_placements(&pools, 100).contains_key(&pool("old")));
        assert_eq!(
            pools.get(&pool("old")).unwrap().state(),
            PoolState::Draining
        );
    }

    #[test]
    fn a_zero_weight_pool_receives_no_placements() {
        let mut pools = Pools::new(pool("root"), store());
        pools.add(pool("idle"), store(), 0).unwrap();
        assert!(!count_placements(&pools, 100).contains_key(&pool("idle")));
    }

    #[test]
    fn placement_falls_back_to_the_root_when_nothing_else_is_eligible() {
        let mut pools = Pools::new(pool("root"), store());
        pools.set_weight(&pool("root"), 0).unwrap();
        assert_eq!(count_placements(&pools, 5)[&pool("root")], 5);
    }

    #[test]
    fn the_root_pool_cannot_be_removed() {
        let mut pools = Pools::new(pool("root"), store());
        assert!(matches!(
            pools.remove(&pool("root")),
            Err(PoolError::RootRemoval(_))
        ));
    }

    #[test]
    fn a_duplicate_pool_is_rejected() {
        let mut pools = Pools::new(pool("root"), store());
        assert!(matches!(
            pools.add(pool("root"), store(), 1),
            Err(PoolError::Duplicate(_))
        ));
    }

    #[test]
    fn removing_a_pool_returns_it() {
        let mut pools = Pools::new(pool("root"), store());
        pools.add(pool("extra"), store(), 1).unwrap();
        let removed = pools.remove(&pool("extra")).unwrap();
        assert_eq!(removed.id(), &pool("extra"));
        assert!(matches!(
            pools.get(&pool("extra")),
            Err(PoolError::Unknown(_))
        ));
    }

    #[test]
    fn resolve_finds_the_store_of_a_reference() {
        let mut pools = Pools::new(pool("root"), store());
        pools.add(pool("extra"), store(), 1).unwrap();
        let reference = ObjectRef {
            pool: pool("extra"),
            key: Key::new("a/b").unwrap(),
        };
        assert!(pools.resolve(&reference).is_ok());
        let unknown = ObjectRef {
            pool: pool("nope"),
            key: Key::new("a/b").unwrap(),
        };
        assert!(matches!(
            pools.resolve(&unknown),
            Err(PoolError::Unknown(_))
        ));
    }

    #[test]
    fn object_ref_serializes_as_pool_and_key() {
        let reference = ObjectRef {
            pool: pool("root"),
            key: Key::new("a/b").unwrap(),
        };
        let json = serde_json::to_string(&reference).unwrap();
        assert_eq!(json, r#"{"pool":"root","key":"a/b"}"#);
        assert_eq!(serde_json::from_str::<ObjectRef>(&json).unwrap(), reference);
    }
}
