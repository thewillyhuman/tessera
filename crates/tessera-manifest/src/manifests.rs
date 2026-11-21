// SPDX-License-Identifier: Apache-2.0

//! Open manifests kept in memory (section 10.8): a commit log remembers
//! the last snapshot it built and reads only what was committed after
//! it, so a node that keeps the manifests it has opened pays for a
//! partition's history once and for its changes thereafter. The cache
//! is local and disposable: dropping an entry costs one re-read.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, PoisonError};

use futures::{StreamExt, TryStreamExt};
use tessera_object::ObjectStore;
use tessera_types::{DatasetName, Env, Kind, PartitionId, TenantId, TimeRange, Timestamp, Version};

use crate::manifest::{Manifest, ManifestError, list_partitions};

/// How many manifests' versions are read at once.
const VERSIONS_AT_ONCE: usize = 16;

/// Partitions whose manifests stay open at once by default.
pub const DEFAULT_CAPACITY: usize = 4096;

/// The manifests a node has opened, oldest forgotten first past the
/// capacity.
pub struct Manifests {
    store: Arc<dyn ObjectStore>,
    env: Env,
    capacity: usize,
    open: Mutex<Open>,
}

#[derive(Default)]
struct Open {
    by_id: HashMap<PartitionId, Arc<Manifest>>,
    order: VecDeque<PartitionId>,
}

impl Manifests {
    /// Manifests in the root pool `store`, at most `capacity` open.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, env: Env, capacity: usize) -> Self {
        Manifests {
            store,
            env,
            capacity: capacity.max(1),
            open: Mutex::new(Open::default()),
        }
    }

    /// The root pool's store.
    #[must_use]
    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    /// The node's environment.
    #[must_use]
    pub fn env(&self) -> &Env {
        &self.env
    }

    /// The manifest of `id`: the one already open, or a new one.
    #[must_use]
    pub fn open(&self, id: &PartitionId) -> Arc<Manifest> {
        let mut open = self.open.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(manifest) = open.by_id.get(id) {
            return Arc::clone(manifest);
        }
        let manifest = Arc::new(Manifest::open(
            Arc::clone(&self.store),
            id.clone(),
            self.env.clone(),
        ));
        open.by_id.insert(id.clone(), Arc::clone(&manifest));
        open.order.push_back(id.clone());
        while open.order.len() > self.capacity {
            if let Some(oldest) = open.order.pop_front() {
                open.by_id.remove(&oldest);
            }
        }
        manifest
    }

    /// How many manifests are open.
    #[must_use]
    pub fn len(&self) -> usize {
        self.open
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .by_id
            .len()
    }

    /// Whether none is open.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The partitions of a dataset and kind whose window overlaps
    /// `time_range`, each with its manifest's current version: what a
    /// result depends on.
    ///
    /// # Errors
    ///
    /// If the partitions cannot be listed or a manifest cannot be read.
    pub async fn versions(
        &self,
        tenant: &TenantId,
        dataset: &DatasetName,
        kind: Kind,
        time_range: TimeRange,
    ) -> Result<Vec<(PartitionId, Version)>, ManifestError> {
        let mut in_range = Vec::new();
        for partition in list_partitions(&*self.store, tenant, dataset, kind).await? {
            let w = partition.window;
            let last = Timestamp::from_nanos(w.end().as_nanos().saturating_sub(1));
            if time_range.overlaps_closed(w.start(), last) {
                in_range.push((self.open(&partition), partition));
            }
        }
        // Each version is a read of a manifest's head: made together,
        // and returned in the order of the partitions.
        let reads = in_range
            .into_iter()
            .map(|(manifest, partition)| async move {
                let version = manifest.snapshot().await?.version();
                Ok::<_, ManifestError>((partition, version))
            });
        let versions = futures::stream::iter(reads)
            .buffered(VERSIONS_AT_ONCE)
            .try_collect()
            .await?;
        Ok(versions)
    }
}

impl std::fmt::Debug for Manifests {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Manifests")
            .field("open", &self.len())
            .field("capacity", &self.capacity)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use tessera_object::MemoryStore;
    use tessera_types::Window;

    use super::*;

    fn partition(day: u64) -> PartitionId {
        PartitionId {
            tenant: TenantId::DEFAULT,
            dataset: DatasetName::new("prod").unwrap(),
            kind: Kind::Log,
            window: Window::containing(
                Timestamp::from_nanos(i64::try_from(day * 86_400 * 1_000_000_000).unwrap()),
                Window::DEFAULT_LENGTH,
            )
            .unwrap(),
        }
    }

    /// The key of a cached result is the versions of the manifests it
    /// read: asked for on every query that can be answered from the
    /// cache, over a bucket where each is a request.
    #[tokio::test]
    async fn the_versions_of_a_range_are_read_at_once() {
        use tessera_types::{Seq, WriterId};

        use crate::Manifest;
        use crate::busy::Busy;

        let env = Env::simulated(1, Timestamp::from_nanos(0));
        let store = MemoryStore::new();
        for day in 0..4 {
            let manifest = Manifest::open(Arc::new(store.clone()), partition(day), env.clone());
            let adopted = manifest.adopt(WriterId::from_u128(1), Seq::new(1), &[]);
            adopted.await.unwrap();
        }
        let busy = Arc::new(Busy::over(store));
        let manifests = Manifests::new(busy.clone(), env, 8);
        let dataset = DatasetName::new("prod").unwrap();
        let versions = manifests
            .versions(&TenantId::DEFAULT, &dataset, Kind::Log, TimeRange::ALL)
            .await
            .unwrap();
        assert_eq!(versions.len(), 4);
        let most = busy.most();
        assert!(most > 1, "at most {most} read at a time");
    }

    #[test]
    fn the_same_manifest_is_handed_out_until_it_is_forgotten() {
        let env = Env::simulated(1, Timestamp::from_nanos(0));
        let manifests = Manifests::new(Arc::new(MemoryStore::new()), env, 2);
        let first = manifests.open(&partition(0));
        assert!(Arc::ptr_eq(&first, &manifests.open(&partition(0))));
        let _ = manifests.open(&partition(1));
        let _ = manifests.open(&partition(2));
        assert_eq!(
            manifests.len(),
            2,
            "the oldest is forgotten past the capacity"
        );
        assert!(
            !Arc::ptr_eq(&first, &manifests.open(&partition(0))),
            "and opened anew"
        );
    }
}
