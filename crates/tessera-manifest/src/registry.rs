// SPDX-License-Identifier: Apache-2.0

//! The writer registry of a dataset: the writers that may hold rows of
//! the dataset not yet adopted by its partitions.
//!
//! A writer registers before it acknowledges its first row for a dataset
//! (IN-6); intake removes it once its log has ended and every flush record
//! has been adopted (IT-3). A reader reads the registry before the
//! manifests (VW-2), so a writer that is missing was either fully adopted
//! before the read or registered after it began.

use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use tessera_object::{ObjectError, ObjectMeta, ObjectStore, layout};
use tessera_types::{DatasetName, Env, NodeId, Seq, TenantId, Timestamp, WriterId};

use crate::ManifestError;

/// How many registrations are read at once.
const REGISTRATIONS_AT_ONCE: usize = 16;

/// One registered writer.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Registration {
    /// The writer.
    pub writer: WriterId,
    /// The node that ran the writer when it registered.
    pub node: NodeId,
    /// When it registered, by that node's clock.
    pub registered_at: Timestamp,
    /// How far the writer's flushes of this dataset are adopted: every
    /// flush record up to this position has its files in the manifests
    /// of all the partitions it touches, so no row of the dataset in a
    /// segment at or before it is to be read from the log. Moved on by
    /// intake ([`advance`]); a reader that starts after it skips
    /// nothing it needs, and one that starts earlier only reads more.
    #[serde(default)]
    pub adopted: Seq,
}

/// Registers `writer` with a dataset. Registering a writer twice is
/// harmless.
///
/// # Errors
///
/// `Store` if the registration cannot be written.
pub async fn register(
    store: &dyn ObjectStore,
    env: &Env,
    tenant: &TenantId,
    dataset: &DatasetName,
    writer: WriterId,
) -> Result<(), ManifestError> {
    let registration = Registration {
        writer,
        node: env.node,
        registered_at: env.clock.now(),
        adopted: Seq::ZERO,
    };
    let body = serde_json::to_vec(&registration)
        .unwrap_or_else(|e| unreachable!("a registration serializes: {e}"));
    match store
        .put_if_absent(
            &layout::writer_registration(tenant, dataset, writer),
            body.into(),
        )
        .await
    {
        Ok(()) | Err(ObjectError::AlreadyExists { .. }) => Ok(()),
        Err(error) => Err(ManifestError::Store(error)),
    }
}

/// The registered writers of a dataset, in writer-id order.
///
/// # Errors
///
/// `Store` if the registry cannot be listed or read; `Corrupt` if an
/// entry cannot be decoded.
pub async fn list(
    store: &dyn ObjectStore,
    tenant: &TenantId,
    dataset: &DatasetName,
) -> Result<Vec<Registration>, ManifestError> {
    let listed: Vec<ObjectMeta> = store
        .list(&layout::writer_registry(tenant, dataset))
        .try_collect()
        .await?;
    // A read each, of one another independent: made together.
    let reads = listed.into_iter().map(|meta| async move {
        let bytes = match store.get(&meta.key).await {
            Ok(bytes) => bytes,
            // Removed between listing and reading: fully adopted, so not a
            // writer the reader needs.
            Err(ObjectError::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(ManifestError::Store(error)),
        };
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|e| ManifestError::Corrupt {
                key: meta.key.clone(),
                source: Box::new(e),
            })
    });
    let read: Vec<Option<Registration>> = futures::stream::iter(reads)
        .buffered(REGISTRATIONS_AT_ONCE)
        .try_collect()
        .await?;
    let mut registrations: Vec<Registration> = read.into_iter().flatten().collect();
    registrations.sort_by_key(|r| r.writer);
    Ok(registrations)
}

/// The datasets of a tenant that have at least one registered writer, in
/// name order. Found from the names under the tenant's prefix and under
/// each dataset's registry, never by walking the tenant's objects.
///
/// # Errors
///
/// `Store` if the prefix cannot be listed.
pub async fn datasets(
    store: &dyn ObjectStore,
    tenant: &TenantId,
) -> Result<Vec<DatasetName>, ManifestError> {
    let prefix = tessera_object::Key::new(tenant.as_str())
        .unwrap_or_else(|e| unreachable!("a tenant id is a valid key: {e}"));
    let mut datasets: Vec<DatasetName> = Vec::new();
    for name in store.list_names(&prefix).await? {
        // What is not a dataset's name is the tenant's own: its control
        // store, its rule state.
        let Ok(dataset) = DatasetName::new(name) else {
            continue;
        };
        let writers = layout::writer_registry(tenant, &dataset);
        if !store.list_names(&writers).await?.is_empty() {
            datasets.push(dataset);
        }
    }
    datasets.sort();
    Ok(datasets)
}

/// Records that `writer`'s flushes of the dataset are adopted through
/// `through`. The position only moves forward, and a writer that has
/// left the registry is not brought back.
///
/// Two nodes running intake for one writer may write one after the
/// other; the later write may then carry the earlier position, which
/// costs readers some reading until the next flush and nothing else.
///
/// # Errors
///
/// `Store` if the registration cannot be read or written; `Corrupt` if
/// it cannot be decoded.
pub async fn advance(
    store: &dyn ObjectStore,
    tenant: &TenantId,
    dataset: &DatasetName,
    writer: WriterId,
    through: Seq,
) -> Result<(), ManifestError> {
    let key = layout::writer_registration(tenant, dataset, writer);
    let bytes = match store.get(&key).await {
        Ok(bytes) => bytes,
        Err(ObjectError::NotFound { .. }) => return Ok(()),
        Err(error) => return Err(ManifestError::Store(error)),
    };
    let mut registration: Registration =
        serde_json::from_slice(&bytes).map_err(|e| ManifestError::Corrupt {
            key: key.clone(),
            source: Box::new(e),
        })?;
    if registration.adopted >= through {
        return Ok(());
    }
    registration.adopted = through;
    let body = serde_json::to_vec(&registration)
        .unwrap_or_else(|e| unreachable!("a registration serializes: {e}"));
    Ok(store.put(&key, body.into()).await?)
}

/// Removes a writer from a dataset's registry (IT-3). Removing an absent
/// writer is harmless.
///
/// # Errors
///
/// `Store` if the entry cannot be deleted.
pub async fn remove(
    store: &dyn ObjectStore,
    tenant: &TenantId,
    dataset: &DatasetName,
    writer: WriterId,
) -> Result<(), ManifestError> {
    Ok(store
        .delete(&layout::writer_registration(tenant, dataset, writer))
        .await?)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use tessera_object::MemoryStore;

    use super::*;

    fn dataset() -> DatasetName {
        DatasetName::new("prod").unwrap()
    }

    /// The background work asks for a tenant's datasets every round.
    #[tokio::test]
    async fn datasets_are_found_without_walking_the_tenant() {
        let store = MemoryStore::new();
        let env = Env::simulated(1, Timestamp::from_nanos(5));
        let other = DatasetName::new("staging").unwrap();
        for dataset in [dataset(), other.clone()] {
            let writer = WriterId::from_u128(1);
            register(&store, &env, &TenantId::DEFAULT, &dataset, writer)
                .await
                .unwrap();
        }
        // A dataset with a log of its own and no writer registered.
        let unwritten = layout::dataset_log(&TenantId::DEFAULT, &DatasetName::new("idle").unwrap());
        let commit = unwritten.join("_log").unwrap().join("1").unwrap();
        store.put(&commit, Bytes::from_static(b"{}")).await.unwrap();

        let shallow = tessera_object::ShallowStore(store);
        let found = datasets(&shallow, &TenantId::DEFAULT).await.unwrap();
        assert_eq!(found, [dataset(), other]);
    }

    #[tokio::test]
    async fn registers_lists_and_removes_writers() {
        let store = MemoryStore::new();
        let env = Env::simulated(1, Timestamp::from_nanos(5));
        let (a, b) = (WriterId::from_u128(2), WriterId::from_u128(1));
        register(&store, &env, &TenantId::DEFAULT, &dataset(), a)
            .await
            .unwrap();
        register(&store, &env, &TenantId::DEFAULT, &dataset(), b)
            .await
            .unwrap();
        register(&store, &env, &TenantId::DEFAULT, &dataset(), a)
            .await
            .expect("registering twice is harmless");
        let listed = list(&store, &TenantId::DEFAULT, &dataset()).await.unwrap();
        assert_eq!(listed.iter().map(|r| r.writer).collect::<Vec<_>>(), [b, a]);
        assert_eq!(listed[0].registered_at, Timestamp::from_nanos(5));
        assert_eq!(listed[0].node, env.node);
        remove(&store, &TenantId::DEFAULT, &dataset(), a)
            .await
            .unwrap();
        remove(&store, &TenantId::DEFAULT, &dataset(), a)
            .await
            .expect("removing twice is harmless");
        let listed = list(&store, &TenantId::DEFAULT, &dataset()).await.unwrap();
        assert_eq!(listed.iter().map(|r| r.writer).collect::<Vec<_>>(), [b]);
    }

    /// What a reader may skip of a writer's log is what intake has put
    /// in the manifests: the registration says how far that is, moves
    /// forward only, and is not brought back once the writer left.
    #[tokio::test]
    async fn a_registration_tells_how_far_the_writers_flushes_are_adopted() {
        let store = MemoryStore::new();
        let env = Env::simulated(1, Timestamp::from_nanos(5));
        let writer = WriterId::from_u128(1);
        let adopted = async || {
            let listed = list(&store, &TenantId::DEFAULT, &dataset()).await.unwrap();
            listed.first().map(|registration| registration.adopted)
        };
        register(&store, &env, &TenantId::DEFAULT, &dataset(), writer)
            .await
            .unwrap();
        assert_eq!(adopted().await, Some(Seq::ZERO));
        for (through, expected) in [(5, 5), (3, 5), (8, 8)] {
            advance(
                &store,
                &TenantId::DEFAULT,
                &dataset(),
                writer,
                Seq::new(through),
            )
            .await
            .unwrap();
            assert_eq!(adopted().await, Some(Seq::new(expected)));
        }
        let listed = list(&store, &TenantId::DEFAULT, &dataset()).await.unwrap();
        assert_eq!(listed[0].registered_at, Timestamp::from_nanos(5));

        remove(&store, &TenantId::DEFAULT, &dataset(), writer)
            .await
            .unwrap();
        advance(&store, &TenantId::DEFAULT, &dataset(), writer, Seq::new(9))
            .await
            .expect("advancing a writer that left is harmless");
        assert_eq!(adopted().await, None);
    }

    /// A registration written before the position was recorded reads as
    /// one that has adopted nothing.
    #[tokio::test]
    async fn a_registration_without_a_position_has_adopted_nothing() {
        let store = MemoryStore::new();
        let writer = WriterId::from_u128(1);
        let old = format!(
            r#"{{"writer":"{writer}","node":"{}","registered_at":5}}"#,
            Env::simulated(1, Timestamp::UNIX_EPOCH).node
        );
        let key = layout::writer_registration(&TenantId::DEFAULT, &dataset(), writer);
        store.put(&key, Bytes::from(old)).await.unwrap();
        let listed = list(&store, &TenantId::DEFAULT, &dataset()).await.unwrap();
        assert_eq!(listed[0].adopted, Seq::ZERO);
    }

    /// A query reads the registration of every writer of its dataset.
    /// Over a bucket, one after another is a round trip each.
    #[tokio::test]
    async fn registrations_are_read_together() {
        let store = MemoryStore::new();
        let env = Env::simulated(1, Timestamp::from_nanos(5));
        for writer in 1..=4 {
            let writer = WriterId::from_u128(writer);
            register(&store, &env, &TenantId::DEFAULT, &dataset(), writer)
                .await
                .unwrap();
        }
        let busy = crate::busy::Busy::over(store);
        let listed = list(&busy, &TenantId::DEFAULT, &dataset()).await.unwrap();
        assert_eq!(
            listed.iter().map(|r| r.writer).collect::<Vec<_>>(),
            (1..=4).map(WriterId::from_u128).collect::<Vec<_>>()
        );
        assert!(busy.most() > 1, "at most {} read at a time", busy.most());
    }

    #[tokio::test]
    async fn lists_the_datasets_that_have_writers() {
        let store = MemoryStore::new();
        let env = Env::simulated(1, Timestamp::UNIX_EPOCH);
        register(
            &store,
            &env,
            &TenantId::DEFAULT,
            &DatasetName::new("staging").unwrap(),
            WriterId::from_u128(1),
        )
        .await
        .unwrap();
        register(
            &store,
            &env,
            &TenantId::DEFAULT,
            &DatasetName::new("prod").unwrap(),
            WriterId::from_u128(2),
        )
        .await
        .unwrap();
        register(
            &store,
            &env,
            &TenantId::DEFAULT,
            &DatasetName::new("prod").unwrap(),
            WriterId::from_u128(3),
        )
        .await
        .unwrap();
        register(
            &store,
            &env,
            &TenantId::SYSTEM,
            &DatasetName::new("audit").unwrap(),
            WriterId::from_u128(4),
        )
        .await
        .unwrap();
        let found = datasets(&store, &TenantId::DEFAULT).await.unwrap();
        assert_eq!(
            found,
            [
                DatasetName::new("prod").unwrap(),
                DatasetName::new("staging").unwrap()
            ]
        );
        assert!(
            datasets(&store, &TenantId::new("nobody").unwrap())
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn an_unknown_dataset_has_no_writers() {
        let store = MemoryStore::new();
        assert!(
            list(&store, &TenantId::DEFAULT, &dataset())
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn a_corrupt_registration_is_reported() {
        let store = MemoryStore::new();
        let key =
            layout::writer_registration(&TenantId::DEFAULT, &dataset(), WriterId::from_u128(1));
        store.put(&key, Bytes::from_static(b"junk")).await.unwrap();
        let error = list(&store, &TenantId::DEFAULT, &dataset())
            .await
            .unwrap_err();
        assert!(matches!(error, ManifestError::Corrupt { .. }), "{error}");
    }
}
