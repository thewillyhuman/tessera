// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The writer registry of a dataset: the writers that may hold rows of
//! the dataset not yet adopted by its partitions.
//!
//! A writer registers before it acknowledges its first row for a dataset
//! (IN-6); intake removes it once its log has ended and every flush record
//! has been adopted (IT-3). A reader reads the registry before the
//! manifests (VW-2), so a writer that is missing was either fully adopted
//! before the read or registered after it began.

use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use tessera_object::{ObjectError, ObjectStore, layout};
use tessera_types::{DatasetName, Env, NodeId, TenantId, Timestamp, WriterId};

use crate::ManifestError;

/// One registered writer.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Registration {
    /// The writer.
    pub writer: WriterId,
    /// The node that ran the writer when it registered.
    pub node: NodeId,
    /// When it registered, by that node's clock.
    pub registered_at: Timestamp,
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
    let mut registrations = Vec::new();
    let mut listing = store.list(&layout::writer_registry(tenant, dataset));
    while let Some(meta) = listing.try_next().await? {
        let bytes = match store.get(&meta.key).await {
            Ok(bytes) => bytes,
            // Removed between listing and reading: fully adopted, so not a
            // writer the reader needs.
            Err(ObjectError::NotFound { .. }) => continue,
            Err(error) => return Err(ManifestError::Store(error)),
        };
        let registration: Registration =
            serde_json::from_slice(&bytes).map_err(|e| ManifestError::Corrupt {
                key: meta.key.clone(),
                source: Box::new(e),
            })?;
        registrations.push(registration);
    }
    registrations.sort_by_key(|r| r.writer);
    Ok(registrations)
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
