// SPDX-License-Identifier: Apache-2.0

//! A check, made of a store before it is trusted, that it has the one
//! property everything rests on (ST-3, P2): an object cannot be
//! created twice.
//!
//! A store that takes a conditional write for a plain one answers
//! every `put_if_absent` with success. Nothing fails: two nodes both
//! win the same commit, the same lease, the same log entry, and each
//! goes on from a history the other has overwritten. The check turns
//! that into a refusal to start.

use bytes::Bytes;

use crate::{Key, ObjectError, ObjectStore};

/// Why a store is not to be used.
#[derive(Debug, thiserror::Error)]
pub enum ProbeError {
    /// The store created an object where one existed.
    #[error(
        "the store let {key} be created twice: it does not refuse a write to a key that exists, which every commit, lease and log entry relies on"
    )]
    CreatedTwice {
        /// The key the check used.
        key: Key,
    },
    /// The store failed the check's own requests.
    #[error("the store could not be checked: {0}")]
    Store(#[from] ObjectError),
}

/// Checks that `store` refuses to create an object at a key that
/// holds one, by trying it at `key`, which must be the caller's alone,
/// and removes the object again.
///
/// # Errors
///
/// `CreatedTwice` if the second creation is not refused; `Store` if
/// the store fails a request of the check.
pub async fn create_if_absent(store: &dyn ObjectStore, key: &Key) -> Result<(), ProbeError> {
    match store.put_if_absent(key, Bytes::from_static(b"1")).await {
        // Present already: left by a check that was cut short.
        Ok(()) | Err(ObjectError::AlreadyExists { .. }) => {}
        Err(error) => return Err(error.into()),
    }
    let second = store.put_if_absent(key, Bytes::from_static(b"2")).await;
    // The object is the check's own whatever the outcome; one left
    // behind by a failed removal is harmless to the next check.
    let _ = store.delete(key).await;
    match second {
        Err(ObjectError::AlreadyExists { .. }) => Ok(()),
        Ok(()) => Err(ProbeError::CreatedTwice { key: key.clone() }),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use async_trait::async_trait;
    use futures::TryStreamExt;
    use futures::stream::BoxStream;

    use super::*;
    use crate::{MemoryStore, ObjectMeta};

    fn key() -> Key {
        Key::new("_cluster/probes/a").unwrap()
    }

    /// A store that takes a conditional write for a plain one, as one
    /// that ignores the condition does.
    #[derive(Debug, Default)]
    struct Unconditional(MemoryStore);

    #[async_trait]
    impl ObjectStore for Unconditional {
        async fn get(&self, key: &Key) -> Result<Bytes, ObjectError> {
            self.0.get(key).await
        }
        async fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Bytes, ObjectError> {
            self.0.get_range(key, range).await
        }
        async fn head(&self, key: &Key) -> Result<ObjectMeta, ObjectError> {
            self.0.head(key).await
        }
        async fn put(&self, key: &Key, body: Bytes) -> Result<(), ObjectError> {
            self.0.put(key, body).await
        }
        async fn put_if_absent(&self, key: &Key, body: Bytes) -> Result<(), ObjectError> {
            self.0.put(key, body).await
        }
        fn list(&self, prefix: &Key) -> BoxStream<'_, Result<ObjectMeta, ObjectError>> {
            self.0.list(prefix)
        }
        async fn delete(&self, key: &Key) -> Result<(), ObjectError> {
            self.0.delete(key).await
        }
    }

    #[tokio::test]
    async fn a_store_that_refuses_the_second_creation_passes() {
        let store = MemoryStore::new();
        create_if_absent(&store, &key()).await.unwrap();
        let left: Vec<ObjectMeta> = store.list(&key()).try_collect().await.unwrap();
        assert!(left.is_empty(), "the check removes its object");
    }

    #[tokio::test]
    async fn a_store_that_creates_twice_is_refused() {
        let error = create_if_absent(&Unconditional::default(), &key())
            .await
            .unwrap_err();
        assert!(matches!(error, ProbeError::CreatedTwice { .. }), "{error}");
    }

    #[tokio::test]
    async fn an_object_left_by_an_earlier_check_does_not_fail_the_next() {
        let store = MemoryStore::new();
        store.put(&key(), Bytes::from_static(b"1")).await.unwrap();
        create_if_absent(&store, &key()).await.unwrap();
    }
}
