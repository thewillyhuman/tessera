// SPDX-License-Identifier: Apache-2.0

//! An in-memory object store for tests and simulation.

use std::collections::BTreeMap;
use std::fmt;
use std::ops::Range;
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream};

use crate::{Key, ObjectError, ObjectMeta, ObjectStore};

/// An object store held in process memory.
///
/// Clones share the same objects, so a test can hand the store to a
/// component and still inspect it. All operations are atomic under one
/// lock, which gives the consistency properties of ST-3 trivially.
#[derive(Clone, Default)]
pub struct MemoryStore {
    objects: Arc<Mutex<BTreeMap<Key, Bytes>>>,
}

impl MemoryStore {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn with_objects<T>(&self, f: impl FnOnce(&mut BTreeMap<Key, Bytes>) -> T) -> T {
        // A poisoned lock means a panic while holding it; the map itself is
        // never left half-updated, so it is safe to keep using.
        let mut objects = self.objects.lock().unwrap_or_else(PoisonError::into_inner);
        f(&mut objects)
    }
}

impl fmt::Debug for MemoryStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self.with_objects(|objects| objects.len());
        f.debug_struct("MemoryStore")
            .field("objects", &count)
            .finish()
    }
}

#[async_trait]
impl ObjectStore for MemoryStore {
    async fn get(&self, key: &Key) -> Result<Bytes, ObjectError> {
        self.with_objects(|objects| objects.get(key).cloned())
            .ok_or_else(|| ObjectError::NotFound { key: key.clone() })
    }

    async fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Bytes, ObjectError> {
        let body = self.get(key).await?;
        slice(key, &body, range)
    }

    async fn head(&self, key: &Key) -> Result<ObjectMeta, ObjectError> {
        let body = self.get(key).await?;
        Ok(ObjectMeta {
            key: key.clone(),
            size: body.len() as u64,
        })
    }

    async fn put(&self, key: &Key, body: Bytes) -> Result<(), ObjectError> {
        self.with_objects(|objects| {
            objects.insert(key.clone(), body);
        });
        Ok(())
    }

    async fn put_if_absent(&self, key: &Key, body: Bytes) -> Result<(), ObjectError> {
        self.with_objects(|objects| {
            if objects.contains_key(key) {
                return Err(ObjectError::AlreadyExists { key: key.clone() });
            }
            objects.insert(key.clone(), body);
            Ok(())
        })
    }

    fn list(&self, prefix: &Key) -> BoxStream<'_, Result<ObjectMeta, ObjectError>> {
        let listed: Vec<ObjectMeta> = self.with_objects(|objects| {
            objects
                .range(prefix.clone()..)
                .take_while(|(key, _)| key.as_str().starts_with(prefix.as_str()))
                .filter(|(key, _)| key.is_under(prefix))
                .map(|(key, body)| ObjectMeta {
                    key: key.clone(),
                    size: body.len() as u64,
                })
                .collect()
        });
        Box::pin(stream::iter(listed.into_iter().map(Ok)))
    }

    async fn delete(&self, key: &Key) -> Result<(), ObjectError> {
        self.with_objects(|objects| {
            objects.remove(key);
        });
        Ok(())
    }
}

/// `body[range]`, with the range checked against the object size as the
/// trait requires.
pub(crate) fn slice(key: &Key, body: &Bytes, range: Range<u64>) -> Result<Bytes, ObjectError> {
    let size = body.len() as u64;
    if range.start > range.end || range.end > size {
        return Err(ObjectError::InvalidRange {
            key: key.clone(),
            range,
            size,
        });
    }
    let start = usize::try_from(range.start).map_err(|_| ObjectError::InvalidRange {
        key: key.clone(),
        range: range.clone(),
        size,
    })?;
    let end = usize::try_from(range.end).map_err(|_| ObjectError::InvalidRange {
        key: key.clone(),
        range: range.clone(),
        size,
    })?;
    Ok(body.slice(start..end))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;

    use super::*;
    use crate::conformance;

    #[tokio::test]
    async fn passes_the_conformance_suite() {
        conformance::run_all(Arc::new(MemoryStore::new())).await;
    }

    #[tokio::test]
    async fn clones_share_the_same_objects() {
        let store = MemoryStore::new();
        let clone = store.clone();
        store
            .put(&Key::new("a").unwrap(), Bytes::from_static(b"x"))
            .await
            .unwrap();
        assert_eq!(
            clone.get(&Key::new("a").unwrap()).await.unwrap(),
            Bytes::from_static(b"x")
        );
    }
}
