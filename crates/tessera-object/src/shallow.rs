// SPDX-License-Identifier: Apache-2.0

//! A store for tests of what must be found without walking objects.
//!
//! What lies one level under a prefix, the datasets of a tenant or
//! the partitions of a dataset, is asked of a store by name
//! ([`ObjectStore::list_names`]). Code that lists the objects instead
//! works on a small store and costs a request per thousand objects on
//! a bucket; over this store it fails.

use std::ops::Range;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::{self, BoxStream};

use crate::{Key, MemoryStore, ObjectError, ObjectMeta, ObjectStore};

/// A store that answers for the names under a prefix and refuses to
/// list the objects under one: a bucket too large to walk.
#[derive(Debug, Default)]
pub struct ShallowStore(pub MemoryStore);

#[async_trait]
impl ObjectStore for ShallowStore {
    async fn get(&self, key: &Key) -> Result<Bytes, ObjectError> {
        self.0.get(key).await
    }
    async fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Bytes, ObjectError> {
        self.0.get_range(key, range).await
    }
    async fn get_tail(&self, key: &Key, length: u64) -> Result<Bytes, ObjectError> {
        self.0.get_tail(key, length).await
    }
    async fn head(&self, key: &Key) -> Result<ObjectMeta, ObjectError> {
        self.0.head(key).await
    }
    async fn put(&self, key: &Key, body: Bytes) -> Result<(), ObjectError> {
        self.0.put(key, body).await
    }
    async fn put_if_absent(&self, key: &Key, body: Bytes) -> Result<(), ObjectError> {
        self.0.put_if_absent(key, body).await
    }
    fn list(&self, prefix: &Key) -> BoxStream<'_, Result<ObjectMeta, ObjectError>> {
        let refused = ObjectError::Failed {
            key: prefix.clone(),
            source: "too many objects to walk".into(),
        };
        stream::once(async move { Err(refused) }).boxed()
    }
    /// What is past a key is what a reader has left, not a walk.
    fn list_after(
        &self,
        prefix: &Key,
        after: &Key,
    ) -> BoxStream<'_, Result<ObjectMeta, ObjectError>> {
        self.0.list_after(prefix, after)
    }
    async fn list_names(&self, prefix: &Key) -> Result<Vec<String>, ObjectError> {
        self.0.list_names(prefix).await
    }
    async fn delete(&self, key: &Key) -> Result<(), ObjectError> {
        self.0.delete(key).await
    }
}
