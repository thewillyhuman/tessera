// SPDX-License-Identifier: Apache-2.0

//! Counting what a node asks of a store.
//!
//! A bucket bills and throttles by the request, and what a node asks
//! of its store when nothing is written or read is the part of its
//! cost nobody sees. [`MeteredStore`] sits directly on the store,
//! under the cache, and counts every call that reaches it by
//! operation; the node exposes the counts as a metric.

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;

use crate::Key;
use crate::store::{ObjectError, ObjectMeta, ObjectStore};

/// The operations of a store, in the order they are reported.
#[derive(Clone, Copy, Debug)]
enum Operation {
    Get,
    GetRange,
    Head,
    Put,
    PutIfAbsent,
    List,
    ListAfter,
    ListNames,
    Delete,
}

impl Operation {
    const ALL: [Operation; 9] = [
        Operation::Get,
        Operation::GetRange,
        Operation::Head,
        Operation::Put,
        Operation::PutIfAbsent,
        Operation::List,
        Operation::ListAfter,
        Operation::ListNames,
        Operation::Delete,
    ];

    fn name(self) -> &'static str {
        match self {
            Operation::Get => "get",
            Operation::GetRange => "get_range",
            Operation::Head => "head",
            Operation::Put => "put",
            Operation::PutIfAbsent => "put_if_absent",
            Operation::List => "list",
            Operation::ListAfter => "list_after",
            Operation::ListNames => "list_names",
            Operation::Delete => "delete",
        }
    }
}

/// How many calls of each operation reached a store.
#[derive(Debug, Default)]
pub struct StoreRequests {
    calls: [AtomicU64; Operation::ALL.len()],
}

impl StoreRequests {
    fn count(&self, operation: Operation) {
        self.calls[operation as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// The calls so far, by operation, always in the same order. A
    /// listing counts once, however many pages the store answers in.
    #[must_use]
    pub fn by_operation(&self) -> Vec<(&'static str, u64)> {
        Operation::ALL
            .into_iter()
            .map(|operation| {
                let calls = self.calls[operation as usize].load(Ordering::Relaxed);
                (operation.name(), calls)
            })
            .collect()
    }
}

/// A store that counts the calls made to it.
#[derive(Debug)]
pub struct MeteredStore {
    inner: Arc<dyn ObjectStore>,
    requests: Arc<StoreRequests>,
}

impl MeteredStore {
    /// `inner`, with its calls counted.
    #[must_use]
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        MeteredStore {
            inner,
            requests: Arc::default(),
        }
    }

    /// The counts, shared with whatever reports them.
    #[must_use]
    pub fn requests(&self) -> Arc<StoreRequests> {
        Arc::clone(&self.requests)
    }
}

#[async_trait]
impl ObjectStore for MeteredStore {
    async fn get(&self, key: &Key) -> Result<Bytes, ObjectError> {
        self.requests.count(Operation::Get);
        self.inner.get(key).await
    }

    async fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Bytes, ObjectError> {
        self.requests.count(Operation::GetRange);
        self.inner.get_range(key, range).await
    }

    async fn head(&self, key: &Key) -> Result<ObjectMeta, ObjectError> {
        self.requests.count(Operation::Head);
        self.inner.head(key).await
    }

    async fn put(&self, key: &Key, body: Bytes) -> Result<(), ObjectError> {
        self.requests.count(Operation::Put);
        self.inner.put(key, body).await
    }

    async fn put_if_absent(&self, key: &Key, body: Bytes) -> Result<(), ObjectError> {
        self.requests.count(Operation::PutIfAbsent);
        self.inner.put_if_absent(key, body).await
    }

    fn list(&self, prefix: &Key) -> BoxStream<'_, Result<ObjectMeta, ObjectError>> {
        self.requests.count(Operation::List);
        self.inner.list(prefix)
    }

    fn list_after(
        &self,
        prefix: &Key,
        after: &Key,
    ) -> BoxStream<'_, Result<ObjectMeta, ObjectError>> {
        self.requests.count(Operation::ListAfter);
        self.inner.list_after(prefix, after)
    }

    async fn list_names(&self, prefix: &Key) -> Result<Vec<String>, ObjectError> {
        self.requests.count(Operation::ListNames);
        self.inner.list_names(prefix).await
    }

    async fn delete(&self, key: &Key) -> Result<(), ObjectError> {
        self.requests.count(Operation::Delete);
        self.inner.delete(key).await
    }
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;

    use super::*;
    use crate::{MemoryStore, conformance};

    #[tokio::test]
    async fn conforms_while_it_counts() {
        conformance::run_all(Arc::new(MeteredStore::new(Arc::new(MemoryStore::new())))).await;
    }

    #[tokio::test]
    async fn every_call_is_counted_under_its_operation() {
        let store = MeteredStore::new(Arc::new(MemoryStore::new()));
        let requests = store.requests();
        let key = Key::new("a/1").unwrap();
        let prefix = Key::new("a").unwrap();
        store.put(&key, Bytes::from_static(b"xy")).await.unwrap();
        store.put_if_absent(&key, Bytes::new()).await.unwrap_err();
        store.get(&key).await.unwrap();
        store.get(&key).await.unwrap();
        store.get_range(&key, 0..1).await.unwrap();
        store.head(&key).await.unwrap();
        let _: Vec<ObjectMeta> = store.list(&prefix).try_collect().await.unwrap();
        let _: Vec<ObjectMeta> = store.list_after(&prefix, &key).try_collect().await.unwrap();
        store.list_names(&prefix).await.unwrap();
        store.delete(&key).await.unwrap();
        assert_eq!(
            requests.by_operation(),
            [
                ("get", 2),
                ("get_range", 1),
                ("head", 1),
                ("put", 1),
                ("put_if_absent", 1),
                ("list", 1),
                ("list_after", 1),
                ("list_names", 1),
                ("delete", 1),
            ],
            "a call that fails reached the store too"
        );
    }
}
