// SPDX-License-Identifier: Apache-2.0

//! A store for tests of what must be asked for together.

use std::future::poll_fn;
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;

use crate::{Key, MemoryStore, ObjectError, ObjectMeta, ObjectStore};

/// A store that says how many reads and deletions were in flight at
/// once. Over a bucket, those made one after another are a round trip
/// each.
#[derive(Debug, Default)]
pub struct BusyStore {
    inner: MemoryStore,
    /// The calls counted: those of keys that start with this.
    under: String,
    reading: AtomicUsize,
    most: AtomicUsize,
}

impl BusyStore {
    /// A store over `inner` that has seen no read yet.
    #[must_use]
    pub fn over(inner: MemoryStore) -> Self {
        BusyStore {
            inner,
            ..BusyStore::default()
        }
    }

    /// A store over `inner` that counts only the reads and deletions
    /// of keys that start with `under`.
    #[must_use]
    pub fn watching(inner: MemoryStore, under: &str) -> Self {
        BusyStore {
            inner,
            under: under.to_owned(),
            ..BusyStore::default()
        }
    }

    /// The most reads and deletions that were in flight at once.
    #[must_use]
    pub fn most(&self) -> usize {
        self.most.load(Ordering::SeqCst)
    }

    /// Counts a read as in flight while the caller's turn passes to
    /// whatever else is ready to read.
    async fn read(&self, key: &Key) {
        if !key.as_str().starts_with(&self.under) {
            return;
        }
        let now = self.reading.fetch_add(1, Ordering::SeqCst) + 1;
        self.most.fetch_max(now, Ordering::SeqCst);
        let mut yielded = false;
        poll_fn(|context| {
            if yielded {
                return Poll::Ready(());
            }
            yielded = true;
            context.waker().wake_by_ref();
            Poll::Pending
        })
        .await;
        self.reading.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl ObjectStore for BusyStore {
    async fn get(&self, key: &Key) -> Result<Bytes, ObjectError> {
        self.read(key).await;
        self.inner.get(key).await
    }
    async fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Bytes, ObjectError> {
        self.read(key).await;
        self.inner.get_range(key, range).await
    }
    async fn head(&self, key: &Key) -> Result<ObjectMeta, ObjectError> {
        self.inner.head(key).await
    }
    async fn put(&self, key: &Key, body: Bytes) -> Result<(), ObjectError> {
        self.inner.put(key, body).await
    }
    async fn put_if_absent(&self, key: &Key, body: Bytes) -> Result<(), ObjectError> {
        self.inner.put_if_absent(key, body).await
    }
    fn list(&self, prefix: &Key) -> BoxStream<'_, Result<ObjectMeta, ObjectError>> {
        self.inner.list(prefix)
    }
    async fn delete(&self, key: &Key) -> Result<(), ObjectError> {
        self.read(key).await;
        self.inner.delete(key).await
    }
}
