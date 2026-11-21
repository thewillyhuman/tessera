// SPDX-License-Identifier: Apache-2.0

//! A store for tests of what must be read together.

use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use tessera_object::{Key, MemoryStore, ObjectError, ObjectMeta, ObjectStore};

/// A store that says how many reads were in flight at once. Over a
/// bucket, reads made one after another are a round trip each.
#[derive(Debug, Default)]
pub struct Busy {
    inner: MemoryStore,
    reading: AtomicUsize,
    most: AtomicUsize,
}

impl Busy {
    /// A store over `inner` that has seen no read yet.
    pub fn over(inner: MemoryStore) -> Self {
        Busy {
            inner,
            ..Busy::default()
        }
    }

    /// The most reads that were in flight at once.
    pub fn most(&self) -> usize {
        self.most.load(Ordering::SeqCst)
    }

    /// Counts a read as in flight while the caller's turn passes to
    /// whatever else is ready to read.
    async fn read(&self) {
        let now = self.reading.fetch_add(1, Ordering::SeqCst) + 1;
        self.most.fetch_max(now, Ordering::SeqCst);
        tokio::task::yield_now().await;
        self.reading.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl ObjectStore for Busy {
    async fn get(&self, key: &Key) -> Result<Bytes, ObjectError> {
        self.read().await;
        self.inner.get(key).await
    }
    async fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Bytes, ObjectError> {
        self.read().await;
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
        self.inner.delete(key).await
    }
}
