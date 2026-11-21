// SPDX-License-Identifier: Apache-2.0

//! A cache of object byte ranges on local disk (spec section 10.8).
//!
//! [`CachedStore`] stands in front of a store whose every read is a
//! request to somewhere else, a bucket, and keeps on local disk what
//! was read of the objects that never change once written: data
//! files, their sidecar indexes and writer-log entries
//! ([`layout::is_immutable`]). A read asked again, the same object
//! and the same range, is served from the disk; anything else goes to
//! the store. Reads that differ in their range share nothing, which
//! is enough for the footers, column chunks and log entries that
//! queries ask for again and again.
//!
//! The cache is local and disposable: its directory is emptied when it
//! is opened, its index lives in memory, and a file that is missing or
//! short is a miss. It holds at most its capacity in bytes, dropping
//! the ranges read longest ago. An object deleted through this store
//! leaves the cache; one deleted by another node may still be answered
//! from here, with the bytes it had, to a reader that still holds a
//! reference to it.

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use tokio::fs;

use crate::{Key, ObjectError, ObjectMeta, ObjectStore, layout};

/// What was read: the bytes of `range` in the object, or the whole
/// object when the range is `None`.
type Read = (Key, Option<(u64, u64)>);

/// How much the cache was used since it was opened.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CacheStats {
    /// Reads served from the disk.
    pub hits: u64,
    /// Reads of cacheable objects that went to the store.
    pub misses: u64,
    /// The bytes the cache holds now.
    pub bytes: u64,
}

/// A kept read: the file that holds it, its size, and when it was last
/// asked for, as a tick of the cache's own counter.
struct Kept {
    file: u64,
    size: u64,
    used: u64,
}

/// The index of what the cache holds.
#[derive(Default)]
struct Index {
    kept: HashMap<Read, Kept>,
    /// The kept reads by last use, oldest first.
    by_use: BTreeMap<u64, Read>,
    bytes: u64,
    /// The next tick and the next file name.
    next: u64,
}

impl Index {
    fn tick(&mut self) -> u64 {
        self.next += 1;
        self.next
    }

    /// The file of `read` and its size, marked as just used.
    fn touch(&mut self, read: &Read) -> Option<(u64, u64)> {
        let now = self.tick();
        let kept = self.kept.get_mut(read)?;
        self.by_use.remove(&kept.used);
        kept.used = now;
        self.by_use.insert(now, read.clone());
        Some((kept.file, kept.size))
    }

    /// Forgets `read`, returning its file.
    fn remove(&mut self, read: &Read) -> Option<u64> {
        let kept = self.kept.remove(read)?;
        self.by_use.remove(&kept.used);
        self.bytes -= kept.size;
        Some(kept.file)
    }

    /// Records `read` as held in `file`, and returns the files to
    /// delete: the one it replaces and those of the reads used longest
    /// ago, until the cache is within `capacity`.
    fn insert(&mut self, read: Read, file: u64, size: u64, capacity: u64) -> Vec<u64> {
        let mut gone: Vec<u64> = self.remove(&read).into_iter().collect();
        let used = self.tick();
        self.by_use.insert(used, read.clone());
        self.kept.insert(read, Kept { file, size, used });
        self.bytes += size;
        while self.bytes > capacity {
            let Some(oldest) = self.by_use.values().next().cloned() else {
                break;
            };
            gone.extend(self.remove(&oldest));
        }
        gone
    }
}

/// A store with a cache of its immutable objects' reads on local disk.
pub struct CachedStore {
    inner: Arc<dyn ObjectStore>,
    dir: PathBuf,
    capacity: u64,
    index: Mutex<Index>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl std::fmt::Debug for CachedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedStore")
            .field("inner", &self.inner)
            .field("dir", &self.dir)
            .field("capacity", &self.capacity)
            .finish_non_exhaustive()
    }
}

impl CachedStore {
    /// A cache of `inner` in `dir`, holding at most `capacity` bytes.
    /// The directory is created if needed and emptied of whatever it
    /// holds, what an earlier run left: it must be the cache's alone.
    ///
    /// # Errors
    ///
    /// If the directory cannot be emptied or created.
    pub async fn open(
        inner: Arc<dyn ObjectStore>,
        dir: impl Into<PathBuf>,
        capacity: u64,
    ) -> io::Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir).await?;
        // Emptied rather than removed and made again: the directory
        // may be a volume's mount point.
        let mut left = fs::read_dir(&dir).await?;
        while let Some(entry) = left.next_entry().await? {
            if entry.file_type().await?.is_dir() {
                fs::remove_dir_all(entry.path()).await?;
            } else {
                fs::remove_file(entry.path()).await?;
            }
        }
        Ok(CachedStore {
            inner,
            dir,
            capacity,
            index: Mutex::new(Index::default()),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        })
    }

    /// How much the cache was used and what it holds.
    #[must_use]
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            bytes: self.index().bytes,
        }
    }

    fn index(&self) -> std::sync::MutexGuard<'_, Index> {
        // The index is never left half-updated by a panic: every
        // change is made under one lock, with no call out of it.
        self.index.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn path(&self, file: u64) -> PathBuf {
        self.dir.join(file.to_string())
    }

    /// The bytes kept for `read`, if they are there and whole.
    async fn kept(&self, read: &Read) -> Option<Bytes> {
        let (file, size) = self.index().touch(read)?;
        match fs::read(self.path(file)).await {
            Ok(bytes) if bytes.len() as u64 == size => Some(bytes.into()),
            // Evicted since, or changed from under the cache.
            _ => {
                self.index().remove(read);
                None
            }
        }
    }

    /// Keeps `bytes` as what `read` returns. A failure to write is not
    /// the reader's: the read succeeded, and the next one asks again.
    async fn keep(&self, read: Read, bytes: &Bytes) {
        let size = bytes.len() as u64;
        if size == 0 || size > self.capacity {
            return;
        }
        let file = self.index().tick();
        // Written under a name of its own and only then recorded, so
        // that no reader finds a file still being written.
        if let Err(error) = fs::write(self.path(file), bytes).await {
            tracing::warn!(dir = %self.dir.display(), error = %error, "cannot write to the object cache");
            return;
        }
        let gone = self.index().insert(read, file, size, self.capacity);
        for file in gone {
            let _ = fs::remove_file(self.path(file)).await;
        }
    }

    /// Serves `read` from the disk, or from the store through `fetch`
    /// and keeps it.
    async fn through<F>(&self, read: Read, fetch: F) -> Result<Bytes, ObjectError>
    where
        F: Future<Output = Result<Bytes, ObjectError>>,
    {
        if !layout::is_immutable(&read.0) {
            return fetch.await;
        }
        if let Some(bytes) = self.kept(&read).await {
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(bytes);
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        let bytes = fetch.await?;
        self.keep(read, &bytes).await;
        Ok(bytes)
    }

    /// Drops every read kept of `key`.
    async fn forget(&self, key: &Key) {
        let files: Vec<u64> = {
            let mut index = self.index();
            let reads: Vec<Read> = index
                .kept
                .keys()
                .filter(|(kept, _)| kept == key)
                .cloned()
                .collect();
            reads.iter().filter_map(|read| index.remove(read)).collect()
        };
        for file in files {
            let _ = fs::remove_file(self.path(file)).await;
        }
    }
}

#[async_trait]
impl ObjectStore for CachedStore {
    async fn get(&self, key: &Key) -> Result<Bytes, ObjectError> {
        self.through((key.clone(), None), self.inner.get(key)).await
    }

    async fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Bytes, ObjectError> {
        let read = (key.clone(), Some((range.start, range.end)));
        self.through(read, self.inner.get_range(key, range)).await
    }

    async fn head(&self, key: &Key) -> Result<ObjectMeta, ObjectError> {
        self.inner.head(key).await
    }

    async fn put(&self, key: &Key, body: Bytes) -> Result<(), ObjectError> {
        // A sidecar is written with `put`; whatever was kept of an
        // object under the key is no longer what the store holds.
        let put = self.inner.put(key, body).await;
        self.forget(key).await;
        put
    }

    async fn put_if_absent(&self, key: &Key, body: Bytes) -> Result<(), ObjectError> {
        self.inner.put_if_absent(key, body).await
    }

    fn list(&self, prefix: &Key) -> BoxStream<'_, Result<ObjectMeta, ObjectError>> {
        self.inner.list(prefix)
    }

    async fn list_names(&self, prefix: &Key) -> Result<Vec<String>, ObjectError> {
        self.inner.list_names(prefix).await
    }

    async fn delete(&self, key: &Key) -> Result<(), ObjectError> {
        let deleted = self.inner.delete(key).await;
        self.forget(key).await;
        deleted
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;
    use crate::{MemoryStore, conformance};

    /// A store that counts the reads that reach it.
    #[derive(Debug, Default)]
    struct Counting {
        inner: MemoryStore,
        reads: AtomicUsize,
    }

    impl Counting {
        fn reads(&self) -> usize {
            self.reads.load(Ordering::Relaxed)
        }
    }

    #[async_trait]
    impl ObjectStore for Counting {
        async fn get(&self, key: &Key) -> Result<Bytes, ObjectError> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            self.inner.get(key).await
        }
        async fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Bytes, ObjectError> {
            self.reads.fetch_add(1, Ordering::Relaxed);
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

    struct Fixture {
        store: Arc<Counting>,
        cache: CachedStore,
        dir: tempfile::TempDir,
    }

    async fn fixture(capacity: u64) -> Fixture {
        let store = Arc::new(Counting::default());
        let dir = tempfile::tempdir().unwrap();
        let inner: Arc<dyn ObjectStore> = store.clone();
        let cache = CachedStore::open(inner, dir.path().join("cache"), capacity)
            .await
            .unwrap();
        Fixture { store, cache, dir }
    }

    fn file(name: &str) -> Key {
        Key::new(format!("acme/prod/p/log/0/d/ab/{name}.parquet")).unwrap()
    }

    #[tokio::test]
    async fn conforms_with_a_cache_in_front() {
        let dir = tempfile::tempdir().unwrap();
        let cache = CachedStore::open(Arc::new(MemoryStore::new()), dir.path(), 1 << 20)
            .await
            .unwrap();
        conformance::run_all(Arc::new(cache)).await;
    }

    #[tokio::test]
    async fn a_range_asked_again_is_read_from_the_disk() {
        let f = fixture(1 << 20).await;
        let key = file("a");
        f.cache
            .put_if_absent(&key, Bytes::from_static(b"0123456789"))
            .await
            .unwrap();

        let first = f.cache.get_range(&key, 2..6).await.unwrap();
        let again = f.cache.get_range(&key, 2..6).await.unwrap();
        assert_eq!((first, again.clone()), (Bytes::from_static(b"2345"), again));
        assert_eq!(f.store.reads(), 1, "the second read stayed here");
        let whole = f.cache.get(&key).await.unwrap();
        assert_eq!(whole, f.cache.get(&key).await.unwrap());
        assert_eq!(f.store.reads(), 2, "the whole object is a read of its own");
        assert_eq!(
            f.cache.stats(),
            CacheStats {
                hits: 2,
                misses: 2,
                bytes: 14
            }
        );
    }

    #[tokio::test]
    async fn an_object_that_may_change_is_always_read_from_the_store() {
        let f = fixture(1 << 20).await;
        let hint = Key::new("_cluster/catalog/_hint").unwrap();
        f.cache.put(&hint, Bytes::from_static(b"1")).await.unwrap();
        assert_eq!(f.cache.get(&hint).await.unwrap(), Bytes::from_static(b"1"));
        f.store
            .inner
            .put(&hint, Bytes::from_static(b"2"))
            .await
            .unwrap();
        assert_eq!(f.cache.get(&hint).await.unwrap(), Bytes::from_static(b"2"));
        assert_eq!(f.cache.stats(), CacheStats::default());
    }

    #[tokio::test]
    async fn the_ranges_read_longest_ago_make_room() {
        let f = fixture(10).await;
        let key = file("a");
        f.cache
            .put_if_absent(&key, Bytes::from_static(b"0123456789ab"))
            .await
            .unwrap();
        f.cache.get_range(&key, 0..4).await.unwrap();
        f.cache.get_range(&key, 4..8).await.unwrap();
        f.cache.get_range(&key, 0..4).await.unwrap();
        // Twelve bytes do not fit in ten: the range used longest ago goes.
        f.cache.get_range(&key, 8..12).await.unwrap();
        assert_eq!(f.cache.stats().bytes, 8);
        let reads = f.store.reads();
        f.cache.get_range(&key, 0..4).await.unwrap();
        f.cache.get_range(&key, 8..12).await.unwrap();
        assert_eq!(f.store.reads(), reads, "the two newest are kept");
        f.cache.get_range(&key, 4..8).await.unwrap();
        assert_eq!(f.store.reads(), reads + 1, "the oldest was dropped");
        let files = std::fs::read_dir(f.dir.path().join("cache"))
            .unwrap()
            .count();
        assert_eq!(files, 2, "and its file with it");
    }

    #[tokio::test]
    async fn a_read_larger_than_the_cache_is_not_kept() {
        let f = fixture(4).await;
        let key = file("a");
        f.cache
            .put_if_absent(&key, Bytes::from_static(b"0123456789"))
            .await
            .unwrap();
        f.cache.get(&key).await.unwrap();
        f.cache.get(&key).await.unwrap();
        assert_eq!(f.store.reads(), 2);
        assert_eq!(f.cache.stats().bytes, 0);
    }

    #[tokio::test]
    async fn a_deleted_object_leaves_the_cache() {
        let f = fixture(1 << 20).await;
        let key = file("a");
        f.cache
            .put_if_absent(&key, Bytes::from_static(b"0123"))
            .await
            .unwrap();
        f.cache.get(&key).await.unwrap();
        f.cache.delete(&key).await.unwrap();
        let error = f.cache.get(&key).await.unwrap_err();
        assert!(matches!(error, ObjectError::NotFound { .. }), "{error}");
        assert_eq!(f.cache.stats().bytes, 0);
    }

    #[tokio::test]
    async fn a_cache_file_that_is_gone_is_a_miss() {
        let f = fixture(1 << 20).await;
        let key = file("a");
        f.cache
            .put_if_absent(&key, Bytes::from_static(b"0123"))
            .await
            .unwrap();
        f.cache.get(&key).await.unwrap();
        for entry in std::fs::read_dir(f.dir.path().join("cache")).unwrap() {
            std::fs::remove_file(entry.unwrap().path()).unwrap();
        }
        assert_eq!(
            f.cache.get(&key).await.unwrap(),
            Bytes::from_static(b"0123")
        );
        assert_eq!(f.store.reads(), 2);
    }

    #[tokio::test]
    async fn what_an_earlier_run_left_is_removed() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("7"), b"stale").unwrap();
        std::fs::create_dir(dir.path().join("lost+found")).unwrap();
        let inner: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
        CachedStore::open(inner, dir.path(), 1 << 20).await.unwrap();
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    /// The directory may be a volume's mount point, which can be
    /// emptied but not removed.
    #[cfg(unix)]
    #[tokio::test]
    async fn the_directory_itself_is_kept() {
        use std::os::unix::fs::MetadataExt;

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("7"), b"stale").unwrap();
        let before = std::fs::metadata(dir.path()).unwrap().ino();
        let inner: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
        CachedStore::open(inner, dir.path(), 1 << 20).await.unwrap();
        assert_eq!(std::fs::metadata(dir.path()).unwrap().ino(), before);
    }
}
