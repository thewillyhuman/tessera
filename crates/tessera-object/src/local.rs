// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! An object store on a local directory, used by single-node mode (AR-2).
//!
//! Each key is a relative path under the root. Writes go to a private
//! `.tmp` directory first and are then moved into place, so a reader never
//! sees a partial object: `put` renames over the destination, and
//! `put_if_absent` hard-links into place, which the filesystem refuses
//! atomically when the destination exists. Keys never start a component
//! with `.`, so `.tmp` cannot collide with an object.
//!
//! Limits: the filesystem must support hard links, and a key cannot be both
//! an object and a prefix of other objects (`a` and `a/b`), which Tessera's
//! key layout never requires.

use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::{Key, ObjectError, ObjectMeta, ObjectStore};

const TEMP_DIR: &str = ".tmp";

/// Numbers the staging files of every store in this process, so that two
/// stores on one directory never stage under the same name.
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// An object store rooted at a local directory.
#[derive(Debug)]
pub struct LocalStore {
    root: PathBuf,
}

impl LocalStore {
    /// Opens (creating if needed) the store at `root`.
    ///
    /// # Errors
    ///
    /// If the directory or its private temporary directory cannot be created.
    pub async fn open(root: impl Into<PathBuf>) -> io::Result<Self> {
        let root = root.into();
        fs::create_dir_all(root.join(TEMP_DIR)).await?;
        Ok(LocalStore { root })
    }

    fn path_for(&self, key: &Key) -> PathBuf {
        self.root.join(key.as_str())
    }

    /// A fresh path in the temporary directory, unique across the processes
    /// and the stores in this process that may share the directory.
    fn temp_path(&self) -> PathBuf {
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        self.root
            .join(TEMP_DIR)
            .join(format!("{}-{counter}", std::process::id()))
    }

    /// Writes `body` to a temporary file and returns its path.
    async fn stage(&self, key: &Key, body: &Bytes) -> Result<PathBuf, ObjectError> {
        let temp = self.temp_path();
        fs::write(&temp, body).await.map_err(|e| io_error(key, e))?;
        Ok(temp)
    }

    async fn create_parent(&self, key: &Key, path: &Path) -> Result<(), ObjectError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| io_error(key, e))?;
        }
        Ok(())
    }

    /// Removes directories left empty above `path`, up to the root.
    async fn prune_empty_parents(&self, path: &Path) {
        let mut current = path.parent();
        while let Some(dir) = current {
            if dir == self.root || fs::remove_dir(dir).await.is_err() {
                break;
            }
            current = dir.parent();
        }
    }

    /// Opens `key` for reading, reporting a directory (a prefix of other
    /// keys) as not found.
    async fn open_file(&self, key: &Key) -> Result<(fs::File, u64), ObjectError> {
        let file = fs::File::open(self.path_for(key))
            .await
            .map_err(|e| io_error(key, e))?;
        let metadata = file.metadata().await.map_err(|e| io_error(key, e))?;
        if !metadata.is_file() {
            return Err(ObjectError::NotFound { key: key.clone() });
        }
        Ok((file, metadata.len()))
    }

    /// Every object under `prefix`, in key order.
    async fn collect(&self, prefix: &Key) -> Result<Vec<ObjectMeta>, ObjectError> {
        let start = self.path_for(prefix);
        let mut found = Vec::new();
        let mut pending = vec![start];
        while let Some(path) = pending.pop() {
            let metadata = match fs::metadata(&path).await {
                Ok(metadata) => metadata,
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(io_error(prefix, e)),
            };
            if metadata.is_file() {
                if let Some(key) = self.key_for(&path) {
                    found.push(ObjectMeta {
                        key,
                        size: metadata.len(),
                    });
                }
                continue;
            }
            let mut entries = fs::read_dir(&path).await.map_err(|e| io_error(prefix, e))?;
            while let Some(entry) = entries
                .next_entry()
                .await
                .map_err(|e| io_error(prefix, e))?
            {
                if entry.file_name().to_string_lossy().starts_with('.') {
                    continue; // implementation-private, never a key component
                }
                pending.push(entry.path());
            }
        }
        found.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(found)
    }

    /// The key whose path is `path`, if it is a well-formed one.
    fn key_for(&self, path: &Path) -> Option<Key> {
        let relative = path.strip_prefix(&self.root).ok()?;
        let components: Vec<&str> = relative
            .components()
            .map(|c| c.as_os_str().to_str())
            .collect::<Option<_>>()?;
        Key::new(components.join("/")).ok()
    }
}

/// Maps an I/O error to the store error that describes it.
fn io_error(key: &Key, error: io::Error) -> ObjectError {
    let key = key.clone();
    match error.kind() {
        io::ErrorKind::NotFound => ObjectError::NotFound { key },
        io::ErrorKind::AlreadyExists => ObjectError::AlreadyExists { key },
        io::ErrorKind::Interrupted | io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock => {
            ObjectError::Unavailable {
                key,
                source: Box::new(error),
            }
        }
        _ => ObjectError::Failed {
            key,
            source: Box::new(error),
        },
    }
}

#[async_trait]
impl ObjectStore for LocalStore {
    async fn get(&self, key: &Key) -> Result<Bytes, ObjectError> {
        let (mut file, size) = self.open_file(key).await?;
        let mut body = Vec::with_capacity(usize::try_from(size).unwrap_or(0));
        file.read_to_end(&mut body)
            .await
            .map_err(|e| io_error(key, e))?;
        Ok(Bytes::from(body))
    }

    async fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Bytes, ObjectError> {
        let (mut file, size) = self.open_file(key).await?;
        if range.start > range.end || range.end > size {
            return Err(ObjectError::InvalidRange {
                key: key.clone(),
                range,
                size,
            });
        }
        let length =
            usize::try_from(range.end - range.start).map_err(|_| ObjectError::InvalidRange {
                key: key.clone(),
                range: range.clone(),
                size,
            })?;
        file.seek(io::SeekFrom::Start(range.start))
            .await
            .map_err(|e| io_error(key, e))?;
        let mut body = vec![0; length];
        file.read_exact(&mut body)
            .await
            .map_err(|e| io_error(key, e))?;
        Ok(Bytes::from(body))
    }

    async fn head(&self, key: &Key) -> Result<ObjectMeta, ObjectError> {
        let (_, size) = self.open_file(key).await?;
        Ok(ObjectMeta {
            key: key.clone(),
            size,
        })
    }

    async fn put(&self, key: &Key, body: Bytes) -> Result<(), ObjectError> {
        let destination = self.path_for(key);
        self.create_parent(key, &destination).await?;
        let temp = self.stage(key, &body).await?;
        fs::rename(&temp, &destination)
            .await
            .map_err(|e| io_error(key, e))
    }

    async fn put_if_absent(&self, key: &Key, body: Bytes) -> Result<(), ObjectError> {
        let destination = self.path_for(key);
        self.create_parent(key, &destination).await?;
        let temp = self.stage(key, &body).await?;
        // A hard link is created atomically and refused if the destination
        // exists, which is exactly create-if-absent.
        let linked = fs::hard_link(&temp, &destination)
            .await
            .map_err(|e| io_error(key, e));
        // The staged file is no longer needed whether or not we won; a
        // failure to remove it only leaves garbage, so it is not reported.
        let _ = fs::remove_file(&temp).await;
        linked
    }

    fn list(&self, prefix: &Key) -> BoxStream<'_, Result<ObjectMeta, ObjectError>> {
        let prefix = prefix.clone();
        Box::pin(
            stream::once(async move { self.collect(&prefix).await }).flat_map(
                |result| match result {
                    Ok(found) => stream::iter(found.into_iter().map(Ok)).left_stream(),
                    Err(e) => stream::once(async move { Err(e) }).right_stream(),
                },
            ),
        )
    }

    async fn list_names(&self, prefix: &Key) -> Result<Vec<String>, ObjectError> {
        let mut entries = match fs::read_dir(self.path_for(prefix)).await {
            Ok(entries) => entries,
            // Nothing under the prefix, or the prefix is an object.
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
                ) =>
            {
                return Ok(Vec::new());
            }
            Err(e) => return Err(io_error(prefix, e)),
        };
        let mut names = Vec::new();
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| io_error(prefix, e))?
        {
            let name = entry.file_name().to_string_lossy().into_owned();
            if !name.starts_with('.') {
                names.push(name);
            }
        }
        Ok(crate::store::in_order(names))
    }

    async fn delete(&self, key: &Key) -> Result<(), ObjectError> {
        let path = self.path_for(key);
        match fs::remove_file(&path).await {
            Ok(()) => {
                self.prune_empty_parents(&path).await;
                Ok(())
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(io_error(key, e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use futures::TryStreamExt;

    use super::*;
    use crate::conformance;

    fn key(text: &str) -> Key {
        Key::new(text).unwrap()
    }

    #[tokio::test]
    async fn two_stores_on_one_directory_never_stage_under_one_name() {
        let dir = tempfile::tempdir().unwrap();
        let a = LocalStore::open(dir.path()).await.unwrap();
        let b = LocalStore::open(dir.path()).await.unwrap();
        let paths: std::collections::BTreeSet<PathBuf> = (0..100)
            .flat_map(|_| [a.temp_path(), b.temp_path()])
            .collect();
        assert_eq!(paths.len(), 200);
    }

    #[tokio::test]
    async fn passes_the_conformance_suite() {
        let dir = tempfile::tempdir().unwrap();
        conformance::run_all(Arc::new(LocalStore::open(dir.path()).await.unwrap())).await;
    }

    #[tokio::test]
    async fn objects_survive_reopening_the_directory() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalStore::open(dir.path()).await.unwrap();
        store
            .put(&key("a/b"), Bytes::from_static(b"durable"))
            .await
            .unwrap();
        drop(store);
        let reopened = LocalStore::open(dir.path()).await.unwrap();
        assert_eq!(
            reopened.get(&key("a/b")).await.unwrap(),
            Bytes::from_static(b"durable")
        );
    }

    #[tokio::test]
    async fn two_handles_on_one_directory_see_each_others_writes() {
        let dir = tempfile::tempdir().unwrap();
        let first = LocalStore::open(dir.path()).await.unwrap();
        let second = LocalStore::open(dir.path()).await.unwrap();
        first
            .put_if_absent(&key("log/1"), Bytes::from_static(b"x"))
            .await
            .unwrap();
        let error = second
            .put_if_absent(&key("log/1"), Bytes::from_static(b"y"))
            .await
            .unwrap_err();
        assert!(matches!(error, ObjectError::AlreadyExists { .. }));
    }

    #[tokio::test]
    async fn leaves_no_temporary_files_behind() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalStore::open(dir.path()).await.unwrap();
        store
            .put(&key("a"), Bytes::from_static(b"x"))
            .await
            .unwrap();
        store
            .put_if_absent(&key("b"), Bytes::from_static(b"x"))
            .await
            .unwrap();
        store
            .put_if_absent(&key("b"), Bytes::from_static(b"x"))
            .await
            .unwrap_err();
        let leftovers = std::fs::read_dir(dir.path().join(".tmp")).unwrap().count();
        assert_eq!(leftovers, 0);
    }

    #[tokio::test]
    async fn a_prefix_is_not_an_object() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalStore::open(dir.path()).await.unwrap();
        store
            .put(&key("a/b"), Bytes::from_static(b"x"))
            .await
            .unwrap();
        assert!(matches!(
            store.get(&key("a")).await.unwrap_err(),
            ObjectError::NotFound { .. }
        ));
        assert!(matches!(
            store.head(&key("a")).await.unwrap_err(),
            ObjectError::NotFound { .. }
        ));
    }

    #[tokio::test]
    async fn delete_removes_empty_parent_directories() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalStore::open(dir.path()).await.unwrap();
        store
            .put(&key("a/b/c"), Bytes::from_static(b"x"))
            .await
            .unwrap();
        store.delete(&key("a/b/c")).await.unwrap();
        assert!(!dir.path().join("a").exists());
        let listed: Vec<Key> = store
            .list(&key("a"))
            .map_ok(|m| m.key)
            .try_collect()
            .await
            .unwrap();
        assert!(listed.is_empty());
    }
}
