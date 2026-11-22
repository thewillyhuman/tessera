// SPDX-License-Identifier: Apache-2.0

//! The `ObjectStore` trait: the only way any crate touches durable storage
//! (spec section 18.2).

use std::fmt;
use std::ops::Range;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};

use crate::Key;

/// What a store can tell about an object without reading it.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ObjectMeta {
    /// The object's key.
    pub key: Key,
    /// The object's size in bytes.
    pub size: u64,
}

/// The source of a store failure, kept as a trait object so that each
/// implementation can attach its own client's error.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Why a store operation failed.
///
/// `Unavailable` is the only retryable variant; callers retry it with
/// backoff and treat everything else as final.
#[derive(Debug, thiserror::Error)]
pub enum ObjectError {
    /// The object does not exist.
    #[error("object {key} not found")]
    NotFound {
        /// The key that was requested.
        key: Key,
    },
    /// `put_if_absent` found the object already present (ST-3).
    #[error("object {key} already exists")]
    AlreadyExists {
        /// The key that was requested.
        key: Key,
    },
    /// A byte range lies outside the object.
    #[error("range {}..{} is outside object {key} of {size} bytes", range.start, range.end)]
    InvalidRange {
        /// The key that was requested.
        key: Key,
        /// The range that was requested.
        range: Range<u64>,
        /// The object's actual size.
        size: u64,
    },
    /// The store could not be reached or answered with a transient failure.
    /// The operation may or may not have taken effect.
    #[error("object store unavailable for {key}: {source}")]
    Unavailable {
        /// The key that was requested.
        key: Key,
        /// The underlying failure.
        #[source]
        source: BoxError,
    },
    /// The store refused or failed the operation for a reason that a retry
    /// will not fix (for example, a permission problem).
    #[error("object store failed for {key}: {source}")]
    Failed {
        /// The key that was requested.
        key: Key,
        /// The underlying failure.
        #[source]
        source: BoxError,
    },
}

impl ObjectError {
    /// Whether retrying the same operation can succeed.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(self, ObjectError::Unavailable { .. })
    }
}

/// `names` in order, each once: what [`ObjectStore::list_names`]
/// returns, whatever order a store found them in.
#[must_use]
pub fn in_order(mut names: Vec<String>) -> Vec<String> {
    names.sort_unstable();
    names.dedup();
    names
}

/// A store of immutable objects with the three properties Tessera relies on
/// (ST-3): create-if-absent writes, read-after-write consistency and
/// strongly consistent listing.
///
/// One instance serves one pool. All implementations, including the local
/// directory used in single-node mode (AR-2), pass the same conformance
/// suite.
#[async_trait]
pub trait ObjectStore: Send + Sync + fmt::Debug {
    /// Reads a whole object.
    ///
    /// # Errors
    ///
    /// `NotFound` if the object does not exist.
    async fn get(&self, key: &Key) -> Result<Bytes, ObjectError>;

    /// Reads the bytes in `range` of an object.
    ///
    /// # Errors
    ///
    /// `NotFound` if the object does not exist; `InvalidRange` if the range
    /// is not within `0..size` or is reversed.
    async fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Bytes, ObjectError>;

    /// Reads the last `length` bytes of an object, or all of it when it
    /// is shorter: what a reader asks who knows that an object ends
    /// with what it wants, and not how large the object is.
    ///
    /// As provided, the size is asked for and then the range; a store
    /// that can be asked for the end of an object in one request does
    /// that, and a store that wraps another passes the call on to it.
    ///
    /// # Errors
    ///
    /// `NotFound` if the object does not exist.
    async fn get_tail(&self, key: &Key, length: u64) -> Result<Bytes, ObjectError> {
        let size = self.head(key).await?.size;
        self.get_range(key, size.saturating_sub(length)..size).await
    }

    /// Reads an object's metadata.
    ///
    /// # Errors
    ///
    /// `NotFound` if the object does not exist.
    async fn head(&self, key: &Key) -> Result<ObjectMeta, ObjectError>;

    /// Writes an object, replacing any existing one. Used only for hints
    /// (P2), never for anything correctness depends on.
    ///
    /// # Errors
    ///
    /// `Unavailable` or `Failed` as the store reports them.
    async fn put(&self, key: &Key, body: Bytes) -> Result<(), ObjectError>;

    /// Creates an object only if none exists at `key`. Exactly one of any
    /// number of concurrent calls for the same key succeeds. This is the
    /// basis of all coordination (P2, CL-1, OP-1).
    ///
    /// # Errors
    ///
    /// `AlreadyExists` if an object is present, whoever created it.
    async fn put_if_absent(&self, key: &Key, body: Bytes) -> Result<(), ObjectError>;

    /// Lists the objects under `prefix` (component-wise, see
    /// [`Key::is_under`]) in key order. The listing reflects every write
    /// that completed before the call.
    fn list(&self, prefix: &Key) -> BoxStream<'_, Result<ObjectMeta, ObjectError>>;

    /// Lists the objects under `prefix` whose key sorts after `after`,
    /// in key order: what is left to see of a prefix gone through up
    /// to `after`, which need not exist. A store that can start a
    /// listing at a key answers in the requests the remainder takes,
    /// however much lies before it; by default the whole prefix is
    /// listed and what comes first dropped.
    ///
    /// # Errors
    ///
    /// `Unavailable` or `Failed` as the store reports them.
    fn list_after(
        &self,
        prefix: &Key,
        after: &Key,
    ) -> BoxStream<'_, Result<ObjectMeta, ObjectError>> {
        let after = after.clone();
        self.list(prefix)
            .try_filter(move |meta| std::future::ready(meta.key.as_str() > after.as_str()))
            .boxed()
    }

    /// The names directly under `prefix`: the next component of every
    /// key below it, each once, in order. It is what the listing of a
    /// directory gives, and a store that can answer it without walking
    /// every object below, as a bucket can, does so: the cost then
    /// follows the number of names, not of objects.
    ///
    /// # Errors
    ///
    /// `Unavailable` or `Failed` as the store reports them.
    async fn list_names(&self, prefix: &Key) -> Result<Vec<String>, ObjectError> {
        let mut names = Vec::new();
        let mut listing = self.list(prefix);
        while let Some(meta) = listing.try_next().await? {
            let below = meta
                .key
                .as_str()
                .strip_prefix(prefix.as_str())
                .and_then(|rest| rest.strip_prefix('/'));
            if let Some(name) = below.and_then(|rest| rest.split('/').next()) {
                names.push(name.to_owned());
            }
        }
        Ok(in_order(names))
    }

    /// Deletes an object. Deleting a missing object succeeds, so that a
    /// retried delete is harmless.
    ///
    /// # Errors
    ///
    /// `Unavailable` or `Failed` as the store reports them.
    async fn delete(&self, key: &Key) -> Result<(), ObjectError>;
}
