// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! An object store in an S3-compatible bucket, or under a prefix of
//! one (ST-1, G2), through the `object_store` crate's client (spec
//! section 18.3).
//!
//! The bucket must give what ST-3 asks: `put_if_absent` is a `PUT`
//! with `If-None-Match: *`, which Amazon S3 and the stores that follow
//! it (`MinIO`, Ceph, R2) refuse atomically when the key exists, and
//! listings must come in key order and reflect every completed write,
//! as a general purpose bucket's do. Directory buckets, which list in
//! no order, are not supported.
//!
//! The client retries what the store answers with a server error or
//! not at all, within [`RETRY_BUDGET`], each request under the client's
//! own timeout; what still fails then is `Unavailable`, and a caller
//! decides whether to try again. A retried `put_if_absent` whose first
//! answer was lost reports `AlreadyExists` for the object it created
//! itself; callers tell by reading the object back, as they do for any
//! lost answer (P2).

use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::prefix::PrefixStore;
use object_store::{
    Error, GetOptions, GetRange, ObjectStore as Client, ObjectStoreExt, PutMode, PutOptions,
    RetryConfig,
};

use crate::{Key, ObjectError, ObjectMeta, ObjectStore};

/// How many times the client tries a request again before the failure
/// is the caller's.
const MAX_RETRIES: usize = 4;
/// How long the client keeps retrying one request: short enough that a
/// write waiting on a store that is down is refused as retryable
/// rather than held (section 14.4).
const RETRY_BUDGET: Duration = Duration::from_secs(30);

/// Why a remote store could not be opened.
#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    /// The URL is not `s3://<bucket>` or `s3://<bucket>/<prefix>` with
    /// a prefix made of valid key components.
    #[error("{url:?} is not an s3://<bucket>[/<prefix>] URL")]
    InvalidUrl {
        /// The URL as given.
        url: String,
    },
    /// The client could not be configured, for example for want of a
    /// region.
    #[error("cannot open {url}: {source}")]
    Client {
        /// The URL as given.
        url: String,
        /// The client's reason.
        #[source]
        source: Error,
    },
}

/// An object store behind an `object_store` client.
#[derive(Debug)]
pub struct RemoteStore {
    client: Arc<dyn Client>,
}

impl RemoteStore {
    /// A store over `client`, every key at the client's root.
    #[must_use]
    pub fn new(client: Arc<dyn Client>) -> Self {
        RemoteStore { client }
    }

    /// Opens the bucket, or the prefix in it, that `url` names:
    /// `s3://<bucket>` or `s3://<bucket>/<prefix>`.
    ///
    /// Everything else comes from the environment, as the AWS tools
    /// read it: `AWS_REGION`, `AWS_ACCESS_KEY_ID` and
    /// `AWS_SECRET_ACCESS_KEY` or the credentials of a web identity, a
    /// container or the instance, and for a store other than Amazon's
    /// `AWS_ENDPOINT_URL`, with `AWS_ALLOW_HTTP=true` when it speaks
    /// plain HTTP.
    ///
    /// # Errors
    ///
    /// If `url` is not such a URL or the client cannot be configured.
    /// Nothing is asked of the store here: a bucket that is missing or
    /// refuses the credentials shows on the first request.
    pub fn s3(url: &str) -> Result<Self, OpenError> {
        let invalid = || OpenError::InvalidUrl {
            url: url.to_owned(),
        };
        let location = url.strip_prefix("s3://").ok_or_else(invalid)?;
        let (bucket, prefix) = match location.split_once('/') {
            Some((bucket, prefix)) => (bucket, prefix.trim_end_matches('/')),
            None => (location, ""),
        };
        if bucket.is_empty() {
            return Err(invalid());
        }
        // Both the TLS of the client and its request signing run on
        // ring; installing it twice is refused and harmless.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let retry = RetryConfig {
            max_retries: MAX_RETRIES,
            retry_timeout: RETRY_BUDGET,
            ..RetryConfig::default()
        };
        let bucket = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .with_retry(retry)
            .build()
            .map_err(|source| OpenError::Client {
                url: url.to_owned(),
                source,
            })?;
        if prefix.is_empty() {
            return Ok(RemoteStore::new(Arc::new(bucket)));
        }
        let prefix = Key::new(prefix).map_err(|_| invalid())?;
        Ok(RemoteStore::new(Arc::new(PrefixStore::new(
            bucket,
            path(&prefix),
        ))))
    }

    /// Why a read of `range` failed with `error`: the range lying
    /// outside the object, if the object's size says so, and the
    /// client's failure otherwise. Stores differ in how they refuse a
    /// range, so the size decides.
    async fn range_error(&self, key: &Key, range: Range<u64>, error: Error) -> ObjectError {
        if matches!(error, Error::NotFound { .. }) {
            return failure(key, error);
        }
        match self.head(key).await {
            Ok(meta) if range.end > meta.size => ObjectError::InvalidRange {
                key: key.clone(),
                range,
                size: meta.size,
            },
            _ => failure(key, error),
        }
    }
}

/// A key as the client's path. A key's characters need no escaping
/// (see [`Key`]), so the two read the same.
fn path(key: &Key) -> Path {
    Path::from(key.as_str())
}

/// The client's failure as the store's. What the client gives up on
/// after its retries, a server error or no answer, is `Unavailable`;
/// a refusal a retry will not change is `Failed`.
fn failure(key: &Key, error: Error) -> ObjectError {
    let key = key.clone();
    match error {
        Error::NotFound { .. } => ObjectError::NotFound { key },
        Error::AlreadyExists { .. } => ObjectError::AlreadyExists { key },
        Error::Generic { .. } | Error::JoinError { .. } => ObjectError::Unavailable {
            key,
            source: Box::new(error),
        },
        _ => ObjectError::Failed {
            key,
            source: Box::new(error),
        },
    }
}

#[async_trait]
impl ObjectStore for RemoteStore {
    async fn get(&self, key: &Key) -> Result<Bytes, ObjectError> {
        let found = self.client.get(&path(key)).await;
        match found {
            Ok(object) => object.bytes().await.map_err(|e| failure(key, e)),
            Err(error) => Err(failure(key, error)),
        }
    }

    async fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Bytes, ObjectError> {
        if range.start >= range.end {
            // No bytes are asked for; the object's size alone says
            // whether the range lies in it.
            let size = self.head(key).await?.size;
            return if range.start == range.end && range.end <= size {
                Ok(Bytes::new())
            } else {
                Err(ObjectError::InvalidRange {
                    key: key.clone(),
                    range,
                    size,
                })
            };
        }
        let options = GetOptions::new().with_range(Some(GetRange::Bounded(range.clone())));
        let object = match self.client.get_opts(&path(key), options).await {
            Ok(object) => object,
            Err(error) => return Err(self.range_error(key, range, error).await),
        };
        // A range that ends past the object is answered with what
        // there is; the contract refuses it.
        if object.range.end < range.end {
            return Err(ObjectError::InvalidRange {
                key: key.clone(),
                range,
                size: object.meta.size,
            });
        }
        object.bytes().await.map_err(|e| failure(key, e))
    }

    async fn head(&self, key: &Key) -> Result<ObjectMeta, ObjectError> {
        match self.client.head(&path(key)).await {
            Ok(meta) => Ok(ObjectMeta {
                key: key.clone(),
                size: meta.size,
            }),
            Err(error) => Err(failure(key, error)),
        }
    }

    async fn put(&self, key: &Key, body: Bytes) -> Result<(), ObjectError> {
        match self.client.put(&path(key), body.into()).await {
            Ok(_) => Ok(()),
            Err(error) => Err(failure(key, error)),
        }
    }

    async fn put_if_absent(&self, key: &Key, body: Bytes) -> Result<(), ObjectError> {
        let create = PutOptions::from(PutMode::Create);
        match self.client.put_opts(&path(key), body.into(), create).await {
            Ok(_) => Ok(()),
            Err(error) => Err(failure(key, error)),
        }
    }

    fn list(&self, prefix: &Key) -> BoxStream<'_, Result<ObjectMeta, ObjectError>> {
        let prefix = prefix.clone();
        self.client
            .list(Some(&path(&prefix)))
            .filter_map(move |listed| {
                let item = match listed {
                    // What is not a key is not an object of Tessera's.
                    Ok(meta) => Key::new(meta.location.as_ref()).ok().map(|key| {
                        Ok(ObjectMeta {
                            key,
                            size: meta.size,
                        })
                    }),
                    Err(error) => Some(Err(failure(&prefix, error))),
                };
                std::future::ready(item)
            })
            .boxed()
    }

    async fn list_names(&self, prefix: &Key) -> Result<Vec<String>, ObjectError> {
        // One listing with the separator as delimiter: the store rolls
        // everything below a name up into the name.
        let listed = match self.client.list_with_delimiter(Some(&path(prefix))).await {
            Ok(listed) => listed,
            Err(error) => return Err(failure(prefix, error)),
        };
        let below = listed.common_prefixes.iter();
        let objects = listed.objects.iter().map(|meta| &meta.location);
        let names = below
            .chain(objects)
            .filter_map(Path::filename)
            // What is not a key component is not of Tessera's.
            .filter(|name| Key::new(*name).is_ok())
            .map(str::to_owned)
            .collect();
        Ok(crate::store::in_order(names))
    }

    async fn delete(&self, key: &Key) -> Result<(), ObjectError> {
        match self.client.delete(&path(key)).await {
            Ok(()) | Err(Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(failure(key, error)),
        }
    }
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;

    use super::*;
    use crate::conformance;

    #[tokio::test]
    async fn conforms_over_the_clients_memory_store() {
        conformance::run_all(Arc::new(RemoteStore::new(Arc::new(InMemory::new())))).await;
    }

    #[tokio::test]
    async fn keys_of_a_prefixed_store_stay_under_the_prefix() {
        let bucket: Arc<dyn Client> = Arc::new(InMemory::new());
        let prefixed = RemoteStore::new(Arc::new(PrefixStore::new(
            Arc::clone(&bucket),
            Path::from("deployments/a"),
        )));
        let key = Key::new("x/1").unwrap();
        prefixed
            .put(&key, Bytes::from_static(b"one"))
            .await
            .unwrap();

        let whole = RemoteStore::new(bucket);
        let stored = Key::new("deployments/a/x/1").unwrap();
        assert_eq!(
            whole.get(&stored).await.unwrap(),
            Bytes::from_static(b"one")
        );
        let listed: Vec<_> = prefixed
            .list(&Key::new("x").unwrap())
            .map(|meta| meta.unwrap().key)
            .collect()
            .await;
        assert_eq!(listed, [key]);
    }

    #[test]
    fn a_url_that_names_no_bucket_is_refused() {
        for url in [
            "s3://",
            "s3:///prefix",
            "gs://bucket",
            "/var/lib/tessera",
            "s3://b/a b",
        ] {
            let error = RemoteStore::s3(url).unwrap_err();
            assert!(
                matches!(error, OpenError::InvalidUrl { .. }),
                "{url}: {error}"
            );
        }
    }

    #[test]
    fn only_what_the_client_gave_up_on_is_retryable() {
        let key = Key::new("a").unwrap();
        let gave_up = Error::Generic {
            store: "S3",
            source: "503 Slow Down".into(),
        };
        assert!(failure(&key, gave_up).is_retryable());
        let refused = Error::PermissionDenied {
            path: "a".to_owned(),
            source: "403 Forbidden".into(),
        };
        assert!(!failure(&key, refused).is_retryable());
    }

    /// The same contract against a real S3-compatible store, when
    /// `TESSERA_S3_TEST_URL` names a bucket or a prefix to use, with
    /// the `AWS_*` variables saying how to reach it. The run's objects
    /// go under a prefix of their own and are removed at the end.
    #[tokio::test]
    async fn conforms_over_an_s3_bucket() {
        let Ok(url) = std::env::var("TESSERA_S3_TEST_URL") else {
            eprintln!("TESSERA_S3_TEST_URL is not set; skipping");
            return;
        };
        let run = format!(
            "{}/conformance-{}",
            url.trim_end_matches('/'),
            std::process::id()
        );
        let store: Arc<dyn ObjectStore> = Arc::new(RemoteStore::s3(&run).unwrap());
        conformance::run_all(Arc::clone(&store)).await;
        let prefixes = (1..=11)
            .map(|check| format!("c{check}"))
            .chain(["c11x".to_owned()]);
        for prefix in prefixes.map(|name| Key::new(name).unwrap()) {
            let keys: Vec<_> = store
                .list(&prefix)
                .map(|meta| meta.unwrap().key)
                .collect()
                .await;
            for key in keys {
                store.delete(&key).await.unwrap();
            }
        }
    }
}
