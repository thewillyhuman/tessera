// SPDX-License-Identifier: Apache-2.0

//! A store that seals what it holds for the tenants that have a key
//! (SE-2, SE-3).
//!
//! Data files, sidecar indexes and the blocks of the writer logs are
//! sealed by the code that writes them, in forms that are read by
//! range. Everything else under a tenant's prefix says as much about
//! the tenant: a manifest carries the least and greatest value of
//! every column of every file, a dataset log the predicates of its
//! deletes, the control store its rules and receivers, the rule state
//! the labels of its alerts. This store seals each such object whole
//! when it is written and opens it when it is read, so that nothing
//! above it needs to know. Which objects those are is the layout's to
//! say ([`layout::sealed_for`]).
//!
//! A sealed object is a mark, the name of the key and the bytes the
//! key sealed, bound to the object's own key: sealed bytes moved to
//! another place do not open there. An object written before the
//! tenant had a key stays plain and reads as before; one sealed under
//! an earlier key opens for as long as the keyring holds that key.
//!
//! An object sealed whole is read whole: a range of it, or its size,
//! costs the object. These objects are small, and the ones read by
//! range seal themselves.

use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use futures::stream::BoxStream;
use tessera_types::{KeyError, Keys, TenantId};

use crate::memory::slice;
use crate::{Key, ObjectError, ObjectMeta, ObjectStore, layout};

/// What a sealed object starts with. No object Tessera writes plain
/// does: they are JSON, Parquet or text.
const MARK: &[u8; 8] = b"\0TSSEAL1";

/// A store over `inner` that seals the objects of the tenants `keys`
/// names a key for.
#[derive(Debug)]
pub struct SealedStore {
    inner: Arc<dyn ObjectStore>,
    keys: Arc<Keys>,
}

impl SealedStore {
    /// The store over `inner`, sealing and opening with `keys`.
    #[must_use]
    pub fn new(inner: Arc<dyn ObjectStore>, keys: Arc<Keys>) -> Self {
        SealedStore { inner, keys }
    }

    /// `body` as it is stored at `key`: sealed when the key belongs
    /// to a tenant that has a key now.
    fn sealed(&self, key: &Key, body: Bytes) -> Result<Bytes, ObjectError> {
        let Some(tenant) = layout::sealed_for(key) else {
            return Ok(body);
        };
        let Some(named) = self
            .keys
            .for_tenant(&tenant)
            .map_err(|error| failed(key, error))?
        else {
            return Ok(body);
        };
        let name = named.name.as_bytes();
        let length = u16::try_from(name.len()).map_err(|_| ObjectError::Failed {
            key: key.clone(),
            source: "the name of the tenant's key is too long to record".into(),
        })?;
        let sealed = named.key.seal(key.as_str().as_bytes(), &body);
        let mut out = BytesMut::with_capacity(MARK.len() + 2 + name.len() + sealed.len());
        out.put_slice(MARK);
        out.put_u16_le(length);
        out.put_slice(name);
        out.put_slice(&sealed);
        Ok(out.freeze())
    }

    /// What `stored`, read at `key`, holds: opened when it is sealed.
    fn opened(&self, key: &Key, stored: Bytes) -> Result<Bytes, ObjectError> {
        if layout::sealed_for(key).is_none() {
            return Ok(stored);
        }
        let Some(rest) = stored.strip_prefix(MARK.as_slice()) else {
            return Ok(stored);
        };
        let cut = |what: &str| ObjectError::Failed {
            key: key.clone(),
            source: format!("a sealed object is cut short: {what}").into(),
        };
        let (length, rest) = rest
            .split_first_chunk::<2>()
            .ok_or_else(|| cut("no name"))?;
        let length = usize::from(u16::from_le_bytes(*length));
        let (name, sealed) = rest
            .split_at_checked(length)
            .ok_or_else(|| cut("half a name"))?;
        let name = std::str::from_utf8(name).map_err(|_| cut("a name that is not text"))?;
        self.keys
            .open(name, key.as_str().as_bytes(), sealed)
            .map(Bytes::from)
            .map_err(|error| failed(key, error))
    }

    /// Whether an object at `key` may be sealed: its tenant has a key.
    /// One whose key the keyring lacks counts, so that the read fails
    /// on the missing key and not on sealed bytes taken for plain.
    fn may_be_sealed(&self, key: &Key) -> bool {
        layout::sealed_for(key).is_some_and(|tenant| self.is_keyed(&tenant))
    }

    fn is_keyed(&self, tenant: &TenantId) -> bool {
        !matches!(self.keys.for_tenant(tenant), Ok(None))
    }
}

/// A key that is missing or does not open is not a fault a retry
/// mends.
fn failed(key: &Key, error: KeyError) -> ObjectError {
    ObjectError::Failed {
        key: key.clone(),
        source: Box::new(error),
    }
}

#[async_trait]
impl ObjectStore for SealedStore {
    async fn get(&self, key: &Key) -> Result<Bytes, ObjectError> {
        let stored = self.inner.get(key).await?;
        self.opened(key, stored)
    }

    async fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Bytes, ObjectError> {
        if !self.may_be_sealed(key) {
            return self.inner.get_range(key, range).await;
        }
        let body = self.get(key).await?;
        slice(key, &body, range)
    }

    async fn get_tail(&self, key: &Key, length: u64) -> Result<Bytes, ObjectError> {
        if !self.may_be_sealed(key) {
            return self.inner.get_tail(key, length).await;
        }
        let body = self.get(key).await?;
        let length = usize::try_from(length).unwrap_or(usize::MAX);
        Ok(body.slice(body.len().saturating_sub(length)..))
    }

    async fn head(&self, key: &Key) -> Result<ObjectMeta, ObjectError> {
        if !self.may_be_sealed(key) {
            return self.inner.head(key).await;
        }
        let body = self.get(key).await?;
        Ok(ObjectMeta {
            key: key.clone(),
            size: body.len() as u64,
        })
    }

    async fn put(&self, key: &Key, body: Bytes) -> Result<(), ObjectError> {
        let stored = self.sealed(key, body)?;
        self.inner.put(key, stored).await
    }

    async fn put_if_absent(&self, key: &Key, body: Bytes) -> Result<(), ObjectError> {
        let stored = self.sealed(key, body)?;
        self.inner.put_if_absent(key, stored).await
    }

    /// The objects as they are stored: a sealed object's size is that
    /// of its sealed bytes.
    fn list(&self, prefix: &Key) -> BoxStream<'_, Result<ObjectMeta, ObjectError>> {
        self.inner.list(prefix)
    }

    fn list_after(
        &self,
        prefix: &Key,
        after: &Key,
    ) -> BoxStream<'_, Result<ObjectMeta, ObjectError>> {
        self.inner.list_after(prefix, after)
    }

    async fn list_names(&self, prefix: &Key) -> Result<Vec<String>, ObjectError> {
        self.inner.list_names(prefix).await
    }

    async fn delete(&self, key: &Key) -> Result<(), ObjectError> {
        self.inner.delete(key).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use tessera_types::{KeyPolicy, Keyring, TenantKey};

    use super::*;
    use crate::MemoryStore;

    /// The key each tenant is under, changed as a test goes.
    #[derive(Default)]
    struct Named(Mutex<BTreeMap<String, String>>);

    impl Named {
        fn set(&self, tenant: &str, name: &str) {
            self.0
                .lock()
                .unwrap()
                .insert(tenant.to_owned(), name.to_owned());
        }
    }

    impl KeyPolicy for Named {
        fn key_name(&self, tenant: &TenantId) -> Option<String> {
            self.0.lock().unwrap().get(tenant.as_str()).cloned()
        }
    }

    /// Every tenant under the one key.
    struct Everyone;

    impl KeyPolicy for Everyone {
        fn key_name(&self, _: &TenantId) -> Option<String> {
            Some("k1".to_owned())
        }
    }

    fn ring() -> Keyring {
        Keyring::default()
            .with("k1", TenantKey::from_bytes([1; 32]))
            .with("k2", TenantKey::from_bytes([2; 32]))
    }

    /// A sealing store over a memory store, `acme` under `k1`, with
    /// the memory store to see what is stored and the policy to change.
    fn stores() -> (SealedStore, Arc<MemoryStore>, Arc<Named>) {
        let inner = Arc::new(MemoryStore::new());
        let policy = Arc::new(Named::default());
        policy.set("acme", "k1");
        let keys = Arc::new(Keys::new(ring(), policy.clone()));
        (SealedStore::new(inner.clone(), keys), inner, policy)
    }

    fn key(text: &str) -> Key {
        Key::new(text).unwrap()
    }

    const BODY: &[u8] = b"{\"min\":\"confidential\"}";

    fn holds(bytes: &[u8], needle: &[u8]) -> bool {
        bytes.windows(needle.len()).any(|window| window == needle)
    }

    #[tokio::test]
    async fn conforms_when_it_seals_nothing() {
        let keys = Arc::new(Keys::none());
        let store = SealedStore::new(Arc::new(MemoryStore::new()), keys);
        crate::conformance::run_all(Arc::new(store)).await;
    }

    #[tokio::test]
    async fn conforms_when_it_seals_everything() {
        let keys = Arc::new(Keys::new(ring(), Arc::new(Everyone)));
        let store = SealedStore::new(Arc::new(MemoryStore::new()), keys);
        crate::conformance::run_all(Arc::new(store)).await;
    }

    #[tokio::test]
    async fn a_keyed_tenants_object_is_stored_sealed_and_reads_as_written() {
        let (store, inner, _) = stores();
        let at = key("acme/prod/_log/00000000000000000001");
        store
            .put_if_absent(&at, Bytes::from_static(BODY))
            .await
            .unwrap();

        let stored = inner.get(&at).await.unwrap();
        assert!(stored.starts_with(MARK), "marked as sealed");
        assert!(!holds(&stored, b"confidential"), "nothing readable");
        assert!(holds(&stored, b"k1"), "the key is named");

        assert_eq!(store.get(&at).await.unwrap(), BODY);
        assert_eq!(store.get_range(&at, 2..5).await.unwrap(), &BODY[2..5]);
        assert_eq!(store.head(&at).await.unwrap().size, BODY.len() as u64);
        let outside = store.get_range(&at, 0..999).await.unwrap_err();
        assert!(
            matches!(outside, ObjectError::InvalidRange { size, .. } if size == BODY.len() as u64),
            "{outside}"
        );
    }

    #[tokio::test]
    async fn a_tenant_without_a_key_is_stored_plain() {
        let (store, inner, _) = stores();
        let at = key("other/prod/_log/00000000000000000001");
        store.put(&at, Bytes::from_static(BODY)).await.unwrap();
        assert_eq!(inner.get(&at).await.unwrap(), BODY);
        assert_eq!(store.get(&at).await.unwrap(), BODY);
    }

    #[tokio::test]
    async fn what_seals_itself_and_what_is_no_tenants_is_left_alone() {
        let (store, inner, _) = stores();
        for text in [
            "acme/prod/p/log/0000000000-0000003600/d/00/07.parquet",
            "acme/prod/p/log/0000000000-0000003600/i/00/07.idx",
            "acme/prod/iceberg/log/metadata/v1.metadata.json",
            "_wal/09/000000000001",
            "_cluster/catalog/_log/00000000000000000001",
        ] {
            let at = key(text);
            store.put(&at, Bytes::from_static(BODY)).await.unwrap();
            assert_eq!(inner.get(&at).await.unwrap(), BODY, "{text}");
            assert_eq!(store.get(&at).await.unwrap(), BODY, "{text}");
        }
    }

    #[tokio::test]
    async fn sealed_bytes_do_not_open_at_another_key() {
        let (store, inner, _) = stores();
        let written = key("acme/prod/_log/00000000000000000001");
        let moved = key("acme/prod/_log/00000000000000000002");
        store.put(&written, Bytes::from_static(BODY)).await.unwrap();
        let stored = inner.get(&written).await.unwrap();
        inner.put(&moved, stored).await.unwrap();

        let error = store.get(&moved).await.unwrap_err();
        assert!(matches!(error, ObjectError::Failed { .. }), "{error}");
        assert!(!error.is_retryable());
    }

    #[tokio::test]
    async fn what_was_plain_before_the_key_still_reads() {
        let (store, inner, _) = stores();
        let at = key("acme/_control/_log/00000000000000000001");
        inner.put(&at, Bytes::from_static(BODY)).await.unwrap();
        assert_eq!(store.get(&at).await.unwrap(), BODY);
        assert_eq!(store.get_range(&at, 0..1).await.unwrap(), &BODY[..1]);
        assert_eq!(store.head(&at).await.unwrap().size, BODY.len() as u64);
    }

    #[tokio::test]
    async fn what_an_earlier_key_sealed_opens_after_the_tenant_moves_on() {
        let (store, inner, policy) = stores();
        let old = key("acme/_eval/slo/0/_log/00000000000000000001");
        let new = key("acme/_eval/slo/0/_log/00000000000000000002");
        store.put(&old, Bytes::from_static(BODY)).await.unwrap();
        policy.set("acme", "k2");
        store.put(&new, Bytes::from_static(BODY)).await.unwrap();

        assert!(holds(&inner.get(&old).await.unwrap(), b"k1"));
        assert!(holds(&inner.get(&new).await.unwrap(), b"k2"));
        assert_eq!(store.get(&old).await.unwrap(), BODY);
        assert_eq!(store.get(&new).await.unwrap(), BODY);
    }

    #[tokio::test]
    async fn a_write_fails_rather_than_store_plain_what_was_to_be_sealed() {
        let (store, inner, policy) = stores();
        policy.set("acme", "lost");
        let at = key("acme/prod/_log/00000000000000000001");
        let error = store
            .put_if_absent(&at, Bytes::from_static(BODY))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("lost"), "{error}");
        assert!(matches!(
            inner.get(&at).await,
            Err(ObjectError::NotFound { .. })
        ));
    }

    #[tokio::test]
    async fn a_read_fails_when_the_keyring_lacks_the_key() {
        let (store, _, _) = stores();
        let at = key("acme/prod/_log/00000000000000000001");
        store.put(&at, Bytes::from_static(BODY)).await.unwrap();
        let inner = Arc::clone(&store.inner);
        let policy = Arc::new(Named::default());
        policy.set("acme", "k1");
        let without = SealedStore::new(inner, Arc::new(Keys::new(Keyring::default(), policy)));

        for error in [
            without.get(&at).await.unwrap_err(),
            without.get_range(&at, 0..1).await.unwrap_err(),
            without.head(&at).await.map(|_| ()).unwrap_err(),
        ] {
            assert!(error.to_string().contains("k1"), "{error}");
        }
    }

    #[tokio::test]
    async fn a_sealed_object_cut_short_is_refused() {
        let (store, inner, _) = stores();
        let at = key("acme/prod/_log/00000000000000000001");
        for stored in [&MARK[..], &[MARK.as_slice(), &[9, 0, b'k']].concat()] {
            inner
                .put(&at, Bytes::copy_from_slice(stored))
                .await
                .unwrap();
            let error = store.get(&at).await.unwrap_err();
            assert!(error.to_string().contains("cut short"), "{error}");
        }
    }
}
