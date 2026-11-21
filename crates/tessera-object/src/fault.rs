// SPDX-License-Identifier: Apache-2.0

//! Fault injection for simulation (RS-2, section 18.5).
//!
//! [`FaultyStore`] wraps any store and makes operations fail at configured
//! rates, drawing from the injected random source so that a run is
//! reproducible from its seed. Two kinds of failure matter for
//! correctness, and both are modelled:
//!
//! - **before**: the operation fails and has no effect;
//! - **after**: a mutating operation takes effect and is reported as
//!   `Unavailable` anyway, as a real store does when the response is lost.
//!   This is the case that commit and fencing logic must survive.

use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use tessera_types::{Random, SharedRandom};

use crate::{Key, ObjectError, ObjectMeta, ObjectStore};

/// How often a [`FaultyStore`] fails, as probabilities in `0.0..=1.0`.
#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub struct FaultPolicy {
    /// Probability that any operation fails before taking effect.
    pub fail_before: f64,
    /// Probability that a mutating operation takes effect and is then
    /// reported as `Unavailable` anyway.
    pub fail_after: f64,
}

impl FaultPolicy {
    /// Never fail: the wrapper is transparent.
    pub const NONE: FaultPolicy = FaultPolicy {
        fail_before: 0.0,
        fail_after: 0.0,
    };
}

/// A store that injects failures into another store.
pub struct FaultyStore {
    inner: Arc<dyn ObjectStore>,
    policy: Mutex<FaultPolicy>,
    random: Mutex<SharedRandom>,
    injected: AtomicU64,
}

impl FaultyStore {
    /// Wraps `inner`, deciding faults with `random` according to `policy`.
    #[must_use]
    pub fn new(inner: Arc<dyn ObjectStore>, random: SharedRandom, policy: FaultPolicy) -> Self {
        FaultyStore {
            inner,
            policy: Mutex::new(policy),
            random: Mutex::new(random),
            injected: AtomicU64::new(0),
        }
    }

    /// Replaces the policy for all later operations.
    pub fn set_policy(&self, policy: FaultPolicy) {
        *self.policy.lock().unwrap_or_else(PoisonError::into_inner) = policy;
    }

    /// The number of failures injected so far.
    pub fn injected(&self) -> u64 {
        self.injected.load(Ordering::Relaxed)
    }

    fn policy(&self) -> FaultPolicy {
        *self.policy.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Decides one fault with the given probability, drawing exactly one
    /// value so that the draw sequence stays predictable.
    fn decide(&self, probability: f64) -> bool {
        let draw = self
            .random
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .next_u64();
        // Precision loss in the conversion is irrelevant for a probability.
        #[allow(clippy::cast_precision_loss)]
        let unit = draw as f64 / u64::MAX as f64;
        probability >= 1.0 || unit < probability
    }

    fn fail_before(&self, key: &Key) -> Result<(), ObjectError> {
        if self.decide(self.policy().fail_before) {
            return Err(self.inject(key, "injected failure before the operation"));
        }
        Ok(())
    }

    fn fail_after(&self, key: &Key) -> Result<(), ObjectError> {
        if self.decide(self.policy().fail_after) {
            return Err(self.inject(key, "injected failure after the operation took effect"));
        }
        Ok(())
    }

    fn inject(&self, key: &Key, reason: &'static str) -> ObjectError {
        self.injected.fetch_add(1, Ordering::Relaxed);
        ObjectError::Unavailable {
            key: key.clone(),
            source: reason.into(),
        }
    }
}

impl std::fmt::Debug for FaultyStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FaultyStore")
            .field("inner", &self.inner)
            .field("policy", &self.policy())
            .field("injected", &self.injected())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl ObjectStore for FaultyStore {
    async fn get(&self, key: &Key) -> Result<Bytes, ObjectError> {
        self.fail_before(key)?;
        self.inner.get(key).await
    }

    async fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Bytes, ObjectError> {
        self.fail_before(key)?;
        self.inner.get_range(key, range).await
    }

    async fn head(&self, key: &Key) -> Result<ObjectMeta, ObjectError> {
        self.fail_before(key)?;
        self.inner.head(key).await
    }

    async fn put(&self, key: &Key, body: Bytes) -> Result<(), ObjectError> {
        self.fail_before(key)?;
        self.inner.put(key, body).await?;
        self.fail_after(key)
    }

    async fn put_if_absent(&self, key: &Key, body: Bytes) -> Result<(), ObjectError> {
        self.fail_before(key)?;
        self.inner.put_if_absent(key, body).await?;
        self.fail_after(key)
    }

    fn list(&self, prefix: &Key) -> BoxStream<'_, Result<ObjectMeta, ObjectError>> {
        match self.fail_before(prefix) {
            Ok(()) => self.inner.list(prefix),
            Err(e) => stream::once(async move { Err(e) }).boxed(),
        }
    }

    async fn list_names(&self, prefix: &Key) -> Result<Vec<String>, ObjectError> {
        self.fail_before(prefix)?;
        self.inner.list_names(prefix).await
    }

    async fn delete(&self, key: &Key) -> Result<(), ObjectError> {
        self.fail_before(key)?;
        self.inner.delete(key).await?;
        self.fail_after(key)
    }
}

/// A store that applies every write under one key prefix but reports it
/// as failed, and whose reads under that prefix fail while it is blind.
/// This is the worst case for a writer: a write that landed with no way
/// to find out, while the rest of the store answers. For tests of
/// lost-response handling.
pub struct LostResponseStore {
    inner: Arc<dyn ObjectStore>,
    lie_under: Key,
    blind: std::sync::atomic::AtomicBool,
}

impl LostResponseStore {
    /// Wraps `inner`, lying about writes under `lie_under`; not blind.
    #[must_use]
    pub fn new(inner: Arc<dyn ObjectStore>, lie_under: Key) -> Self {
        LostResponseStore {
            inner,
            lie_under,
            blind: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Makes reads under the lied-about prefix fail (`true`) or succeed
    /// (`false`).
    pub fn set_blind(&self, blind: bool) {
        self.blind.store(blind, Ordering::Relaxed);
    }

    fn blind(&self) -> bool {
        self.blind.load(Ordering::Relaxed)
    }

    /// Whether a read of `key` fails right now.
    fn blind_to(&self, key: &Key) -> bool {
        self.blind() && key.is_under(&self.lie_under)
    }

    fn lost(key: &Key) -> ObjectError {
        ObjectError::Unavailable {
            key: key.clone(),
            source: "response lost".into(),
        }
    }

    fn lie_after(&self, key: &Key, result: Result<(), ObjectError>) -> Result<(), ObjectError> {
        result?;
        if key.is_under(&self.lie_under) {
            Err(Self::lost(key))
        } else {
            Ok(())
        }
    }
}

impl std::fmt::Debug for LostResponseStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LostResponseStore")
            .field("lie_under", &self.lie_under)
            .field("blind", &self.blind())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl ObjectStore for LostResponseStore {
    async fn get(&self, key: &Key) -> Result<Bytes, ObjectError> {
        if self.blind_to(key) {
            return Err(Self::lost(key));
        }
        self.inner.get(key).await
    }

    async fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Bytes, ObjectError> {
        if self.blind_to(key) {
            return Err(Self::lost(key));
        }
        self.inner.get_range(key, range).await
    }

    async fn head(&self, key: &Key) -> Result<ObjectMeta, ObjectError> {
        if self.blind_to(key) {
            return Err(Self::lost(key));
        }
        self.inner.head(key).await
    }

    async fn put(&self, key: &Key, body: Bytes) -> Result<(), ObjectError> {
        let result = self.inner.put(key, body).await;
        self.lie_after(key, result)
    }

    async fn put_if_absent(&self, key: &Key, body: Bytes) -> Result<(), ObjectError> {
        let result = self.inner.put_if_absent(key, body).await;
        self.lie_after(key, result)
    }

    fn list(&self, prefix: &Key) -> BoxStream<'_, Result<ObjectMeta, ObjectError>> {
        if self.blind_to(prefix) || (self.blind() && self.lie_under.is_under(prefix)) {
            let error = Self::lost(prefix);
            return stream::once(async move { Err(error) }).boxed();
        }
        self.inner.list(prefix)
    }

    async fn list_names(&self, prefix: &Key) -> Result<Vec<String>, ObjectError> {
        if self.blind_to(prefix) || (self.blind() && self.lie_under.is_under(prefix)) {
            return Err(Self::lost(prefix));
        }
        self.inner.list_names(prefix).await
    }

    async fn delete(&self, key: &Key) -> Result<(), ObjectError> {
        self.inner.delete(key).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use tessera_types::SeededRandom;

    use super::*;
    use crate::{MemoryStore, conformance};

    fn key(text: &str) -> Key {
        Key::new(text).unwrap()
    }

    fn faulty(policy: FaultPolicy, seed: u64) -> (FaultyStore, MemoryStore) {
        let inner = MemoryStore::new();
        let store = FaultyStore::new(
            Arc::new(inner.clone()),
            SharedRandom::new(SeededRandom::new(seed)),
            policy,
        );
        (store, inner)
    }

    #[tokio::test]
    async fn without_faults_it_passes_the_conformance_suite() {
        let (store, _) = faulty(FaultPolicy::NONE, 1);
        conformance::run_all(Arc::new(store)).await;
    }

    #[tokio::test]
    async fn a_failure_before_has_no_effect() {
        let (store, inner) = faulty(
            FaultPolicy {
                fail_before: 1.0,
                fail_after: 0.0,
            },
            1,
        );
        let error = store
            .put(&key("a"), Bytes::from_static(b"x"))
            .await
            .unwrap_err();
        assert!(error.is_retryable(), "{error}");
        assert!(matches!(
            inner.get(&key("a")).await.unwrap_err(),
            ObjectError::NotFound { .. }
        ));
        assert_eq!(store.injected(), 1);
    }

    #[tokio::test]
    async fn a_failure_after_takes_effect_but_reports_unavailable() {
        let (store, inner) = faulty(
            FaultPolicy {
                fail_before: 0.0,
                fail_after: 1.0,
            },
            1,
        );
        let error = store
            .put_if_absent(&key("a"), Bytes::from_static(b"x"))
            .await
            .unwrap_err();
        assert!(error.is_retryable(), "{error}");
        assert_eq!(
            inner.get(&key("a")).await.unwrap(),
            Bytes::from_static(b"x")
        );
    }

    #[tokio::test]
    async fn reads_only_fail_before() {
        let (store, inner) = faulty(
            FaultPolicy {
                fail_before: 0.0,
                fail_after: 1.0,
            },
            1,
        );
        inner
            .put(&key("a"), Bytes::from_static(b"x"))
            .await
            .unwrap();
        assert_eq!(
            store.get(&key("a")).await.unwrap(),
            Bytes::from_static(b"x")
        );
        assert_eq!(store.injected(), 0);
    }

    #[tokio::test]
    async fn faults_are_reproducible_from_the_seed() {
        let policy = FaultPolicy {
            fail_before: 0.3,
            fail_after: 0.3,
        };
        let mut outcomes = Vec::new();
        for _ in 0..2 {
            let (store, _) = faulty(policy, 42);
            let mut run = Vec::new();
            for i in 0..32u8 {
                run.push(
                    store
                        .put(&key(&format!("k{i}")), Bytes::from(vec![i]))
                        .await
                        .is_ok(),
                );
            }
            outcomes.push(run);
        }
        assert_eq!(outcomes[0], outcomes[1]);
        assert!(
            outcomes[0].iter().any(|ok| !ok),
            "some operations should have failed"
        );
        assert!(
            outcomes[0].iter().any(|ok| *ok),
            "some operations should have succeeded"
        );
    }

    #[tokio::test]
    async fn the_policy_can_change_while_running() {
        let (store, _) = faulty(
            FaultPolicy {
                fail_before: 1.0,
                fail_after: 0.0,
            },
            1,
        );
        assert!(
            store
                .put(&key("a"), Bytes::from_static(b"x"))
                .await
                .is_err()
        );
        store.set_policy(FaultPolicy::NONE);
        assert!(store.put(&key("a"), Bytes::from_static(b"x")).await.is_ok());
    }

    #[tokio::test]
    async fn a_lost_response_store_applies_writes_it_reports_as_failed() {
        let inner = MemoryStore::new();
        let store = LostResponseStore::new(Arc::new(inner.clone()), key("_wal"));
        let error = store
            .put_if_absent(&key("_wal/a"), Bytes::from_static(b"x"))
            .await
            .unwrap_err();
        assert!(error.is_retryable());
        assert_eq!(
            inner.get(&key("_wal/a")).await.unwrap(),
            Bytes::from_static(b"x"),
            "the write landed"
        );
        store
            .put(&key("other/b"), Bytes::from_static(b"y"))
            .await
            .expect("honest outside the prefix");
        assert!(matches!(
            store
                .put_if_absent(&key("_wal/a"), Bytes::from_static(b"z"))
                .await,
            Err(ObjectError::AlreadyExists { .. })
        ));
        store.set_blind(true);
        assert!(store.get(&key("_wal/a")).await.unwrap_err().is_retryable());
        store.set_blind(false);
        assert!(store.get(&key("_wal/a")).await.is_ok());
    }

    #[tokio::test]
    async fn listing_can_fail_too() {
        use futures::TryStreamExt;
        let (store, _) = faulty(
            FaultPolicy {
                fail_before: 1.0,
                fail_after: 0.0,
            },
            1,
        );
        let result: Result<Vec<_>, _> = store.list(&key("a")).try_collect().await;
        assert!(result.unwrap_err().is_retryable());
    }
}
