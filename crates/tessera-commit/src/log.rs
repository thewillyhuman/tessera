// SPDX-License-Identifier: Apache-2.0

//! The commit log over an object store.
//!
//! Version *N+1* is committed by creating `{prefix}/_log/{N+1:020}` with
//! create-if-absent, so exactly one committer succeeds per version (CL-1).
//! A committer that loses reads the newer commits and decides whether to
//! rebase or abandon (CL-2); [`CommitLog::update`] does that loop.
//!
//! A write whose response is lost is resolved by reading the version back:
//! if the object carries our commit id, we won. If the object store stays
//! unreachable, the result is [`CommitError::Unknown`]; the actions may or
//! may not have been committed, and the caller must re-derive its intent
//! from a fresh snapshot, as `update` does, rather than resend them.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures::TryStreamExt;
use futures::stream::{self, BoxStream};
use tessera_object::{BoxError, Key, ObjectError, ObjectStore};
use tessera_types::{Env, Version};

use crate::{Action, Commit, CommitId, FORMAT, Snapshot, State};

/// How many times a lost write is read back before giving up.
const VERIFY_ATTEMPTS: u32 = 8;
/// Backoff between read-back attempts, doubled each time.
const VERIFY_BACKOFF: Duration = Duration::from_millis(10);

/// How many times an update of any log on this node lost a version to
/// another committer and retried (section 14.7): the commit conflict
/// rate, read by the node's own metrics.
static CONFLICTS: AtomicU64 = AtomicU64::new(0);

/// The conflicts counted so far on this node.
#[must_use]
pub fn conflicts() -> u64 {
    CONFLICTS.load(Ordering::Relaxed)
}

/// Why a commit-log operation failed.
#[derive(Debug, thiserror::Error)]
pub enum CommitError {
    /// Another committer established this version first. Read the new
    /// commits, rebase and retry, or abandon (CL-2).
    #[error("version {version} was committed by another node")]
    Conflict {
        /// The version that was contended.
        version: Version,
    },
    /// The write's response was lost and the store could not be read back:
    /// the commit may or may not exist. Re-derive the intent from a fresh
    /// snapshot; never resend the same actions.
    #[error("the outcome of committing version {version} is unknown")]
    Unknown {
        /// The version that was attempted.
        version: Version,
    },
    /// An object of the log cannot be read or applied.
    #[error("commit log object {key} is corrupt: {source}")]
    Corrupt {
        /// The offending object.
        key: Key,
        /// What is wrong with it.
        #[source]
        source: BoxError,
    },
    /// The object store failed.
    #[error(transparent)]
    Store(#[from] ObjectError),
}

/// Why [`CommitLog::update`] failed.
#[derive(Debug, thiserror::Error)]
pub enum UpdateError<E> {
    /// The caller's closure rejected the current state; nothing was committed.
    #[error("update rejected: {0}")]
    Rejected(E),
    /// The log failed.
    #[error(transparent)]
    Commit(CommitError),
    /// Every attempt lost to another committer.
    #[error("gave up after {attempts} conflicting attempts")]
    Contention {
        /// How many attempts were made.
        attempts: u32,
    },
}

/// A commit log rooted at one key prefix, folding into state `S`.
///
/// Several handles may serve one log from several nodes; the handle caches
/// the latest snapshot it has seen and catches up by tailing.
pub struct CommitLog<S> {
    store: Arc<dyn ObjectStore>,
    prefix: Key,
    env: Env,
    cached: Mutex<Option<Arc<Snapshot<S>>>>,
}

impl<S: State> CommitLog<S> {
    /// How many times `update` retries after a conflict.
    pub const UPDATE_ATTEMPTS: u32 = 16;

    /// A handle to the log at `prefix` in `store`.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, prefix: Key, env: Env) -> Self {
        CommitLog {
            store,
            prefix,
            env,
            cached: Mutex::new(None),
        }
    }

    /// The key prefix of the log.
    #[must_use]
    pub fn prefix(&self) -> &Key {
        &self.prefix
    }

    /// The latest snapshot this handle has seen, without touching the store.
    #[must_use]
    pub fn cached(&self) -> Option<Arc<Snapshot<S>>> {
        self.cached
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The latest committed version and the state at it, read from the
    /// store.
    ///
    /// # Errors
    ///
    /// `Store` if the log cannot be read; `Corrupt` if an object cannot be
    /// decoded or applied.
    pub async fn snapshot(&self) -> Result<Arc<Snapshot<S>>, CommitError> {
        let base = match self.cached() {
            Some(cached) => cached,
            None => Arc::new(self.load_checkpoint().await?),
        };
        let mut version = base.version();
        let mut state = base.state().clone();
        let mut tail = self.tail(version);
        while let Some(commit) = tail.try_next().await? {
            apply_all(&mut state, &commit, &self.commit_key(commit.version))?;
            version = commit.version;
        }
        drop(tail);
        Ok(self.remember(Snapshot::new(version, state)))
    }

    /// Commits `actions` as the version after `base`.
    ///
    /// # Errors
    ///
    /// `Conflict` if another committer won that version; `Unknown` if the
    /// outcome could not be determined; `Store` or `Corrupt` otherwise.
    pub async fn commit(
        &self,
        base: &Snapshot<S>,
        actions: Vec<Action>,
    ) -> Result<Arc<Snapshot<S>>, CommitError> {
        let version = base.version().next();
        let commit = Commit {
            format: FORMAT,
            version,
            id: CommitId::random(&mut self.env.random.clone()),
            committer: self.env.node,
            committed_at: self.env.clock.now(),
            actions,
        };
        let key = self.commit_key(version);
        match self.store.put_if_absent(&key, commit.encode()).await {
            Ok(()) => {}
            // The version is taken, by another committer or by this
            // very write: a store's client that sends a write again
            // after a lost answer reports the refusal of the second.
            // The commit's id says which.
            Err(error) if error.is_retryable() || is_taken(&error) => {
                self.verify_landed(&key, commit.id, error).await?;
            }
            Err(error) => return Err(CommitError::Store(error)),
        }
        let mut state = base.state().clone();
        apply_all(&mut state, &commit, &key)?;
        Ok(self.remember(Snapshot::new(version, state)))
    }

    /// The commits after `from`, in order, until the end of the log.
    pub fn tail(&self, from: Version) -> BoxStream<'_, Result<Commit, CommitError>> {
        Box::pin(stream::try_unfold(from, move |version| async move {
            let next = version.next();
            let key = self.commit_key(next);
            match self.store.get(&key).await {
                Ok(bytes) => {
                    let commit = Commit::decode(&bytes).map_err(|e| corrupt(&key, e))?;
                    Ok(Some((commit, next)))
                }
                Err(ObjectError::NotFound { .. }) => Ok(None),
                Err(error) => Err(CommitError::Store(error)),
            }
        }))
    }

    /// Commits the actions that `decide` derives from the current state,
    /// retrying from a fresh snapshot whenever another committer wins.
    ///
    /// Because `decide` always runs against the latest state, it is safe
    /// under lost responses: an intent that already took effect is simply
    /// not derived again. Returning an empty action list commits nothing.
    ///
    /// # Errors
    ///
    /// `Rejected` with `decide`'s error; `Contention` after
    /// [`UPDATE_ATTEMPTS`](Self::UPDATE_ATTEMPTS) conflicts; `Commit` for
    /// any other log failure.
    pub async fn update<E>(
        &self,
        decide: impl Fn(&S) -> Result<Vec<Action>, E>,
    ) -> Result<Arc<Snapshot<S>>, UpdateError<E>> {
        let mut base = match self.cached() {
            Some(snapshot) => snapshot,
            None => self.snapshot().await.map_err(UpdateError::Commit)?,
        };
        for _ in 0..Self::UPDATE_ATTEMPTS {
            let actions = decide(base.state()).map_err(UpdateError::Rejected)?;
            if actions.is_empty() {
                return Ok(base);
            }
            match self.commit(&base, actions).await {
                Ok(snapshot) => return Ok(snapshot),
                Err(CommitError::Conflict { .. }) => {
                    CONFLICTS.fetch_add(1, Ordering::Relaxed);
                    base = self.snapshot().await.map_err(UpdateError::Commit)?;
                }
                Err(CommitError::Unknown { .. }) => {
                    base = self.snapshot().await.map_err(UpdateError::Commit)?;
                }
                Err(error) => return Err(UpdateError::Commit(error)),
            }
        }
        Err(UpdateError::Contention {
            attempts: Self::UPDATE_ATTEMPTS,
        })
    }

    /// Writes `snapshot` as a checkpoint, points the hint at it and removes
    /// all but the two newest checkpoints.
    ///
    /// Checkpointing a version that is already checkpointed is harmless:
    /// the state at a version is the same whoever folds it.
    ///
    /// # Errors
    ///
    /// `Store` if the checkpoint cannot be written. Failures to update the
    /// hint or to remove old checkpoints are not reported: both are
    /// recoverable by listing.
    pub async fn checkpoint(&self, snapshot: &Snapshot<S>) -> Result<(), CommitError> {
        let version = snapshot.version();
        let encoded = snapshot.state().encode_checkpoint(version);
        match self
            .store
            .put_if_absent(&self.checkpoint_key(version), encoded.into())
            .await
        {
            Ok(()) | Err(ObjectError::AlreadyExists { .. }) => {}
            Err(error) => return Err(CommitError::Store(error)),
        }
        let _ = self
            .store
            .put(&self.hint_key(), version.to_string().into())
            .await;
        self.remove_old_checkpoints(version).await;
        Ok(())
    }

    /// The newest checkpoint, or the initial snapshot if there is none.
    async fn load_checkpoint(&self) -> Result<Snapshot<S>, CommitError> {
        let version = match self.hinted_checkpoint().await {
            Some(version) => Some(version),
            None => self.newest_listed_checkpoint().await?,
        };
        let Some(version) = version else {
            return Ok(Snapshot::initial());
        };
        let key = self.checkpoint_key(version);
        let bytes = self.store.get(&key).await?;
        let state = S::decode_checkpoint(version, &bytes).map_err(|e| corrupt(&key, e))?;
        Ok(Snapshot::new(version, state))
    }

    /// The version of the newest checkpoint, if any: the hint's, else
    /// the newest listed.
    ///
    /// # Errors
    ///
    /// If the checkpoints cannot be listed.
    pub async fn checkpoint_version(&self) -> Result<Option<Version>, CommitError> {
        match self.hinted_checkpoint().await {
            Some(version) => Ok(Some(version)),
            None => self.newest_listed_checkpoint().await,
        }
    }

    /// The store the log lives in.
    #[must_use]
    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    /// The checkpoint the hint names, if the hint is readable and the
    /// checkpoint exists.
    async fn hinted_checkpoint(&self) -> Option<Version> {
        let bytes = self.store.get(&self.hint_key()).await.ok()?;
        let version: Version = std::str::from_utf8(&bytes).ok()?.trim().parse().ok()?;
        self.store
            .head(&self.checkpoint_key(version))
            .await
            .ok()
            .map(|_| version)
    }

    async fn newest_listed_checkpoint(&self) -> Result<Option<Version>, CommitError> {
        let versions = self.listed_checkpoints().await?;
        Ok(versions.last().copied())
    }

    /// Every checkpoint version, ascending.
    async fn listed_checkpoints(&self) -> Result<Vec<Version>, CommitError> {
        let mut versions = Vec::new();
        let mut listing = self.store.list(&self.checkpoint_prefix());
        while let Some(meta) = listing.try_next().await? {
            if let Ok(version) = meta.key.name().parse::<Version>() {
                versions.push(version);
            }
        }
        versions.sort_unstable();
        Ok(versions)
    }

    /// Deletes every checkpoint older than the two newest.
    async fn remove_old_checkpoints(&self, newest: Version) {
        let Ok(versions) = self.listed_checkpoints().await else {
            return;
        };
        let keep_from = versions
            .iter()
            .rev()
            .take(2)
            .copied()
            .min()
            .unwrap_or(newest);
        for version in versions.into_iter().filter(|v| *v < keep_from) {
            let _ = self.store.delete(&self.checkpoint_key(version)).await;
        }
    }

    fn checkpoint_prefix(&self) -> Key {
        self.prefix
            .join("_checkpoint")
            .expect("log key components are valid")
    }

    /// The key of the checkpoint at `version`.
    ///
    /// # Panics
    ///
    /// Never: a version is a valid key component.
    #[must_use]
    pub fn checkpoint_key(&self, version: Version) -> Key {
        self.checkpoint_prefix()
            .join(&version.to_string())
            .expect("log key components are valid")
    }

    fn hint_key(&self) -> Key {
        self.prefix
            .join("_hint")
            .expect("log key components are valid")
    }

    fn commit_key(&self, version: Version) -> Key {
        self.prefix
            .join("_log")
            .and_then(|k| k.join(&version.to_string()))
            .expect("log key components are valid")
    }

    /// Reads back a version whose write failed or was refused and
    /// decides whether our commit is the one that exists.
    async fn verify_landed(
        &self,
        key: &Key,
        id: CommitId,
        cause: ObjectError,
    ) -> Result<(), CommitError> {
        let version: Version = key.name().parse().expect("commit keys end in a version");
        let mut backoff = VERIFY_BACKOFF;
        for _ in 0..VERIFY_ATTEMPTS {
            match self.store.get(key).await {
                Ok(bytes) => {
                    let found = Commit::decode(&bytes).map_err(|e| corrupt(key, e))?;
                    return if found.id == id {
                        Ok(())
                    } else {
                        Err(CommitError::Conflict { version })
                    };
                }
                Err(ObjectError::NotFound { .. }) => return Err(CommitError::Store(cause)),
                Err(error) if error.is_retryable() => {
                    tokio::time::sleep(backoff).await;
                    backoff *= 2;
                }
                Err(error) => return Err(CommitError::Store(error)),
            }
        }
        Err(CommitError::Unknown { version })
    }

    /// Caches `snapshot` if it is newer than the cached one.
    fn remember(&self, snapshot: Snapshot<S>) -> Arc<Snapshot<S>> {
        let snapshot = Arc::new(snapshot);
        let mut cached = self.cached.lock().unwrap_or_else(PoisonError::into_inner);
        if cached
            .as_ref()
            .is_none_or(|c| c.version() < snapshot.version())
        {
            *cached = Some(Arc::clone(&snapshot));
        }
        snapshot
    }
}

impl<S> std::fmt::Debug for CommitLog<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommitLog")
            .field("prefix", &self.prefix)
            .field("node", &self.env.node)
            .finish_non_exhaustive()
    }
}

fn apply_all<S: State>(state: &mut S, commit: &Commit, key: &Key) -> Result<(), CommitError> {
    for action in &commit.actions {
        state.apply(action).map_err(|e| corrupt(key, e))?;
    }
    Ok(())
}

fn corrupt(key: &Key, source: impl std::error::Error + Send + Sync + 'static) -> CommitError {
    CommitError::Corrupt {
        key: key.clone(),
        source: Box::new(source),
    }
}

/// Whether a conditional write was refused because the key exists.
fn is_taken(error: &ObjectError) -> bool {
    matches!(error, ObjectError::AlreadyExists { .. })
}

#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::sync::Arc;

    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::TryStreamExt;
    use futures::stream::BoxStream;
    use tessera_object::{
        FaultPolicy, FaultyStore, Key, MemoryStore, ObjectError, ObjectMeta, ObjectStore,
    };
    use tessera_types::{Env, NodeId, Timestamp, Version};

    use super::*;
    use crate::{Action, Commit, CommitId, FORMAT, KvMutation, KvState};

    fn env(seed: u64) -> Env {
        Env::simulated(seed, Timestamp::UNIX_EPOCH)
    }

    fn prefix() -> Key {
        Key::new("_cluster/catalog").unwrap()
    }

    fn log(store: Arc<dyn ObjectStore>, seed: u64) -> CommitLog<KvState> {
        CommitLog::new(store, prefix(), env(seed))
    }

    fn put(key: &str, value: &str) -> Action {
        Action::Kv(KvMutation::Put {
            key: key.into(),
            value: value.into(),
        })
    }

    #[tokio::test]
    async fn an_empty_log_is_at_version_zero() {
        let log = log(Arc::new(MemoryStore::new()), 1);
        let snapshot = log.snapshot().await.unwrap();
        assert_eq!(snapshot.version(), Version::ZERO);
        assert!(snapshot.state().is_empty());
    }

    #[tokio::test]
    async fn a_commit_advances_the_version_and_applies_its_actions() {
        let log = log(Arc::new(MemoryStore::new()), 1);
        let base = log.snapshot().await.unwrap();
        let next = log.commit(&base, vec![put("a", "1")]).await.unwrap();
        assert_eq!(next.version(), Version::new(1));
        assert_eq!(next.state().get("a"), Some("1"));
        assert_eq!(log.snapshot().await.unwrap().version(), Version::new(1));
    }

    #[tokio::test]
    async fn conflicting_updates_are_counted() {
        let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
        let first = log(Arc::clone(&store), 1);
        let second = log(Arc::clone(&store), 2);
        second.snapshot().await.unwrap();
        first
            .update(|_| Ok::<_, String>(vec![put("a", "1")]))
            .await
            .unwrap();
        let before = conflicts();
        second
            .update(|_| Ok::<_, String>(vec![put("b", "2")]))
            .await
            .unwrap();
        // Other tests conflict concurrently, so the counter grows by at
        // least the one conflict this update suffered.
        assert!(conflicts() > before, "the stale base lost once, then won");
    }

    #[tokio::test]
    async fn a_stale_base_conflicts() {
        let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
        let first = log(Arc::clone(&store), 1);
        let second = log(Arc::clone(&store), 2);
        let base_first = first.snapshot().await.unwrap();
        let base_second = second.snapshot().await.unwrap();
        first
            .commit(&base_first, vec![put("a", "1")])
            .await
            .unwrap();
        let error = second
            .commit(&base_second, vec![put("a", "2")])
            .await
            .unwrap_err();
        assert!(matches!(error, CommitError::Conflict { version } if version == Version::new(1)));
        let first_key = Key::new("_cluster/catalog/_log/00000000000000000001").unwrap();
        assert!(
            store.get(&first_key).await.is_ok(),
            "the winner's commit object exists"
        );
    }

    #[tokio::test]
    async fn a_snapshot_sees_commits_made_through_another_handle() {
        let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
        let first = log(Arc::clone(&store), 1);
        let second = log(Arc::clone(&store), 2);
        let base = first.snapshot().await.unwrap();
        first.commit(&base, vec![put("a", "1")]).await.unwrap();
        let seen = second.snapshot().await.unwrap();
        assert_eq!(seen.version(), Version::new(1));
        assert_eq!(seen.state().get("a"), Some("1"));
    }

    #[tokio::test]
    async fn tail_streams_the_commits_after_a_version() {
        let log = log(Arc::new(MemoryStore::new()), 1);
        let mut base = log.snapshot().await.unwrap();
        for i in 1..=3 {
            base = log
                .commit(&base, vec![put("k", &i.to_string())])
                .await
                .unwrap();
        }
        let commits: Vec<Commit> = log.tail(Version::new(1)).try_collect().await.unwrap();
        let versions: Vec<Version> = commits.iter().map(|c| c.version).collect();
        assert_eq!(versions, [Version::new(2), Version::new(3)]);
        assert_eq!(commits[1].actions, vec![put("k", "3")]);
        assert_eq!(commits[0].committer, env(1).node);
    }

    #[tokio::test(start_paused = true)]
    async fn a_lost_response_is_recognised_as_our_own_commit() {
        let inner = MemoryStore::new();
        let faulty = FaultyStore::new(
            Arc::new(inner.clone()),
            env(3).random,
            FaultPolicy {
                fail_before: 0.0,
                fail_after: 1.0,
            },
        );
        let log = log(Arc::new(faulty), 1);
        let base = log.snapshot().await.unwrap();
        let next = log.commit(&base, vec![put("a", "1")]).await.unwrap();
        assert_eq!(next.version(), Version::new(1));
        assert_eq!(next.state().get("a"), Some("1"));
        assert_eq!(log.snapshot().await.unwrap().version(), Version::new(1));
    }

    /// A store whose conditional writes always fail without effect, while
    /// reads work: the shape of a store that accepts reads but not writes.
    #[derive(Debug)]
    struct ReadOnly(MemoryStore);

    #[async_trait]
    impl ObjectStore for ReadOnly {
        async fn get(&self, key: &Key) -> Result<Bytes, ObjectError> {
            self.0.get(key).await
        }
        async fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Bytes, ObjectError> {
            self.0.get_range(key, range).await
        }
        async fn head(&self, key: &Key) -> Result<ObjectMeta, ObjectError> {
            self.0.head(key).await
        }
        async fn put(&self, key: &Key, _: Bytes) -> Result<(), ObjectError> {
            Err(ObjectError::Unavailable {
                key: key.clone(),
                source: "read only".into(),
            })
        }
        async fn put_if_absent(&self, key: &Key, _: Bytes) -> Result<(), ObjectError> {
            Err(ObjectError::Unavailable {
                key: key.clone(),
                source: "read only".into(),
            })
        }
        fn list(&self, prefix: &Key) -> BoxStream<'_, Result<ObjectMeta, ObjectError>> {
            self.0.list(prefix)
        }
        async fn delete(&self, key: &Key) -> Result<(), ObjectError> {
            self.0.delete(key).await
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_write_that_did_not_land_is_retryable() {
        let log = log(Arc::new(ReadOnly(MemoryStore::new())), 1);
        let base = log.snapshot().await.unwrap();
        let error = log.commit(&base, vec![put("a", "1")]).await.unwrap_err();
        assert!(
            matches!(error, CommitError::Store(ref e) if e.is_retryable()),
            "{error}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_write_beaten_by_another_committer_is_a_conflict() {
        let inner = MemoryStore::new();
        let log = log(Arc::new(ReadOnly(inner.clone())), 1);
        let base = log.snapshot().await.unwrap();
        // Another node commits version 1 while our write is failing.
        let foreign = Commit {
            format: FORMAT,
            version: Version::new(1),
            id: CommitId::from_u128(42),
            committer: NodeId::from_u128(99),
            committed_at: Timestamp::UNIX_EPOCH,
            actions: vec![put("b", "2")],
        };
        inner
            .put(
                &prefix()
                    .join("_log")
                    .unwrap()
                    .join("00000000000000000001")
                    .unwrap(),
                foreign.encode(),
            )
            .await
            .unwrap();
        let error = log.commit(&base, vec![put("a", "1")]).await.unwrap_err();
        assert!(matches!(error, CommitError::Conflict { .. }), "{error}");
    }

    /// A store whose client sends a conditional write again when the
    /// first answer is lost: the write lands, and what comes back is
    /// the refusal of the second.
    #[derive(Debug)]
    struct Resent(MemoryStore);

    #[async_trait]
    impl ObjectStore for Resent {
        async fn get(&self, key: &Key) -> Result<Bytes, ObjectError> {
            self.0.get(key).await
        }
        async fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Bytes, ObjectError> {
            self.0.get_range(key, range).await
        }
        async fn head(&self, key: &Key) -> Result<ObjectMeta, ObjectError> {
            self.0.head(key).await
        }
        async fn put(&self, key: &Key, body: Bytes) -> Result<(), ObjectError> {
            self.0.put(key, body).await
        }
        async fn put_if_absent(&self, key: &Key, body: Bytes) -> Result<(), ObjectError> {
            self.0.put_if_absent(key, body.clone()).await?;
            self.0.put_if_absent(key, body).await
        }
        fn list(&self, prefix: &Key) -> BoxStream<'_, Result<ObjectMeta, ObjectError>> {
            self.0.list(prefix)
        }
        async fn delete(&self, key: &Key) -> Result<(), ObjectError> {
            self.0.delete(key).await
        }
    }

    #[tokio::test]
    async fn a_commit_found_present_on_a_resent_write_is_ours() {
        let log = log(Arc::new(Resent(MemoryStore::new())), 1);
        let base = log.snapshot().await.unwrap();
        let next = log.commit(&base, vec![put("a", "1")]).await.unwrap();
        assert_eq!(next.version(), Version::new(1));
        assert_eq!(next.state().get("a"), Some("1"));
    }

    #[tokio::test(start_paused = true)]
    async fn an_unreachable_store_leaves_the_outcome_unknown() {
        let faulty = FaultyStore::new(
            Arc::new(MemoryStore::new()),
            env(3).random,
            FaultPolicy {
                fail_before: 1.0,
                fail_after: 0.0,
            },
        );
        let log = log(Arc::new(faulty), 1);
        let base = Snapshot::initial();
        let error = log.commit(&base, vec![put("a", "1")]).await.unwrap_err();
        assert!(
            matches!(error, CommitError::Unknown { version } if version == Version::new(1)),
            "{error}"
        );
    }

    #[tokio::test]
    async fn update_rebases_on_a_stale_cached_snapshot() {
        let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
        let first = log(Arc::clone(&store), 1);
        let second = log(Arc::clone(&store), 2);
        let stale = second.snapshot().await.unwrap();
        let base = first.snapshot().await.unwrap();
        first.commit(&base, vec![put("a", "1")]).await.unwrap();
        let result = second
            .update(|state: &KvState| -> Result<Vec<Action>, ()> {
                let count = state.len().to_string();
                Ok(vec![put("count", &count)])
            })
            .await
            .unwrap();
        assert_eq!(stale.version(), Version::ZERO);
        assert_eq!(result.version(), Version::new(2));
        assert_eq!(result.state().get("a"), Some("1"));
        assert_eq!(
            result.state().get("count"),
            Some("1"),
            "the closure ran against the fresh state"
        );
    }

    #[tokio::test]
    async fn update_returns_a_rejection_without_committing() {
        let log = log(Arc::new(MemoryStore::new()), 1);
        let error = log
            .update(|_: &KvState| Err::<Vec<Action>, _>("nope"))
            .await
            .unwrap_err();
        assert!(matches!(error, UpdateError::Rejected("nope")));
        assert_eq!(log.snapshot().await.unwrap().version(), Version::ZERO);
    }

    #[tokio::test]
    async fn update_with_no_actions_commits_nothing() {
        let log = log(Arc::new(MemoryStore::new()), 1);
        let snapshot = log
            .update(|_: &KvState| Ok::<_, ()>(Vec::new()))
            .await
            .unwrap();
        assert_eq!(snapshot.version(), Version::ZERO);
    }

    #[tokio::test]
    async fn a_corrupt_commit_object_is_reported() {
        let store = MemoryStore::new();
        let key = prefix()
            .join("_log")
            .unwrap()
            .join("00000000000000000001")
            .unwrap();
        store
            .put(&key, Bytes::from_static(b"garbage"))
            .await
            .unwrap();
        let log = log(Arc::new(store), 1);
        let error = log.snapshot().await.unwrap_err();
        assert!(
            matches!(error, CommitError::Corrupt { key: ref k, .. } if *k == key),
            "{error}"
        );
    }
}
