// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! One partition's manifest: a commit log of `AddFile`, `RemoveFile`,
//! `AdvanceWriter` and `DropPartition` actions (MF-4).
//!
//! Adoption is idempotent (IT-2): a request whose `through` is not beyond
//! the partition's current position for that writer is ignored, so intake
//! can be retried or run by two owners at once without duplicating files.

use std::collections::BTreeMap;
use std::sync::Arc;

use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use tessera_commit::{
    Action, ApplyError, CommitError, CommitLog, RemoveReason, Snapshot, State, UpdateError,
};
use tessera_object::{ObjectStore, layout};
use tessera_types::{
    DatasetName, Env, FileId, FileMeta, Kind, PartitionId, Seq, TenantId, Timestamp, Window,
    WriterId,
};

/// Why a manifest operation failed.
#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    /// The partition was dropped; nothing may be added to it.
    #[error("partition {0} is dropped")]
    Dropped(PartitionId),
    /// A file is tagged with another partition.
    #[error("file {file} belongs to {actual}, not to {expected}")]
    WrongPartition {
        /// The file.
        file: FileId,
        /// The partition it is tagged with.
        actual: Box<PartitionId>,
        /// The manifest it was offered to.
        expected: Box<PartitionId>,
    },
    /// Too many nodes changed the manifest at once; retry.
    #[error("manifest contention: gave up after {attempts} attempts")]
    Contention {
        /// How many attempts were made.
        attempts: u32,
    },
    /// An object cannot be decoded.
    #[error("object {key} is corrupt: {source}")]
    Corrupt {
        /// The offending object.
        key: tessera_object::Key,
        /// What is wrong with it.
        #[source]
        source: tessera_object::BoxError,
    },
    /// The manifest log failed.
    #[error(transparent)]
    Log(#[from] CommitError),
    /// A writer log could not be read.
    #[error(transparent)]
    Wal(#[from] tessera_wal::WalError),
    /// The object store failed.
    #[error(transparent)]
    Store(#[from] tessera_object::ObjectError),
}

impl From<UpdateError<ManifestError>> for ManifestError {
    fn from(error: UpdateError<ManifestError>) -> Self {
        match error {
            UpdateError::Rejected(rejection) => rejection,
            UpdateError::Commit(commit) => ManifestError::Log(commit),
            UpdateError::Contention { attempts } => ManifestError::Contention { attempts },
        }
    }
}

/// A removed file whose object still exists: deleted once no snapshot
/// within the retention window can reference it (GC-1).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Tombstone {
    /// The file as it was.
    pub meta: FileMeta,
    /// When it was removed, by the committer's clock.
    pub at: Timestamp,
}

/// The folded state of a manifest.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ManifestState {
    files: BTreeMap<FileId, FileMeta>,
    positions: BTreeMap<WriterId, Seq>,
    dropped: bool,
    #[serde(default)]
    removed: BTreeMap<FileId, Tombstone>,
}

impl ManifestState {
    /// The files of the partition, in id order.
    pub fn files(&self) -> impl Iterator<Item = &FileMeta> {
        self.files.values()
    }

    /// The file with the given id.
    #[must_use]
    pub fn file(&self, id: FileId) -> Option<&FileMeta> {
        self.files.get(&id)
    }

    /// The position up to which a writer's segments are represented by
    /// files here: zero if the writer was never adopted (section 6.8).
    #[must_use]
    pub fn position(&self, writer: WriterId) -> Seq {
        self.positions.get(&writer).copied().unwrap_or(Seq::ZERO)
    }

    /// Every writer with a recorded position.
    pub fn positions(&self) -> impl Iterator<Item = (WriterId, Seq)> + '_ {
        self.positions.iter().map(|(w, s)| (*w, *s))
    }

    /// Whether the partition was dropped (MF-6).
    #[must_use]
    pub fn is_dropped(&self) -> bool {
        self.dropped
    }

    /// Removed files whose objects are still to be deleted, in id order.
    pub fn tombstones(&self) -> impl Iterator<Item = &Tombstone> {
        self.removed.values()
    }
}

impl State for ManifestState {
    fn apply(&mut self, action: &Action) -> Result<(), ApplyError> {
        match action {
            Action::AddFile(meta) => {
                self.files.insert(meta.file_id, (**meta).clone());
            }
            Action::RemoveFile { file_id, at, .. } => {
                if let Some(meta) = self.files.remove(file_id) {
                    self.removed.insert(*file_id, Tombstone { meta, at: *at });
                }
            }
            Action::Forget { file_ids } => {
                for file_id in file_ids {
                    self.removed.remove(file_id);
                }
            }
            Action::AdvanceWriter { writer, through } => {
                let position = self.positions.entry(*writer).or_insert(Seq::ZERO);
                *position = (*position).max(*through);
            }
            Action::DropPartition => {
                self.dropped = true;
                // Dropped files become tombstones so that their objects are
                // still collected (MF-6).
                let at = Timestamp::UNIX_EPOCH;
                for (file_id, meta) in std::mem::take(&mut self.files) {
                    self.removed.insert(file_id, Tombstone { meta, at });
                }
            }
            Action::Kv(_) => return Err(ApplyError::new("a manifest has no key-value actions")),
        }
        Ok(())
    }
}

/// A handle to one partition's manifest.
pub struct Manifest {
    id: PartitionId,
    log: CommitLog<ManifestState>,
    env: Env,
}

impl Manifest {
    /// A handle to the manifest of `id` in `store` (the pool of the
    /// partition's dataset).
    #[must_use]
    pub fn open(store: Arc<dyn ObjectStore>, id: PartitionId, env: Env) -> Self {
        let log = CommitLog::new(store, layout::partition(&id), env.clone());
        Manifest { id, log, env }
    }

    /// The partition.
    #[must_use]
    pub fn id(&self) -> &PartitionId {
        &self.id
    }

    /// The latest state, read from the store.
    ///
    /// # Errors
    ///
    /// If the log cannot be read.
    pub async fn snapshot(&self) -> Result<Arc<Snapshot<ManifestState>>, ManifestError> {
        Ok(self.log.snapshot().await?)
    }

    /// Adopts a writer's files: adds `files` and advances the writer's
    /// position to `through`, in one commit (IT-1). Ignored, returning
    /// `false`, if the position is already at or beyond `through` (IT-2).
    ///
    /// # Errors
    ///
    /// `Dropped`, `WrongPartition`, or a log failure.
    pub async fn adopt(
        &self,
        writer: WriterId,
        through: Seq,
        files: &[FileMeta],
    ) -> Result<bool, ManifestError> {
        if let Some(wrong) = files.iter().find(|f| f.partition != self.id) {
            return Err(ManifestError::WrongPartition {
                file: wrong.file_id,
                actual: Box::new(wrong.partition.clone()),
                expected: Box::new(self.id.clone()),
            });
        }
        // Shared with the decision closure; atomic so that the future stays
        // `Send`.
        let changed = std::sync::atomic::AtomicBool::new(false);
        self.log
            .update(|state| {
                if state.is_dropped() {
                    return Err(ManifestError::Dropped(self.id.clone()));
                }
                if state.position(writer) >= through {
                    return Ok(Vec::new());
                }
                let mut actions: Vec<Action> = files
                    .iter()
                    .filter(|f| state.file(f.file_id).is_none())
                    .map(|f| Action::AddFile(Box::new(f.clone())))
                    .collect();
                actions.push(Action::AdvanceWriter { writer, through });
                changed.store(true, std::sync::atomic::Ordering::Relaxed);
                Ok(actions)
            })
            .await?;
        Ok(changed.load(std::sync::atomic::Ordering::Relaxed))
    }

    /// Replaces `inputs` with `outputs` in one commit: the result of a
    /// compaction (CP-1). Returns `false` without committing when some
    /// input is no longer in the manifest, which means the job was
    /// superseded and the outputs are garbage (CP-4).
    ///
    /// # Errors
    ///
    /// `Dropped`, `WrongPartition`, or a log failure.
    pub async fn replace(
        &self,
        inputs: &[FileId],
        outputs: &[FileMeta],
    ) -> Result<bool, ManifestError> {
        if let Some(wrong) = outputs.iter().find(|f| f.partition != self.id) {
            return Err(ManifestError::WrongPartition {
                file: wrong.file_id,
                actual: Box::new(wrong.partition.clone()),
                expected: Box::new(self.id.clone()),
            });
        }
        let at = self.env.clock.now();
        let committed = std::sync::atomic::AtomicBool::new(false);
        self.log
            .update(|state| {
                if state.is_dropped() {
                    return Err(ManifestError::Dropped(self.id.clone()));
                }
                if inputs.iter().any(|id| state.file(*id).is_none()) {
                    return Ok(Vec::new());
                }
                let mut actions: Vec<Action> = inputs
                    .iter()
                    .map(|id| Action::RemoveFile {
                        file_id: *id,
                        reason: RemoveReason::Compacted,
                        at,
                    })
                    .collect();
                actions.extend(outputs.iter().map(|f| Action::AddFile(Box::new(f.clone()))));
                committed.store(true, std::sync::atomic::Ordering::Relaxed);
                Ok(actions)
            })
            .await?;
        Ok(committed.load(std::sync::atomic::Ordering::Relaxed))
    }

    /// Drops the tombstones of files whose objects have been deleted
    /// (GC-3). Tombstones already gone are ignored.
    ///
    /// # Errors
    ///
    /// A log failure.
    pub async fn forget(&self, file_ids: &[FileId]) -> Result<(), ManifestError> {
        self.log
            .update(|state| {
                let present: Vec<FileId> = file_ids
                    .iter()
                    .copied()
                    .filter(|id| state.removed.contains_key(id))
                    .collect();
                if present.is_empty() {
                    return Ok::<_, ManifestError>(Vec::new());
                }
                Ok(vec![Action::Forget { file_ids: present }])
            })
            .await?;
        Ok(())
    }

    /// Drops the partition (MF-6).
    ///
    /// # Errors
    ///
    /// A log failure.
    pub async fn drop_partition(&self) -> Result<(), ManifestError> {
        self.log
            .update(|state| {
                Ok::<_, ManifestError>(if state.is_dropped() {
                    Vec::new()
                } else {
                    vec![Action::DropPartition]
                })
            })
            .await?;
        Ok(())
    }

    /// Writes a checkpoint of the latest state.
    ///
    /// # Errors
    ///
    /// If the log cannot be read or the checkpoint written.
    pub async fn checkpoint(&self) -> Result<(), ManifestError> {
        let snapshot = self.log.snapshot().await?;
        Ok(self.log.checkpoint(&snapshot).await?)
    }
}

impl std::fmt::Debug for Manifest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Manifest")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

/// The partitions of one dataset and kind that exist (MF-3): those whose
/// manifest log has at least one commit, in window order.
///
/// # Errors
///
/// If the store cannot be listed.
pub async fn list_partitions(
    store: &dyn ObjectStore,
    tenant: &TenantId,
    dataset: &DatasetName,
    kind: Kind,
) -> Result<Vec<PartitionId>, ManifestError> {
    let prefix = layout::partitions(tenant, dataset, kind);
    let mut windows: Vec<Window> = Vec::new();
    let mut listing = store.list(&prefix);
    while let Some(meta) = listing.try_next().await? {
        let rest = meta
            .key
            .as_str()
            .strip_prefix(prefix.as_str())
            .and_then(|r| r.strip_prefix('/'));
        let Some((window, tail)) = rest.and_then(|r| r.split_once('/')) else {
            continue;
        };
        if !tail.starts_with("_log/") {
            continue;
        }
        if let Ok(window) = window.parse::<Window>() {
            if !windows.contains(&window) {
                windows.push(window);
            }
        }
    }
    windows.sort_unstable();
    Ok(windows
        .into_iter()
        .map(|window| PartitionId {
            tenant: tenant.clone(),
            dataset: dataset.clone(),
            kind,
            window,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use tessera_object::MemoryStore;
    use tessera_types::{FileSource, Level, PoolId, Version};

    use super::*;

    fn partition(start_seconds: i64) -> PartitionId {
        PartitionId {
            tenant: TenantId::DEFAULT,
            dataset: DatasetName::new("prod").unwrap(),
            kind: Kind::Log,
            window: Window::containing(
                Timestamp::from_nanos(start_seconds * 1_000_000_000),
                Window::DEFAULT_LENGTH,
            )
            .unwrap(),
        }
    }

    fn file(id: u128, partition: &PartitionId, writer: WriterId, through: u64) -> FileMeta {
        FileMeta {
            file_id: FileId::from_u128(id),
            partition: partition.clone(),
            pool: PoolId::new("root").unwrap(),
            level: Level::L1,
            source: FileSource::Writer {
                writer,
                after: Seq::ZERO,
                through: Seq::new(through),
            },
            min_timestamp: Timestamp::UNIX_EPOCH,
            max_timestamp: Timestamp::UNIX_EPOCH,
            row_count: 1,
            size_bytes: 1,
            layout_epoch: 0,
            sort_key: vec!["timestamp".into()],
            columns: Vec::new(),
            indexes: Vec::new(),
        }
    }

    fn env(seed: u64) -> Env {
        Env::simulated(seed, Timestamp::UNIX_EPOCH)
    }

    #[tokio::test]
    async fn adoption_adds_files_and_advances_the_writer_in_one_commit() {
        let store = MemoryStore::new();
        let manifest = Manifest::open(Arc::new(store.clone()), partition(0), env(1));
        let writer = WriterId::from_u128(1);
        let files = [
            file(1, &partition(0), writer, 3),
            file(2, &partition(0), writer, 3),
        ];
        assert!(manifest.adopt(writer, Seq::new(3), &files).await.unwrap());
        let state = manifest.snapshot().await.unwrap();
        assert_eq!(state.version(), Version::new(1));
        assert_eq!(state.state().files().count(), 2);
        assert_eq!(state.state().position(writer), Seq::new(3));
        assert_eq!(state.state().position(WriterId::from_u128(9)), Seq::ZERO);
    }

    #[tokio::test]
    async fn adoption_is_idempotent() {
        let store = MemoryStore::new();
        let first = Manifest::open(Arc::new(store.clone()), partition(0), env(1));
        let second = Manifest::open(Arc::new(store.clone()), partition(0), env(2));
        let writer = WriterId::from_u128(1);
        let files = [file(1, &partition(0), writer, 3)];
        assert!(first.adopt(writer, Seq::new(3), &files).await.unwrap());
        assert!(
            !second.adopt(writer, Seq::new(3), &files).await.unwrap(),
            "a duplicate owner changes nothing"
        );
        assert!(
            !first.adopt(writer, Seq::new(2), &files).await.unwrap(),
            "an older request changes nothing"
        );
        let state = second.snapshot().await.unwrap();
        assert_eq!(state.version(), Version::new(1));
        assert_eq!(state.state().files().count(), 1);
    }

    #[tokio::test]
    async fn a_later_range_adds_only_new_files() {
        let store = MemoryStore::new();
        let manifest = Manifest::open(Arc::new(store.clone()), partition(0), env(1));
        let writer = WriterId::from_u128(1);
        manifest
            .adopt(writer, Seq::new(3), &[file(1, &partition(0), writer, 3)])
            .await
            .unwrap();
        manifest
            .adopt(
                writer,
                Seq::new(6),
                &[
                    file(1, &partition(0), writer, 3),
                    file(2, &partition(0), writer, 6),
                ],
            )
            .await
            .unwrap();
        let state = manifest.snapshot().await.unwrap();
        assert_eq!(state.state().files().count(), 2);
        assert_eq!(state.state().position(writer), Seq::new(6));
    }

    #[tokio::test]
    async fn files_of_another_partition_are_refused() {
        let store = MemoryStore::new();
        let manifest = Manifest::open(Arc::new(store.clone()), partition(0), env(1));
        let writer = WriterId::from_u128(1);
        let error = manifest
            .adopt(
                writer,
                Seq::new(1),
                &[file(1, &partition(100_000), writer, 1)],
            )
            .await
            .unwrap_err();
        assert!(
            matches!(error, ManifestError::WrongPartition { .. }),
            "{error}"
        );
    }

    #[tokio::test]
    async fn a_dropped_partition_refuses_adoption() {
        let store = MemoryStore::new();
        let manifest = Manifest::open(Arc::new(store.clone()), partition(0), env(1));
        let writer = WriterId::from_u128(1);
        manifest
            .adopt(writer, Seq::new(1), &[file(1, &partition(0), writer, 1)])
            .await
            .unwrap();
        manifest.drop_partition().await.unwrap();
        let state = manifest.snapshot().await.unwrap();
        assert!(state.state().is_dropped());
        assert_eq!(state.state().files().count(), 0);
        let error = manifest
            .adopt(writer, Seq::new(2), &[file(2, &partition(0), writer, 2)])
            .await
            .unwrap_err();
        assert!(matches!(error, ManifestError::Dropped(_)), "{error}");
    }

    #[tokio::test]
    async fn partitions_are_discovered_by_listing() {
        let store = MemoryStore::new();
        let writer = WriterId::from_u128(1);
        for start in [100_000, 0] {
            let manifest = Manifest::open(Arc::new(store.clone()), partition(start), env(1));
            manifest
                .adopt(
                    writer,
                    Seq::new(1),
                    &[file(1, &partition(start), writer, 1)],
                )
                .await
                .unwrap();
        }
        let found = list_partitions(
            &store,
            &TenantId::DEFAULT,
            &DatasetName::new("prod").unwrap(),
            Kind::Log,
        )
        .await
        .unwrap();
        assert_eq!(found, [partition(0), partition(100_000)]);
        let none = list_partitions(
            &store,
            &TenantId::DEFAULT,
            &DatasetName::new("prod").unwrap(),
            Kind::Span,
        )
        .await
        .unwrap();
        assert!(none.is_empty());
    }

    #[tokio::test]
    async fn a_checkpoint_serves_a_cold_handle() {
        let store = MemoryStore::new();
        let manifest = Manifest::open(Arc::new(store.clone()), partition(0), env(1));
        let writer = WriterId::from_u128(1);
        manifest
            .adopt(writer, Seq::new(1), &[file(1, &partition(0), writer, 1)])
            .await
            .unwrap();
        manifest.checkpoint().await.unwrap();
        let cold = Manifest::open(Arc::new(store.clone()), partition(0), env(2));
        assert_eq!(cold.snapshot().await.unwrap().state().files().count(), 1);
    }

    #[tokio::test]
    async fn replace_commits_outputs_and_keeps_tombstones() {
        let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
        let env = Env::simulated(1, Timestamp::from_nanos(1_000));
        let partition = partition(0);
        let manifest = Manifest::open(Arc::clone(&store), partition.clone(), env);
        let writer = WriterId::from_u128(1);
        let a = file(1, &partition, writer, 1);
        let b = file(2, &partition, writer, 2);
        manifest
            .adopt(writer, Seq::new(2), &[a.clone(), b.clone()])
            .await
            .unwrap();
        let mut out = file(3, &partition, writer, 2);
        out.level = Level::L2;
        assert!(
            manifest
                .replace(&[a.file_id, b.file_id], &[out.clone()])
                .await
                .unwrap()
        );
        let state = manifest.snapshot().await.unwrap();
        let ids: Vec<FileId> = state.state().files().map(|f| f.file_id).collect();
        assert_eq!(ids, vec![out.file_id]);
        let tombstones: Vec<(FileId, Timestamp)> = state
            .state()
            .tombstones()
            .map(|t| (t.meta.file_id, t.at))
            .collect();
        assert_eq!(
            tombstones,
            vec![
                (a.file_id, Timestamp::from_nanos(1_000)),
                (b.file_id, Timestamp::from_nanos(1_000))
            ]
        );

        // A superseded job finds its inputs gone and commits nothing.
        assert!(
            !manifest
                .replace(&[a.file_id], &[file(4, &partition, writer, 2)])
                .await
                .unwrap()
        );
        assert_eq!(
            manifest.snapshot().await.unwrap().version(),
            state.version()
        );

        manifest
            .forget(&[a.file_id, FileId::from_u128(99)])
            .await
            .unwrap();
        let state = manifest.snapshot().await.unwrap();
        assert_eq!(state.state().tombstones().count(), 1);
        manifest.forget(&[FileId::from_u128(99)]).await.unwrap();
        assert_eq!(
            manifest.snapshot().await.unwrap().version(),
            state.version(),
            "nothing to forget commits nothing"
        );
    }

    #[tokio::test]
    async fn dropping_a_partition_leaves_tombstones_for_its_files() {
        let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
        let partition = partition(0);
        let manifest = Manifest::open(
            Arc::clone(&store),
            partition.clone(),
            Env::simulated(1, Timestamp::UNIX_EPOCH),
        );
        let writer = WriterId::from_u128(1);
        manifest
            .adopt(writer, Seq::new(1), &[file(1, &partition, writer, 1)])
            .await
            .unwrap();
        manifest.drop_partition().await.unwrap();
        let state = manifest.snapshot().await.unwrap();
        assert!(state.state().is_dropped());
        assert_eq!(state.state().files().count(), 0);
        assert_eq!(state.state().tombstones().count(), 1);
    }
}
