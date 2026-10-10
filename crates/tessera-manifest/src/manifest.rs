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
use tessera_commit::{Action, ApplyError, CommitError, CommitLog, Snapshot, State, UpdateError};
use tessera_object::{ObjectStore, layout};
use tessera_types::{
    DatasetName, Env, FileId, FileMeta, Kind, PartitionId, Seq, TenantId, Window, WriterId,
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

/// The folded state of a manifest.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ManifestState {
    files: BTreeMap<FileId, FileMeta>,
    positions: BTreeMap<WriterId, Seq>,
    dropped: bool,
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
}

impl State for ManifestState {
    fn apply(&mut self, action: &Action) -> Result<(), ApplyError> {
        match action {
            Action::AddFile(meta) => {
                self.files.insert(meta.file_id, (**meta).clone());
            }
            Action::RemoveFile { file_id, .. } => {
                self.files.remove(file_id);
            }
            Action::AdvanceWriter { writer, through } => {
                let position = self.positions.entry(*writer).or_insert(Seq::ZERO);
                *position = (*position).max(*through);
            }
            Action::DropPartition => {
                self.dropped = true;
                self.files.clear();
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
}

impl Manifest {
    /// A handle to the manifest of `id` in `store` (the pool of the
    /// partition's dataset).
    #[must_use]
    pub fn open(store: Arc<dyn ObjectStore>, id: PartitionId, env: Env) -> Self {
        let log = CommitLog::new(store, layout::partition(&id), env);
        Manifest { id, log }
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
        let changed = std::cell::Cell::new(false);
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
                changed.set(true);
                Ok(actions)
            })
            .await?;
        Ok(changed.get())
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
    use tessera_types::{FileSource, Level, PoolId, Timestamp, Version};

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
}
