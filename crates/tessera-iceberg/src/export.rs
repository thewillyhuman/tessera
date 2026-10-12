// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The export of one table: the files its partition manifests hold,
//! published as a new snapshot when they changed since the last one.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use tessera_manifest::{Manifest, ManifestError, list_partitions};
use tessera_object::{Key, ObjectError, ObjectStore, layout};
use tessera_types::event::Scalar;
use tessera_types::{DatasetName, Env, FileMeta, Kind, Random, TenantId};

use crate::manifest::{self, DataFile};
use crate::metadata::{
    self, MetadataLog, PartitionSpec, Reference, Snapshot, SnapshotLog, SortOrder, TableMetadata,
};
use crate::schema::TableSchema;

/// The property holding a digest of the files a snapshot lists, by
/// which the next export knows whether anything changed.
pub const FILES_PROPERTY: &str = "tessera.files";

/// Why an export failed.
#[derive(Debug, thiserror::Error)]
pub enum ExportError {
    /// The object store failed.
    #[error(transparent)]
    Store(#[from] ObjectError),
    /// A partition manifest could not be read.
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    /// An earlier export's metadata cannot be read.
    #[error("object {key} is corrupt: {reason}")]
    Corrupt {
        /// The offending object.
        key: Key,
        /// What is wrong with it.
        reason: String,
    },
}

/// What an export did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// There is no table: no file was ever adopted.
    Empty,
    /// The files are the ones the current snapshot lists.
    Unchanged,
    /// A new version was published.
    Published {
        /// The version.
        version: u64,
        /// How many files it lists.
        files: usize,
    },
    /// Another node published this version first; the next export
    /// builds on theirs.
    Lost {
        /// The version.
        version: u64,
    },
}

/// Publishes Iceberg metadata into the store the files live in, with
/// the files' locations under `location`, the store's URI as a reader
/// sees it.
pub struct Exporter {
    store: Arc<dyn ObjectStore>,
    location: String,
    env: Env,
}

impl Exporter {
    /// An exporter into `store`, whose objects a reader finds under
    /// `location` (`s3://bucket/prefix`, `file:///var/lib/tessera`).
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, location: &str, env: Env) -> Self {
        Exporter {
            store,
            location: location.trim_end_matches('/').to_owned(),
            env,
        }
    }

    /// The location URI readers find the store's objects under.
    #[must_use]
    pub fn location(&self) -> &str {
        &self.location
    }

    /// The slot item of a table's export: the node owning it, among
    /// the partition slots (AR-4), exports the table.
    #[must_use]
    pub fn item(tenant: &TenantId, dataset: &DatasetName, kind: Kind) -> String {
        layout::iceberg_table(tenant, dataset, kind)
            .as_str()
            .to_owned()
    }

    /// Exports the table of `tenant`, `dataset` and `kind`: a new
    /// version when the adopted files changed since the current one.
    ///
    /// # Errors
    ///
    /// If the manifests or the store cannot be read or written.
    pub async fn export(
        &self,
        tenant: &TenantId,
        dataset: &DatasetName,
        kind: Kind,
    ) -> Result<Outcome, ExportError> {
        let table = layout::iceberg_table(tenant, dataset, kind);
        let files = self.adopted_files(tenant, dataset, kind).await?;
        let digest = digest(&files);
        let previous = metadata::current(&*self.store, &table).await?;
        match &previous {
            Some((_, metadata)) if metadata.properties.get(FILES_PROPERTY) == Some(&digest) => {
                return Ok(Outcome::Unchanged);
            }
            None if files.is_empty() => return Ok(Outcome::Empty),
            _ => {}
        }
        let (previous_version, previous) = previous.unzip();
        let version = previous_version.map_or(1, |v| v + 1);
        let schema = TableSchema::of(
            kind,
            &columns(&files),
            &samples(&files),
            previous.as_ref().and_then(TableMetadata::current_schema),
        );
        let snapshot = manifest::Snapshot {
            id: i64::try_from(version).unwrap_or(i64::MAX),
            sequence: i64::try_from(version).unwrap_or(i64::MAX),
            first_row_id: previous.as_ref().map_or(0, |m| m.next_row_id),
        };
        let location = format!("{}/{}", self.location, table.as_str());
        let data_files: Vec<DataFile<'_>> = files
            .iter()
            .map(|meta| DataFile {
                path: format!(
                    "{}/{}",
                    self.location,
                    layout::data_file(&meta.partition, meta.file_id).as_str()
                ),
                meta,
            })
            .collect();
        let rows: u64 = files.iter().map(|f| f.row_count).sum();

        let manifest_name = metadata::manifest_file(version);
        let manifest = manifest::manifest(&schema, snapshot, &data_files);
        let manifest_length = manifest.len();
        self.store
            .put(&metadata::key(&table, &manifest_name), manifest.into())
            .await?;
        let list_name = metadata::manifest_list_file(version);
        let list = manifest::manifest_list(
            &format!("{location}/metadata/{manifest_name}"),
            manifest_length,
            snapshot,
            files.len(),
            rows,
        );
        self.store
            .put(&metadata::key(&table, &list_name), list.into())
            .await?;

        let now_ms = self.env.clock.now().as_nanos() / 1_000_000;
        let metadata = self.table_metadata(
            previous.as_ref(),
            previous_version,
            &location,
            &schema,
            snapshot,
            &list_name,
            files.len(),
            rows,
            now_ms,
            digest,
        );
        let json = serde_json::to_vec_pretty(&metadata)
            .unwrap_or_else(|e| unreachable!("table metadata is JSON: {e}"));
        let metadata_key = metadata::key(&table, &metadata::metadata_file(version));
        match self.store.put_if_absent(&metadata_key, json.into()).await {
            Ok(()) => {}
            Err(ObjectError::AlreadyExists { .. }) => return Ok(Outcome::Lost { version }),
            Err(error) => return Err(error.into()),
        }
        self.store
            .put(
                &metadata::key(&table, metadata::HINT),
                version.to_string().into(),
            )
            .await?;
        self.retire(&table, version).await;
        Ok(Outcome::Published {
            version,
            files: files.len(),
        })
    }

    /// Every live file of every partition of the table, oldest first.
    async fn adopted_files(
        &self,
        tenant: &TenantId,
        dataset: &DatasetName,
        kind: Kind,
    ) -> Result<Vec<FileMeta>, ExportError> {
        let mut files = Vec::new();
        for partition in list_partitions(&*self.store, tenant, dataset, kind).await? {
            let manifest = Manifest::open(Arc::clone(&self.store), partition, self.env.clone());
            let snapshot = manifest.snapshot().await?;
            if snapshot.state().is_dropped() {
                continue;
            }
            files.extend(snapshot.state().files().cloned());
        }
        files.sort_by_key(|f| (f.min_timestamp, f.file_id));
        Ok(files)
    }

    #[allow(clippy::too_many_arguments, reason = "every part of one metadata file")]
    fn table_metadata(
        &self,
        previous: Option<&TableMetadata>,
        previous_version: Option<u64>,
        location: &str,
        schema: &TableSchema,
        snapshot: manifest::Snapshot,
        list_name: &str,
        files: usize,
        rows: u64,
        now_ms: i64,
        digest: String,
    ) -> TableMetadata {
        let rows = i64::try_from(rows).unwrap_or(i64::MAX);
        let properties = BTreeMap::from([
            (
                "schema.name-mapping.default".to_owned(),
                schema.name_mapping(),
            ),
            ("write.format.default".to_owned(), "parquet".to_owned()),
            (FILES_PROPERTY.to_owned(), digest),
        ]);
        let summary = BTreeMap::from([
            ("operation".to_owned(), "overwrite".to_owned()),
            ("added-data-files".to_owned(), files.to_string()),
            ("added-records".to_owned(), rows.to_string()),
            ("total-data-files".to_owned(), files.to_string()),
            ("total-records".to_owned(), rows.to_string()),
        ]);
        TableMetadata {
            format_version: 3,
            table_uuid: previous.map_or_else(
                || uuid(&mut self.env.random.fork()),
                |m| m.table_uuid.clone(),
            ),
            location: location.to_owned(),
            last_sequence_number: snapshot.sequence,
            last_updated_ms: now_ms,
            last_column_id: schema.last_column_id(),
            current_schema_id: schema.schema_id,
            schemas: vec![schema.clone()],
            default_spec_id: 0,
            partition_specs: vec![PartitionSpec {
                spec_id: 0,
                fields: Vec::new(),
            }],
            last_partition_id: 999,
            default_sort_order_id: 0,
            sort_orders: vec![SortOrder {
                order_id: 0,
                fields: Vec::new(),
            }],
            properties,
            current_snapshot_id: snapshot.id,
            snapshots: vec![Snapshot {
                snapshot_id: snapshot.id,
                sequence_number: snapshot.sequence,
                timestamp_ms: now_ms,
                manifest_list: format!("{location}/metadata/{list_name}"),
                summary,
                schema_id: schema.schema_id,
                first_row_id: snapshot.first_row_id,
                added_rows: rows,
            }],
            snapshot_log: vec![SnapshotLog {
                timestamp_ms: now_ms,
                snapshot_id: snapshot.id,
            }],
            metadata_log: previous
                .zip(previous_version)
                .map(|(m, v)| {
                    vec![MetadataLog {
                        timestamp_ms: m.last_updated_ms,
                        metadata_file: format!(
                            "{location}/metadata/{}",
                            metadata::metadata_file(v)
                        ),
                    }]
                })
                .unwrap_or_default(),
            refs: BTreeMap::from([(
                "main".to_owned(),
                Reference {
                    snapshot_id: snapshot.id,
                    kind: "branch".to_owned(),
                },
            )]),
            next_row_id: snapshot.first_row_id.saturating_add(rows),
        }
    }

    /// Removes the files of the version before the previous one: the
    /// previous stays for a reader that followed the hint before the
    /// new one was written. Failures are left to the next export.
    async fn retire(&self, table: &Key, version: u64) {
        let Some(old) = version.checked_sub(2).filter(|v| *v >= 1) else {
            return;
        };
        for name in [
            metadata::metadata_file(old),
            metadata::manifest_list_file(old),
            metadata::manifest_file(old),
        ] {
            let _ = self.store.delete(&metadata::key(table, &name)).await;
        }
    }
}

impl std::fmt::Debug for Exporter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Exporter")
            .field("location", &self.location)
            .finish_non_exhaustive()
    }
}

/// The dedicated columns the files hold.
fn columns(files: &[FileMeta]) -> BTreeSet<String> {
    files
        .iter()
        .flat_map(|f| f.columns.iter().map(|c| c.name.clone()))
        .collect()
}

/// A value seen in each column that has one, for its type.
fn samples(files: &[FileMeta]) -> BTreeMap<String, Scalar> {
    let mut samples = BTreeMap::new();
    for stats in files.iter().flat_map(|f| f.columns.iter()) {
        if let Some(value) = stats.min.as_ref().or(stats.max.as_ref()) {
            samples
                .entry(stats.name.clone())
                .or_insert_with(|| value.clone());
        }
    }
    samples
}

/// A digest of the file set: its size and an FNV-1a hash of the ids
/// in order, stable across releases.
fn digest(files: &[FileMeta]) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for file in files {
        for byte in file.file_id.to_string().bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
    }
    format!("{}-{hash:016x}", files.len())
}

/// A random UUID in its canonical text form.
fn uuid(random: &mut impl Random) -> String {
    let hex = format!("{:032x}", random.next_u128());
    format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    )
}

#[cfg(test)]
mod tests {
    use apache_avro::Reader;
    use apache_avro::types::Value as Read;
    use tessera_object::MemoryStore;
    use tessera_types::event::TIMESTAMP;
    use tessera_types::{
        ColumnStats, FileId, FileSource, Level, PartitionId, PoolId, Seq, Timestamp, Window,
        WriterId,
    };

    use super::*;

    const DAY: i64 = 86_400 * 1_000_000_000;

    fn dataset() -> DatasetName {
        DatasetName::new("prod").unwrap()
    }

    fn partition(day: i64) -> PartitionId {
        PartitionId {
            tenant: TenantId::DEFAULT,
            dataset: dataset(),
            kind: Kind::Log,
            window: Window::containing(Timestamp::from_nanos(day * DAY), Window::DEFAULT_LENGTH)
                .unwrap(),
        }
    }

    fn file(id: u128, day: i64, columns: &[&str]) -> FileMeta {
        FileMeta {
            file_id: FileId::from_u128(id),
            partition: partition(day),
            pool: PoolId::new("root").unwrap(),
            level: Level::L1,
            source: FileSource::Writer {
                writer: WriterId::from_u128(1),
                after: Seq::ZERO,
                through: Seq::new(u64::try_from(id).unwrap()),
            },
            min_timestamp: Timestamp::from_nanos(day * DAY + 1),
            max_timestamp: Timestamp::from_nanos(day * DAY + 2),
            row_count: 10,
            size_bytes: 100,
            layout_epoch: 0,
            sort_key: vec![TIMESTAMP.into()],
            columns: columns
                .iter()
                .map(|name| ColumnStats {
                    name: (*name).to_owned(),
                    min: Some(Scalar::Int(1)),
                    max: Some(Scalar::Int(2)),
                    null_count: 0,
                    distinct: Some(2),
                    quantiles: Vec::new(),
                })
                .collect(),
            indexes: Vec::new(),
            ingested_at: None,
        }
    }

    async fn adopt(store: &MemoryStore, env: &Env, file: FileMeta) {
        let manifest = Manifest::open(Arc::new(store.clone()), file.partition.clone(), env.clone());
        let FileSource::Writer {
            writer, through, ..
        } = file.source
        else {
            unreachable!()
        };
        manifest.adopt(writer, through, &[file]).await.unwrap();
    }

    fn exporter(store: &MemoryStore, env: &Env) -> Exporter {
        Exporter::new(Arc::new(store.clone()), "file:///data/", env.clone())
    }

    async fn current(store: &MemoryStore) -> (u64, TableMetadata) {
        let table = layout::iceberg_table(&TenantId::DEFAULT, &dataset(), Kind::Log);
        metadata::current(store, &table).await.unwrap().unwrap()
    }

    async fn manifest_paths(store: &MemoryStore, version: u64) -> Vec<String> {
        let table = layout::iceberg_table(&TenantId::DEFAULT, &dataset(), Kind::Log);
        let bytes = store
            .get(&metadata::key(&table, &metadata::manifest_file(version)))
            .await
            .unwrap();
        Reader::new(&bytes[..])
            .unwrap()
            .map(Result::unwrap)
            .map(|entry| match entry {
                Read::Record(fields) => match &fields[4].1 {
                    Read::Record(data) => match &data[1].1 {
                        Read::String(path) => path.clone(),
                        other => panic!("{other:?}"),
                    },
                    other => panic!("{other:?}"),
                },
                other => panic!("{other:?}"),
            })
            .collect()
    }

    #[tokio::test]
    async fn the_first_export_publishes_every_adopted_file() {
        let store = MemoryStore::new();
        let env = Env::simulated(1, Timestamp::from_nanos(5 * DAY));
        adopt(&store, &env, file(1, 0, &["attr.code#i64"])).await;
        adopt(&store, &env, file(2, 1, &[])).await;
        let outcome = exporter(&store, &env)
            .export(&TenantId::DEFAULT, &dataset(), Kind::Log)
            .await
            .unwrap();
        assert_eq!(
            outcome,
            Outcome::Published {
                version: 1,
                files: 2
            }
        );
        let (version, metadata) = current(&store).await;
        assert_eq!(version, 1);
        assert_eq!(metadata.format_version, 3);
        assert_eq!(metadata.location, "file:///data/default/prod/iceberg/log");
        assert_eq!(metadata.current_snapshot_id, 1);
        assert_eq!(
            metadata.snapshots[0].manifest_list,
            "file:///data/default/prod/iceberg/log/metadata/snap-1.avro"
        );
        assert_eq!(metadata.snapshots[0].added_rows, 20);
        assert_eq!(metadata.next_row_id, 20);
        assert_eq!(metadata.last_updated_ms, 5 * 86_400 * 1_000);
        let schema = metadata.current_schema().unwrap();
        assert_eq!(schema.field(TIMESTAMP).unwrap().id, 1);
        assert_eq!(schema.field("attr.code#i64").unwrap().type_name, "long");
        assert!(metadata.properties["schema.name-mapping.default"].contains("attr.code#i64"));
        assert!(metadata.metadata_log.is_empty());
        let paths = manifest_paths(&store, 1).await;
        assert_eq!(paths.len(), 2);
        assert!(
            paths[0].starts_with("file:///data/default/prod/p/log/")
                && paths[0].ends_with(".parquet"),
            "{paths:?}"
        );
    }

    #[tokio::test]
    async fn an_unchanged_table_and_an_empty_one_are_not_published() {
        let store = MemoryStore::new();
        let env = Env::simulated(1, Timestamp::UNIX_EPOCH);
        let exporter = exporter(&store, &env);
        let table = (TenantId::DEFAULT, dataset(), Kind::Log);
        assert_eq!(
            exporter.export(&table.0, &table.1, table.2).await.unwrap(),
            Outcome::Empty,
            "no files, no table"
        );
        adopt(&store, &env, file(1, 0, &[])).await;
        assert!(matches!(
            exporter.export(&table.0, &table.1, table.2).await.unwrap(),
            Outcome::Published { version: 1, .. }
        ));
        assert_eq!(
            exporter.export(&table.0, &table.1, table.2).await.unwrap(),
            Outcome::Unchanged
        );
        assert_eq!(current(&store).await.0, 1);
    }

    #[tokio::test]
    async fn later_exports_keep_ids_keep_one_old_version_and_retire_the_rest() {
        let store = MemoryStore::new();
        let env = Env::simulated(1, Timestamp::UNIX_EPOCH);
        let exporter = exporter(&store, &env);
        let prod = dataset();
        let export = || exporter.export(&TenantId::DEFAULT, &prod, Kind::Log);
        adopt(&store, &env, file(1, 0, &["attr.b#str"])).await;
        export().await.unwrap();
        let (_, first) = current(&store).await;
        adopt(&store, &env, file(2, 1, &["attr.a#str"])).await;
        assert_eq!(
            export().await.unwrap(),
            Outcome::Published {
                version: 2,
                files: 2
            }
        );
        let (_, second) = current(&store).await;
        assert_eq!(second.table_uuid, first.table_uuid);
        assert_eq!(second.current_schema_id, 1);
        let b = |m: &TableMetadata| m.current_schema().unwrap().field("attr.b#str").unwrap().id;
        assert_eq!(b(&second), b(&first));
        assert_eq!(second.snapshots[0].first_row_id, 10);
        assert_eq!(second.next_row_id, 30);
        assert_eq!(
            second.metadata_log[0].metadata_file,
            "file:///data/default/prod/iceberg/log/metadata/v1.metadata.json"
        );
        adopt(&store, &env, file(3, 2, &[])).await;
        assert_eq!(
            export().await.unwrap(),
            Outcome::Published {
                version: 3,
                files: 3
            }
        );
        let table = layout::iceberg_table(&TenantId::DEFAULT, &dataset(), Kind::Log);
        let exists = |name: String| {
            let store = store.clone();
            let key = metadata::key(&table, &name);
            async move { store.head(&key).await.is_ok() }
        };
        assert!(!exists(metadata::metadata_file(1)).await, "v1 retired");
        assert!(!exists(metadata::manifest_file(1)).await);
        assert!(
            exists(metadata::metadata_file(2)).await,
            "v2 stays for a reader on it"
        );
        assert!(exists(metadata::manifest_list_file(3)).await);
    }

    #[tokio::test]
    async fn a_dropped_partition_leaves_the_table_and_a_race_is_lost_cleanly() {
        let store = MemoryStore::new();
        let env = Env::simulated(1, Timestamp::UNIX_EPOCH);
        let exporter = exporter(&store, &env);
        let prod = dataset();
        let export = || exporter.export(&TenantId::DEFAULT, &prod, Kind::Log);
        adopt(&store, &env, file(1, 0, &[])).await;
        adopt(&store, &env, file(2, 1, &[])).await;
        export().await.unwrap();
        Manifest::open(Arc::new(store.clone()), partition(0), env.clone())
            .drop_partition()
            .await
            .unwrap();
        assert_eq!(
            export().await.unwrap(),
            Outcome::Published {
                version: 2,
                files: 1
            }
        );
        assert_eq!(manifest_paths(&store, 2).await.len(), 1);
        let table = layout::iceberg_table(&TenantId::DEFAULT, &dataset(), Kind::Log);
        store
            .put(
                &metadata::key(&table, &metadata::metadata_file(3)),
                b"{}".to_vec().into(),
            )
            .await
            .unwrap();
        adopt(&store, &env, file(3, 1, &[])).await;
        assert_eq!(export().await.unwrap(), Outcome::Lost { version: 3 });
        assert_eq!(
            current(&store).await.0,
            2,
            "the hint still names the last good version"
        );
    }

    #[test]
    fn the_digest_depends_on_the_files_and_not_on_the_run() {
        let a = [file(1, 0, &[]), file(2, 0, &[])];
        let b = [file(1, 0, &[]), file(2, 0, &[])];
        let c = [file(1, 0, &[]), file(3, 0, &[])];
        assert_eq!(digest(&a), digest(&b));
        assert_ne!(digest(&a), digest(&c));
        assert!(digest(&a).starts_with("2-"));
        let id = uuid(&mut Env::simulated(1, Timestamp::UNIX_EPOCH).random.fork());
        assert_eq!(id.len(), 36);
        assert_eq!(id.matches('-').count(), 4);
    }
}
