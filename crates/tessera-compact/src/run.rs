// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Running a compaction job: read the inputs, merge, deduplicate,
//! promote the attribute paths worth a column (section 8.2), write the
//! output in the dataset's layout with its indexes, commit the
//! replacement.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, FixedSizeBinaryArray, Int64Array, RecordBatch, StringArray,
    TimestampNanosecondArray,
};
use arrow::compute::{concat_batches, filter_record_batch};
use tessera_catalog::{TenantSettings, TenantSettingsSource};
use tessera_format::FileFormat;
use tessera_format::layout::SERVICE;
use tessera_manifest::{LayoutRecord, Manifest, Tombstone};
use tessera_object::{ObjectStore, Pools, layout};
use tessera_types::event::{EVENT_ID, NAME, PATTERN_ID, SERIES_ID, TIMESTAMP};
use tessera_types::{Env, FileId, FileMeta, FileSource, Kind, Level, Meter, PartitionId, Stored};

use crate::plan::Job;
use crate::retention::{self, Retain};
use crate::{CompactError, patterns, promote, tombstones};

/// Runs compaction jobs against the pools.
pub struct Compactor {
    pools: Arc<Pools>,
    format: Arc<dyn FileFormat>,
    env: Env,
    meter: Option<Arc<dyn Meter>>,
    /// Where the tenants' pool and switches come from (CT-7); the
    /// defaults without one.
    tenants: Option<Arc<dyn TenantSettingsSource>>,
}

/// What a job did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The inputs were replaced by the output.
    Replaced {
        /// The output file.
        output: FileId,
        /// Rows in the output.
        rows: u64,
        /// Rows dropped as duplicates (section 7.5).
        duplicates: u64,
    },
    /// Some input had already left the manifest; the output was deleted.
    Superseded,
}

/// Whether a column name is an attribute path.
fn is_path(column: &str) -> bool {
    column.starts_with("attr.") || column.starts_with("resource.") || column.starts_with("scope.")
}

/// A group of stored rows: service, pattern and name.
type StoredKey = (Option<String>, Option<i64>, Option<String>);

/// What a compacted file stores per service, pattern and name (CO-2):
/// the file's bytes shared out by rows; `templates` names the patterns.
#[must_use]
pub fn stored_usage(
    rows: &RecordBatch,
    partition: &PartitionId,
    file_bytes: u64,
    templates: &[(i64, String)],
) -> Vec<Stored> {
    let text = |name: &str| {
        rows.column_by_name(name)
            .and_then(|c| c.as_any().downcast_ref::<StringArray>().cloned())
    };
    let service = text(SERVICE);
    let name = matches!(
        partition.kind,
        Kind::Metric | Kind::Span | Kind::Event | Kind::Alert
    )
    .then(|| text(NAME))
    .flatten();
    let pattern = rows
        .column_by_name(PATTERN_ID)
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>().cloned());
    let mut groups: BTreeMap<StoredKey, u64> = BTreeMap::new();
    for i in 0..rows.num_rows() {
        let cell = |array: &Option<StringArray>| {
            array
                .as_ref()
                .filter(|a| !a.is_null(i))
                .map(|a| a.value(i).to_owned())
        };
        let key = (
            cell(&service),
            pattern
                .as_ref()
                .filter(|a| !a.is_null(i))
                .map(|a| a.value(i)),
            cell(&name),
        );
        *groups.entry(key).or_default() += 1;
    }
    let total = rows.num_rows().max(1) as u64;
    groups
        .into_iter()
        .map(|((service, pattern_id, name), count)| Stored {
            tenant: partition.tenant.clone(),
            dataset: partition.dataset.clone(),
            kind: partition.kind,
            service,
            pattern: pattern_id.and_then(|id| {
                templates
                    .iter()
                    .find(|(known, _)| *known == id)
                    .map(|(_, template)| template.clone())
            }),
            pattern_id,
            name,
            rows: count,
            bytes: file_bytes.saturating_mul(count) / total,
        })
        .collect()
}

/// `batch` restricted to the kind's own columns, in schema order.
/// `rows` without the rows `tombstones` delete and without what
/// `retain`, when a retention policy is due, drops or samples out.
fn trimmed(
    rows: RecordBatch,
    tombstones: &[Tombstone],
    retain: Option<&Retain>,
    partition: &PartitionId,
) -> Result<RecordBatch, CompactError> {
    let rows = if tombstones.is_empty() {
        rows
    } else {
        let (rows, removed) = tombstones::remove_deleted(&rows, tombstones)?;
        if removed > 0 {
            tracing::info!(partition = %partition, removed, "deleted rows removed");
        }
        rows
    };
    match retain {
        Some(retain) => {
            let (rows, applied) = retention::apply(&rows, partition.kind, retain)?;
            tracing::info!(partition = %partition, ?applied, "retention applied");
            Ok(rows)
        }
        None => Ok(rows),
    }
}

/// `rows` with the shape of the traces that lie whole within the
/// partition's window stored (QL-14), as an L3 span file carries it.
fn completed(rows: &RecordBatch, partition: &PartitionId) -> Result<RecordBatch, CompactError> {
    let window = &partition.window;
    let (rows, completion) =
        tessera_kernels::trace::complete(rows, window.start().as_nanos(), window.end().as_nanos())?;
    tracing::info!(partition = %partition, ?completion, "traces completed");
    Ok(rows)
}

fn kind_columns(batch: &RecordBatch, kind: Kind) -> Result<RecordBatch, CompactError> {
    let schema = tessera_types::event::schema(kind);
    let indices = schema
        .fields()
        .iter()
        .map(|f| batch.schema().index_of(f.name()))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(batch.project(&indices)?)
}

impl Compactor {
    /// A compactor writing to `pools` with `format`.
    #[must_use]
    pub fn new(pools: Arc<Pools>, format: Arc<dyn FileFormat>, env: Env) -> Self {
        Compactor {
            pools,
            format,
            env,
            meter: None,
            tenants: None,
        }
    }

    /// The tenant's settings and what they ask of `job` (CT-7): the
    /// attributes to promote, sort-key columns always among them, and
    /// whether to mine log patterns.
    fn tenant_choices(
        &self,
        job: &Job,
        record: &LayoutRecord,
    ) -> (TenantSettings, Vec<String>, bool) {
        let settings = self
            .tenants
            .as_ref()
            .map_or_else(TenantSettings::default, |t| {
                t.settings_of(&job.partition.tenant)
            });
        let mut wanted: Vec<String> = if settings.features.promotion() {
            record.dedicated.clone()
        } else {
            Vec::new()
        };
        wanted.extend(record.sort_key.iter().filter(|c| is_path(c)).cloned());
        let patterns = job.partition.kind == Kind::Log && settings.features.patterns();
        (settings, wanted, patterns)
    }

    /// The compactor placing each tenant's files in its pool and
    /// honouring its switches, as `tenants` reports them (CT-7).
    #[must_use]
    pub fn with_tenants(mut self, tenants: Arc<dyn TenantSettingsSource>) -> Self {
        self.tenants = Some(tenants);
        self
    }

    /// The compactor reporting what it stores to `meter` (CO-2).
    #[must_use]
    pub fn with_meter(mut self, meter: Arc<dyn Meter>) -> Self {
        self.meter = Some(meter);
        self
    }

    /// Runs `job` against `manifest`, which must be the manifest of the
    /// job's partition, writing the output in `record`, the dataset's
    /// current layout (AD-9).
    ///
    /// # Errors
    ///
    /// If an input cannot be read, the output cannot be written, or the
    /// manifest cannot be committed.
    pub async fn run(
        &self,
        manifest: &Manifest,
        job: &Job,
        record: &LayoutRecord,
        retain: Option<&Retain>,
        tombstones: &[Tombstone],
    ) -> Result<Outcome, CompactError> {
        let mut batches = Vec::new();
        for input in &job.inputs {
            for batch in self.read(input).await? {
                // Inputs of different epochs carry different dedicated
                // columns; the kind's own are the common ground, and
                // `_overflow` still holds every attribute.
                batches.push(kind_columns(&batch, job.partition.kind)?);
            }
        }
        let layout = record.to_layout();
        let (settings, wanted, patterns) = self.tenant_choices(job, record);
        let format = Arc::clone(&self.format);
        let keys = Arc::clone(&self.env.keys);
        let partition = job.partition.clone();
        let retain = retain.filter(|_| job.level == Level::L3).cloned();
        let complete_traces = job.level == Level::L3 && job.partition.kind == Kind::Span;
        let tombstones = tombstones.to_vec();
        let (written, duplicates, stored) = tokio::task::spawn_blocking(move || {
            let (rows, duplicates) = deduplicate(&batches)?;
            let rows = trimmed(rows, &tombstones, retain.as_ref(), &partition)?;
            let (rows, _) = promote::promote(&rows, &wanted, promote::MAX_DEDICATED)?;
            let rows = if complete_traces {
                completed(&rows, &partition)?
            } else {
                rows
            };
            let (rows, templates) = if patterns {
                patterns::assign(&rows)?
            } else {
                (rows, Vec::new())
            };
            let key = keys.for_tenant(&partition.tenant)?;
            let written = format.write(std::slice::from_ref(&rows), &layout, key.as_ref())?;
            let stored = stored_usage(&rows, &partition, written.bytes.len() as u64, &templates);
            Ok::<_, CompactError>((written, duplicates, stored))
        })
        .await
        .map_err(|e| CompactError::Task(e.to_string()))??;

        let pool = self
            .pools
            .place_in(settings.storage_pool.as_ref(), &mut self.env.random.clone())
            .clone();
        let file_id = FileId::random(&mut self.env.random.clone());
        let data_key = layout::data_file(&job.partition, file_id);
        pool.store()
            .put_if_absent(&data_key, written.bytes.clone())
            .await?;
        if let Some(sidecar) = &written.sidecar {
            pool.store()
                .put(&layout::sidecar(&job.partition, file_id), sidecar.clone())
                .await?;
        }
        let output = FileMeta {
            file_id,
            partition: job.partition.clone(),
            pool: pool.id().clone(),
            level: job.level,
            source: FileSource::Compaction {
                inputs: job.inputs.iter().map(|f| f.file_id).collect(),
            },
            min_timestamp: written.min_timestamp,
            max_timestamp: written.max_timestamp,
            row_count: written.row_count,
            size_bytes: written.bytes.len() as u64,
            layout_epoch: record.epoch,
            sort_key: written.sort_key,
            columns: written.columns,
            indexes: written.indexes,
        };
        let inputs: Vec<FileId> = job.inputs.iter().map(|f| f.file_id).collect();
        if manifest
            .replace(&inputs, std::slice::from_ref(&output))
            .await?
        {
            if let Some(meter) = &self.meter {
                meter.stored(stored);
            }
            Ok(Outcome::Replaced {
                output: file_id,
                rows: written.row_count,
                duplicates,
            })
        } else {
            // Superseded (CP-4): the output is garbage; remove it now
            // rather than leave it to a sweep that has no reference to it.
            let _ = pool.store().delete(&data_key).await;
            let _ = pool
                .store()
                .delete(&layout::sidecar(&job.partition, file_id))
                .await;
            Ok(Outcome::Superseded)
        }
    }

    async fn read(&self, file: &FileMeta) -> Result<Vec<RecordBatch>, CompactError> {
        let store: Arc<dyn ObjectStore> = Arc::clone(self.pools.get(&file.pool)?.store());
        let bytes = store
            .get(&layout::data_file(&file.partition, file.file_id))
            .await?;
        let format = Arc::clone(&self.format);
        tokio::task::spawn_blocking(move || format.read(bytes))
            .await
            .map_err(|e| CompactError::Task(e.to_string()))?
            .map_err(CompactError::from)
    }
}

/// `batches` as one batch with later rows of a repeated `event_id`
/// dropped (section 7.5), and how many were dropped.
fn deduplicate(batches: &[RecordBatch]) -> Result<(RecordBatch, u64), CompactError> {
    let schema = batches
        .first()
        .map(RecordBatch::schema)
        .ok_or(tessera_format::FormatError::Empty)?;
    let all = concat_batches(&schema, batches)?;
    let keys = identities(&all);
    let mut seen: HashSet<&[u8]> = HashSet::with_capacity(keys.len());
    let keep: BooleanArray = keys
        .iter()
        .map(|key| key.as_deref().is_none_or(|k| seen.insert(k)))
        .collect();
    let duplicates = keep.false_count() as u64;
    if duplicates == 0 {
        return Ok((all, 0));
    }
    Ok((filter_record_batch(&all, &keep)?, duplicates))
}

/// The identity of every row: its event id, or for metrics, which store
/// none, its series and its time; nothing for a row without either.
fn identities(rows: &RecordBatch) -> Vec<Option<Vec<u8>>> {
    if let Some(ids) = rows
        .column_by_name(EVENT_ID)
        .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>())
    {
        return (0..ids.len())
            .map(|i| (!ids.is_null(i)).then(|| ids.value(i).to_vec()))
            .collect();
    }
    let series = rows
        .column_by_name(SERIES_ID)
        .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>());
    let times = rows
        .column_by_name(TIMESTAMP)
        .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>());
    let (Some(series), Some(times)) = (series, times) else {
        return vec![None; rows.num_rows()];
    };
    (0..rows.num_rows())
        .map(|i| {
            if series.is_null(i) {
                return None;
            }
            let mut key = Vec::with_capacity(24);
            key.extend_from_slice(series.value(i));
            key.extend_from_slice(&times.value(i).to_le_bytes());
            Some(key)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use tessera_types::event::Scalar;
    use tessera_types::{DatasetName, EventBatch, LogRow, TenantId, Timestamp};

    use super::*;

    fn batch(ids: &[u8]) -> RecordBatch {
        let rows: Vec<LogRow> = ids
            .iter()
            .map(|i| LogRow {
                timestamp: Timestamp::from_nanos(i64::from(*i)),
                event_id: [*i; 16],
                body: Some(format!("row {i}")),
                attributes: BTreeMap::from([("attr.n".to_owned(), Scalar::Int(i64::from(*i)))]),
                ..LogRow::default()
            })
            .collect();
        EventBatch::from_logs(TenantId::DEFAULT, DatasetName::new("prod").unwrap(), &rows)
            .unwrap()
            .rows()
            .clone()
    }

    #[test]
    fn duplicates_keep_their_first_occurrence() {
        let (rows, duplicates) = deduplicate(&[batch(&[1, 2, 3]), batch(&[2, 4, 1])]).unwrap();
        assert_eq!(duplicates, 2);
        assert_eq!(rows.num_rows(), 4);
        let ids = rows
            .column_by_name(EVENT_ID)
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .iter()
            .map(|v| v.unwrap()[0])
            .collect::<Vec<u8>>();
        assert_eq!(ids, vec![1, 2, 3, 4]);
        let (same, none) = deduplicate(&[batch(&[7, 8])]).unwrap();
        assert_eq!((same.num_rows(), none), (2, 0));
        let _ = Arc::new(());
    }
}
