// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Running a compaction job: read the inputs, merge, deduplicate,
//! promote the attribute paths worth a column (section 8.2), write the
//! output in the dataset's layout with its indexes, commit the
//! replacement.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, FixedSizeBinaryArray, Int64Array, RecordBatch, StringArray,
};
use arrow::compute::{concat_batches, filter_record_batch};
use tessera_format::FileFormat;
use tessera_format::layout::SERVICE;
use tessera_manifest::{LayoutRecord, Manifest};
use tessera_object::{ObjectStore, Pools, layout};
use tessera_types::event::{EVENT_ID, NAME, PATTERN_ID};
use tessera_types::{Env, FileId, FileMeta, FileSource, Kind, Level, Meter, PartitionId, Stored};

use crate::plan::Job;
use crate::retention::{self, Retain};
use crate::{CompactError, patterns, promote};

/// Runs compaction jobs against the pools.
pub struct Compactor {
    pools: Arc<Pools>,
    format: Arc<dyn FileFormat>,
    env: Env,
    meter: Option<Arc<dyn Meter>>,
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
        }
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
        let mut wanted: Vec<String> = record.dedicated.clone();
        wanted.extend(record.sort_key.iter().filter(|c| is_path(c)).cloned());
        let format = Arc::clone(&self.format);
        let partition = job.partition.clone();
        let retain = retain.filter(|_| job.level == Level::L3).cloned();
        let (written, duplicates, stored) = tokio::task::spawn_blocking(move || {
            let (rows, duplicates) = deduplicate(&batches)?;
            let rows = match &retain {
                Some(retain) => {
                    let (rows, applied) = retention::apply(&rows, partition.kind, retain)?;
                    tracing::info!(partition = %partition, ?applied, "retention applied");
                    rows
                }
                None => rows,
            };
            let (rows, _) = promote::promote(&rows, &wanted, promote::MAX_DEDICATED)?;
            let (rows, templates) = if partition.kind == Kind::Log {
                patterns::assign(&rows)?
            } else {
                (rows, Vec::new())
            };
            let written = format.write(std::slice::from_ref(&rows), &layout)?;
            let stored = stored_usage(&rows, &partition, written.bytes.len() as u64, &templates);
            Ok::<_, CompactError>((written, duplicates, stored))
        })
        .await
        .map_err(|e| CompactError::Task(e.to_string()))??;

        let pool = self.pools.place(&mut self.env.random.clone()).clone();
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
    let Some(ids) = all
        .column_by_name(EVENT_ID)
        .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>())
    else {
        return Ok((all, 0));
    };
    let mut seen: HashSet<&[u8]> = HashSet::with_capacity(ids.len());
    let keep: BooleanArray = (0..ids.len())
        .map(|i| ids.is_null(i) || seen.insert(ids.value(i)))
        .collect();
    let duplicates = keep.false_count() as u64;
    if duplicates == 0 {
        return Ok((all, 0));
    }
    Ok((filter_record_batch(&all, &keep)?, duplicates))
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
