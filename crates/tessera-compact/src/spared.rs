// SPDX-License-Identifier: Apache-2.0

//! The traces a sampling policy is to spare though the partition
//! being sampled does not say so (CO-7).
//!
//! A policy that samples spans `EXCEPT ERRORS` keeps a trace with an
//! error span whole. Sampling is applied to one partition at a time,
//! and a trace does not stop at a partition's window: one that begins
//! before midnight and fails after it has its error span in the next
//! window. Its spans on this side would be sampled as those of any
//! trace, and the trace kept in part. The same goes for a trace an
//! alert names, which is in no span at all. So before a span
//! partition is sampled, the error spans of the partitions beside it
//! and the alert events around it are asked which traces they name:
//! two narrow reads, of the trace ids of the rows that qualify.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{Array, BooleanArray, FixedSizeBinaryArray, RecordBatch, StringArray};
use tessera_format::{Bound, FileFormat, RowPredicate, ScanRequest};
use tessera_object::{Pools, layout};
use tessera_types::FileMeta;
use tessera_types::event::{STATUS_CODE, Scalar, TRACE_ID};

use crate::CompactError;

/// Traces to keep whole, by id.
pub type Spared = HashSet<[u8; 16]>;

/// The status an error span has.
const ERROR: &str = "error";

/// The traces that have an error span in `spans`, the files of span
/// partitions, or that a row of `alerts`, the files of alert
/// partitions, names.
///
/// # Errors
///
/// If a file cannot be read.
pub async fn traces(
    pools: &Pools,
    format: &Arc<dyn FileFormat>,
    spans: &[FileMeta],
    alerts: &[FileMeta],
) -> Result<Spared, CompactError> {
    let mut spared = Spared::new();
    for file in spans {
        // A row group none of whose spans can be an error is not read.
        let errors = ScanRequest {
            projection: Some(vec![TRACE_ID.to_owned()]),
            bounds: vec![Bound {
                column: STATUS_CODE.to_owned(),
                min: Some(Scalar::Text(ERROR.to_owned())),
                max: Some(Scalar::Text(ERROR.to_owned())),
            }],
            filter: Some(RowPredicate {
                columns: vec![STATUS_CODE.to_owned()],
                evaluate: Arc::new(is_error),
            }),
            ..ScanRequest::default()
        };
        named(pools, format, file, errors, &mut spared).await?;
    }
    for file in alerts {
        let naming = ScanRequest {
            projection: Some(vec![TRACE_ID.to_owned()]),
            filter: Some(RowPredicate {
                columns: vec![TRACE_ID.to_owned()],
                evaluate: Arc::new(names_a_trace),
            }),
            ..ScanRequest::default()
        };
        named(pools, format, file, naming, &mut spared).await?;
    }
    Ok(spared)
}

/// Adds to `spared` the trace ids `request` returns of `file`.
async fn named(
    pools: &Pools,
    format: &Arc<dyn FileFormat>,
    file: &FileMeta,
    request: ScanRequest,
    spared: &mut Spared,
) -> Result<(), CompactError> {
    let store = Arc::clone(pools.get(&file.pool)?.store());
    let key = layout::data_file(&file.partition, file.file_id);
    let fetched = format
        .open(store, key, file.size_bytes)
        .fetch(request)
        .await?;
    let batches = tokio::task::spawn_blocking(move || fetched.decode())
        .await
        .map_err(|e| CompactError::Task(e.to_string()))??;
    for batch in &batches {
        let Some(traces) = batch
            .column_by_name(TRACE_ID)
            .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>())
        else {
            continue;
        };
        spared.extend(
            (0..traces.len())
                .filter(|&i| !traces.is_null(i))
                .filter_map(|i| <[u8; 16]>::try_from(traces.value(i)).ok()),
        );
    }
    Ok(())
}

/// Which rows of `rows` are error spans.
fn is_error(rows: &RecordBatch) -> Result<BooleanArray, String> {
    let statuses = rows
        .column_by_name(STATUS_CODE)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| format!("no {STATUS_CODE} column"))?;
    Ok((0..statuses.len())
        .map(|i| Some(!statuses.is_null(i) && statuses.value(i) == ERROR))
        .collect())
}

/// Which rows of `rows` name a trace.
fn names_a_trace(rows: &RecordBatch) -> Result<BooleanArray, String> {
    let traces = rows
        .column_by_name(TRACE_ID)
        .ok_or_else(|| format!("no {TRACE_ID} column"))?;
    Ok((0..traces.len())
        .map(|i| Some(!traces.is_null(i)))
        .collect())
}
