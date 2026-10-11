// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Maintaining rollups (section 8.7): each round, the windows of a
//! rollup that closed since its progress mark, allowing for late rows
//! (section 11.5), are evaluated through the engine and written as
//! events of the target dataset, one per group, timestamped at the
//! window's start with the keys and aggregates as attributes. Event
//! ids are deterministic, so a window evaluated twice after a takeover
//! is removed by deduplication. The progress mark lives in the rollup's
//! own state log, as the watermark named `done`, beside `since`, the
//! start of the first window evaluated: together they are what the
//! output covers.

use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    Array, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampNanosecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use tessera_exec::Engine;
use tessera_ir::{BindContext, Expr, Frontend, Literal, Plan, Statement};
use tessera_object::ObjectStore;
use tessera_opt::optimize;
use tessera_sql::SqlFrontend;
use tessera_types::event::{Scalar, TIMESTAMP};
use tessera_types::{
    Attributes, DatasetName, Env, EventBatch, EventRow, TenantId, TimeRange, Timestamp, stable_hash,
};

use crate::evaluate::EvalError;
use crate::incremental::LATENESS;
use crate::rollup::Rollup;
use crate::sink::AlertSink;
use crate::state::{self, EvalState, StateLog};

/// The most windows one round evaluates, so that a rollup behind after
/// downtime catches up in bounded steps.
pub const MAX_WINDOWS_PER_ROUND: usize = 60;

/// The watermark that holds the progress mark.
const DONE: &str = "done";
/// The watermark that holds the start of the first window evaluated.
const SINCE: &str = "since";

/// The time a rollup's output covers in whole windows, from its state:
/// the start of the first window evaluated to the progress mark, or
/// nothing before the first round.
#[must_use]
pub fn coverage(state: &EvalState) -> Option<TimeRange> {
    let since = state.watermarks.get(SINCE)?;
    let done = state.watermarks.get(DONE)?;
    Some(TimeRange::new(*since, *done))
}

/// What one round did for a rollup.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Maintained {
    /// Windows evaluated.
    pub windows: usize,
    /// Rows written to the target dataset.
    pub rows: u64,
}

/// The state log that holds a rollup's progress.
#[must_use]
pub fn progress_log(
    store: Arc<dyn ObjectStore>,
    tenant: &TenantId,
    rollup: &str,
    env: Env,
) -> StateLog {
    state::open(store, tenant, &format!("rollup-{rollup}"), 0, env)
}

/// The windows of `rollup` closed by `now`, less the lateness allowance,
/// that `done_through` has not covered: from the mark, or from the
/// newest closed window when there is none, oldest first, at most
/// [`MAX_WINDOWS_PER_ROUND`].
#[must_use]
pub fn due_windows(
    rollup: &Rollup,
    done_through: Option<Timestamp>,
    now: Timestamp,
) -> Vec<TimeRange> {
    let window = nanos(rollup.window());
    let closed_until = now.as_nanos().saturating_sub(nanos(LATENESS));
    let last_end = closed_until.div_euclid(window) * window;
    let mut start = done_through.map_or(last_end - window, Timestamp::as_nanos);
    let mut due = Vec::new();
    while start + window <= last_end && due.len() < MAX_WINDOWS_PER_ROUND {
        due.push(TimeRange::new(
            Timestamp::from_nanos(start),
            Timestamp::from_nanos(start + window),
        ));
        start += window;
    }
    due
}

fn nanos(duration: Duration) -> i64 {
    i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
}

/// Evaluates `rollup` over `window`: the rows of the target dataset for
/// the groups found, or nothing when the window has none.
///
/// # Errors
///
/// If the query does not lower, plan or run.
pub async fn materialize(
    engine: &Engine,
    tenant: &TenantId,
    rollup: &Rollup,
    window: TimeRange,
    now: Timestamp,
) -> Result<Option<EventBatch>, EvalError> {
    let failed = |message: String| EvalError::Rollup {
        rollup: rollup.name.clone(),
        message,
    };
    let dataset = DatasetName::new(&rollup.dataset).map_err(|e| failed(e.to_string()))?;
    let target = DatasetName::new(&rollup.target).map_err(|e| failed(e.to_string()))?;
    let ctx = BindContext::new(tenant.clone(), dataset, now);
    let plan = match SqlFrontend
        .lower(&rollup.expr, &ctx)
        .map_err(|d| failed(d.describe(&rollup.expr)))?
    {
        Statement::Query(plan) => plan,
        Statement::Explain { .. } | Statement::Control(_) => {
            return Err(failed("the expression is not a query".into()));
        }
    };
    let plan = bounded(plan, window).map_err(failed)?;
    let plan = optimize(plan, engine.registry()).map_err(|e| failed(e.to_string()))?;
    let output = engine.run(&plan).await.map_err(|e| failed(e.to_string()))?;
    let rows: Vec<EventRow> = output
        .batches
        .iter()
        .flat_map(|batch| events_of(batch, rollup, window.start, now))
        .collect();
    if rows.is_empty() {
        return Ok(None);
    }
    EventBatch::from_events(tenant.clone(), target, &rows)
        .map(Some)
        .map_err(|e| failed(e.to_string()))
}

/// `plan` reading only the rows of `window`: the scan under the
/// aggregation is filtered on the timestamp, which the optimizer turns
/// into the scan's time range as well.
fn bounded(plan: Plan, window: TimeRange) -> Result<Plan, String> {
    Ok(match plan {
        Plan::Scan(scan) => Plan::Filter {
            input: Box::new(Plan::Scan(scan)),
            predicate: Expr::column(TIMESTAMP)
                .binary(
                    tessera_ir::BinaryOp::GtEq,
                    Expr::Literal(Literal::Timestamp(window.start)),
                )
                .and(Expr::column(TIMESTAMP).binary(
                    tessera_ir::BinaryOp::Lt,
                    Expr::Literal(Literal::Timestamp(window.end)),
                )),
        },
        Plan::Filter { input, predicate } => Plan::Filter {
            input: Box::new(bounded(*input, window)?),
            predicate,
        },
        Plan::Project { input, exprs } => Plan::Project {
            input: Box::new(bounded(*input, window)?),
            exprs,
        },
        Plan::Aggregate {
            input,
            group_by,
            aggregates,
        } => Plan::Aggregate {
            input: Box::new(bounded(*input, window)?),
            group_by,
            aggregates,
        },
        Plan::Limit {
            input,
            offset,
            count,
        } => Plan::Limit {
            input: Box::new(bounded(*input, window)?),
            offset,
            count,
        },
        other => {
            return Err(format!(
                "a rollup reads one table: {}",
                other.to_string().lines().next().unwrap_or_default()
            ));
        }
    })
}

/// The rows of `batch` as events of the target dataset.
fn events_of(
    batch: &RecordBatch,
    rollup: &Rollup,
    start: Timestamp,
    now: Timestamp,
) -> Vec<EventRow> {
    (0..batch.num_rows())
        .map(|row| {
            let mut attributes: Attributes = BTreeMap::new();
            let mut identity = format!("{}/{}", rollup.name, start.as_nanos());
            for (index, field) in batch.schema().fields().iter().enumerate() {
                let Some(value) = scalar(batch.column(index).as_ref(), row) else {
                    continue;
                };
                let _ = write!(identity, "/{}={value:?}", field.name());
                attributes.insert(format!("attr.{}", field.name()), value);
            }
            let mut event_id = [0u8; 16];
            event_id[..8].copy_from_slice(&stable_hash(identity.as_bytes()).to_be_bytes());
            event_id[8..]
                .copy_from_slice(&stable_hash(format!("{identity}/2").as_bytes()).to_be_bytes());
            EventRow {
                timestamp: start,
                observed_timestamp: now,
                event_id,
                trace_id: None,
                span_id: None,
                name: rollup.name.clone(),
                attributes,
            }
        })
        .collect()
}

/// The value at `row` of `column`, when it is of a type an attribute
/// holds and is not null.
fn scalar(column: &dyn Array, row: usize) -> Option<Scalar> {
    if column.is_null(row) {
        return None;
    }
    let any = column.as_any();
    match column.data_type() {
        DataType::Utf8 => any
            .downcast_ref::<StringArray>()
            .map(|a| Scalar::Text(a.value(row).to_owned())),
        DataType::Int64 => any
            .downcast_ref::<Int64Array>()
            .map(|a| Scalar::Int(a.value(row))),
        DataType::Int32 => any
            .downcast_ref::<Int32Array>()
            .map(|a| Scalar::Int(i64::from(a.value(row)))),
        DataType::UInt64 => any
            .downcast_ref::<UInt64Array>()
            .map(|a| Scalar::Int(i64::try_from(a.value(row)).unwrap_or(i64::MAX))),
        DataType::Float64 => any
            .downcast_ref::<Float64Array>()
            .map(|a| Scalar::Float(a.value(row))),
        DataType::Boolean => any
            .downcast_ref::<BooleanArray>()
            .map(|a| Scalar::Bool(a.value(row))),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => any
            .downcast_ref::<TimestampNanosecondArray>()
            .map(|a| Scalar::Int(a.value(row))),
        _ => None,
    }
}

/// One round for `rollup` at `now`: every due window evaluated and
/// written through `sink`, the progress mark moved after each.
///
/// # Errors
///
/// If the progress log cannot be read or written, a window fails to
/// evaluate, or the sink refuses the rows; the mark stays at the last
/// window that went through.
pub async fn maintain(
    engine: &Engine,
    store: Arc<dyn ObjectStore>,
    env: &Env,
    tenant: &TenantId,
    rollup: &Rollup,
    sink: &dyn AlertSink,
    now: Timestamp,
) -> Result<Maintained, EvalError> {
    let failed = |message: String| EvalError::Rollup {
        rollup: rollup.name.clone(),
        message,
    };
    let log = progress_log(store, tenant, &rollup.name, env.clone());
    let snapshot = log.snapshot().await.map_err(|e| failed(e.to_string()))?;
    let done_through = snapshot.state().watermarks.get(DONE).copied();
    let mut maintained = Maintained::default();
    for window in due_windows(rollup, done_through, now) {
        if let Some(batch) = materialize(engine, tenant, rollup, window, now).await? {
            maintained.rows += u64::try_from(batch.rows().num_rows()).unwrap_or(u64::MAX);
            sink.write(batch).await.map_err(failed)?;
        }
        log.update(|state: &EvalState| {
            let mut actions = vec![EvalState::put_watermark(DONE, window.end)];
            if !state.watermarks.contains_key(SINCE) {
                actions.push(EvalState::put_watermark(SINCE, window.start));
            }
            Ok::<_, String>(actions)
        })
        .await
        .map_err(|e| failed(e.to_string()))?;
        maintained.windows += 1;
    }
    Ok(maintained)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rollup(window_seconds: u64) -> Rollup {
        Rollup {
            name: "r".into(),
            expr: String::new(),
            dataset: "prod".into(),
            target: "rollup_r".into(),
            window_seconds,
            every_seconds: window_seconds,
            origin: crate::rollup::Origin::User,
            enabled: true,
        }
    }

    fn at(seconds: i64) -> Timestamp {
        Timestamp::from_nanos(seconds * 1_000_000_000)
    }

    #[test]
    fn the_first_round_takes_the_newest_closed_window_only() {
        let due = due_windows(&rollup(60), None, at(1_090));
        assert_eq!(due, [TimeRange::new(at(1_020), at(1_080))]);
        let not_yet = due_windows(&rollup(60), None, at(1_085));
        assert_eq!(
            not_yet,
            [TimeRange::new(at(960), at(1_020))],
            "1080 is inside the lateness"
        );
    }

    #[test]
    fn later_rounds_continue_from_the_mark_in_bounded_steps() {
        let due = due_windows(&rollup(60), Some(at(960)), at(1_150));
        assert_eq!(due.len(), 3);
        assert_eq!(due[0], TimeRange::new(at(960), at(1_020)));
        assert_eq!(due[2], TimeRange::new(at(1_080), at(1_140)));
        assert!(due_windows(&rollup(60), Some(at(1_140)), at(1_150)).is_empty());
        let behind = due_windows(&rollup(1), Some(at(0)), at(1_000));
        assert_eq!(behind.len(), MAX_WINDOWS_PER_ROUND);
    }
}
