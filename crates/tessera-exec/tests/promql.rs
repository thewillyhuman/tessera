// SPDX-License-Identifier: Apache-2.0

//! The `PromQL` node over real storage: metric rows in the tail, read
//! through projected scans, evaluated step by step.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Float64Array, StringArray};
use tessera_exec::{CpuPool, Engine, QueryLimits};
use tessera_ir::promql::{PromAggregate, PromExpr};
use tessera_ir::{Expr, NamedExpr, Plan, Registry, Scan, SeriesGrouping, VALUE};
use tessera_manifest::registry;
use tessera_object::{MemoryStore, Pools};
use tessera_opt::optimize;
use tessera_types::event::{
    EXTRACTED, IS_MONOTONIC, METRIC_TYPE, NAME, OVERFLOW, Scalar, TIMESTAMP, UNIT,
};
use tessera_types::{
    DatasetName, Env, EventBatch, Kind, MetricRow, MetricType, PoolId, Temporality, TenantId,
    TimeRange, Timestamp, series_id,
};
use tessera_wal::WriterLog;

const S: i64 = 1_000_000_000;

fn dataset() -> DatasetName {
    DatasetName::new("prod").unwrap()
}

/// A counter `http.requests` for jobs `api` and `web`, sampled every 15
/// seconds for a minute.
fn points() -> EventBatch {
    let mut rows = Vec::new();
    for (job, step) in [("api", 10.0), ("web", 2.0)] {
        let attributes: BTreeMap<String, Scalar> =
            BTreeMap::from([("attr.job".to_owned(), Scalar::Text(job.into()))]);
        for i in 0..5u8 {
            rows.push(MetricRow {
                timestamp: Timestamp::from_nanos(i64::from(i) * 15 * S),
                observed_timestamp: Timestamp::from_nanos(100 * S),
                name: "http.requests".into(),
                metric_type: MetricType::Sum,
                unit: "{requests}".into(),
                value: Some(step * f64::from(i)),
                temporality: Temporality::Cumulative,
                is_monotonic: Some(true),
                series_id: series_id("http.requests", &attributes),
                attributes: attributes.clone(),
                ..MetricRow::default()
            });
        }
    }
    EventBatch::from_metrics(TenantId::DEFAULT, dataset(), &rows).unwrap()
}

async fn engine() -> Engine {
    let store = Arc::new(MemoryStore::new());
    let env = Env::simulated(1, Timestamp::UNIX_EPOCH);
    let mut log = WriterLog::start(store.clone(), env.clone());
    registry::register(
        &*store,
        &env,
        &TenantId::DEFAULT,
        &dataset(),
        log.writer_id(),
    )
    .await
    .unwrap();
    log.append_segment(&[points()]).await.unwrap();
    let pools = Pools::new(PoolId::new("root").unwrap(), store);
    Engine::new(
        Arc::new(pools),
        Arc::new(tessera_format::ParquetFormat::default()),
        env,
        CpuPool::new(2),
        QueryLimits::default(),
    )
}

/// A selector's input: the samples of every metric row with their
/// Prometheus name.
fn input(range: TimeRange) -> Plan {
    let mut scan = Scan::all(TenantId::DEFAULT, dataset(), Kind::Metric);
    scan.time_range = range;
    Plan::Scan(scan).project(vec![
        NamedExpr::new(Expr::column(TIMESTAMP), TIMESTAMP),
        NamedExpr::new(Expr::column(VALUE), VALUE),
        NamedExpr::new(Expr::column(OVERFLOW), OVERFLOW),
        NamedExpr::new(
            Expr::Call {
                name: "prometheus_name".into(),
                args: vec![
                    Expr::column(NAME),
                    Expr::column(UNIT),
                    Expr::column(METRIC_TYPE),
                    Expr::column(IS_MONOTONIC),
                ],
            },
            "__name__",
        ),
    ])
}

#[tokio::test]
async fn rates_are_evaluated_per_step_from_stored_samples() {
    let engine = engine().await;
    let plan = Plan::PromQL {
        inputs: vec![input(TimeRange::ALL)],
        expr: PromExpr::Aggregate {
            op: PromAggregate::Sum,
            input: Box::new(PromExpr::Call {
                function: "rate".into(),
                args: vec![PromExpr::Selector {
                    input: 0,
                    range: Some(Duration::from_secs(60)),
                    offset: 0,
                    at: None,
                    equal: Vec::new(),
                }],
            }),
            parameter: None,
            grouping: Some(SeriesGrouping {
                by: true,
                labels: vec!["job".into()],
            }),
        },
        start: Timestamp::from_nanos(30 * S),
        end: Timestamp::from_nanos(60 * S),
        step: Duration::from_secs(30),
    };
    let plan = optimize(plan, &Registry::builtin()).unwrap();
    let output = engine.run(&plan).await.unwrap();
    assert_eq!(output.num_rows(), 4, "two jobs at two steps");
    let batch = &output.batches[0];
    let labels = batch
        .column_by_name(EXTRACTED)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let values = batch
        .column_by_name(VALUE)
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let api_at_60 = (0..batch.num_rows())
        .find(|&i| {
            labels.value(i) == r#"{"job":"api"}"#
                && batch
                    .column_by_name(TIMESTAMP)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                    .unwrap()
                    .value(i)
                    == 60 * S
        })
        .unwrap();
    // 30 over 45s in (0, 60], extrapolated to the window: 40 / 60.
    assert!((values.value(api_at_60) - 40.0 / 60.0).abs() < 1e-9);

    let named = Plan::PromQL {
        inputs: vec![input(TimeRange::ALL)],
        expr: PromExpr::Selector {
            input: 0,
            range: None,
            offset: 0,
            at: None,
            equal: Vec::new(),
        },
        start: Timestamp::from_nanos(60 * S),
        end: Timestamp::from_nanos(60 * S),
        step: Duration::ZERO,
    };
    let plan = optimize(named, &Registry::builtin()).unwrap();
    let output = engine.run(&plan).await.unwrap();
    assert_eq!(output.num_rows(), 2);
    let labels = output.batches[0]
        .column_by_name(EXTRACTED)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(
        labels
            .value(0)
            .contains(r#""__name__":"http_requests_total""#),
        "the Prometheus name: {}",
        labels.value(0)
    );
}
