// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Trace groups over real storage: the candidate phase, the span phase
//! restricted to the candidate traces (QL-13), files and the tail.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, FixedSizeBinaryArray, Int64Array, StringArray};
use tessera_exec::{CpuPool, Engine, QueryLimits, QueryOutput};
use tessera_format::ParquetFormat;
use tessera_ir::trace::{MATCHED, ROOT_SERVICE};
use tessera_ir::{BinaryOp, Expr, Plan, Registry, Relation, Scan, Spanset, Structural};
use tessera_manifest::{intake, registry};
use tessera_object::{MemoryStore, Pools};
use tessera_opt::optimize;
use tessera_types::event::{NAME, SPAN_ID, Scalar, TRACE_ID};
use tessera_types::{
    DatasetName, Env, EventBatch, Kind, PoolId, Seq, SpanRow, TenantId, Timestamp, Window,
};
use tessera_wal::{EntryBody, FlushTarget, WriterLog, write_l1_files};

fn dataset() -> DatasetName {
    DatasetName::new("prod").unwrap()
}

/// Trace `trace`: a root `GET /` of service `api` with a child `db` of
/// service `db`, which has a child `cache`; all starting at `start`.
fn trace(trace: u8, start: i64) -> EventBatch {
    let span = |id: u8, parent: Option<u8>, name: &str, service: &str, offset: i64| SpanRow {
        timestamp: Timestamp::from_nanos(start + offset),
        observed_timestamp: Timestamp::from_nanos(start + 100),
        event_id: [trace ^ id; 16],
        trace_id: [trace; 16],
        span_id: [id; 8],
        parent_span_id: parent.map(|p| [p; 8]),
        name: name.into(),
        duration_ns: 50 - offset,
        attributes: BTreeMap::from([(
            "resource.service.name".to_owned(),
            Scalar::Text(service.into()),
        )]),
        ..SpanRow::default()
    };
    EventBatch::from_spans(
        TenantId::DEFAULT,
        dataset(),
        &[
            span(1, None, "GET /", "api", 0),
            span(2, Some(1), "db", "db", 10),
            span(3, Some(2), "cache", "db", 20),
        ],
    )
    .unwrap()
}

const DAY: i64 = 86_400_000_000_000;

/// Traces 1 and 2 in a file on day 0, trace 3 in a file on day 1, and
/// trace 4 in the tail.
async fn populate() -> (Arc<MemoryStore>, Env) {
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
    let target = FlushTarget {
        store: store.clone(),
        pool: PoolId::new("root").unwrap(),
        format: Arc::new(ParquetFormat::default()),
    };
    let segments = [
        vec![trace(1, 1_000), trace(2, 2_000)],
        vec![trace(3, DAY + 1_000)],
    ];
    let mut after = Seq::ZERO;
    for segment in &segments {
        let through = log.append_segment(segment).await.unwrap();
        let files = write_l1_files(
            &target,
            &env,
            log.writer_id(),
            after,
            through,
            Window::DEFAULT_LENGTH,
            segment.clone(),
        )
        .await
        .unwrap();
        log.append(EntryBody::Flush {
            tenant: TenantId::DEFAULT,
            dataset: dataset(),
            after,
            through,
            files,
        })
        .await
        .unwrap();
        after = through;
    }
    intake::run(
        store.clone(),
        &env,
        &TenantId::DEFAULT,
        &dataset(),
        log.writer_id(),
    )
    .await
    .unwrap();
    log.append_segment(&[trace(4, DAY + 5_000)]).await.unwrap();
    (store, env)
}

fn engine(store: &Arc<MemoryStore>, env: &Env) -> Engine {
    let pools = Pools::new(PoolId::new("root").unwrap(), store.clone());
    Engine::new(
        Arc::new(pools),
        Arc::new(ParquetFormat::default()),
        env.clone(),
        CpuPool::new(2),
        QueryLimits::default(),
    )
}

fn name_is(name: &str) -> Spanset {
    Spanset::Filter(Some(
        Expr::column(NAME).binary(BinaryOp::Eq, Expr::literal(name)),
    ))
}

/// A trace group whose candidates are the spans the spanset's filters
/// match.
fn group(spanset: Spanset) -> Plan {
    let spans = Scan::all(TenantId::DEFAULT, dataset(), Kind::Span);
    let candidates = spanset.candidates().map(|predicate| {
        let mut scan = spans.clone();
        scan.predicates = vec![predicate];
        Box::new(Plan::Scan(scan))
    });
    Plan::TraceGroup {
        candidates,
        spans: Box::new(Plan::Scan(spans)),
        spanset,
    }
}

async fn run(engine: &Engine, plan: Plan) -> QueryOutput {
    let plan = optimize(plan, &Registry::builtin()).unwrap();
    engine.run(&plan).await.unwrap()
}

/// `(trace, span)` ids of the output, in order.
fn ids(output: &QueryOutput) -> Vec<(u8, u8)> {
    let mut ids = Vec::new();
    for batch in &output.batches {
        let traces = batch
            .column_by_name(TRACE_ID)
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let spans = batch
            .column_by_name(SPAN_ID)
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            ids.push((traces.value(i)[0], spans.value(i)[0]));
        }
    }
    ids
}

#[tokio::test]
async fn a_structural_query_reads_candidates_then_their_traces() {
    let (store, env) = populate().await;
    let engine = engine(&store, &env);
    let output = run(
        &engine,
        group(Spanset::Structural {
            left: Box::new(name_is("GET /")),
            op: Structural {
                relation: Relation::Descendant,
                negated: false,
                union: false,
            },
            right: Box::new(name_is("cache")),
        }),
    )
    .await;
    assert_eq!(ids(&output), vec![(1, 3), (2, 3), (3, 3), (4, 3)]);
    assert_eq!(output.analysis.scans.len(), 2, "candidates, then spans");
    let candidates = &output.analysis.scans[0];
    assert_eq!(
        candidates.rows_from_files + candidates.rows_from_tail,
        8,
        "two spans per trace match a filter"
    );
    let spans = &output.analysis.scans[1];
    assert_eq!(spans.rows_from_files + spans.rows_from_tail, 12);
    let service = output.batches[0]
        .column_by_name(ROOT_SERVICE)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(service.value(0), "api");
}

#[tokio::test]
async fn candidates_restrict_the_span_scan_to_their_traces() {
    let (store, env) = populate().await;
    let engine = engine(&store, &env);
    let only_trace_4 = Spanset::Filter(Some(Expr::column(TRACE_ID).binary(
        BinaryOp::Eq,
        Expr::Literal(tessera_ir::Literal::Bytes(vec![4; 16])),
    )));
    let output = run(
        &engine,
        group(Spanset::And(
            Box::new(only_trace_4),
            Box::new(Spanset::Filter(None)),
        )),
    )
    .await;
    // The second filter matches everything, so every trace is a candidate
    // and only the span scan runs.
    assert_eq!(output.analysis.scans.len(), 1);
    assert_eq!(ids(&output), vec![(4, 1), (4, 2), (4, 3)]);

    let output = run(
        &engine,
        group(Spanset::AggregateFilter {
            input: Box::new(name_is("db")),
            function: tessera_ir::SpanAggregate::Count,
            arg: None,
            op: BinaryOp::Eq,
            value: tessera_ir::Literal::Int(1),
        }),
    )
    .await;
    assert_eq!(output.analysis.scans.len(), 2);
    assert_eq!(
        output.analysis.scans[1].rows_from_files + output.analysis.scans[1].rows_from_tail,
        12,
        "every trace has a db span, so every trace is read"
    );
    let matched = output.batches[0]
        .column_by_name(MATCHED)
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(matched.value(0), 1);
    assert_eq!(output.num_rows(), 4);
}

#[tokio::test]
async fn no_candidates_means_no_span_scan() {
    let (store, env) = populate().await;
    let engine = engine(&store, &env);
    let output = run(&engine, group(name_is("nothing"))).await;
    assert_eq!(output.num_rows(), 0);
    assert_eq!(
        output.analysis.scans.len(),
        1,
        "the span scan is skipped when no trace is a candidate"
    );
}
