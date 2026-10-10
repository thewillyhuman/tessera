// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The engine over real storage: files written by the write path, tail
//! rows in segments, pruning at every stage, and the operators above.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{
    Array, Float64Array, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray,
};
use tessera_exec::{CpuPool, Engine, QueryLimits, QueryOutput};
use tessera_format::ParquetFormat;
use tessera_ir::{AggregateExpr, BinaryOp, Expr, NamedExpr, Plan, Registry, Scan, SortKey};
use tessera_manifest::{intake, registry};
use tessera_object::{MemoryStore, Pools};
use tessera_opt::optimize;
use tessera_types::event::{BODY, SEVERITY_NUMBER, SEVERITY_TEXT, Scalar, TIMESTAMP};
use tessera_types::{
    DatasetName, Env, EventBatch, Kind, LogRow, PoolId, Seq, TenantId, TimeRange, Timestamp, Window,
};
use tessera_wal::{EntryBody, FlushTarget, WriterLog, write_l1_files};

const DAY: i64 = 86_400_000_000_000;

fn dataset() -> DatasetName {
    DatasetName::new("prod").unwrap()
}

/// `count` rows on `day` starting at offset `from`, severities cycling
/// through 9, 13 and 17, bodies naming the row.
fn batch(day: i64, from: u8, count: u8) -> EventBatch {
    let rows: Vec<LogRow> = (from..from + count)
        .map(|i| {
            let severity = [9, 13, 17][usize::from(i % 3)];
            LogRow {
                timestamp: Timestamp::from_nanos(day * DAY + i64::from(i) * 1_000),
                event_id: [i; 16],
                severity_number: Some(severity),
                severity_text: Some(
                    match severity {
                        9 => "INFO",
                        13 => "WARN",
                        _ => "ERROR",
                    }
                    .into(),
                ),
                body: Some(format!("row{i} on day {day}")),
                attributes: BTreeMap::from([
                    ("attr.n".to_owned(), Scalar::Int(i64::from(i))),
                    ("attr.host".to_owned(), Scalar::Text(format!("h{}", i % 2))),
                ]),
                ..LogRow::default()
            }
        })
        .collect();
    EventBatch::from_logs(TenantId::DEFAULT, dataset(), &rows).unwrap()
}

/// Two days of data: day 0 in two files (segments 1 and 2), day 1 in
/// one file (segment 1), and segment 3 still in the tail.
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
        format: Arc::new(ParquetFormat),
    };
    let segments = [
        vec![batch(0, 0, 6), batch(1, 100, 6)],
        vec![batch(0, 50, 6)],
    ];
    // Flush records take sequence numbers too, so each flush covers the
    // segment just appended: `(previous segment, this segment]`.
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
    log.append_segment(&[batch(1, 200, 3)]).await.unwrap();
    (store, env)
}

fn engine(store: &Arc<MemoryStore>, env: &Env) -> Engine {
    let pools = Pools::new(PoolId::new("root").unwrap(), store.clone());
    Engine::new(
        Arc::new(pools),
        Arc::new(ParquetFormat),
        env.clone(),
        CpuPool::new(2),
        QueryLimits::default(),
    )
}

fn scan() -> Scan {
    Scan::all(TenantId::DEFAULT, dataset(), Kind::Log)
}

async fn run(engine: &Engine, plan: Plan) -> QueryOutput {
    let plan = optimize(plan, &Registry::builtin()).unwrap();
    engine.run(&plan).await.unwrap()
}

fn column<'a, T: 'static>(batches: &'a [RecordBatch], name: &str) -> Vec<&'a T> {
    batches
        .iter()
        .map(|b| {
            b.column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<T>()
                .unwrap()
        })
        .collect()
}

fn timestamps(output: &QueryOutput) -> Vec<i64> {
    column::<TimestampNanosecondArray>(&output.batches, TIMESTAMP)
        .iter()
        .flat_map(|c| c.values().to_vec())
        .collect()
}

#[tokio::test]
async fn a_full_scan_reads_files_and_the_tail() {
    let (store, env) = populate().await;
    let engine = engine(&store, &env);
    let output = run(&engine, Plan::Scan(scan())).await;
    assert_eq!(output.num_rows(), 21);
    let scanned = &output.analysis.scans[0];
    assert_eq!(scanned.partitions, 2);
    assert_eq!(scanned.files.files, 3);
    assert_eq!(scanned.files_read, 3);
    assert_eq!(scanned.rows_from_files, 18);
    assert_eq!(scanned.rows_from_tail, 3);
    assert_eq!(scanned.segments, 3);
    assert!(scanned.bytes_read > 0);
}

#[tokio::test]
async fn predicates_prune_files_and_filter_rows() {
    let (store, env) = populate().await;
    let engine = engine(&store, &env);
    let plan = Plan::Scan(scan()).filter(
        Expr::column(SEVERITY_NUMBER)
            .binary(BinaryOp::GtEq, Expr::literal(17))
            .and(
                Expr::Path {
                    path: "attr.host".into(),
                    data_type: arrow::datatypes::DataType::Utf8,
                }
                .binary(BinaryOp::Eq, Expr::literal("h0")),
            ),
    );
    let output = run(&engine, plan).await;
    let severities = column::<arrow::array::Int32Array>(&output.batches, SEVERITY_NUMBER);
    assert!(
        severities
            .iter()
            .all(|c| c.values().iter().all(|s| *s == 17))
    );
    // Row i has severity 17 when i % 3 == 2 and host h0 when i % 2 == 0.
    let total_rows_matching = (0..6u8)
        .chain(100..106)
        .chain(50..56)
        .chain(200..203)
        .filter(|i| i % 3 == 2 && i % 2 == 0)
        .count();
    assert_eq!(output.num_rows(), total_rows_matching);
}

#[tokio::test]
async fn the_time_range_prunes_partitions_and_files() {
    let (store, env) = populate().await;
    let engine = engine(&store, &env);
    let mut day_one = scan();
    day_one.time_range = TimeRange::new(Timestamp::from_nanos(DAY), Timestamp::from_nanos(2 * DAY));
    let output = run(&engine, Plan::Scan(day_one)).await;
    assert_eq!(output.num_rows(), 9, "six in the file, three in the tail");
    let scanned = &output.analysis.scans[0];
    assert_eq!(scanned.partitions_kept, 1);
    assert_eq!(scanned.files.files, 1);

    let plan = Plan::Scan(scan()).filter(
        Expr::column(TIMESTAMP).binary(BinaryOp::Lt, Expr::literal(Timestamp::from_nanos(DAY))),
    );
    let output = run(&engine, plan).await;
    assert_eq!(output.num_rows(), 12);
    let scanned = &output.analysis.scans[0];
    assert_eq!(scanned.partitions_kept, 1);
    assert_eq!(scanned.files.after_time_range, 2);
}

#[tokio::test]
async fn aggregates_group_across_files_and_tail() {
    let (store, env) = populate().await;
    let engine = engine(&store, &env);
    let plan = Plan::Aggregate {
        input: Box::new(Plan::Scan(scan())),
        group_by: vec![NamedExpr::new(Expr::column(SEVERITY_TEXT), "severity")],
        aggregates: vec![
            AggregateExpr {
                function: "count".into(),
                args: vec![],
                name: "n".into(),
            },
            AggregateExpr {
                function: "max".into(),
                args: vec![Expr::column(TIMESTAMP)],
                name: "latest".into(),
            },
        ],
    }
    .sort(vec![SortKey::ascending(Expr::column("severity"))]);
    let output = run(&engine, plan).await;
    assert_eq!(output.num_rows(), 3);
    let names = column::<StringArray>(&output.batches, "severity");
    let counts = column::<Float64Array>(&output.batches, "n");
    let all_names: Vec<&str> = names.iter().flat_map(|c| c.iter().flatten()).collect();
    assert_eq!(all_names, vec!["ERROR", "INFO", "WARN"]);
    let total: f64 = counts.iter().flat_map(|c| c.values().iter()).sum();
    assert!((total - 21.0).abs() < f64::EPSILON);
}

#[tokio::test]
async fn a_newest_first_limit_stops_reading_early() {
    let (store, env) = populate().await;
    let engine = engine(&store, &env);
    let plan = Plan::Scan(scan())
        .sort(vec![SortKey::descending(Expr::column(TIMESTAMP))])
        .limit(0, Some(2));
    let output = run(&engine, plan).await;
    assert_eq!(
        timestamps(&output),
        vec![DAY + 202_000, DAY + 201_000],
        "the two newest rows, from the tail"
    );
    let scanned = &output.analysis.scans[0];
    assert_eq!(scanned.files.files, 3);
    assert!(scanned.files_read < 3, "{scanned:?}");
}

#[tokio::test]
async fn projections_sorts_and_offsets_compose() {
    let (store, env) = populate().await;
    let engine = engine(&store, &env);
    let plan = Plan::Scan(scan())
        .project(vec![
            NamedExpr::new(Expr::column(TIMESTAMP), TIMESTAMP),
            NamedExpr::new(
                Expr::Call {
                    name: "length".into(),
                    args: vec![Expr::column(BODY)],
                },
                "len",
            ),
        ])
        .sort(vec![SortKey::ascending(Expr::column(TIMESTAMP))])
        .limit(1, Some(3));
    let output = run(&engine, plan).await;
    assert_eq!(timestamps(&output), vec![1_000, 2_000, 3_000]);
    let lengths = column::<Int64Array>(&output.batches, "len");
    assert_eq!(lengths[0].value(0), 13, "the length of `row1 on day 0`");
    assert_eq!(output.schema.fields().len(), 2);
}

#[tokio::test]
async fn an_empty_dataset_yields_no_rows_and_no_errors() {
    let store = Arc::new(MemoryStore::new());
    let env = Env::simulated(1, Timestamp::UNIX_EPOCH);
    let engine = engine(&store, &env);
    let output = run(&engine, Plan::Scan(scan())).await;
    assert_eq!(output.num_rows(), 0);
    assert_eq!(output.analysis.scans[0].partitions, 0);
}

/// Compacts every partition of the populated node into indexed L2 files.
async fn compact_all(store: &Arc<MemoryStore>, env: &Env) {
    use tessera_compact::{Compactor, Policy, plan};
    let pools = Arc::new(Pools::new(PoolId::new("root").unwrap(), store.clone()));
    let compactor = Compactor::new(pools, Arc::new(ParquetFormat), env.clone());
    let policy = Policy {
        min_inputs: 1,
        ..Policy::default()
    };
    let root: Arc<dyn tessera_object::ObjectStore> = store.clone();
    for partition in
        tessera_manifest::list_partitions(&*root, &TenantId::DEFAULT, &dataset(), Kind::Log)
            .await
            .unwrap()
    {
        let manifest =
            tessera_manifest::Manifest::open(root.clone(), partition.clone(), env.clone());
        let snapshot = manifest.snapshot().await.unwrap();
        let job = plan(&partition, snapshot.state(), env.clock.now(), &policy).unwrap();
        compactor.run(&manifest, &job).await.unwrap();
    }
}

#[tokio::test]
async fn sidecar_indexes_skip_files_and_row_groups() {
    let (store, env) = populate().await;
    compact_all(&store, &env).await;
    let engine = engine(&store, &env);
    let call = |name: &str, text: &str| Expr::Call {
        name: name.into(),
        args: vec![Expr::column(BODY), Expr::literal(text)],
    };

    // "row10x on day 1" rows live in the day-1 file only.
    let output = run(
        &engine,
        Plan::Scan(scan()).filter(call("contains", "row10")),
    )
    .await;
    assert_eq!(output.num_rows(), 6);
    let scanned = &output.analysis.scans[0];
    assert_eq!(scanned.files.after_statistics, 2);
    assert_eq!(scanned.files_skipped_by_index, 1, "{scanned:?}");
    assert_eq!(scanned.files_read, 1);

    // Nothing anywhere: both files skipped, nothing decoded.
    let output = run(
        &engine,
        Plan::Scan(scan()).filter(call("has_token", "nowhere")),
    )
    .await;
    assert_eq!(output.num_rows(), 0);
    let scanned = &output.analysis.scans[0];
    assert_eq!(scanned.files_skipped_by_index, 2);
    assert_eq!(scanned.row_groups_read, 0);

    // A regular expression with required literals and a LIKE pattern.
    let output = run(
        &engine,
        Plan::Scan(scan()).filter(call("regexp_match", "^row5[0-9] on")),
    )
    .await;
    assert_eq!(output.num_rows(), 6);
    assert_eq!(output.analysis.scans[0].files_skipped_by_index, 1);
    let like = Expr::Like {
        expr: Box::new(Expr::column(BODY)),
        pattern: "%day 1%".into(),
        negated: false,
        case_insensitive: false,
    };
    let output = run(&engine, Plan::Scan(scan()).filter(like)).await;
    assert_eq!(output.num_rows(), 9, "six in the file, three in the tail");
    assert_eq!(output.analysis.scans[0].files_skipped_by_index, 1);
}
