// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Trace completion at L3 (QL-14): once a span partition's window has
//! ended a completion horizon ago, compaction rewrites the partition to
//! L3 with the shape of every trace that lies whole within the window
//! stored in columns of the file; the spans of the other traces carry
//! nulls there and a query numbers them itself.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, FixedSizeBinaryArray, Int64Array, StringArray};
use tessera_compact::{Compactor, Policy, TRACE_COMPLETION_HORIZON, plan};
use tessera_format::ParquetFormat;
use tessera_manifest::{LayoutRecord, Manifest, intake, list_partitions, registry, view};
use tessera_object::{MemoryStore, ObjectStore, Pools};
use tessera_types::event::{SPAN_ID, Scalar};
use tessera_types::{
    DatasetName, Env, EventBatch, Kind, Level, PoolId, Seq, SpanKind, SpanRow, StatusCode,
    TenantId, Timestamp, Window,
};
use tessera_wal::{EntryBody, FlushTarget, WriterLog, write_l1_files};

const SECOND: i64 = 1_000_000_000;
/// The first instant of the second day: the partition's window.
const WINDOW_START: i64 = 86_400 * SECOND;

fn dataset() -> DatasetName {
    DatasetName::new("prod").unwrap()
}

/// A span of `trace` at `at` seconds into the window, lasting a second.
fn span(trace: u8, id: u8, parent: Option<u8>, at: i64, name: &str) -> SpanRow {
    SpanRow {
        timestamp: Timestamp::from_nanos(WINDOW_START + at * SECOND),
        observed_timestamp: Timestamp::from_nanos(WINDOW_START + at * SECOND),
        event_id: [id; 16],
        trace_id: [trace; 16],
        span_id: [id; 8],
        parent_span_id: parent.map(|p| [p; 8]),
        name: name.into(),
        span_kind: SpanKind::Server,
        duration_ns: SECOND,
        status_code: StatusCode::Ok,
        status_message: None,
        links: None,
        attributes: BTreeMap::from([(
            "resource.service.name".to_owned(),
            Scalar::Text("api".into()),
        )]),
    }
}

fn spans(rows: &[SpanRow]) -> EventBatch {
    EventBatch::from_spans(TenantId::DEFAULT, dataset(), rows).unwrap()
}

/// A node with `segments` flushed as one L1 file each and adopted.
async fn node(segments: Vec<EventBatch>) -> (Arc<MemoryStore>, Env, Arc<Pools>) {
    let store = Arc::new(MemoryStore::new());
    let env = Env::simulated(1, Timestamp::from_nanos(WINDOW_START));
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
    let mut after = Seq::ZERO;
    for segment in segments {
        let through = log
            .append_segment(std::slice::from_ref(&segment))
            .await
            .unwrap();
        let files = write_l1_files(
            &target,
            &env,
            log.writer_id(),
            after,
            through,
            Window::DEFAULT_LENGTH,
            vec![segment],
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
    let pools = Arc::new(Pools::new(PoolId::new("root").unwrap(), store.clone()));
    (store, env, pools)
}

/// A span's id, its stored nested-set left and parent numbers and its
/// trace's root name, as the compacted files hold them.
type Stored = (u8, Option<i64>, Option<i64>, Option<String>);

/// What the partition's files store of every span, in span id order.
async fn stored(pools: &Arc<Pools>, env: &Env) -> Vec<Stored> {
    let format: Arc<dyn tessera_format::FileFormat> = Arc::new(ParquetFormat::default());
    let read = view::read(
        pools,
        &format,
        env,
        &TenantId::DEFAULT,
        &dataset(),
        Kind::Span,
        Window::DEFAULT_LENGTH,
    )
    .await
    .unwrap();
    let mut stored: Vec<Stored> = Vec::new();
    for batch in &read.rows {
        let ids = batch
            .column_by_name(SPAN_ID)
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let longs = |name: &str| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .clone()
        };
        let (left, parent) = (longs("nest_left"), longs("nest_parent"));
        let names = batch
            .column_by_name("root_name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            stored.push((
                ids.value(i)[0],
                (!left.is_null(i)).then(|| left.value(i)),
                (!parent.is_null(i)).then(|| parent.value(i)),
                (!names.is_null(i)).then(|| names.value(i).to_owned()),
            ));
        }
    }
    stored.sort();
    stored
}

#[tokio::test]
async fn l3_stores_the_shape_of_the_traces_whole_within_the_partition() {
    // Trace 1 is whole: a root and its child, inside the window. Trace 2
    // is a child whose parent never arrived.
    let (store, env, pools) = node(vec![
        spans(&[span(1, 1, None, 10, "GET /"), span(1, 2, Some(1), 11, "db")]),
        spans(&[span(2, 4, Some(9), 20, "orphan")]),
    ])
    .await;
    let partitions = list_partitions(&*store, &TenantId::DEFAULT, &dataset(), Kind::Span)
        .await
        .unwrap();
    assert_eq!(partitions.len(), 1);
    let root: Arc<dyn ObjectStore> = store.clone();
    let manifest = Manifest::open(root, partitions[0].clone(), env.clone());
    let compactor = Compactor::new(
        pools.clone(),
        Arc::new(ParquetFormat::default()),
        env.clone(),
    );
    let policy = Policy {
        min_inputs: 1,
        ..Policy::default()
    };
    let layout = LayoutRecord::default_for(Kind::Span);

    // L2 stores nothing of the traces.
    let snapshot = manifest.snapshot().await.unwrap();
    let job = plan(manifest.id(), snapshot.state(), env.clock.now(), &policy).unwrap();
    assert_eq!(job.level, Level::L2);
    compactor
        .run(&manifest, &job, &layout, None, &[])
        .await
        .unwrap();
    let snapshot = manifest.snapshot().await.unwrap();
    assert!(
        snapshot
            .state()
            .files()
            .all(|f| f.columns.iter().all(|c| c.name != "nest_left"))
    );

    // Past the completion horizon the partition retires to L3.
    let later = partitions[0]
        .window
        .end()
        .checked_add(TRACE_COMPLETION_HORIZON + Duration::from_secs(1))
        .unwrap();
    let job = plan(manifest.id(), snapshot.state(), later, &policy).unwrap();
    assert_eq!(job.level, Level::L3);
    compactor
        .run(&manifest, &job, &layout, None, &[])
        .await
        .unwrap();
    let snapshot = manifest.snapshot().await.unwrap();
    let file = snapshot
        .state()
        .files()
        .find(|f| f.level == Level::L3)
        .expect("one L3 file");
    assert!(
        file.columns.iter().any(|c| c.name == "nest_left"),
        "statistics of the stored columns: {:?}",
        file.columns.iter().map(|c| &c.name).collect::<Vec<_>>()
    );

    assert_eq!(
        stored(&pools, &env).await,
        [
            (1, Some(1), Some(-1), Some("GET /".into())),
            (2, Some(2), Some(1), Some("GET /".into())),
            (4, None, None, None),
        ]
    );
}
