// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Rollups end to end (section 8.7): closed windows of a rollup's query
//! over rows in a writer log are evaluated and written as events of the
//! target dataset, the progress mark moves, and a round with nothing
//! due writes nothing.

use std::collections::BTreeMap;
use std::sync::Arc;

use tessera_eval::{MemorySink, Origin, Rollup, maintain};
use tessera_exec::{CpuPool, Engine, QueryLimits};
use tessera_format::ParquetFormat;
use tessera_manifest::registry;
use tessera_object::{MemoryStore, Pools};
use tessera_types::event::Scalar;
use tessera_types::{DatasetName, Env, EventBatch, LogRow, PoolId, TenantId, Timestamp};
use tessera_wal::WriterLog;

const SECOND: i64 = 1_000_000_000;

fn dataset() -> DatasetName {
    DatasetName::new("prod").unwrap()
}

fn at(seconds: i64) -> Timestamp {
    Timestamp::from_nanos(seconds * SECOND)
}

/// `count` error lines at `at`, from `service`.
fn errors(at: Timestamp, count: u8, service: &str) -> EventBatch {
    let rows: Vec<LogRow> = (0..count)
        .map(|i| LogRow {
            timestamp: at,
            event_id: [i.wrapping_add(u8::try_from(at.as_nanos() / SECOND % 251).unwrap()); 16],
            severity_number: Some(17),
            body: Some(format!("boom {i}")),
            attributes: BTreeMap::from([(
                "attr.service".to_owned(),
                Scalar::Text(service.to_owned()),
            )]),
            ..LogRow::default()
        })
        .collect();
    EventBatch::from_logs(TenantId::DEFAULT, dataset(), &rows).unwrap()
}

struct Fixture {
    store: Arc<MemoryStore>,
    env: Env,
    engine: Engine,
}

async fn fixture(segments: &[EventBatch], now: Timestamp) -> Fixture {
    let store = Arc::new(MemoryStore::new());
    let env = Env::simulated(1, now);
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
    for segment in segments {
        log.append_segment(std::slice::from_ref(segment))
            .await
            .unwrap();
    }
    let pools = Pools::new(PoolId::new("root").unwrap(), store.clone());
    let engine = Engine::new(
        Arc::new(pools),
        Arc::new(ParquetFormat::default()),
        env.clone(),
        CpuPool::new(2),
        QueryLimits::default(),
    );
    Fixture { store, env, engine }
}

fn rollup() -> Rollup {
    Rollup {
        name: "errors".into(),
        expr: "SELECT attr.service AS service, count(*) AS n FROM logs \
               WHERE severity_number >= 17 \
               GROUP BY attr.service, tumble(timestamp, INTERVAL '1 minute')"
            .into(),
        dataset: "prod".into(),
        target: "rollup_errors".into(),
        window_seconds: 60,
        every_seconds: 60,
        origin: Origin::User,
        enabled: true,
    }
}

#[tokio::test]
async fn closed_windows_are_written_once_as_events_of_the_target() {
    // Rows in the windows [960, 1020) and [1020, 1080); the clock at 1090
    // closes the second window once the lateness allowance has passed.
    let fx = fixture(
        &[
            errors(at(1_000), 3, "checkout"),
            errors(at(1_000), 1, "search"),
            errors(at(1_070), 2, "checkout"),
        ],
        at(1_090),
    )
    .await;
    let sink = MemorySink::default();
    let first = maintain(
        &fx.engine,
        fx.store.clone(),
        &fx.env,
        &TenantId::DEFAULT,
        &rollup(),
        &sink,
        at(1_090),
    )
    .await
    .unwrap();
    assert_eq!(first.windows, 1, "the newest closed window");
    assert_eq!(first.rows, 1, "one group in [1020, 1080)");
    let batches = sink.batches();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].dataset().as_str(), "rollup_errors");
    let rows = batches[0].rows();
    let names = rows
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap()
        .clone();
    assert_eq!(names.value(0), "errors");
    let times = rows
        .column_by_name("timestamp")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::TimestampNanosecondArray>()
        .unwrap()
        .clone();
    assert_eq!(times.value(0), at(1_020).as_nanos(), "the window start");
    let overflow = rows
        .column_by_name("_overflow")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap()
        .clone();
    assert!(
        overflow.value(0).contains("\"attr.service\":\"checkout\""),
        "{}",
        overflow.value(0)
    );
    assert!(
        overflow.value(0).contains("\"attr.n\":2"),
        "{}",
        overflow.value(0)
    );

    // Nothing new has closed: the round writes nothing.
    let again = maintain(
        &fx.engine,
        fx.store.clone(),
        &fx.env,
        &TenantId::DEFAULT,
        &rollup(),
        &sink,
        at(1_090),
    )
    .await
    .unwrap();
    assert_eq!(again.windows, 0);
    assert_eq!(sink.batches().len(), 1);

    // An hour later every window since the mark is evaluated, and the
    // older rows are not revisited.
    let later = maintain(
        &fx.engine,
        fx.store.clone(),
        &fx.env,
        &TenantId::DEFAULT,
        &rollup(),
        &sink,
        at(4_700),
    )
    .await
    .unwrap();
    assert_eq!(later.windows, 60, "[1080, 4680) in one-minute steps");
    assert_eq!(later.rows, 0);
}

#[tokio::test]
async fn a_replayed_window_keeps_its_event_ids() {
    let fx = fixture(&[errors(at(1_070), 2, "checkout")], at(1_090)).await;
    let sink = MemorySink::default();
    let window = tessera_types::TimeRange::new(at(1_020), at(1_080));
    let once =
        tessera_eval::materialize(&fx.engine, &TenantId::DEFAULT, &rollup(), window, at(1_090))
            .await
            .unwrap()
            .unwrap();
    let twice =
        tessera_eval::materialize(&fx.engine, &TenantId::DEFAULT, &rollup(), window, at(1_095))
            .await
            .unwrap()
            .unwrap();
    let id = |batch: &EventBatch| {
        batch
            .rows()
            .column_by_name("event_id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .unwrap()
            .value(0)
            .to_vec()
    };
    assert_eq!(id(&once), id(&twice), "deduplication removes the replay");
    drop(sink);
}
