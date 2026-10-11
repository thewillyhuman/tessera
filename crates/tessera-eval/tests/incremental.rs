// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The incremental subset over writer-log segments: a windowed count
//! fires as soon as it holds, positions move so nothing is counted
//! twice, late rows are ignored, and an average waits for the
//! watermark.

use std::collections::BTreeMap;
use std::sync::Arc;

use tessera_eval::{AlertRule, Language, RuleGroup, Status, state};
use tessera_exec::{CpuPool, Engine, QueryLimits};
use tessera_format::ParquetFormat;
use tessera_manifest::registry;
use tessera_object::{MemoryStore, ObjectStore, Pools};
use tessera_types::event::Scalar;
use tessera_types::{DatasetName, Env, EventBatch, LogRow, PoolId, TenantId, Timestamp};
use tessera_wal::WriterLog;

const SECOND: i64 = 1_000_000_000;

fn at(seconds: i64) -> Timestamp {
    Timestamp::from_nanos(seconds * SECOND)
}

fn dataset() -> DatasetName {
    DatasetName::new("prod").unwrap()
}

fn errors(when: Timestamp, count: u8, service: &str, severity: i32) -> EventBatch {
    let rows: Vec<LogRow> = (0..count)
        .map(|i| LogRow {
            timestamp: when,
            event_id: [i
                .wrapping_add(u8::try_from(service.len()).unwrap())
                .wrapping_add(u8::try_from(when.as_nanos() / SECOND % 200).unwrap());
                16],
            severity_number: Some(severity),
            severity_text: Some(if severity >= 17 { "ERROR" } else { "INFO" }.into()),
            body: Some(format!("line {i}")),
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
    log: WriterLog,
}

async fn fixture() -> Fixture {
    let store = Arc::new(MemoryStore::new());
    let env = Env::simulated(1, at(1_000));
    let log = WriterLog::start(store.clone(), env.clone());
    registry::register(
        &*store,
        &env,
        &TenantId::DEFAULT,
        &dataset(),
        log.writer_id(),
    )
    .await
    .unwrap();
    let pools = Pools::new(PoolId::new("root").unwrap(), store.clone());
    let engine = Engine::new(
        Arc::new(pools),
        Arc::new(ParquetFormat::default()),
        env.clone(),
        CpuPool::new(2),
        QueryLimits::default(),
    );
    Fixture {
        store,
        env,
        engine,
        log,
    }
}

fn group(fx: &Fixture, name: &str, expr: &str) -> RuleGroup {
    let mut rule = AlertRule::new(name, Language::Sql, expr);
    rule.dataset = "prod".into();
    RuleGroup::open(
        fx.store.clone(),
        TenantId::DEFAULT,
        "inc",
        vec![rule],
        fx.env.clone(),
    )
}

async fn windows(fx: &Fixture) -> BTreeMap<String, tessera_eval::incremental::WindowState> {
    let store: Arc<dyn ObjectStore> = fx.store.clone();
    let log = state::open(store, &TenantId::DEFAULT, "inc", 0, fx.env.clone());
    log.snapshot().await.unwrap().state().windows.clone()
}

#[tokio::test]
async fn a_windowed_count_fires_eagerly_and_counts_each_segment_once() {
    let mut fx = fixture().await;
    fx.log
        .append_segment(&[errors(at(1_000), 3, "checkout", 17)])
        .await
        .unwrap();
    fx.log
        .append_segment(&[errors(at(1_000), 1, "search", 17)])
        .await
        .unwrap();
    let group = group(
        &fx,
        "errors",
        "SELECT attr.service AS service, count(*) AS n FROM logs \
         WHERE severity_text = 'ERROR' \
         GROUP BY attr.service, tumble(timestamp, INTERVAL '1 minute') \
         HAVING count(*) >= 2",
    );
    let first = group.evaluate(&fx.engine, at(1_005)).await.unwrap();
    assert_eq!(
        first.transitions.len(),
        1,
        "checkout has three errors in its minute"
    );
    let fired = &first.transitions[0];
    assert_eq!(fired.to, Status::Firing);
    assert_eq!(fired.labels["service"], "checkout");
    assert!((fired.value - 3.0).abs() < f64::EPSILON);
    let seen = windows(&fx).await;
    assert_eq!(seen.len(), 2, "one window per service");

    // Two more in the same window: counted on top, not from scratch.
    fx.log
        .append_segment(&[errors(at(1_010), 2, "checkout", 17)])
        .await
        .unwrap();
    let second = group.evaluate(&fx.engine, at(1_020)).await.unwrap();
    assert!(second.transitions.is_empty(), "still firing");
    let seen = windows(&fx).await;
    let checkout = seen
        .values()
        .find(|w| w.keys["service"] == "checkout")
        .unwrap();
    assert_eq!(checkout.partials[0].count, 5);

    // A late row, older than the allowed lateness, is ignored; a new
    // window for search fires on its own.
    fx.log
        .append_segment(&[
            errors(at(900), 9, "checkout", 17),
            errors(at(1_070), 2, "search", 17),
        ])
        .await
        .unwrap();
    let third = group.evaluate(&fx.engine, at(1_075)).await.unwrap();
    assert_eq!(third.transitions.len(), 1);
    assert_eq!(third.transitions[0].labels["service"], "search");
    let seen = windows(&fx).await;
    let checkout = seen
        .values()
        .find(|w| w.keys["service"] == "checkout")
        .unwrap();
    assert_eq!(checkout.partials[0].count, 5, "the late rows did not count");

    let nothing = group.evaluate(&fx.engine, at(1_080)).await.unwrap();
    assert!(nothing.transitions.is_empty());
    assert_eq!(group.instances().await.unwrap().len(), 2);
}

/// CQ-7: a hopping window counts a row in every window it falls in,
/// and the newest one decides.
#[tokio::test]
async fn a_hopping_window_counts_rows_in_every_window_they_fall_in() {
    let mut fx = fixture().await;
    fx.log
        .append_segment(&[errors(at(1_000), 3, "checkout", 17)])
        .await
        .unwrap();
    let group = group(
        &fx,
        "errors",
        "SELECT attr.service AS service, count(*) AS n FROM logs \
         WHERE severity_text = 'ERROR' \
         GROUP BY attr.service, hop(timestamp, INTERVAL '2 minutes', INTERVAL '1 minute') \
         HAVING count(*) >= 2",
    );
    let first = group.evaluate(&fx.engine, at(1_005)).await.unwrap();
    assert_eq!(first.transitions.len(), 1, "fires once per group");
    let seen = windows(&fx).await;
    let mut starts: Vec<i64> = seen
        .values()
        .map(|w| w.start.as_nanos() / 1_000_000_000)
        .collect();
    starts.sort_unstable();
    assert_eq!(
        starts,
        [900, 960],
        "two windows of two minutes, a minute apart, hold 1000 s"
    );
    assert!(seen.values().all(|w| w.partials[0].count == 3));
    // A row at 1030 s is in the windows starting at 960 and 1020, not 900.
    fx.log
        .append_segment(&[errors(at(1_030), 1, "checkout", 17)])
        .await
        .unwrap();
    group.evaluate(&fx.engine, at(1_035)).await.unwrap();
    let seen = windows(&fx).await;
    let count_at = |start: i64| {
        seen.values()
            .find(|w| w.start.as_nanos() / 1_000_000_000 == start)
            .map(|w| w.partials[0].count)
    };
    assert_eq!(count_at(900), Some(3));
    assert_eq!(count_at(960), Some(4));
    assert_eq!(count_at(1_020), Some(1));
}

/// CQ-7: a group that goes quiet is observed with an empty window once
/// the watermark closes one without its rows, so absence fires.
#[tokio::test]
async fn absence_fires_once_a_closed_window_has_no_rows() {
    let mut fx = fixture().await;
    fx.log
        .append_segment(&[errors(at(1_000), 3, "checkout", 17)])
        .await
        .unwrap();
    let group = group(
        &fx,
        "quiet",
        "SELECT attr.service AS service, count(*) AS n FROM logs \
         GROUP BY attr.service, tumble(timestamp, INTERVAL '1 minute') \
         HAVING count(*) < 1",
    );
    let busy = group.evaluate(&fx.engine, at(1_005)).await.unwrap();
    assert!(
        busy.transitions.is_empty(),
        "the window with rows does not fire"
    );
    let closed = group.evaluate(&fx.engine, at(1_040)).await.unwrap();
    assert!(
        closed.transitions.is_empty(),
        "the window closed with rows in it"
    );
    let quiet = group.evaluate(&fx.engine, at(1_100)).await.unwrap();
    let starts: Vec<i64> = windows(&fx)
        .await
        .values()
        .map(|w| w.start.as_nanos() / 1_000_000_000)
        .collect();
    assert_eq!(
        quiet.transitions.len(),
        1,
        "a whole window passed without rows: windows {starts:?}"
    );
    assert_eq!(quiet.transitions[0].to, Status::Firing);
    assert_eq!(quiet.transitions[0].labels["service"], "checkout");
    assert!(quiet.transitions[0].value.abs() < f64::EPSILON);
}

#[tokio::test]
async fn an_average_condition_waits_for_the_watermark() {
    let mut fx = fixture().await;
    fx.log
        .append_segment(&[errors(at(1_000), 2, "checkout", 17)])
        .await
        .unwrap();
    let group = group(
        &fx,
        "severe",
        "SELECT avg(severity_number) AS s FROM logs \
         GROUP BY tumble(timestamp, INTERVAL '1 minute') HAVING avg(severity_number) > 10",
    );
    let early = group.evaluate(&fx.engine, at(1_005)).await.unwrap();
    assert!(early.transitions.is_empty(), "the window is still open");
    let closed = group.evaluate(&fx.engine, at(1_040)).await.unwrap();
    assert_eq!(
        closed.transitions.len(),
        1,
        "the watermark passed the window"
    );
    assert!((closed.transitions[0].value - 17.0).abs() < f64::EPSILON);
}
