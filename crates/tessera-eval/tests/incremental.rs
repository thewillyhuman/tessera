// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The incremental subset over writer-log segments: a windowed count
//! fires as soon as it holds, positions move so nothing is counted
//! twice, late rows are ignored, and an average waits for the
//! watermark.

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use tessera_eval::{AlertRule, Language, RuleGroup, Status, state};
use tessera_exec::{CpuPool, Engine, QueryLimits};
use tessera_format::ParquetFormat;
use tessera_manifest::{intake, registry};
use tessera_object::{Key, MemoryStore, ObjectError, ObjectMeta, ObjectStore, Pools, layout};
use tessera_types::event::Scalar;
use tessera_types::{
    DatasetName, Env, EventBatch, LogRow, PoolId, Seq, TenantId, Timestamp, Window,
};
use tessera_wal::{EntryBody, FlushTarget, WriterLog, write_l1_files};

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

async fn eval_state(fx: &Fixture) -> state::EvalState {
    let store: Arc<dyn ObjectStore> = fx.store.clone();
    let log = state::open(store, &TenantId::DEFAULT, "inc", 0, fx.env.clone());
    log.snapshot().await.unwrap().state().clone()
}

async fn windows(fx: &Fixture) -> BTreeMap<String, tessera_eval::incremental::WindowState> {
    eval_state(fx).await.windows
}

/// Ends the writer's log, lets intake adopt it and leave the registry,
/// and removes every entry of the log, as garbage collection would.
async fn end_and_collect(fx: &mut Fixture) {
    let last = fx.log.end().await.unwrap();
    intake::run(
        fx.store.clone(),
        &fx.env,
        &TenantId::DEFAULT,
        &dataset(),
        fx.log.writer_id(),
    )
    .await
    .unwrap();
    assert!(
        registry::list(&*fx.store, &TenantId::DEFAULT, &dataset())
            .await
            .unwrap()
            .is_empty(),
        "an ended, adopted log leaves the registry"
    );
    let mut seq = Seq::new(1);
    while seq <= last {
        fx.store
            .delete(&layout::writer_log_entry(fx.log.writer_id(), seq))
            .await
            .unwrap();
        seq = seq.next();
    }
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

/// CQ-7: a sliding window advances by the rule's evaluation interval.
#[tokio::test]
async fn a_sliding_window_advances_by_the_rules_interval() {
    let mut fx = fixture().await;
    fx.log
        .append_segment(&[errors(at(1_000), 3, "checkout", 17)])
        .await
        .unwrap();
    let group = group(
        &fx,
        "sliding",
        "SELECT attr.service AS service, count(*) AS n FROM logs \
         GROUP BY attr.service, slide(timestamp, INTERVAL '3 minutes') \
         HAVING count(*) >= 2",
    );
    group.evaluate(&fx.engine, at(1_005)).await.unwrap();
    let seen = windows(&fx).await;
    let mut starts: Vec<i64> = seen
        .values()
        .map(|w| w.start.as_nanos() / 1_000_000_000)
        .collect();
    starts.sort_unstable();
    assert_eq!(
        starts,
        [840, 900, 960],
        "a minute's step, the rule's EVERY, over three minutes"
    );
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

/// Flushes `batches`, segments `(after, through]`, to files and adopts
/// them, as the ingester and the intake do.
async fn flush_and_adopt(fx: &mut Fixture, after: Seq, through: Seq, batches: Vec<EventBatch>) {
    let target = FlushTarget {
        store: fx.store.clone(),
        pool: PoolId::new("root").unwrap(),
        format: Arc::new(ParquetFormat::default()),
    };
    let files = write_l1_files(
        &target,
        &fx.env,
        fx.log.writer_id(),
        after,
        through,
        Window::DEFAULT_LENGTH,
        batches,
    )
    .await
    .unwrap();
    fx.log
        .append(EntryBody::Flush {
            tenant: TenantId::DEFAULT,
            dataset: dataset(),
            after,
            through,
            files,
        })
        .await
        .unwrap();
    intake::run(
        fx.store.clone(),
        &fx.env,
        &TenantId::DEFAULT,
        &dataset(),
        fx.log.writer_id(),
    )
    .await
    .unwrap();
}

/// Section 11.7: the log of a writer that ended and left the registry
/// was removed whole before the rule read its last entries; the rule
/// rebuilds its windows from the files rather than lose them.
#[tokio::test]
async fn a_log_gone_whole_before_its_end_was_read_rebuilds_from_the_files() {
    let mut fx = fixture().await;
    let first = errors(at(1_000), 3, "checkout", 17);
    fx.log
        .append_segment(std::slice::from_ref(&first))
        .await
        .unwrap();
    let group = group(
        &fx,
        "errors",
        "SELECT attr.service AS service, count(*) AS n FROM logs \
         WHERE severity_text = 'ERROR' \
         GROUP BY attr.service, tumble(timestamp, INTERVAL '1 minute') \
         HAVING count(*) >= 5",
    );
    let early = group.evaluate(&fx.engine, at(1_005)).await.unwrap();
    assert!(early.transitions.is_empty(), "three errors are not five");

    // Two more errors the rule never reads before the log is flushed,
    // ended, adopted and collected whole.
    let second = errors(at(1_010), 2, "checkout", 17);
    let seq2 = fx
        .log
        .append_segment(std::slice::from_ref(&second))
        .await
        .unwrap();
    flush_and_adopt(&mut fx, Seq::ZERO, seq2, vec![first, second]).await;
    end_and_collect(&mut fx).await;

    let rebuilt = group.evaluate(&fx.engine, at(1_015)).await.unwrap();
    assert_eq!(rebuilt.degraded, ["errors"], "the rule is marked degraded");
    assert_eq!(rebuilt.transitions.len(), 1, "the files hold all five");
    assert_eq!(rebuilt.transitions[0].to, Status::Firing);
    assert!((rebuilt.transitions[0].value - 5.0).abs() < f64::EPSILON);
    let state = eval_state(&fx).await;
    assert!(
        !state
            .positions
            .contains_key(&fx.log.writer_id().to_string()),
        "the gone log is forgotten: {:?}",
        state.positions
    );
}

/// Section 11.7: a log read to its end is no gap once gone, and the
/// position and watermark held in it are forgotten with its writer.
#[tokio::test]
async fn a_log_read_to_its_end_is_forgotten_once_gone() {
    let mut fx = fixture().await;
    let rows = errors(at(1_000), 3, "checkout", 17);
    let seq1 = fx
        .log
        .append_segment(std::slice::from_ref(&rows))
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
    let fired = group.evaluate(&fx.engine, at(1_005)).await.unwrap();
    assert_eq!(fired.transitions.len(), 1);
    flush_and_adopt(&mut fx, Seq::ZERO, seq1, vec![rows]).await;
    let last = fx.log.end().await.unwrap();
    intake::run(
        fx.store.clone(),
        &fx.env,
        &TenantId::DEFAULT,
        &dataset(),
        fx.log.writer_id(),
    )
    .await
    .unwrap();

    // The writer left the registry; its log is still read, to its end.
    let drained = group.evaluate(&fx.engine, at(1_010)).await.unwrap();
    assert!(drained.degraded.is_empty());
    let writer = fx.log.writer_id().to_string();
    let state = eval_state(&fx).await;
    assert_eq!(state.positions.get(&writer), Some(&last));
    assert!(state.ended.contains(&writer));

    // Gone whole afterwards, the log is no gap: nothing is rebuilt and
    // the writer is forgotten.
    let mut seq = Seq::new(1);
    while seq <= last {
        fx.store
            .delete(&layout::writer_log_entry(fx.log.writer_id(), seq))
            .await
            .unwrap();
        seq = seq.next();
    }
    let after = group.evaluate(&fx.engine, at(1_015)).await.unwrap();
    assert!(after.degraded.is_empty(), "{:?}", after.degraded);
    let state = eval_state(&fx).await;
    assert!(!state.positions.contains_key(&writer));
    assert!(!state.watermarks.contains_key(&writer));
    assert!(!state.ended.contains(&writer));
    let seen = windows(&fx).await;
    assert_eq!(seen.values().next().unwrap().partials[0].count, 3);
}

/// Section 11.7: a log trimmed past the position a rule holds means
/// segments it never read are gone; the rule rebuilds its open windows
/// from the files and the live tail, resumes from the live log, and
/// stays degraded until the horizon has closed the rebuilt windows.
#[tokio::test]
async fn a_rule_behind_a_trimmed_log_rebuilds_from_the_files() {
    let mut fx = fixture().await;
    let first = errors(at(1_000), 3, "checkout", 17);
    let seq1 = fx
        .log
        .append_segment(std::slice::from_ref(&first))
        .await
        .unwrap();
    let group = group(
        &fx,
        "errors",
        "SELECT attr.service AS service, count(*) AS n FROM logs \
         WHERE severity_text = 'ERROR' \
         GROUP BY attr.service, tumble(timestamp, INTERVAL '1 minute') \
         HAVING count(*) >= 5",
    );
    let early = group.evaluate(&fx.engine, at(1_005)).await.unwrap();
    assert!(early.transitions.is_empty(), "three errors are not five");
    assert!(early.degraded.is_empty());

    // The segment goes to a file and is adopted; then garbage collection
    // removes it and its flush record before the rule reads again, and
    // a new segment lands on the live log.
    flush_and_adopt(&mut fx, Seq::ZERO, seq1, vec![first]).await;
    let writer = fx.log.writer_id();
    fx.store
        .delete(&layout::writer_log_entry(writer, Seq::new(1)))
        .await
        .unwrap();
    fx.store
        .delete(&layout::writer_log_entry(writer, Seq::new(2)))
        .await
        .unwrap();
    fx.log
        .append_segment(&[errors(at(1_010), 2, "checkout", 17)])
        .await
        .unwrap();

    let rebuilt = group.evaluate(&fx.engine, at(1_015)).await.unwrap();
    assert_eq!(rebuilt.degraded, ["errors"], "the rule is marked degraded");
    assert_eq!(
        rebuilt.transitions.len(),
        1,
        "the file's three and the tail's two make five"
    );
    assert_eq!(rebuilt.transitions[0].to, Status::Firing);
    assert!((rebuilt.transitions[0].value - 5.0).abs() < f64::EPSILON);
    let seen = windows(&fx).await;
    assert_eq!(seen.len(), 1);
    assert_eq!(seen.values().next().unwrap().partials[0].count, 5);

    // The live log resumes after what the view read: one more row in
    // the same window is counted once, on top, and the mark holds while
    // the window is open.
    fx.log
        .append_segment(&[errors(at(1_015), 1, "checkout", 17)])
        .await
        .unwrap();
    let resumed = group.evaluate(&fx.engine, at(1_025)).await.unwrap();
    assert_eq!(resumed.degraded, ["errors"]);
    let seen = windows(&fx).await;
    assert_eq!(seen.values().next().unwrap().partials[0].count, 6);

    // A window length after the rebuild the horizon has closed every
    // window it could have touched, and the mark clears.
    let later = group.evaluate(&fx.engine, at(1_100)).await.unwrap();
    assert!(later.degraded.is_empty(), "{:?}", later.degraded);
}

/// A store that watches one writer's log: it remembers the entries
/// fetched, and lets a held-back entry land the second time the log is
/// listed, as a segment written while a rule group evaluates.
#[derive(Debug)]
struct WatchedLog {
    inner: Arc<MemoryStore>,
    log: Key,
    listed: AtomicUsize,
    landing: Mutex<Option<(Key, Bytes)>>,
    fetched: Mutex<Vec<String>>,
}

impl WatchedLog {
    fn of(fx: &Fixture, landing: Option<(Key, Bytes)>) -> Arc<Self> {
        Arc::new(WatchedLog {
            inner: fx.store.clone(),
            log: layout::writer_log(fx.log.writer_id()),
            listed: AtomicUsize::new(0),
            landing: Mutex::new(landing),
            fetched: Mutex::new(Vec::new()),
        })
    }
}

#[async_trait]
impl ObjectStore for WatchedLog {
    async fn get(&self, key: &Key) -> Result<Bytes, ObjectError> {
        if key.is_under(&self.log) {
            self.fetched.lock().unwrap().push(key.name().to_owned());
        }
        self.inner.get(key).await
    }

    async fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Bytes, ObjectError> {
        self.inner.get_range(key, range).await
    }

    async fn head(&self, key: &Key) -> Result<ObjectMeta, ObjectError> {
        self.inner.head(key).await
    }

    async fn put(&self, key: &Key, body: Bytes) -> Result<(), ObjectError> {
        self.inner.put(key, body).await
    }

    async fn put_if_absent(&self, key: &Key, body: Bytes) -> Result<(), ObjectError> {
        self.inner.put_if_absent(key, body).await
    }

    fn list(&self, prefix: &Key) -> BoxStream<'_, Result<ObjectMeta, ObjectError>> {
        if *prefix == self.log
            && self.listed.fetch_add(1, Ordering::SeqCst) == 1
            && let Some((key, body)) = self.landing.lock().unwrap().take()
        {
            // The memory store answers at once, so this never waits.
            futures::executor::block_on(self.inner.put(&key, body)).unwrap();
        }
        self.inner.list(prefix)
    }

    async fn delete(&self, key: &Key) -> Result<(), ObjectError> {
        self.inner.delete(key).await
    }
}

/// A group of two rules that each count the errors of a minute, over
/// `store`.
fn two_counts(fx: &Fixture, store: Arc<dyn ObjectStore>) -> RuleGroup {
    let rules = ["first", "second"]
        .into_iter()
        .map(|name| {
            let mut rule = AlertRule::new(
                name,
                Language::Sql,
                "SELECT count(*) AS n FROM logs WHERE severity_text = 'ERROR' \
                 GROUP BY tumble(timestamp, INTERVAL '1 minute') HAVING count(*) >= 1",
            );
            rule.dataset = "prod".into();
            rule
        })
        .collect();
    RuleGroup::open(store, TenantId::DEFAULT, "inc", rules, fx.env.clone())
}

/// The rows each rule has counted.
async fn counted(fx: &Fixture) -> BTreeMap<String, u64> {
    windows(fx)
        .await
        .values()
        .map(|w| (w.rule.clone(), w.partials[0].count))
        .collect()
}

/// CQ-5: the rules of a group share the positions in the writer logs,
/// so they read the logs together: a segment that lands between one
/// rule and the next is not behind the position for either.
#[tokio::test]
async fn a_segment_landing_while_a_group_evaluates_counts_for_every_rule() {
    let mut fx = fixture().await;
    fx.log
        .append_segment(&[errors(at(1_000), 3, "checkout", 17)])
        .await
        .unwrap();
    let second = fx
        .log
        .append_segment(&[errors(at(1_001), 2, "checkout", 17)])
        .await
        .unwrap();
    let key = layout::writer_log_entry(fx.log.writer_id(), second);
    let body = fx.store.get(&key).await.unwrap();
    fx.store.delete(&key).await.unwrap();
    let group = two_counts(&fx, WatchedLog::of(&fx, Some((key, body))));

    group.evaluate(&fx.engine, at(1_005)).await.unwrap();
    group.evaluate(&fx.engine, at(1_006)).await.unwrap();
    let both = BTreeMap::from([("first".to_owned(), 5), ("second".to_owned(), 5)]);
    assert_eq!(counted(&fx).await, both);
}

/// CQ-2: an entry is fetched once for all the rules of a group.
#[tokio::test]
async fn a_group_fetches_each_entry_once_for_all_its_rules() {
    let mut fx = fixture().await;
    let seq = fx
        .log
        .append_segment(&[errors(at(1_000), 3, "checkout", 17)])
        .await
        .unwrap();
    let store = WatchedLog::of(&fx, None);
    let group = two_counts(&fx, store.clone());

    group.evaluate(&fx.engine, at(1_005)).await.unwrap();
    assert_eq!(*store.fetched.lock().unwrap(), [seq.to_string()]);
    let both = BTreeMap::from([("first".to_owned(), 3), ("second".to_owned(), 3)]);
    assert_eq!(counted(&fx).await, both);
}

/// A rule that only groups has no aggregate to take its value from:
/// its instances carry one, as those of a rule run through the engine
/// do, and the state they are written to reads back.
#[tokio::test]
async fn a_rule_without_aggregates_gives_its_instances_the_value_one() {
    let mut fx = fixture().await;
    fx.log
        .append_segment(&[errors(at(1_000), 3, "checkout", 17)])
        .await
        .unwrap();
    let group = group(
        &fx,
        "seen",
        "SELECT attr.service AS service FROM logs \
         GROUP BY attr.service, tumble(timestamp, INTERVAL '1 minute')",
    );
    let first = group.evaluate(&fx.engine, at(1_005)).await.unwrap();
    assert_eq!(first.transitions.len(), 1);
    assert!((first.transitions[0].value - 1.0).abs() < f64::EPSILON);
    group.evaluate(&fx.engine, at(1_006)).await.unwrap();
    assert_eq!(group.instances().await.unwrap().len(), 1);
}
