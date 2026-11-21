// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Several state partitions per rule group (CQ-6): each partition keeps
//! the instances whose fingerprint hashes to it, and a change of the
//! count re-splits the group's state from a checkpoint partition 0
//! writes, without losing or repeating an instance. With several
//! partitions each writer log is read by one of them, its source,
//! which leaves partial windows for the others (CQ-4).

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use futures::stream::BoxStream;
use tessera_eval::incremental::WindowState;
use tessera_eval::partition::window_partition;
use tessera_eval::{AlertRule, Language, RuleGroup, partition_of, state};
use tessera_exec::{CpuPool, Engine, QueryLimits};
use tessera_format::ParquetFormat;
use tessera_manifest::registry;
use tessera_object::{Key, MemoryStore, ObjectError, ObjectMeta, ObjectStore, Pools, layout};
use tessera_types::event::Scalar;
use tessera_types::{DatasetName, Env, EventBatch, LogRow, PoolId, Seq, TenantId, Timestamp};
use tessera_wal::WriterLog;

const SECOND: i64 = 1_000_000_000;

fn at(seconds: i64) -> Timestamp {
    Timestamp::from_nanos(seconds * SECOND)
}

/// `count` error logs of `service` at `at`.
fn errors(at: Timestamp, count: u8, service: &str) -> EventBatch {
    let rows: Vec<LogRow> = (0..count)
        .map(|i| LogRow {
            timestamp: at,
            event_id: [i.wrapping_add(u8::try_from(service.len()).unwrap()); 16],
            severity_number: Some(17),
            severity_text: Some("ERROR".into()),
            body: Some(format!("boom {i}")),
            attributes: BTreeMap::from([(
                "attr.service".to_owned(),
                Scalar::Text(service.to_owned()),
            )]),
            ..LogRow::default()
        })
        .collect();
    EventBatch::from_logs(TenantId::DEFAULT, DatasetName::new("prod").unwrap(), &rows).unwrap()
}

struct Fixture {
    store: Arc<MemoryStore>,
    env: Env,
    engine: Engine,
    rule: AlertRule,
}

/// A store with one segment per batch in a registered writer log, and
/// a rule with one instance per service with two or more errors.
async fn fixture(segments: &[EventBatch]) -> Fixture {
    let store = Arc::new(MemoryStore::new());
    let env = Env::simulated(1, at(1_000));
    let mut log = WriterLog::start(store.clone(), env.clone());
    registry::register(
        &*store,
        &env,
        &TenantId::DEFAULT,
        &DatasetName::new("prod").unwrap(),
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
    let mut rule = AlertRule::new(
        "errors",
        Language::Sql,
        "SELECT attr.service AS service, count(*) AS n FROM logs \
         WHERE severity_text = 'ERROR' AND timestamp > now() - INTERVAL '5 minutes' \
         GROUP BY attr.service HAVING count(*) >= 2",
    );
    rule.dataset = "prod".into();
    rule.for_seconds = 60;
    rule.every_seconds = 30;
    Fixture {
        store,
        env,
        engine,
        rule,
    }
}

impl Fixture {
    fn partition(&self, part: u32, parts: u32) -> RuleGroup {
        self.partition_over(self.store.clone(), part, parts)
    }

    /// Partition `part` of `parts`, reading and writing through `store`.
    fn partition_over(&self, store: Arc<dyn ObjectStore>, part: u32, parts: u32) -> RuleGroup {
        RuleGroup::open_partition(
            store,
            TenantId::DEFAULT,
            "g",
            vec![self.rule.clone()],
            self.env.clone(),
            part,
            parts,
        )
    }

    async fn fingerprints(&self, group: &RuleGroup) -> BTreeSet<String> {
        group
            .instances()
            .await
            .unwrap()
            .into_iter()
            .map(|i| i.fingerprint)
            .collect()
    }
}

#[tokio::test]
async fn partitions_keep_each_instance_once_across_a_split_and_a_merge() {
    let services = ["checkout", "search", "cart", "login", "billing", "mail"];
    let segments: Vec<EventBatch> = services
        .iter()
        .map(|service| errors(at(1_000), 3, service))
        .collect();
    let fx = fixture(&segments).await;

    // One partition: an instance per service.
    let whole = fx.partition(0, 1);
    let first = whole.evaluate(&fx.engine, at(1_010)).await.unwrap();
    assert_eq!(first.transitions.len(), services.len());
    let all = fx.fingerprints(&whole).await;
    assert_eq!(all.len(), services.len());

    // Split in two. Partition 1 can do nothing until partition 0 has
    // written the split checkpoint; then each keeps its own keys.
    let second = fx.partition(1, 2);
    assert!(
        second
            .evaluate(&fx.engine, at(1_020))
            .await
            .unwrap()
            .rules
            .is_empty()
    );
    assert!(fx.fingerprints(&second).await.is_empty(), "still waiting");
    let first_half = fx.partition(0, 2);
    first_half.evaluate(&fx.engine, at(1_020)).await.unwrap();
    second.evaluate(&fx.engine, at(1_020)).await.unwrap();
    let (mine, theirs) = (
        fx.fingerprints(&first_half).await,
        fx.fingerprints(&second).await,
    );
    assert!(mine.is_disjoint(&theirs));
    assert_eq!(&mine | &theirs, all, "every instance in one partition");
    assert!(mine.iter().all(|fp| partition_of(fp, 2) == 0));
    assert!(theirs.iter().all(|fp| partition_of(fp, 2) == 1));
    assert!(
        !mine.is_empty() && !theirs.is_empty(),
        "{mine:?} {theirs:?}"
    );

    // Each partition fires its own instances when the hold passes: the
    // group fires every instance once between them.
    let fired_0 = first_half.evaluate(&fx.engine, at(1_071)).await.unwrap();
    let fired_1 = second.evaluate(&fx.engine, at(1_071)).await.unwrap();
    assert_eq!(
        fired_0.transitions.len() + fired_1.transitions.len(),
        services.len()
    );
    let fingerprints: BTreeSet<String> = fired_0
        .transitions
        .iter()
        .chain(&fired_1.transitions)
        .map(|t| t.fingerprint.clone())
        .collect();
    assert_eq!(fingerprints, all);

    // Back to one partition: partition 0 merges both and keeps all.
    let merged = fx.partition(0, 1);
    merged.evaluate(&fx.engine, at(1_080)).await.unwrap();
    assert_eq!(fx.fingerprints(&merged).await, all);
    assert!(
        merged
            .instances()
            .await
            .unwrap()
            .iter()
            .all(|i| i.status == tessera_eval::Status::Firing),
        "the instances keep their status through the merge"
    );
}

/// CQ-6: a partition the group did without for a while takes its keys
/// from the checkpoint of the split that brings it back, not from the
/// state it had when the group last counted it.
#[tokio::test]
async fn a_partition_that_sat_out_a_count_takes_the_new_split() {
    let services = ["checkout", "search", "cart", "login", "billing", "mail"];
    let segments: Vec<EventBatch> = services
        .iter()
        .map(|service| errors(at(1_000), 3, service))
        .collect();
    let fx = fixture(&segments).await;

    // Two partitions, each with its instances pending.
    fx.partition(0, 2)
        .evaluate(&fx.engine, at(1_010))
        .await
        .unwrap();
    let second = fx.partition(1, 2);
    second.evaluate(&fx.engine, at(1_010)).await.unwrap();
    assert!(!fx.fingerprints(&second).await.is_empty());

    // One partition, in which every instance fires.
    let whole = fx.partition(0, 1);
    whole.evaluate(&fx.engine, at(1_020)).await.unwrap();
    let fired = whole.evaluate(&fx.engine, at(1_071)).await.unwrap();
    assert_eq!(fired.transitions.len(), services.len());

    // Two again: the second partition's instances are the firing ones
    // of the split, so nothing fires a second time.
    let mut again = Vec::new();
    for part in 0..2 {
        let partition = fx.partition(part, 2);
        let evaluated = partition.evaluate(&fx.engine, at(1_080)).await.unwrap();
        again.extend(evaluated.transitions);
    }
    assert!(again.is_empty(), "{again:?}");
}

/// The fixture's rule over a one-minute tumbling window, which makes
/// it incremental (CQ-7): its windows are kept in the partitions.
fn windowed() -> AlertRule {
    let mut rule = AlertRule::new(
        "errors",
        Language::Sql,
        "SELECT attr.service AS service, count(*) AS n FROM logs \
         WHERE severity_text = 'ERROR' \
         GROUP BY attr.service, tumble(timestamp, INTERVAL '1 minute') \
         HAVING count(*) >= 2",
    );
    rule.dataset = "prod".into();
    rule
}

#[tokio::test]
async fn an_incremental_rule_fires_every_instance_once_between_partitions() {
    let services = ["checkout", "search", "cart", "login", "billing", "mail"];
    let segments: Vec<EventBatch> = services
        .iter()
        .map(|service| errors(at(1_000), 3, service))
        .collect();
    let mut fx = fixture(&segments).await;
    fx.rule = windowed();

    let mut fired: Vec<String> = Vec::new();
    for part in 0..2 {
        let evaluated = fx
            .partition(part, 2)
            .evaluate(&fx.engine, at(1_005))
            .await
            .unwrap();
        fired.extend(
            evaluated
                .transitions
                .iter()
                .map(|t| t.labels["service"].clone()),
        );
    }
    fired.sort();
    let mut expected: Vec<String> = services.iter().map(|s| (*s).to_owned()).collect();
    expected.sort();
    assert_eq!(
        fired, expected,
        "the window of a key is where its instance is"
    );
}

/// A store that remembers the writer-log entries fetched through it.
#[derive(Debug)]
struct Fetches {
    inner: Arc<MemoryStore>,
    entries: Mutex<Vec<String>>,
}

impl Fetches {
    fn over(inner: Arc<MemoryStore>) -> Arc<Self> {
        Arc::new(Fetches {
            inner,
            entries: Mutex::new(Vec::new()),
        })
    }

    fn note(&self, key: &Key) {
        if key.as_str().starts_with("_wal/") {
            self.entries.lock().unwrap().push(key.as_str().to_owned());
        }
    }

    /// The entries fetched since the last call.
    fn take(&self) -> Vec<String> {
        std::mem::take(&mut *self.entries.lock().unwrap())
    }
}

#[async_trait]
impl ObjectStore for Fetches {
    async fn get(&self, key: &Key) -> Result<Bytes, ObjectError> {
        self.note(key);
        self.inner.get(key).await
    }

    async fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Bytes, ObjectError> {
        self.note(key);
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
        self.inner.list(prefix)
    }

    async fn delete(&self, key: &Key) -> Result<(), ObjectError> {
        self.inner.delete(key).await
    }
}

const SERVICES: [&str; 6] = ["checkout", "search", "cart", "login", "billing", "mail"];

/// Two partitions of the windowed rule over two writer logs, each
/// partition the source of one of them (CQ-4): `logs[p]` hashes to
/// partition `p`, which reads through `stores[p]`. Both logs hold one
/// segment and both partitions have evaluated once, which re-split the
/// group and rebuilt the windows from the view.
struct Divided {
    fx: Fixture,
    logs: Vec<WriterLog>,
    stores: Vec<Arc<Fetches>>,
    parts: Vec<RuleGroup>,
}

impl Divided {
    async fn start() -> Self {
        let mut fx = fixture(&[]).await;
        fx.rule = windowed();
        let mut found: [Option<WriterLog>; 2] = [None, None];
        while found.iter().any(Option::is_none) {
            let log = WriterLog::start(fx.store.clone(), fx.env.clone());
            let part = partition_of(&log.writer_id().to_string(), 2);
            found[usize::try_from(part).unwrap()].get_or_insert(log);
        }
        let logs: Vec<WriterLog> = found.into_iter().flatten().collect();
        for log in &logs {
            registry::register(
                &*fx.store,
                &fx.env,
                &TenantId::DEFAULT,
                &DatasetName::new("prod").unwrap(),
                log.writer_id(),
            )
            .await
            .unwrap();
        }
        let stores = vec![
            Fetches::over(fx.store.clone()),
            Fetches::over(fx.store.clone()),
        ];
        let parts = (0..2u32)
            .zip(&stores)
            .map(|(part, store)| fx.partition_over(store.clone(), part, 2))
            .collect();
        let mut divided = Divided {
            fx,
            logs,
            stores,
            parts,
        };
        divided.write(at(1_000)).await;
        divided.evaluate(0, 1_005).await;
        divided.evaluate(1, 1_005).await;
        divided
    }

    /// One more segment in each log: two errors of every service in
    /// the first, three in the second.
    async fn write(&mut self, when: Timestamp) {
        for (log, count) in self.logs.iter_mut().zip([2, 3]) {
            let batches: Vec<EventBatch> = SERVICES
                .iter()
                .map(|service| errors(when, count, service))
                .collect();
            log.append_segment(&batches).await.unwrap();
        }
    }

    async fn evaluate(&self, part: usize, seconds: i64) {
        self.parts[part]
            .evaluate(&self.fx.engine, at(seconds))
            .await
            .unwrap();
    }

    /// The windows partition `part` holds.
    async fn windows(&self, part: u32) -> Vec<WindowState> {
        let store: Arc<dyn ObjectStore> = self.fx.store.clone();
        let log = state::open(store, &TenantId::DEFAULT, "g", part, self.fx.env.clone());
        let snapshot = log.snapshot().await.unwrap();
        snapshot.state().windows.values().cloned().collect()
    }

    /// The entries of `logs[writer]` whose partial windows are left
    /// for partition `part`.
    async fn partials(&self, part: u32, writer: usize) -> Vec<String> {
        let prefix = layout::eval_partials(
            &TenantId::DEFAULT,
            "g",
            2,
            part,
            self.logs[writer].writer_id(),
        );
        self.fx
            .store
            .list(&prefix)
            .map_ok(|meta| meta.key.name().to_owned())
            .try_collect()
            .await
            .unwrap()
    }
}

#[tokio::test]
async fn partitions_read_each_log_once_between_them() {
    let mut divided = Divided::start().await;
    for store in &divided.stores {
        store.take();
    }
    divided.write(at(1_001)).await;
    divided.evaluate(0, 1_008).await;
    divided.evaluate(1, 1_008).await;
    divided.evaluate(0, 1_009).await;

    for (part, store) in divided.stores.iter().enumerate() {
        let entry = layout::writer_log_entry(divided.logs[part].writer_id(), Seq::new(2));
        assert_eq!(
            store.take(),
            [entry.as_str().to_owned()],
            "partition {part} reads the new entry of its own log, once, and no other"
        );
    }
    let mut services = BTreeSet::new();
    for part in 0..2 {
        for window in divided.windows(part).await {
            let service = window.keys["service"].clone();
            assert_eq!(
                window.partials[0].count, 10,
                "{service}: two and three errors, written twice"
            );
            assert_eq!(window_partition(&divided.fx.rule, &window.keys, 2), part);
            assert!(services.insert(service), "a service is in one partition");
        }
    }
    assert_eq!(services.len(), SERVICES.len(), "{services:?}");
}

#[tokio::test]
async fn a_source_trims_the_partials_of_collected_entries() {
    let mut divided = Divided::start().await;
    divided.write(at(1_001)).await;
    divided.evaluate(0, 1_008).await;
    let (first, second) = (Seq::new(1).to_string(), Seq::new(2).to_string());
    for part in 0..2 {
        assert_eq!(
            divided.partials(part, 0).await,
            [first.as_str(), second.as_str()],
            "one object per entry and partition"
        );
    }

    // Garbage collection removes the first entry of the log (GC-2).
    let collected = layout::writer_log_entry(divided.logs[0].writer_id(), Seq::new(1));
    divided.fx.store.delete(&collected).await.unwrap();
    divided.evaluate(0, 1_009).await;
    for part in 0..2 {
        assert_eq!(divided.partials(part, 0).await, [second.as_str()]);
    }
}

#[tokio::test]
async fn a_re_split_clears_the_partials_of_the_old_count() {
    let divided = Divided::start().await;
    assert!(!divided.partials(1, 0).await.is_empty());

    let whole = divided.fx.partition(0, 1);
    whole.evaluate(&divided.fx.engine, at(1_008)).await.unwrap();
    let left: Vec<ObjectMeta> = divided
        .fx
        .store
        .list(&layout::eval_partials_of(&TenantId::DEFAULT, "g", 2))
        .try_collect()
        .await
        .unwrap();
    assert!(left.is_empty(), "{left:?}");
}
