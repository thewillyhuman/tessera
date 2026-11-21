// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Several state partitions per rule group (CQ-6): each partition keeps
//! the instances whose fingerprint hashes to it, and a change of the
//! count re-splits the group's state from a checkpoint partition 0
//! writes, without losing or repeating an instance.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use tessera_eval::{AlertRule, Language, RuleGroup, partition_of};
use tessera_exec::{CpuPool, Engine, QueryLimits};
use tessera_format::ParquetFormat;
use tessera_manifest::registry;
use tessera_object::{MemoryStore, Pools};
use tessera_types::event::Scalar;
use tessera_types::{DatasetName, Env, EventBatch, LogRow, PoolId, TenantId, Timestamp};
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
        RuleGroup::open_partition(
            self.store.clone(),
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
