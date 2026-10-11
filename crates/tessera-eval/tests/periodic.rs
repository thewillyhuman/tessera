// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Periodic evaluation end to end: rules in SQL and `PromQL` over rows
//! in a writer log, the lifecycle through pending, firing and resolved,
//! and alert events delivered once per transition.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, StringArray};
use tessera_eval::{AlertRule, Delivery, Language, MemoryNotifier, MemorySink, RuleGroup, Status};
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

/// `count` error lines at `at`, from `service`.
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
    EventBatch::from_logs(TenantId::DEFAULT, dataset(), &rows).unwrap()
}

struct Fixture {
    store: Arc<MemoryStore>,
    env: Env,
    engine: Engine,
}

async fn fixture(segments: &[EventBatch]) -> Fixture {
    let store = Arc::new(MemoryStore::new());
    let env = Env::simulated(1, Timestamp::from_nanos(1_000 * SECOND));
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

fn at(seconds: i64) -> Timestamp {
    Timestamp::from_nanos(seconds * SECOND)
}

#[tokio::test]
async fn an_sql_rule_fires_per_group_after_its_hold_and_resolves() {
    let fx = fixture(&[
        errors(at(1_000), 3, "checkout"),
        errors(at(1_000), 1, "search"),
    ])
    .await;
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
    rule.severity = "critical".into();
    let group = RuleGroup::open(
        fx.store.clone(),
        TenantId::DEFAULT,
        "default",
        vec![rule],
        fx.env.clone(),
    );

    let first = group.evaluate(&fx.engine, at(1_010)).await.unwrap();
    assert_eq!(first.rules, ["errors"]);
    assert_eq!(first.transitions.len(), 1, "only checkout has two errors");
    let pending = &first.transitions[0];
    assert_eq!(
        (pending.from, pending.to),
        (Status::Inactive, Status::Pending)
    );
    assert_eq!(pending.labels["service"], "checkout");
    assert_eq!(pending.labels["alertname"], "errors");
    assert_eq!(pending.labels["severity"], "critical");
    assert!((pending.value - 3.0).abs() < f64::EPSILON);

    let too_soon = group.evaluate(&fx.engine, at(1_020)).await.unwrap();
    assert!(too_soon.rules.is_empty(), "not due for another ten seconds");

    let held = group.evaluate(&fx.engine, at(1_040)).await.unwrap();
    assert!(held.transitions.is_empty(), "still pending");
    let fired = group.evaluate(&fx.engine, at(1_071)).await.unwrap();
    assert_eq!(fired.transitions[0].to, Status::Firing);
    assert_eq!(fired.transitions[0].number, 2);
    let instances = group.instances().await.unwrap();
    assert_eq!(instances.len(), 1);
    assert_eq!(instances[0].status, Status::Firing);

    // Five minutes later the errors fall out of the window.
    let resolved = group.evaluate(&fx.engine, at(1_400)).await.unwrap();
    assert_eq!(resolved.transitions[0].to, Status::Resolved);
    assert_eq!(
        group.outbox().await.unwrap().len(),
        3,
        "nothing delivered yet"
    );

    let sink = MemorySink::default();
    let notifier = MemoryNotifier::default();
    let delivery = Delivery {
        sink: &sink,
        notifier: &notifier,
        routes: &[],
        silences: &[],
        acknowledgments: &[],
    };
    assert_eq!(group.deliver(&delivery, at(1_401)).await.unwrap().events, 3);
    assert!(group.outbox().await.unwrap().is_empty());
    assert_eq!(
        group.deliver(&delivery, at(1_402)).await.unwrap().events,
        0,
        "delivered once"
    );
    assert!(notifier.sent().is_empty(), "the rule names no route");
    let batches = sink.batches();
    assert_eq!(batches.len(), 1);
    let rows = batches[0].rows();
    assert_eq!(rows.num_rows(), 3);
    assert_eq!(batches[0].dataset().as_str(), "prod");
    let names = rows
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!((0..3).all(|i| names.value(i) == "errors"));
    let overflow = rows
        .column_by_name("_overflow")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let last: serde_json::Value = serde_json::from_str(overflow.value(2)).unwrap();
    assert_eq!(last["attr.alert.status"], "resolved");
    assert_eq!(last["attr.alert.transition"], 3);
    assert_eq!(last["attr.service"], "checkout");
    let ids = rows.column_by_name("event_id").unwrap();
    assert!(!ids.is_null(0));
}

/// A recording rule writes each sample of its result as a gauge of the
/// recorded name with the sample's labels, and judges nothing.
#[tokio::test]
async fn a_recording_rule_writes_its_samples_as_gauges() {
    let fx = fixture(&[
        errors(at(1_000), 3, "checkout"),
        errors(at(1_000), 1, "search"),
    ])
    .await;
    let mut rule = AlertRule::new(
        "service:errors",
        Language::Sql,
        "SELECT attr.service AS service, count(*) AS n FROM logs \
         WHERE timestamp > now() - INTERVAL '5 minutes' GROUP BY attr.service",
    );
    rule.dataset = "prod".into();
    rule.record = Some("service:errors".into());
    let group = RuleGroup::open(
        fx.store.clone(),
        TenantId::DEFAULT,
        "default",
        vec![rule],
        fx.env.clone(),
    );
    let evaluated = group.evaluate(&fx.engine, at(1_010)).await.unwrap();
    assert!(evaluated.transitions.is_empty(), "nothing is judged");
    assert_eq!(evaluated.recorded.len(), 1);
    let rows = &evaluated.recorded[0].rows;
    assert_eq!(rows.len(), 2, "one gauge per service");
    let checkout = rows
        .iter()
        .find(|r| r.attributes.get("attr.service") == Some(&Scalar::Text("checkout".into())))
        .unwrap();
    assert_eq!(checkout.name, "service:errors");
    assert_eq!(checkout.value, Some(3.0));
    assert_ne!(rows[0].series_id, rows[1].series_id);
    let sink = MemorySink::default();
    assert_eq!(group.record(&sink, &evaluated.recorded).await.unwrap(), 2);
    let batches = sink.batches();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].kind(), tessera_types::Kind::Metric);
    assert_eq!(batches[0].dataset().as_str(), "prod");
    assert!(
        group
            .evaluate(&fx.engine, at(1_020))
            .await
            .unwrap()
            .rules
            .is_empty(),
        "not due again before its interval"
    );
}

#[tokio::test]
async fn a_promql_rule_fires_at_once_and_a_bad_rule_is_only_logged() {
    let fx = fixture(&[]).await;
    let always = AlertRule::new("always", Language::Promql, "vector(1)");
    let broken = AlertRule::new("broken", Language::Promql, "rate(up[5m)");
    let streams = AlertRule::new("streams", Language::Logql, "{service=\"x\"}");
    let group = RuleGroup::open(
        fx.store.clone(),
        TenantId::DEFAULT,
        "promql",
        vec![always, broken, streams],
        fx.env.clone(),
    );
    let evaluated = group.evaluate(&fx.engine, at(1_000)).await.unwrap();
    assert_eq!(evaluated.rules.len(), 3, "every rule was due");
    assert_eq!(evaluated.transitions.len(), 1, "only the good one fires");
    assert_eq!(evaluated.transitions[0].to, Status::Firing);
    assert_eq!(evaluated.transitions[0].rule, "always");
    let again = group.evaluate(&fx.engine, at(1_100)).await.unwrap();
    assert!(
        again.transitions.is_empty(),
        "still firing, no new transition"
    );
    assert_eq!(
        group.instances().await.unwrap()[0].since,
        at(1_000),
        "since firing began"
    );
}
