// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Delivery to routes: firing and resolved transitions reach their
//! routes once, silences and acknowledgments hold them back, a route
//! that cannot be reached keeps the transition pending, and a still
//! firing instance is notified again after the repeat interval.

use std::collections::BTreeMap;
use std::sync::Arc;

use tessera_eval::{
    Acknowledgment, AlertRule, Delivery, Language, MemoryNotifier, MemorySink, NotificationRoute,
    RouteKind, RuleGroup, Silence, Status,
};
use tessera_exec::{CpuPool, Engine, QueryLimits};
use tessera_format::ParquetFormat;
use tessera_object::{MemoryStore, Pools};
use tessera_types::{Env, PoolId, TenantId, Timestamp};

const SECOND: i64 = 1_000_000_000;

fn at(seconds: i64) -> Timestamp {
    Timestamp::from_nanos(seconds * SECOND)
}

fn route(name: &str, repeat_seconds: u64) -> NotificationRoute {
    NotificationRoute {
        name: name.into(),
        kind: RouteKind::Webhook,
        url: format!("http://{name}"),
        routing_key: None,
        repeat_seconds,
    }
}

struct Fixture {
    store: Arc<MemoryStore>,
    env: Env,
    engine: Engine,
}

fn fixture() -> Fixture {
    let store = Arc::new(MemoryStore::new());
    let env = Env::simulated(1, at(1_000));
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

/// A group with one rule that always fires and notifies `ops`, with
/// annotations.
fn always_group(fx: &Fixture, expr: &str, notify: &[&str]) -> RuleGroup {
    let mut rule = AlertRule::new("always", Language::Promql, expr);
    rule.notify = notify.iter().map(|s| (*s).to_owned()).collect();
    rule.annotations
        .insert("summary".into(), "it happened".into());
    RuleGroup::open(
        fx.store.clone(),
        TenantId::DEFAULT,
        "g",
        vec![rule],
        fx.env.clone(),
    )
}

#[tokio::test]
async fn firing_reaches_the_route_once_and_repeats_after_the_interval() {
    let fx = fixture();
    let group = always_group(&fx, "vector(1)", &["ops", "missing"]);
    let sink = MemorySink::default();
    let notifier = MemoryNotifier::default();
    let routes = [route("ops", 300)];
    let delivery = Delivery {
        sink: &sink,
        notifier: &notifier,
        routes: &routes,
        silences: &[],
        acknowledgments: &[],
    };
    group.evaluate(&fx.engine, at(1_000)).await.unwrap();
    let first = group.deliver(&delivery, at(1_001)).await.unwrap();
    assert_eq!((first.events, first.notified, first.failed), (1, 1, 0));
    let sent = notifier.sent();
    assert_eq!(sent.len(), 1, "the missing route is skipped, not fatal");
    assert_eq!(sent[0].0, "ops");
    assert_eq!(sent[0].1[0].transition.to, Status::Firing);
    assert_eq!(sent[0].1[0].annotations["summary"], "it happened");
    assert!(!sent[0].1[0].repeat);

    let soon = group.deliver(&delivery, at(1_100)).await.unwrap();
    assert_eq!(soon.repeated, 0);
    let later = group.deliver(&delivery, at(1_301)).await.unwrap();
    assert_eq!(later.repeated, 1);
    let sent = notifier.sent();
    assert_eq!(sent.len(), 2);
    assert!(sent[1].1[0].repeat);
    assert_eq!(
        sent[1].1[0].transition.number, 1,
        "the same transition, again"
    );
    let once_more = group.deliver(&delivery, at(1_302)).await.unwrap();
    assert_eq!(once_more.repeated, 0, "the interval starts over");
}

#[tokio::test]
async fn silences_and_acknowledgments_hold_notifications_back() {
    let fx = fixture();
    let group = always_group(&fx, "vector(1)", &["ops"]);
    let sink = MemorySink::default();
    let notifier = MemoryNotifier::default();
    let routes = [route("ops", 300)];
    let silences = [Silence {
        id: "s".into(),
        matchers: BTreeMap::from([("alertname".to_owned(), "always".to_owned())]),
        starts_at: at(0),
        ends_at: at(2_000),
        comment: String::new(),
    }];
    group.evaluate(&fx.engine, at(1_000)).await.unwrap();
    let delivery = Delivery {
        sink: &sink,
        notifier: &notifier,
        routes: &routes,
        silences: &silences,
        acknowledgments: &[],
    };
    let held_back = group.deliver(&delivery, at(1_001)).await.unwrap();
    assert_eq!(
        (held_back.events, held_back.notified, held_back.suppressed),
        (1, 0, 1)
    );
    assert!(notifier.sent().is_empty());
    assert!(
        group.outbox().await.unwrap().is_empty(),
        "suppressed is done"
    );

    let fingerprint = group.instances().await.unwrap()[0].fingerprint.clone();
    let acknowledgments = [Acknowledgment {
        fingerprint,
        until: at(5_000),
        by: "me".into(),
        comment: String::new(),
    }];
    // A fresh group with the same rule name and an acknowledgment.
    let again = RuleGroup::open(
        fx.store.clone(),
        TenantId::DEFAULT,
        "g2",
        group.rules().to_vec(),
        fx.env.clone(),
    );
    again.evaluate(&fx.engine, at(1_000)).await.unwrap();
    let delivery = Delivery {
        sink: &sink,
        notifier: &notifier,
        routes: &routes,
        silences: &[],
        acknowledgments: &acknowledgments,
    };
    let acknowledged = again.deliver(&delivery, at(1_001)).await.unwrap();
    assert_eq!(acknowledged.suppressed, 1);
    assert!(notifier.sent().is_empty());
}

#[tokio::test]
async fn an_unreachable_route_keeps_the_transition_pending() {
    let fx = fixture();
    let group = always_group(&fx, "vector(1)", &["ops"]);
    let sink = MemorySink::default();
    let notifier = MemoryNotifier::default();
    let routes = [route("ops", 300)];
    let delivery = Delivery {
        sink: &sink,
        notifier: &notifier,
        routes: &routes,
        silences: &[],
        acknowledgments: &[],
    };
    group.evaluate(&fx.engine, at(1_000)).await.unwrap();
    notifier.fail(true);
    let failed = group.deliver(&delivery, at(1_001)).await.unwrap();
    assert_eq!((failed.events, failed.failed, failed.notified), (1, 1, 0));
    assert_eq!(group.outbox().await.unwrap().len(), 1, "still pending");
    notifier.fail(false);
    let retried = group.deliver(&delivery, at(1_002)).await.unwrap();
    assert_eq!((retried.events, retried.notified), (1, 1));
    assert_eq!(
        sink.batches().len(),
        2,
        "the alert event was written twice with the same id, for deduplication"
    );
    assert!(group.outbox().await.unwrap().is_empty());
    assert_eq!(notifier.sent().len(), 1);
}
