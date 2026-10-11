// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! A node as a value: starts on free ports, serves, and stops.

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use tessera_catalog::Table;
use tessera_cluster::Role;
use tessera_ingest::IngestConfig;
use tessera_node::{Config, Node};
use tessera_types::{Env, TenantId, WorkKind};

fn config(dir: &std::path::Path) -> Config {
    let free: SocketAddr = "127.0.0.1:0".parse().unwrap();
    Config {
        listen: free,
        pg_listen: free,
        heartbeat: Duration::from_millis(50),
        lease: Duration::from_secs(2),
        renewal: Duration::from_millis(50),
        eval: Duration::from_millis(100),
        ingest: IngestConfig {
            linger: Duration::from_millis(1),
            ..IngestConfig::default()
        },
        ..Config::single_node(dir.to_path_buf())
    }
}

/// Polls `condition` for up to five seconds.
async fn eventually(what: &str, condition: impl FnMut() -> bool) {
    common::eventually(what, Duration::from_secs(5), condition).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_starts_serves_and_stops() {
    let dir = tempfile::tempdir().unwrap();
    let node = Node::start(config(dir.path()), Env::system())
        .await
        .unwrap();
    assert_ne!(node.http_address().port(), 0, "a free port was picked");
    assert_ne!(node.pg_address().port(), 0);
    let url = format!("http://{}/api/v1/query", node.http_address());
    let body = serde_json::json!({ "dataset": "prod", "query": "SELECT 42" });
    let (status, text) = common::post(url, vec![], body).await;
    assert_eq!(status, 200, "{text}");
    let answer: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(answer["rows"][0][0], 42, "{answer}");
    assert_eq!(
        node.membership().live_ids(Role::Query).len(),
        1,
        "alone, a node sees itself"
    );
    node.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn nodes_in_one_root_pool_find_each_other() {
    let dir = tempfile::tempdir().unwrap();
    let a = Node::start(config(dir.path()), Env::system())
        .await
        .unwrap();
    let b = Node::start(config(dir.path()), Env::system())
        .await
        .unwrap();
    eventually("both see two query nodes", || {
        a.membership().live_ids(Role::Query).len() == 2
            && b.membership().live_ids(Role::Query).len() == 2
    })
    .await;
    assert!(a.membership().is_live(b.env().node));
    a.stop().await;
    eventually("b sees a leave", || {
        b.membership().live_ids(Role::Query).len() == 1
    })
    .await;
    b.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn compact_nodes_share_the_slots_and_take_over_on_leave() {
    let dir = tempfile::tempdir().unwrap();
    let a = Node::start(config(dir.path()), Env::system())
        .await
        .unwrap();
    let catalog = &a.state().catalog;
    for _ in 0..3 {
        catalog.double_slots(WorkKind::Partition).await.unwrap();
    }
    eventually("a owns every partition slot", || {
        a.owned().slots(WorkKind::Partition).len() == 8
    })
    .await;
    let b = Node::start(config(dir.path()), Env::system())
        .await
        .unwrap();
    eventually("a and b split the slots", || {
        let (sa, sb) = (
            a.owned().slots(WorkKind::Partition),
            b.owned().slots(WorkKind::Partition),
        );
        !sa.is_empty() && !sb.is_empty() && sa.is_disjoint(&sb) && sa.len() + sb.len() == 8
    })
    .await;
    b.stop().await;
    eventually("a takes b's slots back", || {
        a.owned().slots(WorkKind::Partition).len() == 8
    })
    .await;
    a.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_eval_node_runs_the_rules_in_the_control_store() {
    let dir = tempfile::tempdir().unwrap();
    let node = Node::start(config(dir.path()), Env::system())
        .await
        .unwrap();
    let control = node.controls().open(&TenantId::DEFAULT);
    let rule = serde_json::json!({
        "name": "always", "language": "promql", "expr": "vector(1)", "every_seconds": 1
    });
    control
        .put(Table::AlertRules, "always", rule)
        .await
        .unwrap();
    eventually("the rule group slot is owned", || {
        node.owned().slots(WorkKind::RuleGroup).len() == 1
    })
    .await;
    let url = format!("http://{}/api/v1/query", node.http_address());
    let count = || async {
        let body =
            serde_json::json!({ "dataset": "default", "query": "SELECT count(*) FROM alerts" });
        let (status, text) = common::post(url.clone(), vec![], body).await;
        assert_eq!(status, 200, "{text}");
        let answer: serde_json::Value = serde_json::from_str(&text).unwrap();
        answer["rows"][0][0].as_f64().unwrap()
    };
    eventually("the firing transition is an alert event", || {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async { count().await >= 1.0 })
        })
    })
    .await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(
        (count().await - 1.0).abs() < f64::EPSILON,
        "one transition, one event"
    );
    node.stop().await;
}
