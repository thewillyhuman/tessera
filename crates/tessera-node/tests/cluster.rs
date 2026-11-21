// SPDX-License-Identifier: Apache-2.0

//! The cluster tests of M6 (G1): scale-up, scale-down and node loss with
//! zero manual steps (OP-4), on three nodes in one process sharing a
//! root pool. The nodes share a manual clock, so that heartbeats, leases
//! and quiet logs age when the test says so and not a second later.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::{eventually, post};
use tessera_cluster::Role;
use tessera_ingest::IngestConfig;
use tessera_manifest::registry;
use tessera_node::{Config, Node};
use tessera_object::LocalStore;
use tessera_types::{
    DatasetName, Env, ManualClock, NodeId, OsRandom, SharedRandom, TenantId, Timestamp, WorkKind,
};

/// How long a step of the cluster may take before the test gives up.
/// Each is waited for only until it happens, a second or two with the
/// intervals these tests set; the rest is for a machine that runs several
/// clusters at once, as CI's does: ten seconds were not always enough
/// there, and a release waits on these tests.
const PATIENCE: Duration = Duration::from_secs(30);
/// Enough partition slots that rendezvous hashing leaves no node of
/// three without one, with overwhelming probability.
const SLOTS: usize = 32;

/// A root pool directory and the clock every node reads.
struct Cluster {
    dir: tempfile::TempDir,
    clock: Arc<ManualClock>,
}

impl Cluster {
    fn new() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        Cluster {
            dir: tempfile::tempdir().unwrap(),
            clock: Arc::new(ManualClock::new(Timestamp::from_nanos(
                i64::try_from(now).unwrap(),
            ))),
        }
    }

    fn env(&self) -> Env {
        Env::new(
            NodeId::random(&mut OsRandom),
            Arc::clone(&self.clock) as _,
            SharedRandom::new(OsRandom),
        )
    }

    /// A node with `roles`, on free ports, with fast loops and an
    /// ingester that writes a file per batch.
    async fn start(&self, roles: &[Role]) -> Node {
        let free: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let config = Config {
            listen: free,
            pg_listen: free,
            roles: roles.iter().copied().collect(),
            heartbeat: Duration::from_millis(50),
            lease: Duration::from_secs(2),
            renewal: Duration::from_millis(50),
            background: Duration::from_millis(200),
            eval: Duration::from_millis(200),
            advise: Duration::from_millis(500),
            ingest: IngestConfig {
                linger: Duration::ZERO,
                flush_bytes: 1,
                ..IngestConfig::default()
            },
            ..Config::single_node(self.dir.path().to_path_buf())
        };
        Node::start(config, self.env()).await.unwrap()
    }

    /// The nodes that registered a writer for the test dataset.
    async fn writer_nodes(&self) -> Vec<NodeId> {
        let store = LocalStore::open(self.dir.path()).await.unwrap();
        registry::list(&store, &TenantId::DEFAULT, &dataset())
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.node)
            .collect()
    }
}

fn dataset() -> DatasetName {
    DatasetName::new("prod").unwrap()
}

/// Pushes `batches` batches of two lines each through `node`, Loki
/// style, so that a batch becomes a file of its own.
async fn push(node: &Node, batches: usize, tag: &str) {
    push_at(
        node.http_address(),
        node.env().clock.now().as_nanos(),
        batches,
        tag,
    )
    .await;
}

/// Pushes `batches` batches of two lines each, timestamped from `now`,
/// to the node at `address`.
async fn push_at(address: SocketAddr, now: i64, batches: usize, tag: &str) {
    let url = format!("http://{address}/loki/api/v1/push");
    for batch in 0..batches {
        let values: Vec<serde_json::Value> = (0..2)
            .map(|i| {
                let ts = now + i64::try_from(batch * 2 + i).unwrap();
                serde_json::json!([ts.to_string(), format!("{tag} {batch} {i}")])
            })
            .collect();
        let body = serde_json::json!({
            "streams": [{ "stream": { "app": "cluster", "via": tag }, "values": values }]
        });
        let (status, text) = post(
            url.clone(),
            vec![("x-tessera-dataset".into(), "prod".into())],
            body,
        )
        .await;
        assert_eq!(status, 204, "{text}");
    }
}

/// Runs `sql` on `node`.
async fn query(node: &Node, sql: &str) -> serde_json::Value {
    let url = format!("http://{}/api/v1/query", node.http_address());
    let body = serde_json::json!({ "dataset": "prod", "query": sql });
    let (status, text) = post(url, vec![], body).await;
    assert_eq!(status, 200, "{text}");
    serde_json::from_str(&text).unwrap()
}

/// How many log rows `node` sees.
async fn count(node: &Node) -> f64 {
    query(node, "SELECT count(*) FROM logs").await["rows"][0][0]
        .as_f64()
        .unwrap()
}

/// The EXPLAIN ANALYZE text of a full scan on `node`.
async fn explain(node: &Node) -> String {
    query(node, "EXPLAIN ANALYZE SELECT count(*) FROM logs").await["explain"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn partition_slots(node: &Node) -> std::collections::BTreeSet<tessera_commit::Slot> {
    node.owned().slots(WorkKind::Partition)
}

/// Whether `nodes` split every partition slot between them.
fn slots_are_shared(nodes: &[&Node]) -> bool {
    let sets: Vec<_> = nodes.iter().copied().map(partition_slots).collect();
    let total: usize = sets.iter().map(std::collections::BTreeSet::len).sum();
    let all_some = sets.iter().all(|s| !s.is_empty());
    let disjoint = sets
        .iter()
        .enumerate()
        .all(|(i, s)| sets.iter().skip(i + 1).all(|t| s.is_disjoint(t)));
    total == SLOTS && all_some && disjoint
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cluster_scales_up_survives_a_loss_and_scales_down() {
    let cluster = Cluster::new();

    // One node, as before M6.
    let a = cluster.start(&Role::ALL).await;
    for _ in 0..SLOTS.ilog2() {
        a.state()
            .catalog
            .double_slots(WorkKind::Partition)
            .await
            .unwrap();
    }
    push(&a, 5, "a").await;
    assert!((count(&a).await - 10.0).abs() < f64::EPSILON);
    eventually("a owns every slot", PATIENCE, || {
        partition_slots(&a).len() == SLOTS
    })
    .await;

    // Scale up: two more nodes, one without the ingest role. Nothing
    // but starting them (OP-4).
    let b = cluster.start(&Role::ALL).await;
    let c = cluster.start(&[Role::Query, Role::Compact]).await;
    for node in [&a, &b, &c] {
        eventually("every node sees three query nodes", PATIENCE, || {
            node.membership().live_ids(Role::Query).len() == 3
                && node.membership().live_ids(Role::Ingest).len() == 2
        })
        .await;
    }
    eventually("the slots are shared three ways", PATIENCE, || {
        slots_are_shared(&[&a, &b, &c])
    })
    .await;

    // Writes through the node without the ingest role are forwarded to
    // an ingester (IN-9) and visible everywhere at once.
    push(&c, 5, "c").await;
    for node in [&a, &b, &c] {
        assert!(
            (count(node).await - 20.0).abs() < f64::EPSILON,
            "every node counts 20"
        );
    }
    let writers = cluster.writer_nodes().await;
    assert!(!writers.contains(&c.env().node), "c wrote nothing itself");
    assert!(writers.contains(&a.env().node));

    // Leaf tasks are shared by file id (QE-15): some node reads files
    // on a peer.
    eventually("files are read on peers", PATIENCE, || {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut peers = false;
                for node in [&a, &b, &c] {
                    // QE-14: the count's partial stage ran on the peers
                    // that scanned the files.
                    let text = explain(node).await;
                    peers |=
                        text.contains("peers:") && text.contains("Stage aggregate: partial on");
                }
                peers
            })
        })
    })
    .await;

    // Node loss: a vanishes without a word. Its heartbeat, leases and
    // log age out, the others take its slots, recover its writer and
    // keep serving (section 14.4).
    let a_node = a.env().node;
    a.abandon();
    cluster.clock.advance(Duration::from_secs(40));
    for node in [&b, &c] {
        eventually("the survivors drop a", PATIENCE, || {
            node.membership().live_ids(Role::Query).len() == 2
        })
        .await;
    }
    eventually("the slots are shared two ways", PATIENCE, || {
        slots_are_shared(&[&b, &c])
    })
    .await;
    push(&b, 5, "b").await;
    push(&c, 5, "c2").await;
    for node in [&b, &c] {
        assert!(
            (count(node).await - 40.0).abs() < f64::EPSILON,
            "every survivor counts 40"
        );
    }
    eventually("a's writer is recovered and retired", PATIENCE, || {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async { !cluster.writer_nodes().await.contains(&a_node) })
        })
    })
    .await;
    assert!(
        (count(&b).await - 40.0).abs() < f64::EPSILON,
        "nothing lost, nothing doubled"
    );

    // Scale down: c leaves cleanly; b has everything.
    c.stop().await;
    eventually("b is alone", PATIENCE, || {
        b.membership().live_ids(Role::Query).len() == 1 && partition_slots(&b).len() == SLOTS
    })
    .await;
    push(&b, 5, "b2").await;
    assert!((count(&b).await - 50.0).abs() < f64::EPSILON);
    b.stop().await;
}

/// OP-7 and section 14.4: a rolling restart under load, each node
/// replaced in turn while writes keep arriving through another, loses
/// nothing and doubles nothing, and every node counts the same.
#[tokio::test(flavor = "multi_thread")]
async fn a_rolling_restart_under_load_loses_nothing() {
    let cluster = Cluster::new();
    let a = cluster.start(&Role::ALL).await;
    for _ in 0..SLOTS.ilog2() {
        a.state()
            .catalog
            .double_slots(WorkKind::Partition)
            .await
            .unwrap();
    }
    let b = cluster.start(&Role::ALL).await;
    let c = cluster.start(&Role::ALL).await;
    let mut nodes = vec![a, b, c];
    let settled = |nodes: &[Node]| {
        let views = nodes
            .iter()
            .all(|n| n.membership().live_ids(Role::Ingest).len() == 3);
        views && slots_are_shared(&nodes.iter().collect::<Vec<_>>())
    };
    eventually("three nodes share the slots", PATIENCE, || settled(&nodes)).await;
    let mut expected = 0.0;
    for round in 0..3i64 {
        // The oldest node leaves while writes keep arriving through the
        // next one; its replacement joins before the load ends.
        let leaving = nodes.remove(0);
        let through = nodes[0].http_address();
        let now = nodes[0].env().clock.now().as_nanos() + round * 1_000_000;
        let tag = format!("round{round}");
        let load = tokio::spawn(async move { push_at(through, now, 10, &tag).await });
        leaving.stop().await;
        nodes.push(cluster.start(&Role::ALL).await);
        load.await.unwrap();
        expected += 20.0;
        eventually("the replacement shares the slots", PATIENCE, || {
            settled(&nodes)
        })
        .await;
        for node in &nodes {
            assert!(
                (count(node).await - expected).abs() < f64::EPSILON,
                "round {round}: every node counts {expected}, nothing lost or doubled"
            );
        }
    }
    for node in nodes {
        node.stop().await;
    }
}
