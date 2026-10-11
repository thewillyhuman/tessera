// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! A node as a value: starts on free ports, serves, and stops.

use std::net::SocketAddr;
use std::time::Duration;

use tessera_ingest::IngestConfig;
use tessera_node::{Config, Node};
use tessera_types::Env;

fn config(dir: &std::path::Path) -> Config {
    let free: SocketAddr = "127.0.0.1:0".parse().unwrap();
    Config {
        data_dir: dir.to_path_buf(),
        listen: free,
        pg_listen: free,
        ingest: IngestConfig {
            linger: Duration::from_millis(1),
            ..IngestConfig::default()
        },
    }
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
    let answer: serde_json::Value = tokio::task::spawn_blocking(move || {
        ureq::post(&url)
            .send_json(body)
            .unwrap()
            .body_mut()
            .read_json()
            .unwrap()
    })
    .await
    .unwrap();
    assert_eq!(answer["rows"][0][0], 42, "{answer}");
    node.stop().await;
}
