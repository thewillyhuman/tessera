// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! A node as a value: starts on free ports, serves, and stops.

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use tessera_catalog::Table;
use tessera_cluster::Role;
use tessera_ingest::IngestConfig;
use tessera_manifest::DatasetLog;
use tessera_node::{Config, Node};
use tessera_object::LocalStore;
use tessera_types::{DatasetName, Env, Kind, TenantId, WorkKind};

fn config(dir: &std::path::Path) -> Config {
    let free: SocketAddr = "127.0.0.1:0".parse().unwrap();
    Config {
        listen: free,
        pg_listen: free,
        heartbeat: Duration::from_millis(50),
        lease: Duration::from_secs(2),
        renewal: Duration::from_millis(50),
        eval: Duration::from_millis(100),
        advise: Duration::from_millis(100),
        ingest: IngestConfig {
            linger: Duration::from_millis(1),
            flush_bytes: 1,
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

/// IN-1: a collector's gRPC exporter reaches the logs service on the
/// node's one port, and the rows come back over HTTP.
#[tokio::test(flavor = "multi_thread")]
async fn otlp_grpc_exports_on_the_same_port() {
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, any_value};
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};

    let dir = tempfile::tempdir().unwrap();
    let node = Node::start(config(dir.path()), Env::system())
        .await
        .unwrap();
    let mut client = LogsServiceClient::connect(format!("http://{}", node.http_address()))
        .await
        .unwrap();
    let mut request = tonic::Request::new(ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: 1_700_000_000_000_000_000,
                    body: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("over grpc".into())),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    });
    request
        .metadata_mut()
        .insert("x-tessera-dataset", "prod".parse().unwrap());
    client.export(request).await.unwrap();
    let url = format!("http://{}/api/v1/query", node.http_address());
    let body = serde_json::json!({ "dataset": "prod", "query": "SELECT body FROM logs" });
    let (status, text) = common::post(url, vec![], body).await;
    assert_eq!(status, 200, "{text}");
    let answer: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(answer["rows"][0][0], "over grpc", "{answer}");
    node.stop().await;
}

/// SE-3: with a certificate and key, the HTTP API serves TLS, the
/// node advertises itself as https, and a client with the CA reaches
/// it.
#[tokio::test(flavor = "multi_thread")]
async fn tls_listeners_serve_https() {
    let dir = tempfile::tempdir().unwrap();
    let signed =
        rcgen::generate_simple_self_signed(vec!["localhost".to_owned(), "127.0.0.1".to_owned()])
            .unwrap();
    let cert = dir.path().join("cert.pem");
    let key = dir.path().join("key.pem");
    std::fs::write(&cert, signed.cert.pem()).unwrap();
    std::fs::write(&key, signed.key_pair.serialize_pem()).unwrap();
    let mut config = config(dir.path());
    config.tls = Some(tessera_node::Tls {
        cert: cert.clone(),
        key,
        ca: Some(cert.clone()),
        client_ca: None,
    });
    let node = Node::start(config, Env::system()).await.unwrap();
    let ca = ureq::tls::Certificate::from_pem(&std::fs::read(&cert).unwrap()).unwrap();
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .tls_config(
            ureq::tls::TlsConfig::builder()
                .root_certs(ureq::tls::RootCerts::new_with_certs(&[ca]))
                .build(),
        )
        .build()
        .into();
    let url = format!("https://127.0.0.1:{}/healthz", node.http_address().port());
    let status =
        tokio::task::spawn_blocking(move || agent.get(&url).call().map(|r| r.status().as_u16()))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(status, 200);
    let plain = format!("http://127.0.0.1:{}/healthz", node.http_address().port());
    let answer =
        tokio::task::spawn_blocking(move || ureq::get(&plain).call().map(|r| r.status().as_u16()))
            .await
            .unwrap();
    assert!(answer.is_err(), "a plain request is not served: {answer:?}");
    assert!(
        node.membership()
            .live(Role::Query)
            .iter()
            .all(|n| n.http.starts_with("https://")),
        "the node advertises https"
    );
    node.stop().await;
}

/// SE-1: with a client CA, a client certificate it signed acts for
/// the tenant its subject's organization names, with the roles its
/// organizational unit names; a client presenting none is refused
/// where anonymous access is off.
#[tokio::test(flavor = "multi_thread")]
async fn client_certificates_authenticate_over_mutual_tls() {
    use rcgen::{BasicConstraints, CertificateParams, DistinguishedName, DnType, IsCa, KeyPair};
    use ureq::tls::{Certificate, ClientCert, PrivateKey, RootCerts, TlsConfig};

    let dir = tempfile::tempdir().unwrap();
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca = ca_params.self_signed(&ca_key).unwrap();
    let server_key = KeyPair::generate().unwrap();
    let server = CertificateParams::new(vec!["localhost".to_owned(), "127.0.0.1".to_owned()])
        .unwrap()
        .signed_by(&server_key, &ca, &ca_key)
        .unwrap();
    let client_key = KeyPair::generate().unwrap();
    let mut client_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    let mut subject = DistinguishedName::new();
    subject.push(DnType::OrganizationName, "default");
    subject.push(DnType::OrganizationalUnitName, "reader");
    subject.push(DnType::CommonName, "grafana");
    client_params.distinguished_name = subject;
    let client = client_params.signed_by(&client_key, &ca, &ca_key).unwrap();
    let ca_path = dir.path().join("ca.pem");
    let cert = dir.path().join("cert.pem");
    let key = dir.path().join("key.pem");
    std::fs::write(&ca_path, ca.pem()).unwrap();
    std::fs::write(&cert, server.pem()).unwrap();
    std::fs::write(&key, server_key.serialize_pem()).unwrap();
    let mut config = config(dir.path());
    config.allow_anonymous = false;
    config.tls = Some(tessera_node::Tls {
        cert,
        key,
        ca: Some(ca_path.clone()),
        client_ca: Some(ca_path),
    });
    let node = Node::start(config, Env::system()).await.unwrap();
    let roots = RootCerts::new_with_certs(&[Certificate::from_pem(ca.pem().as_bytes()).unwrap()]);
    let presented = ClientCert::new_with_certs(
        &[Certificate::from_pem(client.pem().as_bytes()).unwrap()],
        PrivateKey::from_pem(client_key.serialize_pem().as_bytes()).unwrap(),
    );
    let agent = |client_cert: Option<ClientCert>| -> ureq::Agent {
        ureq::Agent::config_builder()
            .http_status_as_error(false)
            .tls_config(
                TlsConfig::builder()
                    .root_certs(roots.clone())
                    .client_cert(client_cert)
                    .build(),
            )
            .build()
            .into()
    };
    let url = format!(
        "https://127.0.0.1:{}/api/v1/query",
        node.http_address().port()
    );
    let ask = |agent: ureq::Agent| {
        let url = url.clone();
        tokio::task::spawn_blocking(move || {
            let query =
                serde_json::json!({ "dataset": "prod", "query": "SELECT count(*) FROM logs" });
            let mut response = agent.post(&url).send_json(query).unwrap();
            (
                response.status().as_u16(),
                response.body_mut().read_to_string().unwrap(),
            )
        })
    };
    let (status, body) = ask(agent(Some(presented))).await.unwrap();
    assert_eq!(status, 200, "{body}");
    let (status, body) = ask(agent(None)).await.unwrap();
    assert_eq!(status, 401, "without a certificate or a key: {body}");
    node.stop().await;
}

/// SE-2: with a keyring, a tenant whose setting names a key has its
/// writer-log blocks sealed and its files encrypted at rest, any index
/// sealed, and reads back as before.
#[tokio::test(flavor = "multi_thread")]
async fn a_keyed_tenants_data_is_sealed_at_rest() {
    use tessera_types::{Keyring, TenantKey};

    let dir = tempfile::tempdir().unwrap();
    let mut config = config(dir.path());
    config.keyring = Keyring::default().with("k2025", TenantKey::from_bytes([5; 32]));
    let node = Node::start(config, Env::system()).await.unwrap();
    let base = format!("http://{}", node.http_address());
    let (status, text) = common::post(
        format!("{base}/api/v1/tenants"),
        vec![],
        serde_json::json!({ "name": "acme", "settings": { "encryption_key": "k2025" } }),
    )
    .await;
    assert_eq!(status, 201, "{text}");
    let created: serde_json::Value = serde_json::from_str(&text).unwrap();
    let id = created["id"].as_str().unwrap().to_owned();
    let as_acme = vec![
        ("x-scope-orgid".to_owned(), "acme".to_owned()),
        ("x-tessera-dataset".to_owned(), "prod".to_owned()),
    ];
    let now = node.env().clock.now().as_nanos();
    let values: Vec<serde_json::Value> = (0..20)
        .map(|i| serde_json::json!([(now + i).to_string(), format!("confidential line {i}")]))
        .collect();
    let body = serde_json::json!({
        "streams": [{ "stream": { "app": "web" }, "values": values }]
    });
    let (status, text) =
        common::post(format!("{base}/loki/api/v1/push"), as_acme.clone(), body).await;
    assert_eq!(status, 204, "{text}");
    let query = |sql: &str| {
        let url = format!("{base}/api/v1/query");
        let headers = as_acme.clone();
        let body = serde_json::json!({ "dataset": "prod", "query": sql });
        async move {
            let (status, text) = common::post(url, headers, body).await;
            assert_eq!(status, 200, "{text}");
            serde_json::from_str::<serde_json::Value>(&text).unwrap()
        }
    };
    let answer = query("SELECT count(*) FROM logs").await;
    assert_eq!(
        answer["rows"][0][0], 20.0,
        "read from the sealed tail: {answer}"
    );
    eventually("the rows reach a file", || {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let explain = query("EXPLAIN ANALYZE SELECT count(*) FROM logs").await;
                !explain["explain"].as_str().unwrap().contains(", 0 read")
            })
        })
    })
    .await;
    let answer = query("SELECT count(*) FROM logs WHERE body LIKE '%line 7%'").await;
    assert_eq!(
        answer["rows"][0][0], 1.0,
        "read from encrypted files: {answer}"
    );

    let mut sealed_entries = 0;
    let mut encrypted_files = 0;
    for path in walkdir(dir.path()) {
        let text = path.to_string_lossy().into_owned();
        let bytes = std::fs::read(&path).unwrap();
        let has = |needle: &str| bytes.windows(needle.len()).any(|w| w == needle.as_bytes());
        let extension = path.extension().and_then(|e| e.to_str());
        if text.contains("/_wal/")
            && has("\"kind\":\"segment\"")
            && has(&format!("\"tenant\":\"{id}\""))
        {
            assert!(
                has("\"key\":\"k2025\""),
                "{text}: a block of acme names its key"
            );
            assert!(
                !has("confidential line"),
                "{text}: a block of acme is sealed"
            );
            sealed_entries += 1;
        }
        if text.contains(&id) && extension == Some("parquet") {
            assert!(bytes.ends_with(b"PARE"), "{text}: an encrypted footer");
            encrypted_files += 1;
        }
        if text.contains(&id) && extension == Some("idx") {
            assert!(bytes.starts_with(b"TSX1"), "{text}: a sealed sidecar");
        }
    }
    assert!(sealed_entries > 0, "acme's segments were found");
    assert!(encrypted_files > 0, "acme's files were found");
    node.stop().await;
}

/// Every file under `root`, recursively.
fn walkdir(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else {
                files.push(path);
            }
        }
    }
    files
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_refuses_to_start_beside_one_that_cannot_read_its_format() {
    use std::collections::BTreeSet;

    use tessera_cluster::{NodeRecord, registry};
    use tessera_object::{ObjectStore, layout};
    use tessera_types::{NodeId, Timestamp};

    let dir = tempfile::tempdir().unwrap();
    let store = std::sync::Arc::new(LocalStore::open(dir.path()).await.unwrap());
    let stale = NodeRecord {
        format: registry::FORMAT,
        node: NodeId::from_u128(42),
        roles: BTreeSet::from([Role::Query]),
        http: "http://old.invalid".into(),
        started_at: Timestamp::from_system_time(std::time::SystemTime::now()),
        heartbeat_at: Timestamp::from_system_time(std::time::SystemTime::now()),
        reads_through: 0,
    };
    store
        .put(
            &layout::node(stale.node),
            serde_json::to_vec(&stale).unwrap().into(),
        )
        .await
        .unwrap();
    let error = Node::start(config(dir.path()), Env::system())
        .await
        .err()
        .expect("a node that cannot read this build's format is live");
    assert!(error.to_string().contains("cannot read"), "{error}");
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
    // Thirty-two slots: rendezvous over few slots leaves a node with none
    // by chance, which is not what the test is about.
    let catalog = &a.state().catalog;
    for _ in 0..5 {
        catalog.double_slots(WorkKind::Partition).await.unwrap();
    }
    eventually("a owns every partition slot", || {
        a.owned().slots(WorkKind::Partition).len() == 32
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
        !sa.is_empty() && !sb.is_empty() && sa.is_disjoint(&sb) && sa.len() + sb.len() == 32
    })
    .await;
    b.stop().await;
    eventually("a takes b's slots back", || {
        a.owned().slots(WorkKind::Partition).len() == 32
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

/// Whether some file of the `prod` logs was written under `epoch` with
/// `column` leading its sort key and present as a column.
async fn rewritten_in(
    store: &std::sync::Arc<dyn tessera_object::ObjectStore>,
    env: &Env,
    epoch: u64,
    column: &str,
) -> bool {
    let partitions = tessera_manifest::list_partitions(
        &**store,
        &TenantId::DEFAULT,
        &DatasetName::new("prod").unwrap(),
        Kind::Log,
    )
    .await
    .unwrap();
    for partition in partitions {
        let manifest = tessera_manifest::Manifest::open(store.clone(), partition, env.clone());
        let snapshot = manifest.snapshot().await.unwrap();
        let found = snapshot.state().files().any(|f| {
            f.layout_epoch == epoch
                && f.sort_key.first().map(String::as_str) == Some(column)
                && f.columns.iter().any(|c| c.name == column)
        });
        if found {
            return true;
        }
    }
    false
}

#[tokio::test(flavor = "multi_thread")]
async fn a_filtered_attribute_becomes_the_sort_key_without_intervention() {
    let dir = tempfile::tempdir().unwrap();
    let node = Node::start(config(dir.path()), Env::system())
        .await
        .unwrap();
    let push = format!("http://{}/loki/api/v1/push", node.http_address());
    let now = node.env().clock.now().as_nanos();
    for host in ["h1", "h2", "h3", "h4"] {
        let values: Vec<serde_json::Value> = (0..50)
            .map(|i| serde_json::json!([(now + i).to_string(), format!("line {i} on {host}")]))
            .collect();
        let body = serde_json::json!({
            "streams": [{ "stream": { "app": "web", "host": host }, "values": values }]
        });
        let (status, text) = common::post(
            push.clone(),
            vec![("x-tessera-dataset".into(), "prod".into())],
            body,
        )
        .await;
        assert_eq!(status, 204, "{text}");
    }
    let url = format!("http://{}/api/v1/query", node.http_address());
    let query = |sql: &str| {
        let url = url.clone();
        let body = serde_json::json!({ "dataset": "prod", "query": sql });
        async move {
            let (status, text) = common::post(url, vec![], body).await;
            assert_eq!(status, 200, "{text}");
            serde_json::from_str::<serde_json::Value>(&text).unwrap()
        }
    };
    eventually("the rows reach a file", || {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let explain = query("EXPLAIN ANALYZE SELECT count(*) FROM logs").await;
                !explain["explain"].as_str().unwrap().contains(", 0 read")
            })
        })
    })
    .await;
    // A workload that always filters on one attribute.
    for _ in 0..30 {
        let answer = query("SELECT count(*) FROM logs WHERE attr.host = 'h2'").await;
        assert_eq!(answer["rows"][0][0], 50.0, "{answer}");
    }
    let store: std::sync::Arc<dyn tessera_object::ObjectStore> =
        std::sync::Arc::new(LocalStore::open(dir.path()).await.unwrap());
    let log = DatasetLog::open(
        store,
        &TenantId::DEFAULT,
        &DatasetName::new("prod").unwrap(),
        node.env().clone(),
    );
    common::eventually(
        "the advisor adopts the attribute as the sort key",
        Duration::from_secs(15),
        || {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    let layout = log.layout(Kind::Log).await.unwrap();
                    layout.epoch >= 1
                        && layout.sort_key.first().map(String::as_str) == Some("attr.host")
                })
            })
        },
    )
    .await;
    let advice = log.advice(Kind::Log).await.unwrap().unwrap();
    assert!(advice.streak >= 2);
    assert_eq!(advice.proposed.sort_key[0], "attr.host");
    assert!(advice.alternatives.iter().any(|a| a.current));
    assert!(!advice.drivers.is_empty());
    assert!(
        advice.drivers[0].contains("attr.host ="),
        "{:?}",
        advice.drivers
    );

    // Compaction re-clusters the files into the adopted layout (AD-10).
    let epoch = log.layout(Kind::Log).await.unwrap().epoch;
    let store: std::sync::Arc<dyn tessera_object::ObjectStore> =
        std::sync::Arc::new(LocalStore::open(dir.path()).await.unwrap());
    common::eventually(
        "a file is rewritten in the new layout",
        Duration::from_secs(15),
        || {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(rewritten_in(
                    &store,
                    node.env(),
                    epoch,
                    "attr.host",
                ))
            })
        },
    )
    .await;
    node.stop().await;
}
