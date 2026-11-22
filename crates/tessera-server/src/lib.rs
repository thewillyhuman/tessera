// SPDX-License-Identifier: Apache-2.0

//! Tessera's network front ends (spec section 16): the HTTP
//! administration API, OTLP ingest over HTTP, SQL over HTTP and over
//! the `PostgreSQL` wire protocol, the Loki API with `LogQL`, the
//! Tempo API with `TraceQL` and the Prometheus API with remote write.
//! The other query languages and compatibility APIs arrive with their
//! milestones.
//!
//! This crate is transport only: it decodes requests, calls the catalog,
//! the ingest service and the query engine, and encodes responses. No
//! rule about tenants or data lives here.

pub mod admin;
pub mod audit;
pub mod auth;
mod bodies;
mod budgets;
pub use budgets::share_budgets;
mod control;
pub mod encode;
mod error;
pub mod events;
pub mod grpc;
pub mod internal;
pub mod loki;
pub mod metering;
pub mod metrics;
pub mod mtls;
pub mod oidc;
pub mod otlp;
mod params;
pub mod pg;
pub mod prometheus;
pub mod query;
pub mod reconcile;
mod redact;
pub mod rollups;
pub mod rules;
pub mod system;
pub mod tempo;
mod volume;
pub mod workload;

use std::sync::Arc;

use axum::Router;
use axum::extract::DefaultBodyLimit;
use tessera_catalog::{Catalog, ControlStores};
use tessera_cluster::{Admission, Ledger, Load, Membership, QueryPermit, Role};
use tessera_exec::Engine;
use tessera_format::FileFormat;
use tessera_ingest::IngestHandle;
use tessera_object::Pools;
use tessera_types::{Env, EventBatch, TenantId};
use tower_http::decompression::RequestDecompressionLayer;

pub use auth::Auth;
pub use error::ApiError;
pub use internal::HttpRemote;
pub use metering::IngestMeter;
pub use params::AckMode;
pub use rollups::RollupEntries;
pub use system::SystemTableRows;
pub use volume::Volumes;

/// What a node in a cluster adds: who else is there, how to reach
/// them, and the limits every tenant runs under.
pub struct Cluster {
    /// The membership view.
    pub membership: Arc<Membership>,
    /// The transport to peers.
    pub remote: HttpRemote,
    /// Admission control.
    pub admission: Arc<Admission>,
    /// The most ingesters a dataset's writes go to (IN-9); a dataset
    /// gets one per sixteen megabytes a second of recent volume.
    pub preferred_ingesters: usize,
    /// The recent volume of each dataset, which sizes its preferred
    /// set (IN-9).
    pub volumes: volume::Volumes,
}

/// What the handlers share.
#[derive(Clone)]
pub struct AppState {
    /// The cluster catalog.
    pub catalog: Arc<Catalog>,
    /// The ingest service of this node.
    pub ingest: IngestHandle,
    /// The storage pools.
    pub pools: Arc<Pools>,
    /// The data file format.
    pub format: Arc<dyn FileFormat>,
    /// The query engine.
    pub engine: Arc<Engine>,
    /// The node's environment.
    pub env: Env,
    /// The tenants' control stores (section 12.3).
    pub controls: Arc<ControlStores>,
    /// The cluster, when the node is in one; alone, nothing is forwarded
    /// and nothing is limited.
    pub cluster: Option<Arc<Cluster>>,
    /// What the tenants and teams have used of their budgets today
    /// (section 13.4).
    pub budgets: Arc<Ledger>,
    /// How requests are authenticated (CT-5, SE-1).
    pub auth: Arc<auth::Auth>,
    /// The node's load signals (OP-5).
    pub load: Arc<Load>,
}

impl AppState {
    /// Writes `batch`: with the tenant's redaction rules applied (SE-4),
    /// as the tenant's and its teams' budgets admit it (section 13.4),
    /// within the tenant's bytes in flight, then here or
    /// through one of the dataset's preferred ingesters (IN-9), chosen by
    /// rendezvous hashing over the live ones; a preferred ingester that
    /// cannot be reached means writing here.
    ///
    /// # Errors
    ///
    /// If a budget refuses the write, the tenant is over its limit of
    /// bytes in flight, or the write fails here.
    pub async fn write(&self, batch: EventBatch) -> Result<(), ApiError> {
        self.write_with(batch, AckMode::Durable).await
    }

    /// [`Self::write`], acknowledged as `ack` says (IN-10). A batch
    /// forwarded to a preferred ingester is acknowledged durable by it
    /// whatever `ack` says.
    ///
    /// # Errors
    ///
    /// As [`Self::write`].
    pub async fn write_with(&self, batch: EventBatch, ack: AckMode) -> Result<(), ApiError> {
        let _inflight = self.load.ingest_started();
        let batch = if batch.tenant() == &TenantId::SYSTEM {
            batch
        } else {
            let batch = bodies::apply(self, redact::apply(self, batch)?)?;
            let Some(admitted) = budgets::admit_ingest(self, batch)? else {
                return Ok(());
            };
            metering::record(self, &metering::ingest_usage(self, &admitted));
            admitted
        };
        let Some(cluster) = &self.cluster else {
            self.ingest.write(batch).await?;
            return Ok(());
        };
        let _budget = cluster
            .admission
            .admit_ingest(batch.tenant(), batch.rows().get_array_memory_size())?;
        let key = format!("{}/{}", batch.tenant(), batch.dataset());
        let volume = cluster.volumes.observe(
            batch.tenant(),
            batch.dataset(),
            batch.rows().get_array_memory_size(),
            self.env.clock.now(),
        );
        let preferred = cluster.membership.preferred(
            Role::Ingest,
            key.as_bytes(),
            volume.preferred(cluster.preferred_ingesters),
        );
        let mine = preferred.is_empty() || preferred.iter().any(|n| n.node == self.env.node);
        if !mine {
            match cluster.remote.forward(&preferred[0].http, &batch).await {
                Ok(()) => return Ok(()),
                Err(error) => {
                    tracing::warn!(peer = %preferred[0].node, error = %error, "forwarding failed; writing here");
                }
            }
        }
        match ack {
            AckMode::Durable => {
                let started = std::time::Instant::now();
                self.ingest.write(batch).await?;
                self.load.write_acked(started.elapsed());
            }
            AckMode::Buffered => self.ingest.write_buffered(batch).await?,
        }
        Ok(())
    }

    /// A turn to run a query for `tenant` (QE-18): held until dropped.
    /// Alone, there is no limit and no permit. A tenant whose query
    /// budget is used up and says to reject gets no turn (section 13.4).
    ///
    /// # Errors
    ///
    /// If the tenant's query budget refuses the query, or its queries
    /// are all busy for longer than the queue allows.
    pub async fn admit_query(
        &self,
        tenant: &TenantId,
        team: Option<&str>,
    ) -> Result<Option<QueryPermit>, ApiError> {
        budgets::admit_query(self, tenant, team)?;
        match &self.cluster {
            Some(cluster) => {
                let waited = std::time::Instant::now();
                let permit = cluster.admission.admit_query(tenant).await;
                self.load.query_waited(waited.elapsed());
                Ok(Some(permit?))
            }
            None => Ok(None),
        }
    }
}

/// The most bytes a request body may hold, once decompressed: room
/// for the batches collectors send, which run to megabytes, and a
/// bound on what one request can make a node buffer.
pub const MAX_REQUEST_BYTES: usize = 32 << 20;

/// Every HTTP route of a node.
pub fn router(state: AppState) -> Router {
    admin::router(state.clone())
        .merge(internal::router(state.clone()))
        .merge(otlp::router(state.clone()))
        .merge(grpc::router(state.clone()))
        .merge(events::router(state.clone()))
        .merge(loki::router(state.clone()))
        .merge(tempo::router(state.clone()))
        .merge(prometheus::router(state.clone()))
        .merge(rules::router(state.clone()))
        .merge(metrics::router(state.clone()))
        .merge(query::router(state))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
        // A gzip body is undone here, before the limit counts it and
        // before any handler reads it; an encoding a handler decodes
        // itself, as remote write's snappy, goes through as it came.
        .layer(RequestDecompressionLayer::new().pass_through_unaccepted(true))
}

impl AppState {
    /// A single-node state over an in-memory store with an ingester that
    /// flushes almost at once: for tests, demos and differential harnesses.
    ///
    /// # Panics
    ///
    /// If the in-memory catalog cannot be opened, which cannot happen
    /// without a store failure.
    pub async fn in_memory() -> AppState {
        Self::in_memory_on(tessera_object::MemoryStore::new()).await
    }

    /// A state over `store`, so that several share one root pool.
    ///
    /// # Panics
    ///
    /// If the catalog cannot be opened in memory, which does not happen.
    pub async fn in_memory_on(store: tessera_object::MemoryStore) -> AppState {
        use std::time::Duration;

        use tessera_catalog::PoolConfig;
        use tessera_exec::{CpuPool, QueryLimits};
        use tessera_format::ParquetFormat;
        use tessera_ingest::{IngestConfig, Ingester};
        use tessera_object::PoolState;
        use tessera_types::PoolId;

        let env = Env::system();
        let pool = PoolId::new("root").unwrap_or_else(|_| unreachable!("a valid id"));
        let root = PoolConfig {
            id: pool.clone(),
            url: "memory://test".into(),
            weight: 1,
            state: PoolState::Active,
        };
        let catalog = Arc::new(
            Catalog::open(Arc::new(store.clone()), root, env.clone())
                .await
                .expect("an in-memory catalog opens"),
        );
        let config = IngestConfig {
            linger: Duration::from_millis(1),
            ..IngestConfig::default()
        };
        let ingester = Ingester::start(
            Arc::new(store.clone()),
            env.clone(),
            config,
            pool.clone(),
            Arc::new(ParquetFormat::default()),
        );
        let (ingest, _task) = tessera_ingest::spawn(ingester, Duration::from_millis(1));
        let pools = Arc::new(Pools::new(pool, Arc::new(store.clone())));
        let controls = Arc::new(ControlStores::new(Arc::new(store.clone()), env.clone()));
        let engine = Engine::new(
            Arc::clone(&pools),
            Arc::new(ParquetFormat::default()),
            env.clone(),
            CpuPool::new(2),
            QueryLimits::default(),
        )
        .with_system_tables(Arc::new(SystemTableRows::new(
            Arc::clone(&controls),
            Arc::new(store.clone()),
            env.clone(),
            catalog.clone(),
        )))
        .with_rollups(Arc::new(rollups::RollupEntries::new(
            Arc::clone(&controls),
            Arc::new(store),
            env.clone(),
        )));
        AppState {
            catalog,
            ingest,
            pools,
            format: Arc::new(ParquetFormat::default()),
            engine: Arc::new(engine),
            controls,
            env,
            cluster: None,
            budgets: Arc::new(Ledger::new()),
            auth: Arc::new(auth::Auth::new(true)),
            load: Load::new(),
        }
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use super::AppState;

    /// A single-node state over an in-memory store with a fast ingester.
    pub async fn state() -> AppState {
        AppState::in_memory().await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::time::Duration;

    use axum::body::Body;
    use axum::http::{Request, StatusCode, header};
    use http_body_util::BodyExt;
    use tessera_cluster::{Limits, NodeRecord, registry};
    use tessera_types::{DatasetName, LogRow, Timestamp};
    use tower::ServiceExt;

    use super::*;

    fn record(state: &AppState, roles: &[Role], http: &str) -> NodeRecord {
        let now = state.env.clock.now();
        NodeRecord {
            format: registry::FORMAT,
            reads_through: registry::READS_THROUGH,
            woken: Vec::new(),
            node: state.env.node,
            roles: roles.iter().copied().collect::<BTreeSet<_>>(),
            http: http.to_owned(),
            started_at: now,
            heartbeat_at: now,
        }
    }

    /// `state` as a member of a cluster that sees `others` as live.
    fn clustered(
        mut state: AppState,
        roles: &[Role],
        others: Vec<NodeRecord>,
        limits: Limits,
    ) -> AppState {
        let own = record(&state, roles, "http://self.invalid");
        let membership = Membership::new(state.env.clone(), own.clone());
        let mut records = others;
        records.push(own);
        membership.replace(records);
        state.cluster = Some(Arc::new(Cluster {
            membership: Arc::new(membership),
            remote: HttpRemote::new(),
            admission: Arc::new(Admission::new(limits)),
            preferred_ingesters: 1,
            volumes: volume::Volumes::default(),
        }));
        state
    }

    /// Sends `body` to `/api/v1/events` of a whole node's router, as
    /// `encoding` when one is named.
    async fn send_events(encoding: Option<&str>, body: Vec<u8>) -> (StatusCode, String) {
        let app = router(testing::state().await);
        let mut request = Request::builder()
            .method("POST")
            .uri("/api/v1/events")
            .header("x-tessera-dataset", "prod");
        if let Some(encoding) = encoding {
            request = request.header(header::CONTENT_ENCODING, encoding);
        }
        let response = app
            .oneshot(request.body(Body::from(body)).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8_lossy(&bytes).into_owned())
    }

    /// `count` events as JSON lines, each padded to about `width` bytes.
    fn event_lines(count: usize, width: usize) -> Vec<u8> {
        use std::fmt::Write;

        let padding = "x".repeat(width);
        let mut lines = String::new();
        for i in 0..count {
            let timestamp = 1_700_000_000_000_000_000 + i64::try_from(i).unwrap();
            let _ = writeln!(
                lines,
                "{{\"timestamp\":{timestamp},\"name\":\"e\",\"pad\":\"{padding}\"}}"
            );
        }
        lines.into_bytes()
    }

    fn gzip(body: &[u8]) -> Vec<u8> {
        use std::io::Write;

        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(body).unwrap();
        encoder.finish().unwrap()
    }

    /// IN-1: an OpenTelemetry Collector compresses what it exports
    /// with gzip unless told otherwise.
    #[tokio::test]
    async fn a_gzip_body_is_read_as_what_it_holds() {
        let (status, text) = send_events(Some("gzip"), gzip(&event_lines(3, 10))).await;
        assert_eq!(status, StatusCode::OK, "{text}");
    }

    /// Remote write names snappy as its encoding and its handler
    /// decodes it: an encoding the router does not undo is the
    /// handler's business, not a refusal.
    #[tokio::test]
    async fn an_encoding_left_to_the_handler_reaches_it() {
        let app = router(testing::state().await);
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/write")
            .header(header::CONTENT_ENCODING, "snappy")
            .body(Body::from(
                snap::raw::Encoder::new().compress_vec(b"").unwrap(),
            ))
            .unwrap();
        let status = app.oneshot(request).await.unwrap().status();
        assert_ne!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    /// A collector's batch runs to megabytes; the limit is on what a
    /// body holds once decompressed, so a small body cannot stand for
    /// a huge one.
    #[tokio::test]
    async fn a_body_is_bounded_by_what_it_holds() {
        let megabyte = 1 << 20;
        let (status, text) = send_events(None, event_lines(4, megabyte)).await;
        assert_eq!(status, StatusCode::OK, "four megabytes: {text}");

        let past = event_lines(MAX_REQUEST_BYTES / megabyte + 1, megabyte);
        let (status, _) = send_events(None, past.clone()).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        let small = gzip(&past);
        assert!(small.len() < megabyte, "the padding compresses away");
        let (status, _) = send_events(Some("gzip"), small).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    }

    fn logs() -> EventBatch {
        let rows: Vec<LogRow> = (0..3u8)
            .map(|i| LogRow {
                timestamp: Timestamp::from_nanos(1_700_000_000_000_000_000 + i64::from(i)),
                event_id: [i; 16],
                body: Some(format!("line {i}")),
                ..LogRow::default()
            })
            .collect();
        EventBatch::from_logs(TenantId::DEFAULT, DatasetName::new("prod").unwrap(), &rows).unwrap()
    }

    async fn count_logs(state: &AppState) -> f64 {
        let query = serde_json::json!({ "dataset": "prod", "query": "SELECT count(*) FROM logs" });
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/query")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(query.to_string()))
            .unwrap();
        let response = router(state.clone()).oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        json["rows"][0][0].as_f64().unwrap()
    }

    /// Serves `state` on a local port; the base URL to reach it.
    async fn serve(state: AppState) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, router(state)).await.unwrap();
        });
        format!("http://{address}")
    }

    #[tokio::test]
    async fn alone_a_node_writes_locally_and_limits_nothing() {
        let state = AppState::in_memory().await;
        state.write(logs()).await.unwrap();
        assert!(
            state
                .admit_query(&TenantId::DEFAULT, None)
                .await
                .unwrap()
                .is_none()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!((count_logs(&state).await - 3.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn queries_over_the_tenant_limit_are_told_to_retry() {
        let limits = Limits {
            concurrent_queries: 1,
            queue: Duration::from_millis(10),
            ..Limits::default()
        };
        let state = clustered(AppState::in_memory().await, &[Role::Query], vec![], limits);
        let _held = state.admit_query(&TenantId::DEFAULT, None).await.unwrap();
        let query = serde_json::json!({ "dataset": "prod", "query": "SELECT 1" });
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/query")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(query.to_string()))
            .unwrap();
        let response = router(state).oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers()[header::RETRY_AFTER], "1");
    }

    #[tokio::test]
    async fn writes_beyond_the_budget_are_rejected_before_memory() {
        let limits = Limits {
            ingest_bytes: 1,
            ..Limits::default()
        };
        let state = clustered(AppState::in_memory().await, &[Role::Ingest], vec![], limits);
        let error = state.write(logs()).await.unwrap_err();
        assert_eq!(error.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn writes_go_to_the_preferred_ingester() {
        let ingester = AppState::in_memory().await;
        let http = serve(ingester.clone()).await;
        let peer = record(&ingester, &[Role::Ingest], &http);
        let querier = clustered(
            AppState::in_memory().await,
            &[Role::Query],
            vec![peer],
            Limits::default(),
        );
        querier.write(logs()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            (count_logs(&ingester).await - 3.0).abs() < f64::EPSILON,
            "the peer has the rows"
        );
        assert!(
            count_logs(&querier).await.abs() < f64::EPSILON,
            "not written here"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_unreachable_ingester_means_writing_here() {
        let gone = AppState::in_memory().await;
        let peer = record(&gone, &[Role::Ingest], "http://127.0.0.1:1");
        let state = clustered(
            AppState::in_memory().await,
            &[Role::Query],
            vec![peer],
            Limits::default(),
        );
        state.write(logs()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!((count_logs(&state).await - 3.0).abs() < f64::EPSILON);
    }

    /// Section 14.1: a gossip exchange folds the caller's records into
    /// the view and answers with the whole view, own record included.
    #[tokio::test]
    async fn gossip_exchanges_membership_views() {
        let state = crate::testing::state().await;
        let other = NodeRecord {
            node: tessera_types::NodeId::random(&mut state.env.random.fork()),
            ..record(&state, &[Role::Query], "http://other.invalid")
        };
        let older = NodeRecord {
            heartbeat_at: Timestamp::from_nanos(other.heartbeat_at.as_nanos() - 1_000_000_000),
            ..other.clone()
        };
        let state = clustered(
            state,
            &[Role::Query, Role::Ingest],
            vec![older],
            Limits::default(),
        );
        let app = router(state.clone());
        let request = Request::builder()
            .method("POST")
            .uri("/internal/v1/gossip")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                serde_json::to_vec(&vec![other.clone()]).unwrap(),
            ))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let view: Vec<NodeRecord> = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(view.len(), 2, "own record and the listed peer");
        let peer = view.iter().find(|r| r.node == other.node).unwrap();
        assert_eq!(peer.heartbeat_at, other.heartbeat_at, "refreshed by gossip");
        assert!(view.iter().any(|r| r.node == state.env.node));
    }
}
