// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! What nodes say to each other (sections 7.3 and 10.7), under
//! `/internal/v1`: a leaf task to run (`POST /internal/v1/scan`, a JSON
//! `LeafTask`, answered with the rows as an Arrow IPC stream), a stage
//! to run over a share of a scan's files (`POST /internal/v1/stage`, a
//! JSON `StageTask`, answered the same way; QE-14) and a
//! batch to write (`POST /internal/v1/ingest`, an Arrow IPC stream with
//! the tenant, dataset and kind in headers, answered once durable), and
//! a membership exchange (`POST /internal/v1/gossip`, the caller's view
//! as JSON node records, answered with this node's view, section 14.1).
//! [`HttpRemote`] is the client side of all three.
//!
//! These routes carry no authentication yet; they are meant for the
//! loopback and private networks a cluster runs on (M10 hardens them).

use std::io::Cursor;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use futures::future::BoxFuture;
use tessera_cluster::NodeRecord;
use tessera_exec::{LeafTask, Peer, RemoteError, RemoteScan, StageTask};
use tessera_ir::Registry;
use tessera_types::{DatasetName, EventBatch, Kind, TenantId};

use crate::{ApiError, AppState};

/// The content type of an Arrow IPC stream body.
pub const ARROW_STREAM: &str = "application/vnd.apache.arrow.stream";
/// The header naming a forwarded batch's tenant.
pub const TENANT_HEADER: &str = "x-tessera-tenant";
/// The header naming a forwarded batch's dataset.
pub const DATASET_HEADER: &str = "x-tessera-dataset";
/// The header naming a forwarded batch's kind.
pub const KIND_HEADER: &str = "x-tessera-kind";

/// The internal router.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/internal/v1/scan", post(scan))
        .route("/internal/v1/stage", post(stage))
        .route("/internal/v1/ingest", post(ingest))
        .route("/internal/v1/gossip", post(gossip))
        .with_state(state)
}

/// `POST /internal/v1/gossip`: folds the caller's view into this node's,
/// refreshing the members it knows, and answers with this node's view;
/// a node outside any cluster answers nothing.
async fn gossip(
    State(state): State<AppState>,
    Json(records): Json<Vec<NodeRecord>>,
) -> Json<Vec<NodeRecord>> {
    let Some(cluster) = &state.cluster else {
        return Json(Vec::new());
    };
    cluster.membership.merge(records);
    Json(cluster.membership.records())
}

/// `batches` as an Arrow IPC stream, `schema` first.
///
/// # Errors
///
/// If a batch does not match the schema.
pub fn encode(
    schema: &SchemaRef,
    batches: &[RecordBatch],
) -> Result<Vec<u8>, arrow::error::ArrowError> {
    let mut out = Vec::new();
    let mut writer = StreamWriter::try_new(&mut out, schema)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    Ok(out)
}

/// The batches of an Arrow IPC stream, with its schema.
///
/// # Errors
///
/// If the bytes are not a stream.
pub fn decode(bytes: &[u8]) -> Result<(SchemaRef, Vec<RecordBatch>), arrow::error::ArrowError> {
    let reader = StreamReader::try_new(Cursor::new(bytes), None)?;
    let schema = reader.schema();
    let batches = reader.collect::<Result<Vec<_>, _>>()?;
    Ok((schema, batches))
}

async fn scan(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    state.auth.check_cluster(&headers)?;
    let task: LeafTask = serde_json::from_slice(&body)
        .map_err(|e| ApiError::bad_request(format!("invalid leaf task: {e}")))?;
    let schema = task
        .scan
        .schema()
        .map_err(|e| ApiError::bad_request(e.to_string()))?;
    let batches = state
        .engine
        .leaf(&task)
        .await
        .map_err(|e| ApiError::query(tessera_exec::QueryError::Engine(e), ""))?;
    let bytes = encode(&schema, &batches).map_err(|e| ApiError::bad_request(e.to_string()))?;
    Ok((
        [(header::CONTENT_TYPE, HeaderValue::from_static(ARROW_STREAM))],
        bytes,
    )
        .into_response())
}

/// A stage sent by a coordinator (QE-14): its plan over its files, the
/// output as an Arrow stream.
async fn stage(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    state.auth.check_cluster(&headers)?;
    let task: StageTask = serde_json::from_slice(&body)
        .map_err(|e| ApiError::bad_request(format!("invalid stage task: {e}")))?;
    let schema = task
        .plan
        .schema(&Registry::builtin())
        .map_err(|e| ApiError::bad_request(e.to_string()))?;
    let batches = state
        .engine
        .stage(&task)
        .await
        .map_err(|e| ApiError::query(tessera_exec::QueryError::Engine(e), ""))?;
    let bytes = encode(&schema_of(&batches, schema), &batches)
        .map_err(|e| ApiError::bad_request(e.to_string()))?;
    Ok((
        [(header::CONTENT_TYPE, HeaderValue::from_static(ARROW_STREAM))],
        bytes,
    )
        .into_response())
}

fn header_text<'a>(headers: &'a HeaderMap, name: &str) -> Result<&'a str, ApiError> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| ApiError::bad_request(format!("missing header {name}")))
}

async fn ingest(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    state.auth.check_cluster(&headers)?;
    let tenant = TenantId::new(header_text(&headers, TENANT_HEADER)?)
        .map_err(|e| ApiError::bad_request(e.to_string()))?;
    let dataset = DatasetName::new(header_text(&headers, DATASET_HEADER)?)
        .map_err(|e| ApiError::bad_request(e.to_string()))?;
    let kind: Kind = header_text(&headers, KIND_HEADER)?
        .parse()
        .map_err(|e: tessera_types::kind::UnknownKind| ApiError::bad_request(e.to_string()))?;
    let (_, batches) = decode(&body).map_err(|e| ApiError::bad_request(e.to_string()))?;
    for rows in batches {
        if rows.num_rows() == 0 {
            continue;
        }
        let batch = EventBatch::new(tenant.clone(), dataset.clone(), kind, rows)
            .map_err(|e| ApiError::bad_request(e.to_string()))?;
        state.ingest.write(batch).await?;
    }
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// The client of the internal routes, over HTTP with a bounded timeout.
#[derive(Clone, Debug)]
pub struct HttpRemote {
    agent: ureq::Agent,
    /// The cluster secret sent as a bearer token (SE-3), if any.
    secret: Option<String>,
}

impl Default for HttpRemote {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpRemote {
    /// A client whose requests time out after a minute.
    #[must_use]
    pub fn new() -> Self {
        HttpRemote {
            agent: ureq::Agent::config_builder()
                .http_status_as_error(false)
                .timeout_global(Some(std::time::Duration::from_secs(60)))
                .build()
                .into(),
            secret: None,
        }
    }

    /// This client presenting `secret` to its peers.
    #[must_use]
    pub fn with_secret(mut self, secret: Option<String>) -> Self {
        self.secret = secret.filter(|s| !s.is_empty());
        self
    }

    /// This client checking its peers' certificates against the CA in
    /// `pem` (SE-3); the system's roots when there is none.
    ///
    /// # Errors
    ///
    /// If `pem` holds no certificate.
    pub fn with_root_ca(mut self, pem: Option<Vec<u8>>) -> Result<Self, RemoteError> {
        let Some(pem) = pem else {
            return Ok(self);
        };
        let ca = ureq::tls::Certificate::from_pem(&pem)
            .map_err(|e| RemoteError(format!("cluster CA: {e}")))?;
        let tls = ureq::tls::TlsConfig::builder()
            .root_certs(ureq::tls::RootCerts::new_with_certs(&[ca]))
            .build();
        self.agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .timeout_global(Some(std::time::Duration::from_secs(60)))
            .tls_config(tls)
            .build()
            .into();
        Ok(self)
    }

    /// Sends `body` to `url` and returns the response body, or the
    /// failure with the response text.
    fn post(
        &self,
        url: &str,
        content_type: &'static str,
        headers: &[(&'static str, String)],
        body: &[u8],
    ) -> Result<Vec<u8>, RemoteError> {
        let mut request = self.agent.post(url).header("content-type", content_type);
        if let Some(secret) = &self.secret {
            request = request.header("authorization", &format!("Bearer {secret}"));
        }
        for (name, value) in headers {
            request = request.header(*name, value);
        }
        let mut response = request
            .send(body)
            .map_err(|e| RemoteError(format!("{url}: {e}")))?;
        let status = response.status();
        let bytes = response
            .body_mut()
            .read_to_vec()
            .map_err(|e| RemoteError(format!("{url}: unreadable response: {e}")))?;
        if !status.is_success() {
            return Err(RemoteError(format!(
                "{url}: {status}: {}",
                String::from_utf8_lossy(&bytes)
            )));
        }
        Ok(bytes)
    }

    /// Exchanges membership views with the node at `http` (section
    /// 14.1): sends `records` and returns the peer's view.
    ///
    /// # Errors
    ///
    /// If the request fails or the answer does not decode.
    pub async fn gossip(
        &self,
        http: &str,
        records: &[NodeRecord],
    ) -> Result<Vec<NodeRecord>, RemoteError> {
        let body = serde_json::to_vec(records).map_err(|e| RemoteError(e.to_string()))?;
        let url = format!("{}/internal/v1/gossip", http.trim_end_matches('/'));
        let client = self.clone();
        let target = url.clone();
        let bytes = tokio::task::spawn_blocking(move || {
            client.post(&target, "application/json", &[], &body)
        })
        .await
        .map_err(|e| RemoteError(e.to_string()))??;
        serde_json::from_slice(&bytes).map_err(|e| RemoteError(format!("{url}: {e}")))
    }

    /// Writes `batch` through the ingester at `http` (IN-9).
    ///
    /// # Errors
    ///
    /// If the request fails or the ingester rejects it.
    pub async fn forward(&self, http: &str, batch: &EventBatch) -> Result<(), RemoteError> {
        let body = encode(&batch.rows().schema(), std::slice::from_ref(batch.rows()))
            .map_err(|e| RemoteError(e.to_string()))?;
        let headers = vec![
            (TENANT_HEADER, batch.tenant().to_string()),
            (DATASET_HEADER, batch.dataset().to_string()),
            (KIND_HEADER, batch.kind().to_string()),
        ];
        let url = format!("{}/internal/v1/ingest", http.trim_end_matches('/'));
        let client = self.clone();
        tokio::task::spawn_blocking(move || client.post(&url, ARROW_STREAM, &headers, &body))
            .await
            .map_err(|e| RemoteError(e.to_string()))??;
        Ok(())
    }
}

impl RemoteScan for HttpRemote {
    fn leaf<'a>(
        &'a self,
        peer: &'a Peer,
        task: &'a LeafTask,
    ) -> BoxFuture<'a, Result<Vec<RecordBatch>, RemoteError>> {
        Box::pin(async move {
            let body = serde_json::to_vec(task).map_err(|e| RemoteError(e.to_string()))?;
            let url = format!("{}/internal/v1/scan", peer.http.trim_end_matches('/'));
            let client = self.clone();
            let bytes = tokio::task::spawn_blocking(move || {
                client.post(&url, "application/json", &[], &body)
            })
            .await
            .map_err(|e| RemoteError(e.to_string()))??;
            let (_, batches) = decode(&bytes).map_err(|e| RemoteError(e.to_string()))?;
            Ok(batches)
        })
    }

    fn stage<'a>(
        &'a self,
        peer: &'a Peer,
        task: &'a StageTask,
    ) -> BoxFuture<'a, Result<Vec<RecordBatch>, RemoteError>> {
        Box::pin(async move {
            let body = serde_json::to_vec(task).map_err(|e| RemoteError(e.to_string()))?;
            let url = format!("{}/internal/v1/stage", peer.http.trim_end_matches('/'));
            let client = self.clone();
            let bytes = tokio::task::spawn_blocking(move || {
                client.post(&url, "application/json", &[], &body)
            })
            .await
            .map_err(|e| RemoteError(e.to_string()))??;
            let (_, batches) = decode(&bytes).map_err(|e| RemoteError(e.to_string()))?;
            Ok(batches)
        })
    }
}

/// The schema of `batches`, or `fallback` when there are none.
#[must_use]
pub fn schema_of(batches: &[RecordBatch], fallback: SchemaRef) -> SchemaRef {
    batches.first().map_or(fallback, RecordBatch::schema)
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use tessera_ir::Scan;
    use tessera_types::event::Scalar;
    use tessera_types::{LogRow, Timestamp};
    use tower::ServiceExt;

    use super::*;

    fn logs() -> EventBatch {
        let rows: Vec<LogRow> = (0..3u8)
            .map(|i| LogRow {
                timestamp: Timestamp::from_nanos(1_700_000_000_000_000_000 + i64::from(i)),
                event_id: [i; 16],
                body: Some(format!("forwarded {i}")),
                attributes: std::collections::BTreeMap::from([(
                    "attr.n".to_owned(),
                    Scalar::Int(i64::from(i)),
                )]),
                ..LogRow::default()
            })
            .collect();
        EventBatch::from_logs(TenantId::DEFAULT, DatasetName::new("prod").unwrap(), &rows).unwrap()
    }

    #[test]
    fn ipc_round_trips_batches() {
        let batch = logs();
        let bytes = encode(&batch.rows().schema(), std::slice::from_ref(batch.rows())).unwrap();
        let (schema, batches) = decode(&bytes).unwrap();
        assert_eq!(schema, batch.rows().schema());
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0], *batch.rows());
        let empty = encode(&batch.rows().schema(), &[]).unwrap();
        let (schema, batches) = decode(&empty).unwrap();
        assert_eq!(schema_of(&batches, schema.clone()), schema);
        assert!(batches.is_empty());
        assert!(decode(b"not arrow").is_err());
    }

    /// SE-3: with a cluster secret configured, the internal routes take
    /// it as a bearer token and refuse requests without it.
    #[tokio::test]
    async fn internal_routes_need_the_cluster_secret_once_one_is_set() {
        let mut state = crate::testing::state().await;
        state.auth = std::sync::Arc::new(
            crate::auth::Auth::new(true).with_cluster_secret(Some("s3cret".into())),
        );
        let app = crate::router(state);
        let batch = logs();
        let body = encode(&batch.rows().schema(), std::slice::from_ref(batch.rows())).unwrap();
        let request = |token: Option<&str>| {
            let mut request = Request::builder()
                .method("POST")
                .uri("/internal/v1/ingest")
                .header(header::CONTENT_TYPE, ARROW_STREAM)
                .header(TENANT_HEADER, "default")
                .header(DATASET_HEADER, "prod")
                .header(KIND_HEADER, "log");
            if let Some(token) = token {
                request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
            }
            request.body(Body::from(body.clone())).unwrap()
        };
        let response = app.clone().oneshot(request(None)).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let response = app.clone().oneshot(request(Some("wrong"))).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let response = app.clone().oneshot(request(Some("s3cret"))).await.unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        let scan = Request::builder()
            .method("POST")
            .uri("/internal/v1/scan")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from("{}"))
            .unwrap();
        let response = app.oneshot(scan).await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "checked before the body"
        );
    }

    #[tokio::test]
    async fn forwarded_batches_are_written_and_leaf_tasks_answer() {
        let app = crate::router(crate::testing::state().await);
        let batch = logs();
        let body = encode(&batch.rows().schema(), std::slice::from_ref(batch.rows())).unwrap();
        let request = Request::builder()
            .method("POST")
            .uri("/internal/v1/ingest")
            .header(header::CONTENT_TYPE, ARROW_STREAM)
            .header(TENANT_HEADER, "default")
            .header(DATASET_HEADER, "prod")
            .header(KIND_HEADER, "log")
            .body(Body::from(body))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let query = serde_json::json!({ "dataset": "prod", "query": "SELECT count(*) FROM logs" });
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/query")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(query.to_string()))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["rows"][0][0], 3.0, "{value}");

        let task = LeafTask {
            scan: Scan::all(
                TenantId::DEFAULT,
                DatasetName::new("prod").unwrap(),
                Kind::Log,
            ),
            files: Vec::new(),
        };
        let request = Request::builder()
            .method("POST")
            .uri("/internal/v1/scan")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&task).unwrap()))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            ARROW_STREAM
        );
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let (schema, batches) = decode(&bytes).unwrap();
        assert_eq!(schema, tessera_types::event::schema(Kind::Log));
        assert!(batches.is_empty(), "no files were named");

        // QE-14: a stage answers with its partial output, here the state
        // of a count over no files.
        let stage = StageTask {
            plan: tessera_ir::Plan::PartialAggregate {
                input: Box::new(tessera_ir::Plan::Scan(Scan::all(
                    TenantId::DEFAULT,
                    DatasetName::new("prod").unwrap(),
                    Kind::Log,
                ))),
                group_by: vec![],
                aggregates: vec![tessera_ir::AggregateExpr {
                    function: "count".into(),
                    args: vec![],
                    name: "n".into(),
                }],
            },
            files: Vec::new(),
            exchange: None,
        };
        let request = Request::builder()
            .method("POST")
            .uri("/internal/v1/stage")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&stage).unwrap()))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let (schema, batches) = decode(&bytes).unwrap();
        assert_eq!(schema.fields()[0].name(), "n:state0");
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);

        let request = Request::builder()
            .method("POST")
            .uri("/internal/v1/ingest")
            .header(TENANT_HEADER, "default")
            .body(Body::from("x"))
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
