// SPDX-License-Identifier: Apache-2.0

//! Arbitrary events over HTTP as JSON lines (IN-4): `POST
//! /api/v1/events` takes one JSON object per line, each with a
//! `timestamp`, and writes them as events of kind `event` to the
//! request's tenant and dataset. `name`, `trace_id` and `span_id` fill
//! the columns of the same name; `resource`, `scope` and `attributes`
//! objects become paths under their scope, and any other member becomes
//! an `attr.` path, nested objects flattened with dots.

use axum::Json;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::request::Parts;
use axum::routing::post;
use serde::Serialize;
use serde_json::Value;
use tessera_types::event::{Attributes, Scalar};
use tessera_types::{EventBatch, EventRow, Random, Timestamp};

use crate::params::{Need, WriteParams, scope};
use crate::{ApiError, AppState};

/// The route.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/v1/events", post(ingest_events))
        .with_state(state)
}

/// What a request wrote.
#[derive(Serialize)]
pub struct Accepted {
    /// Events written.
    pub accepted: usize,
}

async fn ingest_events(
    State(state): State<AppState>,
    Query(params): Query<WriteParams>,
    parts: Parts,
    body: Bytes,
) -> Result<Json<Accepted>, ApiError> {
    let scope = scope(&state, &parts, Need::Write).await?;
    let text = std::str::from_utf8(&body)
        .map_err(|_| ApiError::bad_request("the body must be UTF-8 JSON lines"))?;
    let now = state.env.clock.now();
    let mut random = state.env.random.fork();
    let mut rows = Vec::new();
    for (index, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let row = serde_json::from_str::<Value>(line)
            .map_err(|e| e.to_string())
            .and_then(|value| event_of(&value, now, &mut random))
            .map_err(|e| ApiError::bad_request(format!("line {}: {e}", index + 1)))?;
        rows.push(row);
    }
    if !rows.is_empty() {
        let batch = EventBatch::from_events(scope.tenant, scope.dataset, &rows)
            .map_err(|e| ApiError::bad_request(e.to_string()))?;
        state.write_with(batch, params.ack()?).await?;
    }
    Ok(Json(Accepted {
        accepted: rows.len(),
    }))
}

/// The event one JSON object describes.
fn event_of(value: &Value, now: Timestamp, random: &mut impl Random) -> Result<EventRow, String> {
    let Value::Object(members) = value else {
        return Err("an event is a JSON object".into());
    };
    let timestamp = match members.get("timestamp") {
        None | Some(Value::Null) => return Err("timestamp is required".into()),
        Some(Value::String(text)) => text
            .parse::<Timestamp>()
            .map_err(|e| format!("timestamp: {e}"))?,
        Some(Value::Number(nanos)) => nanos
            .as_i64()
            .map(Timestamp::from_nanos)
            .ok_or_else(|| "timestamp: not nanoseconds".to_owned())?,
        Some(_) => return Err("timestamp must be RFC 3339 text or nanoseconds".into()),
    };
    let name = match members.get("name") {
        None | Some(Value::Null) => "event".to_owned(),
        Some(Value::String(name)) => name.clone(),
        Some(_) => return Err("name must be text".into()),
    };
    let mut attributes = Attributes::new();
    for (key, member) in members {
        match key.as_str() {
            "timestamp" | "name" | "trace_id" | "span_id" => {}
            "resource" | "scope" => flatten(key, member, &mut attributes),
            "attributes" => flatten("attr", member, &mut attributes),
            other => flatten(&format!("attr.{other}"), member, &mut attributes),
        }
    }
    Ok(EventRow {
        timestamp,
        observed_timestamp: now,
        event_id: random.next_u128().to_be_bytes(),
        trace_id: id_of(members.get("trace_id"), "trace_id")?,
        span_id: id_of(members.get("span_id"), "span_id")?,
        name,
        attributes,
    })
}

/// A hex id member of `n` bytes, if present.
fn id_of<const N: usize>(value: Option<&Value>, what: &str) -> Result<Option<[u8; N]>, String> {
    let Some(value) = value.filter(|v| !v.is_null()) else {
        return Ok(None);
    };
    let text = value
        .as_str()
        .ok_or_else(|| format!("{what} must be hex text"))?;
    let digits: Vec<u8> = text
        .as_bytes()
        .chunks(2)
        .map(|pair| {
            std::str::from_utf8(pair)
                .ok()
                .and_then(|p| u8::from_str_radix(p, 16).ok())
        })
        .collect::<Option<_>>()
        .ok_or_else(|| format!("{what} must be hex text"))?;
    let bytes: [u8; N] = digits
        .try_into()
        .map_err(|_| format!("{what} must be {} hex digits", 2 * N))?;
    Ok(Some(bytes))
}

/// Stores `value` under `path`: scalars as themselves, objects by
/// member with a dot, lists as their JSON text, nulls not at all.
fn flatten(path: &str, value: &Value, into: &mut Attributes) {
    match value {
        Value::Null => {}
        Value::Bool(b) => {
            into.insert(path.to_owned(), Scalar::Bool(*b));
        }
        Value::Number(n) => {
            let scalar = n.as_i64().map_or_else(
                || Scalar::Float(n.as_f64().unwrap_or(f64::NAN)),
                Scalar::Int,
            );
            into.insert(path.to_owned(), scalar);
        }
        Value::String(s) => {
            into.insert(path.to_owned(), Scalar::Text(s.clone()));
        }
        Value::Array(_) => {
            into.insert(path.to_owned(), Scalar::Text(value.to_string()));
        }
        Value::Object(members) => {
            for (key, member) in members {
                flatten(&format!("{path}.{key}"), member, into);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, StatusCode, header};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::*;

    async fn post(app: &Router, uri: &str, body: &str) -> (StatusCode, Value) {
        let request = Request::builder()
            .method("POST")
            .uri(uri)
            .header(header::CONTENT_TYPE, "application/x-ndjson")
            .header(crate::otlp::DATASET_HEADER, "prod")
            .body(Body::from(body.to_owned()))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    async fn sql(app: &Router, query: &str) -> Value {
        let body = serde_json::json!({ "dataset": "prod", "query": query });
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/query")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn json_lines_become_events_with_their_members_as_attributes() {
        let app = crate::router(crate::testing::state().await);
        let lines = "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"name\":\"deploy\",\
                     \"trace_id\":\"0123456789abcdef0123456789abcdef\",\"span_id\":\"0123456789abcdef\",\
                     \"resource\":{\"service\":{\"name\":\"api\"}},\"attributes\":{\"version\":\"1.2\"},\
                     \"http\":{\"status\":500},\"tags\":[\"a\",\"b\"],\"ok\":false}\n\
                     \n\
                     {\"timestamp\":1704067201000000000}\n";
        let (status, value) = post(&app, "/api/v1/events?ack=buffered", lines).await;
        assert_eq!(status, StatusCode::OK, "{value}");
        assert_eq!(value["accepted"], 2);
        let (status, _) = post(
            &app,
            "/api/v1/events",
            "{\"timestamp\":\"2024-01-01T00:00:02Z\"}\n",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let value = sql(
            &app,
            "SELECT name, resource.service.name, attr.version, attr.http.status, attr.tags, attr.ok \
             FROM events WHERE kind = 'event' ORDER BY timestamp",
        )
        .await;
        let rows = value["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], "deploy");
        assert_eq!(rows[0][1], "api");
        assert_eq!(rows[0][2], "1.2");
        assert_eq!(rows[0][3], "500");
        assert_eq!(rows[0][4], "[\"a\",\"b\"]");
        assert_eq!(rows[0][5], "false");
        assert_eq!(rows[1][0], "event", "the name defaults");
        let value = sql(
            &app,
            "SELECT count(*) FROM events WHERE kind = 'event' AND trace_id IS NOT NULL",
        )
        .await;
        assert_eq!(value["rows"][0][0], 1.0);
    }

    /// RS-5: no line, however mangled, makes the event conversion panic.
    #[test]
    fn mangled_lines_never_panic() {
        let seeds = [
            "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"name\":\"deploy\",\"trace_id\":\"0123456789abcdef0123456789abcdef\",\"span_id\":\"0123456789abcdef\",\"resource\":{\"service\":{\"name\":\"api\"}},\"attributes\":{\"version\":\"1.2\"},\"http\":{\"status\":500},\"tags\":[\"a\"],\"ok\":false}",
            "{\"timestamp\":1704067201000000000}",
        ];
        let mut random = tessera_types::SeededRandom::new(5);
        let now = Timestamp::from_nanos(1);
        for round in 0..1_500 {
            let input = tessera_types::mutated(seeds[round % seeds.len()].as_bytes(), &mut random);
            if let Ok(value) = serde_json::from_slice::<Value>(&input) {
                let _ = event_of(&value, now, &mut random);
            }
        }
    }

    #[tokio::test]
    async fn a_bad_line_is_refused_by_number() {
        let app = crate::router(crate::testing::state().await);
        let (status, value) = post(
            &app,
            "/api/v1/events",
            "{\"timestamp\":\"2024-01-01T00:00:00Z\"}\n{\"name\":\"no time\"}\n",
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(value["error"], "line 2: timestamp is required");
        let (status, value) = post(&app, "/api/v1/events", "[1, 2]\n").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(value["error"], "line 1: an event is a JSON object");
        let (status, value) = post(
            &app,
            "/api/v1/events",
            "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"trace_id\":\"zz\"}\n",
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(value["error"], "line 1: trace_id must be hex text");
    }
}
