// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! `GET /metrics`: the node's load signals (OP-5) in the Prometheus text
//! exposition format, open like liveness so that an autoscaler or a
//! Prometheus can read them without a key.

use axum::Router;
use axum::extract::State;
use axum::http::header;
use axum::response::{IntoResponse, Response};
use axum::routing::get;

use crate::AppState;

/// The metrics router.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/metrics", get(metrics))
        .with_state(state)
}

async fn metrics(State(state): State<AppState>) -> Response {
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.load.snapshot().prometheus_text(),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    #[tokio::test]
    async fn the_load_signals_are_exported() {
        let state = crate::testing::state().await;
        let request = Request::builder()
            .uri("/metrics")
            .body(Body::empty())
            .unwrap();
        let response = crate::router(state).oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let text = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(text.contains("tessera_ingest_queue_depth 0"), "{text}");
        assert!(text.contains("tessera_query_queue_seconds"), "{text}");
    }
}
