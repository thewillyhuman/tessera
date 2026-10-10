// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The error shape of every endpoint: `{"error": "..."}` with a status
//! that says whether to retry. 503 means the object store or the ingest
//! service is unavailable and the request may be retried; 409 is a
//! conflict with the current state; 404 an unknown name; 400 a malformed
//! request.

use axum::Json;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use tessera_catalog::CatalogError;
use tessera_exec::{EngineError, QueryError};
use tessera_ingest::IngestError;

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

/// An error with the status that tells the client what to do.
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    /// A malformed request.
    #[must_use]
    pub fn bad_request(message: impl Into<String>) -> Self {
        ApiError {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    /// An unsupported media type.
    #[must_use]
    pub fn unsupported_media_type(message: impl Into<String>) -> Self {
        ApiError {
            status: StatusCode::UNSUPPORTED_MEDIA_TYPE,
            message: message.into(),
        }
    }

    /// The service cannot serve the request now; the client may retry.
    #[must_use]
    pub fn unavailable(message: impl Into<String>) -> Self {
        ApiError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: message.into(),
        }
    }

    /// A failed query: syntax and planning errors are the client's, with
    /// the position described against `text`; storage failures may be
    /// retried; anything else failed in the engine.
    #[must_use]
    pub fn query(error: QueryError, text: &str) -> Self {
        match error {
            QueryError::Syntax(diagnostic) => ApiError::bad_request(diagnostic.describe(text)),
            QueryError::Plan(error) => ApiError::bad_request(error.to_string()),
            QueryError::Engine(
                error @ (EngineError::Manifest(_) | EngineError::Format(_) | EngineError::Pool(_)),
            ) => {
                tracing::warn!(error = %error, "query storage read failed");
                ApiError::unavailable(error.to_string())
            }
            QueryError::Engine(error) => {
                tracing::warn!(error = %error, "query failed");
                ApiError {
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    message: error.to_string(),
                }
            }
        }
    }
}

impl From<CatalogError> for ApiError {
    fn from(error: CatalogError) -> Self {
        let status = match &error {
            CatalogError::UnknownTenant(_) | CatalogError::UnknownPool(_) => StatusCode::NOT_FOUND,
            CatalogError::ReservedName(_) => StatusCode::BAD_REQUEST,
            CatalogError::TenantExists(_)
            | CatalogError::BuiltinTenant(_)
            | CatalogError::WrongStatus { .. }
            | CatalogError::GracePeriod(_)
            | CatalogError::PoolExists(_)
            | CatalogError::RootPool(_)
            | CatalogError::SlotsAtMaximum(_) => StatusCode::CONFLICT,
            CatalogError::Contention { .. } | CatalogError::Log(_) => {
                StatusCode::SERVICE_UNAVAILABLE
            }
        };
        if status == StatusCode::SERVICE_UNAVAILABLE {
            tracing::warn!(error = %error, "catalog unavailable");
        }
        ApiError {
            status,
            message: error.to_string(),
        }
    }
}

impl From<IngestError> for ApiError {
    fn from(error: IngestError) -> Self {
        tracing::warn!(error = %error, "write rejected");
        ApiError::unavailable(error.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut response = (
            self.status,
            Json(ErrorBody {
                error: self.message,
            }),
        )
            .into_response();
        if self.status == StatusCode::SERVICE_UNAVAILABLE {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        }
        response
    }
}
