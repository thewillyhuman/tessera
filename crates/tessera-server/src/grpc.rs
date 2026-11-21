// SPDX-License-Identifier: Apache-2.0

//! OTLP over gRPC (IN-1): the logs, traces and metrics export services
//! on the same listener as everything else, as HTTP/2 routes the main
//! router carries, so that a collector's `otlp` exporter pointed at the
//! node works with or without TLS. Each export reads the tenant and
//! dataset from the request's metadata like the HTTP endpoints read
//! their headers, converts the request the same way, and writes it
//! durably.

use axum::Router;
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::{
    LogsService, LogsServiceServer,
};
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::{
    MetricsService, MetricsServiceServer,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::{
    TraceService, TraceServiceServer,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use tessera_ingest::otlp;
use tonic::codec::CompressionEncoding;
use tonic::{Code, Request, Response, Status};

use crate::mtls::ClientCertificate;
use crate::params::{Need, Scope, scope};
use crate::{ApiError, AppState};

/// The three export services as routes of the main router. Each takes
/// messages compressed with gzip, as a collector sends them unless
/// told otherwise, of up to [`crate::MAX_REQUEST_BYTES`] once
/// decompressed, the limit of a request over HTTP.
pub fn router(state: AppState) -> Router {
    let exporter = Exporter { state };
    let logs = LogsServiceServer::new(exporter.clone())
        .accept_compressed(CompressionEncoding::Gzip)
        .max_decoding_message_size(crate::MAX_REQUEST_BYTES);
    let traces = TraceServiceServer::new(exporter.clone())
        .accept_compressed(CompressionEncoding::Gzip)
        .max_decoding_message_size(crate::MAX_REQUEST_BYTES);
    let metrics = MetricsServiceServer::new(exporter)
        .accept_compressed(CompressionEncoding::Gzip)
        .max_decoding_message_size(crate::MAX_REQUEST_BYTES);
    tonic::service::Routes::new(logs)
        .add_service(traces)
        .add_service(metrics)
        .into_axum_router()
}

/// The services' shared state.
#[derive(Clone)]
struct Exporter {
    state: AppState,
}

impl Exporter {
    /// The tenant and dataset a request's metadata names, checked for
    /// writing.
    async fn scope<T>(&self, request: &Request<T>) -> Result<Scope, Status> {
        let (mut parts, ()) = axum::http::Request::new(()).into_parts();
        parts.headers = request.metadata().clone().into_headers();
        if let Some(certificate) = request.extensions().get::<ClientCertificate>() {
            parts.extensions.insert(certificate.clone());
        }
        scope(&self.state, &parts, Need::Write)
            .await
            .map_err(|e| status_of(&e))
    }
}

/// The gRPC status of an API error.
fn status_of(error: &ApiError) -> Status {
    let code = match error.status().as_u16() {
        400 => Code::InvalidArgument,
        401 => Code::Unauthenticated,
        403 => Code::PermissionDenied,
        429 => Code::ResourceExhausted,
        503 => Code::Unavailable,
        _ => Code::Internal,
    };
    Status::new(code, error.message().to_owned())
}

#[tonic::async_trait]
impl LogsService for Exporter {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let scope = self.scope(&request).await?;
        let now = self.state.env.clock.now();
        let batch = otlp::to_batch(
            request.get_ref(),
            scope.tenant,
            scope.dataset,
            now,
            &mut self.state.env.random.fork(),
        )
        .map_err(|e| Status::invalid_argument(e.to_string()))?;
        if let Some(batch) = batch {
            self.state.write(batch).await.map_err(|e| status_of(&e))?;
        }
        Ok(Response::new(ExportLogsServiceResponse::default()))
    }
}

#[tonic::async_trait]
impl TraceService for Exporter {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let scope = self.scope(&request).await?;
        let now = self.state.env.clock.now();
        let batches = otlp::traces::to_batches(request.get_ref(), scope.tenant, scope.dataset, now)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        for batch in batches {
            self.state.write(batch).await.map_err(|e| status_of(&e))?;
        }
        Ok(Response::new(ExportTraceServiceResponse::default()))
    }
}

#[tonic::async_trait]
impl MetricsService for Exporter {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let scope = self.scope(&request).await?;
        let now = self.state.env.clock.now();
        let batch = otlp::metrics::to_batch(request.get_ref(), scope.tenant, scope.dataset, now)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        if let Some(batch) = batch {
            self.state.write(batch).await.map_err(|e| status_of(&e))?;
        }
        Ok(Response::new(ExportMetricsServiceResponse::default()))
    }
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;

    use super::*;

    #[test]
    fn api_errors_map_to_grpc_codes() {
        assert_eq!(
            status_of(&ApiError::bad_request("x")).code(),
            Code::InvalidArgument
        );
        assert_eq!(
            status_of(&ApiError::unauthorized("x")).code(),
            Code::Unauthenticated
        );
        assert_eq!(
            status_of(&ApiError::forbidden("x")).code(),
            Code::PermissionDenied
        );
        assert_eq!(
            status_of(&ApiError::too_many_requests("x")).code(),
            Code::ResourceExhausted
        );
        assert_eq!(
            status_of(&ApiError::unavailable("x")).code(),
            Code::Unavailable
        );
        assert_eq!(ApiError::bad_request("x").status(), StatusCode::BAD_REQUEST);
    }
}
