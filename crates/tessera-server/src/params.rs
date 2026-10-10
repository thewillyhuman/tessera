// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Request parameters the compatibility APIs share: the tenant and
//! dataset a request addresses, and times and durations as the Loki and
//! Tempo APIs write them.
//!
//! The tenant comes from `X-Scope-OrgID` (CT-5), by tenant name,
//! `default` when absent; the dataset from `x-tessera-dataset`,
//! `default` when absent.

use std::time::Duration;

use axum::http::HeaderMap;
use tessera_catalog::TenantName;
use tessera_types::{DatasetName, TenantId, Timestamp};

use crate::{ApiError, AppState};

/// The tenant and dataset a request addresses.
pub(crate) struct Scope {
    /// The tenant.
    pub tenant: TenantId,
    /// The dataset.
    pub dataset: DatasetName,
}

pub(crate) fn scope(state: &AppState, headers: &HeaderMap) -> Result<Scope, ApiError> {
    let tenant = match headers.get("x-scope-orgid").and_then(|v| v.to_str().ok()) {
        None | Some("") => TenantId::DEFAULT,
        Some(name) => {
            let name = TenantName::new(name).map_err(|e| ApiError::bad_request(e.to_string()))?;
            state
                .catalog
                .cached()
                .state()
                .tenant_by_name(&name)
                .map(|t| t.id.clone())
                .ok_or_else(|| ApiError::bad_request(format!("unknown tenant {name}")))?
        }
    };
    let dataset = match headers
        .get("x-tessera-dataset")
        .and_then(|v| v.to_str().ok())
    {
        None | Some("") => {
            DatasetName::new("default").unwrap_or_else(|_| unreachable!("a valid name"))
        }
        Some(name) => DatasetName::new(name).map_err(|e| ApiError::bad_request(e.to_string()))?,
    };
    Ok(Scope { tenant, dataset })
}

/// A Loki timestamp parameter: nanoseconds, seconds with a fraction, or
/// RFC 3339.
pub(crate) fn parse_time(text: &str, now: Timestamp) -> Result<Timestamp, ApiError> {
    if let Ok(nanos) = text.parse::<i64>() {
        // Loki takes integers as nanoseconds; small values are seconds.
        return Ok(if nanos < 100_000_000_000 {
            Timestamp::from_nanos(nanos.saturating_mul(1_000_000_000))
        } else {
            Timestamp::from_nanos(nanos)
        });
    }
    if let Ok(seconds) = text.parse::<f64>() {
        #[allow(clippy::cast_possible_truncation)]
        return Ok(Timestamp::from_nanos((seconds * 1e9) as i64));
    }
    if text == "now" {
        return Ok(now);
    }
    text.parse::<Timestamp>()
        .map_err(|e| ApiError::bad_request(format!("invalid time {text:?}: {e}")))
}

/// A step or duration parameter: a duration such as `15s` or seconds.
pub(crate) fn parse_step(text: &str) -> Result<Duration, ApiError> {
    if let Ok(seconds) = text.parse::<f64>() {
        return Ok(Duration::from_secs_f64(seconds.max(0.0)));
    }
    tessera_logql::lexer::parse_duration(text)
        .ok_or_else(|| ApiError::bad_request(format!("invalid duration {text:?}")))
}
