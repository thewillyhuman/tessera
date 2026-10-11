// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Request parameters the APIs share: who a request is, the tenant and
//! dataset it addresses and what it needs of them, and times and
//! durations as the Loki and Tempo APIs write them.
//!
//! The principal comes from the API key (CT-5); the tenant from
//! `X-Scope-OrgID`, by tenant name, which the principal must be allowed
//! to act for, and is the principal's own tenant when absent, `default`
//! for a cluster administrator; the dataset from `x-tessera-dataset`,
//! `default` when absent. Reading, writing and administering are then
//! checked against the principal's roles (SE-1).

use std::time::Duration;

use axum::http::HeaderMap;
use tessera_catalog::{Principal, TenantName};
use tessera_types::{DatasetName, TenantId, Timestamp};

use crate::{ApiError, AppState, auth};

/// What a request needs of its dataset.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Need {
    /// To read it.
    Read,
    /// To write it.
    Write,
    /// To administer the tenant: rules, control statements.
    Admin,
}

/// The tenant and dataset a request addresses, once who asks was allowed
/// what it needs.
pub(crate) struct Scope {
    /// The tenant.
    pub tenant: TenantId,
    /// The dataset.
    pub dataset: DatasetName,
}

/// The scope of a request with `headers`, allowed `need`.
///
/// # Errors
///
/// 401 or 403 from authentication; 403 when the principal may not act
/// for the tenant or lacks what `need` asks; 400 for a bad name.
pub(crate) async fn scope(
    state: &AppState,
    headers: &HeaderMap,
    need: Need,
) -> Result<Scope, ApiError> {
    let principal = auth::principal(state, headers).await?;
    let tenant = tenant(state, headers, &principal)?;
    let dataset = match headers
        .get("x-tessera-dataset")
        .and_then(|v| v.to_str().ok())
    {
        None | Some("") => {
            DatasetName::new("default").unwrap_or_else(|_| unreachable!("a valid name"))
        }
        Some(name) => DatasetName::new(name).map_err(|e| ApiError::bad_request(e.to_string()))?,
    };
    allow(&principal, need, dataset.as_str())?;
    Ok(Scope { tenant, dataset })
}

/// The tenant `headers` address for `principal`.
///
/// # Errors
///
/// 400 for a bad or unknown name; 403 when the principal may not act
/// for it.
pub(crate) fn tenant(
    state: &AppState,
    headers: &HeaderMap,
    principal: &Principal,
) -> Result<TenantId, ApiError> {
    let tenant = match headers.get("x-scope-orgid").and_then(|v| v.to_str().ok()) {
        None | Some("") if principal.cluster_admin => TenantId::DEFAULT,
        None | Some("") => principal.tenant.clone(),
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
    if !principal.may_act_for(&tenant) {
        return Err(ApiError::forbidden(format!(
            "these credentials do not act for tenant {tenant}"
        )));
    }
    Ok(tenant)
}

/// Checks that `principal` may do `need` on `dataset`.
///
/// # Errors
///
/// 403 when it may not.
pub(crate) fn allow(principal: &Principal, need: Need, dataset: &str) -> Result<(), ApiError> {
    let allowed = match need {
        Need::Read => principal.may_read(dataset),
        Need::Write => principal.may_write(dataset),
        Need::Admin => principal.is_admin(),
    };
    if allowed {
        Ok(())
    } else {
        Err(ApiError::forbidden(match need {
            Need::Read => format!("these credentials may not read dataset {dataset}"),
            Need::Write => format!("these credentials may not write dataset {dataset}"),
            Need::Admin => "these credentials do not administer the tenant".to_owned(),
        }))
    }
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
