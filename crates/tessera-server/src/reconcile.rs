// SPDX-License-Identifier: Apache-2.0

//! Reconciliation of `system.usage` with the object store (section
//! 13.1; the exit criterion of M9): what the usage says each tenant
//! holds, from the latest `held` record of each dataset and kind, next
//! to what the stores list under the tenant's keys, and whether the two
//! agree within [`TOLERANCE`].
//!
//! The difference has known sources: a compaction output written and
//! not yet adopted or superseded, replaced files kept until the
//! collection window passes, and the hour between two `held` records.
//! Indexes and the logs are listed apart from the data files, since
//! the bill counts them and the usage does not.

use std::collections::BTreeMap;

use arrow::array::{Array, Int64Array, StringArray};
use axum::Json;
use axum::extract::State;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use tessera_exec::{RunOptions, query};
use tessera_ir::BindContext;
use tessera_object::Key;
use tessera_sql::SqlFrontend;
use tessera_types::{DatasetName, TenantId};

use crate::{ApiError, AppState, metering};

/// How far apart, as a share of the larger, the usage and the store may
/// be and still count as reconciled.
pub const TOLERANCE: f64 = 0.10;

/// One tenant's reconciliation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Reconciliation {
    /// The tenant.
    pub tenant: String,
    /// Bytes the latest `held` usage records say the tenant's data
    /// files hold.
    pub metered_bytes: u64,
    /// Bytes of data files the stores list for the tenant.
    pub store_bytes: u64,
    /// Bytes of sidecar indexes listed, which the usage leaves out.
    pub index_bytes: u64,
    /// Bytes of everything else under the tenant: logs, manifests,
    /// state and control stores.
    pub other_bytes: u64,
    /// `store_bytes - metered_bytes`.
    pub difference: i64,
    /// Whether the difference is within [`TOLERANCE`] of the larger.
    pub within_tolerance: bool,
}

/// `GET /api/v1/usage/reconcile`: every tenant's reconciliation.
///
/// # Errors
///
/// If the usage cannot be queried or a store cannot be listed.
pub async fn reconcile(
    State(state): State<AppState>,
) -> Result<Json<Vec<Reconciliation>>, ApiError> {
    let tenants: Vec<TenantId> = state
        .catalog
        .cached()
        .state()
        .tenants()
        .map(|t| t.id.clone())
        .collect();
    let mut out = Vec::new();
    for tenant in tenants {
        let metered = metered_bytes(&state, &tenant).await?;
        let listed = listed_bytes(&state, &tenant).await?;
        out.push(reconciliation(&tenant, metered, listed));
    }
    Ok(Json(out))
}

/// Bytes listed under a tenant, by what the keys are.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Listed {
    data: u64,
    index: u64,
    other: u64,
}

fn reconciliation(tenant: &TenantId, metered: u64, listed: Listed) -> Reconciliation {
    let difference =
        i64::try_from(listed.data).unwrap_or(i64::MAX) - i64::try_from(metered).unwrap_or(i64::MAX);
    let larger = listed.data.max(metered);
    #[allow(clippy::cast_precision_loss)]
    let within_tolerance = difference.unsigned_abs() as f64 <= TOLERANCE * larger as f64;
    Reconciliation {
        tenant: tenant.to_string(),
        metered_bytes: metered,
        store_bytes: listed.data,
        index_bytes: listed.index,
        other_bytes: listed.other,
        difference,
        within_tolerance,
    }
}

/// The latest `held` record of each of the tenant's datasets and kinds,
/// summed.
async fn metered_bytes(state: &AppState, tenant: &TenantId) -> Result<u64, ApiError> {
    let dataset = DatasetName::new(metering::DATASET)
        .map_err(|e| ApiError::bad_request(format!("usage dataset: {e}")))?;
    let ctx = BindContext::new(TenantId::SYSTEM, dataset, state.env.clock.now());
    let text = format!(
        "SELECT dataset, kind, bytes FROM system.usage \
         WHERE phase = '{}' AND tenant = '{}' ORDER BY timestamp DESC",
        metering::HELD,
        tenant.as_str()
    );
    let response = query::run_with(
        &state.engine,
        &SqlFrontend,
        &text,
        &ctx,
        RunOptions::default(),
    )
    .await
    .map_err(|e| ApiError::query(e, &text))?;
    let tessera_exec::Response::Rows(output) = response else {
        return Err(ApiError::unavailable("the usage query answered no rows"));
    };
    let mut latest: BTreeMap<(String, String), u64> = BTreeMap::new();
    for batch in &output.batches {
        let text = |name: &str| {
            batch
                .column_by_name(name)
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        };
        let (Some(datasets), Some(kinds)) = (text("dataset"), text("kind")) else {
            continue;
        };
        let Some(bytes) = batch
            .column_by_name("bytes")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        else {
            continue;
        };
        for i in 0..batch.num_rows() {
            if bytes.is_null(i) {
                continue;
            }
            let key = (datasets.value(i).to_owned(), kinds.value(i).to_owned());
            // Newest first: the first row of a key is its latest record.
            latest
                .entry(key)
                .or_insert_with(|| u64::try_from(bytes.value(i)).unwrap_or(0));
        }
    }
    Ok(latest.values().sum())
}

/// Bytes every pool lists under the tenant's keys, by what they are.
async fn listed_bytes(state: &AppState, tenant: &TenantId) -> Result<Listed, ApiError> {
    let prefix =
        Key::new(tenant.as_str()).map_err(|e| ApiError::bad_request(format!("tenant key: {e}")))?;
    let mut listed = Listed::default();
    for pool in state.pools.iter() {
        let objects: Vec<tessera_object::ObjectMeta> = pool
            .store()
            .list(&prefix)
            .try_collect()
            .await
            .map_err(|e| ApiError::unavailable(e.to_string()))?;
        for object in objects {
            match classify(object.key.as_str()) {
                Class::Data => listed.data += object.size,
                Class::Index => listed.index += object.size,
                Class::Other => listed.other += object.size,
            }
        }
    }
    Ok(listed)
}

/// What a key under a tenant is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Class {
    Data,
    Index,
    Other,
}

/// A partition's data files live under `{tenant}/{dataset}/p/{kind}/
/// {window}/d/` and its indexes under `.../i/`; everything else is a
/// log or a store of some kind.
fn classify(key: &str) -> Class {
    let parts: Vec<&str> = key.split('/').collect();
    match parts.as_slice() {
        [_, _, "p", _, _, "d", ..] => Class::Data,
        [_, _, "p", _, _, "i", ..] => Class::Index,
        _ => Class::Other,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tessera_object::layout;
    use tessera_types::{FileId, Held, Kind, Meter, PartitionId, Timestamp, Window};
    use tower::ServiceExt;

    use super::*;

    #[test]
    fn keys_are_classified_by_their_place_in_the_layout() {
        assert_eq!(classify("t/prod/p/log/0-86400/d/ab/x.parquet"), Class::Data);
        assert_eq!(classify("t/prod/p/log/0-86400/i/ab/x.idx"), Class::Index);
        assert_eq!(classify("t/prod/p/log/0-86400/_log/000001"), Class::Other);
        assert_eq!(classify("t/_control/_log/000001"), Class::Other);
    }

    #[test]
    fn the_tolerance_is_a_share_of_the_larger_side() {
        let listed = |data| Listed {
            data,
            index: 0,
            other: 0,
        };
        assert!(reconciliation(&TenantId::DEFAULT, 1_000, listed(1_050)).within_tolerance);
        assert!(reconciliation(&TenantId::DEFAULT, 1_050, listed(1_000)).within_tolerance);
        let apart = reconciliation(&TenantId::DEFAULT, 1_000, listed(1_500));
        assert!(!apart.within_tolerance);
        assert_eq!(apart.difference, 500);
        assert!(reconciliation(&TenantId::DEFAULT, 0, listed(0)).within_tolerance);
    }

    /// M9: the `held` usage the background work records agrees with
    /// what the store lists, and a file the usage knows nothing of
    /// shows as a difference past the tolerance.
    #[tokio::test]
    async fn usage_reconciles_with_the_listed_data_files() {
        let state = crate::testing::state().await;
        let app = crate::router(state.clone());
        let meter = metering::IngestMeter::new(
            state.ingest.clone(),
            state.env.clone(),
            Arc::clone(&state.catalog),
        );
        meter.held(vec![Held {
            tenant: TenantId::DEFAULT,
            dataset: DatasetName::new("prod").unwrap(),
            kind: Kind::Log,
            bytes: 600,
            rows: 3,
        }]);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let partition = PartitionId {
            tenant: TenantId::DEFAULT,
            dataset: DatasetName::new("prod").unwrap(),
            kind: Kind::Log,
            window: Window::containing(Timestamp::UNIX_EPOCH, Window::DEFAULT_LENGTH).unwrap(),
        };
        let store = Arc::clone(state.pools.root().store());
        store
            .put(
                &layout::data_file(&partition, FileId::from_u128(1)),
                vec![0u8; 600].into(),
            )
            .await
            .unwrap();
        store
            .put(
                &layout::sidecar(&partition, FileId::from_u128(1)),
                vec![0u8; 100].into(),
            )
            .await
            .unwrap();
        let fetch = || async {
            let request = Request::builder()
                .uri("/api/v1/usage/reconcile")
                .body(Body::empty())
                .unwrap();
            let response = app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let bytes = response.into_body().collect().await.unwrap().to_bytes();
            let all: Vec<Reconciliation> = serde_json::from_slice(&bytes).unwrap();
            all.into_iter().find(|r| r.tenant == "default").unwrap()
        };
        let agreed = fetch().await;
        assert_eq!(agreed.metered_bytes, 600, "{agreed:?}");
        assert_eq!(agreed.store_bytes, 600);
        assert_eq!(agreed.index_bytes, 100, "indexes are listed apart");
        assert!(agreed.within_tolerance);

        store
            .put(
                &layout::data_file(&partition, FileId::from_u128(2)),
                vec![0u8; 500].into(),
            )
            .await
            .unwrap();
        let apart = fetch().await;
        assert_eq!(apart.store_bytes, 1_100);
        assert_eq!(apart.difference, 500);
        assert!(!apart.within_tolerance);
    }
}
