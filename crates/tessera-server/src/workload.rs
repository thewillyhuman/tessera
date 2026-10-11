// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The workload log (AD-1): every query appends one event per scanned
//! table to `_system.queries`, with the columns it referenced, the
//! predicates and their observed selectivity, how it grouped and
//! ordered, what the scan cost, its latency, language and source. The
//! advisor reads the log back weighted by age (AD-2). Writes go through
//! the node's own write path and never hold a response up.

use std::collections::BTreeMap;
use std::time::Duration;

use tessera_exec::{Analysis, ScanUse};
use tessera_ir::Plan;
use tessera_types::event::Scalar;
use tessera_types::{Attributes, DatasetName, EventBatch, EventRow, Random, TenantId, Timestamp};

use crate::AppState;

/// The dataset of the `_system` tenant that holds the log.
pub const DATASET: &str = "queries";

/// Records what `plan` asked of its scans, as `analysis` saw it.
pub fn record(
    state: &AppState,
    plan: &Plan,
    analysis: &Analysis,
    language: &str,
    source: &str,
    latency: Duration,
) {
    record_uses(
        state,
        tessera_exec::workload::uses(plan, analysis),
        language,
        source,
        latency,
        tessera_exec::cache::fingerprint(plan),
    );
}

/// Records `uses`, one event each, and the query's usage (CO-3), except
/// for the `_system` tenant, whose own queries are not logged.
pub fn record_uses(
    state: &AppState,
    uses: Vec<ScanUse>,
    language: &str,
    source: &str,
    latency: Duration,
    fingerprint: u64,
) {
    let now = state.env.clock.now();
    let metered: Vec<ScanUse> = uses
        .iter()
        .filter(|u| u.tenant != TenantId::SYSTEM)
        .cloned()
        .collect();
    crate::metering::record(
        state,
        &crate::metering::query_usage(&metered, language, source, latency, fingerprint),
    );
    let mut scanned: BTreeMap<&TenantId, u64> = BTreeMap::new();
    for scan in &metered {
        *scanned.entry(&scan.tenant).or_default() += scan.bytes_scanned;
    }
    for (tenant, bytes) in scanned {
        crate::budgets::charge_query(state, tenant, bytes);
    }
    let mut random = state.env.random.clone();
    let rows: Vec<EventRow> = uses
        .into_iter()
        .filter(|u| u.tenant != TenantId::SYSTEM)
        .map(|u| event(&u, language, source, latency, now, &mut random))
        .collect();
    if rows.is_empty() {
        return;
    }
    let Ok(dataset) = DatasetName::new(DATASET) else {
        return;
    };
    let Ok(batch) = EventBatch::from_events(TenantId::SYSTEM, dataset, &rows) else {
        return;
    };
    let state = state.clone();
    tokio::spawn(async move {
        if let Err(error) = state.write(batch).await {
            tracing::debug!(error = %error.message(), "workload event not written");
        }
    });
}

fn text(value: impl Into<String>) -> Scalar {
    Scalar::Text(value.into())
}

fn int(value: u64) -> Scalar {
    Scalar::Int(i64::try_from(value).unwrap_or(i64::MAX))
}

fn json<T: serde::Serialize>(value: &T) -> Scalar {
    Scalar::Text(serde_json::to_string(value).unwrap_or_default())
}

/// The event of one scan use.
fn event(
    scan: &ScanUse,
    language: &str,
    source: &str,
    latency: Duration,
    now: Timestamp,
    random: &mut tessera_types::SharedRandom,
) -> EventRow {
    let mut attributes: Attributes = BTreeMap::new();
    attributes.insert("attr.tenant".into(), text(scan.tenant.as_str()));
    attributes.insert("attr.dataset".into(), text(scan.dataset.as_str()));
    attributes.insert("attr.kind".into(), text(scan.kind.as_str()));
    attributes.insert("attr.language".into(), text(language));
    attributes.insert("attr.source".into(), text(source));
    attributes.insert("attr.principal".into(), text("anonymous"));
    attributes.insert("attr.columns".into(), json(&scan.columns));
    attributes.insert("attr.predicates".into(), json(&scan.predicates));
    attributes.insert("attr.group_by".into(), json(&scan.group_by));
    attributes.insert("attr.order_by".into(), json(&scan.order_by));
    attributes.insert("attr.files_considered".into(), int(scan.files_considered));
    attributes.insert("attr.files_pruned".into(), int(scan.files_pruned));
    attributes.insert("attr.bytes_scanned".into(), int(scan.bytes_scanned));
    attributes.insert("attr.rows_scanned".into(), int(scan.rows_scanned));
    attributes.insert("attr.rows_returned".into(), int(scan.rows_returned));
    if let Some(selectivity) = scan.selectivity {
        attributes.insert("attr.selectivity".into(), Scalar::Float(selectivity));
    }
    attributes.insert(
        "attr.latency_ms".into(),
        int(u64::try_from(latency.as_millis()).unwrap_or(u64::MAX)),
    );
    EventRow {
        timestamp: now,
        observed_timestamp: now,
        event_id: random.next_u128().to_be_bytes(),
        trace_id: None,
        span_id: None,
        name: "query".into(),
        attributes,
    }
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, header};
    use tessera_exec::query;
    use tessera_ir::BindContext;
    use tessera_sql::SqlFrontend;
    use tower::ServiceExt;

    use super::*;

    #[tokio::test]
    async fn every_query_leaves_an_event_in_the_system_tenant() {
        let state = crate::testing::state().await;
        let app = crate::router(state.clone());
        for _ in 0..2 {
            let body = serde_json::json!({
                "dataset": "prod",
                "query": "SELECT attr.host AS host, count(*) FROM logs WHERE severity_number >= 17 GROUP BY attr.host"
            });
            let request = Request::builder()
                .method("POST")
                .uri("/api/v1/query")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))
                .unwrap();
            let response = app.clone().oneshot(request).await.unwrap();
            assert!(response.status().is_success());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        let ctx = BindContext {
            tenant: TenantId::SYSTEM,
            dataset: DatasetName::new(DATASET).unwrap(),
            now: state.env.clock.now(),
        };
        let response = query::run(
            &state.engine,
            &SqlFrontend,
            "SELECT attr.dataset, attr.language, attr.predicates, attr.group_by, count(*) AS n \
             FROM events GROUP BY attr.dataset, attr.language, attr.predicates, attr.group_by",
            &ctx,
        )
        .await
        .unwrap();
        let tessera_exec::Response::Rows(output) = response else {
            panic!("rows");
        };
        let (_, rows) = crate::encode::rows_json(&output.schema, &output.batches).unwrap();
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0][0], "prod");
        assert_eq!(rows[0][1], "sql");
        assert!(rows[0][2].as_str().unwrap().contains("severity_number"));
        assert!(rows[0][3].as_str().unwrap().contains("attr.host"));
        assert_eq!(rows[0][4], 2.0);
        assert!(
            output.uses.iter().all(|u| u.tenant == TenantId::SYSTEM),
            "the log's own query is not logged"
        );
    }
}
