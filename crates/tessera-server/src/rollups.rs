// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The rollups the planner may read (section 8.7, AD-13), as the engine
//! asks for them: the enabled rollups of a tenant from its control
//! store, each with the time its output covers from its progress log.
//! Reading both for every query would cost a round trip per rollup, so
//! a tenant's answer is kept for a few seconds; a query that arrives in
//! that time may miss the newest window, and reads the raw rows for it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use tessera_catalog::{ControlStores, Table};
use tessera_eval::rule::decode_rows;
use tessera_eval::{Rollup, coverage, progress_log};
use tessera_exec::RollupSource;
use tessera_ir::rollup::RollupEntry;
use tessera_object::ObjectStore;
use tessera_types::{Env, TenantId};

/// How long a tenant's answer is kept.
pub const TTL: Duration = Duration::from_secs(5);

/// A tenant's answer and when it was read.
type Kept = (Instant, Arc<Vec<RollupEntry>>);

/// The provider.
pub struct RollupEntries {
    controls: Arc<ControlStores>,
    store: Arc<dyn ObjectStore>,
    env: Env,
    cached: Mutex<HashMap<TenantId, Kept>>,
}

impl RollupEntries {
    /// A provider over the tenants' control stores and the state logs in
    /// `store`.
    #[must_use]
    pub fn new(controls: Arc<ControlStores>, store: Arc<dyn ObjectStore>, env: Env) -> Self {
        RollupEntries {
            controls,
            store,
            env,
            cached: Mutex::new(HashMap::new()),
        }
    }

    /// Forgets what is kept, so the next query reads afresh.
    pub fn invalidate(&self) {
        self.cached
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }

    async fn read(&self, tenant: &TenantId) -> Vec<RollupEntry> {
        let control = self.controls.open(tenant);
        let rows = match control.rows(Table::Rollups).await {
            Ok(rows) => rows,
            Err(error) => {
                tracing::debug!(%tenant, error = %error, "rollups not read");
                return Vec::new();
            }
        };
        let (rollups, _) = decode_rows::<Rollup>(&rows);
        let mut entries = Vec::new();
        for rollup in rollups.into_iter().filter(|r| r.enabled) {
            let log = progress_log(
                Arc::clone(&self.store),
                tenant,
                &rollup.name,
                self.env.clone(),
            );
            let Ok(snapshot) = log.snapshot().await else {
                continue;
            };
            let Some(covered) = coverage(snapshot.state()) else {
                continue;
            };
            match rollup.entry(tenant, covered) {
                Ok(entry) => entries.push(entry),
                Err(reason) => {
                    tracing::debug!(%tenant, rollup = %rollup.name, reason, "rollup not readable");
                }
            }
        }
        entries
    }
}

impl RollupSource for RollupEntries {
    fn entries<'a>(&'a self, tenant: &'a TenantId) -> BoxFuture<'a, Vec<RollupEntry>> {
        Box::pin(async move {
            let fresh = {
                let cached = self.cached.lock().unwrap_or_else(PoisonError::into_inner);
                cached
                    .get(tenant)
                    .filter(|(at, _)| at.elapsed() < TTL)
                    .map(|(_, entries)| Arc::clone(entries))
            };
            if let Some(entries) = fresh {
                return entries.as_ref().clone();
            }
            let entries = Arc::new(self.read(tenant).await);
            self.cached
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(tenant.clone(), (Instant::now(), Arc::clone(&entries)));
            entries.as_ref().clone()
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use axum::body::Body;
    use axum::http::{Request, StatusCode, header};
    use http_body_util::BodyExt;
    use serde_json::{Value, json};
    use tessera_eval::AlertSink;
    use tessera_types::event::Scalar;
    use tessera_types::{DatasetName, EventBatch, LogRow, Timestamp};
    use tower::ServiceExt;

    use super::*;
    use crate::AppState;

    /// 2023-11-14T22:13:00Z, on a minute boundary.
    const BASE: i64 = 1_699_999_980;
    const SECOND: i64 = 1_000_000_000;

    fn at(seconds_after_base: i64) -> Timestamp {
        Timestamp::from_nanos((BASE + seconds_after_base) * SECOND)
    }

    fn errors(at: Timestamp, count: u8, service: &str) -> EventBatch {
        let rows: Vec<LogRow> = (0..count)
            .map(|i| LogRow {
                timestamp: at,
                event_id: [i.wrapping_add(u8::try_from(at.as_nanos() / SECOND % 251).unwrap()); 16],
                severity_number: Some(17),
                body: Some(format!("boom {i}")),
                attributes: BTreeMap::from([(
                    "attr.service".to_owned(),
                    Scalar::Text(service.to_owned()),
                )]),
                ..LogRow::default()
            })
            .collect();
        EventBatch::from_logs(TenantId::DEFAULT, DatasetName::new("prod").unwrap(), &rows).unwrap()
    }

    /// Rollup output written through the node's write path.
    struct WriteSink(AppState);

    impl AlertSink for WriteSink {
        fn write(&self, batch: EventBatch) -> BoxFuture<'_, Result<(), String>> {
            Box::pin(async move {
                self.0
                    .write(batch)
                    .await
                    .map_err(|e| e.message().to_owned())
            })
        }
    }

    async fn sql(state: &AppState, query: &str, rollups: bool) -> (StatusCode, Value) {
        let body = json!({ "dataset": "prod", "query": query, "rollups": rollups });
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/query")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let response = crate::router(state.clone()).oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn a_matching_query_reads_the_rollup_and_gets_the_same_answer() {
        let state = crate::testing::state().await;
        let (status, value) = sql(
            &state,
            "CREATE ROLLUP errors AS SELECT attr.service AS service, count(*) AS n \
             FROM logs WHERE severity_number >= 17 \
             GROUP BY attr.service, tumble(timestamp, INTERVAL '1 minute')",
            true,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        for batch in [
            errors(at(10), 3, "checkout"),
            errors(at(10), 1, "search"),
            errors(at(70), 2, "checkout"),
        ] {
            state.write(batch).await.unwrap();
        }
        // Two rounds of maintenance close the two windows.
        let rollup: Rollup = serde_json::from_value(
            state
                .controls
                .open(&TenantId::DEFAULT)
                .get(Table::Rollups, "errors")
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        let store = Arc::clone(state.pools.root().store());
        let sink = WriteSink(state.clone());
        for now in [at(71), at(131)] {
            let done = tessera_eval::maintain(
                &state.engine,
                Arc::clone(&store),
                &state.env,
                &TenantId::DEFAULT,
                &rollup,
                &sink,
                now,
            )
            .await
            .unwrap();
            assert_eq!(done.windows, 1);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;

        let query = "SELECT attr.service AS service, count(*) AS n FROM logs \
                     WHERE severity_number >= 17 \
                     AND timestamp >= TIMESTAMP '2023-11-14T22:13:00Z' \
                     AND timestamp < TIMESTAMP '2023-11-14T22:15:00Z' \
                     GROUP BY attr.service, tumble(timestamp, INTERVAL '2 minutes') \
                     ORDER BY service, n DESC";
        let (status, explained) = sql(&state, &format!("EXPLAIN {query}"), true).await;
        assert_eq!(status, StatusCode::OK, "{explained}");
        let plan = explained["explain"].as_str().unwrap();
        assert!(plan.contains("rollup_errors"), "{plan}");
        assert!(!plan.contains("prod/log"), "{plan}");
        let (_, raw_plan) = sql(&state, &format!("EXPLAIN {query}"), false).await;
        assert!(
            raw_plan["explain"].as_str().unwrap().contains("prod/log"),
            "{raw_plan}"
        );

        let (status, from_rollup) = sql(&state, query, true).await;
        assert_eq!(status, StatusCode::OK, "{from_rollup}");
        let (_, from_rows) = sql(&state, query, false).await;
        assert_eq!(
            from_rollup["rows"], from_rows["rows"],
            "{from_rollup} vs {from_rows}"
        );
        // The two-minute buckets start at 22:12 and 22:14, so the checkout
        // rows of 22:13 and 22:14 fall in different ones.
        assert_eq!(
            from_rows["rows"],
            json!([["checkout", 3.0], ["checkout", 2.0], ["search", 1.0]])
        );

        // A range past the progress mark reads the rows.
        let open_ended = "SELECT count(*) AS n FROM logs WHERE severity_number >= 17 \
                          GROUP BY attr.service, tumble(timestamp, INTERVAL '1 minute')";
        let (_, explained) = sql(&state, &format!("EXPLAIN {open_ended}"), true).await;
        assert!(
            explained["explain"].as_str().unwrap().contains("prod/log"),
            "{explained}"
        );
    }
}
