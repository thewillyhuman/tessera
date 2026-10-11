// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The rows of the `system` tables (CS-3): the control tables from the
//! tenant's control store, and `alert_instances` as a view over the rule
//! state logs of the tenant's rule groups (CQ-13). Documents become
//! rows by the table's schema: text columns take strings as they are
//! and anything else as JSON text, numbers and timestamps their
//! numbers.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, SchemaRef};
use futures::future::BoxFuture;
use serde_json::Value;
use tessera_catalog::{ControlStores, Table};
use tessera_eval::rule::decode_rows;
use tessera_eval::{AlertRule, state};
use tessera_exec::SystemTables;
use tessera_ir::system_table_schema;
use tessera_object::ObjectStore;
use tessera_types::{Env, TenantId};

/// The provider over a node's control stores and root pool.
pub struct SystemTableRows {
    controls: Arc<ControlStores>,
    store: Arc<dyn ObjectStore>,
    env: Env,
}

impl SystemTableRows {
    /// Rows from `controls` and the state logs in `store`.
    #[must_use]
    pub fn new(controls: Arc<ControlStores>, store: Arc<dyn ObjectStore>, env: Env) -> Self {
        SystemTableRows {
            controls,
            store,
            env,
        }
    }

    async fn documents(&self, tenant: &TenantId, table: &str) -> Result<Vec<Value>, String> {
        let control = self.controls.open(tenant);
        if table == crate::control::ALERT_INSTANCES {
            let rows = control
                .rows(Table::AlertRules)
                .await
                .map_err(|e| e.to_string())?;
            let (rules, _) = decode_rows::<AlertRule>(&rows);
            let mut groups: Vec<String> = rules.iter().map(|r| r.group.clone()).collect();
            groups.sort();
            groups.dedup();
            let mut documents = Vec::new();
            for group in groups {
                let log = state::open(Arc::clone(&self.store), tenant, &group, 0, self.env.clone());
                let snapshot = log.snapshot().await.map_err(|e| e.to_string())?;
                for instance in snapshot.state().instances.values() {
                    let mut document = serde_json::to_value(instance).map_err(|e| e.to_string())?;
                    document["group"] = Value::String(group.clone());
                    documents.push(document);
                }
            }
            return Ok(documents);
        }
        let table: Table = table
            .parse()
            .map_err(|e: tessera_catalog::UnknownTable| e.to_string())?;
        let rows = control.rows(table).await.map_err(|e| e.to_string())?;
        Ok(rows
            .into_iter()
            .map(|row| match row.value {
                Value::Object(_) if typed(table) => row.value,
                other => serde_json::json!({ "key": row.key, "document": other }),
            })
            .collect())
    }
}

/// Whether a table's rows are its record's fields rather than a key and
/// a document.
fn typed(table: Table) -> bool {
    matches!(
        table,
        Table::AlertRules
            | Table::Rollups
            | Table::Silences
            | Table::Acknowledgments
            | Table::Inhibitions
            | Table::NotificationRoutes
            | Table::ApiKeys
            | Table::Roles
    )
}

impl SystemTables for SystemTableRows {
    fn rows<'a>(
        &'a self,
        tenant: &'a TenantId,
        table: &'a str,
    ) -> BoxFuture<'a, Result<Vec<RecordBatch>, String>> {
        Box::pin(async move {
            let schema = system_table_schema(table)
                .ok_or_else(|| format!("unknown system table {table}"))?;
            let documents = self.documents(tenant, table).await?;
            if documents.is_empty() {
                return Ok(Vec::new());
            }
            Ok(vec![batch_of(&schema, &documents)?])
        })
    }
}

/// `documents` as one batch of `schema`.
///
/// # Errors
///
/// If a column cannot be built.
pub fn batch_of(schema: &SchemaRef, documents: &[Value]) -> Result<RecordBatch, String> {
    let columns: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| {
            let values = documents.iter().map(|d| d.get(field.name()));
            let array: ArrayRef = match field.data_type() {
                DataType::Int64 => Arc::new(Int64Array::from(
                    values
                        .map(|v| v.and_then(Value::as_i64))
                        .collect::<Vec<_>>(),
                )),
                DataType::Float64 => Arc::new(Float64Array::from(
                    values
                        .map(|v| v.and_then(Value::as_f64))
                        .collect::<Vec<_>>(),
                )),
                DataType::Boolean => Arc::new(BooleanArray::from(
                    values
                        .map(|v| v.and_then(Value::as_bool))
                        .collect::<Vec<_>>(),
                )),
                DataType::Timestamp(_, _) => Arc::new(TimestampNanosecondArray::from(
                    values
                        .map(|v| v.and_then(Value::as_i64))
                        .collect::<Vec<_>>(),
                )),
                _ => Arc::new(StringArray::from(
                    values
                        .map(|v| match v {
                            None | Some(Value::Null) => None,
                            Some(Value::String(s)) => Some(s.clone()),
                            Some(other) => Some(other.to_string()),
                        })
                        .collect::<Vec<_>>(),
                )),
            };
            array
        })
        .collect();
    RecordBatch::try_new(Arc::clone(schema), columns).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use axum::body::Body;
    use axum::http::{Request, StatusCode, header};
    use http_body_util::BodyExt;
    use serde_json::json;
    use tessera_types::event::Scalar;
    use tessera_types::{DatasetName, EventBatch, EventRow, Timestamp};
    use tower::ServiceExt;

    use crate::AppState;

    async fn sql(state: &AppState, query: &str) -> (StatusCode, Value) {
        let body = json!({ "dataset": "prod", "query": query });
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

    use super::*;

    /// CS-5: a control table joins with telemetry, here acknowledgments
    /// with the alert events they acknowledge.
    #[tokio::test]
    async fn control_tables_join_with_telemetry() {
        let state = crate::testing::state().await;
        let alert = |name: &str, fingerprint: &str, i: u8| EventRow {
            timestamp: Timestamp::from_nanos(1_700_000_000_000_000_000 + i64::from(i)),
            observed_timestamp: Timestamp::from_nanos(1_700_000_000_000_000_000),
            event_id: [i; 16],
            trace_id: None,
            span_id: None,
            name: name.into(),
            attributes: BTreeMap::from([(
                "attr.alert.fingerprint".to_owned(),
                Scalar::Text(fingerprint.into()),
            )]),
        };
        let alerts = EventBatch::from_alerts(
            TenantId::DEFAULT,
            DatasetName::new("prod").unwrap(),
            &[alert("errors", "fp1", 1), alert("latency", "fp2", 2)],
        )
        .unwrap();
        state.write(alerts).await.unwrap();
        let (status, value) = sql(
            &state,
            "INSERT INTO system.acknowledgments (fingerprint, until, by) \
             VALUES ('fp1', TIMESTAMP '2024-01-02T00:00:00Z', 'ann')",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        let (status, value) = sql(
            &state,
            "SELECT a.name, k.by FROM alerts a JOIN system.acknowledgments k \
             ON a.attr.alert.fingerprint = k.fingerprint",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        assert_eq!(value["columns"][0]["name"], "name");
        assert_eq!(value["rows"], json!([["errors", "ann"]]));
        let (status, value) = sql(
            &state,
            "SELECT a.name, k.by FROM alerts a LEFT JOIN system.acknowledgments k \
             ON a.attr.alert.fingerprint = k.fingerprint ORDER BY a.name",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        assert_eq!(value["rows"], json!([["errors", "ann"], ["latency", null]]));
    }

    #[tokio::test]
    async fn system_tables_are_queried_like_any_other() {
        let state = crate::testing::state().await;
        let (status, value) = sql(&state, "SELECT count(*) FROM system.alert_rules").await;
        assert_eq!(status, StatusCode::OK, "{value}");
        assert_eq!(value["rows"][0][0], 0.0);
        sql(
            &state,
            "CREATE ALERT errors SEVERITY critical EVERY 30s LABELS (team = 'pay') \
             AS SELECT count(*) AS n FROM logs HAVING count(*) > 10",
        )
        .await;
        sql(
            &state,
            "INSERT INTO system.silences (id, matchers, starts_at, ends_at) \
             VALUES ('s1', '{}', TIMESTAMP '2024-01-01T00:00:00Z', TIMESTAMP '2024-01-02T00:00:00Z')",
        )
        .await;
        let (status, value) = sql(
            &state,
            "SELECT name, severity, every_seconds, labels FROM system.alert_rules \
             WHERE severity = 'critical'",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        assert_eq!(value["rows"][0][0], "errors");
        assert_eq!(value["rows"][0][2], 30);
        assert_eq!(value["rows"][0][3], "{\"team\":\"pay\"}");
        let (status, value) = sql(&state, "SELECT id, ends_at FROM system.silences").await;
        assert_eq!(status, StatusCode::OK, "{value}");
        assert_eq!(value["rows"][0][0], "s1");
        assert_eq!(value["rows"][0][1], "2024-01-02T00:00:00Z");
        let (status, value) = sql(
            &state,
            "INSERT INTO system.inhibitions (id, source_matchers, target_matchers, equal) \
             VALUES ('db', '{\"alertname\":\"db_down\"}', '{\"severity\":\"warning\"}', '[\"instance\"]')",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        let (status, value) = sql(&state, "SELECT id, equal FROM system.inhibitions").await;
        assert_eq!(status, StatusCode::OK, "{value}");
        assert_eq!(value["rows"][0][0], "db");
        assert_eq!(value["rows"][0][1], "[\"instance\"]");
        let (status, value) = sql(&state, "SELECT count(*) FROM system.alert_instances").await;
        assert_eq!(status, StatusCode::OK, "{value}");
        assert_eq!(value["rows"][0][0], 0.0, "nothing evaluated yet");
    }
}
