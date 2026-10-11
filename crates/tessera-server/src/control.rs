// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Control statements carried out against the tenant's control store
//! (section 9.5, CS-3): `CREATE ALERT`, `DROP ALERT`, `EXPLAIN ALERT`
//! and `INSERT`, `UPDATE` and `DELETE` on the `system` tables. Each
//! statement is one transaction (CS-1); the typed tables are checked
//! against their record before anything is stored.

use serde_json::{Map, Value};
use tessera_catalog::{ControlError, ControlStore, Row, Table, Transaction};
use tessera_eval::{Acknowledgment, AlertRule, NotificationRoute, Silence, mode_of};
use tessera_ir::{ControlStatement, Literal};
use tessera_types::TenantId;

use crate::AppState;
use crate::error::ApiError;

/// The read-only view of alert instances (CQ-13), named like a table.
pub const ALERT_INSTANCES: &str = "alert_instances";

/// Carries `statement` out for `tenant` and says what it did.
///
/// # Errors
///
/// A bad request for an unknown table or alert, an invalid record or a
/// read-only table; service unavailable when the control log fails.
pub async fn execute(
    state: &AppState,
    tenant: &TenantId,
    statement: ControlStatement,
) -> Result<String, ApiError> {
    let control = state.controls.open(tenant);
    match statement {
        ControlStatement::CreateAlert(definition) => {
            let rule = AlertRule::from_definition(&definition);
            let document = serde_json::to_value(&rule).map_err(|e| internal(&e))?;
            control
                .transact(|txn| {
                    if txn.get(Table::AlertRules, &rule.name)?.is_some() {
                        return Err(ControlError::Rejected(format!(
                            "alert {} exists; DROP ALERT it first",
                            rule.name
                        )));
                    }
                    txn.put(Table::AlertRules, &rule.name, &document);
                    Ok(())
                })
                .await
                .map_err(control_error)?;
            Ok(format!("CREATE ALERT {}", rule.name))
        }
        ControlStatement::DropAlert { name } => {
            control
                .transact(|txn| {
                    if txn.get(Table::AlertRules, &name)?.is_none() {
                        return Err(ControlError::Rejected(format!("unknown alert {name}")));
                    }
                    txn.delete(Table::AlertRules, &name);
                    Ok(())
                })
                .await
                .map_err(control_error)?;
            Ok(format!("DROP ALERT {name}"))
        }
        ControlStatement::ExplainAlert { name } => {
            let document = control
                .get(Table::AlertRules, &name)
                .await
                .map_err(control_error)?
                .ok_or_else(|| ApiError::bad_request(format!("unknown alert {name}")))?;
            let rule: AlertRule = serde_json::from_value(document)
                .map_err(|e| ApiError::bad_request(format!("alert {name} does not decode: {e}")))?;
            Ok(mode_of(&rule).to_string())
        }
        ControlStatement::Insert { table, rows } => {
            insert(&control, writable(&table)?, &rows).await
        }
        ControlStatement::Update { table, set, filter } => {
            update(&control, writable(&table)?, &set, &filter).await
        }
        ControlStatement::Delete { table, filter } => {
            delete(&control, writable(&table)?, &filter).await
        }
    }
}

/// `INSERT`: every row is checked and keyed before the transaction, which
/// refuses a key in use.
async fn insert(
    control: &ControlStore,
    table: Table,
    rows: &[Vec<(String, Literal)>],
) -> Result<String, ApiError> {
    let documents: Vec<(String, Value)> = rows
        .iter()
        .map(|row| {
            let document = Value::Object(object(table, row)?);
            validate(table, &document)?;
            Ok((key_of(table, &document)?, document))
        })
        .collect::<Result<_, ApiError>>()?;
    control
        .transact(|txn| {
            for (key, document) in &documents {
                if txn.get(table, key)?.is_some() {
                    return Err(ControlError::Rejected(format!(
                        "{table} already has a row with key {key}"
                    )));
                }
                txn.put(table, key, document);
            }
            Ok(())
        })
        .await
        .map_err(control_error)?;
    Ok(format!("INSERT {}", documents.len()))
}

/// `UPDATE`: the matching rows with the assignments merged in, checked,
/// and re-keyed when the key column changed.
async fn update(
    control: &ControlStore,
    table: Table,
    set: &[(String, Literal)],
    filter: &[(String, Literal)],
) -> Result<String, ApiError> {
    let changes = object(table, set)?;
    let (count, _) = control
        .transact(|txn| {
            let mut count = 0;
            for row in matching(txn, table, filter)? {
                let mut document = row.value.clone();
                if let Value::Object(fields) = &mut document {
                    fields.extend(changes.clone());
                }
                let rejected = |e: ApiError| ControlError::Rejected(e.message().to_owned());
                validate(table, &document).map_err(rejected)?;
                let key = key_of(table, &document).map_err(rejected)?;
                if key != row.key {
                    txn.delete(table, &row.key);
                }
                txn.put(table, &key, &document);
                count += 1;
            }
            Ok(count)
        })
        .await
        .map_err(control_error)?;
    Ok(format!("UPDATE {count}"))
}

/// `DELETE`: the matching rows go.
async fn delete(
    control: &ControlStore,
    table: Table,
    filter: &[(String, Literal)],
) -> Result<String, ApiError> {
    let (count, _) = control
        .transact(|txn| {
            let rows = matching(txn, table, filter)?;
            for row in &rows {
                txn.delete(table, &row.key);
            }
            Ok(rows.len())
        })
        .await
        .map_err(control_error)?;
    Ok(format!("DELETE {count}"))
}

/// The table a statement may write.
fn writable(name: &str) -> Result<Table, ApiError> {
    if name == ALERT_INSTANCES {
        return Err(ApiError::bad_request(
            "system.alert_instances is a view over the rule state logs and is read-only",
        ));
    }
    name.parse::<Table>()
        .map_err(|e| ApiError::bad_request(e.to_string()))
}

/// The column that keys each table's rows.
fn key_column(table: Table) -> &'static str {
    match table {
        Table::AlertRules | Table::NotificationRoutes => "name",
        Table::Silences => "id",
        Table::Acknowledgments => "fingerprint",
        Table::Rollups
        | Table::RetentionPolicies
        | Table::LayoutPins
        | Table::ApiKeys
        | Table::Roles
        | Table::SavedQueries => "key",
    }
}

fn key_of(table: Table, document: &Value) -> Result<String, ApiError> {
    let column = key_column(table);
    document
        .get(column)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| ApiError::bad_request(format!("{table} rows need a text {column}")))
}

/// Checks a document against the table's record, for the typed tables.
fn validate(table: Table, document: &Value) -> Result<(), ApiError> {
    let invalid = |e: serde_json::Error| ApiError::bad_request(format!("invalid {table} row: {e}"));
    match table {
        Table::AlertRules => {
            serde_json::from_value::<AlertRule>(document.clone()).map_err(invalid)?;
        }
        Table::Silences => {
            serde_json::from_value::<Silence>(document.clone()).map_err(invalid)?;
        }
        Table::Acknowledgments => {
            serde_json::from_value::<Acknowledgment>(document.clone()).map_err(invalid)?;
        }
        Table::NotificationRoutes => {
            serde_json::from_value::<NotificationRoute>(document.clone()).map_err(invalid)?;
        }
        Table::Rollups
        | Table::RetentionPolicies
        | Table::LayoutPins
        | Table::ApiKeys
        | Table::Roles
        | Table::SavedQueries => {}
    }
    Ok(())
}

/// The rows of `table` whose columns equal every literal of `filter`.
fn matching(
    txn: &Transaction<'_>,
    table: Table,
    filter: &[(String, Literal)],
) -> Result<Vec<Row>, ControlError> {
    Ok(txn
        .rows(table)?
        .into_iter()
        .filter(|row| {
            filter
                .iter()
                .all(|(column, literal)| row.value.get(column) == Some(&json_of(literal)))
        })
        .collect())
}

/// The columns whose values are JSON documents rather than text, so a
/// text literal in them is parsed.
fn json_columns(table: Table) -> &'static [&'static str] {
    match table {
        Table::AlertRules => &["labels", "annotations", "notify"],
        Table::Silences => &["matchers"],
        Table::Acknowledgments | Table::NotificationRoutes => &[],
        Table::Rollups
        | Table::RetentionPolicies
        | Table::LayoutPins
        | Table::ApiKeys
        | Table::Roles
        | Table::SavedQueries => &["document"],
    }
}

/// The document a row's literals make, JSON columns parsed.
fn object(table: Table, pairs: &[(String, Literal)]) -> Result<Map<String, Value>, ApiError> {
    pairs
        .iter()
        .map(|(column, literal)| {
            let value = match literal {
                Literal::Text(text) if json_columns(table).contains(&column.as_str()) => {
                    serde_json::from_str(text).map_err(|e| {
                        ApiError::bad_request(format!("{table}.{column} must be JSON: {e}"))
                    })?
                }
                other => json_of(other),
            };
            Ok((column.clone(), value))
        })
        .collect()
}

/// A literal as it is stored: timestamps as nanoseconds, durations as
/// seconds, as the records expect.
fn json_of(literal: &Literal) -> Value {
    match literal {
        Literal::Null => Value::Null,
        Literal::Bool(b) => Value::Bool(*b),
        Literal::Int(i) => Value::from(*i),
        Literal::Float(f) => Value::from(*f),
        Literal::Text(t) => Value::String(t.clone()),
        Literal::Timestamp(t) => Value::from(t.as_nanos()),
        Literal::Duration(d) => Value::from(d.as_secs()),
        Literal::Bytes(b) => Value::String(b.iter().fold(String::new(), |mut hex, x| {
            use std::fmt::Write;
            let _ = write!(hex, "{x:02x}");
            hex
        })),
    }
}

fn control_error(error: ControlError) -> ApiError {
    match error {
        ControlError::Rejected(reason) => ApiError::bad_request(reason),
        other => ApiError::unavailable(other.to_string()),
    }
}

fn internal(error: &serde_json::Error) -> ApiError {
    ApiError::unavailable(format!("cannot encode the record: {error}"))
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, StatusCode, header};
    use http_body_util::BodyExt;
    use serde_json::json;
    use tower::ServiceExt;

    use super::*;

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

    fn result(value: &Value) -> &str {
        value["rows"][0][0].as_str().unwrap()
    }

    #[tokio::test]
    async fn create_explain_and_drop_an_alert() {
        let state = crate::testing::state().await;
        let (status, value) = sql(
            &state,
            "CREATE ALERT errors SEVERITY critical FOR 2m EVERY 30s NOTIFY ops \
             LABELS (team = 'payments') AS SELECT count(*) AS n FROM logs HAVING count(*) > 10",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        assert_eq!(result(&value), "CREATE ALERT errors");
        let stored = state
            .controls
            .open(&TenantId::DEFAULT)
            .get(Table::AlertRules, "errors")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored["for_seconds"], 120);
        assert_eq!(stored["every_seconds"], 30);
        assert_eq!(stored["severity"], "critical");
        assert_eq!(stored["dataset"], "prod");
        assert_eq!(stored["notify"], json!(["ops"]));
        assert_eq!(stored["labels"]["team"], "payments");
        assert_eq!(
            stored["expr"],
            "SELECT count(*) AS n FROM logs HAVING count(*) > 10"
        );

        let (status, value) = sql(&state, "CREATE ALERT errors AS SELECT 1").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(value["error"].as_str().unwrap().contains("exists"));

        let (_, value) = sql(&state, "EXPLAIN ALERT errors").await;
        assert!(result(&value).starts_with("periodic:"), "{value}");

        let (status, value) = sql(&state, "DROP ALERT errors").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(result(&value), "DROP ALERT errors");
        let (status, _) = sql(&state, "DROP ALERT errors").await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "gone");
    }

    #[tokio::test]
    async fn rows_are_inserted_updated_and_deleted_with_validation() {
        let state = crate::testing::state().await;
        let (status, value) = sql(
            &state,
            "INSERT INTO system.silences (id, matchers, starts_at, ends_at, comment) \
             VALUES ('s1', 'nope', 0, 10, 'x')",
        )
        .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "matchers must be JSON: {value}"
        );
        let (status, value) = sql(
            &state,
            "INSERT INTO system.silences (id, matchers, starts_at, ends_at, comment) \
             VALUES ('s1', '{\"service\": \"x\"}', 0, 10, 'x')",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");

        let (status, value) = sql(
            &state,
            "INSERT INTO system.notification_routes (name, kind, url) \
             VALUES ('ops', 'webhook', 'http://ops'), ('page', 'pagerduty', 'http://pd')",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        assert_eq!(result(&value), "INSERT 2");
        let (status, _) = sql(
            &state,
            "INSERT INTO system.notification_routes (name, kind, url) VALUES ('ops', 'slack', 'x')",
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "duplicate key");

        let (_, value) = sql(
            &state,
            "UPDATE system.notification_routes SET repeat_seconds = 60 WHERE kind = 'webhook'",
        )
        .await;
        assert_eq!(result(&value), "UPDATE 1");
        let control = state.controls.open(&TenantId::DEFAULT);
        let ops = control
            .get(Table::NotificationRoutes, "ops")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ops["repeat_seconds"], 60);

        let (_, value) = sql(
            &state,
            "DELETE FROM system.notification_routes WHERE name = 'page'",
        )
        .await;
        assert_eq!(result(&value), "DELETE 1");
        assert_eq!(
            control.rows(Table::NotificationRoutes).await.unwrap().len(),
            1
        );

        let (status, value) = sql(&state, "DELETE FROM system.alert_instances").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(value["error"].as_str().unwrap().contains("read-only"));
        let (status, _) = sql(&state, "DELETE FROM system.nope").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
}
