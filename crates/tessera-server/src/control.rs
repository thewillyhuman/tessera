// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Control statements carried out against the tenant's control store
//! (section 9.5, CS-3): `CREATE ALERT`, `DROP ALERT`, `EXPLAIN ALERT`,
//! `CREATE ROLLUP`, `DROP ROLLUP`, `EXPLAIN LAYOUT`,
//! `CREATE RETENTION POLICY`, `DROP RETENTION POLICY`,
//! `EXPLAIN RETENTION`, `INSERT`, `UPDATE` and `DELETE` on the
//! `system` tables, and `DELETE` on a dataset's table (SE-5). Each
//! statement is one transaction (CS-1); the typed tables are checked
//! against their record before anything is stored.

use std::fmt::Write;
use std::sync::Arc;

use serde_json::{Map, Value};
use tessera_catalog::{AccessRole, ApiKey, ControlError, ControlStore, Row, Table, Transaction};
use tessera_eval::{Acknowledgment, AlertRule, NotificationRoute, Rollup, Silence, mode_of};
use tessera_ir::{ControlStatement, Literal};
use tessera_manifest::{DatasetLog, Tombstone};
use tessera_types::{DatasetName, Kind, Random, RetentionPolicy, TenantId};

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
        ControlStatement::CreateRollup(definition) => {
            create_rollup(&control, tenant, &definition).await
        }
        ControlStatement::DropRollup { name } => drop_rollup(&control, &name).await,
        ControlStatement::ExplainAlert { name } => {
            let document = control
                .get(Table::AlertRules, &name)
                .await
                .map_err(control_error)?
                .ok_or_else(|| ApiError::bad_request(format!("unknown alert {name}")))?;
            let rule: AlertRule = serde_json::from_value(document)
                .map_err(|e| ApiError::bad_request(format!("alert {name} does not decode: {e}")))?;
            Ok(mode_of(&rule, tenant).to_string())
        }
        ControlStatement::ExplainLayout { dataset } => {
            explain_layout(state, tenant, &dataset).await
        }
        ControlStatement::CreateRetentionPolicy(policy) => {
            create_retention_policy(&control, &policy).await
        }
        ControlStatement::DropRetentionPolicy { name } => {
            drop_retention_policy(&control, &name).await
        }
        ControlStatement::ExplainRetention { name } => {
            let document = control
                .get(Table::RetentionPolicies, &name)
                .await
                .map_err(control_error)?
                .ok_or_else(|| ApiError::bad_request(format!("unknown retention policy {name}")))?;
            let policy: RetentionPolicy = serde_json::from_value(document).map_err(|e| {
                ApiError::bad_request(format!("retention policy {name} does not decode: {e}"))
            })?;
            explain_retention(state, tenant, &policy).await
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
        ControlStatement::DeleteRows {
            dataset,
            kind,
            predicate,
            text,
        } => delete_rows(state, tenant, &dataset, kind, &predicate, &text).await,
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

/// `EXPLAIN LAYOUT` (AD-11): per kind, the layout in force, the
/// partition window, and the advisor's last report with the
/// alternatives it costed and the queries that drove it.
async fn explain_layout(
    state: &AppState,
    tenant: &TenantId,
    dataset: &DatasetName,
) -> Result<String, ApiError> {
    let log = DatasetLog::open(
        Arc::clone(state.pools.root().store()),
        tenant,
        dataset,
        state.env.clone(),
    );
    let mut out = String::new();
    let window = log.window().await.map_err(unavailable)?;
    let _ = writeln!(
        out,
        "dataset {dataset}: partition window {}s",
        window.as_secs()
    );
    for kind in Kind::ALL {
        let layout = log.layout(kind).await.map_err(unavailable)?;
        let _ = writeln!(
            out,
            "{kind}: epoch {} sort key [{}] dedicated [{}] bloom [{}] term [{}] trigram [{}]",
            layout.epoch,
            layout.sort_key.join(", "),
            layout.dedicated.join(", "),
            layout.bloom.join(", "),
            layout.term.join(", "),
            layout.trigram.join(", "),
        );
        if let Some(advice) = log.advice(kind).await.map_err(unavailable)? {
            let _ = writeln!(
                out,
                "  advisor at {}: proposes [{}], agreed {} run(s)",
                advice.at,
                advice.proposed.sort_key.join(", "),
                advice.streak
            );
            for alternative in &advice.alternatives {
                let _ = writeln!(
                    out,
                    "    [{}] cost {:.0}{}",
                    alternative.sort_key.join(", "),
                    alternative.cost,
                    if alternative.current {
                        " (current)"
                    } else {
                        ""
                    }
                );
            }
            for driver in &advice.drivers {
                let _ = writeln!(out, "    driven by {driver}");
            }
        }
    }
    Ok(out)
}

/// `CREATE RETENTION POLICY`: the policy as a row keyed by its name.
async fn create_retention_policy(
    control: &ControlStore,
    policy: &RetentionPolicy,
) -> Result<String, ApiError> {
    let mut document = serde_json::to_value(policy).map_err(|e| internal(&e))?;
    document["key"] = Value::String(policy.name.clone());
    control
        .transact(|txn| {
            if txn.get(Table::RetentionPolicies, &policy.name)?.is_some() {
                return Err(ControlError::Rejected(format!(
                    "retention policy {} exists; DROP RETENTION POLICY it first",
                    policy.name
                )));
            }
            txn.put(Table::RetentionPolicies, &policy.name, &document);
            Ok(())
        })
        .await
        .map_err(control_error)?;
    Ok(format!("CREATE RETENTION POLICY {}", policy.name))
}

/// `DROP RETENTION POLICY`.
async fn drop_retention_policy(control: &ControlStore, name: &str) -> Result<String, ApiError> {
    control
        .transact(|txn| {
            if txn.get(Table::RetentionPolicies, name)?.is_none() {
                return Err(ControlError::Rejected(format!(
                    "unknown retention policy {name}"
                )));
            }
            txn.delete(Table::RetentionPolicies, name);
            Ok(())
        })
        .await
        .map_err(control_error)?;
    Ok(format!("DROP RETENTION POLICY {name}"))
}

/// `EXPLAIN RETENTION` (CO-9): per clause and action, the partitions
/// old enough today, with their rows and bytes, and what the action
/// would remove. A clause's row conditions are not evaluated here, so
/// a clause on `kind` narrows the kinds and anything else is taken as
/// matching every row of the kinds it names. The estimates round.
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
async fn explain_retention(
    state: &AppState,
    tenant: &TenantId,
    policy: &RetentionPolicy,
) -> Result<String, ApiError> {
    let dataset =
        DatasetName::new(&policy.dataset).map_err(|e| ApiError::bad_request(e.to_string()))?;
    let store = Arc::clone(state.pools.root().store());
    let now = state.env.clock.now();
    let mut partitions: Vec<(Kind, u64, u64, u64)> = Vec::new();
    for kind in Kind::ALL {
        for partition in tessera_manifest::list_partitions(&*store, tenant, &dataset, kind)
            .await
            .map_err(unavailable)?
        {
            let manifest = tessera_manifest::Manifest::open(
                Arc::clone(&store),
                partition.clone(),
                state.env.clone(),
            );
            let snapshot = manifest.snapshot().await.map_err(unavailable)?;
            let (rows, bytes) = snapshot
                .state()
                .files()
                .fold((0, 0), |(r, b), f| (r + f.row_count, b + f.size_bytes));
            let age = now
                .duration_since(partition.window.end())
                .map_or(0, |d| d.as_secs());
            partitions.push((kind, age, rows, bytes));
        }
    }
    let mut out = String::new();
    let _ = writeln!(
        out,
        "retention policy {} on {}: {}",
        policy.name,
        policy.dataset,
        if policy.enabled {
            "enabled"
        } else {
            "disabled"
        }
    );
    for (i, clause) in policy.clauses.iter().enumerate() {
        let kinds: Vec<Kind> = Kind::ALL
            .into_iter()
            .filter(|k| {
                clause
                    .condition
                    .as_deref()
                    .is_none_or(|c| !c.contains("kind = ") || c.contains(&format!("kind = '{k}'")))
            })
            .collect();
        let _ = writeln!(
            out,
            "clause {}: {}",
            i + 1,
            clause.condition.as_deref().map_or_else(
                || "ELSE".to_owned(),
                |c| format!("WHEN {c} (row conditions estimated at partition level)")
            )
        );
        for action in &clause.actions {
            let after = action.after_seconds();
            let due: Vec<&(Kind, u64, u64, u64)> = partitions
                .iter()
                .filter(|(k, age, _, _)| kinds.contains(k) && *age >= after)
                .collect();
            let rows: u64 = due.iter().map(|p| p.2).sum();
            let bytes: u64 = due.iter().map(|p| p.3).sum();
            let (removed_rows, saved_bytes) = match action {
                tessera_types::RetentionAction::Sample { rate, .. } => (
                    (rows as f64 * (1.0 - rate)) as u64,
                    (bytes as f64 * (1.0 - rate)) as u64,
                ),
                tessera_types::RetentionAction::Drop { .. } => (rows, bytes),
                tessera_types::RetentionAction::Downsample { .. } => (0, bytes / 2),
            };
            let _ = writeln!(
                out,
                "  {action}: {} partitions due today, {rows} rows, {bytes} bytes; would remove about {removed_rows} rows and save about {saved_bytes} bytes",
                due.len()
            );
        }
    }
    Ok(out)
}

fn unavailable(error: impl std::fmt::Display) -> ApiError {
    ApiError::unavailable(error.to_string())
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
        Table::AlertRules | Table::NotificationRoutes | Table::Roles => "name",
        Table::Silences | Table::ApiKeys => "id",
        Table::Acknowledgments => "fingerprint",
        Table::Rollups | Table::RetentionPolicies | Table::LayoutPins | Table::SavedQueries => {
            "key"
        }
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
        Table::ApiKeys => {
            serde_json::from_value::<ApiKey>(document.clone()).map_err(invalid)?;
        }
        Table::Roles => {
            serde_json::from_value::<AccessRole>(document.clone()).map_err(invalid)?;
        }
        Table::Rollups | Table::RetentionPolicies | Table::LayoutPins | Table::SavedQueries => {}
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
        Table::ApiKeys => &["roles"],
        Table::Roles => &["read", "write"],
        Table::Acknowledgments | Table::NotificationRoutes => &[],
        Table::Rollups | Table::RetentionPolicies | Table::LayoutPins | Table::SavedQueries => {
            &["document"]
        }
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

/// `DELETE FROM <table> WHERE ...` on a dataset (SE-5): a tombstone in
/// the dataset log, which hides the matching rows at once and opens a
/// layout epoch so compaction rewrites the files without them.
async fn delete_rows(
    state: &AppState,
    tenant: &TenantId,
    dataset: &DatasetName,
    kind: Kind,
    predicate: &tessera_ir::Expr,
    text: &str,
) -> Result<String, ApiError> {
    let log = DatasetLog::open(
        Arc::clone(state.pools.root().store()),
        tenant,
        dataset,
        state.env.clone(),
    );
    let tombstone = Tombstone {
        id: format!("{:016x}", state.env.random.clone().next_u64()),
        predicate: serde_json::to_value(predicate).map_err(|e| internal(&e))?,
        text: text.to_owned(),
        created_at: state.env.clock.now(),
        epoch: 0,
    };
    let stored = log
        .add_tombstone(kind, tombstone)
        .await
        .map_err(|e| ApiError::unavailable(e.to_string()))?;
    Ok(format!(
        "DELETE {dataset}/{kind} WHERE {text}: tombstone {}, hidden now, removed by compaction",
        stored.id
    ))
}

/// `CREATE ROLLUP`: checked as a rollup, then stored unless the name is
/// taken.
async fn create_rollup(
    control: &ControlStore,
    tenant: &TenantId,
    definition: &tessera_ir::RollupDefinition,
) -> Result<String, ApiError> {
    let rollup = Rollup::from_definition(definition, tenant)
        .map_err(|reason| ApiError::bad_request(format!("CREATE ROLLUP: {reason}")))?;
    let document = serde_json::to_value(&rollup).map_err(|e| internal(&e))?;
    control
        .transact(|txn| {
            if txn.get(Table::Rollups, &rollup.name)?.is_some() {
                return Err(ControlError::Rejected(format!(
                    "rollup {} exists; DROP ROLLUP it first",
                    rollup.name
                )));
            }
            txn.put(Table::Rollups, &rollup.name, &document);
            Ok(())
        })
        .await
        .map_err(control_error)?;
    Ok(format!(
        "CREATE ROLLUP {} INTO {}",
        rollup.name, rollup.target
    ))
}

/// `DROP ROLLUP`.
async fn drop_rollup(control: &ControlStore, name: &str) -> Result<String, ApiError> {
    control
        .transact(|txn| {
            if txn.get(Table::Rollups, name)?.is_none() {
                return Err(ControlError::Rejected(format!("unknown rollup {name}")));
            }
            txn.delete(Table::Rollups, name);
            Ok(())
        })
        .await
        .map_err(control_error)?;
    Ok(format!("DROP ROLLUP {name}"))
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
    async fn a_delete_hides_matching_rows_at_once_and_spares_later_ones() {
        use tessera_types::{EventBatch, LogRow, Timestamp};

        let state = crate::testing::state().await;
        // Rows carry when they were observed; the delete applies to the
        // ones observed before it was issued.
        let logs = |users: &[&str], at: i64, observed: Timestamp| {
            let rows: Vec<LogRow> = users
                .iter()
                .enumerate()
                .map(|(i, user)| LogRow {
                    timestamp: Timestamp::from_nanos(
                        1_700_000_000_000_000_000 + at + i64::try_from(i).unwrap(),
                    ),
                    observed_timestamp: observed,
                    event_id: [u8::try_from(i).unwrap() + u8::try_from(at % 200).unwrap(); 16],
                    body: Some(format!("by {user}")),
                    attributes: std::collections::BTreeMap::from([(
                        "attr.user".to_owned(),
                        tessera_types::event::Scalar::Text((*user).to_owned()),
                    )]),
                    ..LogRow::default()
                })
                .collect();
            EventBatch::from_logs(TenantId::DEFAULT, DatasetName::new("prod").unwrap(), &rows)
                .unwrap()
        };
        let before = Timestamp::from_system_time(
            std::time::SystemTime::now() - std::time::Duration::from_secs(1),
        );
        state
            .write(logs(&["u1", "u2", "u1"], 0, before))
            .await
            .unwrap();
        let (status, value) = sql(&state, "DELETE FROM logs WHERE attr.user = 'u1'").await;
        assert_eq!(status, StatusCode::OK, "{value}");
        assert!(result(&value).contains("tombstone"), "{value}");
        let (_, count) = sql(&state, "SELECT count(*) FROM logs").await;
        assert_eq!(count["rows"][0][0], 1.0, "{count}");
        // Rows of the same user ingested after the delete are not affected.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let after = Timestamp::from_system_time(std::time::SystemTime::now());
        state.write(logs(&["u1"], 100, after)).await.unwrap();
        let (_, count) = sql(&state, "SELECT count(*) FROM logs").await;
        assert_eq!(count["rows"][0][0], 2.0, "{count}");
        let (status, value) = sql(&state, "DELETE FROM logs").await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    }

    #[tokio::test]
    async fn roles_are_typed_rows_of_system_roles() {
        let state = crate::testing::state().await;
        let (status, value) = sql(
            &state,
            "INSERT INTO system.roles (name, read, write, admin) \
             VALUES ('payments', '[\"prod\"]', '[]', false)",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        let (status, listed) = sql(&state, "SELECT name, read, admin FROM system.roles").await;
        assert_eq!(status, StatusCode::OK, "{listed}");
        assert_eq!(listed["rows"][0], json!(["payments", "[\"prod\"]", false]));
        let (status, value) = sql(
            &state,
            "INSERT INTO system.roles (name, admin) VALUES ('broken', 'maybe')",
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    }

    #[tokio::test]
    async fn create_list_and_drop_a_rollup() {
        let state = crate::testing::state().await;
        let (status, value) = sql(
            &state,
            "CREATE ROLLUP errors EVERY 5m AS SELECT attr.service AS service, count(*) AS n \
             FROM logs WHERE severity_number >= 17 \
             GROUP BY attr.service, tumble(timestamp, INTERVAL '1 minute')",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        assert_eq!(result(&value), "CREATE ROLLUP errors INTO rollup_errors");
        let (status, listed) = sql(
            &state,
            "SELECT name, target, window_seconds, every_seconds, origin, enabled \
             FROM system.rollups",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{listed}");
        assert_eq!(
            listed["rows"][0],
            json!(["errors", "rollup_errors", 60, 300, "user", true])
        );

        let (status, value) = sql(
            &state,
            "CREATE ROLLUP wrong AS SELECT count(*) AS n FROM logs \
             GROUP BY tumble(timestamp, INTERVAL '1m') HAVING count(*) > 1",
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert!(
            value["error"].as_str().unwrap().contains("HAVING"),
            "{value}"
        );

        let (status, value) = sql(&state, "DROP ROLLUP errors").await;
        assert_eq!(status, StatusCode::OK, "{value}");
        let (status, value) = sql(&state, "DROP ROLLUP errors").await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
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

        let (status, value) = sql(&state, "EXPLAIN LAYOUT prod").await;
        assert_eq!(status, StatusCode::OK, "{value}");
        let text = result(&value);
        assert!(
            text.starts_with("dataset prod: partition window 86400s"),
            "{text}"
        );
        assert!(
            text.contains(
                "log: epoch 0 sort key [resource.service.name, severity_number, timestamp]"
            ),
            "{text}"
        );
        assert!(text.contains("term [body]"));

        let (status, value) = sql(&state, "DROP ALERT errors").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(result(&value), "DROP ALERT errors");
        let (status, _) = sql(&state, "DROP ALERT errors").await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "gone");
    }

    #[tokio::test]
    async fn retention_policies_are_created_explained_and_dropped() {
        let state = crate::testing::state().await;
        let (status, value) = sql(
            &state,
            "CREATE RETENTION POLICY trim ON prod AS \
             WHEN kind = 'log' AND severity_number < 13 THEN SAMPLE 0.1 AFTER 3d, DROP AFTER 30d \
             ELSE DROP AFTER 90d",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        assert_eq!(result(&value), "CREATE RETENTION POLICY trim");
        let (status, _) = sql(
            &state,
            "CREATE RETENTION POLICY trim ON prod AS ELSE DROP AFTER 1d",
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "exists");
        let (status, value) = sql(&state, "EXPLAIN RETENTION trim").await;
        assert_eq!(status, StatusCode::OK, "{value}");
        let text = result(&value);
        assert!(
            text.starts_with("retention policy trim on prod: enabled"),
            "{text}"
        );
        assert!(
            text.contains("clause 1: WHEN kind = 'log' AND severity_number < 13"),
            "{text}"
        );
        assert!(
            text.contains("SAMPLE 0.1 AFTER 259200s: 0 partitions due today"),
            "{text}"
        );
        assert!(text.contains("clause 2: ELSE"), "{text}");
        let (_, value) = sql(&state, "SELECT key FROM system.retention_policies").await;
        assert_eq!(value["rows"][0][0], "trim");
        let (_, value) = sql(
            &state,
            "UPDATE system.retention_policies SET enabled = false WHERE key = 'trim'",
        )
        .await;
        assert_eq!(result(&value), "UPDATE 1");
        let (_, value) = sql(&state, "EXPLAIN RETENTION trim").await;
        assert!(result(&value).contains(": disabled"));
        let (status, value) = sql(&state, "DROP RETENTION POLICY trim").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(result(&value), "DROP RETENTION POLICY trim");
        let (status, _) = sql(&state, "EXPLAIN RETENTION trim").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
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
