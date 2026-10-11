// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Rules and alerts as Prometheus and Loki clients expect them (CQ-3):
//! `GET /api/v1/rules` and `GET /api/v1/alerts` in Prometheus' shape,
//! and the ruler API of both, `POST`, `GET` and `DELETE` under
//! `/api/v1/rules/{namespace}` for `PromQL` rule groups and
//! `/loki/api/v1/rules/{namespace}` for `LogQL` ones, which take a rule
//! file as written for Prometheus or Loki. A loaded group replaces the
//! group of the same name in the namespace; its rules notify the route
//! named `default`, which an operator points at an Alertmanager or any
//! other sink.

use std::collections::BTreeMap;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use tessera_catalog::{ControlError, Table};
use tessera_eval::rule::decode_rows;
use tessera_eval::{AlertRule, Instance, Language, Status, state};
use tessera_types::{TenantId, Timestamp};

use crate::AppState;
use crate::error::ApiError;
use crate::params::{Need, parse_step, scope};

/// The route rules loaded from files notify.
pub const DEFAULT_ROUTE: &str = "default";

/// The routes.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/v1/rules", get(prometheus_rules))
        .route("/api/v1/alerts", get(prometheus_alerts))
        .route(
            "/api/v1/rules/{namespace}",
            get(prometheus_namespace).post(load_promql),
        )
        .route(
            "/api/v1/rules/{namespace}/{group}",
            axum::routing::delete(delete_promql),
        )
        .route("/loki/api/v1/rules", get(loki_rules))
        .route(
            "/loki/api/v1/rules/{namespace}",
            get(loki_namespace).post(load_logql),
        )
        .route(
            "/loki/api/v1/rules/{namespace}/{group}",
            axum::routing::delete(delete_logql),
        )
        .with_state(state)
}

/// A rule group as written in a Prometheus or Loki rule file.
#[derive(Debug, Deserialize, Serialize)]
struct GroupFile {
    name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    interval: Option<String>,
    rules: Vec<RuleFile>,
}

/// One rule of a rule file.
#[derive(Debug, Deserialize, Serialize)]
struct RuleFile {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    alert: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    record: Option<String>,
    expr: String,
    #[serde(default, rename = "for", skip_serializing_if = "Option::is_none")]
    hold: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    labels: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    annotations: BTreeMap<String, String>,
}

/// The rule group name stored for a namespace and a group: both made
/// key-safe, joined by a slash.
fn group_name(namespace: &str, group: &str) -> String {
    let safe = |text: &str| -> String {
        text.chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                    c
                } else {
                    '_'
                }
            })
            .collect()
    };
    format!("{}/{}", safe(namespace), safe(group))
}

/// Loads a rule file into the control store, replacing the group.
async fn load(
    state: &AppState,
    headers: &HeaderMap,
    namespace: &str,
    body: &[u8],
    language: Language,
) -> Result<Response, ApiError> {
    let scope = scope(state, headers, Need::Admin).await?;
    let file: GroupFile = serde_yaml_ng::from_slice(body)
        .map_err(|e| ApiError::bad_request(format!("invalid rule group: {e}")))?;
    let group = group_name(namespace, &file.name);
    let every = match &file.interval {
        Some(interval) => parse_step(interval)?.as_secs(),
        None => 60,
    };
    let mut rules = Vec::new();
    for (position, rule) in file.rules.iter().enumerate() {
        if rule.record.is_some() {
            return Err(ApiError::bad_request(
                "recording rules are rollups, which arrive with M9; only alerting rules are accepted",
            ));
        }
        let name = rule
            .alert
            .clone()
            .ok_or_else(|| ApiError::bad_request("a rule needs `alert` or `record`"))?;
        check_expression(language, &rule.expr)?;
        let hold = match &rule.hold {
            Some(hold) => parse_step(hold)?.as_secs(),
            None => 0,
        };
        let mut labels = rule.labels.clone();
        let severity = labels
            .remove("severity")
            .unwrap_or_else(|| "warning".to_owned());
        rules.push(AlertRule {
            name: format!("{}/{name}", file.name),
            group: group.clone(),
            language,
            expr: rule.expr.clone(),
            dataset: scope.dataset.to_string(),
            every_seconds: every,
            for_seconds: hold,
            severity,
            labels,
            annotations: rule.annotations.clone(),
            notify: vec![DEFAULT_ROUTE.to_owned()],
            position: position as u64,
        });
    }
    let control = state.controls.open(&scope.tenant);
    control
        .transact(|txn| {
            for row in txn.rows(Table::AlertRules)? {
                if row.value.get("group").and_then(Value::as_str) == Some(&group) {
                    txn.delete(Table::AlertRules, &row.key);
                }
            }
            for rule in &rules {
                let document = serde_json::to_value(rule)
                    .map_err(|e| ControlError::Rejected(e.to_string()))?;
                txn.put(Table::AlertRules, &rule.name, &document);
            }
            Ok(())
        })
        .await
        .map_err(|e| ApiError::unavailable(e.to_string()))?;
    crate::audit::record(state, &scope.principal, &scope.tenant, "rules.load", &group);
    Ok(StatusCode::ACCEPTED.into_response())
}

/// Checks that an expression parses in its language before it is stored.
fn check_expression(language: Language, expr: &str) -> Result<(), ApiError> {
    let problem = match language {
        Language::Promql => tessera_promql::parse(expr).err().map(|d| d.describe(expr)),
        Language::Logql => tessera_logql::parse(expr).err().map(|d| d.describe(expr)),
        Language::Sql => tessera_sql::parser::parse(expr)
            .err()
            .map(|d| d.describe(expr)),
    };
    match problem {
        Some(problem) => Err(ApiError::bad_request(format!("invalid expr: {problem}"))),
        None => Ok(()),
    }
}

/// Removes a group from the control store.
async fn remove(
    state: &AppState,
    headers: &HeaderMap,
    namespace: &str,
    group: &str,
) -> Result<Response, ApiError> {
    let scope = scope(state, headers, Need::Admin).await?;
    let group = group_name(namespace, group);
    let control = state.controls.open(&scope.tenant);
    let (removed, _) = control
        .transact(|txn| {
            let mut removed = 0;
            for row in txn.rows(Table::AlertRules)? {
                if row.value.get("group").and_then(Value::as_str) == Some(&group) {
                    txn.delete(Table::AlertRules, &row.key);
                    removed += 1;
                }
            }
            Ok(removed)
        })
        .await
        .map_err(|e| ApiError::unavailable(e.to_string()))?;
    if removed == 0 {
        return Err(ApiError::not_found(format!("no rule group {group}")));
    }
    crate::audit::record(
        state,
        &scope.principal,
        &scope.tenant,
        "rules.remove",
        &group,
    );
    Ok(StatusCode::ACCEPTED.into_response())
}

/// A rule group with what its state log says.
struct Group {
    name: String,
    rules: Vec<AlertRule>,
    instances: Vec<Instance>,
    evaluated: BTreeMap<String, Timestamp>,
}

/// Every group of `tenant` with its instances and evaluation times.
async fn groups_of(state: &AppState, tenant: &TenantId) -> Result<Vec<Group>, ApiError> {
    let control = state.controls.open(tenant);
    let rows = control
        .rows(Table::AlertRules)
        .await
        .map_err(|e| ApiError::unavailable(e.to_string()))?;
    let (rules, _) = decode_rows::<AlertRule>(&rows);
    let mut by_group: BTreeMap<String, Vec<AlertRule>> = BTreeMap::new();
    for rule in rules {
        by_group.entry(rule.group.clone()).or_default().push(rule);
    }
    let store = Arc::clone(state.pools.root().store());
    let mut groups = Vec::new();
    for (name, mut rules) in by_group {
        rules.sort_by(|a, b| (a.position, &a.name).cmp(&(b.position, &b.name)));
        let log = state::open(Arc::clone(&store), tenant, &name, 0, state.env.clone());
        let snapshot = log
            .snapshot()
            .await
            .map_err(|e| ApiError::unavailable(e.to_string()))?;
        groups.push(Group {
            name,
            rules,
            instances: snapshot.state().instances.values().cloned().collect(),
            evaluated: snapshot.state().evaluated.clone(),
        });
    }
    Ok(groups)
}

fn prometheus_state(status: Status) -> &'static str {
    match status {
        Status::Pending => "pending",
        Status::Firing => "firing",
        Status::Inactive | Status::Resolved => "inactive",
    }
}

/// An alert as Prometheus lists it.
fn alert_json(instance: &Instance, annotations: &BTreeMap<String, String>) -> Value {
    json!({
        "labels": instance.labels,
        "annotations": annotations,
        "state": prometheus_state(instance.status),
        "activeAt": instance.since.to_string(),
        "value": instance.value.to_string(),
    })
}

/// A rule as Prometheus lists it, with its active alerts.
fn rule_json(group: &Group, rule: &AlertRule) -> Value {
    let active: Vec<&Instance> = group
        .instances
        .iter()
        .filter(|i| i.rule == rule.name && matches!(i.status, Status::Pending | Status::Firing))
        .collect();
    let state = if active.iter().any(|i| i.status == Status::Firing) {
        "firing"
    } else if active.is_empty() {
        "inactive"
    } else {
        "pending"
    };
    let mut labels = rule.labels.clone();
    labels.insert("severity".into(), rule.severity.clone());
    json!({
        "state": state,
        "name": rule.name.rsplit('/').next().unwrap_or(&rule.name),
        "query": rule.expr,
        "duration": rule.for_seconds,
        "keepFiringFor": 0,
        "labels": labels,
        "annotations": rule.annotations,
        "alerts": active.iter().map(|i| alert_json(i, &rule.annotations)).collect::<Vec<_>>(),
        "health": "ok",
        "evaluationTime": 0.0,
        "lastEvaluation": group.evaluated.get(&rule.name).map_or_else(String::new, ToString::to_string),
        "type": "alerting",
    })
}

fn group_json(group: &Group) -> Value {
    let (namespace, name) = group.name.split_once('/').unwrap_or(("", &group.name));
    json!({
        "name": name,
        "file": namespace,
        "rules": group.rules.iter().map(|r| rule_json(group, r)).collect::<Vec<_>>(),
        "interval": group.rules.first().map_or(60, |r| r.every_seconds),
        "limit": 0,
        "evaluationTime": 0.0,
        "lastEvaluation": group.evaluated.values().max().map_or_else(String::new, ToString::to_string),
    })
}

fn success(data: &Value) -> Response {
    axum::Json(json!({ "status": "success", "data": data })).into_response()
}

async fn prometheus_rules(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let answer = async {
        let scope = scope(&state, &headers, Need::Read).await?;
        let groups = groups_of(&state, &scope.tenant).await?;
        Ok::<_, ApiError>(success(&json!({
            "groups": groups.iter().map(group_json).collect::<Vec<_>>()
        })))
    };
    answer.await.unwrap_or_else(IntoResponse::into_response)
}

async fn prometheus_alerts(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let answer = async {
        let scope = scope(&state, &headers, Need::Read).await?;
        let groups = groups_of(&state, &scope.tenant).await?;
        let mut alerts = Vec::new();
        for group in &groups {
            for rule in &group.rules {
                for instance in group.instances.iter().filter(|i| {
                    i.rule == rule.name && matches!(i.status, Status::Pending | Status::Firing)
                }) {
                    alerts.push(alert_json(instance, &rule.annotations));
                }
            }
        }
        Ok::<_, ApiError>(success(&json!({ "alerts": alerts })))
    };
    answer.await.unwrap_or_else(IntoResponse::into_response)
}

/// The groups of a namespace as rule files, keyed by namespace.
async fn files_of(
    state: &AppState,
    headers: &HeaderMap,
    namespace: Option<&str>,
    language: Language,
) -> Result<BTreeMap<String, Vec<GroupFile>>, ApiError> {
    let scope = scope(state, headers, Need::Read).await?;
    let groups = groups_of(state, &scope.tenant).await?;
    let mut files: BTreeMap<String, Vec<GroupFile>> = BTreeMap::new();
    for group in groups {
        let (ns, name) = group.name.split_once('/').unwrap_or(("", &group.name));
        if namespace.is_some_and(|wanted| wanted != ns) {
            continue;
        }
        let rules: Vec<RuleFile> = group
            .rules
            .iter()
            .filter(|r| r.language == language)
            .map(|r| {
                let mut labels = r.labels.clone();
                labels.insert("severity".into(), r.severity.clone());
                RuleFile {
                    alert: Some(r.name.rsplit('/').next().unwrap_or(&r.name).to_owned()),
                    record: None,
                    expr: r.expr.clone(),
                    hold: (r.for_seconds > 0).then(|| format!("{}s", r.for_seconds)),
                    labels,
                    annotations: r.annotations.clone(),
                }
            })
            .collect();
        if rules.is_empty() {
            continue;
        }
        files.entry(ns.to_owned()).or_default().push(GroupFile {
            name: name.to_owned(),
            interval: Some(format!("{}s", group.rules[0].every_seconds)),
            rules,
        });
    }
    Ok(files)
}

fn yaml(files: &BTreeMap<String, Vec<GroupFile>>) -> Response {
    match serde_yaml_ng::to_string(files) {
        Ok(text) => (StatusCode::OK, [("content-type", "application/yaml")], text).into_response(),
        Err(e) => ApiError::unavailable(e.to_string()).into_response(),
    }
}

async fn prometheus_namespace(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(namespace): Path<String>,
) -> Response {
    match files_of(&state, &headers, Some(&namespace), Language::Promql).await {
        Ok(files) => yaml(&files),
        Err(e) => e.into_response(),
    }
}

async fn load_promql(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(namespace): Path<String>,
    body: Bytes,
) -> Response {
    load(&state, &headers, &namespace, &body, Language::Promql)
        .await
        .unwrap_or_else(IntoResponse::into_response)
}

async fn delete_promql(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((namespace, group)): Path<(String, String)>,
) -> Response {
    remove(&state, &headers, &namespace, &group)
        .await
        .unwrap_or_else(IntoResponse::into_response)
}

async fn loki_rules(State(state): State<AppState>, headers: HeaderMap) -> Response {
    match files_of(&state, &headers, None, Language::Logql).await {
        Ok(files) => yaml(&files),
        Err(e) => e.into_response(),
    }
}

async fn loki_namespace(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(namespace): Path<String>,
) -> Response {
    match files_of(&state, &headers, Some(&namespace), Language::Logql).await {
        Ok(files) => yaml(&files),
        Err(e) => e.into_response(),
    }
}

async fn load_logql(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(namespace): Path<String>,
    body: Bytes,
) -> Response {
    load(&state, &headers, &namespace, &body, Language::Logql)
        .await
        .unwrap_or_else(IntoResponse::into_response)
}

async fn delete_logql(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((namespace, group)): Path<(String, String)>,
) -> Response {
    remove(&state, &headers, &namespace, &group)
        .await
        .unwrap_or_else(IntoResponse::into_response)
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, header};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::*;

    async fn call(app: &Router, request: Request<Body>) -> (StatusCode, String) {
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8_lossy(&bytes).into_owned())
    }

    fn post(uri: &str, body: &str) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(uri)
            .header(header::CONTENT_TYPE, "application/yaml")
            .body(Body::from(body.to_owned()))
            .unwrap()
    }

    fn get(uri: &str) -> Request<Body> {
        Request::builder().uri(uri).body(Body::empty()).unwrap()
    }

    const PROMETHEUS_FILE: &str = "name: availability\ninterval: 30s\nrules:\n  - alert: InstanceDown\n    expr: up == 0\n    for: 5m\n    labels:\n      severity: critical\n      team: sre\n    annotations:\n      summary: '{{ $labels.instance }} is down'\n  - alert: HighLoad\n    expr: node_load1 > 10\n";

    #[tokio::test]
    async fn a_prometheus_rule_file_is_loaded_listed_and_deleted() {
        let app = crate::router(crate::testing::state().await);
        let (status, text) = call(&app, post("/api/v1/rules/prod", PROMETHEUS_FILE)).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{text}");

        let (status, text) = call(&app, get("/api/v1/rules")).await;
        assert_eq!(status, StatusCode::OK);
        let value: Value = serde_json::from_str(&text).unwrap();
        let groups = value["data"]["groups"].as_array().unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0]["name"], "availability");
        assert_eq!(groups[0]["file"], "prod");
        assert_eq!(groups[0]["interval"], 30);
        let rules = groups[0]["rules"].as_array().unwrap();
        assert_eq!(rules.len(), 2);
        assert_eq!(rules[0]["name"], "InstanceDown");
        assert_eq!(rules[0]["duration"], 300);
        assert_eq!(rules[0]["labels"]["severity"], "critical");
        assert_eq!(rules[0]["labels"]["team"], "sre");
        assert_eq!(rules[0]["state"], "inactive");
        assert_eq!(rules[0]["type"], "alerting");
        assert_eq!(rules[1]["labels"]["severity"], "warning");

        let (status, text) = call(&app, get("/api/v1/alerts")).await;
        assert_eq!(status, StatusCode::OK);
        let value: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["data"]["alerts"], json!([]));

        let (status, text) = call(&app, get("/api/v1/rules/prod")).await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            text.contains("InstanceDown") && text.contains("for: 300s"),
            "{text}"
        );

        let replaced = "name: availability\nrules:\n  - alert: Only\n    expr: up == 0\n";
        call(&app, post("/api/v1/rules/prod", replaced)).await;
        let (_, text) = call(&app, get("/api/v1/rules")).await;
        let value: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(
            value["data"]["groups"][0]["rules"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        let (status, _) = call(
            &app,
            Request::builder()
                .method("DELETE")
                .uri("/api/v1/rules/prod/availability")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let (_, text) = call(&app, get("/api/v1/rules")).await;
        let value: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["data"]["groups"], json!([]));
    }

    #[tokio::test]
    async fn bad_rule_files_are_refused() {
        let app = crate::router(crate::testing::state().await);
        let syntax = "name: g\nrules:\n  - alert: Bad\n    expr: rate(up[5m\n";
        let (status, text) = call(&app, post("/api/v1/rules/ns", syntax)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{text}");
        let recording = "name: g\nrules:\n  - record: job:up\n    expr: sum(up)\n";
        let (status, text) = call(&app, post("/api/v1/rules/ns", recording)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(text.contains("rollups"));
        let (status, _) = call(&app, post("/api/v1/rules/ns", "not: [yaml")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn a_loki_rule_file_is_loaded_and_listed_as_yaml() {
        let app = crate::router(crate::testing::state().await);
        let file = "name: errors\ninterval: 1m\nrules:\n  - alert: TooManyErrors\n    expr: sum(rate({app=\"api\"} |= \"error\" [5m])) > 10\n    for: 2m\n";
        let (status, text) = call(&app, post("/loki/api/v1/rules/team-a", file)).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{text}");
        let (status, text) = call(&app, get("/loki/api/v1/rules")).await;
        assert_eq!(status, StatusCode::OK);
        assert!(text.starts_with("team-a:"), "{text}");
        assert!(text.contains("TooManyErrors"));
        let (_, text) = call(&app, get("/api/v1/rules/team-a")).await;
        assert_eq!(text.trim(), "{}", "not a PromQL group");
    }
}
