// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Metering (section 13.1): ingest records the bytes and events it
//! received per tenant, dataset, kind, service and team (CO-1),
//! compaction the bytes it stored per the same and per log pattern or
//! name (CO-2), and queries the bytes scanned, the time taken and the
//! requests made per principal, language, source and fingerprint
//! (CO-3). Everything is an event of `_system.usage`, read back as
//! `system.usage` (CO-4), written through the node's own write path
//! without holding anything up.

use std::collections::BTreeMap;
use std::time::Duration;

use arrow::array::{Array, RecordBatch, StringArray};
use arrow::datatypes::DataType;
use tessera_exec::ScanUse;
use tessera_ingest::IngestHandle;
use tessera_kernels::variant;
use tessera_types::event::{OVERFLOW, Scalar};
use tessera_types::{
    Attributes, DatasetName, Env, EventBatch, EventRow, Held, Kind, Meter, Random, Stored,
    TenantId, Timestamp,
};

use crate::AppState;

/// The dataset of the `_system` tenant that holds the usage.
pub const DATASET: &str = "usage";
/// The attribute the service is read from.
pub const SERVICE: &str = "resource.service.name";

/// One usage record.
#[derive(Clone, Debug, PartialEq)]
pub struct Usage {
    /// `ingest`, `stored` or `query`.
    pub phase: &'static str,
    /// The tenant the usage belongs to.
    pub tenant: TenantId,
    /// The dataset.
    pub dataset: String,
    /// The kind of event.
    pub kind: Option<Kind>,
    /// The service.
    pub service: Option<String>,
    /// The team.
    pub team: Option<String>,
    /// Bytes received, stored or scanned.
    pub bytes: u64,
    /// Events received or rows stored.
    pub events: u64,
    /// The rest, by the phase: `principal`, `language`, `source`,
    /// `fingerprint`, `cpu_ms`, `requests`, `pattern_id`, `name`.
    pub extra: BTreeMap<&'static str, Scalar>,
}

/// The rows of `_system.usage` for `usages`.
fn rows(usages: &[Usage], now: Timestamp, random: &mut impl Random) -> Vec<EventRow> {
    usages
        .iter()
        .map(|u| {
            let mut attributes: Attributes = BTreeMap::new();
            attributes.insert("attr.tenant".into(), Scalar::Text(u.tenant.to_string()));
            attributes.insert("attr.dataset".into(), Scalar::Text(u.dataset.clone()));
            if let Some(kind) = u.kind {
                attributes.insert("attr.kind".into(), Scalar::Text(kind.as_str().into()));
            }
            if let Some(service) = &u.service {
                attributes.insert("attr.service".into(), Scalar::Text(service.clone()));
            }
            if let Some(team) = &u.team {
                attributes.insert("attr.team".into(), Scalar::Text(team.clone()));
            }
            attributes.insert(
                "attr.bytes".into(),
                Scalar::Int(i64::try_from(u.bytes).unwrap_or(i64::MAX)),
            );
            attributes.insert(
                "attr.events".into(),
                Scalar::Int(i64::try_from(u.events).unwrap_or(i64::MAX)),
            );
            for (key, value) in &u.extra {
                attributes.insert(format!("attr.{key}"), value.clone());
            }
            EventRow {
                timestamp: now,
                observed_timestamp: now,
                event_id: random.next_u128().to_be_bytes(),
                trace_id: None,
                span_id: None,
                name: u.phase.into(),
                attributes,
            }
        })
        .collect()
}

/// The batch of `_system.usage` for `usages`, if any.
#[must_use]
pub fn batch_of(usages: &[Usage], now: Timestamp, random: &mut impl Random) -> Option<EventBatch> {
    let rows = rows(usages, now, random);
    let dataset = DatasetName::new(DATASET).ok()?;
    EventBatch::from_events(TenantId::SYSTEM, dataset, &rows).ok()
}

/// Writes `usages` through the node's write path, without waiting.
pub fn record(state: &AppState, usages: &[Usage]) {
    if usages.is_empty() {
        return;
    }
    let now = state.env.clock.now();
    let mut random = state.env.random.fork();
    let Some(batch) = batch_of(usages, now, &mut random) else {
        return;
    };
    let state = state.clone();
    tokio::spawn(async move {
        if let Err(error) = state.write(batch).await {
            tracing::debug!(error = %error.message(), "usage not written");
        }
    });
}

/// The attribute at `path` of every row, as text, when the rows carry
/// overflow attributes.
pub(crate) fn column(rows: &RecordBatch, path: &str) -> Option<StringArray> {
    let overflow = rows
        .column_by_name(OVERFLOW)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())?;
    variant::extract(overflow, path, &DataType::Utf8)
        .ok()
        .and_then(|a| a.as_any().downcast_ref::<StringArray>().cloned())
}

/// What a batch of writes amounts to per service and team (CO-1): the
/// service from `resource.service.name`, the team from the tenant's
/// team attribute when it has one.
#[must_use]
pub fn ingest_usage(state: &AppState, batch: &EventBatch) -> Vec<Usage> {
    let rows = batch.rows();
    let team_attribute = state
        .catalog
        .cached()
        .state()
        .tenant(batch.tenant())
        .and_then(|t| t.settings.team_attribute.clone());
    let services = column(rows, SERVICE);
    let teams = team_attribute
        .as_deref()
        .and_then(|path| column(rows, path));
    let bytes_per_row = rows.get_array_memory_size() as u64 / rows.num_rows().max(1) as u64;
    let mut groups: BTreeMap<(Option<String>, Option<String>), u64> = BTreeMap::new();
    for i in 0..rows.num_rows() {
        let cell = |array: &Option<StringArray>| {
            array
                .as_ref()
                .filter(|a| !a.is_null(i))
                .map(|a| a.value(i).to_owned())
        };
        *groups.entry((cell(&services), cell(&teams))).or_default() += 1;
    }
    groups
        .into_iter()
        .map(|((service, team), events)| Usage {
            phase: "ingest",
            tenant: batch.tenant().clone(),
            dataset: batch.dataset().to_string(),
            kind: Some(batch.kind()),
            service,
            team,
            bytes: bytes_per_row * events,
            events,
            extra: BTreeMap::new(),
        })
        .collect()
}

/// What a query cost (CO-3), over every table it scanned.
#[must_use]
pub fn query_usage(
    uses: &[ScanUse],
    language: &str,
    source: &str,
    latency: Duration,
    fingerprint: u64,
) -> Vec<Usage> {
    let Some(first) = uses.first() else {
        return Vec::new();
    };
    let mut extra: BTreeMap<&'static str, Scalar> = BTreeMap::new();
    extra.insert("principal", Scalar::Text("anonymous".into()));
    extra.insert("language", Scalar::Text(language.to_owned()));
    extra.insert("source", Scalar::Text(source.to_owned()));
    extra.insert("fingerprint", Scalar::Text(format!("{fingerprint:016x}")));
    extra.insert(
        "cpu_ms",
        Scalar::Int(i64::try_from(latency.as_millis()).unwrap_or(i64::MAX)),
    );
    extra.insert(
        "requests",
        Scalar::Int(
            i64::try_from(uses.iter().map(|u| u.requests).sum::<u64>()).unwrap_or(i64::MAX),
        ),
    );
    vec![Usage {
        phase: "query",
        tenant: first.tenant.clone(),
        dataset: uses
            .iter()
            .map(|u| u.dataset.to_string())
            .collect::<Vec<_>>()
            .join(","),
        kind: (uses.len() == 1).then_some(first.kind),
        service: None,
        team: None,
        bytes: uses.iter().map(|u| u.bytes_scanned).sum(),
        events: uses.iter().map(|u| u.rows_returned).sum(),
        extra,
    }]
}

/// What compaction stored, as usage (CO-2).
#[must_use]
pub fn stored_usage(stored: &[Stored]) -> Vec<Usage> {
    stored
        .iter()
        .map(|s| {
            let mut extra: BTreeMap<&'static str, Scalar> = BTreeMap::new();
            if let Some(pattern) = s.pattern_id {
                extra.insert("pattern_id", Scalar::Int(pattern));
            }
            if let Some(template) = &s.pattern {
                extra.insert("pattern", Scalar::Text(template.clone()));
            }
            if let Some(name) = &s.name {
                extra.insert("name", Scalar::Text(name.clone()));
            }
            Usage {
                phase: "stored",
                tenant: s.tenant.clone(),
                dataset: s.dataset.to_string(),
                kind: Some(s.kind),
                service: s.service.clone(),
                team: None,
                bytes: s.bytes,
                events: s.rows,
                extra,
            }
        })
        .collect()
}

/// The `held` usage of `held`: one record per tenant, dataset and
/// kind summing the partitions' bytes and rows, the snapshot of what
/// the object store holds that a bill reconciles with.
#[must_use]
pub fn held_usage(held: &[Held]) -> Vec<Usage> {
    let mut by_key: BTreeMap<(TenantId, String, Kind), (u64, u64)> = BTreeMap::new();
    for h in held {
        let entry = by_key
            .entry((h.tenant.clone(), h.dataset.to_string(), h.kind))
            .or_default();
        entry.0 += h.bytes;
        entry.1 += h.rows;
    }
    by_key
        .into_iter()
        .map(|((tenant, dataset, kind), (bytes, rows))| Usage {
            phase: HELD,
            tenant,
            dataset,
            kind: Some(kind),
            service: None,
            team: None,
            bytes,
            events: rows,
            extra: BTreeMap::new(),
        })
        .collect()
}

/// The phase of the usage records that say what is held.
pub const HELD: &str = "held";

/// A meter for compaction that writes through the local ingester, which
/// exists before the node's write path does.
pub struct IngestMeter {
    ingest: IngestHandle,
    env: Env,
}

impl IngestMeter {
    /// A meter writing through `ingest`.
    #[must_use]
    pub fn new(ingest: IngestHandle, env: Env) -> Self {
        IngestMeter { ingest, env }
    }
}

impl IngestMeter {
    /// Writes `usages` through the ingester without waiting.
    fn write(&self, usages: &[Usage], what: &'static str) {
        let mut random = self.env.random.fork();
        let Some(batch) = batch_of(usages, self.env.clock.now(), &mut random) else {
            return;
        };
        let ingest = self.ingest.clone();
        tokio::spawn(async move {
            if let Err(error) = ingest.write(batch).await {
                tracing::debug!(error = %error, "{what} usage not written");
            }
        });
    }
}

impl Meter for IngestMeter {
    fn stored(&self, stored: Vec<Stored>) {
        self.write(&stored_usage(&stored), "stored");
    }

    fn held(&self, held: Vec<Held>) {
        self.write(&held_usage(&held), "held");
    }
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, header};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::*;

    async fn sql(state: &AppState, query: &str) -> serde_json::Value {
        let body = serde_json::json!({ "dataset": "prod", "query": query });
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/query")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let response = crate::router(state.clone()).oneshot(request).await.unwrap();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    /// CO-4: usage is costed at the cluster's unit prices.
    #[tokio::test]
    async fn usage_is_shown_as_cost_once_prices_are_set() {
        let state = crate::testing::state().await;
        state
            .catalog
            .set_setting("price.ingest_byte", Some("2".into()))
            .await
            .unwrap();
        let app = crate::router(state.clone());
        let body = serde_json::json!({ "resourceLogs": [{ "scopeLogs": [{ "logRecords": [{
            "timeUnixNano": "1700000000000000000", "body": { "stringValue": "priced" } }] }] }] });
        let request = Request::builder()
            .method("POST")
            .uri("/v1/logs")
            .header(header::CONTENT_TYPE, "application/json")
            .header(crate::otlp::DATASET_HEADER, "prod")
            .body(Body::from(body.to_string()))
            .unwrap();
        assert!(
            app.clone()
                .oneshot(request)
                .await
                .unwrap()
                .status()
                .is_success()
        );
        let value = sql(
            &state,
            "SELECT phase, bytes, cost FROM system.usage WHERE phase = 'ingest' AND dataset = 'prod'",
        )
        .await;
        let rows = value["rows"]
            .as_array()
            .unwrap_or_else(|| panic!("{value}"));
        assert!(!rows.is_empty(), "{value}");
        let bytes = rows[0][1]
            .as_f64()
            .unwrap_or_else(|| panic!("bytes in {value}"));
        let cost = rows[0][2]
            .as_f64()
            .unwrap_or_else(|| panic!("cost in {value}"));
        assert!((cost - 2.0 * bytes).abs() < f64::EPSILON, "{value}");
    }

    #[tokio::test]
    async fn ingest_and_queries_are_metered_into_system_usage() {
        let state = crate::testing::state().await;
        let app = crate::router(state.clone());
        let record = |n: u64| {
            serde_json::json!({
                "timeUnixNano": (1_700_000_000_000_000_000u64 + n).to_string(),
                "severityText": "ERROR",
                "body": { "stringValue": format!("line {n}") }
            })
        };
        let resource = |service: &str| serde_json::json!({ "attributes": [{ "key": "service.name", "value": { "stringValue": service } }] });
        let body = serde_json::json!({
            "resourceLogs": [
                { "resource": resource("checkout"), "scopeLogs": [{ "logRecords": [record(0), record(1)] }] },
                { "resource": resource("search"), "scopeLogs": [{ "logRecords": [record(2)] }] }
            ]
        });
        let request = Request::builder()
            .method("POST")
            .uri("/v1/logs")
            .header(header::CONTENT_TYPE, "application/json")
            .header("x-tessera-dataset", "prod")
            .body(Body::from(body.to_string()))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert!(response.status().is_success());
        let answer = sql(&state, "SELECT count(*) FROM logs").await;
        assert_eq!(answer["rows"][0][0], 3.0, "{answer}");
        tokio::time::sleep(Duration::from_millis(100)).await;

        let usage = sql(
            &state,
            "SELECT phase, service, sum(events) AS events, sum(bytes) AS bytes, count(*) AS n \
             FROM system.usage GROUP BY phase, service ORDER BY phase, service",
        )
        .await;
        let rows = usage["rows"].as_array().unwrap();
        assert!(rows.len() >= 2, "{usage}");
        let ingest: Vec<&serde_json::Value> = rows.iter().filter(|r| r[0] == "ingest").collect();
        assert_eq!(ingest.len(), 2, "one per service: {usage}");
        assert!(
            ingest.iter().any(|r| r[1] == "checkout" && r[2] == 2.0),
            "{usage}"
        );
        assert!(ingest.iter().all(|r| r[3].as_f64().unwrap() > 0.0));
        let queries: Vec<&serde_json::Value> = rows.iter().filter(|r| r[0] == "query").collect();
        assert_eq!(queries.len(), 1, "{usage}");
        let detail = sql(
            &state,
            "SELECT language, fingerprint, cpu_ms FROM system.usage WHERE phase = 'query' LIMIT 1",
        )
        .await;
        assert_eq!(detail["rows"][0][0], "sql", "{detail}");
        assert_eq!(detail["rows"][0][1].as_str().unwrap().len(), 16);
    }

    #[test]
    fn held_usage_sums_the_partitions_of_a_dataset() {
        let held = |dataset: &str, bytes, rows| Held {
            tenant: TenantId::DEFAULT,
            dataset: DatasetName::new(dataset).unwrap(),
            kind: Kind::Log,
            bytes,
            rows,
        };
        let usages = held_usage(&[
            held("prod", 100, 10),
            held("prod", 50, 5),
            held("dev", 7, 1),
        ]);
        assert_eq!(usages.len(), 2);
        let prod = usages.iter().find(|u| u.dataset == "prod").unwrap();
        assert_eq!((prod.phase, prod.bytes, prod.events), (HELD, 150, 15));
        assert_eq!(prod.kind, Some(Kind::Log));
    }
}
