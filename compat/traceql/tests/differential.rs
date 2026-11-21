// SPDX-License-Identifier: Apache-2.0

//! The `TraceQL` differential suite: identical pushes, identical
//! requests, normalized answers compared. Runs against Tempo when
//! `TESSERA_TEMPO_URL` is set and reports a skip otherwise.

use std::time::Duration;

use tessera_compat_traceql::{
    Endpoint, difference, fixture, fixture_trace_ids, normalize_metrics, normalize_search,
    normalize_tag_values, normalize_tags, normalize_trace, serve_tessera, tempo_from_env,
};

/// Searches that both engines must answer alike; `RUN` becomes the
/// condition that scopes a filter to this run's spans.
const SEARCHES: &[&str] = &[
    "{ RUN }",
    "{ RUN && span.http.status_code >= 500 }",
    r#"{ RUN && resource.service.name = "api" } >> { status = error }"#,
    r#"{ RUN && name = "GET /" } > { }"#,
    r#"{ RUN && name = "db query" } < { }"#,
    r#"{ RUN && name = "cache" } << { }"#,
    r#"{ RUN && name = "db query" } ~ { }"#,
    r#"{ RUN && name = "GET /" } !> { }"#,
    r#"{ RUN && name = "GET /" } &>> { name = "cache" }"#,
    r#"{ RUN && name = "auth" } && { status = error }"#,
    r#"{ RUN && name = "auth" } || { RUN && name = "db query" }"#,
    "{ RUN } | count() > 2",
    r#"{ RUN && name = "db query" } | avg(duration) >= 30ms"#,
    "{ RUN } | by(resource.service.name)",
    r#"{ RUN && .http.method = "GET" }"#,
    "{ RUN && span.ok = true }",
    r#"{ RUN && name =~ "GET.*" }"#,
    "{ RUN && duration > 30ms && kind = server }",
    r#"{ RUN && rootName = "GET /" && name = "cache" }"#,
    "{ RUN && traceDuration > 60ms }",
    "{ RUN && span.attempt = 2 }",
    r#"{ RUN && name = "db query" } | { status = error }"#,
    "{ RUN } | by(resource.service.name) | count() > 1",
    r#"{ RUN && span.missing != "x" }"#,
    "{ RUN && span.http.status_code >= 500 } | select(span.http.method)",
    r#"{ RUN && name = "db query" } ~ { name = "db query" }"#,
];

/// Metrics queries, at a ten-second step.
const METRICS: &[&str] = &[
    "{ RUN } | count_over_time() by (resource.service.name)",
    "{ RUN && status = error } | rate()",
    "{ RUN } | max_over_time(duration) by (span.http.method)",
    "{ RUN } | avg_over_time(duration)",
    "{ RUN } | min_over_time(span.http.status_code) by (resource.service.name)",
];

/// Tags whose values both engines list alike, with the query that
/// scopes them to this run.
const TAG_VALUES: &[&str] = &[
    "resource.service.name",
    "span.http.status_code",
    ".http.method",
    "status",
    "span.ok",
    "name",
    "rootName",
];

fn now_seconds() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or_default(),
    )
    .unwrap_or_default()
}

/// Waits until `endpoint` lists the fixture's three traces.
fn wait_for(endpoint: &Endpoint, scope: &str, start: i64) -> Result<(), String> {
    for _ in 0..90 {
        let found = endpoint
            .search(&format!("{{ {scope} }}"), start, now_seconds() + 60)
            .map_or(0, |data| normalize_search(&data).len());
        if found == 3 {
            return Ok(());
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    Err(format!(
        "{}: the fixture never became searchable",
        endpoint.name
    ))
}

/// Waits until `endpoint`'s metrics count every span of the fixture:
/// Tempo's generator cuts its blocks some seconds after the push.
fn wait_for_metrics(endpoint: &Endpoint, scope: &str, start: i64) -> Result<(), String> {
    for _ in 0..90 {
        let counted: i64 = endpoint
            .metrics(
                &format!("{{ {scope} }} | count_over_time()"),
                start,
                now_seconds(),
                "10s",
            )
            .map_or(0, |data| {
                normalize_metrics(&data)
                    .iter()
                    .flat_map(|(_, samples)| samples.iter().map(|(_, v)| v / 1_000_000))
                    .sum()
            });
        if counted == 9 {
            return Ok(());
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    Err(format!(
        "{}: the metrics never counted the fixture's nine spans",
        endpoint.name
    ))
}

/// Pushes probe traces until Tempo's metrics count one: its generator
/// joins the ring some time after Tempo reports ready, and spans that
/// arrive before then never reach the metrics.
fn warm_up_metrics(tempo: &Endpoint) -> Result<(), String> {
    for attempt in 0..30u8 {
        let now = now_seconds();
        let run = 255 - attempt;
        tempo.push(&fixture(now - 5, run));
        std::thread::sleep(Duration::from_secs(3));
        let counted = tempo
            .metrics(
                &format!("{{ resource.run = \"r{run}\" }} | count_over_time()"),
                now - 20,
                now_seconds(),
                "10s",
            )
            .is_ok_and(|data| !normalize_metrics(&data).is_empty());
        if counted {
            return Ok(());
        }
    }
    Err("tempo: the metrics generator never counted a probe".into())
}

/// Every difference between the two endpoints over the suite's
/// requests, scoped to `run`'s spans between `start` and `end`.
fn compare_all(tempo: &Endpoint, tessera: &Endpoint, run: u8, start: i64, end: i64) -> Vec<String> {
    let scope = format!("resource.run = \"r{run}\"");
    let mut failures = Vec::new();
    let mut compare = |case: &str,
                       theirs: Result<serde_json::Value, String>,
                       ours: Result<serde_json::Value, String>,
                       normalize: &dyn Fn(&serde_json::Value) -> String| {
        match (theirs, ours) {
            (Ok(theirs), Ok(ours)) => {
                if let Some(d) = difference(case, &normalize(&theirs), &normalize(&ours)) {
                    failures.push(d);
                }
            }
            (Err(theirs), Err(ours)) => {
                eprintln!("{case}: both rejected it\n  tempo:   {theirs}\n  tessera: {ours}");
            }
            (theirs, ours) => failures.push(format!(
                "{case}\n  tempo:   {theirs:?}\n  tessera: {ours:?}"
            )),
        }
    };
    for template in SEARCHES {
        let case = template.replace("RUN", &scope);
        compare(
            &case,
            tempo.search(&case, start, end),
            tessera.search(&case, start, end),
            &|v| format!("{:?}", normalize_search(v)),
        );
    }
    for id in fixture_trace_ids(run) {
        let path = format!("/api/traces/{id}");
        compare(
            &path,
            tempo.get(&path, &[]).map(|v| v["batches"].clone()),
            tessera.get(&path, &[]).map(|v| v["batches"].clone()),
            &|v| format!("{:?}", normalize_trace(v)),
        );
    }
    let window = [("start", start.to_string()), ("end", end.to_string())];
    compare(
        "/api/v2/search/tags",
        tempo.get("/api/v2/search/tags", &window),
        tessera.get("/api/v2/search/tags", &window),
        &|v| format!("{:?}", normalize_tags(v)),
    );
    for tag in TAG_VALUES {
        let path = format!("/api/v2/search/tag/{tag}/values");
        let mut query = window.to_vec();
        query.push(("q", format!("{{ {scope} }}")));
        compare(
            &path,
            tempo.get(&path, &query),
            tessera.get(&path, &query),
            &|v| format!("{:?}", normalize_tag_values(v)),
        );
    }
    for template in METRICS {
        let case = template.replace("RUN", &scope);
        compare(
            &case,
            tempo.metrics(&case, start, end, "10s"),
            tessera.metrics(&case, start, end, "10s"),
            &|v| format!("{:?}", normalize_metrics(v)),
        );
    }
    failures
}

#[tokio::test]
async fn tessera_agrees_with_tempo() {
    let Some(tempo) = tempo_from_env() else {
        eprintln!("TESSERA_TEMPO_URL is not set: the TraceQL differential suite was skipped");
        return;
    };
    let tessera = serve_tessera().await;
    let tempo = tokio::task::spawn_blocking(move || {
        warm_up_metrics(&tempo).unwrap();
        tempo
    })
    .await
    .unwrap();
    // The run byte leads every id, so it is never zero; Tempo's metrics
    // only count spans received within a couple of minutes of their
    // start, so the fixture is less than a minute old.
    let run = u8::try_from(now_seconds() % 200 + 16).unwrap_or(16);
    let start_seconds = now_seconds() - 50;
    let scope = format!("resource.run = \"r{run}\"");
    let data = fixture(start_seconds, run);
    let failures = tokio::task::spawn_blocking(move || {
        tempo.push(&data);
        tessera.push(&data);
        wait_for(&tempo, &scope, start_seconds - 10).unwrap();
        wait_for(&tessera, &scope, start_seconds - 10).unwrap();
        wait_for_metrics(&tempo, &scope, start_seconds - 10).unwrap();
        compare_all(&tempo, &tessera, run, start_seconds - 10, now_seconds())
    })
    .await
    .unwrap();
    assert!(
        failures.is_empty(),
        "{} differences:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
