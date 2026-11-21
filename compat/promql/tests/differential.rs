// SPDX-License-Identifier: Apache-2.0

//! The `PromQL` differential suite: identical pushes, identical
//! queries, normalized answers compared. Runs against Prometheus when
//! `TESSERA_PROMETHEUS_URL` is set and reports a skip otherwise.

use tessera_compat_promql::{
    Endpoint, Normalized, difference, fixture, normalize, prometheus_from_env, serve_tessera,
};

const COUNT: i64 = 12;

/// Queries that both engines must answer alike; `RUN` becomes the
/// matcher that scopes a selector to this run's samples.
const RANGE_CASES: &[&str] = &[
    "http_requests_total{RUN}",
    "rate(http_requests_total{RUN}[1m])",
    "sum by (job) (rate(http_requests_total{RUN}[1m]))",
    r#"increase(http_requests_total{RUN, job="api"}[2m])"#,
    "irate(http_requests_total{RUN}[1m])",
    "delta(temperature{RUN}[1m])",
    "avg_over_time(temperature{RUN}[1m])",
    "max_over_time(temperature{RUN}[45s]) - min_over_time(temperature{RUN}[45s])",
    "count_over_time(temperature{RUN}[1m]) > 2",
    "sum(temperature{RUN}) / count(temperature{RUN})",
    "avg(temperature{RUN}) by (job)",
    "topk(1, temperature{RUN})",
    "temperature{RUN} > 19",
    "temperature{RUN} > bool 19",
    r#"temperature{RUN, job="api"} and on(run) temperature{RUN, job="web"}"#,
    r#"temperature{RUN} unless temperature{RUN, job="api"}"#,
    "http_requests_total{RUN} / on(instance, job) group_left temperature{RUN}",
    "temperature{RUN} < on(job) group_right http_requests_total{RUN}",
    "temperature{RUN} < bool on(job) group_right http_requests_total{RUN}",
    "histogram_quantile(0.9, rate(latency_bucket{RUN}[1m]))",
    "changes(temperature{RUN}[1m])",
    "resets(http_requests_total{RUN}[2m])",
    "deriv(temperature{RUN}[1m])",
    "predict_linear(temperature{RUN}[1m], 60)",
    r#"label_replace(temperature{RUN}, "site", "$1", "instance", "(.*)")"#,
    r#"label_join(temperature{RUN}, "where", "-", "job", "instance")"#,
    "abs(-temperature{RUN}) + 1",
    "round(temperature{RUN} / 3, 0.5)",
    "clamp(temperature{RUN}, 19, 20)",
    "up{RUN}",
    "absent(nothing{RUN})",
    "temperature{RUN} offset 30s",
    "max_over_time(rate(http_requests_total{RUN}[1m])[2m:30s])",
    "quantile(0.5, temperature{RUN})",
    "stddev(temperature{RUN})",
    r#"count_values("v", temperature{RUN})"#,
    "sort_desc(temperature{RUN})",
    "timestamp(temperature{RUN}) - time()",
    "temperature{RUN} @ end()",
    "last_over_time(up{RUN}[1m])",
    "present_over_time(up{RUN}[1m])",
    r#"temperature{RUN, instance=~"a|c"}"#,
    r#"temperature{RUN, instance!~"a"}"#,
    r#"http_requests_total{RUN, instance!=""}"#,
    // Native histograms (QL-5).
    "latency_native{RUN}",
    "rate(latency_native{RUN}[1m])",
    "increase(latency_native{RUN}[1m])",
    "delta(latency_native{RUN}[1m])",
    "irate(latency_native{RUN}[1m])",
    "idelta(latency_native{RUN}[1m])",
    "histogram_quantile(0.9, rate(latency_native{RUN}[1m]))",
    "histogram_quantile(0.5, latency_native{RUN})",
    "histogram_count(rate(latency_native{RUN}[1m]))",
    "histogram_sum(latency_native{RUN})",
    "histogram_avg(latency_native{RUN})",
    "histogram_stddev(latency_native{RUN})",
    "histogram_stdvar(latency_native{RUN})",
    "histogram_fraction(1, 4, latency_native{RUN})",
    "histogram_fraction(0, 2, latency_native{RUN})",
    "sum(rate(latency_native{RUN}[1m]))",
    "avg(latency_native{RUN})",
    "count(latency_native{RUN})",
    "histogram_quantile(0.5, sum by (run) (rate(latency_native{RUN}[1m])))",
    "latency_native{RUN} * 2",
    "latency_native{RUN} / 4",
    "-latency_native{RUN}",
    r#"latency_native{RUN, job="api"} + ignoring(job) latency_native{RUN, job="web"}"#,
    r#"latency_native{RUN, job="api"} - ignoring(job) latency_native{RUN, job="web"}"#,
    "sum_over_time(latency_native{RUN}[1m])",
    "avg_over_time(latency_native{RUN}[1m])",
    "count_over_time(latency_native{RUN}[1m])",
    "last_over_time(latency_native{RUN}[1m])",
    "changes(latency_native{RUN}[1m])",
    "resets(latency_native{RUN}[2m])",
    "histogram_quantile(0.5, latency_nhcb{RUN})",
    "histogram_fraction(0, 0.5, latency_nhcb{RUN})",
    "rate(latency_nhcb{RUN}[1m])",
    "histogram_count(latency_native{RUN}) > 10",
    "max(latency_native{RUN})",
    "abs(latency_native{RUN})",
];

/// Instant queries at the end of the data.
const INSTANT_CASES: &[&str] = &[
    "1 + 2 * 3",
    "temperature{RUN}",
    "http_requests_total{RUN}[1m]",
    "vector(1) + vector(2)",
    r#"scalar(temperature{RUN, job="api"}) * 2"#,
    "sum(rate(http_requests_total{RUN}[2m]))",
    r#""a string""#,
    "year(vector(0))",
    "hour(vector(1700000000)) + minute(vector(1700000000))",
    "latency_native{RUN}[1m]",
    r#"scalar(latency_native{RUN, job="api"})"#,
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

/// Waits until `endpoint` answers the run's gauge at `end`.
fn wait_for(endpoint: &Endpoint, run: &str, end: i64) -> Result<(), String> {
    for _ in 0..60 {
        let visible = endpoint
            .query(&format!("count(temperature{{run=\"{run}\"}})"), end)
            .is_ok_and(|data| matches!(normalize(&data), Normalized::Vector(v) if !v.is_empty()));
        if visible {
            return Ok(());
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
    Err(format!(
        "{}: the fixture never became queryable",
        endpoint.name
    ))
}

/// Records how the two answers to `case` differ, if they do; a case
/// both engines reject is reported but is no difference.
fn compare(
    case: &str,
    theirs: Result<serde_json::Value, String>,
    ours: Result<serde_json::Value, String>,
    failures: &mut Vec<String>,
) {
    match (theirs, ours) {
        (Ok(t), Ok(o)) => {
            if let Some(d) = difference(case, &normalize(&t), &normalize(&o)) {
                failures.push(d);
            }
        }
        (Err(t), Err(o)) => {
            eprintln!("{case}: both rejected it\n  prometheus: {t}\n  tessera:    {o}");
        }
        (t, o) => failures.push(format!("{case}\n  prometheus: {t:?}\n  tessera:    {o:?}")),
    }
}

#[tokio::test]
async fn tessera_agrees_with_prometheus() {
    let Some(prometheus) = prometheus_from_env() else {
        eprintln!("TESSERA_PROMETHEUS_URL is not set: the PromQL differential suite was skipped");
        return;
    };
    let tessera = serve_tessera().await;
    let run = format!("r{}", now_seconds());
    // Prometheus rejects samples far from its clock; three minutes ago is
    // within its window and old enough to be complete.
    let start = (now_seconds() - 180) / 15 * 15;
    let end = start + (COUNT - 1) * 15;
    let data = fixture(start, COUNT, &run);
    let scope = format!("run=\"{run}\"");
    let failures = tokio::task::spawn_blocking(move || {
        prometheus.push(&data);
        tessera.push(&data);
        wait_for(&prometheus, &run, end).unwrap();
        wait_for(&tessera, &run, end).unwrap();
        // Agreement on nothing would prove nothing: Prometheus must hold
        // the native histograms it was sent.
        let native = prometheus
            .query(&format!("count(latency_native{{run=\"{run}\"}})"), end)
            .unwrap();
        assert!(
            matches!(normalize(&native), Normalized::Vector(v) if !v.is_empty()),
            "prometheus ingested no native histograms: {native}"
        );
        let mut failures = Vec::new();
        for template in RANGE_CASES {
            let case = template.replace("RUN", &scope);
            compare(
                &case,
                prometheus.query_range(&case, start, end, "15s"),
                tessera.query_range(&case, start, end, "15s"),
                &mut failures,
            );
        }
        for template in INSTANT_CASES {
            let case = template.replace("RUN", &scope);
            compare(
                &case,
                prometheus.query(&case, end),
                tessera.query(&case, end),
                &mut failures,
            );
        }
        failures
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
