// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The `LogQL` differential suite: identical pushes, identical queries,
//! normalized answers compared. Runs against Loki when
//! `TESSERA_LOKI_URL` is set and reports a skip otherwise.

use tessera_compat_logql::{fixture, loki_from_env, normalize, serve_tessera};

const START: i64 = 1_700_000_000;
const COUNT: i64 = 24;

/// Queries that both engines must answer alike.
const CASES: &[&str] = &[
    r#"{app="checkout"}"#,
    r#"{app=~"check.*|web", level="error"}"#,
    r#"{app!="search"} |= "status=500""#,
    r#"{app=~".+"} |~ "path=/(web|search)" != "latency=50ms""#,
    r#"{app=~".+"} | json | status >= 400"#,
    r#"{app=~".+"} | logfmt | path="/web""#,
    r#"{app=~".+"} | json | latency > 100ms | line_format "{{.path}} {{.status}}""#,
    r#"{app=~".+"} | pattern "<_> path=<path> <_>" | path="/checkout""#,
    r#"{app=~".+"} | regexp "status(=|\": )(?P<code>\d+)" | code="500" | keep code"#,
    r#"{app=~".+"} | json | label_format p="{{.path}}" | drop status, latency"#,
    r#"count_over_time({app="web"}[10s])"#,
    r#"sum by (level) (rate({app=~".+"}[10s]))"#,
    r#"sum(count_over_time({app=~".+"} |= "error" [1m]))"#,
    r#"max by (app) (max_over_time({app=~".+"} | json | unwrap status [20s]))"#,
    r#"bytes_over_time({app="search"}[30s])"#,
    r#"quantile_over_time(0.5, {app=~".+"} | json | unwrap duration(latency) [20s]) by (app)"#,
];

#[tokio::test]
async fn tessera_agrees_with_loki() {
    let Some(loki) = loki_from_env() else {
        eprintln!("TESSERA_LOKI_URL is not set: the LogQL differential suite was skipped");
        return;
    };
    let tessera = serve_tessera().await;
    let data = fixture(START, COUNT);
    let (loki, tessera) = tokio::task::spawn_blocking(move || {
        loki.push(&data);
        tessera.push(&data);
        (loki, tessera)
    })
    .await
    .unwrap();
    // Let both engines make the lines visible.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let start = (START - 5) * 1_000_000_000;
    let end = (START + COUNT + 5) * 1_000_000_000;
    let failures = tokio::task::spawn_blocking(move || {
        let mut failures = Vec::new();
        for case in CASES {
            for direction in ["backward", "forward"] {
                let ours = normalize(&tessera.query_range(case, start, end, "10s", direction));
                let theirs = normalize(&loki.query_range(case, start, end, "10s", direction));
                if ours != theirs {
                    failures.push(format!(
                        "{case} ({direction}):\n  tessera: {ours:?}\n  loki:    {theirs:?}"
                    ));
                }
            }
        }
        failures
    })
    .await
    .unwrap();
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));
}
