// SPDX-License-Identifier: Apache-2.0

//! The `LogQL` differential suite: identical pushes, identical queries,
//! normalized answers compared. Runs against Loki when
//! `TESSERA_LOKI_URL` is set and reports a skip otherwise.

use tessera_compat_logql::{difference, fixture, loki_from_env, normalize, serve_tessera};

const COUNT: i64 = 24;

/// Ten minutes ago, in whole seconds: recent enough for a Loki whose
/// queriers only ask the ingesters about the last hours, old enough for
/// every entry to be in the past.
#[allow(clippy::cast_possible_wrap)]
fn start() -> i64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default();
    ((now as i64) - 600) / 10 * 10
}

/// Queries that both engines must answer alike; `RUN` becomes the
/// matcher that scopes a query to this run's data.
const CASES: &[&str] = &[
    r#"{RUN, app="checkout"}"#,
    r#"{RUN, app=~"check.*|web", level="error"}"#,
    r#"{RUN, app!="search"} |= "status=500""#,
    r#"{RUN, app=~".+"} |~ "path=/(web|search)" != "latency=50ms""#,
    r#"{RUN, app=~".+"} | json | status >= 400"#,
    r#"{RUN, app=~".+"} | logfmt | path="/web""#,
    r#"{RUN, app=~".+"} | json | latency > 100ms | line_format "{{.path}} {{.status}}""#,
    r#"{RUN, app=~".+"} | pattern "<_> path=<path> <_>" | path="/checkout""#,
    // Double-quoted strings use Go escapes, so a regular expression is
    // written raw.
    r#"{RUN, app=~".+"} | regexp `status(=|": )(?P<code>\d+)` | code="500" | keep code"#,
    r#"{RUN, app=~".+"} | json | label_format p="{{.path}}" | drop status, latency"#,
    r#"count_over_time({RUN, app="web"}[10s])"#,
    r#"sum by (level) (rate({RUN, app=~".+"}[10s]))"#,
    r#"sum(count_over_time({RUN, app=~".+"} |= "error" [1m]))"#,
    r#"max by (app) (max_over_time({RUN, app=~".+"} | json | unwrap status [20s]))"#,
    r#"bytes_over_time({RUN, app="search"}[30s])"#,
    r#"quantile_over_time(0.5, {RUN, app=~".+"} | json | unwrap duration(latency) [20s]) by (app)"#,
];

#[tokio::test]
async fn tessera_agrees_with_loki() {
    let Some(loki) = loki_from_env() else {
        eprintln!("TESSERA_LOKI_URL is not set: the LogQL differential suite was skipped");
        return;
    };
    let tessera = serve_tessera().await;
    let run = format!(
        "r{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or_default()
    );
    let start_seconds = start();
    let data = fixture(start_seconds, COUNT, &run);
    let (loki, tessera) = tokio::task::spawn_blocking(move || {
        loki.push(&data);
        tessera.push(&data);
        (loki, tessera)
    })
    .await
    .unwrap();
    // Let both engines make the lines visible.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    // Loki aligns range queries to the step: ask at multiples of it.
    let start = (start_seconds - 10) * 1_000_000_000;
    let end = (start_seconds + COUNT + 6) * 1_000_000_000;
    let failures = tokio::task::spawn_blocking(move || {
        let mut failures = Vec::new();
        for template in CASES {
            let case = template.replace("RUN", &format!("run=\"{run}\""));
            let case = case.as_str();
            for direction in ["backward", "forward"] {
                let ours = tessera.query_range(case, start, end, "10s", direction);
                let theirs = loki.query_range(case, start, end, "10s", direction);
                match (ours, theirs) {
                    (Ok(ours), Ok(theirs)) => {
                        let (ours, theirs) = (normalize(&ours), normalize(&theirs));
                        if ours != theirs {
                            failures.push(format!(
                                "{case} ({direction}):\n{}",
                                difference(&ours, &theirs)
                            ));
                        }
                    }
                    (ours, theirs) => failures.push(format!(
                        "{case} ({direction}): {}",
                        [ours.err(), theirs.err()]
                            .into_iter()
                            .flatten()
                            .collect::<Vec<_>>()
                            .join("; ")
                    )),
                }
            }
        }
        failures
    })
    .await
    .unwrap();
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));
}
