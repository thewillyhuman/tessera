// SPDX-License-Identifier: Apache-2.0

//! The SQL differential suite (QL-4 for SQL, section 18.5): every case is
//! one question in Tessera's dialect and in `SQLite`'s. Rows are ordered by
//! every case so that results compare position by position.

use tessera_compat_sql::{Fixture, generate};

/// A question in both dialects.
struct Case {
    name: &'static str,
    tessera: &'static str,
    sqlite: &'static str,
}

const CASES: &[Case] = &[
    Case {
        name: "filter and order",
        tessera: "SELECT body, severity_number FROM logs WHERE severity_number >= 13 ORDER BY timestamp, body",
        sqlite: "SELECT body, severity_number FROM logs WHERE severity_number >= 13 ORDER BY timestamp, body",
    },
    Case {
        name: "count by severity",
        tessera: "SELECT severity_text, count_raw(*) AS n FROM logs GROUP BY severity_text ORDER BY severity_text",
        sqlite: "SELECT severity_text, count(*) AS n FROM logs GROUP BY severity_text ORDER BY severity_text",
    },
    Case {
        name: "global aggregates",
        tessera: "SELECT count_raw(*), sum_raw(severity_number), min(severity_number), max(body), avg_raw(severity_number), count_raw(body) FROM logs",
        sqlite: "SELECT count(*), sum(severity_number), min(severity_number), max(body), avg(severity_number), count(body) FROM logs",
    },
    Case {
        name: "attributes",
        tessera: "SELECT timestamp, attr.n::int FROM logs WHERE attr.host = 'h1' AND attr.n > 5 ORDER BY timestamp",
        sqlite: "SELECT timestamp, n FROM logs WHERE host = 'h1' AND n > 5 ORDER BY timestamp",
    },
    Case {
        // A bare attribute is text until the advisor promotes it (M8).
        name: "bare attributes are text",
        tessera: "SELECT attr.n, attr.host FROM logs ORDER BY timestamp LIMIT 25",
        sqlite: "SELECT CAST(n AS TEXT), host FROM logs ORDER BY timestamp LIMIT 25",
    },
    Case {
        name: "group by attribute with nulls",
        tessera: "SELECT attr.host AS host, count_raw(*) AS n, sum_raw(attr.n) FROM logs GROUP BY attr.host ORDER BY host",
        sqlite: "SELECT host, count(*) AS n, sum(n) FROM logs GROUP BY host ORDER BY host",
    },
    Case {
        name: "time buckets",
        tessera: "SELECT time_bucket(INTERVAL '1 hour', timestamp) AS b, count_raw(*) FROM logs GROUP BY time_bucket(INTERVAL '1 hour', timestamp) ORDER BY b",
        sqlite: "SELECT (timestamp / 3600000000000) * 3600000000000 AS b, count(*) FROM logs GROUP BY b ORDER BY b",
    },
    Case {
        name: "like in between and null tests",
        tessera: "SELECT timestamp FROM logs WHERE (body LIKE '%7%' OR severity_text IN ('WARN', 'ERROR')) AND severity_number BETWEEN 10 AND 20 AND body IS NOT NULL ORDER BY timestamp",
        sqlite: "SELECT timestamp FROM logs WHERE (body LIKE '%7%' OR severity_text IN ('WARN', 'ERROR')) AND severity_number BETWEEN 10 AND 20 AND body IS NOT NULL ORDER BY timestamp",
    },
    Case {
        name: "missing bodies",
        tessera: "SELECT count_raw(*) FROM logs WHERE body IS NULL",
        sqlite: "SELECT count(*) FROM logs WHERE body IS NULL",
    },
    Case {
        name: "newest first with offset",
        tessera: "SELECT timestamp, body FROM logs ORDER BY timestamp DESC LIMIT 7 OFFSET 3",
        sqlite: "SELECT timestamp, body FROM logs ORDER BY timestamp DESC LIMIT 7 OFFSET 3",
    },
    Case {
        name: "arithmetic and aliases",
        tessera: "SELECT severity_number * 2 + 1 AS x, attr.n - 1 AS y FROM logs ORDER BY timestamp LIMIT 20",
        sqlite: "SELECT severity_number * 2 + 1 AS x, n - 1 AS y FROM logs ORDER BY timestamp LIMIT 20",
    },
    Case {
        name: "time range in the where clause",
        tessera: "SELECT count_raw(*) FROM logs WHERE timestamp >= '1970-01-02' AND timestamp < '1970-01-02T12:00:00Z'",
        sqlite: "SELECT count(*) FROM logs WHERE timestamp >= 86400000000000 AND timestamp < 86400000000000 + 12 * 3600000000000",
    },
    Case {
        name: "not and inequality",
        tessera: "SELECT body FROM logs WHERE NOT (severity_text = 'INFO') AND attr.n <> 3 ORDER BY timestamp, body",
        sqlite: "SELECT body FROM logs WHERE NOT (severity_text = 'INFO') AND n <> 3 ORDER BY timestamp, body",
    },
    Case {
        name: "text functions",
        tessera: "SELECT lower(severity_text), length(body) FROM logs WHERE body IS NOT NULL ORDER BY timestamp LIMIT 15",
        sqlite: "SELECT lower(severity_text), length(body) FROM logs WHERE body IS NOT NULL ORDER BY timestamp LIMIT 15",
    },
    Case {
        name: "coalesce",
        tessera: "SELECT coalesce(body, '-') FROM logs ORDER BY timestamp LIMIT 30",
        sqlite: "SELECT coalesce(body, '-') FROM logs ORDER BY timestamp LIMIT 30",
    },
    Case {
        name: "max timestamp per host",
        tessera: "SELECT attr.host AS host, max(timestamp), min(timestamp) FROM logs WHERE attr.host IS NOT NULL GROUP BY attr.host ORDER BY host",
        sqlite: "SELECT host, max(timestamp), min(timestamp) FROM logs WHERE host IS NOT NULL GROUP BY host ORDER BY host",
    },
];

#[tokio::test]
async fn tessera_agrees_with_sqlite() {
    let mut failures = Vec::new();
    for seed in [1u64, 2, 3] {
        let fixture = Fixture::load(&generate(seed, 300)).await;
        for case in CASES {
            if let Some(difference) = fixture.differ(case.tessera, case.sqlite).await {
                failures.push(format!("seed {seed}, {}: {difference}", case.name));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn an_empty_table_agrees_too() {
    let fixture = Fixture::load(&[]).await;
    for case in CASES {
        if let Some(difference) = fixture.differ(case.tessera, case.sqlite).await {
            panic!("{}: {difference}", case.name);
        }
    }
}
