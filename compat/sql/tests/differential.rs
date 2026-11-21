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
        // The table joined with itself, one attribute read as text on
        // one side and as a number on the other.
        name: "join",
        tessera: "SELECT a.timestamp, b.timestamp, a.attr.n FROM logs a JOIN logs b ON a.body = b.body WHERE a.severity_number = 17 AND b.attr.n > 15 ORDER BY a.timestamp, b.timestamp",
        sqlite: "SELECT a.timestamp, b.timestamp, CAST(a.n AS TEXT) FROM logs a JOIN logs b ON a.body = b.body WHERE a.severity_number = 17 AND b.n > 15 ORDER BY a.timestamp, b.timestamp",
    },
    Case {
        // The third table's keys meet both of the tables before it, and
        // each table's attributes are read through the joins over it.
        name: "join of three tables",
        tessera: "SELECT a.timestamp, b.timestamp, c.timestamp, a.attr.n, c.attr.host FROM logs a JOIN logs b ON a.body = b.body JOIN logs c ON c.body = b.body AND c.severity_number = a.severity_number WHERE a.severity_number = 17 AND b.attr.n > 15 AND c.attr.host = 'h1' ORDER BY a.timestamp, b.timestamp, c.timestamp",
        sqlite: "SELECT a.timestamp, b.timestamp, c.timestamp, CAST(a.n AS TEXT), c.host FROM logs a JOIN logs b ON a.body = b.body JOIN logs c ON c.body = b.body AND c.severity_number = a.severity_number WHERE a.severity_number = 17 AND b.n > 15 AND c.host = 'h1' ORDER BY a.timestamp, b.timestamp, c.timestamp",
    },
    Case {
        // A row of the first table without a match keeps going, null,
        // through the join after.
        name: "left joins in a chain",
        tessera: "SELECT a.timestamp, count_raw(b.timestamp), count_raw(c.timestamp) FROM logs a LEFT JOIN logs b ON b.body = a.body AND b.attr.host = a.attr.host LEFT JOIN logs c ON c.timestamp = b.timestamp AND c.attr.n = a.attr.n WHERE a.attr.n = 3 GROUP BY a.timestamp ORDER BY a.timestamp",
        sqlite: "SELECT a.timestamp, count(b.timestamp), count(c.timestamp) FROM logs a LEFT JOIN logs b ON b.body = a.body AND b.host = a.host LEFT JOIN logs c ON c.timestamp = b.timestamp AND c.n = a.n WHERE a.n = 3 GROUP BY a.timestamp ORDER BY a.timestamp",
    },
    Case {
        // The other conditions of WHERE and the attribute selected
        // cross the join the subquery becomes.
        name: "in subquery among other conditions",
        tessera: "SELECT timestamp, attr.host FROM logs WHERE severity_number = 13 AND timestamp >= TIMESTAMP '1970-01-01T06:00:00Z' AND attr.n IN (SELECT attr.n FROM logs WHERE severity_number = 17 AND attr.host = 'h1') ORDER BY timestamp",
        sqlite: "SELECT timestamp, host FROM logs WHERE severity_number = 13 AND timestamp >= 21600000000000 AND n IN (SELECT n FROM logs WHERE severity_number = 17 AND host = 'h1') ORDER BY timestamp",
    },
    Case {
        // A row whose value is null is left out: NOT IN of it is
        // unknown, not true.
        name: "not in subquery",
        tessera: "SELECT timestamp FROM logs WHERE attr.host IS NOT NULL AND body NOT IN (SELECT body FROM logs WHERE attr.n > 3 AND body IS NOT NULL) ORDER BY timestamp",
        sqlite: "SELECT timestamp FROM logs WHERE host IS NOT NULL AND body NOT IN (SELECT body FROM logs WHERE n > 3 AND body IS NOT NULL) ORDER BY timestamp",
    },
    Case {
        name: "correlated exists",
        tessera: "SELECT timestamp FROM logs a WHERE a.severity_number = 17 AND EXISTS (SELECT 1 FROM logs b WHERE b.body = a.body AND b.attr.n > 17) ORDER BY timestamp",
        sqlite: "SELECT timestamp FROM logs a WHERE a.severity_number = 17 AND EXISTS (SELECT 1 FROM logs b WHERE b.body = a.body AND b.n > 17) ORDER BY timestamp",
    },
    Case {
        name: "scalar subquery",
        tessera: "SELECT timestamp, attr.host FROM logs WHERE severity_number = 9 AND attr.n::int > (SELECT max(severity_number) FROM logs) ORDER BY timestamp",
        sqlite: "SELECT timestamp, host FROM logs WHERE severity_number = 9 AND n > (SELECT max(severity_number) FROM logs) ORDER BY timestamp",
    },
    Case {
        // The condition reads an attribute and one subquery's value
        // between the joins the two subqueries become.
        name: "scalar subqueries in select and where",
        tessera: "SELECT body, (SELECT count_raw(*) FROM logs) AS total FROM logs WHERE attr.n::int >= (SELECT max(severity_number) FROM logs) ORDER BY timestamp",
        sqlite: "SELECT body, (SELECT count(*) FROM logs) AS total FROM logs WHERE n >= (SELECT max(severity_number) FROM logs) ORDER BY timestamp",
    },
    Case {
        name: "window functions over the default frame",
        tessera: "SELECT timestamp, row_number() OVER (PARTITION BY severity_text ORDER BY timestamp), lag(severity_number) OVER (ORDER BY timestamp), count_raw(*) OVER (PARTITION BY severity_text ORDER BY timestamp) FROM logs ORDER BY timestamp",
        sqlite: "SELECT timestamp, row_number() OVER (PARTITION BY severity_text ORDER BY timestamp), lag(severity_number) OVER (ORDER BY timestamp), count(*) OVER (PARTITION BY severity_text ORDER BY timestamp) FROM logs ORDER BY timestamp",
    },
    Case {
        name: "moving sum and count over a frame in rows",
        tessera: "SELECT timestamp, sum_raw(severity_number) OVER (ORDER BY timestamp ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), count_raw(body) OVER (ORDER BY timestamp ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING) FROM logs ORDER BY timestamp",
        sqlite: "SELECT timestamp, sum(severity_number) OVER (ORDER BY timestamp ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), count(body) OVER (ORDER BY timestamp ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING) FROM logs ORDER BY timestamp",
    },
    Case {
        name: "centred average per partition",
        tessera: "SELECT timestamp, avg_raw(attr.n) OVER (PARTITION BY severity_text ORDER BY timestamp ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM logs ORDER BY timestamp",
        sqlite: "SELECT timestamp, avg(n) OVER (PARTITION BY severity_text ORDER BY timestamp ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM logs ORDER BY timestamp",
    },
    Case {
        name: "running and whole-partition frames",
        tessera: "SELECT timestamp, count_raw(*) OVER (ORDER BY timestamp ROWS UNBOUNDED PRECEDING), max(severity_number) OVER (PARTITION BY severity_text ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), min(attr.n::int) OVER (ORDER BY timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM logs ORDER BY timestamp",
        sqlite: "SELECT timestamp, count(*) OVER (ORDER BY timestamp ROWS UNBOUNDED PRECEDING), max(severity_number) OVER (PARTITION BY severity_text ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), min(n) OVER (ORDER BY timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM logs ORDER BY timestamp",
    },
    Case {
        name: "first and last value of a frame",
        tessera: "SELECT timestamp, first_value(body) OVER (ORDER BY timestamp ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), last_value(severity_number) OVER (ORDER BY timestamp ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) FROM logs ORDER BY timestamp",
        sqlite: "SELECT timestamp, first_value(body) OVER (ORDER BY timestamp ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), last_value(severity_number) OVER (ORDER BY timestamp ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) FROM logs ORDER BY timestamp",
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
