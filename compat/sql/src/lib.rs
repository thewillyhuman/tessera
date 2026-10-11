// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Differential testing of SQL against a reference engine (spec section
//! 18.5): the same generated rows are loaded into Tessera and into
//! `SQLite`, the same question is asked of both in their dialects, and the
//! answers must agree.
//!
//! Tessera stores attributes under paths such as `attr.host`; the
//! reference table has a plain column `host`. Timestamps are nanoseconds
//! since the epoch on both sides so that results compare as numbers.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use rusqlite::Connection;
use rusqlite::types::Value;
use tessera_exec::{CpuPool, Engine, QueryLimits, Response, query};
use tessera_format::ParquetFormat;
use tessera_ir::BindContext;
use tessera_manifest::{intake, registry};
use tessera_object::{MemoryStore, Pools};
use tessera_sql::SqlFrontend;
use tessera_types::event::Scalar;
use tessera_types::{
    DatasetName, Env, EventBatch, LogRow, PoolId, Random, SeededRandom, Seq, TenantId, Timestamp,
    Window,
};
use tessera_wal::{EntryBody, FlushTarget, WriterLog, write_l1_files};

/// A generated log row.
#[derive(Clone, Debug)]
pub struct Row {
    /// Event time, nanoseconds.
    pub timestamp: i64,
    /// Severity number.
    pub severity_number: i32,
    /// Severity text.
    pub severity_text: &'static str,
    /// Body, sometimes missing.
    pub body: Option<String>,
    /// Attribute `attr.n`.
    pub n: i64,
    /// Attribute `attr.host`, sometimes missing.
    pub host: Option<String>,
}

/// `count` rows over two days, from `seed`.
#[must_use]
pub fn generate(seed: u64, count: usize) -> Vec<Row> {
    const DAY: i64 = 86_400_000_000_000;
    let mut random = SeededRandom::new(seed);
    (0..count)
        .map(|i| {
            let (severity_number, severity_text) = [(9, "INFO"), (13, "WARN"), (17, "ERROR")]
                [usize::try_from(random.below(3)).unwrap_or(0)];
            Row {
                timestamp: i64::try_from(random.below(2)).unwrap_or(0) * DAY
                    + i64::try_from(random.below(86_400)).unwrap_or(0) * 1_000_000_000
                    + i64::try_from(i).unwrap_or(0),
                severity_number,
                severity_text,
                body: (random.below(10) != 0)
                    .then(|| format!("event {} happened", random.below(50))),
                n: i64::try_from(random.below(20)).unwrap_or(0),
                host: (random.below(5) != 0).then(|| format!("h{}", random.below(3))),
            }
        })
        .collect()
}

/// A canonical cell: what both engines' results reduce to.
#[derive(Clone, Debug, PartialEq)]
pub enum Cell {
    /// SQL `NULL`.
    Null,
    /// Any integer or timestamp (as nanoseconds).
    Int(i64),
    /// A float, compared with a tolerance.
    Float(f64),
    /// Text.
    Text(String),
}

impl Cell {
    fn close_to(&self, other: &Cell) -> bool {
        match (self, other) {
            (Cell::Float(a), Cell::Float(b)) => {
                (a - b).abs() <= 1e-9 * a.abs().max(b.abs()).max(1.0)
            }
            #[allow(clippy::cast_precision_loss)]
            (Cell::Float(a), Cell::Int(b)) | (Cell::Int(b), Cell::Float(a)) => {
                (a - *b as f64).abs() <= 1e-9
            }
            (a, b) => a == b,
        }
    }
}

/// Both engines over the same rows.
pub struct Fixture {
    engine: Engine,
    sqlite: Connection,
    ctx: BindContext,
}

impl Fixture {
    /// Loads `rows` into both engines. In Tessera, the first two thirds
    /// are flushed to files and adopted; the rest stays in the tail.
    ///
    /// # Panics
    ///
    /// If either engine cannot be loaded; tests have nothing to recover.
    pub async fn load(rows: &[Row]) -> Self {
        let sqlite = Connection::open_in_memory().expect("sqlite in memory");
        sqlite
            .execute_batch(
                "CREATE TABLE logs (timestamp INTEGER NOT NULL, severity_number INTEGER, \
                 severity_text TEXT, body TEXT, n INTEGER, host TEXT);",
            )
            .expect("create table");
        for row in rows {
            sqlite
                .execute(
                    "INSERT INTO logs VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    rusqlite::params![
                        row.timestamp,
                        row.severity_number,
                        row.severity_text,
                        row.body,
                        row.n,
                        row.host
                    ],
                )
                .expect("insert");
        }

        let store = Arc::new(MemoryStore::new());
        let env = Env::simulated(1, Timestamp::UNIX_EPOCH);
        let dataset = DatasetName::new("prod").expect("valid name");
        let mut log = WriterLog::start(store.clone(), env.clone());
        registry::register(&*store, &env, &TenantId::DEFAULT, &dataset, log.writer_id())
            .await
            .expect("register");
        let split = rows.len() * 2 / 3;
        let (filed, tail) = rows.split_at(split);
        let mut after = Seq::ZERO;
        for chunk in filed.chunks(50) {
            let batch = event_batch(&dataset, chunk);
            let through = log
                .append_segment(std::slice::from_ref(&batch))
                .await
                .expect("segment");
            let written = write_l1_files(
                &FlushTarget {
                    store: store.clone(),
                    pool: PoolId::new("root").expect("valid id"),
                    format: Arc::new(ParquetFormat::default()),
                },
                &env,
                log.writer_id(),
                after,
                through,
                Window::DEFAULT_LENGTH,
                vec![batch],
            )
            .await
            .expect("flush");
            log.append(EntryBody::Flush {
                tenant: TenantId::DEFAULT,
                dataset: dataset.clone(),
                after,
                through,
                files: written,
            })
            .await
            .expect("flush record");
            after = through;
        }
        intake::run(
            store.clone(),
            &env,
            &TenantId::DEFAULT,
            &dataset,
            log.writer_id(),
        )
        .await
        .expect("intake");
        if !tail.is_empty() {
            log.append_segment(&[event_batch(&dataset, tail)])
                .await
                .expect("tail segment");
        }
        let pools = Pools::new(PoolId::new("root").expect("valid id"), store);
        let engine = Engine::new(
            Arc::new(pools),
            Arc::new(ParquetFormat::default()),
            env.clone(),
            CpuPool::new(2),
            QueryLimits::default(),
        );
        Fixture {
            engine,
            sqlite,
            ctx: BindContext {
                tenant: TenantId::DEFAULT,
                dataset,
                now: env.clock.now(),
            },
        }
    }

    /// Runs `tessera_sql` on Tessera and `sqlite_sql` on `SQLite` and
    /// returns the first difference, if any.
    ///
    /// # Panics
    ///
    /// If either engine rejects its query.
    pub async fn differ(&self, tessera_sql: &str, sqlite_sql: &str) -> Option<String> {
        let ours = self.tessera(tessera_sql).await;
        let theirs = self.sqlite(sqlite_sql);
        if ours.len() != theirs.len() {
            return Some(format!(
                "row counts differ: tessera {} vs sqlite {}\n{ours:?}\n{theirs:?}",
                ours.len(),
                theirs.len()
            ));
        }
        for (i, (a, b)) in ours.iter().zip(&theirs).enumerate() {
            if a.len() != b.len() {
                return Some(format!("row {i}: column counts differ: {a:?} vs {b:?}"));
            }
            for (j, (x, y)) in a.iter().zip(b).enumerate() {
                if !x.close_to(y) {
                    return Some(format!("row {i} column {j}: tessera {x:?} vs sqlite {y:?}"));
                }
            }
        }
        None
    }

    async fn tessera(&self, sql: &str) -> Vec<Vec<Cell>> {
        let response = query::run(&self.engine, &SqlFrontend, sql, &self.ctx)
            .await
            .unwrap_or_else(|e| panic!("tessera rejected {sql:?}: {e}"));
        let Response::Rows(output) = response else {
            panic!("not a query: {sql:?}");
        };
        output.batches.iter().flat_map(cells).collect()
    }

    fn sqlite(&self, sql: &str) -> Vec<Vec<Cell>> {
        let mut statement = self
            .sqlite
            .prepare(sql)
            .unwrap_or_else(|e| panic!("sqlite rejected {sql:?}: {e}"));
        let columns = statement.column_count();
        let rows = statement
            .query_map([], |row| {
                (0..columns)
                    .map(|i| {
                        Ok(match row.get::<_, Value>(i)? {
                            Value::Null => Cell::Null,
                            Value::Integer(i) => Cell::Int(i),
                            Value::Real(f) => Cell::Float(f),
                            Value::Text(s) => Cell::Text(s),
                            Value::Blob(b) => Cell::Text(format!("{b:?}")),
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .expect("query");
        rows.map(|r| r.expect("row")).collect()
    }
}

fn event_batch(dataset: &DatasetName, rows: &[Row]) -> EventBatch {
    let logs: Vec<LogRow> = rows
        .iter()
        .enumerate()
        .map(|(i, row)| {
            let mut attributes = BTreeMap::from([("attr.n".to_owned(), Scalar::Int(row.n))]);
            if let Some(host) = &row.host {
                attributes.insert("attr.host".to_owned(), Scalar::Text(host.clone()));
            }
            let mut event_id = [0u8; 16];
            event_id[..8].copy_from_slice(&row.timestamp.to_le_bytes());
            event_id[8..].copy_from_slice(&(i as u64).to_le_bytes());
            LogRow {
                timestamp: Timestamp::from_nanos(row.timestamp),
                observed_timestamp: Timestamp::from_nanos(row.timestamp),
                event_id,
                severity_number: Some(row.severity_number),
                severity_text: Some(row.severity_text.to_owned()),
                body: row.body.clone(),
                attributes,
                ..LogRow::default()
            }
        })
        .collect();
    EventBatch::from_logs(TenantId::DEFAULT, dataset.clone(), &logs).expect("valid batch")
}

fn cells(batch: &RecordBatch) -> Vec<Vec<Cell>> {
    (0..batch.num_rows())
        .map(|i| {
            batch
                .columns()
                .iter()
                .map(|column| {
                    if column.is_null(i) {
                        return Cell::Null;
                    }
                    let any = column.as_any();
                    match column.data_type() {
                        DataType::Int32 => Cell::Int(i64::from(
                            any.downcast_ref::<Int32Array>().expect("i32").value(i),
                        )),
                        DataType::Int64 => {
                            Cell::Int(any.downcast_ref::<Int64Array>().expect("i64").value(i))
                        }
                        DataType::Float64 => {
                            Cell::Float(any.downcast_ref::<Float64Array>().expect("f64").value(i))
                        }
                        DataType::Utf8 => Cell::Text(
                            any.downcast_ref::<StringArray>()
                                .expect("text")
                                .value(i)
                                .to_owned(),
                        ),
                        DataType::Boolean => Cell::Int(i64::from(
                            any.downcast_ref::<BooleanArray>().expect("bool").value(i),
                        )),
                        DataType::Timestamp(TimeUnit::Nanosecond, _) => Cell::Int(
                            any.downcast_ref::<TimestampNanosecondArray>()
                                .expect("timestamp")
                                .value(i),
                        ),
                        other => Cell::Text(format!("<{other}>")),
                    }
                })
                .collect()
        })
        .collect()
}
