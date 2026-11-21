// SPDX-License-Identifier: Apache-2.0

//! `tessera-bench`: one node, in this process, measured against the
//! performance targets of spec section 17.1 that a single machine can
//! speak to. It ingests representative logs, metrics and spans over the
//! OTLP HTTP endpoints, waits for compaction, says where the stored
//! bytes go, runs the panel, search and trace queries warm and cold,
//! measures an eager alert from write to webhook, and prints a table of
//! targets and measurements. Nothing it prints is a claim about cloud
//! hardware or same-region object storage: by default the root pool is
//! a local directory, so the object-store latencies the targets assume
//! are absent and the numbers bound what the code paths themselves
//! cost. With `--root-pool` it is a bucket, reached as the `AWS_*`
//! variables say, and every read and write is a request to it.
//!
//! ```text
//! cargo run --release -p tessera-bench -- --rows 300000
//! cargo run --release -p tessera-bench -- --root-pool s3://bucket/bench --cache-dir /tmp/bench-cache
//! ```

// Measurements are ratios of counts far below 2^53; the precision the
// casts lose is not one a benchmark reports.
#![allow(clippy::cast_precision_loss)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Parser;
use tessera_node::{Config, Node};
use tessera_object::{ObjectStore, layout};
use tessera_types::{DatasetName, Env, FileMeta, Kind, Level, TenantId};

mod load;
mod measure;
mod web;

use measure::{Percentiles, percentiles};

/// Options.
#[derive(Parser, Debug)]
#[command(name = "tessera-bench", about)]
struct Args {
    /// Log rows to ingest.
    #[arg(long, default_value_t = 300_000)]
    rows: usize,
    /// Rows per OTLP request.
    #[arg(long, default_value_t = 1_000)]
    batch: usize,
    /// Concurrent ingest clients.
    #[arg(long, default_value_t = 4)]
    clients: usize,
    /// Metric series.
    #[arg(long, default_value_t = 200)]
    series: usize,
    /// Samples per series, ten seconds apart.
    #[arg(long, default_value_t = 360)]
    samples: usize,
    /// Traces, of five spans each.
    #[arg(long, default_value_t = 2_000)]
    traces: usize,
    /// Runs per warm query.
    #[arg(long, default_value_t = 20)]
    runs: usize,
    /// The ingester's linger, in milliseconds; the node's default when
    /// absent.
    #[arg(long)]
    linger_ms: Option<u64>,
    /// Writer logs the node appends to, each on a task of its own; the
    /// node's default, one, when absent.
    #[arg(long)]
    writers: Option<usize>,
    /// Alert probes; zero skips the alert measurement.
    #[arg(long, default_value_t = 10)]
    alert_probes: usize,
    /// The root pool; a temporary directory by default.
    #[arg(long)]
    data_dir: Option<PathBuf>,
    /// A bucket, or a prefix in one, as the root pool in place of a
    /// directory: `s3://<bucket>[/<prefix>]`. The run's objects go
    /// under a prefix of its own below it and are left there.
    #[arg(long, conflicts_with = "data_dir")]
    root_pool: Option<String>,
    /// A directory for the node's cache of object reads; none by
    /// default, so that every read reaches the root pool.
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    /// How long to wait for compaction to settle, in seconds.
    #[arg(long, default_value_t = 120)]
    settle_secs: u64,
    /// Seconds to keep the node up after compaction before the queries
    /// run, for looking at it from outside; zero by default.
    #[arg(long, default_value_t = 0)]
    pause_secs: u64,
}

/// One line of the report.
struct Line {
    area: &'static str,
    target: &'static str,
    measured: String,
}

/// The report as it is built.
#[derive(Default)]
struct Report {
    lines: Vec<Line>,
}

impl Report {
    fn add(&mut self, area: &'static str, target: &'static str, measured: String) {
        self.lines.push(Line {
            area,
            target,
            measured,
        });
    }

    fn print(&self) {
        println!();
        println!("| Area | Target (17.1) | Measured |");
        println!("|------|---------------|----------|");
        for line in &self.lines {
            println!("| {} | {} | {} |", line.area, line.target, line.measured);
        }
    }
}

/// What the run tunes on the node.
#[derive(Clone, Copy)]
struct Tuning {
    linger: Option<Duration>,
    writers: Option<usize>,
}

/// Where the node keeps its data: the directory, or the bucket in its
/// place, and the cache in front of either.
struct Root {
    dir: PathBuf,
    bucket: Option<String>,
    cache: Option<PathBuf>,
}

impl std::fmt::Display for Root {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.bucket {
            Some(url) => f.write_str(url),
            None => write!(f, "{}", self.dir.display()),
        }
    }
}

fn config(root: &Root, tuning: Tuning) -> Config {
    let free: SocketAddr = "127.0.0.1:0"
        .parse()
        .unwrap_or_else(|_| unreachable!("a literal"));
    let mut config = Config::single_node(root.dir.clone());
    config.root_pool.clone_from(&root.bucket);
    config.cache = root.cache.clone().map(|dir| tessera_node::DiskCache {
        dir,
        bytes: 8 << 30,
    });
    config.listen = free;
    config.pg_listen = free;
    config.background = Duration::from_secs(1);
    config.eval = Duration::from_millis(250);
    // Small L1 files, so that compaction has inputs to merge within the
    // run for metrics and traces as well as logs.
    config.ingest.flush_bytes = 2 << 20;
    if let Some(linger) = tuning.linger {
        config.ingest.linger = linger;
    }
    if let Some(writers) = tuning.writers {
        config.ingest.writers = writers.max(1);
    }
    config
}

/// Starts a node over `root` and waits until it serves; the node and
/// its base URL and how long that took.
async fn start(root: &Root, tuning: Tuning) -> (Node, String, Duration) {
    let started = Instant::now();
    let node = Node::start(config(root, tuning), Env::system())
        .await
        .expect("the node starts");
    let base = format!("http://{}", node.http_address());
    web::wait_ready(&base);
    (node, base, started.elapsed())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args = Args::parse();
    let temp = tempfile::tempdir().expect("a temporary directory");
    let run = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs());
    let root = Root {
        dir: args
            .data_dir
            .clone()
            .unwrap_or_else(|| temp.path().to_path_buf()),
        bucket: args
            .root_pool
            .as_ref()
            .map(|url| format!("{}/run-{run}", url.trim_end_matches('/'))),
        cache: args.cache_dir.clone(),
    };
    let linger = args.linger_ms.map(Duration::from_millis);
    let tuning = Tuning {
        linger,
        writers: args.writers,
    };
    let cores = std::thread::available_parallelism().map_or(1, usize::from);
    println!(
        "tessera-bench: {} log rows in batches of {} from {} clients, {} series x {} samples, {} traces; {} cores; root pool {}",
        args.rows, args.batch, args.clients, args.series, args.samples, args.traces, cores, root
    );
    let mut report = Report::default();

    let (node, base, startup) = start(&root, tuning).await;
    report.add(
        "New node serving",
        "<= 30 s from process start",
        format!("{:.2} s", startup.as_secs_f64()),
    );
    println!("node up in {:.2} s at {base}", startup.as_secs_f64());

    let ingest = ingest_phase(&base, &args, cores, tuning, &mut report).await;
    let traces = load::ingest_traces(&base, args.traces);
    println!(
        "ingested {} traces of 5 spans in {:.1} s",
        traces.ids.len(),
        traces.wall.as_secs_f64()
    );
    compaction_phase(&node, &args, &ingest, &mut report).await;
    if args.pause_secs > 0 {
        println!("pausing {} s at {base}", args.pause_secs);
        tokio::time::sleep(Duration::from_secs(args.pause_secs)).await;
    }

    let warm = query_phase(&base, &traces.ids, args.runs);
    let alert = if args.alert_probes > 0 {
        Some(web::alert_latency(&base, args.alert_probes).await)
    } else {
        None
    };

    // Cold: the same node restarted over the same root pool, each query once.
    node.stop().await;
    let (node, base, restart) = start(&root, tuning).await;
    let queries = web::queries(&base, &traces.ids);
    let cold_runs: Vec<Duration> = queries
        .iter()
        .map(|(_, run)| {
            let at = Instant::now();
            run();
            at.elapsed()
        })
        .collect();
    node.stop().await;

    for ((name, p), cold_run) in warm.iter().zip(&cold_runs) {
        let (target, caveat) = web::target_of(name);
        report.add(
            name,
            target,
            format!(
                "warm p50 {} ms, p95 {} ms; cold {} ms{caveat}",
                p.p50.as_millis(),
                p.p95.as_millis(),
                cold_run.as_millis()
            ),
        );
    }
    if let Some(alert) = alert {
        let p = percentiles(&alert.latencies);
        report.add(
            "Alert, eager rule: write to notification",
            "p99 <= 3 s",
            format!(
                "p50 {} ms, p99 {} ms over {} probes ({})",
                p.p50.as_millis(),
                p.p99.as_millis(),
                alert.latencies.len(),
                alert.mode
            ),
        );
    }
    report.add(
        "Node restart serving",
        "<= 30 s from process start",
        format!("{:.2} s", restart.as_secs_f64()),
    );
    report.print();
    drop(temp);
}

/// Logs in bulk with freshness probes, then the metrics.
async fn ingest_phase(
    base: &str,
    args: &Args,
    cores: usize,
    tuning: Tuning,
    report: &mut Report,
) -> load::Ingested {
    let ingest = load::ingest_logs(base, args.rows, args.batch, args.clients).await;
    let seconds = ingest.wall.as_secs_f64();
    let otlp_mb = ingest.otlp_bytes as f64 / 1e6;
    let per_core = otlp_mb / seconds / cores as f64;
    println!(
        "ingested {} rows, {:.1} MB of OTLP protobuf ({:.1} MB as OTLP JSON, {:.1} MB as flat JSON lines) in {:.1} s: {:.1} MB/s, {:.2} MB/s per core",
        args.rows,
        otlp_mb,
        ingest.json_bytes as f64 / 1e6,
        ingest.flat_bytes as f64 / 1e6,
        seconds,
        otlp_mb / seconds,
        per_core
    );
    let ack = percentiles(&ingest.ack_latencies);
    report.add(
        "Write acknowledgment (durable)",
        "p50 <= 200 ms, p99 <= 500 ms",
        format!(
            "p50 {} ms, p99 {} ms (linger {} ms, {} clients)",
            ack.p50.as_millis(),
            ack.p99.as_millis(),
            tuning
                .linger
                .unwrap_or_else(|| Config::single_node(PathBuf::new()).ingest.linger)
                .as_millis(),
            args.clients
        ),
    );
    report.add(
        "Ingest throughput",
        ">= 20 MB/s of OTLP per ingest core",
        format!(
            "{:.1} MB/s over {} writer task(s), {:.2} MB/s per machine core ({} clients)",
            otlp_mb / seconds,
            tuning.writers.unwrap_or(1).max(1),
            per_core,
            args.clients
        ),
    );
    let fresh = percentiles(&ingest.freshness);
    report.add(
        "Freshness (acknowledged to queryable)",
        "p99 <= 1 s",
        format!(
            "p50 {} ms, max {} ms over {} probes",
            fresh.p50.as_millis(),
            fresh.max.as_millis(),
            ingest.freshness.len()
        ),
    );
    let metrics = load::ingest_metrics(base, args.series, args.samples);
    println!(
        "ingested {} metric samples in {:.1} s",
        metrics.samples,
        metrics.wall.as_secs_f64()
    );
    ingest
}

/// Waits for compaction, reports compression and metric storage, and
/// says where the bytes of the largest compacted files go.
async fn compaction_phase(node: &Node, args: &Args, ingest: &load::Ingested, report: &mut Report) {
    let store = Arc::clone(node.state().pools.root().store());
    let env = node.env().clone();
    // Compaction runs when enough L1 files are due; with little data a
    // lone file waits for its age, so the wait also ends when nothing has
    // changed for thirty seconds.
    let waited = Instant::now();
    let mut last_change = Instant::now();
    let mut seen = (0usize, 0u64);
    let (logs_files, metric_files) = loop {
        let logs = files_of(&store, &env, Kind::Log).await;
        let metric = files_of(&store, &env, Kind::Metric).await;
        let settled = |f: &[FileMeta]| !f.is_empty() && f.iter().all(|x| x.level != Level::L1);
        let shape = (
            logs.len() + metric.len(),
            logs.iter()
                .chain(&metric)
                .map(|f| f.size_bytes)
                .sum::<u64>(),
        );
        if shape != seen {
            seen = shape;
            last_change = Instant::now();
        }
        let idle = last_change.elapsed() > Duration::from_secs(30)
            && waited.elapsed() > Duration::from_secs(30);
        if (settled(&logs) && settled(&metric))
            || waited.elapsed() > Duration::from_secs(args.settle_secs)
            || idle
        {
            break (logs, metric);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    let compacted = |files: &[FileMeta]| -> (u64, u64) {
        files
            .iter()
            .filter(|f| f.level != Level::L1)
            .fold((0, 0), |(rows, bytes), f| {
                (rows + f.row_count, bytes + f.size_bytes)
            })
    };
    let (log_rows, log_bytes) = compacted(&logs_files);
    let l1_left = logs_files.iter().filter(|f| f.level == Level::L1).count();
    println!(
        "compaction settled in {:.1} s: {} log files ({} still L1), {} compacted rows in {:.1} MB",
        waited.elapsed().as_secs_f64(),
        logs_files.len(),
        l1_left,
        log_rows,
        log_bytes as f64 / 1e6
    );
    let ratio = |raw: u64| {
        if log_rows > 0 && log_bytes > 0 {
            (raw as f64 * log_rows as f64 / args.rows as f64) / log_bytes as f64
        } else {
            0.0
        }
    };
    report.add(
        "Log compression",
        ">= 10x against raw JSON",
        format!(
            "{:.1}x against flat JSON lines, {:.1}x against OTLP JSON, over {log_rows} compacted rows ({l1_left} L1 files left)",
            ratio(ingest.flat_bytes),
            ratio(ingest.json_bytes)
        ),
    );
    let (metric_rows, metric_bytes) = compacted(&metric_files);
    report.add(
        "Metric storage",
        "<= 3 bytes per sample after L2",
        if metric_rows > 0 {
            format!(
                "{:.2} bytes per sample over {metric_rows} samples",
                metric_bytes as f64 / metric_rows as f64
            )
        } else {
            "no compacted metric file".into()
        },
    );
    for (kind, files) in [(Kind::Log, &logs_files), (Kind::Metric, &metric_files)] {
        if let Some(largest) = files
            .iter()
            .filter(|f| f.level != Level::L1)
            .max_by_key(|f| f.size_bytes)
        {
            columns_of(&store, kind, largest).await;
        }
    }
}

/// Prints the compressed bytes per row of each column of `file`, the
/// largest columns first.
async fn columns_of(store: &Arc<dyn ObjectStore>, kind: Kind, file: &FileMeta) {
    use parquet::file::reader::{FileReader, SerializedFileReader};

    let key = layout::data_file(&file.partition, file.file_id);
    let Ok(bytes) = store.get(&key).await else {
        return;
    };
    let Ok(reader) = SerializedFileReader::new(bytes) else {
        return;
    };
    let metadata = reader.metadata();
    let mut sizes: Vec<(String, i64)> = Vec::new();
    for group in metadata.row_groups() {
        for column in group.columns() {
            let name = column.column_path().string();
            match sizes.iter_mut().find(|(n, _)| *n == name) {
                Some((_, size)) => *size += column.compressed_size(),
                None => sizes.push((name, column.compressed_size())),
            }
        }
    }
    sizes.sort_by_key(|(_, size)| std::cmp::Reverse(*size));
    let total: i64 = sizes.iter().map(|(_, s)| s).sum();
    println!(
        "largest compacted {kind} file: {} rows, {:.1} MB, {:.1} bytes per row; per column:",
        file.row_count,
        file.size_bytes as f64 / 1e6,
        file.size_bytes as f64 / file.row_count.max(1) as f64
    );
    for (name, size) in sizes.iter().take(12) {
        println!(
            "  {name:<22} {:>7.2} bytes/row  {:>5.1}%",
            *size as f64 / file.row_count.max(1) as f64,
            *size as f64 * 100.0 / total.max(1) as f64
        );
    }
}

/// Runs every query once to warm up, then `runs` times for the
/// percentiles.
fn query_phase(base: &str, trace_ids: &[String], runs: usize) -> Vec<(&'static str, Percentiles)> {
    let queries = web::queries(base, trace_ids);
    queries
        .iter()
        .map(|(name, run)| {
            run();
            let samples: Vec<Duration> = (0..runs)
                .map(|_| {
                    let at = Instant::now();
                    run();
                    at.elapsed()
                })
                .collect();
            (*name, percentiles(&samples))
        })
        .collect()
}

/// The files of the `prod` dataset's `kind`, as the manifests say.
async fn files_of(store: &Arc<dyn ObjectStore>, env: &Env, kind: Kind) -> Vec<FileMeta> {
    let dataset = DatasetName::new("prod").unwrap_or_else(|_| unreachable!("a valid name"));
    let Ok(partitions) =
        tessera_manifest::list_partitions(&**store, &TenantId::DEFAULT, &dataset, kind).await
    else {
        return Vec::new();
    };
    let mut files = Vec::new();
    for partition in partitions {
        let manifest = tessera_manifest::Manifest::open(Arc::clone(store), partition, env.clone());
        let Ok(snapshot) = manifest.snapshot().await else {
            continue;
        };
        files.extend(snapshot.state().files().cloned());
    }
    files
}

/// Now, as nanoseconds since the epoch.
pub(crate) fn now_nanos() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
    )
    .unwrap_or(u64::MAX)
}

/// A mutex guard that survives a poisoned lock.
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
