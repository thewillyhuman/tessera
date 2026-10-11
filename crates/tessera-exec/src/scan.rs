// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The scan: one dataset and kind through every pruning stage of
//! section 10.4, over files and the tail alike (QE-16).
//!
//! Partitions outside the time range are not opened (stage 1). The
//! surviving files are pruned by their manifest entries (stages 2 to 4),
//! then each file's sidecar indexes, when it has any, skip the file or
//! narrow its row groups (stage 5), and each is fetched with row-group
//! pruning and the predicate as a row filter (stages 6 and 7) and
//! decoded on the CPU pool. Tail rows
//! from segments go through the same predicate. A newest-first limit
//! reads files newest first and stops once the rows found are newer than
//! every unread file (the time-ordered top-K scan of section 10.6).

use std::sync::Arc;

use arrow::array::{BooleanArray, RecordBatch, TimestampNanosecondArray};
use arrow::compute::filter_record_batch;
use arrow::datatypes::SchemaRef;
use futures::stream::{self, StreamExt};
use tessera_format::{RowPredicate, ScanRequest, ScanStats, Sidecar};
use tessera_ir::{BinaryOp, Expr, Scan};
use tessera_kernels::evaluate_predicate;
use tessera_manifest::view;
use tessera_object::layout;
use tessera_opt::{Decision, PruneReport, bounds, decide, prune_files};
use tessera_types::event::TIMESTAMP;
use tessera_types::{FileMeta, Timestamp};

use crate::engine::{Engine, EngineError};
use crate::remote::{LeafTask, Peer};

/// Files fetched concurrently per scan.
const FETCH_CONCURRENCY: usize = 8;

/// What one scan did, for `EXPLAIN ANALYZE` (QE-8).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ScanAnalysis {
    /// `dataset/kind`.
    pub source: String,
    /// Partitions of the dataset and kind.
    pub partitions: usize,
    /// Partitions whose window overlaps the time range.
    pub partitions_kept: usize,
    /// How many files each manifest stage let through.
    pub files: PruneReport,
    /// Files whose sidecar indexes excluded every row (stage 5).
    pub files_skipped_by_index: usize,
    /// Row groups the sidecar indexes excluded in files that were read.
    pub row_groups_skipped_by_index: usize,
    /// Files actually fetched; fewer than pruned when a limit stopped the
    /// scan early or an index skipped the file.
    pub files_read: usize,
    /// Row groups in the files read.
    pub row_groups: usize,
    /// Row groups decoded after statistics pruning.
    pub row_groups_read: usize,
    /// Bytes fetched from object storage.
    pub bytes_read: u64,
    /// Range requests made.
    pub requests: usize,
    /// Files whose footer came from the cache instead of a request.
    pub footers_cached: usize,
    /// Rows in the row groups read, before the predicates.
    pub rows_scanned: u64,
    /// Segments read for the tail.
    pub segments: usize,
    /// Rows produced from files.
    pub rows_from_files: usize,
    /// Rows produced from the tail.
    pub rows_from_tail: usize,
    /// Files read by other nodes (QE-15).
    pub files_remote: usize,
    /// Leaf tasks sent to other nodes.
    pub remote_tasks: usize,
    /// Leaf tasks a peer could not run, read here instead (QE-17).
    pub remote_failed: usize,
}

impl ScanAnalysis {
    fn add(&mut self, stats: ScanStats) {
        self.files_read += 1;
        self.row_groups += stats.row_groups;
        self.row_groups_read += stats.row_groups_read;
        self.bytes_read += stats.bytes_read;
        self.requests += stats.requests;
        self.footers_cached += usize::from(stats.footer_cached);
        self.rows_scanned += stats.rows_scanned;
    }
}

/// Runs `scan`, returning batches in the scan's schema: the tail from
/// the segments, and the files from this node and from its peers.
pub(crate) async fn run(
    engine: &Engine,
    scan: &Scan,
    analysis: &mut ScanAnalysis,
) -> Result<Vec<RecordBatch>, EngineError> {
    analysis.source = format!("{}/{}", scan.dataset, scan.kind);
    let schema = scan.schema()?;
    let found = view::sources(
        &engine.manifests,
        &scan.tenant,
        &scan.dataset,
        scan.kind,
        engine.partition_window,
        scan.time_range,
    )
    .await?;
    analysis.partitions = found.partitions.len();
    analysis.partitions_kept = found
        .partitions
        .iter()
        .filter(|p| {
            scan.time_range.overlaps_closed(
                p.window.start(),
                Timestamp::from_nanos(p.window.end().as_nanos().saturating_sub(1)),
            )
        })
        .count();
    analysis.segments = found.sources.segments;

    let predicate = full_predicate(scan);
    let mut output = Vec::new();
    for rows in found.tail {
        let rows = apply(&rows, predicate.as_ref(), &schema)?;
        if rows.num_rows() > 0 {
            analysis.rows_from_tail += rows.num_rows();
            output.push(rows);
        }
    }

    let (files, report) = prune_files(scan, &found.files);
    analysis.files = report;
    let files: Vec<FileMeta> = files.into_iter().cloned().collect();
    // A newest-first scan stops early, which only works in one place.
    let (local, remote) = if scan.newest_first_limit.is_some() {
        (files, Vec::new())
    } else {
        assign(engine, files)
    };
    let mut local = local;
    for (peer, task_files) in remote {
        let task = LeafTask {
            scan: scan.clone(),
            files: task_files,
        };
        analysis.remote_tasks += 1;
        let Some((_, transport)) = &engine.peers else {
            local.extend(task.files);
            continue;
        };
        match transport.leaf(&peer, &task).await {
            Ok(batches) => {
                analysis.files_remote += task.files.len();
                for rows in batches {
                    let rows = reorder(&rows, &schema)?;
                    analysis.rows_from_files += rows.num_rows();
                    output.push(rows);
                }
            }
            Err(error) => {
                tracing::warn!(peer = %peer.node, error = %error, "leaf task failed; reading here");
                analysis.remote_failed += 1;
                local.extend(task.files);
            }
        }
    }
    let from_files = read_files(engine, scan, local, &output, analysis).await?;
    output.extend(from_files);
    Ok(output)
}

/// The scan's predicates with its time range as conjuncts: rows outside
/// the range are filtered like any predicate, and the range also prunes
/// partitions, files and row groups above.
fn full_predicate(scan: &Scan) -> Option<Expr> {
    let mut conjuncts = scan.predicates.clone();
    if scan.time_range.start != Timestamp::MIN {
        conjuncts.push(
            Expr::column(TIMESTAMP).binary(BinaryOp::GtEq, Expr::literal(scan.time_range.start)),
        );
    }
    if scan.time_range.end != Timestamp::MAX {
        conjuncts
            .push(Expr::column(TIMESTAMP).binary(BinaryOp::Lt, Expr::literal(scan.time_range.end)));
    }
    Expr::all(conjuncts)
}

/// Splits `files` into this node's and each peer's, by the owner the
/// peers assign to each file; everything is local without peers.
fn assign(engine: &Engine, files: Vec<FileMeta>) -> (Vec<FileMeta>, Vec<(Peer, Vec<FileMeta>)>) {
    let Some((peers, _)) = &engine.peers else {
        return (files, Vec::new());
    };
    let mut local = Vec::new();
    let mut by_peer: Vec<(Peer, Vec<FileMeta>)> = Vec::new();
    for file in files {
        match peers.owner_of(file.file_id) {
            None => local.push(file),
            Some(peer) => match by_peer.iter_mut().find(|(p, _)| *p == peer) {
                Some((_, group)) => group.push(file),
                None => by_peer.push((peer, vec![file])),
            },
        }
    }
    (local, by_peer)
}

/// Reads `files` of `scan` on this node, newest first so that a limit
/// can stop early once `already` (the rows read before, the tail among
/// them) and the files read hold the newest rows, and returns their
/// rows in the scan's schema.
pub(crate) async fn read_files(
    engine: &Engine,
    scan: &Scan,
    mut files: Vec<FileMeta>,
    already: &[RecordBatch],
    analysis: &mut ScanAnalysis,
) -> Result<Vec<RecordBatch>, EngineError> {
    let schema = scan.schema()?;
    let predicate = full_predicate(scan);
    let request = IndexedRequest {
        request: ScanRequest {
            projection: Some(schema.fields().iter().map(|f| f.name().clone()).collect()),
            bounds: bounds(&scan.predicates, scan.time_range),
            filter: predicate.as_ref().map(|p| row_predicate(p.clone())),
            row_groups: None,
        },
        predicates_for_indexes: scan.predicates.clone(),
    };
    files.sort_by_key(|f| std::cmp::Reverse(f.max_timestamp));
    let mut fetches = stream::iter(
        files
            .into_iter()
            .map(|file| fetch(engine, file, request.clone())),
    )
    .buffered(FETCH_CONCURRENCY);
    let mut output = Vec::new();
    let mut newest = Newest::new(scan.newest_first_limit);
    newest.observe_batches(already);
    while let Some(result) = fetches.next().await {
        let (file, read) = result?;
        let Some(read) = read else {
            analysis.files_skipped_by_index += 1;
            continue;
        };
        analysis.row_groups_skipped_by_index += read.row_groups_skipped;
        analysis.add(read.stats);
        for rows in read.batches {
            let rows = reorder(&rows, &schema)?;
            if rows.num_rows() > 0 {
                analysis.rows_from_files += rows.num_rows();
                newest.observe(&rows);
                output.push(rows);
            }
        }
        if newest.satisfied_before(file.max_timestamp) {
            break;
        }
    }
    Ok(output)
}

/// Whether a newest-first limit is satisfied: tracks the k-th newest
/// timestamp seen so far.
struct Newest {
    limit: Option<usize>,
    seen: Vec<i64>,
}

impl Newest {
    fn new(limit: Option<usize>) -> Self {
        Newest {
            limit,
            seen: Vec::new(),
        }
    }

    fn observe_batches(&mut self, batches: &[RecordBatch]) {
        for batch in batches {
            self.observe(batch);
        }
    }

    fn observe(&mut self, rows: &RecordBatch) {
        let Some(k) = self.limit else {
            return;
        };
        if let Some(timestamps) = rows
            .column_by_name(TIMESTAMP)
            .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>())
        {
            self.seen.extend(timestamps.values().iter().copied());
            self.seen.sort_unstable_by(|a, b| b.cmp(a));
            self.seen.truncate(k);
        }
    }

    /// Whether the rows seen already hold the `k` newest, given that no
    /// unread file has a row newer than `next_max`.
    fn satisfied_before(&self, next_max: Timestamp) -> bool {
        match self.limit {
            Some(k) if self.seen.len() >= k => self
                .seen
                .last()
                .is_some_and(|kth| *kth > next_max.as_nanos()),
            _ => false,
        }
    }
}

fn row_predicate(predicate: Expr) -> RowPredicate {
    let columns: Vec<String> = predicate.references().into_iter().collect();
    RowPredicate {
        columns,
        evaluate: Arc::new(move |batch| {
            evaluate_predicate(&predicate, batch).map_err(|e| e.to_string())
        }),
    }
}

/// A scan request together with the conjuncts the indexes are consulted
/// about.
#[derive(Clone)]
struct IndexedRequest {
    request: ScanRequest,
    predicates_for_indexes: Vec<Expr>,
}

impl IndexedRequest {
    fn into_request(self) -> ScanRequest {
        self.request
    }
}

/// What reading one file produced.
struct FileRead {
    stats: ScanStats,
    row_groups_skipped: usize,
    batches: Vec<RecordBatch>,
}

/// Reads `file`, or returns `None` when its indexes exclude every row.
async fn fetch(
    engine: &Engine,
    file: FileMeta,
    mut request: IndexedRequest,
) -> Result<(FileMeta, Option<FileRead>), EngineError> {
    let pool = engine.pools.get(&file.pool)?;
    let mut row_groups_skipped = 0;
    if !file.indexes.is_empty() && request.request.filter.is_some() {
        let bytes = pool
            .store()
            .get(&layout::sidecar(&file.partition, file.file_id))
            .await?;
        let sidecar = Sidecar::decode(bytes)?;
        match decide(&file, &sidecar, &request.predicates_for_indexes) {
            Decision::SkipFile => return Ok((file, None)),
            Decision::RowGroups(Some(groups)) => {
                row_groups_skipped = sidecar.row_groups().saturating_sub(groups.len());
                request.request.row_groups = Some(groups);
            }
            Decision::RowGroups(None) => {}
        }
    }
    let key = layout::data_file(&file.partition, file.file_id);
    let reader = engine
        .format
        .open(Arc::clone(pool.store()), key, file.size_bytes);
    let fetched = reader.fetch(request.into_request()).await?;
    let stats = fetched.stats();
    let batches = engine.cpu.spawn(move || fetched.decode()).await??;
    Ok((
        file,
        Some(FileRead {
            stats,
            row_groups_skipped,
            batches,
        }),
    ))
}

/// Filters `rows` by `predicate` and projects them to `schema`.
fn apply(
    rows: &RecordBatch,
    predicate: Option<&Expr>,
    schema: &SchemaRef,
) -> Result<RecordBatch, EngineError> {
    let filtered = match predicate {
        Some(predicate) => {
            let keep: BooleanArray = evaluate_predicate(predicate, rows)?;
            filter_record_batch(rows, &keep)?
        }
        None => rows.clone(),
    };
    reorder(&filtered, schema)
}

/// `rows` with the columns of `schema`, in its order.
fn reorder(rows: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch, EngineError> {
    if rows.schema().fields() == schema.fields() {
        return Ok(rows.clone());
    }
    let indices = schema
        .fields()
        .iter()
        .map(|f| rows.schema().index_of(f.name()))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows.project(&indices)?)
}
