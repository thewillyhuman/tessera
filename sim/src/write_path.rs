// SPDX-License-Identifier: Apache-2.0

//! Scenario: the write path under faults, crashes and duplicate owners.
//!
//! Ingesters write rows with unique ids, are ticked and crash at random;
//! recovery seals and flushes the logs of crashed writers; intake runs
//! with two owners at once (AR-6); writer logs rotate on age. The
//! invariant is VW-1: at every read, the consistent view holds every
//! acknowledged row exactly once, and no row more than once. At the end,
//! once every log has ended and intake has caught up, no writer is left
//! in the registry and the view is served from files alone.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, FixedSizeBinaryArray};
use tessera_format::{FileFormat, ParquetFormat};
use tessera_ingest::{Ack, IngestConfig, Ingester};
use tessera_manifest::{intake, registry, view};
use tessera_object::{FaultPolicy, Pools};
use tessera_types::event::EVENT_ID;
use tessera_types::{
    Clock, DatasetName, EventBatch, Kind, LogRow, PoolId, Random, TenantId, Window, WriterId,
};
use tessera_wal::{FlushTarget, Recovery, recover};

use crate::{Report, Sim, Violation, ensure};

const TICK: Duration = Duration::from_secs(1);
const WRITE_PER_MILLE: u64 = 700;
const CRASH_PER_MILLE: u64 = 40;
const RESTART_PER_MILLE: u64 = 300;
const RECOVER_PER_MILLE: u64 = 200;
const INTAKE_PER_MILLE: u64 = 300;
const READ_PER_MILLE: u64 = 300;

fn dataset() -> DatasetName {
    DatasetName::new("prod").unwrap_or_else(|_| unreachable!("a literal valid name"))
}

fn pool() -> PoolId {
    PoolId::new("root").unwrap_or_else(|_| unreachable!("a literal valid pool id"))
}

fn config() -> IngestConfig {
    IngestConfig {
        linger: Duration::from_millis(500),
        segment_bytes: 1 << 30,
        flush_bytes: 1 << 30,
        flush_age: Duration::from_secs(5),
        max_log_age: Duration::from_secs(15),
        max_log_entries: 1_000_000,
        writers: 1,
    }
}

struct Node {
    ingester: Option<Ingester>,
    pending: Vec<(Vec<u64>, Ack)>,
}

/// Runs the scenario on `sim`.
///
/// # Errors
///
/// The first invariant that does not hold.
pub async fn write_path(sim: &Sim) -> Result<Report, Violation> {
    let format: Arc<dyn FileFormat> = Arc::new(ParquetFormat::default());
    let faulty_pools = Pools::new(pool(), sim.store());
    let pristine_pools = Pools::new(pool(), sim.pristine());
    let target = FlushTarget {
        store: sim.store(),
        pool: pool(),
        format: Arc::clone(&format),
    };
    let mut random = sim.random();
    let mut report = Report::default();

    let mut nodes: Vec<Node> = (0..sim.config().nodes)
        .map(|i| Node {
            ingester: Some(new_ingester(sim, i, &format)),
            pending: Vec::new(),
        })
        .collect();
    let mut acked: BTreeSet<u64> = BTreeSet::new();
    let mut dead_writers: BTreeSet<WriterId> = BTreeSet::new();
    let mut next_id: u64 = 1;

    for _ in 0..sim.config().steps {
        for (index, node) in nodes.iter_mut().enumerate() {
            let Some(ingester) = node.ingester.as_mut() else {
                if random.below(1_000) < RESTART_PER_MILLE {
                    node.ingester = Some(new_ingester(
                        sim,
                        u32::try_from(index).unwrap_or(0),
                        &format,
                    ));
                }
                continue;
            };
            if random.below(1_000) < WRITE_PER_MILLE {
                let count = 1 + random.below(3);
                let ids: Vec<u64> = (0..count).map(|k| next_id + k).collect();
                next_id += count;
                let ack = ingester.write(batch(&ids, sim.clock().now())).await;
                node.pending.push((ids, ack));
            }
            ingester.tick().await;
            settle(node, &mut acked, &mut report);
            if random.below(1_000) < CRASH_PER_MILLE {
                let crashed = node
                    .ingester
                    .take()
                    .unwrap_or_else(|| unreachable!("checked above"));
                dead_writers.insert(crashed.writer_id());
                report.failed += node.pending.drain(..).count() as u64;
            }
        }
        if random.below(1_000) < RECOVER_PER_MILLE
            && let Some(writer) = dead_writers.iter().next().copied()
        {
            match recover(&target, &sim.node_env(0), writer, Window::DEFAULT_LENGTH).await {
                Ok(Recovery::Recovered { .. } | Recovery::AlreadyEnded) => {
                    dead_writers.remove(&writer);
                }
                Ok(_) => {}
                Err(_) => report.failed += 1,
            }
        }
        if random.below(1_000) < INTAKE_PER_MILLE {
            run_intake_twice(sim, &mut report).await;
        }
        if random.below(1_000) < READ_PER_MILLE {
            check_view(&pristine_pools, &format, sim, &acked).await?;
        }
        sim.clock().advance(TICK);
    }

    quiesce(
        sim,
        &mut nodes,
        &mut acked,
        dead_writers,
        &mut report,
        &target,
        &pristine_pools,
        &format,
    )
    .await?;
    drop(faulty_pools);

    report.acknowledged = acked.len() as u64;
    report.faults = sim.faults_injected();
    Ok(report)
}

/// No faults, every ingester shut down, every dead writer recovered,
/// everything adopted: then no writer may remain registered and the view
/// must come from files alone.
#[allow(clippy::too_many_arguments)]
async fn quiesce(
    sim: &Sim,
    nodes: &mut [Node],
    acked: &mut BTreeSet<u64>,
    dead_writers: BTreeSet<WriterId>,
    report: &mut Report,
    target: &FlushTarget,
    pristine_pools: &Pools,
    format: &Arc<dyn FileFormat>,
) -> Result<(), Violation> {
    sim.set_faults(FaultPolicy::NONE);
    for node in nodes.iter_mut() {
        if let Some(mut ingester) = node.ingester.take() {
            ingester
                .shutdown()
                .await
                .map_err(|e| Violation(format!("shutdown: {e}")))?;
            settle(node, acked, report);
        }
    }
    for writer in dead_writers {
        let outcome = recover(target, &sim.node_env(0), writer, Window::DEFAULT_LENGTH)
            .await
            .map_err(|e| Violation(format!("final recovery: {e}")))?;
        ensure!(
            matches!(outcome, Recovery::Recovered { .. } | Recovery::AlreadyEnded),
            "final recovery of {writer}: {outcome:?}"
        );
    }
    for _ in 0..4 {
        run_intake_twice(sim, report).await;
    }
    let remaining = registry::list(&*sim.pristine(), &TenantId::DEFAULT, &dataset())
        .await
        .map_err(|e| Violation(format!("registry: {e}")))?;
    ensure!(
        remaining.is_empty(),
        "writers still registered after every log ended: {remaining:?}"
    );
    let final_view = check_view(pristine_pools, format, sim, acked).await?;
    ensure!(
        final_view.sources.segments == 0,
        "the final view still reads segments"
    );
    ensure!(final_view.len() >= acked.len(), "the final view lost rows");

    Ok(())
}

fn new_ingester(sim: &Sim, index: u32, format: &Arc<dyn FileFormat>) -> Ingester {
    Ingester::start(
        sim.store(),
        sim.node_env(index),
        config(),
        pool(),
        Arc::clone(format),
    )
}

fn batch(ids: &[u64], now: tessera_types::Timestamp) -> EventBatch {
    let rows: Vec<LogRow> = ids
        .iter()
        .map(|id| LogRow {
            timestamp: now,
            observed_timestamp: now,
            event_id: event_id(*id),
            body: Some(format!("row {id}")),
            ..LogRow::default()
        })
        .collect();
    EventBatch::from_logs(TenantId::DEFAULT, dataset(), &rows)
        .unwrap_or_else(|e| unreachable!("rows are valid: {e}"))
}

fn event_id(id: u64) -> [u8; 16] {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&id.to_le_bytes());
    bytes
}

/// Moves resolved acknowledgements into `acked`; all ids of a batch share
/// its acknowledgement.
fn settle(node: &mut Node, acked: &mut BTreeSet<u64>, report: &mut Report) {
    let mut still_pending = Vec::new();
    for (ids, mut ack) in node.pending.drain(..) {
        match ack.try_now() {
            Some(Ok(_)) => acked.extend(ids),
            Some(Err(_)) => report.rejected += ids.len() as u64,
            None => still_pending.push((ids, ack)),
        }
    }
    node.pending = still_pending;
}

/// Runs intake for every registered writer twice concurrently: two owners
/// that both believe they own the work (AR-6).
async fn run_intake_twice(sim: &Sim, report: &mut Report) {
    let Ok(writers) = registry::list(&*sim.store(), &TenantId::DEFAULT, &dataset()).await else {
        report.failed += 1;
        return;
    };
    let (env_a, env_b, dataset) = (sim.node_env(0), sim.node_env(1), dataset());
    for registration in writers {
        let a = intake::run(
            sim.store(),
            &env_a,
            &TenantId::DEFAULT,
            &dataset,
            registration.writer,
        );
        let b = intake::run(
            sim.store(),
            &env_b,
            &TenantId::DEFAULT,
            &dataset,
            registration.writer,
        );
        let (a, b) = futures::future::join(a, b).await;
        report.failed += u64::from(a.is_err()) + u64::from(b.is_err());
    }
}

/// Reads the view from the pristine store and checks VW-1 against the
/// acknowledged ids.
async fn check_view(
    pools: &Pools,
    format: &Arc<dyn FileFormat>,
    sim: &Sim,
    acked: &BTreeSet<u64>,
) -> Result<view::View, Violation> {
    let read = view::read(
        pools,
        format,
        &sim.node_env(0),
        &TenantId::DEFAULT,
        &dataset(),
        Kind::Log,
        Window::DEFAULT_LENGTH,
    )
    .await
    .map_err(|e| Violation(format!("reading the view: {e}")))?;
    let mut seen: BTreeMap<u64, usize> = BTreeMap::new();
    for rows in &read.rows {
        let Some(column) = rows
            .column_by_name(EVENT_ID)
            .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>())
        else {
            return Err(Violation("view rows have no event id".into()));
        };
        for i in 0..column.len() {
            let id = u64::from_le_bytes(column.value(i)[..8].try_into().unwrap_or([0; 8]));
            *seen.entry(id).or_insert(0) += 1;
        }
    }
    if let Some((id, count)) = seen.iter().find(|(_, count)| **count > 1) {
        return Err(Violation(format!(
            "row {id} appears {count} times in the view"
        )));
    }
    if let Some(missing) = acked.iter().find(|id| !seen.contains_key(id)) {
        return Err(Violation(format!(
            "acknowledged row {missing} is missing from the view"
        )));
    }
    Ok(read)
}
