// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Load signals (OP-5): what an autoscaler needs from every node. The
//! roles set the gauges as they work, writes and queries update them
//! as they pass, and the server exports them in the Prometheus text
//! format at `/metrics`:
//!
//! | Gauge | Meaning |
//! |-------|---------|
//! | `tessera_ingest_queue_depth` | Writes in flight on this node |
//! | `tessera_query_queue_seconds` | Time the last queries waited for a turn, smoothed |
//! | `tessera_compaction_backlog` | Compaction jobs due but deferred by the budget in the last round |
//! | `tessera_intake_backlog` | Writers whose logs the last round had to recover |
//! | `tessera_evaluator_lag_seconds` | How far the slowest owned rule group is behind |
//! | `tessera_slots_owned{kind}` | Slots this node holds, per kind of work |

use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tessera_types::WorkKind;

/// The gauges of one node.
#[derive(Default)]
pub struct Load {
    ingest_inflight: AtomicU64,
    query_wait_nanos: AtomicU64,
    compaction_backlog: AtomicU64,
    intake_backlog: AtomicU64,
    evaluator_lag_nanos: AtomicU64,
    slots: Mutex<BTreeMap<WorkKind, u64>>,
}

/// A write in flight; dropping it counts the write as done.
pub struct Inflight(Arc<Load>);

impl Drop for Inflight {
    fn drop(&mut self) {
        self.0.ingest_inflight.fetch_sub(1, Ordering::Relaxed);
    }
}

/// The gauges as read at one moment.
#[derive(Clone, Debug, PartialEq)]
pub struct Snapshot {
    /// Writes in flight.
    pub ingest_queue_depth: u64,
    /// Smoothed time queries waited for a turn.
    pub query_queue: Duration,
    /// Compaction jobs deferred by the budget in the last round.
    pub compaction_backlog: u64,
    /// Writers recovered in the last round.
    pub intake_backlog: u64,
    /// How far the slowest owned rule group is behind.
    pub evaluator_lag: Duration,
    /// Slots held per kind of work.
    pub slots: BTreeMap<WorkKind, u64>,
}

impl Load {
    /// Gauges at zero.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Load::default())
    }

    /// Counts a write as in flight until the returned value drops.
    #[must_use]
    pub fn ingest_started(self: &Arc<Self>) -> Inflight {
        self.ingest_inflight.fetch_add(1, Ordering::Relaxed);
        Inflight(Arc::clone(self))
    }

    /// Records how long a query waited for its turn, smoothed over the
    /// last few: one fifth of the new wait, four fifths of the old.
    pub fn query_waited(&self, wait: Duration) {
        let wait = u64::try_from(wait.as_nanos()).unwrap_or(u64::MAX);
        let _ = self
            .query_wait_nanos
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                Some(old / 5 * 4 + wait / 5)
            });
    }

    /// Sets the compaction jobs deferred in the last round.
    pub fn set_compaction_backlog(&self, jobs: u64) {
        self.compaction_backlog.store(jobs, Ordering::Relaxed);
    }

    /// Sets the writers recovered in the last round.
    pub fn set_intake_backlog(&self, writers: u64) {
        self.intake_backlog.store(writers, Ordering::Relaxed);
    }

    /// Sets how far the slowest owned rule group is behind.
    pub fn set_evaluator_lag(&self, lag: Duration) {
        self.evaluator_lag_nanos.store(
            u64::try_from(lag.as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    /// Sets the slots held of `kind`.
    pub fn set_slots(&self, kind: WorkKind, held: u64) {
        self.slots
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(kind, held);
    }

    /// The gauges now.
    #[must_use]
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            ingest_queue_depth: self.ingest_inflight.load(Ordering::Relaxed),
            query_queue: Duration::from_nanos(self.query_wait_nanos.load(Ordering::Relaxed)),
            compaction_backlog: self.compaction_backlog.load(Ordering::Relaxed),
            intake_backlog: self.intake_backlog.load(Ordering::Relaxed),
            evaluator_lag: Duration::from_nanos(self.evaluator_lag_nanos.load(Ordering::Relaxed)),
            slots: self
                .slots
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone(),
        }
    }
}

impl Snapshot {
    /// The gauges in the Prometheus text exposition format.
    #[must_use]
    pub fn prometheus_text(&self) -> String {
        let mut out = String::new();
        let mut gauge = |name: &str, help: &str, value: String| {
            let _ = writeln!(out, "# HELP {name} {help}");
            let _ = writeln!(out, "# TYPE {name} gauge");
            let _ = writeln!(out, "{name} {value}");
        };
        gauge(
            "tessera_ingest_queue_depth",
            "Writes in flight on this node.",
            self.ingest_queue_depth.to_string(),
        );
        gauge(
            "tessera_query_queue_seconds",
            "Time the last queries waited for a turn, smoothed.",
            format!("{:.6}", self.query_queue.as_secs_f64()),
        );
        gauge(
            "tessera_compaction_backlog",
            "Compaction jobs due but deferred by the budget in the last round.",
            self.compaction_backlog.to_string(),
        );
        gauge(
            "tessera_intake_backlog",
            "Writers whose logs the last round had to recover.",
            self.intake_backlog.to_string(),
        );
        gauge(
            "tessera_evaluator_lag_seconds",
            "How far the slowest owned rule group is behind.",
            format!("{:.3}", self.evaluator_lag.as_secs_f64()),
        );
        let _ = writeln!(
            out,
            "# HELP tessera_slots_owned Slots this node holds, per kind of work."
        );
        let _ = writeln!(out, "# TYPE tessera_slots_owned gauge");
        for (kind, held) in &self.slots {
            let _ = writeln!(out, "tessera_slots_owned{{kind=\"{kind}\"}} {held}");
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_in_flight_are_counted_until_they_finish() {
        let load = Load::new();
        let first = load.ingest_started();
        let second = load.ingest_started();
        assert_eq!(load.snapshot().ingest_queue_depth, 2);
        drop(first);
        assert_eq!(load.snapshot().ingest_queue_depth, 1);
        drop(second);
        assert_eq!(load.snapshot().ingest_queue_depth, 0);
    }

    #[test]
    fn query_waits_are_smoothed() {
        let load = Load::new();
        load.query_waited(Duration::from_secs(5));
        assert_eq!(load.snapshot().query_queue, Duration::from_secs(1));
        for _ in 0..20 {
            load.query_waited(Duration::from_secs(5));
        }
        let smoothed = load.snapshot().query_queue;
        assert!(smoothed > Duration::from_millis(4_900), "{smoothed:?}");
        load.query_waited(Duration::ZERO);
        assert!(load.snapshot().query_queue < smoothed);
    }

    #[test]
    fn the_exposition_names_every_gauge() {
        let load = Load::new();
        load.set_slots(WorkKind::Partition, 3);
        load.set_compaction_backlog(2);
        load.set_evaluator_lag(Duration::from_millis(1_500));
        let text = load.snapshot().prometheus_text();
        assert!(text.contains("tessera_ingest_queue_depth 0\n"), "{text}");
        assert!(text.contains("tessera_compaction_backlog 2\n"), "{text}");
        assert!(
            text.contains("tessera_evaluator_lag_seconds 1.500\n"),
            "{text}"
        );
        assert!(
            text.contains("tessera_slots_owned{kind=\"partition\"} 3\n"),
            "{text}"
        );
        assert!(
            text.contains("# TYPE tessera_query_queue_seconds gauge\n"),
            "{text}"
        );
    }
}
