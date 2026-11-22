// SPDX-License-Identifier: Apache-2.0

//! Self-observability (section 14.7): a node writes its own logs and
//! metrics to the `_system` tenant. The load signals (OP-5) go to
//! `_system.metrics` every interval as gauges labelled by node; the
//! node's log events of level info and above go to `_system.logs`
//! through a `tracing` layer that hands them to a forwarder, which
//! batches them once a second. Both write through the node's own
//! write path. A full channel drops log events rather than slow the
//! node, and the forwarder's own failures are not logged at a level it
//! would forward, so nothing feeds back.

use std::collections::BTreeMap;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use tessera_cluster::load::Snapshot;
use tessera_server::AppState;
use tessera_types::event::{MetricType, Scalar, Temporality};
use tessera_types::{
    Attributes, DatasetName, EventBatch, LogRow, MetricRow, NodeId, Random, TenantId, Timestamp,
    stable_hash,
};
use tokio::sync::mpsc;
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;

/// How often the load signals are written by default.
pub const DEFAULT_INTERVAL: Duration = Duration::from_secs(15);
/// The dataset of the `_system` tenant that holds the node metrics.
pub const METRICS: &str = "metrics";
/// The dataset of the `_system` tenant that holds the node logs.
pub const LOGS: &str = "logs";
/// How many log events wait to be written before new ones are dropped.
const LOG_QUEUE: usize = 4_096;
/// How long log events are gathered before a write.
const LOG_LINGER: Duration = Duration::from_secs(1);

/// A point of a load signal: its metric, its value and, where the
/// signal has one, the label that tells its series apart and the
/// label's value.
type Point = (String, f64, Option<(&'static str, String)>);

/// The load signals as metric points, one gauge per signal, one per
/// kind of work for the slots and one per operation for the calls to
/// the store, all labelled with the node. The counts are far below
/// 2^53, so they lose nothing as floats.
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn metric_rows(
    snapshot: &Snapshot,
    commit_conflicts: u64,
    node: NodeId,
    now: Timestamp,
) -> Vec<MetricRow> {
    let mut points: Vec<Point> = vec![
        (
            "tessera_ingest_queue_depth".into(),
            snapshot.ingest_queue_depth as f64,
            None,
        ),
        (
            "tessera_write_ack_seconds".into(),
            snapshot.write_ack.as_secs_f64(),
            None,
        ),
        (
            "tessera_commit_conflicts_total".into(),
            commit_conflicts as f64,
            None,
        ),
        (
            "tessera_query_queue_seconds".into(),
            snapshot.query_queue.as_secs_f64(),
            None,
        ),
        (
            "tessera_compaction_backlog".into(),
            snapshot.compaction_backlog as f64,
            None,
        ),
        (
            "tessera_intake_backlog".into(),
            snapshot.intake_backlog as f64,
            None,
        ),
        (
            "tessera_evaluator_lag_seconds".into(),
            snapshot.evaluator_lag.as_secs_f64(),
            None,
        ),
        (
            "tessera_rules_degraded".into(),
            snapshot.degraded_rules as f64,
            None,
        ),
        (
            "tessera_alert_latency_seconds".into(),
            snapshot.alert_latency.as_secs_f64(),
            None,
        ),
    ];
    let cache = snapshot.object_cache;
    for (name, value) in [
        ("tessera_object_cache_hits_total", cache.hits),
        ("tessera_object_cache_misses_total", cache.misses),
        ("tessera_object_cache_bytes", cache.bytes),
    ] {
        points.push((name.into(), value as f64, None));
    }
    for (kind, held) in &snapshot.slots {
        points.push((
            "tessera_slots_owned".into(),
            *held as f64,
            Some(("kind", kind.to_string())),
        ));
    }
    for (operation, calls) in &snapshot.store_requests {
        points.push((
            "tessera_object_store_requests_total".into(),
            *calls as f64,
            Some(("operation", (*operation).to_owned())),
        ));
    }
    points
        .into_iter()
        .map(|(name, value, label)| {
            let mut attributes: Attributes = BTreeMap::new();
            attributes.insert("attr.node".into(), Scalar::Text(node.to_string()));
            let series = match &label {
                Some((label, of)) => {
                    attributes.insert(format!("attr.{label}"), Scalar::Text(of.clone()));
                    format!("{name}/{node}/{of}")
                }
                None => format!("{name}/{node}"),
            };
            let mut series_id = [0u8; 16];
            series_id[..8].copy_from_slice(&stable_hash(series.as_bytes()).to_be_bytes());
            MetricRow {
                timestamp: now,
                observed_timestamp: now,
                name,
                metric_type: MetricType::Gauge,
                value: Some(value),
                temporality: Temporality::Unspecified,
                series_id,
                attributes,
                ..MetricRow::default()
            }
        })
        .collect()
}

/// Writes the load signals to `_system.metrics` every `interval`.
pub async fn run_metrics(state: AppState, interval: Duration) {
    let mut ticks = tokio::time::interval(interval);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let Ok(dataset) = DatasetName::new(METRICS) else {
        return;
    };
    loop {
        ticks.tick().await;
        let now = state.env.clock.now();
        let rows = metric_rows(
            &state.load.snapshot(),
            tessera_commit::conflicts(),
            state.env.node,
            now,
        );
        let Ok(batch) = EventBatch::from_metrics(TenantId::SYSTEM, dataset.clone(), &rows) else {
            continue;
        };
        if let Err(error) = state.write(batch).await {
            tracing::debug!(error = %error.message(), "node metrics not written");
        }
    }
}

/// A `tracing` layer that hands log events to the forwarder.
pub struct SystemLogLayer {
    sender: mpsc::Sender<LogRow>,
}

static PENDING: Mutex<Option<mpsc::Receiver<LogRow>>> = Mutex::new(None);

/// The layer to add to the process's subscriber; the node picks the
/// other end up when it starts. Install once per process.
#[must_use]
pub fn install() -> SystemLogLayer {
    let (sender, receiver) = mpsc::channel(LOG_QUEUE);
    *PENDING.lock().unwrap_or_else(PoisonError::into_inner) = Some(receiver);
    SystemLogLayer { sender }
}

/// The forwarder's end of the channel [`install`] made, once.
#[must_use]
pub fn take_logs() -> Option<mpsc::Receiver<LogRow>> {
    PENDING
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take()
}

impl SystemLogLayer {
    /// A layer sending to `sender`, for tests.
    #[must_use]
    pub fn new(sender: mpsc::Sender<LogRow>) -> Self {
        SystemLogLayer { sender }
    }
}

impl<S: Subscriber> Layer<S> for SystemLogLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let metadata = event.metadata();
        if *metadata.level() > Level::INFO || metadata.target().starts_with(module_path!()) {
            return;
        }
        let mut fields = Fields::default();
        event.record(&mut fields);
        let (severity_number, severity_text) = match *metadata.level() {
            Level::ERROR => (17, "ERROR"),
            Level::WARN => (13, "WARN"),
            _ => (9, "INFO"),
        };
        let mut attributes = fields.attributes;
        attributes.insert(
            "attr.target".into(),
            Scalar::Text(metadata.target().to_owned()),
        );
        let row = LogRow {
            timestamp: Timestamp::from_system_time(std::time::SystemTime::now()),
            observed_timestamp: Timestamp::from_system_time(std::time::SystemTime::now()),
            severity_number: Some(severity_number),
            severity_text: Some(severity_text.into()),
            body: Some(fields.message),
            attributes,
            ..LogRow::default()
        };
        let _ = self.sender.try_send(row);
    }
}

/// The message and fields of one event.
#[derive(Default)]
struct Fields {
    message: String,
    attributes: Attributes,
}

impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        } else {
            self.attributes.insert(
                format!("attr.{}", field.name()),
                Scalar::Text(format!("{value:?}")),
            );
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            value.clone_into(&mut self.message);
        } else {
            self.attributes.insert(
                format!("attr.{}", field.name()),
                Scalar::Text(value.to_owned()),
            );
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.attributes
            .insert(format!("attr.{}", field.name()), Scalar::Int(value));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.attributes.insert(
            format!("attr.{}", field.name()),
            Scalar::Int(i64::try_from(value).unwrap_or(i64::MAX)),
        );
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.attributes
            .insert(format!("attr.{}", field.name()), Scalar::Float(value));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.attributes
            .insert(format!("attr.{}", field.name()), Scalar::Bool(value));
    }
}

/// Writes the events `receiver` delivers to `_system.logs`, a batch per
/// second, until the channel closes.
pub async fn run_logs(state: AppState, mut receiver: mpsc::Receiver<LogRow>) {
    let Ok(dataset) = DatasetName::new(LOGS) else {
        return;
    };
    let mut pending: Vec<LogRow> = Vec::new();
    loop {
        let Some(first) = receiver.recv().await else {
            break;
        };
        pending.push(first);
        let deadline = tokio::time::Instant::now() + LOG_LINGER;
        while let Ok(Some(row)) = tokio::time::timeout_at(deadline, receiver.recv()).await {
            pending.push(row);
        }
        for row in &mut pending {
            row.event_id = state.env.random.clone().next_u128().to_be_bytes();
        }
        if let Ok(batch) = EventBatch::from_logs(TenantId::SYSTEM, dataset.clone(), &pending)
            && let Err(error) = state.write(batch).await
        {
            tracing::debug!(error = %error.message(), "node logs not written");
        }
        pending.clear();
    }
}

#[cfg(test)]
mod tests {
    use tessera_types::WorkKind;
    use tracing_subscriber::layer::SubscriberExt;

    use super::*;

    #[test]
    fn the_load_signals_become_gauges_labelled_by_node() {
        let snapshot = Snapshot {
            ingest_queue_depth: 2,
            write_ack: Duration::from_millis(40),
            query_queue: Duration::from_millis(250),
            compaction_backlog: 1,
            intake_backlog: 0,
            evaluator_lag: Duration::from_secs(3),
            degraded_rules: 0,
            object_cache: tessera_cluster::ObjectCache {
                hits: 30,
                misses: 10,
                bytes: 4_096,
            },
            store_requests: vec![("get", 12), ("list", 5)],
            slots: BTreeMap::from([(WorkKind::Partition, 4), (WorkKind::Intake, 1)]),
            alert_latency: Duration::from_millis(1_200),
        };
        let rows = metric_rows(&snapshot, 3, NodeId::from_u128(7), Timestamp::from_nanos(1));
        assert_eq!(rows.len(), 16);
        // What the node asks of its store and what its cache saves it
        // are what an idle cluster costs: they are recorded too.
        let of =
            |name: &str| -> Vec<&MetricRow> { rows.iter().filter(|r| r.name == name).collect() };
        assert_eq!(of("tessera_object_cache_hits_total")[0].value, Some(30.0));
        assert_eq!(of("tessera_object_cache_misses_total")[0].value, Some(10.0));
        assert_eq!(of("tessera_object_cache_bytes")[0].value, Some(4_096.0));
        let requests = of("tessera_object_store_requests_total");
        assert_eq!(requests.len(), 2);
        assert!(requests.iter().any(|r| r.attributes["attr.operation"]
            == Scalar::Text("get".into())
            && r.value == Some(12.0)));
        assert_ne!(requests[0].series_id, requests[1].series_id);
        let latency = rows
            .iter()
            .find(|r| r.name == "tessera_alert_latency_seconds")
            .unwrap();
        assert_eq!(latency.value, Some(1.2));
        let conflicts = rows
            .iter()
            .find(|r| r.name == "tessera_commit_conflicts_total")
            .unwrap();
        assert_eq!(conflicts.value, Some(3.0));
        let queue = rows
            .iter()
            .find(|r| r.name == "tessera_query_queue_seconds")
            .unwrap();
        assert_eq!(queue.value, Some(0.25));
        assert_eq!(queue.metric_type, MetricType::Gauge);
        assert_eq!(
            queue.attributes["attr.node"],
            Scalar::Text(NodeId::from_u128(7).to_string())
        );
        let slots: Vec<&MetricRow> = rows
            .iter()
            .filter(|r| r.name == "tessera_slots_owned")
            .collect();
        assert_eq!(slots.len(), 2);
        assert!(slots.iter().any(|r| r.attributes["attr.kind"]
            == Scalar::Text("partition".into())
            && r.value == Some(4.0)));
        assert_ne!(
            slots[0].series_id, slots[1].series_id,
            "one series per kind"
        );
    }

    #[test]
    fn info_events_become_log_rows_and_debug_ones_do_not() {
        let (sender, mut receiver) = mpsc::channel(8);
        let subscriber = tracing_subscriber::registry().with(SystemLogLayer::new(sender));
        // Events of this module itself are never forwarded, so nothing
        // the forwarder logs can feed back; the test speaks as another.
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(target: "tessera_server::admin", tenant = "acme", rows = 3, "tenant created");
            tracing::debug!(target: "tessera_server::admin", "not forwarded");
            tracing::warn!(target: "tessera_server::admin", "slow");
            tracing::info!("from the forwarder's own module");
        });
        let first = receiver.try_recv().unwrap();
        assert_eq!(first.body.as_deref(), Some("tenant created"));
        assert_eq!(first.severity_text.as_deref(), Some("INFO"));
        assert_eq!(first.attributes["attr.tenant"], Scalar::Text("acme".into()));
        assert_eq!(first.attributes["attr.rows"], Scalar::Int(3));
        assert_eq!(
            first.attributes["attr.target"],
            Scalar::Text("tessera_server::admin".into())
        );
        let second = receiver.try_recv().unwrap();
        assert_eq!(second.severity_number, Some(13));
        assert!(
            receiver.try_recv().is_err(),
            "neither debug nor this module's own events were forwarded"
        );
    }
}
