// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The advisor's loop (spec section 8): every interval, for each
//! tenant, dataset and kind whose partition slot this node holds, the
//! workload log is read back from `_system.queries`, the files are
//! summarised from the manifests, pins are taken from the control store
//! and the advisor runs. Its report goes to the dataset log for
//! `EXPLAIN LAYOUT` (AD-11); a layout it adopts becomes the next epoch
//! (AD-9) and the partition window is set for partitions to come
//! (AD-14). Compaction then writes new files in the layout; old files
//! stay valid until re-clustered. Aggregation shapes the workload
//! repeats become rollups (section 8.7), one owner per tenant.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, Float64Array, Int64Array, StringArray, TimestampNanosecondArray};
use tessera_advisor::rollups::{RepeatedShape, propose};
use tessera_advisor::{FileSummary, Inputs, Observation, Pins, advise};
use tessera_catalog::{Catalog, ControlError, ControlStores, Table, TenantStatus};
use tessera_eval::rule::decode_rows;
use tessera_eval::{Origin, Rollup};
use tessera_exec::{Engine, Response, query};
use tessera_ir::{BindContext, RollupDefinition};
use tessera_manifest::{DatasetLogs, Manifests, list_partitions, registry};
use tessera_object::ObjectStore;
use tessera_sql::SqlFrontend;
use tessera_types::{DatasetName, Env, Kind, TenantId, Timestamp, WorkKind};

use crate::leases::Owned;

/// How often the advisor runs by default.
pub const DEFAULT_INTERVAL: Duration = Duration::from_secs(600);
/// How far back the workload is read.
const LOOKBACK: Duration = Duration::from_secs(30 * 24 * 3600);
/// The most workload events read per run.
const MAX_OBSERVATIONS: usize = 20_000;
/// How far back repeated aggregation shapes are counted (section 8.7).
const SHAPE_LOOKBACK: Duration = Duration::from_secs(24 * 3600);

type Failure = Box<dyn std::error::Error + Send + Sync>;

/// What the loop needs.
pub struct Advisor {
    /// The root pool's store.
    pub store: Arc<dyn ObjectStore>,
    /// The node's environment.
    pub env: Env,
    /// The catalog, for the tenants.
    pub catalog: Arc<Catalog>,
    /// The control stores, for pins.
    pub controls: Arc<ControlStores>,
    /// The dataset logs, for layouts and reports.
    pub datasets: Arc<DatasetLogs>,
    /// The engine the workload log is read through.
    pub engine: Arc<Engine>,
    /// The slots this node owns.
    pub owned: Owned,
    /// How often a round runs.
    pub interval: Duration,
}

/// Runs rounds forever. Failures are logged and retried next round.
pub async fn run(advisor: Advisor) {
    let manifests = Manifests::new(Arc::clone(&advisor.store), advisor.env.clone(), 1024);
    let mut ticks = tokio::time::interval(advisor.interval);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticks.tick().await;
        let tenants: Vec<TenantId> = advisor
            .catalog
            .cached()
            .state()
            .tenants()
            .filter(|t| t.status == TenantStatus::Active && t.id != TenantId::SYSTEM)
            .filter(|t| t.settings.features.advisor())
            .map(|t| t.id.clone())
            .collect();
        for tenant in tenants {
            if let Err(error) = round(&advisor, &manifests, &tenant).await {
                tracing::warn!(%tenant, error = %error, "advisor round failed");
            }
        }
    }
}

async fn round(advisor: &Advisor, manifests: &Manifests, tenant: &TenantId) -> Result<(), Failure> {
    let rollups_key = format!("{tenant}/rollups");
    if advisor
        .owned
        .owns(WorkKind::Partition, rollups_key.as_bytes())
    {
        propose_rollups(advisor, tenant).await?;
    }
    for dataset in registry::datasets(&*advisor.store, tenant).await? {
        for kind in Kind::ALL {
            let key = format!("{tenant}/{dataset}/{kind}");
            if !advisor.owned.owns(WorkKind::Partition, key.as_bytes()) {
                continue;
            }
            let files = file_summaries(advisor, manifests, tenant, &dataset, kind).await?;
            if files.is_empty() {
                continue;
            }
            advise_dataset(advisor, tenant, &dataset, kind, files).await?;
        }
    }
    Ok(())
}

/// One run for one dataset and kind.
async fn advise_dataset(
    advisor: &Advisor,
    tenant: &TenantId,
    dataset: &DatasetName,
    kind: Kind,
    files: Vec<FileSummary>,
) -> Result<(), Failure> {
    let now = advisor.env.clock.now();
    let log = advisor.datasets.open(tenant, dataset);
    let current = log.layout(kind).await?;
    let previous = log.advice(kind).await?;
    let observations = read_observations(&advisor.engine, tenant, dataset, kind, now).await?;
    let pins = pins_of(advisor, tenant, dataset, kind).await?;
    let outcome = advise(&Inputs {
        kind,
        current: current.clone(),
        files,
        observations,
        pins,
        now,
        previous,
    });
    log.set_advice(kind, &outcome.advice).await?;
    log.set_value_score(kind, outcome.value_score).await?;
    if let Some(record) = outcome.adopt {
        let set = log.set_layout(kind, record).await?;
        tracing::info!(%tenant, %dataset, %kind, epoch = set.epoch, sort_key = ?set.sort_key, dedicated = ?set.dedicated, "layout adopted");
    }
    if outcome.window != log.window().await? {
        log.set_window(outcome.window).await?;
        tracing::info!(%tenant, %dataset, window_seconds = outcome.window.as_secs(), "partition window set");
    }
    Ok(())
}

/// The aggregation shapes the tenant's workload repeated in the last
/// day, each made a rollup unless one covers it (section 8.7).
async fn propose_rollups(advisor: &Advisor, tenant: &TenantId) -> Result<(), Failure> {
    let shapes = read_shapes(&advisor.engine, tenant, advisor.env.clock.now()).await?;
    if shapes.is_empty() {
        return Ok(());
    }
    let control = advisor.controls.open(tenant);
    let (rollups, _) = decode_rows::<Rollup>(&control.rows(Table::Rollups).await?);
    let existing: Vec<(String, String)> = rollups
        .iter()
        .map(|r| (r.dataset.clone(), r.expr.clone()))
        .collect();
    for proposal in propose(&shapes, &existing) {
        let definition = RollupDefinition {
            name: proposal.name.clone(),
            every: None,
            target: None,
            query: proposal.sql.clone(),
            backfill: None,
            dataset: DatasetName::new(&proposal.dataset)?,
        };
        let rollup = match Rollup::from_definition(&definition, tenant) {
            Ok(rollup) => Rollup {
                origin: Origin::Advisor,
                ..rollup
            },
            Err(reason) => {
                tracing::debug!(%tenant, sql = %proposal.sql, reason, "shape is not a rollup");
                continue;
            }
        };
        let document = serde_json::to_value(&rollup)?;
        control
            .transact(|txn| {
                if txn.get(Table::Rollups, &rollup.name)?.is_some() {
                    return Err(ControlError::Rejected("exists".into()));
                }
                txn.put(Table::Rollups, &rollup.name, &document);
                Ok(())
            })
            .await?;
        tracing::info!(%tenant, rollup = %rollup.name, dataset = %rollup.dataset, sql = %rollup.expr, "rollup proposed");
    }
    Ok(())
}

/// The aggregation shapes of the tenant's queries in the last day, with
/// how often each ran.
async fn read_shapes(
    engine: &Engine,
    tenant: &TenantId,
    now: Timestamp,
) -> Result<Vec<RepeatedShape>, Failure> {
    let sql = format!(
        "SELECT attr.dataset AS dataset, attr.shape AS shape, count(*) AS n \
         FROM events WHERE kind = 'event' AND attr.tenant = '{tenant}' AND attr.shape IS NOT NULL \
         AND timestamp > now() - INTERVAL '{} seconds' \
         GROUP BY attr.dataset, attr.shape",
        SHAPE_LOOKBACK.as_secs()
    );
    let ctx = BindContext::new(
        TenantId::SYSTEM,
        DatasetName::new(crate::WORKLOAD_DATASET)?,
        now,
    );
    let Response::Rows(output) = query::run(engine, &SqlFrontend, &sql, &ctx).await? else {
        return Ok(Vec::new());
    };
    let mut shapes = Vec::new();
    for batch in &output.batches {
        let column = |name: &str| batch.column_by_name(name);
        let (Some(datasets), Some(texts), Some(counts)) = (
            column("dataset").and_then(|c| c.as_any().downcast_ref::<StringArray>()),
            column("shape").and_then(|c| c.as_any().downcast_ref::<StringArray>()),
            column("n").and_then(|c| c.as_any().downcast_ref::<Float64Array>()),
        ) else {
            continue;
        };
        for i in 0..batch.num_rows() {
            if datasets.is_null(i) || texts.is_null(i) {
                continue;
            }
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let repeats = counts.value(i).max(0.0).round() as u64;
            shapes.push(RepeatedShape {
                dataset: datasets.value(i).to_owned(),
                sql: texts.value(i).to_owned(),
                repeats,
            });
        }
    }
    Ok(shapes)
}

/// The files of a dataset and kind, as the cost model sees them.
async fn file_summaries(
    advisor: &Advisor,
    manifests: &Manifests,
    tenant: &TenantId,
    dataset: &DatasetName,
    kind: Kind,
) -> Result<Vec<FileSummary>, Failure> {
    let mut files = Vec::new();
    for partition in list_partitions(&*advisor.store, tenant, dataset, kind).await? {
        let snapshot = manifests.open(&partition).snapshot().await?;
        files.extend(snapshot.state().files().map(|f| FileSummary {
            size_bytes: f.size_bytes,
            sort_key: f.sort_key.clone(),
            epoch: f.layout_epoch,
            rows: f.row_count,
            window_start: partition.window.start(),
        }));
    }
    Ok(files)
}

/// The pins of a dataset and kind: the `layout_pins` row keyed
/// `{dataset}/{kind}` (AD-12).
async fn pins_of(
    advisor: &Advisor,
    tenant: &TenantId,
    dataset: &DatasetName,
    kind: Kind,
) -> Result<Option<Pins>, Failure> {
    let key = format!("{dataset}/{kind}");
    let Some(document) = advisor
        .controls
        .open(tenant)
        .get(Table::LayoutPins, &key)
        .await?
    else {
        return Ok(None);
    };
    Ok(serde_json::from_value(document).ok())
}

/// The workload events of a dataset and kind from the last month.
async fn read_observations(
    engine: &Engine,
    tenant: &TenantId,
    dataset: &DatasetName,
    kind: Kind,
    now: Timestamp,
) -> Result<Vec<Observation>, Failure> {
    let sql = format!(
        "SELECT timestamp, attr.columns AS columns, attr.predicates AS predicates, \
         attr.group_by AS group_by, attr.bytes_scanned::bigint AS bytes_scanned, \
         attr.rows_scanned::bigint AS rows_scanned, attr.selectivity::float AS selectivity \
         FROM events WHERE kind = 'event' AND attr.tenant = '{tenant}' AND attr.dataset = '{dataset}' \
         AND attr.kind = '{kind}' AND timestamp > now() - INTERVAL '{} seconds' \
         ORDER BY timestamp DESC LIMIT {MAX_OBSERVATIONS}",
        LOOKBACK.as_secs()
    );
    let ctx = BindContext::new(
        TenantId::SYSTEM,
        DatasetName::new(crate::WORKLOAD_DATASET)?,
        now,
    );
    let Response::Rows(output) = query::run(engine, &SqlFrontend, &sql, &ctx).await? else {
        return Ok(Vec::new());
    };
    let mut observations = Vec::new();
    for batch in &output.batches {
        let column = |name: &str| batch.column_by_name(name);
        let (Some(times), Some(columns), Some(predicates), Some(group_by)) = (
            column("timestamp").and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>()),
            column("columns").and_then(|c| c.as_any().downcast_ref::<StringArray>()),
            column("predicates").and_then(|c| c.as_any().downcast_ref::<StringArray>()),
            column("group_by").and_then(|c| c.as_any().downcast_ref::<StringArray>()),
        ) else {
            continue;
        };
        let bytes = column("bytes_scanned").and_then(|c| c.as_any().downcast_ref::<Int64Array>());
        let rows = column("rows_scanned").and_then(|c| c.as_any().downcast_ref::<Int64Array>());
        let selectivity =
            column("selectivity").and_then(|c| c.as_any().downcast_ref::<Float64Array>());
        for i in 0..batch.num_rows() {
            let text = |array: &StringArray| {
                if array.is_null(i) {
                    String::new()
                } else {
                    array.value(i).to_owned()
                }
            };
            let predicate_uses: Vec<tessera_exec::PredicateUse> =
                serde_json::from_str(&text(predicates)).unwrap_or_default();
            let group_by: Vec<String> = serde_json::from_str(&text(group_by)).unwrap_or_default();
            let predicates: Vec<(String, String)> = predicate_uses
                .iter()
                .map(|p| (p.column.clone(), p.op.clone()))
                .collect();
            let summary = format!(
                "predicates {:?}, group by {:?}",
                predicates
                    .iter()
                    .map(|(c, o)| format!("{c} {o}"))
                    .collect::<Vec<_>>(),
                group_by
            );
            observations.push(Observation {
                at: Timestamp::from_nanos(times.value(i)),
                columns: serde_json::from_str::<BTreeSet<String>>(&text(columns))
                    .unwrap_or_default(),
                predicates,
                group_by,
                bytes_scanned: bytes
                    .filter(|b| !b.is_null(i))
                    .map_or(0, |b| u64::try_from(b.value(i)).unwrap_or(0)),
                rows_scanned: rows
                    .filter(|r| !r.is_null(i))
                    .map_or(0, |r| u64::try_from(r.value(i)).unwrap_or(0)),
                selectivity: selectivity.filter(|s| !s.is_null(i)).map(|s| s.value(i)),
                text: summary,
            });
        }
    }
    Ok(observations)
}
