// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The `eval` role's loop: every interval, for every active tenant, the
//! alert rules in the control store are grouped and each group this
//! node owns the slot of (section 11.3, AR-4) is evaluated through the
//! engine and its outbox delivered: alert events through the node's
//! write path (CQ-14), notifications to the tenant's routes (CQ-15),
//! held back by its silences and acknowledgments. The rollups this node
//! owns are maintained the same way (section 8.7), their output written
//! through the same path. What a rule run through the engine asked of
//! its scans goes to the workload log like any query's (AD-1).

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures::future::BoxFuture;
use tessera_catalog::{
    Catalog, ControlState, ControlStores, Table, TenantSettings, TenantSettingsSource, TenantStatus,
};
use tessera_cluster::Load;
use tessera_eval::rule::decode_rows;
use tessera_eval::{
    Acknowledgment, AlertRule, AlertSink, Delivery, Inhibition, NotificationRoute, Notifier,
    Rollup, RuleGroup, Silence, Status, maintain,
};
use tessera_exec::Engine;
use tessera_object::ObjectStore;
use tessera_server::AppState;
use tessera_types::{Env, EventBatch, TenantId, WorkKind};

use crate::leases::Owned;

/// How often the loop runs by default.
pub const DEFAULT_INTERVAL: Duration = Duration::from_secs(5);

/// What the loop needs.
pub struct Evaluator {
    /// The root pool's store, where state logs live.
    pub store: Arc<dyn ObjectStore>,
    /// The node's environment.
    pub env: Env,
    /// The catalog, for the tenants.
    pub catalog: Arc<Catalog>,
    /// The tenants' control stores, for rules, routes, silences and
    /// acknowledgments.
    pub controls: Arc<ControlStores>,
    /// The engine rules run through.
    pub engine: Arc<Engine>,
    /// The slots this node owns.
    pub owned: Owned,
    /// The node's state, whose workload log the rules' runs go to.
    pub state: AppState,
    /// Where alert events go.
    pub sink: Arc<dyn AlertSink>,
    /// How routes are reached.
    pub notifier: Arc<dyn Notifier>,
    /// How often a round runs.
    pub interval: Duration,
    /// Where the evaluator's lag is reported (OP-5).
    pub load: Arc<Load>,
}

/// The open rule group partitions, by tenant, group and partition,
/// rebuilt when their rules or the group's partition count change.
#[derive(Default)]
struct Groups {
    open: HashMap<(TenantId, String, u32), Arc<RuleGroup>>,
}

/// Runs rounds forever. Failures are logged and retried next round.
pub async fn run(evaluator: Evaluator) {
    let mut groups = Groups::default();
    let mut ticks = tokio::time::interval(evaluator.interval);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticks.tick().await;
        round(&evaluator, &mut groups).await;
    }
}

/// One round over every tenant.
async fn round(evaluator: &Evaluator, groups: &mut Groups) {
    let tenants: Vec<TenantId> = evaluator
        .catalog
        .cached()
        .state()
        .tenants()
        .filter(|t| t.status == TenantStatus::Active)
        .map(|t| t.id.clone())
        .collect();
    let mut lag = Duration::ZERO;
    let mut degraded = 0;
    for tenant in tenants {
        match tenant_round(evaluator, groups, &tenant).await {
            Ok(round) => {
                lag = lag.max(round.lag);
                degraded += round.degraded;
            }
            Err(error) => {
                tracing::warn!(%tenant, error = %error, "rule evaluation round failed");
            }
        }
    }
    evaluator.load.set_evaluator_lag(lag);
    evaluator.load.set_degraded_rules(degraded);
}

/// One partition of a rule group this node owns (CQ-6).
struct Partition {
    group: String,
    part: u32,
    parts: u32,
    rules: Vec<AlertRule>,
}

/// The partitions of `tenant`'s rule groups this node owns: each is a
/// slot of its own (CQ-6), the count per group from the settings.
fn owned_partitions(
    evaluator: &Evaluator,
    tenant: &TenantId,
    by_group: BTreeMap<String, Vec<AlertRule>>,
    settings: &TenantSettings,
) -> Vec<Partition> {
    by_group
        .into_iter()
        .flat_map(|(group, rules)| {
            let parts = settings.rule_partitions_of(&group);
            (0..parts).map(move |part| Partition {
                group: group.clone(),
                part,
                parts,
                rules: rules.clone(),
            })
        })
        .filter(|p| {
            let item = format!("{tenant}/{}/{}", p.group, p.part);
            evaluator.owned.owns(WorkKind::RuleGroup, item.as_bytes())
        })
        .collect()
}

/// What one tenant's round reports (OP-5).
#[derive(Default)]
struct Round {
    /// How far the slowest owned group is behind.
    lag: Duration,
    /// Owned rules degraded after the round (section 11.7).
    degraded: u64,
}

/// The labels of every alert firing in `tenant` across `groups`, from
/// their state logs: what inhibition rules match their sources against
/// (section 11.8).
async fn firing_labels(
    evaluator: &Evaluator,
    tenant: &TenantId,
    groups: &[String],
    settings: &TenantSettings,
) -> Result<Vec<BTreeMap<String, String>>, String> {
    let mut firing = Vec::new();
    for group in groups {
        let state = tessera_eval::partition::merged(
            &evaluator.store,
            tenant,
            group,
            settings.rule_partitions_of(group),
            &evaluator.env,
        )
        .await
        .map_err(|e| e.to_string())?;
        firing.extend(
            state
                .instances
                .values()
                .filter(|i| i.status == Status::Firing)
                .map(|i| i.labels.clone()),
        );
    }
    Ok(firing)
}

/// What delivery reads of a tenant's control store.
type DeliveryInputs = (
    Vec<NotificationRoute>,
    Vec<Silence>,
    Vec<Acknowledgment>,
    Vec<Inhibition>,
);

/// The tenant's routes, silences, acknowledgments and inhibition
/// rules, as its control store holds them; rows that do not decode are
/// skipped.
fn delivery_inputs(control: &ControlState) -> Result<DeliveryInputs, String> {
    let rows = |table: Table| control.rows(table).map_err(|e| e.to_string());
    let (routes, _) = decode_rows::<NotificationRoute>(&rows(Table::NotificationRoutes)?);
    let (silences, _) = decode_rows::<Silence>(&rows(Table::Silences)?);
    let (acknowledgments, _) = decode_rows::<Acknowledgment>(&rows(Table::Acknowledgments)?);
    let (inhibitions, _) = decode_rows::<Inhibition>(&rows(Table::Inhibitions)?);
    Ok((routes, silences, acknowledgments, inhibitions))
}

/// One round for `tenant`.
async fn tenant_round(
    evaluator: &Evaluator,
    groups: &mut Groups,
    tenant: &TenantId,
) -> Result<Round, String> {
    // Read once for the round: the rules, the rollups and what
    // delivery needs are tables of the one store.
    let control = evaluator
        .controls
        .open(tenant)
        .state()
        .await
        .map_err(|e| e.to_string())?;
    let rows = control.rows(Table::AlertRules).map_err(|e| e.to_string())?;
    let (rules, bad) = decode_rows::<AlertRule>(&rows);
    for problem in bad {
        tracing::warn!(%tenant, problem, "alert rule does not decode");
    }
    let mut by_group: BTreeMap<String, Vec<AlertRule>> = BTreeMap::new();
    for rule in rules {
        by_group.entry(rule.group.clone()).or_default().push(rule);
    }
    let all_groups: Vec<String> = by_group.keys().cloned().collect();
    let settings = evaluator.catalog.settings_of(tenant);
    let owned = owned_partitions(evaluator, tenant, by_group, &settings);
    groups.open.retain(|(t, g, part), _| {
        t != tenant || owned.iter().any(|p| p.group == *g && p.part == *part)
    });
    rollups_round(evaluator, tenant, &control).await?;
    if owned.is_empty() {
        return Ok(Round::default());
    }
    let (routes, silences, acknowledgments, inhibitions) = delivery_inputs(&control)?;
    let firing = if inhibitions.is_empty() {
        Vec::new()
    } else {
        firing_labels(evaluator, tenant, &all_groups, &settings).await?
    };
    let delivery = Delivery {
        sink: &*evaluator.sink,
        notifier: &*evaluator.notifier,
        routes: &routes,
        silences: &silences,
        acknowledgments: &acknowledgments,
        inhibitions: &inhibitions,
        firing: &firing,
    };
    let mut round = Round::default();
    for Partition {
        group: name,
        part,
        parts,
        rules,
    } in owned
    {
        let key = (tenant.clone(), name.clone(), part);
        let group = match groups.open.get(&key) {
            Some(group) if group.rules() == rules.as_slice() && group.parts() == parts => {
                Arc::clone(group)
            }
            _ => {
                let group = Arc::new(RuleGroup::open_partition(
                    Arc::clone(&evaluator.store),
                    tenant.clone(),
                    name.clone(),
                    rules,
                    evaluator.env.clone(),
                    part,
                    parts,
                ));
                groups.open.insert(key, Arc::clone(&group));
                group
            }
        };
        let now = evaluator.env.clock.now();
        match group.evaluate(&evaluator.engine, now).await {
            Ok(evaluated) => {
                round.degraded += u64::try_from(evaluated.degraded.len()).unwrap_or(u64::MAX);
                settle(evaluator, tenant, &name, &group, delivery.sink, evaluated).await;
            }
            Err(error) => {
                tracing::warn!(%tenant, group = %name, error = %error, "rule group evaluation failed");
            }
        }
        match group.deliver(&delivery, now).await {
            Ok(delivered) => {
                for latency in &delivered.latencies {
                    evaluator.load.alert_sent(*latency);
                }
                if delivered.events > 0 || delivered.repeated > 0 {
                    tracing::info!(%tenant, group = %name, ?delivered, "alerts delivered");
                }
            }
            Err(error) => {
                tracing::warn!(%tenant, group = %name, error = %error, "alert delivery failed");
            }
        }
        round.lag = round
            .lag
            .max(group.lag(now).await.unwrap_or(Duration::ZERO));
    }
    Ok(round)
}

/// What follows one evaluation of `group`: its transitions and
/// degraded rules logged, its runs sent to the workload log (AD-1),
/// its recording rules' samples written through `sink`.
async fn settle(
    evaluator: &Evaluator,
    tenant: &TenantId,
    name: &str,
    group: &RuleGroup,
    sink: &dyn AlertSink,
    evaluated: tessera_eval::Evaluated,
) {
    if !evaluated.transitions.is_empty() {
        tracing::info!(%tenant, group = %name, transitions = evaluated.transitions.len(), "alert transitions");
    }
    if !evaluated.degraded.is_empty() {
        tracing::warn!(%tenant, group = %name, rules = ?evaluated.degraded, "rules degraded: windows rebuilt from the files");
    }
    for run in evaluated.runs {
        tessera_server::workload::record_uses(
            &evaluator.state,
            run.uses,
            run.language.as_str(),
            "rule",
            run.latency,
            run.fingerprint,
            None,
        );
    }
    if !evaluated.recorded.is_empty()
        && let Err(error) = group.record(sink, &evaluated.recorded).await
    {
        tracing::warn!(%tenant, group = %name, error = %error, "recording rules not written");
    }
}

/// The rollups of `tenant` this node owns, each brought up to date.
async fn rollups_round(
    evaluator: &Evaluator,
    tenant: &TenantId,
    control: &ControlState,
) -> Result<(), String> {
    let rows = control.rows(Table::Rollups).map_err(|e| e.to_string())?;
    let (rollups, bad) = decode_rows::<Rollup>(&rows);
    for problem in bad {
        tracing::warn!(%tenant, problem, "rollup does not decode");
    }
    let now = evaluator.env.clock.now();
    for rollup in rollups.into_iter().filter(|r| r.enabled) {
        let slot = format!("{tenant}/rollup-{}", rollup.name);
        if !evaluator.owned.owns(WorkKind::RuleGroup, slot.as_bytes()) {
            continue;
        }
        match maintain(
            &evaluator.engine,
            Arc::clone(&evaluator.store),
            &evaluator.env,
            tenant,
            &rollup,
            &*evaluator.sink,
            now,
        )
        .await
        {
            Ok(maintained) if maintained.rows > 0 => {
                tracing::info!(%tenant, rollup = %rollup.name, windows = maintained.windows, rows = maintained.rows, "rollup maintained");
            }
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(%tenant, rollup = %rollup.name, error = %error, "rollup maintenance failed");
            }
        }
    }
    Ok(())
}

/// Alert events written through the node's own write path, so they are
/// forwarded and admitted like any other write.
pub struct StateSink {
    state: AppState,
    seen: Mutex<usize>,
}

impl StateSink {
    /// A sink over `state`.
    #[must_use]
    pub fn new(state: AppState) -> Self {
        StateSink {
            state,
            seen: Mutex::new(0),
        }
    }

    /// How many batches were written.
    #[must_use]
    pub fn written(&self) -> usize {
        *self.seen.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl AlertSink for StateSink {
    fn write(&self, batch: EventBatch) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async move {
            self.state
                .write(batch)
                .await
                .map_err(|e| e.message().to_owned())?;
            *self.seen.lock().unwrap_or_else(PoisonError::into_inner) += 1;
            Ok(())
        })
    }
}
