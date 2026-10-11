// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The `eval` role's loop: every interval, for every active tenant, the
//! alert rules in the control store are grouped and each group this
//! node owns the slot of (section 11.3, AR-4) is evaluated through the
//! engine and its outbox delivered: alert events through the node's
//! write path (CQ-14), notifications to the tenant's routes (CQ-15),
//! held back by its silences and acknowledgments. The rollups this node
//! owns are maintained the same way (section 8.7), their output written
//! through the same path.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures::future::BoxFuture;
use tessera_catalog::{Catalog, ControlStore, ControlStores, Table, TenantStatus};
use tessera_cluster::Load;
use tessera_eval::rule::decode_rows;
use tessera_eval::{
    Acknowledgment, AlertRule, AlertSink, Delivery, NotificationRoute, Notifier, Rollup, RuleGroup,
    Silence, maintain,
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
    /// Where alert events go.
    pub sink: Arc<dyn AlertSink>,
    /// How routes are reached.
    pub notifier: Arc<dyn Notifier>,
    /// How often a round runs.
    pub interval: Duration,
    /// Where the evaluator's lag is reported (OP-5).
    pub load: Arc<Load>,
}

/// The open rule groups, rebuilt when their rules change.
#[derive(Default)]
struct Groups {
    open: HashMap<(TenantId, String), Arc<RuleGroup>>,
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
    for tenant in tenants {
        match tenant_round(evaluator, groups, &tenant).await {
            Ok(behind) => lag = lag.max(behind),
            Err(error) => {
                tracing::warn!(%tenant, error = %error, "rule evaluation round failed");
            }
        }
    }
    evaluator.load.set_evaluator_lag(lag);
}

/// One round for `tenant`; returns how far its slowest owned group is
/// behind.
async fn tenant_round(
    evaluator: &Evaluator,
    groups: &mut Groups,
    tenant: &TenantId,
) -> Result<Duration, String> {
    let control = evaluator.controls.open(tenant);
    let rows = control
        .rows(Table::AlertRules)
        .await
        .map_err(|e| e.to_string())?;
    let (rules, bad) = decode_rows::<AlertRule>(&rows);
    for problem in bad {
        tracing::warn!(%tenant, problem, "alert rule does not decode");
    }
    let mut by_group: BTreeMap<String, Vec<AlertRule>> = BTreeMap::new();
    for rule in rules {
        by_group.entry(rule.group.clone()).or_default().push(rule);
    }
    let owned: Vec<(String, Vec<AlertRule>)> = by_group
        .into_iter()
        .filter(|(group, _)| {
            evaluator
                .owned
                .owns(WorkKind::RuleGroup, format!("{tenant}/{group}").as_bytes())
        })
        .collect();
    groups
        .open
        .retain(|(t, g), _| t != tenant || owned.iter().any(|(name, _)| name == g));
    rollups_round(evaluator, tenant, &control).await?;
    if owned.is_empty() {
        return Ok(Duration::ZERO);
    }
    let (routes, _) = decode_rows::<NotificationRoute>(
        &control
            .rows(Table::NotificationRoutes)
            .await
            .map_err(|e| e.to_string())?,
    );
    let (silences, _) = decode_rows::<Silence>(
        &control
            .rows(Table::Silences)
            .await
            .map_err(|e| e.to_string())?,
    );
    let (acknowledgments, _) = decode_rows::<Acknowledgment>(
        &control
            .rows(Table::Acknowledgments)
            .await
            .map_err(|e| e.to_string())?,
    );
    let delivery = Delivery {
        sink: &*evaluator.sink,
        notifier: &*evaluator.notifier,
        routes: &routes,
        silences: &silences,
        acknowledgments: &acknowledgments,
    };
    let mut lag = Duration::ZERO;
    for (name, rules) in owned {
        let key = (tenant.clone(), name.clone());
        let group = match groups.open.get(&key) {
            Some(group) if group.rules() == rules.as_slice() => Arc::clone(group),
            _ => {
                let group = Arc::new(RuleGroup::open(
                    Arc::clone(&evaluator.store),
                    tenant.clone(),
                    name.clone(),
                    rules,
                    evaluator.env.clone(),
                ));
                groups.open.insert(key, Arc::clone(&group));
                group
            }
        };
        let now = evaluator.env.clock.now();
        match group.evaluate(&evaluator.engine, now).await {
            Ok(evaluated) => {
                if !evaluated.transitions.is_empty() {
                    tracing::info!(%tenant, group = %name, transitions = evaluated.transitions.len(), "alert transitions");
                }
                if !evaluated.recorded.is_empty() {
                    if let Err(error) = group.record(delivery.sink, &evaluated.recorded).await {
                        tracing::warn!(%tenant, group = %name, error = %error, "recording rules not written");
                    }
                }
            }
            Err(error) => {
                tracing::warn!(%tenant, group = %name, error = %error, "rule group evaluation failed");
            }
        }
        match group.deliver(&delivery, now).await {
            Ok(delivered) if delivered.events > 0 || delivered.repeated > 0 => {
                tracing::info!(%tenant, group = %name, ?delivered, "alerts delivered");
            }
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(%tenant, group = %name, error = %error, "alert delivery failed");
            }
        }
        lag = lag.max(group.lag(now).await.unwrap_or(Duration::ZERO));
    }
    Ok(lag)
}

/// The rollups of `tenant` this node owns, each brought up to date.
async fn rollups_round(
    evaluator: &Evaluator,
    tenant: &TenantId,
    control: &ControlStore,
) -> Result<(), String> {
    let rows = control
        .rows(Table::Rollups)
        .await
        .map_err(|e| e.to_string())?;
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
