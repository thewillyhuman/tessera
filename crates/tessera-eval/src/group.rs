// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! A rule group: the rules one slot owner evaluates together, with one
//! state log (section 11.3; one state partition per group for now). An
//! evaluation observes every due rule through the engine, then commits
//! in one go the instances, the transitions into the outbox and the
//! evaluation times (CQ-12); delivery drains the outbox: the alert
//! events are written (CQ-14), the routes notified (CQ-15) and the
//! transitions marked done.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tessera_commit::{Action, ApplyError, KvMutation, UpdateError};
use tessera_exec::Engine;
use tessera_object::ObjectStore;
use tessera_types::event::Scalar;
use tessera_types::{Attributes, DatasetName, Env, EventBatch, EventRow, Seq, TenantId, Timestamp};

use crate::evaluate::{EvalError, instance_labels, observe};
use crate::incremental;
use crate::lifecycle::{Instance, Observation, Status, Transition, fingerprint, step};
use crate::notify::{Notification, Notifier};
use crate::rule::{Acknowledgment, AlertRule, NotificationRoute, Silence};
use crate::sink::AlertSink;
use crate::state::{EvalState, StateLog};

/// One rule group of one tenant.
pub struct RuleGroup {
    tenant: TenantId,
    name: String,
    rules: Vec<AlertRule>,
    store: Arc<dyn ObjectStore>,
    log: StateLog,
}

/// What one evaluation did.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Evaluated {
    /// The rules that were due and ran.
    pub rules: Vec<String>,
    /// The transitions committed to the outbox.
    pub transitions: Vec<Transition>,
}

impl RuleGroup {
    /// The group `name` of `tenant` with `rules`, over its state log in
    /// `store`.
    #[must_use]
    pub fn open(
        store: Arc<dyn ObjectStore>,
        tenant: TenantId,
        name: impl Into<String>,
        rules: Vec<AlertRule>,
        env: Env,
    ) -> Self {
        let name = name.into();
        let log = crate::state::open(Arc::clone(&store), &tenant, &name, 0, env);
        RuleGroup {
            tenant,
            name,
            rules,
            store,
            log,
        }
    }

    /// The group's name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The rules.
    #[must_use]
    pub fn rules(&self) -> &[AlertRule] {
        &self.rules
    }

    /// The remembered instances, as of the latest state.
    ///
    /// # Errors
    ///
    /// If the state log cannot be read.
    pub async fn instances(&self) -> Result<Vec<Instance>, EvalError> {
        let snapshot = self
            .log
            .snapshot()
            .await
            .map_err(|e| EvalError::State(e.to_string()))?;
        Ok(snapshot.state().instances.values().cloned().collect())
    }

    /// The transitions committed and not yet delivered.
    ///
    /// # Errors
    ///
    /// If the state log cannot be read.
    pub async fn outbox(&self) -> Result<Vec<Transition>, EvalError> {
        let snapshot = self
            .log
            .snapshot()
            .await
            .map_err(|e| EvalError::State(e.to_string()))?;
        Ok(snapshot.state().outbox.values().cloned().collect())
    }

    /// How far the group is behind at `now`: the longest time since one
    /// of its rules was last evaluated, beyond the rule's interval; zero
    /// when every rule is on time or none has run yet.
    ///
    /// # Errors
    ///
    /// If the state log cannot be read.
    pub async fn lag(&self, now: Timestamp) -> Result<Duration, EvalError> {
        let snapshot = self
            .log
            .snapshot()
            .await
            .map_err(|e| EvalError::State(e.to_string()))?;
        Ok(self
            .rules
            .iter()
            .filter_map(|rule| {
                let at = snapshot.state().evaluated.get(&rule.name)?;
                let since = now.duration_since(*at)?;
                Some(since.saturating_sub(rule.every()))
            })
            .max()
            .unwrap_or(Duration::ZERO))
    }

    /// Evaluates every rule that is due at `now` and commits what
    /// changed. Rules in the incremental subset read their new segments
    /// every time (CQ-1); the others run through the engine at their
    /// interval (CQ-8). A rule that fails is logged, counted as
    /// evaluated so that its interval holds, and leaves its instances
    /// as they were.
    ///
    /// # Errors
    ///
    /// If the state log cannot be read or committed.
    pub async fn evaluate(&self, engine: &Engine, now: Timestamp) -> Result<Evaluated, EvalError> {
        let snapshot = self
            .log
            .snapshot()
            .await
            .map_err(|e| EvalError::State(e.to_string()))?;
        let mut observed: Vec<Observed<'_>> = Vec::new();
        for rule in &self.rules {
            let incremental = incremental::shape_of(rule, &self.tenant).ok();
            let due =
                incremental.is_some()
                    || snapshot.state().evaluated.get(&rule.name).is_none_or(|at| {
                        now.duration_since(*at).is_some_and(|d| d >= rule.every())
                    });
            if !due {
                continue;
            }
            let outcome = match &incremental {
                Some(shape) => {
                    self.observe_incrementally(rule, shape, snapshot.state(), now)
                        .await
                }
                None => observe(engine, &self.tenant, rule, now)
                    .await
                    .map(|observations| (observations, Vec::new()))
                    .map_err(|e| e.to_string()),
            };
            let (observations, extra) = match outcome {
                Ok(found) => (Some(found.0), found.1),
                Err(error) => {
                    tracing::warn!(tenant = %self.tenant, group = %self.name, rule = %rule.name, error = %error, "rule evaluation failed");
                    (None, Vec::new())
                }
            };
            observed.push(Observed {
                rule,
                observations,
                extra,
                positions_before: snapshot.state().positions.clone(),
            });
        }
        if observed.is_empty() {
            return Ok(Evaluated::default());
        }
        let transitions = Mutex::new(Vec::new());
        self.log
            .update(|state| {
                let (actions, changed) = decide(state, &observed, now);
                *transitions.lock().unwrap_or_else(PoisonError::into_inner) = changed;
                Ok::<_, ApplyError>(actions)
            })
            .await
            .map_err(|e: UpdateError<ApplyError>| EvalError::State(e.to_string()))?;
        Ok(Evaluated {
            rules: observed.iter().map(|o| o.rule.name.clone()).collect(),
            transitions: transitions
                .into_inner()
                .unwrap_or_else(PoisonError::into_inner),
        })
    }

    /// The incremental path (CQ-1): read the new segments into the
    /// windows, then read the windows; the actions that record the
    /// read come back with the observations.
    async fn observe_incrementally(
        &self,
        rule: &AlertRule,
        shape: &incremental::Shape,
        state: &EvalState,
        now: Timestamp,
    ) -> Result<(Vec<Observation>, Vec<Action>), String> {
        let ingested =
            incremental::ingest(&self.store, &self.tenant, rule, shape, state, now).await?;
        if ingested.late_rows > 0 {
            tracing::info!(tenant = %self.tenant, rule = %rule.name, late_rows = ingested.late_rows, "late rows ignored");
        }
        let writers: Vec<String> = ingested.watermarks.keys().cloned().collect();
        let mut after = state.clone();
        after.positions.clone_from(&ingested.positions);
        after.watermarks.clone_from(&ingested.watermarks);
        after.windows.extend(ingested.windows.clone());
        let horizon = incremental::watermark(&after, &writers, now);
        let observations = incremental::observe(rule, shape, &after.windows, horizon)?;
        let actions = incremental::actions(rule, shape, &ingested, state, horizon);
        Ok((observations, actions))
    }

    /// Delivers the outbox (CQ-15). Every pending transition is written
    /// as an alert event of its rule's dataset (CQ-14); then the firing
    /// and resolved ones go to their rule's routes unless a silence or
    /// an acknowledgment covers the instance (section 11.8); then each
    /// is marked done, except one whose route could not be reached,
    /// which stays for the next delivery. Instances that keep firing
    /// are notified again after their route's repeat interval.
    ///
    /// # Errors
    ///
    /// If the state log cannot be read or committed, or the sink fails.
    pub async fn deliver(
        &self,
        delivery: &Delivery<'_>,
        now: Timestamp,
    ) -> Result<Delivered, EvalError> {
        let snapshot = self
            .log
            .snapshot()
            .await
            .map_err(|e| EvalError::State(e.to_string()))?;
        let state = snapshot.state();
        let pending: Vec<Transition> = state.outbox.values().cloned().collect();
        let mut delivered = Delivered::default();
        self.write_events(delivery.sink, &pending, now).await?;
        delivered.events = pending.len();

        let mut done: Vec<&Transition> = Vec::new();
        let mut notified: BTreeMap<String, Instance> = BTreeMap::new();
        for transition in &pending {
            let instance = state
                .instances
                .get(&format!("{}/{}", transition.rule, transition.fingerprint));
            let outcome = self
                .notify(delivery, transition, instance, false, now)
                .await;
            match outcome {
                Outcome::Sent(instance) => {
                    delivered.notified += 1;
                    if let Some(instance) = instance {
                        notified.insert(instance.key(), instance);
                    }
                    done.push(transition);
                }
                Outcome::Suppressed => {
                    delivered.suppressed += 1;
                    done.push(transition);
                }
                Outcome::NotNotifiable => done.push(transition),
                Outcome::Failed => delivered.failed += 1,
            }
        }
        for instance in state.instances.values() {
            if instance.status != Status::Firing || notified.contains_key(&instance.key()) {
                continue;
            }
            let Some(last) = instance.notified_at else {
                continue;
            };
            let Some(repeat) = self.repeat_of(delivery, &instance.rule) else {
                continue;
            };
            if now.duration_since(last).is_none_or(|since| since < repeat) {
                continue;
            }
            let again = Transition {
                rule: instance.rule.clone(),
                fingerprint: instance.fingerprint.clone(),
                number: instance.transitions,
                from: Status::Firing,
                to: Status::Firing,
                at: now,
                labels: instance.labels.clone(),
                value: instance.value,
            };
            match self
                .notify(delivery, &again, Some(instance), true, now)
                .await
            {
                Outcome::Sent(Some(instance)) => {
                    delivered.repeated += 1;
                    notified.insert(instance.key(), instance);
                }
                Outcome::Sent(None) | Outcome::NotNotifiable | Outcome::Suppressed => {}
                Outcome::Failed => delivered.failed += 1,
            }
        }
        let mut actions: Vec<Action> = done.iter().map(|t| EvalState::delete_outbox(t)).collect();
        actions.extend(notified.values().map(EvalState::put_instance));
        if !actions.is_empty() {
            self.log
                .update(|state| {
                    Ok::<_, ApplyError>(
                        actions
                            .iter()
                            .filter(|action| applies(state, action))
                            .cloned()
                            .collect(),
                    )
                })
                .await
                .map_err(|e: UpdateError<ApplyError>| EvalError::State(e.to_string()))?;
        }
        Ok(delivered)
    }

    /// Writes `pending` as alert events, one batch per dataset.
    async fn write_events(
        &self,
        sink: &dyn AlertSink,
        pending: &[Transition],
        now: Timestamp,
    ) -> Result<(), EvalError> {
        let mut by_dataset: BTreeMap<String, Vec<EventRow>> = BTreeMap::new();
        for transition in pending {
            let dataset = self
                .rule(&transition.rule)
                .map_or_else(|| "default".to_owned(), |r| r.dataset.clone());
            by_dataset
                .entry(dataset)
                .or_default()
                .push(alert_event(&self.tenant, transition, now));
        }
        for (dataset, rows) in by_dataset {
            let dataset = DatasetName::new(&dataset).map_err(|e| EvalError::Sink(e.to_string()))?;
            let batch = EventBatch::from_alerts(self.tenant.clone(), dataset, &rows)
                .map_err(|e| EvalError::Sink(e.to_string()))?;
            sink.write(batch).await.map_err(EvalError::Sink)?;
        }
        Ok(())
    }

    /// Tells the rule's routes about `transition`, unless it is not one
    /// to notify or the instance is silenced or acknowledged.
    async fn notify(
        &self,
        delivery: &Delivery<'_>,
        transition: &Transition,
        instance: Option<&Instance>,
        repeat: bool,
        now: Timestamp,
    ) -> Outcome {
        if !matches!(transition.to, Status::Firing | Status::Resolved) {
            return Outcome::NotNotifiable;
        }
        let Some(rule) = self.rule(&transition.rule) else {
            return Outcome::NotNotifiable;
        };
        if rule.notify.is_empty() {
            return Outcome::NotNotifiable;
        }
        let silenced = delivery
            .silences
            .iter()
            .any(|s| s.covers(&transition.labels, now));
        let acknowledged = delivery
            .acknowledgments
            .iter()
            .any(|a| a.covers(&transition.fingerprint, now));
        if silenced || acknowledged {
            return Outcome::Suppressed;
        }
        let notification = Notification {
            transition: transition.clone(),
            annotations: rule.annotations.clone(),
            repeat,
        };
        let mut failed = false;
        for name in &rule.notify {
            let Some(route) = delivery.routes.iter().find(|r| &r.name == name) else {
                tracing::warn!(tenant = %self.tenant, rule = %rule.name, route = %name, "unknown notification route");
                continue;
            };
            if let Err(error) = delivery
                .notifier
                .send(route, std::slice::from_ref(&notification))
                .await
            {
                tracing::warn!(tenant = %self.tenant, rule = %rule.name, route = %name, error = %error, "notification failed");
                failed = true;
            }
        }
        if failed {
            return Outcome::Failed;
        }
        Outcome::Sent(instance.map(|i| Instance {
            notified_at: Some(now),
            ..i.clone()
        }))
    }

    fn rule(&self, name: &str) -> Option<&AlertRule> {
        self.rules.iter().find(|r| r.name == name)
    }

    /// The shortest repeat interval among a rule's routes.
    fn repeat_of(&self, delivery: &Delivery<'_>, rule: &str) -> Option<Duration> {
        let rule = self.rule(rule)?;
        delivery
            .routes
            .iter()
            .filter(|r| rule.notify.contains(&r.name))
            .map(NotificationRoute::repeat)
            .min()
    }
}

/// What delivery needs besides the group.
pub struct Delivery<'a> {
    /// Where alert events go.
    pub sink: &'a dyn AlertSink,
    /// How routes are reached.
    pub notifier: &'a dyn Notifier,
    /// The tenant's routes.
    pub routes: &'a [NotificationRoute],
    /// The tenant's silences.
    pub silences: &'a [Silence],
    /// The tenant's acknowledgments.
    pub acknowledgments: &'a [Acknowledgment],
}

/// What one delivery did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Delivered {
    /// Transitions written as alert events.
    pub events: usize,
    /// Transitions notified.
    pub notified: usize,
    /// Transitions held back by a silence or an acknowledgment.
    pub suppressed: usize,
    /// Still-firing instances notified again.
    pub repeated: usize,
    /// Notifications that failed and stay pending.
    pub failed: usize,
}

enum Outcome {
    Sent(Option<Instance>),
    Suppressed,
    NotNotifiable,
    Failed,
}

/// Whether a delivery action still makes sense on `state`: a deletion
/// of an outbox entry that is there, or an instance update of an
/// instance that is still remembered.
fn applies(state: &EvalState, action: &Action) -> bool {
    match action {
        Action::Kv(KvMutation::Delete { key }) => key
            .strip_prefix("outbox/")
            .is_some_and(|k| state.outbox.contains_key(k)),
        Action::Kv(KvMutation::Put { key, .. }) => key
            .strip_prefix("instance/")
            .is_some_and(|k| state.instances.contains_key(k)),
        _ => false,
    }
}

/// What one rule's evaluation observed, with what recording it needs.
struct Observed<'a> {
    rule: &'a AlertRule,
    observations: Option<Vec<Observation>>,
    /// Actions of the incremental path: positions, watermarks, windows.
    extra: Vec<Action>,
    /// The writer positions the evaluation started from; a state that
    /// moved past them belongs to another owner (AR-6) and the
    /// evaluation is dropped rather than counted twice.
    positions_before: BTreeMap<String, Seq>,
}

/// The actions that bring `state` up to date with what was observed,
/// and the transitions they record. Pure, so that the commit log can
/// run it again on a newer state after a lost race.
fn decide(
    state: &EvalState,
    observed: &[Observed<'_>],
    now: Timestamp,
) -> (Vec<Action>, Vec<Transition>) {
    let mut actions = Vec::new();
    let mut transitions = Vec::new();
    for Observed {
        rule,
        observations,
        extra,
        positions_before,
    } in observed
    {
        if !extra.is_empty() && state.positions != *positions_before {
            tracing::warn!(rule = %rule.name, "another owner moved the positions; evaluation dropped");
            continue;
        }
        actions.push(EvalState::put_evaluated(&rule.name, now));
        actions.extend(extra.iter().cloned());
        let Some(observations) = observations else {
            continue;
        };
        let mut seen: BTreeMap<String, Observation> = BTreeMap::new();
        for observation in observations {
            let labels = instance_labels(rule, &observation.labels);
            let fp = fingerprint(&labels);
            seen.insert(
                fp,
                Observation {
                    labels,
                    value: observation.value,
                },
            );
        }
        let remembered: Vec<&Instance> = state
            .instances
            .values()
            .filter(|i| i.rule == rule.name)
            .collect();
        let mut fingerprints: Vec<String> = seen.keys().cloned().collect();
        fingerprints.extend(
            remembered
                .iter()
                .map(|i| i.fingerprint.clone())
                .filter(|fp| !seen.contains_key(fp)),
        );
        for fp in fingerprints {
            let current = remembered.iter().copied().find(|i| i.fingerprint == fp);
            let stepped = step(current, &rule.name, &fp, seen.get(&fp), now, rule.hold());
            match &stepped.instance {
                Some(instance) => actions.push(EvalState::put_instance(instance)),
                None if current.is_some() => {
                    actions.push(EvalState::delete_instance(&rule.name, &fp));
                }
                None => {}
            }
            if let Some(transition) = stepped.transition {
                actions.push(EvalState::put_outbox(&transition));
                transitions.push(transition);
            }
        }
    }
    (actions, transitions)
}

/// The alert event of a transition (CQ-14): named after the rule, with
/// the instance's labels and the transition's facts as attributes.
fn alert_event(tenant: &TenantId, transition: &Transition, now: Timestamp) -> EventRow {
    let mut attributes: Attributes = transition
        .labels
        .iter()
        .map(|(k, v)| (format!("attr.{k}"), Scalar::Text(v.clone())))
        .collect();
    attributes.insert(
        "attr.alert.status".into(),
        Scalar::Text(transition.to.to_string()),
    );
    attributes.insert(
        "attr.alert.previous".into(),
        Scalar::Text(transition.from.to_string()),
    );
    attributes.insert(
        "attr.alert.fingerprint".into(),
        Scalar::Text(transition.fingerprint.clone()),
    );
    attributes.insert(
        "attr.alert.transition".into(),
        Scalar::Int(i64::try_from(transition.number).unwrap_or(i64::MAX)),
    );
    attributes.insert("attr.alert.value".into(), Scalar::Float(transition.value));
    EventRow {
        timestamp: transition.at,
        observed_timestamp: now,
        event_id: transition.event_id(tenant.as_str()),
        trace_id: None,
        span_id: None,
        name: transition.rule.clone(),
        attributes,
    }
}
