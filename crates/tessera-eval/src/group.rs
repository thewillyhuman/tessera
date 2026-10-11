// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! A rule group: the rules one slot owner evaluates together, with one
//! state log (section 11.3; one state partition per group for now). An
//! evaluation observes every due rule through the engine, then commits
//! in one go the instances, the transitions into the outbox and the
//! evaluation times (CQ-12); delivery drains the outbox: the alert
//! events are written (CQ-14) and the transitions marked done.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

use tessera_commit::{Action, ApplyError, UpdateError};
use tessera_exec::Engine;
use tessera_object::ObjectStore;
use tessera_types::event::Scalar;
use tessera_types::{Attributes, DatasetName, Env, EventBatch, EventRow, TenantId, Timestamp};

use crate::evaluate::{EvalError, instance_labels, observe};
use crate::lifecycle::{Instance, Observation, Transition, fingerprint, step};
use crate::rule::AlertRule;
use crate::sink::AlertSink;
use crate::state::{EvalState, StateLog};

/// One rule group of one tenant.
pub struct RuleGroup {
    tenant: TenantId,
    name: String,
    rules: Vec<AlertRule>,
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
        let log = crate::state::open(store, &tenant, &name, 0, env);
        RuleGroup {
            tenant,
            name,
            rules,
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

    /// Evaluates every rule that is due at `now` and commits what
    /// changed. A rule that fails is logged, counted as evaluated so
    /// that its interval holds, and leaves its instances as they were.
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
        let mut observed: Vec<(&AlertRule, Option<Vec<Observation>>)> = Vec::new();
        for rule in &self.rules {
            let due = snapshot
                .state()
                .evaluated
                .get(&rule.name)
                .is_none_or(|at| now.duration_since(*at).is_some_and(|d| d >= rule.every()));
            if !due {
                continue;
            }
            match observe(engine, &self.tenant, rule, now).await {
                Ok(observations) => observed.push((rule, Some(observations))),
                Err(error) => {
                    tracing::warn!(tenant = %self.tenant, group = %self.name, rule = %rule.name, error = %error, "rule evaluation failed");
                    observed.push((rule, None));
                }
            }
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
            rules: observed.iter().map(|(r, _)| r.name.clone()).collect(),
            transitions: transitions
                .into_inner()
                .unwrap_or_else(PoisonError::into_inner),
        })
    }

    /// Delivers the outbox: writes every pending transition as an alert
    /// event of its rule's dataset (CQ-14) and marks it done. A sink
    /// failure leaves the outbox as it is, to be delivered again.
    ///
    /// # Errors
    ///
    /// If the state log cannot be read or committed, or the sink fails.
    pub async fn deliver(&self, sink: &dyn AlertSink, now: Timestamp) -> Result<usize, EvalError> {
        let snapshot = self
            .log
            .snapshot()
            .await
            .map_err(|e| EvalError::State(e.to_string()))?;
        let pending: Vec<Transition> = snapshot.state().outbox.values().cloned().collect();
        if pending.is_empty() {
            return Ok(0);
        }
        let mut by_dataset: BTreeMap<String, Vec<EventRow>> = BTreeMap::new();
        for transition in &pending {
            let dataset = self
                .rules
                .iter()
                .find(|r| r.name == transition.rule)
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
        self.log
            .update(|state| {
                Ok::<_, ApplyError>(
                    pending
                        .iter()
                        .filter(|t| state.outbox.contains_key(&outbox_key(t)))
                        .map(EvalState::delete_outbox)
                        .collect(),
                )
            })
            .await
            .map_err(|e: UpdateError<ApplyError>| EvalError::State(e.to_string()))?;
        Ok(pending.len())
    }
}

fn outbox_key(transition: &Transition) -> String {
    format!(
        "{}/{}/{:020}",
        transition.rule, transition.fingerprint, transition.number
    )
}

/// The actions that bring `state` up to date with what was observed,
/// and the transitions they record. Pure, so that the commit log can
/// run it again on a newer state after a lost race.
fn decide(
    state: &EvalState,
    observed: &[(&AlertRule, Option<Vec<Observation>>)],
    now: Timestamp,
) -> (Vec<Action>, Vec<Transition>) {
    let mut actions = Vec::new();
    let mut transitions = Vec::new();
    for (rule, observations) in observed {
        actions.push(EvalState::put_evaluated(&rule.name, now));
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
