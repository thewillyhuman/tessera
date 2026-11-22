// SPDX-License-Identifier: Apache-2.0

//! A rule group: the rules one slot owner evaluates together, with one
//! state log per state partition (section 11.3). A partition of
//! several is also the source of the writer logs that hash to it
//! (CQ-4): it leaves their partial windows for every partition before
//! it evaluates. An
//! evaluation observes every due rule through the engine, then commits
//! in one go the instances, the transitions into the outbox and the
//! evaluation times (CQ-12); delivery drains the outbox: the alert
//! events are written (CQ-14), the routes notified (CQ-15) and the
//! transitions marked done.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tessera_commit::{Action, ApplyError, KvMutation, Snapshot, UpdateError};
use tessera_exec::Engine;
use tessera_manifest::registry;
use tessera_object::{ObjectStore, layout};
use tessera_types::event::{MetricType, Scalar, Temporality};
use tessera_types::{
    Attributes, DatasetName, Env, EventBatch, EventRow, Keys, MetricRow, Seq, TenantId, Timestamp,
    WriterId, stable_hash,
};

use crate::evaluate::{EvalError, RuleRun, instance_labels, observe};
use crate::incremental;
use crate::lifecycle::{Instance, Observation, Status, Transition, fingerprint, step};
use crate::notify::{Notification, Notifier};
use crate::partition;
use crate::rule::{Acknowledgment, AlertRule, Inhibition, NotificationRoute, Silence};
use crate::sink::AlertSink;
use crate::source::{self, SourceTask};
use crate::state::{EvalState, StateLog};

/// One rule group of one tenant.
pub struct RuleGroup {
    tenant: TenantId,
    name: String,
    rules: Vec<AlertRule>,
    store: Arc<dyn ObjectStore>,
    keys: Arc<Keys>,
    env: Env,
    log: StateLog,
    /// The state log of partition 0, which names the split the group
    /// is at, for every other partition to follow; none in partition 0.
    first: Option<StateLog>,
    /// Which partition this is (CQ-6).
    part: u32,
    /// How many partitions the group has.
    parts: u32,
}

/// What one evaluation did.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Evaluated {
    /// The rules that were due and ran.
    pub rules: Vec<String>,
    /// The transitions committed to the outbox.
    pub transitions: Vec<Transition>,
    /// What the recording rules produced, to be written as metrics.
    pub recorded: Vec<Recorded>,
    /// The rules marked degraded after this evaluation: their windows
    /// were rebuilt from the files after a log was trimmed past them
    /// (section 11.7).
    pub degraded: Vec<String>,
    /// What the rules run through the engine asked of their scans, for
    /// the workload log (AD-1).
    pub runs: Vec<RuleRun>,
}

/// The result of one recording rule at one evaluation.
#[derive(Clone, Debug, PartialEq)]
pub struct Recorded {
    /// The dataset the metrics go to.
    pub dataset: String,
    /// One gauge per sample of the result.
    pub rows: Vec<MetricRow>,
}

impl RuleGroup {
    /// The group `name` of `tenant` with `rules`, over its state log in
    /// `store`: the whole group as one partition.
    #[must_use]
    pub fn open(
        store: Arc<dyn ObjectStore>,
        tenant: TenantId,
        name: impl Into<String>,
        rules: Vec<AlertRule>,
        env: Env,
    ) -> Self {
        RuleGroup::open_partition(store, tenant, name, rules, env, 0, 1)
    }

    /// Partition `part` of the `parts` the group `name` of `tenant` is
    /// split in (CQ-6), over its own state log in `store`. It evaluates
    /// every rule and keeps the instances whose fingerprint hashes to
    /// it; a count other than the one its state was written under makes
    /// it re-split first.
    #[must_use]
    pub fn open_partition(
        store: Arc<dyn ObjectStore>,
        tenant: TenantId,
        name: impl Into<String>,
        rules: Vec<AlertRule>,
        env: Env,
        part: u32,
        parts: u32,
    ) -> Self {
        let name = name.into();
        let keys = Arc::clone(&env.keys);
        let log = crate::state::open(Arc::clone(&store), &tenant, &name, part, env.clone());
        let first = (part > 0)
            .then(|| crate::state::open(Arc::clone(&store), &tenant, &name, 0, env.clone()));
        RuleGroup {
            tenant,
            name,
            rules,
            store,
            keys,
            env,
            log,
            first,
            part,
            parts: parts.max(1),
        }
    }

    /// The group's name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Which partition this is.
    #[must_use]
    pub fn part(&self) -> u32 {
        self.part
    }

    /// How many partitions the group has, as this one was opened.
    #[must_use]
    pub fn parts(&self) -> u32 {
        self.parts
    }

    /// Whether an instance with `fingerprint` belongs to this partition.
    fn owns(&self, fingerprint: &str) -> bool {
        partition::partition_of(fingerprint, self.parts) == self.part
    }

    /// The state of this partition under the group's current partition
    /// count, re-split first when the count changed (CQ-6): partition 0
    /// merges the partitions the group had, writes the split checkpoint
    /// and keeps its keys; another partition takes its keys from that
    /// checkpoint, or waits, with nothing to evaluate, until it exists.
    /// Partition 0's state names the split the group is at, and another
    /// partition follows it at every evaluation: a state under the same
    /// count but from an earlier split, as of a partition the group did
    /// without in between, is replaced like one under another count.
    /// A partition past the count evaluates nothing.
    async fn aligned(&self) -> Result<Option<Arc<Snapshot<EvalState>>>, EvalError> {
        let state_error = |e: &dyn std::fmt::Display| EvalError::State(e.to_string());
        let snapshot = self.log.snapshot().await.map_err(|e| state_error(&e))?;
        if self.part >= self.parts {
            return Ok(None);
        }
        let found = match &self.first {
            None if snapshot.state().parts == self.parts => return Ok(Some(snapshot)),
            None => Some(self.write_split(&snapshot).await?),
            Some(first) => {
                let first = first.snapshot().await.map_err(|e| state_error(&e))?;
                let first = first.state();
                let mine = snapshot.state();
                match first.split.as_ref().filter(|_| first.parts == self.parts) {
                    // A state written before partitions recorded their
                    // split names none, and is taken to be of this one.
                    Some(split)
                        if mine.parts == self.parts
                            && mine.split.as_ref().is_none_or(|taken| taken == split) =>
                    {
                        return Ok(Some(snapshot));
                    }
                    Some(split) => {
                        partition::load_split(&*self.store, &self.tenant, &self.name, split)
                            .await?
                            .map(|whole| (whole, split.clone()))
                    }
                    None => None,
                }
            }
        };
        let Some((whole, split)) = found else {
            tracing::info!(tenant = %self.tenant, group = %self.name, part = self.part, parts = self.parts, "waiting for partition 0 to write the split checkpoint");
            return Ok(None);
        };
        let mut next = partition::split(&whole, self.part, self.parts);
        next.split = Some(split);
        next.rebuild = self
            .rules
            .iter()
            .filter(|r| r.record.is_none() && incremental::shape_of(r, &self.tenant).is_ok())
            .map(|r| r.name.clone())
            .collect();
        tracing::info!(tenant = %self.tenant, group = %self.name, part = self.part, parts = self.parts, from = whole.parts, instances = next.instances.len(), "rule group partition re-split");
        self.log
            .update(|state| Ok::<_, ApplyError>(state.replace_with(&next)))
            .await
            .map_err(|e: UpdateError<ApplyError>| state_error(&e))?;
        let snapshot = self.log.snapshot().await.map_err(|e| state_error(&e))?;
        Ok(Some(snapshot))
    }

    /// Partition 0's half of a re-split: the states of the partitions
    /// the group had as one, written as the split checkpoint the other
    /// partitions take their keys from, with the checkpoint's name.
    async fn write_split(
        &self,
        snapshot: &Snapshot<EvalState>,
    ) -> Result<(EvalState, String), EvalError> {
        let mut states = vec![snapshot.state().clone()];
        for part in 1..snapshot.state().parts {
            states.push(
                partition::load(&self.store, &self.tenant, &self.name, part, &self.env).await?,
            );
        }
        let whole = partition::merge(states);
        // Every partition rebuilds its windows after a re-split, so
        // the partials left under the old count serve no one, nor do
        // those a stale source left when the group last had the new
        // count.
        for parts in [snapshot.state().parts, self.parts] {
            if parts > 1 {
                let all = layout::eval_partials_of(&self.tenant, &self.name, parts);
                source::clear(&*self.store, &all)
                    .await
                    .map_err(EvalError::State)?;
            }
        }
        // The state log's version tells this split from every one
        // before it, whatever the clock says.
        let split = format!(
            "{}-{}-{}",
            self.parts,
            snapshot.version(),
            self.env.clock.now().as_nanos()
        );
        partition::save_split(&*self.store, &self.tenant, &self.name, &split, &whole).await?;
        Ok((whole, split))
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
    /// every time (CQ-1), together, since they share the positions in
    /// the logs (CQ-5); the others run through the engine at their
    /// interval (CQ-8). A rule that fails is logged, counted as
    /// evaluated so that its interval holds, and leaves its instances
    /// as they were.
    ///
    /// # Errors
    ///
    /// If the state log cannot be read or committed.
    pub async fn evaluate(&self, engine: &Engine, now: Timestamp) -> Result<Evaluated, EvalError> {
        let Some(snapshot) = self.aligned().await? else {
            return Ok(Evaluated::default());
        };
        let shapes = self.shapes();
        let incremental: Vec<(&AlertRule, &incremental::Shape)> = self
            .rules
            .iter()
            .zip(&shapes)
            .filter_map(|(rule, shape)| Some((rule, shape.as_ref()?)))
            .collect();
        let (ingested, read) = self
            .read_logs(engine, &incremental, snapshot.state(), now)
            .await;
        let mut observed: Vec<Observed<'_>> = Vec::new();
        let mut recorded = Vec::new();
        for (rule, incremental) in self.rules.iter().zip(&shapes) {
            let due =
                incremental.is_some()
                    || snapshot.state().evaluated.get(&rule.name).is_none_or(|at| {
                        now.duration_since(*at).is_some_and(|d| d >= rule.every())
                    });
            if !due {
                continue;
            }
            let outcome = match incremental {
                Some(shape) => ingested
                    .as_ref()
                    .map_err(String::clone)
                    .and_then(|ingested| {
                        observe_windows(rule, shape, ingested, snapshot.state(), now)
                    })
                    .map(|(observations, extra, degraded)| (observations, extra, degraded, None)),
                None => observe(engine, &self.tenant, rule, now)
                    .await
                    .map(|(observations, run)| (observations, Vec::new(), false, Some(run)))
                    .map_err(|e| e.to_string()),
            };
            let (observations, extra, degraded, run) = match outcome {
                Ok(found) => (Some(found.0), found.1, found.2, found.3),
                Err(error) => {
                    tracing::warn!(tenant = %self.tenant, group = %self.name, rule = %rule.name, error = %error, "rule evaluation failed");
                    (None, Vec::new(), false, None)
                }
            };
            // A recording rule's samples are written, not judged: the
            // state machine sees nothing of it, but counts it evaluated.
            let observations = match (&rule.record, observations) {
                (Some(metric), Some(samples)) => {
                    recorded.push(Recorded {
                        dataset: rule.dataset.clone(),
                        rows: metric_rows(metric, &samples, now),
                    });
                    Some(Vec::new())
                }
                (_, observations) => observations,
            };
            observed.push(Observed {
                rule,
                observations,
                extra,
                from_logs: incremental.is_some(),
                degraded,
                run,
            });
        }
        if observed.is_empty() {
            return Ok(Evaluated::default());
        }
        let recorded_rules = recorded.len();
        if recorded_rules > 0 {
            tracing::debug!(tenant = %self.tenant, group = %self.name, rules = recorded_rules, "recording rules evaluated");
        }
        let transitions = Mutex::new(Vec::new());
        self.log
            .update(|state| {
                let (actions, changed) =
                    decide(state, &observed, read.as_ref(), now, &|fp| self.owns(fp));
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
            recorded,
            degraded: observed
                .iter()
                .filter(|o| o.degraded)
                .map(|o| o.rule.name.clone())
                .collect(),
            runs: observed.into_iter().filter_map(|o| o.run).collect(),
        })
    }

    /// Runs the group's source task (CQ-4) over the writer logs this
    /// partition is the source of, the ones that hash to it: those of
    /// the writers registered for the datasets of the incremental
    /// rules, and those the state still holds a position of, whose
    /// partials end or go with the log. A log that cannot be read is
    /// logged and tried again at the next evaluation; the partitions
    /// wait for its partials meanwhile.
    async fn run_sources(&self, rules: &[(&AlertRule, &incremental::Shape)], state: &EvalState) {
        let datasets: BTreeSet<&DatasetName> = rules.iter().map(|(_, s)| &s.dataset).collect();
        let mut writers: BTreeSet<String> = state.positions.keys().cloned().collect();
        for dataset in datasets {
            match registry::list(&*self.store, &self.tenant, dataset).await {
                Ok(registered) => {
                    writers.extend(registered.into_iter().map(|r| r.writer.to_string()));
                }
                Err(error) => {
                    tracing::warn!(tenant = %self.tenant, group = %self.name, %dataset, error = %error, "writer registry not read; its logs wait for the next evaluation");
                }
            }
        }
        let task = SourceTask {
            store: &self.store,
            keys: &self.keys,
            tenant: &self.tenant,
            group: &self.name,
            parts: self.parts,
            rules,
        };
        let mine = |name: &&String| partition::partition_of(name, self.parts) == self.part;
        for name in writers.iter().filter(mine) {
            let Ok(writer) = name.parse::<WriterId>() else {
                continue;
            };
            let held = state.positions.get(name).copied().unwrap_or(Seq::ZERO);
            match task.run(writer, held).await {
                Ok(emitted) => {
                    tracing::debug!(tenant = %self.tenant, group = %self.name, part = self.part, %writer, entries = emitted.entries, trimmed = emitted.trimmed, "source task ran");
                }
                Err(error) => {
                    tracing::warn!(tenant = %self.tenant, group = %self.name, part = self.part, %writer, error = %error, "source task failed");
                }
            }
        }
    }

    /// Writes what the recording rules produced (`recorded` of
    /// [`Evaluated`]) as metrics of their datasets through `sink`, one
    /// batch per dataset. Returns how many samples were written.
    ///
    /// # Errors
    ///
    /// If a batch cannot be built or written.
    pub async fn record(
        &self,
        sink: &dyn AlertSink,
        recorded: &[Recorded],
    ) -> Result<usize, EvalError> {
        let mut by_dataset: BTreeMap<&str, Vec<MetricRow>> = BTreeMap::new();
        for result in recorded {
            by_dataset
                .entry(result.dataset.as_str())
                .or_default()
                .extend(result.rows.iter().cloned());
        }
        let mut written = 0;
        for (dataset, rows) in by_dataset {
            if rows.is_empty() {
                continue;
            }
            let dataset = DatasetName::new(dataset).map_err(|e| EvalError::Sink(e.to_string()))?;
            let batch = EventBatch::from_metrics(self.tenant.clone(), dataset, &rows)
                .map_err(|e| EvalError::Sink(e.to_string()))?;
            sink.write(batch).await.map_err(EvalError::Sink)?;
            written += rows.len();
        }
        Ok(written)
    }

    /// The incremental shape of each rule, in the rules' order; none
    /// for a rule that runs through the engine at its interval, as
    /// every recording rule does.
    fn shapes(&self) -> Vec<Option<incremental::Shape>> {
        self.rules
            .iter()
            .map(|rule| match rule.record {
                Some(_) => None,
                None => incremental::shape_of(rule, &self.tenant).ok(),
            })
            .collect()
    }

    /// Reads the writer logs for the incremental `rules`, as their
    /// source first when the group has several partitions (CQ-4):
    /// what was read, and how to record where it left the logs.
    /// Nothing is read for a group without incremental rules.
    async fn read_logs(
        &self,
        engine: &Engine,
        rules: &[(&AlertRule, &incremental::Shape)],
        state: &EvalState,
        now: Timestamp,
    ) -> (Result<incremental::Ingested, String>, Option<LogRead>) {
        if rules.is_empty() {
            return (Ok(incremental::Ingested::default()), None);
        }
        if self.parts > 1 {
            self.run_sources(rules, state).await;
        }
        let ingested = self.ingest(engine, rules, state, now).await;
        let read = ingested.as_ref().ok().map(|ingested| LogRead {
            positions_before: ingested.positions_before.clone(),
            actions: incremental::position_actions(ingested, state),
        });
        (ingested, read)
    }

    /// The incremental path (CQ-1), first half: the new segments of
    /// every writer folded once into the windows of all of `rules`,
    /// of which this partition keeps those of its own instances.
    async fn ingest(
        &self,
        engine: &Engine,
        rules: &[(&AlertRule, &incremental::Shape)],
        state: &EvalState,
        now: Timestamp,
    ) -> Result<incremental::Ingested, String> {
        let source = incremental::Source {
            store: &self.store,
            keys: &self.keys,
            engine,
            tenant: &self.tenant,
            group: &self.name,
            part: self.part,
            parts: self.parts,
        };
        let mut ingested = incremental::ingest(&source, rules, state, now).await?;
        ingested.windows.retain(|_, window| {
            rules
                .iter()
                .find(|(rule, _)| rule.name == window.rule)
                .is_some_and(|(rule, _)| {
                    partition::window_partition(rule, &window.keys, self.parts) == self.part
                })
        });
        if ingested.late_rows > 0 {
            tracing::info!(tenant = %self.tenant, group = %self.name, late_rows = ingested.late_rows, "late rows ignored");
        }
        Ok(ingested)
    }

    /// Delivers the outbox (CQ-15). Every pending transition is written
    /// as an alert event of its rule's dataset (CQ-14). The firing and
    /// resolved ones then go to their rule's routes, grouped as each
    /// route asks (section 11.9): a group waits `group_wait` after its
    /// first alert and `group_interval` after its last notification, and
    /// goes out in one request. A transition is done once every route
    /// of its rule has it; one held back by a silence, an acknowledgment
    /// or an inhibition rule is done at once (section 11.8); one whose
    /// route could not be reached stays for the next delivery. Instances
    /// that keep firing are notified again after their route's repeat
    /// interval.
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
        let mut candidates: Vec<(&Transition, &AlertRule)> = Vec::new();
        for transition in &pending {
            match self.classify(delivery, transition, now) {
                Classified::NotNotifiable => done.push(transition),
                Classified::Suppressed => {
                    delivered.suppressed += 1;
                    done.push(transition);
                }
                Classified::Notify(rule) => candidates.push((transition, rule)),
            }
        }
        let sent = self.send_groups(delivery, state, &candidates, now).await;
        let mut notified: BTreeMap<String, Instance> = BTreeMap::new();
        for (i, (transition, _)) in candidates.iter().enumerate() {
            if !sent.sendable[i] {
                delivered.waiting += 1;
                continue;
            }
            if sent.failed.contains(&i) {
                delivered.failed += 1;
                continue;
            }
            delivered.notified += 1;
            if let Some(received) = transition.received {
                delivered
                    .latencies
                    .push(now.duration_since(received).unwrap_or_default());
            }
            done.push(transition);
            if let Some(instance) = state
                .instances
                .get(&format!("{}/{}", transition.rule, transition.fingerprint))
            {
                notified.insert(
                    instance.key(),
                    Instance {
                        notified_at: Some(now),
                        ..instance.clone()
                    },
                );
            }
        }
        self.repeat_firing(delivery, state, &mut notified, &mut delivered, now)
            .await;
        let mut actions: Vec<Action> = done.iter().map(|t| EvalState::delete_outbox(t)).collect();
        actions.extend(notified.values().map(EvalState::put_instance));
        actions.extend(
            sent.groups
                .iter()
                .map(|key| EvalState::put_group_sent(key, now)),
        );
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

    /// Sends the due groups of `candidates` (section 11.9): a group is
    /// due `group_wait` after its first alert, or `group_interval` after
    /// its last notification, and a candidate goes once every one of
    /// its routes' groups is due.
    async fn send_groups(
        &self,
        delivery: &Delivery<'_>,
        state: &EvalState,
        candidates: &[(&Transition, &AlertRule)],
        now: Timestamp,
    ) -> Sent {
        let groups = group_candidates(delivery, candidates);
        let due: BTreeSet<&str> = groups
            .iter()
            .filter(|(key, (route, members))| {
                let first = members
                    .iter()
                    .map(|i| candidates[*i].0.at)
                    .min()
                    .unwrap_or(now);
                let (anchor, wait) = match state.group_sent.get(key.as_str()) {
                    Some(last) => (*last, route.group_interval()),
                    None => (first, route.group_wait()),
                };
                now.duration_since(anchor)
                    .is_some_and(|since| since >= wait)
            })
            .map(|(key, _)| key.as_str())
            .collect();
        let sendable: Vec<bool> = candidates
            .iter()
            .map(|(transition, rule)| {
                routes_of(delivery, rule)
                    .all(|route| due.contains(group_key(route, transition).as_str()))
            })
            .collect();
        let mut sent = Sent {
            sendable,
            failed: BTreeSet::new(),
            groups: Vec::new(),
        };
        for (key, (route, members)) in &groups {
            if !due.contains(key.as_str()) {
                continue;
            }
            let members: Vec<usize> = members
                .iter()
                .copied()
                .filter(|i| sent.sendable[*i])
                .collect();
            let notifications: Vec<Notification> = members
                .iter()
                .map(|i| Notification {
                    transition: candidates[*i].0.clone(),
                    annotations: candidates[*i].1.annotations.clone(),
                    repeat: false,
                })
                .collect();
            if notifications.is_empty() {
                continue;
            }
            match delivery.notifier.send(route, &notifications).await {
                Ok(()) => sent.groups.push(key.clone()),
                Err(error) => {
                    tracing::warn!(tenant = %self.tenant, route = %route.name, group = %key, error = %error, "notification failed");
                    sent.failed.extend(members);
                }
            }
        }
        sent
    }

    /// Notifies the instances that keep firing again once their route's
    /// repeat interval has passed since they were last notified.
    async fn repeat_firing(
        &self,
        delivery: &Delivery<'_>,
        state: &EvalState,
        notified: &mut BTreeMap<String, Instance>,
        delivered: &mut Delivered,
        now: Timestamp,
    ) {
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
                received: None,
            };
            match self.notify(delivery, &again, Some(instance), now).await {
                Outcome::Sent(Some(instance)) => {
                    delivered.repeated += 1;
                    notified.insert(instance.key(), instance);
                }
                Outcome::Sent(None) | Outcome::NotNotifiable | Outcome::Suppressed => {}
                Outcome::Failed => delivered.failed += 1,
            }
        }
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

    /// Whether `transition` is one to notify, and if so of which rule;
    /// held back when a silence or an acknowledgment covers the instance
    /// or an inhibition rule holds it behind a firing alert.
    fn classify<'r>(
        &'r self,
        delivery: &Delivery<'_>,
        transition: &Transition,
        now: Timestamp,
    ) -> Classified<'r> {
        if !matches!(transition.to, Status::Firing | Status::Resolved) {
            return Classified::NotNotifiable;
        }
        let Some(rule) = self.rule(&transition.rule) else {
            return Classified::NotNotifiable;
        };
        if rule.notify.is_empty() {
            return Classified::NotNotifiable;
        }
        let silenced = delivery
            .silences
            .iter()
            .any(|s| s.covers(&transition.labels, now));
        let acknowledged = delivery
            .acknowledgments
            .iter()
            .any(|a| a.covers(&transition.fingerprint, now));
        let inhibited = delivery
            .inhibitions
            .iter()
            .any(|i| i.inhibits(&transition.labels, delivery.firing));
        if silenced || acknowledged || inhibited {
            return Classified::Suppressed;
        }
        for name in &rule.notify {
            if !delivery.routes.iter().any(|r| &r.name == name) {
                tracing::warn!(tenant = %self.tenant, rule = %rule.name, route = %name, "unknown notification route");
            }
        }
        Classified::Notify(rule)
    }

    /// Tells the rule's routes about `transition` again, on its own
    /// and at once: how a still-firing instance is repeated.
    async fn notify(
        &self,
        delivery: &Delivery<'_>,
        transition: &Transition,
        instance: Option<&Instance>,
        now: Timestamp,
    ) -> Outcome {
        let rule = match self.classify(delivery, transition, now) {
            Classified::NotNotifiable => return Outcome::NotNotifiable,
            Classified::Suppressed => return Outcome::Suppressed,
            Classified::Notify(rule) => rule,
        };
        let notification = Notification {
            transition: transition.clone(),
            annotations: rule.annotations.clone(),
            repeat: true,
        };
        let mut failed = false;
        for route in routes_of(delivery, rule) {
            if let Err(error) = delivery
                .notifier
                .send(route, std::slice::from_ref(&notification))
                .await
            {
                tracing::warn!(tenant = %self.tenant, rule = %rule.name, route = %route.name, error = %error, "notification failed");
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
    /// The tenant's inhibition rules.
    pub inhibitions: &'a [Inhibition],
    /// The labels of every alert firing in the tenant, across its rule
    /// groups: what inhibition rules match their sources against.
    pub firing: &'a [BTreeMap<String, String>],
}

/// What one delivery did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Delivered {
    /// Transitions written as alert events.
    pub events: usize,
    /// Transitions notified.
    pub notified: usize,
    /// Transitions held back by a silence, an acknowledgment or an
    /// inhibition rule.
    pub suppressed: usize,
    /// Still-firing instances notified again.
    pub repeated: usize,
    /// Transitions held for their group's wait or interval (section
    /// 11.9).
    pub waiting: usize,
    /// Notifications that failed and stay pending.
    pub failed: usize,
    /// For each transition notified that knew when its last row was
    /// received: the time from then to now, the alert latency of
    /// section 17.1.
    pub latencies: Vec<Duration>,
}

enum Outcome {
    Sent(Option<Instance>),
    Suppressed,
    NotNotifiable,
    Failed,
}

/// What sending the due groups did.
struct Sent {
    /// Per candidate, whether every one of its routes' groups was due.
    sendable: Vec<bool>,
    /// Candidates whose route could not be reached.
    failed: BTreeSet<usize>,
    /// The groups notified, by key.
    groups: Vec<String>,
}

/// What a pending transition is to delivery.
enum Classified<'r> {
    Notify(&'r AlertRule),
    Suppressed,
    NotNotifiable,
}

/// The routes `rule` notifies, among those the tenant has.
fn routes_of<'a>(
    delivery: &'a Delivery<'_>,
    rule: &'a AlertRule,
) -> impl Iterator<Item = &'a NotificationRoute> + 'a {
    rule.notify
        .iter()
        .filter_map(move |name| delivery.routes.iter().find(|r| &r.name == name))
}

/// The key of the group `transition` falls in on `route`: how the
/// state remembers when the group was last notified.
fn group_key(route: &NotificationRoute, transition: &Transition) -> String {
    format!("{}/{}", route.name, route.group_of(&transition.labels))
}

/// The candidates by route and group, each group with its route.
fn group_candidates<'a>(
    delivery: &'a Delivery<'_>,
    candidates: &[(&Transition, &'a AlertRule)],
) -> BTreeMap<String, (&'a NotificationRoute, Vec<usize>)> {
    let mut groups: BTreeMap<String, (&NotificationRoute, Vec<usize>)> = BTreeMap::new();
    for (index, (transition, rule)) in candidates.iter().enumerate() {
        for route in routes_of(delivery, rule) {
            groups
                .entry(group_key(route, transition))
                .or_insert_with(|| (route, Vec::new()))
                .1
                .push(index);
        }
    }
    groups
}

/// Whether a delivery action still makes sense on `state`: a deletion
/// of an outbox entry that is there, an instance update of an instance
/// that is still remembered, or a group's notification time.
fn applies(state: &EvalState, action: &Action) -> bool {
    match action {
        Action::Kv(KvMutation::Delete { key }) => key
            .strip_prefix("outbox/")
            .is_some_and(|k| state.outbox.contains_key(k)),
        Action::Kv(KvMutation::Put { key, .. }) => {
            key.starts_with("group_sent/")
                || key
                    .strip_prefix("instance/")
                    .is_some_and(|k| state.instances.contains_key(k))
        }
        _ => false,
    }
}

/// What one rule's evaluation observed, with what recording it needs.
struct Observed<'a> {
    rule: &'a AlertRule,
    observations: Option<Vec<Observation>>,
    /// Actions of the incremental path: the rule's windows.
    extra: Vec<Action>,
    /// Whether the rule was observed from what the evaluation read of
    /// the writer logs.
    from_logs: bool,
    /// Whether the rule is degraded after this evaluation (section 11.7).
    degraded: bool,
    /// What the engine was asked, when the rule ran through it.
    run: Option<RuleRun>,
}

/// The incremental path, second half, for one rule: what its
/// windows say after the read, the actions that record them, and
/// whether the rule is degraded after them (section 11.7).
fn observe_windows(
    rule: &AlertRule,
    shape: &incremental::Shape,
    ingested: &incremental::Ingested,
    state: &EvalState,
    now: Timestamp,
) -> Result<(Vec<Observation>, Vec<Action>, bool), String> {
    if let Some(why) = ingested.failed.get(&rule.name) {
        return Err(why.clone());
    }
    let writers: Vec<String> = ingested.watermarks.keys().cloned().collect();
    let mut after = state.clone();
    after.positions.clone_from(&ingested.positions);
    after.watermarks.clone_from(&ingested.watermarks);
    if ingested.rebuilt.is_some() {
        after.windows.retain(|_, w| w.rule != rule.name);
    }
    after.windows.extend(ingested.windows.clone());
    let horizon = incremental::watermark(&after, &writers, now);
    let observations = incremental::observe(rule, shape, &after.windows, horizon)?;
    let actions = incremental::window_actions(rule, shape, ingested, state, horizon);
    let degraded = incremental::degraded_mark(rule, shape, ingested, state, horizon).is_some();
    Ok((observations, actions, degraded))
}

/// What an evaluation read of the writer logs for its incremental
/// rules.
struct LogRead {
    /// The writer positions the read started from; a state that moved
    /// past them belongs to another owner (AR-6) and what was read is
    /// dropped rather than counted twice.
    positions_before: BTreeMap<String, Seq>,
    /// The actions that record where the read left the logs.
    actions: Vec<Action>,
}

/// The actions that bring `state` up to date with what was observed,
/// and the transitions they record. Pure, so that the commit log can
/// run it again on a newer state after a lost race.
fn decide(
    state: &EvalState,
    observed: &[Observed<'_>],
    read: Option<&LogRead>,
    now: Timestamp,
    owns: &dyn Fn(&str) -> bool,
) -> (Vec<Action>, Vec<Transition>) {
    let mut actions = Vec::new();
    let mut transitions = Vec::new();
    let moved = read.is_some_and(|read| state.positions != read.positions_before);
    match read {
        Some(_) if moved => {
            tracing::warn!(
                "another owner moved the positions; what was read of the logs is dropped"
            );
        }
        Some(read) => actions.extend(read.actions.iter().cloned()),
        None => {}
    }
    for Observed {
        rule,
        observations,
        extra,
        from_logs,
        degraded: _,
        run: _,
    } in observed
    {
        if *from_logs && moved {
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
            if !owns(&fp) {
                continue;
            }
            seen.insert(
                fp,
                Observation {
                    labels,
                    value: observation.value,
                    received: None,
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
/// The samples of a recording rule as gauges named `metric`, labelled
/// as the samples are, in series by name and labels.
fn metric_rows(metric: &str, samples: &[Observation], now: Timestamp) -> Vec<MetricRow> {
    samples
        .iter()
        .map(|sample| {
            let attributes: Attributes = sample
                .labels
                .iter()
                .map(|(k, v)| (format!("attr.{k}"), Scalar::Text(v.clone())))
                .collect();
            let series = sample
                .labels
                .iter()
                .fold(metric.to_owned(), |series, (k, v)| {
                    format!("{series}/{k}={v}")
                });
            let mut series_id = [0u8; 16];
            series_id[..8].copy_from_slice(&stable_hash(series.as_bytes()).to_be_bytes());
            MetricRow {
                timestamp: now,
                observed_timestamp: now,
                name: metric.to_owned(),
                metric_type: MetricType::Gauge,
                value: Some(sample.value),
                temporality: Temporality::Unspecified,
                series_id,
                attributes,
                ..MetricRow::default()
            }
        })
        .collect()
}

/// The label that names the trace an alert is about: a rule that
/// groups by `trace_id` gives each of its instances one.
const TRACE_LABEL: &str = "trace_id";

/// The trace `labels` name, when they name one: thirty-two
/// hexadecimal digits under [`TRACE_LABEL`].
fn trace_of(labels: &BTreeMap<String, String>) -> Option<[u8; 16]> {
    let text = labels.get(TRACE_LABEL)?;
    if text.len() != 32 || !text.is_ascii() {
        return None;
    }
    let mut id = [0u8; 16];
    for (byte, pair) in id.iter_mut().zip(text.as_bytes().chunks(2)) {
        *byte = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
    }
    Some(id)
}

/// The event an alert's change of status is stored as. It names the
/// trace the alert is about, when its labels do: a policy that
/// samples spans can then spare the trace (CO-7), and a reader go
/// from the alert to it.
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
        trace_id: trace_of(&transition.labels),
        span_id: None,
        name: transition.rule.clone(),
        attributes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CO-7: an alert about a trace names it, so that the trace can
    /// be spared from sampling.
    #[test]
    fn an_alert_event_names_the_trace_its_labels_name() {
        let transition = |labels: &[(&str, &str)]| Transition {
            rule: "SlowCheckout".into(),
            fingerprint: "f".into(),
            number: 1,
            from: Status::Pending,
            to: Status::Firing,
            at: Timestamp::from_nanos(1),
            labels: labels
                .iter()
                .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
                .collect(),
            value: 1.0,
            received: None,
        };
        let event = |labels: &[(&str, &str)]| {
            alert_event(
                &TenantId::DEFAULT,
                &transition(labels),
                Timestamp::from_nanos(2),
            )
        };
        let mut id = [0u8; 16];
        id[14] = 0x27;
        id[15] = 0x0f;
        let about = event(&[
            ("service", "checkout"),
            ("trace_id", "0000000000000000000000000000270f"),
        ]);
        assert_eq!(about.trace_id, Some(id));
        assert_eq!(
            about.attributes.get("attr.trace_id"),
            Some(&Scalar::Text("0000000000000000000000000000270f".into())),
            "the label stays a label"
        );
        // Most alerts are about no trace, and a label that is not an
        // id names none.
        assert_eq!(event(&[("service", "checkout")]).trace_id, None);
        for not_an_id in ["270f", "zz000000000000000000000000000000", ""] {
            assert_eq!(
                event(&[("trace_id", not_an_id)]).trace_id,
                None,
                "{not_an_id}"
            );
        }
    }
}
