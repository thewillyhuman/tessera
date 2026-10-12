// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The state log of a rule group partition (CQ-12): a commit log at
//! `{tenant}/_eval/{group}/{part}/_log/` whose one commit records the
//! alert instances, the per-writer input positions and the transitions
//! produced since the previous commit and not yet notified, the outbox
//! of CQ-15. On takeover the new owner loads the last commit; a stale
//! owner's commit fails by CL-1, so no transition is recorded twice.
//! Alert instance state lives here and never in a tenant-wide log
//! (CQ-13).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tessera_commit::{Action, ApplyError, CommitLog, KvMutation, State};
use tessera_object::{ObjectStore, layout};
use tessera_types::{Env, Seq, TenantId, Timestamp};

use crate::incremental::WindowState;
use crate::lifecycle::{Instance, Transition};

/// The folded state of a rule group partition.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EvalState {
    /// The remembered instances, by `rule/fingerprint`.
    pub instances: BTreeMap<String, Instance>,
    /// The last writer-log entry applied, per writer (CQ-5).
    pub positions: BTreeMap<String, Seq>,
    /// Transitions committed but not yet notified, by
    /// `rule/fingerprint/number` (CQ-15).
    pub outbox: BTreeMap<String, Transition>,
    /// When each rule was last evaluated.
    pub evaluated: BTreeMap<String, Timestamp>,
    /// The newest observed time seen per writer (section 11.5).
    #[serde(default)]
    pub watermarks: BTreeMap<String, Timestamp>,
    /// The windows of the incremental rules, by `rule/start/fingerprint`.
    #[serde(default)]
    pub windows: BTreeMap<String, WindowState>,
    /// When each notification group last went out, by route and group
    /// (section 11.9).
    #[serde(default)]
    pub group_sent: BTreeMap<String, Timestamp>,
    /// The rules whose window state was rebuilt from the files after
    /// falling behind the writer logs (section 11.7), with when; a
    /// rule stays marked until the horizon has closed the windows the
    /// rebuild could have touched.
    #[serde(default)]
    pub degraded: BTreeMap<String, Timestamp>,
    /// How many state partitions the group had when this partition was
    /// last written (CQ-6); a count the settings no longer give means
    /// the partition must re-split before it evaluates.
    #[serde(default = "one_partition")]
    pub parts: u32,
    /// The split checkpoint partition 0 wrote for `parts`, by name,
    /// which the other partitions take their keys from (CQ-6).
    #[serde(default)]
    pub split: Option<String>,
    /// The rules whose windows the next ingest rebuilds from the files,
    /// as after a re-split (CQ-6).
    #[serde(default)]
    pub rebuild: BTreeSet<String>,
    /// The writers whose logs were read to their end, so that such a
    /// log gone whole afterwards is no gap (section 11.7).
    #[serde(default)]
    pub ended: BTreeSet<String>,
}

const fn one_partition() -> u32 {
    1
}

impl Default for EvalState {
    /// Empty, as the whole of a group with one partition.
    fn default() -> Self {
        EvalState {
            instances: BTreeMap::new(),
            positions: BTreeMap::new(),
            outbox: BTreeMap::new(),
            evaluated: BTreeMap::new(),
            watermarks: BTreeMap::new(),
            windows: BTreeMap::new(),
            group_sent: BTreeMap::new(),
            degraded: BTreeMap::new(),
            parts: one_partition(),
            split: None,
            rebuild: BTreeSet::new(),
            ended: BTreeSet::new(),
        }
    }
}

const INSTANCE: &str = "instance/";
const PARTS: &str = "parts";
const SPLIT: &str = "split";
const REBUILD: &str = "rebuild/";
const POSITION: &str = "position/";
const OUTBOX: &str = "outbox/";
const EVALUATED: &str = "evaluated/";
const WATERMARK: &str = "watermark/";
const WINDOW: &str = "window/";
const GROUP_SENT: &str = "group_sent/";
const DEGRADED: &str = "degraded/";
const ENDED: &str = "ended/";

impl EvalState {
    /// Records an instance.
    #[must_use]
    pub fn put_instance(instance: &Instance) -> Action {
        put(format!("{INSTANCE}{}", instance.key()), instance)
    }

    /// Forgets an instance.
    #[must_use]
    pub fn delete_instance(rule: &str, fingerprint: &str) -> Action {
        delete(format!("{INSTANCE}{rule}/{fingerprint}"))
    }

    /// Records a transition to notify.
    #[must_use]
    pub fn put_outbox(transition: &Transition) -> Action {
        put(format!("{OUTBOX}{}", outbox_key(transition)), transition)
    }

    /// Marks a transition notified.
    #[must_use]
    pub fn delete_outbox(transition: &Transition) -> Action {
        delete(format!("{OUTBOX}{}", outbox_key(transition)))
    }

    /// Records the last applied entry of a writer.
    #[must_use]
    pub fn put_position(writer: &str, seq: Seq) -> Action {
        put(format!("{POSITION}{writer}"), &seq)
    }

    /// Records when a rule was evaluated.
    #[must_use]
    pub fn put_evaluated(rule: &str, at: Timestamp) -> Action {
        put(format!("{EVALUATED}{rule}"), &at)
    }

    /// Records the newest observed time of a writer.
    #[must_use]
    pub fn put_watermark(writer: &str, at: Timestamp) -> Action {
        put(format!("{WATERMARK}{writer}"), &at)
    }

    /// Records a window.
    #[must_use]
    pub fn put_window(window: &WindowState) -> Action {
        put(format!("{WINDOW}{}", window.key()), window)
    }

    /// Forgets a window.
    #[must_use]
    pub fn delete_window(key: &str) -> Action {
        delete(format!("{WINDOW}{key}"))
    }

    /// Records that the group `key` of a route was notified at `at`.
    #[must_use]
    pub fn put_group_sent(key: &str, at: Timestamp) -> Action {
        put(format!("{GROUP_SENT}{key}"), &at)
    }

    /// Marks `rule` degraded since `at` (section 11.7).
    #[must_use]
    pub fn put_degraded(rule: &str, at: Timestamp) -> Action {
        put(format!("{DEGRADED}{rule}"), &at)
    }

    /// Clears the degraded mark of `rule`.
    #[must_use]
    pub fn delete_degraded(rule: &str) -> Action {
        delete(format!("{DEGRADED}{rule}"))
    }

    /// Records the group's partition count and, for partition 0, the
    /// split checkpoint it wrote (CQ-6).
    #[must_use]
    pub fn put_parts(parts: u32, split: Option<&str>) -> Vec<Action> {
        let mut actions = vec![put(PARTS.to_owned(), &parts)];
        match split {
            Some(name) => actions.push(put(SPLIT.to_owned(), &name)),
            None => actions.push(delete(SPLIT.to_owned())),
        }
        actions
    }

    /// Asks the next ingest of `rule` to rebuild its windows.
    #[must_use]
    pub fn put_rebuild(rule: &str) -> Action {
        put(format!("{REBUILD}{rule}"), &())
    }

    /// Clears the rebuild request of `rule`.
    #[must_use]
    pub fn delete_rebuild(rule: &str) -> Action {
        delete(format!("{REBUILD}{rule}"))
    }

    /// Forgets the position held in a writer's log, once it is gone.
    #[must_use]
    pub fn delete_position(writer: &str) -> Action {
        delete(format!("{POSITION}{writer}"))
    }

    /// Forgets a writer's watermark, once its log is gone.
    #[must_use]
    pub fn delete_watermark(writer: &str) -> Action {
        delete(format!("{WATERMARK}{writer}"))
    }

    /// Records that a writer's log was read to its end.
    #[must_use]
    pub fn put_ended(writer: &str) -> Action {
        put(format!("{ENDED}{writer}"), &())
    }

    /// Forgets that a writer's log ended, with the writer.
    #[must_use]
    pub fn delete_ended(writer: &str) -> Action {
        delete(format!("{ENDED}{writer}"))
    }

    /// The actions that turn this state into `next`, every key
    /// included: how a partition takes the keys of a re-split (CQ-6).
    #[must_use]
    pub fn replace_with(&self, next: &EvalState) -> Vec<Action> {
        let mut actions = Vec::new();
        actions.extend(
            self.instances
                .keys()
                .map(|k| delete(format!("{INSTANCE}{k}"))),
        );
        actions.extend(
            self.positions
                .keys()
                .map(|k| delete(format!("{POSITION}{k}"))),
        );
        actions.extend(self.outbox.keys().map(|k| delete(format!("{OUTBOX}{k}"))));
        actions.extend(
            self.evaluated
                .keys()
                .map(|k| delete(format!("{EVALUATED}{k}"))),
        );
        actions.extend(
            self.watermarks
                .keys()
                .map(|k| delete(format!("{WATERMARK}{k}"))),
        );
        actions.extend(self.windows.keys().map(|k| delete(format!("{WINDOW}{k}"))));
        actions.extend(
            self.group_sent
                .keys()
                .map(|k| delete(format!("{GROUP_SENT}{k}"))),
        );
        actions.extend(
            self.degraded
                .keys()
                .map(|k| delete(format!("{DEGRADED}{k}"))),
        );
        actions.extend(self.rebuild.iter().map(|k| delete(format!("{REBUILD}{k}"))));
        actions.extend(self.ended.iter().map(|k| delete(format!("{ENDED}{k}"))));
        actions.extend(next.instances.values().map(EvalState::put_instance));
        actions.extend(
            next.positions
                .iter()
                .map(|(w, s)| EvalState::put_position(w, *s)),
        );
        actions.extend(next.outbox.values().map(EvalState::put_outbox));
        actions.extend(
            next.evaluated
                .iter()
                .map(|(r, at)| EvalState::put_evaluated(r, *at)),
        );
        actions.extend(
            next.watermarks
                .iter()
                .map(|(w, at)| EvalState::put_watermark(w, *at)),
        );
        actions.extend(next.windows.values().map(EvalState::put_window));
        actions.extend(
            next.group_sent
                .iter()
                .map(|(k, at)| EvalState::put_group_sent(k, *at)),
        );
        actions.extend(
            next.degraded
                .iter()
                .map(|(r, at)| EvalState::put_degraded(r, *at)),
        );
        actions.extend(next.rebuild.iter().map(|r| EvalState::put_rebuild(r)));
        actions.extend(next.ended.iter().map(|w| EvalState::put_ended(w)));
        actions.extend(EvalState::put_parts(next.parts, next.split.as_deref()));
        actions
    }

    /// The outbox in transition order.
    #[must_use]
    pub fn pending_transitions(&self) -> Vec<&Transition> {
        self.outbox.values().collect()
    }
}

fn outbox_key(transition: &Transition) -> String {
    format!(
        "{}/{}/{:020}",
        transition.rule, transition.fingerprint, transition.number
    )
}

fn put<T: Serialize>(key: String, value: &T) -> Action {
    Action::Kv(KvMutation::Put {
        key,
        value: serde_json::to_string(value).unwrap_or_default(),
    })
}

fn delete(key: String) -> Action {
    Action::Kv(KvMutation::Delete { key })
}

fn decode<T: for<'de> Deserialize<'de>>(key: &str, value: &str) -> Result<T, ApplyError> {
    serde_json::from_str(value)
        .map_err(|e| ApplyError::new(format!("rule state entry {key} does not decode: {e}")))
}

impl State for EvalState {
    fn apply(&mut self, action: &Action) -> Result<(), ApplyError> {
        let Action::Kv(mutation) = action else {
            return Err(ApplyError::new(format!(
                "not a rule state action: {action:?}"
            )));
        };
        match mutation {
            KvMutation::Put { key, value } => {
                if let Some(rest) = key.strip_prefix(INSTANCE) {
                    self.instances.insert(rest.to_owned(), decode(key, value)?);
                } else if let Some(rest) = key.strip_prefix(POSITION) {
                    self.positions.insert(rest.to_owned(), decode(key, value)?);
                } else if let Some(rest) = key.strip_prefix(OUTBOX) {
                    self.outbox.insert(rest.to_owned(), decode(key, value)?);
                } else if let Some(rest) = key.strip_prefix(EVALUATED) {
                    self.evaluated.insert(rest.to_owned(), decode(key, value)?);
                } else if let Some(rest) = key.strip_prefix(WATERMARK) {
                    self.watermarks.insert(rest.to_owned(), decode(key, value)?);
                } else if let Some(rest) = key.strip_prefix(WINDOW) {
                    self.windows.insert(rest.to_owned(), decode(key, value)?);
                } else if let Some(rest) = key.strip_prefix(GROUP_SENT) {
                    self.group_sent.insert(rest.to_owned(), decode(key, value)?);
                } else if let Some(rest) = key.strip_prefix(DEGRADED) {
                    self.degraded.insert(rest.to_owned(), decode(key, value)?);
                } else if let Some(rest) = key.strip_prefix(REBUILD) {
                    self.rebuild.insert(rest.to_owned());
                } else if let Some(rest) = key.strip_prefix(ENDED) {
                    self.ended.insert(rest.to_owned());
                } else if key == PARTS {
                    self.parts = decode(key, value)?;
                } else if key == SPLIT {
                    self.split = Some(decode(key, value)?);
                } else {
                    return Err(ApplyError::new(format!("unknown rule state key {key}")));
                }
            }
            KvMutation::Delete { key } => {
                if let Some(rest) = key.strip_prefix(INSTANCE) {
                    self.instances.remove(rest);
                } else if let Some(rest) = key.strip_prefix(POSITION) {
                    self.positions.remove(rest);
                } else if let Some(rest) = key.strip_prefix(OUTBOX) {
                    self.outbox.remove(rest);
                } else if let Some(rest) = key.strip_prefix(EVALUATED) {
                    self.evaluated.remove(rest);
                } else if let Some(rest) = key.strip_prefix(WATERMARK) {
                    self.watermarks.remove(rest);
                } else if let Some(rest) = key.strip_prefix(WINDOW) {
                    self.windows.remove(rest);
                } else if let Some(rest) = key.strip_prefix(GROUP_SENT) {
                    self.group_sent.remove(rest);
                } else if let Some(rest) = key.strip_prefix(DEGRADED) {
                    self.degraded.remove(rest);
                } else if let Some(rest) = key.strip_prefix(REBUILD) {
                    self.rebuild.remove(rest);
                } else if let Some(rest) = key.strip_prefix(ENDED) {
                    self.ended.remove(rest);
                } else if key == SPLIT {
                    self.split = None;
                } else {
                    return Err(ApplyError::new(format!("unknown rule state key {key}")));
                }
            }
        }
        Ok(())
    }
}

/// The state log of one rule group partition.
pub type StateLog = CommitLog<EvalState>;

/// Opens the state log of partition `part` of `group` in `tenant`.
#[must_use]
pub fn open(
    store: Arc<dyn ObjectStore>,
    tenant: &TenantId,
    group: &str,
    part: u32,
    env: Env,
) -> StateLog {
    CommitLog::new(store, layout::eval_log(tenant, group, part), env)
}

#[cfg(test)]
mod tests {
    use tessera_commit::UpdateError;
    use tessera_object::MemoryStore;

    use super::*;
    use crate::lifecycle::Status;

    fn instance(fp: &str, status: Status) -> Instance {
        Instance {
            rule: "r".into(),
            fingerprint: fp.into(),
            labels: BTreeMap::new(),
            value: 1.0,
            status,
            since: Timestamp::from_nanos(0),
            transitions: 1,
            notified_at: None,
        }
    }

    fn transition(fp: &str, number: u64) -> Transition {
        Transition {
            rule: "r".into(),
            fingerprint: fp.into(),
            number,
            from: Status::Inactive,
            to: Status::Firing,
            at: Timestamp::from_nanos(0),
            labels: BTreeMap::new(),
            value: 1.0,
            received: None,
        }
    }

    #[test]
    fn actions_fold_into_instances_positions_outbox_and_times() {
        let mut state = EvalState::default();
        state
            .apply(&EvalState::put_instance(&instance("a", Status::Firing)))
            .unwrap();
        state
            .apply(&EvalState::put_outbox(&transition("a", 2)))
            .unwrap();
        state
            .apply(&EvalState::put_outbox(&transition("a", 1)))
            .unwrap();
        state
            .apply(&EvalState::put_position("w1", Seq::new(7)))
            .unwrap();
        state
            .apply(&EvalState::put_evaluated("r", Timestamp::from_nanos(9)))
            .unwrap();
        assert_eq!(state.instances["r/a"].status, Status::Firing);
        assert_eq!(
            state
                .pending_transitions()
                .iter()
                .map(|t| t.number)
                .collect::<Vec<_>>(),
            [1, 2],
            "in transition order"
        );
        assert_eq!(state.positions["w1"], Seq::new(7));
        assert_eq!(state.evaluated["r"], Timestamp::from_nanos(9));
        state
            .apply(&EvalState::delete_outbox(&transition("a", 1)))
            .unwrap();
        state.apply(&EvalState::delete_instance("r", "a")).unwrap();
        assert_eq!(state.outbox.len(), 1);
        assert!(state.instances.is_empty());
        state
            .apply(&EvalState::put_degraded("r", Timestamp::from_nanos(5)))
            .unwrap();
        assert_eq!(state.degraded["r"], Timestamp::from_nanos(5));
        state.apply(&EvalState::delete_degraded("r")).unwrap();
        assert!(state.degraded.is_empty());
        assert!(
            state
                .apply(&Action::Kv(KvMutation::Put {
                    key: "other/x".into(),
                    value: "1".into()
                }))
                .is_err()
        );
    }

    #[tokio::test]
    async fn a_stale_owner_cannot_commit_over_a_newer_one() {
        let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
        let env = Env::simulated(1, Timestamp::from_nanos(0));
        let old = open(Arc::clone(&store), &TenantId::DEFAULT, "g", 0, env.clone());
        let new = open(store, &TenantId::DEFAULT, "g", 0, env);
        old.snapshot().await.unwrap();
        new.update(|_| Ok::<_, ApplyError>(vec![EvalState::put_outbox(&transition("a", 1))]))
            .await
            .unwrap();
        // The old owner decides from its stale snapshot; the log re-runs
        // the decision on the newer one, so it sees the transition and
        // can refuse to record it again.
        let result = old
            .update(|state| {
                if state.outbox.contains_key("r/a/00000000000000000001") {
                    return Err(ApplyError::new("already recorded"));
                }
                Ok(vec![EvalState::put_outbox(&transition("a", 1))])
            })
            .await;
        assert!(matches!(result, Err(UpdateError::Rejected(_))));
        assert_eq!(new.snapshot().await.unwrap().state().outbox.len(), 1);
    }
}
