// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! State partitions of a rule group (CQ-6).
//!
//! A group's alert instances are spread over `parts` partitions by the
//! hash of their fingerprint; each partition is a slot of its own
//! (section 5.3), evaluates every rule and keeps the instances and
//! windows that hash to it. The count comes from the tenant setting
//! `rule_partitions`; when it changes, partition 0 merges the states
//! of the partitions the group had, writes the merged state as a split
//! checkpoint and keeps its own keys, and each other partition takes
//! its keys from that checkpoint. The windows of incremental rules are
//! rebuilt from the files afterwards (section 11.7), since the
//! partitions had read the writer logs to different positions.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use tessera_object::{ObjectError, ObjectStore, layout};
use tessera_types::{Env, TenantId, hash::stable_hash};

use crate::evaluate::{EvalError, instance_labels};
use crate::lifecycle::fingerprint;
use crate::rule::AlertRule;
use crate::state::{self, EvalState};

/// The partition of `parts` that `fingerprint` belongs to.
#[must_use]
pub fn partition_of(fingerprint: &str, parts: u32) -> u32 {
    let parts = u64::from(parts.max(1));
    u32::try_from(stable_hash(fingerprint.as_bytes()) % parts).unwrap_or(0)
}

/// The partition of `parts` that keeps the window of `rule` for the
/// group `keys`: the one the window's instance belongs to, whose
/// fingerprint is of the instance's labels and not of the keys alone,
/// so that the partition that judges a window holds its alert.
#[must_use]
pub fn window_partition(rule: &AlertRule, keys: &BTreeMap<String, String>, parts: u32) -> u32 {
    partition_of(&fingerprint(&instance_labels(rule, keys)), parts)
}

/// The keys of `whole` that partition `part` of `parts` owns, with
/// what every partition shares: positions, ended logs, watermarks,
/// evaluation times, notification group times and degraded marks. The
/// windows stay behind: a re-split asks for their rebuild.
#[must_use]
pub fn split(whole: &EvalState, part: u32, parts: u32) -> EvalState {
    let mine = |fp: &str| partition_of(fp, parts) == part;
    EvalState {
        instances: whole
            .instances
            .iter()
            .filter(|(_, i)| mine(&i.fingerprint))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        positions: whole.positions.clone(),
        outbox: whole
            .outbox
            .iter()
            .filter(|(_, t)| mine(&t.fingerprint))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        evaluated: whole.evaluated.clone(),
        watermarks: whole.watermarks.clone(),
        windows: BTreeMap::new(),
        group_sent: whole.group_sent.clone(),
        degraded: whole.degraded.clone(),
        parts,
        split: None,
        rebuild: BTreeSet::new(),
        ended: whole.ended.clone(),
    }
}

/// The states of the partitions of one group as one: the union of
/// their keys, the furthest of their positions, watermarks and
/// evaluation times.
#[must_use]
pub fn merge(states: impl IntoIterator<Item = EvalState>) -> EvalState {
    let mut whole = EvalState::default();
    for state in states {
        whole.instances.extend(state.instances);
        whole.outbox.extend(state.outbox);
        whole.windows.extend(state.windows);
        whole.degraded.extend(state.degraded);
        whole.rebuild.extend(state.rebuild);
        furthest(&mut whole.positions, state.positions);
        furthest(&mut whole.evaluated, state.evaluated);
        furthest(&mut whole.watermarks, state.watermarks);
        furthest(&mut whole.group_sent, state.group_sent);
    }
    whole
}

fn furthest<V: Ord>(into: &mut BTreeMap<String, V>, from: BTreeMap<String, V>) {
    for (key, value) in from {
        match into.get(&key) {
            Some(current) if *current >= value => {}
            _ => {
                into.insert(key, value);
            }
        }
    }
}

/// The last state of partition `part` of `group`.
///
/// # Errors
///
/// If the state log cannot be read.
pub async fn load(
    store: &Arc<dyn ObjectStore>,
    tenant: &TenantId,
    group: &str,
    part: u32,
    env: &Env,
) -> Result<EvalState, EvalError> {
    let log = state::open(Arc::clone(store), tenant, group, part, env.clone());
    let snapshot = log
        .snapshot()
        .await
        .map_err(|e| EvalError::State(e.to_string()))?;
    Ok(snapshot.state().clone())
}

/// The states of partitions `0..parts` of `group` as one, for the
/// readers that show a group whole: the rules API, the inhibition
/// sources and `system.alert_instances`.
///
/// # Errors
///
/// If a state log cannot be read.
pub async fn merged(
    store: &Arc<dyn ObjectStore>,
    tenant: &TenantId,
    group: &str,
    parts: u32,
    env: &Env,
) -> Result<EvalState, EvalError> {
    let mut states = Vec::new();
    for part in 0..parts.max(1) {
        states.push(load(store, tenant, group, part, env).await?);
    }
    Ok(merge(states))
}

/// Writes the split checkpoint `name` of `group`: `whole` as JSON.
///
/// # Errors
///
/// If the store refuses the write.
pub async fn save_split(
    store: &dyn ObjectStore,
    tenant: &TenantId,
    group: &str,
    name: &str,
    whole: &EvalState,
) -> Result<(), EvalError> {
    let body = serde_json::to_vec(whole).map_err(|e| EvalError::State(e.to_string()))?;
    match store
        .put_if_absent(&layout::eval_split(tenant, group, name), body.into())
        .await
    {
        Ok(()) | Err(ObjectError::AlreadyExists { .. }) => Ok(()),
        Err(error) => Err(EvalError::State(error.to_string())),
    }
}

/// Reads the split checkpoint `name` of `group`; none when it is not
/// written yet.
///
/// # Errors
///
/// If the store fails or the checkpoint does not decode.
pub async fn load_split(
    store: &dyn ObjectStore,
    tenant: &TenantId,
    group: &str,
    name: &str,
) -> Result<Option<EvalState>, EvalError> {
    match store.get(&layout::eval_split(tenant, group, name)).await {
        Ok(body) => serde_json::from_slice(&body)
            .map(Some)
            .map_err(|e| EvalError::State(e.to_string())),
        Err(ObjectError::NotFound { .. }) => Ok(None),
        Err(error) => Err(EvalError::State(error.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tessera_types::{Seq, Timestamp};

    use super::*;
    use crate::lifecycle::{Instance, Status};

    fn instance(fp: &str) -> Instance {
        Instance {
            rule: "r".into(),
            fingerprint: fp.into(),
            labels: BTreeMap::new(),
            value: 1.0,
            status: Status::Firing,
            since: Timestamp::from_nanos(0),
            transitions: 1,
            notified_at: None,
        }
    }

    #[test]
    fn partitions_are_stable_and_within_range() {
        let fingerprints: Vec<String> = (0..100).map(|i| format!("{i:016x}")).collect();
        for fp in &fingerprints {
            assert!(partition_of(fp, 4) < 4);
            assert_eq!(partition_of(fp, 4), partition_of(fp, 4));
            assert_eq!(partition_of(fp, 1), 0);
            assert_eq!(partition_of(fp, 0), 0, "no partitions reads as one");
        }
        let spread: BTreeSet<u32> = fingerprints.iter().map(|fp| partition_of(fp, 4)).collect();
        assert_eq!(spread.len(), 4, "a hundred keys reach every partition");
    }

    #[test]
    fn a_split_keeps_each_key_in_one_partition_and_merging_restores_them() {
        let mut whole = EvalState::default();
        for i in 0..20 {
            let instance = instance(&format!("{i:016x}"));
            whole.instances.insert(instance.key(), instance);
        }
        whole.positions.insert("w".into(), Seq::new(7));
        whole.evaluated.insert("r".into(), Timestamp::from_nanos(5));
        let parts: Vec<EvalState> = (0..3).map(|part| split(&whole, part, 3)).collect();
        let total: usize = parts.iter().map(|p| p.instances.len()).sum();
        assert_eq!(total, 20, "every instance lands in one partition");
        assert!(parts.iter().all(|p| !p.instances.is_empty()));
        assert!(
            parts
                .iter()
                .all(|p| p.parts == 3 && p.positions["w"] == Seq::new(7))
        );
        let mut behind = parts.clone();
        behind[1].positions.insert("w".into(), Seq::new(9));
        let merged = merge(behind);
        assert_eq!(merged.instances, whole.instances);
        assert_eq!(merged.positions["w"], Seq::new(9), "the furthest position");
        assert_eq!(merged.evaluated["r"], Timestamp::from_nanos(5));
    }
}
