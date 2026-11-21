// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The alert lifecycle (section 11.8):
//!
//! ```text
//! inactive ──condition true──▶ pending ──held for FOR──▶ firing ──condition false──▶ resolved
//!                                 │                         │
//!                                 └──condition false────────┴──▶ inactive
//! ```
//!
//! Each distinct group of a rule's result is one alert instance,
//! identified by a fingerprint of its labels (section 11.2). A step of
//! the machine takes what the rule observed for the instance now and
//! yields its next state and, when the status changed, a numbered
//! transition: the thing that is committed, written as an alert event
//! (CQ-14) and notified (CQ-15).

use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tessera_types::{Timestamp, stable_hash};

/// How long a resolved instance is remembered before it is forgotten.
pub const RESOLVED_FOR: Duration = Duration::from_secs(15 * 60);

/// Where an instance is in its life.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    /// The condition is false and nothing is remembered.
    Inactive,
    /// The condition is true but has not held for `FOR` yet.
    Pending,
    /// The condition has held for `FOR`.
    Firing,
    /// The condition became false after firing.
    Resolved,
}

impl Status {
    /// The lowercase name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Status::Inactive => "inactive",
            Status::Pending => "pending",
            Status::Firing => "firing",
            Status::Resolved => "resolved",
        }
    }
}

impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What a rule observed for one group: its labels and its value.
#[derive(Clone, Debug, PartialEq)]
pub struct Observation {
    /// The group's labels, the rule's own included.
    pub labels: BTreeMap<String, String>,
    /// The group's value.
    pub value: f64,
    /// When the last of the rows behind the value was received, for
    /// the alert latency of section 17.1; none when not known.
    pub received: Option<Timestamp>,
}

/// The fingerprint of a label set: sixteen hex digits of a stable hash
/// of the labels in name order.
#[must_use]
pub fn fingerprint(labels: &BTreeMap<String, String>) -> String {
    let mut text = String::new();
    for (name, value) in labels {
        text.push_str(name);
        text.push('\u{1}');
        text.push_str(value);
        text.push('\u{2}');
    }
    format!("{:016x}", stable_hash(text.as_bytes()))
}

/// One alert instance.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Instance {
    /// The rule.
    pub rule: String,
    /// The fingerprint of the labels.
    pub fingerprint: String,
    /// The labels.
    pub labels: BTreeMap<String, String>,
    /// The last observed value.
    #[serde(with = "crate::float")]
    pub value: f64,
    /// Where it is.
    pub status: Status,
    /// When it entered its status.
    pub since: Timestamp,
    /// How many transitions it has made; the next one is numbered
    /// `transitions + 1`.
    pub transitions: u64,
    /// When it was last notified, if ever.
    #[serde(default)]
    pub notified_at: Option<Timestamp>,
}

impl Instance {
    /// The key of the instance in its rule group: `rule/fingerprint`.
    #[must_use]
    pub fn key(&self) -> String {
        format!("{}/{}", self.rule, self.fingerprint)
    }
}

/// A change of status, numbered within its instance.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Transition {
    /// The rule.
    pub rule: String,
    /// The instance.
    pub fingerprint: String,
    /// The transition's number within the instance, from one.
    pub number: u64,
    /// The status before.
    pub from: Status,
    /// The status after.
    pub to: Status,
    /// When it happened.
    pub at: Timestamp,
    /// The instance's labels.
    pub labels: BTreeMap<String, String>,
    /// The instance's value at the time.
    #[serde(with = "crate::float")]
    pub value: f64,
    /// When the last of the rows behind the value was received, for
    /// the alert latency of section 17.1; none when not known.
    #[serde(default)]
    pub received: Option<Timestamp>,
}

impl Transition {
    /// The idempotency key of CQ-15: rule, fingerprint and number.
    #[must_use]
    pub fn idempotency_key(&self) -> String {
        format!("{}/{}/{}", self.rule, self.fingerprint, self.number)
    }

    /// The deterministic event id of the alert event (CQ-14): the same
    /// transition replayed yields the same id, which deduplication then
    /// removes.
    #[must_use]
    pub fn event_id(&self, tenant: &str) -> [u8; 16] {
        let key = format!("{tenant}/{}", self.idempotency_key());
        let high = stable_hash(key.as_bytes());
        let low = stable_hash(format!("{key}\u{3}").as_bytes());
        let mut id = [0u8; 16];
        id[..8].copy_from_slice(&high.to_be_bytes());
        id[8..].copy_from_slice(&low.to_be_bytes());
        id
    }
}

/// What a step yields: the instance as it is now, or nothing when it
/// is forgotten, and the transition if the status changed.
#[derive(Clone, Debug, PartialEq)]
pub struct Step {
    /// The instance after the step; `None` once inactive and forgotten.
    pub instance: Option<Instance>,
    /// The transition, when the status changed.
    pub transition: Option<Transition>,
}

/// One step of the machine for one instance: `current` is what is
/// remembered, `observed` what the rule saw now, `hold` the rule's `FOR`.
#[must_use]
pub fn step(
    current: Option<&Instance>,
    rule: &str,
    fingerprint: &str,
    observed: Option<&Observation>,
    now: Timestamp,
    hold: Duration,
) -> Step {
    let status = current.map_or(Status::Inactive, |i| i.status);
    let held_long_enough = |since: Timestamp| now.duration_since(since).is_some_and(|d| d >= hold);
    let next = match (status, observed) {
        (Status::Inactive | Status::Resolved, Some(_)) => {
            if hold.is_zero() {
                Status::Firing
            } else {
                Status::Pending
            }
        }
        (Status::Pending, Some(_)) => {
            if current.is_some_and(|i| held_long_enough(i.since)) {
                Status::Firing
            } else {
                Status::Pending
            }
        }
        (Status::Firing, Some(_)) => Status::Firing,
        (Status::Inactive, None) => {
            return Step {
                instance: None,
                transition: None,
            };
        }
        (Status::Pending, None) => Status::Inactive,
        (Status::Firing, None) => Status::Resolved,
        (Status::Resolved, None) => {
            let forgotten = current.is_some_and(|i| {
                now.duration_since(i.since)
                    .is_some_and(|d| d >= RESOLVED_FOR)
            });
            return Step {
                instance: (!forgotten).then(|| current.cloned()).flatten(),
                transition: None,
            };
        }
    };
    let labels = observed
        .map(|o| o.labels.clone())
        .or_else(|| current.map(|i| i.labels.clone()))
        .unwrap_or_default();
    let value = observed
        .map(|o| o.value)
        .or_else(|| current.map(|i| i.value))
        .unwrap_or(f64::NAN);
    let changed = next != status;
    let transitions = current.map_or(0, |i| i.transitions) + u64::from(changed);
    let transition = changed.then(|| Transition {
        rule: rule.to_owned(),
        fingerprint: fingerprint.to_owned(),
        number: transitions,
        from: status,
        to: next,
        at: now,
        labels: labels.clone(),
        value,
        received: observed.and_then(|o| o.received),
    });
    let instance = (next != Status::Inactive).then(|| Instance {
        rule: rule.to_owned(),
        fingerprint: fingerprint.to_owned(),
        labels,
        value,
        status: next,
        since: if changed {
            now
        } else {
            current.map_or(now, |i| i.since)
        },
        transitions,
        notified_at: current.and_then(|i| i.notified_at),
    });
    Step {
        instance,
        transition,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(seconds: i64) -> Timestamp {
        Timestamp::from_nanos(seconds * 1_000_000_000)
    }

    fn observed(value: f64) -> Observation {
        Observation {
            labels: BTreeMap::from([("service".to_owned(), "checkout".to_owned())]),
            value,
            received: None,
        }
    }

    const HOLD: Duration = Duration::from_secs(120);

    #[test]
    fn an_instance_goes_pending_then_firing_then_resolved_then_away() {
        let fp = fingerprint(&observed(1.0).labels);
        let first = step(None, "r", &fp, Some(&observed(1.0)), at(0), HOLD);
        let pending = first.instance.unwrap();
        assert_eq!(pending.status, Status::Pending);
        let t1 = first.transition.unwrap();
        assert_eq!(
            (t1.from, t1.to, t1.number),
            (Status::Inactive, Status::Pending, 1)
        );

        let still = step(Some(&pending), "r", &fp, Some(&observed(2.0)), at(60), HOLD);
        let still_pending = still.instance.unwrap();
        assert_eq!(still_pending.status, Status::Pending);
        assert_eq!(
            still_pending.since,
            at(0),
            "since does not move while pending"
        );
        assert!((still_pending.value - 2.0).abs() < f64::EPSILON);
        assert!(still.transition.is_none());

        let fired = step(
            Some(&still_pending),
            "r",
            &fp,
            Some(&Observation {
                received: Some(at(119)),
                ..observed(3.0)
            }),
            at(120),
            HOLD,
        );
        let firing = fired.instance.unwrap();
        assert_eq!(firing.status, Status::Firing);
        let t2 = fired.transition.unwrap();
        assert_eq!(t2.number, 2);
        assert_eq!(
            t2.received,
            Some(at(119)),
            "the transition carries when its last row came (17.1)"
        );

        let resolved = step(Some(&firing), "r", &fp, None, at(300), HOLD);
        let resolved_instance = resolved.instance.unwrap();
        assert_eq!(resolved_instance.status, Status::Resolved);
        let t3 = resolved.transition.unwrap();
        assert_eq!(
            (t3.from, t3.to, t3.number),
            (Status::Firing, Status::Resolved, 3)
        );
        assert!(
            (t3.value - 3.0).abs() < f64::EPSILON,
            "the last value is kept"
        );

        let kept = step(Some(&resolved_instance), "r", &fp, None, at(400), HOLD);
        assert!(kept.instance.is_some(), "remembered for a while");
        let gone = step(
            Some(&resolved_instance),
            "r",
            &fp,
            None,
            at(300 + 15 * 60),
            HOLD,
        );
        assert!(gone.instance.is_none() && gone.transition.is_none());
    }

    #[test]
    fn pending_goes_back_to_inactive_and_no_hold_fires_at_once() {
        let fp = fingerprint(&observed(1.0).labels);
        let pending = step(None, "r", &fp, Some(&observed(1.0)), at(0), HOLD)
            .instance
            .unwrap();
        let back = step(Some(&pending), "r", &fp, None, at(10), HOLD);
        assert!(back.instance.is_none());
        let t = back.transition.unwrap();
        assert_eq!(
            (t.from, t.to, t.number),
            (Status::Pending, Status::Inactive, 2)
        );

        let at_once = step(None, "r", &fp, Some(&observed(1.0)), at(0), Duration::ZERO);
        assert_eq!(at_once.instance.unwrap().status, Status::Firing);
        assert_eq!(at_once.transition.unwrap().to, Status::Firing);

        let nothing = step(None, "r", &fp, None, at(0), HOLD);
        assert_eq!(
            nothing,
            Step {
                instance: None,
                transition: None
            }
        );
    }

    #[test]
    fn a_resolved_instance_that_returns_starts_a_new_pending() {
        let fp = fingerprint(&observed(1.0).labels);
        let firing = step(None, "r", &fp, Some(&observed(1.0)), at(0), Duration::ZERO)
            .instance
            .unwrap();
        let resolved = step(Some(&firing), "r", &fp, None, at(10), HOLD)
            .instance
            .unwrap();
        let again = step(
            Some(&resolved),
            "r",
            &fp,
            Some(&observed(5.0)),
            at(20),
            HOLD,
        );
        let pending = again.instance.unwrap();
        assert_eq!(pending.status, Status::Pending);
        assert_eq!(pending.transitions, 3);
        assert_eq!(again.transition.unwrap().number, 3);
    }

    #[test]
    fn fingerprints_and_event_ids_are_deterministic() {
        let a = fingerprint(&observed(1.0).labels);
        assert_eq!(a.len(), 16);
        assert_eq!(a, fingerprint(&observed(2.0).labels), "values do not count");
        assert_ne!(a, fingerprint(&BTreeMap::new()));
        let t = Transition {
            rule: "r".into(),
            fingerprint: a.clone(),
            number: 1,
            from: Status::Inactive,
            to: Status::Firing,
            at: at(0),
            labels: BTreeMap::new(),
            value: 1.0,
            received: None,
        };
        assert_eq!(t.idempotency_key(), format!("r/{a}/1"));
        assert_eq!(t.event_id("default"), t.event_id("default"));
        assert_ne!(t.event_id("default"), t.event_id("acme"));
        let mut later = t.clone();
        later.number = 2;
        assert_ne!(t.event_id("default"), later.event_id("default"));
    }
}
