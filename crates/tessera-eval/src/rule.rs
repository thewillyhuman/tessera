// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! What people write into the control store (CS-3): alert rules
//! (section 11.2), where their notifications go (11.9), and the
//! silences and acknowledgments that hold them back (11.8). Each is a
//! JSON document in its table, keyed by its name or id.

use std::collections::BTreeMap;
use std::str::FromStr;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tessera_catalog::{Row, Table};
use tessera_ir::AlertDefinition;
use tessera_types::Timestamp;

/// The language of a rule's expression (CQ-3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Language {
    /// SQL: each result row is an alert instance.
    Sql,
    /// `PromQL`: each sample of the instant vector is an instance.
    Promql,
    /// `LogQL`: each sample of the instant metric result is an instance.
    Logql,
}

impl Language {
    /// The lowercase name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Language::Sql => "sql",
            Language::Promql => "promql",
            Language::Logql => "logql",
        }
    }
}

impl std::fmt::Display for Language {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Why a string is not a rule language.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("unknown rule language {0}; expected sql, promql or logql")]
pub struct UnknownLanguage(pub String);

impl FromStr for Language {
    type Err = UnknownLanguage;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        [Language::Sql, Language::Promql, Language::Logql]
            .into_iter()
            .find(|l| l.as_str() == text.trim().to_ascii_lowercase())
            .ok_or_else(|| UnknownLanguage(text.to_owned()))
    }
}

/// An alert rule: a continuous query whose sink is the alert state
/// machine (section 11.1). Rows of `system.alert_rules`, keyed by name.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AlertRule {
    /// The rule's name, unique in the tenant.
    pub name: String,
    /// The rule group the rule is evaluated with; one slot owner runs a
    /// group (section 11.3).
    #[serde(default = "default_group")]
    pub group: String,
    /// The expression's language.
    pub language: Language,
    /// The expression.
    pub expr: String,
    /// The dataset an unqualified table name in the expression means.
    #[serde(default = "default_dataset")]
    pub dataset: String,
    /// Seconds between evaluations.
    #[serde(default = "default_every")]
    pub every_seconds: u64,
    /// Seconds the condition must hold before `pending` becomes `firing`.
    #[serde(default)]
    pub for_seconds: u64,
    /// The severity label.
    #[serde(default = "default_severity")]
    pub severity: String,
    /// Labels added to every instance.
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    /// Annotations added to every transition.
    #[serde(default)]
    pub annotations: BTreeMap<String, String>,
    /// The notification routes transitions go to, by name.
    #[serde(default)]
    pub notify: Vec<String>,
    /// The rule's place in its group, as written in a rule file; zero
    /// when it has none.
    #[serde(default)]
    pub position: u64,
    /// The metric a recording rule writes its result as, a Prometheus
    /// `record`: each sample of the result becomes a gauge of this name
    /// with the sample's labels, in the rule's dataset. Such a rule has
    /// no condition and no instances.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record: Option<String>,
}

fn default_group() -> String {
    "default".to_owned()
}

fn default_dataset() -> String {
    "default".to_owned()
}

fn default_every() -> u64 {
    60
}

fn default_severity() -> String {
    "warning".to_owned()
}

impl AlertRule {
    /// The table rules live in.
    pub const TABLE: Table = Table::AlertRules;

    /// A rule of `name` over `expr` in `language`, with the defaults.
    #[must_use]
    pub fn new(name: impl Into<String>, language: Language, expr: impl Into<String>) -> Self {
        AlertRule {
            name: name.into(),
            group: default_group(),
            language,
            expr: expr.into(),
            dataset: default_dataset(),
            every_seconds: default_every(),
            for_seconds: 0,
            severity: default_severity(),
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            notify: Vec::new(),
            position: 0,
            record: None,
        }
    }

    /// The rule `CREATE ALERT` defines (section 11.2): SQL, with the
    /// statement's options and the defaults for the rest.
    #[must_use]
    pub fn from_definition(definition: &AlertDefinition) -> Self {
        AlertRule {
            name: definition.name.clone(),
            group: default_group(),
            language: Language::Sql,
            expr: definition.query.clone(),
            dataset: definition.dataset.to_string(),
            every_seconds: definition.every.map_or_else(default_every, |d| d.as_secs()),
            for_seconds: definition.hold.map_or(0, |d| d.as_secs()),
            severity: definition.severity.clone().unwrap_or_else(default_severity),
            labels: definition.labels.iter().cloned().collect(),
            annotations: definition.annotations.iter().cloned().collect(),
            notify: definition.notify.clone(),
            position: 0,
            record: None,
        }
    }

    /// How often the rule is evaluated; at least one second.
    #[must_use]
    pub fn every(&self) -> Duration {
        Duration::from_secs(self.every_seconds.max(1))
    }

    /// How long the condition must hold before firing (`FOR`).
    #[must_use]
    pub fn hold(&self) -> Duration {
        Duration::from_secs(self.for_seconds)
    }

    /// The rule as stored in its row.
    #[must_use]
    pub fn to_row(&self) -> Row {
        Row {
            key: self.name.clone(),
            value: serde_json::to_value(self).unwrap_or_default(),
        }
    }
}

/// The kind of a notification sink (section 11.9).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RouteKind {
    /// A JSON document per batch of transitions to any URL.
    Webhook,
    /// A Slack-compatible incoming webhook: `{"text": ...}`.
    Slack,
    /// `PagerDuty`-compatible Events API v2: trigger and resolve events.
    Pagerduty,
    /// Alertmanager-compatible push to `/api/v2/alerts`.
    Alertmanager,
}

/// Where notifications go. Rows of `system.notification_routes`, keyed
/// by name.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NotificationRoute {
    /// The route's name, which rules refer to.
    pub name: String,
    /// The sink.
    pub kind: RouteKind,
    /// The URL to post to.
    pub url: String,
    /// The integration or routing key, for sinks that take one.
    #[serde(default)]
    pub routing_key: Option<String>,
    /// Seconds between repeated notifications of an instance that keeps
    /// firing; four hours by default, as Alertmanager.
    #[serde(default = "default_repeat")]
    pub repeat_seconds: u64,
}

fn default_repeat() -> u64 {
    4 * 3600
}

impl NotificationRoute {
    /// The table routes live in.
    pub const TABLE: Table = Table::NotificationRoutes;

    /// How often a firing instance is notified again.
    #[must_use]
    pub fn repeat(&self) -> Duration {
        Duration::from_secs(self.repeat_seconds)
    }
}

/// A silence: transitions of instances whose labels match every matcher
/// are not notified while it is in effect (section 11.8). Rows of
/// `system.silences`, keyed by id.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Silence {
    /// The silence's id.
    pub id: String,
    /// Label values the instance must have, all of them.
    pub matchers: BTreeMap<String, String>,
    /// When it starts.
    pub starts_at: Timestamp,
    /// When it ends.
    pub ends_at: Timestamp,
    /// Who set it and why.
    #[serde(default)]
    pub comment: String,
}

impl Silence {
    /// The table silences live in.
    pub const TABLE: Table = Table::Silences;

    /// Whether the silence covers an instance with `labels` at `now`.
    #[must_use]
    pub fn covers(&self, labels: &BTreeMap<String, String>, now: Timestamp) -> bool {
        self.starts_at <= now
            && now < self.ends_at
            && self
                .matchers
                .iter()
                .all(|(name, value)| labels.get(name) == Some(value))
    }
}

/// An acknowledgment: one instance's transitions are not notified
/// until it expires (section 11.8). Rows of `system.acknowledgments`,
/// keyed by the instance's fingerprint.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Acknowledgment {
    /// The acknowledged instance.
    pub fingerprint: String,
    /// When the acknowledgment expires.
    pub until: Timestamp,
    /// Who acknowledged.
    #[serde(default)]
    pub by: String,
    /// Why.
    #[serde(default)]
    pub comment: String,
}

impl Acknowledgment {
    /// The table acknowledgments live in.
    pub const TABLE: Table = Table::Acknowledgments;

    /// Whether the acknowledgment is in effect at `now`.
    #[must_use]
    pub fn covers(&self, fingerprint: &str, now: Timestamp) -> bool {
        self.fingerprint == fingerprint && now < self.until
    }
}

/// Decodes every row of a table as `T`, skipping and reporting the ones
/// that do not decode, so that one bad document never hides the rest.
#[must_use]
pub fn decode_rows<T: for<'de> Deserialize<'de>>(rows: &[Row]) -> (Vec<T>, Vec<String>) {
    let mut decoded = Vec::new();
    let mut bad = Vec::new();
    for row in rows {
        match serde_json::from_value(row.value.clone()) {
            Ok(value) => decoded.push(value),
            Err(error) => bad.push(format!("{}: {error}", row.key)),
        }
    }
    (decoded, bad)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn a_rule_round_trips_with_its_defaults() {
        let rule = AlertRule::new("errors", Language::Sql, "SELECT 1");
        let row = rule.to_row();
        assert_eq!(row.key, "errors");
        let (rules, bad) = decode_rows::<AlertRule>(&[row]);
        assert!(bad.is_empty());
        assert_eq!(rules[0], rule);
        assert_eq!(rule.every(), Duration::from_secs(60));
        assert_eq!(rule.hold(), Duration::ZERO);
        let minimal = Row {
            key: "x".into(),
            value: json!({"name": "x", "language": "promql", "expr": "up == 0", "for_seconds": 120}),
        };
        let (rules, _) = decode_rows::<AlertRule>(&[minimal]);
        assert_eq!(rules[0].group, "default");
        assert_eq!(rules[0].hold(), Duration::from_secs(120));
        assert_eq!("LogQL".parse::<Language>().unwrap(), Language::Logql);
        assert!("cql".parse::<Language>().is_err());
    }

    #[test]
    fn bad_documents_are_reported_not_fatal() {
        let rows = vec![
            Row {
                key: "good".into(),
                value: json!({"name": "good", "language": "sql", "expr": "SELECT 1"}),
            },
            Row {
                key: "bad".into(),
                value: json!({"name": "bad"}),
            },
        ];
        let (rules, bad) = decode_rows::<AlertRule>(&rows);
        assert_eq!(rules.len(), 1);
        assert_eq!(bad.len(), 1);
        assert!(bad[0].starts_with("bad: "));
    }

    #[test]
    fn silences_and_acknowledgments_cover_by_labels_and_time() {
        let labels = BTreeMap::from([
            ("service".to_owned(), "checkout".to_owned()),
            ("severity".to_owned(), "critical".to_owned()),
        ]);
        let silence = Silence {
            id: "s1".into(),
            matchers: BTreeMap::from([("service".to_owned(), "checkout".to_owned())]),
            starts_at: Timestamp::from_nanos(10),
            ends_at: Timestamp::from_nanos(20),
            comment: String::new(),
        };
        assert!(silence.covers(&labels, Timestamp::from_nanos(15)));
        assert!(
            !silence.covers(&labels, Timestamp::from_nanos(20)),
            "end is exclusive"
        );
        let other = BTreeMap::from([("service".to_owned(), "search".to_owned())]);
        assert!(!silence.covers(&other, Timestamp::from_nanos(15)));
        let ack = Acknowledgment {
            fingerprint: "abc".into(),
            until: Timestamp::from_nanos(100),
            by: "me".into(),
            comment: String::new(),
        };
        assert!(ack.covers("abc", Timestamp::from_nanos(99)));
        assert!(!ack.covers("abc", Timestamp::from_nanos(100)));
        assert!(!ack.covers("abd", Timestamp::from_nanos(50)));
        let route = NotificationRoute {
            name: "r".into(),
            kind: RouteKind::Webhook,
            url: "http://x".into(),
            routing_key: None,
            repeat_seconds: 60,
        };
        assert_eq!(route.repeat(), Duration::from_secs(60));
    }
}
