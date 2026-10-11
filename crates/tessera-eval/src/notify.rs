// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Notifications (section 11.9, CQ-15): a transition is committed first,
//! then sent, then marked sent, so delivery is at least once and every
//! request carries an idempotency key of rule, fingerprint and
//! transition number. The sinks are a plain webhook, a Slack-compatible
//! incoming webhook, `PagerDuty`-compatible Events API v2 and an
//! Alertmanager-compatible push; the payloads are built here, the
//! transport is a trait so that tests need no network.

use std::collections::BTreeMap;
use std::sync::{Mutex, PoisonError};

use futures::future::BoxFuture;
use serde_json::{Value, json};

use crate::lifecycle::{Status, Transition};
use crate::rule::{NotificationRoute, RouteKind};

/// One thing to tell a route about.
#[derive(Clone, Debug, PartialEq)]
pub struct Notification {
    /// The transition, or for a repeat the instance's current state as a
    /// transition from firing to firing.
    pub transition: Transition,
    /// The rule's annotations.
    pub annotations: BTreeMap<String, String>,
    /// Whether this repeats an earlier notification of a still-firing
    /// instance.
    pub repeat: bool,
}

impl Notification {
    /// The idempotency key of the request (CQ-15); a repeat is keyed by
    /// its time as well, since it is a new notification of the same
    /// transition.
    #[must_use]
    pub fn idempotency_key(&self) -> String {
        if self.repeat {
            format!(
                "{}/repeat/{}",
                self.transition.idempotency_key(),
                self.transition.at.as_nanos()
            )
        } else {
            self.transition.idempotency_key()
        }
    }
}

/// Sends notifications to a route.
pub trait Notifier: Send + Sync {
    /// Sends `notifications` to `route`, in one request where the sink
    /// allows.
    fn send<'a>(
        &'a self,
        route: &'a NotificationRoute,
        notifications: &'a [Notification],
    ) -> BoxFuture<'a, Result<(), String>>;
}

/// The request bodies for `notifications` on `route`: one for a
/// webhook, Slack or Alertmanager, one per notification for `PagerDuty`.
#[must_use]
pub fn payloads(route: &NotificationRoute, notifications: &[Notification]) -> Vec<Value> {
    match route.kind {
        RouteKind::Webhook => vec![json!({
            "version": "1",
            "route": route.name,
            "status": overall_status(notifications),
            "alerts": notifications.iter().map(alert_json).collect::<Vec<_>>(),
        })],
        RouteKind::Slack => vec![json!({
            "text": notifications.iter().map(line).collect::<Vec<_>>().join("\n"),
        })],
        RouteKind::Pagerduty => notifications
            .iter()
            .map(|n| {
                let t = &n.transition;
                json!({
                    "routing_key": route.routing_key.clone().unwrap_or_default(),
                    "event_action": if t.to == Status::Resolved { "resolve" } else { "trigger" },
                    "dedup_key": format!("{}/{}", t.rule, t.fingerprint),
                    "payload": {
                        "summary": line(n),
                        "severity": pagerduty_severity(t.labels.get("severity").map(String::as_str)),
                        "source": "tessera",
                        "timestamp": t.at.to_string(),
                        "custom_details": { "labels": t.labels, "annotations": n.annotations, "value": t.value },
                    },
                })
            })
            .collect(),
        RouteKind::Alertmanager => vec![Value::Array(
            notifications
                .iter()
                .map(|n| {
                    let t = &n.transition;
                    let mut alert = json!({
                        "labels": t.labels,
                        "annotations": n.annotations,
                        "startsAt": t.at.to_string(),
                        "generatorURL": format!("tessera://alerts/{}/{}", t.rule, t.fingerprint),
                    });
                    if t.to == Status::Resolved {
                        alert["endsAt"] = Value::String(t.at.to_string());
                    }
                    alert
                })
                .collect(),
        )],
    }
}

/// The path appended to an Alertmanager route's URL when it names the
/// server rather than the endpoint.
pub const ALERTMANAGER_PATH: &str = "/api/v2/alerts";

/// The URL a request for `route` goes to.
#[must_use]
pub fn url_of(route: &NotificationRoute) -> String {
    match route.kind {
        RouteKind::Alertmanager if !route.url.ends_with(ALERTMANAGER_PATH) => {
            format!("{}{ALERTMANAGER_PATH}", route.url.trim_end_matches('/'))
        }
        _ => route.url.clone(),
    }
}

fn overall_status(notifications: &[Notification]) -> &'static str {
    if notifications
        .iter()
        .any(|n| n.transition.to == Status::Firing)
    {
        "firing"
    } else {
        "resolved"
    }
}

fn alert_json(notification: &Notification) -> Value {
    let t = &notification.transition;
    json!({
        "rule": t.rule,
        "fingerprint": t.fingerprint,
        "status": t.to.as_str(),
        "previous": t.from.as_str(),
        "transition": t.number,
        "at": t.at.to_string(),
        "labels": t.labels,
        "annotations": notification.annotations,
        "value": t.value,
        "repeat": notification.repeat,
    })
}

/// One human line: `[FIRING] errors {service=checkout} value 3`.
fn line(notification: &Notification) -> String {
    let t = &notification.transition;
    let labels: Vec<String> = t
        .labels
        .iter()
        .filter(|(k, _)| k.as_str() != "alertname")
        .map(|(k, v)| format!("{k}={v}"))
        .collect();
    let status = if notification.repeat {
        "STILL FIRING".to_owned()
    } else {
        t.to.as_str().to_ascii_uppercase()
    };
    let summary = notification
        .annotations
        .get("summary")
        .map(|s| format!(": {s}"))
        .unwrap_or_default();
    format!(
        "[{status}] {} {{{}}} value {}{summary}",
        t.rule,
        labels.join(", "),
        t.value
    )
}

fn pagerduty_severity(severity: Option<&str>) -> &'static str {
    match severity {
        Some("critical") => "critical",
        Some("error") => "error",
        Some("info") => "info",
        _ => "warning",
    }
}

/// A notifier that remembers what it was asked to send, and fails on
/// demand, for tests.
#[derive(Debug, Default)]
pub struct MemoryNotifier {
    sent: Mutex<Vec<(String, Vec<Notification>)>>,
    failing: Mutex<bool>,
}

impl MemoryNotifier {
    /// Every send so far: the route's name and the notifications.
    #[must_use]
    pub fn sent(&self) -> Vec<(String, Vec<Notification>)> {
        self.sent
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Makes every send fail, or succeed again.
    pub fn fail(&self, failing: bool) {
        *self.failing.lock().unwrap_or_else(PoisonError::into_inner) = failing;
    }
}

impl Notifier for MemoryNotifier {
    fn send<'a>(
        &'a self,
        route: &'a NotificationRoute,
        notifications: &'a [Notification],
    ) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async move {
            if *self.failing.lock().unwrap_or_else(PoisonError::into_inner) {
                return Err(format!("{}: unreachable", route.name));
            }
            self.sent
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push((route.name.clone(), notifications.to_vec()));
            Ok(())
        })
    }
}

/// Posts the payloads over HTTP with a bounded timeout.
#[derive(Clone)]
pub struct HttpNotifier {
    agent: ureq::Agent,
}

impl Default for HttpNotifier {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpNotifier {
    /// A notifier whose requests time out after thirty seconds.
    #[must_use]
    pub fn new() -> Self {
        HttpNotifier {
            agent: ureq::Agent::config_builder()
                .http_status_as_error(false)
                .timeout_global(Some(std::time::Duration::from_secs(30)))
                .build()
                .into(),
        }
    }

    fn post(&self, url: &str, key: &str, body: &Value) -> Result<(), String> {
        let text = body.to_string();
        let mut response = self
            .agent
            .post(url)
            .header("content-type", "application/json")
            .header("x-idempotency-key", key)
            .send(text.as_bytes())
            .map_err(|e| format!("{url}: {e}"))?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        let reason = response.body_mut().read_to_string().unwrap_or_default();
        Err(format!("{url}: {status}: {reason}"))
    }
}

impl Notifier for HttpNotifier {
    fn send<'a>(
        &'a self,
        route: &'a NotificationRoute,
        notifications: &'a [Notification],
    ) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async move {
            let url = url_of(route);
            let bodies = payloads(route, notifications);
            let keys: Vec<String> = match route.kind {
                RouteKind::Pagerduty => notifications
                    .iter()
                    .map(Notification::idempotency_key)
                    .collect(),
                _ => vec![
                    notifications
                        .iter()
                        .map(Notification::idempotency_key)
                        .collect::<Vec<_>>()
                        .join(","),
                ],
            };
            let client = self.clone();
            tokio::task::spawn_blocking(move || {
                for (body, key) in bodies.iter().zip(&keys) {
                    client.post(&url, key, body)?;
                }
                Ok(())
            })
            .await
            .map_err(|e| e.to_string())?
        })
    }
}

#[cfg(test)]
mod tests {
    use tessera_types::Timestamp;

    use super::*;

    fn route(kind: RouteKind) -> NotificationRoute {
        NotificationRoute {
            name: "ops".into(),
            kind,
            url: "http://sink".into(),
            routing_key: Some("key".into()),
            repeat_seconds: 60,
            group_by: vec![],
            group_wait_seconds: 0,
            group_interval_seconds: 0,
        }
    }

    fn notification(to: Status, repeat: bool) -> Notification {
        Notification {
            transition: Transition {
                rule: "errors".into(),
                fingerprint: "abcd".into(),
                number: 2,
                from: Status::Pending,
                to,
                at: Timestamp::from_nanos(0),
                labels: BTreeMap::from([
                    ("alertname".to_owned(), "errors".to_owned()),
                    ("service".to_owned(), "checkout".to_owned()),
                    ("severity".to_owned(), "critical".to_owned()),
                ]),
                value: 3.0,
            },
            annotations: BTreeMap::from([("summary".to_owned(), "too many".to_owned())]),
            repeat,
        }
    }

    #[test]
    fn each_sink_gets_its_own_shape() {
        let firing = notification(Status::Firing, false);
        let resolved = notification(Status::Resolved, false);
        let webhook = payloads(
            &route(RouteKind::Webhook),
            &[firing.clone(), resolved.clone()],
        );
        assert_eq!(webhook.len(), 1);
        assert_eq!(webhook[0]["status"], "firing");
        assert_eq!(webhook[0]["alerts"][1]["status"], "resolved");
        assert_eq!(webhook[0]["alerts"][0]["labels"]["service"], "checkout");

        let slack = payloads(&route(RouteKind::Slack), std::slice::from_ref(&firing));
        assert_eq!(
            slack[0]["text"],
            "[FIRING] errors {service=checkout, severity=critical} value 3: too many"
        );
        let still = payloads(
            &route(RouteKind::Slack),
            &[notification(Status::Firing, true)],
        );
        assert!(
            still[0]["text"]
                .as_str()
                .unwrap()
                .starts_with("[STILL FIRING]")
        );

        let pagerduty = payloads(
            &route(RouteKind::Pagerduty),
            &[firing.clone(), resolved.clone()],
        );
        assert_eq!(pagerduty.len(), 2, "one event per transition");
        assert_eq!(pagerduty[0]["event_action"], "trigger");
        assert_eq!(pagerduty[1]["event_action"], "resolve");
        assert_eq!(pagerduty[0]["dedup_key"], "errors/abcd");
        assert_eq!(pagerduty[0]["routing_key"], "key");
        assert_eq!(pagerduty[0]["payload"]["severity"], "critical");

        let alertmanager = payloads(&route(RouteKind::Alertmanager), &[firing, resolved]);
        let alerts = alertmanager[0].as_array().unwrap();
        assert_eq!(alerts.len(), 2);
        assert!(alerts[0].get("endsAt").is_none());
        assert_eq!(alerts[1]["endsAt"], "1970-01-01T00:00:00Z");
        assert_eq!(
            url_of(&route(RouteKind::Alertmanager)),
            "http://sink/api/v2/alerts"
        );
        assert_eq!(url_of(&route(RouteKind::Webhook)), "http://sink");
    }

    #[test]
    fn idempotency_keys_tell_repeats_apart() {
        let first = notification(Status::Firing, false);
        let mut again = notification(Status::Firing, true);
        again.transition.at = Timestamp::from_nanos(5);
        assert_eq!(first.idempotency_key(), "errors/abcd/2");
        assert_eq!(again.idempotency_key(), "errors/abcd/2/repeat/5");
    }
}
