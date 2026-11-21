// SPDX-License-Identifier: Apache-2.0

//! The signal that produced an event.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// The signal an event belongs to. It determines which well-known columns
/// apply (spec section 4.2) and which physical partition holds the event
/// (ST-11).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    /// A log record.
    Log,
    /// A span of a trace.
    Span,
    /// A metric data point.
    Metric,
    /// A free-form event, including span events (DM-8).
    Event,
    /// An alert state transition (DM-3).
    Alert,
}

/// Why a string is not a kind.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("unknown kind {0:?}; expected one of log, span, metric, event, alert")]
pub struct UnknownKind(String);

impl Kind {
    /// Every kind, in declaration order.
    pub const ALL: [Kind; 5] = [
        Kind::Log,
        Kind::Span,
        Kind::Metric,
        Kind::Event,
        Kind::Alert,
    ];

    /// The lowercase name used in keys and in the `kind` column.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Kind::Log => "log",
            Kind::Span => "span",
            Kind::Metric => "metric",
            Kind::Event => "event",
            Kind::Alert => "alert",
        }
    }
}

impl fmt::Display for Kind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Kind {
    type Err = UnknownKind;

    fn from_str(value: &str) -> Result<Self, UnknownKind> {
        Kind::ALL
            .into_iter()
            .find(|kind| kind.as_str() == value)
            .ok_or_else(|| UnknownKind(value.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kind_round_trips_through_its_name() {
        for kind in Kind::ALL {
            assert_eq!(kind.as_str().parse::<Kind>().unwrap(), kind);
            assert_eq!(kind.to_string(), kind.as_str());
        }
    }

    #[test]
    fn rejects_unknown_kind() {
        assert!("trace".parse::<Kind>().is_err());
    }

    #[test]
    fn kind_serializes_as_its_lowercase_name() {
        assert_eq!(serde_json::to_string(&Kind::Span).unwrap(), "\"span\"");
    }
}
