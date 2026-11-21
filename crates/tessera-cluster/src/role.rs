// SPDX-License-Identifier: Apache-2.0

//! The roles of a node (section 5.2).

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// What a node does. One binary runs any combination (AR-1).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    /// Decode, buffer, write a log, acknowledge, flush, serve the tail.
    Ingest,
    /// Parse, plan, prune, execute, cache.
    Query,
    /// Intake, compaction, indexing, retention, recovery, collection.
    Compact,
    /// Continuous queries and notifications.
    Eval,
}

/// Why a string is not a role.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("unknown role {0:?}; expected one of ingest, query, compact, eval")]
pub struct UnknownRole(String);

impl Role {
    /// Every role.
    pub const ALL: [Role; 4] = [Role::Ingest, Role::Query, Role::Compact, Role::Eval];

    /// The lowercase name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Ingest => "ingest",
            Role::Query => "query",
            Role::Compact => "compact",
            Role::Eval => "eval",
        }
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Role {
    type Err = UnknownRole;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Role::ALL
            .into_iter()
            .find(|r| r.as_str() == text.trim().to_ascii_lowercase())
            .ok_or_else(|| UnknownRole(text.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roles_parse_their_names() {
        assert_eq!("query".parse::<Role>(), Ok(Role::Query));
        assert_eq!(" Compact ".parse::<Role>(), Ok(Role::Compact));
        assert!("router".parse::<Role>().is_err());
        assert_eq!(Role::Eval.to_string(), "eval");
        assert_eq!(serde_json::to_string(&Role::Ingest).unwrap(), "\"ingest\"");
    }
}
