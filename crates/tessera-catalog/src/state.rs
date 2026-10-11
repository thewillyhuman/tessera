// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The catalog's state and how it folds the key-value actions of its log.
//!
//! Keys are `tenant/{id}`, `pool/{id}`, `slots/{kind}` and `setting/{name}`;
//! values are JSON records. The typed state is what nodes read; the
//! key-value encoding is how changes are committed.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use tessera_commit::{Action, ApplyError, KvMutation, SlotCount, State};
use tessera_object::PoolState;
use tessera_types::{PoolId, TenantId, WorkKind};

use crate::{Tenant, TenantName};

/// A storage pool as configured in the catalog (ST-2). The live registry
/// of open stores is built from these.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct PoolConfig {
    /// The pool's id.
    pub id: PoolId,
    /// Where the pool is: `file:///path` or `s3://bucket/prefix`.
    pub url: String,
    /// The pool's share of new objects.
    pub weight: u32,
    /// Whether the pool accepts new objects.
    pub state: PoolState,
}

/// The folded state of the cluster catalog.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogState {
    tenants: BTreeMap<TenantId, Tenant>,
    pools: BTreeMap<PoolId, PoolConfig>,
    slot_counts: BTreeMap<WorkKind, SlotCount>,
    settings: BTreeMap<String, String>,
}

impl CatalogState {
    /// The tenant with the given id.
    #[must_use]
    pub fn tenant(&self, id: &TenantId) -> Option<&Tenant> {
        self.tenants.get(id)
    }

    /// The tenant with the given name, dropped or not.
    #[must_use]
    pub fn tenant_by_name(&self, name: &TenantName) -> Option<&Tenant> {
        self.tenants.values().find(|tenant| tenant.name == *name)
    }

    /// Every tenant, in id order.
    pub fn tenants(&self) -> impl Iterator<Item = &Tenant> {
        self.tenants.values()
    }

    /// The pool with the given id.
    #[must_use]
    pub fn pool(&self, id: &PoolId) -> Option<&PoolConfig> {
        self.pools.get(id)
    }

    /// Every pool, in id order.
    pub fn pools(&self) -> impl Iterator<Item = &PoolConfig> {
        self.pools.values()
    }

    /// The slot count of a kind of work; one until it is raised.
    #[must_use]
    pub fn slot_count(&self, kind: WorkKind) -> SlotCount {
        self.slot_counts
            .get(&kind)
            .copied()
            .unwrap_or(SlotCount::ONE)
    }

    /// A cluster-wide setting.
    #[must_use]
    pub fn setting(&self, name: &str) -> Option<&str> {
        self.settings.get(name).map(String::as_str)
    }

    /// Every cluster-wide setting, by name.
    pub fn settings(&self) -> impl Iterator<Item = (&str, &str)> {
        self.settings
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
    }
}

/// The key under which a tenant record is committed.
pub(crate) fn tenant_key(id: &TenantId) -> String {
    format!("tenant/{id}")
}

pub(crate) fn pool_key(id: &PoolId) -> String {
    format!("pool/{id}")
}

pub(crate) fn slots_key(kind: WorkKind) -> String {
    format!("slots/{kind}")
}

pub(crate) fn setting_key(name: &str) -> String {
    format!("setting/{name}")
}

/// A put of a JSON record.
pub(crate) fn put_record(key: String, record: &impl Serialize) -> Action {
    let value = serde_json::to_string(record).expect("catalog records always serialize");
    Action::Kv(KvMutation::Put { key, value })
}

impl State for CatalogState {
    fn apply(&mut self, action: &Action) -> Result<(), ApplyError> {
        let Action::Kv(mutation) = action else {
            return Err(ApplyError::new(format!("not a catalog action: {action:?}")));
        };
        let (key, value) = match mutation {
            KvMutation::Put { key, value } => (key.as_str(), Some(value.as_str())),
            KvMutation::Delete { key } => (key.as_str(), None),
        };
        let (prefix, id) = key
            .split_once('/')
            .ok_or_else(|| ApplyError::new(format!("catalog key {key:?} has no prefix")))?;
        match prefix {
            "tenant" => apply_entry(&mut self.tenants, parse(id)?, value)?,
            "pool" => apply_entry(&mut self.pools, parse(id)?, value)?,
            "slots" => apply_entry(&mut self.slot_counts, parse(id)?, value)?,
            "setting" => match value {
                Some(value) => {
                    self.settings.insert(id.to_owned(), value.to_owned());
                }
                None => {
                    self.settings.remove(id);
                }
            },
            _ => {
                return Err(ApplyError::new(format!(
                    "unknown catalog key prefix in {key:?}"
                )));
            }
        }
        Ok(())
    }
}

fn parse<K: std::str::FromStr>(id: &str) -> Result<K, ApplyError>
where
    K::Err: std::fmt::Display,
{
    id.parse()
        .map_err(|e| ApplyError::new(format!("catalog key id {id:?}: {e}")))
}

fn apply_entry<K: Ord, V: serde::de::DeserializeOwned>(
    map: &mut BTreeMap<K, V>,
    id: K,
    value: Option<&str>,
) -> Result<(), ApplyError> {
    match value {
        Some(json) => {
            let record = serde_json::from_str(json)
                .map_err(|e| ApplyError::new(format!("catalog record: {e}")))?;
            map.insert(id, record);
        }
        None => {
            map.remove(&id);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use tessera_types::Timestamp;

    use super::*;
    use crate::{TenantSettings, TenantStatus};

    fn tenant(id: &str, name: &str) -> Tenant {
        Tenant {
            id: TenantId::new(id).unwrap(),
            name: TenantName::new(name).unwrap(),
            status: TenantStatus::Active,
            settings: TenantSettings::default(),
            created_at: Timestamp::UNIX_EPOCH,
        }
    }

    #[test]
    fn folds_tenant_records() {
        let mut state = CatalogState::default();
        let acme = tenant("abc", "acme");
        state
            .apply(&put_record(tenant_key(&acme.id), &acme))
            .unwrap();
        assert_eq!(state.tenant(&acme.id), Some(&acme));
        assert_eq!(state.tenant_by_name(&acme.name), Some(&acme));
        state
            .apply(&Action::Kv(KvMutation::Delete {
                key: tenant_key(&acme.id),
            }))
            .unwrap();
        assert_eq!(state.tenant(&acme.id), None);
    }

    #[test]
    fn folds_slot_counts_and_settings() {
        let mut state = CatalogState::default();
        assert_eq!(state.slot_count(WorkKind::Intake), SlotCount::ONE);
        state
            .apply(&put_record(
                slots_key(WorkKind::Intake),
                &SlotCount::new(8).unwrap(),
            ))
            .unwrap();
        assert_eq!(state.slot_count(WorkKind::Intake).get(), 8);
        state
            .apply(&Action::Kv(KvMutation::Put {
                key: setting_key("grace"),
                value: "7d".into(),
            }))
            .unwrap();
        assert_eq!(state.setting("grace"), Some("7d"));
    }

    #[test]
    fn rejects_manifest_actions() {
        let mut state = CatalogState::default();
        assert!(state.apply(&Action::DropPartition).is_err());
    }

    #[test]
    fn rejects_unknown_keys_and_malformed_records() {
        let mut state = CatalogState::default();
        let unknown = Action::Kv(KvMutation::Put {
            key: "bogus/1".into(),
            value: "{}".into(),
        });
        assert!(state.apply(&unknown).is_err());
        let malformed = Action::Kv(KvMutation::Put {
            key: "tenant/abc".into(),
            value: "{}".into(),
        });
        assert!(state.apply(&malformed).is_err());
        let bad_id = Action::Kv(KvMutation::Put {
            key: "tenant/Not Valid".into(),
            value: "{}".into(),
        });
        assert!(state.apply(&bad_id).is_err());
    }

    #[test]
    fn state_round_trips_through_a_checkpoint_encoding() {
        let mut state = CatalogState::default();
        let acme = tenant("abc", "acme");
        state
            .apply(&put_record(tenant_key(&acme.id), &acme))
            .unwrap();
        state
            .apply(&put_record(
                slots_key(WorkKind::Partition),
                &SlotCount::new(2).unwrap(),
            ))
            .unwrap();
        let json = serde_json::to_string(&state).unwrap();
        assert_eq!(serde_json::from_str::<CatalogState>(&json).unwrap(), state);
    }
}
