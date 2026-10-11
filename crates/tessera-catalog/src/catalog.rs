// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The catalog service: tenant lifecycle (CT-2, CT-4, CT-8), pool
//! configuration (ST-2) and slot counts (AR-5), all through the commit log
//! in the root pool.
//!
//! Every change is an optimistic transaction: the decision is derived from
//! the latest state and re-derived if another node commits first, so two
//! administrators cannot create the same tenant twice.

use std::sync::Arc;
use std::time::Duration;

use tessera_commit::{
    Action, CommitError, CommitLog, KvMutation, SlotCount, Snapshot, UpdateError,
};
use tessera_object::{Key, ObjectStore, PoolState};
use tessera_types::{Env, KeyPolicy, PoolId, TenantId, WorkKind};

use crate::state::{pool_key, put_record, setting_key, slots_key, tenant_key};
use crate::{
    CatalogState, InvalidSettings, PoolConfig, Tenant, TenantName, TenantSettings,
    TenantSettingsSource, TenantStatus,
};

/// Why a catalog operation failed.
#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    /// A live tenant already has this name.
    #[error("tenant {0} already exists")]
    TenantExists(TenantName),
    /// No tenant has this name.
    #[error("unknown tenant {0}")]
    UnknownTenant(TenantName),
    /// Names starting with `_` belong to built-in tenants.
    #[error("tenant name {0} is reserved")]
    ReservedName(TenantName),
    /// The settings are not valid.
    #[error("invalid settings: {0}")]
    InvalidSettings(#[from] InvalidSettings),
    /// The `default` and `_system` tenants always exist (CT-4).
    #[error("tenant {0} is built in and cannot be dropped, suspended or renamed")]
    BuiltinTenant(TenantName),
    /// The operation needs the tenant to be in another status.
    #[error("tenant {name} is {status:?}")]
    WrongStatus {
        /// The tenant.
        name: TenantName,
        /// Its current status.
        status: TenantStatus,
    },
    /// The grace period of CT-8 has not elapsed.
    #[error("tenant {0} was dropped less than the grace period ago")]
    GracePeriod(TenantName),
    /// A pool with this id is already configured.
    #[error("pool {0} already exists")]
    PoolExists(PoolId),
    /// No pool has this id.
    #[error("unknown pool {0}")]
    UnknownPool(PoolId),
    /// The root pool holds the catalog and cannot be removed.
    #[error("the root pool {0} cannot be removed")]
    RootPool(PoolId),
    /// The slot count is at its maximum.
    #[error("slot count for {0} is at its maximum")]
    SlotsAtMaximum(WorkKind),
    /// Too many nodes changed the catalog at once; retry.
    #[error("catalog contention: gave up after {attempts} attempts")]
    Contention {
        /// How many attempts were made.
        attempts: u32,
    },
    /// The catalog log failed.
    #[error(transparent)]
    Log(#[from] CommitError),
}

impl From<UpdateError<CatalogError>> for CatalogError {
    fn from(error: UpdateError<CatalogError>) -> Self {
        match error {
            UpdateError::Rejected(rejection) => rejection,
            UpdateError::Commit(commit) => CatalogError::Log(commit),
            UpdateError::Contention { attempts } => CatalogError::Contention { attempts },
        }
    }
}

/// The catalog of one deployment, opened by every node.
pub struct Catalog {
    log: CommitLog<CatalogState>,
    env: Env,
}

impl Catalog {
    /// The setting that holds the grace period of CT-8, in seconds.
    pub const DROP_GRACE_SETTING: &'static str = "tenant_drop_grace_seconds";
    /// The default grace period of CT-8.
    pub const DEFAULT_DROP_GRACE: Duration = Duration::from_secs(7 * 24 * 3600);

    /// Opens the catalog in `root`, creating it with the built-in tenants
    /// and the root pool's configuration if it does not exist yet.
    ///
    /// # Errors
    ///
    /// If the log cannot be read or the first commit cannot be made.
    pub async fn open(
        root: Arc<dyn ObjectStore>,
        root_pool: PoolConfig,
        env: Env,
    ) -> Result<Self, CatalogError> {
        let catalog = Catalog {
            log: CommitLog::new(root, Self::prefix(), env.clone()),
            env,
        };
        catalog.bootstrap(root_pool).await?;
        Ok(catalog)
    }

    /// The key prefix of the catalog log in the root pool (section 6.2).
    #[must_use]
    pub fn prefix() -> Key {
        tessera_object::layout::catalog()
    }

    /// The latest state, read from the root pool.
    ///
    /// # Errors
    ///
    /// If the log cannot be read.
    pub async fn snapshot(&self) -> Result<Arc<Snapshot<CatalogState>>, CatalogError> {
        Ok(self.log.snapshot().await?)
    }

    /// The latest state this node has seen, without touching the store.
    /// Always present after `open`.
    #[must_use]
    pub fn cached(&self) -> Arc<Snapshot<CatalogState>> {
        self.log
            .cached()
            .unwrap_or_else(|| Arc::new(Snapshot::initial()))
    }

    /// Writes a checkpoint of the latest state.
    ///
    /// # Errors
    ///
    /// If the log cannot be read or the checkpoint cannot be written.
    pub async fn checkpoint(&self) -> Result<(), CatalogError> {
        let snapshot = self.log.snapshot().await?;
        Ok(self.log.checkpoint(&snapshot).await?)
    }

    /// Creates a tenant with a fresh random id.
    ///
    /// # Errors
    ///
    /// `ReservedName`, `TenantExists`, or a log failure.
    pub async fn create_tenant(
        &self,
        name: TenantName,
        settings: TenantSettings,
    ) -> Result<Tenant, CatalogError> {
        if name.is_reserved() {
            return Err(CatalogError::ReservedName(name));
        }
        settings.validate()?;
        let id = TenantId::random(&mut self.env.random.clone());
        let created_at = self.env.clock.now();
        let tenant = Tenant {
            id,
            name,
            status: TenantStatus::Active,
            settings,
            created_at,
        };
        let snapshot = self
            .log
            .update(|state| {
                if state.tenant_by_name(&tenant.name).is_some() {
                    return Err(CatalogError::TenantExists(tenant.name.clone()));
                }
                Ok(vec![put_record(tenant_key(&tenant.id), &tenant)])
            })
            .await?;
        committed(snapshot.state().tenant(&tenant.id))
    }

    /// Applies `patch` to a tenant's settings (`ALTER TENANT ... SET`).
    ///
    /// # Errors
    ///
    /// `UnknownTenant` or a log failure.
    pub async fn alter_tenant(
        &self,
        name: &TenantName,
        patch: TenantSettings,
    ) -> Result<Tenant, CatalogError> {
        self.modify_tenant(name, |tenant| {
            let merged = tenant.settings.merged(&patch);
            merged.validate()?;
            tenant.settings = merged;
            Ok(())
        })
        .await
    }

    /// Renames a tenant; its id and data are unchanged.
    ///
    /// # Errors
    ///
    /// `UnknownTenant`, `BuiltinTenant`, `ReservedName`, `TenantExists`, or
    /// a log failure.
    pub async fn rename_tenant(
        &self,
        name: &TenantName,
        new_name: TenantName,
    ) -> Result<Tenant, CatalogError> {
        if new_name.is_reserved() {
            return Err(CatalogError::ReservedName(new_name));
        }
        let snapshot = self
            .log
            .update(|state| {
                let tenant = find(state, name)?;
                if tenant.is_builtin() {
                    return Err(CatalogError::BuiltinTenant(name.clone()));
                }
                if state
                    .tenant_by_name(&new_name)
                    .is_some_and(|other| other.id != tenant.id)
                {
                    return Err(CatalogError::TenantExists(new_name.clone()));
                }
                let renamed = Tenant {
                    name: new_name.clone(),
                    ..tenant.clone()
                };
                Ok(vec![put_record(tenant_key(&renamed.id), &renamed)])
            })
            .await?;
        committed(snapshot.state().tenant_by_name(&new_name))
    }

    /// Stops a tenant from writing and querying, keeping everything.
    ///
    /// # Errors
    ///
    /// `UnknownTenant`, `BuiltinTenant`, `WrongStatus` unless active, or a
    /// log failure.
    pub async fn suspend_tenant(&self, name: &TenantName) -> Result<Tenant, CatalogError> {
        self.transition(name, TenantStatus::Active, |_| TenantStatus::Suspended)
            .await
    }

    /// Reactivates a suspended tenant.
    ///
    /// # Errors
    ///
    /// `UnknownTenant`, `WrongStatus` unless suspended, or a log failure.
    pub async fn resume_tenant(&self, name: &TenantName) -> Result<Tenant, CatalogError> {
        self.transition(name, TenantStatus::Suspended, |_| TenantStatus::Active)
            .await
    }

    /// Drops a tenant: unreachable at once, data deleted after the grace
    /// period unless restored (CT-8). The name stays taken until the
    /// tenant is purged.
    ///
    /// # Errors
    ///
    /// `UnknownTenant`, `BuiltinTenant`, `WrongStatus` if already dropped,
    /// or a log failure.
    pub async fn drop_tenant(&self, name: &TenantName) -> Result<Tenant, CatalogError> {
        let now = self.env.clock.now();
        self.modify_tenant(name, |tenant| {
            if tenant.is_builtin() {
                return Err(CatalogError::BuiltinTenant(name.clone()));
            }
            if matches!(tenant.status, TenantStatus::Dropped { .. }) {
                return Err(CatalogError::WrongStatus {
                    name: name.clone(),
                    status: tenant.status,
                });
            }
            tenant.status = TenantStatus::Dropped { at: now };
            Ok(())
        })
        .await
    }

    /// Brings a dropped tenant back, as active.
    ///
    /// # Errors
    ///
    /// `UnknownTenant`, `WrongStatus` unless dropped, or a log failure.
    pub async fn restore_tenant(&self, name: &TenantName) -> Result<Tenant, CatalogError> {
        self.modify_tenant(name, |tenant| {
            if !matches!(tenant.status, TenantStatus::Dropped { .. }) {
                return Err(CatalogError::WrongStatus {
                    name: name.clone(),
                    status: tenant.status,
                });
            }
            tenant.status = TenantStatus::Active;
            Ok(())
        })
        .await
    }

    /// Removes a dropped tenant's record once its grace period has
    /// elapsed. Garbage collection calls this after deleting the data.
    ///
    /// # Errors
    ///
    /// `UnknownTenant`, `WrongStatus` unless dropped, `GracePeriod`, or a
    /// log failure.
    pub async fn purge_tenant(&self, name: &TenantName) -> Result<(), CatalogError> {
        let now = self.env.clock.now();
        self.log
            .update(|state| {
                let tenant = find(state, name)?;
                let TenantStatus::Dropped { at } = tenant.status else {
                    return Err(CatalogError::WrongStatus {
                        name: name.clone(),
                        status: tenant.status,
                    });
                };
                let grace = drop_grace(state);
                if at.checked_add(grace).is_none_or(|deadline| now < deadline) {
                    return Err(CatalogError::GracePeriod(name.clone()));
                }
                Ok(vec![Action::Kv(KvMutation::Delete {
                    key: tenant_key(&tenant.id),
                })])
            })
            .await?;
        Ok(())
    }

    /// Configures a new active pool.
    ///
    /// # Errors
    ///
    /// `PoolExists` or a log failure.
    pub async fn add_pool(&self, pool: PoolConfig) -> Result<(), CatalogError> {
        self.log
            .update(|state| {
                if state.pool(&pool.id).is_some() {
                    return Err(CatalogError::PoolExists(pool.id.clone()));
                }
                Ok(vec![put_record(pool_key(&pool.id), &pool)])
            })
            .await?;
        Ok(())
    }

    /// Changes a pool's weight.
    ///
    /// # Errors
    ///
    /// `UnknownPool` or a log failure.
    pub async fn set_pool_weight(&self, id: &PoolId, weight: u32) -> Result<(), CatalogError> {
        self.modify_pool(id, |pool| pool.weight = weight).await
    }

    /// Stops placing new objects in a pool.
    ///
    /// # Errors
    ///
    /// `UnknownPool` or a log failure.
    pub async fn drain_pool(&self, id: &PoolId) -> Result<(), CatalogError> {
        self.modify_pool(id, |pool| pool.state = PoolState::Draining)
            .await
    }

    /// Removes a pool's configuration. The caller must have moved or
    /// dropped every object it held.
    ///
    /// # Errors
    ///
    /// `RootPool`, `UnknownPool`, or a log failure.
    pub async fn remove_pool(&self, id: &PoolId) -> Result<(), CatalogError> {
        self.log
            .update(|state| {
                if state.pool(id).is_none() {
                    return Err(CatalogError::UnknownPool(id.clone()));
                }
                if root_pool_id(state).as_ref() == Some(id) {
                    return Err(CatalogError::RootPool(id.clone()));
                }
                Ok(vec![Action::Kv(KvMutation::Delete { key: pool_key(id) })])
            })
            .await?;
        Ok(())
    }

    /// Doubles the slot count of a kind of work (AR-5).
    ///
    /// # Errors
    ///
    /// `SlotsAtMaximum` or a log failure.
    pub async fn double_slots(&self, kind: WorkKind) -> Result<SlotCount, CatalogError> {
        let snapshot = self
            .log
            .update(|state| {
                let doubled = state
                    .slot_count(kind)
                    .doubled()
                    .ok_or(CatalogError::SlotsAtMaximum(kind))?;
                Ok(vec![put_record(slots_key(kind), &doubled)])
            })
            .await?;
        Ok(snapshot.state().slot_count(kind))
    }

    /// Creates the built-in tenants and the root pool's record if the log
    /// is empty. Racing nodes converge: whoever commits version one wins
    /// and the others find nothing to do.
    async fn bootstrap(&self, root_pool: PoolConfig) -> Result<(), CatalogError> {
        let created_at = self.env.clock.now();
        self.log
            .update(|state| {
                if state.tenant(&TenantId::DEFAULT).is_some() {
                    return Ok::<_, CatalogError>(Vec::new());
                }
                let builtin = |id: TenantId, name: &str| Tenant {
                    id,
                    name: TenantName::new(name).expect("built-in names are valid"),
                    status: TenantStatus::Active,
                    settings: TenantSettings::default(),
                    created_at,
                };
                let default = builtin(TenantId::DEFAULT, "default");
                let system = builtin(TenantId::SYSTEM, "_system");
                Ok(vec![
                    put_record(tenant_key(&default.id), &default),
                    put_record(tenant_key(&system.id), &system),
                    put_record(pool_key(&root_pool.id), &root_pool),
                    Action::Kv(KvMutation::Put {
                        key: setting_key(ROOT_POOL_SETTING),
                        value: root_pool.id.to_string(),
                    }),
                ])
            })
            .await?;
        Ok(())
    }

    /// Sets a cluster-wide setting (CT-7), or removes it with `None`:
    /// unit prices for usage (CO-4), the drop grace period and the like.
    ///
    /// # Errors
    ///
    /// If the commit fails.
    pub async fn set_setting(&self, name: &str, value: Option<String>) -> Result<(), CatalogError> {
        let key = setting_key(name);
        self.log
            .update(|_| {
                Ok(vec![Action::Kv(match &value {
                    Some(value) => KvMutation::Put {
                        key: key.clone(),
                        value: value.clone(),
                    },
                    None => KvMutation::Delete { key: key.clone() },
                })])
            })
            .await?;
        Ok(())
    }

    /// Commits the tenant after `change` edits it.
    async fn modify_tenant(
        &self,
        name: &TenantName,
        change: impl Fn(&mut Tenant) -> Result<(), CatalogError>,
    ) -> Result<Tenant, CatalogError> {
        let snapshot = self
            .log
            .update(|state| {
                let mut tenant = find(state, name)?.clone();
                change(&mut tenant)?;
                Ok(vec![put_record(tenant_key(&tenant.id), &tenant)])
            })
            .await?;
        committed(snapshot.state().tenant_by_name(name))
    }

    /// A status change from `from`, refused for built-in tenants.
    async fn transition(
        &self,
        name: &TenantName,
        from: TenantStatus,
        to: impl Fn(&Tenant) -> TenantStatus,
    ) -> Result<Tenant, CatalogError> {
        self.modify_tenant(name, |tenant| {
            if tenant.is_builtin() {
                return Err(CatalogError::BuiltinTenant(name.clone()));
            }
            if tenant.status != from {
                return Err(CatalogError::WrongStatus {
                    name: name.clone(),
                    status: tenant.status,
                });
            }
            tenant.status = to(tenant);
            Ok(())
        })
        .await
    }

    async fn modify_pool(
        &self,
        id: &PoolId,
        change: impl Fn(&mut PoolConfig),
    ) -> Result<(), CatalogError> {
        self.log
            .update(|state| {
                let mut pool = state
                    .pool(id)
                    .cloned()
                    .ok_or_else(|| CatalogError::UnknownPool(id.clone()))?;
                change(&mut pool);
                Ok(vec![put_record(pool_key(&pool.id), &pool)])
            })
            .await?;
        Ok(())
    }
}

impl std::fmt::Debug for Catalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Catalog")
            .field("node", &self.env.node)
            .finish_non_exhaustive()
    }
}

/// The setting naming the root pool.
const ROOT_POOL_SETTING: &str = "root_pool";

/// The record a successful update just committed. The snapshot returned by
/// `update` is the one that applied our actions, so the record is there;
/// its absence would mean the log and the state disagree.
fn committed(tenant: Option<&Tenant>) -> Result<Tenant, CatalogError> {
    tenant.cloned().ok_or_else(|| {
        CommitError::Corrupt {
            key: Catalog::prefix(),
            source: "a committed tenant record is missing from the folded state".into(),
        }
        .into()
    })
}

fn find<'a>(state: &'a CatalogState, name: &TenantName) -> Result<&'a Tenant, CatalogError> {
    state
        .tenant_by_name(name)
        .ok_or_else(|| CatalogError::UnknownTenant(name.clone()))
}

fn root_pool_id(state: &CatalogState) -> Option<PoolId> {
    state
        .setting(ROOT_POOL_SETTING)
        .and_then(|id| id.parse().ok())
}

fn drop_grace(state: &CatalogState) -> Duration {
    state
        .setting(Catalog::DROP_GRACE_SETTING)
        .and_then(|seconds| seconds.parse().ok())
        .map_or(Catalog::DEFAULT_DROP_GRACE, Duration::from_secs)
}

/// A tenant's settings as the catalog last read them (CT-7).
impl TenantSettingsSource for Catalog {
    fn settings_of(&self, tenant: &TenantId) -> TenantSettings {
        self.cached()
            .state()
            .tenant(tenant)
            .map(|t| t.settings.clone())
            .unwrap_or_default()
    }
}

/// The tenant's `encryption_key` setting names the key its data is
/// written under (SE-2, CT-7).
impl KeyPolicy for Catalog {
    fn key_name(&self, tenant: &TenantId) -> Option<String> {
        self.cached()
            .state()
            .tenant(tenant)
            .and_then(|t| t.settings.encryption_key.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tessera_object::MemoryStore;
    use tessera_types::{
        Clock, ManualClock, NodeId, SeededRandom, SharedRandom, Timestamp, Version,
    };

    use super::*;

    struct Cluster {
        store: MemoryStore,
        clock: Arc<ManualClock>,
    }

    impl Cluster {
        fn new() -> Self {
            Cluster {
                store: MemoryStore::new(),
                clock: Arc::new(ManualClock::new(Timestamp::from_nanos(1_000))),
            }
        }

        async fn node(&self, id: u64) -> Catalog {
            let env = Env::new(
                NodeId::from_u128(u128::from(id)),
                Arc::clone(&self.clock) as Arc<dyn Clock>,
                SharedRandom::new(SeededRandom::new(id)),
            );
            Catalog::open(Arc::new(self.store.clone()), root_pool(), env)
                .await
                .unwrap()
        }
    }

    fn root_pool() -> PoolConfig {
        PoolConfig {
            id: PoolId::new("root").unwrap(),
            url: "file:///data".into(),
            weight: 1,
            state: PoolState::Active,
        }
    }

    fn name(text: &str) -> TenantName {
        TenantName::new(text).unwrap()
    }

    #[tokio::test]
    async fn cluster_settings_are_set_listed_and_removed() {
        let cluster = Cluster::new();
        let catalog = cluster.node(1).await;
        catalog
            .set_setting("price.ingest_byte", Some("0.5".into()))
            .await
            .unwrap();
        let state = catalog.snapshot().await.unwrap();
        assert_eq!(state.state().setting("price.ingest_byte"), Some("0.5"));
        assert!(
            state
                .state()
                .settings()
                .any(|(name, value)| name == "price.ingest_byte" && value == "0.5")
        );
        catalog
            .set_setting("price.ingest_byte", None)
            .await
            .unwrap();
        let state = catalog.snapshot().await.unwrap();
        assert_eq!(state.state().setting("price.ingest_byte"), None);
    }

    #[tokio::test]
    async fn opening_an_empty_root_pool_creates_the_builtin_tenants() {
        let catalog = Cluster::new().node(1).await;
        let state = catalog.cached();
        assert_eq!(state.version(), Version::new(1));
        assert_eq!(
            state.state().tenant(&TenantId::DEFAULT).unwrap().name,
            name("default")
        );
        assert_eq!(
            state.state().tenant(&TenantId::SYSTEM).unwrap().name,
            name("_system")
        );
        assert_eq!(
            state.state().pool(&PoolId::new("root").unwrap()),
            Some(&root_pool())
        );
    }

    #[tokio::test]
    async fn opening_again_changes_nothing() {
        let cluster = Cluster::new();
        cluster.node(1).await;
        let second = cluster.node(2).await;
        assert_eq!(second.cached().version(), Version::new(1));
    }

    #[tokio::test]
    async fn creates_a_tenant_with_a_random_id() {
        let catalog = Cluster::new().node(1).await;
        let tenant = catalog
            .create_tenant(name("acme"), TenantSettings::default())
            .await
            .unwrap();
        assert_eq!(tenant.id.as_str().len(), 32);
        assert_eq!(tenant.status, TenantStatus::Active);
        assert_eq!(tenant.created_at, Timestamp::from_nanos(1_000));
        assert_eq!(
            catalog.cached().state().tenant_by_name(&name("acme")),
            Some(&tenant)
        );
    }

    #[tokio::test]
    async fn rejects_a_duplicate_name() {
        let catalog = Cluster::new().node(1).await;
        catalog
            .create_tenant(name("acme"), TenantSettings::default())
            .await
            .unwrap();
        let error = catalog
            .create_tenant(name("acme"), TenantSettings::default())
            .await
            .unwrap_err();
        assert!(matches!(error, CatalogError::TenantExists(_)), "{error}");
    }

    #[tokio::test]
    async fn rejects_reserved_names() {
        let catalog = Cluster::new().node(1).await;
        let error = catalog
            .create_tenant(name("_audit"), TenantSettings::default())
            .await
            .unwrap_err();
        assert!(matches!(error, CatalogError::ReservedName(_)), "{error}");
        let error = catalog
            .rename_tenant(&name("default"), name("_x"))
            .await
            .unwrap_err();
        assert!(matches!(error, CatalogError::ReservedName(_)), "{error}");
    }

    #[tokio::test]
    async fn alters_settings_by_merging() {
        let catalog = Cluster::new().node(1).await;
        let initial = TenantSettings {
            retention_seconds: Some(30),
            ..TenantSettings::default()
        };
        catalog.create_tenant(name("acme"), initial).await.unwrap();
        let patch = TenantSettings {
            query_concurrency: Some(64),
            ..TenantSettings::default()
        };
        let altered = catalog.alter_tenant(&name("acme"), patch).await.unwrap();
        assert_eq!(altered.settings.retention_seconds, Some(30));
        assert_eq!(altered.settings.query_concurrency, Some(64));
    }

    /// SE-2: the catalog answers which key a tenant writes under, from
    /// its `encryption_key` setting.
    #[tokio::test]
    async fn names_the_key_a_tenant_writes_under() {
        let catalog = Cluster::new().node(1).await;
        let sealed = TenantSettings {
            encryption_key: Some("k2025".into()),
            ..TenantSettings::default()
        };
        let acme = catalog.create_tenant(name("acme"), sealed).await.unwrap();
        assert_eq!(catalog.key_name(&acme.id).as_deref(), Some("k2025"));
        assert_eq!(catalog.key_name(&TenantId::DEFAULT), None);
        assert_eq!(catalog.key_name(&TenantId::new("nobody").unwrap()), None);
    }

    #[tokio::test]
    async fn refuses_invalid_settings_on_create_and_alter() {
        use tessera_types::{Budget, BudgetAction};

        let catalog = Cluster::new().node(1).await;
        let sampled_queries = TenantSettings {
            query_budget: Some(Budget {
                bytes_per_day: 1,
                action: BudgetAction::Sample { rate: 0.5 },
            }),
            ..TenantSettings::default()
        };
        let error = catalog
            .create_tenant(name("acme"), sampled_queries.clone())
            .await
            .unwrap_err();
        assert!(matches!(error, CatalogError::InvalidSettings(_)), "{error}");
        let error = catalog
            .alter_tenant(&name("default"), sampled_queries)
            .await
            .unwrap_err();
        assert!(matches!(error, CatalogError::InvalidSettings(_)), "{error}");
        let snapshot = catalog.cached();
        let default = snapshot.state().tenant(&TenantId::DEFAULT).unwrap();
        assert_eq!(default.settings.query_budget, None, "nothing was stored");
    }

    #[tokio::test]
    async fn renames_a_tenant_keeping_its_id() {
        let catalog = Cluster::new().node(1).await;
        let created = catalog
            .create_tenant(name("acme"), TenantSettings::default())
            .await
            .unwrap();
        let renamed = catalog
            .rename_tenant(&name("acme"), name("acme_corp"))
            .await
            .unwrap();
        assert_eq!(renamed.id, created.id);
        assert_eq!(catalog.cached().state().tenant_by_name(&name("acme")), None);
        assert!(
            catalog
                .create_tenant(name("acme"), TenantSettings::default())
                .await
                .is_ok(),
            "old name is free"
        );
    }

    #[tokio::test]
    async fn renaming_onto_a_taken_name_is_rejected() {
        let catalog = Cluster::new().node(1).await;
        catalog
            .create_tenant(name("a"), TenantSettings::default())
            .await
            .unwrap();
        catalog
            .create_tenant(name("b"), TenantSettings::default())
            .await
            .unwrap();
        let error = catalog
            .rename_tenant(&name("a"), name("b"))
            .await
            .unwrap_err();
        assert!(matches!(error, CatalogError::TenantExists(_)), "{error}");
    }

    #[tokio::test]
    async fn suspends_and_resumes() {
        let catalog = Cluster::new().node(1).await;
        catalog
            .create_tenant(name("acme"), TenantSettings::default())
            .await
            .unwrap();
        assert_eq!(
            catalog.suspend_tenant(&name("acme")).await.unwrap().status,
            TenantStatus::Suspended
        );
        let error = catalog.suspend_tenant(&name("acme")).await.unwrap_err();
        assert!(matches!(error, CatalogError::WrongStatus { .. }), "{error}");
        assert_eq!(
            catalog.resume_tenant(&name("acme")).await.unwrap().status,
            TenantStatus::Active
        );
    }

    #[tokio::test]
    async fn drops_then_restores() {
        let cluster = Cluster::new();
        let catalog = cluster.node(1).await;
        catalog
            .create_tenant(name("acme"), TenantSettings::default())
            .await
            .unwrap();
        let dropped = catalog.drop_tenant(&name("acme")).await.unwrap();
        assert_eq!(
            dropped.status,
            TenantStatus::Dropped {
                at: cluster.clock.now()
            }
        );
        let error = catalog
            .create_tenant(name("acme"), TenantSettings::default())
            .await
            .unwrap_err();
        assert!(
            matches!(error, CatalogError::TenantExists(_)),
            "a dropped tenant keeps its name"
        );
        assert_eq!(
            catalog.restore_tenant(&name("acme")).await.unwrap().status,
            TenantStatus::Active
        );
    }

    #[tokio::test]
    async fn builtin_tenants_are_protected() {
        let catalog = Cluster::new().node(1).await;
        for builtin in ["default", "_system"] {
            let error = catalog.drop_tenant(&name(builtin)).await.unwrap_err();
            assert!(matches!(error, CatalogError::BuiltinTenant(_)), "{error}");
            let error = catalog.suspend_tenant(&name(builtin)).await.unwrap_err();
            assert!(matches!(error, CatalogError::BuiltinTenant(_)), "{error}");
            let error = catalog
                .rename_tenant(&name(builtin), name("other"))
                .await
                .unwrap_err();
            assert!(matches!(error, CatalogError::BuiltinTenant(_)), "{error}");
        }
        let altered = catalog
            .alter_tenant(
                &name("default"),
                TenantSettings {
                    query_concurrency: Some(8),
                    ..TenantSettings::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            altered.settings.query_concurrency,
            Some(8),
            "settings of built-ins can change"
        );
    }

    #[tokio::test]
    async fn a_dropped_tenant_is_purged_only_after_the_grace_period() {
        let cluster = Cluster::new();
        let catalog = cluster.node(1).await;
        catalog
            .create_tenant(name("acme"), TenantSettings::default())
            .await
            .unwrap();
        catalog.drop_tenant(&name("acme")).await.unwrap();
        let error = catalog.purge_tenant(&name("acme")).await.unwrap_err();
        assert!(matches!(error, CatalogError::GracePeriod(_)), "{error}");
        cluster.clock.advance(Catalog::DEFAULT_DROP_GRACE);
        catalog.purge_tenant(&name("acme")).await.unwrap();
        assert_eq!(catalog.cached().state().tenant_by_name(&name("acme")), None);
        let error = catalog.purge_tenant(&name("acme")).await.unwrap_err();
        assert!(matches!(error, CatalogError::UnknownTenant(_)), "{error}");
    }

    #[tokio::test]
    async fn a_change_is_visible_to_another_node_after_refresh() {
        let cluster = Cluster::new();
        let first = cluster.node(1).await;
        let second = cluster.node(2).await;
        first
            .create_tenant(name("acme"), TenantSettings::default())
            .await
            .unwrap();
        assert!(
            second
                .cached()
                .state()
                .tenant_by_name(&name("acme"))
                .is_none(),
            "not yet refreshed"
        );
        let refreshed = second.snapshot().await.unwrap();
        assert!(refreshed.state().tenant_by_name(&name("acme")).is_some());
    }

    #[tokio::test]
    async fn two_nodes_creating_the_same_name_have_one_winner() {
        let cluster = Cluster::new();
        let first = cluster.node(1).await;
        let second = cluster.node(2).await;
        first
            .create_tenant(name("acme"), TenantSettings::default())
            .await
            .unwrap();
        // The second node's cache is stale; its create rebases and then sees the name.
        let error = second
            .create_tenant(name("acme"), TenantSettings::default())
            .await
            .unwrap_err();
        assert!(matches!(error, CatalogError::TenantExists(_)), "{error}");
        assert_eq!(second.cached().state().tenants().count(), 3);
    }

    #[tokio::test]
    async fn manages_pools() {
        let catalog = Cluster::new().node(1).await;
        let extra = PoolConfig {
            id: PoolId::new("extra").unwrap(),
            url: "file:///x".into(),
            weight: 3,
            state: PoolState::Active,
        };
        catalog.add_pool(extra.clone()).await.unwrap();
        let error = catalog.add_pool(extra.clone()).await.unwrap_err();
        assert!(matches!(error, CatalogError::PoolExists(_)), "{error}");
        catalog.set_pool_weight(&extra.id, 5).await.unwrap();
        catalog.drain_pool(&extra.id).await.unwrap();
        let pool = catalog.cached().state().pool(&extra.id).cloned().unwrap();
        assert_eq!((pool.weight, pool.state), (5, PoolState::Draining));
        let error = catalog
            .remove_pool(&PoolId::new("root").unwrap())
            .await
            .unwrap_err();
        assert!(matches!(error, CatalogError::RootPool(_)), "{error}");
        catalog.remove_pool(&extra.id).await.unwrap();
        assert!(catalog.cached().state().pool(&extra.id).is_none());
    }

    #[tokio::test]
    async fn doubles_slot_counts() {
        let catalog = Cluster::new().node(1).await;
        assert_eq!(
            catalog.cached().state().slot_count(WorkKind::Intake),
            SlotCount::ONE
        );
        assert_eq!(
            catalog.double_slots(WorkKind::Intake).await.unwrap().get(),
            2
        );
        assert_eq!(
            catalog.double_slots(WorkKind::Intake).await.unwrap().get(),
            4
        );
        assert_eq!(
            catalog.cached().state().slot_count(WorkKind::Partition),
            SlotCount::ONE
        );
    }

    #[tokio::test]
    async fn a_checkpoint_serves_a_cold_node() {
        let cluster = Cluster::new();
        let first = cluster.node(1).await;
        first
            .create_tenant(name("acme"), TenantSettings::default())
            .await
            .unwrap();
        first.checkpoint().await.unwrap();
        for v in 1..=2u64 {
            let key = Key::new(format!("_cluster/catalog/_log/{}", Version::new(v))).unwrap();
            cluster.store.delete(&key).await.unwrap();
        }
        let cold = cluster.node(2).await;
        assert!(
            cold.cached()
                .state()
                .tenant_by_name(&name("acme"))
                .is_some()
        );
    }
}
