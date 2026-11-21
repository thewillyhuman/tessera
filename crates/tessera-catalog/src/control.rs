// SPDX-License-Identifier: Apache-2.0

//! The tenant control store (section 12.3): the small transactional
//! corner, a per-tenant key-value store whose only durable form is the
//! tenant's control log at `{tenant}/_control/_log/`. It holds
//! configuration that people change, in the tables of CS-3, never state
//! that machines churn (CS-4).
//!
//! A transaction runs against a snapshot and is committed with the next
//! version of the log by create-if-absent (CL-1); if another committer
//! got there first, the transaction runs again on the newer snapshot
//! (CS-1), so what it read and what it wrote are always of one version.
//! A committer sees its own writes at once and other nodes see a commit
//! when they next read the log (CS-2).

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use serde_json::Value;
use tessera_commit::{Action, CommitError, CommitLog, KvMutation, KvState, Snapshot, UpdateError};
use tessera_object::{ObjectStore, layout};
use tessera_types::{Env, TenantId, Version};

/// The tables of CS-3.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Table {
    /// Alert rules (section 11.2).
    AlertRules,
    /// Rollups (section 8.7).
    Rollups,
    /// Silences (section 11.8).
    Silences,
    /// Acknowledgments (section 11.8).
    Acknowledgments,
    /// Inhibition rules (section 11.8).
    Inhibitions,
    /// Where notifications go (section 11.9).
    NotificationRoutes,
    /// Retention policies (section 13.3).
    RetentionPolicies,
    /// Layout pins (AD-12).
    LayoutPins,
    /// API keys (CT-5).
    ApiKeys,
    /// Roles and grants.
    Roles,
    /// Saved queries.
    SavedQueries,
}

impl Table {
    /// Every table.
    pub const ALL: [Table; 11] = [
        Table::AlertRules,
        Table::Rollups,
        Table::Silences,
        Table::Acknowledgments,
        Table::Inhibitions,
        Table::NotificationRoutes,
        Table::RetentionPolicies,
        Table::LayoutPins,
        Table::ApiKeys,
        Table::Roles,
        Table::SavedQueries,
    ];

    /// The name under `system.` in SQL.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Table::AlertRules => "alert_rules",
            Table::Rollups => "rollups",
            Table::Silences => "silences",
            Table::Acknowledgments => "acknowledgments",
            Table::Inhibitions => "inhibitions",
            Table::NotificationRoutes => "notification_routes",
            Table::RetentionPolicies => "retention_policies",
            Table::LayoutPins => "layout_pins",
            Table::ApiKeys => "api_keys",
            Table::Roles => "roles",
            Table::SavedQueries => "saved_queries",
        }
    }

    fn prefix(self) -> String {
        format!("{}/", self.as_str())
    }
}

impl std::fmt::Display for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Why a name is not a control table.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("unknown control table {0}")]
pub struct UnknownTable(pub String);

impl FromStr for Table {
    type Err = UnknownTable;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Table::ALL
            .into_iter()
            .find(|t| t.as_str() == text)
            .ok_or_else(|| UnknownTable(text.to_owned()))
    }
}

/// One row of a table: its key and its JSON document.
#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    /// The key within the table.
    pub key: String,
    /// The document.
    pub value: Value,
}

/// Why a control store operation failed.
#[derive(Debug, thiserror::Error)]
pub enum ControlError {
    /// The transaction decided against committing.
    #[error("{0}")]
    Rejected(String),
    /// Too many committers at once; retry.
    #[error("control store contention: gave up after {attempts} attempts")]
    Contention {
        /// How many attempts were made.
        attempts: u32,
    },
    /// A stored document is not JSON.
    #[error("control store entry {key} is not a JSON document: {source}")]
    Corrupt {
        /// The entry.
        key: String,
        /// The failure.
        #[source]
        source: serde_json::Error,
    },
    /// The control log failed.
    #[error(transparent)]
    Log(#[from] CommitError),
}

impl From<UpdateError<ControlError>> for ControlError {
    fn from(error: UpdateError<ControlError>) -> Self {
        match error {
            UpdateError::Rejected(rejection) => rejection,
            UpdateError::Commit(commit) => ControlError::Log(commit),
            UpdateError::Contention { attempts } => ControlError::Contention { attempts },
        }
    }
}

/// The control store of one tenant.
pub struct ControlStore {
    tenant: TenantId,
    log: CommitLog<KvState>,
}

impl ControlStore {
    /// The store of `tenant` in `store`. Nothing is read until asked.
    #[must_use]
    pub fn open(store: Arc<dyn ObjectStore>, tenant: TenantId, env: Env) -> Self {
        let log = CommitLog::new(store, layout::control_log(&tenant), env);
        ControlStore { tenant, log }
    }

    /// The tenant.
    #[must_use]
    pub fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    /// The latest state, read from the log.
    ///
    /// # Errors
    ///
    /// If the log cannot be read.
    pub async fn snapshot(&self) -> Result<Arc<Snapshot<KvState>>, ControlError> {
        Ok(self.log.snapshot().await?)
    }

    /// What the store holds now, read once: for a caller that reads
    /// several tables, which `rows` would read the log for again.
    ///
    /// # Errors
    ///
    /// If the log cannot be read.
    pub async fn state(&self) -> Result<ControlState, ControlError> {
        Ok(ControlState(self.snapshot().await?))
    }

    /// Every row of `table`, in key order, as of the latest state.
    ///
    /// # Errors
    ///
    /// If the log cannot be read or a document is not JSON.
    pub async fn rows(&self, table: Table) -> Result<Vec<Row>, ControlError> {
        self.state().await?.rows(table)
    }

    /// The document at `key` in `table`, as of the latest state.
    ///
    /// # Errors
    ///
    /// If the log cannot be read or the document is not JSON.
    pub async fn get(&self, table: Table, key: &str) -> Result<Option<Value>, ControlError> {
        let snapshot = self.snapshot().await?;
        get_from(snapshot.state(), table, key)
    }

    /// Sets `key` in `table` to `value`, in a transaction of its own.
    ///
    /// # Errors
    ///
    /// If the commit fails.
    pub async fn put(
        &self,
        table: Table,
        key: &str,
        value: Value,
    ) -> Result<Version, ControlError> {
        self.transact(|txn| {
            txn.put(table, key, &value);
            Ok(())
        })
        .await
        .map(|((), version)| version)
    }

    /// Removes `key` from `table`, in a transaction of its own.
    ///
    /// # Errors
    ///
    /// If the commit fails.
    pub async fn delete(&self, table: Table, key: &str) -> Result<Version, ControlError> {
        self.transact(|txn| {
            txn.delete(table, key);
            Ok(())
        })
        .await
        .map(|((), version)| version)
    }

    /// Runs `body` as a serializable transaction (CS-1): it reads the
    /// latest snapshot through the transaction and its writes are
    /// committed as the next version; when another commit got there
    /// first, `body` runs again on the newer snapshot. Returns what
    /// `body` returned and the version that holds its writes, which is
    /// the snapshot's own when it wrote nothing.
    ///
    /// # Errors
    ///
    /// `Rejected` with what `body` said, `Contention` after too many
    /// lost races, or a log failure.
    pub async fn transact<T>(
        &self,
        body: impl Fn(&mut Transaction<'_>) -> Result<T, ControlError>,
    ) -> Result<(T, Version), ControlError> {
        let result = Mutex::new(None);
        let snapshot = self
            .log
            .update(|state| {
                let mut txn = Transaction {
                    state,
                    writes: Vec::new(),
                };
                let value = body(&mut txn)?;
                *result.lock().unwrap_or_else(PoisonError::into_inner) = Some(value);
                Ok(txn.writes)
            })
            .await?;
        let value = result
            .into_inner()
            .unwrap_or_else(PoisonError::into_inner)
            .ok_or_else(|| ControlError::Rejected("transaction produced no result".into()))?;
        Ok((value, snapshot.version()))
    }
}

/// One transaction's view: reads from the snapshot, writes to be
/// committed with it.
pub struct Transaction<'a> {
    state: &'a KvState,
    writes: Vec<Action>,
}

impl Transaction<'_> {
    /// The document at `key` in `table`, as of the snapshot.
    ///
    /// # Errors
    ///
    /// If the document is not JSON.
    pub fn get(&self, table: Table, key: &str) -> Result<Option<Value>, ControlError> {
        get_from(self.state, table, key)
    }

    /// Every row of `table`, as of the snapshot.
    ///
    /// # Errors
    ///
    /// If a document is not JSON.
    pub fn rows(&self, table: Table) -> Result<Vec<Row>, ControlError> {
        rows_of(self.state, table)
    }

    /// Sets `key` in `table` to `value`.
    pub fn put(&mut self, table: Table, key: &str, value: &Value) {
        self.writes.push(Action::Kv(KvMutation::Put {
            key: entry_key(table, key),
            value: value.to_string(),
        }));
    }

    /// Removes `key` from `table`.
    pub fn delete(&mut self, table: Table, key: &str) {
        self.writes.push(Action::Kv(KvMutation::Delete {
            key: entry_key(table, key),
        }));
    }
}

fn entry_key(table: Table, key: &str) -> String {
    format!("{}{key}", table.prefix())
}

fn parse(key: &str, text: &str) -> Result<Value, ControlError> {
    serde_json::from_str(text).map_err(|source| ControlError::Corrupt {
        key: key.to_owned(),
        source,
    })
}

fn get_from(state: &KvState, table: Table, key: &str) -> Result<Option<Value>, ControlError> {
    let entry = entry_key(table, key);
    state
        .get(&entry)
        .map(|text| parse(&entry, text))
        .transpose()
}

fn rows_of(state: &KvState, table: Table) -> Result<Vec<Row>, ControlError> {
    let prefix = table.prefix();
    state
        .range(&prefix)
        .map(|(key, text)| {
            Ok(Row {
                key: key[prefix.len()..].to_owned(),
                value: parse(key, text)?,
            })
        })
        .collect()
}

/// What a tenant's control store held at one version: every table,
/// from one read of the log.
#[derive(Clone, Debug)]
pub struct ControlState(Arc<Snapshot<KvState>>);

impl ControlState {
    /// Every row of `table`, in key order.
    ///
    /// # Errors
    ///
    /// If a document is not JSON.
    pub fn rows(&self, table: Table) -> Result<Vec<Row>, ControlError> {
        rows_of(self.0.state(), table)
    }
}

/// The control stores a node has opened, one per tenant.
pub struct ControlStores {
    store: Arc<dyn ObjectStore>,
    env: Env,
    /// The open stores, each with when it was last asked for.
    open: Mutex<HashMap<TenantId, (Instant, Arc<ControlStore>)>>,
}

impl ControlStores {
    /// Stores in `store`.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, env: Env) -> Self {
        ControlStores {
            store,
            env,
            open: Mutex::new(HashMap::new()),
        }
    }

    /// The store of `tenant`: the one already open, or a new one.
    #[must_use]
    pub fn open(&self, tenant: &TenantId) -> Arc<ControlStore> {
        let mut open = self.open.lock().unwrap_or_else(PoisonError::into_inner);
        let (used, store) = open.entry(tenant.clone()).or_insert_with(|| {
            let store =
                ControlStore::open(Arc::clone(&self.store), tenant.clone(), self.env.clone());
            (Instant::now(), Arc::new(store))
        });
        *used = Instant::now();
        Arc::clone(store)
    }

    /// Forgets the stores nobody asked for in `idle` (CT-6); the next
    /// use opens the store again, reading its log. Returns how many were
    /// forgotten.
    pub fn release_idle(&self, idle: Duration) -> usize {
        let mut open = self.open.lock().unwrap_or_else(PoisonError::into_inner);
        let before = open.len();
        open.retain(|_, (used, _)| used.elapsed() < idle);
        before - open.len()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tessera_object::MemoryStore;
    use tessera_types::Timestamp;

    use super::*;

    fn stores() -> (Arc<MemoryStore>, Env) {
        (
            Arc::new(MemoryStore::new()),
            Env::simulated(1, Timestamp::from_nanos(0)),
        )
    }

    #[test]
    fn idle_stores_are_released_and_reopened_on_use() {
        let (store, env) = stores();
        let stores = ControlStores::new(store, env);
        let first = stores.open(&TenantId::DEFAULT);
        assert!(Arc::ptr_eq(&first, &stores.open(&TenantId::DEFAULT)));
        assert_eq!(stores.release_idle(Duration::from_secs(60)), 0, "just used");
        assert_eq!(stores.release_idle(Duration::ZERO), 1);
        assert!(!Arc::ptr_eq(&first, &stores.open(&TenantId::DEFAULT)));
    }

    #[tokio::test]
    async fn rows_live_in_their_table_and_read_back_as_json() {
        let (store, env) = stores();
        let control = ControlStore::open(store, TenantId::DEFAULT, env);
        assert!(control.rows(Table::AlertRules).await.unwrap().is_empty());
        control
            .put(Table::AlertRules, "b", json!({"expr": "b"}))
            .await
            .unwrap();
        control
            .put(Table::AlertRules, "a", json!({"expr": "a"}))
            .await
            .unwrap();
        control
            .put(Table::Silences, "a", json!({"until": 5}))
            .await
            .unwrap();
        let rules = control.rows(Table::AlertRules).await.unwrap();
        assert_eq!(
            rules.iter().map(|r| r.key.as_str()).collect::<Vec<_>>(),
            ["a", "b"],
            "key order"
        );
        assert_eq!(rules[0].value, json!({"expr": "a"}));
        assert_eq!(
            control.get(Table::Silences, "a").await.unwrap(),
            Some(json!({"until": 5})),
            "the same key in another table is another row"
        );
        control.delete(Table::AlertRules, "a").await.unwrap();
        assert_eq!(control.get(Table::AlertRules, "a").await.unwrap(), None);
        assert_eq!(control.rows(Table::AlertRules).await.unwrap().len(), 1);
    }

    /// One read of the log answers for every table, and goes on
    /// saying what the log said then.
    #[tokio::test]
    async fn a_state_is_the_store_as_it_was_read() {
        let (store, env) = stores();
        let control = ControlStore::open(store, TenantId::DEFAULT, env);
        control
            .put(Table::AlertRules, "a", json!({"expr": "a"}))
            .await
            .unwrap();
        let state = control.state().await.unwrap();
        control
            .put(Table::Silences, "s", json!({"matchers": []}))
            .await
            .unwrap();
        control.delete(Table::AlertRules, "a").await.unwrap();

        assert_eq!(state.rows(Table::AlertRules).unwrap().len(), 1);
        assert!(state.rows(Table::Silences).unwrap().is_empty());
        assert!(control.rows(Table::AlertRules).await.unwrap().is_empty());
        assert_eq!(control.rows(Table::Silences).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_transaction_sees_a_concurrent_commit_before_it_writes() {
        let (store, env) = stores();
        let mine = ControlStore::open(store.clone(), TenantId::DEFAULT, env.clone());
        let theirs = ControlStore::open(store, TenantId::DEFAULT, env);
        mine.put(Table::Roles, "counter", json!(0)).await.unwrap();
        theirs
            .put(Table::Roles, "counter", json!(10))
            .await
            .unwrap();
        // `mine` still caches version one; the transaction must not
        // commit an increment of the stale value.
        let (seen, version) = mine
            .transact(|txn| {
                let current = txn
                    .get(Table::Roles, "counter")?
                    .and_then(|v| v.as_i64())
                    .unwrap_or_default();
                txn.put(Table::Roles, "counter", &json!(current + 1));
                Ok(current)
            })
            .await
            .unwrap();
        assert_eq!(seen, 10, "read of the version it committed against");
        assert_eq!(version, Version::new(3));
        assert_eq!(
            theirs.get(Table::Roles, "counter").await.unwrap(),
            Some(json!(11))
        );
    }

    #[tokio::test]
    async fn a_rejected_transaction_commits_nothing() {
        let (store, env) = stores();
        let control = ControlStore::open(store, TenantId::DEFAULT, env);
        let error = control
            .transact(|txn| {
                txn.put(Table::ApiKeys, "k", &json!("secret"));
                Err::<(), _>(ControlError::Rejected("no".into()))
            })
            .await
            .unwrap_err();
        assert!(matches!(error, ControlError::Rejected(reason) if reason == "no"));
        assert!(control.rows(Table::ApiKeys).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn stores_are_shared_per_tenant() {
        let (store, env) = stores();
        let stores = ControlStores::new(store, env);
        let a = stores.open(&TenantId::DEFAULT);
        assert!(Arc::ptr_eq(&a, &stores.open(&TenantId::DEFAULT)));
        assert_eq!("alert_rules".parse::<Table>().unwrap(), Table::AlertRules);
        assert!("alerts".parse::<Table>().is_err());
    }
}
