// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The dataset log (section 6.2, `{tenant}/{dataset}/_log/`): the
//! dataset's physical design as the advisor sets it and the writers
//! read it. A layout is versioned by a layout epoch (AD-9): every
//! `SetLayout` is a new epoch, new files carry the epoch they were
//! written under, and files of older epochs stay valid and queryable.
//! The log also holds the partition window (AD-14) and the advisor's
//! last report, which `EXPLAIN LAYOUT` shows (AD-11).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tessera_commit::{Action, CommitError, CommitLog, KvMutation, KvState, UpdateError};
use tessera_format::{IndexSpec, Layout};
use tessera_object::{ObjectStore, layout};
use tessera_types::{DatasetName, Env, Kind, TenantId, Window};

/// The layout of one kind of event in a dataset, as the dataset log
/// records it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LayoutRecord {
    /// The layout epoch (AD-9); zero is the default layout.
    pub epoch: u64,
    /// The sort key, `timestamp` appended by the writer.
    pub sort_key: Vec<String>,
    /// Attribute paths written as dedicated columns (section 8.2),
    /// besides the ones promoted by density.
    #[serde(default)]
    pub dedicated: Vec<String>,
    /// Columns with a Bloom filter.
    #[serde(default)]
    pub bloom: Vec<String>,
    /// Text columns with a term index.
    #[serde(default)]
    pub term: Vec<String>,
    /// Text columns with a trigram index.
    #[serde(default)]
    pub trigram: Vec<String>,
}

impl LayoutRecord {
    /// The default layout of `kind` (section 8.5), epoch zero.
    #[must_use]
    pub fn default_for(kind: Kind) -> Self {
        let layout = Layout::default_for(kind);
        LayoutRecord {
            epoch: 0,
            sort_key: layout.sort_key,
            dedicated: Vec::new(),
            bloom: layout.indexes.bloom,
            term: layout.indexes.term,
            trigram: layout.indexes.trigram,
        }
    }

    /// The file layout this record asks for.
    #[must_use]
    pub fn to_layout(&self) -> Layout {
        Layout {
            sort_key: self.sort_key.clone(),
            row_group_rows: tessera_format::DEFAULT_ROW_GROUP_ROWS,
            indexes: IndexSpec {
                term: self.term.clone(),
                trigram: self.trigram.clone(),
                bloom: self.bloom.clone(),
            },
        }
    }
}

/// What the advisor last found for one kind, for `EXPLAIN LAYOUT`
/// (AD-11).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Advice {
    /// When the advisor ran.
    pub at: tessera_types::Timestamp,
    /// The layout it proposes, which may be the current one.
    pub proposed: LayoutRecord,
    /// The alternatives considered, with their estimated costs.
    pub alternatives: Vec<Alternative>,
    /// How many consecutive runs proposed the same layout (AD-7).
    pub streak: u32,
    /// The queries that drove the choice, as the workload log shows
    /// them.
    pub drivers: Vec<String>,
}

/// One layout the advisor costed.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Alternative {
    /// The sort key.
    pub sort_key: Vec<String>,
    /// The estimated cost, lower is better.
    pub cost: f64,
    /// Whether this is the current layout.
    pub current: bool,
}

/// Why the dataset log failed.
#[derive(Debug, thiserror::Error)]
pub enum DatasetLogError {
    /// The log failed.
    #[error(transparent)]
    Log(#[from] CommitError),
    /// Too many writers at once.
    #[error("dataset log contention: gave up after {attempts} attempts")]
    Contention {
        /// How many attempts were made.
        attempts: u32,
    },
    /// A record does not decode.
    #[error("dataset log entry {key} does not decode: {source}")]
    Corrupt {
        /// The entry.
        key: String,
        /// The failure.
        #[source]
        source: serde_json::Error,
    },
}

impl From<UpdateError<DatasetLogError>> for DatasetLogError {
    fn from(error: UpdateError<DatasetLogError>) -> Self {
        match error {
            UpdateError::Rejected(rejection) => rejection,
            UpdateError::Commit(commit) => DatasetLogError::Log(commit),
            UpdateError::Contention { attempts } => DatasetLogError::Contention { attempts },
        }
    }
}

/// The log of one dataset.
pub struct DatasetLog {
    log: CommitLog<KvState>,
}

fn layout_key(kind: Kind) -> String {
    format!("layout/{kind}")
}

fn advice_key(kind: Kind) -> String {
    format!("advice/{kind}")
}

const WINDOW_KEY: &str = "window_seconds";

fn decode<T: for<'de> Deserialize<'de>>(key: &str, text: &str) -> Result<T, DatasetLogError> {
    serde_json::from_str(text).map_err(|source| DatasetLogError::Corrupt {
        key: key.to_owned(),
        source,
    })
}

fn put<T: Serialize>(key: String, value: &T) -> Action {
    Action::Kv(KvMutation::Put {
        key,
        value: serde_json::to_string(value).unwrap_or_default(),
    })
}

impl DatasetLog {
    /// The log of `dataset` in `store`.
    #[must_use]
    pub fn open(
        store: Arc<dyn ObjectStore>,
        tenant: &TenantId,
        dataset: &DatasetName,
        env: Env,
    ) -> Self {
        DatasetLog {
            log: CommitLog::new(store, layout::dataset_log(tenant, dataset), env),
        }
    }

    /// The layout of `kind`: the one set, else the default.
    ///
    /// # Errors
    ///
    /// If the log cannot be read or the record does not decode.
    pub async fn layout(&self, kind: Kind) -> Result<LayoutRecord, DatasetLogError> {
        let snapshot = self.log.snapshot().await?;
        let key = layout_key(kind);
        match snapshot.state().get(&key) {
            Some(text) => decode(&key, text),
            None => Ok(LayoutRecord::default_for(kind)),
        }
    }

    /// Sets the layout of `kind` as the next epoch (`SetLayout`, AD-9)
    /// and returns the record with its epoch.
    ///
    /// # Errors
    ///
    /// If the log cannot be committed.
    pub async fn set_layout(
        &self,
        kind: Kind,
        record: LayoutRecord,
    ) -> Result<LayoutRecord, DatasetLogError> {
        let key = layout_key(kind);
        let snapshot = self
            .log
            .update(|state| {
                let current = match state.get(&key) {
                    Some(text) => decode::<LayoutRecord>(&key, text)?.epoch,
                    None => 0,
                };
                let next = LayoutRecord {
                    epoch: current + 1,
                    ..record.clone()
                };
                Ok::<_, DatasetLogError>(vec![put(key.clone(), &next)])
            })
            .await?;
        let text = snapshot
            .state()
            .get(&key)
            .ok_or_else(|| DatasetLogError::Corrupt {
                key: key.clone(),
                source: serde_json::Error::io(std::io::Error::other("layout vanished")),
            })?;
        decode(&key, text)
    }

    /// The partition window of the dataset (MF-1, AD-14): the one set,
    /// else the default.
    ///
    /// # Errors
    ///
    /// If the log cannot be read.
    pub async fn window(&self) -> Result<Duration, DatasetLogError> {
        let snapshot = self.log.snapshot().await?;
        Ok(snapshot
            .state()
            .get(WINDOW_KEY)
            .and_then(|s| s.parse::<u64>().ok())
            .map_or(Window::DEFAULT_LENGTH, Duration::from_secs))
    }

    /// Sets the partition window for partitions not yet created.
    ///
    /// # Errors
    ///
    /// If the log cannot be committed.
    pub async fn set_window(&self, window: Duration) -> Result<(), DatasetLogError> {
        self.log
            .update(|_| {
                Ok::<_, DatasetLogError>(vec![Action::Kv(KvMutation::Put {
                    key: WINDOW_KEY.to_owned(),
                    value: window.as_secs().to_string(),
                })])
            })
            .await?;
        Ok(())
    }

    /// The value score of `kind` (section 13.3): bytes read over bytes
    /// stored, as the advisor last measured it; none until it has.
    ///
    /// # Errors
    ///
    /// If the log cannot be read.
    pub async fn value_score(&self, kind: Kind) -> Result<Option<f64>, DatasetLogError> {
        let snapshot = self.log.snapshot().await?;
        Ok(snapshot
            .state()
            .get(&format!("value_score/{kind}"))
            .and_then(|s| s.parse::<f64>().ok()))
    }

    /// Records the value score of `kind`.
    ///
    /// # Errors
    ///
    /// If the log cannot be committed.
    pub async fn set_value_score(&self, kind: Kind, score: f64) -> Result<(), DatasetLogError> {
        self.log
            .update(|_| {
                Ok::<_, DatasetLogError>(vec![Action::Kv(KvMutation::Put {
                    key: format!("value_score/{kind}"),
                    value: score.to_string(),
                })])
            })
            .await?;
        Ok(())
    }

    /// The advisor's last report for `kind`, if any.
    ///
    /// # Errors
    ///
    /// If the log cannot be read or the record does not decode.
    pub async fn advice(&self, kind: Kind) -> Result<Option<Advice>, DatasetLogError> {
        let snapshot = self.log.snapshot().await?;
        let key = advice_key(kind);
        snapshot
            .state()
            .get(&key)
            .map(|text| decode(&key, text))
            .transpose()
    }

    /// Records the advisor's report for `kind`.
    ///
    /// # Errors
    ///
    /// If the log cannot be committed.
    pub async fn set_advice(&self, kind: Kind, advice: &Advice) -> Result<(), DatasetLogError> {
        let key = advice_key(kind);
        self.log
            .update(|_| Ok::<_, DatasetLogError>(vec![put(key.clone(), advice)]))
            .await?;
        Ok(())
    }
}

/// The dataset logs a node has opened, so that each keeps its snapshot.
pub struct DatasetLogs {
    store: Arc<dyn ObjectStore>,
    env: Env,
    open: Mutex<HashMap<(TenantId, DatasetName), Arc<DatasetLog>>>,
}

impl DatasetLogs {
    /// Logs in `store`.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, env: Env) -> Self {
        DatasetLogs {
            store,
            env,
            open: Mutex::new(HashMap::new()),
        }
    }

    /// The log of `dataset`: the one already open, or a new one.
    #[must_use]
    pub fn open(&self, tenant: &TenantId, dataset: &DatasetName) -> Arc<DatasetLog> {
        let mut open = self.open.lock().unwrap_or_else(PoisonError::into_inner);
        Arc::clone(
            open.entry((tenant.clone(), dataset.clone()))
                .or_insert_with(|| {
                    Arc::new(DatasetLog::open(
                        Arc::clone(&self.store),
                        tenant,
                        dataset,
                        self.env.clone(),
                    ))
                }),
        )
    }
}

#[cfg(test)]
mod tests {
    use tessera_object::MemoryStore;
    use tessera_types::Timestamp;

    use super::*;

    fn log() -> DatasetLog {
        DatasetLog::open(
            Arc::new(MemoryStore::new()),
            &TenantId::DEFAULT,
            &DatasetName::new("prod").unwrap(),
            Env::simulated(1, Timestamp::from_nanos(0)),
        )
    }

    #[tokio::test]
    async fn layouts_start_at_the_default_and_each_set_is_a_new_epoch() {
        let log = log();
        let initial = log.layout(Kind::Log).await.unwrap();
        assert_eq!(initial, LayoutRecord::default_for(Kind::Log));
        assert_eq!(initial.epoch, 0);
        assert_eq!(initial.to_layout(), Layout::default_for(Kind::Log));
        let mut wanted = initial.clone();
        wanted.sort_key = vec!["attr.host".into(), "severity_number".into()];
        wanted.dedicated = vec!["attr.host".into()];
        let set = log.set_layout(Kind::Log, wanted.clone()).await.unwrap();
        assert_eq!(set.epoch, 1);
        assert_eq!(set.sort_key, wanted.sort_key);
        let again = log.set_layout(Kind::Log, wanted).await.unwrap();
        assert_eq!(again.epoch, 2);
        assert_eq!(log.layout(Kind::Log).await.unwrap().epoch, 2);
        assert_eq!(
            log.layout(Kind::Span).await.unwrap(),
            LayoutRecord::default_for(Kind::Span),
            "kinds are independent"
        );
    }

    #[tokio::test]
    async fn the_window_and_the_advice_are_kept() {
        let log = log();
        assert_eq!(log.window().await.unwrap(), Window::DEFAULT_LENGTH);
        log.set_window(Duration::from_secs(3_600)).await.unwrap();
        assert_eq!(log.window().await.unwrap(), Duration::from_secs(3_600));
        assert!(log.advice(Kind::Log).await.unwrap().is_none());
        assert!(log.value_score(Kind::Log).await.unwrap().is_none());
        log.set_value_score(Kind::Log, 0.25).await.unwrap();
        assert_eq!(log.value_score(Kind::Log).await.unwrap(), Some(0.25));
        let advice = Advice {
            at: Timestamp::from_nanos(5),
            proposed: LayoutRecord::default_for(Kind::Log),
            alternatives: vec![Alternative {
                sort_key: vec!["severity_number".into()],
                cost: 1.0,
                current: true,
            }],
            streak: 1,
            drivers: vec!["SELECT ...".into()],
        };
        log.set_advice(Kind::Log, &advice).await.unwrap();
        assert_eq!(log.advice(Kind::Log).await.unwrap(), Some(advice));
    }
}
