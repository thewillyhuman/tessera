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
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tessera_commit::{Action, CommitError, CommitLog, KvMutation, KvState, UpdateError};
use tessera_format::{IndexSpec, Layout};
use tessera_object::{ObjectStore, layout};
use tessera_types::{DatasetName, Env, Kind, TenantId, Timestamp, WindowSchedule};

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
    clock: Arc<dyn tessera_types::Clock>,
}

fn layout_key(kind: Kind) -> String {
    format!("layout/{kind}")
}

fn advice_key(kind: Kind) -> String {
    format!("advice/{kind}")
}

const WINDOW_KEY: &str = "window_seconds";
const WINDOW_SCHEDULE_KEY: &str = "window_schedule";

fn tombstones_key(kind: Kind) -> String {
    format!("tombstones/{kind}")
}

/// A delete by predicate (SE-5): rows of the kind that match the
/// predicate and were ingested before the delete are hidden from every
/// read at once, and removed by compaction, which rewrites the files
/// older than the layout epoch the delete opened. The predicate is the
/// plan expression as JSON, lowered against the kind's schema.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Tombstone {
    /// The tombstone's id.
    pub id: String,
    /// The predicate over the kind's rows, as the plan expression's JSON.
    pub predicate: serde_json::Value,
    /// The predicate as written, for people.
    pub text: String,
    /// When the delete was issued; rows observed after it are not
    /// affected.
    pub created_at: Timestamp,
    /// The layout epoch the delete opened: a file of this epoch or a
    /// later one was written with the tombstone applied.
    pub epoch: u64,
}

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

/// The window schedule `state` holds: the schedule key, else the one
/// length an older release wrote, else the default.
fn window_schedule(state: &KvState) -> WindowSchedule {
    if let Some(schedule) = state
        .get(WINDOW_SCHEDULE_KEY)
        .and_then(|s| serde_json::from_str(s).ok())
    {
        return schedule;
    }
    state
        .get(WINDOW_KEY)
        .and_then(|s| s.parse::<u64>().ok())
        .map_or_else(WindowSchedule::default, |secs| {
            WindowSchedule::of(Duration::from_secs(secs))
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
            clock: Arc::clone(&env.clock),
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

    /// The tombstones of `kind` still to be applied everywhere.
    ///
    /// # Errors
    ///
    /// If the log cannot be read or the record does not decode.
    pub async fn tombstones(&self, kind: Kind) -> Result<Vec<Tombstone>, DatasetLogError> {
        let snapshot = self.log.snapshot().await?;
        let key = tombstones_key(kind);
        match snapshot.state().get(&key) {
            Some(text) => decode(&key, text),
            None => Ok(Vec::new()),
        }
    }

    /// Adds `tombstone` for `kind` and opens the next layout epoch, so
    /// that every file written before it is rewritten with the tombstone
    /// applied (AD-10). Returns the tombstone with its epoch.
    ///
    /// # Errors
    ///
    /// If the log cannot be committed.
    pub async fn add_tombstone(
        &self,
        kind: Kind,
        tombstone: Tombstone,
    ) -> Result<Tombstone, DatasetLogError> {
        let layout_key = layout_key(kind);
        let key = tombstones_key(kind);
        let snapshot = self
            .log
            .update(|state| {
                let layout = match state.get(&layout_key) {
                    Some(text) => decode::<LayoutRecord>(&layout_key, text)?,
                    None => LayoutRecord::default_for(kind),
                };
                let next = LayoutRecord {
                    epoch: layout.epoch + 1,
                    ..layout
                };
                let mut tombstones: Vec<Tombstone> = match state.get(&key) {
                    Some(text) => decode(&key, text)?,
                    None => Vec::new(),
                };
                tombstones.push(Tombstone {
                    epoch: next.epoch,
                    ..tombstone.clone()
                });
                Ok::<_, DatasetLogError>(vec![
                    put(layout_key.clone(), &next),
                    put(key.clone(), &tombstones),
                ])
            })
            .await?;
        let tombstones: Vec<Tombstone> = match snapshot.state().get(&key) {
            Some(text) => decode(&key, text)?,
            None => Vec::new(),
        };
        tombstones
            .into_iter()
            .find(|t| t.id == tombstone.id)
            .ok_or_else(|| DatasetLogError::Corrupt {
                key,
                source: serde_json::Error::io(std::io::Error::other("tombstone vanished")),
            })
    }

    /// Removes the tombstones of `kind` that `done` says are applied
    /// everywhere; returns how many were removed.
    ///
    /// # Errors
    ///
    /// If the log cannot be committed.
    pub async fn remove_tombstones(
        &self,
        kind: Kind,
        done: impl Fn(&Tombstone) -> bool,
    ) -> Result<usize, DatasetLogError> {
        let key = tombstones_key(kind);
        let removed = std::sync::Mutex::new(0);
        self.log
            .update(|state| {
                let tombstones: Vec<Tombstone> = match state.get(&key) {
                    Some(text) => decode(&key, text)?,
                    None => Vec::new(),
                };
                let kept: Vec<Tombstone> =
                    tombstones.iter().filter(|t| !done(t)).cloned().collect();
                *removed
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) =
                    tombstones.len() - kept.len();
                if kept.len() == tombstones.len() {
                    return Ok::<_, DatasetLogError>(Vec::new());
                }
                Ok(vec![put(key.clone(), &kept)])
            })
            .await?;
        let removed = *removed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(removed)
    }

    /// The partition windows of the dataset over time (MF-1, MF-2,
    /// AD-14): the schedule set, else one length for all time, the
    /// one an older release set or the default.
    ///
    /// # Errors
    ///
    /// If the log cannot be read.
    pub async fn window(&self) -> Result<WindowSchedule, DatasetLogError> {
        let snapshot = self.log.snapshot().await?;
        Ok(window_schedule(snapshot.state()))
    }

    /// Schedules `window` for the partitions whose window has not
    /// started: from the end of the window in force now (MF-2).
    /// Nothing is written when the schedule already ends in `window`.
    ///
    /// # Errors
    ///
    /// If the log cannot be committed.
    pub async fn set_window(&self, window: Duration) -> Result<(), DatasetLogError> {
        let now = self.clock.now();
        self.log
            .update(|state| {
                let mut schedule = window_schedule(state);
                if !schedule.set(window, now) {
                    return Ok::<_, DatasetLogError>(Vec::new());
                }
                let value = serde_json::to_string(&schedule)
                    .unwrap_or_else(|e| unreachable!("a schedule is JSON: {e}"));
                Ok(vec![Action::Kv(KvMutation::Put {
                    key: WINDOW_SCHEDULE_KEY.to_owned(),
                    value,
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
    /// The open logs, each with when it was last asked for.
    open: Mutex<HashMap<(TenantId, DatasetName), OpenLog>>,
}

/// An open log and when it was last asked for.
type OpenLog = (Instant, Arc<DatasetLog>);

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
        let (used, log) = open
            .entry((tenant.clone(), dataset.clone()))
            .or_insert_with(|| {
                let log =
                    DatasetLog::open(Arc::clone(&self.store), tenant, dataset, self.env.clone());
                (Instant::now(), Arc::new(log))
            });
        *used = Instant::now();
        Arc::clone(log)
    }

    /// Forgets the logs nobody asked for in `idle` (CT-6), so that a
    /// tenant that sends and queries nothing costs no memory here; the
    /// next use opens the log again. Returns how many were forgotten.
    pub fn release_idle(&self, idle: Duration) -> usize {
        let mut open = self.open.lock().unwrap_or_else(PoisonError::into_inner);
        let before = open.len();
        open.retain(|_, (used, _)| used.elapsed() < idle);
        before - open.len()
    }
}

#[cfg(test)]
mod tests {
    use tessera_types::Window;
    #[test]
    fn idle_logs_are_released_and_reopened_on_use() {
        use std::sync::Arc;

        use tessera_object::MemoryStore;

        let logs = DatasetLogs::new(
            Arc::new(MemoryStore::new()),
            Env::simulated(1, Timestamp::from_nanos(0)),
        );
        let dataset = DatasetName::new("prod").unwrap();
        let first = logs.open(&TenantId::DEFAULT, &dataset);
        assert!(Arc::ptr_eq(
            &first,
            &logs.open(&TenantId::DEFAULT, &dataset)
        ));
        assert_eq!(logs.release_idle(Duration::from_secs(60)), 0, "just used");
        assert_eq!(logs.release_idle(Duration::ZERO), 1);
        assert!(!Arc::ptr_eq(
            &first,
            &logs.open(&TenantId::DEFAULT, &dataset)
        ));
    }

    #[tokio::test]
    async fn a_tombstone_opens_the_next_epoch_and_is_removed_once_done() {
        use std::sync::Arc;

        use tessera_object::MemoryStore;

        let log = DatasetLog::open(
            Arc::new(MemoryStore::new()),
            &TenantId::DEFAULT,
            &DatasetName::new("prod").unwrap(),
            Env::simulated(1, Timestamp::from_nanos(0)),
        );
        assert_eq!(log.layout(Kind::Log).await.unwrap().epoch, 0);
        let stored = log
            .add_tombstone(
                Kind::Log,
                Tombstone {
                    id: "t1".into(),
                    predicate: serde_json::json!({"Column": "body"}),
                    text: "body = 'x'".into(),
                    created_at: Timestamp::from_nanos(5),
                    epoch: 0,
                },
            )
            .await
            .unwrap();
        assert_eq!(stored.epoch, 1, "the delete opened epoch 1");
        assert_eq!(log.layout(Kind::Log).await.unwrap().epoch, 1);
        assert_eq!(
            log.tombstones(Kind::Log).await.unwrap(),
            vec![stored.clone()]
        );
        assert!(log.tombstones(Kind::Span).await.unwrap().is_empty());
        assert_eq!(
            log.remove_tombstones(Kind::Log, |t| t.epoch == 0)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            log.remove_tombstones(Kind::Log, |t| t.epoch <= 1)
                .await
                .unwrap(),
            1
        );
        assert!(log.tombstones(Kind::Log).await.unwrap().is_empty());
    }

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
        assert_eq!(log.window().await.unwrap(), WindowSchedule::default());
        log.set_window(Duration::from_secs(3_600)).await.unwrap();
        // MF-2: the change waits for the end of the day in force; a
        // repeat of it writes nothing.
        let schedule = log.window().await.unwrap();
        assert_eq!(schedule.latest(), Duration::from_secs(3_600));
        assert_eq!(
            schedule.length_at(Timestamp::UNIX_EPOCH),
            Window::DEFAULT_LENGTH
        );
        assert_eq!(
            schedule.changes()[1].from,
            Timestamp::from_nanos(86_400 * 1_000_000_000)
        );
        let version = log.log.snapshot().await.unwrap().version();
        log.set_window(Duration::from_secs(3_600)).await.unwrap();
        assert_eq!(log.log.snapshot().await.unwrap().version(), version);
        // A log an older release wrote with one length reads as one.
        log.log
            .update(|_| {
                Ok::<_, DatasetLogError>(vec![
                    Action::Kv(KvMutation::Delete {
                        key: WINDOW_SCHEDULE_KEY.to_owned(),
                    }),
                    Action::Kv(KvMutation::Put {
                        key: WINDOW_KEY.to_owned(),
                        value: "600".to_owned(),
                    }),
                ])
            })
            .await
            .unwrap();
        assert_eq!(
            log.window().await.unwrap(),
            WindowSchedule::of(Duration::from_secs(600))
        );
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
