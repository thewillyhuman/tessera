// SPDX-License-Identifier: Apache-2.0

//! The source tasks of a rule group with several state partitions
//! (CQ-4).
//!
//! A group with one partition reads the writer logs itself. With more,
//! each writer's log is read once for the whole group: the partition
//! the writer hashes to is the log's source. At each evaluation it
//! reads the new entries, applies each incremental rule's filter and
//! partial aggregation, and leaves the windows each state partition
//! keeps, entry by entry, as one object per partition, named by the
//! last entry it covers:
//! `{tenant}/_eval/{group}/_partials/{parts}/{part}/{writer}/{seq}`.
//! Every partition, the source's own included, then folds the partials
//! left for it instead of the segments (CQ-5). The objects written
//! thus follow the evaluations, not the segments.
//!
//! The task holds no state. A run's objects are written in partition
//! order, so the last partition's object marks a complete set and the
//! newest of them is where the task resumes. A run cut short is done
//! again, perhaps over more entries: a partition that read an object
//! of the first attempt takes from the second the entries after it,
//! since the partials are kept per entry. An entry that holds nothing
//! for the group is still listed, empty: a partition tells a trimmed
//! log from a quiet one by the entries being contiguous, as it does on
//! the log itself.

use std::sync::Arc;

use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use tessera_object::{Key, ObjectError, ObjectStore, layout};
use tessera_types::{Keys, Seq, TenantId, Timestamp, WriterId};
use tessera_wal::{Entry, EntryBody, WalError, reader};

use crate::incremental::{self, Folding, Ingested, Shape, WindowState};
use crate::partition::window_partition;
use crate::rule::AlertRule;
use crate::state::EvalState;

/// How many entries one object of partials covers at most, so that a
/// source far behind catches up an object at a time.
const ENTRIES_PER_OBJECT: usize = 256;

/// What one entry of a writer's log holds for one state partition. An
/// object of partials is the list of them for the entries of one run,
/// in order.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Partials {
    /// The entry.
    pub seq: Seq,
    /// The newest observed time of the entry's segment, for the
    /// writer's watermark; none when the entry is not a segment.
    pub max_observed: Option<Timestamp>,
    /// Whether the entry ends the log.
    pub ended: bool,
    /// The windows of every incremental rule the entry's rows fall in,
    /// folded from nothing, that the partition keeps.
    pub windows: Vec<WindowState>,
}

/// The objects of partials under `prefix`, each by the last entry it
/// covers, ascending.
pub(crate) async fn list(store: &dyn ObjectStore, prefix: &Key) -> Result<Vec<Seq>, String> {
    let mut positions = Vec::new();
    let mut listing = store.list(prefix);
    while let Some(meta) = listing.try_next().await.map_err(|e| e.to_string())? {
        if let Ok(seq) = meta.key.name().parse::<Seq>() {
            positions.push(seq);
        }
    }
    positions.sort_unstable();
    Ok(positions)
}

/// The partials of the object at `key`, in entry order; none when it
/// is gone, trimmed with its entries.
pub(crate) async fn read(
    store: &dyn ObjectStore,
    key: &Key,
) -> Result<Option<Vec<Partials>>, String> {
    match store.get(key).await {
        Ok(body) => serde_json::from_slice(&body)
            .map(Some)
            .map_err(|e| format!("partials at {key} do not decode: {e}")),
        Err(ObjectError::NotFound { .. }) => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}

/// Removes every object under `prefix`: the partials of a partition
/// count the group no longer has.
pub(crate) async fn clear(store: &dyn ObjectStore, prefix: &Key) -> Result<(), String> {
    let mut listing = store.list(prefix);
    while let Some(meta) = listing.try_next().await.map_err(|e| e.to_string())? {
        store.delete(&meta.key).await.map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// The source task of a rule group: what turns the entries of a
/// writer's log into partials for the group's partitions.
pub struct SourceTask<'a> {
    /// The root store, where the writer logs and the partials are.
    pub store: &'a Arc<dyn ObjectStore>,
    /// The keys sealed blocks are unlocked with (SE-2).
    pub keys: &'a Keys,
    /// The tenant.
    pub tenant: &'a TenantId,
    /// The rule group.
    pub group: &'a str,
    /// How many state partitions the group has.
    pub parts: u32,
    /// The group's incremental rules with their shapes.
    pub rules: &'a [(&'a AlertRule, &'a Shape)],
}

/// What a run of a source task did with one writer's log.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Emitted {
    /// The entries whose partials were written.
    pub entries: usize,
    /// The objects of partials removed, garbage collection having
    /// removed every entry they covered (GC-2).
    pub trimmed: usize,
}

impl SourceTask<'_> {
    /// Leaves the partials of the entries of `writer`'s log that have
    /// none yet, and removes those of the entries the log no longer
    /// holds. `held` is where the caller has applied the log to: the
    /// task starts after it when no partials are left to resume from,
    /// as after a re-split, or once the log was trimmed past them all.
    ///
    /// # Errors
    ///
    /// If the log or the partials cannot be listed, read or written.
    pub async fn run(&self, writer: WriterId, held: Seq) -> Result<Emitted, String> {
        let store = &**self.store;
        let log = reader::list(store, writer)
            .await
            .map_err(|e| e.to_string())?;
        let first = log.first().copied();
        let complete = list(store, &self.prefix(self.parts.saturating_sub(1), writer)).await?;
        let mut emitted = Emitted::default();
        if complete
            .first()
            .is_some_and(|oldest| first.is_none_or(|first| *oldest < first))
        {
            emitted.trimmed = self.trim(writer, first).await?;
        }
        let resume = complete
            .last()
            .copied()
            .filter(|newest| first.is_some_and(|first| *newest >= first))
            .unwrap_or(held);
        let new: Vec<Seq> = log.into_iter().filter(|seq| *seq > resume).collect();
        for chunk in new.chunks(ENTRIES_PER_OBJECT) {
            let mut each: Vec<Vec<Partials>> = vec![Vec::new(); self.parts as usize];
            for seq in chunk {
                // An entry collected between the listing and the read
                // ends the run; the next one finds the log trimmed.
                let mut entry = match reader::read(store, writer, *seq).await {
                    Ok(entry) => entry,
                    Err(WalError::Store(ObjectError::NotFound { .. })) => break,
                    Err(error) => return Err(error.to_string()),
                };
                entry.unlock(self.keys).map_err(|e| e.to_string())?;
                for (entries, partials) in each.iter_mut().zip(self.fold(&entry)?) {
                    entries.push(partials);
                }
            }
            let read = each.first().map_or(0, Vec::len);
            let Some(through) = read.checked_sub(1).map(|last| chunk[last]) else {
                break;
            };
            for (part, entries) in (0..self.parts).zip(&each) {
                let body = serde_json::to_vec(entries).map_err(|e| e.to_string())?;
                let key = self.key(part, writer, through);
                match store.put_if_absent(&key, body.into()).await {
                    Ok(()) | Err(ObjectError::AlreadyExists { .. }) => {}
                    Err(error) => return Err(error.to_string()),
                }
            }
            emitted.entries += read;
            if read < chunk.len() {
                break;
            }
        }
        Ok(emitted)
    }

    /// What `entry` holds for each partition, in partition order.
    fn fold(&self, entry: &Entry) -> Result<Vec<Partials>, String> {
        let mut each: Vec<Partials> = (0..self.parts)
            .map(|_| Partials {
                seq: entry.seq(),
                max_observed: None,
                ended: false,
                windows: Vec::new(),
            })
            .collect();
        match entry.body() {
            EntryBody::Segment { blocks } => {
                let newest = blocks.iter().map(|b| b.max_observed).max();
                for partials in &mut each {
                    partials.max_observed = newest;
                }
                // Nothing is late here: the partition that keeps a
                // window judges it against its own horizon (CQ-11).
                let rules: Vec<Folding<'_>> = self
                    .rules
                    .iter()
                    .map(|&(rule, shape)| Folding {
                        rule,
                        shape,
                        horizon: Timestamp::MIN,
                    })
                    .collect();
                let mut folded = Ingested::default();
                incremental::fold_entry(
                    entry,
                    self.tenant,
                    &rules,
                    &EvalState::default(),
                    &mut folded,
                )?;
                for (rule, error) in &folded.failed {
                    tracing::warn!(tenant = %self.tenant, group = %self.group, %rule, seq = %entry.seq(), error = %error, "rule not folded over a log entry; its partials leave the entry out");
                }
                for window in folded.windows.into_values() {
                    let rule = self.rules.iter().find(|(rule, _)| rule.name == window.rule);
                    if let Some((rule, _)) = rule
                        && !folded.failed.contains_key(&rule.name)
                    {
                        let part = window_partition(rule, &window.keys, self.parts);
                        each[part as usize].windows.push(window);
                    }
                }
            }
            EntryBody::End => {
                for partials in &mut each {
                    partials.ended = true;
                }
            }
            EntryBody::Flush { .. } | EntryBody::Seal { .. } => {}
        }
        Ok(each)
    }

    /// Removes the objects of `writer` whose entries are all before
    /// `first`, the oldest the log still holds, or every one when the
    /// log is gone; under every partition, since a run cut short may
    /// have left an object under some only, and the last partition's
    /// last, so that a removal cut short is found again. Returns how
    /// many went.
    async fn trim(&self, writer: WriterId, first: Option<Seq>) -> Result<usize, String> {
        let mut removed = 0;
        for part in 0..self.parts {
            for through in list(&**self.store, &self.prefix(part, writer)).await? {
                if first.is_none_or(|first| through < first) {
                    let key = self.key(part, writer, through);
                    self.store.delete(&key).await.map_err(|e| e.to_string())?;
                    removed += 1;
                }
            }
        }
        Ok(removed)
    }

    fn key(&self, part: u32, writer: WriterId, through: Seq) -> Key {
        layout::eval_partial(self.tenant, self.group, self.parts, part, writer, through)
    }

    fn prefix(&self, part: u32, writer: WriterId) -> Key {
        layout::eval_partials(self.tenant, self.group, self.parts, part, writer)
    }
}
