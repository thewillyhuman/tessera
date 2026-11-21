// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The source tasks of a rule group with several state partitions
//! (CQ-4).
//!
//! A group with one partition reads the writer logs itself. With more,
//! each writer's log is read once for the whole group: the partition
//! the writer hashes to is the log's source. It reads every new entry,
//! applies each incremental rule's filter and partial aggregation, and
//! leaves the windows each state partition keeps as one object per
//! entry and partition, at
//! `{tenant}/_eval/{group}/_partials/{parts}/{part}/{writer}/{seq}`.
//! Every partition, the source's own included, then folds the partials
//! left for it instead of the segments (CQ-5).
//!
//! The task holds no state. An entry's objects are written in
//! partition order, so the last partition's object marks a complete
//! set and the newest of them is where the task resumes; writing a set
//! again is harmless, since it follows from the entry and the rules
//! alone. An entry that holds nothing for the group still leaves its
//! objects, empty: a partition tells a trimmed log from a quiet one by
//! the positions being contiguous, as it does on the log itself.

use std::sync::Arc;

use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use tessera_object::{Key, ObjectError, ObjectStore, layout};
use tessera_types::{Keys, Seq, TenantId, Timestamp, WriterId};
use tessera_wal::{Entry, EntryBody, WalError, reader};

use crate::incremental::{self, Ingested, Shape, WindowState};
use crate::partition::window_partition;
use crate::rule::AlertRule;
use crate::state::EvalState;

/// What one entry of a writer's log holds for one state partition.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Partials {
    /// The newest observed time of the entry's segment, for the
    /// writer's watermark; none when the entry is not a segment.
    pub max_observed: Option<Timestamp>,
    /// Whether the entry ends the log.
    pub ended: bool,
    /// The windows of every incremental rule the entry's rows fall in,
    /// folded from nothing, that the partition keeps.
    pub windows: Vec<WindowState>,
}

/// The positions of the partials under `prefix`, ascending.
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

/// The partials at `key`; none when they are gone, trimmed with their
/// entry.
pub(crate) async fn read(store: &dyn ObjectStore, key: &Key) -> Result<Option<Partials>, String> {
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
    pub rules: &'a [(&'a AlertRule, Shape)],
}

/// What a run of a source task did with one writer's log.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Emitted {
    /// The entries whose partials were written.
    pub entries: usize,
    /// The entries whose partials were removed, garbage collection
    /// having removed the entries (GC-2).
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
    /// If the log or the partials cannot be listed, read or written,
    /// or a rule's expression cannot be evaluated.
    pub async fn run(&self, writer: WriterId, held: Seq) -> Result<Emitted, String> {
        let store = &**self.store;
        let log = reader::list(store, writer)
            .await
            .map_err(|e| e.to_string())?;
        let complete = list(store, &self.prefix(self.parts.saturating_sub(1), writer)).await?;
        let mut emitted = Emitted::default();
        let mut resume = held;
        for seq in complete {
            if log.first().is_some_and(|first| seq >= *first) {
                resume = seq;
            } else {
                self.remove(writer, seq).await?;
                emitted.trimmed += 1;
            }
        }
        for seq in log.into_iter().filter(|seq| *seq > resume) {
            // An entry collected between the listing and the read ends
            // the run; the next one finds the log trimmed.
            let mut entry = match reader::read(store, writer, seq).await {
                Ok(entry) => entry,
                Err(WalError::Store(ObjectError::NotFound { .. })) => break,
                Err(error) => return Err(error.to_string()),
            };
            entry.unlock(self.keys).map_err(|e| e.to_string())?;
            for (part, partials) in (0..self.parts).zip(self.fold(&entry)?) {
                let body = serde_json::to_vec(&partials).map_err(|e| e.to_string())?;
                let key =
                    layout::eval_partial(self.tenant, self.group, self.parts, part, writer, seq);
                match store.put_if_absent(&key, body.into()).await {
                    Ok(()) | Err(ObjectError::AlreadyExists { .. }) => {}
                    Err(error) => return Err(error.to_string()),
                }
            }
            emitted.entries += 1;
        }
        Ok(emitted)
    }

    /// What `entry` holds for each partition, in partition order.
    fn fold(&self, entry: &Entry) -> Result<Vec<Partials>, String> {
        let mut each = vec![Partials::default(); self.parts as usize];
        match entry.body() {
            EntryBody::Segment { blocks } => {
                let newest = blocks.iter().map(|b| b.max_observed).max();
                for partials in &mut each {
                    partials.max_observed = newest;
                }
                // Nothing is late here: the partition that keeps a
                // window judges it against its own horizon (CQ-11).
                let fresh = EvalState::default();
                for (rule, shape) in self.rules {
                    let mut folded = Ingested::default();
                    for batch in entry
                        .blocks_for(self.tenant, &shape.dataset, shape.kind)
                        .map_err(|e| e.to_string())?
                    {
                        incremental::fold_batch(
                            rule,
                            shape,
                            &fresh,
                            &mut folded,
                            batch.rows(),
                            Timestamp::MIN,
                        )?;
                    }
                    for window in folded.windows.into_values() {
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

    /// Removes the partials of entry `seq`, the last partition's last
    /// so that a removal cut short is found again.
    async fn remove(&self, writer: WriterId, seq: Seq) -> Result<(), String> {
        for part in 0..self.parts {
            let key = layout::eval_partial(self.tenant, self.group, self.parts, part, writer, seq);
            self.store.delete(&key).await.map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    fn prefix(&self, part: u32, writer: WriterId) -> Key {
        layout::eval_partials(self.tenant, self.group, self.parts, part, writer)
    }
}
