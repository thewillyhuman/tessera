// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The ingest service: a task owns each [`Ingester`] and drives it from
//! a timer; request handlers reach them through a cloneable
//! [`IngestHandle`], which hands writes to the tasks in turn. Serializing
//! the writes of one log through one task is what makes the log's strict
//! ordering (ST-5) trivial to keep, and several logs are what lets a
//! node take load on several cores.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::{Ack, IngestError, Ingester};

enum Command {
    Write(tessera_types::EventBatch, oneshot::Sender<Ack>),
    Shutdown(oneshot::Sender<Result<(), IngestError>>),
}

/// A handle to the ingest tasks.
#[derive(Clone)]
pub struct IngestHandle {
    commands: Vec<mpsc::Sender<Command>>,
    /// The task the next write goes to.
    next: Arc<AtomicUsize>,
}

impl IngestHandle {
    /// The task the next write goes to, in turn.
    fn task(&self) -> &mpsc::Sender<Command> {
        let turn = self.next.fetch_add(1, Ordering::Relaxed);
        &self.commands[turn % self.commands.len()]
    }

    /// Writes a batch and waits until it is durable (IN-10).
    ///
    /// # Errors
    ///
    /// `Stopped` if the service is gone; otherwise what the segment write
    /// reported.
    pub async fn write(
        &self,
        batch: tessera_types::EventBatch,
    ) -> Result<tessera_types::Seq, IngestError> {
        let (reply, ack) = oneshot::channel();
        self.task()
            .send(Command::Write(batch, reply))
            .await
            .map_err(|_| IngestError::Stopped)?;
        ack.await.map_err(|_| IngestError::Stopped)?.wait().await
    }

    /// Hands `batch` to the ingester and returns once it holds it, before
    /// the segment is stored (IN-10): for callers that prefer latency to
    /// durability. A segment write that then fails is logged, not
    /// reported.
    ///
    /// # Errors
    ///
    /// `Stopped` if the service is shutting down.
    pub async fn write_buffered(
        &self,
        batch: tessera_types::EventBatch,
    ) -> Result<(), IngestError> {
        let (reply, ack) = oneshot::channel();
        self.task()
            .send(Command::Write(batch, reply))
            .await
            .map_err(|_| IngestError::Stopped)?;
        let ack = ack.await.map_err(|_| IngestError::Stopped)?;
        tokio::spawn(async move {
            if let Err(error) = ack.wait().await {
                tracing::warn!(error = %error, "a write acknowledged as buffered was lost");
            }
        });
        Ok(())
    }

    /// Flushes everything, ends every writer log and stops the tasks.
    ///
    /// # Errors
    ///
    /// The first failure a final flush reported; the tasks stop either
    /// way.
    pub async fn shutdown(&self) -> Result<(), IngestError> {
        let mut outcome = Ok(());
        for task in &self.commands {
            let (reply, done) = oneshot::channel();
            let stopped = match task.send(Command::Shutdown(reply)).await {
                Ok(()) => done.await.map_err(|_| IngestError::Stopped)?,
                Err(_) => Err(IngestError::Stopped),
            };
            if outcome.is_ok() {
                outcome = stopped;
            }
        }
        outcome
    }
}

impl std::fmt::Debug for IngestHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("IngestHandle")
    }
}

/// Spawns the task that owns `ingester`, ticking it every `tick`: the
/// handle to reach it and the task itself, which aborting drops the
/// ingester as a crash would, with its log open for recovery.
#[must_use]
pub fn spawn(ingester: Ingester, tick: Duration) -> (IngestHandle, JoinHandle<()>) {
    let (handle, mut tasks) = spawn_all(vec![ingester], tick);
    let task = tasks
        .pop()
        .unwrap_or_else(|| unreachable!("one ingester, one task"));
    (handle, task)
}

/// Spawns one task per ingester, each ticking every `tick`: one handle
/// that hands writes to them in turn, and the tasks.
///
/// # Panics
///
/// With no ingester; a node has at least one.
#[must_use]
pub fn spawn_all(ingesters: Vec<Ingester>, tick: Duration) -> (IngestHandle, Vec<JoinHandle<()>>) {
    assert!(!ingesters.is_empty(), "ingest needs a writer");
    let mut commands = Vec::with_capacity(ingesters.len());
    let mut tasks = Vec::with_capacity(ingesters.len());
    for ingester in ingesters {
        let (sender, receiver) = mpsc::channel(1024);
        commands.push(sender);
        tasks.push(tokio::spawn(run(ingester, receiver, tick)));
    }
    let handle = IngestHandle {
        commands,
        next: Arc::new(AtomicUsize::new(0)),
    };
    (handle, tasks)
}

async fn run(mut ingester: Ingester, mut commands: mpsc::Receiver<Command>, tick: Duration) {
    let mut ticks = tokio::time::interval(tick);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            command = commands.recv() => match command {
                Some(Command::Write(batch, reply)) => {
                    let ack = ingester.write(batch).await;
                    let _ = reply.send(ack);
                }
                Some(Command::Shutdown(reply)) => {
                    let _ = reply.send(ingester.shutdown().await);
                    return;
                }
                None => {
                    let _ = ingester.shutdown().await;
                    return;
                }
            },
            _ = ticks.tick() => ingester.tick().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tessera_format::ParquetFormat;
    use tessera_object::MemoryStore;
    use tessera_types::{DatasetName, Env, EventBatch, LogRow, PoolId, Seq, TenantId, Timestamp};
    use tessera_wal::{EntryBody, reader};

    use super::*;
    use crate::IngestConfig;

    fn batch() -> EventBatch {
        let row = LogRow {
            timestamp: Timestamp::from_nanos(1_000_000_000_000),
            body: Some("x".into()),
            ..LogRow::default()
        };
        EventBatch::from_logs(TenantId::DEFAULT, DatasetName::new("prod").unwrap(), &[row]).unwrap()
    }

    #[tokio::test]
    async fn writes_are_acknowledged_by_the_timer_driven_task() {
        let store = MemoryStore::new();
        let env = Env::system();
        let config = IngestConfig {
            linger: Duration::from_millis(1),
            ..IngestConfig::default()
        };
        let ingester = Ingester::start(
            Arc::new(store.clone()),
            env,
            config,
            PoolId::new("root").unwrap(),
            Arc::new(ParquetFormat::default()),
        );
        let writer = ingester.writer_id();
        let (handle, _task) = spawn(ingester, Duration::from_millis(1));
        assert_eq!(handle.clone().write(batch()).await.unwrap(), Seq::new(1));
        handle.shutdown().await.unwrap();
        assert!(matches!(
            handle.write(batch()).await,
            Err(IngestError::Stopped)
        ));
        let last = reader::last(&store, writer).await.unwrap().unwrap();
        assert_eq!(
            reader::read(&store, writer, last).await.unwrap().body(),
            &EntryBody::End
        );
    }

    /// Several writers take writes in turn, each on its own log, and
    /// all end on shutdown.
    #[tokio::test]
    async fn writes_go_to_the_writers_in_turn() {
        let store = MemoryStore::new();
        let config = IngestConfig {
            linger: Duration::from_millis(1),
            ..IngestConfig::default()
        };
        let ingesters: Vec<Ingester> = (0..2)
            .map(|_| {
                Ingester::start(
                    Arc::new(store.clone()),
                    Env::system(),
                    config,
                    PoolId::new("root").unwrap(),
                    Arc::new(ParquetFormat::default()),
                )
            })
            .collect();
        let writers: Vec<_> = ingesters.iter().map(Ingester::writer_id).collect();
        let (handle, tasks) = spawn_all(ingesters, Duration::from_millis(1));
        assert_eq!(tasks.len(), 2);
        for _ in 0..4 {
            handle.write(batch()).await.unwrap();
        }
        handle.shutdown().await.unwrap();
        for writer in writers {
            let entries = reader::list(&store, writer).await.unwrap();
            assert!(entries.len() >= 3, "two segments and an end: {entries:?}");
            let last = reader::last(&store, writer).await.unwrap().unwrap();
            assert_eq!(
                reader::read(&store, writer, last).await.unwrap().body(),
                &EntryBody::End
            );
        }
    }
}
