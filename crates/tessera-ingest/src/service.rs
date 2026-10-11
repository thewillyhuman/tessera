// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The ingest service: one task owns the [`Ingester`] and drives it from
//! a timer; request handlers reach it through a cloneable [`IngestHandle`].
//! Serializing all writes through one task is what makes the writer log's
//! strict ordering (ST-5) trivial to keep.

use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::{Ack, IngestError, Ingester};

enum Command {
    Write(tessera_types::EventBatch, oneshot::Sender<Ack>),
    Shutdown(oneshot::Sender<Result<(), IngestError>>),
}

/// A handle to the ingest task.
#[derive(Clone)]
pub struct IngestHandle {
    commands: mpsc::Sender<Command>,
}

impl IngestHandle {
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
        self.commands
            .send(Command::Write(batch, reply))
            .await
            .map_err(|_| IngestError::Stopped)?;
        ack.await.map_err(|_| IngestError::Stopped)?.wait().await
    }

    /// Flushes everything, ends the writer log and stops the task.
    ///
    /// # Errors
    ///
    /// What the final flush reported; the task stops either way.
    pub async fn shutdown(&self) -> Result<(), IngestError> {
        let (reply, done) = oneshot::channel();
        self.commands
            .send(Command::Shutdown(reply))
            .await
            .map_err(|_| IngestError::Stopped)?;
        done.await.map_err(|_| IngestError::Stopped)?
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
    let (sender, receiver) = mpsc::channel(1024);
    let task = tokio::spawn(run(ingester, receiver, tick));
    (IngestHandle { commands: sender }, task)
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
            Arc::new(ParquetFormat),
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
}
