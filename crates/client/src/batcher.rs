//! Buffer-and-flush wrapper around `Client`.
//!
//! `Batcher::enqueue` is non-blocking from the producer's perspective:
//! the event lands on an in-memory queue and a single background task
//! sends batches to the server, with the same retry semantics as
//! `Client::send_batch`.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::interval;

use crate::client::Client;
use crate::event::Event;

#[derive(Debug, Clone)]
pub struct BatcherConfig {
    pub max_batch: usize,
    pub flush_interval: Duration,
    pub queue_depth: usize,
}

impl Default for BatcherConfig {
    fn default() -> Self {
        Self {
            max_batch: 64,
            flush_interval: Duration::from_millis(500),
            queue_depth: 1024,
        }
    }
}

pub struct Batcher {
    tx: mpsc::Sender<Event>,
    /// Held so the caller can join the background task on shutdown.
    pub task: JoinHandle<()>,
}

impl Batcher {
    /// Start a background flusher task. Drops the `Batcher` to signal
    /// shutdown — the task drains any remaining events before exiting.
    pub fn spawn(client: Arc<Client>, config: BatcherConfig) -> Self {
        let (tx, rx) = mpsc::channel(config.queue_depth);
        let task = tokio::spawn(run(client, rx, config));
        Self { tx, task }
    }

    /// Push one event onto the queue. Returns `Err(event)` when the
    /// queue is full so the caller can decide to drop or block.
    pub fn try_enqueue(&self, event: Event) -> Result<(), Event> {
        self.tx.try_send(event).map_err(|e| match e {
            mpsc::error::TrySendError::Full(ev) | mpsc::error::TrySendError::Closed(ev) => ev,
        })
    }

    /// Push one event onto the queue. Awaits capacity.
    pub async fn enqueue(&self, event: Event) -> Result<(), Event> {
        self.tx.send(event).await.map_err(|e| e.0)
    }
}

async fn run(client: Arc<Client>, mut rx: mpsc::Receiver<Event>, config: BatcherConfig) {
    let mut buf: Vec<Event> = Vec::with_capacity(config.max_batch);
    let mut ticker = interval(config.flush_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Skip the immediate first tick.
    ticker.tick().await;
    loop {
        tokio::select! {
            maybe = rx.recv() => {
                match maybe {
                    Some(event) => {
                        buf.push(event);
                        if buf.len() >= config.max_batch {
                            flush(&client, &mut buf).await;
                        }
                    }
                    None => {
                        if !buf.is_empty() {
                            flush(&client, &mut buf).await;
                        }
                        return;
                    }
                }
            }
            _ = ticker.tick() => {
                if !buf.is_empty() {
                    flush(&client, &mut buf).await;
                }
            }
        }
    }
}

async fn flush(client: &Client, buf: &mut Vec<Event>) {
    let to_send = std::mem::take(buf);
    if let Err(e) = client.send_batch(&to_send).await {
        tracing::warn!(err = %e, dropped = to_send.len(), "percept batcher flush failed; events dropped");
    }
}
