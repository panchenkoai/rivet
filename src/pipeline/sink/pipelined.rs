//! **Layer: Execution**
//!
//! `PipelinedSink` decouples the source fetch/convert stage from the
//! encode/compress/write stage. The source thread only fetches rows, converts
//! them to Arrow, and hands each `RecordBatch` to a bounded channel; a dedicated
//! worker thread owns the real [`ExportSink`] and does the CPU-heavy work
//! (parquet encode + compression, quality hashing, file rotation).
//!
//! This overlaps the database round-trip wait with the compression CPU work,
//! which today run serially on one thread (see `on_batch` in [`super`]).
//!
//! Ordering and invariants are preserved: a single FIFO worker processes
//! batches in arrival order, so `content_fingerprint` determinism (ADR-0012)
//! and the I1–I8 ordering contracts are unaffected.
//!
//! Memory: the synchronous path holds ~1–2 batches in flight (one being
//! converted, one being encoded). Pipelining raises the ceiling to roughly
//! `(DEPTH + 2) × batch_size` — the in-channel queue plus the batch the
//! encoder is working on plus the one the fetcher is building. It stays
//! *bounded*: the `sync_channel` blocks the fetch thread once the queue is
//! full (backpressure), and the per-batch byte budget (MySQL 64 MB cap / PG
//! `work_mem × 0.7`) is unchanged. `DEPTH` is the memory↔overlap knob:
//! lower it to spend less memory, raise it to absorb burstier fetch latency.
//!
//! Experimental: enabled via the `RIVET_PIPELINE_WRITES` env var. Set it to a
//! positive integer to also pick the channel depth (e.g. `RIVET_PIPELINE_WRITES=2`);
//! `1` / any non-numeric truthy value uses [`DEFAULT_CHANNEL_DEPTH`]. When
//! unset or `0`, the synchronous path is used unchanged.

use std::sync::mpsc::{SyncSender, sync_channel};
use std::thread::JoinHandle;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use super::ExportSink;
use crate::error::Result;
use crate::plan::ResolvedRunPlan;
use crate::source::BatchSink;

/// Channel depth: how many batches may sit between fetch and encode. Small, so
/// peak extra memory stays at ~`DEPTH × batch_size` while still giving the
/// encoder a runway to stay busy across a fetch round-trip.
const DEFAULT_CHANNEL_DEPTH: usize = 3;

enum SinkMsg {
    Schema(SchemaRef),
    Batch(RecordBatch),
}

/// A `BatchSink` that forwards schema/batches to a background encode worker.
pub(crate) struct PipelinedSink {
    tx: Option<SyncSender<SinkMsg>>,
    worker: Option<JoinHandle<Result<ExportSink>>>,
}

impl PipelinedSink {
    /// Returns `true` when the pipelined write path is requested.
    pub fn enabled() -> bool {
        Self::configured_depth().is_some()
    }

    /// Parse the requested channel depth from `RIVET_PIPELINE_WRITES`.
    /// `None` => disabled. A positive integer sets the depth; any other
    /// truthy value uses [`DEFAULT_CHANNEL_DEPTH`].
    fn configured_depth() -> Option<usize> {
        match std::env::var("RIVET_PIPELINE_WRITES") {
            Ok(v) if v.is_empty() || v == "0" => None,
            Ok(v) => Some(v.parse::<usize>().ok().filter(|&n| n > 0).unwrap_or(DEFAULT_CHANNEL_DEPTH)),
            Err(_) => None,
        }
    }

    /// Spawn the encode worker. The worker creates and owns the `ExportSink`
    /// (so the writer trait object never crosses a thread boundary while in
    /// use); `finish` returns the fully-populated sink back to the caller.
    pub fn spawn(plan: &ResolvedRunPlan) -> Result<Self> {
        let plan = plan.clone();
        let depth = Self::configured_depth().unwrap_or(DEFAULT_CHANNEL_DEPTH);
        let (tx, rx) = sync_channel::<SinkMsg>(depth);
        let worker = std::thread::Builder::new()
            .name("rivet-encode".to_string())
            .spawn(move || -> Result<ExportSink> {
                let mut sink = ExportSink::new(&plan)?;
                while let Ok(msg) = rx.recv() {
                    match msg {
                        SinkMsg::Schema(s) => sink.on_schema(s)?,
                        SinkMsg::Batch(b) => sink.on_batch(&b)?,
                    }
                }
                Ok(sink)
            })?;
        Ok(Self {
            tx: Some(tx),
            worker: Some(worker),
        })
    }

    /// Test-only: wrap an already-built `ExportSink` (bypasses plan
    /// construction). Same worker loop as `spawn`.
    #[cfg(test)]
    pub(crate) fn spawn_with_sink(sink: ExportSink) -> Self {
        let (tx, rx) = sync_channel::<SinkMsg>(DEFAULT_CHANNEL_DEPTH);
        let worker = std::thread::Builder::new()
            .name("rivet-encode-test".to_string())
            .spawn(move || -> Result<ExportSink> {
                let mut sink = sink;
                while let Ok(msg) = rx.recv() {
                    match msg {
                        SinkMsg::Schema(s) => sink.on_schema(s)?,
                        SinkMsg::Batch(b) => sink.on_batch(&b)?,
                    }
                }
                Ok(sink)
            })
            .expect("spawn test encode worker");
        Self {
            tx: Some(tx),
            worker: Some(worker),
        }
    }

    fn send(&mut self, msg: SinkMsg) -> Result<()> {
        match self.tx.as_ref() {
            // A send error means the worker hit an error and dropped the
            // receiver. We return a generic abort here; `finish` joins the
            // worker and surfaces the *real* error, which the caller checks
            // before the source-side error (see `run_single_export`).
            Some(tx) => tx
                .send(msg)
                .map_err(|_| anyhow::anyhow!("pipelined encode worker stopped")),
            None => Err(anyhow::anyhow!("pipelined sink already finished")),
        }
    }

    /// Close the channel, join the worker, and recover the populated
    /// `ExportSink`. Propagates a worker error or panic.
    pub fn finish(mut self) -> Result<ExportSink> {
        // Drop the sender so the worker's `recv` loop terminates.
        self.tx.take();
        match self.worker.take() {
            Some(handle) => match handle.join() {
                Ok(result) => result,
                Err(_) => Err(anyhow::anyhow!("pipelined encode worker panicked")),
            },
            None => Err(anyhow::anyhow!("pipelined sink has no worker")),
        }
    }
}

impl BatchSink for PipelinedSink {
    fn on_schema(&mut self, schema: SchemaRef) -> Result<()> {
        self.send(SinkMsg::Schema(schema))
    }

    fn on_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        // RecordBatch clone is a shallow Arc bump per column — cheap. The
        // batch crosses to the worker by value so the source loop can free
        // its own reference immediately on the next fetch.
        self.send(SinkMsg::Batch(batch.clone()))
    }
}
