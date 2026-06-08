//! Streaming engine v2.
//!
//! One HTTP request is the unit of work. A single scheduler task owns all state
//! (no locks) and always hands the next free worker the earliest still-needed
//! range, which naturally prioritises truncation gaps (backfill) over extending
//! the frontier. Each request's block span is projected locally from the
//! byte-density of the nearest already-completed neighbour, aiming at
//! `response_bytes_target`. Delivery is gated on contiguity with a watermark, so
//! the stream still yields exactly one `ArrowResponse` per HTTP response in block
//! order — a drop-in for consumers.
//!
//! See `docs/STREAMING.md` for the full design.

use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, bail, Context, Result};
use arrow::datatypes::DataType as ArrowDataType;
use arrow::{
    array::{
        Array, ArrayRef, AsArray, BinaryArray, BooleanArray, RecordBatch, StringArray, UInt64Array,
        UInt8Array,
    },
    datatypes::UInt64Type,
};
use hypersync_net_types::Query;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::{
    config::HexOutput,
    metrics::{RequestKind, RequestStats, StreamMetrics, StreamObserver},
    rayon_async,
    types::ArrowResponse,
    util::{decode_logs_batch, hex_encode_batch},
    ArrowResponseData, StreamConfig,
};

/// We always wait out server rate limits inside the streaming engine.
const WAIT_ON_RATE_LIMIT: bool = true;

/// Emit the (single, per-stream) diagnostic warning only after this many
/// *consecutive* genuinely-bad responses — ones that are truncated, tiny in bytes
/// (`< response_bytes_target / 2`), AND cover fewer than `min_batch_size` blocks.
///
/// Any response that isn't bad on all three counts resets the counter, and the
/// warning fires at most once per stream (see [`Scheduler::update_warning`]).
/// Together with the strict condition this keeps it silent for healthy streams —
/// including broad/compact queries the server row/time-caps below target while
/// still covering a normal block range — and only speaks up when the server is
/// persistently stalling below our smallest projected range, the one case where
/// batch-size tuning genuinely can't help.
const WARN_THRESHOLD: u32 = 100;

/// `concurrency == 0` is an error; `1` is sequential; `>= 2` uses the scheduler.
fn check_concurrency(concurrency: usize) -> Result<()> {
    if concurrency == 0 {
        bail!("concurrency must be greater than 0");
    }
    Ok(())
}

/// Spawn a stream with no observer (zero metrics overhead).
pub async fn stream_arrow(
    client: &crate::Client,
    query: Query,
    config: StreamConfig,
) -> Result<mpsc::Receiver<Result<ArrowResponse>>> {
    spawn_stream(client, query, config, None).await
}

/// Spawn a stream that reports metrics to `observer`.
pub async fn stream_arrow_with_observer(
    client: &crate::Client,
    query: Query,
    config: StreamConfig,
    observer: Arc<dyn StreamObserver>,
) -> Result<mpsc::Receiver<Result<ArrowResponse>>> {
    spawn_stream(client, query, config, Some(observer)).await
}

async fn spawn_stream(
    client: &crate::Client,
    query: Query,
    config: StreamConfig,
    observer: Option<Arc<dyn StreamObserver>>,
) -> Result<mpsc::Receiver<Result<ArrowResponse>>> {
    check_concurrency(config.concurrency)?;

    let (tx, rx) = mpsc::channel(config.concurrency * 2);
    let client = client.clone();
    tokio::spawn(run_stream(client, query, config, tx, observer));
    Ok(rx)
}

/// Outcome of a single fetched (and mapped) request.
struct FetchOutcome {
    /// Exclusive end actually covered by the response.
    next_block: u64,
    /// Archive height reported by the response, if any.
    archive_height: Option<u64>,
    /// HTTP response body size in bytes.
    size_bytes: u64,
    /// The mapped (hex/decode/column-map/reverse) response.
    resp: ArrowResponse,
}

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// The per-request fetch step, made injectable so the scheduler can be unit
/// tested deterministically with a mock that returns configurable
/// `(next_block, size_bytes)` for any `[from, to)`.
trait Fetcher: Send + Sync + 'static {
    fn fetch(&self, from_block: u64, to_block: u64) -> BoxFuture<Result<FetchOutcome>>;
}

/// Real fetcher: one `get_arrow_with_size` followed by `map_responses`, both on
/// the worker so decode work parallelises across requests.
struct ClientFetcher {
    client: crate::Client,
    query: Query,
    config: StreamConfig,
    reverse: bool,
}

impl Fetcher for ClientFetcher {
    fn fetch(&self, from_block: u64, to_block: u64) -> BoxFuture<Result<FetchOutcome>> {
        let client = self.client.clone();
        let config = self.config.clone();
        let reverse = self.reverse;
        let mut query = self.query.clone();
        query.from_block = from_block;
        query.to_block = Some(to_block);
        Box::pin(async move {
            let result = client
                .get_arrow_with_size(&query, WAIT_ON_RATE_LIMIT)
                .await
                .context("get data")?;
            let next_block = result.response.next_block;
            let archive_height = result.response.archive_height;
            let size_bytes = result.response_bytes;
            let resp = map_responses(config, vec![result.response], reverse)
                .await?
                .remove(0);
            Ok(FetchOutcome {
                next_block,
                archive_height,
                size_bytes,
                resp,
            })
        })
    }
}

/// A fetched chunk awaiting in-order delivery.
struct CompletedChunk {
    /// Exclusive end actually covered.
    next_block: u64,
    /// HTTP response body size, for density.
    size_bytes: u64,
    /// Already-mapped response.
    resp: ArrowResponse,
}

/// Result handed back from a worker task.
struct FetchResult {
    /// First block of the request (inclusive).
    start: u64,
    /// Requested exclusive end of the request.
    req_end: u64,
    /// Block span the projector aimed for, before clamping to the hole.
    projected_blocks: u64,
    /// Whether this request extended the frontier or backfilled a gap.
    kind: RequestKind,
    /// Wall-clock latency of the fetch (request + map).
    duration: Duration,
    /// The fetched outcome, or the error that the fetch failed with.
    outcome: Result<FetchOutcome>,
}

/// Whether the scheduler should keep going or stop (error / entity limit /
/// consumer dropped).
enum Flow {
    Continue,
    Stop,
}

/// All scheduler state, owned by a single task — no locks.
struct Scheduler {
    /// Stream in reverse (high → low) block order.
    reverse: bool,
    /// Query had no `to_block`; follow the live archive height to the head.
    open_ended: bool,
    /// Stream configuration (sizing knobs, entity limits, mapping).
    config: StreamConfig,
    /// Injectable per-request fetch (the real client, or a mock in tests).
    fetcher: Arc<dyn Fetcher>,

    /// Exclusive top: `to_block`, or the (live) archive height when open-ended.
    upper_bound: u64,
    /// Inclusive bottom (`from_block`); used by reverse termination/sizing.
    lower_bound: u64,
    /// Forward: `delivered_up_to`. Reverse: `delivered_down_to`.
    watermark: u64,

    /// Un-fetched, un-assigned ranges, keyed by `start` → exclusive `end`.
    holes: BTreeMap<u64, u64>,
    /// Fetched chunks awaiting in-order delivery, keyed by `start`.
    completed: BTreeMap<u64, CompletedChunk>,
    /// Currently-running fetch tasks (at most `concurrency`).
    in_flight: JoinSet<FetchResult>,

    /// Σ `size_bytes` of undelivered `completed` chunks (the reorder buffer).
    buffered_bytes: u64,
    /// Backpressure cap on `buffered_bytes` for look-ahead fetches.
    max_buffered_bytes: u64,
    /// Whether the cap above is auto-managed (user left `max_buffered_bytes`
    /// unset). When true it grows to hold ~2 responses per worker even if the
    /// server returns responses far larger than `response_bytes_target`; an
    /// explicit user cap is honoured verbatim and never grown.
    max_buffered_adaptive: bool,
    /// Largest response body seen so far, driving the adaptive cap.
    max_observed_response: u64,
    /// Bytes/block of the most recent completed response, used for projection.
    last_density: Option<f64>,

    /// Running entity counts (delivered so far) for the `max_num_*` limits.
    num_blocks: usize,
    num_transactions: usize,
    num_logs: usize,
    num_traces: usize,

    /// Consecutive truncated-and-small responses; resets on a healthy one.
    warn_counter: u32,
    /// Whether the warning has fired for the current run (suppresses repeats).
    warned: bool,
    /// Total warnings emitted this stream (for test observability).
    warnings_emitted: usize,

    /// Optional user metrics observer; `None` ⇒ zero metrics overhead.
    observer: Option<Arc<dyn StreamObserver>>,
    /// Internal aggregator backing `on_finish`'s summary (only when observed).
    agg: Option<Arc<StreamMetrics>>,
    /// When the stream started, for the summary's wall-clock / throughput.
    start: Instant,
}

impl Scheduler {
    fn new(
        reverse: bool,
        open_ended: bool,
        config: StreamConfig,
        fetcher: Arc<dyn Fetcher>,
        observer: Option<Arc<dyn StreamObserver>>,
    ) -> Self {
        let max_buffered_adaptive = config.max_buffered_bytes.is_none();
        let max_buffered_bytes = config
            .max_buffered_bytes
            .unwrap_or_else(|| 2 * config.concurrency as u64 * config.response_bytes_target.max(1))
            .max(1);
        let agg = observer.as_ref().map(|_| Arc::new(StreamMetrics::new()));
        Self {
            reverse,
            open_ended,
            config,
            fetcher,
            upper_bound: 0,
            lower_bound: 0,
            watermark: 0,
            holes: BTreeMap::new(),
            completed: BTreeMap::new(),
            in_flight: JoinSet::new(),
            buffered_bytes: 0,
            max_buffered_bytes,
            max_buffered_adaptive,
            max_observed_response: 0,
            last_density: None,
            num_blocks: 0,
            num_transactions: 0,
            num_logs: 0,
            num_traces: 0,
            warn_counter: 0,
            warned: false,
            warnings_emitted: 0,
            observer,
            agg,
            start: Instant::now(),
        }
    }

    /// Density (bytes/block) used to size the next request from `[hole_start,
    /// hole_end)`: the nearest completed neighbour, else the last reading.
    fn anchor_density(&self, hole_start: u64, hole_end: u64) -> Option<f64> {
        let anchor = if self.reverse {
            self.completed.range(hole_end..).next()
        } else {
            self.completed.range(..hole_start).next_back()
        };
        if let Some((&start, chunk)) = anchor {
            let blocks = chunk.next_block.saturating_sub(start);
            return Some(if blocks > 0 {
                chunk.size_bytes as f64 / blocks as f64
            } else {
                0.0
            });
        }
        self.last_density
    }

    /// Projected block span (pre hole-clamp), aimed at `response_bytes_target`.
    fn project_blocks(&self, hole_start: u64, hole_end: u64) -> u64 {
        let target = self.config.response_bytes_target as f64;
        let mut projected = match self.anchor_density(hole_start, hole_end) {
            Some(d) if d > 0.0 => (target / d).round() as u64,
            // Zero density (e.g. an empty range) — scan as far as the hole allows.
            Some(_) => u64::MAX,
            // Nothing measured yet — first wave uses the configured batch size.
            None => self.config.batch_size,
        };
        projected = projected.max(self.config.min_batch_size).max(1);
        if let Some(m) = self.config.max_batch_size {
            projected = projected.min(m.max(1));
        }
        projected
    }

    /// Lowest-start (forward) / highest-start (reverse) hole that may be
    /// scheduled now, honouring consumer backpressure (the watermark hole is
    /// always exempt to avoid deadlock).
    fn pick_hole(&self) -> Option<(u64, u64)> {
        let (&s, &e) = if self.reverse {
            self.holes.iter().next_back()?
        } else {
            self.holes.iter().next()?
        };
        let exempt = if self.reverse {
            e == self.watermark
        } else {
            s == self.watermark
        };
        if exempt || self.buffered_bytes < self.max_buffered_bytes {
            Some((s, e))
        } else {
            None
        }
    }

    /// Move a slice of `[hole_start, hole_end)` into flight and spawn its fetch.
    fn schedule(&mut self, hole_start: u64, hole_end: u64) {
        let projected = self.project_blocks(hole_start, hole_end);
        let (req_start, req_end) = if self.reverse {
            let rs = hole_end.saturating_sub(projected).max(hole_start);
            (rs, hole_end)
        } else {
            let re = hole_start.saturating_add(projected).min(hole_end);
            (hole_start, re)
        };

        let kind = if self.reverse {
            if hole_start == self.lower_bound {
                RequestKind::Frontier
            } else {
                RequestKind::GapFill
            }
        } else if hole_end == self.upper_bound {
            RequestKind::Frontier
        } else {
            RequestKind::GapFill
        };

        // Remove the whole hole, reinsert the un-scheduled remainder.
        self.holes.remove(&hole_start);
        if self.reverse {
            if req_start > hole_start {
                self.holes.insert(hole_start, req_start);
            }
        } else if req_end < hole_end {
            self.holes.insert(req_end, hole_end);
        }

        let fetcher = self.fetcher.clone();
        self.in_flight.spawn(async move {
            let started = Instant::now();
            let outcome = fetcher.fetch(req_start, req_end).await;
            FetchResult {
                start: req_start,
                req_end,
                projected_blocks: projected,
                kind,
                duration: started.elapsed(),
                outcome,
            }
        });
    }

    /// Extend the top hole (open-ended forward) when the archive height advances.
    fn extend_upper(&mut self, old: u64, new: u64) {
        if let Some((&s, &e)) = self.holes.iter().next_back() {
            if e == old {
                self.holes.insert(s, new);
                return;
            }
        }
        self.holes.insert(old, new);
    }

    /// Grow the adaptive reorder-buffer cap so it can hold ~2 responses per
    /// worker even when the server returns responses far larger than
    /// `response_bytes_target` (byte-heavy queries it size-caps above target).
    /// No-op when the user set an explicit `max_buffered_bytes`.
    fn note_response_size(&mut self, size_bytes: u64) {
        if !self.max_buffered_adaptive || size_bytes <= self.max_observed_response {
            return;
        }
        self.max_observed_response = size_bytes;
        let basis = self.config.response_bytes_target.max(size_bytes).max(1);
        let grown = 2 * self.config.concurrency as u64 * basis;
        if grown > self.max_buffered_bytes {
            self.max_buffered_bytes = grown;
        }
    }

    /// Flag a genuinely pathological pattern, fired **at most once per stream**:
    /// the server keeps truncating responses *and* the responses are both tiny in
    /// bytes (`< response_bytes_target / 2`) and cover fewer blocks than
    /// `min_batch_size` — i.e. it can't even deliver our smallest projected range,
    /// which points at an execution-time/scan limit that batch-size tuning can't
    /// fix. Healthy "small but fine" responses (which cover a normal block range,
    /// just under target bytes) reset the counter and never warn.
    fn update_warning(&mut self, truncated: bool, size_bytes: u64, actual_blocks: u64) {
        if self.warned {
            return; // one warning per stream is enough — never spam.
        }
        let small = size_bytes < self.config.response_bytes_target / 2;
        let stalled = actual_blocks < self.config.min_batch_size;
        if truncated && small && stalled {
            self.warn_counter += 1;
            if self.warn_counter >= WARN_THRESHOLD {
                log::warn!(
                    "hypersync stream: {} consecutive responses were truncated to fewer than \
                     min_batch_size ({}) blocks while staying under half of `response_bytes_target` \
                     ({} bytes). The server is hitting an execution-time/scan limit, not a \
                     response-size limit, so batch-size tuning won't help — narrow the query with \
                     more selective filters (address/topic) or a smaller block range.",
                    self.warn_counter,
                    self.config.min_batch_size,
                    self.config.response_bytes_target,
                );
                self.warned = true;
                self.warnings_emitted += 1;
            }
        } else {
            self.warn_counter = 0;
        }
    }

    fn accumulate_and_check(&mut self, resp: &ArrowResponse) -> bool {
        self.num_blocks += count_rows(&resp.data.blocks);
        self.num_transactions += count_rows(&resp.data.transactions);
        self.num_logs += count_rows(&resp.data.logs);
        self.num_traces += count_rows(&resp.data.traces);
        check_entity_limit(self.num_blocks, self.config.max_num_blocks)
            || check_entity_limit(self.num_transactions, self.config.max_num_transactions)
            || check_entity_limit(self.num_logs, self.config.max_num_logs)
            || check_entity_limit(self.num_traces, self.config.max_num_traces)
    }

    #[allow(clippy::too_many_arguments)]
    fn report_request(
        &self,
        from_block: u64,
        requested_end: u64,
        next_block: u64,
        projected_blocks: u64,
        response_bytes: u64,
        truncated: bool,
        kind: RequestKind,
        duration: Duration,
    ) {
        let Some(agg) = self.agg.as_ref() else {
            return;
        };
        let target = self.config.response_bytes_target;
        let actual_blocks = next_block.saturating_sub(from_block);
        let stats = RequestStats {
            from_block,
            requested_end,
            next_block,
            requested_blocks: requested_end.saturating_sub(from_block),
            actual_blocks,
            projected_blocks,
            response_bytes,
            target_bytes: target,
            size_ratio: if target > 0 {
                response_bytes as f64 / target as f64
            } else {
                0.0
            },
            bytes_per_block: if actual_blocks > 0 {
                response_bytes as f64 / actual_blocks as f64
            } else {
                0.0
            },
            truncated,
            kind,
            duration,
        };
        agg.on_request(&stats);
        if let Some(obs) = self.observer.as_ref() {
            obs.on_request(&stats);
        }
    }

    fn report_progress(&self) {
        if let Some(agg) = self.agg.as_ref() {
            let in_flight = self.in_flight.len() as u64;
            agg.on_progress(in_flight, self.buffered_bytes);
            if let Some(obs) = self.observer.as_ref() {
                obs.on_progress(in_flight, self.buffered_bytes);
            }
        }
    }

    fn finish(&self) {
        if let Some(agg) = self.agg.as_ref() {
            agg.record_elapsed(self.start.elapsed());
            let summary = agg.summary();
            if let Some(obs) = self.observer.as_ref() {
                obs.on_finish(&summary);
            }
        }
    }

    /// Deliver every chunk that now abuts the watermark, in block order.
    async fn deliver_ready(&mut self, tx: &mpsc::Sender<Result<ArrowResponse>>) -> Flow {
        loop {
            let candidate = if self.reverse {
                self.completed.iter().next_back()
            } else {
                self.completed.iter().next()
            }
            .map(|(&s, c)| (s, c.next_block));

            let Some((start, next_block)) = candidate else {
                break;
            };
            let ready = if self.reverse {
                next_block == self.watermark
            } else {
                start == self.watermark
            };
            if !ready {
                break;
            }

            let chunk = self.completed.remove(&start).unwrap();
            self.buffered_bytes -= chunk.size_bytes;
            let stop = self.accumulate_and_check(&chunk.resp);
            self.watermark = if self.reverse {
                start
            } else {
                chunk.next_block
            };
            if tx.send(Ok(chunk.resp)).await.is_err() {
                return Flow::Stop;
            }
            if stop {
                return Flow::Stop;
            }
        }
        Flow::Continue
    }

    async fn on_fetch_complete(
        &mut self,
        fr: FetchResult,
        tx: &mpsc::Sender<Result<ArrowResponse>>,
    ) -> Flow {
        let outcome = match fr.outcome {
            Ok(o) => o,
            Err(e) => {
                tx.send(Err(e)).await.ok();
                return Flow::Stop;
            }
        };

        // A valid response makes progress and stays within the requested range.
        // Anything else (no progress, or over-claiming past req_end) would
        // silently drop or duplicate blocks, so fail loudly rather than fabricate
        // coverage.
        let covered = outcome.next_block;
        if covered <= fr.start || covered > fr.req_end {
            tx.send(Err(anyhow!(
                "server returned next_block {covered} outside the requested range [{}..{})",
                fr.start,
                fr.req_end,
            )))
            .await
            .ok();
            return Flow::Stop;
        }
        let truncated = covered < fr.req_end;

        self.report_request(
            fr.start,
            fr.req_end,
            covered,
            fr.projected_blocks,
            outcome.size_bytes,
            truncated,
            fr.kind,
            fr.duration,
        );

        self.buffered_bytes += outcome.size_bytes;
        self.note_response_size(outcome.size_bytes);
        self.completed.insert(
            fr.start,
            CompletedChunk {
                next_block: covered,
                size_bytes: outcome.size_bytes,
                resp: outcome.resp,
            },
        );

        let blocks = covered - fr.start;
        if blocks > 0 {
            self.last_density = Some(outcome.size_bytes as f64 / blocks as f64);
        }

        if truncated {
            // The residual gap `[covered, req_end)` abuts the hole already
            // starting at `req_end` (if any) — coalesce them.
            let gap_start = covered;
            let mut gap_end = fr.req_end;
            if let Some(e) = self.holes.remove(&gap_end) {
                gap_end = e;
            }
            self.holes.insert(gap_start, gap_end);
        }

        self.update_warning(truncated, outcome.size_bytes, blocks);

        if self.open_ended && !self.reverse {
            if let Some(h) = outcome.archive_height {
                if h > self.upper_bound {
                    let old = self.upper_bound;
                    self.upper_bound = h;
                    self.extend_upper(old, h);
                }
            }
        }

        self.deliver_ready(tx).await
    }

    /// Concurrency >= 2 scheduler loop (also used by reverse at any concurrency).
    async fn run_core(&mut self, tx: &mpsc::Sender<Result<ArrowResponse>>) {
        loop {
            while self.in_flight.len() < self.config.concurrency {
                match self.pick_hole() {
                    Some((s, e)) => self.schedule(s, e),
                    None => break,
                }
            }

            self.report_progress();

            if self.in_flight.is_empty() && self.holes.is_empty() {
                break;
            }

            let joined = match self.in_flight.join_next().await {
                Some(j) => j,
                None => continue,
            };
            let fr = match joined {
                Ok(fr) => fr,
                Err(e) => {
                    tx.send(Err(anyhow!("stream fetch task failed: {e}")))
                        .await
                        .ok();
                    break;
                }
            };
            if let Flow::Stop = self.on_fetch_complete(fr, tx).await {
                break;
            }
        }
    }

    /// Forward `concurrency == 1`: sequential, always querying to the upper limit.
    async fn run_sequential(&mut self, tx: &mpsc::Sender<Result<ArrowResponse>>) {
        while self.watermark < self.upper_bound {
            let req_start = self.watermark;
            let req_end = self.upper_bound;
            let started = Instant::now();
            let outcome = match self.fetcher.fetch(req_start, req_end).await {
                Ok(o) => o,
                Err(e) => {
                    tx.send(Err(e)).await.ok();
                    return;
                }
            };
            let covered = outcome.next_block;
            if covered <= req_start || covered > req_end {
                tx.send(Err(anyhow!(
                    "server returned next_block {covered} outside the requested range [{req_start}..{req_end})"
                )))
                .await
                .ok();
                return;
            }
            let truncated = covered < req_end;
            self.report_request(
                req_start,
                req_end,
                covered,
                req_end - req_start,
                outcome.size_bytes,
                truncated,
                RequestKind::Frontier,
                started.elapsed(),
            );
            let stop = self.accumulate_and_check(&outcome.resp);
            self.watermark = covered;
            if self.open_ended {
                if let Some(h) = outcome.archive_height {
                    if h > self.upper_bound {
                        self.upper_bound = h;
                    }
                }
            }
            self.report_progress();
            if tx.send(Ok(outcome.resp)).await.is_err() {
                return;
            }
            if stop {
                return;
            }
        }
    }
}

async fn run_stream(
    client: crate::Client,
    query: Query,
    config: StreamConfig,
    tx: mpsc::Sender<Result<ArrowResponse>>,
    observer: Option<Arc<dyn StreamObserver>>,
) {
    let reverse = config.reverse;
    let open_ended = query.to_block.is_none();
    let from_block = query.from_block;

    let fetcher: Arc<dyn Fetcher> = Arc::new(ClientFetcher {
        client: client.clone(),
        query: query.clone(),
        config: config.clone(),
        reverse,
    });

    let mut sched = Scheduler::new(reverse, open_ended, config.clone(), fetcher, observer);

    if reverse {
        let upper_bound = match query.to_block {
            Some(t) => t,
            None => match client.get_height().await.context("get height") {
                Ok(h) => h,
                Err(e) => {
                    tx.send(Err(e)).await.ok();
                    sched.finish();
                    return;
                }
            },
        };
        sched.upper_bound = upper_bound;
        sched.lower_bound = from_block;
        sched.watermark = upper_bound;
        if from_block < upper_bound {
            sched.holes.insert(from_block, upper_bound);
            sched.run_core(&tx).await;
        }
        sched.finish();
        return;
    }

    // Forward: fast-track the first request, sent immediately.
    let started = Instant::now();
    let initial = match client
        .get_arrow_with_size(&query, WAIT_ON_RATE_LIMIT)
        .await
        .context("get initial data")
    {
        Ok(i) => i,
        Err(e) => {
            tx.send(Err(e)).await.ok();
            sched.finish();
            return;
        }
    };
    let size = initial.response_bytes;
    let next_block = initial.response.next_block.max(from_block + 1);
    let archive_height = initial.response.archive_height;
    let mapped = match map_responses(config.clone(), vec![initial.response], false).await {
        Ok(mut v) => v.remove(0),
        Err(e) => {
            tx.send(Err(e)).await.ok();
            sched.finish();
            return;
        }
    };

    let upper_bound = match query.to_block {
        Some(t) => t,
        None => match archive_height {
            Some(h) => h,
            None => match client.get_height().await.context("get height") {
                Ok(h) => h,
                Err(e) => {
                    tx.send(Err(e)).await.ok();
                    sched.finish();
                    return;
                }
            },
        },
    };

    sched.upper_bound = upper_bound;
    sched.lower_bound = from_block;
    sched.watermark = next_block;

    // The fast-track is a real request; record it (never flagged truncated, as
    // it intentionally lets the server self-limit the first response).
    sched.report_request(
        from_block,
        next_block,
        next_block,
        next_block - from_block,
        size,
        false,
        RequestKind::Frontier,
        started.elapsed(),
    );
    let stop = sched.accumulate_and_check(&mapped);
    if tx.send(Ok(mapped)).await.is_err() {
        sched.finish();
        return;
    }
    if stop || next_block >= upper_bound {
        sched.finish();
        return;
    }

    sched.holes.insert(next_block, upper_bound);
    if config.concurrency == 1 {
        sched.run_sequential(&tx).await;
    } else {
        sched.run_core(&tx).await;
    }
    sched.finish();
}

fn count_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

fn check_entity_limit(val: usize, limit: Option<usize>) -> bool {
    if let Some(limit) = limit {
        val >= limit
    } else {
        false
    }
}

async fn map_responses(
    cfg: StreamConfig,
    mut responses: Vec<ArrowResponse>,
    reverse: bool,
) -> Result<Vec<ArrowResponse>> {
    if reverse {
        responses.reverse();
        for resp in responses.iter_mut() {
            resp.data.blocks.reverse();
            resp.data.transactions.reverse();
            resp.data.logs.reverse();
            resp.data.traces.reverse();
        }
    }

    rayon_async::spawn(move || {
        responses
            .into_iter()
            .map(|resp| {
                Ok(ArrowResponse {
                    data: ArrowResponseData {
                        decoded_logs: match cfg.event_signature.as_ref() {
                            Some(sig) => resp
                                .data
                                .logs
                                .iter()
                                .map(|batch| {
                                    let batch =
                                        decode_logs_batch(sig, batch).context("decode logs")?;
                                    map_batch(
                                        cfg.column_mapping.as_ref().map(|cm| &cm.decoded_log),
                                        cfg.hex_output,
                                        batch,
                                        reverse,
                                    )
                                    .context("map batch")
                                })
                                .collect::<Result<Vec<_>>>()?,
                            None => Vec::new(),
                        },
                        blocks: resp
                            .data
                            .blocks
                            .into_iter()
                            .map(|batch| {
                                map_batch(
                                    cfg.column_mapping.as_ref().map(|cm| &cm.block),
                                    cfg.hex_output,
                                    batch,
                                    reverse,
                                )
                            })
                            .collect::<Result<Vec<_>>>()?,
                        transactions: resp
                            .data
                            .transactions
                            .into_iter()
                            .map(|batch| {
                                map_batch(
                                    cfg.column_mapping.as_ref().map(|cm| &cm.transaction),
                                    cfg.hex_output,
                                    batch,
                                    reverse,
                                )
                            })
                            .collect::<Result<Vec<_>>>()?,
                        logs: resp
                            .data
                            .logs
                            .into_iter()
                            .map(|batch| {
                                map_batch(
                                    cfg.column_mapping.as_ref().map(|cm| &cm.log),
                                    cfg.hex_output,
                                    batch,
                                    reverse,
                                )
                            })
                            .collect::<Result<Vec<_>>>()?,
                        traces: resp
                            .data
                            .traces
                            .into_iter()
                            .map(|batch| {
                                map_batch(
                                    cfg.column_mapping.as_ref().map(|cm| &cm.trace),
                                    cfg.hex_output,
                                    batch,
                                    reverse,
                                )
                            })
                            .collect::<Result<Vec<_>>>()?,
                    },
                    ..resp
                })
            })
            .collect()
    })
    .await
    .unwrap()
}

fn map_batch(
    column_mapping: Option<&BTreeMap<String, crate::DataType>>,
    hex_output: HexOutput,
    mut batch: RecordBatch,
    reverse: bool,
) -> Result<RecordBatch> {
    if reverse {
        let cols = batch
            .columns()
            .iter()
            .map(|a| reverse_array(a.as_ref()))
            .collect::<Result<Vec<_>>>()
            .context("reverse the arrays")?;

        batch = RecordBatch::try_new(batch.schema(), cols).unwrap();
    }

    if let Some(map) = column_mapping {
        batch =
            crate::column_mapping::apply_to_batch(&batch, map).context("apply column mapping")?;
    }

    match hex_output {
        HexOutput::NonPrefixed => batch = hex_encode_batch(&batch, false),
        HexOutput::Prefixed => batch = hex_encode_batch(&batch, true),
        HexOutput::NoEncode => (),
    }

    Ok(batch)
}

fn reverse_array(array: &dyn Array) -> Result<ArrayRef> {
    match array.data_type() {
        ArrowDataType::Binary => Ok(Arc::new(BinaryArray::from_iter(
            array.as_binary::<i32>().iter().rev(),
        ))),
        ArrowDataType::Utf8 => Ok(Arc::new(StringArray::from_iter(
            array.as_string::<i32>().iter().rev(),
        ))),
        ArrowDataType::Boolean => Ok(Arc::new(BooleanArray::from_iter(
            array.as_boolean().iter().rev(),
        ))),
        ArrowDataType::UInt64 => Ok(Arc::new(UInt64Array::from_iter(
            array.as_primitive::<UInt64Type>().iter().rev(),
        ))),
        ArrowDataType::UInt8 => Ok(Arc::new(UInt8Array::from_iter(
            array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap()
                .iter()
                .rev(),
        ))),
        dt => Err(anyhow!(
            "reversing an array of datatype {:?} is not supported",
            dt
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::StreamMetrics;

    /// `(from, to) -> (next_block, size_bytes, archive_height)`.
    type CoverFn = Box<dyn Fn(u64, u64) -> (u64, u64, Option<u64>) + Send + Sync>;

    /// Mock fetcher driven by a [`CoverFn`]. The covered `from` is stashed in the
    /// response's `total_execution_time` so tests can reconstruct each delivered
    /// range.
    struct MockFetcher {
        cover: CoverFn,
    }

    impl Fetcher for MockFetcher {
        fn fetch(&self, from_block: u64, to_block: u64) -> BoxFuture<Result<FetchOutcome>> {
            let (next_block, size_bytes, archive_height) = (self.cover)(from_block, to_block);
            let resp = ArrowResponse {
                archive_height,
                next_block,
                total_execution_time: from_block,
                data: ArrowResponseData::default(),
                rollback_guard: None,
            };
            Box::pin(async move {
                Ok(FetchOutcome {
                    next_block,
                    archive_height,
                    size_bytes,
                    resp,
                })
            })
        }
    }

    fn cfg(concurrency: usize) -> StreamConfig {
        StreamConfig {
            concurrency,
            batch_size: 1000,
            min_batch_size: 1,
            max_batch_size: None,
            response_bytes_target: 400_000,
            max_buffered_bytes: None,
            reverse: false,
            ..Default::default()
        }
    }

    fn seed(sched: &mut Scheduler, reverse: bool, lo: u64, upper: u64) {
        sched.upper_bound = upper;
        sched.lower_bound = lo;
        sched.watermark = if reverse { upper } else { lo };
        if lo < upper {
            sched.holes.insert(lo, upper);
        }
    }

    /// Drive `run_core` and return `(delivered (start, next) pairs, warnings)`.
    async fn run_core_test(
        reverse: bool,
        open_ended: bool,
        config: StreamConfig,
        lo: u64,
        upper: u64,
        cover: impl Fn(u64, u64) -> (u64, u64, Option<u64>) + Send + Sync + 'static,
        observer: Option<Arc<dyn StreamObserver>>,
    ) -> (Vec<(u64, u64)>, usize) {
        let fetcher: Arc<dyn Fetcher> = Arc::new(MockFetcher {
            cover: Box::new(cover),
        });
        let mut sched = Scheduler::new(reverse, open_ended, config, fetcher, observer);
        seed(&mut sched, reverse, lo, upper);
        let (tx, mut rx) = mpsc::channel(8192);
        sched.run_core(&tx).await;
        sched.finish();
        drop(tx);
        let mut out = Vec::new();
        while let Some(r) = rx.recv().await {
            let resp = r.unwrap();
            out.push((resp.total_execution_time, resp.next_block));
        }
        (out, sched.warnings_emitted)
    }

    async fn run_sequential_test(
        config: StreamConfig,
        lo: u64,
        upper: u64,
        open_ended: bool,
        cover: impl Fn(u64, u64) -> (u64, u64, Option<u64>) + Send + Sync + 'static,
    ) -> Vec<(u64, u64)> {
        let fetcher: Arc<dyn Fetcher> = Arc::new(MockFetcher {
            cover: Box::new(cover),
        });
        let mut sched = Scheduler::new(false, open_ended, config, fetcher, None);
        seed(&mut sched, false, lo, upper);
        let (tx, mut rx) = mpsc::channel(8192);
        sched.run_sequential(&tx).await;
        drop(tx);
        let mut out = Vec::new();
        while let Some(r) = rx.recv().await {
            let resp = r.unwrap();
            out.push((resp.total_execution_time, resp.next_block));
        }
        out
    }

    /// Assert the delivered chunks partition `[lo, hi)`: contiguous, in order,
    /// disjoint, covering every block exactly once.
    fn assert_partition(chunks: &[(u64, u64)], lo: u64, hi: u64, reverse: bool) {
        assert!(
            !chunks.is_empty() || lo == hi,
            "no chunks for non-empty range"
        );
        if reverse {
            let mut top = hi;
            for &(s, n) in chunks {
                assert_eq!(n, top, "reverse contiguity at {s}..{n}");
                assert!(s < n, "non-empty chunk");
                top = s;
            }
            assert_eq!(top, lo, "reverse covers down to lo");
        } else {
            let mut expect = lo;
            for &(s, n) in chunks {
                assert_eq!(s, expect, "forward contiguity at {s}..{n}");
                assert!(n > s, "non-empty chunk");
                expect = n;
            }
            assert_eq!(expect, hi, "forward covers up to hi");
        }
    }

    fn cover_full(
        bpb: u64,
    ) -> impl Fn(u64, u64) -> (u64, u64, Option<u64>) + Send + Sync + 'static {
        move |from, to| (to, (to - from) * bpb, None)
    }

    fn cover_truncate(
        cap: u64,
        bpb: u64,
    ) -> impl Fn(u64, u64) -> (u64, u64, Option<u64>) + Send + Sync + 'static {
        move |from, to| {
            let next = (from + cap).min(to);
            (next, (next - from) * bpb, None)
        }
    }

    #[test]
    fn concurrency_zero_is_rejected() {
        assert!(check_concurrency(0).is_err());
        assert!(check_concurrency(1).is_ok());
        assert!(check_concurrency(10).is_ok());
    }

    #[tokio::test]
    async fn forward_dense_full_coverage() {
        let (chunks, warns) =
            run_core_test(false, false, cfg(4), 0, 100_000, cover_full(100), None).await;
        assert_partition(&chunks, 0, 100_000, false);
        assert_eq!(warns, 0);
    }

    #[tokio::test]
    async fn forward_truncation_is_backfilled() {
        // Every request covers at most 300 blocks; the rest becomes a gap that
        // must be backfilled before delivery can advance.
        let (chunks, _) = run_core_test(
            false,
            false,
            cfg(4),
            0,
            5_000,
            cover_truncate(300, 1000),
            None,
        )
        .await;
        assert_partition(&chunks, 0, 5_000, false);
    }

    #[tokio::test]
    async fn forward_sparse_region_is_scanned() {
        // Zero-byte (empty) responses => density 0 => projection scans the whole
        // remaining hole; coverage must still be complete.
        let (chunks, _) = run_core_test(false, false, cfg(4), 0, 50_000, cover_full(0), None).await;
        assert_partition(&chunks, 0, 50_000, false);
    }

    #[tokio::test]
    async fn forward_backpressure_no_deadlock() {
        let mut config = cfg(4);
        config.max_buffered_bytes = Some(1); // tiny buffer: only the watermark hole runs
        let (chunks, _) =
            run_core_test(false, false, config, 0, 20_000, cover_full(50), None).await;
        assert_partition(&chunks, 0, 20_000, false);
    }

    #[tokio::test]
    async fn reverse_dense_full_coverage() {
        let mut config = cfg(4);
        config.reverse = true;
        let (chunks, _) =
            run_core_test(true, false, config, 0, 100_000, cover_full(100), None).await;
        assert_partition(&chunks, 0, 100_000, true);
    }

    #[tokio::test]
    async fn reverse_truncation_is_backfilled() {
        let mut config = cfg(4);
        config.reverse = true;
        let (chunks, _) = run_core_test(
            true,
            false,
            config,
            0,
            5_000,
            cover_truncate(300, 1000),
            None,
        )
        .await;
        assert_partition(&chunks, 0, 5_000, true);
    }

    #[tokio::test]
    async fn sequential_full_coverage() {
        // Server self-limits each response to 1000 blocks.
        let chunks = run_sequential_test(cfg(1), 0, 10_000, false, cover_truncate(1000, 100)).await;
        assert_partition(&chunks, 0, 10_000, false);
        assert_eq!(chunks.len(), 10);
    }

    #[tokio::test]
    async fn open_ended_follows_advancing_head() {
        // Upper bound seeded at 1000; the head advances to 2000 mid-stream.
        let cover = |from: u64, to: u64| (to, (to - from) * 100, Some(2000u64));
        let (chunks, _) = run_core_test(false, true, cfg(4), 0, 1_000, cover, None).await;
        assert_partition(&chunks, 0, 2_000, false);
    }

    fn bare_scheduler() -> Scheduler {
        let mut config = cfg(2);
        config.min_batch_size = 200;
        let fetcher: Arc<dyn Fetcher> = Arc::new(MockFetcher {
            cover: Box::new(cover_full(100)),
        });
        Scheduler::new(false, false, config, fetcher, None)
    }

    // A "bad" response: truncated, tiny in bytes, AND covering fewer than
    // min_batch_size blocks (server stalled below our smallest projected range).
    const BAD_BLOCKS: u64 = 50; // < min_batch_size (200)

    #[test]
    fn warning_fires_at_most_once_per_stream() {
        let mut s = bare_scheduler();
        let small = s.config.response_bytes_target / 2 - 1;

        // A sustained run of bad responses warns exactly once.
        for _ in 0..WARN_THRESHOLD {
            s.update_warning(true, small, BAD_BLOCKS);
        }
        assert_eq!(s.warnings_emitted, 1);

        // It never warns again this stream — not while the run continues, and not
        // even after a healthy response and a brand-new bad run. No log spam.
        for _ in 0..WARN_THRESHOLD {
            s.update_warning(true, small, BAD_BLOCKS);
        }
        s.update_warning(false, small, BAD_BLOCKS);
        for _ in 0..(WARN_THRESHOLD * 2) {
            s.update_warning(true, small, BAD_BLOCKS);
        }
        assert_eq!(s.warnings_emitted, 1);
    }

    #[test]
    fn warning_silent_when_runs_are_broken() {
        // Bad responses that never reach the threshold in a row (a healthy
        // response keeps resetting the counter) must never warn.
        let mut s = bare_scheduler();
        let small = s.config.response_bytes_target / 2 - 1;
        for _ in 0..(WARN_THRESHOLD * 3) {
            for _ in 0..(WARN_THRESHOLD - 1) {
                s.update_warning(true, small, BAD_BLOCKS);
            }
            s.update_warning(false, small, BAD_BLOCKS);
        }
        assert_eq!(s.warnings_emitted, 0);
    }

    #[test]
    fn warning_silent_for_healthy_small_responses() {
        // The common compact-query case: truncated and under target bytes, but
        // covering a healthy block range (>= min_batch_size). The server is just
        // row/time-capping a fine stream — this must NOT warn, even forever.
        let mut s = bare_scheduler();
        let small = s.config.response_bytes_target / 2 - 1;
        let healthy_blocks = s.config.min_batch_size + 10;
        for _ in 0..(WARN_THRESHOLD * 5) {
            s.update_warning(true, small, healthy_blocks);
        }
        assert_eq!(s.warnings_emitted, 0);
    }

    #[test]
    fn warning_ignores_large_truncations() {
        // Truncated but not small (byte-heavy query hitting the size cap) =>
        // normal/expected; no warning even if block coverage is small.
        let mut s = bare_scheduler();
        let large = s.config.response_bytes_target;
        for _ in 0..(WARN_THRESHOLD * 2) {
            s.update_warning(true, large, BAD_BLOCKS);
        }
        assert_eq!(s.warnings_emitted, 0);
    }

    #[test]
    fn adaptive_buffer_grows_for_large_responses() {
        // Default (unset) buffer: 2 * concurrency * target.
        let mut s = bare_scheduler();
        let concurrency = s.config.concurrency as u64;
        let target = s.config.response_bytes_target;
        assert_eq!(s.max_buffered_bytes, 2 * concurrency * target);

        // A response far larger than target grows the cap to hold ~2 per worker.
        let big = 12_000_000;
        s.note_response_size(big);
        assert_eq!(s.max_buffered_bytes, 2 * concurrency * big);

        // Smaller subsequent responses don't shrink it.
        s.note_response_size(1000);
        assert_eq!(s.max_buffered_bytes, 2 * concurrency * big);
    }

    #[test]
    fn adaptive_buffer_ignores_sub_target_responses() {
        let mut s = bare_scheduler();
        let initial = s.max_buffered_bytes;
        s.note_response_size(s.config.response_bytes_target / 4);
        assert_eq!(s.max_buffered_bytes, initial);
    }

    #[test]
    fn explicit_buffer_is_never_grown() {
        let mut config = cfg(2);
        config.max_buffered_bytes = Some(5_000_000);
        let fetcher: Arc<dyn Fetcher> = Arc::new(MockFetcher {
            cover: Box::new(cover_full(100)),
        });
        let mut s = Scheduler::new(false, false, config, fetcher, None);
        assert_eq!(s.max_buffered_bytes, 5_000_000);
        s.note_response_size(50_000_000);
        assert_eq!(s.max_buffered_bytes, 5_000_000, "explicit cap is honoured");
    }

    #[tokio::test]
    async fn observer_receives_aggregates() {
        let metrics = Arc::new(StreamMetrics::new());
        let obs: Arc<dyn StreamObserver> = metrics.clone();
        let (chunks, _) =
            run_core_test(false, false, cfg(4), 0, 40_000, cover_full(100), Some(obs)).await;
        assert_partition(&chunks, 0, 40_000, false);

        let summary = metrics.summary();
        assert!(summary.num_requests > 0);
        assert_eq!(summary.total_blocks, 40_000, "every block counted once");
        assert_eq!(summary.num_truncated, 0);
        assert_eq!(summary.total_bytes, 40_000 * 100);
        assert!(summary.mean_in_flight > 0.0);
    }
}
