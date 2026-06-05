//! Observability for the streaming engine.
//!
//! The scheduler can optionally report, for every completed HTTP request, a
//! [`RequestStats`] record and, when the stream ends, an aggregate
//! [`StreamSummary`]. These are surfaced through the [`StreamObserver`] trait,
//! which is attached explicitly via
//! [`Client::stream_arrow_with_observer`](crate::Client::stream_arrow_with_observer).
//!
//! [`StreamMetrics`] is a ready-made, thread-safe observer implementation
//! (atomics plus a small fixed histogram) that the caller constructs, passes in,
//! and reads from during or after the stream. Power users can implement
//! [`StreamObserver`] themselves to collect raw [`RequestStats`] (for exact
//! percentiles, custom exporters, etc.).
//!
//! When no observer is attached the engine does no metrics work at all — there
//! is zero overhead on the default streaming paths.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Whether a scheduled request extended the frontier or backfilled a gap left
/// by an earlier truncated response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestKind {
    /// Extends the frontier toward the upper bound.
    Frontier,
    /// Backfills a gap below the frontier left by an earlier truncated response.
    GapFill,
}

/// Metrics for a single completed HTTP request, reported via
/// [`StreamObserver::on_request`].
#[derive(Debug, Clone)]
pub struct RequestStats {
    /// First block of the request (inclusive).
    pub from_block: u64,
    /// Requested exclusive end of the range.
    pub requested_end: u64,
    /// Exclusive end actually covered by the response.
    pub next_block: u64,
    /// `requested_end - from_block`.
    pub requested_blocks: u64,
    /// `next_block - from_block` (blocks actually covered).
    pub actual_blocks: u64,
    /// Block span the projector aimed for, before clamping to the hole.
    pub projected_blocks: u64,
    /// HTTP response body size in bytes.
    pub response_bytes: u64,
    /// Configured `response_bytes_target` at the time of the request.
    pub target_bytes: u64,
    /// `response_bytes / target_bytes`.
    pub size_ratio: f64,
    /// Observed density, `response_bytes / actual_blocks`.
    pub bytes_per_block: f64,
    /// `next_block < requested_end` — the server stopped early.
    pub truncated: bool,
    /// Whether this request extended the frontier or backfilled a gap.
    pub kind: RequestKind,
    /// Wall-clock latency of the request (fetch + decode/map).
    pub duration: Duration,
}

/// Number of size-vs-target histogram buckets.
pub const NUM_SIZE_BUCKETS: usize = 6;

/// Human-readable labels for the size-vs-target histogram buckets, relative to
/// `response_bytes_target`.
pub const SIZE_BUCKET_LABELS: [&str; NUM_SIZE_BUCKETS] = [
    "<0.25", "0.25-0.5", "0.5-0.75", "0.75-1.0", "1.0-1.25", ">1.25",
];

/// Index of the size-vs-target histogram bucket for a given `size_ratio`.
fn size_bucket(ratio: f64) -> usize {
    if ratio < 0.25 {
        0
    } else if ratio < 0.5 {
        1
    } else if ratio < 0.75 {
        2
    } else if ratio < 1.0 {
        3
    } else if ratio < 1.25 {
        4
    } else {
        5
    }
}

/// Observer that receives streaming metrics.
///
/// Implementations must be cheap and non-blocking: `on_request` is called on the
/// scheduler task once per completed HTTP request.
pub trait StreamObserver: Send + Sync {
    /// Called once for every completed HTTP request.
    fn on_request(&self, stats: &RequestStats);
    /// Called once per scheduler iteration with the current in-flight request
    /// count and undelivered reorder-buffer size. Lets observers track buffer
    /// high-water and concurrency saturation live. Default: no-op.
    fn on_progress(&self, _in_flight: u64, _buffered_bytes: u64) {}
    /// Called once when the stream finishes (or is closed early), with the final
    /// aggregate summary. Default: no-op.
    fn on_finish(&self, _summary: &StreamSummary) {}
}

/// Aggregate summary of a stream run, produced by [`StreamMetrics::summary`] and
/// passed to [`StreamObserver::on_finish`].
#[derive(Debug, Clone)]
pub struct StreamSummary {
    /// Total completed requests.
    pub num_requests: u64,
    /// Requests whose response was truncated before the requested end.
    pub num_truncated: u64,
    /// `num_truncated / num_requests` (0 when no requests).
    pub truncation_rate: f64,
    /// Sum of response body sizes.
    pub total_bytes: u64,
    /// Sum of blocks covered.
    pub total_blocks: u64,
    /// Wall-clock span of the run (engine-fed when finished, else measured live
    /// from when the metrics handle was created).
    pub wall_clock: Duration,
    /// `total_blocks / wall_clock` (0 when no elapsed time).
    pub blocks_per_sec: f64,
    /// `total_bytes / wall_clock` (0 when no elapsed time).
    pub bytes_per_sec: f64,
    /// Mean `size_ratio` across requests.
    pub mean_size_ratio: f64,
    /// Size-vs-target histogram counts, see [`SIZE_BUCKET_LABELS`].
    pub size_histogram: [u64; NUM_SIZE_BUCKETS],
    /// Mean density (`total_bytes / total_blocks`).
    pub mean_bytes_per_block: f64,
    /// Smallest block-range size requested-and-covered.
    pub min_blocks: u64,
    /// Mean covered block-range size (`total_blocks / num_requests`).
    pub mean_blocks: f64,
    /// Largest block-range size covered.
    pub max_blocks: u64,
    /// High-water mark of the undelivered reorder buffer, in bytes.
    pub max_buffered_bytes_observed: u64,
    /// Mean number of in-flight requests across scheduler iterations.
    pub mean_in_flight: f64,
    /// Requests that extended the frontier.
    pub num_frontier: u64,
    /// Requests that backfilled a gap.
    pub num_gap_fill: u64,
}

/// Thread-safe aggregate metrics handle.
///
/// Construct one, pass an `Arc<StreamMetrics>` to
/// [`Client::stream_arrow_with_observer`](crate::Client::stream_arrow_with_observer),
/// keep a clone, and call [`summary`](Self::summary) during or after the stream.
#[derive(Debug)]
pub struct StreamMetrics {
    num_requests: AtomicU64,
    num_truncated: AtomicU64,
    total_bytes: AtomicU64,
    total_blocks: AtomicU64,
    /// Sum of `size_ratio * 1_000_000`, for the running mean.
    size_ratio_micros: AtomicU64,
    size_histogram: [AtomicU64; NUM_SIZE_BUCKETS],
    min_blocks: AtomicU64,
    max_blocks: AtomicU64,
    max_buffered_bytes_observed: AtomicU64,
    in_flight_sum: AtomicU64,
    in_flight_samples: AtomicU64,
    num_frontier: AtomicU64,
    num_gap_fill: AtomicU64,
    /// When the handle was created; used to derive wall-clock live.
    created: Instant,
    /// Optional precise elapsed override (nanos) fed by the engine at end of
    /// stream. `0` means "unset", in which case `created.elapsed()` is used.
    elapsed_nanos: AtomicU64,
}

impl Default for StreamMetrics {
    fn default() -> Self {
        Self {
            num_requests: AtomicU64::new(0),
            num_truncated: AtomicU64::new(0),
            total_bytes: AtomicU64::new(0),
            total_blocks: AtomicU64::new(0),
            size_ratio_micros: AtomicU64::new(0),
            size_histogram: Default::default(),
            min_blocks: AtomicU64::new(u64::MAX),
            max_blocks: AtomicU64::new(0),
            max_buffered_bytes_observed: AtomicU64::new(0),
            in_flight_sum: AtomicU64::new(0),
            in_flight_samples: AtomicU64::new(0),
            num_frontier: AtomicU64::new(0),
            num_gap_fill: AtomicU64::new(0),
            created: Instant::now(),
            elapsed_nanos: AtomicU64::new(0),
        }
    }
}

impl StreamMetrics {
    /// Create an empty metrics handle.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the current undelivered-buffer size; keeps the observed maximum.
    pub fn record_buffered_bytes(&self, bytes: u64) {
        self.max_buffered_bytes_observed
            .fetch_max(bytes, Ordering::Relaxed);
    }

    /// Record the current in-flight request count (one sample per scheduler
    /// iteration), for the mean-in-flight estimate.
    pub fn record_in_flight(&self, count: u64) {
        self.in_flight_sum.fetch_add(count, Ordering::Relaxed);
        self.in_flight_samples.fetch_add(1, Ordering::Relaxed);
    }

    /// Feed the engine's monotonic elapsed time so throughput can be derived.
    pub fn record_elapsed(&self, elapsed: Duration) {
        self.elapsed_nanos
            .fetch_max(elapsed.as_nanos() as u64, Ordering::Relaxed);
    }

    /// Snapshot the current aggregate summary. Safe to call at any time.
    pub fn summary(&self) -> StreamSummary {
        let num_requests = self.num_requests.load(Ordering::Relaxed);
        let num_truncated = self.num_truncated.load(Ordering::Relaxed);
        let total_bytes = self.total_bytes.load(Ordering::Relaxed);
        let total_blocks = self.total_blocks.load(Ordering::Relaxed);
        let in_flight_samples = self.in_flight_samples.load(Ordering::Relaxed);
        let in_flight_sum = self.in_flight_sum.load(Ordering::Relaxed);

        let mut size_histogram = [0u64; NUM_SIZE_BUCKETS];
        for (dst, src) in size_histogram.iter_mut().zip(self.size_histogram.iter()) {
            *dst = src.load(Ordering::Relaxed);
        }

        // Prefer the engine-fed precise elapsed override; otherwise measure live
        // from when this handle was created.
        let wall_clock = {
            let fed = self.elapsed_nanos.load(Ordering::Relaxed);
            if fed > 0 {
                Duration::from_nanos(fed)
            } else {
                self.created.elapsed()
            }
        };
        let secs = wall_clock.as_secs_f64();

        let div = |num: f64, den: f64| if den > 0.0 { num / den } else { 0.0 };

        StreamSummary {
            num_requests,
            num_truncated,
            truncation_rate: div(num_truncated as f64, num_requests as f64),
            total_bytes,
            total_blocks,
            wall_clock,
            blocks_per_sec: div(total_blocks as f64, secs),
            bytes_per_sec: div(total_bytes as f64, secs),
            mean_size_ratio: div(
                self.size_ratio_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0,
                num_requests as f64,
            ),
            size_histogram,
            mean_bytes_per_block: div(total_bytes as f64, total_blocks as f64),
            min_blocks: {
                let m = self.min_blocks.load(Ordering::Relaxed);
                if m == u64::MAX {
                    0
                } else {
                    m
                }
            },
            mean_blocks: div(total_blocks as f64, num_requests as f64),
            max_blocks: self.max_blocks.load(Ordering::Relaxed),
            max_buffered_bytes_observed: self.max_buffered_bytes_observed.load(Ordering::Relaxed),
            mean_in_flight: div(in_flight_sum as f64, in_flight_samples as f64),
            num_frontier: self.num_frontier.load(Ordering::Relaxed),
            num_gap_fill: self.num_gap_fill.load(Ordering::Relaxed),
        }
    }
}

impl StreamObserver for StreamMetrics {
    fn on_request(&self, stats: &RequestStats) {
        self.num_requests.fetch_add(1, Ordering::Relaxed);
        if stats.truncated {
            self.num_truncated.fetch_add(1, Ordering::Relaxed);
        }
        self.total_bytes
            .fetch_add(stats.response_bytes, Ordering::Relaxed);
        self.total_blocks
            .fetch_add(stats.actual_blocks, Ordering::Relaxed);
        self.size_ratio_micros.fetch_add(
            (stats.size_ratio * 1_000_000.0).round() as u64,
            Ordering::Relaxed,
        );
        self.size_histogram[size_bucket(stats.size_ratio)].fetch_add(1, Ordering::Relaxed);
        self.min_blocks
            .fetch_min(stats.actual_blocks, Ordering::Relaxed);
        self.max_blocks
            .fetch_max(stats.actual_blocks, Ordering::Relaxed);
        match stats.kind {
            RequestKind::Frontier => self.num_frontier.fetch_add(1, Ordering::Relaxed),
            RequestKind::GapFill => self.num_gap_fill.fetch_add(1, Ordering::Relaxed),
        };
    }

    fn on_progress(&self, in_flight: u64, buffered_bytes: u64) {
        self.record_in_flight(in_flight);
        self.record_buffered_bytes(buffered_bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stats(
        actual_blocks: u64,
        response_bytes: u64,
        target: u64,
        truncated: bool,
    ) -> RequestStats {
        let size_ratio = response_bytes as f64 / target as f64;
        RequestStats {
            from_block: 0,
            requested_end: actual_blocks,
            next_block: actual_blocks,
            requested_blocks: actual_blocks,
            actual_blocks,
            projected_blocks: actual_blocks,
            response_bytes,
            target_bytes: target,
            size_ratio,
            bytes_per_block: response_bytes as f64 / actual_blocks as f64,
            truncated,
            kind: RequestKind::Frontier,
            duration: Duration::from_millis(10),
        }
    }

    #[test]
    fn size_bucket_boundaries() {
        assert_eq!(size_bucket(0.0), 0);
        assert_eq!(size_bucket(0.24), 0);
        assert_eq!(size_bucket(0.25), 1);
        assert_eq!(size_bucket(0.49), 1);
        assert_eq!(size_bucket(0.5), 2);
        assert_eq!(size_bucket(0.74), 2);
        assert_eq!(size_bucket(0.75), 3);
        assert_eq!(size_bucket(0.99), 3);
        assert_eq!(size_bucket(1.0), 4);
        assert_eq!(size_bucket(1.24), 4);
        assert_eq!(size_bucket(1.25), 5);
        assert_eq!(size_bucket(10.0), 5);
    }

    #[test]
    fn empty_summary_is_zeroed() {
        let m = StreamMetrics::new();
        let s = m.summary();
        assert_eq!(s.num_requests, 0);
        assert_eq!(s.truncation_rate, 0.0);
        assert_eq!(s.min_blocks, 0);
        assert_eq!(s.max_blocks, 0);
        assert_eq!(s.mean_in_flight, 0.0);
        assert_eq!(s.blocks_per_sec, 0.0);
    }

    #[test]
    fn aggregates_requests() {
        let m = StreamMetrics::new();
        // target = 400k. Three requests: under, on-target, over.
        m.on_request(&stats(100, 200_000, 400_000, true)); // ratio 0.5 -> bucket 2
        m.on_request(&stats(200, 400_000, 400_000, false)); // ratio 1.0 -> bucket 4
        m.on_request(&stats(50, 600_000, 400_000, false)); // ratio 1.5 -> bucket 5

        let s = m.summary();
        assert_eq!(s.num_requests, 3);
        assert_eq!(s.num_truncated, 1);
        assert!((s.truncation_rate - 1.0 / 3.0).abs() < 1e-9);
        assert_eq!(s.total_bytes, 1_200_000);
        assert_eq!(s.total_blocks, 350);
        assert_eq!(s.min_blocks, 50);
        assert_eq!(s.max_blocks, 200);
        assert!((s.mean_blocks - 350.0 / 3.0).abs() < 1e-9);
        assert!((s.mean_size_ratio - 1.0).abs() < 1e-9); // (0.5+1.0+1.5)/3
        assert_eq!(s.size_histogram[2], 1);
        assert_eq!(s.size_histogram[4], 1);
        assert_eq!(s.size_histogram[5], 1);
        assert!((s.mean_bytes_per_block - 1_200_000.0 / 350.0).abs() < 1e-6);
        assert_eq!(s.num_frontier, 3);
        assert_eq!(s.num_gap_fill, 0);
    }

    #[test]
    fn in_flight_and_buffer_tracking() {
        let m = StreamMetrics::new();
        m.record_in_flight(2);
        m.record_in_flight(4);
        m.record_buffered_bytes(1000);
        m.record_buffered_bytes(500);
        let s = m.summary();
        assert_eq!(s.mean_in_flight, 3.0);
        assert_eq!(s.max_buffered_bytes_observed, 1000);
    }

    #[test]
    fn on_progress_feeds_in_flight_and_buffer() {
        let m = StreamMetrics::new();
        m.on_progress(1, 100);
        m.on_progress(5, 50);
        let s = m.summary();
        assert_eq!(s.mean_in_flight, 3.0);
        assert_eq!(s.max_buffered_bytes_observed, 100);
    }

    #[test]
    fn throughput_from_elapsed() {
        let m = StreamMetrics::new();
        m.on_request(&stats(1000, 400_000, 400_000, false));
        m.record_elapsed(Duration::from_secs(2));
        let s = m.summary();
        assert_eq!(s.blocks_per_sec, 500.0);
        assert_eq!(s.bytes_per_sec, 200_000.0);
    }

    #[test]
    fn gap_fill_counted() {
        let m = StreamMetrics::new();
        let mut st = stats(100, 100_000, 400_000, false);
        st.kind = RequestKind::GapFill;
        m.on_request(&st);
        let s = m.summary();
        assert_eq!(s.num_gap_fill, 1);
        assert_eq!(s.num_frontier, 0);
    }
}
