# Streaming engine design

Status: **implemented** (Rust core) — `hypersync-client/src/stream.rs` rewritten,
`StreamConfig` updated, metrics in `hypersync-client/src/metrics.rs`, tuning CLI in
`examples/tune_stream`. Node/Python rollout (§14) is a follow-up.
Scope: redesign the `stream_arrow` engine in `hypersync-client/src/stream.rs`, then roll
the (rebuilt) core out to the node and python bindings. The Go client is a separate
reimplementation and is **out of scope** for this change.

---

## 1. Motivation

The current streaming engine pins each worker to a **fixed, pre-sliced block range** and
makes it **paginate to the end of that range** before the result is usable. Range
boundaries are chosen up front from a single shared `batch_size` (an `AtomicU64` that drifts
as the run proceeds), and the dynamic size adjustment happens globally, once per
"generation", in the consumer.

Two consequences:

1. A worker that gets a large slice can spend many sequential HTTP round-trips covering it,
   while the slice boundaries were decided from a possibly-stale `batch_size`.
2. Size adaptation is coarse (one global change per generation) and reacts late.

The redesign makes **one HTTP request the unit of work**, lets a shared scheduler always
hand the next free worker the **earliest still-needed range** (filling truncation gaps
before extending the frontier), and **projects each request's size locally** from the most
recently observed byte-density. The result delivers contiguous data to the consumer sooner
and keeps response sizes closer to target.

The stream already emits **one `ArrowResponse` per HTTP response, in block order**, so the
change is a drop-in from the consumer's point of view — no change to the shape of what the
stream yields.

---

## 2. Current design (v1) — summary

`hypersync-client/src/stream.rs`:

- **Fast-track** (forward only, lines ~57-79): one unbounded `get_arrow` from `from_block`,
  sent immediately; cursor advanced to `next_block`.
- **`BlockRangeIterator`** (lines ~427-478): pre-slices `[from, to_block)` into fixed
  `batch_size` chunks; `batch_size` is read from an `AtomicU64` (`step`) whose upper 32 bits
  carry a "generation" counter.
- **`run_query_to_end`** (lines ~392-425): each worker paginates *internally* until it has
  covered its whole assigned slice (potentially many HTTP requests).
- **Scheduler** (lines ~98-137): up to `concurrency` futures on a `JoinSet`; results land in
  a `BTreeMap` keyed by `req_idx` to **re-order**, then forwarded.
- **Consumer / size control** (lines ~144-212): once per generation, compares the chunk's
  byte size against `response_bytes_floor` / `response_bytes_ceiling` and rewrites the
  atomic `batch_size`; forwards each `ArrowResponse`; enforces `max_num_*` entity limits.

---

## 3. New design (v2) — overview

- **One HTTP request = one unit of work.** No more paginate-to-end. If the server truncates
  a request before its assigned end, the remainder becomes a *gap* to be picked up later.
- **Earliest-hole-first scheduling.** A single scheduler task owns all state (no locks). The
  next free worker always takes the lowest-start range still needed — which naturally
  prioritises truncation gaps (earlier in block space) over extending the frontier. This is
  also what lets us **start from a deliberately overestimated batch size and work backwards**
  instead of creeping forward in many tiny, conservative ranges: an over-large request that
  the server truncates simply leaves a gap that gets backfilled. Because overshoot is
  self-correcting this way, a hard `max_batch_size` cap is **no longer required** — by default
  the only bound on a request is the hole it sits in (the next chunk's start, or `upper_bound`)
  plus the server's own response-size limit. `max_batch_size` stays available as an *optional*
  cap for callers who want to bound the number of blocks per chunk.
- **Local, per-request size projection.** Each request's block span is projected from the
  byte-density of the nearest already-completed neighbour, aiming at a single configured
  target. The shared atomic `step`, the generation counter, and `BlockRangeIterator` are all
  removed.
- **Contiguity-gated delivery.** A watermark tracks how far contiguous data has been sent;
  a completed chunk is forwarded only once it abuts the watermark.

---

## 4. Block-space model

At any instant the range `[delivered_up_to, to_block)` (forward) is tiled into four kinds of
region:

```
forward stream, blocks increasing →

 delivered_up_to                                   frontier
      │                                               │
      ▼                                               ▼
┌───────────────┬────────┬─────────┬────────┬─────────┬───────────────────┐
│   DELIVERED   │ DONE   │  HOLE   │ IN-     │  DONE   │  HOLE              │
│  (sent to rx) │ chunk  │ (gap)   │ FLIGHT  │  chunk  │ (frontier→to_block)│
└───────────────┴────────┴─────────┴────────┴─────────┴───────────────────┘
                 ▲      ▲
              start  next_block
                     (server truncated here, leaving the gap to its right)
```

- **DELIVERED** — below `delivered_up_to`; already sent to the receiver.
- **DONE chunk** — fetched, awaiting in-order delivery. Held in
  `completed: BTreeMap<start, CompletedChunk>`.
- **IN-FLIGHT** — a single request a worker is currently running.
- **HOLE** — un-fetched, un-assigned. Held in `holes: BTreeMap<start, end>`, where `end` is
  the start of whatever sits immediately above the hole. The single hole that reaches
  `to_block` is the **frontier hole**; any hole below the frontier is a **gap**.

### Why gaps get priority

A gap directly above `delivered_up_to` blocks all delivery until it is filled. Always
scheduling the lowest-start hole means the block the consumer is waiting for next is always
the most-prioritised work, minimising head-of-line blocking.

---

## 5. Data structures

```rust
struct CompletedChunk {
    next_block: u64,       // exclusive end actually covered by the response
    size_bytes: u64,       // HTTP response body size, for density
    resp: ArrowResponse,   // already mapped (hex/decode/column-mapping/reverse)
}

// All owned by the single scheduler task — no locks.
delivered_up_to: u64,                       // watermark (starts at fast-track next_block)
frontier:        u64,                        // highest block assigned-or-completed
upper_bound:     u64,                        // exclusive top: to_block, or live archive height
holes:           BTreeMap<u64 /*start*/, u64 /*end*/>,
completed:       BTreeMap<u64 /*start*/, CompletedChunk>,
in_flight:       JoinSet<FetchResult>,
buffered_bytes:  u64,                        // Σ size_bytes of undelivered `completed` chunks
last_density:    Option<f64>,                // bytes/block of most recent bounded response
// running entity counts for max_num_* limits
```

`FetchResult` carries `{ start, req_end, next_block, size_bytes, resp }` back from a worker.

---

## 6. Scheduler algorithm (concurrency ≥ 2)

```text
seed:
  open_ended  = query.to_block.is_none()
  upper_bound = query.to_block, else archive height (fast-track resp / initial get_height())
  delivered_up_to = frontier = fast_track.next_block
  holes = { frontier : upper_bound }         # one frontier hole
  last_density = None                        # → first wave uses config.batch_size

loop:
  # (a) fill the pipeline
  while in_flight.len() < concurrency:
      H = holes.first()                      # lowest start first → critical path before look-ahead
      if H is None: break
      # consumer backpressure: once the undelivered buffer is full, pause *look-ahead* fetches.
      # The hole at the watermark (H.start == delivered_up_to) is always allowed — it is the
      # data delivery is waiting on, so exempting it serves the consumer AND avoids deadlock.
      if H.start != delivered_up_to and buffered_bytes >= max_buffered_bytes:
          break
      blocks  = project_blocks(anchor_below(H.start))   # §7
      req_end = min(H.start + blocks, H.end) # cap at next chunk's start / upper_bound
      move [H.start, req_end) : holes → in_flight ; shrink/remove H ; bump frontier
      spawn fetch(H.start, req_end)          # ONE get_arrow_with_size, then map_responses

  if in_flight.is_empty() and holes.is_empty():
      break                                  # done (bounded: hit to_block; open-ended: caught up)

  # (b) take one completion
  r = in_flight.join_next().await
  completed.insert(r.start, CompletedChunk { r.next_block, r.size_bytes, r.resp })
  buffered_bytes += r.size_bytes
  if r.next_block < r.req_end:               # truncated → residual gap
      insert/merge hole [r.next_block, r.req_end)
  last_density = r.size_bytes / (r.next_block - r.start)
  update_warning_counter(truncated = r.next_block < r.req_end, size = r.size_bytes)   # §9
  if open_ended and r.archive_height = Some(h) and h > upper_bound:   # chain advanced mid-stream
      upper_bound = h
      extend/open the frontier hole so the top hole reaches upper_bound

  # (c) drain contiguous deliveries
  while let Some(c) = completed.first() where c.start == delivered_up_to:
      c = completed.pop_first()
      buffered_bytes -= c.size_bytes         # leaves the reorder buffer (enters the channel)
      tx.send(Ok(c.resp)).await              # consumer backpressure pauses the whole loop here
      delivered_up_to = c.next_block
      accumulate entity counts
      if any max_num_* exceeded: return      # close the stream
```

- **Consumer backpressure / memory** is bounded by `max_buffered_bytes` (config): the total
  bytes of fetched-but-undelivered chunks in `completed`. Once it is reached the scheduler
  stops launching *look-ahead* fetches — workers idle rather than race ahead of a slow
  consumer — **except** the hole at the watermark, which is always allowed (it is the data the
  consumer is waiting on, and exempting it prevents a deadlock where buffered look-ahead blocks
  the very gap needed to drain it). In-flight fetches stay capped at `concurrency` and the
  output channel keeps its `concurrency * 2` capacity, so resident memory is roughly
  `max_buffered_bytes + (concurrency * 3) * response_size`. This bytes bound replaces v1's
  count-based queue cap, which gave unpredictable memory as response sizes varied.
- **Adjacent holes are merged** on insert: a residual gap `[next_block, req_end)` abuts the
  hole that already starts at `req_end`, so they coalesce to keep `holes` tidy and keep each
  hole's `end` equal to its true upper neighbour.
- **`map_responses`** (hex encoding, log decoding, column mapping, reverse) runs **inside the
  fetch task** (on the rayon pool), so decode work parallelises across workers instead of
  running serially in the consumer.

### `to_block` and the chain head

Two modes, distinguished by whether `query.to_block` is set:

- **Bounded** (`to_block` given): `upper_bound = to_block`, fixed. The run terminates when
  `delivered_up_to == to_block`.
- **Open-ended** (no `to_block`): `upper_bound` tracks the **archive height**. It is seeded
  once at start (the fast-track response's `archive_height`, falling back to an initial
  `get_height()`) and then advanced on **every** response to `max(upper_bound,
  resp.archive_height)`. The frontier hole always extends to the current `upper_bound`, so if
  the chain advances during a long stream the engine keeps going, and it only stops once it
  has genuinely **caught up** — `holes` and `in_flight` both empty with
  `delivered_up_to == upper_bound`, i.e. a response's `next_block` has reached the live
  archive height.

This fixes a stale-snapshot problem in v1, which resolved `to_block` to `get_height()` **once**
up front: a stream that ran for a while could stop short of a head that advanced while it was
running. Following the response-reported `archive_height` means we always finish at the live
tip. (This is a "catch up to the head and stop" sync, not an indefinite live subscription —
once caught up with no work pending, the stream ends.)

Open-ended follow-to-head applies to **forward** streaming; in reverse the top is the start
snapshot, since a reverse stream moves *away* from the head.

---

## 7. Range projection (sizing)

For a hole starting at `h_start`, the **anchor** is the nearest completed chunk *below*
`h_start` (the "previous chunk in the queue"). This is `completed.range(..=h_start).next_back()`,
falling back to `last_density`, and finally to `config.batch_size` when nothing has been
measured yet.

That `batch_size` fallback is what makes the **first wave exactly `batch_size` across
`concurrency` workers**, after which every request sizes itself from real measurements.

```text
target  = config.response_bytes_target          # single knob, e.g. 400_000
bytes   = anchor.size_bytes
blocks  = anchor.next_block - anchor.start
factor  = target / bytes                        # proportional controller
projected = max(round(blocks * factor), min_batch_size)   # no upper clamp by default
if max_batch_size = Some(m): projected = min(projected, m)   # optional hard cap on blocks/chunk
req_end   = min(h_start + projected, hole.end)  # gap → next chunk's from_block;
                                                #  frontier → upper_bound (bounds the overshoot)
```

We deliberately use a **single target** rather than a `[floor, ceiling]` dead-band:

- In v1 the control variable was a *shared* atomic `batch_size`, so a hysteresis band
  avoided thrashing it. In v2 each request sizes itself independently from a fresh local
  density reading, so there is no shared state to thrash and the dead-band's only effect
  would be to let block sizes drift across a wide 2× window while the controller does
  nothing.
- A single target is a plain proportional controller — predictable and self-correcting:
  "the chunk below me was `d` bytes/block; to land near `target` I need `target/d` blocks."
- The target defaults to the midpoint-ish of the old band (`400_000`), leaving headroom so
  most requests *complete their assigned range without server truncation* (rare gaps, smooth
  delivery), at the cost of slightly more requests than aiming at the ceiling would.

If `anchor.size_bytes` is ~0 (e.g. an empty bounded range), `factor` explodes and `projected`
is bounded only by the hole's end (`upper_bound` for the frontier) unless `max_batch_size` is
set — so by default a sparse region is fast-scanned in a single over-large request, and any
server truncation is simply backfilled. This is the same "overestimate and work backwards"
mechanism as §3, which is why a hard maximum block range is not *required* — though
`max_batch_size` can still impose one when a caller wants to bound blocks per chunk.

---

## 8. Special cases

### `concurrency == 0`
`stream_arrow` returns an error before spawning anything
(`bail!("concurrency must be greater than 0")`). The error propagates through the node and
python bindings unchanged.

### `concurrency == 1`
No scheduler, no holes, no projection. A simple sequential loop that **always queries to the
upper block limit**:

```text
cursor = from_block
loop:
    query.from_block = cursor
    query.to_block   = to_block               # the global upper limit
    resp = get_arrow_with_size(query)
    deliver(map(resp))                        # entity-limit checks as in §6(c)
    cursor = resp.next_block
    if cursor >= to_block: break
```

The server self-limits each response's size, so there's no benefit to sub-sizing when there
is a single worker and nothing to pipeline against.

---

## 9. Warnings

A genuinely-helpful diagnostic for the pathological case where **batch-size tuning cannot
help**: the server keeps truncating responses *before* the requested range end while the
responses are *small* — which points at a server execution-time / scan limit rather than a
response-size limit.

- Maintain a counter of **consecutive** completed requests where
  `next_block < req_end` (truncated) **and** `size_bytes < response_bytes_target / 2` (small).
- Any healthy response resets the counter.
- When the counter reaches `WARN_THRESHOLD` (internal constant, default `100`), emit one
  `log::warn!` and suppress further warnings until the counter resets, e.g.:

  The threshold is deliberately high. Because the counter resets on every healthy response,
  normal streams — even broad/compact ones doing thousands of chunks, where the server
  routinely caps a query below target on row-count/time and so returns *truncated-and-small*
  responses — keep their consecutive runs short and never trip it. Only a server that is
  *persistently* capping responses well below target (with essentially no healthy responses to
  break the run) sustains a run this long, which is exactly the case the advice below addresses.
  A smaller threshold fired on healthy compact queries (e.g. all-ERC20-transfers selecting only
  a couple of narrow columns), which was pure noise.

  > hypersync stream: N consecutive responses were truncated before the requested block
  > range end while staying under half of `response_bytes_target` (T bytes). This usually
  > means the server is hitting an execution-time/scan limit rather than a response-size
  > limit, so batch-size tuning won't help — consider narrowing the query (more selective
  > filters) or lowering `max_batch_size`.

Scoped to the projected path (`concurrency >= 2`); the sequential path always queries to the
upper limit, where truncation is expected and normal.

---

## 10. Delivery & ordering

- Each completed work unit is exactly **one `ArrowResponse`** (one HTTP request) covering
  `[start, next_block)`; it is delivered as one stream item, in block order — identical to
  v1's observable behaviour.
- Delivery is gated on contiguity with `delivered_up_to`; `tx` is a bounded mpsc
  (`capacity = concurrency * 2`) and the undelivered reorder buffer is capped by
  `max_buffered_bytes`, so a slow consumer applies backpressure that pauses *look-ahead*
  fetching (see §6) while still allowing the watermark hole to be filled.
- `archive_height` and `rollback_guard` pass through per response unchanged.

---

## 11. Reverse mode

Mirror of the forward model: the watermark moves **downward**, holes are ordered by
descending start, a gap's size cap becomes the **lower** neighbour's `next_block`, and a
completed chunk is delivered when its **upper** edge meets the watermark. The existing
per-response reversal in `map_responses` is retained. As today, the forward fast-track is
skipped in reverse. Implementation order: forward first, then mirror.

---

## 12. Invariants (tested)

For any run, the set of delivered ranges must:

1. **partition** `[from_block, to_block)` — disjoint, no gaps, full coverage;
2. be **contiguous and strictly ordered** as delivered;
3. fetch **every block exactly once**.

To test deterministically without a network, the fetch step is made **injectable** (a
closure / trait) so the scheduler can be driven by a mock that returns configurable
`(next_block, size_bytes)` for any `[start, end)` — enabling simulation of truncation, sparse
vs dense regions, a stalled gap, reverse, `concurrency` 0/1, and the warning trigger. The
existing `tests/api_test.rs` continues to provide real-endpoint parity coverage.

---

## 13. Configuration changes (breaking)

`StreamConfig` collapses the two-field byte band into a single target, makes `max_batch_size`
optional (`None` ⇒ no cap, since overshoot self-corrects — see §3 and §7), and adds
`max_buffered_bytes` (consumer
backpressure — see §6). This is a **breaking change** to the config surface; released as a
deliberate minor (these params are rarely tuned).

```diff
 pub struct StreamConfig {
     ...
     pub batch_size: u64,            // initial (deliberately overestimated) size + fallback
     pub min_batch_size: u64,        // hard lower clamp on projected blocks (avoids tiny ranges)
     pub concurrency: usize,         // 0 => error, 1 => sequential, >=2 => scheduler
-    pub max_batch_size: u64,           // was: default 200_000, always applied
-    pub response_bytes_ceiling: u64,   // default 500_000
-    pub response_bytes_floor: u64,     // default 250_000
+    pub max_batch_size: Option<u64>,   // now optional — None ⇒ no cap on blocks/chunk
+    pub response_bytes_target: u64,    // default 400_000 — projection aims each response here
+    pub max_buffered_bytes: Option<u64>,  // NEW — reorder-buffer byte cap; None ⇒ 2×concurrency×target
     ...
 }
```

Unchanged fields: `column_mapping`, `event_signature`, `hex_output`, `max_num_*`, `reverse`.

| field | role in v2 |
|---|---|
| `response_bytes_target` | projection target; `/2` is the internal warning threshold |
| `max_buffered_bytes` | cap on undelivered reorder-buffer bytes (`None` ⇒ `2 × concurrency × response_bytes_target`); throttles look-ahead under consumer backpressure |
| `min_batch_size` | hard lower clamp on projected block count (avoids tiny ranges) |
| `max_batch_size` | optional hard upper clamp on blocks/chunk (`None` ⇒ no cap; overshoot self-corrects) |
| `batch_size` | initial, deliberately-overestimated size + fallback before any density is measured |
| `concurrency` | `0` errors, `1` sequential, `>=2` scheduler |

`max_buffered_bytes` defaults to `None`, resolved at stream start to `2 × concurrency ×
response_bytes_target` (≈ 8 MB at the default `concurrency = 10`), so look-ahead stays
proportional to the worker count (matching v1's effective queue depth). Set it explicitly to
bound memory more tightly, or higher to allow deeper buffering.

---

## 14. Rollout

1. **Rust core** — rewrite `stream.rs` (the bulk of the work) and the `StreamConfig` field
   change in `config.rs`. Update tests and `tests/api_test.rs`.
2. **Node** (`hypersync-client-node`) and **Python** (`hypersync-client-python`) — thin
   bindings that convert their own `StreamConfig` into the core one. Each needs the same
   mechanical edit: drop `response_bytes_floor` / `response_bytes_ceiling`, make `max_batch_size`
   optional, add `response_bytes_target` and `max_buffered_bytes`, update the conversion
   (`From` / `try_convert`). Then bump the
   `hypersync-client` dependency, rebuild (napi addon / maturin wheel), refresh
   `index.d.ts` / type stubs, and note the change in the changelog. During development the
   binding crates can point at this branch via a path/git dependency; on release they move
   to the published version.
3. **Go** (`hypersync-client-go`) — separate full reimplementation (`stream.go`,
   `stream_handlers.go`); a manual port for parity is a **deferred follow-up**, not part of
   this change.

Surfacing the metrics handle (§15) in node/python is a small fast-follow on top of the config
edits above; the tuning CLI itself is Rust and usable by any caller via a query JSON.

---

## 15. Observability & tuning

The dynamic knobs (`response_bytes_target`, `concurrency`, `max_batch_size`,
`max_buffered_bytes`) are only useful if their effect is measurable. The engine records
per-request metrics and an aggregate summary — used both to pick good library defaults and by
end users to tune their own config.

### Per-request metrics (`RequestStats`)

Recorded as each request completes:

| field | meaning |
|---|---|
| `from_block`, `requested_end`, `next_block` | requested vs actually-covered range |
| `requested_blocks` / `actual_blocks` / `projected_blocks` | sizing intent vs reality (projection pre-clamp) |
| `response_bytes`, `target_bytes`, `size_ratio` | **response size vs target** (`response_bytes / target_bytes`) |
| `bytes_per_block` | observed density |
| `truncated` | `next_block < requested_end` (server stopped early) |
| `kind` | frontier vs gap-fill |
| `duration` | request latency |

### Aggregate summary (`StreamSummary`)

Rolled up across all requests, readable live and at end-of-stream:

- `num_requests`, `num_truncated` → truncation rate
- `total_bytes`, `total_blocks`, `wall_clock` → `blocks/s`, `bytes/s`
- **size-vs-target distribution**: mean `size_ratio`, p50/p90/p99 `response_bytes`, and
  histogram buckets relative to target (`<0.25 / 0.25–0.5 / 0.5–0.75 / 0.75–1.0 / 1.0–1.25 /
  >1.25 ×target`)
- mean/median `bytes_per_block`; block-range size min/mean/max
- `max_buffered_bytes_observed`, mean in-flight (spot buffer / concurrency saturation)
- frontier vs gap-fill counts

These answer the tuning questions directly: are responses landing near `response_bytes_target`?
how often do we truncate? is throughput limited by `concurrency` or by `max_buffered_bytes`?

### API — explicit, opt-in, additive

A **`StreamMetrics`** aggregate handle **plus** a **`StreamObserver`** trait, exposed
**explicitly** — with **no change to the existing `stream` / `stream_arrow` / `stream_events`
signatures** and **without** touching the serializable `StreamConfig`:

- `StreamObserver` (public trait): `on_request(&self, &RequestStats)`,
  `on_progress(&self, in_flight, buffered_bytes)` (per scheduler iteration, default no-op),
  and `on_finish(&self, &StreamSummary)` (default no-op).
- `StreamMetrics` (public): a built-in `StreamObserver` that aggregates into the
  `StreamSummary` above; a cheap cloneable `Arc` handle the caller reads live or after the run.
- A dedicated entry point — `stream_arrow_with_observer(query, config, observer)` —
  carries the observer. Callers who don't want metrics keep using today's methods unchanged,
  with zero overhead. The observer is passed **explicitly** rather than stashed on
  `StreamConfig`, so config stays pure serde data and the existing API is untouched.

**Zero overhead when unused.** The whole metrics path is gated behind the optional observer:
with none attached (the default — today's `stream*` methods), the engine builds no
`RequestStats`, starts no timers, and updates no histograms — it only reuses values it already
computes for scheduling. `RequestStats` is assembled and the hooks fire **only** when an
observer is present, so callers that don't opt in pay nothing.

### Tuning tool (`examples/tune_stream`)

A standalone runnable example: give it a query (JSON) + block range and a grid of configs
(varying `response_bytes_target`, `concurrency`, `max_batch_size`, `max_buffered_bytes`); it
runs each and prints a comparison table of the summary metrics, so you can pick the best
(highest throughput, sizes near target, low truncation). Because it takes a query JSON it is
usable by **any** user regardless of client language; a single-run mode prints one config's
report, and behind a flag it `log::debug!`s one `RequestStats` line per request for ad-hoc
inspection.

### Rollout

Land `StreamMetrics` + `StreamObserver` + `tune_stream` in the **Rust core first** (enough to
choose library defaults). Surfacing the `StreamMetrics` handle in node/python is a
**fast-follow** after the main version bump, not part of the initial binding update.

---

## 16. Notes / future

- The target could later be biased higher (fewer, bigger requests, with more
  truncation/backfill) or exposed as an explicit tuning knob if demand appears.
- `log::trace!` per scheduled range is worth adding for debugging the scheduler.
