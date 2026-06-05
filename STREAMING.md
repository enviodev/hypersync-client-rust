# Streaming engine design

Status: **proposed** — design agreed, implementation pending.
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
  prioritises truncation gaps (earlier in block space) over extending the frontier.
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
holes:           BTreeMap<u64 /*start*/, u64 /*end*/>,
completed:       BTreeMap<u64 /*start*/, CompletedChunk>,
in_flight:       JoinSet<FetchResult>,
last_density:    Option<f64>,                // bytes/block of most recent bounded response
// running entity counts for max_num_* limits
```

`FetchResult` carries `{ start, req_end, next_block, size_bytes, resp }` back from a worker.

---

## 6. Scheduler algorithm (concurrency ≥ 2)

```text
seed:
  delivered_up_to = frontier = fast_track.next_block
  holes = { frontier : to_block }            # one frontier hole
  last_density = None                        # → first wave uses config.batch_size

loop:
  # (a) fill the pipeline
  while in_flight.len() < concurrency:
      H = holes.first()                      # lowest start = gaps before frontier
      if H is None: break
      if H is the frontier hole
         and in_flight.len() + completed.len() >= prefetch_cap:
          break                              # throttle look-ahead; never blocks gap-fills
      blocks  = project_blocks(anchor_below(H.start))   # §7
      req_end = min(H.start + blocks, H.end) # cap at next chunk's start / to_block
      move [H.start, req_end) : holes → in_flight ; shrink/remove H ; bump frontier
      spawn fetch(H.start, req_end)          # ONE get_arrow_with_size, then map_responses

  if in_flight.is_empty() and holes.is_empty():
      break                                  # done

  # (b) take one completion
  r = in_flight.join_next().await
  completed.insert(r.start, CompletedChunk { r.next_block, r.size_bytes, r.resp })
  if r.next_block < r.req_end:               # truncated → residual gap
      insert/merge hole [r.next_block, r.req_end)
  last_density = r.size_bytes / (r.next_block - r.start)
  update_warning_counter(truncated = r.next_block < r.req_end, size = r.size_bytes)   # §9

  # (c) drain contiguous deliveries
  while let Some(c) = completed.first() where c.start == delivered_up_to:
      completed.pop_first()
      tx.send(Ok(c.resp)).await              # backpressure here pauses the whole loop
      delivered_up_to = c.next_block
      accumulate entity counts
      if any max_num_* exceeded: return      # close the stream
```

- `prefetch_cap = concurrency * 2` (mirrors v1's queue bound). A single slow gap can never
  cause unbounded look-ahead, because frontier extension is throttled while gap-fills (lowest
  holes) are always allowed.
- **Adjacent holes are merged** on insert: a residual gap `[next_block, req_end)` abuts the
  hole that already starts at `req_end`, so they coalesce to keep `holes` tidy and keep each
  hole's `end` equal to its true upper neighbour.
- **`map_responses`** (hex encoding, log decoding, column mapping, reverse) runs **inside the
  fetch task** (on the rayon pool), so decode work parallelises across workers instead of
  running serially in the consumer.

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
projected = clamp(round(blocks * factor), min_batch_size, max_batch_size)
req_end   = min(h_start + projected, hole.end)  # gap → next chunk's from_block;
                                                #  frontier → to_block
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

If `anchor.size_bytes` is ~0 (e.g. an empty bounded range), `factor` explodes and
`projected` clamps to `max_batch_size` — sparse regions are fast-scanned automatically.

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
- When the counter reaches `WARN_THRESHOLD` (internal constant, default `5`), emit one
  `log::warn!` and suppress further warnings until the counter resets, e.g.:

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
  (`capacity = concurrency * 2`), so a slow consumer applies backpressure that naturally
  pauses scheduling.
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

`StreamConfig` collapses the two-field byte band into one target. This is a **breaking
change** to the config surface; released as a deliberate minor (these params are rarely
tuned).

```diff
 pub struct StreamConfig {
     ...
     pub batch_size: u64,            // first-wave size + fallback until density is known
     pub max_batch_size: u64,        // hard clamp on projected blocks
     pub min_batch_size: u64,        // hard clamp on projected blocks
     pub concurrency: usize,         // 0 => error, 1 => sequential, >=2 => scheduler
-    pub response_bytes_ceiling: u64,   // default 500_000
-    pub response_bytes_floor: u64,     // default 250_000
+    pub response_bytes_target: u64,    // default 400_000 — projection aims each response here
     ...
 }
```

Unchanged fields: `column_mapping`, `event_signature`, `hex_output`, `max_num_*`, `reverse`.

| field | role in v2 |
|---|---|
| `response_bytes_target` | projection target; `/2` is the internal warning threshold |
| `min_batch_size` / `max_batch_size` | hard clamps on projected block count |
| `batch_size` | first-wave size + fallback before any density is measured |
| `concurrency` | `0` errors, `1` sequential, `>=2` scheduler |

---

## 14. Rollout

1. **Rust core** — rewrite `stream.rs` (the bulk of the work) and the `StreamConfig` field
   change in `config.rs`. Update tests and `tests/api_test.rs`.
2. **Node** (`hypersync-client-node`) and **Python** (`hypersync-client-python`) — thin
   bindings that convert their own `StreamConfig` into the core one. Each needs the same
   mechanical edit: drop `response_bytes_floor` / `response_bytes_ceiling`, add
   `response_bytes_target`, update the conversion (`From` / `try_convert`). Then bump the
   `hypersync-client` dependency, rebuild (napi addon / maturin wheel), refresh
   `index.d.ts` / type stubs, and note the change in the changelog. During development the
   binding crates can point at this branch via a path/git dependency; on release they move
   to the published version.
3. **Go** (`hypersync-client-go`) — separate full reimplementation (`stream.go`,
   `stream_handlers.go`); a manual port for parity is a **deferred follow-up**, not part of
   this change.

---

## 15. Notes / future

- The target could later be biased toward `response_bytes_ceiling`-style throughput (fewer,
  bigger requests) or exposed as an explicit tuning knob if demand appears.
- `log::trace!` per scheduled range is worth adding for debugging the scheduler.
