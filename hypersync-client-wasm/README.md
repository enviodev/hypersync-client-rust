# hypersync-client-wasm

WASM bindings for hypersync. Thin wasm-bindgen wrapper around
[`hypersync-client`](../hypersync-client) — the same `Client`, the same
`Query`, the same retry / rate-limit / cap'n-proto-cache logic, just exposed
to JavaScript.

```ts
const client = new Client("https://eth.hypersync.xyz", apiToken);
// or: Client.with_config({ url, api_token, max_num_retries: 5, ... })

const height  = await client.get_height();          // bigint
const chainId = await client.get_chain_id();        // bigint

// One-shot Arrow query
const res = await client.get_arrow({
    from_block: 19000000,
    to_block:   19000005,
    logs: [{ topics: [["0xddf2...3b3ef"]] }],
    field_selection: { log: ["address", "topic0", "data"] },
});
// res.{blocks, transactions, logs, traces, decoded_logs} are Uint8Array of
// uncompressed Arrow IPC bytes — feed into apache-arrow's tableFromIPC.

// One-shot decoded query (returns plain JS objects with bigint numbers)
const decoded = await client.get(query);
//  decoded.data.{blocks, transactions, logs, traces}: Array<Array<...>>

// Streamed Arrow (concurrent fetches in the background, ordered chunks out)
const stream = await client.stream_arrow(query /*, optional StreamConfig */);
let chunk;
while ((chunk = await stream.next())) {
    // chunk is an ArrowResponse, same shape as get_arrow's return
}
```

## Architecture

This crate is a thin layer:

- `hypersync-client` itself compiles to `wasm32-unknown-unknown`. Streaming,
  parquet, rayon parallelism, and SSE height-stream are cfg-gated to native
  targets; everything else (retries, payload-too-large halving, rate-limit
  tracking, cap'n proto query caching, alloy-based decoding) compiles for
  both.
- `hypersync-client-wasm` defines wasm-bindgen `#[wasm_bindgen]` exports that
  proxy calls into the inner `hypersync_client::Client` and re-encode the
  resulting `Vec<RecordBatch>` as uncompressed Arrow IPC bytes for the JS
  side. `apache-arrow` (JS) doesn't yet support compressed IPC batches.

## Build

```bash
# Node target
wasm-pack build --target nodejs --release

# Browser/bundler target
wasm-pack build --target bundler --release
```

Output lands in `pkg/`.

### macOS prereq

`zstd-sys` (transitive via `arrow`) needs a clang with wasm32 support. Apple's
clang doesn't include it. Install Homebrew LLVM and point the wasm build at it:

```bash
brew install llvm
export CC_wasm32_unknown_unknown=/opt/homebrew/opt/llvm/bin/clang
export AR_wasm32_unknown_unknown=/opt/homebrew/opt/llvm/bin/llvm-ar
```

## Run the smoke test

```bash
wasm-pack build --target nodejs --release
cd tests/js
npm install
# Token can be set in tests/js/.env (loaded automatically) or as an env var.
ENVIO_API_TOKEN=... node query.test.mjs   # one-shot get_arrow round-trip
ENVIO_API_TOKEN=... node stream.test.mjs  # streaming
```

## Run the benchmark vs `@envio-dev/hypersync-client` (native)

Two benches are provided.

### Compute-only — `decode-bench.mjs` (no network)

Decodes the same in-memory ERC20 Transfer log batch through both clients.
Reports per-batch + per-log latency, JS↔wasm/native boundary cost, and
bundle size.

```bash
wasm-pack build --target nodejs --release
cargo build -p hypersync-client-wasm --target wasm32-unknown-unknown --release
cd tests/js
npm install
node decode-bench.mjs
# tweak BENCH_BATCH=1000 BENCH_ITERS=200 to taste
```

### End-to-end — `bench.mjs` (network)

Runs `get` + `stream` against a live hypersync server through both
clients. Mostly measures network time, but useful as a sanity check.

```bash
ENVIO_API_TOKEN=... npm run bench
```

## Browser demo

`demo/index.html` loads the wasm bindings directly in a browser — no
server, no proxy, no native deps. See [`demo/README.md`](./demo/README.md).

## What's intentionally missing

- `stream_height` — uses `reqwest_eventsource` (SSE) which has no wasm
  support. Native only.
- `collect_parquet` — uses `tokio::fs` and the parquet async writer. Native
  only.
- Per-column parallel decoding (rayon) — falls back to serial iteration on
  wasm. Probably never worth fixing; CPU-bound parsing is rarely the
  bottleneck against network I/O.
