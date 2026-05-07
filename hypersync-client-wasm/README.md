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

const res = await client.get_arrow({
    from_block: 19000000,
    to_block:   19000005,
    logs: [{ topics: [["0xddf2...3b3ef"]] }],
    field_selection: { log: ["address", "topic0", "data"] },
});
// res.{blocks, transactions, logs, traces, decoded_logs} are Uint8Array of
// uncompressed Arrow IPC bytes — feed into apache-arrow's tableFromIPC.
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
ENVIO_API_TOKEN=... node query.test.mjs
```

## What's intentionally missing

These are all native-only methods on `hypersync_client::Client` and would
require additional plumbing for wasm:

- `stream`, `stream_arrow`, `stream_events`, `stream_height` — depend on
  `tokio::spawn` / `JoinSet` / SSE.
- `collect`, `collect_arrow`, `collect_events`, `collect_parquet` — depend on
  `stream_arrow` and (for `collect_parquet`) `tokio::fs` + the parquet async
  writer.
- Per-column parallel decoding (rayon) — falls back to serial iteration on
  wasm.

These can be added incrementally if/when there's a clear use case.
