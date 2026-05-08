---
marp: true
theme: default
paginate: true
---

<!-- _class: lead -->

# hypersync, anywhere

A single client. Every JS-capable platform.

`hypersync-client-wasm`

---

## the problem

`@envio-dev/hypersync-client` ships **native (napi-rs) binaries**.

That means **5 prebuilt `.node` files** per release:

- `aarch64-apple-darwin`
- `aarch64-unknown-linux-gnu`
- `x86_64-apple-darwin`
- `x86_64-unknown-linux-gnu`
- `x86_64-unknown-linux-musl`

Anything else? `npm install` fails.
Any platform that doesn't load `.node` files at all (browsers, edge
runtimes that ban native modules, mobile JS engines)? You can't ship
there.

---

## who can't use the native bindings today

| Platform                                    | Native (napi-rs)   | WASM        |
| ------------------------------------------- | :----------------: | :---------: |
| Linux / macOS server (x64, arm64)           | ✅                 | ✅          |
| Node                                        | ✅                 | ✅          |
| Bun (local)                                 | ✅                 | ✅          |
| Deno (local, ≥1.31)                         | ✅ *(caveats)*     | ✅          |
| **Browsers** (Chrome, Safari, Firefox)      | ❌                 | ✅          |
| **Cloudflare Workers**                      | ❌                 | ✅          |
| **Vercel / Netlify edge runtimes**          | ❌                 | ✅          |
| **Fastly Compute**                          | ❌                 | ✅          |
| **Deno Deploy**                             | ❌                 | ✅          |
| **Electron renderer process**               | ❌                 | ✅          |
| **Browser extensions** (Chrome, Firefox)    | ❌                 | ✅          |
| **React Native (Hermes)**                   | ❌                 | ✅          |
| Windows                                     | ❌                 | ✅          |
| 32-bit Linux / Raspberry Pi (armv7)         | ❌                 | ✅          |
| Alpine / musl on uncommon arch              | partial            | ✅          |
| WASI hosts (Wasmtime, Wasmer)               | ❌                 | ✅          |

> Deno and Bun load `.node` files via Node-API, so the existing native package
> already works there. WASM's win is the rows below: **browser/edge runtimes,
> Electron, mobile, Windows, and uncommon architectures.**

---

## what this unlocks

The native package already covers servers, Node, Bun, and Deno
(local). The interesting frontier is the rest of the JS world:

- **No-backend dapps.** A static page that talks to hypersync directly.
  Drop `index.html` on IPFS / GitHub Pages / S3 → done.
- **Edge-rendered indexers.** Aggregate events in a Cloudflare Worker
  ~10ms from the user, not 200ms from us-east-1. Same on Vercel /
  Netlify / Fastly / Deno Deploy.
- **Browser extensions.** Show on-chain context next to any URL —
  wallet portfolio, NFT history, contract call decoder — without a
  proxy server.
- **Electron / Tauri desktop apps.** Cross-platform out of the box,
  including Windows where napi-rs has no prebuilt today.
- **Mobile.** React Native (Hermes), in-app webviews, PWAs.
- **Browser-side data exploration.** Keep the data on the user's
  machine. Pair with DuckDB-WASM or Apache Arrow JS for in-browser
  SQL over hypersync output.

---

## "compile once, run everywhere" — for real

```bash
# Same source, three artifacts (one .wasm, three JS shims):
wasm-pack build --target web          # browsers, Deno Deploy, Workers
wasm-pack build --target nodejs       # Node CommonJS
wasm-pack build --target bundler      # webpack / vite / rollup / esbuild
```

Each shim loads the `.wasm` differently:
- `nodejs` reads it with `fs.readFileSync` at `require` time.
- `web` does `await init()` → `fetch` + `WebAssembly.instantiate`.
- `bundler` lets the bundler emit the asset and instantiate it natively.

Ship them under one npm package via `package.json` `"exports"` conditions
(`"browser"`, `"node"`, `"default"`) — consumers `npm install` and import,
no Rust toolchain required.

---

## the wasm client mirrors the native API

```js
// Node — looks like the native client
import { Client } from "@envio-dev/hypersync-client-wasm";
const client = new Client(url, token);
const res    = await client.get(query);
const arrow  = await client.get_arrow(query);
const stream = await client.stream_arrow(query, { concurrency: 8 });
```

```js
// Browser — same code, browser-loaded wasm
import init, { Client } from "./pkg/hypersync_client_wasm.js";
await init();
const client = new Client(url, token);
const res    = await client.get(query);
```

Same `Query`. Same `ArrowResponse`. Same retry / payload-too-large /
rate-limit handling. The wasm crate is a **thin wasm-bindgen wrapper**
over `hypersync_client::Client` — not a reimplementation.

---

## the trade-off table

| | Native (napi-rs) | WASM | wasm/native |
| --- | --- | --- | :---: |
| Decode 1000 Transfer logs     | 3.32 ms            | 4.39 ms          | 1.32× |
| Decode boundary (single log)  | 0.004 ms           | 0.005 ms         | 1.27× |
| Decoder construction          | 0.026 ms           | 0.042 ms         | 1.62× |
| `.node` per platform          | 14.7–18.0 MB       | n/a              |       |
| Total native package          | **~83 MB** across 5 platforms | n/a   |       |
| `.wasm` (universal)           | n/a                | **6.8 MB** raw / ~2 MB gzipped | |
| Concurrent in-flight requests | yes (`JoinSet`)    | yes (`FuturesUnordered`) |   |

Streaming is concurrent on both targets — same `concurrency` knob,
multiple requests in flight at once. Network I/O dominates real-world
queries and is identical: `reqwest` + h2 on native, browser/runtime
`fetch` on wasm.

---

## bundle / install size

Native package, per-platform `.node` binaries:

```
@envio-dev/hypersync-client-darwin-arm64       14.7 MB
@envio-dev/hypersync-client-darwin-x64         16.5 MB
@envio-dev/hypersync-client-linux-arm64-gnu    15.7 MB
@envio-dev/hypersync-client-linux-x64-gnu      18.0 MB
@envio-dev/hypersync-client-linux-x64-musl     18.0 MB
                                               --------
                                               ~83 MB on npm (all platforms)
```

A user only downloads their host's binary at install — but CI pipelines
that build on multiple OSes (or Docker images that target multiple arches)
end up pulling several. Lambda layers, Docker images, and CDN caches all
multiply.

WASM package:

```
hypersync_client_wasm_bg.wasm   6.8 MB  (release + wasm-opt, raw)
                                ~2 MB   (gzip on the wire)
hypersync_client_wasm.js          44 KB shim
                                -------
                                6.84 MB total
```

**Smaller than any single `.node` binary.** A universal artifact,
served once, cached by the browser/runtime.

Further trimming is straightforward via Cargo features (drop the
decoder, capnp, retry machinery for a "lite" build) if a target really
cares about sub-MB budgets.

---

## demo

`hypersync-client-wasm/demo/` — a static HTML page that:

1. Loads the wasm client (~50 KB JS shim + 2 MB gzipped wasm).
2. Probes chain height + chain id.
3. Runs a one-shot `get_arrow` and decodes via `apache-arrow`.
4. **Live ERC-20 balance:** input a token + wallet, stream every
   `Transfer` against them, show running balance, last 10 transfers
   with timestamps, % progress through the chain, blocks/s.
5. Decoder microbenchmark.

```bash
wasm-pack build hypersync-client-wasm --target web \
    --out-dir demo/pkg --release
cd hypersync-client-wasm/demo
ERC20_ADDRESS=0x... WALLET_ADDRESS=0x... ENVIO_API_TOKEN=... pnpm start
# open http://localhost:8080
```

No server beyond a CORS-bypass proxy (`server.mjs`).

---

## pluggability — write your own bindings

The demo's live-balance panel doesn't decode Arrow in JS. It hands each
streamed chunk to a custom Rust struct, `BalanceTracker`, that walks
the `RecordBatch`es with `LogReader::iter` / `BlockReader::iter` (the
zero-copy readers in `hypersync_client::arrow_reader`) and returns a
single `BatchDelta` JsValue:

```rust
#[wasm_bindgen]
pub struct BalanceTracker { /* state: U256 totals, ledger ring, ... */ }

#[wasm_bindgen]
impl BalanceTracker {
    #[wasm_bindgen(constructor)]
    pub fn new(wallet_hex: &str, ledger_capacity: usize) -> Result<Self, JsError>;

    pub fn process(&mut self, response: &ArrowResponse) -> Result<JsValue, JsError>;
}
```

```js
const tracker = new BalanceTracker(wallet, 10);
const stream  = await client.stream_arrow(query);
while ((chunk = await stream.next())) {
    const delta = tracker.process(chunk);   // one boundary cross per chunk
    render(delta);                          // balance, ledger, last block, ts
}
```

**One wasm-bindgen call per chunk**, not per row. All the heavy work
(column lookups, address compare, U256 arithmetic, ledger ring) runs
in Rust. JS never sees Arrow.

This is the recommended pattern for hot loops. Anyone publishing a
dapp can fork ~330 lines of `balance.rs`, swap topic0 + decode logic,
and ship a custom-shaped `#[wasm_bindgen]` accumulator that crosses
the boundary exactly when it has something useful to say.

---

## benchmarks

`tests/js/decode-bench.mjs` — synthetic batch, no network:

```
decode 1000 Transfer logs       wasm 4.39 ms    native 3.32 ms    1.32×
decode 1 log (boundary cost)    wasm 5.0 µs     native 4.0 µs     1.27×
Decoder.from_signatures         wasm 0.042 ms   native 0.026 ms   1.62×
```

Per-log cost is flat across batch sizes (4.5–5 µs/log wasm,
3.3 µs/log native), so the gap is **not** wasm↔JS call overhead.
Most of the time is spent building the per-row JS output objects
(`{indexed: [{val:…}], body: [{val:…}]}` + `BigInt`s) — the actual
Rust decode work is microseconds per *batch*, not per log.

For real throughput, push the loop into Rust like `BalanceTracker`:
one boundary cross per chunk, no per-row JS allocations.

---

<!-- _class: lead -->

## takeaway

Native client when you control the host.
**WASM client when you don't.**

Same Rust. Same API. Hypersync, everywhere.

---

## links / appendix

- Branch: `claude/hypersync-wasm-investigation-WHlK8`
- Crate: `hypersync-client-wasm/`
- Demo: `hypersync-client-wasm/demo/`
- Tests: `hypersync-client-wasm/tests/js/`
- Bench: `hypersync-client-wasm/tests/js/bench.mjs`

Build:
```
wasm-pack build --target web     --release  # browsers / Workers / Deno Deploy
wasm-pack build --target nodejs  --release  # Node CommonJS
wasm-pack build --target bundler --release  # webpack / vite / rollup
```
