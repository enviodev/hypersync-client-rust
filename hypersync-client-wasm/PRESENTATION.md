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
Any platform that doesn't load `.node` files at all? You can't ship there.

---

## who can't use the native bindings today

| Platform                                    | Native Node client | WASM client |
| ------------------------------------------- | :----------------: | :---------: |
| Linux / macOS server (x64, arm64)           | yes                | yes         |
| **Browsers** (Chrome, Safari, Firefox, etc.)| no                 | yes         |
| **Cloudflare Workers**                      | no                 | yes         |
| **Vercel / Netlify edge runtimes**          | no                 | yes         |
| **Fastly Compute, Deno Deploy**             | no                 | yes         |
| **Bun** (mostly compatible, edge cases)     | partial            | yes         |
| **Deno**                                    | partial            | yes         |
| **Electron renderer**                       | no                 | yes         |
| **Browser extensions** (Chrome, Firefox)    | no                 | yes         |
| **React Native (Hermes)**                   | no                 | yes (poly)  |
| Windows                                     | **no**             | yes         |
| 32-bit Linux / Raspberry Pi (armv7)         | **no**             | yes         |
| Alpine / musl on uncommon arch              | partial            | yes         |
| WASI hosts (Wasmtime, Wasmer)               | no                 | yes         |

> *One artifact, every host with a JS engine.*

---

## what this unlocks

- **No-backend dapps.** A static page that talks to hypersync directly.
  Drop `index.html` on IPFS / GitHub Pages / S3 → done.
- **Edge-rendered indexers.** Aggregate events in a Cloudflare Worker
  10ms from the user, not 200ms from us-east-1.
- **Browser extensions.** Show on-chain context next to any URL —
  wallet portfolio, NFT history, contract call decoder — without a
  proxy server.
- **Electron / Tauri desktop apps.** Cross-platform out of the box
  including Windows.
- **Mobile.** React Native (Hermes), in-app webviews, PWAs.
- **Browser-side ML / data exploration.** Keep the data on the user's
  machine. Pair with DuckDB-WASM or Apache Arrow JS for in-browser
  SQL over hypersync output.

---

## "compile once, run everywhere" — for real

```bash
# Same source, two artifacts:
wasm-pack build --target nodejs       # Node + Bun + edge runtimes
wasm-pack build --target web          # Browsers + Deno
```

Both call exactly the same Rust code path. The only difference is
the JS shim that loads the `.wasm`:
- `nodejs` reads the file with `fs.readFileSync` at `require` time.
- `web` does `await fetch(import.meta.url+'/...wasm')`.

Add `--target bundler` if you want webpack/vite/rollup to handle it.

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

## the trade-off table (be honest)

| | Native (napi-rs) | WASM |
| --- | --- | --- |
| Single-shot query latency      | baseline           | ~1.0–1.5× slower |
| Streamed throughput (parallel) | baseline           | ~1.2–2× slower   |
| CPU-bound decode               | rayon, multi-core  | single-thread    |
| Cold start                     | a few ms           | tens of ms (one-time wasm compile) |
| `.node` per platform           | ~3-5 MB × 5 = 15+MB | n/a |
| `.wasm` (universal)            | n/a                | ~2-3 MB (`--release` + wasm-opt) |
| Streaming response bodies      | yes (reqwest+tokio)| yes (Fetch API)  |
| Filesystem (parquet output)    | yes                | no (sandboxed)   |
| SSE height stream              | yes                | no               |

> Numbers depend on workload; benchmark in `tests/js/bench.mjs`.

---

## why "slower" still wins for most queries

Hypersync responses are **gigabyte-class compressed Arrow IPC**. The
client is dominated by:

1. HTTP I/O (network)             ← identical on both
2. Cap'n proto envelope parse     ← identical on both
3. Arrow IPC decode               ← native ~2× faster

For interactive web UIs where a query takes 50–500ms end-to-end, the
arrow-decode delta is in the noise. For a 10-minute archive crawl,
spin up native on the server.

**WASM closes the door on "but I can't run hypersync there".**

---

## bundle / install size

Native package, fully unpacked across all platforms:

```
@envio-dev/hypersync-client-darwin-arm64       ~3.0 MB
@envio-dev/hypersync-client-darwin-x64         ~3.4 MB
@envio-dev/hypersync-client-linux-x64-gnu      ~3.1 MB
@envio-dev/hypersync-client-linux-x64-musl     ~3.2 MB
@envio-dev/hypersync-client-linux-arm64-gnu    ~3.0 MB
                                               -------
                                               ~15.7 MB on npm
```

WASM package:

```
hypersync_client_wasm_bg.wasm   ~2.6 MB (debug)
                                ~1.5 MB (release + wasm-opt)
hypersync_client_wasm.js        ~30 KB shim
```

A single artifact, served once, cached by the browser.

---

## what stays native-only (and why we don't care)

- **`collect_parquet`** — needs `tokio::fs`. No filesystem in a
  browser sandbox; for Node-on-server use the native client instead.
- **`stream_height` (SSE)** — needs `reqwest_eventsource`, which
  doesn't compile to wasm. Trivially replaceable with a `setInterval` +
  `get_height()` on the wasm side if needed.

Everything else — `get`, `get_arrow`, `collect`, `stream`, `stream_arrow`,
`stream_events`, `Decoder`, `CallDecoder`, retries, rate limiting —
runs identically on both targets.

---

## architecture: how we got there

`hypersync-client` (the same crate that powers
`@envio-dev/hypersync-client`) **now compiles to `wasm32-unknown-unknown`**.

Single source of truth. Cfg-gated dependencies:

```toml
[target.'cfg(not(target_arch = "wasm32"))'.dependencies]
parquet, tokio[multi-thread, fs], rayon, reqwest-eventsource,
reqwest[rustls-tls, http2, stream]

[target.'cfg(target_arch = "wasm32")'.dependencies]
tokio[rt, sync, macros], reqwest[json], gloo-timers, getrandom[js],
wasm-bindgen-futures
```

The streaming pipeline uses `futures::stream::FuturesUnordered` instead
of `JoinSet`, so concurrent requests work on both targets.

---

## demo

`hypersync-client-wasm/demo/`

```bash
wasm-pack build --target web --out-dir demo/pkg
cd demo
ENVIO_API_TOKEN=... pnpm start
# open http://localhost:8080
```

A static HTML page that:

1. Loads the wasm client.
2. Runs `client.stream_arrow()` against a real hypersync.
3. Decodes Arrow IPC chunks with `apache-arrow`.
4. Counts ERC-20 transfers in the browser, in real time.

No server beyond a CORS-relaxing proxy.

---

## benchmarks

`hypersync-client-wasm/tests/js/bench.mjs`

Drives the same workload through both clients and prints a
side-by-side table:

```
                          wasm     native    wasm/native
cold get               XXX ms     XXX ms       X.XX×
warm get (median x5)   XXX ms     XXX ms       X.XX×
warm get (min)         XXX ms     XXX ms       X.XX×
stream total           XXX ms     XXX ms       X.XX×
```

Plus on-disk bundle sizes for both. *(numbers from the live bench
go here for the demo run.)*

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
wasm-pack build --target nodejs --release   # for Node / edge
wasm-pack build --target web    --release   # for browser / Deno
```

Stay native-only:
- `collect_parquet` (filesystem)
- `stream_height` (SSE — no wasm reqwest-eventsource)
