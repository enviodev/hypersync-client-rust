# hypersync-client-wasm

Minimal WASM bindings for hypersync. **Experimental — `query` only, no streaming, no retries, no rate-limit handling.**

The goal of this first iteration is just to prove that hypersync's query path
can be driven from a browser / Node.js wasm runtime. It exposes one async call:

```ts
new Client(url: string, apiToken: string)
client.get_arrow(query: Query): Promise<ArrowResponse>
```

Where `ArrowResponse` exposes `blocks` / `transactions` / `logs` / `traces` as
`Uint8Array`s of Arrow IPC bytes — feed them into `apache-arrow`'s `tableFromIPC`.

## Build

```bash
# Node target
wasm-pack build --target nodejs --release

# Browser/bundler target
wasm-pack build --target bundler --release
```

Output lands in `pkg/`.

## Run the smoke test

```bash
wasm-pack build --target nodejs --release
cd tests/js
npm install
ENVIO_API_TOKEN=... node query.test.mjs
```

## What's intentionally missing

- HTTP retries / payload-too-large halving
- Rate limit awareness (`RateLimitInfo`)
- Cap'n Proto request encoding (uses JSON path only)
- `stream`, `stream_arrow`, `collect`, `collect_parquet`
- Decoded logs (no alloy ABI decoding in wasm yet)
- `health_check`, `get_height`, `get_chain_id`

These can be added incrementally as the wasm story matures.
