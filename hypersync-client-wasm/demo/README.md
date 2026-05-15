# browser demo

Static HTML page that loads `hypersync-client-wasm` directly in the browser
— no proxy, no Node, no native bindings. Useful for hackathon demos and
sanity-checking that wasm-bindgen's `--target web` build actually works
end-to-end with `Client` and `Decoder`.

## Build + serve

From the `hypersync-client-wasm/` crate root:

```bash
# 1. Build the wasm package targeting the browser. This populates demo/pkg/.
wasm-pack build --target web --release --out-dir demo/pkg

# 2. Serve the demo over HTTP (any static server works; modules and `fetch`
#    don't work over file://).
cd demo
python3 -m http.server 8080
# or: npx http-server -p 8080
# or: deno serve -p 8080 .

# 3. Open http://localhost:8080
```

Stick a real `ENVIO_API_TOKEN` in the **API token** field if you want the
network-bound buttons (`get_height + get_chain_id`, `get_arrow + decode in JS`,
`get`) to do anything useful. The **decoder bench** button works without
any network — it generates synthetic ERC20 transfer logs in JS, runs
`Decoder.decode_logs` on them N times, and prints median/min/throughput.

## What it shows

- **Connection probe**: minimal round-trip via `get_height` + `get_chain_id`.
- **Query: ERC20 transfers** with two buttons:
  - `get_arrow + decode in JS` — wasm returns Arrow IPC bytes, `apache-arrow`
    decodes in JS. Fastest path for large result sets.
  - `get (decoded simple types)` — wasm decodes into Rust types, marshals
    back as JS objects with `bigint` numbers. Convenient but slower.
- **Event decoder bench** — pure-compute, no network. Good for showing the
  wasm vs JS gap on per-event decode cost.

## Deploying

The `demo/` directory is fully static. Drop `index.html` + the contents of
`demo/pkg/` (after building) on any CDN — Vercel, Netlify, GitHub Pages,
S3 + CloudFront. The client makes CORS requests to `https://*.hypersync.xyz`
which the server already supports.
