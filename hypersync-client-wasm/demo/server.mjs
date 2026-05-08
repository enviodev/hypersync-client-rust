// Static file server + CORS-bypass proxy for the wasm demo.
//
// The wasm client fetches /proxy/* on this origin; we forward each request
// to the upstream hypersync URL. This sidesteps the fact that hypersync
// (especially local builds) doesn't return CORS headers for cross-origin
// browser requests.
//
// Config (via env or .env next to this file, or CLI args):
//   HYPERSYNC_URL     upstream (default: https://eth.hypersync.xyz)
//   ENVIO_API_TOKEN   API token (UUID), optional
//   ERC20_ADDRESS     prefill for the live-balance demo (optional)
//   WALLET_ADDRESS    prefill for the live-balance demo (optional)
//   PORT              local port (default: 8080)
//
// Run:
//   pnpm install
//   pnpm start
//   HYPERSYNC_URL=http://localhost:2104 pnpm start
//   pnpm start http://localhost:2104        # url as positional arg
//
// /config.json hands the page a URL like "http://localhost:8080/proxy" plus
// the token, so the demo "just works" with no copy/paste each reload.
//
// Don't run on a public network — /config.json leaks the API token to anyone
// who can reach this server.

import express from "express";
import { fileURLToPath } from "node:url";
import { dirname } from "node:path";

const here = dirname(fileURLToPath(import.meta.url));

try {
    process.loadEnvFile(`${here}/.env`);
} catch (e) {
    if (e.code !== "ENOENT") throw e;
}

const port = Number(process.env.PORT ?? 8080);
const upstream = (process.argv[2] ?? process.env.HYPERSYNC_URL ?? "https://eth.hypersync.xyz")
    .replace(/\/+$/, "");
const apiToken = process.env.ENVIO_API_TOKEN ?? "";
const erc20Address = process.env.ERC20_ADDRESS ?? "";
const walletAddress = process.env.WALLET_ADDRESS ?? "";

const app = express();

express.static.mime.define({ "application/wasm": ["wasm"] });

// Read the request body manually rather than via express.raw — `raw` only
// populates req.body when Content-Type matches its `type` filter, and the
// wasm client sometimes posts capnp bytes with no Content-Type at all,
// leaving req.body undefined and the upstream returning 400.
async function readBody(req) {
    if (["GET", "HEAD"].includes(req.method)) return undefined;
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    return Buffer.concat(chunks);
}

app.all("/proxy/*", async (req, res) => {
    // /proxy/foo/bar?x=1  →  ${upstream}/foo/bar?x=1
    const tail = req.originalUrl.replace(/^\/proxy/, "") || "/";
    const target = `${upstream}${tail}`;

    const headers = {};
    for (const [k, v] of Object.entries(req.headers)) {
        const lk = k.toLowerCase();
        // Strip hop-by-hop, origin-related and length headers; fetch sets its own.
        if (["host", "connection", "content-length", "origin", "referer", "accept-encoding"].includes(lk)) continue;
        headers[k] = v;
    }

    let body;
    try {
        body = await readBody(req);
    } catch (e) {
        return res.status(400).send(`failed to read request body: ${e.message}`);
    }

    console.log(
        `[proxy] ${req.method} ${target} ` +
        `body=${body ? body.byteLength : 0}B ` +
        `ct=${req.headers["content-type"] ?? "(none)"}`
    );

    try {
        const r = await fetch(target, {
            method: req.method,
            headers,
            body,
        });
        res.status(r.status);
        r.headers.forEach((v, k) => {
            const lk = k.toLowerCase();
            if (["transfer-encoding", "content-encoding", "content-length"].includes(lk)) return;
            res.setHeader(k, v);
        });
        const buf = Buffer.from(await r.arrayBuffer());
        res.send(buf);
    } catch (e) {
        console.error(`[proxy] ${req.method} ${target} → ${e.message}`);
        res.status(502).send(`proxy error: ${e.message}`);
    }
});

app.get("/config.json", (req, res) => {
    // Compute the proxy URL relative to the request's host so the demo works
    // when accessed via 127.0.0.1, the LAN IP, etc.
    const proto = req.protocol;
    const host = req.headers.host ?? `localhost:${port}`;
    res.json({
        hypersyncUrl: `${proto}://${host}/proxy`,
        apiToken,
        erc20Address,
        walletAddress,
    });
});

app.use(express.static(here, { extensions: ["html"] }));

app.listen(port, () => {
    console.log(`demo  → http://localhost:${port}`);
    console.log(`proxy → ${upstream}/* (via /proxy/*)`);
    console.log(`token → ${apiToken ? `set (${apiToken.length} chars)` : "(none)"}`);
});
