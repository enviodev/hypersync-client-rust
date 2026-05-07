// Benchmark: hypersync-client-wasm vs @envio-dev/hypersync-client (native).
//
// Both clients are pointed at the same hypersync URL with the same query.
// We measure end-to-end wall time, broken down into:
//
//   - cold get():        first call; includes any one-time setup
//   - warm get():        median of N subsequent calls
//   - cold get_arrow():  same, returning Arrow IPC bytes
//   - stream():          total time + chunks for a larger range
//   - load + module init
//
// Bundle size is reported separately at the end.
//
// Run after `wasm-pack build --target nodejs --release` (or --dev):
//   cd tests/js
//   npm install
//   ENVIO_API_TOKEN=... node bench.mjs

import { fileURLToPath } from "node:url";
import { readdirSync, statSync } from "node:fs";
import { join } from "node:path";

try {
    process.loadEnvFile(fileURLToPath(new URL("./.env", import.meta.url)));
} catch (e) {
    if (e.code !== "ENOENT") throw e;
}

const HYPERSYNC_URL = process.env.HYPERSYNC_URL ?? "https://eth.hypersync.xyz";
const TOKEN = process.env.ENVIO_API_TOKEN ?? "";
const ITERATIONS = Number(process.env.BENCH_ITERATIONS ?? 5);

const TRANSFER_TOPIC0 =
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

// Small one-shot query — 5 blocks, just ERC20 transfers.
// The wasm client takes Rust-style snake_case (matching `Query`'s serde
// shape); the native client takes camelCase JS objects.
const smallQueryWasm = {
    from_block: 19000000,
    to_block: 19000005,
    logs: [{ topics: [[TRANSFER_TOPIC0]] }],
    field_selection: {
        log: ["address", "topic0", "topic1", "topic2", "data", "block_number"],
        block: ["number", "hash", "timestamp"],
    },
};

const smallQueryNative = {
    fromBlock: 19000000,
    toBlock: 19000005,
    logs: [{ topics: [[TRANSFER_TOPIC0]] }],
    fieldSelection: {
        log: ["Address", "Topic0", "Topic1", "Topic2", "Data", "BlockNumber"],
        block: ["Number", "Hash", "Timestamp"],
    },
};

// Larger range for the stream benchmark.
const STREAM_BLOCKS = 2000;
const streamQueryWasm = {
    from_block: 19000000,
    to_block: 19000000 + STREAM_BLOCKS,
    logs: [{ topics: [[TRANSFER_TOPIC0]] }],
    field_selection: {
        log: ["address", "topic0", "topic1", "topic2", "data", "block_number"],
        block: ["number"],
    },
};
const streamQueryNative = {
    fromBlock: 19000000,
    toBlock: 19000000 + STREAM_BLOCKS,
    logs: [{ topics: [[TRANSFER_TOPIC0]] }],
    fieldSelection: {
        log: ["Address", "Topic0", "Topic1", "Topic2", "Data", "BlockNumber"],
        block: ["Number"],
    },
};

const streamConfigWasm = { concurrency: 8 };
const streamConfigNative = { concurrency: 8 };

// ---------- timing helpers ----------

function median(xs) {
    if (xs.length === 0) return 0;
    const sorted = [...xs].sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

async function timed(label, fn) {
    const t0 = performance.now();
    const result = await fn();
    const elapsed = performance.now() - t0;
    return { label, elapsed, result };
}

function fmt(ms) {
    return `${ms.toFixed(0).padStart(6)} ms`;
}

// ---------- bench harness ----------

async function benchClient(name, client, getApi, streamApi, smallQuery, streamQuery, streamConfig) {
    console.log(`\n--- ${name} ---`);

    const cold = await timed("cold get", () => getApi(client, smallQuery));
    console.log(`  cold get               ${fmt(cold.elapsed)}`);

    const warm = [];
    for (let i = 0; i < ITERATIONS; i++) {
        const t = await timed("warm get", () => getApi(client, smallQuery));
        warm.push(t.elapsed);
    }
    console.log(`  warm get  (median x${ITERATIONS}) ${fmt(median(warm))}`);
    console.log(`  warm get  (min)        ${fmt(Math.min(...warm))}`);

    const stream = await timed("stream", () => streamApi(client, streamQuery, streamConfig));
    console.log(
        `  stream ${STREAM_BLOCKS} blocks ` +
            `→ ${stream.result.chunks} chunks, ${stream.result.rows} rows in ${fmt(stream.elapsed)}`,
    );

    return {
        coldMs: cold.elapsed,
        warmMedianMs: median(warm),
        warmMinMs: Math.min(...warm),
        streamMs: stream.elapsed,
        streamChunks: stream.result.chunks,
        streamRows: stream.result.rows,
    };
}

// ---------- bundle size ----------

function statOptional(path) {
    try {
        return statSync(path).size;
    } catch (_) {
        return null;
    }
}

function reportBundleSizes() {
    console.log("\n--- bundle size (raw, on-disk) ---");

    const wasmDir = fileURLToPath(new URL("../../pkg/", import.meta.url));
    const wasmBin = statOptional(join(wasmDir, "hypersync_client_wasm_bg.wasm"));
    const wasmShim = statOptional(join(wasmDir, "hypersync_client_wasm.js"));
    if (wasmBin && wasmShim) {
        console.log(
            `  wasm: hypersync_client_wasm_bg.wasm     ${(wasmBin / 1024).toFixed(0).padStart(7)} KiB`,
        );
        console.log(
            `  wasm: hypersync_client_wasm.js (shim)   ${(wasmShim / 1024).toFixed(0).padStart(7)} KiB`,
        );
        console.log(
            `  wasm: total                             ${((wasmBin + wasmShim) / 1024).toFixed(0).padStart(7)} KiB`,
        );
    } else {
        console.log("  wasm: pkg/ not found — run wasm-pack build first");
    }

    // Native: per-platform `.node` binary loaded at runtime. We report just the
    // host's binary (what an end user would actually ship for that platform).
    const nodeModulesEnvio = fileURLToPath(
        new URL("./node_modules/@envio-dev/", import.meta.url),
    );
    try {
        for (const dir of readdirSync(nodeModulesEnvio)) {
            const full = join(nodeModulesEnvio, dir);
            for (const f of readdirSync(full)) {
                if (f.endsWith(".node")) {
                    const sz = statSync(join(full, f)).size;
                    console.log(
                        `  native: ${dir}/${f}`.padEnd(48) +
                            `${(sz / 1024).toFixed(0).padStart(7)} KiB`,
                    );
                }
            }
        }
    } catch (_) {
        console.log("  native: node_modules/@envio-dev/ not found — run npm install first");
    }
}

// ---------- run ----------

const wasmMod = await import("../../pkg/hypersync_client_wasm.js");
const nativeMod = await import("@envio-dev/hypersync-client");

// WASM client adapters.
const wasmGet = (c, q) => c.get(q);
const wasmStream = async (c, q, cfg) => {
    const stream = await c.stream_arrow(q, cfg);
    let chunks = 0;
    let rows = 0;
    for (let chunk; (chunk = await stream.next()); ) {
        chunks++;
        // Cheap row count via a constant per chunk would lie; decode to
        // count for the stream benchmark only. The native client returns
        // already-decoded rows so its number is more honest.
        if (chunk.logs.byteLength > 0) {
            const { tableFromIPC } = await import("apache-arrow");
            const t = tableFromIPC(chunk.logs);
            rows += t.numRows;
        }
    }
    return { chunks, rows };
};

// Native client adapters.
const nativeGet = (c, q) => c.get(q);
const nativeStream = async (c, q, cfg) => {
    const stream = await c.stream(q, cfg);
    let chunks = 0;
    let rows = 0;
    for (let resp; (resp = await stream.recv()); ) {
        chunks++;
        rows += resp.data.logs.length;
    }
    return { chunks, rows };
};

const wasmClient = new wasmMod.Client(HYPERSYNC_URL, TOKEN);
const nativeClient = new nativeMod.HypersyncClient({
    url: HYPERSYNC_URL,
    apiToken: TOKEN,
});

console.log(`bench config:`);
console.log(`  url=${HYPERSYNC_URL}`);
console.log(`  iterations=${ITERATIONS}`);
console.log(
    `  small_query=${smallQueryWasm.from_block}..${smallQueryWasm.to_block}, ` +
        `stream_blocks=${STREAM_BLOCKS}`,
);

const wasmResult = await benchClient(
    "wasm",
    wasmClient,
    wasmGet,
    wasmStream,
    smallQueryWasm,
    streamQueryWasm,
    streamConfigWasm,
);
const nativeResult = await benchClient(
    "native",
    nativeClient,
    nativeGet,
    nativeStream,
    smallQueryNative,
    streamQueryNative,
    streamConfigNative,
);

// Side-by-side
console.log("\n--- summary ---");
const rows = [
    ["metric", "wasm", "native", "wasm/native"],
    ["cold get",    fmt(wasmResult.coldMs),       fmt(nativeResult.coldMs),       (wasmResult.coldMs / nativeResult.coldMs).toFixed(2) + "×"],
    ["warm get (median)", fmt(wasmResult.warmMedianMs), fmt(nativeResult.warmMedianMs), (wasmResult.warmMedianMs / nativeResult.warmMedianMs).toFixed(2) + "×"],
    ["warm get (min)",    fmt(wasmResult.warmMinMs),    fmt(nativeResult.warmMinMs),    (wasmResult.warmMinMs / nativeResult.warmMinMs).toFixed(2) + "×"],
    ["stream total",      fmt(wasmResult.streamMs),     fmt(nativeResult.streamMs),     (wasmResult.streamMs / nativeResult.streamMs).toFixed(2) + "×"],
];
for (const r of rows) {
    console.log(`  ${r[0].padEnd(20)} ${r[1].padStart(10)}  ${r[2].padStart(10)}  ${r[3].padStart(10)}`);
}

reportBundleSizes();
