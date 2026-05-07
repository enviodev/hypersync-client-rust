// Compute-only benchmark: wasm Decoder vs @envio-dev/hypersync-client
// Decoder. No network — both clients decode the same in-memory log batch
// repeatedly, so we measure CPU + JS↔native/wasm marshalling cost.
//
// Run after `wasm-pack build --target nodejs --release` (or --dev):
//   cd tests/js && npm install
//   node decode-bench.mjs
//
// Knobs:
//   BENCH_BATCH=1000   logs per call (default 1000)
//   BENCH_ITERS=200    number of decode_logs calls (default 200)

import { fileURLToPath } from "node:url";
import { readdirSync, statSync } from "node:fs";
import { join } from "node:path";
import { Decoder as WasmDecoder } from "../../pkg/hypersync_client_wasm.js";
import { Decoder as NativeDecoder } from "@envio-dev/hypersync-client";

const BATCH = Number(process.env.BENCH_BATCH ?? 1000);
const ITERS = Number(process.env.BENCH_ITERS ?? 200);
const SIGNATURES = [
    "Transfer(address indexed from, address indexed to, uint256 amount)",
];
const TRANSFER_TOPIC0 =
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

// Build a synthetic ERC-20 Transfer log batch in memory. Topic1/2 are 32-byte
// padded addresses, data is a 32-byte uint256 amount.
function makeBatch(n) {
    const logs = [];
    for (let i = 0; i < n; i++) {
        const fromHex = "0".repeat(24) + (0xa1b2c3d4_e5f60718n + BigInt(i))
            .toString(16)
            .padStart(40, "0");
        const toHex = "0".repeat(24) + (0xb1c2d3e4_f5061829n + BigInt(i))
            .toString(16)
            .padStart(40, "0");
        const amount = (1_000_000_000_000_000_000n + BigInt(i))
            .toString(16)
            .padStart(64, "0");
        logs.push({
            topics: [TRANSFER_TOPIC0, "0x" + fromHex, "0x" + toHex],
            data: "0x" + amount,
        });
    }
    return logs;
}

const batch = makeBatch(BATCH);

// ---------- timing helpers ----------
function median(xs) {
    const s = [...xs].sort((a, b) => a - b);
    const m = Math.floor(s.length / 2);
    return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}
function fmt(ms) {
    return `${ms.toFixed(3).padStart(9)} ms`;
}

// ---------- benches ----------

async function benchSetup(name, mkDecoder) {
    const t0 = performance.now();
    for (let i = 0; i < 100; i++) mkDecoder();
    const elapsed = (performance.now() - t0) / 100;
    console.log(`  ${name.padEnd(28)} ${fmt(elapsed)}  (per Decoder.from_signatures)`);
}

async function benchDecode(name, decoder, decodeFn) {
    // warm up
    for (let i = 0; i < 5; i++) await decodeFn(decoder, batch);

    const samples = [];
    for (let i = 0; i < ITERS; i++) {
        const t0 = performance.now();
        const out = await decodeFn(decoder, batch);
        samples.push(performance.now() - t0);
        // Cheap sanity: every entry decoded.
        if (i === 0) {
            if (!Array.isArray(out) || out.length !== batch.length) {
                throw new Error(`${name}: decode returned ${out?.length} != ${batch.length}`);
            }
        }
    }
    const med = median(samples);
    const min = Math.min(...samples);
    const total = samples.reduce((a, b) => a + b, 0);
    const perLog = (med / batch.length) * 1000; // µs per log
    console.log(
        `  ${name.padEnd(28)} ${fmt(med)}  ${fmt(min)} (min)  ` +
            `${(perLog).toFixed(2)} µs/log  ` +
            `${(batch.length / med * 1000).toFixed(0)} logs/s`,
    );
    return { med, min, total, perLog };
}

// Single-log call: amplifies JS↔wasm/native boundary cost.
async function benchSingleLog(name, decoder, decodeFn) {
    const single = [batch[0]];
    for (let i = 0; i < 5; i++) await decodeFn(decoder, single); // warm

    const samples = [];
    for (let i = 0; i < ITERS * 5; i++) {
        const t0 = performance.now();
        await decodeFn(decoder, single);
        samples.push(performance.now() - t0);
    }
    const med = median(samples);
    console.log(`  ${name.padEnd(28)} ${fmt(med)}  (median, 1 log per call — boundary cost)`);
    return { med };
}

console.log(`bench config: BATCH=${BATCH} logs, ITERS=${ITERS} calls`);

console.log("\n--- Decoder construction ---");
await benchSetup("wasm Decoder.from_signatures", () =>
    WasmDecoder.from_signatures(SIGNATURES),
);
await benchSetup("native Decoder.fromSignatures", () =>
    NativeDecoder.fromSignatures(SIGNATURES),
);

console.log(`\n--- decode_logs (batch=${BATCH}) ---`);
const wasmDec = WasmDecoder.from_signatures(SIGNATURES);
const wasmRes = await benchDecode("wasm.decode_logs", wasmDec, (d, b) => d.decode_logs(b));

const nativeDec = NativeDecoder.fromSignatures(SIGNATURES);
const nativeAsync = await benchDecode("native.decodeLogs (async)", nativeDec, (d, b) =>
    d.decodeLogs(b),
);
const nativeSync = await benchDecode("native.decodeLogsSync", nativeDec, (d, b) =>
    Promise.resolve(d.decodeLogsSync(b)),
);

console.log("\n--- single-log latency (boundary cost) ---");
const wasmSingle = await benchSingleLog("wasm.decode_logs([1])", wasmDec, (d, b) =>
    d.decode_logs(b),
);
const nativeSingle = await benchSingleLog("native.decodeLogsSync([1])", nativeDec, (d, b) =>
    Promise.resolve(d.decodeLogsSync(b)),
);

console.log("\n--- summary ---");
const ratio = (a, b) => (a / b).toFixed(2) + "×";
console.log(
    `  batch  decode: wasm ${fmt(wasmRes.med)}  vs native(sync) ${fmt(nativeSync.med)}  → ${ratio(wasmRes.med, nativeSync.med)}`,
);
console.log(
    `  single decode: wasm ${fmt(wasmSingle.med)}  vs native(sync) ${fmt(nativeSingle.med)}  → ${ratio(wasmSingle.med, nativeSingle.med)}`,
);

// ---------- bundle sizes ----------
function statOptional(path) {
    try {
        return statSync(path).size;
    } catch (_) {
        return null;
    }
}
function kib(bytes) {
    return `${(bytes / 1024).toFixed(0).padStart(7)} KiB`;
}

console.log("\n--- bundle size (raw, on-disk) ---");
const repoRoot = fileURLToPath(new URL("../../../", import.meta.url));

// `pkg/` is whatever wasm-pack last built. If that was `--release` it has
// already been wasm-opt'd (this is the number a user would actually ship).
// `target/wasm32-unknown-unknown/release/` is what `cargo --release`
// produces without wasm-opt — useful as a "before optimization" baseline.
const wasmPkg = statOptional(join(repoRoot, "hypersync-client-wasm/pkg/hypersync_client_wasm_bg.wasm"));
const wasmPkgShim = statOptional(join(repoRoot, "hypersync-client-wasm/pkg/hypersync_client_wasm.js"));
const wasmCargoRelease = statOptional(join(repoRoot, "target/wasm32-unknown-unknown/release/hypersync_client_wasm.wasm"));

if (wasmPkg) console.log(`  wasm pkg/ .wasm                                       ${kib(wasmPkg)}  (wasm-pack output; --release adds wasm-opt)`);
if (wasmPkgShim) console.log(`  wasm pkg/ .js shim                                    ${kib(wasmPkgShim)}`);
if (wasmPkg && wasmPkgShim) console.log(`  wasm pkg/ total                                       ${kib(wasmPkg + wasmPkgShim)}`);
if (wasmCargoRelease) console.log(`  wasm cargo --release .wasm (no wasm-opt)              ${kib(wasmCargoRelease)}`);

const nativeRoot = fileURLToPath(
    new URL("./node_modules/@envio-dev/", import.meta.url),
);
try {
    for (const dir of readdirSync(nativeRoot)) {
        const full = join(nativeRoot, dir);
        for (const f of readdirSync(full)) {
            if (f.endsWith(".node")) {
                const sz = statSync(join(full, f)).size;
                console.log(`  native ${dir}/${f}`.padEnd(56) + kib(sz));
            }
        }
    }
} catch (_) {
    console.log("  native: node_modules/@envio-dev/ not found — run npm install first");
}
