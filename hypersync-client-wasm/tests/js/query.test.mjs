// End-to-end smoke test for hypersync-client-wasm.
//
// Prereqs:
//   1. Build the wasm package:
//        cd hypersync-client-wasm
//        wasm-pack build --target nodejs --release
//   2. Install JS deps:
//        cd tests/js && npm install
//   3. Optionally export ENVIO_API_TOKEN. The test works without one for the
//      tiny block range below, but a token avoids public-tier throttling.
//
// Run:
//   node tests/js/query.test.mjs
//
// Verifies:
//   - wasm Client constructs
//   - get_arrow() returns an ArrowResponse with sane scalar fields
//   - Returned IPC bytes decode with apache-arrow into RecordBatches
//   - At least one log row is returned for a known-active block range

import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import { tableFromIPC } from "apache-arrow";
import { Client } from "../../pkg/hypersync_client_wasm.js";

// Load .env next to this file if present (Node >=20.6).
try {
    process.loadEnvFile(fileURLToPath(new URL("./.env", import.meta.url)));
} catch (e) {
    if (e.code !== "ENOENT") throw e;
}

const HYPERSYNC_URL = process.env.HYPERSYNC_URL ?? "https://eth.hypersync.xyz";
const TOKEN = process.env.ENVIO_API_TOKEN ?? "";

// Small range with known ERC20 transfer activity.
const TRANSFER_TOPIC0 =
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

const query = {
    from_block: 19000000,
    to_block: 19000005,
    logs: [
        {
            topics: [[TRANSFER_TOPIC0]],
        },
    ],
    field_selection: {
        log: ["address", "topic0", "topic1", "topic2", "data", "block_number"],
        block: ["number", "hash", "timestamp"],
    },
};

console.log(`POST ${HYPERSYNC_URL}/query/arrow-ipc  blocks=${query.from_block}..${query.to_block}`);

const client = new Client(HYPERSYNC_URL, TOKEN);
const t0 = performance.now();
const res = await client.get_arrow(query);
const elapsed = (performance.now() - t0).toFixed(0);
console.log(`get_arrow returned in ${elapsed}ms`);

// Scalar fields
console.log({
    archive_height: res.archive_height,
    next_block: res.next_block,
    total_execution_time: res.total_execution_time,
    blocks_bytes: res.blocks.byteLength,
    transactions_bytes: res.transactions.byteLength,
    logs_bytes: res.logs.byteLength,
    traces_bytes: res.traces.byteLength,
});

assert.equal(typeof res.next_block, "bigint", "next_block should be bigint");
assert.ok(res.next_block >= BigInt(query.from_block), "next_block advanced");
assert.ok(res.logs.byteLength > 0, "logs IPC payload non-empty");

// Decode logs IPC into a table
const logsTable = tableFromIPC(res.logs);
console.log(`logs: ${logsTable.numRows} rows, schema: ${logsTable.schema.fields.map((f) => f.name).join(",")}`);
assert.ok(logsTable.numRows > 0, "expected at least one log row");

// Spot-check the topic0 column matches the transfer signature.
const topic0Col = logsTable.getChild("topic0");
assert.ok(topic0Col, "logs table missing topic0 column");
const firstTopic0 = topic0Col.get(0);
assert.ok(firstTopic0, "first topic0 row is null");
// Bytes -> hex
const hex = "0x" + Array.from(firstTopic0).map((b) => b.toString(16).padStart(2, "0")).join("");
assert.equal(hex, TRANSFER_TOPIC0, "first log topic0 mismatch");

// Blocks should also have rows since the server joins blocks for matching logs
if (res.blocks.byteLength > 0) {
    const blocksTable = tableFromIPC(res.blocks);
    console.log(`blocks: ${blocksTable.numRows} rows`);
}

console.log("OK");
