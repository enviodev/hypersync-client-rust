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
//   - wasm Client constructs (both `new Client(url, token)` and
//     `Client.with_config({...})`)
//   - get_height() / get_chain_id() succeed and return bigints
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
// `url::Url::to_string()` always normalizes to a trailing slash on the
// authority — accept either form.
assert.ok(
    client.url === HYPERSYNC_URL || client.url === HYPERSYNC_URL + "/",
    `url getter unexpected: ${client.url}`,
);

// Probe the simple endpoints first to make sure auth works before we run a query.
const [height, chainId] = await Promise.all([
    client.get_height(),
    client.get_chain_id(),
]);
assert.equal(typeof height, "bigint", "get_height returns bigint");
assert.equal(typeof chainId, "bigint", "get_chain_id returns bigint");
console.log(`server: chain_id=${chainId}, height=${height}`);

// Also verify with_config works as an alternate constructor
const client2 = Client.with_config({ url: HYPERSYNC_URL, api_token: TOKEN });
assert.equal(client2.url, HYPERSYNC_URL, "with_config().url");

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
    decoded_logs_bytes: res.decoded_logs.byteLength,
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

// decoded_logs should be empty here (no event_signature requested).
assert.equal(res.decoded_logs.byteLength, 0, "decoded_logs empty when no signature");

console.log("OK");
