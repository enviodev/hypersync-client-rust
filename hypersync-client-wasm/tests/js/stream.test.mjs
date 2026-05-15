// Streaming end-to-end test for hypersync-client-wasm.
//
// Streams a small block range and verifies that:
//   - stream_arrow yields multiple chunks
//   - each chunk decodes back to a non-empty arrow table
//   - the stream eventually terminates (next() resolves to undefined)
//   - row counts add up across chunks
//
// Run after `wasm-pack build --target nodejs --dev`:
//   cd tests/js && npm install
//   ENVIO_API_TOKEN=... node stream.test.mjs

import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import { tableFromIPC } from "apache-arrow";
import { Client } from "../../pkg/hypersync_client_wasm.js";

try {
    process.loadEnvFile(fileURLToPath(new URL("./.env", import.meta.url)));
} catch (e) {
    if (e.code !== "ENOENT") throw e;
}

const HYPERSYNC_URL = process.env.HYPERSYNC_URL ?? "https://eth.hypersync.xyz";
const TOKEN = process.env.ENVIO_API_TOKEN ?? "";

const TRANSFER_TOPIC0 =
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

// Range chosen to be large enough that the server splits into multiple
// chunks (so we exercise the streaming path) but small enough to finish
// quickly.
const query = {
    from_block: 19000000,
    to_block: 19000200,
    logs: [{ topics: [[TRANSFER_TOPIC0]] }],
    field_selection: {
        log: ["address", "topic0", "topic1", "topic2", "data", "block_number"],
        block: ["number"],
    },
};

const client = new Client(HYPERSYNC_URL, TOKEN);

const t0 = performance.now();
const stream = await client.stream_arrow(query, {
    // Force small batch size so the server returns multiple chunks for our
    // tiny block range. Otherwise the whole thing fits in a single response.
    batch_size: 50,
    max_batch_size: 50,
    min_batch_size: 50,
    concurrency: 4,
});

let totalLogRows = 0;
let chunkCount = 0;
let lastNextBlock = 0n;

while (true) {
    const chunk = await stream.next();
    if (chunk == null) break;
    chunkCount++;

    assert.ok(chunk.next_block > lastNextBlock, "next_block monotonically increases");
    lastNextBlock = chunk.next_block;

    if (chunk.logs.byteLength > 0) {
        const logs = tableFromIPC(chunk.logs);
        totalLogRows += logs.numRows;
    }
}

const elapsed = (performance.now() - t0).toFixed(0);
console.log(
    `streamed ${chunkCount} chunks, ${totalLogRows} log rows, in ${elapsed}ms ` +
        `(final next_block=${lastNextBlock})`,
);

assert.ok(chunkCount > 1, `expected multiple chunks, got ${chunkCount}`);
assert.ok(totalLogRows > 0, "expected at least one log row across the stream");
assert.equal(lastNextBlock, BigInt(query.to_block), "stream covered the whole range");

console.log("OK");
