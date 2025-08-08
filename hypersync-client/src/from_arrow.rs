use arrayvec::ArrayVec;
use polars_arrow::array::{
    BinaryArray, BinaryViewArray, BooleanArray, StaticArray, UInt64Array, UInt8Array, Utf8Array,
};

use crate::{
    simple_types::{Block, Log, Trace, Transaction},
    ArrowBatch,
};

/// Used to do ArrowBatch-Native Rust type conversions while consuming the input value.
pub trait FromArrow: Sized {
    /// Converts to the Vector type from the ArrowBatch type.
    /// B is the type of binary array used to downcast since we use both binary views and
    /// binary arrays
    fn from_arrow_bin_array<B: BinArray + 'static>(batch: &ArrowBatch) -> Vec<Self>;

    /// Default implementation that uses the binary array type.
    /// IPC is written with regular binary arrays.
    /// Named 'from_arrow' for backwards compatibility.
    fn from_arrow(batch: &ArrowBatch) -> Vec<Self> {
        Self::from_arrow_bin_array::<BinaryArray<i32>>(batch)
    }

    /// An additional method that uses the binary view array type.
    /// This is to be able to reuse the trait server side where Binary Views are
    /// used instead.
    fn from_arrow_bin_view_array(batch: &ArrowBatch) -> Vec<Self> {
        Self::from_arrow_bin_array::<BinaryViewArray>(batch)
    }
}

/// A simple trait to compose binary array types that need to be
/// accessed by index
pub trait BinArray {
    fn get_idx(&self, i: usize) -> Option<&[u8]>;
}

impl BinArray for BinaryArray<i32> {
    fn get_idx(&self, i: usize) -> Option<&[u8]> {
        self.get(i)
    }
}

impl BinArray for BinaryViewArray {
    fn get_idx(&self, i: usize) -> Option<&[u8]> {
        self.get(i)
    }
}

fn map_binary<'a, T, B>(i: usize, arr: Option<&'a B>) -> Option<T>
where
    T: TryFrom<&'a [u8]>,
    <T as TryFrom<&'a [u8]>>::Error: std::fmt::Debug,
    B: BinArray,
{
    arr.and_then(|arr| arr.get_idx(i).map(|v| v.try_into().unwrap()))
}

impl FromArrow for Block {
    fn from_arrow_bin_array<B: BinArray + 'static>(batch: &ArrowBatch) -> Vec<Self> {
        let number = batch.column::<UInt64Array>("number").ok();
        let hash = batch.column::<B>("hash").ok();
        let parent_hash = batch.column::<B>("parent_hash").ok();
        let nonce = batch.column::<B>("nonce").ok();
        let sha3_uncles = batch.column::<B>("sha3_uncles").ok();
        let logs_bloom = batch.column::<B>("logs_bloom").ok();
        let transactions_root = batch.column::<B>("transactions_root").ok();
        let state_root = batch.column::<B>("state_root").ok();
        let receipts_root = batch.column::<B>("receipts_root").ok();
        let miner = batch.column::<B>("miner").ok();
        let difficulty = batch.column::<B>("difficulty").ok();
        let total_difficulty = batch.column::<B>("total_difficulty").ok();
        let extra_data = batch.column::<B>("extra_data").ok();
        let size = batch.column::<B>("size").ok();
        let gas_limit = batch.column::<B>("gas_limit").ok();
        let gas_used = batch.column::<B>("gas_used").ok();
        let timestamp = batch.column::<B>("timestamp").ok();
        let uncles = batch.column::<B>("uncles").ok();
        let base_fee_per_gas = batch.column::<B>("base_fee_per_gas").ok();
        let blob_gas_used = batch.column::<B>("blob_gas_used").ok();
        let excess_blob_gas = batch.column::<B>("excess_blob_gas").ok();
        let parent_beacon_block_root = batch.column::<B>("parent_beacon_block_root").ok();
        let withdrawals_root = batch.column::<B>("withdrawals_root").ok();
        let withdrawals = batch.column::<B>("withdrawals").ok();
        let l1_block_number = batch.column::<UInt64Array>("l1_block_number").ok();
        let send_count = batch.column::<B>("send_count").ok();
        let send_root = batch.column::<B>("send_root").ok();
        let mix_hash = batch.column::<B>("mix_hash").ok();

        (0..batch.chunk.len())
            .map(|idx| Self {
                number: number.and_then(|arr| arr.get(idx)),
                hash: map_binary(idx, hash),
                parent_hash: map_binary(idx, parent_hash),
                nonce: map_binary(idx, nonce),
                sha3_uncles: map_binary(idx, sha3_uncles),
                logs_bloom: map_binary(idx, logs_bloom),
                transactions_root: map_binary(idx, transactions_root),
                state_root: map_binary(idx, state_root),
                receipts_root: map_binary(idx, receipts_root),
                miner: map_binary(idx, miner),
                difficulty: map_binary(idx, difficulty),
                total_difficulty: map_binary(idx, total_difficulty),
                extra_data: map_binary(idx, extra_data),
                size: map_binary(idx, size),
                gas_limit: map_binary(idx, gas_limit),
                gas_used: map_binary(idx, gas_used),
                timestamp: map_binary(idx, timestamp),
                uncles: uncles.and_then(|arr| {
                    arr.get_idx(idx).map(|v| {
                        v.chunks(32)
                            .map(|chunk| chunk.try_into().unwrap())
                            .collect()
                    })
                }),
                base_fee_per_gas: map_binary(idx, base_fee_per_gas),
                blob_gas_used: map_binary(idx, blob_gas_used),
                excess_blob_gas: map_binary(idx, excess_blob_gas),
                parent_beacon_block_root: map_binary(idx, parent_beacon_block_root),
                withdrawals_root: map_binary(idx, withdrawals_root),
                withdrawals: withdrawals
                    .and_then(|arr| arr.get_idx(idx).map(|v| bincode::deserialize(v).unwrap())),
                l1_block_number: l1_block_number.and_then(|arr| arr.get(idx).map(|v| v.into())),
                send_count: map_binary(idx, send_count),
                send_root: map_binary(idx, send_root),
                mix_hash: map_binary(idx, mix_hash),
            })
            .collect()
    }
}

impl FromArrow for Transaction {
    fn from_arrow_bin_array<B: BinArray + 'static>(batch: &ArrowBatch) -> Vec<Self> {
        let block_hash = batch.column::<B>("block_hash").ok();
        let block_number = batch.column::<UInt64Array>("block_number").ok();
        let from = batch.column::<B>("from").ok();
        let gas = batch.column::<B>("gas").ok();
        let gas_price = batch.column::<B>("gas_price").ok();
        let hash = batch.column::<B>("hash").ok();
        let input = batch.column::<B>("input").ok();
        let nonce = batch.column::<B>("nonce").ok();
        let to = batch.column::<B>("to").ok();
        let transaction_index = batch.column::<UInt64Array>("transaction_index").ok();
        let value = batch.column::<B>("value").ok();
        let v = batch.column::<B>("v").ok();
        let r = batch.column::<B>("r").ok();
        let s = batch.column::<B>("s").ok();
        let y_parity = batch.column::<B>("y_parity").ok();
        let max_priority_fee_per_gas = batch.column::<B>("max_priority_fee_per_gas").ok();
        let max_fee_per_gas = batch.column::<B>("max_fee_per_gas").ok();
        let chain_id = batch.column::<B>("chain_id").ok();
        let access_list = batch.column::<B>("access_list").ok();
        let authorization_list = batch.column::<B>("authorization_list").ok();
        let max_fee_per_blob_gas = batch.column::<B>("max_fee_per_blob_gas").ok();
        let blob_versioned_hashes = batch.column::<B>("blob_versioned_hashes").ok();
        let cumulative_gas_used = batch.column::<B>("cumulative_gas_used").ok();
        let effective_gas_price = batch.column::<B>("effective_gas_price").ok();
        let gas_used = batch.column::<B>("gas_used").ok();
        let contract_address = batch.column::<B>("contract_address").ok();
        let logs_bloom = batch.column::<B>("logs_bloom").ok();
        let kind = batch.column::<UInt8Array>("type").ok();
        let root = batch.column::<B>("root").ok();
        let status = batch.column::<UInt8Array>("status").ok();
        let l1_fee = batch.column::<B>("l1_fee").ok();
        let l1_gas_price = batch.column::<B>("l1_gas_price").ok();
        let l1_gas_used = batch.column::<B>("l1_gas_used").ok();
        let l1_fee_scalar = batch.column::<B>("l1_fee_scalar").ok();
        let gas_used_for_l1 = batch.column::<B>("gas_used_for_l1").ok();

        (0..batch.chunk.len())
            .map(|idx| Self {
                block_hash: map_binary(idx, block_hash),
                block_number: block_number.and_then(|arr| arr.get(idx).map(|v| v.into())),
                from: map_binary(idx, from),
                gas: map_binary(idx, gas),
                gas_price: map_binary(idx, gas_price),
                hash: map_binary(idx, hash),
                input: map_binary(idx, input),
                nonce: map_binary(idx, nonce),
                to: map_binary(idx, to),
                transaction_index: transaction_index.and_then(|arr| arr.get(idx).map(|v| v.into())),
                value: map_binary(idx, value),
                v: map_binary(idx, v),
                r: map_binary(idx, r),
                s: map_binary(idx, s),
                y_parity: map_binary(idx, y_parity),
                max_priority_fee_per_gas: map_binary(idx, max_priority_fee_per_gas),
                max_fee_per_gas: map_binary(idx, max_fee_per_gas),
                chain_id: map_binary(idx, chain_id),
                access_list: access_list
                    .and_then(|arr| arr.get_idx(idx).map(|v| bincode::deserialize(v).unwrap())),
                authorization_list: authorization_list
                    .and_then(|arr| arr.get_idx(idx).map(|v| bincode::deserialize(v).unwrap())),
                max_fee_per_blob_gas: map_binary(idx, max_fee_per_blob_gas),
                blob_versioned_hashes: blob_versioned_hashes.and_then(|arr| {
                    arr.get_idx(idx).map(|v| {
                        v.chunks(32)
                            .map(|chunk| chunk.try_into().unwrap())
                            .collect()
                    })
                }),
                cumulative_gas_used: map_binary(idx, cumulative_gas_used),
                effective_gas_price: map_binary(idx, effective_gas_price),
                gas_used: map_binary(idx, gas_used),
                contract_address: map_binary(idx, contract_address),
                logs_bloom: map_binary(idx, logs_bloom),
                kind: kind.and_then(|arr| arr.get(idx).map(|v| v.into())),
                root: map_binary(idx, root),
                status: status.and_then(|arr| {
                    arr.get(idx)
                        .map(|v| hypersync_format::TransactionStatus::from_u8(v).unwrap())
                }),
                l1_fee: map_binary(idx, l1_fee),
                l1_gas_price: map_binary(idx, l1_gas_price),
                l1_gas_used: map_binary(idx, l1_gas_used),
                l1_fee_scalar: l1_fee_scalar.and_then(|arr| {
                    arr.get_idx(idx)
                        .map(|v| std::str::from_utf8(v).unwrap().parse().unwrap())
                }),
                gas_used_for_l1: map_binary(idx, gas_used_for_l1),
            })
            .collect()
    }
}

impl FromArrow for Log {
    fn from_arrow_bin_array<B: BinArray + 'static>(batch: &ArrowBatch) -> Vec<Self> {
        let removed = batch.column::<BooleanArray>("removed").ok();
        let log_index = batch.column::<UInt64Array>("log_index").ok();
        let transaction_index = batch.column::<UInt64Array>("transaction_index").ok();
        let transaction_hash = batch.column::<B>("transaction_hash").ok();
        let block_hash = batch.column::<B>("block_hash").ok();
        let block_number = batch.column::<UInt64Array>("block_number").ok();
        let address = batch.column::<B>("address").ok();
        let data = batch.column::<B>("data").ok();
        let topic0 = batch.column::<B>("topic0").ok();
        let topic1 = batch.column::<B>("topic1").ok();
        let topic2 = batch.column::<B>("topic2").ok();
        let topic3 = batch.column::<B>("topic3").ok();

        (0..batch.chunk.len())
            .map(|idx| Self {
                removed: removed.and_then(|arr| arr.get(idx)),
                log_index: log_index.and_then(|arr| arr.get(idx).map(|v| v.into())),
                transaction_index: transaction_index.and_then(|arr| arr.get(idx).map(|v| v.into())),
                transaction_hash: map_binary(idx, transaction_hash),
                block_hash: map_binary(idx, block_hash),
                block_number: block_number.and_then(|arr| arr.get(idx).map(|v| v.into())),
                address: map_binary(idx, address),
                data: map_binary(idx, data),
                topics: {
                    let mut arr = ArrayVec::new();

                    arr.push(map_binary(idx, topic0));
                    arr.push(map_binary(idx, topic1));
                    arr.push(map_binary(idx, topic2));
                    arr.push(map_binary(idx, topic3));

                    arr
                },
            })
            .collect()
    }
}

impl FromArrow for Trace {
    fn from_arrow_bin_array<B: BinArray + 'static>(batch: &ArrowBatch) -> Vec<Self> {
        let from = batch.column::<B>("from").ok();
        let to = batch.column::<B>("to").ok();
        let call_type = batch.column::<Utf8Array<i32>>("call_type").ok();
        let gas = batch.column::<B>("gas").ok();
        let input = batch.column::<B>("input").ok();
        let init = batch.column::<B>("init").ok();
        let value = batch.column::<B>("value").ok();
        let author = batch.column::<B>("author").ok();
        let reward_type = batch.column::<Utf8Array<i32>>("reward_type").ok();
        let block_hash = batch.column::<B>("block_hash").ok();
        let block_number = batch.column::<UInt64Array>("block_number").ok();
        let address = batch.column::<B>("address").ok();
        let code = batch.column::<B>("code").ok();
        let gas_used = batch.column::<B>("gas_used").ok();
        let output = batch.column::<B>("output").ok();
        let subtraces = batch.column::<UInt64Array>("subtraces").ok();
        let trace_address = batch.column::<B>("trace_address").ok();
        let transaction_hash = batch.column::<B>("transaction_hash").ok();
        let transaction_position = batch.column::<UInt64Array>("transaction_position").ok();
        let kind = batch.column::<Utf8Array<i32>>("type").ok();
        let error = batch.column::<Utf8Array<i32>>("error").ok();

        (0..batch.chunk.len())
            .map(|idx| Self {
                from: map_binary(idx, from),
                to: map_binary(idx, to),
                call_type: call_type.and_then(|arr| arr.get(idx).map(|v| v.to_owned())),
                gas: map_binary(idx, gas),
                input: map_binary(idx, input),
                init: map_binary(idx, init),
                value: map_binary(idx, value),
                author: map_binary(idx, author),
                reward_type: reward_type.and_then(|arr| arr.get(idx).map(|v| v.to_owned())),
                block_hash: map_binary(idx, block_hash),
                block_number: block_number.and_then(|arr| arr.get(idx)),
                address: map_binary(idx, address),
                code: map_binary(idx, code),
                gas_used: map_binary(idx, gas_used),
                output: map_binary(idx, output),
                subtraces: subtraces.and_then(|arr| arr.get(idx)),
                trace_address: trace_address
                    .and_then(|arr| arr.get_idx(idx).map(|v| bincode::deserialize(v).unwrap())),
                transaction_hash: map_binary(idx, transaction_hash),
                transaction_position: transaction_position.and_then(|arr| arr.get(idx)),
                kind: kind.and_then(|arr| arr.get(idx).map(|v| v.to_owned())),
                error: error.and_then(|arr| arr.get(idx).map(|v| v.to_owned())),
            })
            .collect()
    }
}
