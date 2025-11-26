use std::fmt::Debug;

use arrayvec::ArrayVec;
use arrow::array::{
    Array, BinaryArray, BooleanArray, RecordBatch, StringArray, UInt64Array, UInt8Array,
};
use hypersync_format::{TransactionStatus, TransactionType, UInt};

use crate::simple_types::{Block, Log, Trace, Transaction};

/// Used to convert arrow `RecordBatch` into a vector of structs of the appropriate type like
/// Log, Transaction, Block etc.
pub trait FromArrow: Sized {
    /// Convert given record batch to a vector of structs
    fn from_arrow(batch: &RecordBatch) -> Vec<Self>;
}

fn get_str<'a, T: From<&'a str>>(array: Option<&'a StringArray>, index: usize) -> Option<T> {
    match array {
        None => None,
        Some(a) => {
            if a.is_valid(index) {
                Some(a.value(index).into())
            } else {
                None
            }
        }
    }
}

fn get_binary<'a, T: TryFrom<&'a [u8]>>(array: Option<&'a BinaryArray>, index: usize) -> Option<T>
where
    <T as TryFrom<&'a [u8]>>::Error: Debug,
{
    match array {
        None => None,
        Some(a) => {
            if a.is_valid(index) {
                Some(a.value(index).try_into().unwrap())
            } else {
                None
            }
        }
    }
}

fn get_bool(array: Option<&BooleanArray>, index: usize) -> Option<bool> {
    match array {
        None => None,
        Some(a) => {
            if a.is_valid(index) {
                Some(a.value(index))
            } else {
                None
            }
        }
    }
}

fn get_u8(array: Option<&UInt8Array>, index: usize) -> Option<u8> {
    match array {
        None => None,
        Some(a) => {
            if a.is_valid(index) {
                Some(a.value(index))
            } else {
                None
            }
        }
    }
}

fn get_u64(array: Option<&UInt64Array>, index: usize) -> Option<u64> {
    match array {
        None => None,
        Some(a) => {
            if a.is_valid(index) {
                Some(a.value(index))
            } else {
                None
            }
        }
    }
}

fn column_as<'a, T: 'static>(batch: &'a RecordBatch, col_name: &str) -> Option<&'a T> {
    match batch.column_by_name(col_name) {
        None => None,
        Some(c) => c.as_any().downcast_ref::<T>(),
    }
}

impl FromArrow for Block {
    fn from_arrow(batch: &RecordBatch) -> Vec<Self> {
        let number = column_as::<UInt64Array>(batch, "number");
        let hash = column_as::<BinaryArray>(batch, "hash");
        let parent_hash = column_as::<BinaryArray>(batch, "parent_hash");
        let nonce = column_as::<BinaryArray>(batch, "nonce");
        let sha3_uncles = column_as::<BinaryArray>(batch, "sha3_uncles");
        let logs_bloom = column_as::<BinaryArray>(batch, "logs_bloom");
        let transactions_root = column_as::<BinaryArray>(batch, "transactions_root");
        let state_root = column_as::<BinaryArray>(batch, "state_root");
        let receipts_root = column_as::<BinaryArray>(batch, "receipts_root");
        let miner = column_as::<BinaryArray>(batch, "miner");
        let difficulty = column_as::<BinaryArray>(batch, "difficulty");
        let total_difficulty = column_as::<BinaryArray>(batch, "total_difficulty");
        let extra_data = column_as::<BinaryArray>(batch, "extra_data");
        let size = column_as::<BinaryArray>(batch, "size");
        let gas_limit = column_as::<BinaryArray>(batch, "gas_limit");
        let gas_used = column_as::<BinaryArray>(batch, "gas_used");
        let timestamp = column_as::<BinaryArray>(batch, "timestamp");
        let uncles = column_as::<BinaryArray>(batch, "uncles");
        let base_fee_per_gas = column_as::<BinaryArray>(batch, "base_fee_per_gas");
        let blob_gas_used = column_as::<BinaryArray>(batch, "blob_gas_used");
        let excess_blob_gas = column_as::<BinaryArray>(batch, "excess_blob_gas");
        let parent_beacon_block_root = column_as::<BinaryArray>(batch, "parent_beacon_block_root");
        let withdrawals_root = column_as::<BinaryArray>(batch, "withdrawals_root");
        let withdrawals = column_as::<BinaryArray>(batch, "withdrawals");
        let l1_block_number = column_as::<UInt64Array>(batch, "l1_block_number");
        let send_count = column_as::<BinaryArray>(batch, "send_count");
        let send_root = column_as::<BinaryArray>(batch, "send_root");
        let mix_hash = column_as::<BinaryArray>(batch, "mix_hash");

        (0..batch.num_rows())
            .map(|idx| Self {
                number: get_u64(number, idx),
                hash: get_binary(hash, idx),
                parent_hash: get_binary(parent_hash, idx),
                nonce: get_binary(nonce, idx),
                sha3_uncles: get_binary(sha3_uncles, idx),
                logs_bloom: get_binary(logs_bloom, idx),
                transactions_root: get_binary(transactions_root, idx),
                state_root: get_binary(state_root, idx),
                receipts_root: get_binary(receipts_root, idx),
                miner: get_binary(miner, idx),
                difficulty: get_binary(difficulty, idx),
                total_difficulty: get_binary(total_difficulty, idx),
                extra_data: get_binary(extra_data, idx),
                size: get_binary(size, idx),
                gas_limit: get_binary(gas_limit, idx),
                gas_used: get_binary(gas_used, idx),
                timestamp: get_binary(timestamp, idx),
                uncles: get_binary(uncles, idx).map(|v: &[u8]| {
                    v.chunks(32)
                        .map(|chunk| chunk.try_into().unwrap())
                        .collect()
                }),
                base_fee_per_gas: get_binary(base_fee_per_gas, idx),
                blob_gas_used: get_binary(blob_gas_used, idx),
                excess_blob_gas: get_binary(excess_blob_gas, idx),
                parent_beacon_block_root: get_binary(parent_beacon_block_root, idx),
                withdrawals_root: get_binary(withdrawals_root, idx),
                withdrawals: get_binary(withdrawals, idx).map(|v| bincode::deserialize(v).unwrap()),
                l1_block_number: get_u64(l1_block_number, idx).map(UInt::from),
                send_count: get_binary(send_count, idx),
                send_root: get_binary(send_root, idx),
                mix_hash: get_binary(mix_hash, idx),
            })
            .collect()
    }
}

impl FromArrow for Transaction {
    fn from_arrow(batch: &RecordBatch) -> Vec<Self> {
        let block_hash = column_as::<BinaryArray>(batch, "block_hash");
        let block_number = column_as::<UInt64Array>(batch, "block_number");
        let from = column_as::<BinaryArray>(batch, "from");
        let gas = column_as::<BinaryArray>(batch, "gas");
        let gas_price = column_as::<BinaryArray>(batch, "gas_price");
        let hash = column_as::<BinaryArray>(batch, "hash");
        let input = column_as::<BinaryArray>(batch, "input");
        let nonce = column_as::<BinaryArray>(batch, "nonce");
        let to = column_as::<BinaryArray>(batch, "to");
        let transaction_index = column_as::<UInt64Array>(batch, "transaction_index");
        let value = column_as::<BinaryArray>(batch, "value");
        let v = column_as::<BinaryArray>(batch, "v");
        let r = column_as::<BinaryArray>(batch, "r");
        let s = column_as::<BinaryArray>(batch, "s");
        let y_parity = column_as::<BinaryArray>(batch, "y_parity");
        let max_priority_fee_per_gas = column_as::<BinaryArray>(batch, "max_priority_fee_per_gas");
        let max_fee_per_gas = column_as::<BinaryArray>(batch, "max_fee_per_gas");
        let chain_id = column_as::<BinaryArray>(batch, "chain_id");
        let access_list = column_as::<BinaryArray>(batch, "access_list");
        let authorization_list = column_as::<BinaryArray>(batch, "authorization_list");
        let max_fee_per_blob_gas = column_as::<BinaryArray>(batch, "max_fee_per_blob_gas");
        let blob_versioned_hashes = column_as::<BinaryArray>(batch, "blob_versioned_hashes");
        let cumulative_gas_used = column_as::<BinaryArray>(batch, "cumulative_gas_used");
        let effective_gas_price = column_as::<BinaryArray>(batch, "effective_gas_price");
        let gas_used = column_as::<BinaryArray>(batch, "gas_used");
        let contract_address = column_as::<BinaryArray>(batch, "contract_address");
        let logs_bloom = column_as::<BinaryArray>(batch, "logs_bloom");
        let type_ = column_as::<UInt8Array>(batch, "type");
        let root = column_as::<BinaryArray>(batch, "root");
        let status = column_as::<UInt8Array>(batch, "status");
        let l1_fee = column_as::<BinaryArray>(batch, "l1_fee");
        let l1_gas_price = column_as::<BinaryArray>(batch, "l1_gas_price");
        let l1_gas_used = column_as::<BinaryArray>(batch, "l1_gas_used");
        let l1_fee_scalar = column_as::<BinaryArray>(batch, "l1_fee_scalar");
        let gas_used_for_l1 = column_as::<BinaryArray>(batch, "gas_used_for_l1");
        let blob_gas_price = column_as::<BinaryArray>(batch, "blob_gas_price");
        let blob_gas_used = column_as::<BinaryArray>(batch, "blob_gas_used");
        let deposit_nonce = column_as::<BinaryArray>(batch, "deposit_nonce");
        let deposit_receipt_version = column_as::<BinaryArray>(batch, "deposit_receipt_version");
        let l1_base_fee_scalar = column_as::<BinaryArray>(batch, "l1_base_fee_scalar");
        let l1_blob_base_fee = column_as::<BinaryArray>(batch, "l1_blob_base_fee");
        let l1_blob_base_fee_scalar = column_as::<BinaryArray>(batch, "l1_blob_base_fee_scalar");
        let l1_block_number = column_as::<BinaryArray>(batch, "l1_block_number");
        let mint = column_as::<BinaryArray>(batch, "mint");
        let sighash = column_as::<BinaryArray>(batch, "sighash");
        let source_hash = column_as::<BinaryArray>(batch, "source_hash");

        (0..batch.num_rows())
            .map(|idx| Self {
                block_hash: get_binary(block_hash, idx),
                block_number: get_u64(block_number, idx).map(UInt::from),
                from: get_binary(from, idx),
                gas: get_binary(gas, idx),
                gas_price: get_binary(gas_price, idx),
                hash: get_binary(hash, idx),
                input: get_binary(input, idx),
                nonce: get_binary(nonce, idx),
                to: get_binary(to, idx),
                transaction_index: get_u64(transaction_index, idx).map(UInt::from),
                value: get_binary(value, idx),
                v: get_binary(v, idx),
                r: get_binary(r, idx),
                s: get_binary(s, idx),
                y_parity: get_binary(y_parity, idx),
                max_priority_fee_per_gas: get_binary(max_priority_fee_per_gas, idx),
                max_fee_per_gas: get_binary(max_fee_per_gas, idx),
                chain_id: get_binary(chain_id, idx),
                access_list: get_binary(access_list, idx).map(|v| bincode::deserialize(v).unwrap()),
                authorization_list: get_binary(authorization_list, idx)
                    .map(|v| bincode::deserialize(v).unwrap()),
                max_fee_per_blob_gas: get_binary(max_fee_per_blob_gas, idx),
                blob_versioned_hashes: get_binary(blob_versioned_hashes, idx).map(|v: &[u8]| {
                    v.chunks(32)
                        .map(|chunk| chunk.try_into().unwrap())
                        .collect()
                }),
                cumulative_gas_used: get_binary(cumulative_gas_used, idx),
                effective_gas_price: get_binary(effective_gas_price, idx),
                gas_used: get_binary(gas_used, idx),
                contract_address: get_binary(contract_address, idx),
                logs_bloom: get_binary(logs_bloom, idx),
                type_: get_u8(type_, idx).map(TransactionType::from),
                root: get_binary(root, idx),
                status: get_u8(status, idx).map(|v| TransactionStatus::from_u8(v).unwrap()),
                l1_fee: get_binary(l1_fee, idx),
                l1_gas_price: get_binary(l1_gas_price, idx),
                l1_gas_used: get_binary(l1_gas_used, idx),
                l1_fee_scalar: get_binary(l1_fee_scalar, idx)
                    .map(|v| std::str::from_utf8(v).unwrap().parse().unwrap()),
                gas_used_for_l1: get_binary(gas_used_for_l1, idx),
                blob_gas_price: get_binary(blob_gas_price, idx),
                blob_gas_used: get_binary(blob_gas_used, idx),
                deposit_nonce: get_binary(deposit_nonce, idx),
                deposit_receipt_version: get_binary(deposit_receipt_version, idx),
                l1_base_fee_scalar: get_binary(l1_base_fee_scalar, idx),
                l1_blob_base_fee: get_binary(l1_blob_base_fee, idx),
                l1_blob_base_fee_scalar: get_binary(l1_blob_base_fee_scalar, idx),
                l1_block_number: get_binary(l1_block_number, idx),
                mint: get_binary(mint, idx),
                sighash: get_binary(sighash, idx),
                source_hash: get_binary(source_hash, idx),
            })
            .collect()
    }
}

impl FromArrow for Log {
    fn from_arrow(batch: &RecordBatch) -> Vec<Self> {
        let removed = column_as::<BooleanArray>(batch, "removed");
        let log_index = column_as::<UInt64Array>(batch, "log_index");
        let transaction_index = column_as::<UInt64Array>(batch, "transaction_index");
        let transaction_hash = column_as::<BinaryArray>(batch, "transaction_hash");
        let block_hash = column_as::<BinaryArray>(batch, "block_hash");
        let block_number = column_as::<UInt64Array>(batch, "block_number");
        let address = column_as::<BinaryArray>(batch, "address");
        let data = column_as::<BinaryArray>(batch, "data");
        let topic0 = column_as::<BinaryArray>(batch, "topic0");
        let topic1 = column_as::<BinaryArray>(batch, "topic1");
        let topic2 = column_as::<BinaryArray>(batch, "topic2");
        let topic3 = column_as::<BinaryArray>(batch, "topic3");

        (0..batch.num_rows())
            .map(|idx| Self {
                removed: get_bool(removed, idx),
                log_index: get_u64(log_index, idx).map(UInt::from),
                transaction_index: get_u64(transaction_index, idx).map(UInt::from),
                transaction_hash: get_binary(transaction_hash, idx),
                block_hash: get_binary(block_hash, idx),
                block_number: get_u64(block_number, idx).map(UInt::from),
                address: get_binary(address, idx),
                data: get_binary(data, idx),
                topics: {
                    let mut arr = ArrayVec::new();

                    arr.push(get_binary(topic0, idx));
                    arr.push(get_binary(topic1, idx));
                    arr.push(get_binary(topic2, idx));
                    arr.push(get_binary(topic3, idx));

                    arr
                },
            })
            .collect()
    }
}

impl FromArrow for Trace {
    fn from_arrow(batch: &RecordBatch) -> Vec<Self> {
        let from = column_as::<BinaryArray>(batch, "from");
        let to = column_as::<BinaryArray>(batch, "to");
        let call_type = column_as::<StringArray>(batch, "call_type");
        let gas = column_as::<BinaryArray>(batch, "gas");
        let input = column_as::<BinaryArray>(batch, "input");
        let init = column_as::<BinaryArray>(batch, "init");
        let value = column_as::<BinaryArray>(batch, "value");
        let author = column_as::<BinaryArray>(batch, "author");
        let reward_type = column_as::<StringArray>(batch, "reward_type");
        let block_hash = column_as::<BinaryArray>(batch, "block_hash");
        let block_number = column_as::<UInt64Array>(batch, "block_number");
        let address = column_as::<BinaryArray>(batch, "address");
        let code = column_as::<BinaryArray>(batch, "code");
        let gas_used = column_as::<BinaryArray>(batch, "gas_used");
        let output = column_as::<BinaryArray>(batch, "output");
        let subtraces = column_as::<UInt64Array>(batch, "subtraces");
        let trace_address = column_as::<BinaryArray>(batch, "trace_address");
        let transaction_hash = column_as::<BinaryArray>(batch, "transaction_hash");
        let transaction_position = column_as::<UInt64Array>(batch, "transaction_position");
        let type_ = column_as::<StringArray>(batch, "type");
        let error = column_as::<StringArray>(batch, "error");
        let sighash = column_as::<BinaryArray>(batch, "sighash");
        let action_address = column_as::<BinaryArray>(batch, "action_address");
        let balance = column_as::<BinaryArray>(batch, "balance");
        let refund_address = column_as::<BinaryArray>(batch, "refund_address");

        (0..batch.num_rows())
            .map(|idx| Self {
                from: get_binary(from, idx),
                to: get_binary(to, idx),
                call_type: get_str(call_type, idx).map(str::to_owned),
                gas: get_binary(gas, idx),
                input: get_binary(input, idx),
                init: get_binary(init, idx),
                value: get_binary(value, idx),
                author: get_binary(author, idx),
                reward_type: get_str(reward_type, idx).map(str::to_owned),
                block_hash: get_binary(block_hash, idx),
                block_number: get_u64(block_number, idx),
                address: get_binary(address, idx),
                code: get_binary(code, idx),
                gas_used: get_binary(gas_used, idx),
                output: get_binary(output, idx),
                subtraces: get_u64(subtraces, idx),
                trace_address: get_binary(trace_address, idx)
                    .map(|v| bincode::deserialize(v).unwrap()),
                transaction_hash: get_binary(transaction_hash, idx),
                transaction_position: get_u64(transaction_position, idx),
                type_: get_str(type_, idx).map(str::to_owned),
                error: get_str(error, idx).map(str::to_owned),
                sighash: get_binary(sighash, idx),
                action_address: get_binary(action_address, idx),
                balance: get_binary(balance, idx),
                refund_address: get_binary(refund_address, idx),
            })
            .collect()
    }
}
