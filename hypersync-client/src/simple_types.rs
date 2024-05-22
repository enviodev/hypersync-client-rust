use std::{collections::HashMap, sync::Arc};

use arrayvec::ArrayVec;
use hypersync_format::{
    AccessList, Address, BlockNumber, BloomFilter, Data, Hash, LogArgument, LogIndex, Nonce,
    Quantity, TransactionIndex, TransactionStatus, TransactionType, Withdrawal,
};
use nohash_hasher::IntMap;
use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::Xxh3Builder;

use crate::types::ResponseData;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Event {
    pub transaction: Option<Arc<Transaction>>,
    pub block: Option<Arc<Block>>,
    pub log: Log,
}

impl From<ResponseData> for Vec<Event> {
    fn from(data: ResponseData) -> Self {
        let mut events = Vec::with_capacity(data.logs.iter().map(|chunk| chunk.len()).sum());

        for ((blocks, transactions), logs) in data
            .blocks
            .into_iter()
            .zip(data.transactions.into_iter())
            .zip(data.logs.into_iter())
        {
            let blocks = blocks
                .into_iter()
                .map(|block| (block.number.unwrap(), Arc::new(block)))
                .collect::<IntMap<u64, _>>();
            let transactions = transactions
                .into_iter()
                .map(|tx| (tx.hash.clone().unwrap(), Arc::new(tx)))
                .collect::<HashMap<_, _, Xxh3Builder>>();

            for log in logs {
                let block = blocks.get(&log.block_number.unwrap().into()).cloned();
                let transaction = transactions
                    .get(log.transaction_hash.as_ref().unwrap())
                    .cloned();

                events.push(Event {
                    transaction,
                    block,
                    log,
                });
            }
        }

        events
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Block {
    pub number: Option<u64>,
    pub hash: Option<Hash>,
    pub parent_hash: Option<Hash>,
    pub nonce: Option<Nonce>,
    pub sha3_uncles: Option<Hash>,
    pub logs_bloom: Option<BloomFilter>,
    pub transactions_root: Option<Hash>,
    pub state_root: Option<Hash>,
    pub receipts_root: Option<Hash>,
    pub miner: Option<Address>,
    pub difficulty: Option<Quantity>,
    pub total_difficulty: Option<Quantity>,
    pub extra_data: Option<Data>,
    pub size: Option<Quantity>,
    pub gas_limit: Option<Quantity>,
    pub gas_used: Option<Quantity>,
    pub timestamp: Option<Quantity>,
    pub uncles: Option<Vec<Hash>>,
    pub base_fee_per_gas: Option<Quantity>,
    pub blob_gas_used: Option<Quantity>,
    pub excess_blob_gas: Option<Quantity>,
    pub parent_beacon_block_root: Option<Hash>,
    pub withdrawals_root: Option<Hash>,
    pub withdrawals: Option<Vec<Withdrawal>>,
    pub l1_block_number: Option<BlockNumber>,
    pub send_count: Option<Quantity>,
    pub send_root: Option<Hash>,
    pub mix_hash: Option<Hash>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Transaction {
    pub block_hash: Option<Hash>,
    pub block_number: Option<BlockNumber>,
    pub from: Option<Address>,
    pub gas: Option<Quantity>,
    pub gas_price: Option<Quantity>,
    pub hash: Option<Hash>,
    pub input: Option<Data>,
    pub nonce: Option<Quantity>,
    pub to: Option<Address>,
    pub transaction_index: Option<TransactionIndex>,
    pub value: Option<Quantity>,
    pub v: Option<Quantity>,
    pub r: Option<Quantity>,
    pub s: Option<Quantity>,
    pub y_parity: Option<Quantity>,
    pub max_priority_fee_per_gas: Option<Quantity>,
    pub max_fee_per_gas: Option<Quantity>,
    pub chain_id: Option<Quantity>,
    pub access_list: Option<Vec<AccessList>>,
    pub max_fee_per_blob_gas: Option<Quantity>,
    pub blob_versioned_hashes: Option<Vec<Hash>>,
    pub cumulative_gas_used: Option<Quantity>,
    pub effective_gas_price: Option<Quantity>,
    pub gas_used: Option<Quantity>,
    pub contract_address: Option<Address>,
    pub logs_bloom: Option<BloomFilter>,
    #[serde(rename = "type")]
    pub kind: Option<TransactionType>,
    pub root: Option<Hash>,
    pub status: Option<TransactionStatus>,
    pub l1_fee: Option<Quantity>,
    pub l1_gas_price: Option<Quantity>,
    pub l1_gas_used: Option<Quantity>,
    pub l1_fee_scalar: Option<Quantity>,
    pub gas_used_for_l1: Option<Quantity>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Log {
    pub removed: Option<bool>,
    pub log_index: Option<LogIndex>,
    pub transaction_index: Option<TransactionIndex>,
    pub transaction_hash: Option<Hash>,
    pub block_hash: Option<Hash>,
    pub block_number: Option<BlockNumber>,
    pub address: Option<Address>,
    pub data: Option<Data>,
    pub topics: ArrayVec<LogArgument, 4>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Trace {
    pub from: Option<Address>,
    pub to: Option<Address>,
    pub call_type: Option<String>,
    pub gas: Option<Quantity>,
    pub input: Option<Data>,
    pub init: Option<Data>,
    pub value: Option<Quantity>,
    pub author: Option<Address>,
    pub reward_type: Option<String>,
    pub block_hash: Option<Hash>,
    pub block_number: Option<u64>,
    pub address: Option<Address>,
    pub code: Option<Data>,
    pub gas_used: Option<Quantity>,
    pub output: Option<Data>,
    pub subtraces: Option<u64>,
    pub trace_address: Option<Vec<u64>>,
    pub transaction_hash: Option<Hash>,
    pub transaction_position: Option<u64>,
    #[serde(rename = "type")]
    pub kind: Option<String>,
    pub error: Option<String>,
}
