use arrayvec::ArrayVec;
use serde::{Deserialize, Serialize};

mod bloom_filter_wrapper;
mod data;
mod fixed_size_data;
mod hex;
mod quantity;
mod transaction_status;
mod transaction_type;
mod uint;
mod util;
mod withdrawal;

pub use bloom_filter_wrapper::FilterWrapper;
pub use data::Data;
pub use fixed_size_data::FixedSizeData;
pub use hex::Hex;
pub use quantity::Quantity;
pub use transaction_status::TransactionStatus;
pub use transaction_type::TransactionType;
pub use withdrawal::Withdrawal;

/// Evm block header object
///
/// See ethereum rpc spec for the meaning of fields
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockHeader {
    pub number: BlockNumber,
    pub hash: Hash,
    pub parent_hash: Hash,
    pub nonce: Option<Nonce>,
    #[serde(default)]
    pub sha3_uncles: Hash,
    pub logs_bloom: BloomFilter,
    pub transactions_root: Hash,
    pub state_root: Hash,
    pub receipts_root: Hash,
    pub miner: Address,
    pub difficulty: Option<Quantity>,
    pub total_difficulty: Option<Quantity>,
    pub extra_data: Data,
    pub size: Quantity,
    pub gas_limit: Quantity,
    pub gas_used: Quantity,
    pub timestamp: Quantity,
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

/// Evm block object
///
/// A block will contain a header and either a list of full transaction objects or
/// a list of only transaction hashes.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Block<Tx> {
    #[serde(flatten)]
    pub header: BlockHeader,
    pub transactions: Vec<Tx>,
}

/// Evm transaction object
///
/// See ethereum rpc spec for the meaning of fields
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Transaction {
    pub block_hash: Hash,
    pub block_number: BlockNumber,
    pub from: Option<Address>,
    pub gas: Quantity,
    pub gas_price: Option<Quantity>,
    pub hash: Hash,
    pub input: Data,
    pub nonce: Quantity,
    pub to: Option<Address>,
    pub transaction_index: TransactionIndex,
    pub value: Quantity,
    #[serde(rename = "type")]
    pub kind: Option<TransactionType>,
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
    // OP stack fields
    pub deposit_receipt_version: Option<Quantity>,
    pub mint: Option<Quantity>,
    pub source_hash: Option<Hash>,
}

/// Evm access list object
///
/// See ethereum rpc spec for the meaning of fields
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccessList {
    pub address: Option<Address>,
    pub storage_keys: Option<Vec<Hash>>,
}

/// Evm transaction receipt object
///
/// See ethereum rpc spec for the meaning of fields
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionReceipt {
    pub transaction_hash: Hash,
    pub transaction_index: TransactionIndex,
    pub block_hash: Hash,
    pub block_number: BlockNumber,
    pub from: Address,
    pub to: Option<Address>,
    pub cumulative_gas_used: Quantity,
    #[serde(default)]
    pub effective_gas_price: Quantity,
    pub gas_used: Quantity,
    pub contract_address: Option<Address>,
    pub logs: Vec<Log>,
    pub logs_bloom: BloomFilter,
    #[serde(rename = "type")]
    pub kind: Option<TransactionType>,
    pub root: Option<Hash>,
    pub status: Option<TransactionStatus>,
    pub l1_fee: Option<Quantity>,
    pub l1_gas_price: Option<Quantity>,
    pub l1_gas_used: Option<Quantity>,
    // This is a float value printed as string, e.g. "0.69"
    pub l1_fee_scalar: Option<String>,
    pub gas_used_for_l1: Option<Quantity>,
    pub blob_gas_price: Option<Quantity>,
    // NOTE: These fields are needed for Optimism (not pressent on other chains)
    pub deposit_nonce: Option<Quantity>,
    pub deposit_receipt_version: Option<Quantity>,
    pub blob_gas_used: Option<Quantity>,

    // Optimism fields
    pub l1_base_fee_scalar: Option<Quantity>,
    pub l1_blob_base_fee: Option<Quantity>,
    pub l1_blob_base_fee_scalar: Option<Quantity>,

    // Arbitrum fields
    pub l1_block_number: Option<Quantity>,
}

/// Evm log object
///
/// See ethereum rpc spec for the meaning of fields
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Log {
    pub removed: Option<bool>,
    pub log_index: LogIndex,
    pub transaction_index: TransactionIndex,
    pub transaction_hash: Hash,
    pub block_hash: Hash,
    pub block_number: BlockNumber,
    pub address: Address,
    pub data: Data,
    pub topics: ArrayVec<LogArgument, 4>,
    // // Many Modern RPCs return blockTimestamp, but it's not part of the official spec (yet).
    // // EIP: https://ethereum-magicians.org/t/proposal-for-adding-blocktimestamp-to-logs-object-returned-by-eth-getlogs-and-related-requests/11183/7 - reth has already merged this: https://github.com/paradigmxyz/reth/pull/7606
    // pub block_timestamp: Option<Quantity>,
}

/// Evm trace object (parity style, returned from trace_block request on RPC)
///
/// See trace_block documentation online for meaning of fields
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Trace {
    pub action: TraceAction,
    pub block_hash: Hash,
    pub block_number: u64,
    pub result: Option<TraceResult>,
    pub subtraces: Option<u64>,
    pub trace_address: Option<Vec<u64>>,
    pub transaction_hash: Option<Hash>,
    pub transaction_position: Option<u64>,
    #[serde(rename = "type")]
    pub kind: Option<String>,
    pub error: Option<String>,
}

/// Action object inside trace object (parity style, returned from trace_block request on RPC)
///
/// See trace_block documentation online for meaning of fields
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceAction {
    pub from: Option<Address>,
    pub to: Option<Address>,
    pub call_type: Option<String>,
    pub gas: Option<Quantity>,
    pub input: Option<Data>,
    pub init: Option<Data>,
    pub value: Option<Quantity>,
    pub author: Option<Address>,
    pub reward_type: Option<String>,
    // For suicide traces
    pub address: Option<Address>,
    pub refund_address: Option<Address>,
    pub balance: Option<Quantity>,
}

/// Result object inside trace object (parity style, returned from trace_block request on RPC)
///
/// See trace_block documentation online for meaning of fields
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceResult {
    pub address: Option<Address>,
    pub code: Option<Data>,
    pub gas_used: Option<Quantity>,
    pub output: Option<Data>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DebugBlockTrace {
    pub result: DebugTxTrace,
    pub tx_hash: Hash,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DebugTxTrace {
    #[serde(rename = "type")]
    pub kind: Option<String>,
    pub from: Option<Address>,
    pub to: Option<Address>,
    pub value: Option<Quantity>,
    pub gas: Option<Quantity>,
    pub gas_used: Option<Quantity>,
    pub input: Option<Data>,
    pub output: Option<Data>,
    pub error: Option<String>,
    pub revert_reason: Option<String>,
    #[serde(default)]
    pub calls: Vec<DebugTxTrace>,
}

/// EVM hash is 32 bytes of data
pub type Hash = FixedSizeData<32>;

/// EVM log argument is 32 bytes of data
pub type LogArgument = FixedSizeData<32>;

/// EVM address is 20 bytes of data
pub type Address = FixedSizeData<20>;

/// EVM nonce is 8 bytes of data
pub type Nonce = FixedSizeData<8>;

pub type BloomFilter = Data;
pub type BlockNumber = uint::UInt;
pub type TransactionIndex = uint::UInt;
pub type LogIndex = uint::UInt;
