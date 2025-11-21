//! Base object types for the Hypersync client.
use std::{collections::HashMap, sync::Arc};

use arrayvec::ArrayVec;
use hypersync_format::{
    AccessList, Address, Authorization, BlockNumber, BloomFilter, Data, Hash, LogArgument,
    LogIndex, Nonce, Quantity, TransactionIndex, TransactionStatus, TransactionType, Withdrawal,
};
use hypersync_net_types::{
    block::BlockField, log::LogField, trace::TraceField, transaction::TransactionField, FieldSelection,
};
use nohash_hasher::IntMap;
use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::Xxh3Builder;

use crate::types::ResponseData;

/// An Ethereum event object.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct Event {
    /// An Ethereum event transaction object.
    pub transaction: Option<Arc<Transaction>>,
    /// An Ethereum event block object.
    pub block: Option<Arc<Block>>,
    /// An Ethereum event log object.
    pub log: Log,
}

// Field lists for implementing event based API, these fields are used for joining
// so they should always be added to the field selection.
const BLOCK_JOIN_FIELD: BlockField = BlockField::Number;
const TX_JOIN_FIELD: TransactionField = TransactionField::Hash;
const LOG_JOIN_FIELD_WITH_TX: LogField = LogField::TransactionHash;
const LOG_JOIN_FIELD_WITH_BLOCK: LogField = LogField::BlockNumber;

enum InternalJoinStrategy {
    NotSelected,
    OnlyLogJoinField,
    FullJoin,
}

/// Internal event join strategy for determining how to join blocks and transactions with logs
pub(crate) struct InternalEventJoinStrategy {
    block: InternalJoinStrategy,
    transaction: InternalJoinStrategy,
}

impl From<&FieldSelection> for InternalEventJoinStrategy {
    fn from(field_selection: &FieldSelection) -> Self {
        let block_fields_num = field_selection.block.len();
        let transaction_fields_num = field_selection.transaction.len();

        Self {
            block: if block_fields_num == 0 {
                InternalJoinStrategy::NotSelected
            } else if block_fields_num == 1 && field_selection.block.contains(&BLOCK_JOIN_FIELD) {
                InternalJoinStrategy::OnlyLogJoinField
            } else {
                InternalJoinStrategy::FullJoin
            },
            transaction: if transaction_fields_num == 0 {
                InternalJoinStrategy::NotSelected
            } else if transaction_fields_num == 1
                && field_selection.transaction.contains(&TX_JOIN_FIELD)
            {
                InternalJoinStrategy::OnlyLogJoinField
            } else {
                InternalJoinStrategy::FullJoin
            },
        }
    }
}

impl InternalEventJoinStrategy {
    /// Add join fields to field selection based on the event join strategy
    pub(crate) fn add_join_fields_to_selection(&self, field_selection: &mut FieldSelection) {
        match self.block {
            InternalJoinStrategy::NotSelected => (),
            InternalJoinStrategy::OnlyLogJoinField => {
                field_selection.log.insert(LOG_JOIN_FIELD_WITH_BLOCK);
                field_selection.block.remove(&BLOCK_JOIN_FIELD);
            }
            InternalJoinStrategy::FullJoin => {
                field_selection.log.insert(LOG_JOIN_FIELD_WITH_BLOCK);
                field_selection.block.insert(BLOCK_JOIN_FIELD);
            }
        }

        match self.transaction {
            InternalJoinStrategy::NotSelected => (),
            InternalJoinStrategy::OnlyLogJoinField => {
                field_selection.log.insert(LOG_JOIN_FIELD_WITH_TX);
                field_selection.transaction.remove(&TX_JOIN_FIELD);
            }
            InternalJoinStrategy::FullJoin => {
                field_selection.log.insert(LOG_JOIN_FIELD_WITH_TX);
                field_selection.transaction.insert(TX_JOIN_FIELD);
            }
        }
    }

    /// Join response data into events based on the event join strategy
    pub(crate) fn join_from_response_data(&self, data: ResponseData) -> Vec<Event> {
        let blocks = data
            .blocks
            .into_iter()
            .flat_map(|blocks| {
                blocks
                    .into_iter()
                    .map(|block| (block.number.unwrap(), Arc::new(block)))
            })
            .collect::<IntMap<u64, _>>();

        let transactions = data
            .transactions
            .into_iter()
            .flat_map(|txs| {
                txs.into_iter()
                    .map(|tx| (tx.hash.clone().unwrap(), Arc::new(tx)))
            })
            .collect::<HashMap<_, _, Xxh3Builder>>();

        data.logs
            .into_iter()
            .flat_map(|logs| {
                logs.into_iter().map(|log| {
                    let block = match self.block {
                        InternalJoinStrategy::NotSelected => None,
                        InternalJoinStrategy::OnlyLogJoinField => Some(Arc::new(Block {
                            number: Some(log.block_number.unwrap().into()),
                            ..Block::default()
                        })),
                        InternalJoinStrategy::FullJoin => {
                            blocks.get(&log.block_number.unwrap().into()).cloned()
                        }
                    };
                    let transaction = match self.transaction {
                        InternalJoinStrategy::NotSelected => None,
                        InternalJoinStrategy::OnlyLogJoinField => Some(Arc::new(Transaction {
                            hash: log.transaction_hash.clone(),
                            ..Transaction::default()
                        })),
                        InternalJoinStrategy::FullJoin => transactions
                            .get(log.transaction_hash.as_ref().unwrap())
                            .cloned(),
                    };

                    Event {
                        transaction,
                        block,
                        log,
                    }
                })
            })
            .collect()
    }
}

/// Block object
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Block {
    /// A scalar value equal to the number of ancestor blocks. The genesis block has a number of
    /// zero; formally Hi.
    pub number: Option<u64>,
    /// The Keccak 256-bit hash of the block
    pub hash: Option<Hash>,
    /// The Keccak 256-bit hash of the parent
    /// block’s header, in its entirety; formally Hp.
    pub parent_hash: Option<Hash>,
    /// A 64-bit value which, combined with the mixhash, proves that a sufficient amount of
    /// computation has been carried out on this block; formally Hn.
    pub nonce: Option<Nonce>,
    /// The Keccak 256-bit hash of the ommers/uncles list portion of this block; formally Ho.
    pub sha3_uncles: Option<Hash>,
    /// The Bloom filter composed from indexable information (logger address and log topics)
    /// contained in each log entry from the receipt of each transaction in the transactions list;
    /// formally Hb.
    pub logs_bloom: Option<BloomFilter>,
    /// The Keccak 256-bit hash of the root node of the trie structure populated with each
    /// transaction in the transactions list portion of the block; formally Ht.
    pub transactions_root: Option<Hash>,
    /// The Keccak 256-bit hash of the root node of the state trie, after all transactions are
    /// executed and finalisations applied; formally Hr.
    pub state_root: Option<Hash>,
    /// The Keccak 256-bit hash of the root node of the trie structure populated with each
    /// transaction in the transactions list portion of the block; formally Ht.
    pub receipts_root: Option<Hash>,
    /// The 160-bit address to which all fees collected from the successful mining of this block
    /// be transferred; formally Hc.
    pub miner: Option<Address>,
    /// A scalar value corresponding to the difficulty level of this block. This can be calculated
    /// from the previous block’s difficulty level and the timestamp; formally Hd.
    pub difficulty: Option<Quantity>,
    /// The cumulative sum of the difficulty of all blocks that have been mined in the Ethereum
    /// network since the inception of the network.
    /// It measures the overall security and integrity of the Ethereum network.
    pub total_difficulty: Option<Quantity>,
    /// An arbitrary byte array containing data relevant to this block. This must be 32 bytes or
    /// fewer; formally Hx.
    pub extra_data: Option<Data>,
    /// The size of this block in bytes as an integer value, encoded as hexadecimal.
    pub size: Option<Quantity>,
    /// A scalar value equal to the current limit of gas expenditure per block; formally Hl.
    pub gas_limit: Option<Quantity>,
    /// A scalar value equal to the total gas used in transactions in this block; formally Hg.
    pub gas_used: Option<Quantity>,
    /// A scalar value equal to the reasonable output of Unix’s time() at this block’s inception;
    /// formally Hs.
    pub timestamp: Option<Quantity>,
    /// Ommers/uncles header.
    pub uncles: Option<Vec<Hash>>,
    /// A scalar representing EIP1559 base fee which can move up or down each block according
    /// to a formula which is a function of gas used in parent block and gas target
    /// (block gas limit divided by elasticity multiplier) of parent block.
    /// The algorithm results in the base fee per gas increasing when blocks are
    /// above the gas target, and decreasing when blocks are below the gas target. The base fee per
    /// gas is burned.
    pub base_fee_per_gas: Option<Quantity>,
    /// The total amount of blob gas consumed by the transactions within the block, added in
    /// EIP-4844.
    pub blob_gas_used: Option<Quantity>,
    /// A running total of blob gas consumed in excess of the target, prior to the block. Blocks
    /// with above-target blob gas consumption increase this value, blocks with below-target blob
    /// gas consumption decrease it (bounded at 0). This was added in EIP-4844.
    pub excess_blob_gas: Option<Quantity>,
    /// The hash of the parent beacon block's root is included in execution blocks, as proposed by
    /// EIP-4788.
    ///
    /// This enables trust-minimized access to consensus state, supporting staking pools, bridges,
    /// and more.
    ///
    /// The beacon roots contract handles root storage, enhancing Ethereum's functionalities.
    pub parent_beacon_block_root: Option<Hash>,
    /// The Keccak 256-bit hash of the withdrawals list portion of this block.
    ///
    /// See [EIP-4895](https://eips.ethereum.org/EIPS/eip-4895).
    pub withdrawals_root: Option<Hash>,
    /// Withdrawal represents a validator withdrawal from the consensus layer.
    pub withdrawals: Option<Vec<Withdrawal>>,
    /// The L1 block number that would be used for block.number calls.
    pub l1_block_number: Option<BlockNumber>,
    /// The number of L2 to L1 messages since Nitro genesis.
    pub send_count: Option<Quantity>,
    /// The Merkle root of the outbox tree state.
    pub send_root: Option<Hash>,
    /// A 256-bit hash which, combined with the
    /// nonce, proves that a sufficient amount of computation has been carried out on this block;
    /// formally Hm.
    pub mix_hash: Option<Hash>,
}

/// Transaction object
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct Transaction {
    /// The Keccak 256-bit hash of the block
    pub block_hash: Option<Hash>,
    /// A scalar value equal to the number of ancestor blocks. The genesis block has a number of
    /// zero; formally Hi.
    pub block_number: Option<BlockNumber>,
    /// The 160-bit address of the message call’s sender
    pub from: Option<Address>,
    /// A scalar value equal to the maximum
    /// amount of gas that should be used in executing
    /// this transaction. This is paid up-front, before any
    /// computation is done and may not be increased
    /// later; formally Tg.
    pub gas: Option<Quantity>,
    /// A scalar value equal to the number of
    /// Wei to be paid per unit of gas for all computation
    /// costs incurred as a result of the execution of this transaction; formally Tp.
    pub gas_price: Option<Quantity>,
    /// A transaction hash is a keccak hash of an RLP encoded signed transaction.
    pub hash: Option<Hash>,
    /// Input has two uses depending if transaction is Create or Call (if `to` field is None or
    /// Some). pub init: An unlimited size byte array specifying the
    /// EVM-code for the account initialisation procedure CREATE,
    /// data: An unlimited size byte array specifying the
    /// input data of the message call, formally Td.
    pub input: Option<Data>,
    /// A scalar value equal to the number of transactions sent by the sender; formally Tn.
    pub nonce: Option<Quantity>,
    /// The 160-bit address of the message call’s recipient or, for a contract creation
    /// transaction, ∅, used here to denote the only member of B0 ; formally Tt.
    pub to: Option<Address>,
    /// Index of the transaction in the block
    pub transaction_index: Option<TransactionIndex>,
    /// A scalar value equal to the number of Wei to
    /// be transferred to the message call’s recipient or,
    /// in the case of contract creation, as an endowment
    /// to the newly created account; formally Tv.
    pub value: Option<Quantity>,
    /// Replay protection value based on chain_id. See EIP-155 for more info.
    pub v: Option<Quantity>,
    /// The R field of the signature; the point on the curve.
    pub r: Option<Quantity>,
    /// The S field of the signature; the point on the curve.
    pub s: Option<Quantity>,
    /// yParity: Signature Y parity; formally Ty
    pub y_parity: Option<Quantity>,
    /// Max Priority fee that transaction is paying
    ///
    /// As ethereum circulation is around 120mil eth as of 2022 that is around
    /// 120000000000000000000000000 wei we are safe to use u128 as its max number is:
    /// 340282366920938463463374607431768211455
    ///
    /// This is also known as `GasTipCap`
    pub max_priority_fee_per_gas: Option<Quantity>,
    /// A scalar value equal to the maximum
    /// amount of gas that should be used in executing
    /// this transaction. This is paid up-front, before any
    /// computation is done and may not be increased
    /// later; formally Tg.
    ///
    /// As ethereum circulation is around 120mil eth as of 2022 that is around
    /// 120000000000000000000000000 wei we are safe to use u128 as its max number is:
    /// 340282366920938463463374607431768211455
    ///
    /// This is also known as `GasFeeCap`
    pub max_fee_per_gas: Option<Quantity>,
    /// Added as EIP-pub 155: Simple replay attack protection
    pub chain_id: Option<Quantity>,
    /// The accessList specifies a list of addresses and storage keys;
    /// these addresses and storage keys are added into the `accessed_addresses`
    /// and `accessed_storage_keys` global sets (introduced in EIP-2929).
    /// A gas cost is charged, though at a discount relative to the cost of
    /// accessing outside the list.
    pub access_list: Option<Vec<AccessList>>,
    /// The authorization_list specifies a list of authorizations for the transaction
    /// (introduced in EIP-7702)
    pub authorization_list: Option<Vec<Authorization>>,
    /// Max fee per data gas
    ///
    /// aka BlobFeeCap or blobGasFeeCap
    pub max_fee_per_blob_gas: Option<Quantity>,
    /// It contains a vector of fixed size hash(32 bytes)
    pub blob_versioned_hashes: Option<Vec<Hash>>,
    /// The total amount of gas used in the block until this transaction was executed.
    pub cumulative_gas_used: Option<Quantity>,
    /// The sum of the base fee and tip paid per unit of gas.
    pub effective_gas_price: Option<Quantity>,
    /// Gas used by transaction
    pub gas_used: Option<Quantity>,
    /// Address of created contract if transaction was a contract creation
    pub contract_address: Option<Address>,
    /// Bloom filter for logs produced by this transaction
    pub logs_bloom: Option<BloomFilter>,
    /// Transaction type. For ethereum: Legacy, Eip2930, Eip1559, Eip4844
    #[serde(rename = "type")]
    pub kind: Option<TransactionType>,
    /// The Keccak 256-bit hash of the root node of the trie structure populated with each
    /// transaction in the transactions list portion of the block; formally Ht.
    pub root: Option<Hash>,
    /// If transaction is executed successfully.
    ///
    /// This is the `statusCode`
    pub status: Option<TransactionStatus>,
    /// The fee associated with a transaction on the Layer 1,
    /// it is calculated as l1GasPrice multiplied by l1GasUsed
    pub l1_fee: Option<Quantity>,
    /// The gas price for transactions on the Layer 1
    pub l1_gas_price: Option<Quantity>,
    /// The amount of gas consumed by a transaction on the Layer 1
    pub l1_gas_used: Option<Quantity>,
    /// A multiplier applied to the actual gas usage on Layer 1 to calculate the dynamic costs.
    /// If set to 1, it has no impact on the L1 gas usage
    pub l1_fee_scalar: Option<f64>,
    /// Amount of gas spent on L1 calldata in units of L2 gas.
    pub gas_used_for_l1: Option<Quantity>,
    /// Gas price for blob transactions
    pub blob_gas_price: Option<Quantity>,
    /// Amount of blob gas used by this transaction
    pub blob_gas_used: Option<Quantity>,
    /// Deposit transaction nonce for Optimism
    pub deposit_nonce: Option<Quantity>,
    /// Deposit receipt version for Optimism
    pub deposit_receipt_version: Option<Quantity>,
    /// Base fee scalar for L1 cost calculation
    pub l1_base_fee_scalar: Option<Quantity>,
    /// L1 blob base fee for cost calculation
    pub l1_blob_base_fee: Option<Quantity>,
    /// L1 blob base fee scalar for cost calculation
    pub l1_blob_base_fee_scalar: Option<Quantity>,
    /// L1 block number associated with transaction
    pub l1_block_number: Option<Quantity>,
    /// Amount of ETH minted in this transaction
    pub mint: Option<Quantity>,
    /// 4-byte function signature hash
    pub sighash: Option<Data>,
    /// Source hash for optimism transactions
    pub source_hash: Option<Hash>,
}

/// Log object
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Log {
    /// The boolean value indicating if the event was removed from the blockchain due
    /// to a chain reorganization. True if the log was removed. False if it is a valid log.
    pub removed: Option<bool>,
    /// The integer identifying the index of the event within the block's list of events.
    pub log_index: Option<LogIndex>,
    /// The integer index of the transaction within the block's list of transactions.
    pub transaction_index: Option<TransactionIndex>,
    /// The hash of the transaction that triggered the event.
    pub transaction_hash: Option<Hash>,
    /// The hash of the block in which the event was included.
    pub block_hash: Option<Hash>,
    /// The block number in which the event was included.
    pub block_number: Option<BlockNumber>,
    /// The contract address from which the event originated.
    pub address: Option<Address>,
    /// The non-indexed data that was emitted along with the event.
    pub data: Option<Data>,
    /// An array of 32-byte data fields containing indexed event parameters.
    pub topics: ArrayVec<Option<LogArgument>, 4>,
}

/// Trace object
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Trace {
    /// The address of the sender who initiated the transaction.
    pub from: Option<Address>,
    /// The address of the recipient of the transaction if it was a transaction to an address.
    /// For contract creation transactions, this field is None.
    pub to: Option<Address>,
    /// The type of trace, `call` or `delegatecall`, two ways to invoke a function in a smart contract.
    ///
    /// `call` creates a new environment for the function to work in, so changes made in that
    /// function won't affect the environment where the function was called.
    ///
    /// `delegatecall` doesn't create a new environment. Instead, it runs the function within the
    /// environment of the caller, so changes made in that function will affect the caller's environment.
    pub call_type: Option<String>,
    /// The units of gas included in the transaction by the sender.
    pub gas: Option<Quantity>,
    /// The optional input data sent with the transaction, usually used to interact with smart contracts.
    pub input: Option<Data>,
    /// The init code.
    pub init: Option<Data>,
    /// The value of the native token transferred along with the transaction, in Wei.
    pub value: Option<Quantity>,
    /// The address of the receiver for reward transaction.
    pub author: Option<Address>,
    /// Kind of reward. `Block` reward or `Uncle` reward.
    pub reward_type: Option<String>,
    /// The hash of the block in which the transaction was included.
    pub block_hash: Option<Hash>,
    /// The number of the block in which the transaction was included.
    pub block_number: Option<u64>,
    /// Destroyed address.
    pub address: Option<Address>,
    /// Contract code.
    pub code: Option<Data>,
    /// The total used gas by the call, encoded as hexadecimal.
    pub gas_used: Option<Quantity>,
    /// The return value of the call, encoded as a hexadecimal string.
    pub output: Option<Data>,
    /// The number of sub-traces created during execution. When a transaction is executed on the EVM,
    /// it may trigger additional sub-executions, such as when a smart contract calls another smart
    /// contract or when an external account is accessed.
    pub subtraces: Option<u64>,
    /// An array that indicates the position of the transaction in the trace.
    pub trace_address: Option<Vec<u64>>,
    /// The hash of the transaction.
    pub transaction_hash: Option<Hash>,
    /// The index of the transaction in the block.
    pub transaction_position: Option<u64>,
    /// The type of action taken by the transaction, `call`, `create`, `reward` and `suicide`.
    ///
    /// `call` is the most common type of trace and occurs when a smart contract invokes another contract's function.
    ///
    /// `create` represents the creation of a new smart contract. This type of trace occurs when a smart contract is deployed to the blockchain.
    #[serde(rename = "type")]
    pub kind: Option<String>,
    /// A string that indicates whether the transaction was successful or not.
    ///
    /// None if successful, Reverted if not.
    pub error: Option<String>,
    /// 4-byte function signature hash for the trace
    pub sighash: Option<Data>,
    /// The action address for traces that create contracts
    pub action_address: Option<Address>,
    /// The balance associated with the trace operation
    pub balance: Option<Quantity>,
    /// The refund address for refund operations
    pub refund_address: Option<Address>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn has_tx_field(tx: &Transaction, field: TransactionField) -> bool {
        match field {
            TransactionField::BlockHash => tx.block_hash.is_none(),
            TransactionField::BlockNumber => tx.block_number.is_none(),
            TransactionField::From => tx.from.is_none(),
            TransactionField::Gas => tx.gas.is_none(),
            TransactionField::Hash => tx.hash.is_none(),
            TransactionField::Input => tx.input.is_none(),
            TransactionField::Nonce => tx.nonce.is_none(),
            TransactionField::TransactionIndex => tx.transaction_index.is_none(),
            TransactionField::Value => tx.value.is_none(),
            TransactionField::CumulativeGasUsed => tx.cumulative_gas_used.is_none(),
            TransactionField::EffectiveGasPrice => tx.effective_gas_price.is_none(),
            TransactionField::GasUsed => tx.gas_used.is_none(),
            TransactionField::LogsBloom => tx.logs_bloom.is_none(),
            TransactionField::GasPrice => tx.gas_price.is_none(),
            TransactionField::To => tx.to.is_none(),
            TransactionField::V => tx.v.is_none(),
            TransactionField::R => tx.r.is_none(),
            TransactionField::S => tx.s.is_none(),
            TransactionField::MaxPriorityFeePerGas => tx.max_priority_fee_per_gas.is_none(),
            TransactionField::MaxFeePerGas => tx.max_fee_per_gas.is_none(),
            TransactionField::ChainId => tx.chain_id.is_none(),
            TransactionField::ContractAddress => tx.contract_address.is_none(),
            TransactionField::Type => tx.kind.is_none(),
            TransactionField::Root => tx.root.is_none(),
            TransactionField::Status => tx.status.is_none(),
            TransactionField::YParity => tx.y_parity.is_none(),
            TransactionField::AccessList => tx.access_list.is_none(),
            TransactionField::AuthorizationList => tx.authorization_list.is_none(),
            TransactionField::L1Fee => tx.l1_fee.is_none(),
            TransactionField::L1GasPrice => tx.l1_gas_price.is_none(),
            TransactionField::L1GasUsed => tx.l1_gas_used.is_none(),
            TransactionField::L1FeeScalar => tx.l1_fee_scalar.is_none(),
            TransactionField::GasUsedForL1 => tx.gas_used_for_l1.is_none(),
            TransactionField::MaxFeePerBlobGas => tx.max_fee_per_blob_gas.is_none(),
            TransactionField::BlobVersionedHashes => tx.blob_versioned_hashes.is_none(),
            TransactionField::BlobGasPrice => tx.blob_gas_price.is_none(),
            TransactionField::BlobGasUsed => tx.blob_gas_used.is_none(),
            TransactionField::DepositNonce => tx.deposit_nonce.is_none(),
            TransactionField::DepositReceiptVersion => tx.deposit_receipt_version.is_none(),
            TransactionField::L1BaseFeeScalar => tx.l1_base_fee_scalar.is_none(),
            TransactionField::L1BlobBaseFee => tx.l1_blob_base_fee.is_none(),
            TransactionField::L1BlobBaseFeeScalar => tx.l1_blob_base_fee_scalar.is_none(),
            TransactionField::L1BlockNumber => tx.l1_block_number.is_none(),
            TransactionField::Mint => tx.mint.is_none(),
            TransactionField::Sighash => tx.sighash.is_none(),
            TransactionField::SourceHash => tx.source_hash.is_none(),
        }
    }

    fn has_block_field(block: &Block, field: BlockField) -> bool {
        match field {
            BlockField::Number => block.number.is_none(),
            BlockField::Hash => block.hash.is_none(),
            BlockField::ParentHash => block.parent_hash.is_none(),
            BlockField::Nonce => block.nonce.is_none(),
            BlockField::Sha3Uncles => block.sha3_uncles.is_none(),
            BlockField::LogsBloom => block.logs_bloom.is_none(),
            BlockField::TransactionsRoot => block.transactions_root.is_none(),
            BlockField::StateRoot => block.state_root.is_none(),
            BlockField::ReceiptsRoot => block.receipts_root.is_none(),
            BlockField::Miner => block.miner.is_none(),
            BlockField::Difficulty => block.difficulty.is_none(),
            BlockField::TotalDifficulty => block.total_difficulty.is_none(),
            BlockField::ExtraData => block.extra_data.is_none(),
            BlockField::Size => block.size.is_none(),
            BlockField::GasLimit => block.gas_limit.is_none(),
            BlockField::GasUsed => block.gas_used.is_none(),
            BlockField::Timestamp => block.timestamp.is_none(),
            BlockField::Uncles => block.uncles.is_none(),
            BlockField::BaseFeePerGas => block.base_fee_per_gas.is_none(),
            BlockField::BlobGasUsed => block.blob_gas_used.is_none(),
            BlockField::ExcessBlobGas => block.excess_blob_gas.is_none(),
            BlockField::ParentBeaconBlockRoot => block.parent_beacon_block_root.is_none(),
            BlockField::WithdrawalsRoot => block.withdrawals_root.is_none(),
            BlockField::Withdrawals => block.withdrawals.is_none(),
            BlockField::L1BlockNumber => block.l1_block_number.is_none(),
            BlockField::SendCount => block.send_count.is_none(),
            BlockField::SendRoot => block.send_root.is_none(),
            BlockField::MixHash => block.mix_hash.is_none(),
        }
    }

    fn has_log_field(log: &Log, field: LogField) -> bool {
        match field {
            LogField::Removed => log.removed.is_none(),
            LogField::LogIndex => log.log_index.is_none(),
            LogField::TransactionIndex => log.transaction_index.is_none(),
            LogField::TransactionHash => log.transaction_hash.is_none(),
            LogField::BlockHash => log.block_hash.is_none(),
            LogField::BlockNumber => log.block_number.is_none(),
            LogField::Address => log.address.is_none(),
            LogField::Data => log.data.is_none(),
            LogField::Topic0 => log.topics.get(0).map_or(true, |t| t.is_none()),
            LogField::Topic1 => log.topics.get(1).map_or(true, |t| t.is_none()),
            LogField::Topic2 => log.topics.get(2).map_or(true, |t| t.is_none()),
            LogField::Topic3 => log.topics.get(3).map_or(true, |t| t.is_none()),
        }
    }

    fn has_trace_field(trace: &Trace, field: TraceField) -> bool {
        match field {
            TraceField::From => trace.from.is_none(),
            TraceField::To => trace.to.is_none(),
            TraceField::CallType => trace.call_type.is_none(),
            TraceField::Gas => trace.gas.is_none(),
            TraceField::Input => trace.input.is_none(),
            TraceField::Init => trace.init.is_none(),
            TraceField::Value => trace.value.is_none(),
            TraceField::Author => trace.author.is_none(),
            TraceField::RewardType => trace.reward_type.is_none(),
            TraceField::BlockHash => trace.block_hash.is_none(),
            TraceField::BlockNumber => trace.block_number.is_none(),
            TraceField::Address => trace.address.is_none(),
            TraceField::Code => trace.code.is_none(),
            TraceField::GasUsed => trace.gas_used.is_none(),
            TraceField::Output => trace.output.is_none(),
            TraceField::Subtraces => trace.subtraces.is_none(),
            TraceField::TraceAddress => trace.trace_address.is_none(),
            TraceField::TransactionHash => trace.transaction_hash.is_none(),
            TraceField::TransactionPosition => trace.transaction_position.is_none(),
            TraceField::Type => trace.kind.is_none(),
            TraceField::Error => trace.error.is_none(),
            TraceField::Sighash => trace.sighash.is_none(),
            TraceField::ActionAddress => trace.action_address.is_none(),
            TraceField::Balance => trace.balance.is_none(),
            TraceField::RefundAddress => trace.refund_address.is_none(),
        }
    }

    #[test]
    fn has_all_tx_fields() {
        let tx = Transaction::default();
        for field in TransactionField::all() {
            assert!(has_tx_field(&tx, field));
        }
    }

    #[test]
    fn has_all_block_fields() {
        let block = Block::default();
        for field in BlockField::all() {
            assert!(has_block_field(&block, field));
        }
    }

    #[test]
    fn has_all_log_fields() {
        let log = Log::default();
        for field in LogField::all() {
            assert!(has_log_field(&log, field));
        }
    }

    #[test]
    fn has_all_trace_fields() {
        let trace = Trace::default();
        for field in TraceField::all() {
            assert!(has_trace_field(&trace, field));
        }
    }
}
