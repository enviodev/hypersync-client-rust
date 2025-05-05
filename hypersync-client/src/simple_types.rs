//! Base object types for the Hypersync client.
use std::{collections::HashMap, sync::Arc};

use arrayvec::ArrayVec;
use hypersync_format::{
    AccessList, Address, Authorization, BlockNumber, BloomFilter, Data, Hash, LogArgument,
    LogIndex, Nonce, Quantity, TransactionIndex, TransactionStatus, TransactionType, Withdrawal,
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

impl From<ResponseData> for Vec<Event> {
    fn from(data: ResponseData) -> Self {
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
                    let block = blocks.get(&log.block_number.unwrap().into()).cloned();
                    let transaction = transactions
                        .get(log.transaction_hash.as_ref().unwrap())
                        .cloned();

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
}
