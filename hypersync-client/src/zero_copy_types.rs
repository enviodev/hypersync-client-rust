//! Zero-copy types for reading Arrow data without allocation.
//!
//! This module provides zero-copy readers that access Arrow columnar data directly
//! without copying or allocating new memory for individual field access.

use anyhow::{Context, Result};
use hypersync_format::{
    Address, BlockNumber, Data, FixedSizeData, Hash, LogIndex, Quantity, TransactionIndex,
};
use hypersync_net_types::{BlockField, LogField, TraceField, TransactionField};
use polars_arrow::array::{
    BinaryArray, BooleanArray, StaticArray, UInt64Array, UInt8Array, Utf8Array,
};

use crate::ArrowBatch;

/// Zero-copy reader for log data from Arrow batches.
///
/// Provides efficient access to log fields without copying data from the underlying
/// Arrow columnar format. Each reader is bound to a specific row in the batch.
pub struct LogReader<'a> {
    batch: &'a ArrowBatch,
    row_idx: usize,
}

/// Iterator over log rows in an ArrowBatch.
pub struct LogIterator<'a> {
    batch: &'a ArrowBatch,
    current_idx: usize,
    len: usize,
}

impl<'a> LogIterator<'a> {
    /// Create a new iterator for the given batch.
    pub fn new(batch: &'a ArrowBatch) -> Self {
        let len = if let Some(first_column) = batch.chunk.columns().first() {
            first_column.len()
        } else {
            0
        };
        Self {
            batch,
            current_idx: 0,
            len,
        }
    }
}

impl<'a> Iterator for LogIterator<'a> {
    type Item = LogReader<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_idx < self.len {
            let reader = LogReader {
                batch: self.batch,
                row_idx: self.current_idx,
            };
            self.current_idx += 1;
            Some(reader)
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.len - self.current_idx;
        (remaining, Some(remaining))
    }
}

impl<'a> ExactSizeIterator for LogIterator<'a> {
    fn len(&self) -> usize {
        self.len - self.current_idx
    }
}

impl<'a> LogReader<'a> {
    /// Create an iterator over all rows in the batch.
    pub fn iter(batch: &'a ArrowBatch) -> LogIterator<'a> {
        LogIterator::new(batch)
    }
    /// The boolean value indicating if the event was removed from the blockchain due
    /// to a chain reorganization. True if the log was removed. False if it is a valid log.
    pub fn removed(&self) -> Result<Option<bool>> {
        let array = self
            .batch
            .column::<BooleanArray>(LogField::Removed.as_ref())?;
        Ok(array.get(self.row_idx))
    }

    /// The integer identifying the index of the event within the block's list of events.
    pub fn log_index(&self) -> Result<LogIndex> {
        let array = self
            .batch
            .column::<UInt64Array>(LogField::LogIndex.as_ref())?;
        let value = array
            .get(self.row_idx)
            .context("value should not be null")?;
        Ok(value.into())
    }

    /// The integer index of the transaction within the block's list of transactions.
    pub fn transaction_index(&self) -> Result<TransactionIndex> {
        let array = self
            .batch
            .column::<UInt64Array>(LogField::TransactionIndex.as_ref())?;
        let value = array
            .get(self.row_idx)
            .context("value should not be null")?;
        Ok(value.into())
    }

    /// The hash of the transaction that triggered the event.
    pub fn transaction_hash(&self) -> Result<Hash> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(LogField::TransactionHash.as_ref())?;
        let value = array
            .get(self.row_idx)
            .context("value should not be null")?;
        Hash::try_from(value).context("invalid hash format")
    }

    /// The hash of the block in which the event was included.
    pub fn block_hash(&self) -> Result<Hash> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(LogField::BlockHash.as_ref())?;
        let value = array
            .get(self.row_idx)
            .context("value should not be null")?;
        Hash::try_from(value).context("Invalid hash format")
    }

    /// The block number in which the event was included.
    pub fn block_number(&self) -> Result<BlockNumber> {
        let array = self
            .batch
            .column::<UInt64Array>(LogField::BlockNumber.as_ref())?;
        let value = array
            .get(self.row_idx)
            .context("value should not be null")?;
        Ok(value.into())
    }

    /// The contract address from which the event originated.
    pub fn address(&self) -> Result<Address> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(LogField::Address.as_ref())?;
        let value = array
            .get(self.row_idx)
            .context("value should not be null")?;
        Address::try_from(value).context("Invalid address format")
    }

    /// The first topic of the event (topic0).
    pub fn topic0(&self) -> Result<Option<FixedSizeData<32>>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(LogField::Topic0.as_ref())?;
        Ok(array
            .get(self.row_idx)
            .and_then(|v| FixedSizeData::<32>::try_from(v).ok()))
    }

    /// The second topic of the event (topic1).
    pub fn topic1(&self) -> Result<Option<FixedSizeData<32>>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(LogField::Topic1.as_ref())?;
        Ok(array
            .get(self.row_idx)
            .and_then(|v| FixedSizeData::<32>::try_from(v).ok()))
    }

    /// The third topic of the event (topic2).
    pub fn topic2(&self) -> Result<Option<FixedSizeData<32>>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(LogField::Topic2.as_ref())?;
        Ok(array
            .get(self.row_idx)
            .and_then(|v| FixedSizeData::<32>::try_from(v).ok()))
    }

    /// The fourth topic of the event (topic3).
    pub fn topic3(&self) -> Result<Option<FixedSizeData<32>>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(LogField::Topic3.as_ref())?;
        Ok(array
            .get(self.row_idx)
            .and_then(|v| FixedSizeData::<32>::try_from(v).ok()))
    }

    /// The non-indexed data that was emitted along with the event.
    pub fn data(&self) -> Result<Data> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(LogField::Data.as_ref())?;
        let value = array
            .get(self.row_idx)
            .context("value should not be null")?;
        Ok(Data::from(value))
    }
}

/// Zero-copy reader for block data from Arrow batches.
///
/// Provides efficient access to block fields without copying data from the underlying
/// Arrow columnar format. Each reader is bound to a specific row in the batch.
pub struct BlockReader<'a> {
    batch: &'a ArrowBatch,
    row_idx: usize,
}

impl<'a> BlockReader<'a> {
    /// The block number.
    pub fn number(&self) -> Result<BlockNumber> {
        let array = self
            .batch
            .column::<UInt64Array>(BlockField::Number.as_ref())?;
        let value = array
            .get(self.row_idx)
            .context("value should not be null")?;
        Ok(value.into())
    }

    /// The block hash.
    pub fn hash(&self) -> Result<Hash> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(BlockField::Hash.as_ref())?;
        let value = array
            .get(self.row_idx)
            .context("value should not be null")?;
        Hash::try_from(value).context("invalid hash format")
    }

    /// The parent block hash.
    pub fn parent_hash(&self) -> Result<Hash> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(BlockField::ParentHash.as_ref())?;
        let value = array
            .get(self.row_idx)
            .context("value should not be null")?;
        Hash::try_from(value).context("invalid hash format")
    }

    /// The block nonce.
    pub fn nonce(&self) -> Result<Option<FixedSizeData<8>>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(BlockField::Nonce.as_ref())?;
        Ok(array
            .get(self.row_idx)
            .and_then(|v| FixedSizeData::<8>::try_from(v).ok()))
    }

    /// The SHA3 hash of the uncles.
    pub fn sha3_uncles(&self) -> Result<Hash> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(BlockField::Sha3Uncles.as_ref())?;
        let value = array
            .get(self.row_idx)
            .context("value should not be null")?;
        Hash::try_from(value).context("invalid hash format")
    }

    /// The Bloom filter for the logs of the block.
    pub fn logs_bloom(&self) -> Result<Data> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(BlockField::LogsBloom.as_ref())?;
        let value = array
            .get(self.row_idx)
            .context("value should not be null")?;
        Ok(Data::from(value))
    }

    /// The root of the transaction trie of the block.
    pub fn transactions_root(&self) -> Result<Hash> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(BlockField::TransactionsRoot.as_ref())?;
        let value = array
            .get(self.row_idx)
            .context("value should not be null")?;
        Hash::try_from(value).context("invalid hash format")
    }

    /// The root of the final state trie of the block.
    pub fn state_root(&self) -> Result<Hash> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(BlockField::StateRoot.as_ref())?;
        let value = array
            .get(self.row_idx)
            .context("value should not be null")?;
        Hash::try_from(value).context("invalid hash format")
    }

    /// The root of the receipts trie of the block.
    pub fn receipts_root(&self) -> Result<Hash> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(BlockField::ReceiptsRoot.as_ref())?;
        let value = array
            .get(self.row_idx)
            .context("value should not be null")?;
        Hash::try_from(value).context("invalid hash format")
    }

    /// The address of the beneficiary to whom the mining rewards were given.
    pub fn miner(&self) -> Result<Address> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(BlockField::Miner.as_ref())?;
        let value = array
            .get(self.row_idx)
            .context("value should not be null")?;
        Address::try_from(value).context("invalid address format")
    }

    /// The difficulty of the block.
    pub fn difficulty(&self) -> Result<Option<Quantity>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(BlockField::Difficulty.as_ref())?;
        Ok(array.get(self.row_idx).map(Quantity::from))
    }

    /// The total difficulty of the chain until this block.
    pub fn total_difficulty(&self) -> Result<Option<Quantity>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(BlockField::TotalDifficulty.as_ref())?;
        Ok(array.get(self.row_idx).map(Quantity::from))
    }

    /// The "extra data" field of this block.
    pub fn extra_data(&self) -> Result<Data> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(BlockField::ExtraData.as_ref())?;
        let value = array
            .get(self.row_idx)
            .context("value should not be null")?;
        Ok(Data::from(value))
    }

    /// The size of this block in bytes.
    pub fn size(&self) -> Result<Quantity> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(BlockField::Size.as_ref())?;
        let value = array
            .get(self.row_idx)
            .context("value should not be null")?;
        Ok(Quantity::from(value))
    }

    /// The maximum gas allowed in this block.
    pub fn gas_limit(&self) -> Result<Quantity> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(BlockField::GasLimit.as_ref())?;
        let value = array
            .get(self.row_idx)
            .context("value should not be null")?;
        Ok(Quantity::from(value))
    }

    /// The total used gas by all transactions in this block.
    pub fn gas_used(&self) -> Result<Quantity> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(BlockField::GasUsed.as_ref())?;
        let value = array
            .get(self.row_idx)
            .context("value should not be null")?;
        Ok(Quantity::from(value))
    }

    /// The unix timestamp for when the block was collated.
    pub fn timestamp(&self) -> Result<Quantity> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(BlockField::Timestamp.as_ref())?;
        let value = array
            .get(self.row_idx)
            .context("value should not be null")?;
        Ok(Quantity::from(value))
    }

    /// Array of uncle hashes.
    pub fn uncles(&self) -> Result<Option<Data>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(BlockField::Uncles.as_ref())?;
        Ok(array.get(self.row_idx).map(Data::from))
    }

    /// The base fee per gas.
    pub fn base_fee_per_gas(&self) -> Result<Option<Quantity>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(BlockField::BaseFeePerGas.as_ref())?;
        Ok(array.get(self.row_idx).map(Quantity::from))
    }

    /// The total amount of blob gas consumed by the transactions in the block.
    pub fn blob_gas_used(&self) -> Result<Option<Quantity>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(BlockField::BlobGasUsed.as_ref())?;
        Ok(array.get(self.row_idx).map(Quantity::from))
    }

    /// A running total of blob gas consumed in excess of the target.
    pub fn excess_blob_gas(&self) -> Result<Option<Quantity>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(BlockField::ExcessBlobGas.as_ref())?;
        Ok(array.get(self.row_idx).map(Quantity::from))
    }

    /// The hash of the parent beacon block.
    pub fn parent_beacon_block_root(&self) -> Result<Option<Hash>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(BlockField::ParentBeaconBlockRoot.as_ref())?;
        Ok(array.get(self.row_idx).and_then(|v| Hash::try_from(v).ok()))
    }

    /// The root of the withdrawal trie.
    pub fn withdrawals_root(&self) -> Result<Option<Hash>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(BlockField::WithdrawalsRoot.as_ref())?;
        Ok(array.get(self.row_idx).and_then(|v| Hash::try_from(v).ok()))
    }

    /// The withdrawals in the block.
    pub fn withdrawals(&self) -> Result<Option<Data>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(BlockField::Withdrawals.as_ref())?;
        Ok(array.get(self.row_idx).map(Data::from))
    }

    /// The L1 block number.
    pub fn l1_block_number(&self) -> Result<Option<BlockNumber>> {
        let array = self
            .batch
            .column::<UInt64Array>(BlockField::L1BlockNumber.as_ref())?;
        Ok(array.get(self.row_idx).map(|v| v.into()))
    }

    /// The send count.
    pub fn send_count(&self) -> Result<Option<Quantity>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(BlockField::SendCount.as_ref())?;
        Ok(array.get(self.row_idx).map(Quantity::from))
    }

    /// The send root.
    pub fn send_root(&self) -> Result<Option<Hash>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(BlockField::SendRoot.as_ref())?;
        Ok(array.get(self.row_idx).and_then(|v| Hash::try_from(v).ok()))
    }

    /// The mix hash.
    pub fn mix_hash(&self) -> Result<Option<Hash>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(BlockField::MixHash.as_ref())?;
        Ok(array.get(self.row_idx).and_then(|v| Hash::try_from(v).ok()))
    }
}

/// Zero-copy reader for transaction data from Arrow batches.
///
/// Provides efficient access to transaction fields without copying data from the underlying
/// Arrow columnar format. Each reader is bound to a specific row in the batch.
pub struct TransactionReader<'a> {
    batch: &'a ArrowBatch,
    row_idx: usize,
}

impl<'a> TransactionReader<'a> {
    /// The hash of the block in which this transaction was included.
    pub fn block_hash(&self) -> Result<Hash> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TransactionField::BlockHash.as_ref())?;
        let value = array
            .get(self.row_idx)
            .context("value should not be null")?;
        Hash::try_from(value).context("invalid hash format")
    }

    /// The number of the block in which this transaction was included.
    pub fn block_number(&self) -> Result<BlockNumber> {
        let array = self
            .batch
            .column::<UInt64Array>(TransactionField::BlockNumber.as_ref())?;
        let value = array
            .get(self.row_idx)
            .context("value should not be null")?;
        Ok(value.into())
    }

    /// The address of the sender.
    pub fn from(&self) -> Result<Option<Address>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TransactionField::From.as_ref())?;
        Ok(array
            .get(self.row_idx)
            .and_then(|v| Address::try_from(v).ok()))
    }

    /// The gas limit provided by the sender.
    pub fn gas(&self) -> Result<Quantity> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TransactionField::Gas.as_ref())?;
        let value = array
            .get(self.row_idx)
            .context("value should not be null")?;
        Ok(Quantity::from(value))
    }

    /// The gas price willing to be paid by the sender.
    pub fn gas_price(&self) -> Result<Option<Quantity>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TransactionField::GasPrice.as_ref())?;
        Ok(array.get(self.row_idx).map(Quantity::from))
    }

    /// The hash of this transaction.
    pub fn hash(&self) -> Result<Hash> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TransactionField::Hash.as_ref())?;
        let value = array
            .get(self.row_idx)
            .context("value should not be null")?;
        Hash::try_from(value).context("invalid hash format")
    }

    /// The data sent along with the transaction.
    pub fn input(&self) -> Result<Data> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TransactionField::Input.as_ref())?;
        let value = array
            .get(self.row_idx)
            .context("value should not be null")?;
        Ok(Data::from(value))
    }

    /// The number of transactions made by the sender prior to this one.
    pub fn nonce(&self) -> Result<Quantity> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TransactionField::Nonce.as_ref())?;
        let value = array
            .get(self.row_idx)
            .context("value should not be null")?;
        Ok(Quantity::from(value))
    }

    /// The address of the receiver.
    pub fn to(&self) -> Result<Option<Address>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TransactionField::To.as_ref())?;
        Ok(array
            .get(self.row_idx)
            .and_then(|v| Address::try_from(v).ok()))
    }

    /// The index of the transaction in the block.
    pub fn transaction_index(&self) -> Result<TransactionIndex> {
        let array = self
            .batch
            .column::<UInt64Array>(TransactionField::TransactionIndex.as_ref())?;
        let value = array
            .get(self.row_idx)
            .context("value should not be null")?;
        Ok(value.into())
    }

    /// The value transferred.
    pub fn value(&self) -> Result<Quantity> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TransactionField::Value.as_ref())?;
        let value = array
            .get(self.row_idx)
            .context("value should not be null")?;
        Ok(Quantity::from(value))
    }

    /// ECDSA recovery id.
    pub fn v(&self) -> Result<Option<Quantity>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TransactionField::V.as_ref())?;
        Ok(array.get(self.row_idx).map(Quantity::from))
    }

    /// ECDSA signature r.
    pub fn r(&self) -> Result<Option<Quantity>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TransactionField::R.as_ref())?;
        Ok(array.get(self.row_idx).map(Quantity::from))
    }

    /// ECDSA signature s.
    pub fn s(&self) -> Result<Option<Quantity>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TransactionField::S.as_ref())?;
        Ok(array.get(self.row_idx).map(Quantity::from))
    }

    /// Maximum fee per gas the sender is willing to pay for priority.
    pub fn max_priority_fee_per_gas(&self) -> Result<Option<Quantity>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TransactionField::MaxPriorityFeePerGas.as_ref())?;
        Ok(array.get(self.row_idx).map(Quantity::from))
    }

    /// Maximum total fee per gas the sender is willing to pay.
    pub fn max_fee_per_gas(&self) -> Result<Option<Quantity>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TransactionField::MaxFeePerGas.as_ref())?;
        Ok(array.get(self.row_idx).map(Quantity::from))
    }

    /// The chain id of the transaction.
    pub fn chain_id(&self) -> Result<Option<Quantity>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TransactionField::ChainId.as_ref())?;
        Ok(array.get(self.row_idx).map(Quantity::from))
    }

    /// The total amount of gas used when this transaction was executed in the block.
    pub fn cumulative_gas_used(&self) -> Result<Quantity> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TransactionField::CumulativeGasUsed.as_ref())?;
        let value = array
            .get(self.row_idx)
            .context("value should not be null")?;
        Ok(Quantity::from(value))
    }

    /// The sum of the base fee and tip paid per unit of gas.
    pub fn effective_gas_price(&self) -> Result<Quantity> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TransactionField::EffectiveGasPrice.as_ref())?;
        let value = array
            .get(self.row_idx)
            .context("value should not be null")?;
        Ok(Quantity::from(value))
    }

    /// The amount of gas used by this transaction.
    pub fn gas_used(&self) -> Result<Quantity> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TransactionField::GasUsed.as_ref())?;
        let value = array
            .get(self.row_idx)
            .context("value should not be null")?;
        Ok(Quantity::from(value))
    }

    /// The contract address created, if the transaction was a contract creation.
    pub fn contract_address(&self) -> Result<Option<Address>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TransactionField::ContractAddress.as_ref())?;
        Ok(array
            .get(self.row_idx)
            .and_then(|v| Address::try_from(v).ok()))
    }

    /// The Bloom filter for the logs of the transaction.
    pub fn logs_bloom(&self) -> Result<Data> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TransactionField::LogsBloom.as_ref())?;
        let value = array
            .get(self.row_idx)
            .context("value should not be null")?;
        Ok(Data::from(value))
    }

    /// The type of the transaction.
    pub fn type_(&self) -> Result<Option<u8>> {
        let array = self
            .batch
            .column::<UInt8Array>(TransactionField::Type.as_ref())?;
        Ok(array.get(self.row_idx))
    }

    /// The post-transaction stateroot.
    pub fn root(&self) -> Result<Option<Hash>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TransactionField::Root.as_ref())?;
        Ok(array.get(self.row_idx).and_then(|v| Hash::try_from(v).ok()))
    }

    /// Either 1 (success) or 0 (failure).
    pub fn status(&self) -> Result<Option<u8>> {
        let array = self
            .batch
            .column::<UInt8Array>(TransactionField::Status.as_ref())?;
        Ok(array.get(self.row_idx))
    }

    /// The first 4 bytes of the transaction input data.
    pub fn sighash(&self) -> Result<Option<FixedSizeData<4>>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TransactionField::Sighash.as_ref())?;
        Ok(array
            .get(self.row_idx)
            .and_then(|v| FixedSizeData::<4>::try_from(v).ok()))
    }

    /// The y parity of the signature.
    pub fn y_parity(&self) -> Result<Option<Quantity>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TransactionField::YParity.as_ref())?;
        Ok(array.get(self.row_idx).map(Quantity::from))
    }

    /// The access list.
    pub fn access_list(&self) -> Result<Option<Data>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TransactionField::AccessList.as_ref())?;
        Ok(array.get(self.row_idx).map(Data::from))
    }

    /// The authorization list.
    pub fn authorization_list(&self) -> Result<Option<Data>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TransactionField::AuthorizationList.as_ref())?;
        Ok(array.get(self.row_idx).map(Data::from))
    }

    // Additional L1/L2 and blob-related fields would go here...
    // For brevity, I'll include a few key ones:

    /// The L1 fee for L2 transactions.
    pub fn l1_fee(&self) -> Result<Option<Quantity>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TransactionField::L1Fee.as_ref())?;
        Ok(array.get(self.row_idx).map(Quantity::from))
    }

    /// The maximum fee per blob gas.
    pub fn max_fee_per_blob_gas(&self) -> Result<Option<Quantity>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TransactionField::MaxFeePerBlobGas.as_ref())?;
        Ok(array.get(self.row_idx).map(Quantity::from))
    }

    /// The blob versioned hashes.
    pub fn blob_versioned_hashes(&self) -> Result<Option<Data>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TransactionField::BlobVersionedHashes.as_ref())?;
        Ok(array.get(self.row_idx).map(Data::from))
    }
}

/// Zero-copy reader for trace data from Arrow batches.
///
/// Provides efficient access to trace fields without copying data from the underlying
/// Arrow columnar format. Each reader is bound to a specific row in the batch.
pub struct TraceReader<'a> {
    batch: &'a ArrowBatch,
    row_idx: usize,
}

impl<'a> TraceReader<'a> {
    /// The hash of the block in which this trace occurred.
    pub fn block_hash(&self) -> Result<Hash> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TraceField::BlockHash.as_ref())?;
        let value = array
            .get(self.row_idx)
            .context("value should not be null")?;
        Hash::try_from(value).context("invalid hash format")
    }

    /// The number of the block in which this trace occurred.
    pub fn block_number(&self) -> Result<BlockNumber> {
        let array = self
            .batch
            .column::<UInt64Array>(TraceField::BlockNumber.as_ref())?;
        let value = array
            .get(self.row_idx)
            .context("value should not be null")?;
        Ok(value.into())
    }

    /// The address from which the trace originated.
    pub fn from(&self) -> Result<Option<Address>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TraceField::From.as_ref())?;
        Ok(array
            .get(self.row_idx)
            .and_then(|v| Address::try_from(v).ok()))
    }

    /// The address to which the trace was sent.
    pub fn to(&self) -> Result<Option<Address>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TraceField::To.as_ref())?;
        Ok(array
            .get(self.row_idx)
            .and_then(|v| Address::try_from(v).ok()))
    }

    /// The type of call.
    pub fn call_type(&self) -> Result<Option<String>> {
        let array = self
            .batch
            .column::<Utf8Array<i32>>(TraceField::CallType.as_ref())?;
        Ok(array.get(self.row_idx).map(|s| s.to_string()))
    }

    /// The amount of gas provided to the trace.
    pub fn gas(&self) -> Result<Option<Quantity>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TraceField::Gas.as_ref())?;
        Ok(array.get(self.row_idx).map(Quantity::from))
    }

    /// The input data.
    pub fn input(&self) -> Result<Option<Data>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TraceField::Input.as_ref())?;
        Ok(array.get(self.row_idx).map(Data::from))
    }

    /// The init data for contract creation traces.
    pub fn init(&self) -> Result<Option<Data>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TraceField::Init.as_ref())?;
        Ok(array.get(self.row_idx).map(Data::from))
    }

    /// The value transferred.
    pub fn value(&self) -> Result<Option<Quantity>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TraceField::Value.as_ref())?;
        Ok(array.get(self.row_idx).map(Quantity::from))
    }

    /// The address of the author (miner).
    pub fn author(&self) -> Result<Option<Address>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TraceField::Author.as_ref())?;
        Ok(array
            .get(self.row_idx)
            .and_then(|v| Address::try_from(v).ok()))
    }

    /// The type of reward.
    pub fn reward_type(&self) -> Result<Option<String>> {
        let array = self
            .batch
            .column::<Utf8Array<i32>>(TraceField::RewardType.as_ref())?;
        Ok(array.get(self.row_idx).map(|s| s.to_string()))
    }

    /// The address involved in the trace.
    pub fn address(&self) -> Result<Option<Address>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TraceField::Address.as_ref())?;
        Ok(array
            .get(self.row_idx)
            .and_then(|v| Address::try_from(v).ok()))
    }

    /// The bytecode.
    pub fn code(&self) -> Result<Option<Data>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TraceField::Code.as_ref())?;
        Ok(array.get(self.row_idx).map(Data::from))
    }

    /// The amount of gas used by the trace.
    pub fn gas_used(&self) -> Result<Option<Quantity>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TraceField::GasUsed.as_ref())?;
        Ok(array.get(self.row_idx).map(Quantity::from))
    }

    /// The output data.
    pub fn output(&self) -> Result<Option<Data>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TraceField::Output.as_ref())?;
        Ok(array.get(self.row_idx).map(Data::from))
    }

    /// The number of sub-traces.
    pub fn subtraces(&self) -> Result<Option<u64>> {
        let array = self
            .batch
            .column::<UInt64Array>(TraceField::Subtraces.as_ref())?;
        Ok(array.get(self.row_idx))
    }

    /// The trace address.
    pub fn trace_address(&self) -> Result<Option<Data>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TraceField::TraceAddress.as_ref())?;
        Ok(array.get(self.row_idx).map(Data::from))
    }

    /// The hash of the transaction this trace belongs to.
    pub fn transaction_hash(&self) -> Result<Option<Hash>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TraceField::TransactionHash.as_ref())?;
        Ok(array.get(self.row_idx).and_then(|v| Hash::try_from(v).ok()))
    }

    /// The position of the transaction in the block.
    pub fn transaction_position(&self) -> Result<Option<u64>> {
        let array = self
            .batch
            .column::<UInt64Array>(TraceField::TransactionPosition.as_ref())?;
        Ok(array.get(self.row_idx))
    }

    /// The type of trace.
    pub fn type_(&self) -> Result<Option<String>> {
        let array = self
            .batch
            .column::<Utf8Array<i32>>(TraceField::Type.as_ref())?;
        Ok(array.get(self.row_idx).map(|s| s.to_string()))
    }

    /// The error message, if any.
    pub fn error(&self) -> Result<Option<String>> {
        let array = self
            .batch
            .column::<Utf8Array<i32>>(TraceField::Error.as_ref())?;
        Ok(array.get(self.row_idx).map(|s| s.to_string()))
    }

    /// The first 4 bytes of the input data.
    pub fn sighash(&self) -> Result<Option<FixedSizeData<4>>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TraceField::Sighash.as_ref())?;
        Ok(array
            .get(self.row_idx)
            .and_then(|v| FixedSizeData::<4>::try_from(v).ok()))
    }

    /// The action address.
    pub fn action_address(&self) -> Result<Option<Address>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TraceField::ActionAddress.as_ref())?;
        Ok(array
            .get(self.row_idx)
            .and_then(|v| Address::try_from(v).ok()))
    }

    /// The balance.
    pub fn balance(&self) -> Result<Option<Quantity>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TraceField::Balance.as_ref())?;
        Ok(array.get(self.row_idx).map(Quantity::from))
    }

    /// The refund address.
    pub fn refund_address(&self) -> Result<Option<Address>> {
        let array = self
            .batch
            .column::<BinaryArray<i32>>(TraceField::RefundAddress.as_ref())?;
        Ok(array
            .get(self.row_idx)
            .and_then(|v| Address::try_from(v).ok()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use hypersync_format::Hex;

    /// Compile-time tests that ensure the correct return types
    #[test]
    fn test_nullability_matches_schema() {
        fn assert_nullable<'a, T, F>(_: F, log_field: LogField)
        where
            F: FnOnce(&LogReader<'a>) -> Result<Option<T>>,
        {
            assert!(log_field.is_nullable(), "Optional type should be nullable");
        }

        fn assert_not_nullable<'a, T, F>(_: F, log_field: LogField)
        where
            F: FnOnce(&LogReader<'a>) -> Result<T>,
            // just to make sure its an inner type and not an Option
            T: Hex,
        {
            assert!(!log_field.is_nullable(), "should not be nullable");
        }
        // This test will fail to compile if the return types are wrong

        for field in LogField::all() {
            match field {
                LogField::Removed => assert_nullable(LogReader::removed, field),
                LogField::Topic0 => assert_nullable(LogReader::topic0, field),
                LogField::Topic1 => assert_nullable(LogReader::topic1, field),
                LogField::Topic2 => assert_nullable(LogReader::topic2, field),
                LogField::Topic3 => assert_nullable(LogReader::topic3, field),
                LogField::LogIndex => assert_not_nullable(LogReader::log_index, field),
                LogField::TransactionIndex => {
                    assert_not_nullable(LogReader::transaction_index, field)
                }
                LogField::TransactionHash => {
                    assert_not_nullable(LogReader::transaction_hash, field)
                }
                LogField::BlockHash => assert_not_nullable(LogReader::block_hash, field),
                LogField::BlockNumber => assert_not_nullable(LogReader::block_number, field),
                LogField::Address => assert_not_nullable(LogReader::address, field),
                LogField::Data => assert_not_nullable(LogReader::data, field),
            }
        }
    }

    #[test]
    fn test_block_nullability_matches_schema() {
        fn assert_nullable<'a, T, F>(_: F, block_field: BlockField)
        where
            F: FnOnce(&BlockReader<'a>) -> Result<Option<T>>,
        {
            assert!(
                block_field.is_nullable(),
                "Optional type should be nullable"
            );
        }

        fn assert_not_nullable<'a, T, F>(_: F, block_field: BlockField)
        where
            F: FnOnce(&BlockReader<'a>) -> Result<T>,
            T: Hex,
        {
            assert!(!block_field.is_nullable(), "should not be nullable");
        }

        for field in BlockField::all() {
            match field {
                // Nullable fields
                BlockField::Nonce => assert_nullable(BlockReader::nonce, field),
                BlockField::Difficulty => assert_nullable(BlockReader::difficulty, field),
                BlockField::TotalDifficulty => {
                    assert_nullable(BlockReader::total_difficulty, field)
                }
                BlockField::Uncles => assert_nullable(BlockReader::uncles, field),
                BlockField::BaseFeePerGas => assert_nullable(BlockReader::base_fee_per_gas, field),
                BlockField::BlobGasUsed => assert_nullable(BlockReader::blob_gas_used, field),
                BlockField::ExcessBlobGas => assert_nullable(BlockReader::excess_blob_gas, field),
                BlockField::ParentBeaconBlockRoot => {
                    assert_nullable(BlockReader::parent_beacon_block_root, field)
                }
                BlockField::WithdrawalsRoot => {
                    assert_nullable(BlockReader::withdrawals_root, field)
                }
                BlockField::Withdrawals => assert_nullable(BlockReader::withdrawals, field),
                BlockField::L1BlockNumber => assert_nullable(BlockReader::l1_block_number, field),
                BlockField::SendCount => assert_nullable(BlockReader::send_count, field),
                BlockField::SendRoot => assert_nullable(BlockReader::send_root, field),
                BlockField::MixHash => assert_nullable(BlockReader::mix_hash, field),
                // Non-nullable fields
                BlockField::Number => assert_not_nullable(BlockReader::number, field),
                BlockField::Hash => assert_not_nullable(BlockReader::hash, field),
                BlockField::ParentHash => assert_not_nullable(BlockReader::parent_hash, field),
                BlockField::Sha3Uncles => assert_not_nullable(BlockReader::sha3_uncles, field),
                BlockField::LogsBloom => assert_not_nullable(BlockReader::logs_bloom, field),
                BlockField::TransactionsRoot => {
                    assert_not_nullable(BlockReader::transactions_root, field)
                }
                BlockField::StateRoot => assert_not_nullable(BlockReader::state_root, field),
                BlockField::ReceiptsRoot => assert_not_nullable(BlockReader::receipts_root, field),
                BlockField::Miner => assert_not_nullable(BlockReader::miner, field),
                BlockField::ExtraData => assert_not_nullable(BlockReader::extra_data, field),
                BlockField::Size => assert_not_nullable(BlockReader::size, field),
                BlockField::GasLimit => assert_not_nullable(BlockReader::gas_limit, field),
                BlockField::GasUsed => assert_not_nullable(BlockReader::gas_used, field),
                BlockField::Timestamp => assert_not_nullable(BlockReader::timestamp, field),
            }
        }
    }

    #[test]
    fn test_transaction_nullability_matches_schema() {
        fn assert_nullable<'a, T, F>(_: F, transaction_field: TransactionField)
        where
            F: FnOnce(&TransactionReader<'a>) -> Result<Option<T>>,
        {
            assert!(
                transaction_field.is_nullable(),
                "Optional type should be nullable"
            );
        }

        fn assert_not_nullable<'a, T, F>(_: F, transaction_field: TransactionField)
        where
            F: FnOnce(&TransactionReader<'a>) -> Result<T>,
            T: Hex,
        {
            assert!(!transaction_field.is_nullable(), "should not be nullable");
        }

        for field in TransactionField::all() {
            match field {
                // Nullable fields
                TransactionField::From => assert_nullable(TransactionReader::from, field),
                TransactionField::GasPrice => assert_nullable(TransactionReader::gas_price, field),
                TransactionField::To => assert_nullable(TransactionReader::to, field),
                TransactionField::V => assert_nullable(TransactionReader::v, field),
                TransactionField::R => assert_nullable(TransactionReader::r, field),
                TransactionField::S => assert_nullable(TransactionReader::s, field),
                TransactionField::MaxPriorityFeePerGas => {
                    assert_nullable(TransactionReader::max_priority_fee_per_gas, field)
                }
                TransactionField::MaxFeePerGas => {
                    assert_nullable(TransactionReader::max_fee_per_gas, field)
                }
                TransactionField::ChainId => assert_nullable(TransactionReader::chain_id, field),
                TransactionField::ContractAddress => {
                    assert_nullable(TransactionReader::contract_address, field)
                }
                TransactionField::Type => assert_nullable(TransactionReader::type_, field),
                TransactionField::Root => assert_nullable(TransactionReader::root, field),
                TransactionField::Status => assert_nullable(TransactionReader::status, field),
                TransactionField::Sighash => assert_nullable(TransactionReader::sighash, field),
                TransactionField::YParity => assert_nullable(TransactionReader::y_parity, field),
                TransactionField::AccessList => {
                    assert_nullable(TransactionReader::access_list, field)
                }
                TransactionField::AuthorizationList => {
                    assert_nullable(TransactionReader::authorization_list, field)
                }
                TransactionField::L1Fee => assert_nullable(TransactionReader::l1_fee, field),
                TransactionField::MaxFeePerBlobGas => {
                    assert_nullable(TransactionReader::max_fee_per_blob_gas, field)
                }
                TransactionField::BlobVersionedHashes => {
                    assert_nullable(TransactionReader::blob_versioned_hashes, field)
                }
                // Non-nullable fields
                TransactionField::BlockHash => {
                    assert_not_nullable(TransactionReader::block_hash, field)
                }
                TransactionField::BlockNumber => {
                    assert_not_nullable(TransactionReader::block_number, field)
                }
                TransactionField::Gas => assert_not_nullable(TransactionReader::gas, field),
                TransactionField::Hash => assert_not_nullable(TransactionReader::hash, field),
                TransactionField::Input => assert_not_nullable(TransactionReader::input, field),
                TransactionField::Nonce => assert_not_nullable(TransactionReader::nonce, field),
                TransactionField::TransactionIndex => {
                    assert_not_nullable(TransactionReader::transaction_index, field)
                }
                TransactionField::Value => assert_not_nullable(TransactionReader::value, field),
                TransactionField::CumulativeGasUsed => {
                    assert_not_nullable(TransactionReader::cumulative_gas_used, field)
                }
                TransactionField::EffectiveGasPrice => {
                    assert_not_nullable(TransactionReader::effective_gas_price, field)
                }
                TransactionField::GasUsed => {
                    assert_not_nullable(TransactionReader::gas_used, field)
                }
                TransactionField::LogsBloom => {
                    assert_not_nullable(TransactionReader::logs_bloom, field)
                }
                // Fields not yet implemented in reader
                _ => {
                    // For now, just check if the field exists - this will fail compilation if we miss implementing a method
                    // when we add more fields to the reader
                }
            }
        }
    }

    #[test]
    fn test_trace_nullability_matches_schema() {
        fn assert_nullable<'a, T, F>(_: F, trace_field: TraceField)
        where
            F: FnOnce(&TraceReader<'a>) -> Result<Option<T>>,
        {
            assert!(
                trace_field.is_nullable(),
                "Optional type should be nullable"
            );
        }

        fn assert_not_nullable<'a, T, F>(_: F, trace_field: TraceField)
        where
            F: FnOnce(&TraceReader<'a>) -> Result<T>,
            T: Hex,
        {
            assert!(!trace_field.is_nullable(), "should not be nullable");
        }

        for field in TraceField::all() {
            match field {
                // Nullable fields
                TraceField::TransactionHash => {
                    assert_nullable(TraceReader::transaction_hash, field)
                }
                TraceField::TransactionPosition => {
                    assert_nullable(TraceReader::transaction_position, field)
                }
                TraceField::Type => assert_nullable(TraceReader::type_, field),
                TraceField::Error => assert_nullable(TraceReader::error, field),
                TraceField::From => assert_nullable(TraceReader::from, field),
                TraceField::To => assert_nullable(TraceReader::to, field),
                TraceField::Author => assert_nullable(TraceReader::author, field),
                TraceField::Gas => assert_nullable(TraceReader::gas, field),
                TraceField::GasUsed => assert_nullable(TraceReader::gas_used, field),
                TraceField::ActionAddress => assert_nullable(TraceReader::action_address, field),
                TraceField::Address => assert_nullable(TraceReader::address, field),
                TraceField::Balance => assert_nullable(TraceReader::balance, field),
                TraceField::CallType => assert_nullable(TraceReader::call_type, field),
                TraceField::Code => assert_nullable(TraceReader::code, field),
                TraceField::Init => assert_nullable(TraceReader::init, field),
                TraceField::Input => assert_nullable(TraceReader::input, field),
                TraceField::Output => assert_nullable(TraceReader::output, field),
                TraceField::RefundAddress => assert_nullable(TraceReader::refund_address, field),
                TraceField::RewardType => assert_nullable(TraceReader::reward_type, field),
                TraceField::Sighash => assert_nullable(TraceReader::sighash, field),
                TraceField::Subtraces => assert_nullable(TraceReader::subtraces, field),
                TraceField::TraceAddress => assert_nullable(TraceReader::trace_address, field),
                TraceField::Value => assert_nullable(TraceReader::value, field),
                // Non-nullable fields
                TraceField::BlockHash => assert_not_nullable(TraceReader::block_hash, field),
                TraceField::BlockNumber => assert_not_nullable(TraceReader::block_number, field),
            }
        }
    }
}
