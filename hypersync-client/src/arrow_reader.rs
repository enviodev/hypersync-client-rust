//! Zero-copy types for reading Arrow data without allocation.
//!
//! This module provides zero-copy readers that access Arrow columnar data directly
//! without copying or allocating new memory for individual field access.

use anyhow::Context;
use arrow::{
    array::{
        Array, ArrayAccessor, BinaryArray, BooleanArray, RecordBatch, StringArray, UInt64Array,
        UInt8Array,
    },
    datatypes::DataType,
};
use hypersync_format::{
    AccessList, Address, Authorization, BlockNumber, Data, FixedSizeData, Hash, LogIndex, Quantity,
    TransactionIndex, TransactionStatus, TransactionType, Withdrawal,
};
use hypersync_net_types::{BlockField, LogField, TraceField, TransactionField};

type ColResult<T> = std::result::Result<T, ColumnError>;

/// Error that occurs when trying to access a column in Arrow data.
#[derive(Debug, thiserror::Error)]
#[error("column {col_name} {err}")]
pub struct ColumnError {
    /// The name of the column that caused the error.
    pub col_name: &'static str,
    /// The specific type of column error that occurred.
    pub err: ColumnErrorType,
}

impl ColumnError {
    fn not_found(col_name: &'static str) -> Self {
        Self {
            col_name,
            err: ColumnErrorType::NotFound,
        }
    }

    fn wrong_type(
        col_name: &'static str,
        expected_type: &'static str,
        actual_type: DataType,
    ) -> Self {
        Self {
            col_name,
            err: ColumnErrorType::WrongType {
                expected_type,
                actual_type,
            },
        }
    }
}

/// The specific type of error that can occur when accessing a column.
#[derive(Debug, thiserror::Error)]
pub enum ColumnErrorType {
    /// The column was not found in the Arrow schema.
    #[error("not found")]
    NotFound,
    /// The column exists but has a different type than expected.
    #[error("expected to be of type {expected_type} but found {actual_type}")]
    WrongType {
        /// The expected Arrow data type.
        expected_type: &'static str,
        /// The actual Arrow data type found in the schema.
        actual_type: DataType,
    },
}

fn column_as<'a, T: 'static>(batch: &'a RecordBatch, col_name: &'static str) -> ColResult<&'a T> {
    match batch.column_by_name(col_name) {
        None => Err(ColumnError::not_found(col_name)),
        Some(c) => {
            let Some(val) = c.as_any().downcast_ref::<T>() else {
                let expected_type = std::any::type_name::<T>();
                let actual_type = c.data_type().clone();
                return Err(ColumnError::wrong_type(
                    col_name,
                    expected_type,
                    actual_type,
                ));
            };
            Ok(val)
        }
    }
}

/// Error that can occur when reading data from Arrow columns.
#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    /// A value was expected to be non-null but was null.
    #[error("value was unexpectedly null")]
    UnexpectedNull,
    /// An error occurred while accessing a column.
    #[error(transparent)]
    ColumnError(#[from] ColumnError),
    /// An error occurred during data type conversion.
    #[error("{0:?}")]
    ConversionError(anyhow::Error),
}

/// A reader for accessing individual row data from an Arrow RecordBatch.
///
/// This struct provides zero-copy access to columnar data in Arrow format,
/// allowing efficient reading of specific fields from a single row.
pub struct ArrowRowReader<'a> {
    batch: &'a RecordBatch,
    row_idx: usize,
}

impl<'a> ArrowRowReader<'a> {
    /// Safely create a new reader for the given batch at row index and check
    /// that row_idx is within the bounds of the batch.
    fn new(batch: &'a RecordBatch, row_idx: usize) -> anyhow::Result<Self> {
        let len = if let Some(first_column) = batch.columns().first() {
            first_column.len()
        } else {
            0
        };
        if row_idx >= len {
            anyhow::bail!("row index out of bounds");
        }

        Ok(Self { batch, row_idx })
    }

    /// Read and convert the value at col_name that could be null
    fn get_nullable<Col, T>(&self, col_name: &'static str) -> Result<Option<T>, ReadError>
    where
        Col: 'static,
        &'a Col: ArrayAccessor,

        <&'a Col as ArrayAccessor>::Item: TryInto<T>,
        <<&'a Col as ArrayAccessor>::Item as TryInto<T>>::Error:
            std::error::Error + Send + Sync + 'static,
    {
        let arr = column_as::<Col>(self.batch, col_name)?;

        if arr.is_valid(self.row_idx) {
            let value = arr.value(self.row_idx);
            let converted: T = value
                .try_into()
                .map_err(|e| ReadError::ConversionError(anyhow::Error::new(e)))?;
            Ok(Some(converted))
        } else {
            Ok(None)
        }
    }

    /// Read and convert the value at col_name where it should not be null
    fn get<Col, T>(&self, col_name: &'static str) -> Result<T, ReadError>
    where
        Col: 'static,
        &'a Col: ArrayAccessor,

        <&'a Col as ArrayAccessor>::Item: TryInto<T>,
        <<&'a Col as ArrayAccessor>::Item as TryInto<T>>::Error:
            std::error::Error + Send + Sync + 'static,
    {
        match self.get_nullable::<Col, T>(col_name) {
            Ok(Some(val)) => Ok(val),
            Ok(None) => Err(ReadError::UnexpectedNull),
            Err(e) => Err(e),
        }
    }
}

/// Iterator over log rows in an RecordBatch.
pub struct ArrowRowIterator<'a, R> {
    batch: &'a RecordBatch,
    current_idx: usize,
    len: usize,
    phantom: std::marker::PhantomData<R>,
}

impl<'a, R: From<ArrowRowReader<'a>>> ArrowRowIterator<'a, R> {
    /// Create a new iterator for the given batch.
    pub fn new(batch: &'a RecordBatch) -> Self {
        let len = if let Some(first_column) = batch.columns().first() {
            first_column.len()
        } else {
            0
        };
        Self {
            batch,
            current_idx: 0,
            len,
            phantom: std::marker::PhantomData,
        }
    }
}

impl<'a, R: From<ArrowRowReader<'a>>> Iterator for ArrowRowIterator<'a, R> {
    type Item = R;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_idx < self.len {
            let reader = ArrowRowReader {
                batch: self.batch,
                row_idx: self.current_idx,
            };
            self.current_idx += 1;
            Some(reader.into())
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.len - self.current_idx;
        (remaining, Some(remaining))
    }
}

impl<'a, R: From<ArrowRowReader<'a>>> ExactSizeIterator for ArrowRowIterator<'a, R> {
    fn len(&self) -> usize {
        self.len - self.current_idx
    }
}

/// Zero-copy reader for log data from Arrow batches.
///
/// Provides efficient access to log fields without copying data from the underlying
/// Arrow columnar format. Each reader is bound to a specific row in the batch.
pub struct LogReader<'a> {
    inner: ArrowRowReader<'a>,
}

impl<'a> From<ArrowRowReader<'a>> for LogReader<'a> {
    fn from(inner: ArrowRowReader<'a>) -> Self {
        Self { inner }
    }
}

/// Iterator over log rows in an RecordBatch.
pub type LogIterator<'a> = ArrowRowIterator<'a, LogReader<'a>>;

impl<'a> LogReader<'a> {
    /// Safely create a new reader for the given batch at row index and check
    /// that row_idx is within the bounds of the batch.
    pub fn new(batch: &'a RecordBatch, row_idx: usize) -> anyhow::Result<Self> {
        let inner = ArrowRowReader::new(batch, row_idx)?;
        Ok(Self { inner })
    }
    /// Create an iterator over all rows in the batch.
    pub fn iter(batch: &'a RecordBatch) -> LogIterator<'a> {
        LogIterator::new(batch)
    }

    /// The boolean value indicating if the event was removed from the blockchain due
    /// to a chain reorganization. True if the log was removed. False if it is a valid log.
    pub fn removed(&self) -> Result<Option<bool>, ReadError> {
        self.inner
            .get_nullable::<BooleanArray, bool>(LogField::Removed.as_ref())
    }

    /// The integer identifying the index of the event within the block's list of events.
    pub fn log_index(&self) -> Result<LogIndex, ReadError> {
        self.inner
            .get::<UInt64Array, LogIndex>(LogField::LogIndex.as_ref())
    }

    /// The integer index of the transaction within the block's list of transactions.
    pub fn transaction_index(&self) -> Result<TransactionIndex, ReadError> {
        self.inner
            .get::<UInt64Array, TransactionIndex>(LogField::TransactionIndex.as_ref())
    }

    /// The hash of the transaction that triggered the event.
    pub fn transaction_hash(&self) -> Result<Hash, ReadError> {
        self.inner
            .get::<BinaryArray, Hash>(LogField::TransactionHash.as_ref())
    }

    /// The hash of the block in which the event was included.
    pub fn block_hash(&self) -> Result<Hash, ReadError> {
        self.inner
            .get::<BinaryArray, Hash>(LogField::BlockHash.as_ref())
    }

    /// The block number in which the event was included.
    pub fn block_number(&self) -> Result<BlockNumber, ReadError> {
        self.inner
            .get::<UInt64Array, BlockNumber>(LogField::BlockNumber.as_ref())
    }

    /// The contract address from which the event originated.
    pub fn address(&self) -> Result<Address, ReadError> {
        self.inner
            .get::<BinaryArray, Address>(LogField::Address.as_ref())
    }

    /// The first topic of the event (topic0).
    pub fn topic0(&self) -> Result<Option<FixedSizeData<32>>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, FixedSizeData<32>>(LogField::Topic0.as_ref())
    }

    /// The second topic of the event (topic1).
    pub fn topic1(&self) -> Result<Option<FixedSizeData<32>>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, FixedSizeData<32>>(LogField::Topic1.as_ref())
    }

    /// The third topic of the event (topic2).
    pub fn topic2(&self) -> Result<Option<FixedSizeData<32>>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, FixedSizeData<32>>(LogField::Topic2.as_ref())
    }

    /// The fourth topic of the event (topic3).
    pub fn topic3(&self) -> Result<Option<FixedSizeData<32>>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, FixedSizeData<32>>(LogField::Topic3.as_ref())
    }

    /// The non-indexed data that was emitted along with the event.
    pub fn data(&self) -> Result<Data, ReadError> {
        self.inner.get::<BinaryArray, Data>(LogField::Data.as_ref())
    }
}

/// Zero-copy reader for block data from Arrow batches.
///
/// Provides efficient access to block fields without copying data from the underlying
/// Arrow columnar format. Each reader is bound to a specific row in the batch.
pub struct BlockReader<'a> {
    inner: ArrowRowReader<'a>,
}

impl<'a> From<ArrowRowReader<'a>> for BlockReader<'a> {
    fn from(inner: ArrowRowReader<'a>) -> Self {
        Self { inner }
    }
}

/// Iterator over block rows in an RecordBatch.
pub type BlockIterator<'a> = ArrowRowIterator<'a, BlockReader<'a>>;

impl<'a> BlockReader<'a> {
    /// Safely create a new reader for the given batch at row index and check
    /// that row_idx is within the bounds of the batch.
    pub fn new(batch: &'a RecordBatch, row_idx: usize) -> anyhow::Result<Self> {
        let inner = ArrowRowReader::new(batch, row_idx)?;
        Ok(Self { inner })
    }

    /// Create an iterator over all rows in the batch.
    pub fn iter(batch: &'a RecordBatch) -> BlockIterator<'a> {
        BlockIterator::new(batch)
    }

    /// The block number.
    pub fn number(&self) -> Result<u64, ReadError> {
        self.inner
            .get::<UInt64Array, _>(BlockField::Number.as_ref())
    }

    /// The block hash.
    pub fn hash(&self) -> Result<Hash, ReadError> {
        self.inner
            .get::<BinaryArray, Hash>(BlockField::Hash.as_ref())
    }

    /// The parent block hash.
    pub fn parent_hash(&self) -> Result<Hash, ReadError> {
        self.inner
            .get::<BinaryArray, Hash>(BlockField::ParentHash.as_ref())
    }

    /// The block nonce.
    pub fn nonce(&self) -> Result<Option<FixedSizeData<8>>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, FixedSizeData<8>>(BlockField::Nonce.as_ref())
    }

    /// The SHA3 hash of the uncles.
    pub fn sha3_uncles(&self) -> Result<Hash, ReadError> {
        self.inner
            .get::<BinaryArray, Hash>(BlockField::Sha3Uncles.as_ref())
    }

    /// The Bloom filter for the logs of the block.
    pub fn logs_bloom(&self) -> Result<Data, ReadError> {
        self.inner
            .get::<BinaryArray, Data>(BlockField::LogsBloom.as_ref())
    }

    /// The root of the transaction trie of the block.
    pub fn transactions_root(&self) -> Result<Hash, ReadError> {
        self.inner
            .get::<BinaryArray, Hash>(BlockField::TransactionsRoot.as_ref())
    }

    /// The root of the final state trie of the block.
    pub fn state_root(&self) -> Result<Hash, ReadError> {
        self.inner
            .get::<BinaryArray, Hash>(BlockField::StateRoot.as_ref())
    }

    /// The root of the receipts trie of the block.
    pub fn receipts_root(&self) -> Result<Hash, ReadError> {
        self.inner
            .get::<BinaryArray, Hash>(BlockField::ReceiptsRoot.as_ref())
    }

    /// The address of the beneficiary to whom the mining rewards were given.
    pub fn miner(&self) -> Result<Address, ReadError> {
        self.inner
            .get::<BinaryArray, Address>(BlockField::Miner.as_ref())
    }

    /// The difficulty of the block.
    pub fn difficulty(&self) -> Result<Option<Quantity>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Quantity>(BlockField::Difficulty.as_ref())
    }

    /// The total difficulty of the chain until this block.
    pub fn total_difficulty(&self) -> Result<Option<Quantity>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Quantity>(BlockField::TotalDifficulty.as_ref())
    }

    /// The "extra data" field of this block.
    pub fn extra_data(&self) -> Result<Data, ReadError> {
        self.inner
            .get::<BinaryArray, Data>(BlockField::ExtraData.as_ref())
    }

    /// The size of this block in bytes.
    pub fn size(&self) -> Result<Quantity, ReadError> {
        self.inner
            .get::<BinaryArray, Quantity>(BlockField::Size.as_ref())
    }

    /// The maximum gas allowed in this block.
    pub fn gas_limit(&self) -> Result<Quantity, ReadError> {
        self.inner
            .get::<BinaryArray, Quantity>(BlockField::GasLimit.as_ref())
    }

    /// The total used gas by all transactions in this block.
    pub fn gas_used(&self) -> Result<Quantity, ReadError> {
        self.inner
            .get::<BinaryArray, Quantity>(BlockField::GasUsed.as_ref())
    }

    /// The unix timestamp for when the block was collated.
    pub fn timestamp(&self) -> Result<Quantity, ReadError> {
        self.inner
            .get::<BinaryArray, Quantity>(BlockField::Timestamp.as_ref())
    }

    /// Array of uncle hashes.
    pub fn uncles(&self) -> Result<Option<Vec<FixedSizeData<32>>>, ReadError> {
        let all = self
            .inner
            .get_nullable::<BinaryArray, Data>(BlockField::Uncles.as_ref())?;
        let Some(data) = all else {
            return Ok(None);
        };
        let mut uncles = Vec::new();
        for uncle_bytes in data.chunks(32) {
            let uncle = FixedSizeData::<32>::try_from(uncle_bytes)
                .context("convert uncle bytes to uncle")
                .map_err(ReadError::ConversionError)?;
            uncles.push(uncle);
        }
        Ok(Some(uncles))
    }

    /// The base fee per gas.
    pub fn base_fee_per_gas(&self) -> Result<Option<Quantity>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Quantity>(BlockField::BaseFeePerGas.as_ref())
    }

    /// The total amount of blob gas consumed by the transactions in the block.
    pub fn blob_gas_used(&self) -> Result<Option<Quantity>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Quantity>(BlockField::BlobGasUsed.as_ref())
    }

    /// A running total of blob gas consumed in excess of the target.
    pub fn excess_blob_gas(&self) -> Result<Option<Quantity>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Quantity>(BlockField::ExcessBlobGas.as_ref())
    }

    /// The hash of the parent beacon block.
    pub fn parent_beacon_block_root(&self) -> Result<Option<Hash>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Hash>(BlockField::ParentBeaconBlockRoot.as_ref())
    }

    /// The root of the withdrawal trie.
    pub fn withdrawals_root(&self) -> Result<Option<Hash>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Hash>(BlockField::WithdrawalsRoot.as_ref())
    }

    /// The withdrawals in the block.
    pub fn withdrawals(&self) -> Result<Option<Vec<Withdrawal>>, ReadError> {
        let withdrawals_bin = self
            .inner
            .get_nullable::<BinaryArray, Data>(BlockField::Withdrawals.as_ref())?;
        let Some(withdrawals_bin) = withdrawals_bin else {
            return Ok(None);
        };

        let deser = bincode::deserialize(&withdrawals_bin)
            .context("deserialize withdrawals")
            .map_err(ReadError::ConversionError)?;

        Ok(Some(deser))
    }

    /// The L1 block number.
    pub fn l1_block_number(&self) -> Result<Option<BlockNumber>, ReadError> {
        self.inner
            .get_nullable::<UInt64Array, _>(BlockField::L1BlockNumber.as_ref())
    }

    /// The send count.
    pub fn send_count(&self) -> Result<Option<Quantity>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Quantity>(BlockField::SendCount.as_ref())
    }

    /// The send root.
    pub fn send_root(&self) -> Result<Option<Hash>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Hash>(BlockField::SendRoot.as_ref())
    }

    /// The mix hash.
    pub fn mix_hash(&self) -> Result<Option<Hash>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Hash>(BlockField::MixHash.as_ref())
    }
}

/// Zero-copy reader for transaction data from Arrow batches.
///
/// Provides efficient access to transaction fields without copying data from the underlying
/// Arrow columnar format. Each reader is bound to a specific row in the batch.
pub struct TransactionReader<'a> {
    inner: ArrowRowReader<'a>,
}

impl<'a> From<ArrowRowReader<'a>> for TransactionReader<'a> {
    fn from(inner: ArrowRowReader<'a>) -> Self {
        Self { inner }
    }
}

/// Iterator over transaction rows in an RecordBatch.
pub type TransactionIterator<'a> = ArrowRowIterator<'a, TransactionReader<'a>>;

impl<'a> TransactionReader<'a> {
    /// Safely create a new reader for the given batch at row index and check
    /// that row_idx is within the bounds of the batch.
    pub fn new(batch: &'a RecordBatch, row_idx: usize) -> anyhow::Result<Self> {
        let inner = ArrowRowReader::new(batch, row_idx)?;
        Ok(Self { inner })
    }

    /// Create an iterator over all rows in the batch.
    pub fn iter(batch: &'a RecordBatch) -> TransactionIterator<'a> {
        TransactionIterator::new(batch)
    }

    /// The hash of the block in which this transaction was included.
    pub fn block_hash(&self) -> Result<Hash, ReadError> {
        self.inner
            .get::<BinaryArray, Hash>(TransactionField::BlockHash.as_ref())
    }

    /// The number of the block in which this transaction was included.
    pub fn block_number(&self) -> Result<BlockNumber, ReadError> {
        self.inner
            .get::<UInt64Array, BlockNumber>(TransactionField::BlockNumber.as_ref())
    }

    /// The address of the sender.
    pub fn from(&self) -> Result<Option<Address>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Address>(TransactionField::From.as_ref())
    }

    /// The gas limit provided by the sender.
    pub fn gas(&self) -> Result<Quantity, ReadError> {
        self.inner
            .get::<BinaryArray, Quantity>(TransactionField::Gas.as_ref())
    }

    /// The gas price willing to be paid by the sender.
    pub fn gas_price(&self) -> Result<Option<Quantity>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Quantity>(TransactionField::GasPrice.as_ref())
    }

    /// The hash of this transaction.
    pub fn hash(&self) -> Result<Hash, ReadError> {
        self.inner
            .get::<BinaryArray, Hash>(TransactionField::Hash.as_ref())
    }

    /// The data sent along with the transaction.
    pub fn input(&self) -> Result<Data, ReadError> {
        self.inner
            .get::<BinaryArray, Data>(TransactionField::Input.as_ref())
    }

    /// The number of transactions made by the sender prior to this one.
    pub fn nonce(&self) -> Result<Quantity, ReadError> {
        self.inner
            .get::<BinaryArray, Quantity>(TransactionField::Nonce.as_ref())
    }

    /// The address of the receiver.
    pub fn to(&self) -> Result<Option<Address>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Address>(TransactionField::To.as_ref())
    }

    /// The index of the transaction in the block.
    pub fn transaction_index(&self) -> Result<TransactionIndex, ReadError> {
        self.inner
            .get::<UInt64Array, TransactionIndex>(TransactionField::TransactionIndex.as_ref())
    }

    /// The value transferred.
    pub fn value(&self) -> Result<Quantity, ReadError> {
        self.inner
            .get::<BinaryArray, Quantity>(TransactionField::Value.as_ref())
    }

    /// ECDSA recovery id.
    pub fn v(&self) -> Result<Option<Quantity>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Quantity>(TransactionField::V.as_ref())
    }

    /// ECDSA signature r.
    pub fn r(&self) -> Result<Option<Quantity>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Quantity>(TransactionField::R.as_ref())
    }

    /// ECDSA signature s.
    pub fn s(&self) -> Result<Option<Quantity>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Quantity>(TransactionField::S.as_ref())
    }

    /// Maximum fee per gas the sender is willing to pay for priority.
    pub fn max_priority_fee_per_gas(&self) -> Result<Option<Quantity>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Quantity>(TransactionField::MaxPriorityFeePerGas.as_ref())
    }

    /// Maximum total fee per gas the sender is willing to pay.
    pub fn max_fee_per_gas(&self) -> Result<Option<Quantity>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Quantity>(TransactionField::MaxFeePerGas.as_ref())
    }

    /// The chain id of the transaction.
    pub fn chain_id(&self) -> Result<Option<Quantity>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Quantity>(TransactionField::ChainId.as_ref())
    }

    /// The total amount of gas used when this transaction was executed in the block.
    pub fn cumulative_gas_used(&self) -> Result<Quantity, ReadError> {
        self.inner
            .get::<BinaryArray, Quantity>(TransactionField::CumulativeGasUsed.as_ref())
    }

    /// The sum of the base fee and tip paid per unit of gas.
    pub fn effective_gas_price(&self) -> Result<Quantity, ReadError> {
        self.inner
            .get::<BinaryArray, Quantity>(TransactionField::EffectiveGasPrice.as_ref())
    }

    /// The amount of gas used by this transaction.
    pub fn gas_used(&self) -> Result<Quantity, ReadError> {
        self.inner
            .get::<BinaryArray, Quantity>(TransactionField::GasUsed.as_ref())
    }

    /// The contract address created, if the transaction was a contract creation.
    pub fn contract_address(&self) -> Result<Option<Address>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Address>(TransactionField::ContractAddress.as_ref())
    }

    /// The Bloom filter for the logs of the transaction.
    pub fn logs_bloom(&self) -> Result<Data, ReadError> {
        self.inner
            .get::<BinaryArray, Data>(TransactionField::LogsBloom.as_ref())
    }

    /// The type of the transaction.
    pub fn type_(&self) -> Result<Option<TransactionType>, ReadError> {
        let type_ = self
            .inner
            .get_nullable::<UInt8Array, u8>(TransactionField::Type.as_ref())?;
        Ok(type_.map(TransactionType::from))
    }

    /// The post-transaction stateroot.
    pub fn root(&self) -> Result<Option<Hash>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Hash>(TransactionField::Root.as_ref())
    }

    /// Either 1 (success) or 0 (failure).
    pub fn status(&self) -> Result<Option<TransactionStatus>, ReadError> {
        let status = self
            .inner
            .get_nullable::<UInt8Array, u8>(TransactionField::Status.as_ref())?;
        let Some(status) = status else {
            return Ok(None);
        };
        let status = TransactionStatus::from_u8(status)
            .context("convert u8 to transaction status")
            .map_err(ReadError::ConversionError)?;
        Ok(Some(status))
    }

    /// The first 4 bytes of the transaction input data.
    pub fn sighash(&self) -> Result<Option<Data>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, _>(TransactionField::Sighash.as_ref())
    }

    /// The y parity of the signature.
    pub fn y_parity(&self) -> Result<Option<Quantity>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Quantity>(TransactionField::YParity.as_ref())
    }

    /// The access list.
    pub fn access_list(&self) -> Result<Option<Vec<AccessList>>, ReadError> {
        let bin = self
            .inner
            .get_nullable::<BinaryArray, Data>(TransactionField::AccessList.as_ref())?;
        let Some(bin) = bin else {
            return Ok(None);
        };
        let deser = bincode::deserialize(&bin)
            .context("deserialize access list")
            .map_err(ReadError::ConversionError)?;
        Ok(Some(deser))
    }

    /// The authorization list.
    pub fn authorization_list(&self) -> Result<Option<Vec<Authorization>>, ReadError> {
        let bin = self
            .inner
            .get_nullable::<BinaryArray, Data>(TransactionField::AuthorizationList.as_ref())?;
        let Some(bin) = bin else {
            return Ok(None);
        };
        let deser = bincode::deserialize(&bin)
            .context("deserialize authorization list")
            .map_err(ReadError::ConversionError)?;
        Ok(Some(deser))
    }

    // Additional L1/L2 and blob-related fields would go here...
    // For brevity, I'll include a few key ones:

    /// The L1 fee for L2 transactions.
    pub fn l1_fee(&self) -> Result<Option<Quantity>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Quantity>(TransactionField::L1Fee.as_ref())
    }

    /// The maximum fee per blob gas.
    pub fn max_fee_per_blob_gas(&self) -> Result<Option<Quantity>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Quantity>(TransactionField::MaxFeePerBlobGas.as_ref())
    }

    /// The blob versioned hashes.
    pub fn blob_versioned_hashes(&self) -> Result<Option<Vec<Hash>>, ReadError> {
        let bin = self
            .inner
            .get_nullable::<BinaryArray, Data>(TransactionField::BlobVersionedHashes.as_ref())?;
        let Some(bin) = bin else {
            return Ok(None);
        };
        let mut hashes = Vec::new();
        for hash_bytes in bin.chunks(32) {
            let hash = Hash::try_from(hash_bytes)
                .context("convert blob versioned hash bytes to hash")
                .map_err(ReadError::ConversionError)?;
            hashes.push(hash);
        }
        Ok(Some(hashes))
    }

    /// The L1 gas price for L2 transactions.
    pub fn l1_gas_price(&self) -> Result<Option<Quantity>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Quantity>(TransactionField::L1GasPrice.as_ref())
    }

    /// The L1 gas used for L2 transactions.
    pub fn l1_gas_used(&self) -> Result<Option<Quantity>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Quantity>(TransactionField::L1GasUsed.as_ref())
    }

    /// The L1 fee scalar for L2 transactions.
    pub fn l1_fee_scalar(&self) -> Result<Option<f64>, ReadError> {
        let scalar = self
            .inner
            .get_nullable::<BinaryArray, Quantity>(TransactionField::L1FeeScalar.as_ref())?;
        let Some(scalar_utf8) = scalar else {
            return Ok(None);
        };
        // stored as a string of float eg 0.69 (utf8 encoded)
        let scalar_str = std::str::from_utf8(&scalar_utf8)
            .context("convert l1 fee scalar to string")
            .map_err(ReadError::ConversionError)?;

        let scalar_f64: f64 = scalar_str
            .parse()
            .context("parse l1 fee scalar as f64")
            .map_err(ReadError::ConversionError)?;
        Ok(Some(scalar_f64))
    }

    /// The gas used for L1 for L2 transactions.
    pub fn gas_used_for_l1(&self) -> Result<Option<Quantity>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Quantity>(TransactionField::GasUsedForL1.as_ref())
    }

    /// The blob gas price.
    pub fn blob_gas_price(&self) -> Result<Option<Quantity>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Quantity>(TransactionField::BlobGasPrice.as_ref())
    }

    /// The blob gas used.
    pub fn blob_gas_used(&self) -> Result<Option<Quantity>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Quantity>(TransactionField::BlobGasUsed.as_ref())
    }

    /// The deposit nonce for deposit transactions.
    pub fn deposit_nonce(&self) -> Result<Option<Quantity>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Quantity>(TransactionField::DepositNonce.as_ref())
    }

    /// The deposit receipt version for deposit transactions.
    pub fn deposit_receipt_version(&self) -> Result<Option<Quantity>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Quantity>(TransactionField::DepositReceiptVersion.as_ref())
    }

    /// The L1 base fee scalar for L2 transactions.
    pub fn l1_base_fee_scalar(&self) -> Result<Option<Quantity>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Quantity>(TransactionField::L1BaseFeeScalar.as_ref())
    }

    /// The L1 blob base fee for L2 transactions.
    pub fn l1_blob_base_fee(&self) -> Result<Option<Quantity>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Quantity>(TransactionField::L1BlobBaseFee.as_ref())
    }

    /// The L1 blob base fee scalar for L2 transactions.
    pub fn l1_blob_base_fee_scalar(&self) -> Result<Option<Quantity>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Quantity>(TransactionField::L1BlobBaseFeeScalar.as_ref())
    }

    /// The L1 block number for L2 transactions.
    pub fn l1_block_number(&self) -> Result<Option<Quantity>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, _>(TransactionField::L1BlockNumber.as_ref())
    }

    /// The mint value for deposit transactions.
    pub fn mint(&self) -> Result<Option<Quantity>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Quantity>(TransactionField::Mint.as_ref())
    }

    /// The source hash for deposit transactions.
    pub fn source_hash(&self) -> Result<Option<Hash>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Hash>(TransactionField::SourceHash.as_ref())
    }
}

/// Zero-copy reader for trace data from Arrow batches.
///
/// Provides efficient access to trace fields without copying data from the underlying
/// Arrow columnar format. Each reader is bound to a specific row in the batch.
pub struct TraceReader<'a> {
    inner: ArrowRowReader<'a>,
}

impl<'a> From<ArrowRowReader<'a>> for TraceReader<'a> {
    fn from(inner: ArrowRowReader<'a>) -> Self {
        Self { inner }
    }
}

/// Iterator over trace rows in an RecordBatch.
pub type TraceIterator<'a> = ArrowRowIterator<'a, TraceReader<'a>>;

impl<'a> TraceReader<'a> {
    /// Safely create a new reader for the given batch at row index and check
    /// that row_idx is within the bounds of the batch.
    pub fn new(batch: &'a RecordBatch, row_idx: usize) -> anyhow::Result<Self> {
        let inner = ArrowRowReader::new(batch, row_idx)?;
        Ok(Self { inner })
    }

    /// Create an iterator over all rows in the batch.
    pub fn iter(batch: &'a RecordBatch) -> TraceIterator<'a> {
        TraceIterator::new(batch)
    }

    /// The hash of the block in which this trace occurred.
    pub fn block_hash(&self) -> Result<Hash, ReadError> {
        self.inner
            .get::<BinaryArray, Hash>(TraceField::BlockHash.as_ref())
    }

    /// The number of the block in which this trace occurred.
    pub fn block_number(&self) -> Result<u64, ReadError> {
        self.inner
            .get::<UInt64Array, _>(TraceField::BlockNumber.as_ref())
    }

    /// The address from which the trace originated.
    pub fn from(&self) -> Result<Option<Address>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Address>(TraceField::From.as_ref())
    }

    /// The address to which the trace was sent.
    pub fn to(&self) -> Result<Option<Address>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Address>(TraceField::To.as_ref())
    }

    /// The type of call.
    pub fn call_type(&self) -> Result<Option<String>, ReadError> {
        self.inner
            .get_nullable::<StringArray, String>(TraceField::CallType.as_ref())
    }

    /// The amount of gas provided to the trace.
    pub fn gas(&self) -> Result<Option<Quantity>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Quantity>(TraceField::Gas.as_ref())
    }

    /// The input data.
    pub fn input(&self) -> Result<Option<Data>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Data>(TraceField::Input.as_ref())
    }

    /// The init data for contract creation traces.
    pub fn init(&self) -> Result<Option<Data>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Data>(TraceField::Init.as_ref())
    }

    /// The value transferred.
    pub fn value(&self) -> Result<Option<Quantity>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Quantity>(TraceField::Value.as_ref())
    }

    /// The address of the author (miner).
    pub fn author(&self) -> Result<Option<Address>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Address>(TraceField::Author.as_ref())
    }

    /// The type of reward.
    pub fn reward_type(&self) -> Result<Option<String>, ReadError> {
        self.inner
            .get_nullable::<StringArray, String>(TraceField::RewardType.as_ref())
    }

    /// The address involved in the trace.
    pub fn address(&self) -> Result<Option<Address>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Address>(TraceField::Address.as_ref())
    }

    /// The bytecode.
    pub fn code(&self) -> Result<Option<Data>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Data>(TraceField::Code.as_ref())
    }

    /// The amount of gas used by the trace.
    pub fn gas_used(&self) -> Result<Option<Quantity>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Quantity>(TraceField::GasUsed.as_ref())
    }

    /// The output data.
    pub fn output(&self) -> Result<Option<Data>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Data>(TraceField::Output.as_ref())
    }

    /// The number of sub-traces.
    pub fn subtraces(&self) -> Result<Option<u64>, ReadError> {
        self.inner
            .get_nullable::<UInt64Array, u64>(TraceField::Subtraces.as_ref())
    }

    /// The trace address.
    pub fn trace_address(&self) -> Result<Option<Vec<u64>>, ReadError> {
        let bin = self
            .inner
            .get_nullable::<BinaryArray, Data>(TraceField::TraceAddress.as_ref())?;
        let Some(bin) = bin else {
            return Ok(None);
        };
        let deser = bincode::deserialize(&bin)
            .context("deserialize trace address")
            .map_err(ReadError::ConversionError)?;
        Ok(Some(deser))
    }

    /// The hash of the transaction this trace belongs to.
    pub fn transaction_hash(&self) -> Result<Option<Hash>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Hash>(TraceField::TransactionHash.as_ref())
    }

    /// The position of the transaction in the block.
    pub fn transaction_position(&self) -> Result<Option<u64>, ReadError> {
        self.inner
            .get_nullable::<UInt64Array, u64>(TraceField::TransactionPosition.as_ref())
    }

    /// The type of trace.
    pub fn type_(&self) -> Result<Option<String>, ReadError> {
        self.inner
            .get_nullable::<StringArray, String>(TraceField::Type.as_ref())
    }

    /// The error message, if any.
    pub fn error(&self) -> Result<Option<String>, ReadError> {
        self.inner
            .get_nullable::<StringArray, String>(TraceField::Error.as_ref())
    }

    /// The first 4 bytes of the input data.
    pub fn sighash(&self) -> Result<Option<Data>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, _>(TraceField::Sighash.as_ref())
    }

    /// The action address.
    pub fn action_address(&self) -> Result<Option<Address>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Address>(TraceField::ActionAddress.as_ref())
    }

    /// The balance.
    pub fn balance(&self) -> Result<Option<Quantity>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Quantity>(TraceField::Balance.as_ref())
    }

    /// The refund address.
    pub fn refund_address(&self) -> Result<Option<Address>, ReadError> {
        self.inner
            .get_nullable::<BinaryArray, Address>(TraceField::RefundAddress.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;

    trait NotOption {}

    impl NotOption for hypersync_format::Quantity {}
    impl NotOption for hypersync_format::Data {}
    impl NotOption for hypersync_format::UInt {}
    impl<const N: usize> NotOption for hypersync_format::FixedSizeData<N> {}
    impl NotOption for u64 {}

    /// Compile-time tests that ensure the correct return types
    #[test]
    fn test_nullability_matches_schema() {
        fn assert_nullable<'a, T, F>(_: F, log_field: LogField)
        where
            F: FnOnce(&LogReader<'a>) -> Result<Option<T>, ReadError>,
        {
            assert!(log_field.is_nullable(), "Optional type should be nullable");
        }

        fn assert_not_nullable<'a, T, F>(_: F, log_field: LogField)
        where
            F: FnOnce(&LogReader<'a>) -> Result<T, ReadError>,
            // just to make sure its an inner type and not an Option
            T: NotOption,
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
            F: FnOnce(&BlockReader<'a>) -> Result<Option<T>, ReadError>,
        {
            assert!(
                block_field.is_nullable(),
                "Optional type should be nullable"
            );
        }

        fn assert_not_nullable<'a, T, F>(_: F, block_field: BlockField)
        where
            F: FnOnce(&BlockReader<'a>) -> Result<T, ReadError>,
            T: NotOption,
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
            F: FnOnce(&TransactionReader<'a>) -> Result<Option<T>, ReadError>,
        {
            assert!(
                transaction_field.is_nullable(),
                "Optional type should be nullable"
            );
        }

        fn assert_not_nullable<'a, T, F>(_: F, transaction_field: TransactionField)
        where
            F: FnOnce(&TransactionReader<'a>) -> Result<T, ReadError>,
            T: NotOption,
        {
            assert!(!transaction_field.is_nullable(), "should not be nullable");
        }

        for field in TransactionField::all() {
            match field {
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
                TransactionField::L1GasPrice => {
                    assert_nullable(TransactionReader::l1_gas_price, field)
                }
                TransactionField::L1GasUsed => {
                    assert_nullable(TransactionReader::l1_gas_used, field)
                }
                TransactionField::L1FeeScalar => {
                    assert_nullable(TransactionReader::l1_fee_scalar, field)
                }
                TransactionField::GasUsedForL1 => {
                    assert_nullable(TransactionReader::gas_used_for_l1, field)
                }
                TransactionField::BlobGasPrice => {
                    assert_nullable(TransactionReader::blob_gas_price, field)
                }
                TransactionField::BlobGasUsed => {
                    assert_nullable(TransactionReader::blob_gas_used, field)
                }
                TransactionField::DepositNonce => {
                    assert_nullable(TransactionReader::deposit_nonce, field)
                }
                TransactionField::DepositReceiptVersion => {
                    assert_nullable(TransactionReader::deposit_receipt_version, field)
                }
                TransactionField::L1BaseFeeScalar => {
                    assert_nullable(TransactionReader::l1_base_fee_scalar, field)
                }
                TransactionField::L1BlobBaseFee => {
                    assert_nullable(TransactionReader::l1_blob_base_fee, field)
                }
                TransactionField::L1BlobBaseFeeScalar => {
                    assert_nullable(TransactionReader::l1_blob_base_fee_scalar, field)
                }

                TransactionField::L1BlockNumber => {
                    assert_nullable(TransactionReader::l1_block_number, field)
                }
                TransactionField::Mint => assert_nullable(TransactionReader::mint, field),
                TransactionField::SourceHash => {
                    assert_nullable(TransactionReader::source_hash, field)
                }
            }
        }
    }

    #[test]
    fn test_trace_nullability_matches_schema() {
        fn assert_nullable<'a, T, F>(_: F, trace_field: TraceField)
        where
            F: FnOnce(&TraceReader<'a>) -> Result<Option<T>, ReadError>,
        {
            assert!(
                trace_field.is_nullable(),
                "Optional type should be nullable"
            );
        }

        fn assert_not_nullable<'a, T, F>(_: F, trace_field: TraceField)
        where
            F: FnOnce(&TraceReader<'a>) -> Result<T, ReadError>,
            T: NotOption,
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

    fn assert_ok<T, E: std::fmt::Debug>(result: Result<T, E>) {
        let _ = result.expect("should be ok");
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "integration test for returned schema"]
    async fn test_readers_integration() -> anyhow::Result<()> {
        use crate::{
            net_types::{LogField, LogFilter, TransactionField, TransactionFilter},
            Client, Query,
        };
        let client = Client::builder()
            .url("https://eth-traces.hypersync.xyz")
            .api_token(dotenvy::var("HYPERSYNC_API_TOKEN")?)
            .build()
            .context("Failed to build client")?;

        let query = Query::new()
            .from_block(20_000_000)
            .to_block_excl(20_000_001)
            .include_all_blocks()
            .where_logs(LogFilter::all())
            .where_transactions(TransactionFilter::all())
            .select_log_fields(LogField::all())
            .select_block_fields(BlockField::all())
            .select_transaction_fields(TransactionField::all())
            .select_trace_fields(TraceField::all());

        let res = client.collect_arrow(query, Default::default()).await?;

        let mut num_logs = 0;

        for batch in res.data.logs {
            for log_reader in LogReader::iter(&batch) {
                num_logs += 1;

                for log_field in LogField::all() {
                    match log_field {
                        LogField::Removed => assert_ok(log_reader.removed()),
                        LogField::LogIndex => assert_ok(log_reader.log_index()),
                        LogField::TransactionIndex => assert_ok(log_reader.transaction_index()),
                        LogField::TransactionHash => assert_ok(log_reader.transaction_hash()),
                        LogField::BlockHash => assert_ok(log_reader.block_hash()),
                        LogField::BlockNumber => assert_ok(log_reader.block_number()),
                        LogField::Address => assert_ok(log_reader.address()),
                        LogField::Data => assert_ok(log_reader.data()),
                        LogField::Topic0 => assert_ok(log_reader.topic0()),
                        LogField::Topic1 => assert_ok(log_reader.topic1()),
                        LogField::Topic2 => assert_ok(log_reader.topic2()),
                        LogField::Topic3 => assert_ok(log_reader.topic3()),
                    }
                }
            }
        }

        println!("num_logs: {}", num_logs);

        let mut num_transactions = 0;

        for batch in res.data.transactions {
            for transaction_reader in TransactionReader::iter(&batch) {
                num_transactions += 1;

                for transaction_field in TransactionField::all() {
                    match transaction_field {
                        TransactionField::BlockHash => assert_ok(transaction_reader.block_hash()),
                        TransactionField::BlockNumber => {
                            assert_ok(transaction_reader.block_number())
                        }
                        TransactionField::From => assert_ok(transaction_reader.from()),
                        TransactionField::Gas => assert_ok(transaction_reader.gas()),
                        TransactionField::GasPrice => assert_ok(transaction_reader.gas_price()),
                        TransactionField::Hash => assert_ok(transaction_reader.hash()),
                        TransactionField::Input => assert_ok(transaction_reader.input()),
                        TransactionField::Nonce => assert_ok(transaction_reader.nonce()),
                        TransactionField::To => assert_ok(transaction_reader.to()),
                        TransactionField::TransactionIndex => {
                            assert_ok(transaction_reader.transaction_index())
                        }
                        TransactionField::Value => assert_ok(transaction_reader.value()),
                        TransactionField::V => assert_ok(transaction_reader.v()),
                        TransactionField::R => assert_ok(transaction_reader.r()),
                        TransactionField::S => assert_ok(transaction_reader.s()),
                        TransactionField::MaxPriorityFeePerGas => {
                            assert_ok(transaction_reader.max_priority_fee_per_gas())
                        }
                        TransactionField::MaxFeePerGas => {
                            assert_ok(transaction_reader.max_fee_per_gas())
                        }
                        TransactionField::ChainId => assert_ok(transaction_reader.chain_id()),
                        TransactionField::CumulativeGasUsed => {
                            assert_ok(transaction_reader.cumulative_gas_used())
                        }
                        TransactionField::EffectiveGasPrice => {
                            assert_ok(transaction_reader.effective_gas_price())
                        }
                        TransactionField::GasUsed => assert_ok(transaction_reader.gas_used()),
                        TransactionField::ContractAddress => {
                            assert_ok(transaction_reader.contract_address())
                        }
                        TransactionField::LogsBloom => assert_ok(transaction_reader.logs_bloom()),
                        TransactionField::Type => assert_ok(transaction_reader.type_()),
                        TransactionField::Root => assert_ok(transaction_reader.root()),
                        TransactionField::Status => assert_ok(transaction_reader.status()),
                        TransactionField::Sighash => assert_ok(transaction_reader.sighash()),
                        TransactionField::YParity => assert_ok(transaction_reader.y_parity()),
                        TransactionField::AccessList => assert_ok(transaction_reader.access_list()),
                        TransactionField::AuthorizationList => {
                            assert_ok(transaction_reader.authorization_list())
                        }
                        TransactionField::L1Fee => assert_ok(transaction_reader.l1_fee()),
                        TransactionField::MaxFeePerBlobGas => {
                            assert_ok(transaction_reader.max_fee_per_blob_gas())
                        }
                        TransactionField::BlobVersionedHashes => {
                            assert_ok(transaction_reader.blob_versioned_hashes())
                        }
                        TransactionField::L1GasPrice => {
                            assert_ok(transaction_reader.l1_gas_price())
                        }
                        TransactionField::L1GasUsed => assert_ok(transaction_reader.l1_gas_used()),
                        TransactionField::L1FeeScalar => {
                            assert_ok(transaction_reader.l1_fee_scalar())
                        }
                        TransactionField::GasUsedForL1 => {
                            assert_ok(transaction_reader.gas_used_for_l1())
                        }
                        TransactionField::BlobGasPrice => {
                            assert_ok(transaction_reader.blob_gas_price())
                        }
                        TransactionField::BlobGasUsed => {
                            assert_ok(transaction_reader.blob_gas_used())
                        }
                        TransactionField::DepositNonce => {
                            assert_ok(transaction_reader.deposit_nonce())
                        }
                        TransactionField::DepositReceiptVersion => {
                            assert_ok(transaction_reader.deposit_receipt_version())
                        }
                        TransactionField::L1BaseFeeScalar => {
                            assert_ok(transaction_reader.l1_base_fee_scalar())
                        }
                        TransactionField::L1BlobBaseFee => {
                            assert_ok(transaction_reader.l1_blob_base_fee())
                        }
                        TransactionField::L1BlobBaseFeeScalar => {
                            assert_ok(transaction_reader.l1_blob_base_fee_scalar())
                        }
                        TransactionField::L1BlockNumber => {
                            assert_ok(transaction_reader.l1_block_number())
                        }
                        TransactionField::Mint => assert_ok(transaction_reader.mint()),
                        TransactionField::SourceHash => assert_ok(transaction_reader.source_hash()),
                    }
                }
            }
        }

        println!("num_transactions: {}", num_transactions);
        let mut num_blocks = 0;

        for batch in res.data.blocks {
            for block_reader in BlockReader::iter(&batch) {
                num_blocks += 1;

                for block_field in BlockField::all() {
                    match block_field {
                        BlockField::Number => assert_ok(block_reader.number()),
                        BlockField::Hash => assert_ok(block_reader.hash()),
                        BlockField::ParentHash => assert_ok(block_reader.parent_hash()),
                        BlockField::Nonce => assert_ok(block_reader.nonce()),
                        BlockField::Sha3Uncles => assert_ok(block_reader.sha3_uncles()),
                        BlockField::LogsBloom => assert_ok(block_reader.logs_bloom()),
                        BlockField::TransactionsRoot => assert_ok(block_reader.transactions_root()),
                        BlockField::StateRoot => assert_ok(block_reader.state_root()),
                        BlockField::ReceiptsRoot => assert_ok(block_reader.receipts_root()),
                        BlockField::Miner => assert_ok(block_reader.miner()),
                        BlockField::Difficulty => assert_ok(block_reader.difficulty()),
                        BlockField::TotalDifficulty => assert_ok(block_reader.total_difficulty()),
                        BlockField::ExtraData => assert_ok(block_reader.extra_data()),
                        BlockField::Size => assert_ok(block_reader.size()),
                        BlockField::GasLimit => assert_ok(block_reader.gas_limit()),
                        BlockField::GasUsed => assert_ok(block_reader.gas_used()),
                        BlockField::Timestamp => assert_ok(block_reader.timestamp()),
                        BlockField::Uncles => assert_ok(block_reader.uncles()),
                        BlockField::BaseFeePerGas => assert_ok(block_reader.base_fee_per_gas()),
                        BlockField::BlobGasUsed => assert_ok(block_reader.blob_gas_used()),
                        BlockField::ExcessBlobGas => assert_ok(block_reader.excess_blob_gas()),
                        BlockField::ParentBeaconBlockRoot => {
                            assert_ok(block_reader.parent_beacon_block_root())
                        }
                        BlockField::WithdrawalsRoot => assert_ok(block_reader.withdrawals_root()),
                        BlockField::Withdrawals => assert_ok(block_reader.withdrawals()),
                        BlockField::L1BlockNumber => assert_ok(block_reader.l1_block_number()),
                        BlockField::SendCount => assert_ok(block_reader.send_count()),
                        BlockField::SendRoot => assert_ok(block_reader.send_root()),
                        BlockField::MixHash => assert_ok(block_reader.mix_hash()),
                    }
                }
            }
        }

        println!("num_blocks: {}", num_blocks);

        let mut num_traces = 0;

        for batch in res.data.traces {
            for trace_reader in TraceReader::iter(&batch) {
                num_traces += 1;

                for trace_field in TraceField::all() {
                    match trace_field {
                        TraceField::BlockHash => assert_ok(trace_reader.block_hash()),
                        TraceField::BlockNumber => assert_ok(trace_reader.block_number()),
                        TraceField::From => assert_ok(trace_reader.from()),
                        TraceField::To => assert_ok(trace_reader.to()),
                        TraceField::CallType => assert_ok(trace_reader.call_type()),
                        TraceField::Gas => assert_ok(trace_reader.gas()),
                        TraceField::Input => assert_ok(trace_reader.input()),
                        TraceField::Init => assert_ok(trace_reader.init()),
                        TraceField::Value => assert_ok(trace_reader.value()),
                        TraceField::Author => assert_ok(trace_reader.author()),
                        TraceField::RewardType => assert_ok(trace_reader.reward_type()),
                        TraceField::Address => assert_ok(trace_reader.address()),
                        TraceField::Code => assert_ok(trace_reader.code()),
                        TraceField::GasUsed => assert_ok(trace_reader.gas_used()),
                        TraceField::Output => assert_ok(trace_reader.output()),
                        TraceField::Subtraces => assert_ok(trace_reader.subtraces()),
                        TraceField::TraceAddress => assert_ok(trace_reader.trace_address()),
                        TraceField::TransactionHash => assert_ok(trace_reader.transaction_hash()),
                        TraceField::TransactionPosition => {
                            assert_ok(trace_reader.transaction_position())
                        }
                        TraceField::Type => assert_ok(trace_reader.type_()),
                        TraceField::Error => assert_ok(trace_reader.error()),
                        TraceField::Sighash => assert_ok(trace_reader.sighash()),
                        TraceField::ActionAddress => assert_ok(trace_reader.action_address()),
                        TraceField::Balance => assert_ok(trace_reader.balance()),
                        TraceField::RefundAddress => assert_ok(trace_reader.refund_address()),
                    }
                }
            }
        }

        println!("num_traces: {}", num_traces);

        assert!(num_traces > 0, "no traces found");
        assert!(num_logs > 0, "no logs found");
        assert!(num_transactions > 0, "no transactions found");
        assert!(num_blocks > 0, "no blocks found");

        Ok(())
    }
}
