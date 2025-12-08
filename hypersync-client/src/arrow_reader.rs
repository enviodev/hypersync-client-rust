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
    Address, BlockNumber, Data, FixedSizeData, Hash, LogArgument, LogIndex, Quantity,
    TransactionIndex,
};
use hypersync_net_types::{BlockField, LogField, TraceField, TransactionField};

type ColResult<T> = std::result::Result<T, ColumnError>;

#[derive(Debug, thiserror::Error)]
#[error("column {col_name} {err}")]
pub struct ColumnError {
    pub col_name: &'static str,
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

#[derive(Debug, thiserror::Error)]
pub enum ColumnErrorType {
    #[error("not found")]
    NotFound,
    #[error("expected to be of type {expected_type} but found {actual_type}")]
    WrongType {
        expected_type: &'static str,
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

#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    #[error("value was unexpectedly null")]
    UnexpectedNull,
    #[error(transparent)]
    ColumnError(#[from] ColumnError),
    #[error(transparent)]
    ConversionError(#[from] anyhow::Error),
}

pub struct ArrowRowReader<'a> {
    batch: &'a RecordBatch,
    row_idx: usize,
}

impl<'a> ArrowRowReader<'a> {
    /// Safely create a new reader for the given batch at row index and check
    /// that row_idx is within the bounds of the batch.
    pub fn new(batch: &'a RecordBatch, row_idx: usize) -> anyhow::Result<Self> {
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

    fn get_nullable<A, T>(&self, col_name: &'static str) -> Result<Option<T>, ReadError>
    where
        A: 'static,
        &'a A: ArrayAccessor,

        <&'a A as ArrayAccessor>::Item: TryInto<T>,
        <<&'a A as ArrayAccessor>::Item as TryInto<T>>::Error:
            std::error::Error + Send + Sync + 'static,
    {
        let arr = column_as::<A>(self.batch, col_name)?;

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

    fn get<A, T>(&self, col_name: &'static str) -> Result<T, ReadError>
    where
        A: 'static,
        &'a A: ArrayAccessor,

        <&'a A as ArrayAccessor>::Item: TryInto<T>,
        <<&'a A as ArrayAccessor>::Item as TryInto<T>>::Error:
            std::error::Error + Send + Sync + 'static,
    {
        match self.get_nullable::<A, T>(col_name) {
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

#[tokio::test(flavor = "multi_thread")]
async fn test_log_reader() -> anyhow::Result<()> {
    use crate::{
        net_types::{LogField, LogFilter, TransactionField, TransactionFilter},
        Client, Query,
    };
    let client = Client::builder()
        .chain_id(1)
        .api_token(dotenvy::var("HYPERSYNC_API_TOKEN")?)
        .build()
        .context("Failed to build client")?;

    let query = Query::new()
        .from_block(20_000_000)
        .to_block_excl(20_000_100)
        .include_all_blocks()
        // .where_transactions(TransactionFilter::all())
        .where_logs(LogFilter::all())
        .select_log_fields(LogField::all());
    // .select_block_fields(BlockField::all())
    // .select_transaction_fields(TransactionField::all());

    let res = client.collect_arrow(query, Default::default()).await?;

    println!("Retrieved {} log batches", res.data.logs.len());
    println!("Retrieved {} block batches", res.data.blocks.len());
    println!(
        "Retrieved {} transaction batches",
        res.data.transactions.len()
    );

    let mut num_logs = 0;

    for batch in res.data.logs {
        for log_reader in LogReader::iter(&batch) {
            num_logs += 1;
            let _ = log_reader.removed().unwrap();
            let _ = log_reader.log_index().unwrap();
            let _ = log_reader.transaction_index().unwrap();
            let _ = log_reader.transaction_hash().unwrap();
            let _ = log_reader.block_hash().unwrap();
            let _ = log_reader.block_number().unwrap();
            let _ = log_reader.address().unwrap();
            let _ = log_reader.data().unwrap();
            let _ = log_reader.topic0().unwrap();
            let _ = log_reader.topic1().unwrap();
            let _ = log_reader.topic2().unwrap();
            let _ = log_reader.topic3().unwrap();
        }
    }

    println!("num_logs: {}", num_logs);

    Ok(())
}

//
// /// Zero-copy reader for block data from Arrow batches.
// ///
// /// Provides efficient access to block fields without copying data from the underlying
// /// Arrow columnar format. Each reader is bound to a specific row in the batch.
// pub struct BlockReader<'a> {
//     batch: &'a RecordBatch,
//     row_idx: usize,
// }
//
// /// Iterator over block rows in an RecordBatch.
// pub struct BlockIterator<'a> {
//     batch: &'a RecordBatch,
//     current_idx: usize,
//     len: usize,
// }
//
// impl<'a> BlockIterator<'a> {
//     /// Create a new iterator for the given batch.
//     pub fn new(batch: &'a RecordBatch) -> Self {
//         let len = if let Some(first_column) = batch.columns().first() {
//             first_column.len()
//         } else {
//             0
//         };
//         Self {
//             batch,
//             current_idx: 0,
//             len,
//         }
//     }
// }
//
// impl<'a> Iterator for BlockIterator<'a> {
//     type Item = BlockReader<'a>;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         if self.current_idx < self.len {
//             let reader = BlockReader {
//                 batch: self.batch,
//                 row_idx: self.current_idx,
//             };
//             self.current_idx += 1;
//             Some(reader)
//         } else {
//             None
//         }
//     }
//
//     fn size_hint(&self) -> (usize, Option<usize>) {
//         let remaining = self.len - self.current_idx;
//         (remaining, Some(remaining))
//     }
// }
//
// impl<'a> ExactSizeIterator for BlockIterator<'a> {
//     fn len(&self) -> usize {
//         self.len - self.current_idx
//     }
// }
//
// impl<'a> BlockReader<'a> {
//     /// Create an iterator over all rows in the batch.
//     pub fn iter(batch: &'a RecordBatch) -> BlockIterator<'a> {
//         BlockIterator::new(batch)
//     }
//     /// The block number.
//     pub fn number(&self) -> ColResult<BlockNumber> {
//         let array = column_as::<UInt64Array>(self.batch, BlockField::Number.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         let value = if array.is_valid(self.row_idx) {
//             array.value(self.row_idx)
//         } else {
//             return Err(anyhow::anyhow!("value should not be null"));
//         };
//         Ok(value.into())
//     }
//
//     /// The block hash.
//     pub fn hash(&self) -> ColResult<Hash> {
//         let array = column_as::<BinaryArray>(self.batch, BlockField::Hash.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         let value = if array.is_valid(self.row_idx) {
//             array.value(self.row_idx)
//         } else {
//             return Err(anyhow::anyhow!("value should not be null"));
//         };
//         Hash::try_from(value).context("invalid hash format")
//     }
//
//     /// The parent block hash.
//     pub fn parent_hash(&self) -> ColResult<Hash> {
//         let array = column_as::<BinaryArray>(self.batch, BlockField::ParentHash.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         let value = if array.is_valid(self.row_idx) {
//             array.value(self.row_idx)
//         } else {
//             return Err(anyhow::anyhow!("value should not be null"));
//         };
//         Hash::try_from(value).context("invalid hash format")
//     }
//
//     /// The block nonce.
//     pub fn nonce(&self) -> ColResult<Option<FixedSizeData<8>>> {
//         let array = column_as::<BinaryArray>(self.batch, BlockField::Nonce.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             FixedSizeData::<8>::try_from(array.value(self.row_idx)).ok()
//         } else {
//             None
//         })
//     }
//
//     /// The SHA3 hash of the uncles.
//     pub fn sha3_uncles(&self) -> ColResult<Hash> {
//         let array = column_as::<BinaryArray>(self.batch, BlockField::Sha3Uncles.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         let value = if array.is_valid(self.row_idx) {
//             array.value(self.row_idx)
//         } else {
//             return Err(anyhow::anyhow!("value should not be null"));
//         };
//         Hash::try_from(value).context("invalid hash format")
//     }
//
//     /// The Bloom filter for the logs of the block.
//     pub fn logs_bloom(&self) -> ColResult<Data> {
//         let array = column_as::<BinaryArray>(self.batch, BlockField::LogsBloom.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         let value = if array.is_valid(self.row_idx) {
//             array.value(self.row_idx)
//         } else {
//             return Err(anyhow::anyhow!("value should not be null"));
//         };
//         Ok(Data::from(value))
//     }
//
//     /// The root of the transaction trie of the block.
//     pub fn transactions_root(&self) -> ColResult<Hash> {
//         let array = column_as::<BinaryArray>(self.batch, BlockField::TransactionsRoot.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         let value = if array.is_valid(self.row_idx) {
//             array.value(self.row_idx)
//         } else {
//             return Err(anyhow::anyhow!("value should not be null"));
//         };
//         Hash::try_from(value).context("invalid hash format")
//     }
//
//     /// The root of the final state trie of the block.
//     pub fn state_root(&self) -> ColResult<Hash> {
//         let array = column_as::<BinaryArray>(self.batch, BlockField::StateRoot.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         let value = if array.is_valid(self.row_idx) {
//             array.value(self.row_idx)
//         } else {
//             return Err(anyhow::anyhow!("value should not be null"));
//         };
//         Hash::try_from(value).context("invalid hash format")
//     }
//
//     /// The root of the receipts trie of the block.
//     pub fn receipts_root(&self) -> ColResult<Hash> {
//         let array = column_as::<BinaryArray>(self.batch, BlockField::ReceiptsRoot.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         let value = if array.is_valid(self.row_idx) {
//             array.value(self.row_idx)
//         } else {
//             return Err(anyhow::anyhow!("value should not be null"));
//         };
//         Hash::try_from(value).context("invalid hash format")
//     }
//
//     /// The address of the beneficiary to whom the mining rewards were given.
//     pub fn miner(&self) -> ColResult<Address> {
//         let array = column_as::<BinaryArray>(self.batch, BlockField::Miner.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         let value = if array.is_valid(self.row_idx) {
//             array.value(self.row_idx)
//         } else {
//             return Err(anyhow::anyhow!("value should not be null"));
//         };
//         Address::try_from(value).context("invalid address format")
//     }
//
//     /// The difficulty of the block.
//     pub fn difficulty(&self) -> ColResult<Option<Quantity>> {
//         let array = column_as::<BinaryArray>(self.batch, BlockField::Difficulty.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(Quantity::from(array.value(self.row_idx)))
//         } else {
//             None
//         })
//     }
//
//     /// The total difficulty of the chain until this block.
//     pub fn total_difficulty(&self) -> ColResult<Option<Quantity>> {
//         let array = column_as::<BinaryArray>(self.batch, BlockField::TotalDifficulty.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(Quantity::from(array.value(self.row_idx)))
//         } else {
//             None
//         })
//     }
//
//     /// The "extra data" field of this block.
//     pub fn extra_data(&self) -> ColResult<Data> {
//         let array = column_as::<BinaryArray>(self.batch, BlockField::ExtraData.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         let value = if array.is_valid(self.row_idx) {
//             array.value(self.row_idx)
//         } else {
//             return Err(anyhow::anyhow!("value should not be null"));
//         };
//         Ok(Data::from(value))
//     }
//
//     /// The size of this block in bytes.
//     pub fn size(&self) -> ColResult<Quantity> {
//         let array = column_as::<BinaryArray>(self.batch, BlockField::Size.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         let value = if array.is_valid(self.row_idx) {
//             array.value(self.row_idx)
//         } else {
//             return Err(anyhow::anyhow!("value should not be null"));
//         };
//         Ok(Quantity::from(value))
//     }
//
//     /// The maximum gas allowed in this block.
//     pub fn gas_limit(&self) -> ColResult<Quantity> {
//         let array = column_as::<BinaryArray>(self.batch, BlockField::GasLimit.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         let value = if array.is_valid(self.row_idx) {
//             array.value(self.row_idx)
//         } else {
//             return Err(anyhow::anyhow!("value should not be null"));
//         };
//         Ok(Quantity::from(value))
//     }
//
//     /// The total used gas by all transactions in this block.
//     pub fn gas_used(&self) -> ColResult<Quantity> {
//         let array = column_as::<BinaryArray>(self.batch, BlockField::GasUsed.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         let value = if array.is_valid(self.row_idx) {
//             array.value(self.row_idx)
//         } else {
//             return Err(anyhow::anyhow!("value should not be null"));
//         };
//         Ok(Quantity::from(value))
//     }
//
//     /// The unix timestamp for when the block was collated.
//     pub fn timestamp(&self) -> ColResult<Quantity> {
//         let array = column_as::<BinaryArray>(self.batch, BlockField::Timestamp.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         let value = if array.is_valid(self.row_idx) {
//             array.value(self.row_idx)
//         } else {
//             return Err(anyhow::anyhow!("value should not be null"));
//         };
//         Ok(Quantity::from(value))
//     }
//
//     /// Array of uncle hashes.
//     pub fn uncles(&self) -> ColResult<Option<Data>> {
//         let array = column_as::<BinaryArray>(self.batch, BlockField::Uncles.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(Data::from(array.value(self.row_idx)))
//         } else {
//             None
//         })
//     }
//
//     /// The base fee per gas.
//     pub fn base_fee_per_gas(&self) -> ColResult<Option<Quantity>> {
//         let array = column_as::<BinaryArray>(self.batch, BlockField::BaseFeePerGas.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(Quantity::from(array.value(self.row_idx)))
//         } else {
//             None
//         })
//     }
//
//     /// The total amount of blob gas consumed by the transactions in the block.
//     pub fn blob_gas_used(&self) -> ColResult<Option<Quantity>> {
//         let array = column_as::<BinaryArray>(self.batch, BlockField::BlobGasUsed.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(Quantity::from(array.value(self.row_idx)))
//         } else {
//             None
//         })
//     }
//
//     /// A running total of blob gas consumed in excess of the target.
//     pub fn excess_blob_gas(&self) -> ColResult<Option<Quantity>> {
//         let array = column_as::<BinaryArray>(self.batch, BlockField::ExcessBlobGas.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(Quantity::from(array.value(self.row_idx)))
//         } else {
//             None
//         })
//     }
//
//     /// The hash of the parent beacon block.
//     pub fn parent_beacon_block_root(&self) -> ColResult<Option<Hash>> {
//         let array =
//             column_as::<BinaryArray>(self.batch, BlockField::ParentBeaconBlockRoot.as_ref())
//                 .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Hash::try_from(array.value(self.row_idx)).ok()
//         } else {
//             None
//         })
//     }
//
//     /// The root of the withdrawal trie.
//     pub fn withdrawals_root(&self) -> ColResult<Option<Hash>> {
//         let array = column_as::<BinaryArray>(self.batch, BlockField::WithdrawalsRoot.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Hash::try_from(array.value(self.row_idx)).ok()
//         } else {
//             None
//         })
//     }
//
//     /// The withdrawals in the block.
//     pub fn withdrawals(&self) -> ColResult<Option<Data>> {
//         let array = column_as::<BinaryArray>(self.batch, BlockField::Withdrawals.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(Data::from(array.value(self.row_idx)))
//         } else {
//             None
//         })
//     }
//
//     /// The L1 block number.
//     pub fn l1_block_number(&self) -> ColResult<Option<BlockNumber>> {
//         let array = column_as::<UInt64Array>(self.batch, BlockField::L1BlockNumber.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(array.value(self.row_idx).into())
//         } else {
//             None
//         })
//     }
//
//     /// The send count.
//     pub fn send_count(&self) -> ColResult<Option<Quantity>> {
//         let array = column_as::<BinaryArray>(self.batch, BlockField::SendCount.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(Quantity::from(array.value(self.row_idx)))
//         } else {
//             None
//         })
//     }
//
//     /// The send root.
//     pub fn send_root(&self) -> ColResult<Option<Hash>> {
//         let array = column_as::<BinaryArray>(self.batch, BlockField::SendRoot.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Hash::try_from(array.value(self.row_idx)).ok()
//         } else {
//             None
//         })
//     }
//
//     /// The mix hash.
//     pub fn mix_hash(&self) -> ColResult<Option<Hash>> {
//         let array = column_as::<BinaryArray>(self.batch, BlockField::MixHash.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Hash::try_from(array.value(self.row_idx)).ok()
//         } else {
//             None
//         })
//     }
// }
//
// /// Zero-copy reader for transaction data from Arrow batches.
// ///
// /// Provides efficient access to transaction fields without copying data from the underlying
// /// Arrow columnar format. Each reader is bound to a specific row in the batch.
// pub struct TransactionReader<'a> {
//     batch: &'a RecordBatch,
//     row_idx: usize,
// }
//
// /// Iterator over transaction rows in an RecordBatch.
// pub struct TransactionIterator<'a> {
//     batch: &'a RecordBatch,
//     current_idx: usize,
//     len: usize,
// }
//
// impl<'a> TransactionIterator<'a> {
//     /// Create a new iterator for the given batch.
//     pub fn new(batch: &'a RecordBatch) -> Self {
//         let len = if let Some(first_column) = batch.columns().first() {
//             first_column.len()
//         } else {
//             0
//         };
//         Self {
//             batch,
//             current_idx: 0,
//             len,
//         }
//     }
// }
//
// impl<'a> Iterator for TransactionIterator<'a> {
//     type Item = TransactionReader<'a>;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         if self.current_idx < self.len {
//             let reader = TransactionReader {
//                 batch: self.batch,
//                 row_idx: self.current_idx,
//             };
//             self.current_idx += 1;
//             Some(reader)
//         } else {
//             None
//         }
//     }
//
//     fn size_hint(&self) -> (usize, Option<usize>) {
//         let remaining = self.len - self.current_idx;
//         (remaining, Some(remaining))
//     }
// }
//
// impl<'a> ExactSizeIterator for TransactionIterator<'a> {
//     fn len(&self) -> usize {
//         self.len - self.current_idx
//     }
// }
//
// impl<'a> TransactionReader<'a> {
//     /// Create an iterator over all rows in the batch.
//     pub fn iter(batch: &'a RecordBatch) -> TransactionIterator<'a> {
//         TransactionIterator::new(batch)
//     }
//     /// The hash of the block in which this transaction was included.
//     pub fn block_hash(&self) -> ColResult<Hash> {
//         let array = column_as::<BinaryArray>(self.batch, TransactionField::BlockHash.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         let value = if array.is_valid(self.row_idx) {
//             array.value(self.row_idx)
//         } else {
//             return Err(anyhow::anyhow!("value should not be null"));
//         };
//         Hash::try_from(value).context("invalid hash format")
//     }
//
//     /// The number of the block in which this transaction was included.
//     pub fn block_number(&self) -> ColResult<BlockNumber> {
//         let array = column_as::<UInt64Array>(self.batch, TransactionField::BlockNumber.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         let value = if array.is_valid(self.row_idx) {
//             array.value(self.row_idx)
//         } else {
//             return Err(anyhow::anyhow!("value should not be null"));
//         };
//         Ok(value.into())
//     }
//
//     /// The address of the sender.
//     pub fn from(&self) -> ColResult<Option<Address>> {
//         let array = column_as::<BinaryArray>(self.batch, TransactionField::From.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Address::try_from(array.value(self.row_idx)).ok()
//         } else {
//             None
//         })
//     }
//
//     /// The gas limit provided by the sender.
//     pub fn gas(&self) -> ColResult<Quantity> {
//         let array = column_as::<BinaryArray>(self.batch, TransactionField::Gas.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         let value = if array.is_valid(self.row_idx) {
//             array.value(self.row_idx)
//         } else {
//             return Err(anyhow::anyhow!("value should not be null"));
//         };
//         Ok(Quantity::from(value))
//     }
//
//     /// The gas price willing to be paid by the sender.
//     pub fn gas_price(&self) -> ColResult<Option<Quantity>> {
//         let array = column_as::<BinaryArray>(self.batch, TransactionField::GasPrice.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(Quantity::from(array.value(self.row_idx)))
//         } else {
//             None
//         })
//     }
//
//     /// The hash of this transaction.
//     pub fn hash(&self) -> ColResult<Hash> {
//         let array = column_as::<BinaryArray>(self.batch, TransactionField::Hash.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         let value = if array.is_valid(self.row_idx) {
//             array.value(self.row_idx)
//         } else {
//             return Err(anyhow::anyhow!("value should not be null"));
//         };
//         Hash::try_from(value).context("invalid hash format")
//     }
//
//     /// The data sent along with the transaction.
//     pub fn input(&self) -> ColResult<Data> {
//         let array = column_as::<BinaryArray>(self.batch, TransactionField::Input.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         let value = if array.is_valid(self.row_idx) {
//             array.value(self.row_idx)
//         } else {
//             return Err(anyhow::anyhow!("value should not be null"));
//         };
//         Ok(Data::from(value))
//     }
//
//     /// The number of transactions made by the sender prior to this one.
//     pub fn nonce(&self) -> ColResult<Quantity> {
//         let array = column_as::<BinaryArray>(self.batch, TransactionField::Nonce.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         let value = if array.is_valid(self.row_idx) {
//             array.value(self.row_idx)
//         } else {
//             return Err(anyhow::anyhow!("value should not be null"));
//         };
//         Ok(Quantity::from(value))
//     }
//
//     /// The address of the receiver.
//     pub fn to(&self) -> ColResult<Option<Address>> {
//         let array = column_as::<BinaryArray>(self.batch, TransactionField::To.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Address::try_from(array.value(self.row_idx)).ok()
//         } else {
//             None
//         })
//     }
//
//     /// The index of the transaction in the block.
//     pub fn transaction_index(&self) -> ColResult<TransactionIndex> {
//         let array =
//             column_as::<UInt64Array>(self.batch, TransactionField::TransactionIndex.as_ref())
//                 .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         let value = if array.is_valid(self.row_idx) {
//             array.value(self.row_idx)
//         } else {
//             return Err(anyhow::anyhow!("value should not be null"));
//         };
//         Ok(value.into())
//     }
//
//     /// The value transferred.
//     pub fn value(&self) -> ColResult<Quantity> {
//         let array = column_as::<BinaryArray>(self.batch, TransactionField::Value.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         let value = if array.is_valid(self.row_idx) {
//             array.value(self.row_idx)
//         } else {
//             return Err(anyhow::anyhow!("value should not be null"));
//         };
//         Ok(Quantity::from(value))
//     }
//
//     /// ECDSA recovery id.
//     pub fn v(&self) -> ColResult<Option<Quantity>> {
//         let array = column_as::<BinaryArray>(self.batch, TransactionField::V.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(Quantity::from(array.value(self.row_idx)))
//         } else {
//             None
//         })
//     }
//
//     /// ECDSA signature r.
//     pub fn r(&self) -> ColResult<Option<Quantity>> {
//         let array = column_as::<BinaryArray>(self.batch, TransactionField::R.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(Quantity::from(array.value(self.row_idx)))
//         } else {
//             None
//         })
//     }
//
//     /// ECDSA signature s.
//     pub fn s(&self) -> ColResult<Option<Quantity>> {
//         let array = column_as::<BinaryArray>(self.batch, TransactionField::S.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(Quantity::from(array.value(self.row_idx)))
//         } else {
//             None
//         })
//     }
//
//     /// Maximum fee per gas the sender is willing to pay for priority.
//     pub fn max_priority_fee_per_gas(&self) -> ColResult<Option<Quantity>> {
//         let array =
//             column_as::<BinaryArray>(self.batch, TransactionField::MaxPriorityFeePerGas.as_ref())
//                 .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(Quantity::from(array.value(self.row_idx)))
//         } else {
//             None
//         })
//     }
//
//     /// Maximum total fee per gas the sender is willing to pay.
//     pub fn max_fee_per_gas(&self) -> ColResult<Option<Quantity>> {
//         let array = column_as::<BinaryArray>(self.batch, TransactionField::MaxFeePerGas.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(Quantity::from(array.value(self.row_idx)))
//         } else {
//             None
//         })
//     }
//
//     /// The chain id of the transaction.
//     pub fn chain_id(&self) -> ColResult<Option<Quantity>> {
//         let array = column_as::<BinaryArray>(self.batch, TransactionField::ChainId.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(Quantity::from(array.value(self.row_idx)))
//         } else {
//             None
//         })
//     }
//
//     /// The total amount of gas used when this transaction was executed in the block.
//     pub fn cumulative_gas_used(&self) -> ColResult<Quantity> {
//         let array =
//             column_as::<BinaryArray>(self.batch, TransactionField::CumulativeGasUsed.as_ref())
//                 .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         let value = if array.is_valid(self.row_idx) {
//             array.value(self.row_idx)
//         } else {
//             return Err(anyhow::anyhow!("value should not be null"));
//         };
//         Ok(Quantity::from(value))
//     }
//
//     /// The sum of the base fee and tip paid per unit of gas.
//     pub fn effective_gas_price(&self) -> ColResult<Quantity> {
//         let array =
//             column_as::<BinaryArray>(self.batch, TransactionField::EffectiveGasPrice.as_ref())
//                 .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         let value = if array.is_valid(self.row_idx) {
//             array.value(self.row_idx)
//         } else {
//             return Err(anyhow::anyhow!("value should not be null"));
//         };
//         Ok(Quantity::from(value))
//     }
//
//     /// The amount of gas used by this transaction.
//     pub fn gas_used(&self) -> ColResult<Quantity> {
//         let array = column_as::<BinaryArray>(self.batch, TransactionField::GasUsed.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         let value = if array.is_valid(self.row_idx) {
//             array.value(self.row_idx)
//         } else {
//             return Err(anyhow::anyhow!("value should not be null"));
//         };
//         Ok(Quantity::from(value))
//     }
//
//     /// The contract address created, if the transaction was a contract creation.
//     pub fn contract_address(&self) -> ColResult<Option<Address>> {
//         let array =
//             column_as::<BinaryArray>(self.batch, TransactionField::ContractAddress.as_ref())
//                 .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Address::try_from(array.value(self.row_idx)).ok()
//         } else {
//             None
//         })
//     }
//
//     /// The Bloom filter for the logs of the transaction.
//     pub fn logs_bloom(&self) -> ColResult<Data> {
//         let array = column_as::<BinaryArray>(self.batch, TransactionField::LogsBloom.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         let value = if array.is_valid(self.row_idx) {
//             array.value(self.row_idx)
//         } else {
//             return Err(anyhow::anyhow!("value should not be null"));
//         };
//         Ok(Data::from(value))
//     }
//
//     /// The type of the transaction.
//     pub fn type_(&self) -> ColResult<Option<u8>> {
//         let array = column_as::<UInt8Array>(self.batch, TransactionField::Type.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(array.value(self.row_idx))
//         } else {
//             None
//         })
//     }
//
//     /// The post-transaction stateroot.
//     pub fn root(&self) -> ColResult<Option<Hash>> {
//         let array = column_as::<BinaryArray>(self.batch, TransactionField::Root.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Hash::try_from(array.value(self.row_idx)).ok()
//         } else {
//             None
//         })
//     }
//
//     /// Either 1 (success) or 0 (failure).
//     pub fn status(&self) -> ColResult<Option<u8>> {
//         let array = column_as::<UInt8Array>(self.batch, TransactionField::Status.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(array.value(self.row_idx))
//         } else {
//             None
//         })
//     }
//
//     /// The first 4 bytes of the transaction input data.
//     pub fn sighash(&self) -> ColResult<Option<FixedSizeData<4>>> {
//         let array = column_as::<BinaryArray>(self.batch, TransactionField::Sighash.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             FixedSizeData::<4>::try_from(array.value(self.row_idx)).ok()
//         } else {
//             None
//         })
//     }
//
//     /// The y parity of the signature.
//     pub fn y_parity(&self) -> ColResult<Option<Quantity>> {
//         let array = column_as::<BinaryArray>(self.batch, TransactionField::YParity.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(Quantity::from(array.value(self.row_idx)))
//         } else {
//             None
//         })
//     }
//
//     /// The access list.
//     pub fn access_list(&self) -> ColResult<Option<Data>> {
//         let array = column_as::<BinaryArray>(self.batch, TransactionField::AccessList.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(Data::from(array.value(self.row_idx)))
//         } else {
//             None
//         })
//     }
//
//     /// The authorization list.
//     pub fn authorization_list(&self) -> ColResult<Option<Data>> {
//         let array =
//             column_as::<BinaryArray>(self.batch, TransactionField::AuthorizationList.as_ref())
//                 .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(Data::from(array.value(self.row_idx)))
//         } else {
//             None
//         })
//     }
//
//     // Additional L1/L2 and blob-related fields would go here...
//     // For brevity, I'll include a few key ones:
//
//     /// The L1 fee for L2 transactions.
//     pub fn l1_fee(&self) -> ColResult<Option<Quantity>> {
//         let array = column_as::<BinaryArray>(self.batch, TransactionField::L1Fee.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(Quantity::from(array.value(self.row_idx)))
//         } else {
//             None
//         })
//     }
//
//     /// The maximum fee per blob gas.
//     pub fn max_fee_per_blob_gas(&self) -> ColResult<Option<Quantity>> {
//         let array =
//             column_as::<BinaryArray>(self.batch, TransactionField::MaxFeePerBlobGas.as_ref())
//                 .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(Quantity::from(array.value(self.row_idx)))
//         } else {
//             None
//         })
//     }
//
//     /// The blob versioned hashes.
//     pub fn blob_versioned_hashes(&self) -> ColResult<Option<Data>> {
//         let array =
//             column_as::<BinaryArray>(self.batch, TransactionField::BlobVersionedHashes.as_ref())
//                 .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(Data::from(array.value(self.row_idx)))
//         } else {
//             None
//         })
//     }
// }
//
// /// Zero-copy reader for trace data from Arrow batches.
// ///
// /// Provides efficient access to trace fields without copying data from the underlying
// /// Arrow columnar format. Each reader is bound to a specific row in the batch.
// pub struct TraceReader<'a> {
//     batch: &'a RecordBatch,
//     row_idx: usize,
// }
//
// /// Iterator over trace rows in an RecordBatch.
// pub struct TraceIterator<'a> {
//     batch: &'a RecordBatch,
//     current_idx: usize,
//     len: usize,
// }
//
// impl<'a> TraceIterator<'a> {
//     /// Create a new iterator for the given batch.
//     pub fn new(batch: &'a RecordBatch) -> Self {
//         let len = if let Some(first_column) = batch.columns().first() {
//             first_column.len()
//         } else {
//             0
//         };
//         Self {
//             batch,
//             current_idx: 0,
//             len,
//         }
//     }
// }
//
// impl<'a> Iterator for TraceIterator<'a> {
//     type Item = TraceReader<'a>;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         if self.current_idx < self.len {
//             let reader = TraceReader {
//                 batch: self.batch,
//                 row_idx: self.current_idx,
//             };
//             self.current_idx += 1;
//             Some(reader)
//         } else {
//             None
//         }
//     }
//
//     fn size_hint(&self) -> (usize, Option<usize>) {
//         let remaining = self.len - self.current_idx;
//         (remaining, Some(remaining))
//     }
// }
//
// impl<'a> ExactSizeIterator for TraceIterator<'a> {
//     fn len(&self) -> usize {
//         self.len - self.current_idx
//     }
// }
//
// impl<'a> TraceReader<'a> {
//     /// Create an iterator over all rows in the batch.
//     pub fn iter(batch: &'a RecordBatch) -> TraceIterator<'a> {
//         TraceIterator::new(batch)
//     }
//     /// The hash of the block in which this trace occurred.
//     pub fn block_hash(&self) -> ColResult<Hash> {
//         let array = column_as::<BinaryArray>(self.batch, TraceField::BlockHash.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         let value = if array.is_valid(self.row_idx) {
//             array.value(self.row_idx)
//         } else {
//             return Err(anyhow::anyhow!("value should not be null"));
//         };
//         Hash::try_from(value).context("invalid hash format")
//     }
//
//     /// The number of the block in which this trace occurred.
//     pub fn block_number(&self) -> ColResult<BlockNumber> {
//         let array = column_as::<UInt64Array>(self.batch, TraceField::BlockNumber.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         let value = if array.is_valid(self.row_idx) {
//             array.value(self.row_idx)
//         } else {
//             return Err(anyhow::anyhow!("value should not be null"));
//         };
//         Ok(value.into())
//     }
//
//     /// The address from which the trace originated.
//     pub fn from(&self) -> ColResult<Option<Address>> {
//         let array = column_as::<BinaryArray>(self.batch, TraceField::From.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Address::try_from(array.value(self.row_idx)).ok()
//         } else {
//             None
//         })
//     }
//
//     /// The address to which the trace was sent.
//     pub fn to(&self) -> ColResult<Option<Address>> {
//         let array = column_as::<BinaryArray>(self.batch, TraceField::To.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Address::try_from(array.value(self.row_idx)).ok()
//         } else {
//             None
//         })
//     }
//
//     /// The type of call.
//     pub fn call_type(&self) -> ColResult<Option<String>> {
//         let array = column_as::<StringArray>(self.batch, TraceField::CallType.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(array.value(self.row_idx).to_string())
//         } else {
//             None
//         })
//     }
//
//     /// The amount of gas provided to the trace.
//     pub fn gas(&self) -> ColResult<Option<Quantity>> {
//         let array = column_as::<BinaryArray>(self.batch, TraceField::Gas.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(Quantity::from(array.value(self.row_idx)))
//         } else {
//             None
//         })
//     }
//
//     /// The input data.
//     pub fn input(&self) -> ColResult<Option<Data>> {
//         let array = column_as::<BinaryArray>(self.batch, TraceField::Input.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(Data::from(array.value(self.row_idx)))
//         } else {
//             None
//         })
//     }
//
//     /// The init data for contract creation traces.
//     pub fn init(&self) -> ColResult<Option<Data>> {
//         let array = column_as::<BinaryArray>(self.batch, TraceField::Init.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(Data::from(array.value(self.row_idx)))
//         } else {
//             None
//         })
//     }
//
//     /// The value transferred.
//     pub fn value(&self) -> ColResult<Option<Quantity>> {
//         let array = column_as::<BinaryArray>(self.batch, TraceField::Value.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(Quantity::from(array.value(self.row_idx)))
//         } else {
//             None
//         })
//     }
//
//     /// The address of the author (miner).
//     pub fn author(&self) -> ColResult<Option<Address>> {
//         let array = column_as::<BinaryArray>(self.batch, TraceField::Author.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Address::try_from(array.value(self.row_idx)).ok()
//         } else {
//             None
//         })
//     }
//
//     /// The type of reward.
//     pub fn reward_type(&self) -> ColResult<Option<String>> {
//         let array = column_as::<StringArray>(self.batch, TraceField::RewardType.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(array.value(self.row_idx).to_string())
//         } else {
//             None
//         })
//     }
//
//     /// The address involved in the trace.
//     pub fn address(&self) -> ColResult<Option<Address>> {
//         let array = column_as::<BinaryArray>(self.batch, TraceField::Address.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Address::try_from(array.value(self.row_idx)).ok()
//         } else {
//             None
//         })
//     }
//
//     /// The bytecode.
//     pub fn code(&self) -> ColResult<Option<Data>> {
//         let array = column_as::<BinaryArray>(self.batch, TraceField::Code.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(Data::from(array.value(self.row_idx)))
//         } else {
//             None
//         })
//     }
//
//     /// The amount of gas used by the trace.
//     pub fn gas_used(&self) -> ColResult<Option<Quantity>> {
//         let array = column_as::<BinaryArray>(self.batch, TraceField::GasUsed.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(Quantity::from(array.value(self.row_idx)))
//         } else {
//             None
//         })
//     }
//
//     /// The output data.
//     pub fn output(&self) -> ColResult<Option<Data>> {
//         let array = column_as::<BinaryArray>(self.batch, TraceField::Output.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(Data::from(array.value(self.row_idx)))
//         } else {
//             None
//         })
//     }
//
//     /// The number of sub-traces.
//     pub fn subtraces(&self) -> ColResult<Option<u64>> {
//         let array = column_as::<UInt64Array>(self.batch, TraceField::Subtraces.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(array.value(self.row_idx))
//         } else {
//             None
//         })
//     }
//
//     /// The trace address.
//     pub fn trace_address(&self) -> ColResult<Option<Data>> {
//         let array = column_as::<BinaryArray>(self.batch, TraceField::TraceAddress.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(Data::from(array.value(self.row_idx)))
//         } else {
//             None
//         })
//     }
//
//     /// The hash of the transaction this trace belongs to.
//     pub fn transaction_hash(&self) -> ColResult<Option<Hash>> {
//         let array = column_as::<BinaryArray>(self.batch, TraceField::TransactionHash.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Hash::try_from(array.value(self.row_idx)).ok()
//         } else {
//             None
//         })
//     }
//
//     /// The position of the transaction in the block.
//     pub fn transaction_position(&self) -> ColResult<Option<u64>> {
//         let array = column_as::<UInt64Array>(self.batch, TraceField::TransactionPosition.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(array.value(self.row_idx))
//         } else {
//             None
//         })
//     }
//
//     /// The type of trace.
//     pub fn type_(&self) -> ColResult<Option<String>> {
//         let array = column_as::<StringArray>(self.batch, TraceField::Type.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(array.value(self.row_idx).to_string())
//         } else {
//             None
//         })
//     }
//
//     /// The error message, if any.
//     pub fn error(&self) -> ColResult<Option<String>> {
//         let array = column_as::<StringArray>(self.batch, TraceField::Error.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(array.value(self.row_idx).to_string())
//         } else {
//             None
//         })
//     }
//
//     /// The first 4 bytes of the input data.
//     pub fn sighash(&self) -> ColResult<Option<FixedSizeData<4>>> {
//         let array = column_as::<BinaryArray>(self.batch, TraceField::Sighash.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             FixedSizeData::<4>::try_from(array.value(self.row_idx)).ok()
//         } else {
//             None
//         })
//     }
//
//     /// The action address.
//     pub fn action_address(&self) -> ColResult<Option<Address>> {
//         let array = column_as::<BinaryArray>(self.batch, TraceField::ActionAddress.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Address::try_from(array.value(self.row_idx)).ok()
//         } else {
//             None
//         })
//     }
//
//     /// The balance.
//     pub fn balance(&self) -> ColResult<Option<Quantity>> {
//         let array = column_as::<BinaryArray>(self.batch, TraceField::Balance.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Some(Quantity::from(array.value(self.row_idx)))
//         } else {
//             None
//         })
//     }
//
//     /// The refund address.
//     pub fn refund_address(&self) -> ColResult<Option<Address>> {
//         let array = column_as::<BinaryArray>(self.batch, TraceField::RefundAddress.as_ref())
//             .ok_or_else(|| anyhow::anyhow!("column not found or wrong type"))?;
//         Ok(if array.is_valid(self.row_idx) {
//             Address::try_from(array.value(self.row_idx)).ok()
//         } else {
//             None
//         })
//     }
// }
//
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use anyhow::Result;
//     use hypersync_format::Hex;
//
//     /// Compile-time tests that ensure the correct return types
//     #[test]
//     fn test_nullability_matches_schema() {
//         fn assert_nullable<'a, T, F>(_: F, log_field: LogField)
//         where
//             F: FnOnce(&LogReader<'a>) -> Result<Option<T>>,
//         {
//             assert!(log_field.is_nullable(), "Optional type should be nullable");
//         }
//
//         fn assert_not_nullable<'a, T, F>(_: F, log_field: LogField)
//         where
//             F: FnOnce(&LogReader<'a>) -> Result<T>,
//             // just to make sure its an inner type and not an Option
//             T: Hex,
//         {
//             assert!(!log_field.is_nullable(), "should not be nullable");
//         }
//         // This test will fail to compile if the return types are wrong
//
//         for field in LogField::all() {
//             match field {
//                 LogField::Removed => assert_nullable(LogReader::removed, field),
//                 LogField::Topic0 => assert_nullable(LogReader::topic0, field),
//                 LogField::Topic1 => assert_nullable(LogReader::topic1, field),
//                 LogField::Topic2 => assert_nullable(LogReader::topic2, field),
//                 LogField::Topic3 => assert_nullable(LogReader::topic3, field),
//                 LogField::LogIndex => assert_not_nullable(LogReader::log_index, field),
//                 LogField::TransactionIndex => {
//                     assert_not_nullable(LogReader::transaction_index, field)
//                 }
//                 LogField::TransactionHash => {
//                     assert_not_nullable(LogReader::transaction_hash, field)
//                 }
//                 LogField::BlockHash => assert_not_nullable(LogReader::block_hash, field),
//                 LogField::BlockNumber => assert_not_nullable(LogReader::block_number, field),
//                 LogField::Address => assert_not_nullable(LogReader::address, field),
//                 LogField::Data => assert_not_nullable(LogReader::data, field),
//             }
//         }
//     }
//
//     #[test]
//     fn test_block_nullability_matches_schema() {
//         fn assert_nullable<'a, T, F>(_: F, block_field: BlockField)
//         where
//             F: FnOnce(&BlockReader<'a>) -> Result<Option<T>>,
//         {
//             assert!(
//                 block_field.is_nullable(),
//                 "Optional type should be nullable"
//             );
//         }
//
//         fn assert_not_nullable<'a, T, F>(_: F, block_field: BlockField)
//         where
//             F: FnOnce(&BlockReader<'a>) -> Result<T>,
//             T: Hex,
//         {
//             assert!(!block_field.is_nullable(), "should not be nullable");
//         }
//
//         for field in BlockField::all() {
//             match field {
//                 // Nullable fields
//                 BlockField::Nonce => assert_nullable(BlockReader::nonce, field),
//                 BlockField::Difficulty => assert_nullable(BlockReader::difficulty, field),
//                 BlockField::TotalDifficulty => {
//                     assert_nullable(BlockReader::total_difficulty, field)
//                 }
//                 BlockField::Uncles => assert_nullable(BlockReader::uncles, field),
//                 BlockField::BaseFeePerGas => assert_nullable(BlockReader::base_fee_per_gas, field),
//                 BlockField::BlobGasUsed => assert_nullable(BlockReader::blob_gas_used, field),
//                 BlockField::ExcessBlobGas => assert_nullable(BlockReader::excess_blob_gas, field),
//                 BlockField::ParentBeaconBlockRoot => {
//                     assert_nullable(BlockReader::parent_beacon_block_root, field)
//                 }
//                 BlockField::WithdrawalsRoot => {
//                     assert_nullable(BlockReader::withdrawals_root, field)
//                 }
//                 BlockField::Withdrawals => assert_nullable(BlockReader::withdrawals, field),
//                 BlockField::L1BlockNumber => assert_nullable(BlockReader::l1_block_number, field),
//                 BlockField::SendCount => assert_nullable(BlockReader::send_count, field),
//                 BlockField::SendRoot => assert_nullable(BlockReader::send_root, field),
//                 BlockField::MixHash => assert_nullable(BlockReader::mix_hash, field),
//                 // Non-nullable fields
//                 BlockField::Number => assert_not_nullable(BlockReader::number, field),
//                 BlockField::Hash => assert_not_nullable(BlockReader::hash, field),
//                 BlockField::ParentHash => assert_not_nullable(BlockReader::parent_hash, field),
//                 BlockField::Sha3Uncles => assert_not_nullable(BlockReader::sha3_uncles, field),
//                 BlockField::LogsBloom => assert_not_nullable(BlockReader::logs_bloom, field),
//                 BlockField::TransactionsRoot => {
//                     assert_not_nullable(BlockReader::transactions_root, field)
//                 }
//                 BlockField::StateRoot => assert_not_nullable(BlockReader::state_root, field),
//                 BlockField::ReceiptsRoot => assert_not_nullable(BlockReader::receipts_root, field),
//                 BlockField::Miner => assert_not_nullable(BlockReader::miner, field),
//                 BlockField::ExtraData => assert_not_nullable(BlockReader::extra_data, field),
//                 BlockField::Size => assert_not_nullable(BlockReader::size, field),
//                 BlockField::GasLimit => assert_not_nullable(BlockReader::gas_limit, field),
//                 BlockField::GasUsed => assert_not_nullable(BlockReader::gas_used, field),
//                 BlockField::Timestamp => assert_not_nullable(BlockReader::timestamp, field),
//             }
//         }
//     }
//
//     #[test]
//     fn test_transaction_nullability_matches_schema() {
//         fn assert_nullable<'a, T, F>(_: F, transaction_field: TransactionField)
//         where
//             F: FnOnce(&TransactionReader<'a>) -> Result<Option<T>>,
//         {
//             assert!(
//                 transaction_field.is_nullable(),
//                 "Optional type should be nullable"
//             );
//         }
//
//         fn assert_not_nullable<'a, T, F>(_: F, transaction_field: TransactionField)
//         where
//             F: FnOnce(&TransactionReader<'a>) -> Result<T>,
//             T: Hex,
//         {
//             assert!(!transaction_field.is_nullable(), "should not be nullable");
//         }
//
//         for field in TransactionField::all() {
//             match field {
//                 // Nullable fields
//                 TransactionField::From => assert_nullable(TransactionReader::from, field),
//                 TransactionField::GasPrice => assert_nullable(TransactionReader::gas_price, field),
//                 TransactionField::To => assert_nullable(TransactionReader::to, field),
//                 TransactionField::V => assert_nullable(TransactionReader::v, field),
//                 TransactionField::R => assert_nullable(TransactionReader::r, field),
//                 TransactionField::S => assert_nullable(TransactionReader::s, field),
//                 TransactionField::MaxPriorityFeePerGas => {
//                     assert_nullable(TransactionReader::max_priority_fee_per_gas, field)
//                 }
//                 TransactionField::MaxFeePerGas => {
//                     assert_nullable(TransactionReader::max_fee_per_gas, field)
//                 }
//                 TransactionField::ChainId => assert_nullable(TransactionReader::chain_id, field),
//                 TransactionField::ContractAddress => {
//                     assert_nullable(TransactionReader::contract_address, field)
//                 }
//                 TransactionField::Type => assert_nullable(TransactionReader::type_, field),
//                 TransactionField::Root => assert_nullable(TransactionReader::root, field),
//                 TransactionField::Status => assert_nullable(TransactionReader::status, field),
//                 TransactionField::Sighash => assert_nullable(TransactionReader::sighash, field),
//                 TransactionField::YParity => assert_nullable(TransactionReader::y_parity, field),
//                 TransactionField::AccessList => {
//                     assert_nullable(TransactionReader::access_list, field)
//                 }
//                 TransactionField::AuthorizationList => {
//                     assert_nullable(TransactionReader::authorization_list, field)
//                 }
//                 TransactionField::L1Fee => assert_nullable(TransactionReader::l1_fee, field),
//                 TransactionField::MaxFeePerBlobGas => {
//                     assert_nullable(TransactionReader::max_fee_per_blob_gas, field)
//                 }
//                 TransactionField::BlobVersionedHashes => {
//                     assert_nullable(TransactionReader::blob_versioned_hashes, field)
//                 }
//                 // Non-nullable fields
//                 TransactionField::BlockHash => {
//                     assert_not_nullable(TransactionReader::block_hash, field)
//                 }
//                 TransactionField::BlockNumber => {
//                     assert_not_nullable(TransactionReader::block_number, field)
//                 }
//                 TransactionField::Gas => assert_not_nullable(TransactionReader::gas, field),
//                 TransactionField::Hash => assert_not_nullable(TransactionReader::hash, field),
//                 TransactionField::Input => assert_not_nullable(TransactionReader::input, field),
//                 TransactionField::Nonce => assert_not_nullable(TransactionReader::nonce, field),
//                 TransactionField::TransactionIndex => {
//                     assert_not_nullable(TransactionReader::transaction_index, field)
//                 }
//                 TransactionField::Value => assert_not_nullable(TransactionReader::value, field),
//                 TransactionField::CumulativeGasUsed => {
//                     assert_not_nullable(TransactionReader::cumulative_gas_used, field)
//                 }
//                 TransactionField::EffectiveGasPrice => {
//                     assert_not_nullable(TransactionReader::effective_gas_price, field)
//                 }
//                 TransactionField::GasUsed => {
//                     assert_not_nullable(TransactionReader::gas_used, field)
//                 }
//                 TransactionField::LogsBloom => {
//                     assert_not_nullable(TransactionReader::logs_bloom, field)
//                 }
//                 // Fields not yet implemented in reader
//                 _ => {
//                     // For now, just check if the field exists - this will fail compilation if we miss implementing a method
//                     // when we add more fields to the reader
//                 }
//             }
//         }
//     }
//
//     #[test]
//     fn test_trace_nullability_matches_schema() {
//         fn assert_nullable<'a, T, F>(_: F, trace_field: TraceField)
//         where
//             F: FnOnce(&TraceReader<'a>) -> Result<Option<T>>,
//         {
//             assert!(
//                 trace_field.is_nullable(),
//                 "Optional type should be nullable"
//             );
//         }
//
//         fn assert_not_nullable<'a, T, F>(_: F, trace_field: TraceField)
//         where
//             F: FnOnce(&TraceReader<'a>) -> Result<T>,
//             T: Hex,
//         {
//             assert!(!trace_field.is_nullable(), "should not be nullable");
//         }
//
//         for field in TraceField::all() {
//             match field {
//                 // Nullable fields
//                 TraceField::TransactionHash => {
//                     assert_nullable(TraceReader::transaction_hash, field)
//                 }
//                 TraceField::TransactionPosition => {
//                     assert_nullable(TraceReader::transaction_position, field)
//                 }
//                 TraceField::Type => assert_nullable(TraceReader::type_, field),
//                 TraceField::Error => assert_nullable(TraceReader::error, field),
//                 TraceField::From => assert_nullable(TraceReader::from, field),
//                 TraceField::To => assert_nullable(TraceReader::to, field),
//                 TraceField::Author => assert_nullable(TraceReader::author, field),
//                 TraceField::Gas => assert_nullable(TraceReader::gas, field),
//                 TraceField::GasUsed => assert_nullable(TraceReader::gas_used, field),
//                 TraceField::ActionAddress => assert_nullable(TraceReader::action_address, field),
//                 TraceField::Address => assert_nullable(TraceReader::address, field),
//                 TraceField::Balance => assert_nullable(TraceReader::balance, field),
//                 TraceField::CallType => assert_nullable(TraceReader::call_type, field),
//                 TraceField::Code => assert_nullable(TraceReader::code, field),
//                 TraceField::Init => assert_nullable(TraceReader::init, field),
//                 TraceField::Input => assert_nullable(TraceReader::input, field),
//                 TraceField::Output => assert_nullable(TraceReader::output, field),
//                 TraceField::RefundAddress => assert_nullable(TraceReader::refund_address, field),
//                 TraceField::RewardType => assert_nullable(TraceReader::reward_type, field),
//                 TraceField::Sighash => assert_nullable(TraceReader::sighash, field),
//                 TraceField::Subtraces => assert_nullable(TraceReader::subtraces, field),
//                 TraceField::TraceAddress => assert_nullable(TraceReader::trace_address, field),
//                 TraceField::Value => assert_nullable(TraceReader::value, field),
//                 // Non-nullable fields
//                 TraceField::BlockHash => assert_not_nullable(TraceReader::block_hash, field),
//                 TraceField::BlockNumber => assert_not_nullable(TraceReader::block_number, field),
//             }
//         }
//     }
// }
