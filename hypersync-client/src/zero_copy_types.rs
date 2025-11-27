//! Zero-copy types for reading Arrow data without allocation.
//!
//! This module provides zero-copy readers that access Arrow columnar data directly
//! without copying or allocating new memory for individual field access.

use anyhow::Result;
use hypersync_format::{
    Address, BlockNumber, Data, FixedSizeData, Hash, LogIndex, TransactionIndex,
};
use polars_arrow::array::{BinaryArray, BooleanArray, StaticArray, UInt64Array};

use crate::ArrowBatch;

fn map_binary<'a, T>(i: usize, arr: Option<&'a BinaryArray<i32>>) -> Option<T>
where
    T: TryFrom<&'a [u8]>,
    <T as TryFrom<&'a [u8]>>::Error: std::fmt::Debug,
{
    arr.and_then(|arr| arr.get(i).map(|v| T::try_from(v).unwrap()))
}

/// Zero-copy reader for log data from Arrow batches.
///
/// Provides efficient access to log fields without copying data from the underlying
/// Arrow columnar format. Each reader is bound to a specific row in the batch.
pub struct LogReader<'a> {
    batch: &'a ArrowBatch,
    row_idx: usize,
}

impl<'a> LogReader<'a> {
    /// Create a new LogReader for a specific row in the batch
    pub fn new(batch: &'a ArrowBatch, row_idx: usize) -> Self {
        Self { batch, row_idx }
    }
    /// The boolean value indicating if the event was removed from the blockchain due
    /// to a chain reorganization. True if the log was removed. False if it is a valid log.
    pub fn removed(&self) -> Result<Option<bool>> {
        let array = self.batch.optional_column::<BooleanArray>("removed")?;
        Ok(array.and_then(|arr| StaticArray::get(arr, self.row_idx)))
    }

    /// The integer identifying the index of the event within the block's list of events.
    pub fn log_index(&self) -> Result<Option<LogIndex>> {
        let array = self.batch.optional_column::<UInt64Array>("log_index")?;
        Ok(array.and_then(|arr| StaticArray::get(arr, self.row_idx).map(|v| v.into())))
    }

    /// The integer index of the transaction within the block's list of transactions.
    pub fn transaction_index(&self) -> Result<Option<TransactionIndex>> {
        let array = self
            .batch
            .optional_column::<UInt64Array>("transaction_index")?;
        Ok(array.and_then(|arr| StaticArray::get(arr, self.row_idx).map(|v| v.into())))
    }

    /// The hash of the transaction that triggered the event.
    pub fn transaction_hash(&self) -> Result<Option<Hash>> {
        let array = self
            .batch
            .optional_column::<BinaryArray<i32>>("transaction_hash")?;
        Ok(map_binary(self.row_idx, array))
    }

    /// The hash of the block in which the event was included.
    pub fn block_hash(&self) -> Result<Option<Hash>> {
        let array = self
            .batch
            .optional_column::<BinaryArray<i32>>("block_hash")?;
        Ok(map_binary(self.row_idx, array))
    }

    /// The block number in which the event was included.
    pub fn block_number(&self) -> Result<Option<BlockNumber>> {
        let array = self.batch.optional_column::<UInt64Array>("block_number")?;
        Ok(array.and_then(|arr| StaticArray::get(arr, self.row_idx).map(|v| v.into())))
    }

    /// The contract address from which the event originated.
    pub fn address(&self) -> Result<Option<Address>> {
        let array = self.batch.optional_column::<BinaryArray<i32>>("address")?;
        Ok(map_binary(self.row_idx, array))
    }

    /// The first topic of the event (topic0).
    pub fn topic0(&self) -> Result<Option<FixedSizeData<32>>> {
        let array = self.batch.optional_column::<BinaryArray<i32>>("topic0")?;
        Ok(map_binary(self.row_idx, array))
    }

    /// The second topic of the event (topic1).
    pub fn topic1(&self) -> Result<Option<FixedSizeData<32>>> {
        let array = self.batch.optional_column::<BinaryArray<i32>>("topic1")?;
        Ok(map_binary(self.row_idx, array))
    }

    /// The third topic of the event (topic2).
    pub fn topic2(&self) -> Result<Option<FixedSizeData<32>>> {
        let array = self.batch.optional_column::<BinaryArray<i32>>("topic2")?;
        Ok(map_binary(self.row_idx, array))
    }

    /// The fourth topic of the event (topic3).
    pub fn topic3(&self) -> Result<Option<FixedSizeData<32>>> {
        let array = self.batch.optional_column::<BinaryArray<i32>>("topic3")?;
        Ok(map_binary(self.row_idx, array))
    }

    /// The non-indexed data that was emitted along with the event.
    pub fn data(&self) -> Result<Option<Data>> {
        let array = self.batch.optional_column::<BinaryArray<i32>>("data")?;
        Ok(map_binary(self.row_idx, array))
    }
}
