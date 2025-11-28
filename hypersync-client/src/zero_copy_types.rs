//! Zero-copy types for reading Arrow data without allocation.
//!
//! This module provides zero-copy readers that access Arrow columnar data directly
//! without copying or allocating new memory for individual field access.

use anyhow::{Context, Result};
use hypersync_format::{
    Address, BlockNumber, Data, FixedSizeData, Hash, LogIndex, TransactionIndex,
};
use hypersync_net_types::LogField;
use polars_arrow::array::{BinaryArray, BooleanArray, StaticArray, UInt64Array};

use crate::ArrowBatch;

/// Zero-copy reader for log data from Arrow batches.
///
/// Provides efficient access to log fields without copying data from the underlying
/// Arrow columnar format. Each reader is bound to a specific row in the batch.
pub struct LogReader<'a> {
    batch: &'a ArrowBatch,
    row_idx: usize,
}

impl<'a> LogReader<'a> {
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

        assert_nullable(LogReader::removed, LogField::Removed);
        assert_nullable(LogReader::topic0, LogField::Topic0);
        assert_nullable(LogReader::topic1, LogField::Topic1);
        assert_nullable(LogReader::topic2, LogField::Topic2);
        assert_nullable(LogReader::topic3, LogField::Topic3);

        assert_not_nullable(LogReader::log_index, LogField::LogIndex);
        assert_not_nullable(LogReader::log_index, LogField::LogIndex);
        assert_not_nullable(LogReader::transaction_index, LogField::TransactionIndex);
        assert_not_nullable(LogReader::transaction_hash, LogField::TransactionHash);
        assert_not_nullable(LogReader::block_hash, LogField::BlockHash);
        assert_not_nullable(LogReader::block_number, LogField::BlockNumber);
        assert_not_nullable(LogReader::address, LogField::Address);
        assert_not_nullable(LogReader::data, LogField::Data);
    }
}

