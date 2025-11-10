use crate::{hypersync_net_types_capnp, types::AnyOf, CapnpBuilder, CapnpReader, Selection};
use anyhow::Context;
use hypersync_format::{Address, Hash};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub type BlockSelection = Selection<BlockFilter>;

impl From<BlockFilter> for AnyOf<BlockFilter> {
    fn from(filter: BlockFilter) -> Self {
        Self::new(filter)
    }
}

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct BlockFilter {
    /// Hash of a block, any blocks that have one of these hashes will be returned.
    /// Empty means match all.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hash: Vec<Hash>,
    /// Miner address of a block, any blocks that have one of these miners will be returned.
    /// Empty means match all.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub miner: Vec<Address>,
}

impl BlockFilter {
    /// Create a block filter that matches any block.
    ///
    /// This creates an empty filter with no constraints, which will match all blocks.
    /// You can then use the builder methods to add specific filtering criteria.
    pub fn any() -> Self {
        Default::default()
    }

    /// Combine this filter with another using logical OR.
    ///
    /// Creates an `AnyOf` that matches blocks satisfying either this filter or the other filter.
    /// This allows for fluent chaining of multiple block filters with OR semantics.
    ///
    /// # Arguments
    /// * `other` - Another `BlockFilter` to combine with this one
    ///
    /// # Returns
    /// An `AnyOf<BlockFilter>` that matches blocks satisfying either filter
    ///
    /// # Examples
    ///
    /// ```
    /// use hypersync_net_types::BlockFilter;
    ///
    /// // Match blocks from specific miners OR with specific hashes
    /// let filter = BlockFilter::any()
    ///     .and_miner_address(["0x1234567890123456789012345678901234567890"])?
    ///     .or(
    ///         BlockFilter::any()
    ///             .and_hash(["0x40d008f2a1653f09b7b028d30c7fd1ba7c84900fcfb032040b3eb3d16f84d294"])?
    ///     );
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn or(self, other: Self) -> AnyOf<Self> {
        AnyOf::new(self).or(other)
    }

    /// Filter blocks by any of the provided block hashes.
    ///
    /// This method accepts any iterable of values that can be converted to `Hash`.
    /// Common input types include string slices, byte arrays, and `Hash` objects.
    ///
    /// # Arguments
    /// * `hashes` - An iterable of block hashes to filter by
    ///
    /// # Returns
    /// * `Ok(Self)` - The updated filter on success
    /// * `Err(anyhow::Error)` - If any hash fails to convert
    ///
    /// # Examples
    ///
    /// ```
    /// use hypersync_net_types::BlockFilter;
    ///
    /// // Filter by a single block hash
    /// let filter = BlockFilter::any()
    ///     .and_hash(["0x40d008f2a1653f09b7b028d30c7fd1ba7c84900fcfb032040b3eb3d16f84d294"])?;
    ///
    /// // Filter by multiple block hashes
    /// let filter = BlockFilter::any()
    ///     .and_hash([
    ///         "0x40d008f2a1653f09b7b028d30c7fd1ba7c84900fcfb032040b3eb3d16f84d294",
    ///         "0x88e96d4537bea4d9c05d12549907b32561d3bf31f45aae734cdc119f13406cb6",
    ///     ])?;
    ///
    /// // Using byte arrays
    /// let block_hash = [
    ///     0x40, 0xd0, 0x08, 0xf2, 0xa1, 0x65, 0x3f, 0x09, 0xb7, 0xb0, 0x28, 0xd3, 0x0c, 0x7f, 0xd1, 0xba,
    ///     0x7c, 0x84, 0x90, 0x0f, 0xcf, 0xb0, 0x32, 0x04, 0x0b, 0x3e, 0xb3, 0xd1, 0x6f, 0x84, 0xd2, 0x94
    /// ];
    /// let filter = BlockFilter::any()
    ///     .and_hash([block_hash])?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn and_hash<I, A>(mut self, hashes: I) -> anyhow::Result<Self>
    where
        I: IntoIterator<Item = A>,
        A: TryInto<Hash>,
        A::Error: std::error::Error + Send + Sync + 'static,
    {
        let mut converted_hashes: Vec<Hash> = Vec::new();
        for (idx, hash) in hashes.into_iter().enumerate() {
            converted_hashes.push(
                hash.try_into()
                    .with_context(|| format!("invalid block hash value at position {idx}"))?,
            );
        }
        self.hash = converted_hashes;
        Ok(self)
    }

    /// Filter blocks by any of the provided miner addresses.
    ///
    /// This method accepts any iterable of values that can be converted to `Address`.
    /// Common input types include string slices, byte arrays, and `Address` objects.
    ///
    /// # Arguments
    /// * `addresses` - An iterable of miner addresses to filter by
    ///
    /// # Returns
    /// * `Ok(Self)` - The updated filter on success
    /// * `Err(anyhow::Error)` - If any address fails to convert
    ///
    /// # Examples
    ///
    /// ```
    /// use hypersync_net_types::BlockFilter;
    ///
    /// // Filter by a single miner address
    /// let filter = BlockFilter::any()
    ///     .and_miner_address(["0xdac17f958d2ee523a2206206994597c13d831ec7"])?;
    ///
    /// // Filter by multiple miner addresses (e.g., major mining pools)
    /// let filter = BlockFilter::any()
    ///     .and_miner_address([
    ///         "0xdac17f958d2ee523a2206206994597c13d831ec7", // Pool 1
    ///         "0xa0b86a33e6c11c8c0c5c0b5e6adee30d1a234567", // Pool 2
    ///     ])?;
    ///
    /// // Using byte arrays
    /// let miner_address = [
    ///     0xda, 0xc1, 0x7f, 0x95, 0x8d, 0x2e, 0xe5, 0x23, 0xa2, 0x20,
    ///     0x62, 0x06, 0x99, 0x45, 0x97, 0xc1, 0x3d, 0x83, 0x1e, 0xc7
    /// ];
    /// let filter = BlockFilter::any()
    ///     .and_miner_address([miner_address])?;
    ///
    /// // Chain with other filter methods
    /// let filter = BlockFilter::any()
    ///     .and_hash(["0x40d008f2a1653f09b7b028d30c7fd1ba7c84900fcfb032040b3eb3d16f84d294"])?
    ///     .and_miner_address(["0xdac17f958d2ee523a2206206994597c13d831ec7"])?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn and_miner_address<I, A>(mut self, addresses: I) -> anyhow::Result<Self>
    where
        I: IntoIterator<Item = A>,
        A: TryInto<Address>,
        A::Error: std::error::Error + Send + Sync + 'static,
    {
        let mut converted_addresses: Vec<Address> = Vec::new();
        for (idx, address) in addresses.into_iter().enumerate() {
            converted_addresses.push(
                address
                    .try_into()
                    .with_context(|| format!("invalid miner address value at position {idx}"))?,
            );
        }
        self.miner = converted_addresses;
        Ok(self)
    }
}

impl CapnpReader<hypersync_net_types_capnp::block_filter::Owned> for BlockFilter {
    /// Deserialize BlockSelection from Cap'n Proto reader
    fn from_reader(
        reader: hypersync_net_types_capnp::block_filter::Reader,
    ) -> Result<Self, capnp::Error> {
        let mut hash = Vec::new();

        // Parse hashes
        if reader.has_hash() {
            let hash_list = reader.get_hash()?;
            for i in 0..hash_list.len() {
                let hash_data = hash_list.get(i)?;
                if hash_data.len() == 32 {
                    let mut hash_bytes = [0u8; 32];
                    hash_bytes.copy_from_slice(hash_data);
                    hash.push(Hash::from(hash_bytes));
                }
            }
        }

        let mut miner = Vec::new();

        // Parse miners
        if reader.has_miner() {
            let miner_list = reader.get_miner()?;
            for i in 0..miner_list.len() {
                let addr_data = miner_list.get(i)?;
                if addr_data.len() == 20 {
                    let mut addr_bytes = [0u8; 20];
                    addr_bytes.copy_from_slice(addr_data);
                    miner.push(Address::from(addr_bytes));
                }
            }
        }

        Ok(Self { hash, miner })
    }
}

impl CapnpBuilder<hypersync_net_types_capnp::block_filter::Owned> for BlockFilter {
    fn populate_builder(
        &self,
        builder: &mut hypersync_net_types_capnp::block_filter::Builder,
    ) -> Result<(), capnp::Error> {
        // Set hashes
        if !self.hash.is_empty() {
            let mut hash_list = builder.reborrow().init_hash(self.hash.len() as u32);
            for (i, hash) in self.hash.iter().enumerate() {
                hash_list.set(i as u32, hash.as_slice());
            }
        }

        // Set miners
        if !self.miner.is_empty() {
            let mut miner_list = builder.reborrow().init_miner(self.miner.len() as u32);
            for (i, miner) in self.miner.iter().enumerate() {
                miner_list.set(i as u32, miner.as_slice());
            }
        }

        Ok(())
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    schemars::JsonSchema,
    strum_macros::EnumIter,
    strum_macros::AsRefStr,
    strum_macros::Display,
    strum_macros::EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum BlockField {
    // Non-nullable fields (required)
    Number,
    Hash,
    ParentHash,
    Sha3Uncles,
    LogsBloom,
    TransactionsRoot,
    StateRoot,
    ReceiptsRoot,
    Miner,
    ExtraData,
    Size,
    GasLimit,
    GasUsed,
    Timestamp,
    MixHash,

    // Nullable fields (optional)
    Nonce,
    Difficulty,
    TotalDifficulty,
    Uncles,
    BaseFeePerGas,
    BlobGasUsed,
    ExcessBlobGas,
    ParentBeaconBlockRoot,
    WithdrawalsRoot,
    Withdrawals,
    L1BlockNumber,
    SendCount,
    SendRoot,
}

impl Ord for BlockField {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_ref().cmp(other.as_ref())
    }
}

impl PartialOrd for BlockField {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl BlockField {
    pub fn all() -> BTreeSet<Self> {
        use strum::IntoEnumIterator;
        Self::iter().collect()
    }

    /// Convert BlockField to Cap'n Proto enum
    pub fn to_capnp(&self) -> crate::hypersync_net_types_capnp::BlockField {
        match self {
            BlockField::Number => crate::hypersync_net_types_capnp::BlockField::Number,
            BlockField::Hash => crate::hypersync_net_types_capnp::BlockField::Hash,
            BlockField::ParentHash => crate::hypersync_net_types_capnp::BlockField::ParentHash,
            BlockField::Sha3Uncles => crate::hypersync_net_types_capnp::BlockField::Sha3Uncles,
            BlockField::LogsBloom => crate::hypersync_net_types_capnp::BlockField::LogsBloom,
            BlockField::TransactionsRoot => {
                crate::hypersync_net_types_capnp::BlockField::TransactionsRoot
            }
            BlockField::StateRoot => crate::hypersync_net_types_capnp::BlockField::StateRoot,
            BlockField::ReceiptsRoot => crate::hypersync_net_types_capnp::BlockField::ReceiptsRoot,
            BlockField::Miner => crate::hypersync_net_types_capnp::BlockField::Miner,
            BlockField::ExtraData => crate::hypersync_net_types_capnp::BlockField::ExtraData,
            BlockField::Size => crate::hypersync_net_types_capnp::BlockField::Size,
            BlockField::GasLimit => crate::hypersync_net_types_capnp::BlockField::GasLimit,
            BlockField::GasUsed => crate::hypersync_net_types_capnp::BlockField::GasUsed,
            BlockField::Timestamp => crate::hypersync_net_types_capnp::BlockField::Timestamp,
            BlockField::MixHash => crate::hypersync_net_types_capnp::BlockField::MixHash,
            BlockField::Nonce => crate::hypersync_net_types_capnp::BlockField::Nonce,
            BlockField::Difficulty => crate::hypersync_net_types_capnp::BlockField::Difficulty,
            BlockField::TotalDifficulty => {
                crate::hypersync_net_types_capnp::BlockField::TotalDifficulty
            }
            BlockField::Uncles => crate::hypersync_net_types_capnp::BlockField::Uncles,
            BlockField::BaseFeePerGas => {
                crate::hypersync_net_types_capnp::BlockField::BaseFeePerGas
            }
            BlockField::BlobGasUsed => crate::hypersync_net_types_capnp::BlockField::BlobGasUsed,
            BlockField::ExcessBlobGas => {
                crate::hypersync_net_types_capnp::BlockField::ExcessBlobGas
            }
            BlockField::ParentBeaconBlockRoot => {
                crate::hypersync_net_types_capnp::BlockField::ParentBeaconBlockRoot
            }
            BlockField::WithdrawalsRoot => {
                crate::hypersync_net_types_capnp::BlockField::WithdrawalsRoot
            }
            BlockField::Withdrawals => crate::hypersync_net_types_capnp::BlockField::Withdrawals,
            BlockField::L1BlockNumber => {
                crate::hypersync_net_types_capnp::BlockField::L1BlockNumber
            }
            BlockField::SendCount => crate::hypersync_net_types_capnp::BlockField::SendCount,
            BlockField::SendRoot => crate::hypersync_net_types_capnp::BlockField::SendRoot,
        }
    }

    /// Convert Cap'n Proto enum to BlockField
    pub fn from_capnp(field: crate::hypersync_net_types_capnp::BlockField) -> Self {
        match field {
            crate::hypersync_net_types_capnp::BlockField::Number => BlockField::Number,
            crate::hypersync_net_types_capnp::BlockField::Hash => BlockField::Hash,
            crate::hypersync_net_types_capnp::BlockField::ParentHash => BlockField::ParentHash,
            crate::hypersync_net_types_capnp::BlockField::Sha3Uncles => BlockField::Sha3Uncles,
            crate::hypersync_net_types_capnp::BlockField::LogsBloom => BlockField::LogsBloom,
            crate::hypersync_net_types_capnp::BlockField::TransactionsRoot => {
                BlockField::TransactionsRoot
            }
            crate::hypersync_net_types_capnp::BlockField::StateRoot => BlockField::StateRoot,
            crate::hypersync_net_types_capnp::BlockField::ReceiptsRoot => BlockField::ReceiptsRoot,
            crate::hypersync_net_types_capnp::BlockField::Miner => BlockField::Miner,
            crate::hypersync_net_types_capnp::BlockField::ExtraData => BlockField::ExtraData,
            crate::hypersync_net_types_capnp::BlockField::Size => BlockField::Size,
            crate::hypersync_net_types_capnp::BlockField::GasLimit => BlockField::GasLimit,
            crate::hypersync_net_types_capnp::BlockField::GasUsed => BlockField::GasUsed,
            crate::hypersync_net_types_capnp::BlockField::Timestamp => BlockField::Timestamp,
            crate::hypersync_net_types_capnp::BlockField::MixHash => BlockField::MixHash,
            crate::hypersync_net_types_capnp::BlockField::Nonce => BlockField::Nonce,
            crate::hypersync_net_types_capnp::BlockField::Difficulty => BlockField::Difficulty,
            crate::hypersync_net_types_capnp::BlockField::TotalDifficulty => {
                BlockField::TotalDifficulty
            }
            crate::hypersync_net_types_capnp::BlockField::Uncles => BlockField::Uncles,
            crate::hypersync_net_types_capnp::BlockField::BaseFeePerGas => {
                BlockField::BaseFeePerGas
            }
            crate::hypersync_net_types_capnp::BlockField::BlobGasUsed => BlockField::BlobGasUsed,
            crate::hypersync_net_types_capnp::BlockField::ExcessBlobGas => {
                BlockField::ExcessBlobGas
            }
            crate::hypersync_net_types_capnp::BlockField::ParentBeaconBlockRoot => {
                BlockField::ParentBeaconBlockRoot
            }
            crate::hypersync_net_types_capnp::BlockField::WithdrawalsRoot => {
                BlockField::WithdrawalsRoot
            }
            crate::hypersync_net_types_capnp::BlockField::Withdrawals => BlockField::Withdrawals,
            crate::hypersync_net_types_capnp::BlockField::L1BlockNumber => {
                BlockField::L1BlockNumber
            }
            crate::hypersync_net_types_capnp::BlockField::SendCount => BlockField::SendCount,
            crate::hypersync_net_types_capnp::BlockField::SendRoot => BlockField::SendRoot,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use hypersync_format::Hex;

    use super::*;
    use crate::{query::tests::test_query_serde, Query};

    #[test]
    fn test_all_fields_in_schema() {
        let schema = hypersync_schema::block_header();
        let schema_fields = schema
            .fields
            .iter()
            .map(|f| f.name.clone())
            .collect::<BTreeSet<_>>();
        let all_fields = BlockField::all()
            .into_iter()
            .map(|f| f.as_ref().to_string())
            .collect::<BTreeSet<_>>();
        assert_eq!(schema_fields, all_fields);
    }

    #[test]
    fn test_serde_matches_strum() {
        for field in BlockField::all() {
            let serialized = serde_json::to_string(&field).unwrap();
            let strum = serde_json::to_string(&field.as_ref()).unwrap();
            assert_eq!(serialized, strum, "strum value should be the same as serde");
        }
    }

    #[test]
    fn test_block_filter_serde_with_values() {
        let block_filter = BlockFilter {
            hash: vec![Hash::decode_hex(
                "0x40d008f2a1653f09b7b028d30c7fd1ba7c84900fcfb032040b3eb3d16f84d294",
            )
            .unwrap()],
            miner: vec![Address::decode_hex("0xdadB0d80178819F2319190D340ce9A924f783711").unwrap()],
        };
        let query = Query::new()
            .where_blocks(block_filter)
            .select_block_fields(BlockField::all());

        test_query_serde(query, "block selection with rest defaults");
    }

    #[test]
    fn test_as_str() {
        let block_field = BlockField::Number;
        let from_str = BlockField::from_str("number").unwrap();
        assert_eq!(block_field, from_str);
    }
}
