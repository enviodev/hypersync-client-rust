use crate::hypersync_net_types_capnp;
use hypersync_format::{Address, Hash};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct BlockSelection {
    /// Hash of a block, any blocks that have one of these hashes will be returned.
    /// Empty means match all.
    #[serde(default)]
    pub hash: Vec<Hash>,
    /// Miner address of a block, any blocks that have one of these miners will be returned.
    /// Empty means match all.
    #[serde(default)]
    pub miner: Vec<Address>,
}

impl BlockSelection {
    pub(crate) fn populate_capnp_builder(
        block_sel: &BlockSelection,
        mut builder: hypersync_net_types_capnp::block_selection::Builder,
    ) -> Result<(), capnp::Error> {
        // Set hashes
        {
            let mut hash_list = builder.reborrow().init_hash(block_sel.hash.len() as u32);
            for (i, hash) in block_sel.hash.iter().enumerate() {
                hash_list.set(i as u32, hash.as_slice());
            }
        }

        // Set miners
        {
            let mut miner_list = builder.reborrow().init_miner(block_sel.miner.len() as u32);
            for (i, miner) in block_sel.miner.iter().enumerate() {
                miner_list.set(i as u32, miner.as_slice());
            }
        }

        Ok(())
    }

    /// Deserialize BlockSelection from Cap'n Proto reader
    pub fn from_capnp(
        reader: hypersync_net_types_capnp::block_selection::Reader,
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

        Ok(BlockSelection { hash, miner })
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
    use crate::{query::tests::test_query_serde, FieldSelection, Query};

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
    fn test_block_selection_serde_with_values() {
        let block_selection = BlockSelection {
            hash: vec![Hash::decode_hex(
                "0x40d008f2a1653f09b7b028d30c7fd1ba7c84900fcfb032040b3eb3d16f84d294",
            )
            .unwrap()],
            miner: vec![Address::decode_hex("0xdadB0d80178819F2319190D340ce9A924f783711").unwrap()],
        };
        let field_selection = FieldSelection {
            block: BlockField::all(),
            ..Default::default()
        };
        let query = Query {
            blocks: vec![block_selection],
            field_selection,
            ..Default::default()
        };

        test_query_serde(query, "block selection with rest defaults");
    }

    #[test]
    fn test_as_str() {
        let block_field = BlockField::Number;
        let from_str = BlockField::from_str("number").unwrap();
        assert_eq!(block_field, from_str);
    }
}
