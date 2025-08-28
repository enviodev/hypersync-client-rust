use crate::hypersync_net_types_capnp;
use hypersync_format::{Address, Hash};
use serde::{Deserialize, Serialize};

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq)]
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
}
