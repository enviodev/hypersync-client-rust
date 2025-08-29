use hypersync_format::Hash;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Deserialize, Serialize, Debug)]
pub struct ArchiveHeight {
    pub height: Option<u64>,
}

#[derive(Clone, Copy, Deserialize, Serialize, Debug)]
pub struct ChainId {
    pub chain_id: u64,
}

/// Guard for detecting rollbacks
#[derive(Debug, Clone, Serialize)]
pub struct RollbackGuard {
    /// Block number of last block scanned in memory
    pub block_number: u64,
    /// Block timestamp of last block scanned in memory
    pub timestamp: i64,
    /// Block hash of last block scanned in memory
    pub hash: Hash,
    /// Block number of first block scanned in memory
    pub first_block_number: u64,
    /// Parent hash of first block scanned in memory
    pub first_parent_hash: Hash,
}
