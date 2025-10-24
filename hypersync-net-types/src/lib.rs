use std::collections::BTreeSet;

/// Used to skip serializing a defaulted serde field if
/// the value matches the default value.
fn is_default<T: Default + PartialEq>(t: &T) -> bool {
    t == &T::default()
}

use arrayvec::ArrayVec;
use hypersync_format::{Address, FilterWrapper, FixedSizeData, Hash, LogArgument};
use serde::{Deserialize, Serialize};

pub type Sighash = FixedSizeData<4>;

#[allow(clippy::all)]
pub mod hypersync_net_types_capnp {
    include!(concat!(env!("OUT_DIR"), "/hypersync_net_types_capnp.rs"));
}

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Selection<T> {
    /// Filters where matching values should be included in the response
    /// Default::default() means include everything
    #[serde(default, flatten)]
    pub include: T,
    /// Filters where matching values should be excluded from the response
    /// None means exclude nothing, Some(Default::default()) means exclude everything
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exclude: Option<T>,
}

impl<T> From<T> for Selection<T> {
    fn from(include: T) -> Self {
        Self {
            include,
            exclude: None,
        }
    }
}

pub type BlockSelection = Selection<BlockFilter>;

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

pub type LogSelection = Selection<LogFilter>;

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct LogFilter {
    /// Address of the contract, any logs that has any of these addresses will be returned.
    /// Empty means match all.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub address: Vec<Address>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address_filter: Option<FilterWrapper>,
    /// Topics to match, each member of the top level array is another array, if the nth topic matches any
    ///  topic specified in nth element of topics, the log will be returned. Empty means match all.
    #[serde(default, skip_serializing_if = "ArrayVec::is_empty")]
    pub topics: ArrayVec<Vec<LogArgument>, 4>,
}

pub type TransactionSelection = Selection<TransactionFilter>;

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct TransactionFilter {
    /// Address the transaction should originate from. If transaction.from matches any of these, the transaction
    /// will be returned. Keep in mind that this has an and relationship with to filter, so each transaction should
    /// match both of them. Empty means match all.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub from: Vec<Address>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_filter: Option<FilterWrapper>,
    /// Address the transaction should go to. If transaction.to matches any of these, the transaction will
    /// be returned. Keep in mind that this has an and relationship with from filter, so each transaction should
    /// match both of them. Empty means match all.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub to: Vec<Address>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to_filter: Option<FilterWrapper>,
    /// If first 4 bytes of transaction input matches any of these, transaction will be returned. Empty means match all.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sighash: Vec<Sighash>,
    /// If transaction.status matches this value, the transaction will be returned.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<u8>,
    /// If transaction.type matches any of these values, the transaction will be returned
    #[serde(rename = "type")]
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub kind: Vec<u8>,
    /// If transaction.contract_address matches any of these values, the transaction will be returned.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub contract_address: Vec<Address>,
    /// Bloom filter to filter by transaction.contract_address field. If the bloom filter contains the hash
    /// of transaction.contract_address then the transaction will be returned. This field doesn't utilize the server side filtering
    /// so it should be used alongside some non-probabilistic filters if possible.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contract_address_filter: Option<FilterWrapper>,
    /// If transaction.hash matches any of these values the transaction will be returned.
    /// empty means match all.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hash: Vec<Hash>,

    /// List of authorizations from eip-7702 transactions, the query will return transactions that match any of these selections
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub authorization_list: Vec<AuthorizationSelection>,
}

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct AuthorizationSelection {
    /// List of chain ids to match in the transaction authorizationList
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub chain_id: Vec<u64>,
    /// List of addresses to match in the transaction authorizationList
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub address: Vec<Address>,
}

pub type TraceSelection = Selection<TraceFilter>;

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct TraceFilter {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub from: Vec<Address>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_filter: Option<FilterWrapper>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub to: Vec<Address>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to_filter: Option<FilterWrapper>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub address: Vec<Address>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address_filter: Option<FilterWrapper>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub call_type: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reward_type: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "type")]
    pub kind: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sighash: Vec<Sighash>,
}

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Query {
    /// The block to start the query from
    pub from_block: u64,
    /// The block to end the query at. If not specified, the query will go until the
    ///  end of data. Exclusive, the returned range will be [from_block..to_block).
    ///
    /// The query will return before it reaches this target block if it hits the time limit
    ///  configured on the server. The user should continue their query by putting the
    ///  next_block field in the response into from_block field of their next query. This implements
    ///  pagination.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_block: Option<u64>,
    /// List of log selections, these have an OR relationship between them, so the query will return logs
    /// that match any of these selections.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub logs: Vec<LogSelection>,
    /// List of transaction selections, the query will return transactions that match any of these selections
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transactions: Vec<TransactionSelection>,
    /// List of trace selections, the query will return traces that match any of these selections
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub traces: Vec<TraceSelection>,
    /// List of block selections, the query will return blocks that match any of these selections
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blocks: Vec<BlockSelection>,
    /// Weather to include all blocks regardless of if they are related to a returned transaction or log. Normally
    ///  the server will return only the blocks that are related to the transaction or logs in the response. But if this
    ///  is set to true, the server will return data for all blocks in the requested range [from_block, to_block).
    #[serde(default, skip_serializing_if = "is_default")]
    pub include_all_blocks: bool,
    /// Field selection. The user can select which fields they are interested in, requesting less fields will improve
    ///  query execution time and reduce the payload size so the user should always use a minimal number of fields.
    #[serde(default, skip_serializing_if = "is_default")]
    pub field_selection: FieldSelection,
    /// Maximum number of blocks that should be returned, the server might return more blocks than this number but
    ///  it won't overshoot by too much.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_num_blocks: Option<usize>,
    /// Maximum number of transactions that should be returned, the server might return more transactions than this number but
    ///  it won't overshoot by too much.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_num_transactions: Option<usize>,
    /// Maximum number of logs that should be returned, the server might return more logs than this number but
    ///  it won't overshoot by too much.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_num_logs: Option<usize>,
    /// Maximum number of traces that should be returned, the server might return more traces than this number but
    ///  it won't overshoot by too much.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_num_traces: Option<usize>,
    /// Selects join mode for the query,
    /// Default: join in this order logs -> transactions -> traces -> blocks
    /// JoinAll: join everything to everything. For example if logSelection matches log0, we get the
    /// associated transaction of log0 and then we get associated logs of that transaction as well. Applites similarly
    /// to blocks, traces.
    /// JoinNothing: join nothing.
    #[serde(default, skip_serializing_if = "is_default")]
    pub join_mode: JoinMode,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum JoinMode {
    /// Join in this order logs -> transactions -> traces -> blocks
    Default,
    /// Join everything to everything. For example if logSelection matches log0, we get the
    /// associated transaction of log0 and then we get associated logs of that transaction as well. Applites similarly
    /// to blocks, traces.
    JoinAll,
    /// JoinNothing: join nothing.
    JoinNothing,
}

impl Default for JoinMode {
    fn default() -> Self {
        Self::Default
    }
}

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct FieldSelection {
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub block: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub transaction: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub log: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub trace: BTreeSet<String>,
}

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
