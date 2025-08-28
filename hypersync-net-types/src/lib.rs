use std::collections::BTreeSet;

use arrayvec::ArrayVec;
use hypersync_format::{Address, FilterWrapper, FixedSizeData, Hash, LogArgument};
use serde::{Deserialize, Serialize};
use capnp::message::Builder;
use capnp::serialize;

pub type Sighash = FixedSizeData<4>;

#[allow(clippy::all)]
pub mod hypersync_net_types_capnp {
    include!(concat!(env!("OUT_DIR"), "/hypersync_net_types_capnp.rs"));
}

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

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct LogSelection {
    /// Address of the contract, any logs that has any of these addresses will be returned.
    /// Empty means match all.
    #[serde(default)]
    pub address: Vec<Address>,
    #[serde(default)]
    pub address_filter: Option<FilterWrapper>,
    /// Topics to match, each member of the top level array is another array, if the nth topic matches any
    ///  topic specified in nth element of topics, the log will be returned. Empty means match all.
    #[serde(default)]
    pub topics: ArrayVec<Vec<LogArgument>, 4>,
}

#[derive(Default, Serialize, Deserialize, Clone, Debug)]
pub struct TransactionSelection {
    /// Address the transaction should originate from. If transaction.from matches any of these, the transaction
    /// will be returned. Keep in mind that this has an and relationship with to filter, so each transaction should
    /// match both of them. Empty means match all.
    #[serde(default)]
    pub from: Vec<Address>,
    #[serde(default)]
    pub from_filter: Option<FilterWrapper>,
    /// Address the transaction should go to. If transaction.to matches any of these, the transaction will
    /// be returned. Keep in mind that this has an and relationship with from filter, so each transaction should
    /// match both of them. Empty means match all.
    #[serde(default)]
    pub to: Vec<Address>,
    #[serde(default)]
    pub to_filter: Option<FilterWrapper>,
    /// If first 4 bytes of transaction input matches any of these, transaction will be returned. Empty means match all.
    #[serde(default)]
    pub sighash: Vec<Sighash>,
    /// If transaction.status matches this value, the transaction will be returned.
    pub status: Option<u8>,
    /// If transaction.type matches any of these values, the transaction will be returned
    #[serde(rename = "type")]
    #[serde(default)]
    pub kind: Vec<u8>,
    /// If transaction.contract_address matches any of these values, the transaction will be returned.
    #[serde(default)]
    pub contract_address: Vec<Address>,
    /// Bloom filter to filter by transaction.contract_address field. If the bloom filter contains the hash
    /// of transaction.contract_address then the transaction will be returned. This field doesn't utilize the server side filtering
    /// so it should be used alongside some non-probabilistic filters if possible.
    #[serde(default)]
    pub contract_address_filter: Option<FilterWrapper>,
    /// If transaction.hash matches any of these values the transaction will be returned.
    /// empty means match all.
    #[serde(default)]
    pub hash: Vec<Hash>,

    /// List of authorizations from eip-7702 transactions, the query will return transactions that match any of these selections
    #[serde(default)]
    pub authorization_list: Vec<AuthorizationSelection>,
}

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct AuthorizationSelection {
    /// List of chain ids to match in the transaction authorizationList
    #[serde(default)]
    pub chain_id: Vec<u64>,
    /// List of addresses to match in the transaction authorizationList
    #[serde(default)]
    pub address: Vec<Address>,
}

#[derive(Default, Serialize, Deserialize, Clone, Debug)]
pub struct TraceSelection {
    #[serde(default)]
    pub from: Vec<Address>,
    #[serde(default)]
    pub from_filter: Option<FilterWrapper>,
    #[serde(default)]
    pub to: Vec<Address>,
    #[serde(default)]
    pub to_filter: Option<FilterWrapper>,
    #[serde(default)]
    pub address: Vec<Address>,
    #[serde(default)]
    pub address_filter: Option<FilterWrapper>,
    #[serde(default)]
    pub call_type: Vec<String>,
    #[serde(default)]
    pub reward_type: Vec<String>,
    #[serde(default)]
    #[serde(rename = "type")]
    pub kind: Vec<String>,
    #[serde(default)]
    pub sighash: Vec<Sighash>,
}

#[derive(Default, Serialize, Deserialize, Clone, Debug)]
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
    pub to_block: Option<u64>,
    /// List of log selections, these have an OR relationship between them, so the query will return logs
    /// that match any of these selections.
    #[serde(default)]
    pub logs: Vec<LogSelection>,
    /// List of transaction selections, the query will return transactions that match any of these selections
    #[serde(default)]
    pub transactions: Vec<TransactionSelection>,
    /// List of trace selections, the query will return traces that match any of these selections
    #[serde(default)]
    pub traces: Vec<TraceSelection>,
    /// List of block selections, the query will return blocks that match any of these selections
    #[serde(default)]
    pub blocks: Vec<BlockSelection>,
    /// Weather to include all blocks regardless of if they are related to a returned transaction or log. Normally
    ///  the server will return only the blocks that are related to the transaction or logs in the response. But if this
    ///  is set to true, the server will return data for all blocks in the requested range [from_block, to_block).
    #[serde(default)]
    pub include_all_blocks: bool,
    /// Field selection. The user can select which fields they are interested in, requesting less fields will improve
    ///  query execution time and reduce the payload size so the user should always use a minimal number of fields.
    #[serde(default)]
    pub field_selection: FieldSelection,
    /// Maximum number of blocks that should be returned, the server might return more blocks than this number but
    ///  it won't overshoot by too much.
    #[serde(default)]
    pub max_num_blocks: Option<usize>,
    /// Maximum number of transactions that should be returned, the server might return more transactions than this number but
    ///  it won't overshoot by too much.
    #[serde(default)]
    pub max_num_transactions: Option<usize>,
    /// Maximum number of logs that should be returned, the server might return more logs than this number but
    ///  it won't overshoot by too much.
    #[serde(default)]
    pub max_num_logs: Option<usize>,
    /// Maximum number of traces that should be returned, the server might return more traces than this number but
    ///  it won't overshoot by too much.
    #[serde(default)]
    pub max_num_traces: Option<usize>,
    /// Selects join mode for the query,
    /// Default: join in this order logs -> transactions -> traces -> blocks
    /// JoinAll: join everything to everything. For example if logSelection matches log0, we get the
    /// associated transaction of log0 and then we get associated logs of that transaction as well. Applites similarly
    /// to blocks, traces.
    /// JoinNothing: join nothing.
    #[serde(default)]
    pub join_mode: JoinMode,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum JoinMode {
    Default,
    JoinAll,
    JoinNothing,
}

impl Default for JoinMode {
    fn default() -> Self {
        Self::Default
    }
}

#[derive(Default, Serialize, Deserialize, Clone, Debug)]
pub struct FieldSelection {
    #[serde(default)]
    pub block: BTreeSet<String>,
    #[serde(default)]
    pub transaction: BTreeSet<String>,
    #[serde(default)]
    pub log: BTreeSet<String>,
    #[serde(default)]
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

impl Query {
    /// Serialize Query to Cap'n Proto format and return as bytes
    pub fn to_capnp_bytes(&self) -> Result<Vec<u8>, capnp::Error> {
        let mut message = Builder::new_default();
        let query = message.init_root::<hypersync_net_types_capnp::query::Builder>();
        
        self.populate_capnp_query(query)?;
        
        let mut buf = Vec::new();
        serialize::write_message(&mut buf, &message)?;
        Ok(buf)
    }

    fn populate_capnp_query(&self, mut query: hypersync_net_types_capnp::query::Builder) -> Result<(), capnp::Error> {
        query.reborrow().set_from_block(self.from_block);
        
        if let Some(to_block) = self.to_block {
            query.reborrow().set_to_block(to_block);
        }

        query.reborrow().set_include_all_blocks(self.include_all_blocks);

        // Set max nums
        if let Some(max_num_blocks) = self.max_num_blocks {
            query.reborrow().set_max_num_blocks(max_num_blocks as u64);
        }
        if let Some(max_num_transactions) = self.max_num_transactions {
            query.reborrow().set_max_num_transactions(max_num_transactions as u64);
        }
        if let Some(max_num_logs) = self.max_num_logs {
            query.reborrow().set_max_num_logs(max_num_logs as u64);
        }
        if let Some(max_num_traces) = self.max_num_traces {
            query.reborrow().set_max_num_traces(max_num_traces as u64);
        }

        // Set join mode
        let join_mode = match self.join_mode {
            JoinMode::Default => hypersync_net_types_capnp::JoinMode::Default,
            JoinMode::JoinAll => hypersync_net_types_capnp::JoinMode::JoinAll,
            JoinMode::JoinNothing => hypersync_net_types_capnp::JoinMode::JoinNothing,
        };
        query.reborrow().set_join_mode(join_mode);

        // Set field selection
        {
            let mut field_selection = query.reborrow().init_field_selection();
            
            let block_fields: Vec<&str> = self.field_selection.block.iter().map(|s| s.as_str()).collect();
            let mut block_list = field_selection.reborrow().init_block(block_fields.len() as u32);
            for (i, field) in block_fields.iter().enumerate() {
                block_list.set(i as u32, field);
            }

            let tx_fields: Vec<&str> = self.field_selection.transaction.iter().map(|s| s.as_str()).collect();
            let mut tx_list = field_selection.reborrow().init_transaction(tx_fields.len() as u32);
            for (i, field) in tx_fields.iter().enumerate() {
                tx_list.set(i as u32, field);
            }

            let log_fields: Vec<&str> = self.field_selection.log.iter().map(|s| s.as_str()).collect();
            let mut log_list = field_selection.reborrow().init_log(log_fields.len() as u32);
            for (i, field) in log_fields.iter().enumerate() {
                log_list.set(i as u32, field);
            }

            let trace_fields: Vec<&str> = self.field_selection.trace.iter().map(|s| s.as_str()).collect();
            let mut trace_list = field_selection.reborrow().init_trace(trace_fields.len() as u32);
            for (i, field) in trace_fields.iter().enumerate() {
                trace_list.set(i as u32, field);
            }
        }

        // Set logs
        {
            let mut logs_list = query.reborrow().init_logs(self.logs.len() as u32);
            for (i, log_selection) in self.logs.iter().enumerate() {
                let log_sel = logs_list.reborrow().get(i as u32);
                Self::populate_log_selection(log_selection, log_sel)?;
            }
        }

        // Set transactions
        {
            let mut tx_list = query.reborrow().init_transactions(self.transactions.len() as u32);
            for (i, tx_selection) in self.transactions.iter().enumerate() {
                let tx_sel = tx_list.reborrow().get(i as u32);
                Self::populate_transaction_selection(tx_selection, tx_sel)?;
            }
        }

        // Set traces
        {
            let mut trace_list = query.reborrow().init_traces(self.traces.len() as u32);
            for (i, trace_selection) in self.traces.iter().enumerate() {
                let trace_sel = trace_list.reborrow().get(i as u32);
                Self::populate_trace_selection(trace_selection, trace_sel)?;
            }
        }

        // Set blocks
        {
            let mut block_list = query.reborrow().init_blocks(self.blocks.len() as u32);
            for (i, block_selection) in self.blocks.iter().enumerate() {
                let block_sel = block_list.reborrow().get(i as u32);
                Self::populate_block_selection(block_selection, block_sel)?;
            }
        }

        Ok(())
    }

    fn populate_log_selection(
        log_sel: &LogSelection, 
        mut builder: hypersync_net_types_capnp::log_selection::Builder
    ) -> Result<(), capnp::Error> {
        // Set addresses
        {
            let mut addr_list = builder.reborrow().init_address(log_sel.address.len() as u32);
            for (i, addr) in log_sel.address.iter().enumerate() {
                addr_list.set(i as u32, addr.as_slice());
            }
        }

        // Set address filter
        if let Some(filter) = &log_sel.address_filter {
            builder.reborrow().set_address_filter(filter.0.as_bytes());
        }

        // Set topics
        {
            let mut topics_list = builder.reborrow().init_topics(log_sel.topics.len() as u32);
            for (i, topic_vec) in log_sel.topics.iter().enumerate() {
                let mut topic_list = topics_list.reborrow().init(i as u32, topic_vec.len() as u32);
                for (j, topic) in topic_vec.iter().enumerate() {
                    topic_list.set(j as u32, topic.as_slice());
                }
            }
        }

        Ok(())
    }

    fn populate_transaction_selection(
        tx_sel: &TransactionSelection, 
        mut builder: hypersync_net_types_capnp::transaction_selection::Builder
    ) -> Result<(), capnp::Error> {
        // Set from addresses
        {
            let mut from_list = builder.reborrow().init_from(tx_sel.from.len() as u32);
            for (i, addr) in tx_sel.from.iter().enumerate() {
                from_list.set(i as u32, addr.as_slice());
            }
        }

        // Set from filter
        if let Some(filter) = &tx_sel.from_filter {
            builder.reborrow().set_from_filter(filter.0.as_bytes());
        }

        // Set to addresses
        {
            let mut to_list = builder.reborrow().init_to(tx_sel.to.len() as u32);
            for (i, addr) in tx_sel.to.iter().enumerate() {
                to_list.set(i as u32, addr.as_slice());
            }
        }

        // Set to filter
        if let Some(filter) = &tx_sel.to_filter {
            builder.reborrow().set_to_filter(filter.0.as_bytes());
        }

        // Set sighash
        {
            let mut sighash_list = builder.reborrow().init_sighash(tx_sel.sighash.len() as u32);
            for (i, sighash) in tx_sel.sighash.iter().enumerate() {
                sighash_list.set(i as u32, sighash.as_slice());
            }
        }

        // Set status
        if let Some(status) = tx_sel.status {
            builder.reborrow().set_status(status);
        }

        // Set kind
        {
            let mut kind_list = builder.reborrow().init_kind(tx_sel.kind.len() as u32);
            for (i, kind) in tx_sel.kind.iter().enumerate() {
                kind_list.set(i as u32, *kind);
            }
        }

        // Set contract addresses
        {
            let mut contract_list = builder.reborrow().init_contract_address(tx_sel.contract_address.len() as u32);
            for (i, addr) in tx_sel.contract_address.iter().enumerate() {
                contract_list.set(i as u32, addr.as_slice());
            }
        }

        // Set contract address filter
        if let Some(filter) = &tx_sel.contract_address_filter {
            builder.reborrow().set_contract_address_filter(filter.0.as_bytes());
        }

        // Set hashes
        {
            let mut hash_list = builder.reborrow().init_hash(tx_sel.hash.len() as u32);
            for (i, hash) in tx_sel.hash.iter().enumerate() {
                hash_list.set(i as u32, hash.as_slice());
            }
        }

        // Set authorization list
        {
            let mut auth_list = builder.reborrow().init_authorization_list(tx_sel.authorization_list.len() as u32);
            for (i, auth_sel) in tx_sel.authorization_list.iter().enumerate() {
                let auth_builder = auth_list.reborrow().get(i as u32);
                Self::populate_authorization_selection(auth_sel, auth_builder)?;
            }
        }

        Ok(())
    }

    fn populate_authorization_selection(
        auth_sel: &AuthorizationSelection,
        mut builder: hypersync_net_types_capnp::authorization_selection::Builder
    ) -> Result<(), capnp::Error> {
        // Set chain ids
        {
            let mut chain_list = builder.reborrow().init_chain_id(auth_sel.chain_id.len() as u32);
            for (i, chain_id) in auth_sel.chain_id.iter().enumerate() {
                chain_list.set(i as u32, *chain_id);
            }
        }

        // Set addresses
        {
            let mut addr_list = builder.reborrow().init_address(auth_sel.address.len() as u32);
            for (i, addr) in auth_sel.address.iter().enumerate() {
                addr_list.set(i as u32, addr.as_slice());
            }
        }

        Ok(())
    }

    fn populate_trace_selection(
        trace_sel: &TraceSelection, 
        mut builder: hypersync_net_types_capnp::trace_selection::Builder
    ) -> Result<(), capnp::Error> {
        // Set from addresses
        {
            let mut from_list = builder.reborrow().init_from(trace_sel.from.len() as u32);
            for (i, addr) in trace_sel.from.iter().enumerate() {
                from_list.set(i as u32, addr.as_slice());
            }
        }

        // Set from filter
        if let Some(filter) = &trace_sel.from_filter {
            builder.reborrow().set_from_filter(filter.0.as_bytes());
        }

        // Set to addresses
        {
            let mut to_list = builder.reborrow().init_to(trace_sel.to.len() as u32);
            for (i, addr) in trace_sel.to.iter().enumerate() {
                to_list.set(i as u32, addr.as_slice());
            }
        }

        // Set to filter
        if let Some(filter) = &trace_sel.to_filter {
            builder.reborrow().set_to_filter(filter.0.as_bytes());
        }

        // Set addresses
        {
            let mut addr_list = builder.reborrow().init_address(trace_sel.address.len() as u32);
            for (i, addr) in trace_sel.address.iter().enumerate() {
                addr_list.set(i as u32, addr.as_slice());
            }
        }

        // Set address filter
        if let Some(filter) = &trace_sel.address_filter {
            builder.reborrow().set_address_filter(filter.0.as_bytes());
        }

        // Set call types
        {
            let mut call_type_list = builder.reborrow().init_call_type(trace_sel.call_type.len() as u32);
            for (i, call_type) in trace_sel.call_type.iter().enumerate() {
                call_type_list.set(i as u32, call_type);
            }
        }

        // Set reward types
        {
            let mut reward_type_list = builder.reborrow().init_reward_type(trace_sel.reward_type.len() as u32);
            for (i, reward_type) in trace_sel.reward_type.iter().enumerate() {
                reward_type_list.set(i as u32, reward_type);
            }
        }

        // Set kinds
        {
            let mut kind_list = builder.reborrow().init_kind(trace_sel.kind.len() as u32);
            for (i, kind) in trace_sel.kind.iter().enumerate() {
                kind_list.set(i as u32, kind);
            }
        }

        // Set sighash
        {
            let mut sighash_list = builder.reborrow().init_sighash(trace_sel.sighash.len() as u32);
            for (i, sighash) in trace_sel.sighash.iter().enumerate() {
                sighash_list.set(i as u32, sighash.as_slice());
            }
        }

        Ok(())
    }

    fn populate_block_selection(
        block_sel: &BlockSelection, 
        mut builder: hypersync_net_types_capnp::block_selection::Builder
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
