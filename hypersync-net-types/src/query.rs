use crate::block::{BlockField, BlockSelection};
use crate::hypersync_net_types_capnp;
use crate::log::{LogField, LogSelection};
use crate::trace::{TraceField, TraceSelection};
use crate::transaction::{TransactionField, TransactionSelection};
use capnp::message::Builder;
use capnp::{serialize_packed, message::ReaderOptions};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

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
    pub block: BTreeSet<BlockField>,
    #[serde(default)]
    pub transaction: BTreeSet<TransactionField>,
    #[serde(default)]
    pub log: BTreeSet<LogField>,
    #[serde(default)]
    pub trace: BTreeSet<TraceField>,
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

impl Query {
    /// Serialize Query to Cap'n Proto format and return as bytes
    pub fn to_capnp_bytes(&self) -> Result<Vec<u8>, capnp::Error> {
        let mut message = Builder::new_default();
        let query = message.init_root::<hypersync_net_types_capnp::query::Builder>();

        self.populate_capnp_query(query)?;

        let mut buf = Vec::new();
        serialize_packed::write_message(&mut buf, &message)?;
        Ok(buf)
    }

    /// Deserialize Query from Cap'n Proto packed bytes
    pub fn from_capnp_bytes(bytes: &[u8]) -> Result<Self, capnp::Error> {
        let message_reader = serialize_packed::read_message(
            &mut std::io::Cursor::new(bytes),
            ReaderOptions::new()
        )?;
        let query = message_reader.get_root::<hypersync_net_types_capnp::query::Reader>()?;
        
        Self::from_capnp_query(query)
    }

    fn populate_capnp_query(
        &self,
        mut query: hypersync_net_types_capnp::query::Builder,
    ) -> Result<(), capnp::Error> {
        query.reborrow().set_from_block(self.from_block);

        if let Some(to_block) = self.to_block {
            query.reborrow().set_to_block(to_block);
        }

        query
            .reborrow()
            .set_include_all_blocks(self.include_all_blocks);

        // Set max nums
        if let Some(max_num_blocks) = self.max_num_blocks {
            query.reborrow().set_max_num_blocks(max_num_blocks as u64);
        }
        if let Some(max_num_transactions) = self.max_num_transactions {
            query
                .reborrow()
                .set_max_num_transactions(max_num_transactions as u64);
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

            // Set block fields
            let mut block_list = field_selection
                .reborrow()
                .init_block(self.field_selection.block.len() as u32);
            for (i, field) in self.field_selection.block.iter().enumerate() {
                block_list.set(i as u32, field.to_capnp());
            }

            // Set transaction fields
            let mut tx_list = field_selection
                .reborrow()
                .init_transaction(self.field_selection.transaction.len() as u32);
            for (i, field) in self.field_selection.transaction.iter().enumerate() {
                tx_list.set(i as u32, field.to_capnp());
            }

            // Set log fields
            let mut log_list = field_selection
                .reborrow()
                .init_log(self.field_selection.log.len() as u32);
            for (i, field) in self.field_selection.log.iter().enumerate() {
                log_list.set(i as u32, field.to_capnp());
            }

            // Set trace fields
            let mut trace_list = field_selection
                .reborrow()
                .init_trace(self.field_selection.trace.len() as u32);
            for (i, field) in self.field_selection.trace.iter().enumerate() {
                trace_list.set(i as u32, field.to_capnp());
            }
        }

        // Set logs
        {
            let mut logs_list = query.reborrow().init_logs(self.logs.len() as u32);
            for (i, log_selection) in self.logs.iter().enumerate() {
                let log_sel = logs_list.reborrow().get(i as u32);
                LogSelection::populate_capnp_builder(log_selection, log_sel)?;
            }
        }

        // Set transactions
        {
            let mut tx_list = query
                .reborrow()
                .init_transactions(self.transactions.len() as u32);
            for (i, tx_selection) in self.transactions.iter().enumerate() {
                let tx_sel = tx_list.reborrow().get(i as u32);
                TransactionSelection::populate_capnp_builder(tx_selection, tx_sel)?;
            }
        }

        // Set traces
        {
            let mut trace_list = query.reborrow().init_traces(self.traces.len() as u32);
            for (i, trace_selection) in self.traces.iter().enumerate() {
                let trace_sel = trace_list.reborrow().get(i as u32);
                TraceSelection::populate_capnp_builder(trace_selection, trace_sel)?;
            }
        }

        // Set blocks
        {
            let mut block_list = query.reborrow().init_blocks(self.blocks.len() as u32);
            for (i, block_selection) in self.blocks.iter().enumerate() {
                let block_sel = block_list.reborrow().get(i as u32);
                BlockSelection::populate_capnp_builder(block_selection, block_sel)?;
            }
        }

        Ok(())
    }

    fn from_capnp_query(query: hypersync_net_types_capnp::query::Reader) -> Result<Self, capnp::Error> {
        let from_block = query.get_from_block();
        let to_block = if query.has_to_block() { Some(query.get_to_block()) } else { None };
        let include_all_blocks = query.get_include_all_blocks();
        
        // Parse field selection
        let field_selection = if query.has_field_selection() {
            let fs = query.get_field_selection()?;
            
            let block_fields = if fs.has_block() {
                let block_list = fs.get_block()?;
                (0..block_list.len()).map(|i| {
                    BlockField::from_capnp(block_list.get(i))
                }).collect::<BTreeSet<_>>()
            } else {
                BTreeSet::new()
            };
            
            let transaction_fields = if fs.has_transaction() {
                let tx_list = fs.get_transaction()?;
                (0..tx_list.len()).map(|i| {
                    TransactionField::from_capnp(tx_list.get(i))
                }).collect::<BTreeSet<_>>()
            } else {
                BTreeSet::new()
            };
            
            let log_fields = if fs.has_log() {
                let log_list = fs.get_log()?;
                (0..log_list.len()).map(|i| {
                    LogField::from_capnp(log_list.get(i))
                }).collect::<BTreeSet<_>>()
            } else {
                BTreeSet::new()
            };
            
            let trace_fields = if fs.has_trace() {
                let trace_list = fs.get_trace()?;
                (0..trace_list.len()).map(|i| {
                    TraceField::from_capnp(trace_list.get(i))
                }).collect::<BTreeSet<_>>()
            } else {
                BTreeSet::new()
            };
            
            FieldSelection {
                block: block_fields,
                transaction: transaction_fields,
                log: log_fields,
                trace: trace_fields,
            }
        } else {
            FieldSelection::default()
        };

        // Parse max values
        let max_num_blocks = if query.has_max_num_blocks() { Some(query.get_max_num_blocks() as usize) } else { None };
        let max_num_transactions = if query.has_max_num_transactions() { Some(query.get_max_num_transactions() as usize) } else { None };
        let max_num_logs = if query.has_max_num_logs() { Some(query.get_max_num_logs() as usize) } else { None };
        let max_num_traces = if query.has_max_num_traces() { Some(query.get_max_num_traces() as usize) } else { None };

        // Parse join mode
        let join_mode = match query.get_join_mode()? {
            hypersync_net_types_capnp::JoinMode::Default => JoinMode::Default,
            hypersync_net_types_capnp::JoinMode::JoinAll => JoinMode::JoinAll,
            hypersync_net_types_capnp::JoinMode::JoinNothing => JoinMode::JoinNothing,
        };

        // For now, we'll leave logs, transactions, traces, and blocks as empty vectors
        // since implementing the full deserialization of those would require similar
        // deserialization methods for LogSelection, TransactionSelection, etc.
        Ok(Query {
            from_block,
            to_block,
            logs: Vec::new(),
            transactions: Vec::new(), 
            traces: Vec::new(),
            blocks: Vec::new(),
            include_all_blocks,
            field_selection,
            max_num_blocks,
            max_num_transactions,
            max_num_logs,
            max_num_traces,
            join_mode,
        })
    }

}
