use crate::block::{BlockField, BlockSelection};
use crate::hypersync_net_types_capnp;
use crate::log::{LogField, LogSelection};
use crate::trace::{TraceField, TraceSelection};
use crate::transaction::{TransactionField, TransactionSelection};
use capnp::message::Builder;
use capnp::serialize;
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
        serialize::write_message(&mut buf, &message)?;
        Ok(buf)
    }

    // Helper functions to convert Rust enums to Cap'n Proto enums
    fn block_field_to_capnp(field: &BlockField) -> hypersync_net_types_capnp::BlockField {
        // Use the integer representation which should match the enum ordinal
        match field {
            BlockField::Number => hypersync_net_types_capnp::BlockField::Number,
            BlockField::Hash => hypersync_net_types_capnp::BlockField::Hash,
            BlockField::ParentHash => hypersync_net_types_capnp::BlockField::ParentHash,
            BlockField::Sha3Uncles => hypersync_net_types_capnp::BlockField::Sha3Uncles,
            BlockField::LogsBloom => hypersync_net_types_capnp::BlockField::LogsBloom,
            BlockField::TransactionsRoot => hypersync_net_types_capnp::BlockField::TransactionsRoot,
            BlockField::StateRoot => hypersync_net_types_capnp::BlockField::StateRoot,
            BlockField::ReceiptsRoot => hypersync_net_types_capnp::BlockField::ReceiptsRoot,
            BlockField::Miner => hypersync_net_types_capnp::BlockField::Miner,
            BlockField::ExtraData => hypersync_net_types_capnp::BlockField::ExtraData,
            BlockField::Size => hypersync_net_types_capnp::BlockField::Size,
            BlockField::GasLimit => hypersync_net_types_capnp::BlockField::GasLimit,
            BlockField::GasUsed => hypersync_net_types_capnp::BlockField::GasUsed,
            BlockField::Timestamp => hypersync_net_types_capnp::BlockField::Timestamp,
            BlockField::MixHash => hypersync_net_types_capnp::BlockField::MixHash,
            BlockField::Nonce => hypersync_net_types_capnp::BlockField::Nonce,
            BlockField::Difficulty => hypersync_net_types_capnp::BlockField::Difficulty,
            BlockField::TotalDifficulty => hypersync_net_types_capnp::BlockField::TotalDifficulty,
            BlockField::Uncles => hypersync_net_types_capnp::BlockField::Uncles,
            BlockField::BaseFeePerGas => hypersync_net_types_capnp::BlockField::BaseFeePerGas,
            BlockField::BlobGasUsed => hypersync_net_types_capnp::BlockField::BlobGasUsed,
            BlockField::ExcessBlobGas => hypersync_net_types_capnp::BlockField::ExcessBlobGas,
            BlockField::ParentBeaconBlockRoot => {
                hypersync_net_types_capnp::BlockField::ParentBeaconBlockRoot
            }
            BlockField::WithdrawalsRoot => hypersync_net_types_capnp::BlockField::WithdrawalsRoot,
            BlockField::Withdrawals => hypersync_net_types_capnp::BlockField::Withdrawals,
            BlockField::L1BlockNumber => hypersync_net_types_capnp::BlockField::L1BlockNumber,
            BlockField::SendCount => hypersync_net_types_capnp::BlockField::SendCount,
            BlockField::SendRoot => hypersync_net_types_capnp::BlockField::SendRoot,
        }
    }

    fn transaction_field_to_capnp(
        field: &TransactionField,
    ) -> hypersync_net_types_capnp::TransactionField {
        match field {
            TransactionField::BlockHash => hypersync_net_types_capnp::TransactionField::BlockHash,
            TransactionField::BlockNumber => {
                hypersync_net_types_capnp::TransactionField::BlockNumber
            }
            TransactionField::Gas => hypersync_net_types_capnp::TransactionField::Gas,
            TransactionField::Hash => hypersync_net_types_capnp::TransactionField::Hash,
            TransactionField::Input => hypersync_net_types_capnp::TransactionField::Input,
            TransactionField::Nonce => hypersync_net_types_capnp::TransactionField::Nonce,
            TransactionField::TransactionIndex => {
                hypersync_net_types_capnp::TransactionField::TransactionIndex
            }
            TransactionField::Value => hypersync_net_types_capnp::TransactionField::Value,
            TransactionField::CumulativeGasUsed => {
                hypersync_net_types_capnp::TransactionField::CumulativeGasUsed
            }
            TransactionField::EffectiveGasPrice => {
                hypersync_net_types_capnp::TransactionField::EffectiveGasPrice
            }
            TransactionField::GasUsed => hypersync_net_types_capnp::TransactionField::GasUsed,
            TransactionField::LogsBloom => hypersync_net_types_capnp::TransactionField::LogsBloom,
            TransactionField::From => hypersync_net_types_capnp::TransactionField::From,
            TransactionField::GasPrice => hypersync_net_types_capnp::TransactionField::GasPrice,
            TransactionField::To => hypersync_net_types_capnp::TransactionField::To,
            TransactionField::V => hypersync_net_types_capnp::TransactionField::V,
            TransactionField::R => hypersync_net_types_capnp::TransactionField::R,
            TransactionField::S => hypersync_net_types_capnp::TransactionField::S,
            TransactionField::MaxPriorityFeePerGas => {
                hypersync_net_types_capnp::TransactionField::MaxPriorityFeePerGas
            }
            TransactionField::MaxFeePerGas => {
                hypersync_net_types_capnp::TransactionField::MaxFeePerGas
            }
            TransactionField::ChainId => hypersync_net_types_capnp::TransactionField::ChainId,
            TransactionField::ContractAddress => {
                hypersync_net_types_capnp::TransactionField::ContractAddress
            }
            TransactionField::Type => hypersync_net_types_capnp::TransactionField::Type,
            TransactionField::Root => hypersync_net_types_capnp::TransactionField::Root,
            TransactionField::Status => hypersync_net_types_capnp::TransactionField::Status,
            TransactionField::YParity => hypersync_net_types_capnp::TransactionField::YParity,
            TransactionField::AccessList => hypersync_net_types_capnp::TransactionField::AccessList,
            TransactionField::AuthorizationList => {
                hypersync_net_types_capnp::TransactionField::AuthorizationList
            }
            TransactionField::L1Fee => hypersync_net_types_capnp::TransactionField::L1Fee,
            TransactionField::L1GasPrice => hypersync_net_types_capnp::TransactionField::L1GasPrice,
            TransactionField::L1GasUsed => hypersync_net_types_capnp::TransactionField::L1GasUsed,
            TransactionField::L1FeeScalar => {
                hypersync_net_types_capnp::TransactionField::L1FeeScalar
            }
            TransactionField::GasUsedForL1 => {
                hypersync_net_types_capnp::TransactionField::GasUsedForL1
            }
            TransactionField::MaxFeePerBlobGas => {
                hypersync_net_types_capnp::TransactionField::MaxFeePerBlobGas
            }
            TransactionField::BlobVersionedHashes => {
                hypersync_net_types_capnp::TransactionField::BlobVersionedHashes
            }
            TransactionField::BlobGasPrice => {
                hypersync_net_types_capnp::TransactionField::BlobGasPrice
            }
            TransactionField::BlobGasUsed => {
                hypersync_net_types_capnp::TransactionField::BlobGasUsed
            }
            TransactionField::DepositNonce => {
                hypersync_net_types_capnp::TransactionField::DepositNonce
            }
            TransactionField::DepositReceiptVersion => {
                hypersync_net_types_capnp::TransactionField::DepositReceiptVersion
            }
            TransactionField::L1BaseFeeScalar => {
                hypersync_net_types_capnp::TransactionField::L1BaseFeeScalar
            }
            TransactionField::L1BlobBaseFee => {
                hypersync_net_types_capnp::TransactionField::L1BlobBaseFee
            }
            TransactionField::L1BlobBaseFeeScalar => {
                hypersync_net_types_capnp::TransactionField::L1BlobBaseFeeScalar
            }
            TransactionField::L1BlockNumber => {
                hypersync_net_types_capnp::TransactionField::L1BlockNumber
            }
            TransactionField::Mint => hypersync_net_types_capnp::TransactionField::Mint,
            TransactionField::Sighash => hypersync_net_types_capnp::TransactionField::Sighash,
            TransactionField::SourceHash => hypersync_net_types_capnp::TransactionField::SourceHash,
        }
    }

    fn log_field_to_capnp(field: &LogField) -> hypersync_net_types_capnp::LogField {
        match field {
            LogField::TransactionHash => hypersync_net_types_capnp::LogField::TransactionHash,
            LogField::BlockHash => hypersync_net_types_capnp::LogField::BlockHash,
            LogField::BlockNumber => hypersync_net_types_capnp::LogField::BlockNumber,
            LogField::TransactionIndex => hypersync_net_types_capnp::LogField::TransactionIndex,
            LogField::LogIndex => hypersync_net_types_capnp::LogField::LogIndex,
            LogField::Address => hypersync_net_types_capnp::LogField::Address,
            LogField::Data => hypersync_net_types_capnp::LogField::Data,
            LogField::Removed => hypersync_net_types_capnp::LogField::Removed,
            LogField::Topic0 => hypersync_net_types_capnp::LogField::Topic0,
            LogField::Topic1 => hypersync_net_types_capnp::LogField::Topic1,
            LogField::Topic2 => hypersync_net_types_capnp::LogField::Topic2,
            LogField::Topic3 => hypersync_net_types_capnp::LogField::Topic3,
        }
    }

    fn trace_field_to_capnp(field: &TraceField) -> hypersync_net_types_capnp::TraceField {
        match field {
            TraceField::TransactionHash => hypersync_net_types_capnp::TraceField::TransactionHash,
            TraceField::BlockHash => hypersync_net_types_capnp::TraceField::BlockHash,
            TraceField::BlockNumber => hypersync_net_types_capnp::TraceField::BlockNumber,
            TraceField::TransactionPosition => {
                hypersync_net_types_capnp::TraceField::TransactionPosition
            }
            TraceField::Type => hypersync_net_types_capnp::TraceField::Type,
            TraceField::Error => hypersync_net_types_capnp::TraceField::Error,
            TraceField::From => hypersync_net_types_capnp::TraceField::From,
            TraceField::To => hypersync_net_types_capnp::TraceField::To,
            TraceField::Author => hypersync_net_types_capnp::TraceField::Author,
            TraceField::Gas => hypersync_net_types_capnp::TraceField::Gas,
            TraceField::GasUsed => hypersync_net_types_capnp::TraceField::GasUsed,
            TraceField::ActionAddress => hypersync_net_types_capnp::TraceField::ActionAddress,
            TraceField::Address => hypersync_net_types_capnp::TraceField::Address,
            TraceField::Balance => hypersync_net_types_capnp::TraceField::Balance,
            TraceField::CallType => hypersync_net_types_capnp::TraceField::CallType,
            TraceField::Code => hypersync_net_types_capnp::TraceField::Code,
            TraceField::Init => hypersync_net_types_capnp::TraceField::Init,
            TraceField::Input => hypersync_net_types_capnp::TraceField::Input,
            TraceField::Output => hypersync_net_types_capnp::TraceField::Output,
            TraceField::RefundAddress => hypersync_net_types_capnp::TraceField::RefundAddress,
            TraceField::RewardType => hypersync_net_types_capnp::TraceField::RewardType,
            TraceField::Sighash => hypersync_net_types_capnp::TraceField::Sighash,
            TraceField::Subtraces => hypersync_net_types_capnp::TraceField::Subtraces,
            TraceField::TraceAddress => hypersync_net_types_capnp::TraceField::TraceAddress,
            TraceField::Value => hypersync_net_types_capnp::TraceField::Value,
        }
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
                block_list.set(i as u32, Self::block_field_to_capnp(field));
            }

            // Set transaction fields
            let mut tx_list = field_selection
                .reborrow()
                .init_transaction(self.field_selection.transaction.len() as u32);
            for (i, field) in self.field_selection.transaction.iter().enumerate() {
                tx_list.set(i as u32, Self::transaction_field_to_capnp(field));
            }

            // Set log fields
            let mut log_list = field_selection
                .reborrow()
                .init_log(self.field_selection.log.len() as u32);
            for (i, field) in self.field_selection.log.iter().enumerate() {
                log_list.set(i as u32, Self::log_field_to_capnp(field));
            }

            // Set trace fields
            let mut trace_list = field_selection
                .reborrow()
                .init_trace(self.field_selection.trace.len() as u32);
            for (i, field) in self.field_selection.trace.iter().enumerate() {
                trace_list.set(i as u32, Self::trace_field_to_capnp(field));
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
}
