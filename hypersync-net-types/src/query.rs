use crate::block::{BlockField, BlockSelection};
use crate::hypersync_net_types_capnp::{query_body, request};
use crate::log::{LogField, LogSelection};
use crate::trace::{TraceField, TraceSelection};
use crate::transaction::{TransactionField, TransactionSelection};
use crate::{hypersync_net_types_capnp, CapnpBuilder, CapnpReader};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

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
    /// associated transaction of log0 and then we get associated logs of that transaction as well. Applies similarly
    /// to blocks, traces.
    /// JoinNothing: join nothing.
    #[serde(default, skip_serializing_if = "is_default")]
    pub join_mode: JoinMode,
}

/// Used to skip serializing a defaulted serde field if
/// the value matches the default value.
fn is_default<T: Default + PartialEq>(t: &T) -> bool {
    t == &T::default()
}

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum JoinMode {
    /// Join in this order logs -> transactions -> traces -> blocks
    #[default]
    Default,
    /// Join everything to everything. For example if logSelection matches log0, we get the
    /// associated transaction of log0 and then we get associated logs of that transaction as well. Applies similarly
    /// to blocks, traces.
    JoinAll,
    /// JoinNothing: join nothing.
    JoinNothing,
}

impl Query {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn from_block(mut self, from_block: u64) -> Self {
        self.from_block = from_block;
        self
    }

    pub fn to_block(mut self, to_block: u64) -> Self {
        self.to_block = Some(to_block);
        self
    }

    pub fn match_logs_any<I>(mut self, logs: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<LogSelection>,
    {
        let log_selection: Vec<LogSelection> = logs.into_iter().map(Into::into).collect();
        self.logs = log_selection;
        self
    }

    pub fn add_log_selection(mut self, log_selection: LogSelection) -> Self {
        self.logs.push(log_selection);
        self
    }

    pub fn select_fields(mut self, field_selection: FieldSelection) -> Self {
        self.field_selection = field_selection;
        self
    }

    pub fn from_capnp_query_body_reader(
        query_body_reader: &query_body::Reader,
        from_block: u64,
        to_block: Option<u64>,
    ) -> Result<Self, capnp::Error> {
        let include_all_blocks = query_body_reader.get_include_all_blocks();

        // Parse field selection
        let field_selection = if query_body_reader.has_field_selection() {
            let fs = query_body_reader.get_field_selection()?;
            FieldSelection::from_reader(fs)?
        } else {
            FieldSelection::default()
        };

        // Parse max values using OptUInt64
        let max_num_blocks = if query_body_reader.has_max_num_blocks() {
            let max_blocks_reader = query_body_reader.get_max_num_blocks()?;
            let value = max_blocks_reader.get_value();
            Some(value as usize)
        } else {
            None
        };
        let max_num_transactions = if query_body_reader.has_max_num_transactions() {
            let max_tx_reader = query_body_reader.get_max_num_transactions()?;
            let value = max_tx_reader.get_value();
            Some(value as usize)
        } else {
            None
        };
        let max_num_logs = if query_body_reader.has_max_num_logs() {
            let max_logs_reader = query_body_reader.get_max_num_logs()?;
            let value = max_logs_reader.get_value();
            Some(value as usize)
        } else {
            None
        };
        let max_num_traces = if query_body_reader.has_max_num_traces() {
            let max_traces_reader = query_body_reader.get_max_num_traces()?;
            let value = max_traces_reader.get_value();
            Some(value as usize)
        } else {
            None
        };

        // Parse join mode
        let join_mode = match query_body_reader.get_join_mode()? {
            hypersync_net_types_capnp::JoinMode::Default => JoinMode::Default,
            hypersync_net_types_capnp::JoinMode::JoinAll => JoinMode::JoinAll,
            hypersync_net_types_capnp::JoinMode::JoinNothing => JoinMode::JoinNothing,
        };

        // Parse selections
        let mut logs = Vec::new();
        if query_body_reader.has_logs() {
            let logs_list = query_body_reader.get_logs()?;
            for log_reader in logs_list {
                logs.push(LogSelection::from_reader(log_reader)?);
            }
        }

        let mut transactions = Vec::new();
        if query_body_reader.has_transactions() {
            let tx_list = query_body_reader.get_transactions()?;
            for tx_reader in tx_list {
                transactions.push(TransactionSelection::from_reader(tx_reader)?);
            }
        }

        let mut traces = Vec::new();
        if query_body_reader.has_traces() {
            let traces_list = query_body_reader.get_traces()?;
            for trace_reader in traces_list {
                traces.push(TraceSelection::from_reader(trace_reader)?);
            }
        }

        let mut blocks = Vec::new();
        if query_body_reader.has_blocks() {
            let blocks_list = query_body_reader.get_blocks()?;
            for block_reader in blocks_list {
                blocks.push(BlockSelection::from_reader(block_reader)?);
            }
        }

        Ok(Self {
            from_block,
            to_block,
            logs,
            transactions,
            traces,
            blocks,
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

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct FieldSelection {
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub block: BTreeSet<BlockField>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub transaction: BTreeSet<TransactionField>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub log: BTreeSet<LogField>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub trace: BTreeSet<TraceField>,
}

impl FieldSelection {
    /// Create a new empty field selection.
    ///
    /// This creates a field selection with no fields selected. You can then use the builder
    /// methods to select specific fields for each data type (block, transaction, log, trace).
    ///
    /// # Examples
    ///
    /// ```
    /// use hypersync_net_types::FieldSelection;
    ///
    /// // Create empty field selection
    /// let field_selection = FieldSelection::new();
    ///
    /// // All field sets are empty by default
    /// assert!(field_selection.block.is_empty());
    /// assert!(field_selection.transaction.is_empty());
    /// assert!(field_selection.log.is_empty());
    /// assert!(field_selection.trace.is_empty());
    /// ```
    pub fn new() -> Self {
        Self::default()
    }
    /// Select specific block fields to include in query results.
    ///
    /// This method allows you to specify which block fields should be returned in the query response.
    /// Only the selected fields will be included, which can improve performance and reduce payload size.
    ///
    /// # Arguments
    /// * `fields` - An iterable of `BlockField` values to select
    ///
    /// # Examples
    ///
    /// ```
    /// use hypersync_net_types::{FieldSelection, block::BlockField, transaction::TransactionField};
    ///
    /// // Select specific block fields
    /// let field_selection = FieldSelection::new()
    ///     .block([BlockField::Number, BlockField::Hash, BlockField::Timestamp]);
    ///
    /// // Can also use a vector
    /// let fields = vec![BlockField::Number, BlockField::ParentHash];
    /// let field_selection = FieldSelection::new()
    ///     .block(fields);
    ///
    /// // Chain with other field selections
    /// let field_selection = FieldSelection::new()
    ///     .block([BlockField::Number])
    ///     .transaction([TransactionField::Hash]);
    /// ```
    pub fn block<T: IntoIterator<Item = BlockField>>(mut self, fields: T) -> Self {
        self.block.extend(fields);
        self
    }
    /// Select specific transaction fields to include in query results.
    ///
    /// This method allows you to specify which transaction fields should be returned in the query response.
    /// Only the selected fields will be included, which can improve performance and reduce payload size.
    ///
    /// # Arguments
    /// * `fields` - An iterable of `TransactionField` values to select
    ///
    /// # Examples
    ///
    /// ```
    /// use hypersync_net_types::{FieldSelection, transaction::TransactionField, block::BlockField};
    ///
    /// // Select specific transaction fields
    /// let field_selection = FieldSelection::new()
    ///     .transaction([TransactionField::Hash, TransactionField::From, TransactionField::To]);
    ///
    /// // Select fields related to gas and value
    /// let field_selection = FieldSelection::new()
    ///     .transaction([
    ///         TransactionField::GasPrice,
    ///         TransactionField::GasUsed,
    ///         TransactionField::Value,
    ///     ]);
    ///
    /// // Chain with other field selections
    /// let field_selection = FieldSelection::new()
    ///     .block([BlockField::Number])
    ///     .transaction([TransactionField::Hash]);
    /// ```
    pub fn transaction<T: IntoIterator<Item = TransactionField>>(mut self, fields: T) -> Self {
        self.transaction.extend(fields);
        self
    }
    /// Select specific log fields to include in query results.
    ///
    /// This method allows you to specify which log fields should be returned in the query response.
    /// Only the selected fields will be included, which can improve performance and reduce payload size.
    ///
    /// # Arguments
    /// * `fields` - An iterable of `LogField` values to select
    ///
    /// # Examples
    ///
    /// ```
    /// use hypersync_net_types::{FieldSelection, log::LogField, transaction::TransactionField};
    ///
    /// // Select essential log fields
    /// let field_selection = FieldSelection::new()
    ///     .log([LogField::Address, LogField::Data, LogField::Topic0]);
    ///
    /// // Select all topic fields for event analysis
    /// let field_selection = FieldSelection::new()
    ///     .log([
    ///         LogField::Topic0,
    ///         LogField::Topic1,
    ///         LogField::Topic2,
    ///         LogField::Topic3,
    ///     ]);
    ///
    /// // Chain with transaction fields for full context
    /// let field_selection = FieldSelection::new()
    ///     .transaction([TransactionField::Hash])
    ///     .log([LogField::Address, LogField::Data]);
    /// ```
    pub fn log<T: IntoIterator<Item = LogField>>(mut self, fields: T) -> Self {
        self.log.extend(fields);
        self
    }
    /// Select specific trace fields to include in query results.
    ///
    /// This method allows you to specify which trace fields should be returned in the query response.
    /// Only the selected fields will be included, which can improve performance and reduce payload size.
    ///
    /// # Arguments
    /// * `fields` - An iterable of `TraceField` values to select
    ///
    /// # Examples
    ///
    /// ```
    /// use hypersync_net_types::{
    ///     FieldSelection,
    ///     trace::TraceField,
    ///     transaction::TransactionField,
    ///     log::LogField
    /// };
    ///
    /// // Select basic trace information
    /// let field_selection = FieldSelection::new()
    ///     .trace([TraceField::From, TraceField::To, TraceField::Value]);
    ///
    /// // Select trace execution details
    /// let field_selection = FieldSelection::new()
    ///     .trace([
    ///         TraceField::CallType,
    ///         TraceField::Input,
    ///         TraceField::Output,
    ///         TraceField::Gas,
    ///         TraceField::GasUsed,
    ///     ]);
    ///
    /// // Combine with other data types for comprehensive analysis
    /// let field_selection = FieldSelection::new()
    ///     .transaction([TransactionField::Hash])
    ///     .trace([TraceField::From, TraceField::To])
    ///     .log([LogField::Address]);
    /// ```
    pub fn trace<T: IntoIterator<Item = TraceField>>(mut self, fields: T) -> Self {
        self.trace.extend(fields);
        self
    }
}

impl query_body::Builder<'_> {
    pub fn build_from_query(&mut self, query: &Query) -> Result<(), capnp::Error> {
        self.reborrow()
            .set_include_all_blocks(query.include_all_blocks);

        // Set max nums using OptUInt64
        if let Some(max_num_blocks) = query.max_num_blocks {
            let mut max_blocks_builder = self.reborrow().init_max_num_blocks();
            max_blocks_builder.set_value(max_num_blocks as u64);
        }
        if let Some(max_num_transactions) = query.max_num_transactions {
            let mut max_tx_builder = self.reborrow().init_max_num_transactions();
            max_tx_builder.set_value(max_num_transactions as u64);
        }
        if let Some(max_num_logs) = query.max_num_logs {
            let mut max_logs_builder = self.reborrow().init_max_num_logs();
            max_logs_builder.set_value(max_num_logs as u64);
        }
        if let Some(max_num_traces) = query.max_num_traces {
            let mut max_traces_builder = self.reborrow().init_max_num_traces();
            max_traces_builder.set_value(max_num_traces as u64);
        }

        // Set join mode
        let join_mode = match query.join_mode {
            JoinMode::Default => hypersync_net_types_capnp::JoinMode::Default,
            JoinMode::JoinAll => hypersync_net_types_capnp::JoinMode::JoinAll,
            JoinMode::JoinNothing => hypersync_net_types_capnp::JoinMode::JoinNothing,
        };
        self.reborrow().set_join_mode(join_mode);

        // Set field selection
        {
            let mut field_selection = self.reborrow().init_field_selection();
            query
                .field_selection
                .populate_builder(&mut field_selection)?;
        }

        // Set logs
        {
            let mut logs_list = self.reborrow().init_logs(query.logs.len() as u32);
            for (i, log_selection) in query.logs.iter().enumerate() {
                let mut log_sel = logs_list.reborrow().get(i as u32);
                log_selection.populate_builder(&mut log_sel)?;
            }
        }

        // Set transactions
        {
            let mut tx_list = self
                .reborrow()
                .init_transactions(query.transactions.len() as u32);
            for (i, tx_selection) in query.transactions.iter().enumerate() {
                let mut tx_sel = tx_list.reborrow().get(i as u32);
                tx_selection.populate_builder(&mut tx_sel)?;
            }
        }

        // Set traces
        {
            let mut trace_list = self.reborrow().init_traces(query.traces.len() as u32);
            for (i, trace_selection) in query.traces.iter().enumerate() {
                let mut trace_sel = trace_list.reborrow().get(i as u32);
                trace_selection.populate_builder(&mut trace_sel)?;
            }
        }

        // Set blocks
        {
            let mut block_list = self.reborrow().init_blocks(query.blocks.len() as u32);
            for (i, block_selection) in query.blocks.iter().enumerate() {
                let mut block_sel = block_list.reborrow().get(i as u32);
                block_selection.populate_builder(&mut block_sel)?;
            }
        }
        Ok(())
    }
}

impl CapnpReader<hypersync_net_types_capnp::request::Owned> for Query {
    fn from_reader(
        query: hypersync_net_types_capnp::request::Reader,
    ) -> Result<Self, capnp::Error> {
        let block_range = query.get_block_range()?;
        let from_block = block_range.get_from_block();
        let to_block = if block_range.has_to_block() {
            Some(block_range.get_to_block()?.get_value())
        } else {
            None
        };
        let body_reader = match query.get_body().which()? {
            request::body::Which::Query(query_body_reader) => query_body_reader?,
            request::body::Which::QueryId(_) => {
                return Err(capnp::Error::failed(
                    "QueryId cannot be read from capnp request with QueryBody".to_string(),
                ));
            }
        };

        Query::from_capnp_query_body_reader(&body_reader, from_block, to_block)
    }
}
impl CapnpBuilder<hypersync_net_types_capnp::field_selection::Owned> for FieldSelection {
    fn populate_builder(
        &self,
        field_selection: &mut hypersync_net_types_capnp::field_selection::Builder,
    ) -> Result<(), capnp::Error> {
        // Set block fields
        if !self.block.is_empty() {
            let mut block_list = field_selection
                .reborrow()
                .init_block(self.block.len() as u32);
            for (i, field) in self.block.iter().enumerate() {
                block_list.set(i as u32, field.to_capnp());
            }
        }

        if !self.transaction.is_empty() {
            // Set transaction fields
            let mut tx_list = field_selection
                .reborrow()
                .init_transaction(self.transaction.len() as u32);
            for (i, field) in self.transaction.iter().enumerate() {
                tx_list.set(i as u32, field.to_capnp());
            }
        }

        if !self.log.is_empty() {
            // Set log fields
            let mut log_list = field_selection.reborrow().init_log(self.log.len() as u32);
            for (i, field) in self.log.iter().enumerate() {
                log_list.set(i as u32, field.to_capnp());
            }
        }

        if !self.trace.is_empty() {
            // Set trace fields
            let mut trace_list = field_selection
                .reborrow()
                .init_trace(self.trace.len() as u32);
            for (i, field) in self.trace.iter().enumerate() {
                trace_list.set(i as u32, field.to_capnp());
            }
        }

        Ok(())
    }
}

impl CapnpReader<hypersync_net_types_capnp::field_selection::Owned> for FieldSelection {
    fn from_reader(
        fs: hypersync_net_types_capnp::field_selection::Reader,
    ) -> Result<Self, capnp::Error> {
        let mut block_fields = BTreeSet::new();
        if fs.has_block() {
            let block_list = fs.get_block()?;
            for block in block_list {
                block_fields.insert(BlockField::from_capnp(block?));
            }
        }

        let mut transaction_fields = BTreeSet::new();
        if fs.has_transaction() {
            let tx_list = fs.get_transaction()?;
            for tx in tx_list {
                transaction_fields.insert(TransactionField::from_capnp(tx?));
            }
        }

        let mut log_fields = BTreeSet::new();
        if fs.has_log() {
            let log_list = fs.get_log()?;
            for log in log_list {
                log_fields.insert(LogField::from_capnp(log?));
            }
        }

        let mut trace_fields = BTreeSet::new();
        if fs.has_trace() {
            let trace_list = fs.get_trace()?;
            for trace in trace_list {
                trace_fields.insert(TraceField::from_capnp(trace?));
            }
        };

        Ok(FieldSelection {
            block: block_fields,
            transaction: transaction_fields,
            log: log_fields,
            trace: trace_fields,
        })
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use capnp::message::{Builder, ReaderOptions};
    use pretty_assertions::assert_eq;

    pub fn test_query_serde(query: Query, label: &str) {
        fn test_encode_decode<T: PartialEq + std::fmt::Debug>(
            input: &T,
            label: String,
            encode: impl FnOnce(&T) -> Vec<u8>,
            decode: impl FnOnce(&[u8]) -> T,
        ) {
            let val = encode(input);
            let decoded = decode(&val);
            assert_eq!(input, &decoded, "{label} does not match");
        }

        fn to_capnp_bytes(query: &Query) -> Vec<u8> {
            let mut message = Builder::new_default();
            let mut query_builder =
                message.init_root::<hypersync_net_types_capnp::request::Builder>();

            query_builder
                .build_full_query_from_query(query, false)
                .unwrap();

            let mut buf = Vec::new();
            capnp::serialize::write_message(&mut buf, &message).unwrap();
            buf
        }

        fn from_capnp_bytes(bytes: &[u8]) -> Query {
            let message_reader = capnp::serialize::read_message(
                &mut std::io::Cursor::new(bytes),
                ReaderOptions::new(),
            )
            .unwrap();
            let query = message_reader
                .get_root::<hypersync_net_types_capnp::request::Reader>()
                .unwrap();

            Query::from_reader(query).unwrap()
        }

        test_encode_decode(
            &query,
            label.to_string() + "-capnp",
            to_capnp_bytes,
            from_capnp_bytes,
        );
        test_encode_decode(
            &query,
            label.to_string() + "-json",
            |q| serde_json::to_vec(q).unwrap(),
            |bytes| serde_json::from_slice(bytes).unwrap(),
        );
    }

    #[test]
    pub fn test_query_serde_default() {
        let query = Query::default();
        test_query_serde(query, "default");
    }

    #[test]
    pub fn test_query_serde_with_non_null_defaults() {
        let query = Query {
            from_block: u64::default(),
            to_block: Some(u64::default()),
            logs: Vec::default(),
            transactions: Vec::default(),
            traces: Vec::default(),
            blocks: Vec::default(),
            include_all_blocks: bool::default(),
            field_selection: FieldSelection::default(),
            max_num_blocks: Some(usize::default()),
            max_num_transactions: Some(usize::default()),
            max_num_logs: Some(usize::default()),
            max_num_traces: Some(usize::default()),
            join_mode: JoinMode::default(),
        };
        test_query_serde(query, "base query with_non_null_defaults");
    }

    #[test]
    pub fn test_query_serde_with_non_null_values() {
        let query = Query {
            from_block: 50,
            to_block: Some(500),
            logs: Vec::default(),
            transactions: Vec::default(),
            traces: Vec::default(),
            blocks: Vec::default(),
            include_all_blocks: true,
            field_selection: FieldSelection::default(),
            max_num_blocks: Some(50),
            max_num_transactions: Some(100),
            max_num_logs: Some(150),
            max_num_traces: Some(200),
            join_mode: JoinMode::JoinAll,
        };
        test_query_serde(query, "base query with_non_null_values");
    }
}
