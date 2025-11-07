use crate::block::{BlockField, BlockSelection};
use crate::hypersync_net_types_capnp::{query_body, request};
use crate::log::{LogField, LogSelection};
use crate::trace::{TraceField, TraceSelection};
use crate::transaction::{TransactionField, TransactionSelection};
use crate::{hypersync_net_types_capnp, CapnpBuilder, CapnpReader};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// A hypersync query that defines what blockchain data to retrieve.
///
/// The `Query` struct provides a fluent builder API for constructing complex blockchain queries
/// using hypersync. It allows you to specify block ranges, filter by logs/transactions/traces/blocks,
/// select which fields to return, and control query behavior like join modes.
///
/// # Core Concepts
///
/// - **Block Range**: Define the range of blocks to query with `from_block` and optional `to_block`
/// - **Filters**: Specify what data to match using `LogFilter`, `TransactionFilter`, `BlockFilter`, and `TraceFilter`
/// - **Field Selection**: Choose which fields to include in the response to optimize performance
/// - **Join Modes**: Control how different data types are related and joined
///
/// # Performance Tips
///
/// - Specify the minimum `FieldSelection` to only request the data you need
/// - Use specific filters rather than broad queries when possible
/// - Be mindful of setting `include_all_blocks: true` as it can significantly increase response size
/// - Traces are only available on select hypersync instances
///
/// # Basic Examples
///
/// ```
/// use hypersync_net_types::{
///     Query, FieldSelection, LogFilter, BlockFilter, TransactionFilter,
///     block::BlockField, log::LogField, transaction::TransactionField
/// };
///
/// // Simple log query for USDT transfers
/// let usdt_transfers = Query::new()
///     .from_block(18_000_000)
///     .to_block(18_001_000)
///     .select_fields(
///         FieldSelection::new()
///             .block([BlockField::Number, BlockField::Timestamp])
///             .log([LogField::Address, LogField::Data, LogField::Topic0, LogField::Topic1, LogField::Topic2])
///             .transaction([TransactionField::Hash, TransactionField::From, TransactionField::To])
///     )
///     .where_logs_any([
///         LogFilter::any()
///             .and_address_any(["0xdac17f958d2ee523a2206206994597c13d831ec7"])? // USDT contract
///             .and_topic0_any(["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"])? // Transfer event
///     ]);
/// # Ok::<(), anyhow::Error>(())
/// ```
///
/// # Advanced Examples
///
/// ```
/// use hypersync_net_types::{
///     Query, FieldSelection, JoinMode, LogFilter, TransactionFilter, Selection,
///     block::BlockField, log::LogField, transaction::TransactionField
/// };
///
/// // Complex query with multiple different filter combinations and exclusions
/// let complex_query = Query::new()
///     .from_block(18_000_000)
///     .to_block(18_010_000)
///     .join_mode(JoinMode::JoinAll)
///     .select_fields(
///         FieldSelection::new()
///             .block(BlockField::all())
///             .transaction(TransactionField::all())
///             .log(LogField::all())
///     )
///     .where_logs_any([
///         // Transfer events from USDT and USDC contracts (multiple addresses in one filter)
///         LogFilter::any()
///             .and_address_any([
///                 "0xdac17f958d2ee523a2206206994597c13d831ec7", // USDT
///                 "0xa0b86a33e6c11c8c0c5c0b5e6adee30d1a234567", // USDC
///             ])?
///             .and_topic0_any(["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"])?, // Transfer event
///         
///         // Approval events from any ERC20 contract (different topic combination)
///         LogFilter::any()
///             .and_topic0_any(["0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"])?, // Approval event
///             
///         // Swap events from Uniswap V2 pairs (another distinct filter combination)
///         LogFilter::any()
///             .and_topic0_any(["0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"])?, // Swap event
///     ])
///     .where_transactions_any([
///         TransactionFilter::any()
///             .and_sighash_any([
///                 "0xa9059cbb", // transfer(address,uint256)
///                 "0x095ea7b3", // approve(address,uint256)
///             ])?,
///         TransactionFilter::any()
///             .and_status(0), // Failed transactions
///     ]);
/// # Ok::<(), anyhow::Error>(())
/// ```
///
/// # Exclusion with `and_not`
///
/// ```
/// use hypersync_net_types::{Query, FieldSelection, LogFilter, Selection, log::LogField};
///
/// // Query for ERC20 transfers but exclude specific problematic contracts
/// let filtered_query = Query::new()
///     .from_block(18_000_000)
///     .to_block(18_001_000)
///     .select_fields(
///         FieldSelection::new()
///             .log([LogField::Address, LogField::Data, LogField::Topic0, LogField::Topic1, LogField::Topic2])
///     )
///     .where_logs_any([
///         // Include Transfer events from all contracts, but exclude specific problematic contracts
///         Selection::new(
///             LogFilter::any()
///                 .and_topic0_any(["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"])? // Transfer event
///         )
///         // But exclude specific problematic contracts
///         .and_not(
///             LogFilter::any()
///                 .and_address_any([
///                     "0x1234567890123456789012345678901234567890", // Problematic contract 1
///                     "0x0987654321098765432109876543210987654321", // Problematic contract 2
///                 ])?
///         ),
///     ]);
/// # Ok::<(), anyhow::Error>(())
/// ```
///
/// # Builder Pattern
///
/// The Query struct uses a fluent builder pattern where all methods return `Self`, allowing for easy chaining:
///
/// ```
/// use hypersync_net_types::{Query, FieldSelection, JoinMode, block::BlockField};
///
/// let query = Query::new()
///     .from_block(18_000_000)
///     .to_block(18_010_000)
///     .join_mode(JoinMode::Default)
///     .include_all_blocks()
///     .select_fields(FieldSelection::new().block(BlockField::all()));
/// ```
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

    /// Set log filters that the query will match against.
    ///
    /// This method accepts any iterable of items that can be converted to `LogSelection`.
    /// Common input types include `LogFilter` objects and `LogSelection` objects.
    /// The query will return logs that match any of the provided filters.
    ///
    /// # Arguments
    /// * `logs` - An iterable of log filters/selections to match
    ///
    /// # Examples
    ///
    /// ```
    /// use hypersync_net_types::{Query, LogFilter};
    ///
    /// // Match logs from multiple contracts (multiple addresses in one filter)
    /// let query = Query::new()
    ///     .from_block(18_000_000)
    ///     .where_logs_any([
    ///         LogFilter::any()
    ///             .and_address_any([
    ///                 "0xdac17f958d2ee523a2206206994597c13d831ec7", // USDT
    ///                 "0xa0b86a33e6c11c8c0c5c0b5e6adee30d1a234567", // USDC
    ///             ])?
    ///     ]);
    ///
    /// // Multiple different filter combinations (Transfer vs Approval events)
    /// let query = Query::new()
    ///     .where_logs_any([
    ///         // Transfer events from specific contracts
    ///         LogFilter::any()
    ///             .and_address_any(["0xdac17f958d2ee523a2206206994597c13d831ec7"])?
    ///             .and_topic0_any(["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"])?, // Transfer
    ///         // Approval events from any contract
    ///         LogFilter::any()
    ///             .and_topic0_any(["0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"])?, // Approval
    ///     ]);
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn where_logs_any<I>(mut self, logs: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<LogSelection>,
    {
        let log_selection: Vec<LogSelection> = logs.into_iter().map(Into::into).collect();
        self.logs = log_selection;
        self
    }

    /// Set block filters that the query will match against.
    ///
    /// This method accepts any iterable of items that can be converted to `BlockSelection`.
    /// Common input types include `BlockFilter` objects and `BlockSelection` objects.
    /// The query will return blocks that match any of the provided filters.
    ///
    /// # Arguments
    /// * `blocks` - An iterable of block filters/selections to match
    ///
    /// # Examples
    ///
    /// ```
    /// use hypersync_net_types::{Query, BlockFilter};
    ///
    /// // Match blocks by specific hashes
    /// let query = Query::new()
    ///     .from_block(18_000_000)
    ///     .where_blocks_any([
    ///         BlockFilter::any()
    ///             .and_hash_any(["0x40d008f2a1653f09b7b028d30c7fd1ba7c84900fcfb032040b3eb3d16f84d294"])?,
    ///     ]);
    ///
    /// // Match blocks by specific miners
    /// let query = Query::new()
    ///     .where_blocks_any([
    ///         BlockFilter::any()
    ///             .and_miner_address_any([
    ///                 "0xdac17f958d2ee523a2206206994597c13d831ec7", // Mining pool 1
    ///                 "0xa0b86a33e6c11c8c0c5c0b5e6adee30d1a234567", // Mining pool 2
    ///             ])?
    ///     ]);
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn where_blocks_any<I>(mut self, blocks: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<BlockSelection>,
    {
        let block_selections: Vec<BlockSelection> = blocks.into_iter().map(Into::into).collect();
        self.blocks = block_selections;
        self
    }

    /// Set transaction filters that the query will match against.
    ///
    /// This method accepts any iterable of items that can be converted to `TransactionSelection`.
    /// Common input types include `TransactionFilter` objects and `TransactionSelection` objects.
    /// The query will return transactions that match any of the provided filters.
    ///
    /// # Arguments
    /// * `transactions` - An iterable of transaction filters/selections to match
    ///
    /// # Examples
    ///
    /// ```
    /// use hypersync_net_types::{Query, TransactionFilter};
    ///
    /// // Match transactions from specific addresses
    /// let query = Query::new()
    ///     .from_block(18_000_000)
    ///     .where_transactions_any([
    ///         TransactionFilter::any()
    ///             .and_from_address_any(["0xa0b86a33e6c11c8c0c5c0b5e6adee30d1a234567"])?,
    ///     ]);
    ///
    /// // Match transactions by function signature (e.g., transfer calls)
    /// let transfer_sig = "0xa9059cbb"; // transfer(address,uint256)
    /// let query = Query::new()
    ///     .where_transactions_any([
    ///         TransactionFilter::any().and_sighash_any([transfer_sig])?
    ///     ]);
    ///
    /// // Match failed transactions
    /// let query = Query::new()
    ///     .where_transactions_any([
    ///         TransactionFilter::any().and_status(0) // 0 = failed
    ///     ]);
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn where_transactions_any<I>(mut self, transactions: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<TransactionSelection>,
    {
        let transaction_selections: Vec<TransactionSelection> =
            transactions.into_iter().map(Into::into).collect();
        self.transactions = transaction_selections;
        self
    }

    /// Set trace filters that the query will match against.
    ///
    /// This method accepts any iterable of items that can be converted to `TraceSelection`.
    /// Common input types include `TraceFilter` objects and `TraceSelection` objects.
    /// The query will return traces that match any of the provided filters.
    ///
    /// # Availability
    /// **Note**: Trace data is only available on select hypersync instances. Not all blockchain
    /// networks provide trace data, and it requires additional infrastructure to collect and serve.
    /// Check your hypersync instance documentation to confirm trace availability for your target network.
    ///
    /// # Arguments
    /// * `traces` - An iterable of trace filters/selections to match
    ///
    /// # Examples
    ///
    /// ```
    /// use hypersync_net_types::{Query, TraceFilter};
    ///
    /// // Match traces from specific caller addresses
    /// let query = Query::new()
    ///     .from_block(18_000_000)
    ///     .where_traces_any([
    ///         TraceFilter::any()
    ///             .and_from_address_any(["0xa0b86a33e6c11c8c0c5c0b5e6adee30d1a234567"])?,
    ///     ]);
    ///
    /// // Match contract creation traces
    /// let query = Query::new()
    ///     .where_traces_any([
    ///         TraceFilter::any().and_call_type_any(["create", "suicide"])
    ///     ]);
    ///
    /// // Match traces by function signature
    /// let transfer_sig = "0xa9059cbb"; // transfer(address,uint256)
    /// let query = Query::new()
    ///     .where_traces_any([
    ///         TraceFilter::any().and_sighash_any([transfer_sig])?
    ///     ]);
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn where_traces_any<I>(mut self, traces: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<TraceSelection>,
    {
        let trace_selections: Vec<TraceSelection> = traces.into_iter().map(Into::into).collect();
        self.traces = trace_selections;
        self
    }

    /// Set the field selection for the query to specify which fields should be returned.
    ///
    /// Field selection allows you to optimize query performance and reduce payload size by
    /// requesting only the fields you need. By default, no fields are selected, which means
    /// you need to explicitly specify which fields to include in the response.
    ///
    /// # Arguments
    /// * `field_selection` - A `FieldSelection` object specifying which fields to include
    ///
    /// # Examples
    ///
    /// ```
    /// use hypersync_net_types::{Query, FieldSelection, block::BlockField, transaction::TransactionField, log::LogField, trace::TraceField};
    ///
    /// // Select only essential block and transaction fields
    /// let field_selection = FieldSelection::new()
    ///     .block([BlockField::Number, BlockField::Hash, BlockField::Timestamp])
    ///     .transaction([TransactionField::Hash, TransactionField::From, TransactionField::To]);
    ///
    /// let query = Query::new()
    ///     .from_block(18_000_000)
    ///     .select_fields(field_selection);
    ///
    /// // Select all available fields for comprehensive analysis
    /// let all_fields = FieldSelection::new()
    ///     .block(BlockField::all())
    ///     .transaction(TransactionField::all())
    ///     .log(LogField::all())
    ///     .trace(TraceField::all());
    ///
    /// let query = Query::new()
    ///     .from_block(18_000_000)
    ///     .select_fields(all_fields);
    ///
    /// // Mixed approach - all blocks and logs, specific transaction fields
    /// let mixed_fields = FieldSelection::new()
    ///     .block(BlockField::all())
    ///     .transaction([
    ///         TransactionField::Hash,
    ///         TransactionField::From,
    ///         TransactionField::To,
    ///         TransactionField::Value,
    ///         TransactionField::GasPrice,
    ///         TransactionField::GasUsed
    ///     ])
    ///     .log(LogField::all());
    ///
    /// let query = Query::new()
    ///     .from_block(18_000_000)
    ///     .select_fields(mixed_fields);
    /// ```
    pub fn select_fields(mut self, field_selection: FieldSelection) -> Self {
        self.field_selection = field_selection;
        self
    }

    /// Set the join mode for the query to control how different data types are related.
    ///
    /// Join mode determines how hypersync correlates data between blocks, transactions, logs, and traces.
    /// This affects which additional related data is included in the response beyond what your filters directly match.
    ///
    /// # Arguments
    /// * `join_mode` - The `JoinMode` to use for the query
    ///
    /// # Join Modes:
    /// - `JoinMode::Default`: Join in order logs → transactions → traces → blocks
    /// - `JoinMode::JoinAll`: Join everything to everything (comprehensive but larger responses)
    /// - `JoinMode::JoinNothing`: No joins, return only directly matched data
    ///
    /// # Examples
    ///
    /// ```
    /// use hypersync_net_types::{Query, JoinMode, LogFilter};
    ///
    /// // Default join mode - get transactions and blocks for matching logs
    /// let query = Query::new()
    ///     .from_block(18_000_000)
    ///     .join_mode(JoinMode::Default)
    ///     .where_logs_any([LogFilter::any()]);
    ///
    /// // Join everything - comprehensive data for analysis
    /// let query = Query::new()
    ///     .from_block(18_000_000)
    ///     .join_mode(JoinMode::JoinAll)
    ///     .where_logs_any([LogFilter::any()]);
    ///
    /// // No joins - only the exact logs that match the filter
    /// let query = Query::new()
    ///     .from_block(18_000_000)
    ///     .join_mode(JoinMode::JoinNothing)
    ///     .where_logs_any([LogFilter::any()]);
    /// ```
    pub fn join_mode(mut self, join_mode: JoinMode) -> Self {
        self.join_mode = join_mode;
        self
    }

    /// Set whether to include all blocks in the requested range, regardless of matches.
    ///
    /// By default, hypersync only returns blocks that are related to matched transactions, logs, or traces.
    /// Setting this to `true` forces the server to return data for all blocks in the range [from_block, to_block),
    /// even if they don't contain any matching transactions or logs.
    ///
    /// # Use Cases:
    /// - Block-level analytics requiring complete block data
    /// - Ensuring no blocks are missed in sequential processing
    /// - Getting block headers for every block in a range
    ///
    /// # Performance Note:
    /// Setting this to `true` can significantly increase response size and processing time,
    /// especially for large block ranges. Use judiciously.
    ///
    /// # Examples
    ///
    /// ```
    /// use hypersync_net_types::{Query, LogFilter, FieldSelection, block::BlockField};
    ///
    /// // Include all blocks for complete block header data
    /// let query = Query::new()
    ///     .from_block(18_000_000)
    ///     .to_block(18_000_100)
    ///     .include_all_blocks()
    ///     .select_fields(
    ///         FieldSelection::new()
    ///             .block([BlockField::Number, BlockField::Hash, BlockField::Timestamp])
    ///     );
    ///
    /// // Normal mode - only blocks with matching logs
    /// let query = Query::new()
    ///     .from_block(18_000_000)
    ///     .include_all_blocks()
    ///     .where_logs_any([
    ///         LogFilter::any()
    ///             .and_address_any(["0xdac17f958d2ee523a2206206994597c13d831ec7"])?
    ///     ]);
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn include_all_blocks(mut self) -> Self {
        self.include_all_blocks = true;
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
    /// // Select all block fields for comprehensive data
    /// let field_selection = FieldSelection::new()
    ///     .block(BlockField::all());
    ///
    /// // Can also use a vector for specific fields
    /// let fields = vec![BlockField::Number, BlockField::ParentHash];
    /// let field_selection = FieldSelection::new()
    ///     .block(fields);
    ///
    /// // Chain with other field selections - mix all and specific
    /// let field_selection = FieldSelection::new()
    ///     .block(BlockField::all())
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
    /// // Select all transaction fields for complete transaction data
    /// let field_selection = FieldSelection::new()
    ///     .transaction(TransactionField::all());
    ///
    /// // Select fields related to gas and value
    /// let field_selection = FieldSelection::new()
    ///     .transaction([
    ///         TransactionField::GasPrice,
    ///         TransactionField::GasUsed,
    ///         TransactionField::Value,
    ///     ]);
    ///
    /// // Chain with other field selections - mix all and specific
    /// let field_selection = FieldSelection::new()
    ///     .block([BlockField::Number])
    ///     .transaction(TransactionField::all());
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
    /// // Select all log fields for comprehensive event data
    /// let field_selection = FieldSelection::new()
    ///     .log(LogField::all());
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
    /// // Chain with transaction fields - mix all and specific
    /// let field_selection = FieldSelection::new()
    ///     .transaction([TransactionField::Hash])
    ///     .log(LogField::all());
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
    /// # Availability
    /// **Note**: Trace data is only available on select hypersync instances. Not all blockchain
    /// networks provide trace data, and it requires additional infrastructure to collect and serve.
    /// Check your hypersync instance documentation to confirm trace availability for your target network.
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
    /// // Select all trace fields for comprehensive trace analysis
    /// let field_selection = FieldSelection::new()
    ///     .trace(TraceField::all());
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
    /// // Combine with other data types - mix all and specific
    /// let field_selection = FieldSelection::new()
    ///     .transaction([TransactionField::Hash])
    ///     .trace(TraceField::all())
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
