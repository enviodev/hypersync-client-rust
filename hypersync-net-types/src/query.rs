use crate::block::{BlockField, BlockSelection};
use crate::log::{LogField, LogSelection};
use crate::trace::{TraceField, TraceSelection};
use crate::transaction::{TransactionField, TransactionSelection};
use crate::{hypersync_net_types_capnp, CapnpBuilder, CapnpReader};
use anyhow::Context;
use capnp::message::Builder;
use capnp::message::ReaderOptions;
use hypersync_format::FixedSizeData;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// A 128 bit hash of the query body, used as a unique identifier for the query body
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct QueryId(pub FixedSizeData<16>);
impl QueryId {
    pub fn from_query_body_reader(
        reader: hypersync_net_types_capnp::query_body::Reader<'_>,
    ) -> Result<QueryId, capnp::Error> {
        // See https://capnproto.org/encoding.html#canonicalization
        // we need to ensure the query body is canonicalized for hashing
        let mut canon_builder = capnp::message::Builder::new_default();
        canon_builder.set_root_canonical(reader)?;

        // After canonicalization, there is only one segment.
        // We can just hash this withouth the segment table
        let segment = match canon_builder.get_segments_for_output() {
            capnp::OutputSegments::SingleSegment([segment]) => segment,
            capnp::OutputSegments::MultiSegment(items) => {
                return Err(capnp::Error::failed(format!(
                    "Expected exactly 1 segment, found {}",
                    items.len(),
                )))
            }
        };

        let hash: u128 = xxhash_rust::xxh3::xxh3_128(segment);

        Ok(QueryId(FixedSizeData::<16>::from(hash.to_be_bytes())))
    }

    pub fn from_query(query: &Query) -> Result<Self, capnp::Error> {
        let mut message = Builder::new_default();
        let mut query_body_builder =
            message.init_root::<hypersync_net_types_capnp::query_body::Builder>();
        query_body_builder.build_from_query(query)?;
        Self::from_query_body_reader(query_body_builder.into_reader())
    }
}

#[derive(Default, Serialize, Deserialize, Clone, Copy, Debug, PartialEq)]
pub struct BlockRange {
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
}

pub enum Request {
    QueryBody(Box<Query>),
    QueryId {
        from_block: u64,
        to_block: Option<u64>,
        id: QueryId,
    },
}

impl hypersync_net_types_capnp::block_range::Builder<'_> {
    pub fn set(&mut self, from_block: u64, to_block: Option<u64>) -> Result<(), capnp::Error> {
        self.reborrow().set_from_block(from_block);

        if let Some(to_block) = to_block {
            let mut to_block_builder = self.reborrow().init_to_block();
            to_block_builder.set_value(to_block)
        }

        Ok(())
    }
}

impl hypersync_net_types_capnp::query::Builder<'_> {
    pub fn build_full_query_from_query(&mut self, query: &Query) -> Result<(), capnp::Error> {
        let mut block_range_builder = self.reborrow().init_block_range();
        block_range_builder.set(query.from_block, query.to_block)?;

        let mut query_body_builder = self.reborrow().init_body().init_query();
        query_body_builder.build_from_query(query)?;

        Ok(())
    }

    pub fn build_query_id_from_query(&mut self, query: &Query) -> Result<(), capnp::Error> {
        self.reborrow()
            .init_block_range()
            .set(query.from_block, query.to_block)?;

        let id = QueryId::from_query(query)?;
        self.reborrow().init_body().set_query_id(id.0.as_slice());
        Ok(())
    }
}

impl hypersync_net_types_capnp::query_body::Builder<'_> {
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

impl CapnpReader<hypersync_net_types_capnp::query::Owned> for Query {
    fn from_reader(query: hypersync_net_types_capnp::query::Reader) -> Result<Self, capnp::Error> {
        let block_range = query.get_block_range()?;
        let from_block = block_range.get_from_block();
        let to_block = if block_range.has_to_block() {
            Some(block_range.get_to_block()?.get_value())
        } else {
            None
        };
        let body_reader = match query.get_body().which()? {
            hypersync_net_types_capnp::query::body::Which::Query(query_body_reader) => {
                query_body_reader?
            }
            hypersync_net_types_capnp::query::body::Which::QueryId(_) => {
                return Err(capnp::Error::failed(
                    "QueryId cannot be read from capnp request with QueryBody".to_string(),
                ));
            }
        };

        let include_all_blocks = body_reader.get_include_all_blocks();

        // Parse field selection
        let field_selection = if body_reader.has_field_selection() {
            let fs = body_reader.get_field_selection()?;
            FieldSelection::from_reader(fs)?
        } else {
            FieldSelection::default()
        };

        // Parse max values using OptUInt64
        let max_num_blocks = if body_reader.has_max_num_blocks() {
            let max_blocks_reader = body_reader.get_max_num_blocks()?;
            let value = max_blocks_reader.get_value();
            Some(value as usize)
        } else {
            None
        };
        let max_num_transactions = if body_reader.has_max_num_transactions() {
            let max_tx_reader = body_reader.get_max_num_transactions()?;
            let value = max_tx_reader.get_value();
            Some(value as usize)
        } else {
            None
        };
        let max_num_logs = if body_reader.has_max_num_logs() {
            let max_logs_reader = body_reader.get_max_num_logs()?;
            let value = max_logs_reader.get_value();
            Some(value as usize)
        } else {
            None
        };
        let max_num_traces = if body_reader.has_max_num_traces() {
            let max_traces_reader = body_reader.get_max_num_traces()?;
            let value = max_traces_reader.get_value();
            Some(value as usize)
        } else {
            None
        };

        // Parse join mode
        let join_mode = match body_reader.get_join_mode()? {
            hypersync_net_types_capnp::JoinMode::Default => JoinMode::Default,
            hypersync_net_types_capnp::JoinMode::JoinAll => JoinMode::JoinAll,
            hypersync_net_types_capnp::JoinMode::JoinNothing => JoinMode::JoinNothing,
        };

        // Parse selections
        let mut logs = Vec::new();
        if body_reader.has_logs() {
            let logs_list = body_reader.get_logs()?;
            for log_reader in logs_list {
                logs.push(LogSelection::from_reader(log_reader)?);
            }
        }

        let mut transactions = Vec::new();
        if body_reader.has_transactions() {
            let tx_list = body_reader.get_transactions()?;
            for tx_reader in tx_list {
                transactions.push(TransactionSelection::from_reader(tx_reader)?);
            }
        }

        let mut traces = Vec::new();
        if body_reader.has_traces() {
            let traces_list = body_reader.get_traces()?;
            for trace_reader in traces_list {
                traces.push(TraceSelection::from_reader(trace_reader)?);
            }
        }

        let mut blocks = Vec::new();
        if body_reader.has_blocks() {
            let blocks_list = body_reader.get_blocks()?;
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

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Copy)]
pub enum JoinMode {
    /// Join in this order logs -> transactions -> traces -> blocks
    Default,
    /// Join everything to everything. For example if logSelection matches log0, we get the
    /// associated transaction of log0 and then we get associated logs of that transaction as well. Applies similarly
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
impl Query {
    pub fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        // Check compression.rs benchmarks
        // regulas capnp bytes compresses better with zstd than
        // capnp packed bytes
        let capnp_bytes = self
            .to_capnp_bytes()
            .context("Failed converting query to capnp message")?;

        // ZSTD level 6 seems to have the best tradeoffs in terms of achieving
        // a small payload, and being fast to decode once encoded.
        let compressed_bytes = zstd::encode_all(capnp_bytes.as_slice(), 6)
            .context("Failed compressing capnp message to bytes")?;
        Ok(compressed_bytes)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        // Check compression.rs benchmarks
        let decompressed_bytes = zstd::decode_all(bytes)?;
        let query = Self::from_capnp_bytes(&decompressed_bytes)?;
        Ok(query)
    }

    /// Serialize Query to Cap'n Proto format and return as bytes
    pub fn to_capnp_bytes(&self) -> Result<Vec<u8>, capnp::Error> {
        let mut message = Builder::new_default();
        let mut query_builder = message.init_root::<hypersync_net_types_capnp::query::Builder>();

        query_builder.build_full_query_from_query(self)?;

        // self.populate_builder(&mut query)?;

        let mut buf = Vec::new();
        capnp::serialize::write_message(&mut buf, &message)?;
        Ok(buf)
    }

    /// Deserialize Query from Cap'n Proto bytes
    pub fn from_capnp_bytes(bytes: &[u8]) -> Result<Self, capnp::Error> {
        let message_reader =
            capnp::serialize::read_message(&mut std::io::Cursor::new(bytes), ReaderOptions::new())?;
        let query = message_reader.get_root::<hypersync_net_types_capnp::query::Reader>()?;

        Self::from_reader(query)
    }
    /// Serialize using packed format (for testing)
    pub fn to_capnp_bytes_packed(&self) -> Result<Vec<u8>, capnp::Error> {
        let mut message = Builder::new_default();
        let mut query_builder = message.init_root::<hypersync_net_types_capnp::query::Builder>();

        query_builder.build_full_query_from_query(self)?;

        let mut buf = Vec::new();
        capnp::serialize_packed::write_message(&mut buf, &message)?;
        Ok(buf)
    }

    /// Deserialize using packed format (for testing)
    pub fn from_capnp_bytes_packed(bytes: &[u8]) -> Result<Self, capnp::Error> {
        let message_reader = capnp::serialize_packed::read_message(
            &mut std::io::Cursor::new(bytes),
            ReaderOptions::new(),
        )?;
        let query = message_reader.get_root::<hypersync_net_types_capnp::query::Reader>()?;

        Self::from_reader(query)
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
    use crate::{BlockFilter, LogFilter};

    use super::*;
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

        test_encode_decode(
            &query,
            label.to_string() + "-capnp",
            |q| q.to_capnp_bytes().unwrap(),
            |bytes| Query::from_capnp_bytes(bytes).unwrap(),
        );
        test_encode_decode(
            &query,
            label.to_string() + "-capnp-packed",
            |q| q.to_capnp_bytes_packed().unwrap(),
            |bytes| Query::from_capnp_bytes_packed(bytes).unwrap(),
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

    #[test]
    fn test_query_id() {
        let query = Query {
            logs: vec![Default::default()].into_iter().collect(),
            field_selection: FieldSelection {
                log: LogField::all(),
                ..Default::default()
            },
            ..Default::default()
        };

        let query_id = QueryId::from_query(&query).unwrap();
        println!("{query_id:?}");
    }

    #[test]
    fn test_needs_canonicalization_for_hashing() {
        fn add_log_selection(
            query_body_builder: &mut hypersync_net_types_capnp::query_body::Builder,
        ) {
            let mut logs_builder = query_body_builder.reborrow().init_logs(1).get(0);
            LogSelection::from(LogFilter {
                address: vec![FixedSizeData::<20>::from([1u8; 20])],
                ..Default::default()
            })
            .populate_builder(&mut logs_builder)
            .unwrap();
        }
        fn add_block_selection(
            query_body_builder: &mut hypersync_net_types_capnp::query_body::Builder,
        ) {
            let mut block_selection_builder = query_body_builder.reborrow().init_blocks(1).get(0);
            BlockSelection::from(BlockFilter {
                hash: vec![FixedSizeData::<32>::from([1u8; 32])],
                ..Default::default()
            })
            .populate_builder(&mut block_selection_builder)
            .unwrap();
        }
        let (hash_a, hash_a_canon) = {
            let mut message = Builder::new_default();
            let mut query_body_builder =
                message.init_root::<hypersync_net_types_capnp::query_body::Builder>();
            add_log_selection(&mut query_body_builder);
            add_block_selection(&mut query_body_builder);

            let mut message_canon = Builder::new_default();
            message_canon
                .set_root_canonical(query_body_builder.into_reader())
                .unwrap();

            let mut buf = Vec::new();
            capnp::serialize::write_message(&mut buf, &message).unwrap();
            let hash = xxhash_rust::xxh3::xxh3_128(&buf);
            let mut buf = Vec::new();
            capnp::serialize::write_message(&mut buf, &message_canon).unwrap();
            let hash_canon = xxhash_rust::xxh3::xxh3_128(&buf);
            (hash, hash_canon)
        };

        let (hash_b, hash_b_canon) = {
            let mut message = Builder::new_default();
            let mut query_body_builder =
                message.init_root::<hypersync_net_types_capnp::query_body::Builder>();
            // Insert block then log (the opposite order), allocater will not canonicalize
            add_block_selection(&mut query_body_builder);
            add_log_selection(&mut query_body_builder);

            let mut message_canon = Builder::new_default();
            message_canon
                .set_root_canonical(query_body_builder.into_reader())
                .unwrap();

            let mut buf = Vec::new();
            capnp::serialize::write_message(&mut buf, &message).unwrap();
            let hash = xxhash_rust::xxh3::xxh3_128(&buf);
            let mut buf = Vec::new();
            capnp::serialize::write_message(&mut buf, &message_canon).unwrap();
            let hash_canon = xxhash_rust::xxh3::xxh3_128(&buf);
            (hash, hash_canon)
        };
        assert_ne!(
            hash_a, hash_b,
            "queries should be different since they are not canonicalized"
        );

        assert_eq!(
            hash_a_canon, hash_b_canon,
            "queries should be the same since they are canonicalized"
        );
    }
}
