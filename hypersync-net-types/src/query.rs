use crate::block::{BlockField, BlockSelection};
use crate::log::{LogField, LogSelection};
use crate::trace::{TraceField, TraceSelection};
use crate::transaction::{TransactionField, TransactionSelection};
use crate::{hypersync_net_types_capnp, BuilderReader};
use anyhow::Context;
use capnp::message::Builder;
use capnp::message::ReaderOptions;
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
        let query = Query::from_capnp_bytes(&decompressed_bytes)?;
        Ok(query)
    }

    /// Serialize Query to Cap'n Proto format and return as bytes
    pub fn to_capnp_bytes(&self) -> Result<Vec<u8>, capnp::Error> {
        let mut message = Builder::new_default();
        let query = message.init_root::<hypersync_net_types_capnp::query::Builder>();

        self.populate_capnp_query(query)?;

        let mut buf = Vec::new();
        capnp::serialize::write_message(&mut buf, &message)?;
        Ok(buf)
    }

    /// Deserialize Query from Cap'n Proto bytes
    pub fn from_capnp_bytes(bytes: &[u8]) -> Result<Self, capnp::Error> {
        let message_reader =
            capnp::serialize::read_message(&mut std::io::Cursor::new(bytes), ReaderOptions::new())?;
        let query = message_reader.get_root::<hypersync_net_types_capnp::query::Reader>()?;

        Self::from_capnp_query(query)
    }
    /// Serialize using packed format (for testing)
    pub fn to_capnp_bytes_packed(&self) -> Result<Vec<u8>, capnp::Error> {
        let mut message = Builder::new_default();
        let query = message.init_root::<hypersync_net_types_capnp::query::Builder>();

        self.populate_capnp_query(query)?;

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

        Self::from_capnp_query(query)
    }

    fn populate_capnp_query(
        &self,
        mut query: hypersync_net_types_capnp::query::Builder,
    ) -> Result<(), capnp::Error> {
        let mut block_range_builder = query.reborrow().init_block_range();
        block_range_builder.set_from_block(self.from_block);

        if let Some(to_block) = self.to_block {
            let mut to_block_builder = block_range_builder.reborrow().init_to_block();
            to_block_builder.set_value(to_block)
        }

        // Hehe
        let mut body_builder = query.reborrow().init_body().init_query();

        body_builder
            .reborrow()
            .set_include_all_blocks(self.include_all_blocks);

        // Set max nums using OptUInt64
        if let Some(max_num_blocks) = self.max_num_blocks {
            let mut max_blocks_builder = body_builder.reborrow().init_max_num_blocks();
            max_blocks_builder.set_value(max_num_blocks as u64);
        }
        if let Some(max_num_transactions) = self.max_num_transactions {
            let mut max_tx_builder = body_builder.reborrow().init_max_num_transactions();
            max_tx_builder.set_value(max_num_transactions as u64);
        }
        if let Some(max_num_logs) = self.max_num_logs {
            let mut max_logs_builder = body_builder.reborrow().init_max_num_logs();
            max_logs_builder.set_value(max_num_logs as u64);
        }
        if let Some(max_num_traces) = self.max_num_traces {
            let mut max_traces_builder = body_builder.reborrow().init_max_num_traces();
            max_traces_builder.set_value(max_num_traces as u64);
        }

        // Set join mode
        let join_mode = match self.join_mode {
            JoinMode::Default => hypersync_net_types_capnp::JoinMode::Default,
            JoinMode::JoinAll => hypersync_net_types_capnp::JoinMode::JoinAll,
            JoinMode::JoinNothing => hypersync_net_types_capnp::JoinMode::JoinNothing,
        };
        body_builder.reborrow().set_join_mode(join_mode);

        // Set field selection
        {
            let mut field_selection = body_builder.reborrow().init_field_selection();

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
            let mut logs_list = body_builder.reborrow().init_logs(self.logs.len() as u32);
            for (i, log_selection) in self.logs.iter().enumerate() {
                let mut log_sel = logs_list.reborrow().get(i as u32);
                log_selection.populate_builder(&mut log_sel)?;
            }
        }

        // Set transactions
        {
            let mut tx_list = body_builder
                .reborrow()
                .init_transactions(self.transactions.len() as u32);
            for (i, tx_selection) in self.transactions.iter().enumerate() {
                let mut tx_sel = tx_list.reborrow().get(i as u32);
                tx_selection.populate_builder(&mut tx_sel)?;
            }
        }

        // Set traces
        {
            let mut trace_list = body_builder
                .reborrow()
                .init_traces(self.traces.len() as u32);
            for (i, trace_selection) in self.traces.iter().enumerate() {
                let mut trace_sel = trace_list.reborrow().get(i as u32);
                trace_selection.populate_builder(&mut trace_sel)?;
            }
        }

        // Set blocks
        {
            let mut block_list = body_builder
                .reborrow()
                .init_blocks(self.blocks.len() as u32);
            for (i, block_selection) in self.blocks.iter().enumerate() {
                let mut block_sel = block_list.reborrow().get(i as u32);
                block_selection.populate_builder(&mut block_sel)?;
            }
        }

        Ok(())
    }

    fn from_capnp_query(
        query: hypersync_net_types_capnp::query::Reader,
    ) -> Result<Self, capnp::Error> {
        let block_range = query.get_block_range()?;

        let from_block = block_range.get_from_block();
        let to_block = if block_range.has_to_block() {
            Some(block_range.get_to_block()?.get_value())
        } else {
            None
        };
        let body = match query.get_body().which()? {
            hypersync_net_types_capnp::query::body::Which::Query(query) => query?,
            hypersync_net_types_capnp::query::body::Which::QueryId(_) => {
                return Err(capnp::Error::unimplemented(
                    "QueryId not yet implemented".to_string(),
                ))
            }
        };

        let include_all_blocks = body.get_include_all_blocks();

        // Parse field selection
        let field_selection = if body.has_field_selection() {
            let fs = body.get_field_selection()?;

            let block_fields = if fs.has_block() {
                let block_list = fs.get_block()?;
                (0..block_list.len())
                    .map(|i| block_list.get(i).map(BlockField::from_capnp))
                    .collect::<Result<BTreeSet<_>, capnp::NotInSchema>>()?
            } else {
                BTreeSet::new()
            };

            let transaction_fields = if fs.has_transaction() {
                let tx_list = fs.get_transaction()?;
                (0..tx_list.len())
                    .map(|i| tx_list.get(i).map(TransactionField::from_capnp))
                    .collect::<Result<BTreeSet<_>, capnp::NotInSchema>>()?
            } else {
                BTreeSet::new()
            };

            let log_fields = if fs.has_log() {
                let log_list = fs.get_log()?;
                (0..log_list.len())
                    .map(|i| log_list.get(i).map(LogField::from_capnp))
                    .collect::<Result<BTreeSet<_>, capnp::NotInSchema>>()?
            } else {
                BTreeSet::new()
            };

            let trace_fields = if fs.has_trace() {
                let trace_list = fs.get_trace()?;
                (0..trace_list.len())
                    .map(|i| trace_list.get(i).map(TraceField::from_capnp))
                    .collect::<Result<BTreeSet<_>, capnp::NotInSchema>>()?
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

        // Parse max values using OptUInt64
        let max_num_blocks = if body.has_max_num_blocks() {
            let max_blocks_reader = body.get_max_num_blocks()?;
            let value = max_blocks_reader.get_value();
            Some(value as usize)
        } else {
            None
        };
        let max_num_transactions = if body.has_max_num_transactions() {
            let max_tx_reader = body.get_max_num_transactions()?;
            let value = max_tx_reader.get_value();
            Some(value as usize)
        } else {
            None
        };
        let max_num_logs = if body.has_max_num_logs() {
            let max_logs_reader = body.get_max_num_logs()?;
            let value = max_logs_reader.get_value();
            Some(value as usize)
        } else {
            None
        };
        let max_num_traces = if body.has_max_num_traces() {
            let max_traces_reader = body.get_max_num_traces()?;
            let value = max_traces_reader.get_value();
            Some(value as usize)
        } else {
            None
        };

        // Parse join mode
        let join_mode = match body.get_join_mode()? {
            hypersync_net_types_capnp::JoinMode::Default => JoinMode::Default,
            hypersync_net_types_capnp::JoinMode::JoinAll => JoinMode::JoinAll,
            hypersync_net_types_capnp::JoinMode::JoinNothing => JoinMode::JoinNothing,
        };

        // Parse selections
        let logs = if body.has_logs() {
            let logs_list = body.get_logs()?;
            let mut logs = Vec::new();
            for i in 0..logs_list.len() {
                let log_reader = logs_list.get(i);
                logs.push(LogSelection::from_reader(log_reader)?);
            }
            logs
        } else {
            Vec::new()
        };

        let transactions = if body.has_transactions() {
            let tx_list = body.get_transactions()?;
            let mut transactions = Vec::new();
            for i in 0..tx_list.len() {
                let tx_reader = tx_list.get(i);
                transactions.push(TransactionSelection::from_reader(tx_reader)?);
            }
            transactions
        } else {
            Vec::new()
        };

        let traces = if body.has_traces() {
            let traces_list = body.get_traces()?;
            let mut traces = Vec::new();
            for i in 0..traces_list.len() {
                let trace_reader = traces_list.get(i);
                traces.push(TraceSelection::from_reader(trace_reader)?);
            }
            traces
        } else {
            Vec::new()
        };

        let blocks = if body.has_blocks() {
            let blocks_list = body.get_blocks()?;
            let mut blocks = Vec::new();
            for i in 0..blocks_list.len() {
                let block_reader = blocks_list.get(i);
                blocks.push(BlockSelection::from_reader(block_reader)?);
            }
            blocks
        } else {
            Vec::new()
        };

        Ok(Query {
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

#[cfg(test)]
pub mod tests {
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
}
