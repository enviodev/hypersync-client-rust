use crate::block::{BlockField, BlockSelection};
use crate::hypersync_net_types_capnp;
use crate::log::{LogField, LogSelection};
use crate::trace::{TraceField, TraceSelection};
use crate::transaction::{TransactionField, TransactionSelection};
use capnp::message::Builder;
use capnp::{message::ReaderOptions, serialize_packed};
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

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
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

#[derive(Default, Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
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
        let message_reader =
            serialize_packed::read_message(&mut std::io::Cursor::new(bytes), ReaderOptions::new())?;
        let query = message_reader.get_root::<hypersync_net_types_capnp::query::Reader>()?;

        Self::from_capnp_query(query)
    }

    fn populate_capnp_query(
        &self,
        mut query: hypersync_net_types_capnp::query::Builder,
    ) -> Result<(), capnp::Error> {
        query.reborrow().set_from_block(self.from_block);

        if let Some(to_block) = self.to_block {
            let mut to_block_builder = query.reborrow().init_to_block();
            to_block_builder.set_value(to_block)
        }

        query
            .reborrow()
            .set_include_all_blocks(self.include_all_blocks);

        // Set max nums using OptUInt64
        if let Some(max_num_blocks) = self.max_num_blocks {
            let mut max_blocks_builder = query.reborrow().init_max_num_blocks();
            max_blocks_builder.set_value(max_num_blocks as u64);
        }
        if let Some(max_num_transactions) = self.max_num_transactions {
            let mut max_tx_builder = query.reborrow().init_max_num_transactions();
            max_tx_builder.set_value(max_num_transactions as u64);
        }
        if let Some(max_num_logs) = self.max_num_logs {
            let mut max_logs_builder = query.reborrow().init_max_num_logs();
            max_logs_builder.set_value(max_num_logs as u64);
        }
        if let Some(max_num_traces) = self.max_num_traces {
            let mut max_traces_builder = query.reborrow().init_max_num_traces();
            max_traces_builder.set_value(max_num_traces as u64);
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

    fn from_capnp_query(
        query: hypersync_net_types_capnp::query::Reader,
    ) -> Result<Self, capnp::Error> {
        let from_block = query.get_from_block();
        let to_block = if query.has_to_block() {
            Some(query.get_to_block()?.get_value())
        } else {
            None
        };
        let include_all_blocks = query.get_include_all_blocks();

        // Parse field selection
        let field_selection =
            if query.has_field_selection() {
                let fs = query.get_field_selection()?;

                let block_fields = if fs.has_block() {
                    let block_list = fs.get_block()?;
                    (0..block_list.len())
                        .map(|i| {
                            BlockField::from_capnp(
                                block_list
                                    .get(i)
                                    .ok()
                                    .unwrap_or(hypersync_net_types_capnp::BlockField::Number),
                            )
                        })
                        .collect::<BTreeSet<_>>()
                } else {
                    BTreeSet::new()
                };

                let transaction_fields =
                    if fs.has_transaction() {
                        let tx_list = fs.get_transaction()?;
                        (0..tx_list.len())
                            .map(|i| {
                                TransactionField::from_capnp(tx_list.get(i).ok().unwrap_or(
                                    hypersync_net_types_capnp::TransactionField::BlockHash,
                                ))
                            })
                            .collect::<BTreeSet<_>>()
                    } else {
                        BTreeSet::new()
                    };

                let log_fields =
                    if fs.has_log() {
                        let log_list = fs.get_log()?;
                        (0..log_list.len())
                            .map(|i| {
                                LogField::from_capnp(log_list.get(i).ok().unwrap_or(
                                    hypersync_net_types_capnp::LogField::TransactionHash,
                                ))
                            })
                            .collect::<BTreeSet<_>>()
                    } else {
                        BTreeSet::new()
                    };

                let trace_fields =
                    if fs.has_trace() {
                        let trace_list = fs.get_trace()?;
                        (0..trace_list.len())
                            .map(|i| {
                                TraceField::from_capnp(trace_list.get(i).ok().unwrap_or(
                                    hypersync_net_types_capnp::TraceField::TransactionHash,
                                ))
                            })
                            .collect::<BTreeSet<_>>()
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
        let max_num_blocks = if query.has_max_num_blocks() {
            let max_blocks_reader = query.get_max_num_blocks()?;
            let value = max_blocks_reader.get_value();
            Some(value as usize)
        } else {
            None
        };
        let max_num_transactions = if query.has_max_num_transactions() {
            let max_tx_reader = query.get_max_num_transactions()?;
            let value = max_tx_reader.get_value();
            Some(value as usize)
        } else {
            None
        };
        let max_num_logs = if query.has_max_num_logs() {
            let max_logs_reader = query.get_max_num_logs()?;
            let value = max_logs_reader.get_value();
            Some(value as usize)
        } else {
            None
        };
        let max_num_traces = if query.has_max_num_traces() {
            let max_traces_reader = query.get_max_num_traces()?;
            let value = max_traces_reader.get_value();
            Some(value as usize)
        } else {
            None
        };

        // Parse join mode
        let join_mode = match query.get_join_mode()? {
            hypersync_net_types_capnp::JoinMode::Default => JoinMode::Default,
            hypersync_net_types_capnp::JoinMode::JoinAll => JoinMode::JoinAll,
            hypersync_net_types_capnp::JoinMode::JoinNothing => JoinMode::JoinNothing,
        };

        // Parse selections
        let logs = if query.has_logs() {
            let logs_list = query.get_logs()?;
            let mut logs = Vec::new();
            for i in 0..logs_list.len() {
                let log_reader = logs_list.get(i);
                logs.push(LogSelection::from_capnp(log_reader)?);
            }
            logs
        } else {
            Vec::new()
        };

        let transactions = if query.has_transactions() {
            let tx_list = query.get_transactions()?;
            let mut transactions = Vec::new();
            for i in 0..tx_list.len() {
                let tx_reader = tx_list.get(i);
                transactions.push(TransactionSelection::from_capnp(tx_reader)?);
            }
            transactions
        } else {
            Vec::new()
        };

        let traces = if query.has_traces() {
            let traces_list = query.get_traces()?;
            let mut traces = Vec::new();
            for i in 0..traces_list.len() {
                let trace_reader = traces_list.get(i);
                traces.push(TraceSelection::from_capnp(trace_reader)?);
            }
            traces
        } else {
            Vec::new()
        };

        let blocks = if query.has_blocks() {
            let blocks_list = query.get_blocks()?;
            let mut blocks = Vec::new();
            for i in 0..blocks_list.len() {
                let block_reader = blocks_list.get(i);
                blocks.push(BlockSelection::from_capnp(block_reader)?);
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
    use arrayvec::ArrayVec;
    use hypersync_format::{Address, Hex, LogArgument};
    use pretty_assertions::assert_eq;

    pub fn test_query_serde(query: Query, label: &str) {
        // time start
        let ser_start = std::time::Instant::now();
        let ser = query.to_capnp_bytes().unwrap();
        let ser_elapsed = ser_start.elapsed();

        let deser_start = std::time::Instant::now();
        let deser = Query::from_capnp_bytes(&ser).unwrap();
        let deser_elapsed = deser_start.elapsed();

        assert_eq!(query, deser);

        let ser_json_start = std::time::Instant::now();
        let ser_json = serde_json::to_string(&query).unwrap();
        let ser_json_elapsed = ser_json_start.elapsed();

        let deser_json_start = std::time::Instant::now();
        let _deser_json: Query = serde_json::from_str(&ser_json).unwrap();
        let deser_json_elapsed = deser_json_start.elapsed();

        let bincode_config = bincode::config::standard();
        let ser_bincode_start = std::time::Instant::now();
        let ser_bincode = bincode::serde::encode_to_vec(&query, bincode_config).unwrap();
        let ser_bincode_elapsed = ser_bincode_start.elapsed();

        let deser_bincode_start = std::time::Instant::now();
        let _: (Query, _) =
            bincode::serde::decode_from_slice(&ser_bincode, bincode_config).unwrap();
        let deser_bincode_elapsed = deser_bincode_start.elapsed();

        fn make_bench(
            ser: std::time::Duration,
            deser: std::time::Duration,
            size: usize,
        ) -> serde_json::Value {
            serde_json::json!({
                "ser": ser.as_micros(),
                "deser": deser.as_micros(),
                "size": size,
            })
        }

        println!(
            "\nBenchmark {}\ncapnp: {}\njson:  {}\nbin:   {}\n",
            label,
            make_bench(ser_elapsed, deser_elapsed, ser.len()),
            make_bench(ser_json_elapsed, deser_json_elapsed, ser_json.len()),
            make_bench(
                ser_bincode_elapsed,
                deser_bincode_elapsed,
                ser_bincode.len()
            )
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
    pub fn test_huge_payload() {
        let mut logs: Vec<LogSelection> = Vec::new();

        for contract_idx in 0..5 {
            let mut topics = ArrayVec::new();
            topics.push(vec![]);
            let mut log_selection = LogSelection {
                address: vec![],
                address_filter: None,
                topics,
            };

            for topic_idx in 0..6 {
                log_selection.topics[0].push(
                    LogArgument::decode_hex(
                        format!(
                            "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7{}{}",
                            contract_idx, topic_idx
                        )
                        .as_str(),
                    )
                    .unwrap(),
                );
            }

            for addr_idx in 0..1000 {
                let zero_padded_addr_idx = format!("{:04}", addr_idx);
                let address = Address::decode_hex(
                    format!(
                        "0x3Cb124E1cDcEECF6E464BB185325608dbe6{}{}",
                        contract_idx, zero_padded_addr_idx,
                    )
                    .as_str(),
                )
                .unwrap();
                log_selection.address.push(address);
            }
            logs.push(log_selection);
        }

        let query = Query {
            from_block: 50,
            to_block: Some(500),
            logs,
            ..Default::default()
        };
        test_query_serde(query, "huge payload");
    }
}
