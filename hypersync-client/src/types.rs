use std::sync::Arc;

use crate::{
    simple_types::{Block, Event, Log, Trace, Transaction},
    ArrowChunk, FromArrow,
};
use anyhow::{anyhow, Context, Result};
use hypersync_net_types::RollbackGuard;
use polars_arrow::datatypes::SchemaRef;

#[derive(Default, Debug, Clone)]
pub struct ArrowResponseData {
    pub blocks: Vec<ArrowBatch>,
    pub transactions: Vec<ArrowBatch>,
    pub logs: Vec<ArrowBatch>,
    pub traces: Vec<ArrowBatch>,
    pub decoded_logs: Vec<ArrowBatch>,
}

#[derive(Default, Debug, Clone)]
pub struct ResponseData {
    pub blocks: Vec<Vec<Block>>,
    pub transactions: Vec<Vec<Transaction>>,
    pub logs: Vec<Vec<Log>>,
    pub traces: Vec<Vec<Trace>>,
}

impl From<&'_ ArrowResponse> for EventResponse {
    fn from(arrow_response: &'_ ArrowResponse) -> Self {
        let r: QueryResponse = arrow_response.into();
        Self {
            archive_height: r.archive_height,
            next_block: r.next_block,
            total_execution_time: r.total_execution_time,
            data: vec![r.data.into()],
            rollback_guard: r.rollback_guard,
        }
    }
}

impl From<&'_ ArrowResponse> for QueryResponse {
    fn from(arrow_response: &ArrowResponse) -> Self {
        let blocks = arrow_response
            .data
            .blocks
            .iter()
            .map(Block::from_arrow)
            .collect();
        let transactions = arrow_response
            .data
            .transactions
            .iter()
            .map(Transaction::from_arrow)
            .collect();
        let logs = arrow_response
            .data
            .logs
            .iter()
            .map(Log::from_arrow)
            .collect();
        let traces = arrow_response
            .data
            .traces
            .iter()
            .map(Trace::from_arrow)
            .collect();

        QueryResponse {
            archive_height: arrow_response.archive_height,
            next_block: arrow_response.next_block,
            total_execution_time: arrow_response.total_execution_time,
            data: ResponseData {
                blocks,
                transactions,
                logs,
                traces,
            },
            rollback_guard: arrow_response.rollback_guard.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct QueryResponse<T = ResponseData> {
    /// Current height of the source hypersync instance
    pub archive_height: Option<u64>,
    /// Next block to query for, the responses are paginated so
    /// the caller should continue the query from this block if they
    /// didn't get responses up to the to_block they specified in the Query.
    pub next_block: u64,
    /// Total time it took the hypersync instance to execute the query.
    pub total_execution_time: u64,
    /// Response data
    pub data: T,
    /// Rollback guard
    pub rollback_guard: Option<RollbackGuard>,
}

pub type ArrowResponse = QueryResponse<ArrowResponseData>;
pub type EventResponse = QueryResponse<Vec<Vec<Event>>>;

#[derive(Debug, Clone)]
pub struct ArrowBatch {
    pub chunk: Arc<ArrowChunk>,
    pub schema: SchemaRef,
}

impl ArrowBatch {
    pub fn column<T: 'static>(&self, name: &str) -> Result<&T> {
        match self
            .schema
            .fields
            .iter()
            .enumerate()
            .find(|(_, f)| f.name == name)
        {
            Some((idx, _)) => {
                let col = self
                    .chunk
                    .columns()
                    .get(idx)
                    .context("get column using index")?;
                let col = col.as_any().downcast_ref::<T>().with_context(|| {
                    anyhow!(
                        "cast type of column '{}', it was {:?}",
                        name,
                        col.data_type()
                    )
                })?;
                Ok(col)
            }
            None => Err(anyhow!("field {} not found in schema", name)),
        }
    }
}
