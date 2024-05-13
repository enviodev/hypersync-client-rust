use std::sync::Arc;

use crate::ArrowChunk;
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

#[derive(Debug, Clone)]
pub struct QueryResponse<T> {
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
                    .context("get column")?
                    .as_any()
                    .downcast_ref::<T>()
                    .with_context(|| anyhow!("cast type of column '{}'", name))?;
                Ok(col)
            }
            None => Err(anyhow!("field {} not found in schema", name)),
        }
    }
}
