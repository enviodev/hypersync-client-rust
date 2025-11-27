use crate::simple_types::{Block, Event, InternalEventJoinStrategy, Log, Trace, Transaction};
use arrow::array::RecordBatch;
use hypersync_net_types::RollbackGuard;

/// Query response in Arrow format
#[derive(Default, Debug, Clone)]
pub struct ArrowResponseData {
    /// Query blocks response
    pub blocks: Vec<RecordBatch>,
    /// Query transactions response
    pub transactions: Vec<RecordBatch>,
    /// Query logs response
    pub logs: Vec<RecordBatch>,
    /// Query traces response
    pub traces: Vec<RecordBatch>,
    /// Query decoded_logs response.
    ///
    /// Populated only if event_signature is present.
    pub decoded_logs: Vec<RecordBatch>,
}

/// Query response data in Rust native format
#[derive(Default, Debug, Clone)]
pub struct ResponseData {
    /// Query blocks response
    pub blocks: Vec<Vec<Block>>,
    /// Query transactions response
    pub transactions: Vec<Vec<Transaction>>,
    /// Query logs response
    pub logs: Vec<Vec<Log>>,
    /// Query traces response
    pub traces: Vec<Vec<Trace>>,
}

impl EventResponse {
    /// Create EventResponse from ArrowResponse with the specified event join strategy
    pub(crate) fn from_arrow_response(
        arrow_response: &ArrowResponse,
        event_join_strategy: &InternalEventJoinStrategy,
    ) -> Self {
        let r: QueryResponse = arrow_response.into();
        Self {
            archive_height: r.archive_height,
            next_block: r.next_block,
            total_execution_time: r.total_execution_time,
            data: event_join_strategy.join_from_response_data(r.data),
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

/// Query response from hypersync instance.
/// Contain next_block field in case query didn't process all the block range
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

/// Alias for Arrow Query response
pub type ArrowResponse = QueryResponse<ArrowResponseData>;
/// Alias for Event oriented, vectorized QueryResponse
pub type EventResponse = QueryResponse<Vec<Event>>;
