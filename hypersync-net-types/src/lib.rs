//! Hypersync network types for transport and queries.
//!
//! This library provides types and serialization capabilities for interacting with hypersync servers.
//! It supports both JSON and Cap'n Proto serialization formats for efficient network communication.

// Module declarations
pub mod block;
pub mod log;
pub mod query;
pub mod response;
pub mod trace;
pub mod transaction;
pub mod types;

// Cap'n Proto generated code
#[allow(clippy::all)]
pub mod hypersync_net_types_capnp {
    include!(concat!(env!("OUT_DIR"), "/hypersync_net_types_capnp.rs"));
}

// Re-export types from modules for backward compatibility and convenience
pub use block::{BlockFilter, BlockSelection};
pub use log::{LogFilter, LogSelection};
pub use query::{FieldSelection, JoinMode, Query};
pub use response::{ArchiveHeight, ChainId, RollbackGuard};
pub use trace::{TraceFilter, TraceSelection};
pub use transaction::{AuthorizationSelection, TransactionFilter, TransactionSelection};
pub use types::Sighash;

use serde::{Deserialize, Serialize};

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Selection<T> {
    /// Filters where matching values should be included in the response
    /// Default::default() means include everything
    #[serde(default, flatten)]
    pub include: T,
    /// Filters where matching values should be excluded from the response
    /// None means exclude nothing, Some(Default::default()) means exclude everything
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exclude: Option<T>,
}

impl<T> From<T> for Selection<T> {
    fn from(include: T) -> Self {
        Self {
            include,
            exclude: None,
        }
    }
}
