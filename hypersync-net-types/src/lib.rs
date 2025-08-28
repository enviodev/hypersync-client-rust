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
pub use block::BlockSelection;
pub use log::LogSelection;
pub use query::{FieldSelection, JoinMode, Query};
pub use response::{ArchiveHeight, ChainId, RollbackGuard};
pub use trace::TraceSelection;
pub use transaction::{AuthorizationSelection, TransactionSelection};
pub use types::Sighash;
