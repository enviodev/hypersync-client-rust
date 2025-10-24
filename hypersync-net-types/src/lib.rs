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

pub(crate) trait BuilderReader<O: capnp::traits::Owned, E = capnp::Error> {
    fn populate_builder<'a>(&self, builder: &mut O::Builder<'a>) -> Result<(), E>;

    fn from_reader<'a>(reader: O::Reader<'a>) -> Result<Self, E>
    where
        Self: Sized;
}

impl<O, T> BuilderReader<hypersync_net_types_capnp::selection::Owned<O>> for Selection<T>
where
    O: capnp::traits::Owned,
    T: BuilderReader<O, capnp::Error>,
{
    fn populate_builder<'a>(
        &self,
        builder: &mut hypersync_net_types_capnp::selection::Builder<'a, O>,
    ) -> Result<(), capnp::Error> {
        {
            let mut include_builder = builder.reborrow().init_include();
            self.include.populate_builder(&mut include_builder)?;
        } // include borrow ends

        if let Some(exclude) = &self.exclude {
            let mut exclude_builder = builder.reborrow().init_exclude();
            exclude.populate_builder(&mut exclude_builder)?;
        } // exclude borrow ends

        Ok(())
    }

    fn from_reader<'a>(
        reader: hypersync_net_types_capnp::selection::Reader<'a, O>,
    ) -> Result<Self, capnp::Error> {
        let include = T::from_reader(reader.get_include()?)?;
        let exclude = if reader.has_exclude() {
            Some(T::from_reader(reader.get_exclude()?)?)
        } else {
            None
        };
        Ok(Self { include, exclude })
    }
}
