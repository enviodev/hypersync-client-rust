//! # HyperSync Network Types
//!
//! Core network types and query builders for the HyperSync protocol.
//!
//! This crate provides the fundamental types for constructing queries and handling
//! responses when communicating with HyperSync servers. It supports both JSON 
//! and Cap'n Proto serialization formats for efficient network communication.
//!
//! ## Features
//!
//! - **Query builder API**: Fluent interface for building complex blockchain queries
//! - **Type-safe filtering**: Strongly-typed filters for blocks, transactions, logs, and traces
//! - **Field selection**: Choose exactly which data fields to retrieve
//! - **Multiple serialization**: Support for JSON and Cap'n Proto protocols
//! - **Validation**: Built-in query validation and optimization
//!
//! ## Key Types
//!
//! - [`Query`] - Main query builder for specifying blockchain data to retrieve
//! - [`BlockFilter`] - Filter blocks by number, hash, or other criteria
//! - [`TransactionFilter`] - Filter transactions by from/to addresses, value, etc.
//! - [`LogFilter`] - Filter event logs by contract address, topics, etc.
//! - [`TraceFilter`] - Filter execution traces by various criteria
//!
//! ## Example
//!
//! ```
//! use hypersync_net_types::{Query, LogFilter, LogField};
//!
//! // Build a query for ERC20 transfer events
//! let mut query = Query::new().from_block(19000000);
//! query.to_block = Some(19001000);
//! query = query.where_logs(
//!     LogFilter::all()
//!         .and_topic0(["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"])?
//! ).select_log_fields([
//!     LogField::Address,
//!     LogField::Topic1, 
//!     LogField::Topic2,
//!     LogField::Data,
//! ]);
//!
//! println!("Query: {:?}", query);
//! # Ok::<(), anyhow::Error>(())
//! ```

// Module declarations
pub mod block;
pub mod log;
pub mod query;
pub mod request;
pub mod response;
pub mod trace;
pub mod transaction;
pub mod types;

// Cap'n Proto generated code
mod __generated__;
pub use __generated__::hypersync_net_types_capnp;

// Re-export types from modules for backward compatibility and convenience
pub use block::{BlockField, BlockFilter, BlockSelection};
pub use log::{LogField, LogFilter, LogSelection};
pub use query::{FieldSelection, JoinMode, Query};
pub use response::{ArchiveHeight, ChainId, RollbackGuard};
pub use trace::{TraceField, TraceFilter, TraceSelection};
pub use transaction::{
    AuthorizationSelection, TransactionField, TransactionFilter, TransactionSelection,
};
pub use types::Sighash;

use serde::{Deserialize, Serialize};

use crate::types::AnyOf;

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

impl<T> From<Selection<T>> for AnyOf<Selection<T>> {
    fn from(selection: Selection<T>) -> AnyOf<Selection<T>> {
        AnyOf::new(selection)
    }
}

impl<T> Selection<T> {
    /// Create a new selection with the given filter.
    ///
    /// # Arguments
    /// * `filter` - The filter to include in the selection
    ///
    /// # Examples
    ///
    /// ```
    /// use hypersync_net_types::{LogSelection, LogFilter};
    ///
    /// // Create a selection that includes the filter
    ///
    /// let selection = LogSelection::new(
    ///     LogFilter::all().and_address(["0xdadB0d80178819F2319190D340ce9A924f783711"])?,
    /// );
    ///
    /// Ok::<(), anyhow::Error>(())
    /// ```
    pub fn new(filter: T) -> Self {
        Self {
            include: filter,
            exclude: None,
        }
    }

    /// Add a filter to exclude from the selection
    ///
    /// # Arguments
    /// * `filter` - The filter to exclude from the selection
    ///
    /// # Examples
    ///
    /// ```
    /// use hypersync_net_types::{LogSelection, LogFilter};
    ///
    /// // Create a selection with a filter that matches any log. (or your own filter)
    /// let all = LogSelection::new(LogFilter::all());
    ///
    /// // Create a selection that excludes only logs from a specific address
    /// let selection = all.and_not(
    ///     LogFilter::all().and_address(["0xa0b86a33e6c11c8c0c5c0b5e6adee30d1a234567"])?,
    /// );
    ///
    /// Ok::<(), anyhow::Error>(())
    /// ```
    pub fn and_not(mut self, filter: T) -> Self {
        self.exclude = Some(filter);
        self
    }

    /// Combine this selection with another selection using logical OR.
    ///
    /// This creates an `AnyOf` type that will match if either this selection
    /// or the provided selection matches.
    ///
    /// # Arguments
    /// * `selection` - Another selection to combine with this one
    ///
    /// # Returns
    /// An `AnyOf<Self>` that represents the logical OR of both selections
    ///
    /// # Examples
    ///
    /// ```
    /// use hypersync_net_types::{LogSelection, LogFilter};
    ///
    /// // Create two different selections
    /// let selection1 = LogSelection::new(LogFilter::all())
    ///     .and_not(LogFilter::all().and_address(["0xdadB0d80178819F2319190D340ce9A924f783711"])?);
    /// let selection2 = LogSelection::new(
    ///     LogFilter::all().and_address(["0xa0b86a33e6c11c8c0c5c0b5e6adee30d1a234567"])?,
    /// );
    ///
    /// // Combine them with OR - matches logs from either address
    /// let combined = selection1.or(selection2);
    ///
    /// Ok::<(), anyhow::Error>(())
    /// ```
    pub fn or(self, selection: Self) -> AnyOf<Self> {
        AnyOf::new(self).or(selection)
    }
}

impl<T> From<T> for Selection<T> {
    fn from(include: T) -> Self {
        Self {
            include,
            exclude: None,
        }
    }
}

pub trait CapnpBuilder<O: capnp::traits::Owned> {
    fn populate_builder<'a>(&self, builder: &mut O::Builder<'a>) -> Result<(), capnp::Error>;
}

pub trait CapnpReader<O: capnp::traits::Owned> {
    fn from_reader<'a>(reader: O::Reader<'a>) -> Result<Self, capnp::Error>
    where
        Self: Sized;
}

impl<O, T> CapnpBuilder<hypersync_net_types_capnp::selection::Owned<O>> for Selection<T>
where
    O: capnp::traits::Owned,
    T: CapnpBuilder<O>,
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
}

impl<O, T> CapnpReader<hypersync_net_types_capnp::selection::Owned<O>> for Selection<T>
where
    O: capnp::traits::Owned,
    T: CapnpReader<O>,
{
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
