use hypersync_format::FixedSizeData;

pub type Sighash = FixedSizeData<4>;

/// A collection of filter clauses combined with logical OR semantics.
///
/// `AnyOf<T>` represents multiple filter conditions where a match occurs if ANY of the
/// contained filters match. This enables fluent chaining of filters with OR logic using the
/// `.or()` method.
///
/// This type is typically created by calling `.or()` on filter types like `LogFilter`,
/// `BlockFilter`, `TransactionFilter`, or `TraceFilter`, and can be passed directly to
/// query methods like `where_logs()`, `where_blocks()`, etc.
///
/// # Examples
///
/// ```
/// use hypersync_net_types::{LogFilter, Query};
///
/// // Create filters that match logs from USDT OR USDC contracts
/// let filter = LogFilter::all()
///     .and_address(["0xdac17f958d2ee523a2206206994597c13d831ec7"])? // USDT
///     .or(
///         LogFilter::all()
///             .and_address(["0xa0b86a33e6c11c8c0c5c0b5e6adee30d1a234567"])? // USDC
///     );
///
/// // Use the combined filter in a query
/// let query = Query::new()
///     .from_block(18_000_000)
///     .where_logs(filter);
/// # Ok::<(), anyhow::Error>(())
/// ```
///
/// # Type System
///
/// The generic parameter `T` represents the filter type being combined. For example:
/// - `AnyOf<LogFilter>` for combining log filters
/// - `AnyOf<TransactionFilter>` for combining transaction filters
/// - `AnyOf<BlockFilter>` for combining block filters
/// - `AnyOf<TraceFilter>` for combining trace filters
pub struct AnyOf<T>(Vec<T>);

impl<T> IntoIterator for AnyOf<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<T> AnyOf<T> {
    /// Create a new `AnyOf` containing a single filter clause.
    ///
    /// This is typically used internally when converting from a single filter to an `AnyOf`.
    /// Most users will create `AnyOf` instances by calling `.or()` on filter types.
    ///
    /// # Arguments
    /// * `clause` - The initial filter clause
    ///
    /// # Examples
    ///
    /// ```
    /// use hypersync_net_types::{LogFilter, types::AnyOf};
    ///
    /// let filter = LogFilter::all();
    /// let any_clause = AnyOf::new(filter);
    /// ```
    pub fn new(clause: T) -> Self {
        Self(vec![clause])
    }

    /// Create an `AnyOf` from multiple filter clauses.
    ///
    /// This method accepts any iterable collection of filter clauses and creates an `AnyOf`
    /// that matches if any of the provided clauses match. This is useful when you have a
    /// collection of filters to combine.
    ///
    /// # Arguments
    /// * `clauses` - An iterable collection of filter clauses
    ///
    /// # Examples
    ///
    /// ```
    /// use hypersync_net_types::{LogFilter, types::AnyOf};
    ///
    /// let filters = vec![
    ///     LogFilter::all().and_address(["0xdac17f958d2ee523a2206206994597c13d831ec7"]).unwrap(),
    ///     LogFilter::all().and_address(["0xa0b86a33e6c11c8c0c5c0b5e6adee30d1a234567"]).unwrap(),
    /// ];
    /// let any_clause = AnyOf::any(filters);
    /// ```
    pub fn any<I>(clauses: I) -> Self
    where
        I: IntoIterator<Item = T>,
    {
        Self(clauses.into_iter().collect())
    }

    /// Add another filter clause to this `AnyOf` with OR logic.
    ///
    /// This method extends the current `AnyOf` with an additional filter clause.
    /// The resulting `AnyOf` will match if any of the contained clauses match,
    /// including both the existing clauses and the newly added one.
    ///
    /// This enables fluent chaining: `clause1.or(clause2).or(clause3)`.
    ///
    /// # Arguments
    /// * `clause` - The filter clause to add
    ///
    /// # Returns
    /// The updated `AnyOf` containing the additional clause
    ///
    /// # Examples
    ///
    /// ```
    /// use hypersync_net_types::LogFilter;
    ///
    /// // Chain multiple filters with OR logic
    /// let filter = LogFilter::all()
    ///     .and_address(["0xdac17f958d2ee523a2206206994597c13d831ec7"])? // USDT
    ///     .or(LogFilter::all().and_address(["0xa0b86a33e6c11c8c0c5c0b5e6adee30d1a234567"])?) // USDC  
    ///     .or(LogFilter::all().and_address(["0x6b175474e89094c44da98b954eedeac495271d0f"])?); // DAI
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn or(mut self, clause: T) -> Self {
        self.0.push(clause);
        self
    }
}
