use crate::{hypersync_net_types_capnp, CapnpBuilder, CapnpReader, Selection};
use anyhow::Context;
use arrayvec::ArrayVec;
use hypersync_format::{Address, FilterWrapper, LogArgument};
use serde::{Deserialize, Serialize};

pub type LogSelection = Selection<LogFilter>;

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct LogFilter {
    /// Address of the contract, any logs that has any of these addresses will be returned.
    /// Empty means match all.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub address: Vec<Address>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address_filter: Option<FilterWrapper>,
    /// Topics to match, each member of the top level array is another array, if the nth topic matches any
    ///  topic specified in nth element of topics, the log will be returned. Empty means match all.
    #[serde(default, skip_serializing_if = "ArrayVec::is_empty")]
    pub topics: ArrayVec<Vec<LogArgument>, 4>,
}

impl LogFilter {
    /// Base filter to match all logs
    pub fn new() -> Self {
        Default::default()
    }

    /// Filter logs by any of the provided contract addresses.
    ///
    /// This method accepts any iterable of values that can be converted to `Address`.
    /// Common input types include string slices, byte arrays, and `Address` objects.
    ///
    /// # Arguments
    /// * `addresses` - An iterable of addresses to filter by
    ///
    /// # Returns
    /// * `Ok(Self)` - The updated filter on success
    /// * `Err(anyhow::Error)` - If any address fails to convert
    ///
    /// # Examples
    ///
    /// ```
    /// use hypersync_net_types::LogFilter;
    ///
    /// // Filter by a single address using string
    /// let filter = LogFilter::new()
    ///     .address_any(["0xdac17f958d2ee523a2206206994597c13d831ec7"])?;
    ///
    /// // Filter by multiple addresses
    /// let filter = LogFilter::new()
    ///     .address_any([
    ///         "0xdac17f958d2ee523a2206206994597c13d831ec7", // USDT
    ///         "0xa0b86a33e6c11c8c0c5c0b5e6adee30d1a234567", // Another contract
    ///     ])?;
    ///
    /// // Using byte arrays
    /// let usdt_address = [
    ///     0xda, 0xc1, 0x7f, 0x95, 0x8d, 0x2e, 0xe5, 0x23, 0xa2, 0x20,
    ///     0x62, 0x06, 0x99, 0x45, 0x97, 0xc1, 0x3d, 0x83, 0x1e, 0xc7
    /// ];
    /// let filter = LogFilter::new()
    ///     .address_any([usdt_address])?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn address_any<I, A>(mut self, addresses: I) -> anyhow::Result<Self>
    where
        I: IntoIterator<Item = A>,
        A: TryInto<Address>,
        A::Error: std::error::Error + Send + Sync + 'static,
    {
        let mut converted_addresses: Vec<Address> = Vec::new();
        for (idx, address) in addresses.into_iter().enumerate() {
            converted_addresses.push(
                address
                    .try_into()
                    .with_context(|| format!("invalid address at position {idx}"))?,
            );
        }
        self.address = converted_addresses;
        Ok(self)
    }

    fn topic_any<I, T>(mut self, topic_idx: usize, topics: I) -> anyhow::Result<Self>
    where
        I: IntoIterator<Item = T>,
        T: TryInto<LogArgument>,
        T::Error: std::error::Error + Send + Sync + 'static,
    {
        if topic_idx > 3 {
            anyhow::bail!("topic index should not be greater than 3");
        }

        if self.topics.len() <= topic_idx {
            for _ in 0..=(topic_idx - self.topics.len()) {
                self.topics.push(Vec::new());
            }
        }
        let topic_selection = self
            .topics
            .get_mut(topic_idx)
            .expect("topic should exist from previous check");
        topic_selection.clear();
        for (idx, topic) in topics.into_iter().enumerate() {
            topic_selection.push(
                topic
                    .try_into()
                    .with_context(|| format!("invalid topic at position {idx}"))?,
            );
        }
        Ok(self)
    }

    /// Filter logs by any of the provided topic0 values.
    ///
    /// Topic0 typically contains the event signature hash for Ethereum logs.
    /// This method accepts any iterable of values that can be converted to `LogArgument`.
    /// Common input types include string slices, byte arrays, and `LogArgument` objects.
    ///
    /// # Arguments
    /// * `topics` - An iterable of topic0 values to filter by
    ///
    /// # Returns
    /// * `Ok(Self)` - The updated filter on success
    /// * `Err(anyhow::Error)` - If any topic fails to convert
    ///
    /// # Examples
    ///
    /// ```
    /// use hypersync_net_types::LogFilter;
    ///
    /// // Filter by Transfer event signature
    /// let transfer_sig = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
    /// let filter = LogFilter::new()
    ///     .topic0_any([transfer_sig])?;
    ///
    /// // Filter by multiple event signatures
    /// let filter = LogFilter::new()
    ///     .topic0_any([
    ///         "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef", // Transfer
    ///         "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925", // Approval
    ///     ])?;
    ///
    /// // Using byte arrays
    /// let transfer_bytes = [
    ///     0xdd, 0xf2, 0x52, 0xad, 0x1b, 0xe2, 0xc8, 0x9b, 0x69, 0xc2, 0xb0, 0x68, 0xfc, 0x37, 0x8d, 0xaa,
    ///     0x95, 0x2b, 0xa7, 0xf1, 0x63, 0xc4, 0xa1, 0x16, 0x28, 0xf5, 0x5a, 0x4d, 0xf5, 0x23, 0xb3, 0xef
    /// ];
    /// let filter = LogFilter::new()
    ///     .topic0_any([transfer_bytes])?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn topic0_any<I, T>(self, topics: I) -> anyhow::Result<Self>
    where
        I: IntoIterator<Item = T>,
        T: TryInto<LogArgument>,
        T::Error: std::error::Error + Send + Sync + 'static,
    {
        self.topic_any(0, topics)
    }
    /// Filter logs by any of the provided topic1 values.
    ///
    /// Topic1 typically contains the first indexed parameter of an Ethereum event.
    /// This method accepts any iterable of values that can be converted to `LogArgument`.
    /// Common input types include string slices, byte arrays, and `LogArgument` objects.
    ///
    /// # Arguments
    /// * `topics` - An iterable of topic1 values to filter by
    ///
    /// # Returns
    /// * `Ok(Self)` - The updated filter on success
    /// * `Err(anyhow::Error)` - If any topic fails to convert
    ///
    /// # Examples
    ///
    /// ```
    /// use hypersync_net_types::LogFilter;
    ///
    /// // Filter by specific sender address in Transfer events (topic1 = from)
    /// let sender_address = "0x000000000000000000000000dac17f958d2ee523a2206206994597c13d831ec7";
    /// let filter = LogFilter::new()
    ///     .topic1_any([sender_address])?;
    ///
    /// // Filter by multiple possible senders
    /// let filter = LogFilter::new()
    ///     .topic1_any([
    ///         "0x000000000000000000000000dac17f958d2ee523a2206206994597c13d831ec7",
    ///         "0x000000000000000000000000a0b86a33e6c11c8c0c5c0b5e6adee30d1a234567",
    ///     ])?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn topic1_any<I, T>(self, topics: I) -> anyhow::Result<Self>
    where
        I: IntoIterator<Item = T>,
        T: TryInto<LogArgument>,
        T::Error: std::error::Error + Send + Sync + 'static,
    {
        self.topic_any(1, topics)
    }
    /// Filter logs by any of the provided topic2 values.
    ///
    /// Topic2 typically contains the second indexed parameter of an Ethereum event.
    /// This method accepts any iterable of values that can be converted to `LogArgument`.
    /// Common input types include string slices, byte arrays, and `LogArgument` objects.
    ///
    /// # Arguments
    /// * `topics` - An iterable of topic2 values to filter by
    ///
    /// # Returns
    /// * `Ok(Self)` - The updated filter on success
    /// * `Err(anyhow::Error)` - If any topic fails to convert
    ///
    /// # Examples
    ///
    /// ```
    /// use hypersync_net_types::LogFilter;
    ///
    /// // Filter by specific recipient address in Transfer events (topic2 = to)
    /// let recipient_address = "0x000000000000000000000000a0b86a33e6c11c8c0c5c0b5e6adee30d1a234567";
    /// let filter = LogFilter::new()
    ///     .topic2_any([recipient_address])?;
    ///
    /// // Filter by multiple possible recipients
    /// let filter = LogFilter::new()
    ///     .topic2_any([
    ///         "0x000000000000000000000000a0b86a33e6c11c8c0c5c0b5e6adee30d1a234567",
    ///         "0x000000000000000000000000dac17f958d2ee523a2206206994597c13d831ec7",
    ///     ])?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn topic2_any<I, T>(self, topics: I) -> anyhow::Result<Self>
    where
        I: IntoIterator<Item = T>,
        T: TryInto<LogArgument>,
        T::Error: std::error::Error + Send + Sync + 'static,
    {
        self.topic_any(2, topics)
    }
    /// Filter logs by any of the provided topic3 values.
    ///
    /// Topic3 typically contains the third indexed parameter of an Ethereum event.
    /// This method accepts any iterable of values that can be converted to `LogArgument`.
    /// Common input types include string slices, byte arrays, and `LogArgument` objects.
    ///
    /// # Arguments
    /// * `topics` - An iterable of topic3 values to filter by
    ///
    /// # Returns
    /// * `Ok(Self)` - The updated filter on success
    /// * `Err(anyhow::Error)` - If any topic fails to convert
    ///
    /// # Examples
    ///
    /// ```
    /// use hypersync_net_types::LogFilter;
    ///
    /// // Filter by specific token ID in NFT Transfer events (topic3 = tokenId)
    /// let token_id = "0x0000000000000000000000000000000000000000000000000000000000000001";
    /// let filter = LogFilter::new()
    ///     .topic3_any([token_id])?;
    ///
    /// // Filter by multiple token IDs
    /// let filter = LogFilter::new()
    ///     .topic3_any([
    ///         "0x0000000000000000000000000000000000000000000000000000000000000001",
    ///         "0x0000000000000000000000000000000000000000000000000000000000000002",
    ///     ])?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn topic3_any<I, T>(self, topics: I) -> anyhow::Result<Self>
    where
        I: IntoIterator<Item = T>,
        T: TryInto<LogArgument>,
        T::Error: std::error::Error + Send + Sync + 'static,
    {
        self.topic_any(3, topics)
    }
}

impl CapnpBuilder<hypersync_net_types_capnp::log_filter::Owned> for LogFilter {
    fn populate_builder(
        &self,
        builder: &mut hypersync_net_types_capnp::log_filter::Builder,
    ) -> Result<(), capnp::Error> {
        // Set addresses
        if !self.address.is_empty() {
            let mut addr_list = builder.reborrow().init_address(self.address.len() as u32);
            for (i, addr) in self.address.iter().enumerate() {
                addr_list.set(i as u32, addr.as_slice());
            }
        }

        // Set address filter
        if let Some(filter) = &self.address_filter {
            builder.reborrow().set_address_filter(filter.0.as_bytes());
        }

        // Set topics
        if !self.topics.is_empty() {
            let mut topics_list = builder.reborrow().init_topics(self.topics.len() as u32);
            for (i, topic_vec) in self.topics.iter().enumerate() {
                let mut topic_list = topics_list
                    .reborrow()
                    .init(i as u32, topic_vec.len() as u32);
                for (j, topic) in topic_vec.iter().enumerate() {
                    topic_list.set(j as u32, topic.as_slice());
                }
            }
        }

        Ok(())
    }
}

impl CapnpReader<hypersync_net_types_capnp::log_filter::Owned> for LogFilter {
    /// Deserialize LogSelection from Cap'n Proto reader
    fn from_reader(
        reader: hypersync_net_types_capnp::log_filter::Reader,
    ) -> Result<Self, capnp::Error> {
        let mut address = Vec::new();

        // Parse addresses
        if reader.has_address() {
            let addr_list = reader.get_address()?;
            for i in 0..addr_list.len() {
                let addr_data = addr_list.get(i)?;
                if addr_data.len() == 20 {
                    let mut addr_bytes = [0u8; 20];
                    addr_bytes.copy_from_slice(addr_data);
                    address.push(Address::from(addr_bytes));
                }
            }
        }

        let mut address_filter = None;

        // Parse address filter
        if reader.has_address_filter() {
            let filter_data = reader.get_address_filter()?;
            // For now, skip filter deserialization - this would need proper Filter construction
            // log_selection.address_filter = Some(FilterWrapper::from_keys(std::iter::empty(), None).unwrap());

            let Ok(wrapper) = FilterWrapper::from_bytes(filter_data) else {
                return Err(capnp::Error::failed("Invalid address filter".to_string()));
            };
            address_filter = Some(wrapper);
        }

        let mut topics = ArrayVec::new();

        // Parse topics
        if reader.has_topics() {
            let topics_list = reader.get_topics()?;
            for i in 0..topics_list.len() {
                let topic_list = topics_list.get(i)?;
                let mut topic_vec = Vec::new();
                for j in 0..topic_list.len() {
                    let topic_data = topic_list.get(j)?;
                    if topic_data.len() == 32 {
                        let mut topic_bytes = [0u8; 32];
                        topic_bytes.copy_from_slice(topic_data);
                        topic_vec.push(LogArgument::from(topic_bytes));
                    }
                }
                if i < 4 && !topic_vec.is_empty() {
                    topics.push(topic_vec);
                }
            }
        }

        Ok(Self {
            address,
            address_filter,
            topics,
        })
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    schemars::JsonSchema,
    strum_macros::EnumIter,
    strum_macros::AsRefStr,
    strum_macros::Display,
    strum_macros::EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum LogField {
    // Core log fields
    TransactionHash,
    BlockHash,
    BlockNumber,
    TransactionIndex,
    LogIndex,
    Address,
    Data,
    Removed,

    // Topic fields
    Topic0,
    Topic1,
    Topic2,
    Topic3,
}

impl Ord for LogField {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_ref().cmp(other.as_ref())
    }
}

impl PartialOrd for LogField {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl LogField {
    pub fn all() -> std::collections::BTreeSet<Self> {
        use strum::IntoEnumIterator;
        Self::iter().collect()
    }

    /// Convert LogField to Cap'n Proto enum
    pub fn to_capnp(&self) -> crate::hypersync_net_types_capnp::LogField {
        match self {
            LogField::TransactionHash => {
                crate::hypersync_net_types_capnp::LogField::TransactionHash
            }
            LogField::BlockHash => crate::hypersync_net_types_capnp::LogField::BlockHash,
            LogField::BlockNumber => crate::hypersync_net_types_capnp::LogField::BlockNumber,
            LogField::TransactionIndex => {
                crate::hypersync_net_types_capnp::LogField::TransactionIndex
            }
            LogField::LogIndex => crate::hypersync_net_types_capnp::LogField::LogIndex,
            LogField::Address => crate::hypersync_net_types_capnp::LogField::Address,
            LogField::Data => crate::hypersync_net_types_capnp::LogField::Data,
            LogField::Removed => crate::hypersync_net_types_capnp::LogField::Removed,
            LogField::Topic0 => crate::hypersync_net_types_capnp::LogField::Topic0,
            LogField::Topic1 => crate::hypersync_net_types_capnp::LogField::Topic1,
            LogField::Topic2 => crate::hypersync_net_types_capnp::LogField::Topic2,
            LogField::Topic3 => crate::hypersync_net_types_capnp::LogField::Topic3,
        }
    }

    /// Convert Cap'n Proto enum to LogField
    pub fn from_capnp(field: crate::hypersync_net_types_capnp::LogField) -> Self {
        match field {
            crate::hypersync_net_types_capnp::LogField::TransactionHash => {
                LogField::TransactionHash
            }
            crate::hypersync_net_types_capnp::LogField::BlockHash => LogField::BlockHash,
            crate::hypersync_net_types_capnp::LogField::BlockNumber => LogField::BlockNumber,
            crate::hypersync_net_types_capnp::LogField::TransactionIndex => {
                LogField::TransactionIndex
            }
            crate::hypersync_net_types_capnp::LogField::LogIndex => LogField::LogIndex,
            crate::hypersync_net_types_capnp::LogField::Address => LogField::Address,
            crate::hypersync_net_types_capnp::LogField::Data => LogField::Data,
            crate::hypersync_net_types_capnp::LogField::Removed => LogField::Removed,
            crate::hypersync_net_types_capnp::LogField::Topic0 => LogField::Topic0,
            crate::hypersync_net_types_capnp::LogField::Topic1 => LogField::Topic1,
            crate::hypersync_net_types_capnp::LogField::Topic2 => LogField::Topic2,
            crate::hypersync_net_types_capnp::LogField::Topic3 => LogField::Topic3,
        }
    }
}

#[cfg(test)]
mod tests {
    use hypersync_format::Hex;

    use super::*;
    use crate::{query::tests::test_query_serde, FieldSelection, Query};

    #[test]
    fn test_all_fields_in_schema() {
        let schema = hypersync_schema::log();
        let schema_fields = schema
            .fields
            .iter()
            .map(|f| f.name.clone())
            .collect::<std::collections::BTreeSet<_>>();
        let all_fields = LogField::all()
            .into_iter()
            .map(|f| f.as_ref().to_string())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(schema_fields, all_fields);
    }

    #[test]
    fn test_serde_matches_strum() {
        for field in LogField::all() {
            let serialized = serde_json::to_string(&field).unwrap();
            let strum = serde_json::to_string(&field.as_ref()).unwrap();
            assert_eq!(serialized, strum, "strum value should be the same as serde");
        }
    }

    #[test]
    fn test_log_selection_serde_with_defaults() {
        let log_selection = LogSelection::default();
        let field_selection = FieldSelection {
            log: LogField::all(),
            ..Default::default()
        };
        let query = Query {
            logs: vec![log_selection],
            field_selection,
            ..Default::default()
        };

        test_query_serde(query, "log selection with defaults");
    }

    #[test]
    fn test_log_selection_serde_with_full_values() {
        let log_selection = LogFilter {
            address: vec![
                Address::decode_hex("0xdadB0d80178819F2319190D340ce9A924f783711").unwrap(),
            ],
            address_filter: Some(FilterWrapper::new(16, 1)),
            topics: {
                let mut topics = ArrayVec::new();
                topics.push(vec![LogArgument::decode_hex(
                    "0x1234567890123456789012345678901234567890123456789012345678901234",
                )
                .unwrap()]);
                topics.push(vec![LogArgument::decode_hex(
                    "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
                )
                .unwrap()]);
                topics
            },
        };
        let field_selection = FieldSelection {
            log: LogField::all(),
            ..Default::default()
        };
        let query = Query {
            logs: vec![log_selection.into()],
            field_selection,
            ..Default::default()
        };

        test_query_serde(query, "log selection with full values");
    }

    #[test]
    fn test_log_filter_builder() -> anyhow::Result<()> {
        let lf = LogFilter::new()
            .address_any([
                "0xdadB0d80178819F2319190D340ce9A924f783711",
                "0xdadB0d80178819F2319190D340ce9A924f783712",
            ])?
            .topic0_any([
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
            ])?;

        assert_eq!(lf.address.len(), 2);
        assert_eq!(lf.topics.len(), 1);
        assert_eq!(lf.topics[0].len(), 2);
        assert_eq!(lf.address_filter, None);

        let lf =
            lf.topic0_any(["0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"])?;
        assert_eq!(
            lf.topics[0].len(),
            1,
            "shoul overwrite previous topic0 selection"
        );

        let lf = lf.topic3_any([
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
        ])?;

        assert_eq!(lf.topics[3].len(), 2, "should have correctly added topic3");
        assert_eq!(
            lf.topics[2].len(),
            0,
            "should have added empty topics before the first non-empty topic"
        );
        assert_eq!(
            lf.topics[1].len(),
            0,
            "should have added empty topics before the first non-empty topic"
        );
        assert_eq!(lf.topics[0].len(), 1, "topic0 should not have been changed");

        Ok(())
    }
}
