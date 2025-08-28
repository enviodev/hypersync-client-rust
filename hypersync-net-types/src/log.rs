use crate::hypersync_net_types_capnp;
use arrayvec::ArrayVec;
use hypersync_format::{Address, FilterWrapper, LogArgument};
use serde::{Deserialize, Serialize};

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct LogSelection {
    /// Address of the contract, any logs that has any of these addresses will be returned.
    /// Empty means match all.
    #[serde(default)]
    pub address: Vec<Address>,
    #[serde(default)]
    pub address_filter: Option<FilterWrapper>,
    /// Topics to match, each member of the top level array is another array, if the nth topic matches any
    ///  topic specified in nth element of topics, the log will be returned. Empty means match all.
    #[serde(default)]
    pub topics: ArrayVec<Vec<LogArgument>, 4>,
}

impl LogSelection {
    pub(crate) fn populate_capnp_builder(
        log_sel: &LogSelection,
        mut builder: hypersync_net_types_capnp::log_selection::Builder,
    ) -> Result<(), capnp::Error> {
        // Set addresses
        {
            let mut addr_list = builder
                .reborrow()
                .init_address(log_sel.address.len() as u32);
            for (i, addr) in log_sel.address.iter().enumerate() {
                addr_list.set(i as u32, addr.as_slice());
            }
        }

        // Set address filter
        if let Some(filter) = &log_sel.address_filter {
            builder.reborrow().set_address_filter(filter.0.as_bytes());
        }

        // Set topics
        {
            let mut topics_list = builder.reborrow().init_topics(log_sel.topics.len() as u32);
            for (i, topic_vec) in log_sel.topics.iter().enumerate() {
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

    /// Deserialize LogSelection from Cap'n Proto reader
    pub fn from_capnp(
        reader: hypersync_net_types_capnp::log_selection::Reader,
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

        Ok(LogSelection {
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
    use super::*;

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
}
