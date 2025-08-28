use crate::hypersync_net_types_capnp;
use arrayvec::ArrayVec;
use hypersync_format::{Address, FilterWrapper, LogArgument};
use serde::{Deserialize, Serialize};

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq)]
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
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema, strum_macros::EnumIter)]
#[serde(rename_all = "snake_case")]
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

impl LogField {
    pub fn all() -> Vec<Self> {
        use strum::IntoEnumIterator;
        Self::iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_fields_in_schema() {
        let schema = hypersync_schema::log();
        let mut schema_fields = schema
            .fields
            .iter()
            .map(|f| f.name.clone())
            .collect::<Vec<_>>();
        schema_fields.sort();
        let mut all_fields = LogField::all();
        all_fields.sort_by(|a, b| std::cmp::Ord::cmp(&format!("{:?}", a), &format!("{:?}", b)));
        assert_eq!(
            serde_json::to_string(&schema_fields).unwrap(),
            serde_json::to_string(&all_fields).unwrap()
        );
    }
}
