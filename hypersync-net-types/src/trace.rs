use crate::{hypersync_net_types_capnp, types::Sighash};
use hypersync_format::{Address, FilterWrapper};
use serde::{Deserialize, Serialize};

#[derive(Default, Serialize, Deserialize, Clone, Debug)]
pub struct TraceSelection {
    #[serde(default)]
    pub from: Vec<Address>,
    #[serde(default)]
    pub from_filter: Option<FilterWrapper>,
    #[serde(default)]
    pub to: Vec<Address>,
    #[serde(default)]
    pub to_filter: Option<FilterWrapper>,
    #[serde(default)]
    pub address: Vec<Address>,
    #[serde(default)]
    pub address_filter: Option<FilterWrapper>,
    #[serde(default)]
    pub call_type: Vec<String>,
    #[serde(default)]
    pub reward_type: Vec<String>,
    #[serde(default)]
    #[serde(rename = "type")]
    pub type_: Vec<String>,
    #[serde(default)]
    pub sighash: Vec<Sighash>,
}

impl TraceSelection {
    pub(crate) fn populate_capnp_builder(
        trace_sel: &TraceSelection,
        mut builder: hypersync_net_types_capnp::trace_selection::Builder,
    ) -> Result<(), capnp::Error> {
        // Set from addresses
        {
            let mut from_list = builder.reborrow().init_from(trace_sel.from.len() as u32);
            for (i, addr) in trace_sel.from.iter().enumerate() {
                from_list.set(i as u32, addr.as_slice());
            }
        }

        // Set from filter
        if let Some(filter) = &trace_sel.from_filter {
            builder.reborrow().set_from_filter(filter.0.as_bytes());
        }

        // Set to addresses
        {
            let mut to_list = builder.reborrow().init_to(trace_sel.to.len() as u32);
            for (i, addr) in trace_sel.to.iter().enumerate() {
                to_list.set(i as u32, addr.as_slice());
            }
        }

        // Set to filter
        if let Some(filter) = &trace_sel.to_filter {
            builder.reborrow().set_to_filter(filter.0.as_bytes());
        }

        // Set addresses
        {
            let mut addr_list = builder
                .reborrow()
                .init_address(trace_sel.address.len() as u32);
            for (i, addr) in trace_sel.address.iter().enumerate() {
                addr_list.set(i as u32, addr.as_slice());
            }
        }

        // Set address filter
        if let Some(filter) = &trace_sel.address_filter {
            builder.reborrow().set_address_filter(filter.0.as_bytes());
        }

        // Set call types
        {
            let mut call_type_list = builder
                .reborrow()
                .init_call_type(trace_sel.call_type.len() as u32);
            for (i, call_type) in trace_sel.call_type.iter().enumerate() {
                call_type_list.set(i as u32, call_type);
            }
        }

        // Set reward types
        {
            let mut reward_type_list = builder
                .reborrow()
                .init_reward_type(trace_sel.reward_type.len() as u32);
            for (i, reward_type) in trace_sel.reward_type.iter().enumerate() {
                reward_type_list.set(i as u32, reward_type);
            }
        }

        // Set kinds
        {
            let mut kind_list = builder.reborrow().init_kind(trace_sel.type_.len() as u32);
            for (i, kind) in trace_sel.type_.iter().enumerate() {
                kind_list.set(i as u32, kind);
            }
        }

        // Set sighash
        {
            let mut sighash_list = builder
                .reborrow()
                .init_sighash(trace_sel.sighash.len() as u32);
            for (i, sighash) in trace_sel.sighash.iter().enumerate() {
                sighash_list.set(i as u32, sighash.as_slice());
            }
        }

        Ok(())
    }
}

#[derive(
    Debug,
    Clone,
    Eq,
    PartialEq,
    Hash,
    Serialize,
    Deserialize,
    schemars::JsonSchema,
    strum_macros::EnumIter,
)]
#[serde(rename_all = "snake_case")]
pub enum TraceField {
    // Core trace fields
    TransactionHash,
    BlockHash,
    BlockNumber,
    TransactionPosition,
    Type,
    Error,

    // Address fields
    From,
    To,
    Author,

    // Gas fields
    Gas,
    GasUsed,

    // Additional trace fields from Arrow schema
    ActionAddress,
    Address,
    Balance,
    CallType,
    Code,
    Init,
    Input,
    Output,
    RefundAddress,
    RewardType,
    Sighash,
    Subtraces,
    TraceAddress,
    Value,
}

impl TraceField {
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
        let schema = hypersync_schema::trace();
        let mut schema_fields = schema
            .fields
            .iter()
            .map(|f| f.name.clone())
            .collect::<Vec<_>>();
        schema_fields.sort();
        let mut all_fields = TraceField::all();
        all_fields.sort_by(|a, b| std::cmp::Ord::cmp(&format!("{:?}", a), &format!("{:?}", b)));
        assert_eq!(
            serde_json::to_string(&schema_fields).unwrap(),
            serde_json::to_string(&all_fields).unwrap()
        );
    }
}
