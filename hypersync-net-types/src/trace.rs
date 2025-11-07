use crate::{hypersync_net_types_capnp, types::Sighash, CapnpBuilder, CapnpReader, Selection};
use anyhow::Context;
use hypersync_format::{Address, FilterWrapper};
use serde::{Deserialize, Serialize};

pub type TraceSelection = Selection<TraceFilter>;

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct TraceFilter {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub from: Vec<Address>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_filter: Option<FilterWrapper>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub to: Vec<Address>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to_filter: Option<FilterWrapper>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub address: Vec<Address>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address_filter: Option<FilterWrapper>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub call_type: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reward_type: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "type")]
    pub type_: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sighash: Vec<Sighash>,
}

impl TraceFilter {
    /// Create a trace filter that matches any trace.
    ///
    /// This creates an empty filter with no constraints, which will match all traces.
    /// You can then use the builder methods to add specific filtering criteria.
    ///
    /// # Examples
    ///
    /// ```
    /// use hypersync_net_types::TraceFilter;
    ///
    /// // Create a filter that matches any trace
    /// let filter = TraceFilter::any();
    ///
    /// // Chain with other filter methods
    /// let filter = TraceFilter::any()
    ///     .and_from_address(["0xa0b86a33e6c11c8c0c5c0b5e6adee30d1a234567"])?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn any() -> Self {
        Default::default()
    }

    /// Filter traces by any of the provided "from" addresses.
    ///
    /// This method accepts any iterable of values that can be converted to `Address`.
    /// Common input types include string slices, byte arrays, and `Address` objects.
    /// The "from" address typically represents the caller or originator of the trace.
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
    /// use hypersync_net_types::TraceFilter;
    ///
    /// // Filter by a single caller address
    /// let filter = TraceFilter::any()
    ///     .and_from_address(["0xa0b86a33e6c11c8c0c5c0b5e6adee30d1a234567"])?;
    ///
    /// // Filter by multiple caller addresses
    /// let filter = TraceFilter::any()
    ///     .and_from_address([
    ///         "0xa0b86a33e6c11c8c0c5c0b5e6adee30d1a234567",
    ///         "0xdac17f958d2ee523a2206206994597c13d831ec7",
    ///     ])?;
    ///
    /// // Using byte arrays
    /// let caller_address = [
    ///     0xa0, 0xb8, 0x6a, 0x33, 0xe6, 0xc1, 0x1c, 0x8c, 0x0c, 0x5c,
    ///     0x0b, 0x5e, 0x6a, 0xde, 0xe3, 0x0d, 0x1a, 0x23, 0x45, 0x67
    /// ];
    /// let filter = TraceFilter::any()
    ///     .and_from_address([caller_address])?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn and_from_address<I, A>(mut self, addresses: I) -> anyhow::Result<Self>
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
                    .with_context(|| format!("invalid from address at position {idx}"))?,
            );
        }
        self.from = converted_addresses;
        Ok(self)
    }

    /// Filter traces by any of the provided "to" addresses.
    ///
    /// This method accepts any iterable of values that can be converted to `Address`.
    /// Common input types include string slices, byte arrays, and `Address` objects.
    /// The "to" address typically represents the target or recipient of the trace.
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
    /// use hypersync_net_types::TraceFilter;
    ///
    /// // Filter by a single target address
    /// let filter = TraceFilter::any()
    ///     .and_to_address(["0xdac17f958d2ee523a2206206994597c13d831ec7"])?;
    ///
    /// // Filter by multiple target addresses
    /// let filter = TraceFilter::any()
    ///     .and_to_address([
    ///         "0xdac17f958d2ee523a2206206994597c13d831ec7",
    ///         "0xa0b86a33e6c11c8c0c5c0b5e6adee30d1a234567",
    ///     ])?;
    ///
    /// // Chain with from address filtering
    /// let filter = TraceFilter::any()
    ///     .and_from_address(["0xa0b86a33e6c11c8c0c5c0b5e6adee30d1a234567"])?
    ///     .and_to_address(["0xdac17f958d2ee523a2206206994597c13d831ec7"])?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn and_to_address<I, A>(mut self, addresses: I) -> anyhow::Result<Self>
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
                    .with_context(|| format!("invalid to address at position {idx}"))?,
            );
        }
        self.to = converted_addresses;
        Ok(self)
    }

    /// Filter traces by any of the provided contract addresses.
    ///
    /// This method accepts any iterable of values that can be converted to `Address`.
    /// Common input types include string slices, byte arrays, and `Address` objects.
    /// The address field typically represents the contract address involved in the trace.
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
    /// use hypersync_net_types::TraceFilter;
    ///
    /// // Filter by a single contract address
    /// let filter = TraceFilter::any()
    ///     .and_address(["0xa0b86a33e6c11c8c0c5c0b5e6adee30d1a234567"])?;
    ///
    /// // Filter by multiple contract addresses
    /// let filter = TraceFilter::any()
    ///     .and_address([
    ///         "0xa0b86a33e6c11c8c0c5c0b5e6adee30d1a234567",
    ///         "0xdac17f958d2ee523a2206206994597c13d831ec7",
    ///     ])?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn and_address<I, A>(mut self, addresses: I) -> anyhow::Result<Self>
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

    /// Filter traces by any of the provided call types.
    ///
    /// This method accepts any iterable of values that can be converted to `String`.
    /// Common call types include "call", "staticcall", "delegatecall", "create", "create2", etc.
    ///
    /// # Arguments
    /// * `call_types` - An iterable of call type strings to filter by
    ///
    /// # Examples
    ///
    /// ```
    /// use hypersync_net_types::TraceFilter;
    ///
    /// // Filter by specific call types
    /// let filter = TraceFilter::any()
    ///     .and_call_type(["call", "delegatecall"]);
    ///
    /// // Filter by contract creation traces
    /// let filter = TraceFilter::any()
    ///     .and_call_type(["create", "create2"]);
    ///
    /// // Chain with address filtering
    /// let filter = TraceFilter::any()
    ///     .and_from_address(["0xa0b86a33e6c11c8c0c5c0b5e6adee30d1a234567"])?
    ///     .and_call_type(["call"]);
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn and_call_type<I, S>(mut self, call_types: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.call_type = call_types.into_iter().map(Into::into).collect();
        self
    }

    /// Filter traces by any of the provided reward types.
    ///
    /// This method accepts any iterable of values that can be converted to `String`.
    /// Common reward types include "block", "uncle", etc., typically used for mining rewards.
    ///
    /// # Arguments
    /// * `reward_types` - An iterable of reward type strings to filter by
    ///
    /// # Examples
    ///
    /// ```
    /// use hypersync_net_types::TraceFilter;
    ///
    /// // Filter by block rewards
    /// let filter = TraceFilter::any()
    ///     .and_reward_type(["block"]);
    ///
    /// // Filter by both block and uncle rewards
    /// let filter = TraceFilter::any()
    ///     .and_reward_type(["block", "uncle"]);
    /// ```
    pub fn and_reward_type<I, S>(mut self, reward_types: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.reward_type = reward_types.into_iter().map(Into::into).collect();
        self
    }

    /// Filter traces by any of the provided trace types.
    ///
    /// This method accepts any iterable of values that can be converted to `String`.
    /// Common trace types include "call", "create", "suicide", "reward", etc.
    ///
    /// # Arguments
    /// * `types` - An iterable of trace type strings to filter by
    ///
    /// # Examples
    ///
    /// ```
    /// use hypersync_net_types::TraceFilter;
    ///
    /// // Filter by call traces
    /// let filter = TraceFilter::any()
    ///     .and_type(["call"]);
    ///
    /// // Filter by multiple trace types
    /// let filter = TraceFilter::any()
    ///     .and_type(["call", "create", "reward"]);
    /// ```
    pub fn and_type<I, S>(mut self, types: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.type_ = types.into_iter().map(Into::into).collect();
        self
    }

    /// Filter traces by any of the provided function signature hashes (sighashes).
    ///
    /// This method accepts any iterable of values that can be converted to `Sighash`.
    /// Common input types include string slices, byte arrays, and `Sighash` objects.
    /// Sighashes are the first 4 bytes of the keccak256 hash of a function signature.
    ///
    /// # Arguments
    /// * `sighashes` - An iterable of sighash values to filter by
    ///
    /// # Returns
    /// * `Ok(Self)` - The updated filter on success
    /// * `Err(anyhow::Error)` - If any sighash fails to convert
    ///
    /// # Examples
    ///
    /// ```
    /// use hypersync_net_types::TraceFilter;
    ///
    /// // Filter by transfer function signature
    /// let transfer_sig = "0xa9059cbb"; // transfer(address,uint256)
    /// let filter = TraceFilter::any()
    ///     .and_sighash([transfer_sig])?;
    ///
    /// // Filter by multiple function signatures
    /// let filter = TraceFilter::any()
    ///     .and_sighash([
    ///         "0xa9059cbb", // transfer(address,uint256)
    ///         "0x095ea7b3", // approve(address,uint256)
    ///     ])?;
    ///
    /// // Using byte arrays
    /// let transfer_bytes = [0xa9, 0x05, 0x9c, 0xbb];
    /// let filter = TraceFilter::any()
    ///     .and_sighash([transfer_bytes])?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn and_sighash<I, S>(mut self, sighashes: I) -> anyhow::Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: TryInto<Sighash>,
        S::Error: std::error::Error + Send + Sync + 'static,
    {
        let mut converted_sighashes: Vec<Sighash> = Vec::new();
        for (idx, sighash) in sighashes.into_iter().enumerate() {
            converted_sighashes.push(
                sighash
                    .try_into()
                    .with_context(|| format!("invalid sighash at position {idx}"))?,
            );
        }
        self.sighash = converted_sighashes;
        Ok(self)
    }
}

impl CapnpBuilder<hypersync_net_types_capnp::trace_filter::Owned> for TraceFilter {
    fn populate_builder(
        &self,
        builder: &mut hypersync_net_types_capnp::trace_filter::Builder,
    ) -> Result<(), capnp::Error> {
        // Set from addresses
        if !self.from.is_empty() {
            let mut from_list = builder.reborrow().init_from(self.from.len() as u32);
            for (i, addr) in self.from.iter().enumerate() {
                from_list.set(i as u32, addr.as_slice());
            }
        }

        // Set from filter
        if let Some(filter) = &self.from_filter {
            builder.reborrow().set_from_filter(filter.0.as_bytes());
        }

        // Set to addresses
        if !self.to.is_empty() {
            let mut to_list = builder.reborrow().init_to(self.to.len() as u32);
            for (i, addr) in self.to.iter().enumerate() {
                to_list.set(i as u32, addr.as_slice());
            }
        }

        // Set to filter
        if let Some(filter) = &self.to_filter {
            builder.reborrow().set_to_filter(filter.0.as_bytes());
        }

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

        // Set call types
        if !self.call_type.is_empty() {
            let mut call_type_list = builder
                .reborrow()
                .init_call_type(self.call_type.len() as u32);
            for (i, call_type) in self.call_type.iter().enumerate() {
                call_type_list.set(i as u32, call_type);
            }
        }

        // Set reward types
        if !self.reward_type.is_empty() {
            let mut reward_type_list = builder
                .reborrow()
                .init_reward_type(self.reward_type.len() as u32);
            for (i, reward_type) in self.reward_type.iter().enumerate() {
                reward_type_list.set(i as u32, reward_type);
            }
        }

        // Set types
        if !self.type_.is_empty() {
            let mut type_list = builder.reborrow().init_type(self.type_.len() as u32);
            for (i, type_) in self.type_.iter().enumerate() {
                type_list.set(i as u32, type_);
            }
        }

        // Set sighash
        if !self.sighash.is_empty() {
            let mut sighash_list = builder.reborrow().init_sighash(self.sighash.len() as u32);
            for (i, sighash) in self.sighash.iter().enumerate() {
                sighash_list.set(i as u32, sighash.as_slice());
            }
        }

        Ok(())
    }
}

impl CapnpReader<hypersync_net_types_capnp::trace_filter::Owned> for TraceFilter {
    /// Deserialize TraceSelection from Cap'n Proto reader
    fn from_reader(
        reader: hypersync_net_types_capnp::trace_filter::Reader,
    ) -> Result<Self, capnp::Error> {
        let mut from = Vec::new();

        // Parse from addresses
        if reader.has_from() {
            let from_list = reader.get_from()?;
            for i in 0..from_list.len() {
                let addr_data = from_list.get(i)?;
                if addr_data.len() == 20 {
                    let mut addr_bytes = [0u8; 20];
                    addr_bytes.copy_from_slice(addr_data);
                    from.push(Address::from(addr_bytes));
                }
            }
        }

        let mut from_filter = None;

        // Parse from filter
        if reader.has_from_filter() {
            let filter_data = reader.get_from_filter()?;
            let Ok(wrapper) = FilterWrapper::from_bytes(filter_data) else {
                return Err(capnp::Error::failed("Invalid from filter".to_string()));
            };
            from_filter = Some(wrapper);
        }

        let mut to = Vec::new();

        // Parse to addresses
        if reader.has_to() {
            let to_list = reader.get_to()?;
            for i in 0..to_list.len() {
                let addr_data = to_list.get(i)?;
                if addr_data.len() == 20 {
                    let mut addr_bytes = [0u8; 20];
                    addr_bytes.copy_from_slice(addr_data);
                    to.push(Address::from(addr_bytes));
                }
            }
        }

        let mut to_filter = None;

        // Parse to filter
        if reader.has_to_filter() {
            let filter_data = reader.get_to_filter()?;
            let Ok(wrapper) = FilterWrapper::from_bytes(filter_data) else {
                return Err(capnp::Error::failed("Invalid to filter".to_string()));
            };
            to_filter = Some(wrapper);
        }

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
            let Ok(wrapper) = FilterWrapper::from_bytes(filter_data) else {
                return Err(capnp::Error::failed("Invalid address filter".to_string()));
            };
            address_filter = Some(wrapper);
        }

        let mut call_type = Vec::new();

        // Parse call types
        if reader.has_call_type() {
            let call_type_list = reader.get_call_type()?;
            for i in 0..call_type_list.len() {
                let call_type_val = call_type_list.get(i)?;
                call_type.push(call_type_val.to_string()?);
            }
        }

        let mut reward_type = Vec::new();
        // Parse reward types
        if reader.has_reward_type() {
            let reward_type_list = reader.get_reward_type()?;
            for i in 0..reward_type_list.len() {
                let reward_type_val = reward_type_list.get(i)?;
                reward_type.push(reward_type_val.to_string()?);
            }
        }

        let mut type_ = Vec::new();

        // Parse types
        if reader.has_type() {
            let type_list = reader.get_type()?;
            for i in 0..type_list.len() {
                let type_val = type_list.get(i)?;
                type_.push(type_val.to_string()?);
            }
        }

        let mut sighash = Vec::new();

        // Parse sighash
        if reader.has_sighash() {
            let sighash_list = reader.get_sighash()?;
            for i in 0..sighash_list.len() {
                let sighash_data = sighash_list.get(i)?;
                if sighash_data.len() == 4 {
                    let mut sighash_bytes = [0u8; 4];
                    sighash_bytes.copy_from_slice(sighash_data);
                    sighash.push(Sighash::from(sighash_bytes));
                }
            }
        }

        Ok(Self {
            from,
            from_filter,
            to,
            to_filter,
            address,
            address_filter,
            call_type,
            reward_type,
            type_,
            sighash,
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

impl Ord for TraceField {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_ref().cmp(other.as_ref())
    }
}

impl PartialOrd for TraceField {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl TraceField {
    pub fn all() -> std::collections::BTreeSet<Self> {
        use strum::IntoEnumIterator;
        Self::iter().collect()
    }

    /// Convert TraceField to Cap'n Proto enum
    pub fn to_capnp(&self) -> crate::hypersync_net_types_capnp::TraceField {
        match self {
            TraceField::TransactionHash => {
                crate::hypersync_net_types_capnp::TraceField::TransactionHash
            }
            TraceField::BlockHash => crate::hypersync_net_types_capnp::TraceField::BlockHash,
            TraceField::BlockNumber => crate::hypersync_net_types_capnp::TraceField::BlockNumber,
            TraceField::TransactionPosition => {
                crate::hypersync_net_types_capnp::TraceField::TransactionPosition
            }
            TraceField::Type => crate::hypersync_net_types_capnp::TraceField::Type,
            TraceField::Error => crate::hypersync_net_types_capnp::TraceField::Error,
            TraceField::From => crate::hypersync_net_types_capnp::TraceField::From,
            TraceField::To => crate::hypersync_net_types_capnp::TraceField::To,
            TraceField::Author => crate::hypersync_net_types_capnp::TraceField::Author,
            TraceField::Gas => crate::hypersync_net_types_capnp::TraceField::Gas,
            TraceField::GasUsed => crate::hypersync_net_types_capnp::TraceField::GasUsed,
            TraceField::ActionAddress => {
                crate::hypersync_net_types_capnp::TraceField::ActionAddress
            }
            TraceField::Address => crate::hypersync_net_types_capnp::TraceField::Address,
            TraceField::Balance => crate::hypersync_net_types_capnp::TraceField::Balance,
            TraceField::CallType => crate::hypersync_net_types_capnp::TraceField::CallType,
            TraceField::Code => crate::hypersync_net_types_capnp::TraceField::Code,
            TraceField::Init => crate::hypersync_net_types_capnp::TraceField::Init,
            TraceField::Input => crate::hypersync_net_types_capnp::TraceField::Input,
            TraceField::Output => crate::hypersync_net_types_capnp::TraceField::Output,
            TraceField::RefundAddress => {
                crate::hypersync_net_types_capnp::TraceField::RefundAddress
            }
            TraceField::RewardType => crate::hypersync_net_types_capnp::TraceField::RewardType,
            TraceField::Sighash => crate::hypersync_net_types_capnp::TraceField::Sighash,
            TraceField::Subtraces => crate::hypersync_net_types_capnp::TraceField::Subtraces,
            TraceField::TraceAddress => crate::hypersync_net_types_capnp::TraceField::TraceAddress,
            TraceField::Value => crate::hypersync_net_types_capnp::TraceField::Value,
        }
    }

    /// Convert Cap'n Proto enum to TraceField
    pub fn from_capnp(field: crate::hypersync_net_types_capnp::TraceField) -> Self {
        match field {
            crate::hypersync_net_types_capnp::TraceField::TransactionHash => {
                TraceField::TransactionHash
            }
            crate::hypersync_net_types_capnp::TraceField::BlockHash => TraceField::BlockHash,
            crate::hypersync_net_types_capnp::TraceField::BlockNumber => TraceField::BlockNumber,
            crate::hypersync_net_types_capnp::TraceField::TransactionPosition => {
                TraceField::TransactionPosition
            }
            crate::hypersync_net_types_capnp::TraceField::Type => TraceField::Type,
            crate::hypersync_net_types_capnp::TraceField::Error => TraceField::Error,
            crate::hypersync_net_types_capnp::TraceField::From => TraceField::From,
            crate::hypersync_net_types_capnp::TraceField::To => TraceField::To,
            crate::hypersync_net_types_capnp::TraceField::Author => TraceField::Author,
            crate::hypersync_net_types_capnp::TraceField::Gas => TraceField::Gas,
            crate::hypersync_net_types_capnp::TraceField::GasUsed => TraceField::GasUsed,
            crate::hypersync_net_types_capnp::TraceField::ActionAddress => {
                TraceField::ActionAddress
            }
            crate::hypersync_net_types_capnp::TraceField::Address => TraceField::Address,
            crate::hypersync_net_types_capnp::TraceField::Balance => TraceField::Balance,
            crate::hypersync_net_types_capnp::TraceField::CallType => TraceField::CallType,
            crate::hypersync_net_types_capnp::TraceField::Code => TraceField::Code,
            crate::hypersync_net_types_capnp::TraceField::Init => TraceField::Init,
            crate::hypersync_net_types_capnp::TraceField::Input => TraceField::Input,
            crate::hypersync_net_types_capnp::TraceField::Output => TraceField::Output,
            crate::hypersync_net_types_capnp::TraceField::RefundAddress => {
                TraceField::RefundAddress
            }
            crate::hypersync_net_types_capnp::TraceField::RewardType => TraceField::RewardType,
            crate::hypersync_net_types_capnp::TraceField::Sighash => TraceField::Sighash,
            crate::hypersync_net_types_capnp::TraceField::Subtraces => TraceField::Subtraces,
            crate::hypersync_net_types_capnp::TraceField::TraceAddress => TraceField::TraceAddress,
            crate::hypersync_net_types_capnp::TraceField::Value => TraceField::Value,
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
        let schema = hypersync_schema::trace();
        let schema_fields = schema
            .fields
            .iter()
            .map(|f| f.name.clone())
            .collect::<std::collections::BTreeSet<_>>();
        let all_fields = TraceField::all()
            .into_iter()
            .map(|f| f.as_ref().to_string())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(schema_fields, all_fields);
    }

    #[test]
    fn test_serde_matches_strum() {
        for field in TraceField::all() {
            let serialized = serde_json::to_string(&field).unwrap();
            let strum = serde_json::to_string(&field.as_ref()).unwrap();
            assert_eq!(serialized, strum, "strum value should be the same as serde");
        }
    }

    #[test]
    fn test_trace_filter_serde_with_defaults() {
        let trace_filter = TraceSelection::default();
        let field_selection = FieldSelection {
            trace: TraceField::all(),
            ..Default::default()
        };
        let query = Query {
            traces: vec![trace_filter],
            field_selection,
            ..Default::default()
        };

        test_query_serde(query, "trace selection with defaults");
    }

    #[test]
    fn test_trace_filter_serde_with_full_values() {
        let trace_filter = TraceFilter {
            from: vec![Address::decode_hex("0xdadB0d80178819F2319190D340ce9A924f783711").unwrap()],
            from_filter: Some(FilterWrapper::new(16, 1)),
            to: vec![Address::decode_hex("0x742d35Cc6634C0532925a3b8D4C9db96C4b4d8b6").unwrap()],
            to_filter: Some(FilterWrapper::new(16, 1)),
            address: vec![
                Address::decode_hex("0x1234567890123456789012345678901234567890").unwrap(),
            ],
            address_filter: Some(FilterWrapper::new(16, 1)),
            call_type: vec!["call".to_string(), "create".to_string()],
            reward_type: vec!["block".to_string(), "uncle".to_string()],
            type_: vec!["call".to_string(), "create".to_string()],
            sighash: vec![Sighash::from([0x12, 0x34, 0x56, 0x78])],
        };
        let field_selection = FieldSelection {
            trace: TraceField::all(),
            ..Default::default()
        };
        let query = Query {
            traces: vec![trace_filter.into()],
            field_selection,
            ..Default::default()
        };

        test_query_serde(query, "trace selection with full values");
    }
}
