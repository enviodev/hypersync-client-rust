use crate::{hypersync_net_types_capnp, types::Sighash, CapnpBuilder, CapnpReader, Selection};
use hypersync_format::{Address, FilterWrapper, Hash};
use serde::{Deserialize, Serialize};

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct AuthorizationSelection {
    /// List of chain ids to match in the transaction authorizationList
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub chain_id: Vec<u64>,
    /// List of addresses to match in the transaction authorizationList
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub address: Vec<Address>,
}
pub type TransactionSelection = Selection<TransactionFilter>;

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct TransactionFilter {
    /// Address the transaction should originate from. If transaction.from matches any of these, the transaction
    /// will be returned. Keep in mind that this has an and relationship with to filter, so each transaction should
    /// match both of them. Empty means match all.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub from: Vec<Address>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_filter: Option<FilterWrapper>,
    /// Address the transaction should go to. If transaction.to matches any of these, the transaction will
    /// be returned. Keep in mind that this has an and relationship with from filter, so each transaction should
    /// match both of them. Empty means match all.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub to: Vec<Address>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to_filter: Option<FilterWrapper>,
    /// If first 4 bytes of transaction input matches any of these, transaction will be returned. Empty means match all.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sighash: Vec<Sighash>,
    /// If transaction.status matches this value, the transaction will be returned.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<u8>,
    /// If transaction.type matches any of these values, the transaction will be returned
    #[serde(rename = "type")]
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub type_: Vec<u8>,
    /// If transaction.contract_address matches any of these values, the transaction will be returned.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub contract_address: Vec<Address>,
    /// Bloom filter to filter by transaction.contract_address field. If the bloom filter contains the hash
    /// of transaction.contract_address then the transaction will be returned. This field doesn't utilize the server side filtering
    /// so it should be used alongside some non-probabilistic filters if possible.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contract_address_filter: Option<FilterWrapper>,
    /// If transaction.hash matches any of these values the transaction will be returned.
    /// empty means match all.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hash: Vec<Hash>,

    /// List of authorizations from eip-7702 transactions, the query will return transactions that match any of these selections
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub authorization_list: Vec<AuthorizationSelection>,
}

impl CapnpBuilder<hypersync_net_types_capnp::authorization_selection::Owned>
    for AuthorizationSelection
{
    fn populate_builder(
        &self,
        builder: &mut hypersync_net_types_capnp::authorization_selection::Builder,
    ) -> Result<(), capnp::Error> {
        // Set chain ids
        if !self.chain_id.is_empty() {
            let mut chain_list = builder.reborrow().init_chain_id(self.chain_id.len() as u32);
            for (i, chain_id) in self.chain_id.iter().enumerate() {
                chain_list.set(i as u32, *chain_id);
            }
        }

        // Set addresses
        if !self.address.is_empty() {
            let mut addr_list = builder.reborrow().init_address(self.address.len() as u32);
            for (i, addr) in self.address.iter().enumerate() {
                addr_list.set(i as u32, addr.as_slice());
            }
        }

        Ok(())
    }
}

impl CapnpReader<hypersync_net_types_capnp::authorization_selection::Owned>
    for AuthorizationSelection
{
    /// Deserialize AuthorizationSelection from Cap'n Proto reader
    fn from_reader(
        reader: hypersync_net_types_capnp::authorization_selection::Reader,
    ) -> Result<Self, capnp::Error> {
        let mut auth_selection = AuthorizationSelection::default();

        // Parse chain ids
        if reader.has_chain_id() {
            let chain_list = reader.get_chain_id()?;
            for i in 0..chain_list.len() {
                auth_selection.chain_id.push(chain_list.get(i));
            }
        }

        // Parse addresses
        if reader.has_address() {
            let addr_list = reader.get_address()?;
            for i in 0..addr_list.len() {
                let addr_data = addr_list.get(i)?;
                if addr_data.len() == 20 {
                    let mut addr_bytes = [0u8; 20];
                    addr_bytes.copy_from_slice(addr_data);
                    auth_selection.address.push(Address::from(addr_bytes));
                }
            }
        }

        Ok(auth_selection)
    }
}

impl CapnpBuilder<hypersync_net_types_capnp::transaction_filter::Owned> for TransactionFilter {
    fn populate_builder(
        &self,
        builder: &mut hypersync_net_types_capnp::transaction_filter::Builder,
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

        // Set sighash
        if !self.sighash.is_empty() {
            let mut sighash_list = builder.reborrow().init_sighash(self.sighash.len() as u32);
            for (i, sighash) in self.sighash.iter().enumerate() {
                sighash_list.set(i as u32, sighash.as_slice());
            }
        }

        // Set status
        if let Some(status) = self.status {
            let mut status_builder = builder.reborrow().init_status();
            status_builder.set_value(status);
        }

        // Set type
        if !self.type_.is_empty() {
            let mut type_list = builder.reborrow().init_type(self.type_.len() as u32);
            for (i, type_) in self.type_.iter().enumerate() {
                type_list.set(i as u32, *type_);
            }
        }

        // Set contract addresses
        if !self.contract_address.is_empty() {
            let mut contract_list = builder
                .reborrow()
                .init_contract_address(self.contract_address.len() as u32);
            for (i, addr) in self.contract_address.iter().enumerate() {
                contract_list.set(i as u32, addr.as_slice());
            }
        }

        // Set contract address filter
        if let Some(filter) = &self.contract_address_filter {
            builder
                .reborrow()
                .set_contract_address_filter(filter.0.as_bytes());
        }

        // Set hashes
        if !self.hash.is_empty() {
            let mut hash_list = builder.reborrow().init_hash(self.hash.len() as u32);
            for (i, hash) in self.hash.iter().enumerate() {
                hash_list.set(i as u32, hash.as_slice());
            }
        }

        // Set authorization list
        if !self.authorization_list.is_empty() {
            let mut auth_list = builder
                .reborrow()
                .init_authorization_list(self.authorization_list.len() as u32);
            for (i, auth_sel) in self.authorization_list.iter().enumerate() {
                let mut auth_builder = auth_list.reborrow().get(i as u32);
                AuthorizationSelection::populate_builder(auth_sel, &mut auth_builder)?;
            }
        }

        Ok(())
    }
}

impl CapnpReader<hypersync_net_types_capnp::transaction_filter::Owned> for TransactionFilter {
    /// Deserialize TransactionSelection from Cap'n Proto reader
    fn from_reader(
        reader: hypersync_net_types_capnp::transaction_filter::Reader,
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
            // For now, skip filter deserialization - this would need proper Filter construction
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

        // Parse status
        let mut status = None;
        if reader.has_status() {
            let status_reader = reader.get_status()?;
            status = Some(status_reader.get_value());
        }

        let mut type_ = Vec::new();

        // Parse type
        if reader.has_type() {
            let type_list = reader.get_type()?;
            for i in 0..type_list.len() {
                type_.push(type_list.get(i));
            }
        }

        let mut contract_address = Vec::new();
        // Parse contract addresses
        if reader.has_contract_address() {
            let contract_list = reader.get_contract_address()?;
            for i in 0..contract_list.len() {
                let addr_data = contract_list.get(i)?;
                if addr_data.len() == 20 {
                    let mut addr_bytes = [0u8; 20];
                    addr_bytes.copy_from_slice(addr_data);
                    contract_address.push(Address::from(addr_bytes));
                }
            }
        }

        let mut contract_address_filter = None;

        // Parse contract address filter
        if reader.has_contract_address_filter() {
            let filter_data = reader.get_contract_address_filter()?;
            let Ok(wrapper) = FilterWrapper::from_bytes(filter_data) else {
                return Err(capnp::Error::failed(
                    "Invalid contract address filter".to_string(),
                ));
            };
            contract_address_filter = Some(wrapper);
        }

        let mut hash = Vec::new();

        // Parse hashes
        if reader.has_hash() {
            let hash_list = reader.get_hash()?;
            for i in 0..hash_list.len() {
                let hash_data = hash_list.get(i)?;
                if hash_data.len() == 32 {
                    let mut hash_bytes = [0u8; 32];
                    hash_bytes.copy_from_slice(hash_data);
                    hash.push(Hash::from(hash_bytes));
                }
            }
        }

        let mut authorization_list = Vec::new();

        // Parse authorization list
        if reader.has_authorization_list() {
            let auth_list = reader.get_authorization_list()?;
            for i in 0..auth_list.len() {
                let auth_reader = auth_list.get(i);
                let auth_selection = AuthorizationSelection::from_reader(auth_reader)?;
                authorization_list.push(auth_selection);
            }
        }

        Ok(Self {
            from,
            from_filter,
            to,
            to_filter,
            sighash,
            status,
            type_,
            contract_address,
            contract_address_filter,
            hash,
            authorization_list,
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
pub enum TransactionField {
    // Non-nullable fields (required)
    BlockHash,
    BlockNumber,
    Gas,
    Hash,
    Input,
    Nonce,
    TransactionIndex,
    Value,
    CumulativeGasUsed,
    EffectiveGasPrice,
    GasUsed,
    LogsBloom,

    // Nullable fields (optional)
    From,
    GasPrice,
    To,
    V,
    R,
    S,
    MaxPriorityFeePerGas,
    MaxFeePerGas,
    ChainId,
    ContractAddress,
    Type,
    Root,
    Status,
    YParity,
    AccessList,
    AuthorizationList,
    L1Fee,
    L1GasPrice,
    L1GasUsed,
    L1FeeScalar,
    GasUsedForL1,
    MaxFeePerBlobGas,
    BlobVersionedHashes,
    BlobGasPrice,
    BlobGasUsed,
    DepositNonce,
    DepositReceiptVersion,
    L1BaseFeeScalar,
    L1BlobBaseFee,
    L1BlobBaseFeeScalar,
    L1BlockNumber,
    Mint,
    Sighash,
    SourceHash,
}

impl Ord for TransactionField {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_ref().cmp(other.as_ref())
    }
}

impl PartialOrd for TransactionField {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl TransactionField {
    pub fn all() -> std::collections::BTreeSet<Self> {
        use strum::IntoEnumIterator;
        Self::iter().collect()
    }

    /// Convert TransactionField to Cap'n Proto enum
    pub fn to_capnp(&self) -> crate::hypersync_net_types_capnp::TransactionField {
        match self {
            TransactionField::BlockHash => {
                crate::hypersync_net_types_capnp::TransactionField::BlockHash
            }
            TransactionField::BlockNumber => {
                crate::hypersync_net_types_capnp::TransactionField::BlockNumber
            }
            TransactionField::Gas => crate::hypersync_net_types_capnp::TransactionField::Gas,
            TransactionField::Hash => crate::hypersync_net_types_capnp::TransactionField::Hash,
            TransactionField::Input => crate::hypersync_net_types_capnp::TransactionField::Input,
            TransactionField::Nonce => crate::hypersync_net_types_capnp::TransactionField::Nonce,
            TransactionField::TransactionIndex => {
                crate::hypersync_net_types_capnp::TransactionField::TransactionIndex
            }
            TransactionField::Value => crate::hypersync_net_types_capnp::TransactionField::Value,
            TransactionField::CumulativeGasUsed => {
                crate::hypersync_net_types_capnp::TransactionField::CumulativeGasUsed
            }
            TransactionField::EffectiveGasPrice => {
                crate::hypersync_net_types_capnp::TransactionField::EffectiveGasPrice
            }
            TransactionField::GasUsed => {
                crate::hypersync_net_types_capnp::TransactionField::GasUsed
            }
            TransactionField::LogsBloom => {
                crate::hypersync_net_types_capnp::TransactionField::LogsBloom
            }
            TransactionField::From => crate::hypersync_net_types_capnp::TransactionField::From,
            TransactionField::GasPrice => {
                crate::hypersync_net_types_capnp::TransactionField::GasPrice
            }
            TransactionField::To => crate::hypersync_net_types_capnp::TransactionField::To,
            TransactionField::V => crate::hypersync_net_types_capnp::TransactionField::V,
            TransactionField::R => crate::hypersync_net_types_capnp::TransactionField::R,
            TransactionField::S => crate::hypersync_net_types_capnp::TransactionField::S,
            TransactionField::MaxPriorityFeePerGas => {
                crate::hypersync_net_types_capnp::TransactionField::MaxPriorityFeePerGas
            }
            TransactionField::MaxFeePerGas => {
                crate::hypersync_net_types_capnp::TransactionField::MaxFeePerGas
            }
            TransactionField::ChainId => {
                crate::hypersync_net_types_capnp::TransactionField::ChainId
            }
            TransactionField::ContractAddress => {
                crate::hypersync_net_types_capnp::TransactionField::ContractAddress
            }
            TransactionField::Type => crate::hypersync_net_types_capnp::TransactionField::Type,
            TransactionField::Root => crate::hypersync_net_types_capnp::TransactionField::Root,
            TransactionField::Status => crate::hypersync_net_types_capnp::TransactionField::Status,
            TransactionField::YParity => {
                crate::hypersync_net_types_capnp::TransactionField::YParity
            }
            TransactionField::AccessList => {
                crate::hypersync_net_types_capnp::TransactionField::AccessList
            }
            TransactionField::AuthorizationList => {
                crate::hypersync_net_types_capnp::TransactionField::AuthorizationList
            }
            TransactionField::L1Fee => crate::hypersync_net_types_capnp::TransactionField::L1Fee,
            TransactionField::L1GasPrice => {
                crate::hypersync_net_types_capnp::TransactionField::L1GasPrice
            }
            TransactionField::L1GasUsed => {
                crate::hypersync_net_types_capnp::TransactionField::L1GasUsed
            }
            TransactionField::L1FeeScalar => {
                crate::hypersync_net_types_capnp::TransactionField::L1FeeScalar
            }
            TransactionField::GasUsedForL1 => {
                crate::hypersync_net_types_capnp::TransactionField::GasUsedForL1
            }
            TransactionField::MaxFeePerBlobGas => {
                crate::hypersync_net_types_capnp::TransactionField::MaxFeePerBlobGas
            }
            TransactionField::BlobVersionedHashes => {
                crate::hypersync_net_types_capnp::TransactionField::BlobVersionedHashes
            }
            TransactionField::BlobGasPrice => {
                crate::hypersync_net_types_capnp::TransactionField::BlobGasPrice
            }
            TransactionField::BlobGasUsed => {
                crate::hypersync_net_types_capnp::TransactionField::BlobGasUsed
            }
            TransactionField::DepositNonce => {
                crate::hypersync_net_types_capnp::TransactionField::DepositNonce
            }
            TransactionField::DepositReceiptVersion => {
                crate::hypersync_net_types_capnp::TransactionField::DepositReceiptVersion
            }
            TransactionField::L1BaseFeeScalar => {
                crate::hypersync_net_types_capnp::TransactionField::L1BaseFeeScalar
            }
            TransactionField::L1BlobBaseFee => {
                crate::hypersync_net_types_capnp::TransactionField::L1BlobBaseFee
            }
            TransactionField::L1BlobBaseFeeScalar => {
                crate::hypersync_net_types_capnp::TransactionField::L1BlobBaseFeeScalar
            }
            TransactionField::L1BlockNumber => {
                crate::hypersync_net_types_capnp::TransactionField::L1BlockNumber
            }
            TransactionField::Mint => crate::hypersync_net_types_capnp::TransactionField::Mint,
            TransactionField::Sighash => {
                crate::hypersync_net_types_capnp::TransactionField::Sighash
            }
            TransactionField::SourceHash => {
                crate::hypersync_net_types_capnp::TransactionField::SourceHash
            }
        }
    }

    /// Convert Cap'n Proto enum to TransactionField
    pub fn from_capnp(field: crate::hypersync_net_types_capnp::TransactionField) -> Self {
        match field {
            crate::hypersync_net_types_capnp::TransactionField::BlockHash => {
                TransactionField::BlockHash
            }
            crate::hypersync_net_types_capnp::TransactionField::BlockNumber => {
                TransactionField::BlockNumber
            }
            crate::hypersync_net_types_capnp::TransactionField::Gas => TransactionField::Gas,
            crate::hypersync_net_types_capnp::TransactionField::Hash => TransactionField::Hash,
            crate::hypersync_net_types_capnp::TransactionField::Input => TransactionField::Input,
            crate::hypersync_net_types_capnp::TransactionField::Nonce => TransactionField::Nonce,
            crate::hypersync_net_types_capnp::TransactionField::TransactionIndex => {
                TransactionField::TransactionIndex
            }
            crate::hypersync_net_types_capnp::TransactionField::Value => TransactionField::Value,
            crate::hypersync_net_types_capnp::TransactionField::CumulativeGasUsed => {
                TransactionField::CumulativeGasUsed
            }
            crate::hypersync_net_types_capnp::TransactionField::EffectiveGasPrice => {
                TransactionField::EffectiveGasPrice
            }
            crate::hypersync_net_types_capnp::TransactionField::GasUsed => {
                TransactionField::GasUsed
            }
            crate::hypersync_net_types_capnp::TransactionField::LogsBloom => {
                TransactionField::LogsBloom
            }
            crate::hypersync_net_types_capnp::TransactionField::From => TransactionField::From,
            crate::hypersync_net_types_capnp::TransactionField::GasPrice => {
                TransactionField::GasPrice
            }
            crate::hypersync_net_types_capnp::TransactionField::To => TransactionField::To,
            crate::hypersync_net_types_capnp::TransactionField::V => TransactionField::V,
            crate::hypersync_net_types_capnp::TransactionField::R => TransactionField::R,
            crate::hypersync_net_types_capnp::TransactionField::S => TransactionField::S,
            crate::hypersync_net_types_capnp::TransactionField::MaxPriorityFeePerGas => {
                TransactionField::MaxPriorityFeePerGas
            }
            crate::hypersync_net_types_capnp::TransactionField::MaxFeePerGas => {
                TransactionField::MaxFeePerGas
            }
            crate::hypersync_net_types_capnp::TransactionField::ChainId => {
                TransactionField::ChainId
            }
            crate::hypersync_net_types_capnp::TransactionField::ContractAddress => {
                TransactionField::ContractAddress
            }
            crate::hypersync_net_types_capnp::TransactionField::Type => TransactionField::Type,
            crate::hypersync_net_types_capnp::TransactionField::Root => TransactionField::Root,
            crate::hypersync_net_types_capnp::TransactionField::Status => TransactionField::Status,
            crate::hypersync_net_types_capnp::TransactionField::YParity => {
                TransactionField::YParity
            }
            crate::hypersync_net_types_capnp::TransactionField::AccessList => {
                TransactionField::AccessList
            }
            crate::hypersync_net_types_capnp::TransactionField::AuthorizationList => {
                TransactionField::AuthorizationList
            }
            crate::hypersync_net_types_capnp::TransactionField::L1Fee => TransactionField::L1Fee,
            crate::hypersync_net_types_capnp::TransactionField::L1GasPrice => {
                TransactionField::L1GasPrice
            }
            crate::hypersync_net_types_capnp::TransactionField::L1GasUsed => {
                TransactionField::L1GasUsed
            }
            crate::hypersync_net_types_capnp::TransactionField::L1FeeScalar => {
                TransactionField::L1FeeScalar
            }
            crate::hypersync_net_types_capnp::TransactionField::GasUsedForL1 => {
                TransactionField::GasUsedForL1
            }
            crate::hypersync_net_types_capnp::TransactionField::MaxFeePerBlobGas => {
                TransactionField::MaxFeePerBlobGas
            }
            crate::hypersync_net_types_capnp::TransactionField::BlobVersionedHashes => {
                TransactionField::BlobVersionedHashes
            }
            crate::hypersync_net_types_capnp::TransactionField::BlobGasPrice => {
                TransactionField::BlobGasPrice
            }
            crate::hypersync_net_types_capnp::TransactionField::BlobGasUsed => {
                TransactionField::BlobGasUsed
            }
            crate::hypersync_net_types_capnp::TransactionField::DepositNonce => {
                TransactionField::DepositNonce
            }
            crate::hypersync_net_types_capnp::TransactionField::DepositReceiptVersion => {
                TransactionField::DepositReceiptVersion
            }
            crate::hypersync_net_types_capnp::TransactionField::L1BaseFeeScalar => {
                TransactionField::L1BaseFeeScalar
            }
            crate::hypersync_net_types_capnp::TransactionField::L1BlobBaseFee => {
                TransactionField::L1BlobBaseFee
            }
            crate::hypersync_net_types_capnp::TransactionField::L1BlobBaseFeeScalar => {
                TransactionField::L1BlobBaseFeeScalar
            }
            crate::hypersync_net_types_capnp::TransactionField::L1BlockNumber => {
                TransactionField::L1BlockNumber
            }
            crate::hypersync_net_types_capnp::TransactionField::Mint => TransactionField::Mint,
            crate::hypersync_net_types_capnp::TransactionField::Sighash => {
                TransactionField::Sighash
            }
            crate::hypersync_net_types_capnp::TransactionField::SourceHash => {
                TransactionField::SourceHash
            }
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
        let schema = hypersync_schema::transaction();
        let schema_fields = schema
            .fields
            .iter()
            .map(|f| f.name.clone())
            .collect::<std::collections::BTreeSet<_>>();
        let all_fields = TransactionField::all()
            .into_iter()
            .map(|f| f.as_ref().to_string())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(schema_fields, all_fields);
    }

    #[test]
    fn test_serde_matches_strum() {
        for field in TransactionField::all() {
            let serialized = serde_json::to_string(&field).unwrap();
            let strum = serde_json::to_string(&field.as_ref()).unwrap();
            assert_eq!(serialized, strum, "strum value should be the same as serde");
        }
    }

    #[test]
    fn test_transaction_filter_serde_with_defaults() {
        let transaction_filter = TransactionSelection::default();
        let field_selection = FieldSelection {
            transaction: TransactionField::all(),
            ..Default::default()
        };
        let query = Query {
            transactions: vec![transaction_filter],
            field_selection,
            ..Default::default()
        };

        test_query_serde(query, "transaction selection with defaults");
    }
    #[test]
    fn test_transaction_filter_serde_with_explicit_defaults() {
        let transaction_filter = TransactionFilter {
            from: Vec::default(),
            from_filter: Some(FilterWrapper::new(16, 0)),
            to: Vec::default(),
            to_filter: Some(FilterWrapper::new(16, 0)),
            sighash: Vec::default(),
            status: Some(u8::default()),
            type_: Vec::default(),
            contract_address: Vec::default(),
            contract_address_filter: Some(FilterWrapper::new(16, 0)),
            hash: Vec::default(),
            authorization_list: Vec::default(),
        };
        let field_selection = FieldSelection {
            transaction: TransactionField::all(),
            ..Default::default()
        };
        let query = Query {
            transactions: vec![transaction_filter.into()],
            field_selection,
            ..Default::default()
        };

        test_query_serde(query, "transaction selection with explicit defaults");
    }

    #[test]
    fn test_transaction_filter_serde_with_full_values() {
        let transaction_filter = TransactionFilter {
            from: vec![Address::decode_hex("0xdadB0d80178819F2319190D340ce9A924f783711").unwrap()],
            from_filter: Some(FilterWrapper::new(16, 1)),
            to: vec![Address::decode_hex("0x742d35Cc6634C0532925a3b8D4C9db96C4b4d8b6").unwrap()],
            to_filter: Some(FilterWrapper::new(16, 1)),
            sighash: vec![Sighash::from([0x12, 0x34, 0x56, 0x78])],
            status: Some(1),
            type_: vec![2],
            contract_address: vec![Address::decode_hex(
                "0x1234567890123456789012345678901234567890",
            )
            .unwrap()],
            contract_address_filter: Some(FilterWrapper::new(16, 1)),
            hash: vec![Hash::decode_hex(
                "0x40d008f2a1653f09b7b028d30c7fd1ba7c84900fcfb032040b3eb3d16f84d294",
            )
            .unwrap()],
            authorization_list: Vec::default(),
        };
        let field_selection = FieldSelection {
            transaction: TransactionField::all(),
            ..Default::default()
        };
        let query = Query {
            transactions: vec![transaction_filter.into()],
            field_selection,
            ..Default::default()
        };

        test_query_serde(query, "transaction selection with full values");
    }

    #[test]
    fn test_authorization_selection_serde_with_values() {
        let auth_selection = AuthorizationSelection {
            chain_id: vec![1, 137, 42161],
            address: vec![
                Address::decode_hex("0xdadB0d80178819F2319190D340ce9A924f783711").unwrap(),
            ],
        };
        let transaction_filter = TransactionFilter {
            from: Vec::default(),
            from_filter: Some(FilterWrapper::new(16, 0)),
            to: Vec::default(),
            to_filter: Some(FilterWrapper::new(16, 0)),
            sighash: Vec::default(),
            status: Some(u8::default()),
            type_: Vec::default(),
            contract_address: Vec::default(),
            contract_address_filter: Some(FilterWrapper::new(16, 0)),
            hash: Vec::default(),
            authorization_list: vec![auth_selection],
        };
        let field_selection = FieldSelection {
            transaction: TransactionField::all(),
            ..Default::default()
        };
        let query = Query {
            transactions: vec![transaction_filter.into()],
            field_selection,
            ..Default::default()
        };

        test_query_serde(query, "authorization selection with rest defaults");
    }
}
