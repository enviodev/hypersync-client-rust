use crate::{hypersync_net_types_capnp, types::Sighash};
use hypersync_format::{Address, FilterWrapper, Hash};
use serde::{Deserialize, Serialize};

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct AuthorizationSelection {
    /// List of chain ids to match in the transaction authorizationList
    #[serde(default)]
    pub chain_id: Vec<u64>,
    /// List of addresses to match in the transaction authorizationList
    #[serde(default)]
    pub address: Vec<Address>,
}

#[derive(Default, Serialize, Deserialize, Clone, Debug)]
pub struct TransactionSelection {
    /// Address the transaction should originate from. If transaction.from matches any of these, the transaction
    /// will be returned. Keep in mind that this has an and relationship with to filter, so each transaction should
    /// match both of them. Empty means match all.
    #[serde(default)]
    pub from: Vec<Address>,
    #[serde(default)]
    pub from_filter: Option<FilterWrapper>,
    /// Address the transaction should go to. If transaction.to matches any of these, the transaction will
    /// be returned. Keep in mind that this has an and relationship with from filter, so each transaction should
    /// match both of them. Empty means match all.
    #[serde(default)]
    pub to: Vec<Address>,
    #[serde(default)]
    pub to_filter: Option<FilterWrapper>,
    /// If first 4 bytes of transaction input matches any of these, transaction will be returned. Empty means match all.
    #[serde(default)]
    pub sighash: Vec<Sighash>,
    /// If transaction.status matches this value, the transaction will be returned.
    pub status: Option<u8>,
    /// If transaction.type matches any of these values, the transaction will be returned
    #[serde(rename = "type")]
    #[serde(default)]
    pub kind: Vec<u8>,
    /// If transaction.contract_address matches any of these values, the transaction will be returned.
    #[serde(default)]
    pub contract_address: Vec<Address>,
    /// Bloom filter to filter by transaction.contract_address field. If the bloom filter contains the hash
    /// of transaction.contract_address then the transaction will be returned. This field doesn't utilize the server side filtering
    /// so it should be used alongside some non-probabilistic filters if possible.
    #[serde(default)]
    pub contract_address_filter: Option<FilterWrapper>,
    /// If transaction.hash matches any of these values the transaction will be returned.
    /// empty means match all.
    #[serde(default)]
    pub hash: Vec<Hash>,

    /// List of authorizations from eip-7702 transactions, the query will return transactions that match any of these selections
    #[serde(default)]
    pub authorization_list: Vec<AuthorizationSelection>,
}

impl AuthorizationSelection {
    pub(crate) fn populate_capnp_builder(
        auth_sel: &AuthorizationSelection,
        mut builder: hypersync_net_types_capnp::authorization_selection::Builder,
    ) -> Result<(), capnp::Error> {
        // Set chain ids
        {
            let mut chain_list = builder
                .reborrow()
                .init_chain_id(auth_sel.chain_id.len() as u32);
            for (i, chain_id) in auth_sel.chain_id.iter().enumerate() {
                chain_list.set(i as u32, *chain_id);
            }
        }

        // Set addresses
        {
            let mut addr_list = builder
                .reborrow()
                .init_address(auth_sel.address.len() as u32);
            for (i, addr) in auth_sel.address.iter().enumerate() {
                addr_list.set(i as u32, addr.as_slice());
            }
        }

        Ok(())
    }
}

impl TransactionSelection {
    pub(crate) fn populate_capnp_builder(
        tx_sel: &TransactionSelection,
        mut builder: hypersync_net_types_capnp::transaction_selection::Builder,
    ) -> Result<(), capnp::Error> {
        // Set from addresses
        {
            let mut from_list = builder.reborrow().init_from(tx_sel.from.len() as u32);
            for (i, addr) in tx_sel.from.iter().enumerate() {
                from_list.set(i as u32, addr.as_slice());
            }
        }

        // Set from filter
        if let Some(filter) = &tx_sel.from_filter {
            builder.reborrow().set_from_filter(filter.0.as_bytes());
        }

        // Set to addresses
        {
            let mut to_list = builder.reborrow().init_to(tx_sel.to.len() as u32);
            for (i, addr) in tx_sel.to.iter().enumerate() {
                to_list.set(i as u32, addr.as_slice());
            }
        }

        // Set to filter
        if let Some(filter) = &tx_sel.to_filter {
            builder.reborrow().set_to_filter(filter.0.as_bytes());
        }

        // Set sighash
        {
            let mut sighash_list = builder.reborrow().init_sighash(tx_sel.sighash.len() as u32);
            for (i, sighash) in tx_sel.sighash.iter().enumerate() {
                sighash_list.set(i as u32, sighash.as_slice());
            }
        }

        // Set status
        if let Some(status) = tx_sel.status {
            builder.reborrow().set_status(status);
        }

        // Set kind
        {
            let mut kind_list = builder.reborrow().init_kind(tx_sel.kind.len() as u32);
            for (i, kind) in tx_sel.kind.iter().enumerate() {
                kind_list.set(i as u32, *kind);
            }
        }

        // Set contract addresses
        {
            let mut contract_list = builder
                .reborrow()
                .init_contract_address(tx_sel.contract_address.len() as u32);
            for (i, addr) in tx_sel.contract_address.iter().enumerate() {
                contract_list.set(i as u32, addr.as_slice());
            }
        }

        // Set contract address filter
        if let Some(filter) = &tx_sel.contract_address_filter {
            builder
                .reborrow()
                .set_contract_address_filter(filter.0.as_bytes());
        }

        // Set hashes
        {
            let mut hash_list = builder.reborrow().init_hash(tx_sel.hash.len() as u32);
            for (i, hash) in tx_sel.hash.iter().enumerate() {
                hash_list.set(i as u32, hash.as_slice());
            }
        }

        // Set authorization list
        {
            let mut auth_list = builder
                .reborrow()
                .init_authorization_list(tx_sel.authorization_list.len() as u32);
            for (i, auth_sel) in tx_sel.authorization_list.iter().enumerate() {
                let auth_builder = auth_list.reborrow().get(i as u32);
                AuthorizationSelection::populate_capnp_builder(auth_sel, auth_builder)?;
            }
        }

        Ok(())
    }
}
