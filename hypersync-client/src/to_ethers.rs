//! This module implement specification for Provider generic from ethers.
use crate::simple_types::{Block, Log, Trace, Transaction};
use ethers;
use ethers::prelude::{CallResult, CreateResult};
use ethers::types::{
    transaction::eip2930::AccessList as EtherAccessList,
    transaction::eip2930::AccessListItem as EtherAccessListItem, Action, ActionType,
    Address as EtherAddress, Block as EtherBlock, Bloom, Bytes, Call, CallType, Create,
    Log as EtherLog, Res, Reward, RewardType, Suicide, Trace as EtherTrace,
    Transaction as EtherTransaction, TxHash, H256, U256,
};
use hypersync_format::{AccessList, Address, Data, Hash, Quantity};
use polars_arrow::array::ViewType;
use std::default::Default;
use std::fmt::{Display, Formatter};

/// Error happened during hypersync -> 3rd-party type conversion
#[derive(Debug, Clone)]
pub enum ConversionError {
    /// Value is missing. Make sure FieldSelection is correct.
    MissingValue(String),
    /// Conversation between types failed.
    ConvertError(String),
}

impl Display for ConversionError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            ConversionError::MissingValue(s) => write!(f, "Missing value: {}", s),
            ConversionError::ConvertError(s) => write!(f, "Conversion error: {}", s),
        }
    }
}
/// Trait for conversion between hypersync types to ethers types
pub trait TryIntoEthers {
    /// Error type during conversation. By default, `ConversionError`
    type Error;
    /// Converts hypersync Block + Vec<Transaction> to ethers Block
    fn try_into_ethers(
        self,
        txs: Vec<Transaction>,
    ) -> Result<EtherBlock<EtherTransaction>, Self::Error>;
    /// Converts hypersync Block + Vec<TxHash> to ethers Block
    fn try_into_ethers_hash(self, txs: Vec<Hash>) -> Result<EtherBlock<TxHash>, Self::Error>;
}

impl TryIntoEthers for Block {
    type Error = ConversionError;
    fn try_into_ethers(
        self,
        txs: Vec<Transaction>,
    ) -> Result<EtherBlock<EtherTransaction>, Self::Error> {
        Ok(EtherBlock {
            hash: self.hash.map(Into::into),
            parent_hash: self
                .parent_hash
                .ok_or(ConversionError::MissingValue(
                    "Field `parent_hash` is null".into(),
                ))?
                .into(),
            uncles_hash: self
                .sha3_uncles
                .ok_or(ConversionError::MissingValue(
                    "Field `sha3_uncles` is null".into(),
                ))?
                .into(),
            author: self.miner.map(Into::into),
            state_root: self
                .state_root
                .ok_or(ConversionError::MissingValue(
                    "Field `state_root` is null".into(),
                ))?
                .into(),
            transactions_root: self
                .transactions_root
                .ok_or(ConversionError::MissingValue(
                    "Field `transactions_root` is null".into(),
                ))?
                .into(),
            receipts_root: self
                .receipts_root
                .ok_or(ConversionError::MissingValue(
                    "Field `receipts_root` is null".into(),
                ))?
                .into(),
            number: self.number.map(Into::into),
            gas_used: self
                .gas_used
                .ok_or(ConversionError::MissingValue(
                    "Field `gas_used` is null".into(),
                ))?
                .try_into()
                .map_err(|_| {
                    ConversionError::ConvertError("Failed to convert field `gas_used`".into())
                })?,
            gas_limit: self
                .gas_limit
                .ok_or(ConversionError::MissingValue(
                    "Field `gas_limit` is null".into(),
                ))?
                .try_into()
                .map_err(|_| {
                    ConversionError::ConvertError("Failed to convert field `gas_limit`".into())
                })?,
            extra_data: Bytes::from_iter(
                self.extra_data
                    .ok_or(ConversionError::MissingValue(
                        "Field `extra_data` is null".into(),
                    ))?
                    .to_bytes(),
            ),
            logs_bloom: self
                .logs_bloom
                .map(|bloom| Bloom::from_slice(bloom.as_ref())),
            timestamp: self
                .timestamp
                .ok_or(ConversionError::MissingValue(
                    "Field `timestamp` is null".into(),
                ))?
                .try_into()
                .map_err(|_| {
                    ConversionError::ConvertError("Failed to convert field `timestamp`".into())
                })?,
            difficulty: self
                .difficulty
                .ok_or(ConversionError::MissingValue(
                    "Field `difficulty` is null".into(),
                ))?
                .try_into()
                .map_err(|_| {
                    ConversionError::ConvertError("Failed to convert field `difficulty`".into())
                })?,
            total_difficulty: self
                .total_difficulty
                .map(|total_difficulty| {
                    total_difficulty.try_into().map_err(|_| {
                        ConversionError::ConvertError(
                            "Failed to convert field `total_difficulty`".into(),
                        )
                    })
                })
                .transpose()?,
            // Hypersync does not support seal field
            seal_fields: Vec::new(),
            uncles: self
                .uncles
                .map(|uncles| uncles.into_iter().map(Into::into).collect())
                .ok_or(ConversionError::MissingValue(
                    "Field `uncles` is null".into(),
                ))?,
            transactions: txs
                .into_iter()
                .map(|tx| {
                    tx.try_into().map_err(|_| {
                        ConversionError::ConvertError(
                            "Failed to convert field `transactions`".into(),
                        )
                    })
                })
                .collect::<Result<_, ConversionError>>()?,
            size: self
                .size
                .map(|size| {
                    size.try_into().map_err(|_| {
                        ConversionError::ConvertError("Failed to convert field `size`".into())
                    })
                })
                .transpose()?,
            mix_hash: self.mix_hash.map(Into::into),
            nonce: self.nonce.map(Into::into),
            base_fee_per_gas: self
                .base_fee_per_gas
                .map(|base_fee_per_gas| {
                    base_fee_per_gas.try_into().map_err(|_| {
                        ConversionError::ConvertError(
                            "Failed to convert field `base_fee_per_gas`".into(),
                        )
                    })
                })
                .transpose()?,
            blob_gas_used: self
                .blob_gas_used
                .map(|blob_gas_used| {
                    blob_gas_used.try_into().map_err(|_| {
                        ConversionError::ConvertError(
                            "Failed to convert field `blob_gas_used`".into(),
                        )
                    })
                })
                .transpose()?,
            excess_blob_gas: self
                .excess_blob_gas
                .map(|excess_blob_gas| {
                    excess_blob_gas.try_into().map_err(|_| {
                        ConversionError::ConvertError(
                            "Failed to convert field `excess_blob_gas`".into(),
                        )
                    })
                })
                .transpose()?,
            withdrawals_root: self.withdrawals_root.map(Into::into),
            withdrawals: self.withdrawals.map(|withdrawals| {
                withdrawals
                    .into_iter()
                    .filter_map(|withdrawal| withdrawal.try_into().ok())
                    .collect()
            }),
            parent_beacon_block_root: self.parent_beacon_block_root.map(Into::into),
            other: Default::default(),
        })
    }

    fn try_into_ethers_hash(self, txs: Vec<Hash>) -> Result<EtherBlock<TxHash>, Self::Error> {
        Ok(EtherBlock {
            hash: self.hash.map(Into::into),
            parent_hash: self
                .parent_hash
                .ok_or(ConversionError::MissingValue(
                    "Field `parent_hash` is null".into(),
                ))?
                .into(),
            uncles_hash: self
                .sha3_uncles
                .ok_or(ConversionError::MissingValue(
                    "Field `sha3_uncles` is null".into(),
                ))?
                .into(),
            author: self.miner.map(Into::into),
            state_root: self
                .state_root
                .ok_or(ConversionError::MissingValue(
                    "Field `state_root` is null".into(),
                ))?
                .into(),
            transactions_root: self
                .transactions_root
                .ok_or(ConversionError::MissingValue(
                    "Field `transactions_root` is null".into(),
                ))?
                .into(),
            receipts_root: self
                .receipts_root
                .ok_or(ConversionError::MissingValue(
                    "Field `receipts_root` is null".into(),
                ))?
                .into(),
            number: self.number.map(Into::into),
            gas_used: self
                .gas_used
                .ok_or(ConversionError::MissingValue(
                    "Field `gas_used` is null".into(),
                ))?
                .try_into()
                .map_err(|_| {
                    ConversionError::ConvertError("Failed to convert field `gas_used`".into())
                })?,
            gas_limit: self
                .gas_limit
                .ok_or(ConversionError::MissingValue(
                    "Field `gas_limit` is null".into(),
                ))?
                .try_into()
                .map_err(|_| {
                    ConversionError::ConvertError("Failed to convert field `gas_limit`".into())
                })?,
            extra_data: Bytes::from_iter(
                self.extra_data
                    .ok_or(ConversionError::MissingValue(
                        "Field `extra_data` is null".into(),
                    ))?
                    .to_bytes(),
            ),
            logs_bloom: self
                .logs_bloom
                .map(|bloom| Bloom::from_slice(bloom.as_ref())),
            timestamp: self
                .timestamp
                .ok_or(ConversionError::MissingValue(
                    "Field `timestamp` is null".into(),
                ))?
                .try_into()
                .map_err(|_| {
                    ConversionError::ConvertError("Failed to convert field `timestamp`".into())
                })?,
            difficulty: self
                .difficulty
                .ok_or(ConversionError::MissingValue(
                    "Field `difficulty` is null".into(),
                ))?
                .try_into()
                .map_err(|_| {
                    ConversionError::ConvertError("Failed to convert field `difficulty`".into())
                })?,
            total_difficulty: self
                .total_difficulty
                .map(|total_difficulty| {
                    total_difficulty.try_into().map_err(|_| {
                        ConversionError::ConvertError(
                            "Failed to convert field `total_difficulty`".into(),
                        )
                    })
                })
                .transpose()?,
            // Hypersync does not support seal field
            seal_fields: Vec::new(),
            uncles: self
                .uncles
                .map(|uncles| uncles.into_iter().map(Into::into).collect())
                .ok_or(ConversionError::MissingValue(
                    "Field `uncles` is null".into(),
                ))?,
            transactions: txs.into_iter().map(Into::into).collect(),
            size: self
                .size
                .map(|size| {
                    size.try_into().map_err(|_| {
                        ConversionError::ConvertError("Failed to convert field `size`".into())
                    })
                })
                .transpose()?,
            mix_hash: self.mix_hash.map(Into::into),
            nonce: self.nonce.map(Into::into),
            base_fee_per_gas: self
                .base_fee_per_gas
                .map(|base_fee_per_gas| {
                    base_fee_per_gas.try_into().map_err(|_| {
                        ConversionError::ConvertError(
                            "Failed to convert field `base_fee_per_gas`".into(),
                        )
                    })
                })
                .transpose()?,
            blob_gas_used: self
                .blob_gas_used
                .map(|blob_gas_used| {
                    blob_gas_used.try_into().map_err(|_| {
                        ConversionError::ConvertError(
                            "Failed to convert field `blob_gas_used`".into(),
                        )
                    })
                })
                .transpose()?,
            excess_blob_gas: self
                .excess_blob_gas
                .map(|excess_blob_gas| {
                    excess_blob_gas.try_into().map_err(|_| {
                        ConversionError::ConvertError(
                            "Failed to convert field `excess_blob_gas`".into(),
                        )
                    })
                })
                .transpose()?,
            withdrawals_root: self.withdrawals_root.map(Into::into),
            withdrawals: self.withdrawals.map(|withdrawals| {
                withdrawals
                    .into_iter()
                    .filter_map(|withdrawal| withdrawal.try_into().ok())
                    .collect()
            }),
            parent_beacon_block_root: self.parent_beacon_block_root.map(Into::into),
            other: Default::default(),
        })
    }
}

impl TryFrom<Transaction> for EtherTransaction {
    type Error = ConversionError;
    fn try_from(value: Transaction) -> Result<Self, Self::Error> {
        Ok(EtherTransaction {
            hash: value
                .hash
                .ok_or(ConversionError::MissingValue("Field `hash` is null".into()))?
                .into(),
            nonce: value
                .nonce
                .ok_or(ConversionError::MissingValue(
                    "Field `nonce` is null".into(),
                ))?
                .try_into()
                .map_err(|_| {
                    ConversionError::ConvertError("Failed to convert field `nonce`".into())
                })?,
            block_hash: value.block_hash.map(Into::into),
            block_number: value.block_number.map(Into::into),
            transaction_index: value.transaction_index.map(Into::into),
            from: value
                .from
                .ok_or(ConversionError::MissingValue("Field `from` is null".into()))?
                .into(),
            to: value.to.map(Into::into),
            value: value
                .value
                .ok_or(ConversionError::MissingValue(
                    "Field `value` is null".into(),
                ))?
                .try_into()
                .map_err(|_| {
                    ConversionError::ConvertError("Failed to convert field `value`".into())
                })?,
            gas_price: value
                .gas_price
                .map(|gas_price| {
                    gas_price.try_into().map_err(|_| {
                        ConversionError::ConvertError("Failed to convert field `value`".into())
                    })
                })
                .transpose()?,
            gas: value
                .gas
                .ok_or(ConversionError::MissingValue("Field `gas` is null".into()))?
                .try_into()
                .map_err(|_| {
                    ConversionError::ConvertError("Failed to convert field `gas`".into())
                })?,
            input: Bytes::from_iter(
                value
                    .input
                    .ok_or(ConversionError::MissingValue(
                        "Field `input` is null".into(),
                    ))?
                    .to_bytes(),
            ),
            v: value
                .v
                .ok_or(ConversionError::MissingValue("Field `v` is null".into()))?
                .try_into()
                .map_err(|_| ConversionError::ConvertError("Failed to convert field `v`".into()))?,
            r: value
                .r
                .ok_or(ConversionError::MissingValue("Field `r` is null".into()))?
                .try_into()
                .map_err(|_| ConversionError::ConvertError("Failed to convert field `r`".into()))?,
            s: value
                .s
                .ok_or(ConversionError::MissingValue("Field `s` is null".into()))?
                .try_into()
                .map_err(|_| ConversionError::ConvertError("Failed to convert field `s`".into()))?,
            transaction_type: value.kind.map(|kind| kind.0.into()),
            access_list: value.access_list.map(access_conversion).transpose()?,
            max_priority_fee_per_gas: value
                .max_priority_fee_per_gas
                .map(|max_priority_fee_per_gas| {
                    max_priority_fee_per_gas.try_into().map_err(|_| {
                        ConversionError::ConvertError(
                            "Failed to convert field `max_priority_fee_per_gas`".into(),
                        )
                    })
                })
                .transpose()?,
            max_fee_per_gas: value
                .max_fee_per_gas
                .map(|max_fee_per_gas| {
                    max_fee_per_gas.try_into().map_err(|_| {
                        ConversionError::ConvertError(
                            "Failed to convert field `max_fee_per_gas`".into(),
                        )
                    })
                })
                .transpose()?,
            chain_id: value
                .chain_id
                .map(|chain_id| {
                    chain_id.try_into().map_err(|_| {
                        ConversionError::ConvertError("Failed to convert field `chain_id`".into())
                    })
                })
                .transpose()?,
            other: Default::default(),
        })
    }
}

/// Converts hypersync Access List to ethers Access list
pub fn access_conversion(value: Vec<AccessList>) -> Result<EtherAccessList, ConversionError> {
    Ok(EtherAccessList(
        value
            .into_iter()
            .map(|acc_item| {
                Ok(EtherAccessListItem {
                    address: acc_item
                        .address
                        .map(ethers::types::Address::from)
                        .unwrap_or(ethers::types::Address::zero()),
                    storage_keys: acc_item
                        .storage_keys
                        .ok_or(ConversionError::MissingValue(
                            "Field `storage_keys` is null".into(),
                        ))?
                        .into_iter()
                        .map(H256::from)
                        .collect::<Vec<H256>>(),
                })
            })
            .collect::<Result<_, ConversionError>>()?,
    ))
}

impl TryFrom<Trace> for EtherTrace {
    type Error = ConversionError;
    fn try_from(value: Trace) -> Result<Self, Self::Error> {
        Ok(EtherTrace {
            action: convert_action(
                value.kind.as_deref(),
                value.from,
                value.to,
                value.value,
                value.gas,
                value.input,
                value.call_type,
                value.init,
                value.author,
                value.reward_type,
                // Sadly we need to clone x)
                value.address.clone(),
            )?,
            result: convert_result(
                value.kind.as_deref(),
                value.gas_used,
                value.output,
                value.code,
                value.address,
            )?,
            // We expect trace_address to not exceed usize, so truncating is fine
            trace_address: value
                .trace_address
                .ok_or(ConversionError::MissingValue(
                    "Field `trace_address` is null".into(),
                ))?
                .into_iter()
                .map(|address| {
                    address.try_into().map_err(|_| {
                        ConversionError::ConvertError(
                            "Failed to convert `trace_address` into usize".into(),
                        )
                    })
                })
                .collect::<Result<_, ConversionError>>()?,
            // We expect transaction_position to not exceed usize, so truncating is fine
            subtraces: value
                .subtraces
                .ok_or(ConversionError::MissingValue(
                    "Field `subtraces` is null".into(),
                ))?
                .try_into()
                .map_err(|_| {
                    ConversionError::ConvertError("Failed to convert `subtraces` into usize".into())
                })?,
            // We expect transaction_position to not exceed usize, so truncating is fine
            transaction_position: value
                .transaction_position
                .map(|pos| {
                    usize::try_from(pos).map_err(|_| {
                        ConversionError::ConvertError(
                            "Failed to convert `transaction_position` into usize".into(),
                        )
                    })
                })
                .transpose()?,
            transaction_hash: value.transaction_hash.map(Into::into),
            block_number: value.block_number.ok_or(ConversionError::MissingValue(
                "Field `block_number` is null".into(),
            ))?,
            block_hash: value
                .block_hash
                .ok_or(ConversionError::MissingValue(
                    "Field `block_hash` is null".into(),
                ))?
                .into(),
            action_type: value
                .kind
                .map(|kind| match kind.as_str() {
                    "call" => ActionType::Call,
                    "create" => ActionType::Create,
                    "reward" => ActionType::Reward,
                    "suicide" => ActionType::Suicide,
                    _ => ActionType::Call,
                })
                .ok_or(ConversionError::MissingValue("Field `kind` is null".into()))?,
            error: value.error,
        })
    }
}

/// Extracts ethers result from hypersync Trace
fn convert_result(
    kind: Option<&str>,
    gas_used: Option<Quantity>,
    output: Option<Data>,
    code: Option<Data>,
    address: Option<Address>,
) -> Result<Option<Res>, ConversionError> {
    match kind {
        Some("call") => Ok(Some(Res::Call(CallResult {
            gas_used: gas_used
                .ok_or(ConversionError::MissingValue(
                    "Field `gas_used` is null".into(),
                ))?
                .try_into()
                .map_err(|_| {
                    ConversionError::ConvertError("Failed to convert field `gas_used`".into())
                })?,
            output: Bytes::from_iter(
                output
                    .ok_or(ConversionError::MissingValue(
                        "Field `output` is null".into(),
                    ))?
                    .to_bytes(),
            ),
        }))),
        Some("create") => Ok(Some(Res::Create(CreateResult {
            gas_used: gas_used
                .ok_or(ConversionError::MissingValue(
                    "Field `gas_used` is null".into(),
                ))?
                .try_into()
                .map_err(|_| {
                    ConversionError::ConvertError("Failed to convert field `gas_used`".into())
                })?,
            code: Bytes::from_iter(
                code.ok_or(ConversionError::MissingValue("Field `code` is null".into()))?
                    .to_bytes(),
            ),
            address: address
                .ok_or(ConversionError::MissingValue(
                    "Field `address` is null".into(),
                ))?
                .into(),
        }))),
        Some(_) => Err(ConversionError::ConvertError(
            "Field `kind` is missing or unknown".into(),
        )),
        None => Ok(None),
    }
}

/// Extracts ethers action from hypersync Trace
#[allow(clippy::too_many_arguments)]
fn convert_action(
    kind: Option<&str>,
    from: Option<Address>,
    to: Option<Address>,
    value: Option<Quantity>,
    gas: Option<Quantity>,
    input: Option<Data>,
    call_type: Option<String>,
    init: Option<Data>,
    author: Option<Address>,
    reward_type: Option<String>,
    address: Option<Address>,
) -> Result<Action, ConversionError> {
    match kind {
        Some("call") => Ok(Action::Call(Call {
            from: from
                .ok_or(ConversionError::MissingValue("Field `from` is null".into()))?
                .into(),
            to: to
                .ok_or(ConversionError::MissingValue("Field `to` is null".into()))?
                .into(),
            value: value
                .ok_or(ConversionError::MissingValue(
                    "Field `value` is null".into(),
                ))?
                .try_into()
                .map_err(|_| {
                    ConversionError::ConvertError("Failed to convert field `value`".into())
                })?,
            gas: gas
                .ok_or(ConversionError::MissingValue("Field `gas` is null".into()))?
                .try_into()
                .map_err(|_| {
                    ConversionError::ConvertError("Failed to convert field `gas`".into())
                })?,
            input: Bytes::from_iter(
                input
                    .ok_or(ConversionError::MissingValue(
                        "Field `input` is null".into(),
                    ))?
                    .to_bytes(),
            ),
            call_type: {
                let call_type = call_type.ok_or(ConversionError::MissingValue(
                    "Field `call_type` is null".into(),
                ))?;
                match call_type.as_str() {
                    "delegatecall" => Ok(CallType::DelegateCall),
                    "staticcall" => Ok(CallType::StaticCall),
                    "callcode" => Ok(CallType::CallCode),
                    "call" => Ok(CallType::Call),
                    _ => Err(ConversionError::ConvertError("Unknown call_type".into())),
                }
            }?,
        })),
        Some("create") => Ok(Action::Create(Create {
            from: from
                .ok_or(ConversionError::MissingValue("Field `from` is null".into()))?
                .into(),
            value: value
                .ok_or(ConversionError::MissingValue(
                    "Field `value` is null".into(),
                ))?
                .try_into()
                .map_err(|_| {
                    ConversionError::ConvertError("Failed to convert field `value`".into())
                })?,
            gas: gas
                .ok_or(ConversionError::MissingValue("Field `gas` is null".into()))?
                .try_into()
                .map_err(|_| {
                    ConversionError::ConvertError("Failed to convert field `gas`".into())
                })?,
            init: Bytes::from_iter(
                init.ok_or(ConversionError::MissingValue("Field `init` is null".into()))?
                    .to_bytes(),
            ),
        })),
        Some("reward") => Ok(Action::Reward(Reward {
            author: author
                .ok_or(ConversionError::MissingValue(
                    "Field `author` is null".into(),
                ))?
                .into(),
            value: value
                .ok_or(ConversionError::MissingValue(
                    "Field `value` is null".into(),
                ))?
                .try_into()
                .map_err(|_| {
                    ConversionError::ConvertError("Failed to convert field `value`".into())
                })?,
            reward_type: {
                let reward_type = reward_type.ok_or(ConversionError::MissingValue(
                    "Field `value` is null".into(),
                ))?;
                match reward_type.as_str() {
                    "uncle" => Ok(RewardType::Uncle),
                    "block" => Ok(RewardType::Block),
                    _ => Err(ConversionError::ConvertError(
                        "Failed to convert field `reward_type`".into(),
                    )),
                }?
            },
        })),
        // hypersync doesn't support self-destruct/suicide traces
        Some("suicide") => Ok(Action::Suicide(Suicide {
            address: address
                .ok_or(ConversionError::MissingValue(
                    "Field `address` is null".into(),
                ))?
                .into(),
            // Note: Not implemented in hypersync
            refund_address: EtherAddress::zero(),
            // Note: Not implemented in hypersync
            balance: U256::zero(),
        })),
        None | Some(_) => Err(ConversionError::MissingValue(
            "Field `kind` is missing or unknown".into(),
        )),
    }
}

impl TryFrom<Log> for EtherLog {
    type Error = ConversionError;

    fn try_from(value: Log) -> Result<Self, Self::Error> {
        Ok(EtherLog {
            address: value
                .address
                .ok_or(ConversionError::MissingValue(
                    "Field `address` is null".into(),
                ))?
                .into(),
            topics: value
                .topics
                .into_iter()
                .filter_map(|topic| {
                    if let Some(topic) = topic {
                        Some(topic.into())
                    } else {
                        None
                    }
                })
                .collect::<Vec<H256>>(),
            data: Bytes::from_iter(
                value
                    .data
                    .ok_or(ConversionError::MissingValue("Field `data` is null".into()))?
                    .to_bytes(),
            ),
            block_hash: value.block_hash.map(Into::into),
            block_number: value.block_number.map(Into::into),
            transaction_hash: value.transaction_hash.map(Into::into),
            transaction_index: value.transaction_index.map(Into::into),
            log_index: value.log_index.map(Into::into),
            // This field is kinda strange too, it's seems like duplicate for transaction_index
            // and not in the spec.
            // Hypersync does not support this field.
            transaction_log_index: value.transaction_index.map(Into::into),
            // This field is not used anywhere in ethers, and we don't really know where has it came
            // from, considering there is no mention of it in spec either.
            // Hypersync does not support this field.
            log_type: None,
            removed: value.removed,
        })
    }
}
