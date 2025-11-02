use crate::{Address, Quantity};
use serde::{Deserialize, Serialize};

/// Evm withdrawal object
///
/// See ethereum rpc spec for the meaning of fields
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(rename_all = "camelCase")]
pub struct Withdrawal {
    pub index: Option<Quantity>,
    pub validator_index: Option<Quantity>,
    pub address: Option<Address>,
    pub amount: Option<Quantity>,
}

#[cfg(feature = "ethers")]
pub enum WithdrawalNullError {
    Index,
    ValidatorIndex,
    Address,
    Amount,
}
#[cfg(feature = "ethers")]
impl From<ethers::types::Withdrawal> for Withdrawal {
    fn from(value: ethers::prelude::Withdrawal) -> Self {
        Withdrawal {
            index: Some(value.index.into()),
            validator_index: Some(value.validator_index.into()),
            address: Some(value.address.into()),
            amount: Some(value.amount.into()),
        }
    }
}

#[cfg(feature = "ethers")]
impl TryFrom<Withdrawal> for ethers::types::Withdrawal {
    type Error = WithdrawalNullError;

    fn try_from(value: Withdrawal) -> Result<Self, Self::Error> {
        Ok(ethers::types::Withdrawal {
            index: value
                .index
                .ok_or(WithdrawalNullError::Index)?
                .try_into()
                .map_err(|_| WithdrawalNullError::Index)?,
            validator_index: value
                .validator_index
                .ok_or(WithdrawalNullError::ValidatorIndex)?
                .try_into()
                .map_err(|_| WithdrawalNullError::ValidatorIndex)?,
            address: value.address.ok_or(WithdrawalNullError::Address)?.into(),
            amount: value
                .amount
                .ok_or(WithdrawalNullError::Amount)?
                .try_into()
                .map_err(|_| WithdrawalNullError::Amount)?,
        })
    }
}
