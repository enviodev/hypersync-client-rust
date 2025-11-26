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
