use crate::{Error, Result};
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::result::Result as StdResult;
use std::str::FromStr;

use super::Hex;

#[derive(Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum TransactionStatus {
    Success,
    Failure,
}

impl TransactionStatus {
    pub fn from_u8(val: u8) -> Result<Self> {
        match val {
            1 => Ok(Self::Success),
            0 => Ok(Self::Failure),
            _ => Err(Error::UnknownTransactionStatus(val.to_string())),
        }
    }

    pub fn to_u8(&self) -> u8 {
        match self {
            Self::Success => 1,
            Self::Failure => 0,
        }
    }
}

impl FromStr for TransactionStatus {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "0x1" => Ok(Self::Success),
            "0x0" => Ok(Self::Failure),
            _ => Err(Error::UnknownTransactionStatus(s.to_owned())),
        }
    }
}

impl TransactionStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Success => "0x1",
            Self::Failure => "0x0",
        }
    }
}

struct TransactionStatusVisitor;

impl Visitor<'_> for TransactionStatusVisitor {
    type Value = TransactionStatus;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("hex string for transaction status")
    }

    fn visit_str<E>(self, value: &str) -> StdResult<Self::Value, E>
    where
        E: de::Error,
    {
        TransactionStatus::from_str(value).map_err(|e| E::custom(e.to_string()))
    }
}

impl<'de> Deserialize<'de> for TransactionStatus {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(TransactionStatusVisitor)
    }
}

impl Serialize for TransactionStatus {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl Hex for TransactionStatus {
    fn encode_hex(&self) -> String {
        self.as_str().to_owned()
    }

    fn decode_hex(hex: &str) -> Result<Self> {
        Self::from_str(hex)
    }
}

impl fmt::Debug for TransactionStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TransactionStatus({})", self.encode_hex())
    }
}

#[cfg(test)]
mod tests {
    use super::TransactionStatus;
    use serde_test::{assert_de_tokens, assert_tokens, Token};

    #[test]
    fn test_serde() {
        assert_tokens(&TransactionStatus::Success, &[Token::Str("0x1")]);
        assert_tokens(&TransactionStatus::Failure, &[Token::Str("0x0")]);
    }

    #[test]
    #[should_panic]
    fn test_de_unknown() {
        assert_de_tokens(&TransactionStatus::Success, &[Token::Str("0x3")]);
    }
}
