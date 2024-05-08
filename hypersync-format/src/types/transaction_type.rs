use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::result::Result as StdResult;

use super::quantity::encode_hex;
use super::Hex;
use crate::{Error, Result};

#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    derive_more::From,
    derive_more::Into,
    derive_more::Deref,
    derive_more::Add,
    derive_more::Sub,
)]
pub struct TransactionType(u8);

struct TransactionTypeVisitor;

impl<'de> Visitor<'de> for TransactionTypeVisitor {
    type Value = TransactionType;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("hex string for integer")
    }

    fn visit_str<E>(self, value: &str) -> StdResult<Self::Value, E>
    where
        E: de::Error,
    {
        TransactionType::decode_hex(value).map_err(|e| E::custom(e.to_string()))
    }
}

impl<'de> Deserialize<'de> for TransactionType {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(TransactionTypeVisitor)
    }
}

impl Serialize for TransactionType {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.encode_hex())
    }
}

impl Hex for TransactionType {
    fn encode_hex(&self) -> String {
        encode_hex(&self.to_be_bytes())
    }

    fn decode_hex(hex: &str) -> Result<Self> {
        let value = hex
            .strip_prefix("0x")
            .ok_or_else(|| Error::InvalidHexPrefix(hex.to_owned()))?;

        u8::from_str_radix(value, 16)
            .map_err(|_| Error::DecodeNumberFromHex(hex.to_string()))
            .map(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::TransactionType;
    use serde_test::{assert_de_tokens, assert_tokens, Token};

    #[test]
    fn test_serde_zero() {
        assert_eq!(TransactionType::default(), TransactionType::from(0));

        assert_tokens(&TransactionType::from(0), &[Token::Str("0x0")]);
    }

    #[test]
    fn test_serde_max() {
        assert_tokens(&TransactionType::from(std::u8::MAX), &[Token::Str("0xff")]);
    }

    #[test]
    fn test_serde() {
        assert_tokens(&TransactionType::from(19), &[Token::Str("0x13")]);
    }

    #[test]
    #[should_panic(expected = "Invalid Number from Hex")]
    fn test_serde_overflow() {
        assert_de_tokens(
            &TransactionType::from(19),
            &[Token::Str("0xffffffffffffffffa")],
        );
    }
}
