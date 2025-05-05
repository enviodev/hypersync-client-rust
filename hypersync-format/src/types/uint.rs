use super::quantity::encode_hex;
use crate::{Error, Hex};
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::result::Result as StdResult;
use std::str::FromStr;

#[derive(
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
pub struct UInt(u64);

impl FromStr for UInt {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Error> {
        let value = value
            .strip_prefix("0x")
            .ok_or_else(|| Error::InvalidHexPrefix(value.to_owned()))?;

        u64::from_str_radix(value, 16)
            .map_err(|e| Error::DecodeNumberFromHex(e.to_string()))
            .map(Into::into)
    }
}

#[cfg(feature = "ethers")]
impl From<ethabi::ethereum_types::U64> for UInt {
    fn from(value: ethabi::ethereum_types::U64) -> Self {
        value.0[0].into()
    }
}

#[cfg(feature = "ethers")]
impl From<UInt> for ethabi::ethereum_types::U64 {
    fn from(value: UInt) -> Self {
        value.0.into()
    }
}

#[cfg(feature = "ethers")]
impl From<UInt> for ethabi::ethereum_types::U256 {
    fn from(value: UInt) -> Self {
        value.0.into()
    }
}

struct UIntVisitor;

impl Visitor<'_> for UIntVisitor {
    type Value = UInt;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("hex string for integer")
    }

    fn visit_str<E>(self, value: &str) -> StdResult<Self::Value, E>
    where
        E: de::Error,
    {
        UInt::from_str(value).map_err(|e| E::custom(e.to_string()))
    }
}

impl<'de> Deserialize<'de> for UInt {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(UIntVisitor)
    }
}

impl Serialize for UInt {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.encode_hex())
    }
}

impl Hex for UInt {
    fn encode_hex(&self) -> String {
        encode_hex(&self.to_be_bytes())
    }

    fn decode_hex(hex: &str) -> crate::Result<Self> {
        Self::from_str(hex)
    }
}

impl fmt::Debug for UInt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UInt({})", self.encode_hex())
    }
}

#[cfg(test)]
mod tests {
    use super::UInt;
    use serde_test::{assert_de_tokens, assert_tokens, Token};

    #[test]
    fn test_serde_zero() {
        assert_eq!(UInt::default(), UInt::from(0));

        assert_tokens(&UInt::from(0), &[Token::Str("0x0")]);
    }

    #[test]
    fn test_serde_max() {
        assert_tokens(&UInt::from(u64::MAX), &[Token::Str("0xffffffffffffffff")]);
    }

    #[test]
    fn test_serde() {
        assert_tokens(&UInt::from(19), &[Token::Str("0x13")]);
    }

    #[test]
    #[should_panic(expected = "number too large")]
    fn test_serde_overflow() {
        assert_de_tokens(&UInt::from(19), &[Token::Str("0xffffffffffffffffa")]);
    }
}
