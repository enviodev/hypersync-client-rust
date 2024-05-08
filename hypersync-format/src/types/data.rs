use crate::{Error, Result};
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::borrow::Cow;
use std::fmt;
use std::result::Result as StdResult;

use super::Hex;

#[derive(
    Debug,
    Default,
    Clone,
    PartialEq,
    Eq,
    Hash,
    derive_more::From,
    derive_more::Into,
    derive_more::Deref,
)]
pub struct Data(Box<[u8]>);

impl AsRef<[u8]> for Data {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<Vec<u8>> for Data {
    fn from(buf: Vec<u8>) -> Self {
        Self(buf.into())
    }
}

impl From<&[u8]> for Data {
    fn from(buf: &[u8]) -> Self {
        Self(buf.into())
    }
}

impl<const N: usize> From<[u8; N]> for Data {
    fn from(buf: [u8; N]) -> Self {
        Self(buf.into())
    }
}

struct DataVisitor;

impl<'de> Visitor<'de> for DataVisitor {
    type Value = Data;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("hex string for data")
    }

    fn visit_str<E>(self, value: &str) -> StdResult<Self::Value, E>
    where
        E: de::Error,
    {
        let buf: Vec<u8> = decode_hex(value).map_err(|e| E::custom(e.to_string()))?;

        Ok(Data::from(buf))
    }
}

impl<'de> Deserialize<'de> for Data {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(DataVisitor)
    }
}

impl Serialize for Data {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&encode_hex(self))
    }
}

impl Hex for Data {
    fn encode_hex(&self) -> String {
        encode_hex(&self.0)
    }

    fn decode_hex(hex: &str) -> Result<Self> {
        let hex = decode_hex(hex)?;
        Ok(Self::from(hex))
    }
}

fn decode_hex(value: &str) -> Result<Vec<u8>> {
    let value = value
        .strip_prefix("0x")
        .ok_or_else(|| Error::InvalidHexPrefix(value.to_owned()))?;

    let mut val: Cow<_> = value.into();

    if val.len() % 2 != 0 {
        val = format!("0{val}").into();
    }

    super::util::decode_hex(val.as_ref()).map_err(Error::DecodeHex)
}

fn encode_hex(buf: &[u8]) -> String {
    if buf.is_empty() {
        return "0x".into();
    }

    format!("0x{}", faster_hex::hex_string(buf))
}

#[cfg(test)]
mod tests {
    use super::Data;
    use hex_literal::hex;
    use serde_test::{assert_tokens, Token};

    #[test]
    fn test_serde_empty() {
        assert_tokens(&Data::default(), &[Token::Str("0x")]);
    }

    #[test]
    fn test_serde() {
        assert_tokens(&Data::from(hex!("004200")), &[Token::Str("0x004200")]);
        assert_tokens(&Data::from(hex!("420000")), &[Token::Str("0x420000")]);
        assert_tokens(&Data::from(hex!("000042")), &[Token::Str("0x000042")]);
        assert_tokens(&Data::from(hex!("00")), &[Token::Str("0x00")]);
    }
}
