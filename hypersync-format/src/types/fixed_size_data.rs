use crate::{Error, Result};
use alloy_primitives::FixedBytes;
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::result::Result as StdResult;

use super::Hex;

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    derive_more::From,
    derive_more::Into,
    derive_more::Deref,
    PartialOrd,
    Ord,
)]
pub struct FixedSizeData<const N: usize>(Box<[u8; N]>);

impl<const N: usize> Default for FixedSizeData<N> {
    fn default() -> Self {
        Self(Box::new([0; N]))
    }
}

impl<const N: usize> From<&'_ FixedSizeData<N>> for FixedBytes<N> {
    fn from(data: &'_ FixedSizeData<N>) -> Self {
        Self::from(*data.0)
    }
}

impl<const N: usize> AsRef<[u8]> for FixedSizeData<N> {
    fn as_ref(&self) -> &[u8] {
        &*self.0
    }
}

impl<const N: usize> From<[u8; N]> for FixedSizeData<N> {
    fn from(buf: [u8; N]) -> Self {
        Self(Box::new(buf))
    }
}

impl<const N: usize> TryFrom<&[u8]> for FixedSizeData<N> {
    type Error = Error;

    fn try_from(buf: &[u8]) -> Result<FixedSizeData<N>> {
        let buf: [u8; N] = buf.try_into().map_err(|_| Error::UnexpectedLength {
            expected: N,
            got: buf.len(),
        })?;

        Ok(FixedSizeData(Box::new(buf)))
    }
}

impl<const N: usize> TryFrom<Vec<u8>> for FixedSizeData<N> {
    type Error = Error;

    fn try_from(buf: Vec<u8>) -> Result<FixedSizeData<N>> {
        let len = buf.len();
        let buf: Box<[u8; N]> = buf.try_into().map_err(|_| Error::UnexpectedLength {
            expected: N,
            got: len,
        })?;

        Ok(FixedSizeData(buf))
    }
}

impl<const N: usize> Hex for FixedSizeData<N> {
    fn encode_hex(&self) -> String {
        encode_hex(self.as_slice())
    }

    fn decode_hex(hex: &str) -> Result<Self> {
        let hex = decode_hex(hex)?;
        Self::try_from(hex)
    }
}

struct FixedSizeDataVisitor<const N: usize>;

impl<'de, const N: usize> Visitor<'de> for FixedSizeDataVisitor<N> {
    type Value = FixedSizeData<N>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str(&format!("hex string for {N} byte data"))
    }

    fn visit_str<E>(self, value: &str) -> StdResult<Self::Value, E>
    where
        E: de::Error,
    {
        let buf = decode_hex(value).map_err(|e| E::custom(e.to_string()))?;

        Self::Value::try_from(buf).map_err(|e| E::custom(e.to_string()))
    }
}

impl<'de, const N: usize> Deserialize<'de> for FixedSizeData<N> {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(FixedSizeDataVisitor)
    }
}

impl<const N: usize> Serialize for FixedSizeData<N> {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&encode_hex(self.as_ref()))
    }
}

fn encode_hex(buf: &[u8]) -> String {
    format!("0x{}", faster_hex::hex_string(buf))
}

fn decode_hex(value: &str) -> Result<Vec<u8>> {
    let val = value
        .strip_prefix("0x")
        .ok_or_else(|| Error::InvalidHexPrefix(value.to_owned()))?;

    super::util::decode_hex(val).map_err(Error::DecodeHex)
}

#[cfg(test)]
mod tests {
    type FixedSizeData = super::FixedSizeData<4>;
    use hex_literal::hex;
    use serde_test::{assert_tokens, Token};

    #[test]
    fn test_serde_empty() {
        assert_tokens(&FixedSizeData::default(), &[Token::Str("0x00000000")]);
    }

    #[test]
    fn test_serde() {
        assert_tokens(
            &FixedSizeData::from(hex!("00420000")),
            &[Token::Str("0x00420000")],
        );
        assert_tokens(
            &FixedSizeData::from(hex!("42000000")),
            &[Token::Str("0x42000000")],
        );
        assert_tokens(
            &FixedSizeData::from(hex!("00000042")),
            &[Token::Str("0x00000042")],
        );
    }
}
