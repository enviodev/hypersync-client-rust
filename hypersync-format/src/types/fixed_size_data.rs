use super::Hex;
use crate::types::util::canonicalize_bytes;
use crate::{Error, Result};
use alloy_primitives::FixedBytes;
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::result::Result as StdResult;

#[derive(
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
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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

impl<const N: usize> std::str::FromStr for FixedSizeData<N> {
    type Err = Error;

    fn from_str(s: &str) -> StdResult<Self, Self::Err> {
        // Use your existing decode logic
        let bytes = decode_hex(s)?;
        FixedSizeData::try_from(bytes)
    }
}

impl<const N: usize> TryFrom<&str> for FixedSizeData<N> {
    type Error = Error;

    fn try_from(s: &str) -> StdResult<Self, Self::Error> {
        std::str::FromStr::from_str(s)
    }
}

impl<const N: usize> TryFrom<String> for FixedSizeData<N> {
    type Error = Error;

    fn try_from(s: String) -> StdResult<Self, Self::Error> {
        std::str::FromStr::from_str(&s)
    }
}

impl<const N: usize> fmt::Display for FixedSizeData<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Reuse your existing `encode_hex` function for printing
        write!(f, "{}", self.encode_hex())
    }
}

struct FixedSizeDataVisitor<const N: usize>;

impl<const N: usize> Visitor<'_> for FixedSizeDataVisitor<N> {
    type Value = FixedSizeData<N>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str(&format!("hex string for {N} byte data"))
    }

    fn visit_str<E>(self, value: &str) -> StdResult<Self::Value, E>
    where
        E: de::Error,
    {
        let mut buf = decode_hex(value).map_err(|e| E::custom(e.to_string()))?;

        if buf.len() != N {
            // To handle bad json hexes like on tron we need to pad the hex with 0s
            // or remove additonal padded zeros
            // Handle padding/truncating from the beginning for proper byte alignment

            // Normalize to canonical form by removing leading zero bytes
            buf = canonicalize_bytes(buf);

            // Pad with zeros if the length is less than N (if the length is greater than N, it will fail at try_into so no need to handle that)
            if buf.len() < N {
                let mut padded = vec![0; N];
                padded[N - buf.len()..].copy_from_slice(&buf);
                buf = padded;
            }
        }

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

impl<const N: usize> fmt::Debug for FixedSizeData<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FixedSizeData<{}>({})", N, self.encode_hex())
    }
}

#[cfg(test)]
mod tests {
    type FSD4 = super::FixedSizeData<4>;
    use std::str::FromStr;

    use hex_literal::hex;
    use serde_test::{assert_tokens, Token};

    #[test]
    fn test_serde_empty() {
        assert_tokens(&FSD4::default(), &[Token::Str("0x00000000")]);
    }

    #[test]
    fn test_serde() {
        assert_tokens(&FSD4::from(hex!("00420000")), &[Token::Str("0x00420000")]);
        assert_tokens(&FSD4::from(hex!("42000000")), &[Token::Str("0x42000000")]);
        assert_tokens(&FSD4::from(hex!("00000042")), &[Token::Str("0x00000042")]);
    }

    /// test from_string
    #[test]
    fn test_from_str_valid() {
        let data = FSD4::from_str("0x00420000").expect("valid 4-byte hex");
        assert_eq!(data, FSD4::from(hex!("00420000")));
    }

    #[test]
    fn test_from_str_missing_prefix() {
        // Missing "0x" prefix: should fail
        let data = FSD4::from_str("00420000");
        assert!(data.is_err());
    }

    #[test]
    fn test_from_str_wrong_length() {
        // Only 3 bytes (0x004200) instead of 4
        let data = FSD4::from_str("0x004200");
        assert!(data.is_err());
    }

    /// test to_string
    #[test]
    fn test_display() {
        let data = FSD4::from(hex!("42feed00"));
        // Check that Display prints the 0x-prefixed hex
        assert_eq!(data.to_string(), "0x42feed00");
    }
}
