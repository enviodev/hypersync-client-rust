use super::{util::canonicalize_bytes, Hex};
use crate::{Error, Result};
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::borrow::Cow;
use std::fmt;
use std::result::Result as StdResult;

#[derive(Clone, PartialEq, Eq, Hash, derive_more::From, derive_more::Into, derive_more::Deref)]
pub struct Quantity(Box<[u8]>);

impl AsRef<[u8]> for Quantity {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<Vec<u8>> for Quantity {
    fn from(buf: Vec<u8>) -> Self {
        assert!(!buf.is_empty());
        assert!(buf.len() == 1 || buf[0] != 0);

        Self(buf.into())
    }
}

#[cfg(feature = "ethers")]
impl From<ethabi::ethereum_types::U256> for Quantity {
    fn from(value: ethabi::ethereum_types::U256) -> Self {
        let mut buf = Box::new([]);
        value.to_big_endian(buf.as_mut());
        Self(buf)
    }
}

#[cfg(feature = "ethers")]
impl TryFrom<Quantity> for ethabi::ethereum_types::U256 {
    type Error = ();

    fn try_from(value: Quantity) -> StdResult<Self, Self::Error> {
        // Comparison comes from assert!($n_words * 8 >= slice.len());
        if value.0.len() > 32 {
            return Err(());
        }
        Ok(ethabi::ethereum_types::U256::from_big_endian(&value.0))
    }
}

#[cfg(feature = "ethers")]
impl From<ethabi::ethereum_types::U64> for Quantity {
    fn from(value: ethabi::ethereum_types::U64) -> Self {
        let mut buf = Box::new([]);
        value.to_big_endian(buf.as_mut());
        Self(buf)
    }
}

#[cfg(feature = "ethers")]
impl TryFrom<Quantity> for ethabi::ethereum_types::U64 {
    type Error = ();

    fn try_from(value: Quantity) -> StdResult<Self, Self::Error> {
        // Comparison comes from assert!($n_words * 8 >= slice.len());
        if value.0.len() > 32 {
            return Err(());
        }
        Ok(ethabi::ethereum_types::U64::from_big_endian(&value.0))
    }
}

impl From<&[u8]> for Quantity {
    fn from(buf: &[u8]) -> Self {
        assert!(!buf.is_empty());
        assert!(buf.len() == 1 || buf[0] != 0);

        Self(buf.into())
    }
}

impl Default for Quantity {
    fn default() -> Quantity {
        Quantity(Box::new([0]))
    }
}

impl<const N: usize> From<[u8; N]> for Quantity {
    fn from(buf: [u8; N]) -> Self {
        Self(buf.into())
    }
}

struct QuantityVisitor;

impl Visitor<'_> for QuantityVisitor {
    type Value = Quantity;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("hex string for a quantity")
    }

    fn visit_str<E>(self, value: &str) -> StdResult<Self::Value, E>
    where
        E: de::Error,
    {
        let buf: Vec<u8> = decode_hex(value).map_err(|e| E::custom(e.to_string()))?;

        Ok(Quantity::from(buf))
    }

    fn visit_i64<E>(self, value: i64) -> StdResult<Self::Value, E>
    where
        E: de::Error,
    {
        let hex = encode_hex(&value.to_be_bytes());
        self.visit_str(&hex)
    }

    fn visit_u64<E>(self, value: u64) -> StdResult<Self::Value, E>
    where
        E: de::Error,
    {
        let hex = encode_hex(&value.to_be_bytes());
        self.visit_str(&hex)
    }
}

impl<'de> Deserialize<'de> for Quantity {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(QuantityVisitor)
    }
}

impl Serialize for Quantity {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&encode_hex(self))
    }
}

impl Hex for Quantity {
    fn encode_hex(&self) -> String {
        encode_hex(&self.0)
    }

    fn decode_hex(hex: &str) -> Result<Self> {
        let hex = decode_hex(hex)?;
        Ok(Self::from(hex))
    }
}

pub fn decode_hex(value: &str) -> Result<Vec<u8>> {
    if value == "0x0" {
        return Ok(vec![0]);
    }

    let val = value
        .strip_prefix("0x")
        .ok_or_else(|| Error::InvalidHexPrefix(value.to_owned()))?;

    if val.is_empty() {
        return Err(Error::UnexpectedQuantity(value.to_owned()));
    }

    let mut val: Cow<_> = val.into();

    if val.len() % 2 != 0 {
        val = format!("0{val}").into();
    }

    let bytes = super::util::decode_hex(val.as_ref()).map_err(Error::DecodeHex)?;

    // Normalize to canonical form by removing leading zero bytes
    // This handles zero-padded values from non-compliant RPCs like Tron
    let canonical_bytes = canonicalize_bytes(bytes);

    Ok(canonical_bytes)
}

pub fn encode_hex(buf: &[u8]) -> String {
    let hex_val = faster_hex::hex_string(buf);

    match hex_val.find(|c| c != '0') {
        Some(idx) => format!("0x{}", &hex_val[idx..]),
        None => "0x0".into(),
    }
}

impl fmt::Debug for Quantity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Quantity({})", self.encode_hex())
    }
}

#[cfg(test)]
mod tests {
    use super::Quantity;
    use hex_literal::hex;
    use serde_test::{assert_de_tokens, assert_ser_tokens, assert_tokens, Token};

    #[test]
    fn test_serde_zero() {
        assert_eq!(Quantity::default(), Quantity::from(vec![0]));
        assert_tokens(&Quantity::default(), &[Token::Str("0x0")]);
    }

    #[test]
    fn test_serialize() {
        assert_ser_tokens(&Quantity::from(hex!("004200")), &[Token::Str("0x4200")]);
        assert_ser_tokens(&Quantity::from(hex!("420000")), &[Token::Str("0x420000")]);
        assert_ser_tokens(&Quantity::from(hex!("000042")), &[Token::Str("0x42")]);
    }

    #[test]
    fn test_deserialize() {
        assert_de_tokens(&Quantity::from(hex!("420000")), &[Token::Str("0x420000")]);
    }

    #[test]
    fn test_deserialize_leading_zeroes() {
        // Should now accept zero-padded values and normalize them
        assert_de_tokens(&Quantity::from(hex!("420000")), &[Token::Str("0x00420000")]);
    }

    #[test]
    #[should_panic(expected = "Unexpected quantity")]
    fn test_deserialize_empty() {
        assert_de_tokens(&Quantity::default(), &[Token::Str("0x")]);
    }

    #[test]
    fn test_from_vec_zero() {
        assert_eq!(Quantity::default(), Quantity::from(vec![0]))
    }

    #[test]
    #[should_panic]
    fn test_from_vec_empty() {
        let _ = Quantity::from(Vec::new());
    }

    #[test]
    #[should_panic]
    fn test_from_vec_leading_zeroes() {
        let _ = Quantity::from(vec![0, 1]);
    }

    #[test]
    fn test_from_slice_zero() {
        assert_eq!(Quantity::default(), Quantity::from(vec![0].as_slice()))
    }

    #[test]
    #[should_panic]
    fn test_from_slice_empty() {
        let _ = Quantity::from(Vec::new().as_slice());
    }

    #[test]
    #[should_panic]
    fn test_from_slice_leading_zeroes() {
        let _ = Quantity::from(vec![0, 1].as_slice());
    }

    #[test]
    fn test_normalize_zero_padded_values() {
        // Test various zero-padded scenarios
        // These occur on chains like Tron and Taraxa RPC implementations
        assert_de_tokens(&Quantity::from(hex!("01")), &[Token::Str("0x0001")]);
        assert_de_tokens(&Quantity::from(hex!("0a")), &[Token::Str("0x000a")]);
        assert_de_tokens(&Quantity::from(hex!("42")), &[Token::Str("0x000042")]);
        assert_de_tokens(&Quantity::from(hex!("1234")), &[Token::Str("0x00001234")]);
    }

    #[test]
    fn test_zero_value_handling() {
        // Zero should still be handled correctly
        assert_de_tokens(&Quantity::from(hex!("00")), &[Token::Str("0x0")]);
        assert_de_tokens(&Quantity::from(hex!("00")), &[Token::Str("0x00")]);
        assert_de_tokens(&Quantity::from(hex!("00")), &[Token::Str("0x0000")]);
    }

    #[test]
    fn test_deserialize_numeric_u64() {
        // Numeric JSON values should be accepted (e.g., Sonic timestamps)
        assert_de_tokens(&Quantity::from(hex!("66a7c725")), &[Token::U64(0x66a7c725)]);
        assert_de_tokens(&Quantity::from(vec![0]), &[Token::U64(0)]);
        assert_de_tokens(&Quantity::from(hex!("01")), &[Token::U64(1)]);
    }

    #[test]
    fn test_deserialize_numeric_i64() {
        assert_de_tokens(&Quantity::from(hex!("66a7c725")), &[Token::I64(0x66a7c725)]);
        assert_de_tokens(&Quantity::from(vec![0]), &[Token::I64(0)]);
        assert_de_tokens(&Quantity::from(hex!("01")), &[Token::I64(1)]);
    }

    #[test]
    fn test_bincode_compat() {
        let val = Quantity::from(hex!("01"));

        let data = bincode::serialize(&val).unwrap();

        assert_eq!(val, bincode::deserialize(&data).unwrap());
    }
}
