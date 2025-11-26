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
        formatter.write_str("hex string or an integer for a quantity")
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
        if value < 0 {
            return Err(serde::de::Error::custom(
                "negative int quantity not allowed",
            ));
        }
        Ok(Quantity::from(canonicalize_bytes(
            value.to_be_bytes().as_slice().to_vec(),
        )))
    }

    fn visit_u64<E>(self, value: u64) -> StdResult<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Quantity::from(canonicalize_bytes(
            value.to_be_bytes().as_slice().to_vec(),
        )))
    }
}

impl<'de> Deserialize<'de> for Quantity {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Use deserialize_any for human-readable formats (JSON, etc.) which allows
        // both hex strings and plain integers (needed for chains like Sonic).
        // Use deserialize_str for binary formats (bincode, etc.) for compatibility.
        if deserializer.is_human_readable() {
            deserializer.deserialize_any(QuantityVisitor)
        } else {
            deserializer.deserialize_str(QuantityVisitor)
        }
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

#[cfg(feature = "arbitrary")]
impl<'input> arbitrary::Arbitrary<'input> for Quantity {
    fn arbitrary(u: &mut arbitrary::Unstructured<'input>) -> arbitrary::Result<Self> {
        let value = u.arbitrary::<u64>()?;
        Ok(Quantity::from(canonicalize_bytes(
            value.to_be_bytes().as_slice().to_vec(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::Quantity;
    use hex_literal::hex;
    use serde_test::{assert_de_tokens, assert_ser_tokens, assert_tokens, Token};

    #[test]
    fn test_serde_zero() {
        use serde_test::Configure;
        assert_eq!(Quantity::default(), Quantity::from(vec![0]));
        assert_tokens(&Quantity::default().readable(), &[Token::Str("0x0")]);
    }

    #[test]
    fn test_serialize() {
        assert_ser_tokens(&Quantity::from(hex!("004200")), &[Token::Str("0x4200")]);
        assert_ser_tokens(&Quantity::from(hex!("420000")), &[Token::Str("0x420000")]);
        assert_ser_tokens(&Quantity::from(hex!("000042")), &[Token::Str("0x42")]);
    }

    #[test]
    fn test_deserialize() {
        use serde_test::Configure;
        assert_de_tokens(
            &Quantity::from(hex!("420000")).readable(),
            &[Token::Str("0x420000")],
        );
    }

    #[test]
    fn test_deserialize_leading_zeroes() {
        use serde_test::Configure;
        assert_de_tokens(
            &Quantity::from(hex!("420000")).readable(),
            &[Token::Str("0x00420000")],
        );
    }

    #[test]
    #[should_panic(expected = "Unexpected quantity")]
    fn test_deserialize_empty() {
        use serde_test::Configure;
        assert_de_tokens(&Quantity::default().readable(), &[Token::Str("0x")]);
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
        use serde_test::Configure;
        assert_de_tokens(
            &Quantity::from(hex!("01")).readable(),
            &[Token::Str("0x0001")],
        );
        assert_de_tokens(
            &Quantity::from(hex!("0a")).readable(),
            &[Token::Str("0x000a")],
        );
        assert_de_tokens(
            &Quantity::from(hex!("42")).readable(),
            &[Token::Str("0x000042")],
        );
        assert_de_tokens(
            &Quantity::from(hex!("1234")).readable(),
            &[Token::Str("0x00001234")],
        );
    }

    #[test]
    fn test_zero_value_handling() {
        use serde_test::Configure;
        assert_de_tokens(&Quantity::from(hex!("00")).readable(), &[Token::Str("0x0")]);
        assert_de_tokens(
            &Quantity::from(hex!("00")).readable(),
            &[Token::Str("0x00")],
        );
        assert_de_tokens(
            &Quantity::from(hex!("00")).readable(),
            &[Token::Str("0x0000")],
        );
    }

    #[test]
    fn test_deserialize_numeric_u64() {
        use serde_test::Configure;
        // Numeric JSON values should be accepted (e.g., Sonic timestamps)
        assert_de_tokens(
            &Quantity::from(hex!("66a7c725")).readable(),
            &[Token::U64(0x66a7c725)],
        );
        assert_de_tokens(&Quantity::from(vec![0]).readable(), &[Token::U64(0)]);
        assert_de_tokens(&Quantity::from(hex!("01")).readable(), &[Token::U64(1)]);
    }

    #[test]
    fn test_deserialize_numeric_i64() {
        use serde_test::Configure;
        assert_de_tokens(
            &Quantity::from(hex!("66a7c725")).readable(),
            &[Token::I64(0x66a7c725)],
        );
        assert_de_tokens(&Quantity::from(vec![0]).readable(), &[Token::I64(0)]);
        assert_de_tokens(&Quantity::from(hex!("01")).readable(), &[Token::I64(1)]);
    }

    #[test]
    fn test_json_deserialize_integer() {
        // Test that JSON integers are accepted (like Sonic's blockTimestamp)
        let json_int = "1754986612";
        let quantity: Quantity = serde_json::from_str(json_int).unwrap();
        assert_eq!(quantity, Quantity::from(hex!("689af874")));
    }

    #[test]
    fn test_json_deserialize_hex_string() {
        // Test that hex strings still work
        let json_hex = "\"0x689af874\"";
        let quantity: Quantity = serde_json::from_str(json_hex).unwrap();
        assert_eq!(quantity, Quantity::from(hex!("689af874")));
    }

    #[test]
    fn test_json_deserialize_mixed_object() {
        // Test a realistic scenario like Sonic's response
        use serde::Deserialize;

        #[derive(Deserialize, Debug)]
        struct MockReceipt {
            #[serde(rename = "blockNumber")]
            block_number: Quantity,
            #[serde(rename = "blockTimestamp")]
            block_timestamp: Quantity,
        }

        let json = r#"{
        "blockNumber": "0x74",
        "blockTimestamp": 1754986612
    }"#;

        let receipt: MockReceipt = serde_json::from_str(json).unwrap();
        assert_eq!(receipt.block_number, Quantity::from(hex!("74")));
        assert_eq!(receipt.block_timestamp, Quantity::from(hex!("689af874")));
    }

    #[test]
    fn test_bincode_compat() {
        let val = Quantity::from(hex!("01"));

        let data = bincode::serialize(&val).unwrap();

        assert_eq!(val, bincode::deserialize(&data).unwrap());
    }
}
