use hex::{decode, encode};
use std::fmt;

use serde::{
    de::{self, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};

#[derive(Debug, Clone)]
pub struct FilterWrapper(sbbf_rs_safe::Filter);

// Implement Serialize and Deserialize for FilterWrapper using hex encoding
impl Serialize for FilterWrapper {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let bytes = self.0.as_bytes();
        let hex_str = encode(bytes);
        serializer.serialize_str(&hex_str)
    }
}

impl PartialEq for FilterWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_bytes() == other.0.as_bytes()
    }
}

impl<'de> Deserialize<'de> for FilterWrapper {
    fn deserialize<D>(deserializer: D) -> Result<FilterWrapper, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FilterWrapperVisitor;

        impl<'de> Visitor<'de> for FilterWrapperVisitor {
            type Value = FilterWrapper;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a hex-encoded string representing a FilterWrapper")
            }

            fn visit_str<E>(self, v: &str) -> Result<FilterWrapper, E>
            where
                E: de::Error,
            {
                let bytes = decode(v).map_err(E::custom)?;
                sbbf_rs_safe::Filter::from_bytes(&bytes)
                    .map(|f| FilterWrapper(f))
                    .ok_or_else(|| E::custom("invalid bytes for FilterWrapper"))
            }
        }

        deserializer.deserialize_str(FilterWrapperVisitor)
    }
}
