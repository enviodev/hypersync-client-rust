use hex::{decode, encode};
use std::fmt;

use serde::{
    de::{self, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};

#[derive(Debug, Clone)]
pub struct FilterWrapper(sbbf_rs_safe::Filter);

impl PartialEq for FilterWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_bytes() == other.0.as_bytes()
    }
}

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
                    .map(FilterWrapper)
                    .ok_or_else(|| E::custom("invalid bytes for FilterWrapper"))
            }
        }

        deserializer.deserialize_str(FilterWrapperVisitor)
    }
}

impl From<sbbf_rs_safe::Filter> for FilterWrapper {
    fn from(filter: sbbf_rs_safe::Filter) -> Self {
        FilterWrapper(filter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sbbf_rs_safe::Filter;

    #[test]
    fn test_serialize_deserialize() {
        let set = [0, 12, 99];

        let filter = FilterWrapper(Filter::new(32, set.len()));

        let serialized_filter = serde_json::to_string(&filter).unwrap();

        let deserialized_filter: FilterWrapper = serde_json::from_str(&serialized_filter).unwrap();

        assert_eq!(filter, deserialized_filter);
    }
}
