use crate::{Error, Hex, Result};
use nohash_hasher::IntSet;
use sbbf_rs_safe::Filter;
use std::fmt;
use std::result::Result as StdResult;
use xxhash_rust::xxh3::xxh3_64;

use serde::{
    de::{self, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};

use crate::Data;

#[derive(Clone)]
pub struct FilterWrapper(pub Filter);

impl FilterWrapper {
    pub fn new(bits_per_key: usize, num_keys: usize) -> Self {
        Self(Filter::new(bits_per_key, num_keys))
    }

    pub fn contains_hash(&self, hash: u64) -> bool {
        self.0.contains_hash(hash)
    }

    /// Creates a bloom filter out of a vec of bytes.  Suitable for using
    /// as a query param that takes a filter.
    /// bits_per_key defaults to 16
    pub fn from_keys<'a, I>(keys: I, bits_per_key: Option<usize>) -> Result<Self>
    where
        I: Iterator<Item = &'a [u8]>,
    {
        let bits_per_key = if let Some(bits) = bits_per_key {
            bits
        } else {
            16
        };

        // first put into hash set to remove duplicates
        let keys = keys.into_iter().map(xxh3_64).collect::<IntSet<u64>>();
        let num_keys = keys.len();

        let mut filter = Filter::new(bits_per_key, num_keys);

        // insert each address into the filter
        for hash in keys {
            filter.insert_hash(hash);
        }

        Ok(FilterWrapper(filter))
    }
}

impl PartialEq for FilterWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_bytes() == other.0.as_bytes()
    }
}

// Implement Serialize and Deserialize for FilterWrapper using hex encoding
impl Serialize for FilterWrapper {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let hex_str = self.encode_hex();
        serializer.serialize_str(&hex_str)
    }
}

impl<'de> Deserialize<'de> for FilterWrapper {
    fn deserialize<D>(deserializer: D) -> StdResult<FilterWrapper, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FilterWrapperVisitor;

        impl<'de> Visitor<'de> for FilterWrapperVisitor {
            type Value = FilterWrapper;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a hex-encoded string representing a FilterWrapper")
            }

            fn visit_str<E>(self, v: &str) -> StdResult<FilterWrapper, E>
            where
                E: de::Error,
            {
                FilterWrapper::decode_hex(v).map_err(|e| E::custom(e.to_string()))
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

impl Hex for FilterWrapper {
    fn encode_hex(&self) -> String {
        let data = Data::from(self.0.as_bytes());
        data.encode_hex()
    }

    fn decode_hex(hex: &str) -> Result<Self> {
        let data = Data::decode_hex(hex)?;
        Filter::from_bytes(data.as_ref())
            .ok_or(Error::BloomFilterFromBytes)
            .map(FilterWrapper)
    }
}

impl fmt::Debug for FilterWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FilterWrapper({})", self.encode_hex())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sbbf_rs_safe::Filter;
    use xxhash_rust::xxh3::xxh3_64;

    #[test]
    fn test_serialize_deserialize() {
        let set = [
            xxh3_64("hello".as_bytes()),
            xxh3_64("cool world".as_bytes()),
        ];

        let mut filter = FilterWrapper(Filter::new(32, set.len()));
        for hash in set.into_iter() {
            filter.0.insert_hash(hash);
        }

        let serialized_filter = serde_json::to_string(&filter).unwrap();

        let deserialized_filter: FilterWrapper = serde_json::from_str(&serialized_filter).unwrap();

        assert!(deserialized_filter
            .0
            .contains_hash(xxh3_64("hello".as_bytes())));

        assert!(deserialized_filter
            .0
            .contains_hash(xxh3_64("cool world".as_bytes())));

        assert_eq!(filter, deserialized_filter);
    }
}
