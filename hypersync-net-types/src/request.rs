use crate::hypersync_net_types_capnp::{query_body, request};
use crate::{hypersync_net_types_capnp, CapnpReader, Query};
use serde::{Deserialize, Serialize};

use anyhow::Context;
use capnp::message::Builder;
use hypersync_format::FixedSizeData;

/// A 128 bit hash of the query body, used as a unique identifier for the query body
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct QueryId(pub FixedSizeData<16>);
impl QueryId {
    pub fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        let data = FixedSizeData::<16>::try_from(bytes).context("invalid query id length")?;
        Ok(Self(data))
    }

    pub fn from_query_body_reader(reader: query_body::Reader<'_>) -> Result<QueryId, capnp::Error> {
        // See https://capnproto.org/encoding.html#canonicalization
        // we need to ensure the query body is canonicalized for hashing
        let mut canon_builder = capnp::message::Builder::new_default();
        canon_builder.set_root_canonical(reader)?;

        // After canonicalization, there is only one segment.
        // We can just hash this withouth the segment table
        let segment = match canon_builder.get_segments_for_output() {
            capnp::OutputSegments::SingleSegment([segment]) => segment,
            capnp::OutputSegments::MultiSegment(items) => {
                return Err(capnp::Error::failed(format!(
                    "Expected exactly 1 segment, found {}",
                    items.len(),
                )))
            }
        };

        let hash: u128 = xxhash_rust::xxh3::xxh3_128(segment);

        Ok(QueryId(FixedSizeData::<16>::from(hash.to_be_bytes())))
    }

    pub fn from_query(query: &Query) -> Result<Self, capnp::Error> {
        let mut message = Builder::new_default();
        let mut query_body_builder = message.init_root::<query_body::Builder>();
        query_body_builder.build_from_query(query)?;
        Self::from_query_body_reader(query_body_builder.into_reader())
    }
}

pub enum Request {
    QueryBody {
        should_cache: bool,
        query: Box<Query>,
    },
    QueryId {
        from_block: u64,
        to_block: Option<u64>,
        id: QueryId,
    },
}

impl Request {
    pub fn from_capnp_bytes(bytes: &[u8]) -> Result<Self, capnp::Error> {
        let message_reader =
            capnp::serialize_packed::read_message(bytes, capnp::message::ReaderOptions::new())?;
        let query = message_reader.get_root::<request::Reader>()?;
        Request::from_reader(query)
    }
}

impl CapnpReader<request::Owned> for Request {
    fn from_reader(query: request::Reader) -> Result<Self, capnp::Error> {
        let block_range = query.get_block_range()?;
        let from_block = block_range.get_from_block();
        let to_block = if block_range.has_to_block() {
            Some(block_range.get_to_block()?.get_value())
        } else {
            None
        };

        match query.get_body().which()? {
            request::body::Which::Query(query_body_reader) => {
                let body_reader = query_body_reader?;
                Ok(Self::QueryBody {
                    should_cache: query.get_should_cache(),
                    query: Box::new(Query::from_capnp_query_body_reader(
                        &body_reader,
                        from_block,
                        to_block,
                    )?),
                })
            }
            request::body::Which::QueryId(id_bytes) => {
                let id = QueryId::from_bytes(id_bytes?)
                    .map_err(|_| capnp::Error::failed("Invalid query id bytes".to_string()))?;

                Ok(Self::QueryId {
                    from_block,
                    to_block,
                    id,
                })
            }
        }
    }
}

impl hypersync_net_types_capnp::block_range::Builder<'_> {
    pub fn set(&mut self, from_block: u64, to_block: Option<u64>) -> Result<(), capnp::Error> {
        self.reborrow().set_from_block(from_block);

        if let Some(to_block) = to_block {
            let mut to_block_builder = self.reborrow().init_to_block();
            to_block_builder.set_value(to_block)
        }

        Ok(())
    }
}

impl request::Builder<'_> {
    pub fn build_full_query_from_query(
        &mut self,
        query: &Query,
        should_cache: bool,
    ) -> Result<(), capnp::Error> {
        let mut block_range_builder = self.reborrow().init_block_range();
        block_range_builder.set(query.from_block, query.to_block)?;

        let mut query_body_builder = self.reborrow().init_body().init_query();
        query_body_builder.build_from_query(query)?;

        self.set_should_cache(should_cache);

        Ok(())
    }

    pub fn build_query_id_from_query(&mut self, query: &Query) -> Result<(), capnp::Error> {
        self.reborrow()
            .init_block_range()
            .set(query.from_block, query.to_block)?;

        let id = QueryId::from_query(query)?;
        self.reborrow().init_body().set_query_id(id.0.as_slice());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        log::LogField, BlockFilter, BlockSelection, CapnpBuilder, FieldSelection, LogFilter,
        LogSelection,
    };

    use super::*;

    #[test]
    fn test_query_id() {
        let query = Query {
            logs: vec![Default::default()].into_iter().collect(),
            field_selection: FieldSelection {
                log: LogField::all(),
                ..Default::default()
            },
            ..Default::default()
        };

        let query_id = QueryId::from_query(&query).unwrap();
        println!("{query_id:?}");
    }

    #[test]
    fn test_needs_canonicalization_for_hashing() {
        fn add_log_selection(query_body_builder: &mut query_body::Builder) {
            let mut logs_builder = query_body_builder.reborrow().init_logs(1).get(0);
            LogSelection::from(LogFilter {
                address: vec![FixedSizeData::<20>::from([1u8; 20])],
                ..Default::default()
            })
            .populate_builder(&mut logs_builder)
            .unwrap();
        }
        fn add_block_selection(query_body_builder: &mut query_body::Builder) {
            let mut block_selection_builder = query_body_builder.reborrow().init_blocks(1).get(0);
            BlockSelection::from(BlockFilter {
                hash: vec![FixedSizeData::<32>::from([1u8; 32])],
                ..Default::default()
            })
            .populate_builder(&mut block_selection_builder)
            .unwrap();
        }
        let (hash_a, hash_a_canon) = {
            let mut message = Builder::new_default();
            let mut query_body_builder = message.init_root::<query_body::Builder>();
            add_log_selection(&mut query_body_builder);
            add_block_selection(&mut query_body_builder);

            let mut message_canon = Builder::new_default();
            message_canon
                .set_root_canonical(query_body_builder.into_reader())
                .unwrap();

            let mut buf = Vec::new();
            capnp::serialize::write_message(&mut buf, &message).unwrap();
            let hash = xxhash_rust::xxh3::xxh3_128(&buf);
            let mut buf = Vec::new();
            capnp::serialize::write_message(&mut buf, &message_canon).unwrap();
            let hash_canon = xxhash_rust::xxh3::xxh3_128(&buf);
            (hash, hash_canon)
        };

        let (hash_b, hash_b_canon) = {
            let mut message = Builder::new_default();
            let mut query_body_builder = message.init_root::<query_body::Builder>();
            // Insert block then log (the opposite order), allocater will not canonicalize
            add_block_selection(&mut query_body_builder);
            add_log_selection(&mut query_body_builder);

            let mut message_canon = Builder::new_default();
            message_canon
                .set_root_canonical(query_body_builder.into_reader())
                .unwrap();

            let mut buf = Vec::new();
            capnp::serialize::write_message(&mut buf, &message).unwrap();
            let hash = xxhash_rust::xxh3::xxh3_128(&buf);
            let mut buf = Vec::new();
            capnp::serialize::write_message(&mut buf, &message_canon).unwrap();
            let hash_canon = xxhash_rust::xxh3::xxh3_128(&buf);
            (hash, hash_canon)
        };
        assert_ne!(
            hash_a, hash_b,
            "queries should be different since they are not canonicalized"
        );

        assert_eq!(
            hash_a_canon, hash_b_canon,
            "queries should be the same since they are canonicalized"
        );
    }
}
