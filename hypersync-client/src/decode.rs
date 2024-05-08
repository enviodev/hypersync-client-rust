use std::collections::HashMap as StdHashMap;

use alloy_dyn_abi::{DecodedEvent, DynSolEvent, DynSolType, Specifier};
use alloy_json_abi::JsonAbi;
use anyhow::{anyhow, Context, Result};
use hypersync_format::Hex;
use polars_arrow::array::BinaryViewArray;
use xxhash_rust::xxh3::Xxh3Builder;

use crate::ArrowBatch;

pub type FastMap<K, V> = StdHashMap<K, V, Xxh3Builder>;

pub struct Decoder {
    contracts: FastMap<Vec<u8>, FastMap<Vec<u8>, DynSolEvent>>,
}

impl Decoder {
    pub fn new(json_abis: &[(hypersync_format::Address, JsonAbi)]) -> Result<Self> {
        let mut contracts = FastMap::default();

        for (addr, abi) in json_abis.iter() {
            let mut event_map = FastMap::default();

            for (_, events) in abi.events.iter() {
                for event in events {
                    event_map.insert(
                        event.selector().to_vec(),
                        event.resolve().context("resolve event")?,
                    );
                }
            }

            if contracts.insert(addr.to_vec(), event_map).is_some() {
                return Err(anyhow!("duplicate contract address {}", addr.encode_hex()));
            }
        }

        Ok(Self { contracts })
    }

    #[inline]
    pub fn decode(
        &self,
        address: &[u8],
        topic0: &[u8],
        topics: &[Option<&[u8]>],
        data: &[u8],
    ) -> Result<Option<DecodedEvent>> {
        let contract = match self.contracts.get(address) {
            Some(contract) => contract,
            None => return Ok(None),
        };
        let event = match contract.get(topic0) {
            Some(event) => event,
            None => return Ok(None),
        };

        let topics = topics
            .iter()
            .filter_map(|&t| t.map(|t| t.try_into().unwrap()));

        // Check if we are decoding a single u256 and the body is empty
        //
        // This case can happen when decoding zero value erc20 transfers
        let decoded = if data.is_empty() && event.body() == [DynSolType::Uint(256)] {
            event
                .decode_log_parts(topics, [0; 32].as_slice(), false)
                .context("decode log parts")?
        } else {
            event
                .decode_log_parts(topics, data, false)
                .context("decode log parts")?
        };

        Ok(Some(decoded))
    }

    pub fn decode_logs(&self, logs: &[ArrowBatch]) -> Result<Option<Vec<Option<DecodedEvent>>>> {
        let mut events = Vec::new();

        for batch in logs {
            let address = match batch.column::<BinaryViewArray>("address") {
                Ok(address) => address,
                Err(_) => return Ok(None),
            };
            let data = match batch.column::<BinaryViewArray>("data") {
                Ok(data) => data,
                Err(_) => return Ok(None),
            };
            let topic0 = match batch.column::<BinaryViewArray>("topic0") {
                Ok(topic0) => topic0,
                Err(_) => return Ok(None),
            };
            let topic1 = match batch.column::<BinaryViewArray>("topic1") {
                Ok(topic1) => topic1,
                Err(_) => return Ok(None),
            };
            let topic2 = match batch.column::<BinaryViewArray>("topic2") {
                Ok(topic2) => topic2,
                Err(_) => return Ok(None),
            };
            let topic3 = match batch.column::<BinaryViewArray>("topic3") {
                Ok(topic3) => topic3,
                Err(_) => return Ok(None),
            };

            for (((((address, data), topic0), topic1), topic2), topic3) in address
                .values_iter()
                .zip(data.values_iter())
                .zip(topic0.iter())
                .zip(topic1.iter())
                .zip(topic2.iter())
                .zip(topic3.iter())
            {
                let topic0 = match topic0 {
                    Some(topic0) => topic0,
                    None => {
                        events.push(None);
                        continue;
                    }
                };

                let decoded = self
                    .decode(
                        address,
                        topic0,
                        &[Some(topic0), topic1, topic2, topic3],
                        data,
                    )
                    .context("decode event")?;

                events.push(decoded);
            }
        }

        Ok(Some(events))
    }
}
