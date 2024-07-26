use crate::simple_types::Log;
use alloy_dyn_abi::{DecodedEvent, DynSolEvent, Specifier};
use anyhow::{Context, Result};
use hypersync_format::LogArgument;
use std::collections::HashMap;

#[derive(Debug, Hash, Eq, PartialEq)]
struct EventKey {
    topic0: Vec<u8>,
    num_topics: usize,
}

type DecoderMap = HashMap<EventKey, DynSolEvent>;

/// Decode logs parsing topics and log data.
pub struct Decoder {
    // A map of topic0 => Event decoder
    map: DecoderMap,
}

impl Decoder {
    /// Initialize decoder from event signatures.
    ///
    ///     use hypersync_client::Decoder;
    ///     let decoder = Decoder::from_signatures(&[
    ///        "Transfer(address indexed from, address indexed to, uint amount)",
    ///     ]).unwrap();
    pub fn from_signatures<S: AsRef<str>>(signatures: &[S]) -> Result<Self> {
        let map: DecoderMap = signatures
            .iter()
            .map(|sig| {
                let event =
                    alloy_json_abi::Event::parse(sig.as_ref()).context("parse event signature")?;
                let topic0 = event.selector().to_vec();
                let num_topics = event.num_topics();
                let event_key = EventKey { topic0, num_topics };
                let event = event.resolve().context("resolve event")?;
                Ok((event_key, event))
            })
            .collect::<Result<DecoderMap>>()
            .context("construct event decoder map")?;

        Ok(Self { map })
    }

    /// Parse log and return decoded event.
    ///
    /// Returns Ok(None) if topic0 not found.
    pub fn decode_log(&self, log: &Log) -> Result<Option<DecodedEvent>> {
        let topic0 = log
            .topics
            .first()
            .context("get topic0")?
            .as_ref()
            .context("get topic0")?;
        let data = log.data.as_ref().context("get log.data")?;
        self.decode(topic0.as_slice(), &log.topics, data)
    }

    /// Decode log.data into event using parsed topic0 and topics.
    pub fn decode(
        &self,
        topic0: &[u8],
        topics: &[Option<LogArgument>],
        data: &[u8],
    ) -> Result<Option<DecodedEvent>> {
        let event_key = EventKey {
            topic0: topic0.into(),
            num_topics: topics.iter().fold(
                0,
                |accum, topic| {
                    if topic.is_some() {
                        accum + 1
                    } else {
                        accum
                    }
                },
            ),
        };

        let event = match self.map.get(&event_key) {
            Some(event) => event,
            None => return Ok(None),
        };

        let topics = topics
            .iter()
            .take_while(|t| t.is_some())
            .map(|t| t.as_ref().unwrap().into());

        let decoded = event
            .decode_log_parts(topics, data, false)
            .context("decode log parts")?;

        Ok(Some(decoded))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simple_types::Log;
    use alloy_dyn_abi::{DynSolType, DynSolValue};
    use alloy_primitives::Signed;
    use hypersync_format::{Data, FixedSizeData, Hex};

    #[test]
    fn test_decode_event_with_bytes() {
        let signature = alloy_json_abi::Event::parse(
            "CommitmentStored(bytes32 indexed commitmentIndex, address bidder, address commiter, \
             uint256 bid, uint64 blockNumber, bytes32 bidHash, uint64 decayStartTimeStamp, uint64 \
             decayEndTimeStamp, string txnHash, string revertingTxHashes, bytes32 commitmentHash, \
             bytes bidSignature, bytes commitmentSignature, uint64 dispatchTimestamp, bytes \
             sharedSecretKey)",
        )
        .unwrap();
        let decoder = signature.resolve().unwrap();
        let decoder = DynSolType::Tuple(decoder.body().to_vec());
        let data = "0x0000000000000000000000006875d4607c6cb4dfce1300545ab91a4005e33fd00000000000000000000000008280f34750068c67acf5366a5c7caea554c36fb5000000000000000000000000000000000000000000000000001b432e3907129c00000000000000000000000000000000000000000000000000000000001e3cc984c827ef3f2d18d8adca7259b89fde393749d3c2286bcd3e22d7dc8b46166d0b00000000000000000000000000000000000000000000000000000190dc7b0a0b00000000000000000000000000000000000000000000000000000190dc7b488b00000000000000000000000000000000000000000000000000000000000001c00000000000000000000000000000000000000000000000000000000000000220a7bf0040bf8800e406be82addb4f1bdc926ab7b7e4634d6cba572c6a981624ee000000000000000000000000000000000000000000000000000000000000024000000000000000000000000000000000000000000000000000000000000002c000000000000000000000000000000000000000000000000000000190dc7b2ffd000000000000000000000000000000000000000000000000000000000000034000000000000000000000000000000000000000000000000000000000000000403433666339623636366532613764306462366235313763306364313439386665366361646261373135376331353038303764616332326633376136326165656300000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000041dba6cf2d4520c006bbe215eba31cbb3c26f834e6f0eeae8d29c6b86613361930618bc734e88450af5d017aeca32b0ad007368fb6699802501f1260d6a92162611b00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004179c7644cf559d284118d09b2f440f19ec6a9bc8138425553229493ac0c2fc12a796d10a77958f6c007ed50d73fa2cf526d0d50ad66c824965681b37a7b3b50b51c0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000203a99c7cbb18c68ce8c92ff5ee2087c35b41cf6a0c8412eeef82f5e40c3fbbc90";
        let data = Data::decode_hex(data).unwrap();
        let res = decoder.abi_decode_sequence(&data).unwrap();
        dbg!(res.as_tuple().unwrap().len());
        for v in res.as_tuple().unwrap().iter() {
            dbg!(v);
            if let DynSolValue::Bytes(s) = v {
                dbg!(Data::from(s.as_slice()).encode_hex());
            }
        }
    }
    #[test]
    fn decodes_i24_event() {
        //https://basescan.org/tx/0x76aeccc2815612c23344557c07fff57aada63625f1977096d5e9c88f63c257a7#eventlog#176
        //This event was decoding tickLower incorrectly in a users indexer. Setup this test to
        //validate decoder is working as expected
        let decoder = Decoder::from_signatures(&["event Mint(address sender, address indexed \
                                                  owner, int24 indexed tickLower, int24 indexed \
                                                  tickUpper, uint128 amount, uint256 amount0, \
                                                  uint256 amount1)"])
        .unwrap();

        let log = Log {
         removed: None,
         log_index: Some(176.into()),
         transaction_index: Some(0.into()),
         transaction_hash: Some(FixedSizeData::decode_hex("0x76aeccc2815612c23344557c07fff57aada63625f1977096d5e9c88f63c257a7").unwrap()),
         block_hash: Some(FixedSizeData::decode_hex("0xd83042b6a32dc9b18d4c7c9819b914bc04470f77458e7276cb72f3d8fde5eb3d").unwrap()),
         block_number: Some(13899663.into()),
         address: Some(FixedSizeData::decode_hex("0x98c7A2338336d2d354663246F64676009c7bDa97").unwrap()),
         data: Some(Data::decode_hex("0x000000000000000000000000827922686190790b37229fd06084350e74485b72000000000000000000000000000000000000000000000000000000000bebae76000000000000000000000000000000000000000000000000000000000000270f000000000000000000000000000000000000000000000000000000000000270f").unwrap()),
         topics: vec![
             "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde", 
             "0x000000000000000000000000827922686190790b37229fd06084350e74485b72", 
             "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", 
             "0x0000000000000000000000000000000000000000000000000000000000000001"
         ].into_iter().map(|s|FixedSizeData::decode_hex(s).ok()).collect(),
        };

        let decoded = decoder.decode_log(&log).unwrap().unwrap();

        let owner = decoded.indexed[0].clone();
        let tick_lower = decoded.indexed[1].clone();
        let tick_upper = decoded.indexed[2].clone();
        assert_eq!(
            owner,
            DynSolValue::Address(
                "0x827922686190790b37229fd06084350E74485b72"
                    .parse()
                    .unwrap()
            )
        );
        assert_eq!(tick_lower, DynSolValue::Int(Signed::MINUS_ONE, 24));
        assert_eq!(tick_upper, DynSolValue::Int(Signed::ONE, 24));
    }
}
