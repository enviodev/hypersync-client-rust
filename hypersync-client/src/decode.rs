use alloy_dyn_abi::{DecodedEvent, DynSolEvent, Specifier};
use anyhow::{anyhow, Context, Result};

pub struct Decoder {
    // A map of topic0 => Event decoder
    map: Vec<(Vec<u8>, DynSolEvent)>,
}

impl Decoder {
    pub fn from_signatures(signatures: &[String]) -> Result<Self> {
        let mut map = signatures
            .iter()
            .map(|sig| {
                let event = alloy_json_abi::Event::parse(sig).context("parse event signature")?;
                let topic0 = event.selector().to_vec();
                let event = event.resolve().context("resolve event")?;
                Ok((topic0, event))
            })
            .collect::<Result<Vec<_>>>()
            .context("construct event decoder map")?;

        map.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        let initial_len = map.len();

        map.dedup_by(|a, b| a.0 == b.0);

        let new_len = map.len();

        if initial_len != new_len {
            return Err(anyhow!(
                "duplicate event signature selectors (topic0) found.
This might be because the 'indexed' keyword doesn't effect the selector of an event signature."
            ));
        }

        Ok(Self { map })
    }

    pub fn decode(
        &self,
        topic0: &[u8],
        topics: &[Option<&[u8]>],
        data: &[u8],
    ) -> Result<Option<DecodedEvent>> {
        let event = match self.map.iter().find(|e| e.0 == topic0) {
            Some(event) => &event.1,
            None => return Ok(None),
        };

        let topics = topics
            .iter()
            .filter_map(|&t| t.map(|t| t.try_into().unwrap()));

        let decoded = event
            .decode_log_parts(topics, data, false)
            .context("decode log parts")?;

        Ok(Some(decoded))
    }
}
