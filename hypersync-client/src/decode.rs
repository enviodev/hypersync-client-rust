use alloy_dyn_abi::{DecodedEvent, DynSolEvent, Specifier};
use anyhow::{anyhow, Context, Result};
use hypersync_format::LogArgument;

use crate::simple_types::Log;

pub struct Decoder {
    // A map of topic0 => Event decoder
    map: Vec<(Vec<u8>, DynSolEvent)>,
}

impl Decoder {
    pub fn from_signatures<S: AsRef<str>>(signatures: &[S]) -> Result<Self> {
        let mut map = signatures
            .iter()
            .map(|sig| {
                let event =
                    alloy_json_abi::Event::parse(sig.as_ref()).context("parse event signature")?;
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

    pub fn decode(
        &self,
        topic0: &[u8],
        topics: &[Option<LogArgument>],
        data: &[u8],
    ) -> Result<Option<DecodedEvent>> {
        let event = match self.map.iter().find(|e| e.0 == topic0) {
            Some(event) => &event.1,
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
