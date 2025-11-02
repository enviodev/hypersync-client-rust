#![no_main]

use libfuzzer_sys::fuzz_target;

use serde::{Deserialize, Serialize};

use hypersync_format as f;

#[derive(arbitrary::Arbitrary, PartialEq, Debug, Serialize, Deserialize)]
struct Input {
    block_with_tx: f::Block<f::Transaction>,
    block_with_hashes: f::Block<f::Hash>,
    trace: f::Trace,
}

fuzz_target!(|data: Input| {
    let json = serde_json::to_vec(&data).unwrap();

    let out: Input = serde_json::from_slice(&json).unwrap();

    assert_eq!(out, data);

    let out_json = serde_json::to_vec(&out).unwrap();

    assert_eq!(&json, &out_json);
});
