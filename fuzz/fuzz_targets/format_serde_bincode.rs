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

// Test if roundtripping the data through bincode produces same results
fuzz_target!(|data: Input| {
    let bin = bincode::serialize(&data).expect("should serialize");

    let out: Input = bincode::deserialize(&bin).expect("should deserialize");

    assert_eq!(out, data, "deserialized data should match original");

    let out_bin = bincode::serialize(&out).expect("should serialize again");

    assert_eq!(&bin, &out_bin, "reserialized should match bin");
});
