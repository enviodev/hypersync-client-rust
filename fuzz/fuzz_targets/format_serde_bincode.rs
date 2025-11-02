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
    let bin = bincode::serialize(&data).unwrap();

    let out: Input = bincode::deserialize(&bin).unwrap();

    assert_eq!(out, data);

    let out_bin = bincode::serialize(&out).unwrap();

    assert_eq!(&bin, &out_bin);
});
