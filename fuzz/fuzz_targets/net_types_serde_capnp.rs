#![no_main]

use hypersync_net_types::hypersync_net_types_capnp;
use hypersync_net_types::{CapnpReader, Query};
use libfuzzer_sys::fuzz_target;

// Test if roundtripping the data through json produces same results
fuzz_target!(|data: Query| {
    let json = serde_json::to_vec(&data).unwrap();
    let out: Query = serde_json::from_slice(&json).unwrap();
    assert_eq!(out, data, "json should read to the same value as the query");
    let out_json = serde_json::to_vec(&out).unwrap();
    assert_eq!(&json, &out_json);

    let mut message = capnp::message::Builder::new_default();
    let mut request_builder = message.init_root::<hypersync_net_types_capnp::request::Builder>();
    request_builder
        .build_full_query_from_query(&data, true)
        .unwrap();

    let read_query = Query::from_reader(request_builder.into_reader()).unwrap();
    assert_eq!(
        data, read_query,
        "capnp should read to the same value as the query"
    );
});
