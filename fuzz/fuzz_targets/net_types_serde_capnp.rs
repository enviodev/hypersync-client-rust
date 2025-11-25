#![no_main]

use hypersync_net_types::hypersync_net_types_capnp;
use hypersync_net_types::{request::QueryId, CapnpReader, Query};
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

    let mut message = capnp::message::Builder::new_default();
    let mut request_builder = message.init_root::<hypersync_net_types_capnp::request::Builder>();
    request_builder.build_query_id_from_query(&data).unwrap();

    let hypersync_net_types_capnp::request::body::Which::QueryId(Ok(bytes)) =
        request_builder.into_reader().get_body().which().unwrap()
    else {
        panic!("should have read query id");
    };

    let query_id = QueryId::from_query(&data).unwrap();
    let read_query_id = QueryId::from_query(&read_query).unwrap();
    assert_eq!(query_id, read_query_id, "query id should match built from reconstructed query");
    assert_eq!(
        query_id.0.as_slice(), bytes,
        "query id bytes in capnp body should match constructed query id"
    );
});
