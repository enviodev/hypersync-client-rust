use std::env;
fn main() {
    // if you want to rebuild the capnp schema, run this command
    // cargo build --features capnp-build
    if env::var("CARGO_FEATURE_CAPNP_BUILD").is_ok() {
        let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
        let output_path = format!("{manifest_dir}/src");
        capnpc::CompilerCommand::new()
            .file("hypersync_net_types.capnp")
            .output_path(output_path)
            // .raw_code_generator_request_path("./hypersync-net-types/src/capnp_schema")
            .run()
            .expect("compiling schema");
    }
}
