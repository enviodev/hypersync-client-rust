# hypersync-net-types

This crate contains the capnp schema for the hypersync network protocol.

## Building

To build the schema, run the following command:

```bash
cargo build --features capnp-build
```

This will generate the `hypersync_net_types_capnp.rs` file in the `src` directory.

The build script will not run on a normal build, so you will need to run it manually if you make changes to the schema.

The ci tests will fail if the schema is not up to date.

This is to avoid the need to have a capnp compiler installed on the consumers of this crate.
