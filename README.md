# hypersync-client

[![CI](https://github.com/enviodev/hypersync-client-rust/actions/workflows/ci.yaml/badge.svg?branch=main)](https://github.com/enviodev/hypersync-client-rust/actions/workflows/ci.yaml)
<a href="https://crates.io/crates/hypersync-client">
<img src="https://img.shields.io/crates/v/hypersync-client.svg?style=flat-square"
    alt="Crates.io version" />
</a>

Rust crate for [Envio's](https://envio.dev/) HyperSync client.

[Documentation Page](https://docs.envio.dev/docs/hypersync-clients)

### Dependencies

In order to update the capnp schema on hypersync-net-types, you will need to install the `capnproto` tool.

#### Linux

```bash
apt-get install -y capnproto libcapnp-dev
```

#### Windows

```bash
choco install capnproto
```

#### MacOS

```bash
brew install capnp
```

## Building

To build the schema, run the following command:

```bash
cd hypersync-net-types
make capnp-build
```

This triggers the build script in hypersync-net-types, which will generate the `hypersync_net_types_capnp.rs` file in the `src` directory.
