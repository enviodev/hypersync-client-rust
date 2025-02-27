.PHONY: capnp-build

capnp-build:
	cargo install capnpc
	capnpc -orust:src hypersync_net_types.capnp
	rustfmt -- src/hypersync_net_types_capnp.rs
