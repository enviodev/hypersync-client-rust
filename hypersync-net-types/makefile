.PHONY: capnp-build

capnp-build:
	cargo install capnpc
	capnp compile -orust:src hypersync_net_types.capnp
	rustfmt -- src/hypersync_net_types_capnp.rs
