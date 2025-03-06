.PHONY: capnp-build

capnp-build:
	# Ensure we are building with the same capnp (1.1.0) in all environments
	capnp --version | grep -q "1.1.0"
	cargo install capnpc
	capnpc -orust:src hypersync_net_types.capnp
	rustfmt -- src/hypersync_net_types_capnp.rs
