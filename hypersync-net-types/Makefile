generate_capnp_types:
	capnp compile hypersync_net_types.capnp --output rust:./src/__generated__
	rustfmt src/__generated__/hypersync_net_types_capnp.rs
