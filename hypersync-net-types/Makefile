.PHONY: generate_capnp_types clean_generated_capnp_types

generate_capnp_types:
	DOCKER_BUILDKIT=1 docker build -f capnp-codegen.Dockerfile --target export \
		--output type=local,dest=src/__generated__ .

clean_generated_capnp_types:
	rm -f src/__generated__/hypersync_net_types_capnp.rs
