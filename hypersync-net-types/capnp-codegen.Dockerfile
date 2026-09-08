# Pinned toolchain for deterministic Cap'n Proto code generation. Generating
# inside this image keeps the committed output byte-for-byte reproducible across
# developer machines and CI, regardless of the host's capnp/rustfmt versions.
FROM rust:1.94-slim-trixie@sha256:cf09adf8c3ebaba10779e5c23ff7fe4df4cccdab8a91f199b0c142c53fef3e1a AS codegen

# Must stay compatible with the `capnp` runtime dependency in Cargo.toml.
ARG CAPNPC_VERSION=0.27.0
# The capnp compiler version affects the generated schema blobs, so pin it too.
ARG CAPNPROTO_VERSION=1.1.0-2

RUN apt-get update \
    && apt-get install -y --no-install-recommends "capnproto=${CAPNPROTO_VERSION}" \
    && rm -rf /var/lib/apt/lists/*
RUN rustup component add rustfmt
RUN cargo install capnpc --version "=${CAPNPC_VERSION}" --locked

WORKDIR /work
COPY hypersync_net_types.capnp .
RUN mkdir -p out \
    && capnp compile hypersync_net_types.capnp -o rust:out \
    && rustfmt out/hypersync_net_types_capnp.rs

# Minimal stage so `docker build --output` exports only the generated file.
FROM scratch AS export
COPY --from=codegen /work/out/hypersync_net_types_capnp.rs /
