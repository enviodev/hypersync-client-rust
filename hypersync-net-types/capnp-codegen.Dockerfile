# Pinned toolchain for deterministic Cap'n Proto code generation. Running codegen
# inside this image keeps the committed output byte-for-byte reproducible across
# developer machines and CI, regardless of the host's capnp/rustfmt versions.
FROM rust:1.94-slim-bookworm

# Must stay compatible with the `capnp` runtime dependency in Cargo.toml.
ARG CAPNPC_VERSION=0.23.2

RUN apt-get update \
    && apt-get install -y --no-install-recommends capnproto \
    && rm -rf /var/lib/apt/lists/*
RUN rustup component add rustfmt
RUN cargo install capnpc --version "=${CAPNPC_VERSION}" --locked

WORKDIR /work
