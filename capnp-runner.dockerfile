# This Dockerfile is used for ci to have the latest version of Cap'n Proto (1.1.0) 
# built from source. Since ubuntu package manager is not up to date.

# Start with a base image
FROM ubuntu:22.04

# Set non-interactive mode to prevent prompts during installation
ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies
RUN apt update && apt install -y \
    build-essential \
    cmake \
    curl \
    git \
    sudo \
    && rm -rf /var/lib/apt/lists/*

# Download and install Cap'n Proto 1.1.0
WORKDIR /opt
RUN curl -O https://capnproto.org/capnproto-c++-1.1.0.tar.gz && \
    tar zxf capnproto-c++-1.1.0.tar.gz && \
    cd capnproto-c++-1.1.0 && \
    ./configure && \
    make -j$(nproc) check && \
    sudo make install && \
    cd .. && rm -rf capnproto-c++-1.1.0 capnproto-c++-1.1.0.tar.gz

# Set environment variables
ENV PATH="/usr/local/bin:$PATH"

# Keep the container running
CMD ["tail", "-f", "/dev/null"]
