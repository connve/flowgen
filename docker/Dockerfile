# Rust base image with cargo-chef and sccache. The channel comes from
# `rust-toolchain.toml`; `rustup` inside this image resolves it at the
# first `cargo` invocation once the source is copied in.
FROM rust:slim-bookworm AS rust_base

# Base system dependencies required to build native crates (rdkafka, openssl,
# protobuf). Kept in a separate layer because they change rarely.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    libssl-dev \
    libcurl4-openssl-dev \
    cmake \
    protobuf-compiler \
    git \
    ca-certificates \
    curl \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# Node is required by `flowgen/app/build.rs`. Installed separately so updates
# to Node do not invalidate the cached system dependency layer above.
RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

# Rust build tooling. Kept in its own layer so cargo-chef/sccache version
# bumps do not invalidate the (much slower) apt layers.
RUN cargo install cargo-chef --version 0.1.68
RUN cargo install sccache --version 0.8.2
ENV RUSTC_WRAPPER=sccache
ENV SCCACHE_DIR=/sccache
WORKDIR /app

# Planner stage - analyze dependencies.
FROM rust_base AS planner
COPY ./ ./
RUN cargo chef prepare --recipe-path recipe.json

# Builder stage - compile with caching.
FROM rust_base AS builder
ARG SERVICE_NAME
ARG CARGO_FEATURES=""

# Copy recipe and cook dependencies (cached layer).
COPY --from=planner /app/recipe.json recipe.json
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/sccache,sharing=locked \
    --mount=type=cache,target=/tmp \
    cargo chef cook --release --locked --features "$CARGO_FEATURES" --recipe-path recipe.json

# Copy source and build application.
COPY ./ ./
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/sccache,sharing=locked \
    --mount=type=cache,target=/tmp \
    cargo build --release --locked --bin $SERVICE_NAME && \
    cp ./target/release/$SERVICE_NAME /bin/$SERVICE_NAME && \
    sccache --show-stats

# Final runtime image.
FROM gcr.io/distroless/cc-debian12:nonroot AS final
ARG SERVICE_NAME
COPY --from=builder /bin/$SERVICE_NAME /bin/
ENTRYPOINT ["/bin/flowgen"]