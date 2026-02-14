# Rust base image with cargo-chef and sccache.
FROM rust:bookworm AS rust_base
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    git \
    && rm -rf /var/lib/apt/lists/*
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
    cargo chef cook --release --features "$CARGO_FEATURES" --recipe-path recipe.json

# Copy source and build application.
COPY ./ ./
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/sccache,sharing=locked \
    cargo build --release --bin $SERVICE_NAME && \
    cp ./target/release/$SERVICE_NAME /bin/$SERVICE_NAME && \
    sccache --show-stats

# Final runtime image.
FROM gcr.io/distroless/cc-debian12:nonroot AS final
ARG SERVICE_NAME
COPY --from=builder /bin/$SERVICE_NAME /bin/