# Get service name as build arg.
FROM rust:alpine AS builder
ARG SERVICE_NAME

# Set the working directory in the container to /app.
WORKDIR /app

# Install build dependencies.
RUN apk add --no-cache musl-dev pkgconfig openssl-dev openssl-libs-static protobuf-dev

# Copy dependencies and install them.
COPY ./ ./
RUN mkdir -p .cargo && \
    printf '[source.vendored-sources]\n\
directory = "vendor"\n\
\n\
[source."git+https://gitlab.com/connve/platform/flowgen.git"]\n\
git = "https://gitlab.com/connve/platform/flowgen.git"\n\
replace-with = "vendored-sources"\n' > .cargo/config.toml
RUN cargo build --release --bin $SERVICE_NAME
RUN cp ./target/release/$SERVICE_NAME /bin/$SERVICE_NAME

FROM gcr.io/distroless/cc-debian12:nonroot AS final
ARG SERVICE_NAME

COPY --from=builder /bin/$SERVICE_NAME /bin/