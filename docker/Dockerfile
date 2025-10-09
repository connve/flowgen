# Get service name as build arg.
FROM rust AS builder
ARG SERVICE_NAME

# Set the working directory in the container to /app.
WORKDIR /app

# Copy dependencies and install them.
COPY ./ ./
RUN mkdir .cargo
RUN echo "[source.vendored-sources]\ndirectory = \"vendor\"\n\n[source.\"git+https://gitlab.com/connve/platform/flowgen.git\"]\ngit = \"https://gitlab.com/connve/platform/flowgen.git\"\nreplace-with = \"vendored-sources\"" > .cargo/config
RUN apt update -qq
RUN apt-get -qq install pkg-config libssl-dev protobuf-compiler
RUN cargo build --release --bin $SERVICE_NAME
RUN cp ./target/release/$SERVICE_NAME /bin/$SERVICE_NAME

FROM gcr.io/distroless/cc AS final
ARG SERVICE_NAME

COPY --from=builder /bin/$SERVICE_NAME /bin/