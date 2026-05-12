# Installation

## From source

Requires [Rust](https://rustup.rs) 1.80+.

```bash
git clone https://github.com/connve/flowgen.git
cd flowgen
cargo install --path flowgen/app
```

This installs the `flowgen` binary to `~/.cargo/bin/`. Make sure it's in your `PATH`.

```bash
flowgen --config config.yaml
```

## Docker

```bash
docker run -v $(pwd)/config.yaml:/etc/app/config.yaml \
           -v $(pwd)/flows:/etc/app/flows \
           ghcr.io/connve/flowgen:latest \
           --config /etc/app/config.yaml
```

## Kubernetes

Add the Helm repository and install:

```bash
helm install flowgen oci://ghcr.io/connve/charts/flowgen
```

See the [Helm chart values](https://github.com/connve/flowgen/blob/main/charts/flowgen/values.yaml) for configuration options.

## Verify

```bash
flowgen --version
flowgen --help
```
