# Installation

## Pre-built binaries

Download the latest release for your platform from [GitHub Releases](https://github.com/connve/flowgen/releases):

| Platform | Architecture | Archive |
|---|---|---|
| Linux | `x86_64` | `flowgen-linux-amd64-VERSION.tar.gz` |
| Linux | `ARM64` | `flowgen-linux-arm64-VERSION.tar.gz` |
| macOS | `Intel` | `flowgen-darwin-amd64-VERSION.tar.gz` |
| macOS | `Apple Silicon` | `flowgen-darwin-arm64-VERSION.tar.gz` |

```bash
# Example: Linux x86_64, replace version as needed
VERSION=0.117.0
curl -LO "https://github.com/connve/flowgen/releases/download/v${VERSION}/flowgen-linux-amd64-${VERSION}.tar.gz"
tar -xzf "flowgen-linux-amd64-${VERSION}.tar.gz"
sudo mv flowgen /usr/local/bin/
```

## From source

Requires [Rust](https://rustup.rs) 1.88+ and `protoc` (the Protocol Buffers compiler).

```bash
# macOS
brew install protobuf

# Ubuntu / Debian
sudo apt install -y protobuf-compiler

# Verify
protoc --version
```

Then build and install:

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
helm repo add connve https://helm.connve.com
helm install flowgen connve/flowgen
```

See the [Helm chart values](https://github.com/connve/flowgen/blob/main/charts/flowgen/values.yaml) for configuration options.

## Verify

```bash
flowgen --version
flowgen --help
```
