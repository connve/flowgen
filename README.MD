# Flowgen

[![CI](https://github.com/connve/flowgen/actions/workflows/test.yml/badge.svg)](https://github.com/connve/flowgen/actions/workflows/test.yml) [![License: MPL 2.0](https://img.shields.io/badge/License-MPL%202.0-brightgreen.svg)](https://opensource.org/licenses/MPL-2.0) [![GitHub release](https://img.shields.io/github/v/release/connve/flowgen)](https://github.com/connve/flowgen/releases)

Flowgen is a configuration-based data activation workflow application written in Rust.

## Overview

Flowgen executes data activation workflows defined in YAML configuration files. A flow consists of sequential tasks with support for error handling and retries. The system supports integrations with object stores, HTTP APIs, Salesforce, and other data sources.

## Deployment

### Kubernetes

Install using Helm:

```bash
helm repo add connve https://helm.connve.com
helm repo update
helm install flowgen connve/flowgen --namespace flowgen --create-namespace
```

Chart configuration options are documented in [charts/flowgen/values.yaml](charts/flowgen/values.yaml).

### Docker

```bash
docker pull ghcr.io/connve-labs/flowgen:latest

docker run \
  -v $(pwd)/config.yaml:/etc/app/config.yaml \
  -v $(pwd)/flows:/etc/app/flows \
  ghcr.io/connve-labs/flowgen:latest
```

### Local Development

1. Copy `config.example.yaml` to `config.yaml` and configure as needed
2. Create flow definitions in the flows directory
3. Update the flow path in `config.yaml`
4. Run with `cargo run`

The repo pins its Rust toolchain via `rust-toolchain.toml` — `rustup`
picks it up automatically, so `cargo` in this checkout uses the same
`stable` version CI enforces (no need to remember `+stable` flags).

Enable the shared pre-commit hook (fmt + clippy + tests + audit) after
cloning:

```bash
git config --local core.hooksPath .githooks
```

The hook lives in [`.githooks/pre-commit`](.githooks/pre-commit) and is
versioned with the repo so all contributors run the same checks.

### Testing

```bash
cargo test                        # unit tests
cargo test -- --ignored           # integration tests (requires Docker)
cargo test -- --include-ignored   # both
```

Tests that spin up real containers (via
[`testcontainers`](https://docs.rs/testcontainers)) or depend on a
running Docker daemon are marked `#[ignore]` so a plain
`cargo test` stays fast and works on machines without Docker. CI
runs the ignored set in a dedicated `test-integration` job.

## Configuration

- `config.yaml`: Application configuration
- Flow files: YAML files defining workflow tasks

See `config.example.yaml` for configuration reference.

## Examples

Example flow configurations are available in the [`examples/`](examples/) directory.

## License

Licensed under the [Mozilla Public License 2.0](LICENSE).


