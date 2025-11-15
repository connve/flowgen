# Flowgen

[![CI](https://github.com/connve-dev/flowgen/actions/workflows/test.yml/badge.svg)](https://github.com/connve-dev/flowgen/actions/workflows/test.yml) [![License: MPL 2.0](https://img.shields.io/badge/License-MPL%202.0-brightgreen.svg)](https://opensource.org/licenses/MPL-2.0) [![GitHub release](https://img.shields.io/github/v/release/connve-dev/flowgen)](https://github.com/connve-dev/flowgen/releases)

Flowgen is a configuration-based data activation workflow application written in Rust.

## Overview

Flowgen executes data activation workflows defined in YAML configuration files. A flow consists of sequential tasks with support for error handling and retries. The system supports integrations with object stores, HTTP APIs, Salesforce, and other data sources.

## Deployment

### Kubernetes

Install using Helm:

```bash
helm repo add connve https://connve-dev.github.io/helm
helm repo update
helm install flowgen connve/flowgen --namespace flowgen --create-namespace
```

Chart configuration options are documented in [charts/flowgen/values.yaml](charts/flowgen/values.yaml).

### Docker

```bash
docker pull ghcr.io/connve-dev/flowgen:latest

docker run \
  -v $(pwd)/config.yaml:/etc/app/config.yaml \
  -v $(pwd)/flows:/etc/app/flows \
  ghcr.io/connve-dev/flowgen:latest
```

### Local Development

1. Copy `config.example.yaml` to `config.yaml` and configure as needed
2. Create flow definitions in the flows directory
3. Update the flow path in `config.yaml`
4. Run with `cargo run`

## Configuration

- `config.yaml`: Application configuration
- Flow files: YAML files defining workflow tasks

See `config.example.yaml` for configuration reference.

## Examples

Example flow configurations are available in the [`examples/`](examples/) directory.

## License

Licensed under the [Mozilla Public License 2.0](LICENSE).


