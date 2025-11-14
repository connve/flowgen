# Flowgen Helm Charts

This directory contains Helm charts for deploying Flowgen on Kubernetes.

## Usage

### Add the Helm repository

```bash
helm repo add flowgen https://connve-dev.github.io/flowgen
helm repo update
```

### Install the chart

```bash
helm install flowgen flowgen/flowgen \
  --namespace flowgen \
  --create-namespace \
  -f values.yaml
```

### Install from local chart (for development)

```bash
helm install flowgen ./charts/flowgen \
  --namespace flowgen \
  --create-namespace \
  -f values.yaml
```

## Available Charts

- **flowgen** - Data activation with a blast 💥

## Configuration

See the [flowgen chart values](./flowgen/values.yaml) for available configuration options.

## Development

To test chart changes locally:

```bash
# Lint the chart
helm lint charts/flowgen

# Template the chart
helm template flowgen charts/flowgen -f your-values.yaml

# Install locally
helm install flowgen charts/flowgen --namespace flowgen --create-namespace
```

## Releases

Charts are automatically released to GitHub Pages when changes are pushed to the `main` branch.
The Helm repository is available at: `https://connve-dev.github.io/flowgen`
