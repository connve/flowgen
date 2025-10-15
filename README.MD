[![CI](https://github.com/connve-dev/flowgen/actions/workflows/test.yml/badge.svg)](https://github.com/connve-dev/flowgen/actions/workflows/test.yml) [![License: MPL 2.0](https://img.shields.io/badge/License-MPL%202.0-brightgreen.svg)](https://opensource.org/licenses/MPL-2.0) [![GitHub release](https://img.shields.io/github/v/release/connve-dev/flowgen)](https://github.com/connve-dev/flowgen/releases)

This is Flowgen - our platform to run streaming jobs with a blast.

The easiest way to get start is to run Flowgen as cargo run.

## Contributors

### Setup Development Environment

After cloning the repository, set up Git hooks to ensure code quality:

```bash
git config core.hooksPath .githooks
```

This will enable pre-commit hooks that automatically:
- Format code with `cargo fmt`
- Run `cargo clippy` to catch common issues
- Check for security vulnerabilities with `cargo audit` (if installed)