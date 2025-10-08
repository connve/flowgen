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