use crate::config::{AppConfig, FlowConfig};
use config::{Config, File};
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::{event, Level};

/// Errors that can occur during application execution.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    Glob(#[from] glob::GlobError),
    #[error(transparent)]
    Pattern(#[from] glob::PatternError),
    #[error(transparent)]
    Config(#[from] config::ConfigError),
    #[error("Invalid path")]
    InvalidPath,
}
/// Main application that loads and runs flows concurrently.
pub struct App {
    /// Global application configuration.
    pub config: AppConfig,
}

impl flowgen_core::task::runner::Runner for App {
    /// Loads flow configs, registers routes, starts server, then runs tasks.
    type Error = Error;
    async fn run(self) -> Result<(), Error> {
        let app_config = Arc::new(self.config);

        let glob_pattern = app_config
            .flows
            .dir
            .as_ref()
            .and_then(|path| path.to_str())
            .ok_or(Error::InvalidPath)?;

        let flow_configs: Vec<FlowConfig> = glob::glob(glob_pattern)?
            .map(|path| -> Result<FlowConfig, Error> {
                let path = path?;
                event!(Level::INFO, "Loading flow: {:?}", path);
                let config = Config::builder().add_source(File::from(path)).build()?;
                Ok(config.try_deserialize::<FlowConfig>()?)
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Create shared HTTP Server.
        let http_server = Arc::new(flowgen_http::server::HttpServer::new());

        // Initialize flows and register routes
        let mut flow_tasks = Vec::new();
        for config in flow_configs {
            let app_config = Arc::clone(&app_config);
            let http_server = Arc::clone(&http_server);

            let mut flow_builder = super::flow::FlowBuilder::new()
                .config(Arc::new(config))
                .http_server(http_server);

            if let Some(cache) = &app_config.cache {
                if cache.enabled {
                    flow_builder = flow_builder.cache_credentials_path(&cache.credentials_path);
                }
            }

            // Register routes and collect tasks
            let flow = match flow_builder.build() {
                Ok(flow) => flow,
                Err(e) => {
                    event!(Level::ERROR, "Flow build failed: {}", e);
                    continue;
                }
            };

            let flow = match flow.run().await {
                Ok(flow) => flow,
                Err(e) => {
                    event!(Level::ERROR, "{}", e);
                    continue;
                }
            };

            // Collect tasks for concurrent execution
            if let Some(tasks) = flow.task_list {
                flow_tasks.extend(tasks);
            }
        }

        // Start server with registered routes
        let server_handle = tokio::spawn(async move {
            if let Err(e) = http_server.start_server().await {
                event!(Level::ERROR, "Failed to start HTTP Server: {}", e);
            }
        });

        // Run tasks concurrently with server
        let flow_handle = tokio::spawn(async move {
            handle_task_results(flow_tasks).await;
        });

        // Wait for server and tasks
        let (_, _) = tokio::join!(server_handle, flow_handle);

        Ok(())
    }
}

/// Processes task results and logs errors.
async fn handle_task_results(tasks: Vec<JoinHandle<Result<(), super::flow::Error>>>) {
    let task_results = futures_util::future::join_all(tasks).await;
    for result in task_results {
        log_task_error(result);
    }
}

/// Logs task execution and logic errors.
fn log_task_error(result: Result<Result<(), super::flow::Error>, tokio::task::JoinError>) {
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            event!(Level::ERROR, "{}", error);
        }
        Err(error) => {
            event!(Level::ERROR, "{}", error);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CacheOptions, FlowOptions};
    use flowgen_core::task::runner::Runner;
    use std::path::PathBuf;
    use tempfile::TempDir;
    use tokio::fs;

    fn create_test_app_config(flow_dir: Option<PathBuf>) -> AppConfig {
        AppConfig {
            cache: Some(CacheOptions {
                enabled: true,
                credentials_path: PathBuf::from("/tmp/cache"),
            }),
            flows: FlowOptions { dir: flow_dir },
        }
    }

    fn create_test_flow_config() -> String {
        r#"
        [flow]
        name = "test_flow"
        tasks = []
        "#
        .to_string()
    }

    #[tokio::test]
    async fn test_app_creation() {
        let config = create_test_app_config(Some(PathBuf::from("/test/flows/*")));
        let app = App { config };
        assert!(app.config.cache.is_some());
        assert!(app.config.flows.dir.is_some());
    }

    #[tokio::test]
    async fn test_invalid_path_error() {
        let config = AppConfig {
            cache: None,
            flows: FlowOptions { dir: None },
        };
        let app = App { config };

        let result = app.run().await;
        assert!(matches!(result, Err(Error::InvalidPath)));
    }

    #[tokio::test]
    async fn test_run_with_empty_flow_dir() {
        let temp_dir = TempDir::new().unwrap();
        let flow_pattern = temp_dir.path().join("*.toml");
        let config = create_test_app_config(Some(flow_pattern));
        let app = App { config };

        let result = app.run().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_run_with_valid_flow_config() {
        let temp_dir = TempDir::new().unwrap();
        let flow_file = temp_dir.path().join("test_flow.toml");

        fs::write(&flow_file, create_test_flow_config())
            .await
            .unwrap();

        let flow_pattern = temp_dir.path().join("*.toml");
        let config = create_test_app_config(Some(flow_pattern));
        let app = App { config };

        let result = app.run().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_run_with_cache_disabled() {
        let temp_dir = TempDir::new().unwrap();
        let flow_file = temp_dir.path().join("test_flow.toml");

        fs::write(&flow_file, create_test_flow_config())
            .await
            .unwrap();

        let flow_pattern = temp_dir.path().join("*.toml");
        let config = AppConfig {
            cache: Some(CacheOptions {
                enabled: false,
                credentials_path: PathBuf::from("/tmp/cache"),
            }),
            flows: FlowOptions {
                dir: Some(flow_pattern),
            },
        };
        let app = App { config };

        let result = app.run().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_run_with_no_cache() {
        let temp_dir = TempDir::new().unwrap();
        let flow_file = temp_dir.path().join("test_flow.toml");

        fs::write(&flow_file, create_test_flow_config())
            .await
            .unwrap();

        let flow_pattern = temp_dir.path().join("*.toml");
        let config = AppConfig {
            cache: None,
            flows: FlowOptions {
                dir: Some(flow_pattern),
            },
        };
        let app = App { config };

        let result = app.run().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_error_display() {
        let error = Error::InvalidPath;
        assert_eq!(error.to_string(), "Invalid path");
    }

    #[tokio::test]
    async fn test_error_from_glob_error() {
        let pattern_error = glob::Pattern::new("[").unwrap_err();
        let error = Error::from(pattern_error);
        assert!(matches!(error, Error::Pattern(_)));
    }
}
