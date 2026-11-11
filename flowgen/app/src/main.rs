use clap::Parser;
use flowgen::app::App;
use flowgen::config::AppConfig;
use flowgen::local::Workspace;
use flowgen_core::flow::activity::{MetricsStore, OtlpMetricsStore};
use flowgen_core::flow::activity_layer::FlowActivityLayer;
use flowgen_core::telemetry::query::MemoryLogsStoreWriter;
use std::env;
use std::io::IsTerminal;
use std::process;
use std::sync::Arc;
use tokio::sync::oneshot;
use tracing::{error, info, warn};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[derive(Parser)]
#[command(
    name = "flowgen",
    version,
    about = "Data activation with a blast 💥",
    subcommand_negates_reqs = true
)]
struct Cli {
    /// Path to configuration file. Without one, flowgen runs on the workspace
    /// in `FLOWGEN_HOME`, or `flowgen` in the home folder, creating it as needed.
    #[arg(short, long, env = "CONFIG_PATH")]
    config: Option<String>,
    /// Workspace folder to run without `--config`, such as `.` or one folder
    /// per environment in a larger repository, which then holds its commits.
    workspace: Option<std::path::PathBuf>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(clap::Subcommand)]
enum Command {
    /// Checks the flows and resources of a workspace without running them.
    Validate {
        /// The workspace folder, which holds `flows/` and `resources/`.
        #[arg(default_value = ".")]
        workspace: std::path::PathBuf,
        /// Prints the issues as JSON.
        #[arg(long)]
        json: bool,
    },
    /// Runs a Rhai script on sample events, as the `script` task runs it, and
    /// prints the events it emits.
    Test {
        /// The script, such as `resources/scripts/transform.rhai`.
        script: std::path::PathBuf,
        /// A sample file or a folder of them: `.json`, `.yaml` or `.yml` hold
        /// one event each, `.ndjson` or `.jsonl` one event per line.
        #[arg(long)]
        data: std::path::PathBuf,
    },
    /// Commits the system flows of this version to a workspace started without
    /// `--config`, where they are missing or differ, such as in one an earlier
    /// version made.
    Bootstrap {
        /// The workspace folder, as for a run; the default one when omitted.
        workspace: Option<std::path::PathBuf>,
    },
}

/// Prints the issues of the workspace at `root` and exits with 1 when there are any.
fn validate(root: &std::path::Path, json: bool) -> ! {
    let workspace = match flowgen::validation::validate_workspace(root) {
        Ok(workspace) => workspace,
        Err(e) => {
            eprintln!("{e}");
            process::exit(2);
        }
    };
    match json {
        true => match serde_json::to_string_pretty(&workspace.issues) {
            Ok(issues) => println!("{issues}"),
            Err(e) => {
                eprintln!("Failed to print the issues: {e}");
                process::exit(2);
            }
        },
        false => {
            for issue in &workspace.issues {
                match &issue.location {
                    Some(location) => println!("{}: {location}: {}", issue.path, issue.message),
                    None => println!("{}: {}", issue.path, issue.message),
                }
            }
            println!(
                "Checked {} files, {} issues",
                workspace.files,
                workspace.issues.len()
            );
        }
    }
    match workspace.issues.is_empty() {
        true => process::exit(0),
        false => process::exit(1),
    }
}

/// Prints what the script made of each sample and exits with 1 when one failed.
async fn test(script: &std::path::Path, data: &std::path::Path) -> ! {
    let outcomes = match flowgen_core::task::script::samples::run(script, data).await {
        Ok(outcomes) => outcomes,
        Err(e) => {
            eprintln!("{e}");
            process::exit(2);
        }
    };
    let mut failed = 0;
    for outcome in &outcomes {
        match &outcome.result {
            Ok(events) => {
                match events.len() {
                    1 => println!("{}: 1 event", outcome.sample),
                    count => println!("{}: {count} events", outcome.sample),
                }
                for event in events {
                    match serde_json::to_string_pretty(event) {
                        Ok(json) => println!("{json}"),
                        Err(e) => println!("{e}"),
                    }
                }
            }
            Err(error) => {
                failed += 1;
                println!("{}: {error}", outcome.sample);
            }
        }
    }
    println!("Ran {} samples, {failed} failed", outcomes.len());
    match failed {
        0 => process::exit(0),
        _ => process::exit(1),
    }
}

/// Creates the workspace, or adds the system flows it lacks, and exits.
async fn bootstrap(workspace: &Workspace) -> ! {
    let created = match workspace.create().await {
        Ok(created) => created,
        Err(e) => {
            eprintln!("{e}");
            process::exit(1)
        }
    };
    if created {
        println!("Created the workspace at {}", workspace.folder.display());
        process::exit(0)
    }
    match flowgen::local::update_system_flows(workspace).await {
        Ok(updated) if updated.is_empty() => {
            println!(
                "The workspace at {} has the system flows of this version",
                workspace.folder.display()
            );
            process::exit(0)
        }
        Ok(updated) => {
            for path in updated.written {
                println!("Updated {path}");
            }
            for path in updated.removed {
                println!("Removed {path}");
            }
            process::exit(0)
        }
        Err(e) => {
            eprintln!("{e}");
            process::exit(1)
        }
    }
}

/// The workspace of a run without a config file: `folder` within the
/// repository holding it, or the default folder as its own repository.
fn resolve_workspace(folder: Option<&std::path::Path>) -> Workspace {
    let resolved = match folder {
        Some(folder) => Workspace::within(folder),
        None => match flowgen::local::root() {
            Some(root) => Workspace::own(&root),
            None => {
                eprintln!(
                    "No home folder to create the workspace in: set {}, pass a folder or pass --config",
                    flowgen::local::HOME_ENV
                );
                process::exit(1);
            }
        },
    };
    match resolved {
        Ok(workspace) => workspace,
        Err(e) => {
            eprintln!("{e}");
            process::exit(1);
        }
    }
}

/// Prepares a run without a config file: creates the workspace when its folder
/// is new, enters it and starts its lifecycle flows.
async fn local_workspace(workspace: &Workspace) {
    match workspace.create().await {
        Ok(true) => eprintln!("Created the workspace at {}", workspace.folder.display()),
        Ok(false) => {}
        Err(e) => {
            eprintln!("{e}");
            process::exit(1);
        }
    }
    if let Err(e) = env::set_current_dir(&workspace.folder) {
        eprintln!(
            "Failed to enter the workspace at {}: {e}",
            workspace.folder.display()
        );
        process::exit(1);
    }
    if let Err(e) = flowgen::local::start_lifecycle(workspace).await {
        eprintln!("{e}");
        process::exit(1);
    }
    match flowgen::local::outdated_system_flows(workspace) {
        Ok(outdated) if !outdated.is_empty() => eprintln!(
            "The system flows {} differ from this version's; run `flowgen bootstrap` to update them",
            outdated.join(", ")
        ),
        Ok(_) => {}
        Err(e) => eprintln!("{e}"),
    }
    if let Err(e) = flowgen_core::credentials::use_system_keychain() {
        eprintln!("No system keychain, so credentials come from files only: {e}");
    }
}

/// Opens the web UI of a local run in the browser once the returned sender
/// says it accepts connections, when someone is at the terminal.
fn open_web_ui(app_config: &AppConfig) -> Option<tokio::sync::oneshot::Sender<()>> {
    let web = app_config.web.as_ref().filter(|web| web.enabled)?;
    let url = format!(
        "http://{}:{}{}/",
        app_config.host,
        web.port,
        web.path.trim_end_matches('/')
    );
    let (ready, listening) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        if listening.await.is_err() {
            return;
        }
        info!(url = %url, "Flowgen is running");
        if std::io::stdout().is_terminal() {
            if let Err(e) = webbrowser::open(&url) {
                warn!(error = %e, "Failed to open the browser");
            }
        }
    });
    Some(ready)
}

enum LogFormat {
    Compact,
    Json,
}

/// Determines the log format from LOG_FORMAT env var or TTY detection.
fn determine_log_format() -> LogFormat {
    if let Ok(format) = env::var("LOG_FORMAT") {
        return match format.to_lowercase().as_str() {
            "compact" => LogFormat::Compact,
            _ => LogFormat::Json,
        };
    }

    match std::io::stdout().is_terminal() {
        true => LogFormat::Compact,
        false => LogFormat::Json,
    }
}

fn init_tracing(metrics_store: Arc<dyn MetricsStore>, logs_writer: Option<MemoryLogsStoreWriter>) {
    let format = determine_log_format();

    let env_filter = match tracing_subscriber::EnvFilter::try_from_default_env() {
        Ok(filter) => filter,
        Err(_) => {
            tracing_subscriber::EnvFilter::new("info,opentelemetry=warn,opentelemetry_sdk=warn")
        }
    };

    let activity_layer = FlowActivityLayer::new(metrics_store);
    // Memory backend gets its own JSON fmt layer feeding the in-process
    // ring buffer. Absent on remote backends.
    let memory_layer = logs_writer.map(|w| tracing_subscriber::fmt::layer().json().with_writer(w));

    match format {
        LogFormat::Compact => {
            let fmt_layer = tracing_subscriber::fmt::layer().compact();
            tracing_subscriber::registry()
                .with(env_filter)
                .with(activity_layer)
                .with(memory_layer)
                .with(fmt_layer)
                .init();
        }
        LogFormat::Json => {
            let fmt_layer = tracing_subscriber::fmt::layer().json();
            tracing_subscriber::registry()
                .with(env_filter)
                .with(activity_layer)
                .with(memory_layer)
                .with(fmt_layer)
                .init();
        }
    }
}

/// What one invocation does.
enum Mode {
    Validate {
        workspace: std::path::PathBuf,
        json: bool,
    },
    Test {
        script: std::path::PathBuf,
        data: std::path::PathBuf,
    },
    Bootstrap(Workspace),
    Config(Box<AppConfig>),
    Local(Workspace, Box<AppConfig>),
}

/// The config `loaded`, or exits naming why it is invalid.
fn config_or_exit(loaded: Result<AppConfig, config::ConfigError>) -> Box<AppConfig> {
    match loaded {
        Ok(config) => Box::new(config),
        Err(e) => {
            let cwd = env::current_dir()
                .map(|d| d.display().to_string())
                .unwrap_or_default();
            let msg = e.to_string();
            let msg = msg
                .chars()
                .next()
                .map(|c| c.to_uppercase().to_string() + &msg[c.len_utf8()..])
                .unwrap_or(msg);
            eprintln!("{msg} (working directory: {cwd})");
            process::exit(1);
        }
    }
}

/// Loads the config and sets the variables flows read, the version, the
/// addresses of this run's servers and the local workspace's repository and
/// folder, before any other thread starts, then runs on the tokio runtime.
fn main() {
    let cli = Cli::parse();
    let mode = match (cli.command, cli.config) {
        (Some(Command::Validate { workspace, json }), _) => Mode::Validate { workspace, json },
        (Some(Command::Test { script, data }), _) => Mode::Test { script, data },
        (Some(Command::Bootstrap { workspace }), _) => {
            Mode::Bootstrap(resolve_workspace(workspace.as_deref()))
        }
        (None, Some(path)) => Mode::Config(config_or_exit(AppConfig::load(
            config::File::with_name(&path),
            flowgen::config::env_source(),
        ))),
        (None, None) => {
            let workspace = resolve_workspace(cli.workspace.as_deref());
            let loaded = flowgen::local::load(&workspace.folder, flowgen::config::env_source());
            Mode::Local(workspace, config_or_exit(loaded))
        }
    };
    env::set_var(flowgen::config::VERSION_ENV, env!("CARGO_PKG_VERSION"));
    if let Mode::Config(app_config) | Mode::Local(_, app_config) = &mode {
        for (name, value) in app_config.run_variables() {
            if env::var_os(name).is_none() {
                env::set_var(name, value);
            }
        }
    }
    if let Mode::Bootstrap(workspace) | Mode::Local(workspace, _) = &mode {
        for (name, value) in workspace.variables() {
            env::set_var(name, value);
        }
    }
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("Failed to start the runtime: {e}");
            process::exit(1);
        }
    };
    runtime.block_on(run(mode));
}

async fn run(mode: Mode) {
    let _ = rustls::crypto::ring::default_provider().install_default();

    // Boot-time errors go straight to stderr, before tracing is up: tracing
    // needs the metrics store, which needs the cache defined in config.
    let (app_config, local) = match mode {
        Mode::Validate { workspace, json } => validate(&workspace, json),
        Mode::Test { script, data } => test(&script, &data).await,
        Mode::Bootstrap(workspace) => bootstrap(&workspace).await,
        Mode::Config(app_config) => (*app_config, false),
        Mode::Local(workspace, app_config) => {
            local_workspace(&workspace).await;
            (*app_config, true)
        }
    };

    if let Err(e) = app_config.validate() {
        eprintln!("{e}");
        process::exit(1);
    }

    let cache = match App::init_cache(&app_config, None).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to build cache: {}", e);
            process::exit(1);
        }
    };

    let metrics_store = OtlpMetricsStore::builder().build();

    let telemetry_config = match &app_config.telemetry {
        Some(t) if t.enabled => {
            let backend = match &t.backend {
                Some(flowgen::config::TelemetryBackendOptions::Remote { endpoint }) => {
                    flowgen_core::telemetry::Backend::Remote {
                        endpoint: endpoint.clone(),
                    }
                }
                Some(flowgen::config::TelemetryBackendOptions::Memory {
                    logs_per_flow,
                    metrics_per_flow,
                    ..
                }) => flowgen_core::telemetry::Backend::Memory {
                    logs_per_flow: *logs_per_flow,
                    metrics_per_flow: *metrics_per_flow,
                },
                None => flowgen_core::telemetry::Backend::Memory {
                    logs_per_flow: flowgen_core::telemetry::MEMORY_LOG_CAPACITY,
                    metrics_per_flow: flowgen_core::telemetry::MEMORY_METRIC_CAPACITY,
                },
            };
            flowgen_core::telemetry::TelemetryConfig {
                backend,
                service_name: t.service_name.clone(),
                service_version: env!("CARGO_PKG_VERSION").to_string(),
                metrics_export_interval_secs: t.metrics_export_interval.as_secs(),
            }
        }
        _ => flowgen_core::telemetry::TelemetryConfig::default(),
    };
    let telemetry = match flowgen_core::telemetry::init_telemetry(telemetry_config) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("Failed to initialize OpenTelemetry: {e}");
            process::exit(1);
        }
    };

    init_tracing(
        Arc::clone(&metrics_store) as Arc<dyn MetricsStore>,
        telemetry.logs_writer.clone(),
    );

    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};

            let mut sigterm = match signal(SignalKind::terminate()) {
                Ok(s) => s,
                Err(e) => {
                    error!("Failed to setup SIGTERM handler: {}", e);
                    return;
                }
            };

            let mut sigint = match signal(SignalKind::interrupt()) {
                Ok(s) => s,
                Err(e) => {
                    error!("Failed to setup SIGINT handler: {}", e);
                    return;
                }
            };

            tokio::select! {
                _ = sigterm.recv() => {
                    info!("Received SIGTERM, initiating graceful shutdown...");
                }
                _ = sigint.recv() => {
                    info!("Received SIGINT, initiating graceful shutdown...");
                }
            }

            let _ = shutdown_tx.send(());

            tokio::select! {
                _ = sigterm.recv() => {}
                _ = sigint.recv() => {}
            }
            warn!("Received second signal, forcing shutdown");
            process::exit(1);
        }

        #[cfg(not(unix))]
        {
            match tokio::signal::ctrl_c().await {
                Ok(()) => {
                    info!("Received shutdown signal, initiating graceful shutdown...");
                }
                Err(err) => {
                    error!("Failed to listen for shutdown signal: {}", err);
                    return;
                }
            }

            let _ = shutdown_tx.send(());

            if tokio::signal::ctrl_c().await.is_ok() {
                warn!("Received second signal, forcing shutdown");
                process::exit(1);
            }
        }
    });

    let web_ready = match local {
        true => open_web_ui(&app_config),
        false => None,
    };

    let app = App {
        config: app_config,
        metrics_store: Arc::clone(&metrics_store) as Arc<dyn MetricsStore>,
        cache: Arc::clone(&cache),
        logs_store: telemetry.logs_store.clone(),
        web_ready,
    };
    if let Err(e) = app.start(shutdown_rx).await {
        error!("Application failed to run: {}", e);
        process::exit(1);
    }
}
