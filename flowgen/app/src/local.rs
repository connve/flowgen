//! Runs flowgen without a config file, on a workspace folder: its own git
//! repository, or a folder in a larger one, such as one folder per environment.

use crate::config::{
    default_shutdown_timeout, AgentsOptions, AiGatewayOptions, AppConfig, AuthoringOptions,
    CacheOptions, FlowCacheOptions, FlowOptions, HttpServerOptions, McpServerOptions,
    ResourceCacheOptions, ResourceOptions, TelemetryOptions, WebOptions,
};
use crate::hubs::SYSTEM_FOLDER;
use flowgen_core::cache::{Cache, RedbCache};
use flowgen_core::workspace::{CACHE_FILE, FLOWS_DIR, STATE_DIR};
use rust_embed::RustEmbed;
use std::path::{Path, PathBuf};

/// The system flows: every file in `config/`, which a new workspace starts
/// with in [`SYSTEM_FOLDER`] and [`update_system_flows`] brings an older one
/// up to, as they depend on how the tasks of this version behave.
#[derive(RustEmbed)]
#[folder = "../../config/"]
struct SystemFlows;

/// The system flows that commit approved changes, release each commit and
/// deploy the latest release, by their path in [`SYSTEM_FOLDER`];
/// [`start_lifecycle`] starts them on every run.
const LIFECYCLE_FLOWS: [&str; 3] = [
    "commit_workspace.yaml",
    "release_workspace.yaml",
    "deploy_workspace.yaml",
];

/// `file` of [`SYSTEM_FOLDER`], by its path in the workspace.
fn system_path(file: &str) -> String {
    format!("{FLOWS_DIR}/{SYSTEM_FOLDER}/{file}")
}

/// The files a new workspace starts with, by their path in the workspace.
pub fn starter_files() -> Vec<(String, String)> {
    SystemFlows::iter()
        .filter_map(|file| {
            let content = SystemFlows::get(&file)?;
            let content = String::from_utf8_lossy(&content.data).into_owned();
            Some((system_path(&file), content))
        })
        .collect()
}

/// The system flows as flows to run, for a run with `flows.system`; one that
/// fails to load is logged and left out.
pub fn system_flows() -> Vec<crate::config::FlowConfig> {
    let folder = format!("{FLOWS_DIR}/");
    starter_files()
        .into_iter()
        .filter_map(|(path, content)| {
            let identity = crate::validation::flow_identity(path.trim_start_matches(&folder));
            let loaded = crate::config::FlowConfigRaw::parse(&path, &content)
                .map_err(|e| e.to_string())
                .and_then(|raw| {
                    crate::config::FlowConfig::from_path(
                        raw,
                        identity.to_string(),
                        Some(content.clone()),
                    )
                    .map_err(|e| e.to_string())
                });
            match loaded {
                Ok(flow) => Some(flow),
                Err(error) => {
                    tracing::error!(flow = %identity, error = %error, "Failed to load a system flow");
                    None
                }
            }
        })
        .collect()
}

/// The message of the commit [`update_system_flows`] makes.
const UPDATE_SYSTEM_FLOWS_MESSAGE: &str = "Update the system flows";

/// The file at the root of a workspace listing the system flows its last
/// update wrote, so the next one deletes those this version no longer has.
const SYSTEM_FLOWS_FILE: &str = ".system-flows";

/// The system flows with the list of them, by their path in the workspace.
fn system_files() -> Vec<(String, String)> {
    let mut files = starter_files();
    let mut paths: Vec<String> = files.iter().map(|(path, _)| path.clone()).collect();
    paths.sort();
    let list: String = paths.iter().map(|path| format!("{path}\n")).collect();
    files.push((SYSTEM_FLOWS_FILE.to_string(), list));
    files
}

/// The variable a local run sets to the root of the workspace's repository,
/// which the lifecycle flows commit to and release from.
pub const REPOSITORY_ENV: &str = "FLOWGEN_REPOSITORY";

/// The variable a local run sets to the workspace's folder in its
/// repository: empty for the whole repository, else ending in `/`.
pub const WORKSPACE_PATH_ENV: &str = "FLOWGEN_WORKSPACE_PATH";

/// Failure to create, add or start the lifecycle flows of a workspace.
#[derive(thiserror::Error, Debug)]
pub enum LifecycleError {
    #[error(transparent)]
    Workspace(#[from] flowgen_git::workspace::Error),
    #[error("Failed to open the workspace folder {path}: {source}")]
    Folder {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("Failed to start the lifecycle flows in the cache: {0}")]
    Cache(#[from] flowgen_core::cache::Error),
    #[error("The workspace has no {}; run `flowgen bootstrap` to add the system flows of this version", .paths.join(", "))]
    Missing { paths: Vec<String> },
}

/// A local workspace: its folder, the git repository holding it, and the
/// folder within that repository.
#[derive(Debug, Clone, PartialEq)]
pub struct Workspace {
    /// The workspace folder, holding `flows/` and `resources/`.
    pub folder: PathBuf,
    /// The root of the repository the workspace is committed to.
    pub repository: PathBuf,
    /// [`Self::folder`] within [`Self::repository`]: empty, or ending in `/`.
    pub path: String,
}

impl Workspace {
    /// The workspace at `folder`, created as needed, as its own repository.
    pub fn own(folder: &Path) -> Result<Self, LifecycleError> {
        let folder = Self::open(folder)?;
        Ok(Self {
            repository: folder.clone(),
            folder,
            path: String::new(),
        })
    }

    /// The workspace at `folder`, created as needed, in the repository holding
    /// it, or as its own repository when none does.
    pub fn within(folder: &Path) -> Result<Self, LifecycleError> {
        let folder = Self::open(folder)?;
        let repository = match flowgen_git::workspace::repository_of(&folder) {
            Some(repository) => repository,
            None => folder.clone(),
        };
        let path = match folder.strip_prefix(&repository) {
            Ok(within) if !within.as_os_str().is_empty() => {
                format!("{}/", within.to_string_lossy().replace('\\', "/"))
            }
            _ => String::new(),
        };
        Ok(Self {
            folder,
            repository,
            path,
        })
    }

    fn open(folder: &Path) -> Result<PathBuf, LifecycleError> {
        let failed = |source| LifecycleError::Folder {
            path: folder.display().to_string(),
            source,
        };
        std::fs::create_dir_all(folder).map_err(failed)?;
        std::fs::canonicalize(folder).map_err(failed)
    }

    /// The variables the lifecycle flows read the repository and folder from.
    pub fn variables(&self) -> [(&'static str, String); 2] {
        [
            (REPOSITORY_ENV, self.repository.display().to_string()),
            (WORKSPACE_PATH_ENV, self.path.clone()),
        ]
    }

    /// `files`, by their path in the workspace, by their path in the repository.
    fn in_repository<'a>(&self, files: &'a [(String, String)]) -> Vec<(String, &'a str)> {
        files
            .iter()
            .map(|(path, content)| (format!("{}{path}", self.path), content.as_str()))
            .collect()
    }

    /// Creates the workspace with the starter files in one commit when its
    /// folder holds nothing yet; returns whether it did.
    pub async fn create(&self) -> Result<bool, LifecycleError> {
        let starter = system_files();
        if self.path.is_empty() {
            return Ok(flowgen_git::workspace::init(
                &self.folder,
                &self.in_repository(&starter),
            )?);
        }
        let empty = match std::fs::read_dir(&self.folder) {
            Ok(mut entries) => entries.next().is_none(),
            Err(source) => {
                return Err(LifecycleError::Folder {
                    path: self.folder.display().to_string(),
                    source,
                })
            }
        };
        if !empty {
            return Ok(false);
        }
        let gitignore = flowgen_git::workspace::gitignore();
        let mut files = self.in_repository(&starter);
        files.push((
            format!("{}{}", self.path, flowgen_git::workspace::GITIGNORE_FILE),
            &gitignore,
        ));
        flowgen_git::workspace::add_missing(
            &self.repository,
            &files,
            flowgen_git::workspace::MESSAGE,
        )
        .await?;
        Ok(true)
    }
}

/// What [`update_system_flows`] committed, by path in the repository.
#[derive(Debug, Default, PartialEq)]
pub struct SystemFlowsUpdate {
    /// Flows written, new or changed.
    pub written: Vec<String>,
    /// Flows deleted, as this version no longer has them.
    pub removed: Vec<String>,
}

impl SystemFlowsUpdate {
    /// Whether the update committed nothing.
    pub fn is_empty(&self) -> bool {
        self.written.is_empty() && self.removed.is_empty()
    }
}

/// Commits the system flows of this version that the workspace's last commit
/// lacks or holds with other content, and deletes those an earlier update
/// wrote that this version no longer has.
pub async fn update_system_flows(
    workspace: &Workspace,
) -> Result<SystemFlowsUpdate, LifecycleError> {
    let files = system_files();
    let shipped: std::collections::HashSet<&str> =
        files.iter().map(|(path, _)| path.as_str()).collect();
    let system = format!("{FLOWS_DIR}/{SYSTEM_FOLDER}/");
    let listed = flowgen_git::workspace::committed(
        &workspace.repository,
        &[format!("{}{SYSTEM_FLOWS_FILE}", workspace.path)],
    )?;
    let retired: Vec<String> = match listed.into_iter().next().flatten() {
        Some(list) => list
            .lines()
            .filter(|path| path.starts_with(&system) && !shipped.contains(path))
            .map(|path| format!("{}{path}", workspace.path))
            .collect(),
        None => Vec::new(),
    };
    let committed = flowgen_git::workspace::update(
        &workspace.repository,
        &workspace.in_repository(&files),
        &retired,
        UPDATE_SYSTEM_FLOWS_MESSAGE,
    )
    .await?;
    let (removed, written) = committed
        .into_iter()
        .partition(|path| retired.contains(path));
    Ok(SystemFlowsUpdate { written, removed })
}

/// The system flows of this version that the workspace's last commit lacks
/// or holds with other content, by their path in the repository.
pub fn outdated_system_flows(workspace: &Workspace) -> Result<Vec<String>, LifecycleError> {
    Ok(flowgen_git::workspace::outdated(
        &workspace.repository,
        &workspace.in_repository(&system_files()),
    )?)
}

/// Puts the lifecycle flows of the workspace's last commit into the cache a
/// run loads flows from, as no flow can deploy the flow that deploys it; the
/// deploy flow keeps them, and every other flow, up to date from then on.
/// Fails, starting none, when the commit lacks one.
pub async fn start_lifecycle(workspace: &Workspace) -> Result<(), LifecycleError> {
    let paths: Vec<String> = LIFECYCLE_FLOWS
        .iter()
        .map(|file| format!("{}{}", workspace.path, system_path(file)))
        .collect();
    let committed = flowgen_git::workspace::committed(&workspace.repository, &paths)?;
    let missing: Vec<String> = paths
        .iter()
        .zip(&committed)
        .filter(|(_, content)| content.is_none())
        .map(|(path, _)| path.clone())
        .collect();
    if !missing.is_empty() {
        return Err(LifecycleError::Missing { paths: missing });
    }
    let options = defaults(&workspace.folder).cache_options();
    let cache = RedbCache::open(&options.path, &options.system.db_name)?;
    for (file, content) in LIFECYCLE_FLOWS.iter().zip(committed.into_iter().flatten()) {
        let path = format!("{SYSTEM_FOLDER}/{file}");
        let identity = crate::validation::flow_identity(&path);
        cache
            .put(
                &format!("{}.{identity}", crate::config::FLOWS_CACHE_PREFIX),
                content.into(),
                None,
            )
            .await?;
    }
    Ok(())
}

/// The variable naming the default workspace folder, its own repository.
pub const HOME_ENV: &str = "FLOWGEN_HOME";

/// The workspace folder in the home folder, when [`HOME_ENV`] is unset.
pub const HOME_DIR: &str = "flowgen";

/// The address of a local run, reachable only from this machine.
pub const LOCAL_HOST: &str = "127.0.0.1";

/// The workspace folder: [`HOME_ENV`], or [`HOME_DIR`] in the home folder.
pub fn root() -> Option<PathBuf> {
    match std::env::var_os(HOME_ENV) {
        Some(home) => Some(PathBuf::from(home)),
        None => std::env::home_dir().map(|home| home.join(HOME_DIR)),
    }
}

/// The config of a local run: every server on, reachable only from this machine.
pub fn defaults(root: &Path) -> AppConfig {
    AppConfig {
        cache: Some(CacheOptions {
            path: root.join(STATE_DIR).join(CACHE_FILE),
            ..CacheOptions::default()
        }),
        flows: FlowOptions {
            cache: Some(FlowCacheOptions { enabled: true }),
            ..FlowOptions::default()
        },
        resources: Some(ResourceOptions {
            cache: Some(ResourceCacheOptions { enabled: true }),
            ..ResourceOptions::default()
        }),
        http_server: Some(HttpServerOptions {
            enabled: true,
            ..HttpServerOptions::default()
        }),
        mcp_server: Some(McpServerOptions {
            enabled: true,
            ..McpServerOptions::default()
        }),
        ai_gateway: Some(AiGatewayOptions {
            enabled: true,
            ..AiGatewayOptions::default()
        }),
        web: Some(WebOptions {
            enabled: true,
            path: crate::web::BUILT_BASE_PATH.to_string(),
            agents: Some(AgentsOptions {
                enabled: true,
                ..AgentsOptions::default()
            }),
            authoring: Some(AuthoringOptions {
                enabled: true,
                ..AuthoringOptions::default()
            }),
            ..WebOptions::default()
        }),
        health: Default::default(),
        retry: None,
        telemetry: Some(TelemetryOptions {
            enabled: true,
            ..TelemetryOptions::default()
        }),
        shutdown_timeout: default_shutdown_timeout(),
        host: LOCAL_HOST.to_string(),
        ai_budget: None,
    }
}

/// [`defaults`] with `env`, the `APP_` environment variables, over it.
pub fn load(root: &Path, env: config::Environment) -> Result<AppConfig, config::ConfigError> {
    let source = config::Config::try_from(&defaults(root))?;
    AppConfig::load(source, env)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{default_host, env_source, CacheType};
    use flowgen_core::workspace::CREDENTIALS_DIR;

    fn variables(set: &[(&str, &str)]) -> config::Environment {
        let set = set
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect();
        env_source().source(Some(set))
    }

    #[test]
    fn a_local_run_serves_everything_on_this_machine_only() {
        let root = std::env::temp_dir().join(HOME_DIR);
        let config = load(&root, variables(&[])).unwrap();

        config.validate().expect("The local config is valid");
        assert_eq!(
            config,
            defaults(&root),
            "The config survives loading as it is"
        );
        assert_eq!(config.host, LOCAL_HOST);
        assert_ne!(config.host, default_host());
        assert_eq!(config.flows.path, None);
        assert!(config.flows.cache.as_ref().unwrap().enabled);
        assert!(
            config
                .resources
                .as_ref()
                .unwrap()
                .cache
                .as_ref()
                .unwrap()
                .enabled
        );
        let cache = config.cache_options();
        assert_eq!(cache.cache_type, CacheType::Redb);
        assert_eq!(cache.path, root.join(STATE_DIR).join(CACHE_FILE));
        assert!(config.http_server.unwrap().enabled);
        assert!(config.mcp_server.unwrap().enabled);
        assert!(config.ai_gateway.unwrap().enabled);
        let web = config.web.unwrap();
        assert!(web.enabled);
        assert_eq!(web.path, crate::web::BUILT_BASE_PATH);
        assert!(web.agents.unwrap().enabled);
        let authoring = web.authoring.unwrap();
        assert!(authoring.enabled);
        let commit = format!("{}.yaml", authoring.targets[0].commit_flow);
        assert!(LIFECYCLE_FLOWS
            .iter()
            .any(|file| format!("{SYSTEM_FOLDER}/{file}") == commit));
        assert!(config.telemetry.unwrap().enabled);
    }

    fn system_flow(file: &str) -> String {
        let path = system_path(file);
        match starter_files()
            .into_iter()
            .find(|(starter, _)| *starter == path)
        {
            Some((_, content)) => content,
            None => panic!("No system flow {path}"),
        }
    }

    #[test]
    fn release_and_deploy_meet_in_the_layout_and_the_cache_the_app_reads() {
        let root = std::env::temp_dir().join(HOME_DIR);
        let config = defaults(&root);
        let variables: std::collections::HashMap<_, _> =
            config.run_variables().into_iter().collect();
        assert_eq!(
            variables[crate::config::CACHE_URL_ENV],
            format!("redb://{}", root.join(STATE_DIR).join(CACHE_FILE).display())
        );
        assert_eq!(
            variables[crate::config::SYSTEM_BUCKET_ENV],
            config.cache_options().system.db_name
        );
        assert_eq!(
            variables[crate::config::RELEASES_ENV],
            format!("oci-layout://{STATE_DIR}/releases")
        );

        let env = |name: &str| format!("\"{{{{env.{name}}}}}");
        let cache = format!("url: {}\"", env(crate::config::CACHE_URL_ENV));
        let bucket = format!("bucket: {}\"", env(crate::config::SYSTEM_BUCKET_ENV));
        let release = system_flow("release_workspace.yaml");
        for expected in [
            format!("repository: {}\"", env(crate::config::RELEASES_ENV)),
            cache.clone(),
            bucket.clone(),
            format!(
                "key: \"{}.{{{{event.data.commit}}}}\"",
                crate::authoring::RELEASES_PREFIX
            ),
        ] {
            assert!(release.contains(&expected), "release: {expected}");
        }
        let deploy = system_flow("deploy_workspace.yaml");
        for expected in [
            format!("artifact: {}:latest\"", env(crate::config::RELEASES_ENV)),
            cache,
            bucket,
            format!("key_prefix: \"{}.\"", crate::config::FLOWS_CACHE_PREFIX),
            format!("key_prefix: \"{}.\"", crate::config::RESOURCES_CACHE_PREFIX),
        ] {
            assert!(deploy.contains(&expected), "deploy: {expected}");
        }
    }

    #[tokio::test]
    async fn an_older_workspace_runs_its_lifecycle_flows_once_they_are_added() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join(HOME_DIR);
        let edited = "flow:\n  tasks: []\n";
        flowgen_git::workspace::init(&root, &[(system_path("commit_workspace.yaml"), edited)])
            .unwrap();
        let workspace = Workspace::own(&root).unwrap();
        let options = defaults(&root).cache_options();
        let cache = RedbCache::open(&options.path, &options.system.db_name).unwrap();
        let started = |identity: &str| {
            let cache = cache.clone();
            let key = format!("{}.{identity}", crate::config::FLOWS_CACHE_PREFIX);
            async move {
                cache
                    .get(&key)
                    .await
                    .unwrap()
                    .map(|value| String::from_utf8(value.to_vec()).unwrap())
            }
        };

        let missing = start_lifecycle(&workspace).await;
        assert!(
            matches!(&missing, Err(LifecycleError::Missing { paths }) if paths.len() == 2),
            "{missing:?}"
        );
        assert_eq!(
            started("system/commit_workspace").await,
            None,
            "Nothing starts"
        );

        assert_eq!(
            outdated_system_flows(&workspace).unwrap().len(),
            system_files().len()
        );
        let mut updated = update_system_flows(&workspace).await.unwrap();
        updated.written.sort();
        let mut expected: Vec<String> = system_files().into_iter().map(|(path, _)| path).collect();
        expected.sort();
        assert_eq!(
            updated,
            SystemFlowsUpdate {
                written: expected,
                removed: Vec::new()
            },
            "Every system flow, the edited one too"
        );
        start_lifecycle(&workspace).await.unwrap();

        assert_eq!(
            started("system/commit_workspace").await,
            Some(system_flow("commit_workspace.yaml")),
            "The edited flow is brought to this version"
        );
        assert_eq!(
            started("system/deploy_workspace").await,
            Some(system_flow("deploy_workspace.yaml"))
        );
        assert!(outdated_system_flows(&workspace).unwrap().is_empty());
        assert!(
            update_system_flows(&workspace).await.unwrap().is_empty(),
            "Up to date, nothing to commit"
        );
    }

    #[tokio::test]
    async fn an_update_deletes_the_system_flows_this_version_no_longer_has() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join(HOME_DIR);
        let workspace = Workspace::own(&root).unwrap();
        assert!(workspace.create().await.unwrap());
        let retired = system_path("retired.yaml");
        let mine = system_path("mine_hub.yaml");
        let listed = format!("{}{retired}\n", system_files().last().unwrap().1);
        let flow = "flow:\n  tasks: []\n";
        flowgen_git::workspace::update(
            &root,
            &[
                (retired.clone(), flow),
                (mine.clone(), flow),
                (SYSTEM_FLOWS_FILE.to_string(), listed.as_str()),
            ],
            &[],
            "An earlier version",
        )
        .await
        .unwrap();

        let updated = update_system_flows(&workspace).await.unwrap();

        assert_eq!(
            updated,
            SystemFlowsUpdate {
                written: vec![SYSTEM_FLOWS_FILE.to_string()],
                removed: vec![retired.clone()]
            },
            "The list is rewritten and the retired flow deleted"
        );
        let committed = flowgen_git::workspace::committed(&root, &[retired, mine]).unwrap();
        assert_eq!(
            committed,
            vec![None, Some(flow.to_string())],
            "A flow the list never named is kept"
        );
    }

    #[tokio::test]
    async fn each_environment_is_a_workspace_folder_in_one_repository() {
        let dir = tempfile::tempdir().unwrap();
        let repository = dir.path().join("repo");
        flowgen_git::workspace::init(&repository, &[]).unwrap();
        let staging = Workspace::within(&repository.join("workspaces/staging")).unwrap();
        let production = Workspace::within(&repository.join("workspaces/production")).unwrap();

        assert!(staging.create().await.unwrap());
        assert!(production.create().await.unwrap());
        assert!(!staging.create().await.unwrap(), "Created once");

        let canonical = std::fs::canonicalize(&repository).unwrap();
        assert_eq!(staging.repository, canonical);
        assert_eq!(staging.path, "workspaces/staging/");
        assert_eq!(
            staging.variables(),
            [
                (REPOSITORY_ENV, canonical.display().to_string()),
                (WORKSPACE_PATH_ENV, "workspaces/staging/".to_string())
            ]
        );
        assert!(
            !staging.folder.join(".git").exists(),
            "No nested repository"
        );
        assert!(staging.folder.join(".gitignore").exists());
        for workspace in [&staging, &production] {
            start_lifecycle(workspace).await.unwrap();
        }
        let committed = flowgen_git::workspace::committed(
            &repository,
            &[format!(
                "workspaces/production/{}",
                system_path("deploy_workspace.yaml")
            )],
        )
        .unwrap();
        assert_eq!(committed, vec![Some(system_flow("deploy_workspace.yaml"))]);
    }

    #[test]
    fn every_flow_in_config_is_a_valid_system_flow_reading_keys_from_credentials() {
        let config = concat!(env!("CARGO_MANIFEST_DIR"), "/../../config");
        let mut in_config: Vec<String> = walkdir::WalkDir::new(config)
            .into_iter()
            .map(|entry| entry.unwrap())
            .filter(|entry| entry.file_type().is_file())
            .map(|entry| {
                let file = entry.path().strip_prefix(config).unwrap();
                system_path(&file.to_string_lossy())
            })
            .collect();
        in_config.sort();
        let files = starter_files();
        let mut starter: Vec<String> = files.iter().map(|(path, _)| path.clone()).collect();
        starter.sort();
        assert_eq!(starter, in_config);
        assert!(LIFECYCLE_FLOWS
            .iter()
            .all(|file| starter.contains(&system_path(file))));

        let system = format!("{FLOWS_DIR}/");
        for (path, content) in &files {
            let identity = path.trim_start_matches(&system);
            let issues = crate::validation::validate_flow(identity, content);
            assert!(issues.is_empty(), "{path}: {issues:?}");
            for line in content
                .lines()
                .filter(|line| line.contains("credentials_path:"))
            {
                let line = line.trim();
                let in_dir = line.starts_with(&format!("credentials_path: {CREDENTIALS_DIR}/"));
                let from_run = line.starts_with("credentials_path: \"{{env.FLOWGEN_")
                    && line.ends_with("_CREDENTIALS}}\"");
                assert!(in_dir || from_run, "{path}: {line}");
            }
        }
        assert!(starter.iter().any(|path| {
            let identity = crate::validation::flow_identity(path.trim_start_matches(&system));
            identity.starts_with(&format!("{SYSTEM_FOLDER}/"))
                && identity.ends_with(crate::hubs::HUB_SUFFIX)
        }));
    }

    #[test]
    fn a_run_with_system_flows_runs_every_flow_in_config_under_system() {
        let flows = system_flows();
        let mut identities: Vec<&str> = flows.iter().map(|flow| flow.identity()).collect();
        identities.sort();
        let folder = format!("{FLOWS_DIR}/");
        let mut expected: Vec<String> = starter_files()
            .iter()
            .map(|(path, _)| {
                crate::validation::flow_identity(path.trim_start_matches(&folder)).to_string()
            })
            .collect();
        expected.sort();
        assert_eq!(identities, expected);
        assert!(identities
            .iter()
            .all(|identity| identity.starts_with(&format!("{SYSTEM_FOLDER}/"))));
    }

    #[test]
    fn app_variables_change_a_local_run() {
        let root = std::env::temp_dir().join(HOME_DIR);
        let config = load(&root, variables(&[("APP_MCP_SERVER__PORT", "4999")])).unwrap();

        assert_eq!(config.mcp_server.unwrap().port, 4999);
    }
}
