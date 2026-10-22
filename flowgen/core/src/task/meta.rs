use crate::event::Event;
use crate::task::context::{Services, TaskContext};
use crate::task::runner::Runner;
use std::future::Future;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};

/// A system task types connect to, as shown in the web UI and docs.
#[derive(Clone, Copy, Debug)]
pub struct Connector {
    /// Stable identifier, such as `salesforce`.
    pub name: &'static str,
    /// Display name, such as `Salesforce`.
    pub label: &'static str,
    /// Logo, kept in the crate's `icons/` directory.
    pub icon: Icon,
    /// Names the account a credentials file points to, from its non-secret fields.
    pub describe_credentials: Option<fn(&Path) -> Option<String>>,
}

/// A connector logo.
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum Icon {
    Svg(&'static str),
    Png(&'static [u8]),
}

impl Icon {
    /// The media type to serve the logo with.
    pub const fn content_type(&self) -> &'static str {
        match self {
            Icon::Svg(_) => "image/svg+xml",
            Icon::Png(_) => "image/png",
        }
    }

    /// The logo's file content.
    pub const fn bytes(&self) -> &'static [u8] {
        match self {
            Icon::Svg(svg) => svg.as_bytes(),
            Icon::Png(png) => png,
        }
    }
}

/// A system a task connects to, as declared by its config.
#[derive(Clone, Debug)]
pub struct Connection {
    pub connector: Connector,
    /// The credentials file as written in the config.
    pub credentials_path: Option<String>,
    /// Where it connects, from the config; `None` when set per event.
    pub target: Option<String>,
    /// Whether the target or the credentials come from the event.
    pub per_event: bool,
    /// Whether it is called as the signed-in user, with their token.
    pub on_behalf_of_user: bool,
}

impl Connection {
    /// A connection through `connector` with the credentials at `credentials_path`.
    pub fn new(connector: Connector, credentials_path: Option<&Path>) -> Self {
        let credentials_path = credentials_path.map(|path| path.to_string_lossy().into_owned());
        let per_event = credentials_path.as_deref().is_some_and(is_per_event);
        Self {
            connector,
            credentials_path,
            target: None,
            per_event,
            on_behalf_of_user: false,
        }
    }

    /// Sets the target from config values, leaving out those set per event.
    pub fn target(mut self, values: &[&str]) -> Self {
        let mut shown = Vec::new();
        for value in values {
            let part = host(value);
            match (is_per_event(&part), part.contains("{{")) {
                (true, _) => self.per_event = true,
                (false, true) => match render_env(&part) {
                    Some(rendered) => shown.push(host(&rendered)),
                    None => shown.push(part),
                },
                (false, false) => shown.push(part),
            }
        }
        self.target = match shown.is_empty() {
            true => None,
            false => Some(shown.join(" · ")),
        };
        self
    }
}

/// Whether `value` is a template that reads the event.
pub fn is_per_event(value: &str) -> bool {
    value.contains("{{") && value.contains("event.")
}

/// What `value` renders to with the process environment; `None` when it renders to nothing.
pub fn render_env(value: &str) -> Option<String> {
    match crate::config::render_template(value, &serde_json::Map::new()) {
        Ok(rendered) if !rendered.is_empty() => Some(rendered),
        _ => None,
    }
}

/// Reads a credentials file as `T`; `None` when it cannot be read or parsed.
pub fn read_credentials<T: serde::de::DeserializeOwned>(path: &Path) -> Option<T> {
    let content = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&content).ok()
}

/// A config value stripped of anything that could hold a secret.
pub fn host(value: &str) -> String {
    let url_host = match url::Url::parse(value) {
        Ok(url) => match (url.host_str(), url.port()) {
            (Some(host), Some(port)) => Some(format!("{host}:{port}")),
            (Some(host), None) => Some(host.to_string()),
            (None, _) => None,
        },
        Err(_) => None,
    };
    if let Some(host) = url_host {
        return host;
    }
    let without_user = match value.rsplit_once('@') {
        Some((_, rest)) => rest,
        None => value,
    };
    let cut: &[char] = match value.contains("{{") {
        true => &['/', '?', '#'],
        false => &['?', '#'],
    };
    match without_user.find(cut) {
        Some(end) => without_user[..end].to_string(),
        None => without_user.to_string(),
    }
}

/// The connector of the tasks in this crate.
pub const CONNECTOR: Connector = Connector {
    name: "core",
    label: "Core",
    icon: Icon::Svg(include_str!("../../icons/core.svg")),
    describe_credentials: None,
};

/// The error a task returns, typed by the crate that owns the task.
pub type TaskError = Box<dyn std::error::Error + Send + Sync>;

/// Implemented by each task type's config, in the crate that owns it.
pub trait TaskMeta {
    /// The connector the task type belongs to.
    const CONNECTOR: Connector = CONNECTOR;

    /// Whether the task mostly uses the CPU, such as parsing or encoding
    /// batches, rather than waiting on an external service.
    const CPU_INTENSIVE: bool = false;

    /// Takes and sends events. False for registration-only tasks.
    const PIPELINE_IO: bool = true;

    /// Registers its flow with a shared server: runs on every pod, never leader elected.
    const REGISTERS: bool = false;

    /// Starts without the startup jitter.
    const STARTS_AT_ONCE: bool = Self::REGISTERS;

    /// The external systems the task connects to.
    fn connections(&self) -> Vec<Connection> {
        Vec::new()
    }

    /// Checks the config at load time, before anything runs.
    fn validate(&self) -> Result<(), TaskError> {
        Ok(())
    }

    /// Handles events whose upstream task failed; other tasks pass them on.
    fn handles_failed(&self) -> bool {
        false
    }
}

/// The fields every task config has, implemented with `#[derive(TaskConfig)]`.
pub trait TaskConfig {
    /// The task's name, unique within its flow.
    fn name(&self) -> &str;
    /// The tasks this one takes events from; `None` means the previous one.
    fn depends_on(&self) -> Option<&[String]>;
    /// Retry settings that override the app's.
    fn retry(&self) -> Option<&crate::retry::RetryConfig>;
    /// Events handled at once, overriding the default.
    fn max_in_flight(&self) -> Option<NonZeroUsize>;
}

/// A task's place in the flow: its id and the channels it is wired to.
pub struct TaskIo {
    pub task_id: usize,
    pub task_type: &'static str,
    pub rx: Option<Receiver<Event>>,
    pub tx: Option<Sender<Event>>,
}

/// How a task runs, implemented by each task type's config.
pub trait Task: TaskConfig + TaskMeta + Send + Sync + 'static {
    /// Fails when a shared server the task needs isn't running; called when the flow loads.
    fn check(&self, _services: &Services) -> Result<(), TaskError> {
        Ok(())
    }

    /// Runs the task until its input ends or the flow is cancelled.
    fn run(
        self,
        io: TaskIo,
        context: Arc<TaskContext>,
    ) -> impl Future<Output = Result<(), TaskError>> + Send;
}

impl TaskMeta for super::buffer::Buffer {}
impl TaskMeta for super::convert::Convert {
    const CPU_INTENSIVE: bool = true;
}
impl TaskMeta for super::generate::Generate {
    const STARTS_AT_ONCE: bool = true;

    fn validate(&self) -> Result<(), TaskError> {
        Ok(super::generate::Generate::validate(self)?)
    }
}
impl TaskMeta for super::inproc::endpoint::Endpoint {
    const REGISTERS: bool = true;
}
impl TaskMeta for super::inproc::request::Request {}
impl TaskMeta for super::iterate::Iterate {
    const CPU_INTENSIVE: bool = true;
}
impl TaskMeta for super::log::Log {
    const CPU_INTENSIVE: bool = true;
}
impl TaskMeta for super::script::Script {
    const CPU_INTENSIVE: bool = true;

    fn validate(&self) -> Result<(), TaskError> {
        match &self.code {
            crate::resource::Source::Inline(code) => Ok(self.limits.compile(code)?),
            crate::resource::Source::Resource { .. } => Ok(()),
        }
    }

    fn handles_failed(&self) -> bool {
        self.handles_failed
    }
}

impl Task for super::buffer::Buffer {
    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let mut builder = super::buffer::ProcessorBuilder::new()
            .config(Arc::new(self))
            .task_id(io.task_id)
            .task_type(io.task_type)
            .task_context(context);
        if let Some(rx) = io.rx {
            builder = builder.receiver(rx);
        }
        if let Some(tx) = io.tx {
            builder = builder.sender(tx);
        }
        builder.build().await?.run().await?;
        Ok(())
    }
}

impl Task for super::convert::Convert {
    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let mut builder = super::convert::ProcessorBuilder::new()
            .config(Arc::new(self))
            .task_id(io.task_id)
            .task_type(io.task_type)
            .task_context(context);
        if let Some(rx) = io.rx {
            builder = builder.receiver(rx);
        }
        if let Some(tx) = io.tx {
            builder = builder.sender(tx);
        }
        builder.build().await?.run().await?;
        Ok(())
    }
}

impl Task for super::generate::Generate {
    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let mut builder = super::generate::SubscriberBuilder::new()
            .config(Arc::new(self))
            .task_id(io.task_id)
            .task_type(io.task_type)
            .task_context(context);
        if let Some(tx) = io.tx {
            builder = builder.sender(tx);
        }
        builder.build().await?.run().await?;
        Ok(())
    }
}

impl Task for super::inproc::endpoint::Endpoint {
    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let mut builder = super::inproc::endpoint::ProcessorBuilder::new()
            .config(Arc::new(self))
            .task_id(io.task_id)
            .task_type(io.task_type)
            .task_context(context);
        if let Some(tx) = io.tx {
            builder = builder.sender(tx);
        }
        builder.build().await?.run().await?;
        Ok(())
    }
}

impl Task for super::inproc::request::Request {
    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let mut builder = super::inproc::request::ProcessorBuilder::new()
            .config(Arc::new(self))
            .task_id(io.task_id)
            .task_type(io.task_type)
            .task_context(context);
        if let Some(rx) = io.rx {
            builder = builder.receiver(rx);
        }
        if let Some(tx) = io.tx {
            builder = builder.sender(tx);
        }
        builder.build().await?.run().await?;
        Ok(())
    }
}

impl Task for super::iterate::Iterate {
    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let mut builder = super::iterate::ProcessorBuilder::new()
            .config(Arc::new(self))
            .task_id(io.task_id)
            .task_type(io.task_type)
            .task_context(context);
        if let Some(rx) = io.rx {
            builder = builder.receiver(rx);
        }
        if let Some(tx) = io.tx {
            builder = builder.sender(tx);
        }
        builder.build().await?.run().await?;
        Ok(())
    }
}

impl Task for super::log::Log {
    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let mut builder = super::log::ProcessorBuilder::new()
            .config(Arc::new(self))
            .task_id(io.task_id)
            .task_type(io.task_type)
            .task_context(context);
        if let Some(rx) = io.rx {
            builder = builder.receiver(rx);
        }
        if let Some(tx) = io.tx {
            builder = builder.sender(tx);
        }
        builder.build().await?.run().await?;
        Ok(())
    }
}

impl Task for super::script::Script {
    async fn run(self, io: TaskIo, context: Arc<TaskContext>) -> Result<(), TaskError> {
        let mut builder = super::script::ProcessorBuilder::new()
            .config(Arc::new(self))
            .task_id(io.task_id)
            .task_type(io.task_type)
            .task_context(context);
        if let Some(rx) = io.rx {
            builder = builder.receiver(rx);
        }
        if let Some(tx) = io.tx {
            builder = builder.sender(tx);
        }
        builder.build().await?.run().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_url_shows_as_its_host_without_the_password() {
        let connection = Connection::new(CONNECTOR, None).target(&["nats://user:secret@nats:4222"]);

        assert_eq!(connection.target.as_deref(), Some("nats:4222"));
    }

    #[test]
    fn values_set_per_event_are_left_out_of_the_target() {
        let connection = Connection::new(CONNECTOR, Some(Path::new("{{event.data.credentials}}")))
            .target(&["http://gateway:3002/v1", "{{event.data.model}}"]);

        assert_eq!(connection.target.as_deref(), Some("gateway:3002"));
        assert!(connection.per_event);
    }

    #[test]
    fn environment_templates_show_what_they_render_to_without_the_password() {
        std::env::set_var("META_TEST_NATS_URL", "nats://user:secret@nats:4222");
        std::env::set_var("META_TEST_BROKERS", "a:9092,b:9092");

        let connection = Connection::new(CONNECTOR, None).target(&[
            "{{env.META_TEST_NATS_URL}}",
            "{{env.META_TEST_BROKERS}}/topic",
        ]);

        assert_eq!(
            connection.target.as_deref(),
            Some("nats:4222 · a:9092,b:9092")
        );
        assert!(!connection.per_event);
    }

    #[test]
    fn an_unset_environment_variable_shows_as_written() {
        let connection = Connection::new(CONNECTOR, None).target(&["{{env.META_TEST_UNSET}}"]);

        assert_eq!(
            connection.target.as_deref(),
            Some("{{env.META_TEST_UNSET}}")
        );
    }

    #[test]
    fn values_that_are_not_urls_lose_anything_that_could_be_a_secret() {
        assert_eq!(host("user:secret@nats:4222"), "nats:4222");
        assert_eq!(host("{{env.BASE}}/v1?api_key=secret"), "{{env.BASE}}");
        assert_eq!(host("a:9092,b:9092"), "a:9092,b:9092");
        assert_eq!(host("ghcr.io/a/b?token=secret"), "ghcr.io/a/b");
        assert_eq!(host("provider/model-1.5"), "provider/model-1.5");
    }

    #[test]
    fn an_event_value_in_the_path_keeps_the_host() {
        let connection =
            Connection::new(CONNECTOR, None).target(&["https://api.a.com/users/{{event.data.id}}"]);

        assert_eq!(connection.target.as_deref(), Some("api.a.com"));
        assert!(!connection.per_event);
    }
}
