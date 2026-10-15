use std::path::Path;

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

/// Implemented by each task type's config, in the crate that owns it.
pub trait TaskMeta {
    /// The connector the task type belongs to.
    const CONNECTOR: Connector = CONNECTOR;

    /// Whether the task mostly uses the CPU, such as parsing or encoding
    /// batches, rather than waiting on an external service.
    const CPU_INTENSIVE: bool = false;

    /// The external systems the task connects to.
    fn connections(&self) -> Vec<Connection> {
        Vec::new()
    }
}

impl TaskMeta for super::buffer::config::Processor {}
impl TaskMeta for super::convert::config::Processor {
    const CPU_INTENSIVE: bool = true;
}
impl TaskMeta for super::generate::config::Subscriber {}
impl TaskMeta for super::inproc::config::Endpoint {}
impl TaskMeta for super::inproc::config::Request {}
impl TaskMeta for super::iterate::config::Processor {
    const CPU_INTENSIVE: bool = true;
}
impl TaskMeta for super::log::config::Processor {
    const CPU_INTENSIVE: bool = true;
}
impl TaskMeta for super::script::config::Processor {
    const CPU_INTENSIVE: bool = true;
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
