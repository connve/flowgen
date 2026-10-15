//! HTML processing tasks for flowgen.
//!
//! Provides HTML parsing and extraction capabilities. Tasks in this crate operate
//! on HTML content received in event data (typically fetched by an upstream
//! `http_request` task).

use flowgen_core::task::meta::{Connector, Icon, TaskMeta};

/// HTML scrape task for extracting structured data from HTML documents.
pub mod scrape {
    /// Configuration for the HTML scrape processor.
    pub mod config;
    /// Processor implementation for HTML extraction.
    pub mod processor;
}

/// The connector of this crate's task types.
pub const CONNECTOR: Connector = Connector {
    name: "html",
    label: "HTML",
    icon: Icon::Svg(include_str!("../icons/html.svg")),
    describe_credentials: None,
};

impl TaskMeta for scrape::config::Processor {
    const CONNECTOR: Connector = CONNECTOR;
    const CPU_INTENSIVE: bool = true;
}
