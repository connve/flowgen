//! HTML processing tasks for flowgen.
//!
//! Provides HTML parsing and extraction capabilities. Tasks in this crate operate
//! on HTML content received in event data (typically fetched by an upstream
//! `http_request` task).

/// HTML scrape task for extracting structured data from HTML documents.
pub mod scrape {
    /// Configuration for the HTML scrape processor.
    pub mod config;
    /// Processor implementation for HTML extraction.
    pub mod processor;
}
