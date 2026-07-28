//! # Flowgen MongoDB Integration
//!
//! This module provides MongoDB integration components for the Flowgen data processing framework.
//! It includes support for change data capture (CDC), collection read/write, and message conversion
//! utilities for working with MongoDB collections and change streams.
/// Configuration for Mongo change data capture.
pub mod change_stream;
/// Configuration for Mongo client creator.
pub mod client;
/// Mongo collection read/write processor.
pub mod collection;
/// Configuration structures for Mongo processors.
pub mod config;
/// Message conversion utilities for Mongo integration.
pub mod message;
