//! # Flowgen MongoDB Integration
//!
//! This module provides MongoDB integration components for the Flowgen data processing framework.
//! It includes support for change data capture (CDC), batch reading, and message conversion
//! utilities for working with MongoDB collections and change streams.
/// Configuration for Mongo change data capture.
pub mod change_stream;
/// Configuration structures for Mongo processors.
pub mod config;
/// Message conversion utilities for Mongo integration.
pub mod message;
/// Configuration for Mongo batch reader.
pub mod reader;
