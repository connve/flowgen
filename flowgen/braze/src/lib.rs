//! Flowgen integration for the Braze REST API.
//!
//! This crate exposes Braze operations as flowgen tasks. The first supported
//! task is [`export::users::processor::Processor`], which calls Braze's
//! `POST /users/export/ids` endpoint.

#![deny(missing_docs)]

pub mod export;
