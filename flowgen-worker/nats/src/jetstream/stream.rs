use async_nats::jetstream::{self, stream::Config};
use std::time::Duration;

/// Default maximum age for messages in seconds (24 hours).
const DEFAULT_MAX_AGE_SECS: u64 = 86400;

/// Default maximum messages per subject.
const DEFAULT_MAX_MESSAGES_PER_SUBJECT: i64 = 1;

/// Errors that can occur during stream operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Failed to create JetStream stream.
    #[error("Failed to create JetStream stream: {source}")]
    CreateStream {
        #[source]
        source: async_nats::jetstream::context::CreateStreamError,
    },
    /// Failed to get existing JetStream stream.
    #[error("Failed to get JetStream stream: {source}")]
    GetStream {
        #[source]
        source: async_nats::jetstream::context::GetStreamError,
    },
    /// Failed to make request to JetStream.
    #[error("Failed to make request to JetStream: {source}")]
    Request {
        #[source]
        source: async_nats::jetstream::context::RequestError,
    },
}

/// Creates or updates a JetStream stream based on the provided configuration.
///
/// If the stream exists:
/// - Merges subjects (existing + new, deduplicated)
/// - Uses new config values if provided, otherwise keeps existing values
///
/// If the stream doesn't exist:
/// - Creates it with provided config
/// - Uses sensible defaults for unspecified values
///
/// Returns the JetStream context for further use.
pub async fn create_or_update_stream(
    jetstream: jetstream::Context,
    stream_opts: &super::config::StreamOptions,
) -> Result<jetstream::Context, Error> {
    let existing_stream = jetstream.get_stream(&stream_opts.name).await;

    match existing_stream {
        Ok(mut stream) => {
            // Stream exists, update it by merging with existing config
            let existing_config = stream
                .info()
                .await
                .map_err(|e| Error::Request { source: e })?
                .config
                .clone();

            // Merge subjects
            let mut subjects = existing_config.subjects.clone();
            subjects.extend(stream_opts.subjects.clone());
            subjects.sort();
            subjects.dedup();

            // Use new values if provided, otherwise keep existing
            let retention = stream_opts
                .retention
                .as_ref()
                .map(|r| match r {
                    super::config::RetentionPolicy::Limits => {
                        jetstream::stream::RetentionPolicy::Limits
                    }
                    super::config::RetentionPolicy::Interest => {
                        jetstream::stream::RetentionPolicy::Interest
                    }
                    super::config::RetentionPolicy::WorkQueue => {
                        jetstream::stream::RetentionPolicy::WorkQueue
                    }
                })
                .unwrap_or(existing_config.retention);

            let discard = stream_opts
                .discard
                .as_ref()
                .map(|d| match d {
                    super::config::DiscardPolicy::Old => jetstream::stream::DiscardPolicy::Old,
                    super::config::DiscardPolicy::New => jetstream::stream::DiscardPolicy::New,
                })
                .unwrap_or(existing_config.discard);

            let max_age = stream_opts
                .max_age_secs
                .map(Duration::from_secs)
                .unwrap_or(existing_config.max_age);

            let max_messages_per_subject = stream_opts
                .max_messages_per_subject
                .unwrap_or(existing_config.max_messages_per_subject);

            let description = stream_opts
                .description
                .clone()
                .or(existing_config.description);

            let updated_config = Config {
                name: stream_opts.name.clone(),
                description,
                max_messages_per_subject,
                subjects,
                discard,
                retention,
                max_age,
                ..existing_config
            };

            jetstream
                .update_stream(updated_config)
                .await
                .map_err(|e| Error::CreateStream { source: e })?;
        }
        Err(_) => {
            // Stream doesn't exist, create it with defaults for unspecified values
            let retention = stream_opts
                .retention
                .as_ref()
                .map(|r| match r {
                    super::config::RetentionPolicy::Limits => {
                        jetstream::stream::RetentionPolicy::Limits
                    }
                    super::config::RetentionPolicy::Interest => {
                        jetstream::stream::RetentionPolicy::Interest
                    }
                    super::config::RetentionPolicy::WorkQueue => {
                        jetstream::stream::RetentionPolicy::WorkQueue
                    }
                })
                .unwrap_or(jetstream::stream::RetentionPolicy::Limits);

            let discard = stream_opts
                .discard
                .as_ref()
                .map(|d| match d {
                    super::config::DiscardPolicy::Old => jetstream::stream::DiscardPolicy::Old,
                    super::config::DiscardPolicy::New => jetstream::stream::DiscardPolicy::New,
                })
                .unwrap_or(jetstream::stream::DiscardPolicy::Old);

            let max_age = stream_opts
                .max_age_secs
                .map(Duration::from_secs)
                .unwrap_or_else(|| Duration::from_secs(DEFAULT_MAX_AGE_SECS));

            let max_messages_per_subject = stream_opts
                .max_messages_per_subject
                .unwrap_or(DEFAULT_MAX_MESSAGES_PER_SUBJECT);

            // Default to wildcard if no subjects specified
            let subjects = if stream_opts.subjects.is_empty() {
                vec![">".to_string()]
            } else {
                stream_opts.subjects.clone()
            };

            let stream_config = Config {
                name: stream_opts.name.clone(),
                description: stream_opts.description.clone(),
                max_messages_per_subject,
                subjects,
                discard,
                retention,
                max_age,
                ..Default::default()
            };

            jetstream
                .create_stream(stream_config)
                .await
                .map_err(|e| Error::CreateStream { source: e })?;
        }
    }

    Ok(jetstream)
}
