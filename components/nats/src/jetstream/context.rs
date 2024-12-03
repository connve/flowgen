use std::time::Duration;

use async_nats::jetstream::stream::{Config, DiscardPolicy, RetentionPolicy};
use flowgen_core::client::Client;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failed to connect to a Nats Server.")]
    NatsClientAuth(#[source] crate::client::Error),
    #[error("Nats Client is missing / not initialized properly.")]
    NatsClientMissing(),
    #[error("Failed to publish message to Nats Jetstream.")]
    NatsPublish(#[source] async_nats::jetstream::context::PublishError),
    #[error("Failed to create or update Nats Jetstream.")]
    NatsCreateStream(#[source] async_nats::jetstream::context::CreateStreamError),
    #[error("Failed to get Nats Jetstream.")]
    NatsGetStream(#[source] async_nats::jetstream::context::GetStreamError),
    #[error("Failed to get process request to Nats Server.")]
    NatsRequest(#[source] async_nats::jetstream::context::RequestError),
}

pub struct Context {
    pub jetstream: async_nats::jetstream::Context,
}

pub struct Builder {
    config: super::config::Target,
}

impl Builder {
    // Creates a new instance of a Builder.
    pub fn new(config: super::config::Target) -> Builder {
        Builder { config }
    }

    pub async fn build(self) -> Result<Context, Error> {
        // Connect to Nats Server.
        let client = crate::client::Builder::new()
            .with_credentials_path(self.config.credentials.into())
            .build()
            .map_err(Error::NatsClientAuth)?
            .connect()
            .await
            .map_err(Error::NatsClientAuth)?;

        if let Some(nats_client) = client.nats_client {
            let context = async_nats::jetstream::new(nats_client);

            let mut max_age = 86400;
            if let Some(config_max_age) = self.config.max_age {
                max_age = config_max_age
            }

            // Create or update stream according to config.
            let mut stream_config = Config {
                name: self.config.stream_name.clone(),
                description: self.config.stream_description,
                max_messages_per_subject: 1,
                subjects: self.config.subjects.clone(),
                discard: DiscardPolicy::Old,
                retention: RetentionPolicy::Limits,
                max_age: Duration::new(max_age, 0),
                ..Default::default()
            };

            let stream = context.get_stream(self.config.stream_name).await;

            match stream {
                Ok(_) => {
                    let mut subjects = stream
                        .map_err(Error::NatsGetStream)?
                        .info()
                        .await
                        .map_err(Error::NatsRequest)?
                        .config
                        .subjects
                        .clone();

                    subjects.extend(self.config.subjects);
                    subjects.sort();
                    subjects.dedup();
                    stream_config.subjects = subjects;

                    context
                        .update_stream(stream_config)
                        .await
                        .map_err(Error::NatsCreateStream)?;
                }
                Err(_) => {
                    context
                        .create_stream(stream_config)
                        .await
                        .map_err(Error::NatsCreateStream)?;
                }
            }

            Ok(Context { jetstream: context })
        } else {
            Err(Error::NatsClientMissing())
        }
    }
}
