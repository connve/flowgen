use async_nats::jetstream::{self, stream::Config};
use flowgen_core::client::Client;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("There was an error with connecting to a Nats Server.")]
    NatsClientAuth(#[source] crate::client::Error),
    #[error("Nats Client is missing / not initialized properly.")]
    NatsClientMissing(),
    #[error("Failed to publish message to Nats Jetstream.")]
    NatsPublish(#[source] async_nats::jetstream::context::PublishError),
    #[error("Failed to create or update Nats Jetstream.")]
    NatsCreateStream(#[source] async_nats::jetstream::context::CreateStreamError),
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

            // Create or update stream according to config.
            let stream_config = Config {
                name: self.config.stream_name.clone(),
                description: self.config.stream_description,
                subjects: self.config.subjects,
                ..Default::default()
            };

            let stream = context.get_stream(self.config.stream_name).await;

            match stream {
                Ok(_) => {
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
