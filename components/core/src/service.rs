//! This Source Code Form is subject to the terms of the Mozilla Public
//! License, v. 2.0. If a copy of the MPL was not distributed with this
//! file, You can obtain one at https://mozilla.org/MPL/2.0/.

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("An error resulting from a failed attempt to construct a URI")]
    InvalidUri(#[source] tonic::codegen::http::uri::InvalidUri),
    #[error("Error that originate from the client or server")]
    Transport(#[source] tonic::transport::Error),
}

#[derive(Debug)]
pub struct Service {
    endpoint: Option<String>,
    pub channel: Option<tonic::transport::Channel>,
}

impl super::client::Client for Service {
    type Error = Error;
    async fn connect(mut self) -> Result<Self, Error> {
        if let Some(endpoint) = self.endpoint.take() {
            let tls_config = tonic::transport::ClientTlsConfig::new();
            let channel = tonic::transport::Channel::from_shared(endpoint)
                .map_err(Error::InvalidUri)?
                .tls_config(tls_config)
                .map_err(Error::Transport)?
                .connect()
                .await
                .map_err(Error::Transport)?;
            self.channel = Some(channel);
        }
        Ok(self)
    }
}

#[derive(Default)]
pub struct Builder {
    endpoint: Option<String>,
}

impl Builder {
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> Self {
        Builder {
            ..Default::default()
        }
    }

    pub fn with_endpoint(&mut self, endpoint: String) -> &mut Self {
        self.endpoint = Some(endpoint);
        self
    }

    pub fn build(&mut self) -> Result<Service, Error> {
        Ok(Service {
            endpoint: self.endpoint.take(),
            channel: None,
        })
    }
}
