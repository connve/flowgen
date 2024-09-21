/// This Source Code Form is subject to the terms of the Mozilla Public
/// License, v. 2.0. If a copy of the MPL was not distributed with this
/// file, You can obtain one at https://mozilla.org/MPL/2.0/.
#[derive(thiserror::Error, Debug)]
pub enum Error {}

#[derive(Debug)]
pub struct Client {
    endpoint: Option<String>,
    pub channel: Option<tonic::transport::Channel>,
}

impl Client {
    pub async fn connect(mut self) -> Result<Self, Box<dyn std::error::Error>> {
        if let Some(endpoint) = self.endpoint.take() {
            let tls_config = tonic::transport::ClientTlsConfig::new();
            let channel = tonic::transport::Channel::from_shared(endpoint)?
                .tls_config(tls_config)?
                .connect()
                .await?;
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

    pub fn build(&mut self) -> Result<Client, Error> {
        Ok(Client {
            endpoint: self.endpoint.take(),
            channel: None,
        })
    }
}
