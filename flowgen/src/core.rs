/// This Source Code Form is subject to the terms of the Mozilla Public
/// License, v. 2.0. If a copy of the MPL was not distributed with this
/// file, You can obtain one at https://mozilla.org/MPL/2.0/.
use tonic::transport::{Channel, ClientTlsConfig};

#[derive(Debug)]
pub struct Client {
    pub endpoint: String,
}

impl Client {
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> ClientBuilder {
        ClientBuilder::default()
    }
    pub async fn connect(&self) -> Result<Channel, Box<dyn std::error::Error>> {
        let tls_config = ClientTlsConfig::new();
        let channel = tonic::transport::Channel::from_shared(self.endpoint.clone())?
            .tls_config(tls_config)?
            .connect()
            .await?;

        Ok(channel)
    }
}

#[derive(Default)]
pub struct ClientBuilder {
    endpoint: String,
}

impl ClientBuilder {
    pub fn with_endpoint(&mut self, endpoint: String) -> &mut Self {
        self.endpoint = endpoint;
        self
    }

    pub fn build(&mut self) -> Result<Client, Box<dyn std::error::Error>> {
        Ok(Client {
            endpoint: self.endpoint.clone(),
        })
    }
}
