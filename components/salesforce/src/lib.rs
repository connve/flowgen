//! This Source Code Form is subject to the terms of the Mozilla Public
//! License, v. 2.0. If a copy of the MPL was not distributed with this
//! file, You can obtain one at https://mozilla.org/MPL/2.0/.
pub mod client;
pub mod pubsub {
    pub mod config;
    pub mod context;
    pub mod subscriber;
    pub mod eventbus {
        pub mod v1 {
            include!(concat!("pubsub", "/eventbus.v1.rs"));
        }
        pub const ENDPOINT: &str = "https://api.pubsub.salesforce.com";
        pub const DE_ENDPOINT: &str = "https://api.deu.pubsub.salesforce.com";
    }
}
