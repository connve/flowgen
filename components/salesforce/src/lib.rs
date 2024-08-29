/// This Source Code Form is subject to the terms of the Mozilla Public
/// License, v. 2.0. If a copy of the MPL was not distributed with this
/// file, You can obtain one at https://mozilla.org/MPL/2.0/.
#[path = ""]
pub mod eventbus {
    #[path = "eventbus.v1.rs"]
    pub mod v1;
    pub const ENDPOINT: &str = "https://api.pubsub.salesforce.com";
    pub const DE_ENDPOINT: &str = "https://api.deu.pubsub.salesforce.com";
}
pub mod auth;
pub mod pubsub;
