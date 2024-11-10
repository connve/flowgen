//! This Source Code Form is subject to the terms of the Mozilla Public
//! License, v. 2.0. If a copy of the MPL was not distributed with this
//! file, You can obtain one at https://mozilla.org/MPL/2.0/.
use serde::Deserialize;

#[derive(Deserialize, Clone)]
pub struct Source {
    pub credentials: String,
    pub topic_list: Vec<String>,
}

#[derive(Deserialize)]
pub struct Target {
    pub credentials: String,
    pub topic: String,
}
