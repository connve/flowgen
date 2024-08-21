/// This Source Code Form is subject to the terms of the Mozilla Public
/// License, v. 2.0. If a copy of the MPL was not distributed with this
/// file, You can obtain one at https://mozilla.org/MPL/2.0/.
#[path = ""]
pub mod storage {
    #[path = "google.storage.v2.rs"]
    pub mod v2;
    pub const ENDPOINT: &str = "https://storage.googleapis.com";
}

#[path = ""]
pub mod iam {
    #[path = "google.iam.v1.rs"]
    pub mod v1;
}

#[path = "google.r#type.rs"]
pub mod r#type;
