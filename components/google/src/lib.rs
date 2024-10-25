/// This Source Code Form is subject to the terms of the Mozilla Public
/// License, v. 2.0. If a copy of the MPL was not distributed with this
/// file, You can obtain one at https://mozilla.org/MPL/2.0/.
pub mod api {
    include!("google.api.rs");
}
pub mod iam {
    pub mod v1 {
        include!("google.iam.v1.rs");
    }
}
pub mod protobuf {
    include!("google.protobuf.rs");
}
pub mod r#type {
    include!("google.r#type.rs");
}
pub mod storage {
    pub mod v2 {
        include!("google.storage.v2.rs");
    }
    pub const ENDPOINT: &str = "https://storage.googleapis.com";
}
