pub mod api {
    include!(concat!("out", "/google.api.rs"));
}
pub mod iam {
    pub mod v1 {
        include!(concat!("out", "/google.iam.v1.rs"));
    }
}
pub mod protobuf {
    include!(concat!("out", "/google.protobuf.rs"));
}
pub mod r#type {
    include!(concat!("out", "/google.r#type.rs"));
}
pub mod storage {
    pub mod v2 {
        include!(concat!("out", "/google.storage.v2.rs"));
    }
    pub const ENDPOINT: &str = "https://storage.googleapis.com";
    pub mod config;
    pub mod context;
    pub mod subscriber;
}

pub mod client;
