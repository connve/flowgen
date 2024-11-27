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
