///
/// Contains information about a topic and uniquely identifies it. TopicInfo is returned by the GetTopic RPC method.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TopicInfo {
    /// Topic name
    #[prost(string, tag = "1")]
    pub topic_name: ::prost::alloc::string::String,
    /// Tenant/org GUID
    #[prost(string, tag = "2")]
    pub tenant_guid: ::prost::alloc::string::String,
    /// Is publishing allowed?
    #[prost(bool, tag = "3")]
    pub can_publish: bool,
    /// Is subscription allowed?
    #[prost(bool, tag = "4")]
    pub can_subscribe: bool,
    /// ID of the current topic schema, which can be used for
    /// publishing of generically serialized events.
    #[prost(string, tag = "5")]
    pub schema_id: ::prost::alloc::string::String,
    /// RPC ID used to trace errors.
    #[prost(string, tag = "6")]
    pub rpc_id: ::prost::alloc::string::String,
}
///
/// A request message for GetTopic. Note that the tenant/org is not directly referenced
/// in the request, but is implicitly identified by the authentication headers.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TopicRequest {
    /// The name of the topic to retrieve.
    #[prost(string, tag = "1")]
    pub topic_name: ::prost::alloc::string::String,
}
///
/// Reserved for future use.
/// Header that contains information for distributed tracing, filtering, routing, etc.
/// For example, X-B3-* headers assigned by a publisher are stored with the event and
/// can provide a full distributed trace of the event across its entire lifecycle.
#[derive(serde::Serialize, serde::Deserialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EventHeader {
    #[prost(string, tag = "1")]
    pub key: ::prost::alloc::string::String,
    #[prost(bytes = "vec", tag = "2")]
    pub value: ::prost::alloc::vec::Vec<u8>,
}
///
/// Represents an event that an event publishing app creates.
#[derive(serde::Serialize, serde::Deserialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProducerEvent {
    /// Either a user-provided ID or a system generated guid
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    /// Schema fingerprint for this event which is hash of the schema
    #[prost(string, tag = "2")]
    pub schema_id: ::prost::alloc::string::String,
    /// The message data field
    #[prost(bytes = "vec", tag = "3")]
    pub payload: ::prost::alloc::vec::Vec<u8>,
    /// Reserved for future use. Key-value pairs of headers.
    #[prost(message, repeated, tag = "4")]
    pub headers: ::prost::alloc::vec::Vec<EventHeader>,
}
///
/// Represents an event that is consumed in a subscriber client.
/// In addition to the fields in ProducerEvent, ConsumerEvent has the replay_id field.
#[derive(serde::Serialize, serde::Deserialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConsumerEvent {
    /// The event with fields identical to ProducerEvent
    #[prost(message, optional, tag = "1")]
    pub event: ::core::option::Option<ProducerEvent>,
    /// The replay ID of the event.
    /// A subscriber app can store the replay ID. When the app restarts, it can resume subscription
    /// starting from events in the event bus after the event with that replay ID.
    #[prost(bytes = "vec", tag = "2")]
    pub replay_id: ::prost::alloc::vec::Vec<u8>,
}
///
/// Event publish result that the Publish RPC method returns. The result contains replay_id or a publish error.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PublishResult {
    /// Replay ID of the event
    #[prost(bytes = "vec", tag = "1")]
    pub replay_id: ::prost::alloc::vec::Vec<u8>,
    /// Publish error if any
    #[prost(message, optional, tag = "2")]
    pub error: ::core::option::Option<Error>,
    /// Correlation key of the ProducerEvent
    #[prost(string, tag = "3")]
    pub correlation_key: ::prost::alloc::string::String,
}
/// Contains error information for an error that an RPC method returns.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Error {
    /// Error code
    #[prost(enumeration = "ErrorCode", tag = "1")]
    pub code: i32,
    /// Error message
    #[prost(string, tag = "2")]
    pub msg: ::prost::alloc::string::String,
}
///
/// Request for the Subscribe streaming RPC method. This request is used to:
/// 1. Establish the initial subscribe stream.
/// 2. Request more events from the subscription stream.
/// Flow Control is handled by the subscriber via num_requested.
/// A client can specify a starting point for the subscription with replay_preset and replay_id combinations.
/// If no replay_preset is specified, the subscription starts at LATEST (tip of the stream).
/// replay_preset and replay_id values are only consumed as part of the first FetchRequest. If
/// a client needs to start at another point in the stream, it must start a new subscription.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchRequest {
    ///
    /// Identifies a topic for subscription in the very first FetchRequest of the stream. The topic cannot change
    /// in subsequent FetchRequests within the same subscribe stream, but can be omitted for efficiency.
    #[prost(string, tag = "1")]
    pub topic_name: ::prost::alloc::string::String,
    ///
    /// Subscription starting point. This is consumed only as part of the first FetchRequest
    /// when the subscription is set up.
    #[prost(enumeration = "ReplayPreset", tag = "2")]
    pub replay_preset: i32,
    ///
    /// If replay_preset of CUSTOM is selected, specify the subscription point to start after.
    /// This is consumed only as part of the first FetchRequest when the subscription is set up.
    #[prost(bytes = "vec", tag = "3")]
    pub replay_id: ::prost::alloc::vec::Vec<u8>,
    ///
    /// Number of events a client is ready to accept. Each subsequent FetchRequest informs the server
    /// of additional processing capacity available on the client side. There is no guarantee of equal number of
    /// FetchResponse messages to be sent back. There is not necessarily a correspondence between
    /// number of requested events in FetchRequest and the number of events returned in subsequent
    /// FetchResponses.
    #[prost(int32, tag = "4")]
    pub num_requested: i32,
    /// For internal Salesforce use only.
    #[prost(string, tag = "5")]
    pub auth_refresh: ::prost::alloc::string::String,
}
///
/// Response for the Subscribe streaming RPC method. This returns ConsumerEvent(s).
/// If there are no events to deliver, the server sends an empty batch fetch response with the latest replay ID. The
/// empty fetch response is sent within 270 seconds. An empty fetch response provides a periodic keepalive from the
/// server and the latest replay ID.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchResponse {
    /// Received events for subscription for client consumption
    #[prost(message, repeated, tag = "1")]
    pub events: ::prost::alloc::vec::Vec<ConsumerEvent>,
    /// Latest replay ID of a subscription. Enables clients with an updated replay value so that they can keep track
    /// of their last consumed replay. Clients will not have to start a subscription at a very old replay in the case where a resubscribe is necessary.
    #[prost(bytes = "vec", tag = "2")]
    pub latest_replay_id: ::prost::alloc::vec::Vec<u8>,
    /// RPC ID used to trace errors.
    #[prost(string, tag = "3")]
    pub rpc_id: ::prost::alloc::string::String,
    /// Number of remaining events to be delivered to the client for a Subscribe RPC call.
    #[prost(int32, tag = "4")]
    pub pending_num_requested: i32,
}
///
/// Request for the GetSchema RPC method. The schema request is based on the event schema ID.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SchemaRequest {
    /// Schema fingerprint for this event, which is a hash of the schema.
    #[prost(string, tag = "1")]
    pub schema_id: ::prost::alloc::string::String,
}
///
/// Response for the GetSchema RPC method. This returns the schema ID and schema of an event.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SchemaInfo {
    /// Avro schema in JSON format
    #[prost(string, tag = "1")]
    pub schema_json: ::prost::alloc::string::String,
    /// Schema fingerprint
    #[prost(string, tag = "2")]
    pub schema_id: ::prost::alloc::string::String,
    /// RPC ID used to trace errors.
    #[prost(string, tag = "3")]
    pub rpc_id: ::prost::alloc::string::String,
}
/// Request for the Publish and PublishStream RPC method.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PublishRequest {
    /// Topic to publish on
    #[prost(string, tag = "1")]
    pub topic_name: ::prost::alloc::string::String,
    /// Batch of ProducerEvent(s) to send
    #[prost(message, repeated, tag = "2")]
    pub events: ::prost::alloc::vec::Vec<ProducerEvent>,
    /// For internal Salesforce use only.
    #[prost(string, tag = "3")]
    pub auth_refresh: ::prost::alloc::string::String,
}
///
/// Response for the Publish and PublishStream RPC methods. This returns
/// a list of PublishResults for each event that the client attempted to
/// publish. PublishResult indicates if publish succeeded or not
/// for each event. It also returns the schema ID that was used to create
/// the ProducerEvents in the PublishRequest.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PublishResponse {
    /// Publish results
    #[prost(message, repeated, tag = "1")]
    pub results: ::prost::alloc::vec::Vec<PublishResult>,
    /// Schema fingerprint for this event, which is a hash of the schema
    #[prost(string, tag = "2")]
    pub schema_id: ::prost::alloc::string::String,
    /// RPC ID used to trace errors.
    #[prost(string, tag = "3")]
    pub rpc_id: ::prost::alloc::string::String,
}
///
/// This feature is part of an open beta release and is subject to the applicable
/// Beta Services Terms provided at Agreements and Terms
/// (<https://www.salesforce.com/company/legal/agreements/>).
///
/// Request for the ManagedSubscribe streaming RPC method. This request is used to:
/// 1. Establish the initial managed subscribe stream.
/// 2. Request more events from the subscription stream.
/// 3. Commit a Replay ID using CommitReplayRequest.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ManagedFetchRequest {
    ///
    /// Managed subscription ID or developer name. This value corresponds to the
    /// ID or developer name of the ManagedEventSubscription Tooling API record.
    /// This value is consumed as part of the first ManagedFetchRequest only.
    /// The subscription_id cannot change in subsequent ManagedFetchRequests
    /// within the same subscribe stream, but can be omitted for efficiency.
    #[prost(string, tag = "1")]
    pub subscription_id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub developer_name: ::prost::alloc::string::String,
    ///
    /// Number of events a client is ready to accept. Each subsequent FetchRequest informs the server
    /// of additional processing capacity available on the client side. There is no guarantee of equal number of
    /// FetchResponse messages to be sent back. There is not necessarily a correspondence between
    /// number of requested events in FetchRequest and the number of events returned in subsequent
    /// FetchResponses.
    #[prost(int32, tag = "3")]
    pub num_requested: i32,
    /// For internal Salesforce use only.
    #[prost(string, tag = "4")]
    pub auth_refresh: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "5")]
    pub commit_replay_id_request: ::core::option::Option<CommitReplayRequest>,
}
///
/// This feature is part of an open beta release and is subject to the applicable
/// Beta Services Terms provided at Agreements and Terms
/// (<https://www.salesforce.com/company/legal/agreements/>).
///
/// Response for the ManagedSubscribe streaming RPC method. This can return
/// ConsumerEvent(s) or CommitReplayResponse along with other metadata.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ManagedFetchResponse {
    /// Received events for subscription for client consumption
    #[prost(message, repeated, tag = "1")]
    pub events: ::prost::alloc::vec::Vec<ConsumerEvent>,
    /// Latest replay ID of a subscription.
    #[prost(bytes = "vec", tag = "2")]
    pub latest_replay_id: ::prost::alloc::vec::Vec<u8>,
    /// RPC ID used to trace errors.
    #[prost(string, tag = "3")]
    pub rpc_id: ::prost::alloc::string::String,
    /// Number of remaining events to be delivered to the client for a Subscribe RPC call.
    #[prost(int32, tag = "4")]
    pub pending_num_requested: i32,
    /// commit response
    #[prost(message, optional, tag = "5")]
    pub commit_response: ::core::option::Option<CommitReplayResponse>,
}
///
/// This feature is part of an open beta release and is subject to the applicable
/// Beta Services Terms provided at Agreements and Terms
/// (<https://www.salesforce.com/company/legal/agreements/>).
///
/// Request to commit a Replay ID for the last processed event or for the latest
/// replay ID received in an empty batch of events.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommitReplayRequest {
    /// commit_request_id to identify commit responses
    #[prost(string, tag = "1")]
    pub commit_request_id: ::prost::alloc::string::String,
    /// replayId to commit
    #[prost(bytes = "vec", tag = "2")]
    pub replay_id: ::prost::alloc::vec::Vec<u8>,
}
///
/// This feature is part of an open beta release and is subject to the applicable
/// Beta Services Terms provided at Agreements and Terms
/// (<https://www.salesforce.com/company/legal/agreements/>).
///
/// There is no guaranteed 1:1 CommitReplayRequest to CommitReplayResponse.
/// N CommitReplayRequest(s) can get compressed in a batch resulting in a single
/// CommitReplayResponse which reflects the latest values of last
/// CommitReplayRequest in that batch.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommitReplayResponse {
    /// commit_request_id to identify commit responses.
    #[prost(string, tag = "1")]
    pub commit_request_id: ::prost::alloc::string::String,
    /// replayId that may have been committed
    #[prost(bytes = "vec", tag = "2")]
    pub replay_id: ::prost::alloc::vec::Vec<u8>,
    /// for failed commits
    #[prost(message, optional, tag = "3")]
    pub error: ::core::option::Option<Error>,
    /// time when server received request in epoch ms
    #[prost(int64, tag = "4")]
    pub process_time: i64,
}
/// Supported error codes
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ErrorCode {
    Unknown = 0,
    Publish = 1,
    /// ErrorCode for unrecoverable commit errors.
    Commit = 2,
}
impl ErrorCode {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ErrorCode::Unknown => "UNKNOWN",
            ErrorCode::Publish => "PUBLISH",
            ErrorCode::Commit => "COMMIT",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "UNKNOWN" => Some(Self::Unknown),
            "PUBLISH" => Some(Self::Publish),
            "COMMIT" => Some(Self::Commit),
            _ => None,
        }
    }
}
///
/// Supported subscription replay start values.
/// By default, the subscription will start at the tip of the stream if ReplayPreset is not specified.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ReplayPreset {
    /// Start the subscription at the tip of the stream.
    Latest = 0,
    /// Start the subscription at the earliest point in the stream.
    Earliest = 1,
    /// Start the subscription after a custom point in the stream. This must be set with a valid replay_id in the FetchRequest.
    Custom = 2,
}
impl ReplayPreset {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ReplayPreset::Latest => "LATEST",
            ReplayPreset::Earliest => "EARLIEST",
            ReplayPreset::Custom => "CUSTOM",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "LATEST" => Some(Self::Latest),
            "EARLIEST" => Some(Self::Earliest),
            "CUSTOM" => Some(Self::Custom),
            _ => None,
        }
    }
}
/// Generated client implementations.
pub mod pub_sub_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    ///
    /// The Pub/Sub API provides a single interface for publishing and subscribing to platform events, including real-time
    /// event monitoring events, and change data capture events. The Pub/Sub API is a gRPC API that is based on HTTP/2.
    ///
    /// A session token is needed to authenticate. Any of the Salesforce supported
    /// OAuth flows can be used to obtain a session token:
    /// https://help.salesforce.com/articleView?id=sf.remoteaccess_oauth_flows.htm&type=5
    ///
    /// For each RPC, a client needs to pass authentication information
    /// as metadata headers (https://www.grpc.io/docs/guides/concepts/#metadata) with their method call.
    ///
    /// For Salesforce session token authentication, use:
    ///   accesstoken : access token
    ///   instanceurl : Salesforce instance URL
    ///   tenantid : tenant/org id of the client
    ///
    /// StatusException is thrown in case of response failure for any request.
    #[derive(Debug, Clone)]
    pub struct PubSubClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl PubSubClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> PubSubClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> PubSubClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            PubSubClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        ///
        /// Bidirectional streaming RPC to subscribe to a Topic. The subscription is pull-based. A client can request
        /// for more events as it consumes events. This enables a client to handle flow control based on the client's processing speed.
        ///
        /// Typical flow:
        /// 1. Client requests for X number of events via FetchRequest.
        /// 2. Server receives request and delivers events until X events are delivered to the client via one or more FetchResponse messages.
        /// 3. Client consumes the FetchResponse messages as they come.
        /// 4. Client issues new FetchRequest for Y more number of events. This request can
        ///    come before the server has delivered the earlier requested X number of events
        ///    so the client gets a continuous stream of events if any.
        ///
        /// If a client requests more events before the server finishes the last
        /// requested amount, the server appends the new amount to the current amount of
        /// events it still needs to fetch and deliver.
        ///
        /// A client can subscribe at any point in the stream by providing a replay option in the first FetchRequest.
        /// The replay option is honored for the first FetchRequest received from a client. Any subsequent FetchRequests with a
        /// new replay option are ignored. A client needs to call the Subscribe RPC again to restart the subscription
        /// at a new point in the stream.
        ///
        /// The first FetchRequest of the stream identifies the topic to subscribe to.
        /// If any subsequent FetchRequest provides topic_name, it must match what
        /// was provided in the first FetchRequest; otherwise, the RPC returns an error
        /// with INVALID_ARGUMENT status.
        pub async fn subscribe(
            &mut self,
            request: impl tonic::IntoStreamingRequest<Message = super::FetchRequest>,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::FetchResponse>>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/eventbus.v1.PubSub/Subscribe",
            );
            let mut req = request.into_streaming_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("eventbus.v1.PubSub", "Subscribe"));
            self.inner.streaming(req, path, codec).await
        }
        /// Get the event schema for a topic based on a schema ID.
        pub async fn get_schema(
            &mut self,
            request: impl tonic::IntoRequest<super::SchemaRequest>,
        ) -> std::result::Result<tonic::Response<super::SchemaInfo>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/eventbus.v1.PubSub/GetSchema",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("eventbus.v1.PubSub", "GetSchema"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Get the topic Information related to the specified topic.
        pub async fn get_topic(
            &mut self,
            request: impl tonic::IntoRequest<super::TopicRequest>,
        ) -> std::result::Result<tonic::Response<super::TopicInfo>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/eventbus.v1.PubSub/GetTopic",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("eventbus.v1.PubSub", "GetTopic"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Send a publish request to synchronously publish events to a topic.
        pub async fn publish(
            &mut self,
            request: impl tonic::IntoRequest<super::PublishRequest>,
        ) -> std::result::Result<
            tonic::Response<super::PublishResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/eventbus.v1.PubSub/Publish",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("eventbus.v1.PubSub", "Publish"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Bidirectional Streaming RPC to publish events to the event bus.
        /// PublishRequest contains the batch of events to publish.
        ///
        /// The first PublishRequest of the stream identifies the topic to publish on.
        /// If any subsequent PublishRequest provides topic_name, it must match what
        /// was provided in the first PublishRequest; otherwise, the RPC returns an error
        /// with INVALID_ARGUMENT status.
        ///
        /// The server returns a PublishResponse for each PublishRequest when publish is
        /// complete for the batch. A client does not have to wait for a PublishResponse
        /// before sending a new PublishRequest, i.e. multiple publish batches can be queued
        /// up, which allows for higher publish rate as a client can asynchronously
        /// publish more events while publishes are still in flight on the server side.
        ///
        /// PublishResponse holds a PublishResult for each event published that indicates success
        /// or failure of the publish. A client can then retry the publish as needed before sending
        /// more PublishRequests for new events to publish.
        ///
        /// A client must send a valid publish request with one or more events every 70 seconds to hold on to the stream.
        /// Otherwise, the server closes the stream and notifies the client. Once the client is notified of the stream closure,
        /// it must make a new PublishStream call to resume publishing.
        pub async fn publish_stream(
            &mut self,
            request: impl tonic::IntoStreamingRequest<Message = super::PublishRequest>,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::PublishResponse>>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/eventbus.v1.PubSub/PublishStream",
            );
            let mut req = request.into_streaming_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("eventbus.v1.PubSub", "PublishStream"));
            self.inner.streaming(req, path, codec).await
        }
        ///
        /// This feature is part of an open beta release and is subject to the applicable
        /// Beta Services Terms provided at Agreements and Terms
        /// (https://www.salesforce.com/company/legal/agreements/).
        ///
        /// Same as Subscribe, but for Managed Subscription clients.
        /// This feature is part of an open beta release.
        pub async fn managed_subscribe(
            &mut self,
            request: impl tonic::IntoStreamingRequest<
                Message = super::ManagedFetchRequest,
            >,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::ManagedFetchResponse>>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/eventbus.v1.PubSub/ManagedSubscribe",
            );
            let mut req = request.into_streaming_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("eventbus.v1.PubSub", "ManagedSubscribe"));
            self.inner.streaming(req, path, codec).await
        }
    }
}
