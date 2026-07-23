//! Protocol translations for the AI gateway.
//!
//! Each submodule owns the wire types and translation logic for one
//! client-facing protocol. The dispatcher in `super::server` picks the
//! submodule via the registration's `Protocol` value.
//!
//! - `completions` — OpenAI Chat Completions API (`POST /chat/completions`).
//! - `messages` — Anthropic Messages API (`POST /messages`).
//!
//! The dispatcher stays generic through the `ProtocolAdapter` trait
//! below: everything from "we have an `EventPayload` in hand" through
//! "we finished awaiting completion" is shared code; everything that
//! is protocol-shaped (parse the request, build the wire response,
//! format the streaming events) sits behind trait hooks.

use crate::ai_gateway::config::Usage;
use axum::response::Response;

pub mod completions;
pub mod messages;

// ---------------------------------------------------------------------------
// Adapter trait
// ---------------------------------------------------------------------------

/// Protocol adapter — bundles the small set of protocol-specific
/// hooks the shared dispatcher needs.
///
/// All state relevant to one request is threaded through method
/// arguments; adapters are stateless zero-sized types so the
/// dispatcher can materialise them per request without allocation.
pub trait ProtocolAdapter: Send + Sync + 'static {
    /// Streaming state machine specific to this protocol. Owns the
    /// wire format for the SSE frames the dispatcher emits.
    type StreamWriter: StreamWriter;

    /// Wire prefix for the request id (`chatcmpl-...` on OpenAI,
    /// `msg_...` on Anthropic).
    const REQUEST_ID_PREFIX: &'static str;

    /// Canonical protocol name written into `event.meta.protocol` and
    /// structured logs (`"openai"` or `"anthropic"`).
    const PROTOCOL_NAME: &'static str;

    /// Build the non-streaming HTTP response body from the leaf's
    /// completion data. Returns an `axum::Response` because the wire
    /// shape differs between protocols and axum's `Json` erases the
    /// concrete body type.
    fn build_blocking_response(
        request_id: String,
        model: String,
        created: i64,
        text: String,
        tool_calls: Vec<crate::ai_gateway::config::ToolCall>,
        usage: Option<Usage>,
    ) -> Response;

    /// Instantiate a stream writer scoped to one response.
    fn new_stream_writer(request_id: String, model: String, created: i64) -> Self::StreamWriter;

    /// Format an error frame that will be embedded inside an already
    /// open SSE stream (i.e. leaf-task failure surfaces as a valid
    /// event frame in the client's expected wire shape). One frame,
    /// including the trailing `\n\n`.
    fn error_sse_frame(message: &str) -> String;

    /// Whether the terminal `[DONE]` sentinel should be emitted after
    /// the last real frame. OpenAI requires it; Anthropic does not.
    const EMIT_DONE_SENTINEL: bool;
}

// ---------------------------------------------------------------------------
// Streaming writer
// ---------------------------------------------------------------------------

/// Stream state machine driven by the shared select loop.
///
/// Method call sequence is fixed:
/// 1. `open()` once at stream start;
/// 2. any number of `text_delta(fragment)` calls for progress chunks;
/// 3. optional `tool_calls(list)` when the leaf terminal event carries
///    tool invocations (called before `close`);
/// 4. `close(stop, usage, include_usage)` exactly once at stream end.
///
/// Each method returns pre-formatted SSE frames ready to push onto the
/// wire — the dispatcher never touches JSON directly.
pub trait StreamWriter: Send {
    /// Terminal reason distinguishing "normal completion" from
    /// "stopped because the model wants tools invoked". Concrete
    /// wire spelling lives inside the adapter.
    fn open(&mut self) -> Vec<String>;

    /// Incremental text fragment.
    fn text_delta(&mut self, text: String) -> Vec<String>;

    /// Terminal tool invocations attached to the last completion
    /// event. Emit any tool-related SSE frames before `close`.
    fn tool_calls(&mut self, calls: Vec<crate::ai_gateway::config::ToolCall>) -> Vec<String>;

    /// An informational tool step (the server already ran the tool). Unlike
    /// `tool_calls`, does not terminate the turn as "tool_calls". Default no-op.
    fn tool_step(&mut self, _name: String, _arguments: serde_json::Value) -> Vec<String> {
        Vec::new()
    }

    /// Close the stream. `include_usage` is honoured by adapters that
    /// gate the usage frame behind a client opt-in flag (OpenAI's
    /// `stream_options.include_usage`); adapters that always emit
    /// usage (Anthropic) ignore it.
    fn close(&mut self, stop: StopReason, usage: Option<Usage>, include_usage: bool)
        -> Vec<String>;
}

/// Terminal stop signal handed to `StreamWriter::close`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    /// Model completed normally.
    End,
    /// Model stopped because it wants tools invoked.
    ToolUse,
}
