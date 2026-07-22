//! Structured `event.meta` fields for AI gateway + completion tasks.
//!
//! Two audiences read these fields:
//!
//! 1. **Downstream tasks** — Rhai budget guards, `bigquery_write`
//!    billing warehouse, NATS-published usage events — read the
//!    fields directly off `event.meta.<field>` without re-parsing
//!    the completion response body.
//! 2. **Observability tooling** — tracing → Loki / Grafana — sees
//!    the same fields on structured-log lines via `EventLogger::context`.
//!
//! Contexts are split by *who populates them*:
//! - `GatewayContext` — request-side. Populated by `dispatch_chat_completions`
//!   / `dispatch_messages` the moment the client's request lands: protocol,
//!   proxy name, downstream model, stream flag, resolved user id.
//! - `CompletionContext` — response-side. Populated by the `ai_completion`
//!   leaf after the upstream provider returns: provider category, token
//!   counts, wall-clock latency.
//!
//! Both flatten into `event.meta` as top-level keys — no `ai.` namespace
//! prefix — so a Rhai script reads `event.meta.prompt_tokens` directly.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

// --- Field key constants ---
//
// Kept as `pub const` so downstream code (Rhai scripts, tests,
// bigquery_write column mappings) can reach for a single canonical
// name without hardcoding the string.

/// Wire protocol used to reach the gateway (`"openai"` or `"anthropic"`).
pub const PROTOCOL: &str = "protocol";
/// Registered `llm_proxy` task name (the `<name>/<model>` prefix).
pub const PROXY_NAME: &str = "proxy_name";
/// Model the client asked for (the `<model>` portion of the
/// `<name>/<model>` routing key). This is what the client typed —
/// distinct from `MODEL` on the completion side, which holds the
/// actual downstream model the leaf ended up calling (e.g. a Rhai
/// `route_provider` script may rewrite an unknown model to a fallback).
pub const REQUESTED_MODEL: &str = "requested_model";
/// Model the completion leaf actually called upstream. Set on the
/// response side; distinct from `REQUESTED_MODEL` which is the
/// client's original alias.
pub const MODEL: &str = "model";
/// Whether the client asked for a streaming response.
pub const STREAM: &str = "stream";
/// Authenticated user id when endpoint / user auth is enabled.
pub const USER_ID: &str = "user_id";
/// Provider category the completion leaf actually hit.
pub const PROVIDER: &str = "provider";
/// Prompt-side token count reported by the upstream provider.
pub const PROMPT_TOKENS: &str = "prompt_tokens";
/// Completion-side token count reported by the upstream provider.
pub const COMPLETION_TOKENS: &str = "completion_tokens";
/// Sum of prompt + completion tokens.
pub const TOTAL_TOKENS: &str = "total_tokens";
/// Wall-clock milliseconds spent in the completion leaf.
pub const LATENCY_MS: &str = "latency_ms";

// --- Request-side context ---

/// Fields the gateway knows at request entry, before the pipeline runs.
/// Written into `event.meta` before the event is dispatched into the
/// flow so intermediate tasks (Rhai routers, script guards) can
/// inspect who and how the request came in.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayContext {
    /// `"openai"` or `"anthropic"`.
    pub protocol: &'static str,
    /// Registered `llm_proxy` task name.
    pub proxy_name: String,
    /// Model the client asked for — the `<model>` portion of the
    /// `<name>/<model>` routing key. Note this is the *alias* the
    /// client typed and may be rewritten by an intermediate Rhai
    /// script before hitting the completion leaf.
    pub requested_model: String,
    /// Whether the client asked for streaming.
    pub stream: bool,
    /// Authenticated user id, `None` when auth is off.
    pub user_id: Option<String>,
}

impl GatewayContext {
    /// Flatten into `event.meta` — one key per field, no nesting.
    pub fn insert_into(&self, meta: &mut Map<String, Value>) {
        meta.insert(PROTOCOL.into(), Value::String(self.protocol.into()));
        meta.insert(PROXY_NAME.into(), Value::String(self.proxy_name.clone()));
        meta.insert(
            REQUESTED_MODEL.into(),
            Value::String(self.requested_model.clone()),
        );
        meta.insert(STREAM.into(), Value::Bool(self.stream));
        if let Some(id) = &self.user_id {
            meta.insert(USER_ID.into(), Value::String(id.clone()));
        }
    }
}

// --- Response-side context ---

/// Fields the completion leaf knows once the upstream provider has
/// returned. Written onto the response event's `meta` and attached to
/// the structured-log line for the leaf.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionContext {
    /// Provider category the leaf actually hit (`"openai"`, `"anthropic"`,
    /// `"google_vertex"`, `"custom"`, ...).
    pub provider: String,
    /// Model the leaf actually called.
    pub model: String,
    /// Prompt-side tokens, `None` when the provider did not report usage.
    pub prompt_tokens: Option<u64>,
    /// Completion-side tokens, `None` when the provider did not report usage.
    pub completion_tokens: Option<u64>,
    /// Total tokens, `None` when the provider did not report usage.
    pub total_tokens: Option<u64>,
    /// Wall-clock milliseconds spent in the leaf.
    pub latency_ms: u64,
}

impl CompletionContext {
    /// Flatten into `event.meta`. Token fields are skipped when
    /// missing rather than written as `null` so budget guards can
    /// use a plain `event.meta.total_tokens ?? 0` idiom.
    pub fn insert_into(&self, meta: &mut Map<String, Value>) {
        meta.insert(PROVIDER.into(), Value::String(self.provider.clone()));
        meta.insert(MODEL.into(), Value::String(self.model.clone()));
        if let Some(v) = self.prompt_tokens {
            meta.insert(PROMPT_TOKENS.into(), Value::Number(v.into()));
        }
        if let Some(v) = self.completion_tokens {
            meta.insert(COMPLETION_TOKENS.into(), Value::Number(v.into()));
        }
        if let Some(v) = self.total_tokens {
            meta.insert(TOTAL_TOKENS.into(), Value::Number(v.into()));
        }
        meta.insert(LATENCY_MS.into(), Value::Number(self.latency_ms.into()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gateway_context_flattens_all_fields() {
        let ctx = GatewayContext {
            protocol: "anthropic",
            proxy_name: "flowgen_anthropic".into(),
            requested_model: "kimi".into(),
            stream: true,
            user_id: Some("user_42".into()),
        };
        let mut meta = Map::new();
        ctx.insert_into(&mut meta);
        assert_eq!(meta.get(PROTOCOL), Some(&Value::String("anthropic".into())));
        assert_eq!(
            meta.get(PROXY_NAME),
            Some(&Value::String("flowgen_anthropic".into()))
        );
        assert_eq!(
            meta.get(REQUESTED_MODEL),
            Some(&Value::String("kimi".into()))
        );
        assert_eq!(meta.get(STREAM), Some(&Value::Bool(true)));
        assert_eq!(meta.get(USER_ID), Some(&Value::String("user_42".into())));
    }

    #[test]
    fn gateway_context_skips_missing_user_id() {
        let ctx = GatewayContext {
            protocol: "openai",
            proxy_name: "flowgen_openai".into(),
            requested_model: "gpt-4".into(),
            stream: false,
            user_id: None,
        };
        let mut meta = Map::new();
        ctx.insert_into(&mut meta);
        assert!(!meta.contains_key(USER_ID));
    }

    #[test]
    fn completion_context_flattens_and_skips_missing_tokens() {
        let ctx = CompletionContext {
            provider: "custom".into(),
            model: "kimi-k2".into(),
            prompt_tokens: Some(42),
            completion_tokens: Some(17),
            total_tokens: Some(59),
            latency_ms: 4213,
        };
        let mut meta = Map::new();
        ctx.insert_into(&mut meta);
        assert_eq!(meta.get(PROVIDER), Some(&Value::String("custom".into())));
        assert_eq!(meta.get(PROMPT_TOKENS).and_then(|v| v.as_u64()), Some(42));
        assert_eq!(meta.get(LATENCY_MS).and_then(|v| v.as_u64()), Some(4213));
    }

    #[test]
    fn completion_context_skips_missing_token_counts() {
        let ctx = CompletionContext {
            provider: "custom".into(),
            model: "kimi-k2".into(),
            prompt_tokens: None,
            completion_tokens: None,
            total_tokens: None,
            latency_ms: 1000,
        };
        let mut meta = Map::new();
        ctx.insert_into(&mut meta);
        assert!(!meta.contains_key(PROMPT_TOKENS));
        assert!(!meta.contains_key(COMPLETION_TOKENS));
        assert!(!meta.contains_key(TOTAL_TOKENS));
        // Provider / model / latency are always populated.
        assert_eq!(meta.get(PROVIDER), Some(&Value::String("custom".into())));
        assert_eq!(meta.get(LATENCY_MS).and_then(|v| v.as_u64()), Some(1000));
    }
}
