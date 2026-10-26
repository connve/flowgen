//! Links an agent's tool calls to the flow runs they start: the run id is
//! derived from the request, the call id and the call id's occurrence, and
//! sent as `correlation_id` in the MCP call's `_meta`.

use flowgen_core::registry::CORRELATION_ID;
use rig_agent::agent::{AgentHook, DispatchAction, DispatchEvent, HookContext};
use rig_core::message::CallId;
use rig_rmcp::rmcp::model::{JsonObject, MetaObject, RequestMetaObject};
use rig_rmcp::McpToolCall;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};

/// The run ids of one agent request's tool calls, shared by its stream and its tool dispatch.
pub struct ToolRuns {
    request: uuid::Uuid,
    state: Mutex<State>,
}

/// Providers may reuse a call id across turns, so the stream and the dispatch
/// each number the occurrences of a call id; both see calls in commit order.
#[derive(Default)]
struct State {
    streamed: HashMap<String, usize>,
    dispatched: HashMap<String, usize>,
    pending: HashMap<(String, String), VecDeque<String>>,
}

impl ToolRuns {
    /// Run ids for a new request.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            request: uuid::Uuid::now_v7(),
            state: Mutex::default(),
        })
    }

    /// The run id of the next streamed call with this call id.
    pub fn streamed(&self, call: &CallId) -> String {
        let occurrence = next_occurrence(&mut self.state().streamed, call);
        self.run_id(call, occurrence)
    }

    fn dispatch(&self, call: &CallId, tool: &str, arguments: &serde_json::Value) {
        let mut state = self.state();
        let occurrence = next_occurrence(&mut state.dispatched, call);
        let run = self.run_id(call, occurrence);
        state
            .pending
            .entry((tool.to_string(), arguments.to_string()))
            .or_default()
            .push_back(run);
    }

    fn take(&self, tool: &str, arguments: &serde_json::Value) -> Option<String> {
        self.state()
            .pending
            .get_mut(&(tool.to_string(), arguments.to_string()))?
            .pop_front()
    }

    fn run_id(&self, call: &CallId, occurrence: usize) -> String {
        let name = format!("{occurrence}:{}", call.wire());
        uuid::Uuid::new_v5(&self.request, name.as_bytes()).to_string()
    }

    fn state(&self) -> MutexGuard<'_, State> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// The `_meta` of an MCP tool call, carrying the run id its dispatch recorded.
    pub fn call_meta(call: &McpToolCall<'_>) -> Option<RequestMetaObject> {
        let runs = call.context.scope::<ToolRuns>()?;
        let run = runs.take(call.tool, &arguments_value(call.arguments))?;
        let mut meta = JsonObject::new();
        meta.insert(CORRELATION_ID.to_string(), serde_json::Value::String(run));
        Some(RequestMetaObject(MetaObject(meta)))
    }
}

/// The occurrence of `call` among the calls counted in `seen`, from 0.
fn next_occurrence(seen: &mut HashMap<String, usize>, call: &CallId) -> usize {
    let count = seen.entry(call.wire().to_string()).or_default();
    let occurrence = *count;
    *count += 1;
    occurrence
}

fn arguments_value(arguments: Option<&JsonObject>) -> serde_json::Value {
    match arguments {
        Some(arguments) => serde_json::Value::Object(arguments.clone()),
        None => serde_json::Value::Object(JsonObject::new()),
    }
}

/// Records the run id of each tool call rig dispatches, for its MCP request.
pub struct RecordRuns(pub Arc<ToolRuns>);

impl AgentHook for RecordRuns {
    async fn on_dispatch(&self, _ctx: &HookContext, event: DispatchEvent<'_>) -> DispatchAction {
        if let (Some(call), Some(tool)) = (event.call_id, event.tool_name()) {
            let arguments = match event.tool_args().map(serde_json::from_str::<JsonObject>) {
                Some(Ok(arguments)) => arguments_value(Some(&arguments)),
                _ => arguments_value(None),
            };
            self.0.dispatch(call, tool, &arguments);
        }
        DispatchAction::Proceed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_dispatched_call_is_found_by_its_tool_and_arguments() {
        let runs = ToolRuns::new();
        let call = CallId::from_wire("c1");
        let arguments = serde_json::json!({"a": 1});

        runs.dispatch(&call, "t", &arguments);

        assert_eq!(runs.take("t", &arguments), Some(runs.streamed(&call)));
        assert_eq!(runs.take("t", &arguments), None);
    }

    #[test]
    fn the_same_call_in_another_request_gets_another_run() {
        let call = CallId::from_wire("functions.t:0");

        assert_ne!(
            ToolRuns::new().streamed(&call),
            ToolRuns::new().streamed(&call)
        );
    }

    #[test]
    fn a_call_id_reused_in_one_request_gets_a_run_per_call() {
        let runs = ToolRuns::new();
        let call = CallId::from_wire("functions.t:0");

        assert_ne!(runs.streamed(&call), runs.streamed(&call));
    }

    #[test]
    fn the_stream_and_the_dispatch_give_a_reused_call_id_the_same_runs() {
        let runs = ToolRuns::new();
        let call = CallId::from_wire("functions.t:0");
        let first = serde_json::json!({"a": 1});
        let second = serde_json::json!({"a": 2});

        let streamed_first = runs.streamed(&call);
        runs.dispatch(&call, "t", &first);
        let streamed_second = runs.streamed(&call);
        runs.dispatch(&call, "t", &second);

        assert_eq!(runs.take("t", &second), Some(streamed_second));
        assert_eq!(runs.take("t", &first), Some(streamed_first));
    }
}
