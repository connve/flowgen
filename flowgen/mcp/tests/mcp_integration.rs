//! Integration tests for the MCP server + processor stack.
//!
//! Boots a real `McpServer` on an ephemeral TCP port, runs the
//! `mcp_prompt`, `mcp_resource`, and `mcp_tool` processors through their
//! public builders, and drives them with `reqwest` as an external MCP
//! client would. Covers the full protocol surface exposed to clients:
//! initialize, prompts/list + get, resources/list + templates/list + read,
//! tools/list + call with SSE completion, completion/complete, and
//! `list_changed` notifications on the long-lived GET stream.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use flowgen_core::event::Event;
use flowgen_core::resource::Source;
use flowgen_core::task::context::{TaskContext, TaskContextBuilder};
use flowgen_mcp::completion::Completion;
use flowgen_mcp::config::Processor as ToolConfig;
use flowgen_mcp::processor::ProcessorBuilder as ToolProcessorBuilder;
use flowgen_mcp::prompt::config::{Argument, Processor as PromptConfig};
use flowgen_mcp::prompt::processor::ProcessorBuilder as PromptProcessorBuilder;
use flowgen_mcp::resource::config::{Parameter, Processor as ResourceConfig};
use flowgen_mcp::resource::processor::ProcessorBuilder as ResourceProcessorBuilder;
use flowgen_mcp::server::{new_mcp_server, McpServer};
use futures_util::StreamExt;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio::time::timeout;

/// Boots an `McpServer` on an ephemeral port. Returns the server handle,
/// the base URL clients should POST to, and a JoinHandle running the
/// axum listener. Callers keep the handle alive for the duration of the
/// test; dropping the JoinHandle tears down the server.
async fn boot_server() -> (Arc<McpServer>, String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("read local addr");
    drop(listener);

    let server = Arc::new(new_mcp_server(
        "/mcp/v1".to_string(),
        None,
        None,
        "flowgen".to_string(),
    ));

    let server_clone = Arc::clone(&server);
    let handle = tokio::spawn(async move {
        let _ = server_clone.start_server(addr.port()).await;
    });

    // Poll until the listener is accepting.
    let base_url = format!("http://{}/mcp/v1", addr);
    let client = reqwest::Client::new();
    for _ in 0..50 {
        let probe = client
            .post(&base_url)
            .json(&json!({"jsonrpc":"2.0","id":0,"method":"initialize","params":{}}))
            .send()
            .await;
        if probe.is_ok() {
            return (server, base_url, handle);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("MCP server did not come up on {addr}");
}

fn task_context(flow_name: &str) -> Arc<TaskContext> {
    let task_manager = Arc::new(
        flowgen_core::task::manager::TaskManagerBuilder::new()
            .build()
            .expect("build TaskManager"),
    );
    let cache = Arc::new(flowgen_core::cache::memory::MemoryCache::new())
        as Arc<dyn flowgen_core::cache::Cache>;
    Arc::new(
        TaskContextBuilder::new()
            .flow_name(flow_name.to_string())
            .task_manager(task_manager)
            .cache(cache)
            .build()
            .expect("build TaskContext"),
    )
}

async fn post(client: &reqwest::Client, url: &str, body: Value) -> Value {
    post_with_header(client, url, body, None).await
}

async fn post_with_header(
    client: &reqwest::Client,
    url: &str,
    body: Value,
    header: Option<(&str, &str)>,
) -> Value {
    let mut req = client
        .post(url)
        .header("Accept", "application/json, text/event-stream")
        .json(&body);
    if let Some((name, value)) = header {
        req = req.header(name, value);
    }
    let resp = req.send().await.expect("post to MCP server");
    let text = resp.text().await.expect("read response body");
    // tools/call returns SSE (`data: <json>\n\n`) even for the terminal
    // event; strip the SSE prefix so callers can parse JSON directly.
    match text.strip_prefix("data: ") {
        Some(rest) => serde_json::from_str(rest.trim_end()).expect("parse SSE JSON payload"),
        None => serde_json::from_str(&text).expect("parse JSON-RPC body"),
    }
}

#[tokio::test]
async fn initialize_advertises_all_capabilities() {
    let (_server, url, _handle) = boot_server().await;
    let client = reqwest::Client::new();
    let resp = post(
        &client,
        &url,
        json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}),
    )
    .await;

    assert_eq!(resp["result"]["serverInfo"]["name"], "flowgen");
    let caps = &resp["result"]["capabilities"];
    assert!(caps["tools"].is_object(), "tools capability missing");
    assert!(caps["prompts"].is_object(), "prompts capability missing");
    assert!(
        caps["resources"].is_object(),
        "resources capability missing"
    );
    assert!(
        caps["completions"].is_object(),
        "completions capability missing"
    );
}

#[tokio::test]
async fn mcp_prompt_registers_lists_and_renders_via_handlebars() {
    let (server, url, _handle) = boot_server().await;

    let cfg = Arc::new(PromptConfig {
        name: "greet".to_string(),
        description: "Say hi".to_string(),
        arguments: vec![
            Argument {
                name: "who".to_string(),
                description: "who to greet".to_string(),
                required: true,
                default: None,
                completion: None,
            },
            Argument {
                name: "tone".to_string(),
                description: "tone".to_string(),
                required: false,
                default: Some("warm".to_string()),
                completion: Some(Completion::Values {
                    values: vec!["warm".to_string(), "formal".to_string()],
                }),
            },
        ],
        template: Some(Source::Inline(
            "Hello {{arguments.who}}, {{arguments.tone}} greetings.".to_string(),
        )),
        messages: None,
        headers: std::collections::HashMap::new(),
        depends_on: None,
        retry: None,
    });

    let processor = PromptProcessorBuilder::new()
        .config(cfg)
        .task_id(0)
        .task_type("mcp_prompt")
        .task_context(task_context("prompt_flow"))
        .mcp_server(Arc::clone(&server))
        .build()
        .await
        .expect("build prompt processor");
    flowgen_core::task::runner::Runner::run(processor)
        .await
        .expect("register prompt");

    let client = reqwest::Client::new();

    // prompts/list
    let list = post(
        &client,
        &url,
        json!({"jsonrpc":"2.0","id":1,"method":"prompts/list"}),
    )
    .await;
    let prompts = list["result"]["prompts"].as_array().expect("prompts array");
    assert_eq!(prompts.len(), 1);
    assert_eq!(prompts[0]["name"], "greet");
    let args = prompts[0]["arguments"].as_array().expect("arguments array");
    assert_eq!(args.len(), 2);
    assert_eq!(args[0]["name"], "who");
    assert_eq!(args[0]["required"], true);

    // prompts/get renders the Handlebars template.
    let get = post(
        &client,
        &url,
        json!({
            "jsonrpc":"2.0","id":2,
            "method":"prompts/get",
            "params":{"name":"greet","arguments":{"who":"Ada"}}
        }),
    )
    .await;
    let messages = get["result"]["messages"]
        .as_array()
        .expect("messages array");
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0]["role"], "user");
    assert_eq!(
        messages[0]["content"]["text"], "Hello Ada, warm greetings.",
        "missing tone should fall back to the declared default"
    );

    // completion/complete returns the declared inline values.
    let complete = post(
        &client,
        &url,
        json!({
            "jsonrpc":"2.0","id":3,
            "method":"completion/complete",
            "params":{
                "ref":{"type":"ref/prompt","name":"greet"},
                "argument":{"name":"tone","value":"w"}
            }
        }),
    )
    .await;
    let values = complete["result"]["completion"]["values"]
        .as_array()
        .expect("values array");
    assert_eq!(values.len(), 1, "prefix filter kept only `warm`");
    assert_eq!(values[0], "warm");

    // Missing required argument surfaces as an error.
    let missing = post(
        &client,
        &url,
        json!({
            "jsonrpc":"2.0","id":4,
            "method":"prompts/get",
            "params":{"name":"greet","arguments":{}}
        }),
    )
    .await;
    assert!(
        missing["error"]["message"]
            .as_str()
            .unwrap()
            .contains("who"),
        "missing required arg must surface the arg name"
    );
}

#[tokio::test]
async fn mcp_resource_registers_concrete_and_templated_variants() {
    let (server, url, _handle) = boot_server().await;

    let concrete = Arc::new(ResourceConfig {
        name: "glossary".to_string(),
        uri: None,
        uri_template: None,
        description: "Glossary of terms".to_string(),
        mime_type: "text/markdown".to_string(),
        content: Source::Inline("# Terms\n- ABM: Account-Based".to_string()),
        parameters: Vec::new(),
        headers: std::collections::HashMap::new(),
        depends_on: None,
        retry: None,
    });
    let template = Arc::new(ResourceConfig {
        name: "account_summary".to_string(),
        uri: None,
        uri_template: Some("flowgen://account/{id}".to_string()),
        description: "Per-account summary".to_string(),
        mime_type: "text/markdown".to_string(),
        content: Source::Inline("# Account {{id}}".to_string()),
        parameters: vec![Parameter {
            name: "id".to_string(),
            completion: Some(Completion::Values {
                values: vec!["001".to_string(), "002".to_string()],
            }),
        }],
        headers: std::collections::HashMap::new(),
        depends_on: None,
        retry: None,
    });

    for cfg in [concrete, template] {
        let processor = ResourceProcessorBuilder::new()
            .config(cfg)
            .task_id(0)
            .task_type("mcp_resource")
            .task_context(task_context("resource_flow"))
            .mcp_server(Arc::clone(&server))
            .build()
            .await
            .expect("build resource processor");
        flowgen_core::task::runner::Runner::run(processor)
            .await
            .expect("register resource");
    }

    let client = reqwest::Client::new();

    // Concrete resource: auto-generated URI includes flow_name + name.
    let list = post(
        &client,
        &url,
        json!({"jsonrpc":"2.0","id":1,"method":"resources/list"}),
    )
    .await;
    let resources = list["result"]["resources"]
        .as_array()
        .expect("resources array");
    assert_eq!(resources.len(), 1, "only the concrete resource lists here");
    let concrete_uri = resources[0]["uri"].as_str().unwrap().to_string();
    assert!(
        concrete_uri.starts_with("flowgen://resource_flow/glossary"),
        "unexpected auto-generated URI: {concrete_uri}"
    );

    // Reading the concrete resource returns the inline body verbatim.
    let read = post(
        &client,
        &url,
        json!({
            "jsonrpc":"2.0","id":2,
            "method":"resources/read",
            "params":{"uri":concrete_uri}
        }),
    )
    .await;
    let contents = read["result"]["contents"]
        .as_array()
        .expect("contents array");
    assert_eq!(contents.len(), 1);
    assert!(
        contents[0]["text"]
            .as_str()
            .unwrap()
            .contains("Account-Based"),
        "concrete body must round-trip through resources/read"
    );

    // Templated resources come back on the templates listing.
    let templates = post(
        &client,
        &url,
        json!({"jsonrpc":"2.0","id":3,"method":"resources/templates/list"}),
    )
    .await;
    let entries = templates["result"]["resourceTemplates"]
        .as_array()
        .expect("resourceTemplates array");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["uriTemplate"], "flowgen://account/{id}");

    // Templated read binds {id} and renders the Handlebars body.
    let bound = post(
        &client,
        &url,
        json!({
            "jsonrpc":"2.0","id":4,
            "method":"resources/read",
            "params":{"uri":"flowgen://account/001Xyz"}
        }),
    )
    .await;
    assert_eq!(
        bound["result"]["contents"][0]["text"], "# Account 001Xyz",
        "URI-template binding must drive Handlebars rendering"
    );

    // completion/complete for a template parameter.
    let complete = post(
        &client,
        &url,
        json!({
            "jsonrpc":"2.0","id":5,
            "method":"completion/complete",
            "params":{
                "ref":{"type":"ref/resource","uri":"flowgen://account/{id}"},
                "argument":{"name":"id","value":""}
            }
        }),
    )
    .await;
    let values = complete["result"]["completion"]["values"]
        .as_array()
        .expect("values array");
    assert_eq!(values.len(), 2, "empty prefix returns all candidates");
}

#[tokio::test]
async fn mcp_resource_and_prompt_headers_scope_listing_and_access() {
    let (server, url, _handle) = boot_server().await;

    let scoped_header = std::collections::HashMap::from([(
        "X-Flowgen-Client".to_string(),
        "marketing_agent".to_string(),
    )]);

    let resource_cfg = Arc::new(ResourceConfig {
        name: "scoped_doc".to_string(),
        uri: None,
        uri_template: None,
        description: "Scoped resource".to_string(),
        mime_type: "text/plain".to_string(),
        content: Source::Inline("secret".to_string()),
        parameters: Vec::new(),
        headers: scoped_header.clone(),
        depends_on: None,
        retry: None,
    });
    let resource_processor = ResourceProcessorBuilder::new()
        .config(resource_cfg)
        .task_id(0)
        .task_type("mcp_resource")
        .task_context(task_context("scoped_flow"))
        .mcp_server(Arc::clone(&server))
        .build()
        .await
        .expect("build resource processor");
    flowgen_core::task::runner::Runner::run(resource_processor)
        .await
        .expect("register resource");

    let prompt_cfg = Arc::new(PromptConfig {
        name: "scoped_prompt".to_string(),
        description: "Scoped prompt".to_string(),
        arguments: Vec::new(),
        template: Some(Source::Inline("hi".to_string())),
        messages: None,
        headers: scoped_header,
        depends_on: None,
        retry: None,
    });
    let prompt_processor = PromptProcessorBuilder::new()
        .config(prompt_cfg)
        .task_id(0)
        .task_type("mcp_prompt")
        .task_context(task_context("scoped_flow"))
        .mcp_server(Arc::clone(&server))
        .build()
        .await
        .expect("build prompt processor");
    flowgen_core::task::runner::Runner::run(prompt_processor)
        .await
        .expect("register prompt");

    let client = reqwest::Client::new();
    let auth_header = Some(("X-Flowgen-Client", "marketing_agent"));

    // Unauthorized caller: neither listing shows the scoped entries.
    let resources = post(
        &client,
        &url,
        json!({"jsonrpc":"2.0","id":1,"method":"resources/list"}),
    )
    .await;
    assert_eq!(
        resources["result"]["resources"]
            .as_array()
            .expect("resources array")
            .len(),
        0,
        "unauthorized caller must not see the scoped resource"
    );
    let prompts = post(
        &client,
        &url,
        json!({"jsonrpc":"2.0","id":2,"method":"prompts/list"}),
    )
    .await;
    assert_eq!(
        prompts["result"]["prompts"]
            .as_array()
            .expect("prompts array")
            .len(),
        0,
        "unauthorized caller must not see the scoped prompt"
    );

    // Unauthorized caller: read/get is blocked, not just hidden from listing.
    let read = post(
        &client,
        &url,
        json!({
            "jsonrpc":"2.0","id":3,
            "method":"resources/read",
            "params":{"uri":"flowgen://scoped_flow/scoped_doc"}
        }),
    )
    .await;
    assert!(
        read["error"].is_object(),
        "unauthorized read must fail, got {read:?}"
    );
    let get = post(
        &client,
        &url,
        json!({
            "jsonrpc":"2.0","id":4,
            "method":"prompts/get",
            "params":{"name":"scoped_prompt","arguments":{}}
        }),
    )
    .await;
    assert!(
        get["error"].is_object(),
        "unauthorized get must fail, got {get:?}"
    );

    // Authorized caller: sees and can use both.
    let resources = post_with_header(
        &client,
        &url,
        json!({"jsonrpc":"2.0","id":5,"method":"resources/list"}),
        auth_header,
    )
    .await;
    assert_eq!(
        resources["result"]["resources"]
            .as_array()
            .expect("resources array")
            .len(),
        1,
        "authorized caller must see the scoped resource"
    );
    let read = post_with_header(
        &client,
        &url,
        json!({
            "jsonrpc":"2.0","id":6,
            "method":"resources/read",
            "params":{"uri":"flowgen://scoped_flow/scoped_doc"}
        }),
        auth_header,
    )
    .await;
    assert_eq!(read["result"]["contents"][0]["text"], "secret");
}

#[tokio::test]
async fn mcp_tool_call_routes_through_downstream_and_returns_result() {
    let (server, url, _handle) = boot_server().await;

    let cfg = Arc::new(ToolConfig {
        name: "echo".to_string(),
        description: "Echo input.".to_string(),
        input_schema: json!({
            "type":"object",
            "properties":{"msg":{"type":"string"}},
            "required":["msg"]
        }),
        credentials_path: None,
        headers: std::collections::HashMap::new(),
        ack_timeout: Some(Duration::from_secs(5)),
        auth: None,
        depends_on: None,
        retry: None,
    });

    // Downstream mpsc pair the tool processor sends onto.
    let (tool_tx, mut tool_rx) = mpsc::channel::<Event>(4);

    let processor = ToolProcessorBuilder::new()
        .config(cfg)
        .sender(tool_tx)
        .task_id(0)
        .task_type("mcp_tool")
        .task_context(task_context("tool_flow"))
        .mcp_server(Arc::clone(&server))
        .build()
        .await
        .expect("build tool processor");
    flowgen_core::task::runner::Runner::run(processor)
        .await
        .expect("register tool");

    // Fake downstream leaf: receive the tool event and signal completion
    // with a synthetic result so the MCP server has something to stream
    // back to the caller.
    tokio::spawn(async move {
        while let Some(event) = tool_rx.recv().await {
            let input = event
                .data_as_json()
                .expect("json input")
                .get("msg")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if let Some(tx) = event.completion_tx.as_ref() {
                tx.signal_completion(Some(json!({ "echo": input })));
            }
        }
    });

    let client = reqwest::Client::new();

    // tools/list sees the newly registered tool.
    let list = post(
        &client,
        &url,
        json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}),
    )
    .await;
    let tools = list["result"]["tools"].as_array().expect("tools array");
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0]["name"], "echo");

    // tools/call: response is SSE; `post` strips the `data:` prefix.
    let call = post(
        &client,
        &url,
        json!({
            "jsonrpc":"2.0","id":2,
            "method":"tools/call",
            "params":{"name":"echo","arguments":{"msg":"hi"}}
        }),
    )
    .await;
    let content = &call["result"]["content"];
    assert_eq!(call["result"]["isError"], false);
    let text = content[0]["text"].as_str().expect("text content");
    assert!(
        text.contains("\"echo\""),
        "text must contain result payload: {text}"
    );
    assert!(text.contains("\"hi\""), "text must echo the input: {text}");
}

#[tokio::test]
async fn deregister_flow_emits_list_changed_on_open_sse_session() {
    let (server, url, _handle) = boot_server().await;

    // Open the long-lived SSE session first so the notification lands
    // on a live receiver.
    let client = reqwest::Client::new();
    let sse_resp = client
        .get(&url)
        .header("Accept", "text/event-stream")
        .send()
        .await
        .expect("open SSE session");
    assert_eq!(sse_resp.status(), 200);
    let mut sse_stream = sse_resp.bytes_stream();

    // Register a prompt so there's something to deregister.
    let cfg = Arc::new(PromptConfig {
        name: "notify_probe".to_string(),
        description: "probe".to_string(),
        arguments: Vec::new(),
        template: Some(Source::Inline("hi".to_string())),
        messages: None,
        headers: std::collections::HashMap::new(),
        depends_on: None,
        retry: None,
    });
    let processor = PromptProcessorBuilder::new()
        .config(cfg)
        .task_id(0)
        .task_type("mcp_prompt")
        .task_context(task_context("notify_flow"))
        .mcp_server(Arc::clone(&server))
        .build()
        .await
        .expect("build prompt processor");
    flowgen_core::task::runner::Runner::run(processor)
        .await
        .expect("register prompt");

    // Drain the notification that fired on the register above.
    let registered = read_next_notification(&mut sse_stream).await;
    assert_eq!(registered, "notifications/prompts/list_changed");

    // Now deregister everything owned by the flow and expect the same
    // notification on the SSE stream.
    flowgen_mcp::server::deregister_flow_all(server.as_ref(), "notify_flow");
    let deregistered = read_next_notification(&mut sse_stream).await;
    assert_eq!(deregistered, "notifications/prompts/list_changed");

    // Confirm the prompt is actually gone from the list.
    let list = post(
        &client,
        &url,
        json!({"jsonrpc":"2.0","id":1,"method":"prompts/list"}),
    )
    .await;
    assert!(
        list["result"]["prompts"].as_array().unwrap().is_empty(),
        "prompt list must be empty after deregister_flow_all"
    );
}

async fn read_next_notification(
    stream: &mut (impl futures_util::Stream<Item = reqwest::Result<Bytes>> + Unpin),
) -> String {
    let mut buffer = String::new();
    let deadline = Duration::from_secs(5);
    loop {
        let chunk = timeout(deadline, stream.next())
            .await
            .expect("SSE notification timed out")
            .expect("SSE stream ended")
            .expect("SSE chunk error");
        buffer.push_str(std::str::from_utf8(&chunk).expect("utf-8 SSE payload"));

        // SSE events end with `\n\n`. Parse `data: <json>` line by line.
        while let Some(event_end) = buffer.find("\n\n") {
            let event = buffer[..event_end].to_string();
            buffer = buffer[event_end + 2..].to_string();
            for line in event.lines() {
                if let Some(payload) = line.strip_prefix("data: ") {
                    let v: Value = serde_json::from_str(payload).expect("SSE payload is JSON");
                    if let Some(method) = v.get("method").and_then(|m| m.as_str()) {
                        return method.to_string();
                    }
                }
            }
        }
    }
}
