//! OpenAI ↔ rig conversion for AI-gateway tool-use passthrough.
//!
//! Converts OpenAI wire types coming out of the gateway server into
//! rig's provider-agnostic `CompletionRequest`, and converts rig's
//! `AssistantContent` output back into OpenAI `ToolCall` records the
//! gateway can hand to the client.

use crate::ai_gateway::config::{
    Message as OaiMessage, ToolCall as OaiToolCall, ToolCallFunction as OaiToolCallFunction,
    ToolDefinition as OaiToolDefinition, ROLE_ASSISTANT, ROLE_SYSTEM, ROLE_TOOL,
    TOOL_TYPE_FUNCTION,
};
use rig::completion::message::{
    AssistantContent, Message as RigMessage, Text, ToolCall as RigToolCall,
    ToolFunction as RigToolFunction, ToolResult, ToolResultContent, UserContent,
};
use rig::completion::{CompletionRequest, ToolDefinition as RigToolDefinition};
use rig::message::ToolChoice as RigToolChoice;
use rig::OneOrMany;

/// Errors that occur when translating between OpenAI and rig types.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Chat history is empty; passthrough requires at least one message")]
    EmptyHistory,
    #[error("Unsupported role '{role}' on inbound message")]
    UnsupportedRole { role: String },
    #[error("Assistant message must carry either content or tool_calls")]
    EmptyAssistantMessage,
    #[error("Tool message is missing tool_call_id")]
    MissingToolCallId,
    #[error("Tool message is missing content")]
    MissingToolContent,
    #[error("User message is missing content")]
    MissingUserContent,
    #[error("Failed to build a valid multi-item content list")]
    EmptyContentList,
    #[error("Tool call arguments are not valid JSON: {source}")]
    InvalidToolArguments {
        #[source]
        source: serde_json::Error,
    },
}

/// Builds a rig `CompletionRequest` from the OpenAI-shape inputs
/// captured by the AI gateway server.
///
/// `model` is the resolved downstream model name; `preamble` is the
/// system prompt extracted from the message list, if any.
pub fn build_request(
    model: Option<String>,
    preamble: Option<String>,
    messages: &[OaiMessage],
    tools: &[OaiToolDefinition],
    tool_choice: Option<serde_json::Value>,
    temperature: Option<f32>,
    max_tokens: Option<u32>,
) -> Result<CompletionRequest, Error> {
    let mut history: Vec<RigMessage> = Vec::with_capacity(messages.len());
    for msg in messages {
        // The system prompt was already extracted into `preamble`.
        if msg.role == ROLE_SYSTEM {
            continue;
        }
        history.push(oai_message_to_rig(msg)?);
    }
    let chat_history = OneOrMany::many(history).map_err(|_| Error::EmptyHistory)?;

    let rig_tools = tools.iter().map(oai_tool_to_rig).collect::<Vec<_>>();

    Ok(CompletionRequest {
        model,
        preamble,
        chat_history,
        documents: Vec::new(),
        tools: rig_tools,
        temperature: temperature.map(|t| t as f64),
        max_tokens: max_tokens.map(|t| t as u64),
        tool_choice: tool_choice.and_then(parse_tool_choice),
        additional_params: None,
        output_schema: None,
    })
}

/// Extracts `tool_calls` from rig's assistant-side response into the
/// OpenAI wire format. Text and reasoning items are dropped; the caller
/// pulls text separately when it wants a text-only completion.
pub fn tool_calls_from_choice(choice: &OneOrMany<AssistantContent>) -> Vec<OaiToolCall> {
    choice
        .iter()
        .filter_map(|item| match item {
            AssistantContent::ToolCall(call) => Some(rig_tool_call_to_oai(call)),
            _ => None,
        })
        .collect()
}

/// Concatenates every `Text` block in the assistant response into a
/// single string. Empty when the model only emitted tool calls.
pub fn text_from_choice(choice: &OneOrMany<AssistantContent>) -> String {
    let mut out = String::new();
    for item in choice.iter() {
        if let AssistantContent::Text(text) = item {
            out.push_str(&text.text);
        }
    }
    out
}

fn oai_message_to_rig(msg: &OaiMessage) -> Result<RigMessage, Error> {
    match msg.role.as_str() {
        ROLE_ASSISTANT => {
            let mut content: Vec<AssistantContent> = Vec::new();
            if let Some(text) = msg.content.as_ref() {
                if !text.is_empty() {
                    content.push(AssistantContent::Text(Text { text: text.clone() }));
                }
            }
            for call in &msg.tool_calls {
                content.push(AssistantContent::ToolCall(oai_tool_call_to_rig(call)?));
            }
            let content = OneOrMany::many(content).map_err(|_| Error::EmptyAssistantMessage)?;
            Ok(RigMessage::Assistant { id: None, content })
        }
        ROLE_TOOL => {
            let tool_call_id = msg.tool_call_id.clone().ok_or(Error::MissingToolCallId)?;
            let text = msg.content.clone().ok_or(Error::MissingToolContent)?;
            let result = ToolResult {
                id: tool_call_id.clone(),
                call_id: Some(tool_call_id),
                content: OneOrMany::one(ToolResultContent::Text(Text { text })),
            };
            Ok(RigMessage::User {
                content: OneOrMany::one(UserContent::ToolResult(result)),
            })
        }
        "user" => {
            let text = msg.content.clone().ok_or(Error::MissingUserContent)?;
            Ok(RigMessage::User {
                content: OneOrMany::one(UserContent::Text(Text { text })),
            })
        }
        other => Err(Error::UnsupportedRole {
            role: other.to_string(),
        }),
    }
}

fn oai_tool_to_rig(t: &OaiToolDefinition) -> RigToolDefinition {
    let description = match &t.function.description {
        Some(d) => d.clone(),
        None => String::new(),
    };
    RigToolDefinition {
        name: t.function.name.clone(),
        description,
        parameters: t.function.parameters.clone(),
    }
}

fn oai_tool_call_to_rig(call: &OaiToolCall) -> Result<RigToolCall, Error> {
    let arguments: serde_json::Value = serde_json::from_str(&call.function.arguments)
        .map_err(|source| Error::InvalidToolArguments { source })?;
    Ok(RigToolCall {
        id: call.id.clone(),
        call_id: Some(call.id.clone()),
        function: RigToolFunction {
            name: call.function.name.clone(),
            arguments,
        },
        signature: None,
        additional_params: None,
    })
}

pub fn rig_tool_call_to_oai_public(call: &rig::completion::message::ToolCall) -> OaiToolCall {
    rig_tool_call_to_oai(call)
}

fn rig_tool_call_to_oai(call: &RigToolCall) -> OaiToolCall {
    OaiToolCall {
        id: call.id.clone(),
        kind: TOOL_TYPE_FUNCTION.to_string(),
        function: OaiToolCallFunction {
            name: call.function.name.clone(),
            arguments: call.function.arguments.to_string(),
        },
    }
}

fn parse_tool_choice(v: serde_json::Value) -> Option<RigToolChoice> {
    match v {
        serde_json::Value::String(s) => match s.as_str() {
            "auto" => Some(RigToolChoice::Auto),
            "none" => Some(RigToolChoice::None),
            "required" => Some(RigToolChoice::Required),
            _ => None,
        },
        serde_json::Value::Object(mut map) => {
            let function = map.remove("function")?;
            let name = function.get("name")?.as_str()?.to_string();
            Some(RigToolChoice::Specific {
                function_names: vec![name],
            })
        }
        _ => None,
    }
}
