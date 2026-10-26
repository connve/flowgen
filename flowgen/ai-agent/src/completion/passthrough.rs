//! OpenAI ↔ rig conversion for AI-gateway tool-use passthrough.
//!
//! Converts OpenAI wire types coming out of the gateway server into
//! rig's provider-agnostic `CompletionRequest`, and converts rig's
//! `AssistantContent` output back into OpenAI `ToolCall` records the
//! gateway can hand to the client.

use std::collections::HashMap;

use crate::ai_gateway::config::{
    Message as OaiMessage, ToolCall as OaiToolCall, ToolCallFunction as OaiToolCallFunction,
    ToolDefinition as OaiToolDefinition, ROLE_ASSISTANT, ROLE_SYSTEM, ROLE_TOOL,
    TOOL_TYPE_FUNCTION,
};
use rig_core::completion::{CompletionRequest, ToolDefinition as RigToolDefinition};
use rig_core::message::{
    AssistantContent, CallId, Message as RigMessage, ToolCall as RigToolCall,
    ToolChoice as RigToolChoice, ToolFunction as RigToolFunction, ToolName, ToolResultContent,
    UserContent,
};

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
    #[error("A tool definition or tool call has an empty name")]
    EmptyToolName {
        #[source]
        source: rig_core::message::EmptyToolName,
    },
    #[error("Tool message answers tool call '{id}', which no earlier assistant message made")]
    UnknownToolCallId { id: String },
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
    let mut tool_names: HashMap<String, ToolName> = HashMap::new();
    let mut history: Vec<RigMessage> = Vec::with_capacity(messages.len());
    for msg in messages {
        // The system prompt was already extracted into `preamble`.
        if msg.role == ROLE_SYSTEM {
            continue;
        }
        history.push(oai_message_to_rig(msg, &mut tool_names)?);
    }
    let prompt = history.pop().ok_or(Error::EmptyHistory)?;

    let rig_tools = tools
        .iter()
        .map(oai_tool_to_rig)
        .collect::<Result<Vec<_>, _>>()?;

    let mut request = CompletionRequest::new(prompt)
        .messages(history)
        .model::<String>(model)
        .tools(rig_tools)
        .temperature(temperature.map(|t| t as f64))
        .max_tokens(max_tokens.map(|t| t as u64));
    if let Some(preamble) = preamble {
        request = request.preamble(preamble);
    }
    if let Some(choice) = tool_choice.and_then(parse_tool_choice) {
        request = request.tool_choice(choice);
    }
    Ok(request)
}

/// Extracts `tool_calls` from rig's assistant-side response into the
/// OpenAI wire format. Text and reasoning items are dropped; the caller
/// pulls text separately when it wants a text-only completion.
pub fn tool_calls_from_choice(choice: &[AssistantContent]) -> Vec<OaiToolCall> {
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
pub fn text_from_choice(choice: &[AssistantContent]) -> String {
    let mut out = String::new();
    for item in choice {
        if let AssistantContent::Text(text) = item {
            out.push_str(&text.text);
        }
    }
    out
}

/// Converts one OpenAI message, recording the tool names of assistant tool
/// calls so a later tool message can name the tool it answers.
fn oai_message_to_rig(
    msg: &OaiMessage,
    tool_names: &mut HashMap<String, ToolName>,
) -> Result<RigMessage, Error> {
    match msg.role.as_str() {
        ROLE_ASSISTANT => {
            let mut content: Vec<AssistantContent> = Vec::new();
            if let Some(text) = msg.content.as_ref() {
                if !text.is_empty() {
                    content.push(AssistantContent::text(text.clone()));
                }
            }
            for call in &msg.tool_calls {
                let call = oai_tool_call_to_rig(call)?;
                tool_names.insert(call.id.wire().into_owned(), call.function.name.clone());
                content.push(AssistantContent::ToolCall(call));
            }
            match content.is_empty() {
                true => Err(Error::EmptyAssistantMessage),
                false => Ok(RigMessage::from(content)),
            }
        }
        ROLE_TOOL => {
            let tool_call_id = msg.tool_call_id.clone().ok_or(Error::MissingToolCallId)?;
            let text = msg.content.clone().ok_or(Error::MissingToolContent)?;
            let name =
                tool_names
                    .get(&tool_call_id)
                    .cloned()
                    .ok_or_else(|| Error::UnknownToolCallId {
                        id: tool_call_id.clone(),
                    })?;
            Ok(RigMessage::from(UserContent::tool_result(
                CallId::from_wire(tool_call_id),
                name,
                vec![ToolResultContent::text(text)],
            )))
        }
        "user" => {
            let text = msg.content.clone().ok_or(Error::MissingUserContent)?;
            Ok(RigMessage::from(UserContent::text(text)))
        }
        other => Err(Error::UnsupportedRole {
            role: other.to_string(),
        }),
    }
}

fn oai_tool_to_rig(t: &OaiToolDefinition) -> Result<RigToolDefinition, Error> {
    let description = match &t.function.description {
        Some(d) => d.clone(),
        None => String::new(),
    };
    let name =
        ToolName::new(t.function.name.clone()).map_err(|source| Error::EmptyToolName { source })?;
    Ok(RigToolDefinition::new(
        name,
        description,
        t.function.parameters.clone(),
    ))
}

fn oai_tool_call_to_rig(call: &OaiToolCall) -> Result<RigToolCall, Error> {
    let arguments: serde_json::Value = serde_json::from_str(&call.function.arguments)
        .map_err(|source| Error::InvalidToolArguments { source })?;
    let name = ToolName::new(call.function.name.clone())
        .map_err(|source| Error::EmptyToolName { source })?;
    Ok(RigToolCall::from_wire(
        call.id.clone(),
        RigToolFunction::new(name, arguments),
    ))
}

pub fn rig_tool_call_to_oai_public(call: &RigToolCall) -> OaiToolCall {
    rig_tool_call_to_oai(call)
}

fn rig_tool_call_to_oai(call: &RigToolCall) -> OaiToolCall {
    OaiToolCall {
        id: call.id.wire().into_owned(),
        kind: TOOL_TYPE_FUNCTION.to_string(),
        function: OaiToolCallFunction {
            name: call.function.name.as_str().to_string(),
            arguments: call.function.raw_arguments(),
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
            let name = ToolName::new(function.get("name")?.as_str()?).ok()?;
            Some(RigToolChoice::Specific {
                function_names: vec![name],
            })
        }
        _ => None,
    }
}
