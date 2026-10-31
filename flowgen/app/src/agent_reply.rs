//! Agent replies the server writes into Agents conversations. A reply runs in
//! its own task, so it goes on when the browser leaves; browsers follow it
//! while it runs and read the conversation once it ends. Each write replaces
//! only the reply's message of the stored conversation, compare-and-swap, so
//! a rename, a delete, a stop from another pod or a flow's write in the
//! meantime is kept.

use crate::web::{
    conversation_key, conversation_owner, conversation_ttl, find_conversation, gateway_base_url,
    gateway_headers, now_millis, reply_idle_timeout, reply_save_interval, valid_conversation_id,
    wire_level, write_read_at, WebState,
};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::Json;
use flowgen_client::types as api;
use flowgen_core::cache::CacheError;
use flowgen_core::telemetry::query::{LogFilter, MAX_QUERY_LIMIT};
use flowgen_core::telemetry::StoredLog;
use futures::stream::Stream;
use futures_util::StreamExt;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::warn;

/// Longest conversation title taken from a first message.
const TITLE_MAX: usize = 60;

/// Longest error text from the AI gateway kept in a reply.
const GATEWAY_TEXT_MAX: usize = 300;

/// Why a reply couldn't start, stop, or finish. Messages are shown to users,
/// so none includes its source.
#[derive(thiserror::Error, Debug)]
enum Error {
    #[error("Conversation store unavailable")]
    Store {
        #[source]
        source: CacheError,
    },
    #[error("Corrupt conversation")]
    Corrupt {
        #[source]
        source: serde_json::Error,
    },
    #[error("Failed to store conversation")]
    Serialize {
        #[source]
        source: serde_json::Error,
    },
    #[error("A reply is already in progress")]
    InProgress,
    #[error("The reply was interrupted")]
    Interrupted,
    #[error("Failed to set up the AI gateway client")]
    Client {
        #[source]
        source: reqwest::Error,
    },
    #[error("Failed to reach the AI gateway")]
    Connect {
        #[source]
        source: reqwest::Error,
    },
    #[error("The AI gateway answered {status}{}", detail(.text))]
    Status {
        status: reqwest::StatusCode,
        text: String,
    },
    #[error("The AI gateway stream broke")]
    Stream {
        #[source]
        source: reqwest::Error,
    },
    #[error("The agent failed{}", detail(.text))]
    Agent { text: String },
    #[error("The agent sent nothing for {}", humantime_serde::re::humantime::format_duration(*.timeout))]
    Idle { timeout: Duration },
    #[error("The reply ended before the agent finished")]
    Incomplete,
}

impl Error {
    /// The source the message leaves out, for the logs.
    fn cause(&self) -> String {
        match std::error::Error::source(self) {
            Some(source) => source.to_string(),
            None => String::new(),
        }
    }
}

fn detail(text: &str) -> String {
    match text.is_empty() {
        true => String::new(),
        false => format!(": {text}"),
    }
}

/// The replies this pod is writing, by conversation key.
#[derive(Default)]
pub struct Replies {
    running: Mutex<HashMap<String, Running>>,
}

struct Running {
    message: watch::Receiver<api::ConversationMessage>,
    stop: CancellationToken,
}

impl Replies {
    fn get(
        &self,
        key: &str,
    ) -> Option<(watch::Receiver<api::ConversationMessage>, CancellationToken)> {
        let running = self.running.lock().ok()?;
        let reply = running.get(key)?;
        Some((reply.message.clone(), reply.stop.clone()))
    }

    /// Registers a reply unless one is running for the conversation.
    fn claim(&self, key: &str, running: Running) -> bool {
        match self.running.lock() {
            Ok(mut replies) if !replies.contains_key(key) => {
                replies.insert(key.to_string(), running);
                true
            }
            _ => false,
        }
    }

    fn release(&self, key: &str) {
        if let Ok(mut replies) = self.running.lock() {
            replies.remove(key);
        }
    }
}

/// Marks the pending replies of a conversation not saved for ten save
/// intervals as interrupted, so they don't show as in progress after their
/// pod went away.
pub(crate) fn settle_interrupted(conversation: &mut api::Conversation, save_interval: Duration) {
    let stale_after = match i64::try_from(save_interval.as_millis()) {
        Ok(interval) => interval.saturating_mul(10),
        Err(_) => i64::MAX,
    };
    if now_millis().saturating_sub(conversation.updated_at) <= stale_after {
        return;
    }
    for message in &mut conversation.messages {
        if message.pending == Some(true) {
            message.pending = None;
            message.error = Some(Error::Interrupted.to_string());
        }
    }
}

/// A conversation title from its first message, cut on a word boundary.
fn title_of(message: &str) -> String {
    let text = message.split_whitespace().collect::<Vec<_>>().join(" ");
    if text.chars().count() <= TITLE_MAX {
        return text;
    }
    let clipped: String = text.chars().take(TITLE_MAX).collect();
    let head = match clipped.rfind(' ') {
        Some(space) if space > 0 => &clipped[..space],
        _ => clipped.as_str(),
    };
    format!("{head}…")
}

/// Text from the AI gateway, cut to a length a reply can show.
fn clip(text: &str) -> String {
    let text = text.trim();
    match text.char_indices().nth(GATEWAY_TEXT_MAX) {
        Some((end, _)) => format!("{}…", &text[..end]),
        None => text.to_string(),
    }
}

fn unavailable(what: &'static str) -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, what).into_response()
}

fn not_in_progress() -> Response {
    (StatusCode::NOT_FOUND, "No reply in progress").into_response()
}

/// The response for an error, logged with its source when the server failed.
fn respond(id: &str, error: Error) -> Response {
    let status = match &error {
        Error::InProgress => StatusCode::CONFLICT,
        Error::Corrupt { .. } | Error::Serialize { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    };
    if status.is_server_error() {
        warn!(id = %id, error = %error, cause = %error.cause(), "Failed to change an agent reply");
    }
    (status, error.to_string()).into_response()
}

/// The storage key of conversation `id` as the caller sees it, if it exists.
async fn stored_key(
    state: &WebState,
    owner: Option<&str>,
    id: &str,
) -> Result<Option<String>, Error> {
    match find_conversation(state, owner, id).await {
        Ok(Some((key, _))) => Ok(Some(key)),
        Ok(None) => Ok(None),
        Err(source) => Err(Error::Store { source }),
    }
}

/// `POST /api/agents/conversations/{id}/reply`.
pub(crate) async fn start_reply(
    State(state): State<Arc<WebState>>,
    Path(id): Path<String>,
    user: Option<axum::Extension<flowgen_core::auth::UserContext>>,
    session: Option<axum::Extension<Arc<flowgen_core::auth::UserSession>>>,
    Json(request): Json<api::ReplyRequest>,
) -> Response {
    if !valid_conversation_id(&id) {
        return (StatusCode::BAD_REQUEST, "Invalid conversation id").into_response();
    }
    if request.content.trim().is_empty() {
        return (StatusCode::BAD_REQUEST, "The message is empty").into_response();
    }
    let Some(base) = gateway_base_url(&state) else {
        return unavailable("AI gateway is not configured");
    };
    let owner = conversation_owner(user.as_ref());
    let key = match stored_key(&state, owner.as_deref(), &id).await {
        Ok(Some(key)) => key,
        Ok(None) => conversation_key(&state, owner.as_deref(), &id),
        Err(error) => return respond(&id, error),
    };

    let now = now_millis();
    let reply = api::ConversationMessage {
        role: "assistant".to_string(),
        content: String::new(),
        at: now,
        steps: Vec::new(),
        pending: Some(true),
        error: None,
        stopped: None,
    };
    let (sender, receiver) = watch::channel(reply.clone());
    let stop = CancellationToken::new();
    let running = Running {
        message: receiver,
        stop: stop.clone(),
    };
    if !state.replies.claim(&key, running) {
        return respond(&id, Error::InProgress);
    }
    let save_interval = reply_save_interval(&state);
    let added = edit(&state, &key, |stored| {
        let mut conversation = match stored {
            Some(conversation) => conversation,
            None => api::Conversation {
                id: id.clone(),
                title: title_of(&request.content),
                messages: Vec::new(),
                model: None,
                origin: None,
                unread: None,
                folder: None,
                updated_at: 0,
            },
        };
        settle_interrupted(&mut conversation, save_interval);
        if conversation
            .messages
            .iter()
            .any(|message| message.pending == Some(true))
        {
            return Err(Error::InProgress);
        }
        conversation.model = Some(request.model.clone());
        conversation.messages.push(api::ConversationMessage {
            role: "user".to_string(),
            content: request.content.clone(),
            at: now,
            steps: Vec::new(),
            pending: None,
            error: None,
            stopped: None,
        });
        let history = chat_messages(&conversation.messages);
        conversation.messages.push(reply.clone());
        conversation.updated_at = now;
        Ok(Edit::Write(conversation, history))
    })
    .await;
    let history = match added {
        Ok(history) => history,
        Err(error) => {
            state.replies.release(&key);
            return respond(&id, error);
        }
    };
    if let Err(source) = write_read_at(&state, owner.as_deref(), &id, now).await {
        warn!(id = %id, error = %source, "Failed to mark the conversation read");
    }

    let headers = gateway_headers(
        &state,
        session.as_ref().map(|axum::Extension(s)| s.as_ref()),
    );
    let request = Request {
        url: format!("{base}/chat/completions"),
        headers,
        body: ChatRequest {
            model: request.model,
            stream: true,
            messages: history,
        },
    };
    let store = Store {
        state: Arc::clone(&state),
        key,
        owner,
        id,
        started_at: now,
    };
    tokio::spawn(run(store, sender, stop, request));
    StatusCode::ACCEPTED.into_response()
}

/// `GET /api/agents/conversations/{id}/reply`.
pub(crate) async fn follow_reply(
    State(state): State<Arc<WebState>>,
    Path(id): Path<String>,
    user: Option<axum::Extension<flowgen_core::auth::UserContext>>,
) -> Response {
    if !valid_conversation_id(&id) {
        return (StatusCode::BAD_REQUEST, "Invalid conversation id").into_response();
    }
    let owner = conversation_owner(user.as_ref());
    let key = match stored_key(&state, owner.as_deref(), &id).await {
        Ok(Some(key)) => key,
        Ok(None) => return not_in_progress(),
        Err(error) => return respond(&id, error),
    };
    let Some((receiver, _)) = state.replies.get(&key) else {
        return not_in_progress();
    };
    Sse::new(message_events(receiver))
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("keep-alive"),
        )
        .into_response()
}

/// The reply as SSE events: its state now, then each change until it ends.
fn message_events(
    receiver: watch::Receiver<api::ConversationMessage>,
) -> impl Stream<Item = Result<SseEvent, axum::Error>> {
    futures::stream::unfold(Some(receiver), |receiver| async move {
        let mut receiver = receiver?;
        let message = receiver.borrow_and_update().clone();
        let done = message.pending != Some(true);
        let event = SseEvent::default().json_data(&message);
        let next = match done {
            true => None,
            false => match receiver.changed().await {
                Ok(()) => Some(receiver),
                Err(_) => None,
            },
        };
        Some((event, next))
    })
}

/// `DELETE /api/agents/conversations/{id}/reply`. A reply another pod is
/// writing is marked stopped, and that pod stops at its next save.
pub(crate) async fn stop_reply(
    State(state): State<Arc<WebState>>,
    Path(id): Path<String>,
    user: Option<axum::Extension<flowgen_core::auth::UserContext>>,
) -> Response {
    if !valid_conversation_id(&id) {
        return (StatusCode::BAD_REQUEST, "Invalid conversation id").into_response();
    }
    let owner = conversation_owner(user.as_ref());
    let key = match stored_key(&state, owner.as_deref(), &id).await {
        Ok(Some(key)) => key,
        Ok(None) => return not_in_progress(),
        Err(error) => return respond(&id, error),
    };
    if let Some((_, stop)) = state.replies.get(&key) {
        stop.cancel();
        return StatusCode::NO_CONTENT.into_response();
    }
    let marked = edit(&state, &key, |stored| {
        let Some(mut conversation) = stored else {
            return Ok(Edit::Keep(false));
        };
        match conversation.messages.last_mut() {
            Some(last) if last.pending == Some(true) => {
                last.pending = None;
                last.stopped = Some(true);
                conversation.updated_at = now_millis();
                Ok(Edit::Write(conversation, true))
            }
            _ => Ok(Edit::Keep(false)),
        }
    })
    .await;
    match marked {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => not_in_progress(),
        Err(error) => respond(&id, error),
    }
}

/// What an edit does with the stored conversation.
enum Edit<T> {
    Write(api::Conversation, T),
    Keep(T),
}

/// Applies `change` to the conversation stored at `key` and writes it back,
/// starting over when another writer changed it in the meantime.
async fn edit<T>(
    state: &WebState,
    key: &str,
    mut change: impl FnMut(Option<api::Conversation>) -> Result<Edit<T>, Error>,
) -> Result<T, Error> {
    let cache = &state.conversation_cache;
    loop {
        let (stored, revision) = match cache.get_with_revision(key).await {
            Ok(Some((bytes, revision))) => match serde_json::from_slice(&bytes) {
                Ok(conversation) => (Some(conversation), Some(revision)),
                Err(source) => return Err(Error::Corrupt { source }),
            },
            Ok(None) => (None, None),
            Err(source) => return Err(Error::Store { source }),
        };
        let (conversation, value) = match change(stored)? {
            Edit::Write(conversation, value) => (conversation, value),
            Edit::Keep(value) => return Ok(value),
        };
        let bytes = match serde_json::to_vec(&conversation) {
            Ok(bytes) => bytes::Bytes::from(bytes),
            Err(source) => return Err(Error::Serialize { source }),
        };
        let ttl = conversation_ttl(state);
        let written = match revision {
            Some(revision) => cache.update(key, bytes, revision, ttl).await,
            None => match cache.get_revision(key).await {
                Ok(Some(deleted)) => cache.update(key, bytes, deleted, ttl).await,
                Ok(None) => cache.create(key, bytes, ttl).await,
                Err(source) => Err(source),
            },
        };
        match written {
            Ok(_) => return Ok(value),
            Err(
                CacheError::RevisionMismatch { .. }
                | CacheError::AlreadyExists
                | CacheError::NotFound,
            ) => {}
            Err(source) => return Err(Error::Store { source }),
        }
    }
}

/// The OpenAI chat request sent to the AI gateway.
#[derive(serde::Serialize)]
struct ChatRequest {
    model: String,
    stream: bool,
    messages: Vec<ChatMessage>,
}

#[derive(serde::Serialize)]
struct ChatMessage {
    role: String,
    content: String,
}

/// The history the agent replies to, without replies that failed, are still
/// being written, or said nothing.
fn chat_messages(messages: &[api::ConversationMessage]) -> Vec<ChatMessage> {
    messages
        .iter()
        .filter(|message| {
            message.role != "assistant"
                || (message.error.is_none()
                    && message.pending != Some(true)
                    && !message.content.is_empty())
        })
        .map(|message| ChatMessage {
            role: message.role.clone(),
            content: message.content.clone(),
        })
        .collect()
}

/// The chat request a reply streams from the AI gateway.
struct Request {
    url: String,
    headers: reqwest::header::HeaderMap,
    body: ChatRequest,
}

/// One frame of the gateway's OpenAI chat stream. The gateway reports a
/// failure in an `error` frame, and in the body of a failed response.
#[derive(serde::Deserialize, Default)]
struct ChatChunk {
    #[serde(default)]
    choices: Vec<ChatChoice>,
    #[serde(default)]
    error: Option<GatewayError>,
}

#[derive(serde::Deserialize, Default)]
struct GatewayError {
    #[serde(default)]
    message: String,
}

#[derive(serde::Deserialize, Default)]
struct ChatChoice {
    #[serde(default)]
    delta: ChatDelta,
}

#[derive(serde::Deserialize, Default)]
struct ChatDelta {
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    tool_calls: Vec<ToolCallDelta>,
}

#[derive(serde::Deserialize)]
struct ToolCallDelta {
    #[serde(default)]
    index: usize,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    function: Option<FunctionDelta>,
}

#[derive(serde::Deserialize)]
struct FunctionDelta {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    arguments: Option<String>,
}

/// One line of the gateway's event stream.
enum Frame {
    Chunk(ChatChunk),
    Done,
    Other,
}

fn frame_of(line: &str) -> Frame {
    let Some(payload) = line.trim().strip_prefix("data:") else {
        return Frame::Other;
    };
    match payload.trim() {
        "[DONE]" => Frame::Done,
        payload => match serde_json::from_str::<ChatChunk>(payload) {
            Ok(chunk) => Frame::Chunk(chunk),
            Err(source) => {
                warn!(error = %source, "Skipping an unparseable AI gateway frame");
                Frame::Other
            }
        },
    }
}

/// Takes the complete lines off the bytes received so far. A line is decoded
/// once whole, so a character split across chunks stays intact.
fn take_lines(buffer: &mut Vec<u8>, bytes: &[u8]) -> Vec<String> {
    buffer.extend_from_slice(bytes);
    let mut lines = Vec::new();
    while let Some(end) = buffer.iter().position(|byte| *byte == b'\n') {
        let line: Vec<u8> = buffer.drain(..=end).collect();
        lines.push(String::from_utf8_lossy(&line).into_owned());
    }
    lines
}

/// What a failed gateway response says: its error message, or its body.
fn gateway_text(body: &str) -> String {
    match serde_json::from_str::<ChatChunk>(body) {
        Ok(ChatChunk {
            error: Some(error), ..
        }) => clip(&error.message),
        _ => clip(body),
    }
}

/// Folds one gateway frame into the reply. Returns whether a tool call
/// started, so the reply is saved with it. A tool call past the next step is
/// ignored.
fn apply(message: &mut api::ConversationMessage, chunk: ChatChunk) -> bool {
    let mut tool_started = false;
    for choice in chunk.choices {
        if let Some(text) = choice.delta.content {
            message.content.push_str(&text);
        }
        for call in choice.delta.tool_calls {
            if call.index == message.steps.len() {
                message.steps.push(api::ToolStep {
                    name: String::new(),
                    arguments: String::new(),
                    run: None,
                    duration_ms: None,
                    tasks: Vec::new(),
                });
            }
            let Some(step) = message.steps.get_mut(call.index) else {
                continue;
            };
            if let Some(id) = call.id {
                step.run = Some(id);
            }
            let Some(function) = call.function else {
                continue;
            };
            if let Some(name) = function.name {
                step.name = name;
                tool_started = true;
                if !message.content.is_empty() && !message.content.ends_with('\n') {
                    message.content.push_str("\n\n");
                }
            }
            if let Some(arguments) = function.arguments {
                step.arguments.push_str(&arguments);
            }
        }
    }
    tool_started
}

/// The stored conversation a reply writes into.
struct Store {
    state: Arc<WebState>,
    key: String,
    owner: Option<String>,
    id: String,
    /// When the reply started, which tells its message from the others.
    started_at: i64,
}

/// What became of a save.
enum Saved {
    Written,
    /// The reply's message is no longer pending, e.g. stopped from another
    /// pod, and stays as stored.
    Stopped(api::ConversationMessage),
    Deleted,
}

impl Store {
    /// Writes the reply over its message in the stored conversation. A reply
    /// someone follows live stays read for its writer; one that ends unwatched
    /// turns unread.
    async fn save(&self, reply: &api::ConversationMessage, followed: bool) -> Result<Saved, Error> {
        let saved = self.write(reply).await?;
        if followed && matches!(saved, Saved::Written) {
            let read = write_read_at(&self.state, self.owner.as_deref(), &self.id, now_millis());
            if let Err(source) = read.await {
                warn!(key = %self.key, error = %source, "Failed to mark the conversation read");
            }
        }
        Ok(saved)
    }

    async fn write(&self, reply: &api::ConversationMessage) -> Result<Saved, Error> {
        edit(&self.state, &self.key, |stored| {
            let Some(mut conversation) = stored else {
                return Ok(Edit::Keep(Saved::Deleted));
            };
            let message = conversation
                .messages
                .iter_mut()
                .rev()
                .find(|message| message.role == "assistant" && message.at == self.started_at);
            match message {
                Some(message) if message.pending == Some(true) => {
                    *message = reply.clone();
                    conversation.updated_at = now_millis();
                    Ok(Edit::Write(conversation, Saved::Written))
                }
                Some(message) => Ok(Edit::Keep(Saved::Stopped(message.clone()))),
                None => {
                    let mut ended = reply.clone();
                    ended.pending = None;
                    Ok(Edit::Keep(Saved::Stopped(ended)))
                }
            }
        })
        .await
    }
}

/// How a reply's stream ended.
enum Ending {
    /// The agent finished or failed.
    Finished,
    /// The user stopped the reply on this pod.
    Halted,
    /// The reply stopped elsewhere; its message stays as stored.
    Stopped(api::ConversationMessage),
    /// The conversation was deleted.
    Deleted,
}

/// Streams the reply from the AI gateway into the conversation until it
/// ends, fails or is stopped.
async fn run(
    store: Store,
    sender: watch::Sender<api::ConversationMessage>,
    stop: CancellationToken,
    request: Request,
) {
    let mut reply = sender.borrow().clone();
    let ending = tokio::select! {
        ending = stream_reply(&store, &sender, &mut reply, request) => ending,
        _ = stop.cancelled() => Ok(Ending::Halted),
    };
    let ending = match ending {
        Ok(ending) => ending,
        Err(error) => {
            warn!(key = %store.key, error = %error, cause = %error.cause(), "Agent reply failed");
            reply.error = Some(error.to_string());
            Ending::Finished
        }
    };
    let ended = match ending {
        Ending::Finished => finish(&store, reply, followed(&sender)).await,
        Ending::Halted => {
            reply.stopped = Some(true);
            finish(&store, reply, followed(&sender)).await
        }
        Ending::Stopped(stored) => stored,
        Ending::Deleted => {
            reply.pending = None;
            reply
        }
    };
    sender.send_replace(ended);
    store.state.replies.release(&store.key);
}

/// Whether a browser follows the reply, besides the receiver `Replies` holds.
fn followed(sender: &watch::Sender<api::ConversationMessage>) -> bool {
    sender.receiver_count() > 1
}

/// Saves the ended reply with the flow runs of its tool calls.
async fn finish(
    store: &Store,
    mut reply: api::ConversationMessage,
    followed: bool,
) -> api::ConversationMessage {
    record_runs(&store.state, &mut reply, store.started_at).await;
    reply.pending = None;
    reply.at = now_millis();
    match store.save(&reply, followed).await {
        Ok(Saved::Stopped(stored)) => stored,
        Ok(Saved::Written | Saved::Deleted) => reply,
        Err(error) => {
            warn!(key = %store.key, error = %error, cause = %error.cause(), "Failed to save the agent reply");
            reply
        }
    }
}

/// Saves each tool call's flow run with its step: the logs it comes from are
/// kept in memory only. Reads the logs of the tools' flows since the reply
/// started.
async fn record_runs(state: &Arc<WebState>, reply: &mut api::ConversationMessage, since_ms: i64) {
    let Some(logs) = state.logs_store.as_ref() else {
        return;
    };
    if reply.steps.iter().all(|step| step.run.is_none()) {
        return;
    }
    let Json(tools) = crate::connections::list_agent_tools(State(Arc::clone(state))).await;
    let mut flows: Vec<&str> = Vec::new();
    for step in reply.steps.iter().filter(|step| step.run.is_some()) {
        if let Some(tool) = tools.iter().find(|tool| tool.name == step.name) {
            if !flows.contains(&tool.flow.as_str()) {
                flows.push(&tool.flow);
            }
        }
    }
    let until_ms = now_millis();
    let mut records = Vec::new();
    for flow in flows {
        let filter = LogFilter {
            flow: Some(flow.to_string()),
            levels: vec!["info".to_string(), "warn".to_string(), "error".to_string()],
            since_ms: Some(since_ms.max(0) as u64),
            until_ms: Some(until_ms.max(0) as u64),
            ..LogFilter::default()
        };
        match logs.query(filter, MAX_QUERY_LIMIT).await {
            Ok(found) => records.extend(found),
            Err(source) => {
                warn!(flow = %flow, error = %source, "Failed to read the logs of tool calls")
            }
        }
    }
    for step in &mut reply.steps {
        let Some(run) = step.run.as_deref() else {
            continue;
        };
        if let Some((duration_ms, tasks)) = run_of(&records, run) {
            step.duration_ms = duration_ms;
            step.tasks = tasks;
        }
    }
}

/// The run's duration, when its logs carry timestamps, and its tasks in the
/// order they first ran.
fn run_of(records: &[StoredLog], run: &str) -> Option<(Option<i64>, Vec<api::ToolStepTask>)> {
    let mut tasks: Vec<api::ToolStepTask> = Vec::new();
    let mut span: Option<(i64, i64)> = None;
    for record in records {
        let in_run = record
            .fields
            .iter()
            .any(|(key, value)| key == "correlation_id" && value == run);
        if !in_run {
            continue;
        }
        let Some(task) = span_field(record, "task") else {
            continue;
        };
        let duration_ms = match span_field(record, "duration_ms").map(str::parse::<i64>) {
            Some(Ok(duration_ms)) => duration_ms,
            _ => 0,
        };
        if let Some(Ok(at)) = record
            .timestamp
            .as_deref()
            .map(chrono::DateTime::parse_from_rfc3339)
        {
            let ended = at.timestamp_millis();
            let started = ended.saturating_sub(duration_ms);
            span = match span {
                Some((first, last)) => Some((first.min(started), last.max(ended))),
                None => Some((started, ended)),
            };
        }
        let level = wire_level(&record.level);
        match tasks.iter_mut().find(|known| known.task == task) {
            Some(known) => {
                known.count += 1;
                known.duration_ms = known.duration_ms.saturating_add(duration_ms);
                if severity(&level) > severity(&known.level) {
                    known.level = level;
                }
            }
            None => tasks.push(api::ToolStepTask {
                task: task.to_string(),
                task_type: span_field(record, "task_type").map(str::to_string),
                count: 1,
                duration_ms,
                level,
            }),
        }
    }
    let duration_ms = span.map(|(started, ended)| ended.saturating_sub(started).max(0));
    match tasks.is_empty() {
        true => None,
        false => Some((duration_ms, tasks)),
    }
}

fn severity(level: &api::LogLevel) -> u8 {
    match level {
        api::LogLevel::Error => 2,
        api::LogLevel::Warn => 1,
        _ => 0,
    }
}

fn span_field<'a>(record: &'a StoredLog, key: &str) -> Option<&'a str> {
    record
        .spans
        .iter()
        .rev()
        .flat_map(|span| span.fields.iter())
        .find(|(field, _)| field == key)
        .map(|(_, value)| value.as_str())
}

/// Streams the reply from the AI gateway, saving it at every save interval,
/// which also tells other pods it is alive, and as soon as a tool call
/// starts.
async fn stream_reply(
    store: &Store,
    sender: &watch::Sender<api::ConversationMessage>,
    reply: &mut api::ConversationMessage,
    request: Request,
) -> Result<Ending, Error> {
    let idle_timeout = reply_idle_timeout(&store.state);
    let save_interval = reply_save_interval(&store.state);
    let client = reqwest::Client::builder()
        .connect_timeout(idle_timeout)
        .build()
        .map_err(|source| Error::Client { source })?;
    let idle = tokio::time::sleep(idle_timeout);
    tokio::pin!(idle);
    let sent = client
        .post(&request.url)
        .headers(request.headers)
        .json(&request.body)
        .send();
    let response = tokio::select! {
        response = sent => response.map_err(|source| Error::Connect { source })?,
        _ = &mut idle => return Err(Error::Idle { timeout: idle_timeout }),
    };
    let status = response.status();
    if !status.is_success() {
        let body = tokio::select! {
            body = response.text() => match body {
                Ok(body) => body,
                Err(source) => {
                    warn!(error = %source, "Failed to read the AI gateway's answer");
                    String::new()
                }
            },
            _ = &mut idle => String::new(),
        };
        return Err(Error::Status {
            status,
            text: gateway_text(&body),
        });
    }

    let mut frames = response.bytes_stream();
    let mut buffer = Vec::new();
    let mut saves = tokio::time::interval_at(Instant::now() + save_interval, save_interval);
    saves.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        let save = tokio::select! {
            frame = frames.next() => match frame {
                Some(Ok(bytes)) => {
                    idle.as_mut().reset(Instant::now() + idle_timeout);
                    let mut tool_started = false;
                    let mut done = false;
                    for line in take_lines(&mut buffer, &bytes) {
                        match frame_of(&line) {
                            Frame::Chunk(ChatChunk { error: Some(error), .. }) => {
                                return Err(Error::Agent { text: clip(&error.message) });
                            }
                            Frame::Chunk(chunk) => tool_started |= apply(reply, chunk),
                            Frame::Done => done = true,
                            Frame::Other => {}
                        }
                    }
                    sender.send_replace(reply.clone());
                    if done {
                        return Ok(Ending::Finished);
                    }
                    tool_started
                }
                Some(Err(source)) => return Err(Error::Stream { source }),
                None => return Err(Error::Incomplete),
            },
            _ = saves.tick() => true,
            _ = &mut idle => return Err(Error::Idle { timeout: idle_timeout }),
        };
        if !save {
            continue;
        }
        match store.save(reply, followed(sender)).await {
            Ok(Saved::Written) => saves.reset(),
            Ok(Saved::Stopped(stored)) => return Ok(Ending::Stopped(stored)),
            Ok(Saved::Deleted) => return Ok(Ending::Deleted),
            Err(error) => {
                warn!(key = %store.key, error = %error, cause = %error.cause(), "Failed to save the agent reply")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message() -> api::ConversationMessage {
        api::ConversationMessage {
            role: "assistant".to_string(),
            content: String::new(),
            at: 0,
            steps: Vec::new(),
            pending: Some(true),
            error: None,
            stopped: None,
        }
    }

    fn conversation(messages: Vec<api::ConversationMessage>, updated_at: i64) -> api::Conversation {
        api::Conversation {
            id: "a1".to_string(),
            title: "a".to_string(),
            messages,
            model: None,
            origin: None,
            unread: None,
            folder: None,
            updated_at,
        }
    }

    fn chunk(json: &str) -> ChatChunk {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn frames_fold_into_text_and_tool_calls() {
        let mut reply = message();

        apply(
            &mut reply,
            chunk(r#"{"choices":[{"delta":{"content":"Checking."}}]}"#),
        );
        let started = apply(
            &mut reply,
            chunk(
                r#"{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"r1","function":{"name":"tool","arguments":""}}]}}]}"#,
            ),
        );
        apply(
            &mut reply,
            chunk(
                r#"{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"a\":1}"}}]}}]}"#,
            ),
        );
        apply(
            &mut reply,
            chunk(r#"{"choices":[{"delta":{"content":"Done."}}]}"#),
        );

        assert!(started);
        assert_eq!(reply.content, "Checking.\n\nDone.");
        assert_eq!(reply.steps[0].name, "tool");
        assert_eq!(reply.steps[0].arguments, r#"{"a":1}"#);
        assert_eq!(reply.steps[0].run.as_deref(), Some("r1"));
    }

    #[test]
    fn a_tool_call_past_the_next_step_is_ignored() {
        let mut reply = message();

        let started = apply(
            &mut reply,
            chunk(
                r#"{"choices":[{"delta":{"tool_calls":[{"index":1000000,"function":{"name":"tool"}}]}}]}"#,
            ),
        );

        assert!(!started);
        assert!(reply.steps.is_empty());
    }

    #[test]
    fn lines_are_decoded_once_whole_so_a_split_character_survives() {
        let line = "data: {\"choices\":[{\"delta\":{\"content\":\"é\"}}]}\n".as_bytes();
        let split = line.iter().position(|byte| *byte == 0xC3).unwrap() + 1;
        let mut buffer = Vec::new();

        let first = take_lines(&mut buffer, &line[..split]);
        let second = take_lines(&mut buffer, &line[split..]);

        assert!(first.is_empty());
        assert_eq!(second.len(), 1);
        assert!(second[0].contains('é'));
    }

    #[test]
    fn a_gateway_error_frame_carries_its_message() {
        let frame = frame_of(r#"data: {"error":{"message":"a","type":"server_error"}}"#);

        let Frame::Chunk(ChatChunk {
            error: Some(error), ..
        }) = frame
        else {
            panic!("not an error frame");
        };
        assert_eq!(error.message, "a");
        assert!(matches!(frame_of("data: [DONE]"), Frame::Done));
    }

    #[test]
    fn a_failed_gateway_answer_shows_its_error_message_or_the_start_of_its_body() {
        let long = "a".repeat(GATEWAY_TEXT_MAX + 10);

        assert_eq!(gateway_text(r#"{"error":{"message":"b"}}"#), "b");
        assert_eq!(gateway_text(" c "), "c");
        assert_eq!(gateway_text(&long).chars().count(), GATEWAY_TEXT_MAX + 1);
        assert_eq!(
            Error::Status {
                status: reqwest::StatusCode::BAD_GATEWAY,
                text: String::new()
            }
            .to_string(),
            "The AI gateway answered 502 Bad Gateway"
        );
    }

    #[test]
    fn the_history_leaves_out_failed_pending_and_empty_replies() {
        let reply =
            |content: &str, pending: Option<bool>, error: Option<&str>| api::ConversationMessage {
                content: content.to_string(),
                pending,
                error: error.map(str::to_string),
                ..message()
            };
        let user = api::ConversationMessage {
            role: "user".to_string(),
            ..reply("a", None, None)
        };
        let messages = [
            user,
            reply("b", None, None),
            reply("c", None, Some("d")),
            reply("", None, None),
            reply("e", Some(true), None),
        ];

        let history: Vec<String> = chat_messages(&messages)
            .into_iter()
            .map(|message| message.content)
            .collect();

        assert_eq!(history, vec!["a", "b"]);
    }

    fn log(run: &str, task: &str, level: &str, at: Option<&str>, duration_ms: &str) -> StoredLog {
        StoredLog {
            body: String::new(),
            level: level.to_string(),
            timestamp: at.map(str::to_string),
            target: String::new(),
            spans: vec![flowgen_core::telemetry::StoredSpan {
                name: "task.handle".to_string(),
                fields: vec![
                    ("task".to_string(), task.to_string()),
                    ("duration_ms".to_string(), duration_ms.to_string()),
                ],
            }],
            fields: vec![("correlation_id".to_string(), run.to_string())],
        }
    }

    #[test]
    fn a_run_is_read_from_the_logs_of_its_tasks() {
        let records = [
            log("r1", "a", "info", Some("2026-01-01T00:00:01.000Z"), "100"),
            log("r2", "x", "info", Some("2026-01-01T00:00:01.500Z"), "5"),
            log("r1", "b", "info", Some("2026-01-01T00:00:02.000Z"), "200"),
            log("r1", "b", "error", Some("2026-01-01T00:00:03.000Z"), "300"),
        ];

        let (duration_ms, tasks) = run_of(&records, "r1").unwrap();

        assert_eq!(duration_ms, Some(2100));
        assert_eq!(tasks.len(), 2);
        assert_eq!(tasks[1].task, "b");
        assert_eq!(tasks[1].count, 2);
        assert_eq!(tasks[1].duration_ms, 500);
        assert_eq!(tasks[1].level, api::LogLevel::Error);
        assert!(run_of(&records, "r3").is_none());
    }

    #[test]
    fn a_run_whose_logs_have_no_timestamps_has_no_duration() {
        let records = [log("r1", "a", "info", None, "100")];

        let (duration_ms, tasks) = run_of(&records, "r1").unwrap();

        assert_eq!(duration_ms, None);
        assert_eq!(tasks[0].duration_ms, 100);
    }

    #[test]
    fn a_pending_reply_not_saved_for_a_while_is_interrupted() {
        let mut fresh = conversation(vec![message()], now_millis());
        let mut stale = conversation(vec![message(), message()], now_millis() - 30_001);

        settle_interrupted(&mut fresh, Duration::from_secs(3));
        settle_interrupted(&mut stale, Duration::from_secs(3));

        assert_eq!(fresh.messages[0].pending, Some(true));
        for message in &stale.messages {
            assert_eq!(message.pending, None);
            assert!(message.error.is_some());
        }
    }

    #[test]
    fn titles_are_cut_on_a_word_boundary() {
        assert_eq!(title_of("  a  b   c "), "a b c");
        let long = "word ".repeat(20);
        let title = title_of(&long);
        assert!(title.ends_with('…'));
        assert!(title.chars().count() <= TITLE_MAX + 1);
    }

    async fn store_with(stored: Option<api::Conversation>) -> Store {
        let state = Arc::new(crate::web::tests::test_state());
        let key = conversation_key(&state, None, "a1");
        if let Some(stored) = stored {
            state
                .conversation_cache
                .put(&key, serde_json::to_vec(&stored).unwrap().into(), None)
                .await
                .unwrap();
        }
        Store {
            state,
            key,
            owner: None,
            id: "a1".to_string(),
            started_at: 0,
        }
    }

    async fn stored(store: &Store) -> Option<api::Conversation> {
        let bytes = store
            .state
            .conversation_cache
            .get(&store.key)
            .await
            .unwrap()?;
        Some(serde_json::from_slice(&bytes).unwrap())
    }

    fn written(content: &str) -> api::ConversationMessage {
        api::ConversationMessage {
            content: content.to_string(),
            ..message()
        }
    }

    #[tokio::test]
    async fn a_save_replaces_only_the_reply_and_keeps_a_rename_made_meanwhile() {
        let store = store_with(Some(conversation(vec![message()], 1))).await;
        let mut renamed = stored(&store).await.unwrap();
        renamed.title = "b".to_string();
        store
            .state
            .conversation_cache
            .put(
                &store.key,
                serde_json::to_vec(&renamed).unwrap().into(),
                None,
            )
            .await
            .unwrap();

        let saved = store.save(&written("c"), false).await.unwrap();

        let after = stored(&store).await.unwrap();
        assert!(matches!(saved, Saved::Written));
        assert_eq!(after.title, "b");
        assert_eq!(after.messages[0].content, "c");
        assert!(after.updated_at > 1);
    }

    #[tokio::test]
    async fn a_save_does_not_bring_back_a_deleted_conversation() {
        let store = store_with(None).await;

        let saved = store.save(&written("c"), false).await.unwrap();

        assert!(matches!(saved, Saved::Deleted));
        assert!(stored(&store).await.is_none());
    }

    #[tokio::test]
    async fn a_reply_stopped_from_another_pod_stays_as_stored() {
        let stopped = api::ConversationMessage {
            pending: None,
            ..written("a")
        };
        let store = store_with(Some(conversation(vec![stopped], 1))).await;

        let saved = store.save(&written("b"), false).await.unwrap();

        let Saved::Stopped(kept) = saved else {
            panic!("the save went through");
        };
        assert_eq!(kept.content, "a");
        assert_eq!(stored(&store).await.unwrap().messages[0].content, "a");
    }

    async fn unread_after_save(followed: bool) -> bool {
        let store = store_with(Some(conversation(vec![message()], 1))).await;
        crate::web::write_read_at(&store.state, None, "a1", 1)
            .await
            .unwrap();

        store.save(&written("c"), followed).await.unwrap();

        let bytes = serde_json::to_vec(&stored(&store).await.unwrap()).unwrap();
        let seen = crate::web::conversation_for(&store.state, None, &store.key, &bytes)
            .await
            .unwrap();
        seen.unread == Some(true)
    }

    #[tokio::test]
    async fn a_followed_reply_stays_read_and_an_unwatched_one_turns_unread() {
        assert!(!unread_after_save(true).await);
        assert!(unread_after_save(false).await);
    }
}
