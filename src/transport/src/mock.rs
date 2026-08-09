//! Stateful mock app-server used by all integration tests.

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::{collections::HashMap, io, path::PathBuf, sync::Arc};
use tokio::{
    net::{UnixListener, UnixStream},
    sync::{Mutex, broadcast, oneshot},
};
use tokio_tungstenite::{
    accept_hdr_async,
    tungstenite::{
        Message,
        handshake::server::{Request, Response},
    },
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Fault {
    None,
    DisconnectOnMethod(String),
    HalfOpen,
    InterruptCompletionBeforeResponse,
}

#[derive(Clone, Debug)]
pub struct MockThread {
    pub id: String,
    pub status: String,
    pub turn_id: Option<String>,
    pub ephemeral: bool,
    pub updated_at: u64,
}

impl MockThread {
    fn json(&self) -> Value {
        let turns = match &self.turn_id {
            Some(id) => serde_json::json!([{"id": id, "status": self.status}]),
            None => serde_json::json!([]),
        };
        serde_json::json!({"id":self.id,"status":{"type":self.status},"turns":turns,
            "ephemeral":self.ephemeral,"updatedAt":self.updated_at})
    }
}

struct State {
    fault: Fault,
    threads: HashMap<String, MockThread>,
    received: Vec<Value>,
    responses_to_server_requests: usize,
    next_server_id: u64,
    resume_failures_remaining: usize,
}

#[derive(Clone)]
pub struct MockAppServer {
    state: Arc<Mutex<State>>,
    outgoing: broadcast::Sender<Value>,
    shutdown: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    path: PathBuf,
}

impl MockAppServer {
    pub async fn start(path: PathBuf) -> io::Result<Self> {
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        let listener = UnixListener::bind(&path)?;
        let state = Arc::new(Mutex::new(State {
            fault: Fault::None,
            threads: HashMap::new(),
            received: Vec::new(),
            responses_to_server_requests: 0,
            next_server_id: 9000,
            resume_failures_remaining: 0,
        }));
        let (outgoing, _) = broadcast::channel(128);
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let server = Self {
            state: state.clone(),
            outgoing: outgoing.clone(),
            shutdown: Arc::new(Mutex::new(Some(shutdown_tx))),
            path,
        };
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        let Ok((stream, _)) = result else { break };
                        let state = state.clone();
                        let receiver = outgoing.subscribe();
                        tokio::spawn(async move { let _ = serve_connection(stream, state, receiver).await; });
                    }
                    _ = &mut shutdown_rx => break,
                }
            }
        });
        Ok(server)
    }

    pub async fn add_thread(&self, thread: MockThread) {
        self.state
            .lock()
            .await
            .threads
            .insert(thread.id.clone(), thread);
    }
    pub async fn set_fault(&self, fault: Fault) {
        self.state.lock().await.fault = fault;
        let _ = self.outgoing.send(Value::Null);
    }
    pub async fn set_resume_failures(&self, count: usize) {
        self.state.lock().await.resume_failures_remaining = count;
    }
    pub async fn emit_notification(&self, method: &str, params: Value) {
        let _ = self
            .outgoing
            .send(serde_json::json!({"jsonrpc":"2.0","method":method,"params":params}));
    }
    pub async fn emit_server_request(&self, method: &str, params: Value) {
        let id = {
            let mut state = self.state.lock().await;
            state.next_server_id += 1;
            state.next_server_id
        };
        let _ = self
            .outgoing
            .send(serde_json::json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}));
    }
    pub async fn received(&self) -> Vec<Value> {
        self.state.lock().await.received.clone()
    }
    pub async fn responses_to_server_requests(&self) -> usize {
        self.state.lock().await.responses_to_server_requests
    }
    pub async fn shutdown(&self) {
        if let Some(done) = self.shutdown.lock().await.take() {
            let _ = done.send(());
        }
        let _ = tokio::fs::remove_file(&self.path).await;
    }
}

// `accept_hdr_async` fixes the callback's error type to tungstenite's HTTP response.
// That upstream type is intentionally returned by value and cannot be boxed here.
#[allow(clippy::result_large_err)]
async fn serve_connection(
    stream: UnixStream,
    state: Arc<Mutex<State>>,
    mut outgoing: broadcast::Receiver<Value>,
) -> Result<(), String> {
    let mut socket = accept_hdr_async(stream, |request: &Request, response: Response| {
        if request.uri().path() != "/rpc" {
            return Err(http_error(404));
        }
        Ok(response)
    })
    .await
    .map_err(|e| e.to_string())?;
    loop {
        if matches!(state.lock().await.fault, Fault::HalfOpen) {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            continue;
        }
        tokio::select! {
            message = socket.next() => {
                let Some(message) = message else { return Ok(()) };
                let message = message.map_err(|e| e.to_string())?;
                if matches!(state.lock().await.fault, Fault::HalfOpen) { continue; }
                if let Message::Ping(payload) = message { socket.send(Message::Pong(payload)).await.map_err(|e| e.to_string())?; continue; }
                let value: Value = match message { Message::Text(text) => serde_json::from_str(&text).map_err(|e| e.to_string())?,
                    Message::Binary(bytes) => serde_json::from_slice(&bytes).map_err(|e| e.to_string())?,
                    Message::Close(_) => return Ok(()), _ => continue };
                let interrupt_before_response = {
                    let mut locked = state.lock().await;
                    if value.get("method").is_none() && value.get("id").is_some() { locked.responses_to_server_requests += 1; }
                    locked.received.push(value.clone());
                    if matches!(&locked.fault, Fault::DisconnectOnMethod(target) if value["method"] == *target) { return Ok(()); }
                    matches!(locked.fault, Fault::InterruptCompletionBeforeResponse)
                        && value["method"] == "turn/interrupt"
                };
                if interrupt_before_response {
                    let params = value.get("params").cloned().unwrap_or(Value::Null);
                    let event = serde_json::json!({"jsonrpc":"2.0","method":"turn/completed","params":{
                        "threadId":params["threadId"],"turn":{"id":params["turnId"],"status":"interrupted"}
                    }});
                    socket.send(Message::Text(event.to_string().into())).await.map_err(|e| e.to_string())?;
                }
                if let Some(response) = handle_request(&state, &value).await {
                    socket.send(Message::Text(response.to_string().into())).await.map_err(|e| e.to_string())?;
                }
            }
            result = outgoing.recv() => if let Ok(value) = result {
                if matches!(state.lock().await.fault, Fault::HalfOpen) || value.is_null() { continue; }
                socket.send(Message::Text(value.to_string().into())).await.map_err(|e| e.to_string())?;
            }
        }
    }
}

fn http_error(status: u16) -> tokio_tungstenite::tungstenite::http::Response<Option<String>> {
    tokio_tungstenite::tungstenite::http::Response::builder()
        .status(status)
        .body(None)
        .unwrap()
}

async fn handle_request(state: &Arc<Mutex<State>>, value: &Value) -> Option<Value> {
    let id = value.get("id")?.clone();
    let method = value.get("method")?.as_str()?;
    let params = value.get("params").cloned().unwrap_or(Value::Null);
    if method == "thread/resume" {
        let mut locked = state.lock().await;
        if locked.resume_failures_remaining > 0 {
            locked.resume_failures_remaining -= 1;
            return Some(serde_json::json!({"jsonrpc":"2.0","id":id,
                "error":{"code":-32000,"message":"no rollout found"}}));
        }
    }
    let result = match method {
        "initialize" => serde_json::json!({"serverInfo":{"name":"mock","version":"0.144.6"}}),
        "thread/list" => {
            let mut threads: Vec<_> = state.lock().await.threads.values().cloned().collect();
            threads.sort_by_key(|t| std::cmp::Reverse(t.updated_at));
            serde_json::json!({"data":threads.into_iter().map(|t|t.json()).collect::<Vec<_>>(),"nextCursor":null})
        }
        "thread/read" | "thread/resume" => {
            let thread_id = params
                .get("threadId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let thread = state
                .lock()
                .await
                .threads
                .get(thread_id)
                .cloned()
                .unwrap_or(MockThread {
                    id: thread_id.into(),
                    status: "idle".into(),
                    turn_id: None,
                    ephemeral: false,
                    updated_at: 0,
                });
            serde_json::json!({"thread":thread.json()})
        }
        "thread/subscribe" => {
            serde_json::json!({"subscriptionId":format!("sub-{}", params["threadId"].as_str().unwrap_or_default())})
        }
        "thread/unsubscribe" => serde_json::json!({"status":"unsubscribed"}),
        "skills/extraRoots/set" => serde_json::json!({}),
        "skills/list" => {
            let cwd = params
                .get("cwds")
                .and_then(Value::as_array)
                .and_then(|cwds| cwds.first())
                .cloned()
                .unwrap_or_else(|| Value::String("/mock/project".into()));
            serde_json::json!({"data":[{"cwd":cwd,"skills":[],"errors":[]}]})
        }
        "turn/interrupt" => serde_json::json!({}),
        "turn/start" => serde_json::json!({"turn":{"id":"mock-turn"}}),
        "turn/steer" => serde_json::json!({"turnId":params["expectedTurnId"]}),
        _ => serde_json::json!({}),
    };
    Some(serde_json::json!({"jsonrpc":"2.0","id":id,"result":result}))
}
