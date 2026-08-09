//! Reconnecting JSON-RPC WebSocket transport over a Unix domain socket.

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use protocol::{IncomingFrame, RpcId, SequencedEvent, notification, request};
use serde_json::Value;
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::{
    net::UnixStream,
    sync::{broadcast, mpsc, oneshot, watch},
    time,
};
use tokio_tungstenite::{
    WebSocketStream, client_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};

pub mod mock;

#[derive(Clone, Debug)]
pub struct TransportConfig {
    pub socket_path: PathBuf,
    pub connect_timeout: Duration,
    pub read_idle_timeout: Duration,
    pub request_timeout: Duration,
    pub retry_initial: Duration,
    pub retry_max: Duration,
    /// Maximum size of one trusted local app-server message or frame.
    pub max_incoming_bytes: usize,
    pub client_name: String,
    pub client_version: String,
}

impl Default for TransportConfig {
    fn default() -> Self {
        let socket_path = std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/var/empty"))
            .join(".codex/app-server-control/app-server-control.sock");
        Self {
            socket_path,
            connect_timeout: Duration::from_secs(3),
            read_idle_timeout: Duration::from_secs(30),
            request_timeout: Duration::from_secs(15),
            retry_initial: Duration::from_millis(100),
            retry_max: Duration::from_secs(5),
            max_incoming_bytes: 32 << 20,
            client_name: "codex-control".into(),
            client_version: env!("CARGO_PKG_VERSION").into(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionPhase {
    Connecting,
    Handshaking,
    Reconciling,
    Connected,
    Degraded,
    Stopped,
}

#[derive(Clone, Debug)]
pub struct Health {
    pub phase: ConnectionPhase,
    pub reconnect_attempts: u64,
    pub last_frame_sequence: u64,
    pub detail: Option<String>,
}

impl Default for Health {
    fn default() -> Self {
        Self {
            phase: ConnectionPhase::Connecting,
            reconnect_attempts: 0,
            last_frame_sequence: 0,
            detail: None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PendingWriteState {
    NotWritten,
    WrittenOutcomeUnknown,
    Answered,
}

#[derive(Debug, Error, Clone)]
pub enum RequestError {
    #[error("request was not written: {0}")]
    NotWritten(String),
    #[error("request {method} ({id:?}) was written but its outcome is unknown: {detail}")]
    WrittenOutcomeUnknown {
        id: RpcId,
        method: String,
        detail: String,
    },
    #[error("request {method} was rejected: {error}")]
    Rejected { method: String, error: Value },
    #[error("request timed out after it may have been written: {method}")]
    TimedOut { method: String },
    #[error("transport is shut down")]
    Closed,
}

enum Command {
    Request {
        id: RpcId,
        method: String,
        params: Value,
        response: oneshot::Sender<Result<Value, RequestError>>,
    },
    Notify {
        method: String,
        params: Value,
    },
    Reconciled {
        done: oneshot::Sender<()>,
    },
    Shutdown {
        done: oneshot::Sender<()>,
    },
}

struct Pending {
    method: String,
    response: oneshot::Sender<Result<Value, RequestError>>,
}

#[async_trait]
pub trait RpcClient: Clone + Send + Sync + 'static {
    async fn request(&self, method: &str, params: Value) -> Result<Value, RequestError>;
    async fn request_action(&self, method: &str, params: Value) -> Result<Value, RequestError>;
    async fn notify(&self, method: &str, params: Value) -> Result<(), RequestError>;
}

#[derive(Clone)]
pub struct TransportHandle {
    commands: mpsc::Sender<Command>,
    inbound: broadcast::Sender<Arc<SequencedEvent>>,
    ingress: Arc<Mutex<Option<mpsc::Receiver<Arc<SequencedEvent>>>>>,
    health: watch::Receiver<Health>,
    next_id: Arc<AtomicU64>,
    reconcile_ready: Arc<AtomicBool>,
    request_timeout: Duration,
}

impl TransportHandle {
    pub fn spawn(config: TransportConfig) -> Self {
        let (commands, receiver) = mpsc::channel(256);
        let (inbound, _) = broadcast::channel(4096);
        let (ingress_tx, ingress_rx) = mpsc::channel(8192);
        let (health_tx, health) = watch::channel(Health::default());
        let reconcile_ready = Arc::new(AtomicBool::new(false));
        tokio::spawn(connection_loop(
            config.clone(),
            receiver,
            inbound.clone(),
            ingress_tx,
            health_tx,
            reconcile_ready.clone(),
        ));
        Self {
            commands,
            inbound,
            ingress: Arc::new(Mutex::new(Some(ingress_rx))),
            health,
            next_id: Arc::new(AtomicU64::new(1)),
            reconcile_ready,
            request_timeout: config.request_timeout,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Arc<SequencedEvent>> {
        self.inbound.subscribe()
    }
    /// Takes the single lossless authoritative ingress receiver. This may only be called once.
    pub fn take_ingress(&self) -> Option<mpsc::Receiver<Arc<SequencedEvent>>> {
        self.ingress.lock().expect("ingress lock poisoned").take()
    }
    pub fn health(&self) -> watch::Receiver<Health> {
        self.health.clone()
    }
    pub fn is_reconciled(&self) -> bool {
        self.reconcile_ready.load(Ordering::Acquire)
    }

    pub async fn mark_reconciled(&self) -> Result<(), RequestError> {
        self.reconcile_ready.store(true, Ordering::Release);
        let (done, wait) = oneshot::channel();
        if self
            .commands
            .send(Command::Reconciled { done })
            .await
            .is_err()
        {
            self.reconcile_ready.store(false, Ordering::Release);
            return Err(RequestError::Closed);
        }
        wait.await.map_err(|_| RequestError::Closed)
    }

    pub async fn shutdown(&self) {
        let (done, wait) = oneshot::channel();
        if self.commands.send(Command::Shutdown { done }).await.is_ok() {
            let _ = wait.await;
        }
    }

    async fn request_inner(
        &self,
        method: &str,
        params: Value,
        action: bool,
    ) -> Result<Value, RequestError> {
        if action && !self.is_reconciled() {
            return Err(RequestError::NotWritten(
                "reconciliation gate is closed".into(),
            ));
        }
        let id = RpcId::Number(self.next_id.fetch_add(1, Ordering::Relaxed));
        let (response, wait) = oneshot::channel();
        self.commands
            .send(Command::Request {
                id,
                method: method.into(),
                params,
                response,
            })
            .await
            .map_err(|_| RequestError::Closed)?;
        match time::timeout(self.request_timeout, wait).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(RequestError::Closed),
            Err(_) => Err(RequestError::TimedOut {
                method: method.into(),
            }),
        }
    }
}

#[async_trait]
impl RpcClient for TransportHandle {
    async fn request(&self, method: &str, params: Value) -> Result<Value, RequestError> {
        self.request_inner(method, params, false).await
    }
    async fn request_action(&self, method: &str, params: Value) -> Result<Value, RequestError> {
        self.request_inner(method, params, true).await
    }
    async fn notify(&self, method: &str, params: Value) -> Result<(), RequestError> {
        self.commands
            .send(Command::Notify {
                method: method.into(),
                params,
            })
            .await
            .map_err(|_| RequestError::Closed)
    }
}

async fn connect(config: &TransportConfig) -> Result<WebSocketStream<UnixStream>, String> {
    let stream = time::timeout(
        config.connect_timeout,
        UnixStream::connect(&config.socket_path),
    )
    .await
    .map_err(|_| "unix socket connect timed out".to_owned())?
    .map_err(|error| format!("unix socket connect: {error}"))?;
    let (socket, response) = time::timeout(
        config.connect_timeout,
        client_async_with_config(
            "ws://localhost/rpc",
            stream,
            Some(
                WebSocketConfig::default()
                    .max_message_size(Some(config.max_incoming_bytes))
                    .max_frame_size(Some(config.max_incoming_bytes)),
            ),
        ),
    )
    .await
    .map_err(|_| "websocket handshake timed out".to_owned())?
    .map_err(|error| format!("websocket handshake: {error}"))?;
    if response.status() != 101 {
        return Err(format!("unexpected websocket status {}", response.status()));
    }
    Ok(socket)
}

#[allow(clippy::too_many_arguments)]
async fn initialize(
    socket: &mut WebSocketStream<UnixStream>,
    config: &TransportConfig,
    sequence: &AtomicU64,
    inbound: &broadcast::Sender<Arc<SequencedEvent>>,
    ingress: &mpsc::Sender<Arc<SequencedEvent>>,
    started: Instant,
    health: &watch::Sender<Health>,
    attempts: u64,
) -> Result<(), String> {
    let params = serde_json::json!({
        "clientInfo": {"name":config.client_name,"title":"Codex Control","version":config.client_version},
        "capabilities": {"serverRequests": false}
    });
    socket
        .send(Message::Text(
            request(RpcId::Number(0), "initialize", params)
                .to_string()
                .into(),
        ))
        .await
        .map_err(|error| error.to_string())?;
    loop {
        let message = time::timeout(config.connect_timeout, socket.next())
            .await
            .map_err(|_| "initialize response timed out".to_owned())?
            .ok_or_else(|| "socket closed during initialize".to_owned())?
            .map_err(|error| error.to_string())?;
        let Some(value) = message_json(message)? else {
            continue;
        };
        let frame = IncomingFrame::parse(value).map_err(str::to_owned)?;
        let seq = sequence.fetch_add(1, Ordering::Relaxed) + 1;
        let event = Arc::new(SequencedEvent::from_frame(
            seq,
            started.elapsed(),
            frame.clone(),
        ));
        ingress
            .send(event.clone())
            .await
            .map_err(|_| "authoritative ingress closed".to_owned())?;
        let _ = inbound.send(event);
        let _ = health.send(Health {
            phase: ConnectionPhase::Handshaking,
            reconnect_attempts: attempts,
            last_frame_sequence: seq,
            detail: None,
        });
        if matches!(
            frame,
            IncomingFrame::Response {
                id: RpcId::Number(0),
                error: None,
                ..
            }
        ) {
            break;
        }
        if matches!(
            frame,
            IncomingFrame::Response {
                id: RpcId::Number(0),
                error: Some(_),
                ..
            }
        ) {
            return Err("initialize rejected".into());
        }
    }
    socket
        .send(Message::Text(
            notification("initialized", serde_json::json!({}))
                .to_string()
                .into(),
        ))
        .await
        .map_err(|error| error.to_string())
}

fn message_json(message: Message) -> Result<Option<Value>, String> {
    match message {
        Message::Text(text) => serde_json::from_str(&text)
            .map(Some)
            .map_err(|e| e.to_string()),
        Message::Binary(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|e| e.to_string()),
        Message::Close(_) => Err("peer closed websocket".into()),
        _ => Ok(None),
    }
}

async fn connection_loop(
    config: TransportConfig,
    mut commands: mpsc::Receiver<Command>,
    inbound: broadcast::Sender<Arc<SequencedEvent>>,
    ingress: mpsc::Sender<Arc<SequencedEvent>>,
    health: watch::Sender<Health>,
    reconcile_ready: Arc<AtomicBool>,
) {
    let sequence = AtomicU64::new(0);
    let started = Instant::now();
    let mut attempts = 0u64;
    let mut backoff = config.retry_initial;
    loop {
        let _ = health.send(Health {
            phase: ConnectionPhase::Connecting,
            reconnect_attempts: attempts,
            last_frame_sequence: sequence.load(Ordering::Relaxed),
            detail: None,
        });
        let mut socket = match connect(&config).await {
            Ok(socket) => socket,
            Err(detail) => {
                attempts += 1;
                let _ = health.send(Health {
                    phase: ConnectionPhase::Degraded,
                    reconnect_attempts: attempts,
                    last_frame_sequence: sequence.load(Ordering::Relaxed),
                    detail: Some(detail.clone()),
                });
                tokio::select! {
                    command = commands.recv() => if reject_while_disconnected(command, &detail) { break; },
                    () = time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(config.retry_max);
                continue;
            }
        };
        let _ = health.send(Health {
            phase: ConnectionPhase::Handshaking,
            reconnect_attempts: attempts,
            last_frame_sequence: sequence.load(Ordering::Relaxed),
            detail: None,
        });
        if let Err(detail) = initialize(
            &mut socket,
            &config,
            &sequence,
            &inbound,
            &ingress,
            started,
            &health,
            attempts,
        )
        .await
        {
            attempts += 1;
            let _ = health.send(Health {
                phase: ConnectionPhase::Degraded,
                reconnect_attempts: attempts,
                last_frame_sequence: sequence.load(Ordering::Relaxed),
                detail: Some(detail.clone()),
            });
            tokio::select! {
                command = commands.recv() => if reject_while_disconnected(command, &detail) { break; },
                () = time::sleep(backoff) => {}
            }
            backoff = (backoff * 2).min(config.retry_max);
            continue;
        }
        reconcile_ready.store(false, Ordering::Release);
        backoff = config.retry_initial;
        let _ = health.send(Health {
            phase: ConnectionPhase::Reconciling,
            reconnect_attempts: attempts,
            last_frame_sequence: sequence.load(Ordering::Relaxed),
            detail: None,
        });
        match drive(
            &mut socket,
            &config,
            &mut commands,
            &inbound,
            &ingress,
            &health,
            &sequence,
            started,
            &reconcile_ready,
            attempts,
        )
        .await
        {
            DriveExit::Shutdown(done) => {
                let _ = socket.close(None).await;
                let _ = health.send(Health {
                    phase: ConnectionPhase::Stopped,
                    reconnect_attempts: attempts,
                    last_frame_sequence: sequence.load(Ordering::Relaxed),
                    detail: None,
                });
                let _ = done.send(());
                break;
            }
            DriveExit::Disconnected(detail) => {
                reconcile_ready.store(false, Ordering::Release);
                attempts += 1;
                let _ = health.send(Health {
                    phase: ConnectionPhase::Degraded,
                    reconnect_attempts: attempts,
                    last_frame_sequence: sequence.load(Ordering::Relaxed),
                    detail: Some(detail),
                });
            }
            DriveExit::CommandsClosed => break,
        }
    }
}

fn reject_while_disconnected(command: Option<Command>, detail: &str) -> bool {
    match command {
        Some(Command::Request { response, .. }) => {
            let _ = response.send(Err(RequestError::NotWritten(detail.into())));
            false
        }
        Some(Command::Shutdown { done }) => {
            let _ = done.send(());
            true
        }
        Some(Command::Notify { .. }) => false,
        Some(Command::Reconciled { done }) => {
            let _ = done.send(());
            false
        }
        None => true,
    }
}

enum DriveExit {
    Shutdown(oneshot::Sender<()>),
    Disconnected(String),
    CommandsClosed,
}

#[allow(clippy::too_many_arguments)]
async fn drive(
    socket: &mut WebSocketStream<UnixStream>,
    config: &TransportConfig,
    commands: &mut mpsc::Receiver<Command>,
    inbound: &broadcast::Sender<Arc<SequencedEvent>>,
    ingress: &mpsc::Sender<Arc<SequencedEvent>>,
    health: &watch::Sender<Health>,
    sequence: &AtomicU64,
    started: Instant,
    reconcile_ready: &AtomicBool,
    attempts: u64,
) -> DriveExit {
    let mut pending: HashMap<RpcId, Pending> = HashMap::new();
    let mut last_read = Instant::now();
    let tick_duration = (config.read_idle_timeout / 2).max(Duration::from_millis(20));
    let mut tick = time::interval(tick_duration);
    let exit = loop {
        tokio::select! {
            command = commands.recv() => match command {
                Some(Command::Request { id, method, params, response }) => {
                    let frame = request(id.clone(), &method, params);
                    if let Err(error) = socket.send(Message::Text(frame.to_string().into())).await {
                        let _ = response.send(Err(RequestError::NotWritten(error.to_string())));
                        break DriveExit::Disconnected(error.to_string());
                    }
                    pending.insert(id, Pending { method, response });
                }
                Some(Command::Notify { method, params }) => {
                    if let Err(error) = socket.send(Message::Text(notification(&method, params).to_string().into())).await {
                        break DriveExit::Disconnected(error.to_string());
                    }
                }
                Some(Command::Reconciled { done }) => {
                    reconcile_ready.store(true, Ordering::Release);
                    let _ = health.send(Health { phase: ConnectionPhase::Connected, reconnect_attempts: attempts,
                        last_frame_sequence: sequence.load(Ordering::Relaxed), detail: None });
                    let _ = done.send(());
                }
                Some(Command::Shutdown { done }) => break DriveExit::Shutdown(done),
                None => break DriveExit::CommandsClosed,
            },
            message = socket.next() => {
                let Some(message) = message else { break DriveExit::Disconnected("websocket stream ended".into()) };
                let message = match message { Ok(message) => message, Err(error) => break DriveExit::Disconnected(error.to_string()) };
                last_read = Instant::now();
                if matches!(message, Message::Ping(_)) {
                    if let Message::Ping(payload) = message { let _ = socket.send(Message::Pong(payload)).await; }
                    continue;
                }
                let value = match message_json(message) { Ok(Some(value)) => value, Ok(None) => continue, Err(error) => break DriveExit::Disconnected(error) };
                let frame = match IncomingFrame::parse(value) { Ok(frame) => frame, Err(error) => {
                    tracing::warn!(error, "ignored malformed JSON-RPC frame"); continue;
                }};
                let seq = sequence.fetch_add(1, Ordering::Relaxed) + 1;
                if let IncomingFrame::Response { id, result, error, .. } = &frame
                    && let Some(pending) = pending.remove(id) {
                        let answer = if let Some(error) = error {
                            Err(RequestError::Rejected { method: pending.method, error: error.clone() })
                        } else { Ok(result.clone().unwrap_or(Value::Null)) };
                        let _ = pending.response.send(answer);
                }
                let event = Arc::new(SequencedEvent::from_frame(seq, started.elapsed(), frame));
                if ingress.send(event.clone()).await.is_err() { break DriveExit::CommandsClosed; }
                let _ = inbound.send(event);
                let current = health.borrow().clone();
                let _ = health.send(Health { last_frame_sequence: seq, ..current });
            }
            _ = tick.tick() => {
                let idle = last_read.elapsed();
                if idle >= config.read_idle_timeout * 2 {
                    break DriveExit::Disconnected(format!("read idle for {idle:?}"));
                }
                if idle >= config.read_idle_timeout && socket.send(Message::Ping(Vec::new().into())).await.is_err() {
                    break DriveExit::Disconnected("ping write failed".into());
                }
            }
        }
    };
    let detail = match &exit {
        DriveExit::Disconnected(detail) => detail.clone(),
        _ => "connection closed".into(),
    };
    for (id, pending) in pending {
        let _ = pending
            .response
            .send(Err(RequestError::WrittenOutcomeUnknown {
                id,
                method: pending.method,
                detail: detail.clone(),
            }));
    }
    exit
}

#[cfg(test)]
mod tests {
    use super::*;
    use mock::{Fault, MockAppServer};

    fn config(path: PathBuf) -> TransportConfig {
        TransportConfig {
            socket_path: path,
            connect_timeout: Duration::from_millis(300),
            read_idle_timeout: Duration::from_millis(100),
            request_timeout: Duration::from_secs(2),
            retry_initial: Duration::from_millis(20),
            retry_max: Duration::from_millis(50),
            ..TransportConfig::default()
        }
    }

    #[tokio::test]
    async fn handshake_path_correlation_and_server_request_silence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rpc.sock");
        let server = MockAppServer::start(path.clone()).await.unwrap();
        let client = TransportHandle::spawn(config(path));
        let mut inbound = client.subscribe();
        let result = client
            .request("thread/list", serde_json::json!({}))
            .await
            .unwrap();
        assert!(result["data"].is_array());
        server
            .emit_server_request(
                "item/commandExecution/requestApproval",
                serde_json::json!({"threadId":"t"}),
            )
            .await;
        let event = time::timeout(Duration::from_secs(1), async {
            loop {
                let e = inbound.recv().await.unwrap();
                if e.frame.is_server_request() {
                    break e;
                }
            }
        })
        .await
        .unwrap();
        assert!(event.frame.is_server_request());
        time::sleep(Duration::from_millis(30)).await;
        assert_eq!(server.responses_to_server_requests().await, 0);
        client.shutdown().await;
        server.shutdown().await;
    }

    #[tokio::test]
    async fn receives_a_legitimate_frame_larger_than_tungstenites_default() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rpc.sock");
        let server = MockAppServer::start(path.clone()).await.unwrap();
        let client = TransportHandle::spawn(TransportConfig {
            read_idle_timeout: Duration::from_secs(30),
            ..config(path)
        });
        let mut inbound = client.subscribe();
        client
            .request("thread/list", serde_json::json!({}))
            .await
            .unwrap();

        let text = "x".repeat((17 << 20) + 1);
        server
            .emit_notification("large/local-frame", serde_json::json!({"text": text}))
            .await;
        let event = time::timeout(Duration::from_secs(30), async {
            loop {
                let event = inbound.recv().await.unwrap();
                if event.method() == Some("large/local-frame") {
                    break event;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(
            event.frame.params().unwrap()["text"]
                .as_str()
                .unwrap()
                .len(),
            (17 << 20) + 1
        );
        client.shutdown().await;
        server.shutdown().await;
    }

    #[tokio::test]
    async fn disconnect_after_read_is_ambiguous_and_reconnects() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rpc.sock");
        let server = MockAppServer::start(path.clone()).await.unwrap();
        let client = TransportHandle::spawn(config(path));
        assert!(
            client
                .request("thread/list", serde_json::json!({}))
                .await
                .is_ok()
        );
        client.mark_reconciled().await.unwrap();
        server
            .set_fault(Fault::DisconnectOnMethod("turn/start".into()))
            .await;
        let error = client
            .request_action("turn/start", serde_json::json!({"threadId":"t","input":[]}))
            .await
            .unwrap_err();
        assert!(matches!(error, RequestError::WrittenOutcomeUnknown { .. }));
        server.set_fault(Fault::None).await;
        time::sleep(Duration::from_millis(120)).await;
        assert!(
            client
                .request("thread/list", serde_json::json!({}))
                .await
                .is_ok()
        );
        client.shutdown().await;
        server.shutdown().await;
    }

    #[tokio::test]
    async fn half_open_is_detected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rpc.sock");
        let server = MockAppServer::start(path.clone()).await.unwrap();
        let client = TransportHandle::spawn(config(path));
        assert!(
            client
                .request("thread/list", serde_json::json!({}))
                .await
                .is_ok()
        );
        server.set_fault(Fault::HalfOpen).await;
        time::sleep(Duration::from_millis(750)).await;
        assert!(client.health().borrow().reconnect_attempts > 0);
        client.shutdown().await;
        server.shutdown().await;
    }
}
