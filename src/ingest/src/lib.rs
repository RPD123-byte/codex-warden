//! Shared-daemon thread discovery, reconciliation, and subscription management.

use protocol::SequencedEvent;
use reducer::Reducer;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashMap, sync::Arc, time::Duration};
use store::EventStore;
use streaming::EventHub;
use thiserror::Error;
use tokio::sync::{Mutex, mpsc};
use transport::{RequestError, RpcClient};

#[derive(Clone, Debug)]
pub struct IngestConfig {
    pub reconcile_page_size: usize,
    pub reconcile_page_bound: usize,
    pub reconcile_candidate_limit: usize,
    pub subscription_cap: usize,
    pub resume_retry_limit: usize,
    pub retry_delay: Duration,
    pub release_idle_subscriptions: bool,
    /// Must only be set after the live two-client status spike succeeds on this host/version.
    pub reactivation_verified: bool,
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            reconcile_page_size: 50,
            reconcile_page_bound: 4,
            reconcile_candidate_limit: 200,
            subscription_cap: 512,
            resume_retry_limit: 8,
            retry_delay: Duration::from_millis(250),
            release_idle_subscriptions: false,
            reactivation_verified: false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiscoverySource {
    ThreadStarted,
    ActivationStatus,
    Reconciliation,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubscriptionState {
    Unseen,
    Subscribing {
        source: DiscoverySource,
        attempt: usize,
    },
    Subscribed,
    RetryWait {
        attempt: usize,
    },
    Ephemeral,
    CapacityDegraded,
    Released,
    Failed(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DiscoveryOutcome {
    Subscribed,
    AlreadyManaged,
    Ephemeral,
    CapacityDegraded,
    Failed(String),
}

#[derive(Debug, Error)]
pub enum IngestError {
    #[error("reconciliation request failed: {0}")]
    Request(#[from] RequestError),
    #[error("malformed thread/list response")]
    MalformedList,
}

#[derive(Clone)]
pub struct AuthoritativeIngress {
    reducer: Reducer,
    store: EventStore,
    hub: EventHub,
}

impl AuthoritativeIngress {
    pub fn new(reducer: Reducer, store: EventStore, hub: EventHub) -> Self {
        Self {
            reducer,
            store,
            hub,
        }
    }

    /// Applies every authoritative consumer before publishing to lossy fan-out.
    pub async fn process(&self, event: Arc<SequencedEvent>) {
        self.reducer.apply(event.clone()).await;
        self.store.record(event.clone()).await;
        self.hub.publish(event);
    }

    pub async fn run(&self, mut receiver: mpsc::Receiver<Arc<SequencedEvent>>) {
        while let Some(event) = receiver.recv().await {
            self.process(event).await;
        }
    }
}

#[derive(Clone)]
pub struct ThreadIngestor<C: RpcClient> {
    client: C,
    reducer: Reducer,
    config: IngestConfig,
    states: Arc<Mutex<HashMap<String, SubscriptionState>>>,
    watermark: Arc<Mutex<u64>>,
    reactivation_verified: Arc<Mutex<bool>>,
    store: Option<EventStore>,
}

impl<C: RpcClient> ThreadIngestor<C> {
    pub fn new(client: C, reducer: Reducer, config: IngestConfig) -> Self {
        let reactivation_verified = config.reactivation_verified;
        Self {
            client,
            reducer,
            config,
            states: Arc::new(Mutex::new(HashMap::new())),
            watermark: Arc::new(Mutex::new(0)),
            reactivation_verified: Arc::new(Mutex::new(reactivation_verified)),
            store: None,
        }
    }

    /// Attaches the event store so subscribe-boundary in-flight turns get a reconstructed anchor.
    pub fn with_store(mut self, store: EventStore) -> Self {
        self.store = Some(store);
        self
    }

    pub async fn state(&self, thread_id: &str) -> SubscriptionState {
        self.states
            .lock()
            .await
            .get(thread_id)
            .cloned()
            .unwrap_or(SubscriptionState::Unseen)
    }

    pub async fn states(&self) -> HashMap<String, SubscriptionState> {
        self.states.lock().await.clone()
    }

    pub async fn set_reactivation_verified(&self, verified: bool) {
        *self.reactivation_verified.lock().await = verified;
    }

    pub async fn discover(&self, thread_id: &str, source: DiscoverySource) -> DiscoveryOutcome {
        {
            let mut states = self.states.lock().await;
            if matches!(
                states.get(thread_id),
                Some(
                    SubscriptionState::Subscribed
                        | SubscriptionState::Subscribing { .. }
                        | SubscriptionState::RetryWait { .. }
                )
            ) {
                return DiscoveryOutcome::AlreadyManaged;
            }
            let retained = states
                .values()
                .filter(|state| {
                    matches!(
                        state,
                        SubscriptionState::Subscribed
                            | SubscriptionState::Subscribing { .. }
                            | SubscriptionState::RetryWait { .. }
                    )
                })
                .count();
            if retained >= self.config.subscription_cap {
                states.insert(thread_id.into(), SubscriptionState::CapacityDegraded);
                return DiscoveryOutcome::CapacityDegraded;
            }
            states.insert(
                thread_id.into(),
                SubscriptionState::Subscribing { source, attempt: 1 },
            );
        }
        self.reducer.begin_subscription(thread_id).await;

        let params = serde_json::json!({"threadId":thread_id});
        let mut attempt = 1usize;
        loop {
            match self.client.request("thread/resume", params.clone()).await {
                Ok(_) => break,
                Err(RequestError::NotWritten(_)) if attempt < self.config.resume_retry_limit => {
                    attempt += 1;
                    self.states
                        .lock()
                        .await
                        .insert(thread_id.into(), SubscriptionState::RetryWait { attempt });
                    tokio::time::sleep(self.config.retry_delay).await;
                    self.states.lock().await.insert(
                        thread_id.into(),
                        SubscriptionState::Subscribing { source, attempt },
                    );
                }
                Err(RequestError::Rejected { error, .. })
                    if is_ephemeral_error(&error) && attempt < self.config.resume_retry_limit =>
                {
                    attempt += 1;
                    self.states
                        .lock()
                        .await
                        .insert(thread_id.into(), SubscriptionState::RetryWait { attempt });
                    tokio::time::sleep(self.config.retry_delay).await;
                    self.states.lock().await.insert(
                        thread_id.into(),
                        SubscriptionState::Subscribing { source, attempt },
                    );
                }
                Err(RequestError::Rejected { error, .. }) if is_ephemeral_error(&error) => {
                    self.states
                        .lock()
                        .await
                        .insert(thread_id.into(), SubscriptionState::Ephemeral);
                    self.reducer.cancel_subscription(thread_id).await;
                    return DiscoveryOutcome::Ephemeral;
                }
                Err(error) => {
                    let detail = error.to_string();
                    self.states
                        .lock()
                        .await
                        .insert(thread_id.into(), SubscriptionState::Failed(detail.clone()));
                    self.reducer.cancel_subscription(thread_id).await;
                    return DiscoveryOutcome::Failed(detail);
                }
            }
        }
        self.reducer.finish_subscription(thread_id).await;
        self.states
            .lock()
            .await
            .insert(thread_id.into(), SubscriptionState::Subscribed);
        tracing::info!(thread_id, ?source, "subscribed via discover");

        // The resume subscribes this connection; read immediately afterward to close the race with an in-flight turn.
        match self
            .client
            .request(
                "thread/read",
                serde_json::json!({"threadId":thread_id,"includeTurns":true}),
            )
            .await
        {
            Ok(response) => {
                if let Some(thread) = response.get("thread") {
                    self.reducer.reconcile_thread(thread.clone()).await;
                    // If a turn was already in flight at subscribe time, its wire turn/started was
                    // missed; record an honest reconstructed anchor so the store can correlate it.
                    let inflight = in_flight_turn_id(thread);
                    tracing::info!(thread_id, ?inflight, "post-resume read; in-flight turn");
                    if let (Some(store), Some(turn_id)) = (&self.store, inflight) {
                        let sequence = self.reducer.current_sequence().await;
                        store
                            .record_reconstructed_active_turn(sequence, thread_id, &turn_id)
                            .await;
                    }
                }
            }
            Err(error) => {
                tracing::warn!(thread_id, %error, "post-resume thread/read failed; live subscription remains retained")
            }
        }
        DiscoveryOutcome::Subscribed
    }

    pub async fn handle_signal(&self, event: &SequencedEvent) -> Option<DiscoveryOutcome> {
        let thread_id = event.thread_id.as_deref()?;
        let method = event.method()?;
        tracing::info!(thread_id, method, status = ?status_type(event.frame.params()), "handle_signal");
        match method {
            "thread/started" => Some(
                self.discover(thread_id, DiscoverySource::ThreadStarted)
                    .await,
            ),
            "thread/status/changed" if status_type(event.frame.params()) == Some("active") => Some(
                self.discover(thread_id, DiscoverySource::ActivationStatus)
                    .await,
            ),
            _ => None,
        }
    }

    pub async fn reconcile(&self) -> Result<Vec<(String, DiscoveryOutcome)>, IngestError> {
        let previous_watermark = *self.watermark.lock().await;
        let mut newest_watermark = previous_watermark;
        let mut cursor: Option<String> = None;
        let mut candidates = Vec::new();
        for _ in 0..self.config.reconcile_page_bound {
            if candidates.len() >= self.config.reconcile_candidate_limit {
                break;
            }
            let result = self
                .client
                .request(
                    "thread/list",
                    serde_json::json!({
                            "cursor":cursor,"limit":self.config.reconcile_page_size,
                    "sortKey":"updated_at","sortDirection":"desc"
                        }),
                )
                .await?;
            let data = result
                .get("data")
                .and_then(Value::as_array)
                .ok_or(IngestError::MalformedList)?;
            let mut reached_watermark = false;
            for thread in data {
                let Some(id) = thread.get("id").and_then(Value::as_str) else {
                    continue;
                };
                let updated = numeric_timestamp(thread.get("updatedAt")).unwrap_or_default();
                newest_watermark = newest_watermark.max(updated);
                if previous_watermark > 0 && updated <= previous_watermark {
                    reached_watermark = true;
                    break;
                }
                candidates.push(id.to_owned());
                if candidates.len() >= self.config.reconcile_candidate_limit {
                    break;
                }
            }
            cursor = result
                .get("nextCursor")
                .and_then(Value::as_str)
                .map(str::to_owned);
            if reached_watermark || cursor.is_none() {
                break;
            }
        }
        *self.watermark.lock().await = newest_watermark;

        let mut outcomes = Vec::new();
        for thread_id in candidates {
            let read = self
                .client
                .request(
                    "thread/read",
                    serde_json::json!({"threadId":thread_id,"includeTurns":true}),
                )
                .await;
            let active = read
                .as_ref()
                .ok()
                .and_then(|value| value.get("thread"))
                .is_some_and(thread_is_active);
            // Only retain reducer state for active threads; idle history stays out of the snapshot.
            if active {
                if let Ok(response) = &read
                    && let Some(thread) = response.get("thread")
                {
                    self.reducer.reconcile_thread(thread.clone()).await;
                }
                let outcome = self
                    .discover(&thread_id, DiscoverySource::Reconciliation)
                    .await;
                outcomes.push((thread_id, outcome));
            }
        }
        Ok(outcomes)
    }

    pub async fn release_if_idle(&self, thread_id: &str) -> bool {
        if !self.config.release_idle_subscriptions || !*self.reactivation_verified.lock().await {
            return false;
        }
        let snapshot = self.reducer.snapshot().await;
        if snapshot
            .threads
            .get(thread_id)
            .is_some_and(|thread| thread.status != "idle")
        {
            return false;
        }
        if self
            .client
            .request(
                "thread/unsubscribe",
                serde_json::json!({"threadId":thread_id}),
            )
            .await
            .is_ok()
        {
            self.states
                .lock()
                .await
                .insert(thread_id.into(), SubscriptionState::Released);
            self.reducer.set_subscribed(thread_id, false).await;
            return true;
        }
        false
    }

    pub async fn unsubscribe_all_best_effort(&self) {
        let ids: Vec<_> = self
            .states
            .lock()
            .await
            .iter()
            .filter(|(_, state)| matches!(state, SubscriptionState::Subscribed))
            .map(|(id, _)| id.clone())
            .collect();
        for thread_id in ids {
            let _ = self
                .client
                .request(
                    "thread/unsubscribe",
                    serde_json::json!({"threadId":thread_id}),
                )
                .await;
        }
    }
}

fn status_type(params: Option<&Value>) -> Option<&str> {
    params?.get("status").and_then(|status| {
        status
            .get("type")
            .and_then(Value::as_str)
            .or_else(|| status.as_str())
    })
}

fn in_flight_turn_id(thread: &Value) -> Option<String> {
    thread
        .get("turns")
        .and_then(Value::as_array)
        .and_then(|turns| {
            turns
                .iter()
                .rev()
                .find(|turn| {
                    !matches!(
                        turn.get("status").and_then(Value::as_str),
                        Some("completed" | "failed" | "interrupted")
                    )
                })
                .and_then(|turn| turn.get("id"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
}

fn thread_is_active(thread: &Value) -> bool {
    thread
        .get("status")
        .and_then(|status| status.get("type"))
        .and_then(Value::as_str)
        == Some("active")
        || thread
            .get("turns")
            .and_then(Value::as_array)
            .is_some_and(|turns| {
                turns.iter().any(|turn| {
                    !matches!(
                        turn.get("status").and_then(Value::as_str),
                        Some("completed" | "failed" | "interrupted")
                    )
                })
            })
}

fn numeric_timestamp(value: Option<&Value>) -> Option<u64> {
    value.and_then(|v| {
        v.as_u64()
            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
    })
}

fn is_ephemeral_error(error: &Value) -> bool {
    let text = error.to_string().to_ascii_lowercase();
    text.contains("ephemeral") || text.contains("rollout") || text.contains("not found")
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::IncomingFrame;
    use std::path::PathBuf;
    use transport::{
        TransportConfig, TransportHandle,
        mock::{MockAppServer, MockThread},
    };

    async fn setup(
        cap: usize,
        release: bool,
    ) -> (
        tempfile::TempDir,
        MockAppServer,
        TransportHandle,
        ThreadIngestor<TransportHandle>,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rpc.sock");
        let server = MockAppServer::start(path.clone()).await.unwrap();
        let client = TransportHandle::spawn(TransportConfig {
            socket_path: path,
            connect_timeout: Duration::from_millis(300),
            request_timeout: Duration::from_secs(2),
            retry_initial: Duration::from_millis(20),
            retry_max: Duration::from_millis(50),
            ..TransportConfig::default()
        });
        let reducer = Reducer::default();
        let ingestor = ThreadIngestor::new(
            client.clone(),
            reducer,
            IngestConfig {
                subscription_cap: cap,
                release_idle_subscriptions: release,
                retry_delay: Duration::from_millis(5),
                ..IngestConfig::default()
            },
        );
        (dir, server, client, ingestor)
    }

    #[tokio::test]
    async fn reconciliation_duplicate_discovery_and_capacity() {
        let (_dir, server, client, ingestor) = setup(1, false).await;
        server
            .add_thread(MockThread {
                id: "a".into(),
                cwd: PathBuf::from("/mock/a"),
                status: "active".into(),
                turn_id: Some("x".into()),
                ephemeral: false,
                updated_at: 2,
            })
            .await;
        server
            .add_thread(MockThread {
                id: "b".into(),
                cwd: PathBuf::from("/mock/b"),
                status: "active".into(),
                turn_id: Some("y".into()),
                ephemeral: false,
                updated_at: 1,
            })
            .await;
        let outcomes = ingestor.reconcile().await.unwrap();
        assert_eq!(outcomes.len(), 2);
        assert_eq!(ingestor.state("a").await, SubscriptionState::Subscribed);
        assert_eq!(
            ingestor.state("b").await,
            SubscriptionState::CapacityDegraded
        );
        assert_eq!(
            ingestor.discover("a", DiscoverySource::ThreadStarted).await,
            DiscoveryOutcome::AlreadyManaged
        );
        let resumes = server
            .received()
            .await
            .into_iter()
            .filter(|v| v["method"] == "thread/resume")
            .count();
        assert_eq!(resumes, 1);
        client.shutdown().await;
        server.shutdown().await;
    }

    #[tokio::test]
    async fn idle_release_requires_verified_global_reactivation() {
        let (_dir, server, client, ingestor) = setup(2, true).await;
        assert_eq!(
            ingestor.discover("a", DiscoverySource::ThreadStarted).await,
            DiscoveryOutcome::Subscribed
        );
        assert!(!ingestor.release_if_idle("a").await);
        ingestor.set_reactivation_verified(true).await;
        assert!(ingestor.release_if_idle("a").await);
        client.shutdown().await;
        server.shutdown().await;
    }

    #[tokio::test]
    async fn in_flight_turn_at_subscribe_yields_reconstructed_anchor() {
        let (_dir, server, client, ingestor) = setup(4, false).await;
        let store = store::EventStore::new(store::StoreConfig::default());
        let ingestor = ingestor.with_store(store.clone());
        server
            .add_thread(MockThread {
                id: "a".into(),
                cwd: PathBuf::from("/mock/a"),
                status: "active".into(),
                turn_id: Some("t1".into()),
                ephemeral: false,
                updated_at: 1,
            })
            .await;
        assert_eq!(
            ingestor.discover("a", DiscoverySource::ThreadStarted).await,
            DiscoveryOutcome::Subscribed
        );
        let anchors: Vec<_> = store
            .query_time(Some("a"), 0, u64::MAX)
            .await
            .events
            .into_iter()
            .filter(|e| e.reconstructed)
            .collect();
        assert_eq!(
            anchors.len(),
            1,
            "expected exactly one reconstructed anchor"
        );
        assert_eq!(anchors[0].turn_id.as_deref(), Some("t1"));
        assert_eq!(anchors[0].method(), Some("turn/started"));
        assert!(anchors[0].unix_receipt_ms > 0);
        client.shutdown().await;
        server.shutdown().await;
    }

    #[tokio::test]
    async fn genuine_turn_started_prevents_duplicate_reconstructed_anchor() {
        let (_dir, server, client, ingestor) = setup(4, false).await;
        let store = store::EventStore::new(store::StoreConfig::default());
        let genuine = Arc::new(SequencedEvent::from_frame(
            7,
            Duration::from_millis(7),
            IncomingFrame::parse(serde_json::json!({
                "jsonrpc": "2.0",
                "method": "turn/started",
                "params": {"threadId": "a", "turn": {"id": "t1"}}
            }))
            .unwrap(),
        ));
        store.record(genuine).await;
        let ingestor = ingestor.with_store(store.clone());
        server
            .add_thread(MockThread {
                id: "a".into(),
                cwd: PathBuf::from("/mock/a"),
                status: "active".into(),
                turn_id: Some("t1".into()),
                ephemeral: false,
                updated_at: 1,
            })
            .await;

        assert_eq!(
            ingestor.discover("a", DiscoverySource::ThreadStarted).await,
            DiscoveryOutcome::Subscribed
        );
        let starts: Vec<_> = store
            .query_time(Some("a"), 0, u64::MAX)
            .await
            .events
            .into_iter()
            .filter(|event| event.method() == Some("turn/started"))
            .collect();
        assert_eq!(starts.len(), 1);
        assert!(!starts[0].reconstructed, "the genuine wire event must win");
        client.shutdown().await;
        server.shutdown().await;
    }

    #[tokio::test]
    async fn retry_wait_deduplicates_concurrent_discovery() {
        let (_dir, server, client, ingestor) = setup(4, false).await;
        server.set_resume_failures(1).await;
        let first = {
            let ingestor = ingestor.clone();
            tokio::spawn(async move {
                ingestor
                    .discover("retrying", DiscoverySource::ThreadStarted)
                    .await
            })
        };
        for _ in 0..100 {
            if matches!(
                ingestor.state("retrying").await,
                SubscriptionState::RetryWait { .. }
            ) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(
            ingestor
                .discover("retrying", DiscoverySource::ActivationStatus)
                .await,
            DiscoveryOutcome::AlreadyManaged
        );
        assert_eq!(first.await.unwrap(), DiscoveryOutcome::Subscribed);
        let resumes = server
            .received()
            .await
            .into_iter()
            .filter(|request| request["method"] == "thread/resume")
            .count();
        assert_eq!(resumes, 2, "one failed attempt and one bounded retry");
        client.shutdown().await;
        server.shutdown().await;
    }

    #[tokio::test]
    async fn authoritative_ingress_does_not_accumulate_idle_global_noise() {
        let reducer = Reducer::default();
        let ingress =
            AuthoritativeIngress::new(reducer.clone(), EventStore::default(), EventHub::new(8, 8));
        for sequence in 1..=100 {
            let status = if sequence % 2 == 0 { "active" } else { "idle" };
            let event = Arc::new(SequencedEvent::from_frame(
                sequence,
                Duration::from_millis(sequence),
                IncomingFrame::parse(serde_json::json!({
                    "jsonrpc": "2.0",
                    "method": "thread/status/changed",
                    "params": {
                        "threadId": format!("idle-{sequence}"),
                        "status": {"type": status}
                    }
                }))
                .unwrap(),
            ));
            ingress.process(event).await;
        }

        assert!(reducer.snapshot().await.threads.is_empty());
    }

    #[tokio::test]
    async fn rollout_race_retries_then_ephemeral_threads_give_up() {
        let (_dir, server, client, ingestor) = setup(2, false).await;
        server.set_resume_failures(2).await;
        assert_eq!(
            ingestor
                .discover("persisted", DiscoverySource::ThreadStarted)
                .await,
            DiscoveryOutcome::Subscribed
        );
        server.set_resume_failures(20).await;
        assert_eq!(
            ingestor
                .discover("ephemeral", DiscoverySource::ThreadStarted)
                .await,
            DiscoveryOutcome::Ephemeral
        );
        client.shutdown().await;
        server.shutdown().await;
    }
}
