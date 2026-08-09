//! Embedded Codex control library.

mod skills;
mod threads;

use control::{ControlConfig, Controller};
use ingest::{AuthoritativeIngress, IngestConfig, IngestError, SubscriptionState, ThreadIngestor};
use reducer::{Reducer, Snapshot};
use std::{
    collections::HashMap,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use store::{EventStore, QueryResult, StoreConfig};
use streaming::{DeltaStream, EventHub, LifecycleStream};
use supervisor::{SupervisionState, Supervisor, SupervisorConfig, SupervisorError};
use thiserror::Error;
use tokio::sync::watch;
use transport::{Health, TransportConfig, TransportHandle};

pub use control::{ActionOutcome, ActionTarget, Evidence};
pub use ingest::DiscoveryOutcome;
pub use protocol::{IncomingFrame, Plane, Sequence, SequencedEvent};
pub use reducer::ReplayResult;
pub use skills::{
    SkillLoadError, SkillManagementError, SkillMetadata, SkillsListEntry, SkillsListResponse,
};
pub use streaming::LifecycleItem;
pub use threads::{ListedThread, ReadThread, ReadTurn, ThreadListError, ThreadReadError};
pub use transport::{ConnectionPhase, RequestError};

/// Complete runtime configuration. Defaults are deliberately bounded and conservative.
#[derive(Clone, Debug)]
pub struct Config {
    /// Launch/repair the macOS GUI and shared daemon. Defaults to `true`.
    pub manage_gui: bool,
    /// Unix socket, bounded timeouts/reconnects, and a 256 MiB trusted-local frame limit.
    pub transport: TransportConfig,
    /// Ten-minute, 8 MiB/thread, 64 MiB/global retention defaults.
    pub store: StoreConfig,
    /// Four pages/200 candidates, 512 retained subscriptions, eight resume attempts.
    pub ingest: IngestConfig,
    /// Three-second evidence-correlation window and two `NotWritten` retries.
    pub control: ControlConfig,
    /// Retained lifecycle event count used before returning `GapTooOld + snapshot`.
    pub lifecycle_replay_capacity: usize,
    pub lifecycle_broadcast_capacity: usize,
    pub delta_broadcast_capacity: usize,
    pub supervisor: SupervisorConfig,
}

impl Default for Config {
    fn default() -> Self {
        let supervisor = SupervisorConfig::default();
        let transport = TransportConfig {
            socket_path: supervisor.socket_path.clone(),
            ..TransportConfig::default()
        };
        Self {
            manage_gui: true,
            transport,
            store: StoreConfig::default(),
            ingest: IngestConfig::default(),
            control: ControlConfig::default(),
            lifecycle_replay_capacity: 4096,
            lifecycle_broadcast_capacity: 1024,
            delta_broadcast_capacity: 2048,
            supervisor,
        }
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Supervisor(#[from] SupervisorError),
    #[error(transparent)]
    Ingest(#[from] IngestError),
    #[error("transport authoritative ingress receiver was already taken")]
    IngressAlreadyTaken,
    #[error(transparent)]
    Transport(#[from] RequestError),
}

#[derive(Clone)]
pub struct Handle {
    transport: TransportHandle,
    reducer: Reducer,
    hub: EventHub,
    store: EventStore,
    ingestor: ThreadIngestor<TransportHandle>,
    controller: Controller<TransportHandle>,
    skills: skills::SkillController<TransportHandle>,
    threads: threads::ThreadController<TransportHandle>,
    shutdown_tx: watch::Sender<bool>,
    stopped: Arc<AtomicBool>,
    supervision: Option<SupervisionState>,
}

impl Handle {
    pub fn lifecycle(&self, after: Sequence) -> LifecycleStream {
        self.hub.lifecycle(self.reducer.clone(), after)
    }
    pub fn deltas(&self) -> DeltaStream {
        self.hub.deltas()
    }
    pub async fn events_since(&self, after: Sequence) -> ReplayResult {
        self.reducer.events_since(after).await
    }
    pub async fn snapshot(&self) -> Snapshot {
        self.reducer.snapshot().await
    }
    pub async fn query_sequence(
        &self,
        thread_id: Option<&str>,
        after: Sequence,
        through: Option<Sequence>,
    ) -> QueryResult {
        self.store.query_sequence(thread_id, after, through).await
    }
    pub async fn query_time(
        &self,
        thread_id: Option<&str>,
        start_unix_ms: u64,
        end_unix_ms: u64,
    ) -> QueryResult {
        self.store
            .query_time(thread_id, start_unix_ms, end_unix_ms)
            .await
    }
    pub fn health(&self) -> watch::Receiver<Health> {
        self.transport.health()
    }
    pub fn supervision(&self) -> Option<&SupervisionState> {
        self.supervision.as_ref()
    }
    pub async fn subscription_states(&self) -> HashMap<String, SubscriptionState> {
        self.ingestor.states().await
    }

    pub async fn interrupt(
        &self,
        thread_id: impl Into<String>,
        turn_id: impl Into<String>,
    ) -> ActionOutcome {
        self.controller.interrupt(thread_id, turn_id).await
    }
    pub async fn steer(
        &self,
        thread_id: impl Into<String>,
        expected_turn_id: impl Into<String>,
        input: Vec<serde_json::Value>,
    ) -> ActionOutcome {
        self.controller
            .steer(thread_id, expected_turn_id, input)
            .await
    }
    pub async fn start(
        &self,
        thread_id: impl Into<String>,
        input: Vec<serde_json::Value>,
    ) -> ActionOutcome {
        self.controller.start(thread_id, input).await
    }

    /// Replaces the app-server's complete set of extra skill roots.
    ///
    /// Roots must be absolute, lexically normalized paths. The last successfully applied set is
    /// automatically restored whenever the shared app-server connection is re-established.
    pub async fn set_skill_extra_roots<I, P>(
        &self,
        extra_roots: I,
    ) -> Result<(), SkillManagementError>
    where
        I: IntoIterator<Item = P>,
        P: Into<std::path::PathBuf>,
    {
        self.skills
            .set_extra_roots(extra_roots.into_iter().map(Into::into).collect())
            .await
    }

    /// Forces the app-server to bypass its skill cache and returns typed discovery results.
    pub async fn force_refresh_skills<I, P>(
        &self,
        cwds: I,
    ) -> Result<SkillsListResponse, SkillManagementError>
    where
        I: IntoIterator<Item = P>,
        P: Into<std::path::PathBuf>,
    {
        self.skills
            .force_refresh(cwds.into_iter().map(Into::into).collect())
            .await
    }

    /// Lists every non-archived interactive thread reported by the app-server, including idle
    /// threads that are intentionally absent from the reducer snapshot.
    pub async fn list_threads(&self) -> Result<Vec<ListedThread>, ThreadListError> {
        self.threads.list_all().await
    }

    /// Reads one authoritative thread including retained turns and their items.
    ///
    /// This is the recovery surface for input omitted from lightweight lifecycle notifications.
    pub async fn read_thread(
        &self,
        thread_id: impl AsRef<str>,
    ) -> Result<ReadThread, ThreadReadError> {
        self.threads.read(thread_id.as_ref()).await
    }

    /// Ensures this observer receives subscription-scoped lifecycle items for a thread.
    ///
    /// Consumers that need item-level events for idle tasks should call this for authoritative
    /// `thread/list` results during startup instead of waiting for an activation-status signal.
    pub async fn observe_thread(&self, thread_id: impl AsRef<str>) -> DiscoveryOutcome {
        self.ingestor
            .discover(thread_id.as_ref(), ingest::DiscoverySource::Reconciliation)
            .await
    }

    pub async fn shutdown(&self) {
        if self.stopped.swap(true, Ordering::AcqRel) {
            return;
        }
        // Stop discovery first, then make a best-effort remote cleanup while transport still exists.
        let _ = self.shutdown_tx.send(true);
        self.ingestor.unsubscribe_all_best_effort().await;
        self.transport.shutdown().await;
    }

    fn shutdown_in_background(&self) {
        if self.stopped.load(Ordering::Acquire) {
            return;
        }
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let handle = self.clone();
            runtime.spawn(async move {
                handle.shutdown().await;
            });
        }
    }
}

struct RunGuard(Handle);
impl Drop for RunGuard {
    fn drop(&mut self) {
        self.0.shutdown_in_background();
    }
}

pub struct CodexControl;

impl CodexControl {
    /// Initializes supervision and transport, starts background loops, then runs `operation`
    /// concurrently with those loops. Closure return performs graceful cleanup.
    pub async fn run<F, Fut, T>(config: Config, operation: F) -> Result<T, Error>
    where
        F: FnOnce(Handle) -> Fut,
        Fut: Future<Output = T>,
    {
        let supervision = if config.manage_gui {
            Some(
                Supervisor::new(config.supervisor.clone())
                    .initialize()
                    .await?,
            )
        } else {
            None
        };
        let transport = TransportHandle::spawn(config.transport.clone());
        let receiver = transport.take_ingress().ok_or(Error::IngressAlreadyTaken)?;
        let reducer = Reducer::new(config.lifecycle_replay_capacity);
        let store = EventStore::new(config.store.clone());
        let hub = EventHub::new(
            config.lifecycle_broadcast_capacity,
            config.delta_broadcast_capacity,
        );
        let authoritative = AuthoritativeIngress::new(reducer.clone(), store.clone(), hub.clone());
        let ingestor =
            ThreadIngestor::new(transport.clone(), reducer.clone(), config.ingest.clone())
                .with_store(store.clone());
        let controller =
            Controller::new(transport.clone(), reducer.clone(), config.control.clone());
        let skills = skills::SkillController::new(transport.clone());
        let threads = threads::ThreadController::new(transport.clone());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let mut ingress_shutdown = shutdown_rx.clone();
        let signal_ingestor = ingestor.clone();
        tokio::spawn(async move {
            let mut receiver = receiver;
            loop {
                tokio::select! {
                    changed = ingress_shutdown.changed() => if changed.is_err() || *ingress_shutdown.borrow() { break; },
                    event = receiver.recv() => {
                        let Some(event) = event else { break };
                        authoritative.process(event.clone()).await;
                        if matches!(event.method(), Some("thread/started" | "thread/status/changed")) {
                            let ingestor = signal_ingestor.clone();
                            tokio::spawn(async move { let _ = ingestor.handle_signal(&event).await; });
                        }
                    }
                }
            }
        });

        // Reconciliation closes the reconnect gap before callers may send action traffic.
        if let Err(error) = ingestor.reconcile().await {
            transport.shutdown().await;
            return Err(error.into());
        }
        transport.mark_reconciled().await?;

        // Every subsequent connection has its own gate. A reconnect cannot reopen action
        // traffic until discovery/reconciliation has run against that new connection.
        let mut reconnect_health = transport.health();
        let reconnect_ingestor = ingestor.clone();
        let reconnect_skills = skills.clone();
        let reconnect_transport = transport.clone();
        let mut reconnect_shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    changed = reconnect_shutdown.changed() => if changed.is_err() || *reconnect_shutdown.borrow() { break; },
                    changed = reconnect_health.changed() => {
                        if changed.is_err() { break; }
                        if reconnect_health.borrow().phase == ConnectionPhase::Reconciling
                            && !reconnect_transport.is_reconciled()
                        {
                            let mut retry = Duration::from_millis(50);
                            while reconnect_health.borrow().phase == ConnectionPhase::Reconciling
                                && !reconnect_transport.is_reconciled()
                            {
                                let restored = match reconnect_ingestor.reconcile().await {
                                    Ok(_) => match reconnect_skills.reapply_extra_roots().await {
                                        Ok(_) => true,
                                        Err(error) => {
                                            tracing::error!(%error, "post-reconnect skill-root restoration failed; retrying before opening action gate");
                                            false
                                        }
                                    },
                                    Err(error) => {
                                        tracing::error!(%error, "post-reconnect reconciliation failed; retrying before opening action gate");
                                        false
                                    }
                                };
                                if restored {
                                    let _ = reconnect_transport.mark_reconciled().await;
                                    break;
                                }
                                tokio::select! {
                                    changed = reconnect_shutdown.changed() => {
                                        if changed.is_err() || *reconnect_shutdown.borrow() { return; }
                                    }
                                    () = tokio::time::sleep(retry) => {}
                                }
                                retry = (retry * 2).min(Duration::from_secs(2));
                            }
                        }
                    }
                }
            }
        });

        if config.manage_gui {
            let supervisor = Supervisor::new(config.supervisor);
            let _monitor = supervisor.monitor(shutdown_rx);
            // The monitor task owns its sender; consumers use health() for transport and may call
            // Supervisor directly when detailed GUI lifecycle events are required.
        }
        let handle = Handle {
            transport,
            reducer,
            hub,
            store,
            ingestor,
            controller,
            skills,
            threads,
            shutdown_tx,
            stopped: Arc::new(AtomicBool::new(false)),
            supervision,
        };
        let guard = RunGuard(handle.clone());
        let result = operation(handle.clone()).await;
        handle.shutdown().await;
        drop(guard);
        Ok(result)
    }
}

/// Installs an env-filtered tracing subscriber if the process has not installed one already.
pub fn init_tracing(default_filter: &str) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_filter));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{path::PathBuf, time::Duration};
    use transport::mock::{Fault, MockAppServer, MockThread};

    #[tokio::test]
    async fn run_invokes_closure_with_live_background_loops_and_cleans_up() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rpc.sock");
        let server = MockAppServer::start(path.clone()).await.unwrap();
        server
            .add_thread(MockThread {
                id: "active".into(),
                cwd: PathBuf::from("/mock/active"),
                status: "active".into(),
                turn_id: Some("turn".into()),
                ephemeral: false,
                updated_at: 1,
            })
            .await;
        let config = Config {
            manage_gui: false,
            transport: TransportConfig {
                socket_path: path,
                connect_timeout: Duration::from_millis(300),
                request_timeout: Duration::from_secs(1),
                retry_initial: Duration::from_millis(20),
                retry_max: Duration::from_millis(50),
                ..TransportConfig::default()
            },
            ..Config::default()
        };
        let server_for_run = server.clone();
        let result = CodexControl::run(config, move |handle| async move {
            assert!(handle.health().borrow().phase == ConnectionPhase::Connected);
            assert_eq!(
                handle.subscription_states().await.get("active"),
                Some(&SubscriptionState::Subscribed)
            );
            let mut lifecycle = handle.lifecycle(0);
            server_for_run
                .emit_notification(
                    "turn/completed",
                    serde_json::json!({"threadId":"active","turn":{"id":"turn"}}),
                )
                .await;
            let item = tokio::time::timeout(Duration::from_secs(1), lifecycle.recv())
                .await
                .unwrap();
            assert!(matches!(item, LifecycleItem::Event(_)));
            42
        })
        .await
        .unwrap();
        assert_eq!(result, 42);
        server.shutdown().await;
        let _ = IncomingFrame::parse(serde_json::json!({"id":1,"result":{}}));
    }

    #[tokio::test]
    async fn typed_thread_list_includes_idle_threads_absent_from_the_reducer() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rpc.sock");
        let idle_cwd = dir.path().join("idle-project");
        let server = MockAppServer::start(path.clone()).await.unwrap();
        server
            .add_thread(MockThread {
                id: "idle".into(),
                cwd: idle_cwd.clone(),
                status: "idle".into(),
                turn_id: None,
                ephemeral: false,
                updated_at: 1,
            })
            .await;
        let config = Config {
            manage_gui: false,
            transport: TransportConfig {
                socket_path: path,
                connect_timeout: Duration::from_millis(300),
                request_timeout: Duration::from_secs(1),
                retry_initial: Duration::from_millis(20),
                retry_max: Duration::from_millis(50),
                ..TransportConfig::default()
            },
            ..Config::default()
        };
        let server_for_run = server.clone();

        CodexControl::run(config, move |handle| async move {
            assert!(!handle.snapshot().await.threads.contains_key("idle"));
            let threads = handle.list_threads().await.unwrap();
            assert_eq!(threads.len(), 1);
            assert_eq!(threads[0].id, "idle");
            assert_eq!(threads[0].cwd, idle_cwd);
            assert_eq!(
                handle.observe_thread("idle").await,
                DiscoveryOutcome::Subscribed
            );
            assert_eq!(
                handle.subscription_states().await.get("idle"),
                Some(&SubscriptionState::Subscribed)
            );
            assert!(server_for_run.received().await.iter().any(|request| {
                request["method"] == "thread/list"
                    && request["params"]["sortKey"] == "updated_at"
                    && request["params"]["limit"] == 100
            }));
        })
        .await
        .unwrap();
        server.shutdown().await;
    }

    #[tokio::test]
    async fn ambiguous_action_is_exposed_end_to_end() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rpc.sock");
        let server = MockAppServer::start(path.clone()).await.unwrap();
        let config = Config {
            manage_gui: false,
            transport: TransportConfig {
                socket_path: path,
                connect_timeout: Duration::from_millis(300),
                request_timeout: Duration::from_secs(1),
                retry_initial: Duration::from_millis(20),
                retry_max: Duration::from_millis(50),
                ..TransportConfig::default()
            },
            ..Config::default()
        };
        let server_for_run = server.clone();
        CodexControl::run(config, move |handle| async move {
            server_for_run
                .set_fault(Fault::DisconnectOnMethod("turn/steer".into()))
                .await;
            assert!(matches!(
                handle.steer("t", "turn", vec![]).await,
                ActionOutcome::OutcomeUnknown { .. }
            ));
        })
        .await
        .unwrap();
        server.shutdown().await;
    }

    #[tokio::test]
    async fn skill_roots_and_forced_refresh_follow_the_mock_contract_and_survive_reconnect() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rpc.sock");
        let skill_root = dir.path().join("generated-skills");
        let project = dir.path().join("project");
        let server = MockAppServer::start(path.clone()).await.unwrap();
        let config = Config {
            manage_gui: false,
            transport: TransportConfig {
                socket_path: path,
                connect_timeout: Duration::from_millis(300),
                request_timeout: Duration::from_secs(1),
                retry_initial: Duration::from_millis(20),
                retry_max: Duration::from_millis(50),
                ..TransportConfig::default()
            },
            ..Config::default()
        };
        let server_for_run = server.clone();
        CodexControl::run(config, move |handle| async move {
            handle
                .set_skill_extra_roots([skill_root.clone()])
                .await
                .unwrap();
            let listed = handle
                .force_refresh_skills([project.clone()])
                .await
                .unwrap();
            assert_eq!(listed.data.len(), 1);
            assert_eq!(listed.data[0].cwd, project);

            let received = server_for_run.received().await;
            assert!(received.iter().any(|request| {
                request["method"] == "skills/extraRoots/set"
                    && request["params"] == serde_json::json!({"extraRoots":[skill_root.clone()]})
            }));
            assert!(received.iter().any(|request| {
                request["method"] == "skills/list"
                    && request["params"]
                        == serde_json::json!({"cwds":[project.clone()],"forceReload":true})
            }));

            // Force the current connection closed through a read-only skill request. Once the
            // transport reconnects, CodexControl must replay its remembered root set before it
            // marks the connection reconciled again.
            server_for_run
                .set_request_failures("skills/extraRoots/set", 1)
                .await;
            server_for_run
                .set_fault(Fault::DisconnectOnMethod("skills/list".into()))
                .await;
            assert!(matches!(
                handle.force_refresh_skills(Vec::<PathBuf>::new()).await,
                Err(SkillManagementError::Request(
                    RequestError::WrittenOutcomeUnknown { .. }
                ))
            ));
            server_for_run.set_fault(Fault::None).await;

            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let received = server_for_run.received().await;
                    let root_sets = received
                        .iter()
                        .filter(|request| request["method"] == "skills/extraRoots/set")
                        .count();
                    if root_sets >= 3
                        && handle.health().borrow().phase == ConnectionPhase::Connected
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("skill roots were not reapplied after reconnect");

            let received = server_for_run.received().await;
            let root_sets: Vec<_> = received
                .iter()
                .filter(|request| request["method"] == "skills/extraRoots/set")
                .collect();
            assert_eq!(root_sets.len(), 3);
            assert!(root_sets.iter().all(|request| {
                request["params"] == serde_json::json!({"extraRoots":[skill_root.clone()]})
            }));
        })
        .await
        .unwrap();
        server.shutdown().await;
    }
}
