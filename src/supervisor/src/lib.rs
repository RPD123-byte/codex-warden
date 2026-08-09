//! macOS GUI and shared-daemon supervision.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};
use thiserror::Error;
use tokio::{
    process::Command,
    sync::{broadcast, watch},
    time,
};

#[derive(Clone, Debug)]
pub struct SupervisorConfig {
    pub standalone_codex: PathBuf,
    pub socket_path: PathBuf,
    pub gui_executable: PathBuf,
    /// Gracefully restart the GUI during initialization even when it is already attached.
    pub restart_gui_on_initialize: bool,
    pub startup_timeout: Duration,
    pub graceful_quit_timeout: Duration,
    pub monitor_interval: Duration,
}

impl Default for SupervisorConfig {
    fn default() -> Self {
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/var/empty"));
        Self {
            standalone_codex: home.join(".codex/packages/standalone/current/codex"),
            socket_path: home.join(".codex/app-server-control/app-server-control.sock"),
            gui_executable: PathBuf::from("/Applications/ChatGPT.app/Contents/MacOS/ChatGPT"),
            restart_gui_on_initialize: true,
            startup_timeout: Duration::from_secs(10),
            graceful_quit_timeout: Duration::from_secs(8),
            monitor_interval: Duration::from_secs(5),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DaemonOwnership {
    Attached,
    Started,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupervisionState {
    pub daemon_ownership: DaemonOwnership,
    pub daemon_version: String,
    pub gui_attached: bool,
    pub daemon_left_running: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MonitorEvent {
    Healthy,
    DaemonRestarted,
    GuiRestarted,
    Incompatible(String),
    UserActionRequired(String),
}

#[derive(Debug, Error)]
pub enum SupervisorError {
    #[error(
        "standalone Codex is missing at {path}; install it with the official standalone installer"
    )]
    MissingStandalone { path: PathBuf },
    #[error("ChatGPT executable is missing at {path}")]
    MissingGui { path: PathBuf },
    #[error("daemon command failed: {0}")]
    DaemonCommand(String),
    #[error("daemon version is incompatible or unavailable: {0}")]
    IncompatibleVersion(String),
    #[error(
        "ChatGPT did not attach to the shared daemon; check the installed GUI/Codex compatibility: {0}"
    )]
    AttachmentUnverified(String),
    #[error("user action required: {0}")]
    UserActionRequired(String),
    #[error("process inspection failed: {0}")]
    Inspection(String),
}

#[derive(Clone, Debug)]
pub struct CommandOutput {
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
}

#[async_trait]
pub trait ProcessOps: Send + Sync {
    async fn exists(&self, path: &Path) -> bool;
    async fn ps_environment(&self) -> Result<String, String>;
    async fn socket_users(&self, socket: &Path) -> Result<String, String>;
    async fn output(&self, program: &Path, args: &[&str]) -> Result<CommandOutput, String>;
    async fn spawn(
        &self,
        program: &Path,
        args: &[&str],
        env: &[(&str, &str)],
    ) -> Result<(), String>;
    async fn graceful_quit_chatgpt(&self) -> Result<(), String>;
}

#[derive(Default)]
pub struct RealProcessOps;

fn child_command(program: &Path, args: &[&str], env: &[(&str, &str)]) -> Command {
    let mut command = Command::new(program);
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    for (key, value) in env {
        command.env(key, value);
    }
    // A GUI/daemon launched by a foreground experiment must not inherit its terminal
    // process group. Otherwise Ctrl-C (or another group signal) also reaches ChatGPT.
    #[cfg(unix)]
    command.process_group(0);
    command
}

#[async_trait]
impl ProcessOps for RealProcessOps {
    async fn exists(&self, path: &Path) -> bool {
        tokio::fs::metadata(path).await.is_ok()
    }

    async fn ps_environment(&self) -> Result<String, String> {
        let output = Command::new("/bin/ps")
            .args(["eww", "-axo", "pid=,command="])
            .output()
            .await
            .map_err(|e| e.to_string())?;
        if !output.status.success() {
            return Err(String::from_utf8_lossy(&output.stderr).into_owned());
        }
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    async fn socket_users(&self, socket: &Path) -> Result<String, String> {
        let output = Command::new("/usr/sbin/lsof")
            .arg("-n")
            .arg(socket)
            .output()
            .await
            .map_err(|e| e.to_string())?;
        if !output.status.success() {
            return Ok(String::new());
        }
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    async fn output(&self, program: &Path, args: &[&str]) -> Result<CommandOutput, String> {
        let output = Command::new(program)
            .args(args)
            .output()
            .await
            .map_err(|e| e.to_string())?;
        Ok(CommandOutput {
            success: output.status.success(),
            stdout: String::from_utf8_lossy(&output.stdout).trim().into(),
            stderr: String::from_utf8_lossy(&output.stderr).trim().into(),
        })
    }

    async fn spawn(
        &self,
        program: &Path,
        args: &[&str],
        env: &[(&str, &str)],
    ) -> Result<(), String> {
        let mut command = child_command(program, args, env);
        command.spawn().map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn graceful_quit_chatgpt(&self) -> Result<(), String> {
        let output = Command::new("/usr/bin/osascript")
            .args(["-e", "tell application \"ChatGPT\" to quit"])
            .output()
            .await
            .map_err(|e| e.to_string())?;
        if output.status.success() {
            Ok(())
        } else {
            Err(String::from_utf8_lossy(&output.stderr).into_owned())
        }
    }
}

#[derive(Clone)]
pub struct Supervisor {
    config: SupervisorConfig,
    ops: Arc<dyn ProcessOps>,
}

impl Supervisor {
    pub fn new(config: SupervisorConfig) -> Self {
        Self {
            config,
            ops: Arc::new(RealProcessOps),
        }
    }
    pub fn with_ops(config: SupervisorConfig, ops: Arc<dyn ProcessOps>) -> Self {
        Self { config, ops }
    }

    async fn launch_gui(&self) -> Result<(), String> {
        let application = application_bundle(&self.config.gui_executable);
        let application = application
            .to_str()
            .ok_or_else(|| "ChatGPT application path is not valid UTF-8".to_string())?;
        // Launch Services owns the GUI process. A terminal or process manager can then
        // tear down the embedding process tree without treating ChatGPT as its child.
        // `open` does not forward its own environment to the launched application, so the
        // variables must be supplied through its explicit `--env` launch options.
        self.ops
            .spawn(
                Path::new("/usr/bin/open"),
                &[
                    "--env",
                    "CODEX_APP_SERVER_USE_LOCAL_DAEMON=1",
                    "--env",
                    "CODEX_APP_SERVER_FORCE_CLI=0",
                    "-a",
                    application,
                ],
                &[],
            )
            .await
    }

    pub async fn initialize(&self) -> Result<SupervisionState, SupervisorError> {
        if !self.ops.exists(&self.config.standalone_codex).await {
            return Err(SupervisorError::MissingStandalone {
                path: self.config.standalone_codex.clone(),
            });
        }
        if !self.ops.exists(&self.config.gui_executable).await {
            return Err(SupervisorError::MissingGui {
                path: self.config.gui_executable.clone(),
            });
        }
        let processes = self
            .ops
            .ps_environment()
            .await
            .map_err(SupervisorError::Inspection)?;
        let daemon_running = has_managed_daemon(&processes, &self.config.standalone_codex);
        let ownership = if daemon_running {
            DaemonOwnership::Attached
        } else {
            self.ops
                .spawn(
                    &self.config.standalone_codex,
                    &["app-server", "daemon", "start"],
                    &[],
                )
                .await
                .map_err(SupervisorError::DaemonCommand)?;
            DaemonOwnership::Started
        };
        let version = self.wait_for_daemon_version().await?;

        let mut processes = self
            .ops
            .ps_environment()
            .await
            .map_err(SupervisorError::Inspection)?;
        if self.config.restart_gui_on_initialize || !self.gui_attached(&processes).await {
            if gui_running(&processes, &self.config.gui_executable) {
                self.ops
                    .graceful_quit_chatgpt()
                    .await
                    .map_err(SupervisorError::UserActionRequired)?;
                let deadline = time::Instant::now() + self.config.graceful_quit_timeout;
                while time::Instant::now() < deadline {
                    time::sleep(Duration::from_millis(100)).await;
                    processes = self
                        .ops
                        .ps_environment()
                        .await
                        .map_err(SupervisorError::Inspection)?;
                    if !gui_running(&processes, &self.config.gui_executable) {
                        break;
                    }
                }
                if gui_running(&processes, &self.config.gui_executable) {
                    return Err(SupervisorError::UserActionRequired(
                        "ChatGPT ignored graceful quit; quit it manually. It was not force-killed."
                            .into(),
                    ));
                }
            }
            self.launch_gui()
                .await
                .map_err(SupervisorError::UserActionRequired)?;
            self.wait_for_gui_attachment().await?;
        }
        Ok(SupervisionState {
            daemon_ownership: ownership,
            daemon_version: version,
            gui_attached: true,
            daemon_left_running: true,
        })
    }

    async fn wait_for_daemon_version(&self) -> Result<String, SupervisorError> {
        let deadline = time::Instant::now() + self.config.startup_timeout;
        let mut detail = String::new();
        while time::Instant::now() < deadline {
            match self
                .ops
                .output(
                    &self.config.standalone_codex,
                    &["app-server", "daemon", "version"],
                )
                .await
            {
                Ok(output) if output.success && !output.stdout.is_empty() => {
                    return Ok(output.stdout);
                }
                Ok(output) => {
                    detail = if output.stderr.is_empty() {
                        output.stdout
                    } else {
                        output.stderr
                    }
                }
                Err(error) => detail = error,
            }
            time::sleep(Duration::from_millis(100)).await;
        }
        Err(SupervisorError::IncompatibleVersion(detail))
    }

    async fn wait_for_gui_attachment(&self) -> Result<(), SupervisorError> {
        let deadline = time::Instant::now() + self.config.startup_timeout;
        while time::Instant::now() < deadline {
            let processes = self
                .ops
                .ps_environment()
                .await
                .map_err(SupervisorError::Inspection)?;
            if self.gui_attached(&processes).await {
                return Ok(());
            }
            time::sleep(Duration::from_millis(100)).await;
        }
        Err(SupervisorError::AttachmentUnverified(
            "ChatGPT did not establish an accepted connection to the managed daemon within the startup timeout".into(),
        ))
    }

    async fn gui_attached(&self, processes: &str) -> bool {
        if !gui_mode_enabled(processes, &self.config.gui_executable) {
            return false;
        }
        self.ops
            .socket_users(&self.config.socket_path)
            .await
            .is_ok_and(|users| Self::has_accepted_socket_connection(&users))
    }

    /// `lsof <unix-socket-path>` reports the listener and accepted endpoints under the
    /// daemon process. The GUI's peer endpoint is unnamed on macOS, so it does not appear
    /// as a `ChatGPT` row for the path. The environment check above identifies the GUI;
    /// more than one socket record proves that a client has actually connected.
    fn has_accepted_socket_connection(users: &str) -> bool {
        users
            .lines()
            .filter(|line| {
                let line = line.trim();
                !line.is_empty() && !line.starts_with("COMMAND")
            })
            .count()
            > 1
    }

    pub async fn check(&self) -> MonitorEvent {
        let Ok(processes) = self.ops.ps_environment().await else {
            return MonitorEvent::UserActionRequired("cannot inspect processes".into());
        };
        if !has_managed_daemon(&processes, &self.config.standalone_codex) {
            return match self
                .ops
                .spawn(
                    &self.config.standalone_codex,
                    &["app-server", "daemon", "start"],
                    &[],
                )
                .await
            {
                Ok(()) => MonitorEvent::DaemonRestarted,
                Err(error) => MonitorEvent::UserActionRequired(error),
            };
        }
        match self
            .ops
            .output(
                &self.config.standalone_codex,
                &["app-server", "daemon", "version"],
            )
            .await
        {
            Ok(output) if !output.success => return MonitorEvent::Incompatible(output.stderr),
            Err(error) => return MonitorEvent::Incompatible(error),
            _ => {}
        }
        if !self.gui_attached(&processes).await {
            if !gui_running(&processes, &self.config.gui_executable) {
                return match self.launch_gui().await {
                    Ok(()) => MonitorEvent::GuiRestarted,
                    Err(error) => MonitorEvent::UserActionRequired(error),
                };
            }
            return MonitorEvent::UserActionRequired(
                "ChatGPT is not attached to the shared daemon".into(),
            );
        }
        MonitorEvent::Healthy
    }

    pub fn monitor(
        &self,
        mut shutdown: watch::Receiver<bool>,
    ) -> broadcast::Receiver<MonitorEvent> {
        let (events, receiver) = broadcast::channel(32);
        let this = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    changed = shutdown.changed() => if changed.is_err() || *shutdown.borrow() { break; },
                    event = async {
                        time::sleep(this.config.monitor_interval).await;
                        this.check().await
                    } => { let _ = events.send(event); }
                }
            }
        });
        receiver
    }

    /// Shutdown intentionally does not stop a daemon that this library started or attached to.
    pub fn shutdown_state(&self, mut state: SupervisionState) -> SupervisionState {
        state.daemon_left_running = true;
        state
    }
}

fn application_bundle(executable: &Path) -> &Path {
    executable
        .ancestors()
        .find(|path| path.extension().is_some_and(|extension| extension == "app"))
        .unwrap_or(executable)
}

fn has_managed_daemon(processes: &str, codex: &Path) -> bool {
    let codex = codex.to_string_lossy();
    processes.lines().any(|line| {
        line.contains(codex.as_ref())
            && line.contains("app-server")
            && (line.contains("daemon") || line.contains("--listen unix://"))
    })
}

fn gui_running(processes: &str, gui: &Path) -> bool {
    let gui = gui.to_string_lossy();
    processes.lines().any(|line| line.contains(gui.as_ref()))
}

fn gui_mode_enabled(processes: &str, gui: &Path) -> bool {
    let gui = gui.to_string_lossy();
    processes.lines().any(|line| {
        line.contains(gui.as_ref())
            && line.contains("CODEX_APP_SERVER_USE_LOCAL_DAEMON=1")
            && !line.contains("CODEX_APP_SERVER_FORCE_CLI=1")
            && !line.contains("CODEX_CLI_PATH=")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::Mutex;

    #[derive(Default)]
    struct FakeState {
        standalone: bool,
        gui: bool,
        daemon: bool,
        attached: bool,
        stubborn: bool,
        compatible: bool,
        spawns: Vec<String>,
        spawn_args: Vec<Vec<String>>,
        spawn_env: Vec<Vec<(String, String)>>,
        quit_requests: usize,
    }
    #[derive(Default)]
    struct FakeOps {
        state: Mutex<FakeState>,
    }

    #[async_trait]
    impl ProcessOps for FakeOps {
        async fn exists(&self, path: &Path) -> bool {
            let state = self.state.lock().await;
            if path.to_string_lossy().contains("codex") {
                state.standalone
            } else {
                true
            }
        }
        async fn ps_environment(&self) -> Result<String, String> {
            let state = self.state.lock().await;
            let mut lines = String::new();
            if state.daemon {
                lines.push_str("/mock/codex app-server --listen unix://\n");
            }
            if state.gui {
                lines.push_str("/mock/ChatGPT");
                if state.attached {
                    lines.push_str(" CODEX_APP_SERVER_USE_LOCAL_DAEMON=1");
                }
                lines.push('\n');
            }
            Ok(lines)
        }
        async fn socket_users(&self, _socket: &Path) -> Result<String, String> {
            let state = self.state.lock().await;
            Ok(if state.attached {
                "COMMAND PID USER FD TYPE NAME\ncodex 123 user 15u unix /mock/socket\ncodex 123 user 31u unix /mock/socket".into()
            } else {
                String::new()
            })
        }
        async fn output(&self, _program: &Path, _args: &[&str]) -> Result<CommandOutput, String> {
            let state = self.state.lock().await;
            Ok(if state.compatible {
                CommandOutput {
                    success: true,
                    stdout: "codex-cli 0.144.6".into(),
                    stderr: String::new(),
                }
            } else {
                CommandOutput {
                    success: false,
                    stdout: String::new(),
                    stderr: "mismatch".into(),
                }
            })
        }
        async fn spawn(
            &self,
            program: &Path,
            args: &[&str],
            env: &[(&str, &str)],
        ) -> Result<(), String> {
            let mut state = self.state.lock().await;
            state.spawns.push(program.display().to_string());
            state
                .spawn_args
                .push(args.iter().map(|value| (*value).to_string()).collect());
            state.spawn_env.push(
                env.iter()
                    .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                    .collect(),
            );
            if program == Path::new("/usr/bin/open")
                || program.to_string_lossy().contains("ChatGPT")
            {
                state.gui = true;
                state.attached = env.contains(&("CODEX_APP_SERVER_USE_LOCAL_DAEMON", "1"))
                    || args
                        .windows(2)
                        .any(|pair| pair == ["--env", "CODEX_APP_SERVER_USE_LOCAL_DAEMON=1"]);
            } else {
                state.daemon = true;
            }
            Ok(())
        }
        async fn graceful_quit_chatgpt(&self) -> Result<(), String> {
            let mut state = self.state.lock().await;
            state.quit_requests += 1;
            if !state.stubborn {
                state.gui = false;
                state.attached = false;
            }
            Ok(())
        }
    }

    fn config() -> SupervisorConfig {
        SupervisorConfig {
            standalone_codex: "/mock/codex".into(),
            gui_executable: "/mock/ChatGPT".into(),
            socket_path: "/mock/socket".into(),
            restart_gui_on_initialize: false,
            startup_timeout: Duration::from_millis(80),
            graceful_quit_timeout: Duration::from_millis(30),
            monitor_interval: Duration::from_millis(10),
        }
    }

    #[test]
    fn accepted_socket_detection_matches_macos_lsof_shape() {
        assert!(Supervisor::has_accepted_socket_connection(
            "COMMAND PID USER FD TYPE NAME\ncodex 123 user 15u unix /mock/socket\ncodex 123 user 31u unix /mock/socket"
        ));
        assert!(!Supervisor::has_accepted_socket_connection(
            "COMMAND PID USER FD TYPE NAME\ncodex 123 user 15u unix /mock/socket"
        ));
    }

    #[test]
    fn launch_services_receives_the_application_bundle_instead_of_the_executable() {
        assert_eq!(
            application_bundle(Path::new(
                "/Applications/ChatGPT.app/Contents/MacOS/ChatGPT"
            )),
            Path::new("/Applications/ChatGPT.app")
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn supervisor_children_run_in_their_own_process_group() {
        let mut command = child_command(Path::new("/bin/sleep"), &["5"], &[]);
        let mut child = command.spawn().unwrap();
        let pid = child.id().unwrap();
        let output = Command::new("/bin/ps")
            .args(["-o", "pgid=", "-p", &pid.to_string()])
            .output()
            .await
            .unwrap();
        let process_group: u32 = String::from_utf8(output.stdout)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let _ = child.kill().await;

        assert_eq!(process_group, pid);
    }

    #[tokio::test]
    async fn attaches_existing_and_leaves_daemon_running() {
        let ops = Arc::new(FakeOps::default());
        *ops.state.lock().await = FakeState {
            standalone: true,
            gui: true,
            daemon: true,
            attached: true,
            compatible: true,
            ..FakeState::default()
        };
        let supervisor = Supervisor::with_ops(config(), ops);
        let state = supervisor.initialize().await.unwrap();
        assert_eq!(state.daemon_ownership, DaemonOwnership::Attached);
        assert!(supervisor.shutdown_state(state).daemon_left_running);
    }

    #[tokio::test]
    async fn starts_daemon_and_relaunches_gui_with_owned_environment() {
        let ops = Arc::new(FakeOps::default());
        *ops.state.lock().await = FakeState {
            standalone: true,
            gui: true,
            compatible: true,
            ..FakeState::default()
        };
        let supervisor = Supervisor::with_ops(config(), ops.clone());
        let state = supervisor.initialize().await.unwrap();
        assert_eq!(state.daemon_ownership, DaemonOwnership::Started);
        assert!(ops.state.lock().await.attached);
    }

    #[tokio::test]
    async fn requested_startup_restart_relaunches_attached_gui_but_shutdown_leaves_it_running() {
        let ops = Arc::new(FakeOps::default());
        *ops.state.lock().await = FakeState {
            standalone: true,
            gui: true,
            daemon: true,
            attached: true,
            compatible: true,
            ..FakeState::default()
        };
        let mut restart_config = config();
        restart_config.restart_gui_on_initialize = true;
        let supervisor = Supervisor::with_ops(restart_config, ops.clone());

        let state = supervisor.initialize().await.unwrap();
        let state = supervisor.shutdown_state(state);
        let process_state = ops.state.lock().await;

        assert_eq!(process_state.quit_requests, 1);
        assert_eq!(process_state.spawns, vec!["/usr/bin/open"]);
        assert_eq!(
            process_state.spawn_args,
            vec![vec![
                "--env",
                "CODEX_APP_SERVER_USE_LOCAL_DAEMON=1",
                "--env",
                "CODEX_APP_SERVER_FORCE_CLI=0",
                "-a",
                "/mock/ChatGPT",
            ]]
        );
        assert_eq!(
            process_state.spawn_env,
            vec![Vec::<(String, String)>::new()]
        );
        assert!(process_state.gui);
        assert!(process_state.attached);
        assert!(state.daemon_left_running);
    }

    #[tokio::test]
    async fn stubborn_gui_is_never_force_killed() {
        let ops = Arc::new(FakeOps::default());
        *ops.state.lock().await = FakeState {
            standalone: true,
            gui: true,
            daemon: true,
            stubborn: true,
            compatible: true,
            ..FakeState::default()
        };
        let supervisor = Supervisor::with_ops(config(), ops);
        assert!(matches!(
            supervisor.initialize().await,
            Err(SupervisorError::UserActionRequired(_))
        ));
    }

    #[tokio::test]
    async fn monitor_reports_restart_and_incompatibility() {
        let ops = Arc::new(FakeOps::default());
        *ops.state.lock().await = FakeState {
            standalone: true,
            gui: true,
            attached: true,
            compatible: true,
            ..FakeState::default()
        };
        let supervisor = Supervisor::with_ops(config(), ops.clone());
        assert_eq!(supervisor.check().await, MonitorEvent::DaemonRestarted);
        ops.state.lock().await.compatible = false;
        assert!(matches!(
            supervisor.check().await,
            MonitorEvent::Incompatible(_)
        ));
    }

    #[tokio::test]
    async fn monitor_restarts_a_missing_gui_with_daemon_environment() {
        let ops = Arc::new(FakeOps::default());
        *ops.state.lock().await = FakeState {
            standalone: true,
            daemon: true,
            compatible: true,
            ..FakeState::default()
        };
        let supervisor = Supervisor::with_ops(config(), ops.clone());
        assert_eq!(supervisor.check().await, MonitorEvent::GuiRestarted);
        assert!(ops.state.lock().await.attached);
    }
}
