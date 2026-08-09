use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    path::{Component, Path, PathBuf},
    sync::Arc,
};
use thiserror::Error;
use tokio::sync::Mutex;
use transport::{RequestError, RpcClient};

/// A typed response from `skills/list`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SkillsListResponse {
    pub data: Vec<SkillsListEntry>,
}

/// Skills and load errors discovered for one requested working directory.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SkillsListEntry {
    pub cwd: PathBuf,
    pub skills: Vec<SkillMetadata>,
    pub errors: Vec<SkillLoadError>,
}

/// The stable skill fields exposed by the current app-server protocol.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillMetadata {
    pub name: String,
    pub description: String,
    pub path: PathBuf,
    pub scope: String,
    pub enabled: bool,
    #[serde(default)]
    pub short_description: Option<String>,
    #[serde(default)]
    pub interface: Option<Value>,
    #[serde(default)]
    pub dependencies: Option<Value>,
}

/// A skill file that could not be loaded by the app-server.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillLoadError {
    pub path: PathBuf,
    pub message: String,
}

#[derive(Debug, Error)]
pub enum SkillManagementError {
    #[error("{field} path {path:?} must be {requirement}")]
    InvalidPath {
        field: &'static str,
        path: PathBuf,
        requirement: &'static str,
    },
    #[error(transparent)]
    Request(#[from] RequestError),
    #[error("invalid response from {method}: {source}")]
    InvalidResponse {
        method: &'static str,
        #[source]
        source: serde_json::Error,
    },
}

#[derive(Default)]
struct SkillState {
    // `Some([])` is meaningful: it clears previously registered extra roots and must also
    // be replayed after reconnect. `None` means the caller has never set this connection state.
    extra_roots: Option<Vec<PathBuf>>,
}

/// Owns the narrow skill-management RPC surface and the connection-local state that must be
/// restored after the app-server transport reconnects.
#[derive(Clone)]
pub(crate) struct SkillController<C: RpcClient> {
    client: C,
    state: Arc<Mutex<SkillState>>,
}

impl<C: RpcClient> SkillController<C> {
    pub(crate) fn new(client: C) -> Self {
        Self {
            client,
            state: Arc::new(Mutex::new(SkillState::default())),
        }
    }

    pub(crate) async fn set_extra_roots(
        &self,
        extra_roots: Vec<PathBuf>,
    ) -> Result<(), SkillManagementError> {
        let encoded = encode_extra_roots(&extra_roots)?;
        // Serialize root updates with reconnect replay. This ensures the remembered state is
        // exactly the last root set whose app-server response was observed successfully.
        let mut state = self.state.lock().await;
        self.send_extra_roots(encoded).await?;
        state.extra_roots = Some(extra_roots);
        Ok(())
    }

    pub(crate) async fn force_refresh(
        &self,
        cwds: Vec<PathBuf>,
    ) -> Result<SkillsListResponse, SkillManagementError> {
        let cwds = encode_paths(&cwds, "skill-list cwd")?;
        let value = self
            .client
            .request(
                "skills/list",
                serde_json::json!({"cwds": cwds, "forceReload": true}),
            )
            .await?;
        serde_json::from_value(value).map_err(|source| SkillManagementError::InvalidResponse {
            method: "skills/list",
            source,
        })
    }

    /// Reapplies the last successful root set to a new app-server connection. Returns whether
    /// there was remembered connection-local state to restore.
    pub(crate) async fn reapply_extra_roots(&self) -> Result<bool, SkillManagementError> {
        let state = self.state.lock().await;
        let Some(extra_roots) = state.extra_roots.as_ref() else {
            return Ok(false);
        };
        let encoded = encode_extra_roots(extra_roots)?;
        self.send_extra_roots(encoded).await?;
        Ok(true)
    }

    async fn send_extra_roots(&self, encoded: Vec<String>) -> Result<(), SkillManagementError> {
        let value = self
            .client
            .request(
                "skills/extraRoots/set",
                serde_json::json!({"extraRoots": encoded}),
            )
            .await?;
        serde_json::from_value(value)
            .map(|_: EmptyResponse| ())
            .map_err(|source| SkillManagementError::InvalidResponse {
                method: "skills/extraRoots/set",
                source,
            })
    }
}

#[derive(Deserialize)]
struct EmptyResponse {}

fn encode_extra_roots(paths: &[PathBuf]) -> Result<Vec<String>, SkillManagementError> {
    for path in paths {
        let normalized: PathBuf = path.components().collect();
        if !path.is_absolute()
            || path.as_os_str() != normalized.as_os_str()
            || path
                .components()
                .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
        {
            return Err(SkillManagementError::InvalidPath {
                field: "skill extra root",
                path: path.clone(),
                requirement: "absolute and lexically normalized",
            });
        }
    }
    encode_paths(paths, "skill extra root")
}

fn encode_paths(
    paths: &[PathBuf],
    field: &'static str,
) -> Result<Vec<String>, SkillManagementError> {
    paths.iter().map(|path| encode_path(path, field)).collect()
}

fn encode_path(path: &Path, field: &'static str) -> Result<String, SkillManagementError> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| SkillManagementError::InvalidPath {
            field,
            path: path.to_owned(),
            requirement: "valid UTF-8 for the JSON-RPC protocol",
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Arc;

    #[derive(Clone, Default)]
    struct RecordingClient {
        requests: Arc<Mutex<Vec<(String, Value)>>>,
    }

    #[async_trait]
    impl RpcClient for RecordingClient {
        async fn request(&self, method: &str, params: Value) -> Result<Value, RequestError> {
            self.requests
                .lock()
                .await
                .push((method.to_owned(), params.clone()));
            Ok(match method {
                "skills/list" => serde_json::json!({"data":[{
                    "cwd": params["cwds"][0],
                    "skills": [{
                        "name": "marker",
                        "description": "Warden marker",
                        "path": "/tmp/skills/marker/SKILL.md",
                        "scope": "user",
                        "enabled": true
                    }],
                    "errors": []
                }]}),
                _ => serde_json::json!({}),
            })
        }

        async fn request_action(
            &self,
            _method: &str,
            _params: Value,
        ) -> Result<Value, RequestError> {
            unreachable!("skill management does not use the action RPC path")
        }

        async fn notify(&self, _method: &str, _params: Value) -> Result<(), RequestError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn operations_use_only_the_typed_skill_contract() {
        let client = RecordingClient::default();
        let controller = SkillController::new(client.clone());
        controller
            .set_extra_roots(vec![PathBuf::from("/tmp/skills")])
            .await
            .unwrap();
        let response = controller
            .force_refresh(vec![PathBuf::from("/tmp/project")])
            .await
            .unwrap();

        assert_eq!(response.data[0].skills[0].name, "marker");
        assert_eq!(
            *client.requests.lock().await,
            vec![
                (
                    "skills/extraRoots/set".into(),
                    serde_json::json!({"extraRoots":["/tmp/skills"]}),
                ),
                (
                    "skills/list".into(),
                    serde_json::json!({"cwds":["/tmp/project"],"forceReload":true}),
                ),
            ]
        );
    }

    #[tokio::test]
    async fn relative_or_unnormalized_extra_roots_are_rejected_before_rpc() {
        let client = RecordingClient::default();
        let controller = SkillController::new(client.clone());

        for invalid in ["relative", "/tmp/../skills", "/tmp/./skills"] {
            assert!(matches!(
                controller
                    .set_extra_roots(vec![PathBuf::from(invalid)])
                    .await,
                Err(SkillManagementError::InvalidPath { .. })
            ));
        }
        assert!(client.requests.lock().await.is_empty());
    }
}
