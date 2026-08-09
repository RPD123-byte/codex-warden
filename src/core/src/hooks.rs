use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::path::PathBuf;
use thiserror::Error;
use transport::{RequestError, RpcClient};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HooksListResponse {
    pub data: Vec<HooksListEntry>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HooksListEntry {
    pub cwd: PathBuf,
    pub hooks: Vec<HookMetadata>,
    #[serde(default)]
    pub warnings: Vec<String>,
    #[serde(default)]
    pub errors: Vec<Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HookMetadata {
    pub key: String,
    pub event_name: String,
    pub command: Option<String>,
    pub source_path: PathBuf,
    pub enabled: bool,
    pub current_hash: String,
    pub trust_status: String,
    pub execution_mode: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HookTrustUpdate {
    pub key: String,
    pub current_hash: String,
}

#[derive(Debug, Error)]
pub enum HookManagementError {
    #[error(transparent)]
    Request(#[from] RequestError),
    #[error("invalid response from {method}: {source}")]
    InvalidResponse {
        method: &'static str,
        #[source]
        source: serde_json::Error,
    },
}

#[derive(Clone)]
pub(crate) struct HookController<C: RpcClient> {
    client: C,
}

impl<C: RpcClient> HookController<C> {
    pub(crate) fn new(client: C) -> Self {
        Self { client }
    }

    pub(crate) async fn list(
        &self,
        cwds: Vec<PathBuf>,
    ) -> Result<HooksListResponse, HookManagementError> {
        let value = self
            .client
            .request("hooks/list", json!({"cwds": cwds}))
            .await?;
        serde_json::from_value(value).map_err(|source| HookManagementError::InvalidResponse {
            method: "hooks/list",
            source,
        })
    }

    pub(crate) async fn trust(
        &self,
        updates: Vec<HookTrustUpdate>,
    ) -> Result<(), HookManagementError> {
        let values = updates
            .into_iter()
            .map(|update| (update.key, json!({"trusted_hash": update.current_hash})))
            .collect::<serde_json::Map<_, _>>();
        let value = self
            .client
            .request(
                "config/batchWrite",
                json!({
                    "edits": [{
                        "keyPath": "hooks.state",
                        "value": values,
                        "mergeStrategy": "upsert"
                    }],
                    "filePath": null,
                    "expectedVersion": null,
                    "reloadUserConfig": true
                }),
            )
            .await?;
        serde_json::from_value(value)
            .map(|_: serde_json::Map<String, Value>| ())
            .map_err(|source| HookManagementError::InvalidResponse {
                method: "config/batchWrite",
                source,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[derive(Clone, Default)]
    struct RecordingClient {
        requests: Arc<Mutex<Vec<(String, Value)>>>,
    }

    #[async_trait]
    impl RpcClient for RecordingClient {
        async fn request(&self, method: &str, params: Value) -> Result<Value, RequestError> {
            self.requests.lock().await.push((method.to_owned(), params));
            Ok(match method {
                "hooks/list" => json!({"data": [{
                    "cwd": "/tmp/project",
                    "hooks": [{
                        "key": "hook-key",
                        "eventName": "userPromptSubmit",
                        "command": "python3 bridge.py",
                        "sourcePath": "/tmp/hooks.json",
                        "enabled": true,
                        "currentHash": "sha256:abc",
                        "trustStatus": "untrusted",
                        "executionMode": "sync"
                    }],
                    "warnings": [],
                    "errors": []
                }]}),
                _ => json!({}),
            })
        }

        async fn request_action(
            &self,
            _method: &str,
            _params: Value,
        ) -> Result<Value, RequestError> {
            unreachable!("hook management does not use action RPC")
        }

        async fn notify(&self, _method: &str, _params: Value) -> Result<(), RequestError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn lists_and_trusts_hooks_through_typed_operations() {
        let client = RecordingClient::default();
        let controller = HookController::new(client.clone());
        let listed = controller
            .list(vec![PathBuf::from("/tmp/project")])
            .await
            .unwrap();
        assert_eq!(listed.data[0].hooks[0].key, "hook-key");

        controller
            .trust(vec![HookTrustUpdate {
                key: "hook-key".into(),
                current_hash: "sha256:abc".into(),
            }])
            .await
            .unwrap();

        let requests = client.requests.lock().await;
        assert_eq!(requests[0].0, "hooks/list");
        assert_eq!(requests[1].0, "config/batchWrite");
        assert_eq!(
            requests[1].1["edits"][0]["value"]["hook-key"]["trusted_hash"],
            "sha256:abc"
        );
    }

    #[derive(Clone)]
    struct FixedClient(Result<Value, RequestError>);

    #[async_trait]
    impl RpcClient for FixedClient {
        async fn request(&self, _method: &str, _params: Value) -> Result<Value, RequestError> {
            self.0.clone()
        }

        async fn request_action(
            &self,
            _method: &str,
            _params: Value,
        ) -> Result<Value, RequestError> {
            unreachable!("hook management does not use action RPC")
        }

        async fn notify(&self, _method: &str, _params: Value) -> Result<(), RequestError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn reports_malformed_list_and_trust_responses() {
        let malformed = HookController::new(FixedClient(Ok(json!([]))));
        assert!(matches!(
            malformed.list(Vec::new()).await,
            Err(HookManagementError::InvalidResponse {
                method: "hooks/list",
                ..
            })
        ));
        assert!(matches!(
            malformed
                .trust(vec![HookTrustUpdate {
                    key: "key".into(),
                    current_hash: "sha256:value".into(),
                }])
                .await,
            Err(HookManagementError::InvalidResponse {
                method: "config/batchWrite",
                ..
            })
        ));
    }

    #[tokio::test]
    async fn preserves_app_server_rejections() {
        let controller = HookController::new(FixedClient(Err(RequestError::Rejected {
            method: "hooks/list".into(),
            error: json!({"code": -32601, "message": "unsupported"}),
        })));
        assert!(matches!(
            controller.list(Vec::new()).await,
            Err(HookManagementError::Request(RequestError::Rejected { .. }))
        ));
    }
}
