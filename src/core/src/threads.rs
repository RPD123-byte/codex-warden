use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::{collections::HashSet, path::PathBuf};
use thiserror::Error;
use transport::{RequestError, RpcClient};

const PAGE_SIZE: usize = 100;

/// A thread returned by the app-server's authoritative `thread/list` endpoint.
///
/// The stable identity and working directory are typed. Additional app-server metadata is
/// retained so callers such as Warden can expose a useful thread listing without depending on
/// the reducer's active-thread retention policy.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ListedThread {
    pub id: String,
    pub cwd: PathBuf,
    #[serde(flatten)]
    pub metadata: Map<String, Value>,
}

/// A thread returned by the app-server's authoritative `thread/read` endpoint.
///
/// Turn and item content are retained so observers can recover input that is intentionally
/// omitted from the lightweight `turn/started` notification.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadThread {
    pub id: String,
    #[serde(default)]
    pub turns: Vec<ReadTurn>,
    #[serde(flatten)]
    pub metadata: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadTurn {
    pub id: String,
    #[serde(default)]
    pub items: Vec<Value>,
    #[serde(flatten)]
    pub metadata: Map<String, Value>,
}

#[derive(Debug, Error)]
pub enum ThreadListError {
    #[error(transparent)]
    Request(#[from] RequestError),
    #[error("invalid response from thread/list: {0}")]
    InvalidResponse(#[from] serde_json::Error),
    #[error("thread/list repeated pagination cursor {0:?}")]
    RepeatedCursor(String),
}

#[derive(Debug, Error)]
pub enum ThreadReadError {
    #[error(transparent)]
    Request(#[from] RequestError),
    #[error("invalid response from thread/read: {0}")]
    InvalidResponse(#[from] serde_json::Error),
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ThreadListPage {
    data: Vec<ListedThread>,
    #[serde(default)]
    next_cursor: Option<String>,
}

#[derive(Deserialize)]
struct ThreadReadResponse {
    thread: ReadThread,
}

/// Owns the narrow, read-only app-server thread-listing RPC surface.
#[derive(Clone)]
pub(crate) struct ThreadController<C: RpcClient> {
    client: C,
}

impl<C: RpcClient> ThreadController<C> {
    pub(crate) fn new(client: C) -> Self {
        Self { client }
    }

    pub(crate) async fn list_all(&self) -> Result<Vec<ListedThread>, ThreadListError> {
        let mut cursor: Option<String> = None;
        let mut seen_cursors = HashSet::new();
        let mut seen_threads = HashSet::new();
        let mut threads = Vec::new();

        loop {
            let value = self
                .client
                .request(
                    "thread/list",
                    json!({
                        "cursor": cursor,
                        "limit": PAGE_SIZE,
                        "sortKey": "updated_at",
                        "sortDirection": "desc",
                    }),
                )
                .await?;
            let page: ThreadListPage = serde_json::from_value(value)?;
            for thread in page.data {
                if seen_threads.insert(thread.id.clone()) {
                    threads.push(thread);
                }
            }

            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            if !seen_cursors.insert(next_cursor.clone()) {
                return Err(ThreadListError::RepeatedCursor(next_cursor));
            }
            cursor = Some(next_cursor);
        }

        Ok(threads)
    }

    pub(crate) async fn read(&self, thread_id: &str) -> Result<ReadThread, ThreadReadError> {
        let value = self
            .client
            .request(
                "thread/read",
                json!({"threadId": thread_id, "includeTurns": true}),
            )
            .await?;
        Ok(serde_json::from_value::<ThreadReadResponse>(value)?.thread)
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
            self.requests
                .lock()
                .await
                .push((method.to_owned(), params.clone()));
            Ok(if params["cursor"].is_null() {
                json!({
                    "data": [{
                        "id": "idle",
                        "cwd": "/tmp/idle-project",
                        "status": {"type": "idle"},
                        "updatedAt": 2
                    }],
                    "nextCursor": "next-page"
                })
            } else {
                json!({
                    "data": [{
                        "id": "active",
                        "cwd": "/tmp/active-project",
                        "status": {"type": "active"},
                        "updatedAt": 1
                    }],
                    "nextCursor": null
                })
            })
        }

        async fn request_action(
            &self,
            _method: &str,
            _params: Value,
        ) -> Result<Value, RequestError> {
            unreachable!("thread listing does not use the action RPC path")
        }

        async fn notify(&self, _method: &str, _params: Value) -> Result<(), RequestError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn list_all_uses_typed_paginated_thread_contract_and_keeps_idle_threads() {
        let client = RecordingClient::default();
        let threads = ThreadController::new(client.clone())
            .list_all()
            .await
            .unwrap();

        assert_eq!(
            threads
                .iter()
                .map(|thread| (thread.id.as_str(), thread.cwd.as_path()))
                .collect::<Vec<_>>(),
            vec![
                ("idle", std::path::Path::new("/tmp/idle-project")),
                ("active", std::path::Path::new("/tmp/active-project")),
            ]
        );
        assert_eq!(threads[0].metadata["status"]["type"], "idle");
        assert_eq!(
            *client.requests.lock().await,
            vec![
                (
                    "thread/list".into(),
                    json!({
                        "cursor": null,
                        "limit": PAGE_SIZE,
                        "sortKey": "updated_at",
                        "sortDirection": "desc",
                    }),
                ),
                (
                    "thread/list".into(),
                    json!({
                        "cursor": "next-page",
                        "limit": PAGE_SIZE,
                        "sortKey": "updated_at",
                        "sortDirection": "desc",
                    }),
                ),
            ]
        );
    }

    #[tokio::test]
    async fn read_retains_turn_items_omitted_from_turn_started() {
        #[derive(Clone)]
        struct ReadClient;

        #[async_trait]
        impl RpcClient for ReadClient {
            async fn request(&self, method: &str, params: Value) -> Result<Value, RequestError> {
                assert_eq!(method, "thread/read");
                assert_eq!(params, json!({"threadId":"thread","includeTurns":true}));
                Ok(json!({"thread":{
                    "id":"thread",
                    "cwd":"/tmp/project",
                    "turns":[{"id":"turn","status":"inProgress","items":[
                        {"id":"user","type":"userMessage","content":[{"type":"text","text":"hello"}]}
                    ]}]
                }}))
            }

            async fn request_action(
                &self,
                _method: &str,
                _params: Value,
            ) -> Result<Value, RequestError> {
                unreachable!("thread reading does not use the action RPC path")
            }

            async fn notify(&self, _method: &str, _params: Value) -> Result<(), RequestError> {
                Ok(())
            }
        }

        let thread = ThreadController::new(ReadClient)
            .read("thread")
            .await
            .unwrap();
        assert_eq!(thread.id, "thread");
        assert_eq!(thread.turns[0].id, "turn");
        assert_eq!(thread.turns[0].items[0]["type"], "userMessage");
        assert_eq!(thread.metadata["cwd"], "/tmp/project");
        assert_eq!(thread.turns[0].metadata["status"], "inProgress");
    }
}
