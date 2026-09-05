//! Versioned stdio protocol used when CLF3 is hosted by Fluorine.
//!
//! Stdout is reserved for newline-delimited JSON. Human-readable logging stays
//! on stderr. Nexus credentials never cross this boundary: the host returns
//! short-lived, ordered download URLs after it has performed authorization.

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::io::{self, BufRead};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::sync::{oneshot, Notify};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::progress_json::JsonEventWriter;

pub const HOST_PROTOCOL_VERSION: u32 = 1;

#[derive(Debug, Clone)]
pub struct HostedNexusRequest {
    pub archive_name: String,
    pub expected_size: u64,
    pub domain: String,
    pub mod_id: u64,
    pub file_id: u64,
}

#[derive(Debug, Clone)]
pub struct HostedManualRequest {
    pub archive_name: String,
    pub expected_size: u64,
    pub expected_hash: String,
    pub url: String,
    pub prompt: String,
}

#[async_trait]
pub trait HostedDownloadProvider: Send + Sync {
    async fn resolve_nexus(&self, request: HostedNexusRequest) -> Result<Vec<String>>;
    async fn resolve_manual(&self, request: HostedManualRequest) -> Result<PathBuf>;
    fn cancellation_token(&self) -> CancellationToken;
}

#[derive(Debug)]
enum HostResolution {
    Urls(Vec<String>),
    LocalPath(PathBuf),
}

type PendingSender = oneshot::Sender<Result<HostResolution, String>>;

pub struct StdioHost {
    writer: JsonEventWriter,
    pending: Mutex<HashMap<String, PendingSender>>,
    ready: Notify,
    ready_protocol: Mutex<Option<u32>>,
    cancellation: CancellationToken,
}

impl StdioHost {
    pub fn start(writer: JsonEventWriter) -> Arc<Self> {
        let host = Arc::new(Self {
            writer,
            pending: Mutex::new(HashMap::new()),
            ready: Notify::new(),
            ready_protocol: Mutex::new(None),
            cancellation: CancellationToken::new(),
        });

        let reader_host = Arc::clone(&host);
        std::thread::Builder::new()
            .name("clf3-host-stdin".into())
            .spawn(move || reader_host.read_commands())
            .expect("spawn host protocol reader");

        host.writer.emit_value(&json!({
            "type": "hello",
            "protocol_version": HOST_PROTOCOL_VERSION,
            "engine_version": env!("CARGO_PKG_VERSION")
        }));
        host
    }

    pub async fn wait_until_ready(&self) -> Result<()> {
        if self
            .ready_protocol
            .lock()
            .expect("ready protocol lock")
            .is_none()
        {
            tokio::time::timeout(std::time::Duration::from_secs(30), self.ready.notified())
                .await
                .context("Timed out waiting for Fluorine protocol handshake")?;
        }
        let version = self
            .ready_protocol
            .lock()
            .expect("ready protocol lock")
            .unwrap_or_default();
        if version != HOST_PROTOCOL_VERSION {
            bail!(
                "Unsupported host protocol version {version}; CLF3 requires {HOST_PROTOCOL_VERSION}"
            );
        }
        Ok(())
    }

    pub fn emit_install_completed<T: serde::Serialize>(
        &self,
        stats: &T,
        game_path: &std::path::Path,
    ) {
        self.writer.emit_value(&json!({
            "type": "install_completed",
            "stats": stats,
            "game_path": game_path.to_string_lossy()
        }));
    }

    pub fn emit_install_failed<T: serde::Serialize>(&self, stats: &T, message: &str) {
        self.writer.emit_value(&json!({
            "type": "install_failed",
            "message": message,
            "stats": stats
        }));
    }

    fn read_commands(&self) {
        for line in io::stdin().lock().lines() {
            let Ok(line) = line else {
                self.cancellation.cancel();
                break;
            };
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<HostCommand>(&line) {
                Ok(HostCommand::HelloAck { protocol_version }) => {
                    *self.ready_protocol.lock().expect("ready protocol lock") =
                        Some(protocol_version);
                    self.ready.notify_one();
                }
                Ok(HostCommand::DownloadAuthorizationResult { request_id, urls }) => {
                    self.complete(&request_id, Ok(HostResolution::Urls(urls)));
                }
                Ok(HostCommand::ManualDownloadResult { request_id, path }) => {
                    self.complete(
                        &request_id,
                        Ok(HostResolution::LocalPath(PathBuf::from(path))),
                    );
                }
                Ok(HostCommand::AuthorizationFailed { request_id, error }) => {
                    self.complete(&request_id, Err(error));
                }
                Ok(HostCommand::Cancel) => self.cancellation.cancel(),
                Err(error) => self.writer.emit_value(&json!({
                    "type": "protocol_warning",
                    "message": format!("Ignoring malformed host command: {error}")
                })),
            }
        }
    }

    fn complete(&self, request_id: &str, result: Result<HostResolution, String>) {
        if let Some(sender) = self
            .pending
            .lock()
            .expect("pending request lock")
            .remove(request_id)
        {
            let _ = sender.send(result);
        } else {
            self.writer.emit_value(&json!({
                "type": "protocol_warning",
                "message": format!("Ignoring response for unknown request {request_id}")
            }));
        }
    }

    async fn wait_for_resolution(
        &self,
        request_id: &str,
        receiver: oneshot::Receiver<Result<HostResolution, String>>,
    ) -> Result<HostResolution> {
        tokio::select! {
            _ = self.cancellation.cancelled() => {
                self.pending.lock().expect("pending request lock").remove(request_id);
                bail!("Installation cancelled by host")
            }
            response = receiver => {
                response.context("Host disconnected while authorization was pending")?
                    .map_err(anyhow::Error::msg)
            }
        }
    }
}

#[async_trait]
impl HostedDownloadProvider for StdioHost {
    async fn resolve_nexus(&self, request: HostedNexusRequest) -> Result<Vec<String>> {
        let request_id = Uuid::new_v4().to_string();
        let (sender, receiver) = oneshot::channel();
        self.pending
            .lock()
            .expect("pending request lock")
            .insert(request_id.clone(), sender);
        self.writer.emit_value(&json!({
            "type": "download_authorization_required",
            "request_id": request_id,
            "archive_name": request.archive_name,
            "expected_size": request.expected_size,
            "domain": request.domain,
            "mod_id": request.mod_id,
            "file_id": request.file_id
        }));
        match self.wait_for_resolution(&request_id, receiver).await? {
            HostResolution::Urls(urls) if !urls.is_empty() => Ok(urls),
            HostResolution::Urls(_) => bail!("Host returned no Nexus download URLs"),
            HostResolution::LocalPath(_) => bail!("Host returned a file for a Nexus URL request"),
        }
    }

    async fn resolve_manual(&self, request: HostedManualRequest) -> Result<PathBuf> {
        let request_id = Uuid::new_v4().to_string();
        let (sender, receiver) = oneshot::channel();
        self.pending
            .lock()
            .expect("pending request lock")
            .insert(request_id.clone(), sender);
        self.writer.emit_value(&json!({
            "type": "manual_download_required",
            "request_id": request_id,
            "archive_name": request.archive_name,
            "expected_size": request.expected_size,
            "expected_hash": request.expected_hash,
            "url": request.url,
            "prompt": request.prompt
        }));
        match self.wait_for_resolution(&request_id, receiver).await? {
            HostResolution::LocalPath(path) => Ok(path),
            HostResolution::Urls(_) => bail!("Host returned URLs for a manual file request"),
        }
    }

    fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum HostCommand {
    HelloAck {
        protocol_version: u32,
    },
    DownloadAuthorizationResult {
        request_id: String,
        urls: Vec<String>,
    },
    ManualDownloadResult {
        request_id: String,
        path: String,
    },
    AuthorizationFailed {
        request_id: String,
        error: String,
    },
    Cancel,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_commands_are_versioned_and_typed() {
        let ack: HostCommand =
            serde_json::from_str(r#"{"type":"hello_ack","protocol_version":1}"#).unwrap();
        assert!(matches!(
            ack,
            HostCommand::HelloAck {
                protocol_version: HOST_PROTOCOL_VERSION
            }
        ));
    }

    #[test]
    fn host_command_rejects_untyped_input() {
        assert!(serde_json::from_str::<HostCommand>(r#"{"urls":[]}"#).is_err());
    }
}
