//! NXM protocol handler for Nexus Mods downloads
//!
//! Registers as a handler for nxm:// links and runs a local server
//! to receive download requests from browser clicks.

use anyhow::{Context, Result, bail};
use axum::{
    Router,
    extract::State,
    response::{Html, IntoResponse},
    routing::post,
    Json,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::sync::mpsc;


/// Parsed NXM link with auth credentials
#[derive(Debug, Clone)]
pub struct NxmLink {
    pub game_domain: String,
    pub mod_id: u64,
    pub file_id: u64,
    pub key: String,
    pub expires: u64,
}

impl NxmLink {
    /// Parse an nxm:// URL into its components
    /// Format: nxm://game/mods/mod_id/files/file_id?key=xxx&expires=yyy&user_id=zzz
    pub fn parse(url: &str) -> Result<Self> {
        let url = url.strip_prefix("nxm://").context("URL must start with nxm://")?;

        let (path, query) = url.split_once('?').context("URL must have query params")?;

        let parts: Vec<&str> = path.split('/').collect();
        if parts.len() != 5 || parts[1] != "mods" || parts[3] != "files" {
            bail!("Invalid NXM path format: {}", path);
        }

        let game_domain = parts[0].to_string();
        let mod_id: u64 = parts[2].parse().context("Invalid mod_id")?;
        let file_id: u64 = parts[4].parse().context("Invalid file_id")?;

        // Parse query params
        let params: HashMap<&str, &str> = query
            .split('&')
            .filter_map(|pair| pair.split_once('='))
            .collect();

        let key = params.get("key").context("Missing 'key' param")?.to_string();
        let expires: u64 = params.get("expires").context("Missing 'expires' param")?.parse()?;

        Ok(Self {
            game_domain,
            mod_id,
            file_id,
            key,
            expires,
        })
    }

    /// Build the Nexus API URL with auth params
    pub fn api_url(&self) -> String {
        format!(
            "https://api.nexusmods.com/v1/games/{}/mods/{}/files/{}/download_link.json?key={}&expires={}",
            self.game_domain, self.mod_id, self.file_id, self.key, self.expires
        )
    }

    /// Create a unique key for matching against pending downloads
    pub fn lookup_key(&self) -> String {
        format!("{}:{}:{}", self.game_domain, self.mod_id, self.file_id)
    }
}

/// Message sent to the NXM server
#[derive(Debug, Serialize, Deserialize)]
pub struct NxmMessage {
    pub url: String,
}

/// Shared state for the NXM handler
pub struct NxmState {
    /// Channel to send received NXM links
    pub tx: mpsc::UnboundedSender<NxmLink>,
}

/// Start the NXM handler server
pub async fn start_server(port: u16) -> Result<(mpsc::UnboundedReceiver<NxmLink>, Arc<NxmState>)> {
    let (tx, rx) = mpsc::unbounded_channel();

    let state = Arc::new(NxmState { tx });

    let app = Router::new()
        .route("/", post(handle_nxm))
        .with_state(state.clone());

    let addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("Failed to bind to {}", addr))?;

    println!("NXM handler listening on http://{}", addr);

    // Spawn server in background
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            eprintln!("NXM server error: {}", e);
        }
    });

    Ok((rx, state))
}

/// Handle incoming NXM link
async fn handle_nxm(
    State(state): State<Arc<NxmState>>,
    Json(msg): Json<NxmMessage>,
) -> impl IntoResponse {
    match NxmLink::parse(&msg.url) {
        Ok(link) => {
            println!("Received NXM link: {}:{}", link.mod_id, link.file_id);
            if let Err(e) = state.tx.send(link) {
                return (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    Html(format!("<h1>Error: {}</h1>", e)),
                );
            }
            (
                axum::http::StatusCode::OK,
                Html("<h1>Download queued!</h1>".to_string()),
            )
        }
        Err(e) => (
            axum::http::StatusCode::BAD_REQUEST,
            Html(format!("<h1>Invalid NXM link: {}</h1>", e)),
        ),
    }
}

/// Send an NXM link to a running handler server
pub async fn send_to_server(port: u16, nxm_url: &str) -> Result<()> {
    let client = reqwest::Client::new();
    let addr = format!("http://127.0.0.1:{}", port);

    client
        .post(&addr)
        .json(&NxmMessage { url: nxm_url.to_string() })
        .send()
        .await
        .context("Failed to send NXM link to server")?
        .error_for_status()
        .context("Server returned error")?;

    Ok(())
}

/// Register as the system handler for nxm:// protocol (Linux only)
pub fn register_handler(exe_path: &str, port: u16) -> Result<()> {
    let desktop_entry = format!(
        r#"[Desktop Entry]
Type=Application
Name=CLF3 NXM Handler
Exec={} nxm-handle --port {} %u
MimeType=x-scheme-handler/nxm;
NoDisplay=true
"#,
        exe_path, port
    );

    // Write desktop file
    let home = std::env::var("HOME").context("HOME not set")?;
    let desktop_path = format!("{}/.local/share/applications/clf3-nxm.desktop", home);
    std::fs::write(&desktop_path, &desktop_entry)
        .with_context(|| format!("Failed to write {}", desktop_path))?;

    // Register as handler
    let status = std::process::Command::new("xdg-mime")
        .args(["default", "clf3-nxm.desktop", "x-scheme-handler/nxm"])
        .status()
        .context("Failed to run xdg-mime")?;

    if !status.success() {
        bail!("xdg-mime failed with status: {}", status);
    }

    println!("Registered as nxm:// handler");
    println!("Desktop file: {}", desktop_path);

    Ok(())
}

/// Generate the Nexus website URL for a mod file (to open in browser)
pub fn nexus_mod_url(game_domain: &str, mod_id: u64, file_id: u64) -> String {
    format!(
        "https://www.nexusmods.com/{}/mods/{}?tab=files&file_id={}&nmm=1",
        game_domain.to_lowercase(),
        mod_id,
        file_id
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_nxm_link() {
        let url = "nxm://skyrimspecialedition/mods/12345/files/67890?key=abc123&expires=9999999999&user_id=42";
        let link = NxmLink::parse(url).unwrap();

        assert_eq!(link.game_domain, "skyrimspecialedition");
        assert_eq!(link.mod_id, 12345);
        assert_eq!(link.file_id, 67890);
        assert_eq!(link.key, "abc123");
        assert_eq!(link.expires, 9999999999);
    }

    #[test]
    fn test_lookup_key() {
        let url = "nxm://skyrimspecialedition/mods/12345/files/67890?key=abc&expires=999";
        let link = NxmLink::parse(url).unwrap();
        assert_eq!(link.lookup_key(), "skyrimspecialedition:12345:67890");
    }
}
