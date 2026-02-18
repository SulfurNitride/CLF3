//! NXM protocol handler for Nexus Mods downloads
//!
//! Registers as a handler for nxm:// links and uses a Unix domain socket
//! to receive download requests from browser clicks.

use anyhow::{bail, Context, Result};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::io::AsyncBufReadExt;
use tokio::sync::mpsc;

/// Well-known socket path for NXM IPC
fn socket_path() -> PathBuf {
    if let Ok(runtime_dir) = std::env::var("XDG_RUNTIME_DIR") {
        PathBuf::from(runtime_dir).join("clf3-nxm.sock")
    } else {
        PathBuf::from("/tmp/clf3-nxm.sock")
    }
}

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
        let url = url
            .strip_prefix("nxm://")
            .context("URL must start with nxm://")?;

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

        let key = params
            .get("key")
            .context("Missing 'key' param")?
            .to_string();
        let expires: u64 = params
            .get("expires")
            .context("Missing 'expires' param")?
            .parse()?;

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

/// Start the NXM Unix domain socket listener
///
/// Listens at the well-known socket path. Each connection reads one line
/// (an nxm:// URL), parses it, and sends the result to the returned channel.
pub async fn start_listener() -> Result<(mpsc::UnboundedReceiver<NxmLink>, PathBuf)> {
    let (tx, rx) = mpsc::unbounded_channel();
    let path = socket_path();

    // Remove stale socket file before binding
    if path.exists() {
        std::fs::remove_file(&path)
            .with_context(|| format!("Failed to remove stale socket: {}", path.display()))?;
    }

    let listener = tokio::net::UnixListener::bind(&path)
        .with_context(|| format!("Failed to bind Unix socket: {}", path.display()))?;

    println!("NXM handler listening on {}", path.display());

    // Spawn accept loop in background
    tokio::spawn(async move {
        loop {
            let (stream, _addr) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    eprintln!("NXM socket accept error: {}", e);
                    continue;
                }
            };

            let mut reader = tokio::io::BufReader::new(stream);
            let mut line = String::new();
            if let Err(e) = reader.read_line(&mut line).await {
                eprintln!("NXM socket read error: {}", e);
                continue;
            }

            let url = line.trim();
            if url.is_empty() {
                continue;
            }

            match NxmLink::parse(url) {
                Ok(link) => {
                    println!("Received NXM link: {}:{}", link.mod_id, link.file_id);
                    if tx.send(link).is_err() {
                        // Receiver dropped, stop accepting
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("Invalid NXM link received: {}", e);
                }
            }
        }
    });

    Ok((rx, path))
}

/// Send an NXM link to a running listener (blocking, for fire-and-forget subprocess use)
///
/// Logs errors to `$XDG_RUNTIME_DIR/clf3-nxm.log` since this runs as a desktop-launched
/// subprocess where stderr is invisible.
pub fn send_to_socket(nxm_url: &str) -> Result<()> {
    use std::io::Write;
    use std::os::unix::net::UnixStream;

    let path = socket_path();
    let result = (|| -> Result<()> {
        let mut stream = UnixStream::connect(&path)
            .with_context(|| format!("Failed to connect to NXM socket at {}", path.display()))?;
        writeln!(stream, "{}", nxm_url).context("Failed to write NXM URL to socket")?;
        Ok(())
    })();

    if let Err(ref e) = result {
        // Log to a file next to the socket so the user can debug
        let log_path = path.with_extension("log");
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
        {
            let _ = writeln!(f, "[nxm-handle] Error sending '{}': {:#}", nxm_url, e);
        }
    }

    result
}

/// Register as the system handler for nxm:// protocol (Linux only)
pub fn register_handler(exe_path: &str) -> Result<()> {
    // Quote the exe path in case it contains spaces (desktop entry spec requires this)
    let desktop_entry = format!(
        r#"[Desktop Entry]
Type=Application
Name=CLF3 NXM Handler
Exec="{}" nxm-handle %u
MimeType=x-scheme-handler/nxm;
NoDisplay=true
"#,
        exe_path
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
