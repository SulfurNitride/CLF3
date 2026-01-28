//! Download handlers for various sources
//!
//! Supports Nexus Mods, Wabbajack CDN, Google Drive, MediaFire, HTTP direct,
//! and manual downloads (Mega, etc.)

// Module not yet integrated into main installation pipeline
#![allow(dead_code)]
#![allow(unused_imports)]

mod google_drive;
mod http;
mod mediafire;
mod nexus;
mod wabbajack_cdn;

pub use google_drive::GoogleDriveDownloader;
pub use http::{download_file, download_file_with_progress, HttpClient};
pub use mediafire::MediaFireDownloader;
pub use nexus::{NexusDownloader, NexusRateLimits};
pub use wabbajack_cdn::WabbajackCdnDownloader;

use anyhow::{Context, Result};
use std::future::Future;
use std::path::Path;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, warn};

/// Default retry configuration
pub const MAX_RETRIES: u32 = 3;
pub const RETRY_DELAY: Duration = Duration::from_secs(5);

/// Download result with metadata
#[derive(Debug)]
pub struct DownloadResult {
    pub path: std::path::PathBuf,
    pub size: u64,
    pub retries_used: u32,
}

/// Wrapper that adds retry logic to any async download function
pub async fn with_retry<F, Fut, T>(operation_name: &str, max_retries: u32, mut f: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut last_error = None;

    for attempt in 1..=max_retries {
        match f().await {
            Ok(result) => {
                if attempt > 1 {
                    info!("{} succeeded on attempt {}/{}", operation_name, attempt, max_retries);
                }
                return Ok(result);
            }
            Err(e) => {
                warn!(
                    "{} failed (attempt {}/{}): {}",
                    operation_name, attempt, max_retries, e
                );
                last_error = Some(e);

                if attempt < max_retries {
                    info!("Retrying in {} seconds...", RETRY_DELAY.as_secs());
                    sleep(RETRY_DELAY).await;
                }
            }
        }
    }

    Err(last_error.unwrap()).with_context(|| {
        format!(
            "{} failed after {} attempts",
            operation_name, max_retries
        )
    })
}

/// Download source types (matches Wabbajack modlist format)
#[derive(Debug, Clone)]
pub enum DownloadSource {
    Nexus {
        game_name: String,
        mod_id: u64,
        file_id: u64,
    },
    Http {
        url: String,
    },
    WabbajackCdn {
        url: String,
    },
    GoogleDrive {
        id: String,
    },
    MediaFire {
        url: String,
    },
    Manual {
        url: String,
        prompt: String,
    },
    Mega {
        url: String,
    },
    GameFile {
        game: String,
        path: String,
    },
}

impl DownloadSource {
    /// Get a human-readable description
    pub fn description(&self) -> String {
        match self {
            DownloadSource::Nexus { game_name, mod_id, .. } => {
                format!("Nexus: {}/mods/{}", game_name, mod_id)
            }
            DownloadSource::Http { url } => format!("HTTP: {}", truncate_url(url)),
            DownloadSource::WabbajackCdn { url } => format!("CDN: {}", truncate_url(url)),
            DownloadSource::GoogleDrive { id } => format!("Google Drive: {}", id),
            DownloadSource::MediaFire { url } => format!("MediaFire: {}", truncate_url(url)),
            DownloadSource::Manual { url, .. } => format!("Manual: {}", truncate_url(url)),
            DownloadSource::Mega { url } => format!("Mega: {}", truncate_url(url)),
            DownloadSource::GameFile { game, path } => format!("Game File: {}/{}", game, path),
        }
    }

    /// Check if this requires manual user action
    pub fn requires_manual(&self) -> bool {
        matches!(self, DownloadSource::Manual { .. } | DownloadSource::Mega { .. })
    }
}

/// Truncate URL for display
fn truncate_url(url: &str) -> String {
    if url.len() > 60 {
        format!("{}...", &url[..57])
    } else {
        url.to_string()
    }
}

/// Manual download instruction for user
#[derive(Debug, Clone)]
pub struct ManualDownload {
    pub url: String,
    pub filename: String,
    pub expected_size: u64,
    pub prompt: Option<String>,
}

impl ManualDownload {
    /// Format instructions for the user
    pub fn instructions(&self) -> String {
        let mut msg = format!(
            "Please download this file manually:\n\n\
             URL: {}\n\
             Save as: {}\n\
             Expected size: {} bytes",
            self.url, self.filename, self.expected_size
        );

        if let Some(prompt) = &self.prompt {
            msg.push_str(&format!("\n\nNote: {}", prompt));
        }

        msg.push_str("\n\nPlace the file in your downloads folder and press Enter to continue.");
        msg
    }
}

/// Check if a downloaded file exists and has the expected size
pub fn verify_download(path: &Path, expected_size: u64) -> bool {
    match std::fs::metadata(path) {
        Ok(meta) => meta.len() == expected_size,
        Err(_) => false,
    }
}
