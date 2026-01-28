//! HTTP client with stall detection and progress tracking

use anyhow::{bail, Context, Result};
use futures::StreamExt;
use indicatif::ProgressBar;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::watch;
use tracing::{debug, warn};

/// Stall detection: timeout if no progress for this duration
const STALL_TIMEOUT: Duration = Duration::from_secs(30);

/// Progress check interval
const PROGRESS_CHECK_INTERVAL: Duration = Duration::from_secs(5);

/// Global HTTP client
pub struct HttpClient {
    client: reqwest::Client,
}

impl HttpClient {
    pub fn new() -> Result<Self> {
        let client = reqwest::Client::builder()
            .user_agent(concat!("clf3/", env!("CARGO_PKG_VERSION")))
            .timeout(Duration::from_secs(300)) // 5 min timeout
            .connect_timeout(Duration::from_secs(30))
            .build()
            .context("Failed to create HTTP client")?;

        Ok(Self { client })
    }

    /// Get the underlying reqwest client
    pub fn inner(&self) -> &reqwest::Client {
        &self.client
    }
}

impl Default for HttpClient {
    fn default() -> Self {
        Self::new().expect("Failed to create HTTP client")
    }
}

/// Shared progress state for stall detection
struct DownloadProgress {
    bytes_downloaded: AtomicU64,
    last_progress_time: std::sync::Mutex<Instant>,
}

impl DownloadProgress {
    fn new() -> Self {
        Self {
            bytes_downloaded: AtomicU64::new(0),
            last_progress_time: std::sync::Mutex::new(Instant::now()),
        }
    }

    fn add_bytes(&self, count: u64) {
        self.bytes_downloaded.fetch_add(count, Ordering::Relaxed);
        *self.last_progress_time.lock().unwrap() = Instant::now();
    }

    fn total_bytes(&self) -> u64 {
        self.bytes_downloaded.load(Ordering::Relaxed)
    }

    fn time_since_progress(&self) -> Duration {
        self.last_progress_time.lock().unwrap().elapsed()
    }
}

/// Download a file with stall detection and optional progress bar
pub async fn download_file(
    client: &HttpClient,
    url: &str,
    output_path: &Path,
    expected_size: Option<u64>,
) -> Result<u64> {
    download_file_with_progress(client, url, output_path, expected_size, None).await
}

/// Download a file with stall detection and progress bar updates
pub async fn download_file_with_progress(
    client: &HttpClient,
    url: &str,
    output_path: &Path,
    expected_size: Option<u64>,
    progress_bar: Option<&ProgressBar>,
) -> Result<u64> {
    // Start the request
    let response = client
        .inner()
        .get(url)
        .send()
        .await
        .with_context(|| format!("Connection failed: {}", truncate_url(url)))?;

    // Check for HTTP errors with detailed message
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        bail!("HTTP {} - {}", status.as_u16(), truncate_error(&body));
    }

    let content_length = response.content_length();

    // Create output directory if needed
    if let Some(parent) = output_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    // Open output file
    let mut file = File::create(output_path)
        .await
        .with_context(|| format!("Failed to create {}", output_path.display()))?;

    // Setup progress tracking
    let progress = Arc::new(DownloadProgress::new());
    let progress_clone = progress.clone();

    // Shutdown signal for stall detector
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

    // Spawn stall detector task
    let stall_detector = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(PROGRESS_CHECK_INTERVAL) => {
                    let stall_time = progress_clone.time_since_progress();
                    if stall_time >= STALL_TIMEOUT {
                        warn!("Download stalled - no progress for {:?}", stall_time);
                        return Err(anyhow::anyhow!(
                            "Stalled: no data for {}s",
                            STALL_TIMEOUT.as_secs()
                        ));
                    }

                    // Log progress periodically
                    let bytes = progress_clone.total_bytes();
                    if let Some(total) = content_length {
                        let percent = (bytes as f64 / total as f64) * 100.0;
                        debug!("Progress: {:.1}% ({} / {} bytes)", percent, bytes, total);
                    } else {
                        debug!("Downloaded: {} bytes", bytes);
                    }
                }
                _ = shutdown_rx.changed() => {
                    return Ok(());
                }
            }
        }
    });

    // Download with streaming
    let mut stream = response.bytes_stream();
    let download_result: Result<u64> = async {
        while let Some(chunk_result) = stream.next().await {
            let chunk = chunk_result.context("Failed to read chunk")?;
            file.write_all(&chunk).await.context("Failed to write chunk")?;
            let len = chunk.len() as u64;
            progress.add_bytes(len);

            // Update progress bar if provided
            if let Some(pb) = progress_bar {
                pb.inc(len);
                pb.tick(); // Force update for speed calculation
            }
        }
        file.flush().await.context("Failed to flush file")?;
        Ok(progress.total_bytes())
    }
    .await;

    // Stop stall detector
    let _ = shutdown_tx.send(true);

    // Check stall detector result
    match stall_detector.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return Err(e),
        Err(e) => warn!("Stall detector task failed: {}", e),
    }

    let total_bytes = download_result?;

    // Verify size if expected
    if let Some(expected) = expected_size {
        if total_bytes != expected {
            bail!(
                "Size mismatch: expected {} bytes, got {}",
                expected,
                total_bytes
            );
        }
    }

    Ok(total_bytes)
}

/// Truncate URL for error messages
fn truncate_url(url: &str) -> String {
    if url.len() > 80 {
        format!("{}...", &url[..77])
    } else {
        url.to_string()
    }
}

/// Truncate error body for display
fn truncate_error(body: &str) -> String {
    let trimmed = body.trim();
    if trimmed.len() > 100 {
        format!("{}...", &trimmed[..97])
    } else if trimmed.is_empty() {
        "No details".to_string()
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http_client_creation() {
        let client = HttpClient::new();
        assert!(client.is_ok());
    }
}
