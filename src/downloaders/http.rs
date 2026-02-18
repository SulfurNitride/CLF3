//! HTTP client with stall detection and progress tracking

use anyhow::{bail, Context, Result};
use futures::StreamExt;
use indicatif::ProgressBar;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::watch;
use tracing::{debug, warn};

/// Stall detection: timeout if no progress for this duration.
///
/// Nexus and CDN downloads can legitimately pause for short windows on
/// congested/slow links, especially for multi-GB archives. Keep this lenient
/// enough to avoid false "stalled" failures.
const STALL_TIMEOUT: Duration = Duration::from_secs(180);

/// Progress check interval
const PROGRESS_CHECK_INTERVAL: Duration = Duration::from_secs(5);

/// How often to invoke the progress callback (in milliseconds)
const CALLBACK_INTERVAL_MS: u128 = 250;

/// Maximum retries for resumable download recovery.
const MAX_RESUME_RETRIES: u32 = 8;

/// Base delay between resumable retries.
const RESUME_RETRY_BASE_DELAY: Duration = Duration::from_secs(2);

/// Global HTTP client
pub struct HttpClient {
    client: reqwest::Client,
}

/// Connection timeout: time to establish TCP connection
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// Read timeout: maximum time to wait for data between chunks
const READ_TIMEOUT: Duration = Duration::from_secs(60);

/// Overall request timeout for a single HTTP request.
///
/// Large mod archives can take well over an hour on slower connections; a
/// short global timeout causes repeated failures even while data is still
/// flowing. Keep this high and rely on stall detection for true dead links.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(4 * 60 * 60);

impl HttpClient {
    pub fn new() -> Result<Self> {
        let client = reqwest::Client::builder()
            .user_agent(concat!("clf3/", env!("CARGO_PKG_VERSION")))
            .connect_timeout(CONNECT_TIMEOUT)
            .read_timeout(READ_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
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

/// Shared progress state for stall detection and speed calculation
struct DownloadProgress {
    bytes_downloaded: AtomicU64,
    last_progress_time: std::sync::Mutex<Instant>,
    start_time: Instant,
}

impl DownloadProgress {
    fn new(initial_bytes: u64) -> Self {
        Self {
            bytes_downloaded: AtomicU64::new(initial_bytes),
            last_progress_time: std::sync::Mutex::new(Instant::now()),
            start_time: Instant::now(),
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

    /// Calculate average bytes per second since start
    fn bytes_per_second(&self) -> f64 {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            self.total_bytes() as f64 / elapsed
        } else {
            0.0
        }
    }
}

/// Progress callback type for GUI updates
pub type ProgressCallback = Box<dyn Fn(u64, u64, f64) + Send + Sync>;

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
    download_file_with_callback(client, url, output_path, expected_size, progress_bar, None).await
}

/// Download a file with stall detection, progress bar updates, and progress callback
///
/// The callback receives (downloaded_bytes, total_bytes, bytes_per_second).
pub async fn download_file_with_callback(
    client: &HttpClient,
    url: &str,
    output_path: &Path,
    expected_size: Option<u64>,
    progress_bar: Option<&ProgressBar>,
    progress_callback: Option<&ProgressCallback>,
) -> Result<u64> {
    if let Some(parent) = output_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let mut attempts = 0u32;
    let mut offset = match tokio::fs::metadata(output_path).await {
        Ok(meta) => meta.len(),
        Err(_) => 0,
    };

    if let Some(expected) = expected_size {
        if offset > expected {
            warn!(
                "Partial file larger than expected ({} > {}), restarting: {}",
                offset,
                expected,
                output_path.display()
            );
            let _ = tokio::fs::remove_file(output_path).await;
            offset = 0;
        } else if offset == expected {
            if let Some(pb) = progress_bar {
                pb.set_position(expected);
                pb.tick();
            }
            if let Some(callback) = progress_callback {
                callback(expected, expected, 0.0);
            }
            return Ok(expected);
        }
    }

    loop {
        let mut request = client.inner().get(url);
        if offset > 0 {
            request = request.header(reqwest::header::RANGE, format!("bytes={}-", offset));
        }

        let response = match request.send().await {
            Ok(resp) => resp,
            Err(e) => {
                if attempts < MAX_RESUME_RETRIES {
                    attempts += 1;
                    let delay = RESUME_RETRY_BASE_DELAY
                        .as_secs()
                        .saturating_mul(1u64 << (attempts - 1).min(4))
                        .min(60);
                    warn!(
                        "Request failed for {} (attempt {}/{}), retrying in {}s: {}",
                        truncate_url(url),
                        attempts,
                        MAX_RESUME_RETRIES,
                        delay,
                        e
                    );
                    tokio::time::sleep(Duration::from_secs(delay)).await;
                    continue;
                }
                return Err(e).with_context(|| format!("Connection failed: {}", truncate_url(url)));
            }
        };

        let status = response.status();
        let mut append_mode = offset > 0 && status == reqwest::StatusCode::PARTIAL_CONTENT;

        if offset > 0 && status == reqwest::StatusCode::RANGE_NOT_SATISFIABLE {
            if let Some(expected) = expected_size {
                if offset == expected {
                    if let Some(pb) = progress_bar {
                        pb.set_position(expected);
                        pb.tick();
                    }
                    if let Some(callback) = progress_callback {
                        callback(expected, expected, 0.0);
                    }
                    return Ok(expected);
                }
            }
            warn!(
                "Server rejected resume range for {}, restarting from 0",
                truncate_url(url)
            );
            let _ = tokio::fs::remove_file(output_path).await;
            offset = 0;
            append_mode = false;
            if attempts < MAX_RESUME_RETRIES {
                attempts += 1;
                tokio::time::sleep(RESUME_RETRY_BASE_DELAY).await;
                continue;
            }
            bail!("HTTP 416 - resume range rejected");
        }

        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            bail!("HTTP {} - {}", status.as_u16(), truncate_error(&body));
        }

        if offset > 0 && status == reqwest::StatusCode::OK {
            // Server ignored Range header; restart from scratch.
            warn!(
                "Server ignored resume request for {}, restarting from 0",
                truncate_url(url)
            );
            offset = 0;
            append_mode = false;
        }

        let total_size = expected_size
            .or_else(|| response.content_length().map(|len| len + offset))
            .unwrap_or(0);

        let mut file = if append_mode {
            OpenOptions::new()
                .append(true)
                .open(output_path)
                .await
                .with_context(|| format!("Failed to append {}", output_path.display()))?
        } else {
            File::create(output_path)
                .await
                .with_context(|| format!("Failed to create {}", output_path.display()))?
        };

        let progress = Arc::new(DownloadProgress::new(offset));
        let progress_clone = progress.clone();
        let content_length = if total_size > 0 { Some(total_size) } else { None };
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

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

        if let Some(pb) = progress_bar {
            pb.set_position(offset);
            pb.tick();
        }

        let mut last_callback_time = Instant::now();
        let mut stream = response.bytes_stream();
        let download_result: Result<u64> = async {
            while let Some(chunk_result) = stream.next().await {
                let chunk = chunk_result.context("Failed to read chunk")?;
                file.write_all(&chunk)
                    .await
                    .context("Failed to write chunk")?;
                let len = chunk.len() as u64;
                progress.add_bytes(len);

                if let Some(pb) = progress_bar {
                    pb.inc(len);
                    pb.tick();
                }

                if let Some(callback) = progress_callback {
                    let now = Instant::now();
                    if now.duration_since(last_callback_time).as_millis() >= CALLBACK_INTERVAL_MS {
                        let downloaded = progress.total_bytes();
                        let speed = progress.bytes_per_second();
                        callback(downloaded, total_size, speed);
                        last_callback_time = now;
                    }
                }
            }

            if let Some(callback) = progress_callback {
                let downloaded = progress.total_bytes();
                let speed = progress.bytes_per_second();
                callback(downloaded, total_size, speed);
            }

            file.flush().await.context("Failed to flush file")?;
            Ok(progress.total_bytes())
        }
        .await;

        let _ = shutdown_tx.send(true);
        let detector_result = stall_detector.await;
        let total_bytes = match detector_result {
            Ok(Ok(())) => download_result?,
            Ok(Err(e)) => return Err(e),
            Err(e) => {
                warn!("Stall detector task failed: {}", e);
                download_result?
            }
        };

        if let Some(expected) = expected_size {
            if total_bytes == expected {
                return Ok(total_bytes);
            }
            if total_bytes > expected {
                bail!(
                    "Size mismatch: expected {} bytes, got {}",
                    expected,
                    total_bytes
                );
            }
        } else {
            return Ok(total_bytes);
        }

        offset = total_bytes;
        if attempts < MAX_RESUME_RETRIES {
            attempts += 1;
            let delay = RESUME_RETRY_BASE_DELAY
                .as_secs()
                .saturating_mul(1u64 << (attempts - 1).min(4))
                .min(60);
            tokio::time::sleep(Duration::from_secs(delay)).await;
            continue;
        }

        bail!(
            "Download incomplete after {} retries: got {} bytes",
            MAX_RESUME_RETRIES,
            total_bytes
        );
    }
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
