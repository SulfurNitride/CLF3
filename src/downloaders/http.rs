//! HTTP client with stall detection and progress tracking

use anyhow::{bail, Context, Result};
use futures::StreamExt;
use std::io::ErrorKind;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tracing::{debug, warn};

/// Stall detection: timeout if no progress for this duration.
///
/// Nexus and CDN downloads can legitimately pause for short windows on
/// congested/slow links, especially for multi-GB archives. Keep this lenient
/// enough to avoid false "stalled" failures.
#[cfg(not(test))]
const STALL_TIMEOUT: Duration = Duration::from_secs(180);
#[cfg(test)]
const STALL_TIMEOUT: Duration = Duration::from_millis(150);

/// Progress check interval
#[cfg(not(test))]
const PROGRESS_CHECK_INTERVAL: Duration = Duration::from_secs(5);
#[cfg(test)]
const PROGRESS_CHECK_INTERVAL: Duration = Duration::from_millis(25);

/// How often to invoke the progress callback (in milliseconds)
const CALLBACK_INTERVAL_MS: u128 = 250;

/// Maximum retries for resumable download recovery.
const MAX_RESUME_RETRIES: u32 = 8;

/// Base delay between resumable retries.
#[cfg(not(test))]
const RESUME_RETRY_BASE_DELAY: Duration = Duration::from_secs(2);
#[cfg(test)]
const RESUME_RETRY_BASE_DELAY: Duration = Duration::from_millis(10);

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
    initial_bytes: u64,
    last_progress_time: std::sync::Mutex<Instant>,
    start_time: Instant,
}

impl DownloadProgress {
    fn new(initial_bytes: u64) -> Self {
        Self {
            bytes_downloaded: AtomicU64::new(initial_bytes),
            initial_bytes,
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
            self.total_bytes()
                .saturating_sub(self.initial_bytes) as f64
                / elapsed
        } else {
            0.0
        }
    }
}

/// Progress callback type for GUI updates
pub type ProgressCallback = Box<dyn Fn(u64, u64, f64) + Send + Sync>;

/// Download a file with stall detection
pub async fn download_file(
    client: &HttpClient,
    url: &str,
    output_path: &Path,
    expected_size: Option<u64>,
) -> Result<u64> {
    download_file_with_callback(client, url, output_path, expected_size, None).await
}

/// Download a file with stall detection and progress callback
pub async fn download_file_with_progress(
    client: &HttpClient,
    url: &str,
    output_path: &Path,
    expected_size: Option<u64>,
) -> Result<u64> {
    download_file_with_callback(client, url, output_path, expected_size, None).await
}

/// Download a file with stall detection and progress callback
///
/// The callback receives (downloaded_bytes, total_bytes, bytes_per_second).
pub async fn download_file_with_callback(
    client: &HttpClient,
    url: &str,
    output_path: &Path,
    expected_size: Option<u64>,
    progress_callback: Option<&ProgressCallback>,
) -> Result<u64> {
    if let Some(parent) = output_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let mut attempts = 0u32;
    let mut offset = match tokio::fs::metadata(output_path).await {
        Ok(meta) if !meta.is_file() => {
            bail!(
                "Download destination is not a regular file: {}",
                output_path.display()
            );
        }
        Ok(meta) => meta.len(),
        Err(error) if error.kind() == ErrorKind::NotFound => 0,
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "Failed to inspect download destination {}",
                    output_path.display()
                )
            });
        }
    };

    if let Some(expected) = expected_size {
        if offset > expected {
            warn!(
                "Partial file larger than expected ({} > {}), restarting: {}",
                offset,
                expected,
                output_path.display()
            );
            tokio::fs::remove_file(output_path).await.with_context(|| {
                format!(
                    "Failed to remove oversized partial download {}",
                    output_path.display()
                )
            })?;
            offset = 0;
        } else if offset == expected {
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
                let kind = reqwest_error_kind(&e);
                if attempts < MAX_RESUME_RETRIES {
                    attempts += 1;
                    let delay = retry_delay(attempts);
                    warn!(
                        "Request to {} failed ({}) (retry {}/{} in {:?})",
                        safe_url_origin(url),
                        kind,
                        attempts,
                        MAX_RESUME_RETRIES,
                        delay
                    );
                    tokio::time::sleep(delay).await;
                    continue;
                }
                bail!(
                    "Connection to {} failed after {} retries ({})",
                    safe_url_origin(url),
                    attempts,
                    kind
                );
            }
        };

        let status = response.status();
        let mut append_mode = false;
        let mut range_total = None;
        let mut expected_response_length = None;

        if status == reqwest::StatusCode::PARTIAL_CONTENT {
            match validate_content_range(&response, offset, expected_size) {
                Ok(range) => {
                    if expected_size.is_none() && range.total.is_none() {
                        bail!(
                            "Cannot verify HTTP range response from {} because its total size is unknown",
                            safe_url_origin(url)
                        );
                    }
                    append_mode = offset > 0;
                    range_total = range.total;
                    expected_response_length = Some(range.end - range.start + 1);
                }
                Err(reason) => {
                    warn!(
                        "Rejected invalid range response from {} ({}); restarting at byte 0",
                        safe_url_origin(url),
                        reason
                    );
                    if attempts >= MAX_RESUME_RETRIES {
                        bail!(
                            "Invalid HTTP range response from {} after {} retries ({})",
                            safe_url_origin(url),
                            attempts,
                            reason
                        );
                    }
                    attempts += 1;
                    offset = 0;
                    tokio::time::sleep(retry_delay(attempts)).await;
                    continue;
                }
            }
        }

        if offset > 0 && status == reqwest::StatusCode::RANGE_NOT_SATISFIABLE {
            if let Some(expected) = expected_size {
                if offset == expected {
                    if let Some(callback) = progress_callback {
                        callback(expected, expected, 0.0);
                    }
                    return Ok(expected);
                }
            }
            warn!(
                "Server rejected resume range for {}, restarting from 0",
                safe_url_origin(url)
            );
            offset = 0;
            if attempts < MAX_RESUME_RETRIES {
                attempts += 1;
                tokio::time::sleep(retry_delay(attempts)).await;
                continue;
            }
            bail!("HTTP 416 - resume range rejected");
        }

        if status != reqwest::StatusCode::OK && status != reqwest::StatusCode::PARTIAL_CONTENT {
            bail!(
                "Unexpected HTTP {} response from {} (response body omitted)",
                status.as_u16(),
                safe_url_origin(url)
            );
        }

        if offset > 0 && status == reqwest::StatusCode::OK {
            // Server ignored Range header; restart from scratch.
            warn!(
                "Server ignored resume request for {}, restarting from 0",
                safe_url_origin(url)
            );
            offset = 0;
        }

        let total_size = expected_size
            .or(range_total)
            .or_else(|| {
                response
                    .content_length()
                    .map(|len| len.saturating_add(offset))
            })
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

        let progress = DownloadProgress::new(offset);

        let mut last_callback_time = Instant::now();
        let mut stream = response.bytes_stream();
        let mut interrupted = None;
        let mut response_bytes = 0u64;
        let mut stall_deadline = tokio::time::Instant::now() + STALL_TIMEOUT;
        let mut progress_log_deadline = tokio::time::Instant::now() + PROGRESS_CHECK_INTERVAL;
        loop {
            tokio::select! {
              _ = tokio::time::sleep_until(stall_deadline) => {
                let stalled_for = progress.time_since_progress();
                warn!("Download stalled without receiving body data for {:?}", stalled_for);
                interrupted = Some(format!("body stalled for {:?}", stalled_for));
                break;
              }
              _ = tokio::time::sleep_until(progress_log_deadline) => {
                let downloaded = progress.total_bytes();
                if total_size > 0 {
                  let percent = (downloaded as f64 / total_size as f64) * 100.0;
                  debug!("Progress: {:.1}% ({} / {} bytes)", percent, downloaded, total_size);
                } else {
                  debug!("Downloaded: {} bytes", downloaded);
                }
                progress_log_deadline = tokio::time::Instant::now() + PROGRESS_CHECK_INTERVAL;
              }
              next = stream.next() => {
                let Some(chunk_result) = next else { break; };
                let chunk = match chunk_result {
                  Ok(chunk) => chunk,
                  Err(error) => {
                    let kind = reqwest_error_kind(&error);
                    warn!("HTTP response body interrupted from {} ({})",
                          safe_url_origin(url), kind);
                    interrupted = Some(format!("response body interrupted ({})", kind));
                    break;
                  }
                };

                let len = chunk.len() as u64;
                if expected_response_length
                  .is_some_and(|expected| response_bytes.saturating_add(len) > expected)
                {
                  bail!("HTTP range response exceeded its declared byte extent");
                }
                let current = progress.total_bytes();
                if let Some(expected) = expected_size {
                  if current.saturating_add(len) > expected {
                    bail!("Size mismatch: expected at most {} bytes, response exceeds it",
                          expected);
                  }
                }
                file.write_all(&chunk).await.context("Failed to write chunk")?;
                response_bytes = response_bytes.saturating_add(len);
                if len > 0 {
                    progress.add_bytes(len);
                    stall_deadline = tokio::time::Instant::now() + STALL_TIMEOUT;
                }

                if let Some(callback) = progress_callback {
                  let now = Instant::now();
                  if now.duration_since(last_callback_time).as_millis() >= CALLBACK_INTERVAL_MS {
                    callback(progress.total_bytes(), total_size, progress.bytes_per_second());
                    last_callback_time = now;
                  }
                }
              }
            }
        }

        if let Some(callback) = progress_callback {
            callback(
                progress.total_bytes(),
                total_size,
                progress.bytes_per_second(),
            );
        }
        file.flush().await.context("Failed to flush file")?;
        drop(file);

        // Stat the file after flushing: the on-disk length is the only safe resume
        // offset after a stream error or stall.
        let total_bytes = tokio::fs::metadata(output_path)
            .await
            .with_context(|| {
                format!(
                    "Failed to inspect partial download {}",
                    output_path.display()
                )
            })?
            .len();
        if let Some(expected_response_length) = expected_response_length {
            if response_bytes < expected_response_length && interrupted.is_none() {
                interrupted = Some("response ended before its declared range extent".to_string());
            }
        }
        if let Some(expected) = expected_size {
            if total_bytes > expected {
                bail!(
                    "Size mismatch: expected {} bytes, got {}",
                    expected,
                    total_bytes
                );
            }
            if total_bytes == expected {
                return Ok(total_bytes);
            }
        } else if interrupted.is_none()
            && range_total.map_or(true, |range_total| total_bytes >= range_total)
        {
            return Ok(total_bytes);
        }

        offset = total_bytes;
        if let Some(reason) = interrupted {
            if attempts >= MAX_RESUME_RETRIES {
                bail!(
                    "Download interrupted after {} retries from {} ({})",
                    attempts,
                    safe_url_origin(url),
                    reason
                );
            }
            attempts += 1;
            let delay = retry_delay(attempts);
            warn!(
                "Resuming {} after interrupted response ({}) at byte {} (retry {}/{}, wait {:?})",
                safe_url_origin(url),
                reason,
                offset,
                attempts,
                MAX_RESUME_RETRIES,
                delay
            );
            tokio::time::sleep(delay).await;
            continue;
        }

        if (total_size == 0 && range_total.is_none()) || total_bytes == total_size {
            return Ok(total_bytes);
        }

        if attempts < MAX_RESUME_RETRIES {
            attempts += 1;
            tokio::time::sleep(retry_delay(attempts)).await;
            continue;
        }

        bail!(
            "Download incomplete after {} retries: got {} bytes",
            MAX_RESUME_RETRIES,
            total_bytes
        );
    }
}

fn retry_delay(attempt: u32) -> Duration {
    RESUME_RETRY_BASE_DELAY.saturating_mul(1u32 << (attempt.saturating_sub(1)).min(4))
}

fn safe_url_origin(url: &str) -> String {
    match reqwest::Url::parse(url) {
        Ok(parsed) => {
            let host = parsed.host_str().unwrap_or("unknown host");
            match parsed.port() {
                Some(port) => format!("{}://{}:{}", parsed.scheme(), host, port),
                None => format!("{}://{}", parsed.scheme(), host),
            }
        }
        Err(_) => "<invalid URL>".to_string(),
    }
}

fn reqwest_error_kind(error: &reqwest::Error) -> &'static str {
    if error.is_timeout() {
        "timeout"
    } else if error.is_connect() {
        "connection"
    } else if let Some(kind) = source_io_error_kind(error) {
        match kind {
            ErrorKind::TimedOut => "I/O timeout",
            ErrorKind::ConnectionReset => "connection reset",
            ErrorKind::ConnectionAborted => "connection aborted",
            ErrorKind::BrokenPipe => "connection closed",
            ErrorKind::UnexpectedEof => "unexpected end of response",
            _ => "I/O failure",
        }
    } else if error.is_body() {
        "response body"
    } else if error.is_decode() {
        "response decode"
    } else if error.is_redirect() {
        "redirect"
    } else {
        "request"
    }
}

fn source_io_error_kind(error: &(dyn std::error::Error + 'static)) -> Option<ErrorKind> {
    let mut source = std::error::Error::source(error);
    while let Some(cause) = source {
        if let Some(io_error) = cause.downcast_ref::<std::io::Error>() {
            return Some(io_error.kind());
        }
        source = std::error::Error::source(cause);
    }
    None
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ContentRange {
    start: u64,
    end: u64,
    total: Option<u64>,
}

fn parse_content_range(value: &str) -> Option<ContentRange> {
    let value = value.trim();
    let (unit, value) = value.split_once(' ')?;
    if !unit.eq_ignore_ascii_case("bytes") {
        return None;
    }
    let (range, total) = value.split_once('/')?;
    let (start, end) = range.split_once('-')?;
    let start = start.parse().ok()?;
    let end = end.parse().ok()?;
    if end < start {
        return None;
    }
    let total = if total == "*" {
        None
    } else {
        Some(total.parse().ok()?)
    };
    if total.is_some_and(|total| total <= end) {
        return None;
    }
    Some(ContentRange { start, end, total })
}

fn validate_content_range(
    response: &reqwest::Response,
    requested_offset: u64,
    expected_size: Option<u64>,
) -> std::result::Result<ContentRange, &'static str> {
    let value = response
        .headers()
        .get(reqwest::header::CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
        .ok_or("missing or malformed Content-Range")?;
    let range = parse_content_range(value).ok_or("invalid Content-Range")?;
    if range.start != requested_offset {
        return Err("range start does not match requested offset");
    }
    let range_length = range
        .end
        .checked_sub(range.start)
        .and_then(|length| length.checked_add(1))
        .ok_or("range length overflow")?;
    if response
        .content_length()
        .is_some_and(|content_length| content_length != range_length)
    {
        return Err("range length does not match Content-Length");
    }
    if expected_size.is_some_and(|expected| range.total.is_some_and(|total| total != expected)) {
        return Err("range total does not match expected file size");
    }
    if expected_size.is_some_and(|expected| range.end >= expected) {
        return Err("range end exceeds expected file size");
    }
    Ok(range)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    async fn bind_server() -> (String, TcpListener) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        (format!("http://{address}/archive"), listener)
    }

    async fn read_request(stream: &mut TcpStream) -> String {
        let mut request = Vec::new();
        let mut buffer = [0u8; 1024];
        loop {
            let count = stream.read(&mut buffer).await.unwrap();
            if count == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..count]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        String::from_utf8_lossy(&request).into_owned()
    }

    async fn write_response(stream: &mut TcpStream, status: &str, headers: &str, body: &[u8]) {
        let response = format!(
            "HTTP/1.1 {status}\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
        stream.write_all(body).await.unwrap();
        stream.shutdown().await.unwrap();
    }

    #[test]
    fn test_http_client_creation() {
        let client = HttpClient::new();
        assert!(client.is_ok());
    }

    #[test]
    fn safe_url_origin_omits_userinfo_paths_and_signed_query() {
        assert_eq!(
            safe_url_origin("https://user:secret@example.test/files/archive?token=signed"),
            "https://example.test"
        );
    }

    #[test]
    fn content_range_parser_rejects_invalid_ranges() {
        assert_eq!(
            parse_content_range("bytes 5-9/10"),
            Some(ContentRange {
                start: 5,
                end: 9,
                total: Some(10)
            })
        );
        assert_eq!(parse_content_range("bytes 9-5/10"), None);
        assert_eq!(parse_content_range("items 0-4/5"), None);
        assert_eq!(parse_content_range("bytes 0-4/4"), None);
        assert_eq!(
            parse_content_range("BYTES 5-9/10"),
            Some(ContentRange {
                start: 5,
                end: 9,
                total: Some(10)
            })
        );
    }

    #[test]
    fn resumed_download_speed_excludes_bytes_from_the_partial_file() {
        let progress = DownloadProgress::new(1024 * 1024);
        assert_eq!(progress.bytes_per_second(), 0.0);
        progress.add_bytes(1024);
        assert!(progress.bytes_per_second() > 0.0);
    }

    #[tokio::test]
    async fn interrupted_body_resumes_from_flushed_file_offset() {
        let (url, listener) = bind_server().await;
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            let first_request = read_request(&mut first).await;
            assert!(!first_request.to_ascii_lowercase().contains("range:"));
            first
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nConnection: close\r\n\r\n12345",
                )
                .await
                .unwrap();
            first.shutdown().await.unwrap();
            drop(first);

            let (mut resumed, _) = listener.accept().await.unwrap();
            let request = read_request(&mut resumed).await;
            assert!(request.to_ascii_lowercase().contains("range: bytes=5-"));
            write_response(
                &mut resumed,
                "206 Partial Content",
                "Content-Range: bytes 5-9/10\r\n",
                b"67890",
            )
            .await;
        });

        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("archive.bin");
        let client = HttpClient::new().unwrap();
        let size = download_file(&client, &url, &output, Some(10))
            .await
            .unwrap();
        assert_eq!(size, 10);
        assert_eq!(tokio::fs::read(&output).await.unwrap(), b"1234567890");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn twenty_four_concurrent_interrupted_downloads_resume_independently() {
        const DOWNLOADS: usize = 24;
        const SIZE: usize = 16 * 1024;
        const PARTIAL: usize = SIZE / 2;

        fn payload(index: usize) -> Vec<u8> {
            (0..SIZE)
                .map(|position| ((position * 7 + index * 13) % 251) as u8)
                .collect()
        }

        tokio::time::timeout(Duration::from_secs(15), async {
            let (base_url, listener) = bind_server().await;
            let first_wave = std::sync::Arc::new(tokio::sync::Barrier::new(DOWNLOADS));
            let server = tokio::spawn(async move {
                let mut requests = tokio::task::JoinSet::new();
                for _ in 0..DOWNLOADS * 2 {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    let first_wave = first_wave.clone();
                    requests.spawn(async move {
                        let request = read_request(&mut stream).await;
                        let target = request
                            .lines()
                            .next()
                            .unwrap()
                            .split_whitespace()
                            .nth(1)
                            .unwrap();
                        let index: usize = target.rsplit('/').next().unwrap().parse().unwrap();
                        assert!(index < DOWNLOADS);
                        let body = payload(index);
                        let resumed = request.to_ascii_lowercase().contains("range:");
                        if resumed {
                            assert!(request
                                .to_ascii_lowercase()
                                .contains(&format!("range: bytes={PARTIAL}-\r\n")));
                            write_response(
                                &mut stream,
                                "206 Partial Content",
                                &format!("Content-Range: bytes {PARTIAL}-{}/{SIZE}\r\n", SIZE - 1),
                                &body[PARTIAL..],
                            )
                            .await;
                        } else {
                            // All 24 requests must be active before any initial
                            // transfer is interrupted. A lower concurrency limit
                            // cannot make this fixture pass.
                            first_wave.wait().await;
                            stream
                                .write_all(format!(
                                    "HTTP/1.1 200 OK\r\nContent-Length: {SIZE}\r\nConnection: close\r\n\r\n"
                                ).as_bytes())
                                .await
                                .unwrap();
                            stream.write_all(&body[..PARTIAL]).await.unwrap();
                            stream.shutdown().await.unwrap();
                        }
                        (index, resumed)
                    });
                }
                let mut counts = vec![[0usize; 2]; DOWNLOADS];
                while let Some(result) = requests.join_next().await {
                    let (index, resumed) = result.unwrap();
                    counts[index][usize::from(resumed)] += 1;
                }
                assert!(counts.iter().all(|count| *count == [1, 1]));
            });

            let directory = tempfile::tempdir().unwrap();
            let client = HttpClient::new().unwrap();
            let downloads = (0..DOWNLOADS).map(|index| {
                let url = format!("{base_url}/{index}");
                let output = directory.path().join(format!("archive-{index}.bin"));
                let client = &client;
                async move {
                    let size = download_file(client, &url, &output, Some(SIZE as u64))
                        .await
                        .unwrap();
                    assert_eq!(size, SIZE as u64);
                    assert_eq!(tokio::fs::read(&output).await.unwrap(), payload(index));
                }
            });
            futures::future::join_all(downloads).await;
            server.await.unwrap();
        })
        .await
        .expect("all concurrent interrupted downloads should resume and complete");
    }

    #[tokio::test]
    async fn chunked_partial_range_does_not_report_success_before_known_total() {
        let (url, listener) = bind_server().await;
        let server = tokio::spawn(async move {
            let (mut partial, _) = listener.accept().await.unwrap();
            let first_request = read_request(&mut partial).await;
            assert!(!first_request.to_ascii_lowercase().contains("range:"));
            partial
                .write_all(
                    b"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-3/4\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n2\r\nab\r\n0\r\n\r\n",
                )
                .await
                .unwrap();
            partial.shutdown().await.unwrap();

            let (mut resumed, _) = listener.accept().await.unwrap();
            let request = read_request(&mut resumed).await;
            assert!(request.to_ascii_lowercase().contains("range: bytes=2-"));
            write_response(
                &mut resumed,
                "206 Partial Content",
                "Content-Range: bytes 2-3/4\r\n",
                b"cd",
            )
            .await;
        });

        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("archive.bin");
        let client = HttpClient::new().unwrap();
        assert_eq!(
            download_file(&client, &url, &output, None).await.unwrap(),
            4
        );
        assert_eq!(tokio::fs::read(&output).await.unwrap(), b"abcd");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn unknown_content_range_total_is_rejected_without_changing_partial_file() {
        let (url, listener) = bind_server().await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_request(&mut stream).await;
            assert!(request.to_ascii_lowercase().contains("range: bytes=5-"));
            write_response(
                &mut stream,
                "206 Partial Content",
                "Content-Range: bytes 5-9/*\r\n",
                b"67890",
            )
            .await;
        });

        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("archive.bin");
        tokio::fs::write(&output, b"12345").await.unwrap();
        let client = HttpClient::new().unwrap();
        let error = download_file(&client, &url, &output, None)
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("total size is unknown"));
        assert_eq!(tokio::fs::read(&output).await.unwrap(), b"12345");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn ignored_range_restarts_file_instead_of_appending() {
        let (url, listener) = bind_server().await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_request(&mut stream).await;
            assert!(request.to_ascii_lowercase().contains("range: bytes=5-"));
            write_response(&mut stream, "200 OK", "", b"abcdefghij").await;
        });

        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("archive.bin");
        tokio::fs::write(&output, b"12345").await.unwrap();
        let client = HttpClient::new().unwrap();
        assert_eq!(
            download_file(&client, &url, &output, Some(10))
                .await
                .unwrap(),
            10
        );
        assert_eq!(tokio::fs::read(&output).await.unwrap(), b"abcdefghij");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn unexpected_success_status_preserves_partial_file() {
        let (url, listener) = bind_server().await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_request(&mut stream).await;
            assert!(request.to_ascii_lowercase().contains("range: bytes=5-"));
            stream
                .write_all(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            stream.shutdown().await.unwrap();
        });

        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("archive.bin");
        tokio::fs::write(&output, b"12345").await.unwrap();
        let client = HttpClient::new().unwrap();
        let error = download_file(&client, &url, &output, None)
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("Unexpected HTTP 204"));
        assert_eq!(tokio::fs::read(&output).await.unwrap(), b"12345");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn misaligned_content_range_is_discarded_before_full_restart() {
        let (url, listener) = bind_server().await;
        let server = tokio::spawn(async move {
            let (mut partial, _) = listener.accept().await.unwrap();
            let request = read_request(&mut partial).await;
            assert!(request.to_ascii_lowercase().contains("range: bytes=5-"));
            write_response(
                &mut partial,
                "206 Partial Content",
                "Content-Range: bytes 0-9/10\r\n",
                b"wrong-data",
            )
            .await;

            let (mut full, _) = listener.accept().await.unwrap();
            let request = read_request(&mut full).await;
            assert!(!request.to_ascii_lowercase().contains("range:"));
            write_response(&mut full, "200 OK", "", b"0123456789").await;
        });

        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("archive.bin");
        tokio::fs::write(&output, b"12345").await.unwrap();
        let client = HttpClient::new().unwrap();
        assert_eq!(
            download_file(&client, &url, &output, Some(10))
                .await
                .unwrap(),
            10
        );
        assert_eq!(tokio::fs::read(&output).await.unwrap(), b"0123456789");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn selected_stall_deadline_interrupts_a_pending_body_read() {
        let (url, listener) = bind_server().await;
        let server = tokio::spawn(async move {
            let (mut stalled, _) = listener.accept().await.unwrap();
            let _ = read_request(&mut stalled).await;
            stalled
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(300)).await;
            drop(stalled);

            let (mut resumed, _) = listener.accept().await.unwrap();
            let request = read_request(&mut resumed).await;
            assert!(!request.to_ascii_lowercase().contains("range:"));
            write_response(&mut resumed, "200 OK", "", b"done").await;
        });

        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("archive.bin");
        let client = HttpClient::new().unwrap();
        assert_eq!(
            download_file(&client, &url, &output, Some(4))
                .await
                .unwrap(),
            4
        );
        assert_eq!(tokio::fs::read(&output).await.unwrap(), b"done");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn non_regular_destination_is_rejected_without_network_request() {
        let (url, listener) = bind_server().await;
        let server = tokio::spawn(async move {
            assert!(
                tokio::time::timeout(Duration::from_millis(75), listener.accept())
                    .await
                    .is_err(),
                "a disk error must not trigger a network retry"
            );
        });

        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("is-a-directory");
        tokio::fs::create_dir(&output).await.unwrap();
        let client = HttpClient::new().unwrap();
        let error = download_file(&client, &url, &output, Some(4))
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("not a regular file"));
        server.await.unwrap();
    }
}
