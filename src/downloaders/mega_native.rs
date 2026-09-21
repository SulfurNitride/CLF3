//! Native Mega download support using the `mega` crate.
//!
//! Downloads files directly from Mega's API without going through the
//! Wabbajack proxy. No login required for public files.
//!
//! Note: the `mega` crate bundles reqwest 0.12 as its HTTP transport,
//! which coexists with our reqwest 0.13 in the dependency tree. This
//! is harmless — the mega Client only uses its own reqwest internally.

use super::http::ProgressCallback;
use anyhow::{Context, Result};
use futures::io::AsyncWrite;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};
use std::time::{Duration, Instant};
use tokio::fs::File;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use tracing::info;

#[path = "mega_transfer.rs"]
mod transfer;

/// Normalize a Mega URL to the format the mega crate expects: `https://mega.nz/file/ID#KEY`
fn normalize_mega_url(url: &str) -> String {
    let mut url = url.to_string();

    // Strip trailing whitespace/newlines
    url = url.trim().to_string();

    // Normalize protocol
    if url.starts_with("http://") {
        url = format!("https://{}", &url[7..]);
    }

    // Remove www. prefix
    url = url.replace("://www.mega.nz", "://mega.nz");

    // Convert old format mega.nz/#!ID!KEY to mega.nz/file/ID#KEY
    if url.contains("mega.nz/#!") {
        if let Some(rest) = url.strip_prefix("https://mega.nz/#!") {
            let rest = rest.replace('!', "#");
            url = format!("https://mega.nz/file/{}", rest);
        }
    }

    url
}

/// Extract the `/file/<handle>` suffix from a folder URL, if present.
/// Folder-file URL: `https://mega.nz/folder/ID#KEY/file/FILEHANDLE`
fn extract_folder_file_handle(url: &str) -> Option<String> {
    if !url.contains("/folder/") {
        return None;
    }
    let idx = url.find("/file/")?;
    let rest = &url[idx + 6..];
    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let handle = &rest[..end];
    (!handle.is_empty()).then(|| handle.to_string())
}

/// Download a file from a public Mega URL to disk.
///
/// Supports both direct file URLs (`/file/ID#KEY`) and folder-file URLs
/// (`/folder/ID#KEY/file/HANDLE`) where a specific file is selected inside
/// a shared folder.
pub async fn download_mega_file(mega_url: &str, output_path: &Path) -> Result<()> {
    download_mega_file_with_callback(mega_url, output_path, None).await
}

pub async fn download_mega_file_with_callback(
    mega_url: &str,
    output_path: &Path,
    progress: Option<&ProgressCallback>,
) -> Result<()> {
    let mega_url = normalize_mega_url(mega_url);
    info!("Resolving Mega file");

    let file_handle_override = extract_folder_file_handle(&mega_url);

    // mega::Client::builder().build() accepts impl mega::http::HttpClient.
    // The mega crate provides an impl for its bundled reqwest 0.12 Client.
    // We construct that Client directly here (it's a different type from our reqwest 0.13).
    let http_client = reqwest_012::Client::builder()
        .timeout(std::time::Duration::from_secs(4 * 60 * 60)) // 4 hours for large files
        .connect_timeout(std::time::Duration::from_secs(30))
        .build()
        .unwrap_or_else(|_| reqwest_012::Client::new());

    let mega_client = mega::Client::builder()
        .build(http_client.clone())
        .context("Failed to create Mega client")?;

    let nodes = mega_client
        .fetch_public_nodes(&mega_url)
        .await
        .context("Failed to fetch Mega file info")?;

    let file_node = if let Some(handle) = file_handle_override.as_deref() {
        nodes
            .get_node_by_handle(handle)
            .with_context(|| format!("File handle {} not found in Mega folder listing", handle))?
    } else {
        nodes
            .roots()
            .find(|n| n.kind().is_file())
            .context("No downloadable file found at Mega URL")?
    };

    info!(
        "Mega file: {} ({} bytes)",
        file_node.name(),
        file_node.size()
    );

    // Create parent directories
    if let Some(parent) = output_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    // Write directly through a counting writer. Keeping the transfer and file
    // in one future also prevents detached writers from racing a retry.
    let file = File::create(output_path).await?;
    let mut writer = ProgressWriter::new(file.compat_write(), file_node.size(), progress);
    let result: Result<()> = async {
        transfer::download_node(&http_client, file_node, &mut writer)
            .await
            .context("Mega download failed")?;
        futures::io::AsyncWriteExt::flush(&mut writer).await?;
        if writer.written != file_node.size() {
            anyhow::bail!(
                "Mega download incomplete: got {} bytes, expected {}",
                writer.written,
                file_node.size()
            );
        }
        writer.report();
        Ok(())
    }
    .await;
    drop(writer);
    if result.is_err() {
        let _ = tokio::fs::remove_file(output_path).await;
    }
    result?;

    if let Ok(meta) = std::fs::metadata(output_path) {
        info!(
            "Mega download complete: {} ({} bytes)",
            output_path.display(),
            meta.len()
        );
    }

    Ok(())
}

struct ProgressWriter<'a, W> {
    inner: W,
    callback: Option<&'a ProgressCallback>,
    total: u64,
    written: u64,
    started: Instant,
    last_report: Instant,
}

impl<'a, W> ProgressWriter<'a, W> {
    fn new(inner: W, total: u64, callback: Option<&'a ProgressCallback>) -> Self {
        if let Some(callback) = callback {
            callback(0, total, 0.0);
        }
        Self {
            inner,
            callback,
            total,
            written: 0,
            started: Instant::now(),
            last_report: Instant::now(),
        }
    }

    fn report(&mut self) {
        if let Some(callback) = self.callback {
            let speed = self.written as f64 / self.started.elapsed().as_secs_f64().max(0.001);
            callback(self.written, self.total, speed);
        }
        self.last_report = Instant::now();
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for ProgressWriter<'_, W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_write(cx, buf) {
            Poll::Ready(Ok(written)) => {
                this.written += written as u64;
                if this.last_report.elapsed() >= Duration::from_millis(250)
                    || this.written == this.total
                {
                    this.report();
                }
                Poll::Ready(Ok(written))
            }
            other => other,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_close(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "downloads a complete file; requires CLF3_MEGA_TEST_URL and CLF3_MEGA_TEST_HASH"]
    async fn test_live_mega_verified() {
        let url = std::env::var("CLF3_MEGA_TEST_URL").unwrap();
        let expected = std::env::var("CLF3_MEGA_TEST_HASH").unwrap();
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("verified.7z");
        let started = Instant::now();
        let last_report = std::sync::Mutex::new(Instant::now());
        let callback: ProgressCallback = Box::new(move |written, total, speed| {
            let mut last = last_report.lock().unwrap();
            if last.elapsed() >= Duration::from_secs(5) || written == total {
                println!(
                    "Mega: {written}/{total} bytes, {:.2} MiB/s",
                    speed / 1048576.0
                );
                *last = Instant::now();
            }
        });
        download_mega_file_with_callback(&url, &output, Some(&callback))
            .await
            .unwrap();
        let elapsed = started.elapsed().as_secs_f64();
        let size = std::fs::metadata(&output).unwrap().len();
        let hash = crate::hash::compute_file_hash(&output).unwrap();
        assert_eq!(hash, expected);
        println!("Verified Mega file: {size} bytes in {elapsed:.2}s ({:.2} MiB/s), Wabbajack hash {hash}", size as f64 / elapsed / 1048576.0);
    }

    #[tokio::test]
    async fn progress_counts_partial_writes_and_reports_before_completion() {
        struct ShortWriter(Vec<u8>);
        impl AsyncWrite for ShortWriter {
            fn poll_write(
                mut self: Pin<&mut Self>,
                _: &mut TaskContext<'_>,
                data: &[u8],
            ) -> Poll<std::io::Result<usize>> {
                let count = data.len().min(3);
                self.0.extend_from_slice(&data[..count]);
                Poll::Ready(Ok(count))
            }
            fn poll_flush(
                self: Pin<&mut Self>,
                _: &mut TaskContext<'_>,
            ) -> Poll<std::io::Result<()>> {
                Poll::Ready(Ok(()))
            }
            fn poll_close(
                self: Pin<&mut Self>,
                _: &mut TaskContext<'_>,
            ) -> Poll<std::io::Result<()>> {
                Poll::Ready(Ok(()))
            }
        }
        let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured = events.clone();
        let callback: ProgressCallback = Box::new(move |written, total, speed| {
            captured.lock().unwrap().push((written, total, speed))
        });
        let mut writer = ProgressWriter::new(ShortWriter(Vec::new()), 10, Some(&callback));
        writer.last_report = Instant::now() - Duration::from_secs(1);
        futures::io::AsyncWriteExt::write_all(&mut writer, b"0123456789")
            .await
            .unwrap();
        assert_eq!(writer.inner.0, b"0123456789");
        let events = events.lock().unwrap();
        assert_eq!((events[0].0, events[0].1), (0, 10));
        assert_eq!((events[1].0, events[1].1), (3, 10));
        assert!(events[1].2 > 0.0);
        assert_eq!(events.last().unwrap().0, 10);
    }

    #[tokio::test]
    #[ignore = "requires CLF3_MEGA_TEST_URL; stops after observing transfer progress"]
    async fn test_live_mega_progress() {
        let url = std::env::var("CLF3_MEGA_TEST_URL").unwrap();
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("partial.7z");
        let (tx, rx) = tokio::sync::oneshot::channel();
        let tx = std::sync::Mutex::new(Some(tx));
        let callback: ProgressCallback = Box::new(move |written, total, speed| {
            if written > 0 && written < total {
                if let Some(tx) = tx.lock().unwrap().take() {
                    let _ = tx.send((written, total, speed));
                }
            }
        });
        tokio::select! {
            event = rx => {
                let (written, total, speed) = event.unwrap();
                assert!(written > 0 && written < total && speed > 0.0);
                println!("Mega live progress: {written}/{total} bytes at {speed:.0} bytes/s; bounded probe complete");
            }
            result = download_mega_file_with_callback(&url, &output, Some(&callback)) => {
                result.unwrap();
                panic!("Expected intermediate progress on a large test file");
            }
            _ = tokio::time::sleep(Duration::from_secs(60)) => panic!("No Mega progress within 60 seconds"),
        }
    }

    #[test]
    fn test_extract_folder_file_handle() {
        assert_eq!(
            extract_folder_file_handle(
                "https://mega.nz/folder/VsQV2RjY#0hVLXv1g3Y7LkTlt9D7YtQ/file/V4RwQBQR"
            ),
            Some("V4RwQBQR".to_string())
        );
        // Plain file URL — no folder
        assert_eq!(
            extract_folder_file_handle("https://mega.nz/file/ABC#KEY"),
            None
        );
        // Folder URL without file selection
        assert_eq!(
            extract_folder_file_handle("https://mega.nz/folder/VsQV2RjY#KEY"),
            None
        );
    }

    #[test]
    fn test_normalize_mega_url() {
        // Standard format — no change
        assert_eq!(
            normalize_mega_url("https://mega.nz/file/ABC#KEY"),
            "https://mega.nz/file/ABC#KEY"
        );

        // www prefix
        assert_eq!(
            normalize_mega_url("https://www.mega.nz/file/ABC#KEY"),
            "https://mega.nz/file/ABC#KEY"
        );

        // Old format
        assert_eq!(
            normalize_mega_url("https://mega.nz/#!ABC!KEY"),
            "https://mega.nz/file/ABC#KEY"
        );

        // http:// to https://
        assert_eq!(
            normalize_mega_url("http://mega.nz/file/ABC#KEY"),
            "https://mega.nz/file/ABC#KEY"
        );
    }

    /// Integration test: verify we can connect to Mega and fetch file metadata.
    /// Run with: cargo test --lib downloaders::mega_native::tests::test_mega_fetch_info -- --ignored
    #[tokio::test]
    #[ignore]
    async fn test_mega_fetch_info() {
        // Devious Devices on Mega (2GB — we only fetch metadata, don't download)
        let url = "https://mega.nz/file/3A90hQZS#wqqyGkKrYGPGFfPiluHfSmyTL5RqHJGT0-MtinqXTxU";

        let http_client = reqwest_012::Client::new();
        let mega_client = mega::Client::builder()
            .build(http_client)
            .expect("Failed to create Mega client");

        let nodes = mega_client
            .fetch_public_nodes(url)
            .await
            .expect("Failed to fetch Mega nodes");

        let file_node = nodes
            .roots()
            .find(|n| n.kind().is_file())
            .expect("No file node found");

        println!(
            "Mega file: {} ({} bytes)",
            file_node.name(),
            file_node.size()
        );

        // Verify we got the right file
        assert!(file_node.size() > 0);
        assert!(
            file_node.name().contains("Devious") || file_node.size() > 1_000_000_000,
            "Expected Devious Devices file, got: {} ({} bytes)",
            file_node.name(),
            file_node.size()
        );
    }
}
