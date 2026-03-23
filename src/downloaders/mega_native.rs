//! Native Mega download support using the `mega` crate.
//!
//! Downloads files directly from Mega's API without going through the
//! Wabbajack proxy. No login required for public files.
//!
//! Note: the `mega` crate bundles reqwest 0.12 as its HTTP transport,
//! which coexists with our reqwest 0.13 in the dependency tree. This
//! is harmless — the mega Client only uses its own reqwest internally.

use anyhow::{Context, Result};
use std::path::Path;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use tracing::info;

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

/// Download a file from a public Mega URL to disk.
pub async fn download_mega_file(mega_url: &str, output_path: &Path) -> Result<()> {
    let mega_url = normalize_mega_url(mega_url);
    info!("Downloading from Mega: {}", mega_url);

    // mega::Client::builder().build() accepts impl mega::http::HttpClient.
    // The mega crate provides an impl for its bundled reqwest 0.12 Client.
    // We construct that Client directly here (it's a different type from our reqwest 0.13).
    let http_client = reqwest_012::Client::builder()
        .timeout(std::time::Duration::from_secs(4 * 60 * 60)) // 4 hours for large files
        .connect_timeout(std::time::Duration::from_secs(30))
        .build()
        .unwrap_or_else(|_| reqwest_012::Client::new());

    let mega_client = mega::Client::builder()
        .build(http_client)
        .context("Failed to create Mega client")?;

    let nodes = mega_client
        .fetch_public_nodes(&mega_url)
        .await
        .context("Failed to fetch Mega file info")?;

    let file_node = nodes
        .roots()
        .find(|n| n.kind().is_file())
        .context("No downloadable file found at Mega URL")?;

    info!(
        "Mega file: {} ({} bytes)",
        file_node.name(),
        file_node.size()
    );

    // Create parent directories
    if let Some(parent) = output_path.parent() {
        tokio::fs::create_dir_all(parent).await.ok();
    }

    // Set up pipe: mega writes to pipe_writer, we read from pipe_reader into file
    let (pipe_reader, pipe_writer) = sluice::pipe::pipe();

    let output = output_path.to_path_buf();
    let write_handle = tokio::spawn(async move {
        let file = File::create(&output).await?;
        let mut compat = file.compat_write();
        futures::io::copy(pipe_reader, &mut compat).await?;
        Ok::<(), anyhow::Error>(())
    });

    mega_client
        .download_node(file_node, pipe_writer)
        .await
        .context("Mega download failed")?;

    write_handle
        .await
        .context("Mega write task panicked")?
        .context("Failed to write Mega download to disk")?;

    if let Ok(meta) = std::fs::metadata(output_path) {
        info!(
            "Mega download complete: {} ({} bytes)",
            output_path.display(),
            meta.len()
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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

