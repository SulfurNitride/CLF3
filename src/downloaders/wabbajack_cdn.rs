//! Wabbajack CDN multi-part downloader
//!
//! Wabbajack CDN stores large files in multiple parts (chunks).
//! This module fetches the definition file and downloads parts in parallel.

use anyhow::{bail, Context, Result};
use flate2::read::GzDecoder;
use futures::stream::{self, StreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::io::Read;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tracing::{debug, info};

/// CDN domain remapping (B-CDN to official domains)
const CDN_REMAPS: &[(&str, &str)] = &[
    ("wabbajack.b-cdn.net", "authored-files.wabbajack.org"),
    ("wabbajack-mirror.b-cdn.net", "mirror.wabbajack.org"),
    ("wabbajack-patches.b-cdn.net", "patches.wabbajack.org"),
    ("wabbajacktest.b-cdn.net", "test-files.wabbajack.org"),
];

/// Definition filename (magic constant from Wabbajack)
const DEFINITION_FILE: &str = "definition.json.gz";

/// Fallback direct download URL base (when CDN multi-part fails)
const DIRECT_LINK_BASE: &str = "https://build.wabbajack.org/authored_files/direct_link";

/// CDN file definition (from definition.json.gz)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct CdnFileDefinition {
    pub author: String,
    #[serde(default)]
    pub server_assigned_unique_id: Option<uuid::Uuid>,
    pub hash: String,
    pub munged_name: String,
    pub original_file_name: String,
    pub size: u64,
    pub parts: Vec<CdnPart>,
}

/// A part/chunk of a CDN file
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct CdnPart {
    pub hash: String,
    pub index: usize,
    pub offset: usize,
    pub size: usize,
}

/// Wabbajack CDN downloader
pub struct WabbajackCdnDownloader {
    client: Client,
}

impl WabbajackCdnDownloader {
    pub fn new() -> Result<Self> {
        let client = Client::builder()
            .user_agent(concat!("clf3/", env!("CARGO_PKG_VERSION")))
            .build()
            .context("Failed to create HTTP client")?;

        Ok(Self { client })
    }

    /// Remap B-CDN URLs to official Wabbajack domains
    pub fn remap_url(url: &str) -> String {
        let mut result = url.to_string();
        for (from, to) in CDN_REMAPS {
            result = result.replace(from, to);
        }
        result
    }

    /// Fetch and parse the CDN file definition
    pub async fn get_definition(&self, base_url: &str) -> Result<CdnFileDefinition> {
        let url = Self::remap_url(base_url);
        let definition_url = format!("{}/{}", url.trim_end_matches('/'), DEFINITION_FILE);

        debug!("Fetching CDN definition from: {}", definition_url);

        let response = self
            .client
            .get(&definition_url)
            .send()
            .await
            .with_context(|| format!("Failed to fetch definition from {}", definition_url))?
            .error_for_status()
            .with_context(|| format!("Server error for {}", definition_url))?;

        let bytes = response
            .bytes()
            .await
            .context("Failed to read definition response")?;

        // Decompress gzip
        let mut decoder = GzDecoder::new(&bytes[..]);
        let mut json_str = String::new();
        decoder
            .read_to_string(&mut json_str)
            .context("Failed to decompress definition.json.gz")?;

        // Parse JSON (with fallback for Unicode issues)
        parse_definition(&json_str)
    }

    /// Get URLs for all parts of a CDN file
    /// The base_url already contains the file identifier (e.g. Tuxborn.wabbajack_uuid)
    pub fn get_part_urls(base_url: &str, definition: &CdnFileDefinition) -> Vec<String> {
        let url = Self::remap_url(base_url);
        definition
            .parts
            .iter()
            .map(|part| {
                format!(
                    "{}/parts/{}",
                    url.trim_end_matches('/'),
                    part.index
                )
            })
            .collect()
    }

    /// Download a CDN file (all parts) to the output path with parallel downloads
    /// Uses 16 concurrent connections for optimal speed (~100+ MB/s)
    pub async fn download(
        &self,
        base_url: &str,
        output_path: &Path,
        expected_size: u64,
    ) -> Result<u64> {
        self.download_with_progress(base_url, output_path, expected_size, |_, _| {})
            .await
    }

    /// Download a CDN file with progress callback
    /// Progress callback receives (bytes_downloaded, total_bytes)
    pub async fn download_with_progress<F>(
        &self,
        base_url: &str,
        output_path: &Path,
        expected_size: u64,
        progress_callback: F,
    ) -> Result<u64>
    where
        F: Fn(u64, u64) + Send + Sync + 'static,
    {
        const PARALLEL_DOWNLOADS: usize = 16;

        // Fetch the definition file
        let definition = self.get_definition(base_url).await?;

        info!(
            "CDN file: {} ({} parts, {} bytes)",
            definition.original_file_name,
            definition.parts.len(),
            definition.size
        );

        // Verify expected size matches (if provided)
        if expected_size > 0 && definition.size != expected_size {
            bail!(
                "CDN definition size mismatch: expected {}, got {}",
                expected_size,
                definition.size
            );
        }

        let total_size = definition.size;
        let part_urls = Self::get_part_urls(base_url, &definition);

        // Create output directory
        if let Some(parent) = output_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // Pre-allocate the output file
        let file = File::create(output_path)
            .await
            .with_context(|| format!("Failed to create {}", output_path.display()))?;
        file.set_len(total_size).await?;
        drop(file);

        // Progress tracking
        let downloaded_bytes = Arc::new(AtomicU64::new(0));
        let progress_callback = Arc::new(progress_callback);

        // Download parts in parallel
        let parts_with_urls: Vec<_> = definition
            .parts
            .iter()
            .zip(part_urls.iter())
            .collect();

        let client = self.client.clone();
        let output_path_owned = output_path.to_path_buf();

        let results: Vec<Result<()>> = stream::iter(parts_with_urls)
            .map(|(part, url)| {
                let client = client.clone();
                let url = url.clone();
                let output_path = output_path_owned.clone();
                let downloaded_bytes = downloaded_bytes.clone();
                let progress_callback = progress_callback.clone();
                let part_index = part.index;
                let part_offset = part.offset as u64;
                let part_size = part.size;

                async move {
                    // Download the part with retry
                    let bytes = super::with_retry(&format!("CDN part {}", part_index), super::MAX_RETRIES, || {
                        Self::download_part_static(&client, &url)
                    })
                    .await?;

                    if bytes.len() != part_size {
                        bail!(
                            "Part {} size mismatch: expected {}, got {}",
                            part_index,
                            part_size,
                            bytes.len()
                        );
                    }

                    // Write to the correct offset in the file
                    let mut file = tokio::fs::OpenOptions::new()
                        .write(true)
                        .open(&output_path)
                        .await
                        .with_context(|| format!("Failed to open {} for writing", output_path.display()))?;

                    file.seek(std::io::SeekFrom::Start(part_offset)).await?;
                    file.write_all(&bytes).await?;

                    // Update progress
                    let new_total = downloaded_bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed) + bytes.len() as u64;
                    progress_callback(new_total, total_size);

                    Ok(())
                }
            })
            .buffer_unordered(PARALLEL_DOWNLOADS)
            .collect()
            .await;

        // Check for any errors
        for result in results {
            result?;
        }

        let total_downloaded = downloaded_bytes.load(Ordering::Relaxed);
        if expected_size > 0 && total_downloaded != expected_size {
            bail!(
                "Total size mismatch: expected {}, got {}",
                expected_size,
                total_downloaded
            );
        }

        info!("Downloaded {} bytes to {}", total_downloaded, output_path.display());
        Ok(total_downloaded)
    }

    /// Static version of download_part for use in async closures
    async fn download_part_static(client: &Client, url: &str) -> Result<Vec<u8>> {
        let response = client
            .get(url)
            .send()
            .await
            .with_context(|| format!("Failed to fetch part from {}", url))?
            .error_for_status()
            .with_context(|| format!("Server error for {}", url))?;

        response
            .bytes()
            .await
            .map(|b| b.to_vec())
            .context("Failed to read part data")
    }

    /// Extract the file ID from a CDN URL for direct download fallback
    /// e.g., "https://authored-files.wabbajack.org/AMM_xLODGen_108.7z_2692911c-ae6d-4f07-b1e1-5dfe87623dd2"
    /// returns "AMM_xLODGen_108.7z_2692911c-ae6d-4f07-b1e1-5dfe87623dd2"
    fn extract_file_id(url: &str) -> Option<&str> {
        url.trim_end_matches('/').rsplit('/').next()
    }

    /// Fallback: download directly from build.wabbajack.org when CDN multi-part fails
    async fn download_direct(
        &self,
        base_url: &str,
        output_path: &Path,
        expected_size: u64,
    ) -> Result<u64> {
        let url = Self::remap_url(base_url);
        let file_id = Self::extract_file_id(&url)
            .context("Failed to extract file ID from CDN URL")?;

        let direct_url = format!("{}/{}", DIRECT_LINK_BASE, file_id);
        info!("Using direct download fallback: {}", direct_url);

        // Create output directory
        if let Some(parent) = output_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // Download the file directly - requires browser User-Agent!
        let response = self
            .client
            .get(&direct_url)
            .header("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            .header("Accept", "*/*")
            .send()
            .await
            .with_context(|| format!("Failed to fetch from {}", direct_url))?
            .error_for_status()
            .with_context(|| format!("Server error for {}", direct_url))?;

        let bytes = response
            .bytes()
            .await
            .context("Failed to read response")?;

        // Verify size
        if bytes.len() as u64 != expected_size {
            bail!(
                "Size mismatch: expected {}, got {}",
                expected_size,
                bytes.len()
            );
        }

        // Write to file
        let mut file = File::create(output_path)
            .await
            .with_context(|| format!("Failed to create {}", output_path.display()))?;

        file.write_all(&bytes).await?;
        file.flush().await?;

        info!("Downloaded {} bytes via direct link", bytes.len());
        Ok(bytes.len() as u64)
    }
}

impl Default for WabbajackCdnDownloader {
    fn default() -> Self {
        Self::new().expect("Failed to create CDN downloader")
    }
}

/// Parse CDN definition JSON with fallback for malformed responses
fn parse_definition(json_str: &str) -> Result<CdnFileDefinition> {
    // Try direct parsing first
    if let Ok(def) = serde_json::from_str::<CdnFileDefinition>(json_str.trim()) {
        return Ok(def);
    }

    // Fallback: skip any leading garbage characters (Unicode BOM, etc.)
    let cleaned: String = json_str
        .trim()
        .chars()
        .skip_while(|c| *c != '{')
        .collect();

    serde_json::from_str(&cleaned).context("Failed to parse CDN definition JSON")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_url_remap() {
        assert_eq!(
            WabbajackCdnDownloader::remap_url("https://wabbajack.b-cdn.net/files/test"),
            "https://authored-files.wabbajack.org/files/test"
        );
        assert_eq!(
            WabbajackCdnDownloader::remap_url("https://wabbajack-mirror.b-cdn.net/data"),
            "https://mirror.wabbajack.org/data"
        );
        // Non-CDN URLs pass through unchanged
        assert_eq!(
            WabbajackCdnDownloader::remap_url("https://example.com/file"),
            "https://example.com/file"
        );
    }

    #[test]
    fn test_parse_definition() {
        let json = r#"{"Author":"test","OriginalFileName":"test.7z","Size":1000,"Hash":"abc=","Parts":[{"Size":1000,"Offset":0,"Hash":"abc=","Index":0}],"MungedName":"test.7z_123"}"#;
        let def = parse_definition(json).unwrap();
        assert_eq!(def.author, "test");
        assert_eq!(def.original_file_name, "test.7z");
        assert_eq!(def.size, 1000);
        assert_eq!(def.parts.len(), 1);
    }

    #[test]
    fn test_parse_definition_with_bom() {
        // Simulate response with leading garbage
        let json = "\u{feff}{\"Author\":\"test\",\"OriginalFileName\":\"file.7z\",\"Size\":500,\"Hash\":\"x=\",\"Parts\":[],\"MungedName\":\"file\"}";
        let def = parse_definition(json).unwrap();
        assert_eq!(def.author, "test");
    }

    #[test]
    fn test_get_part_urls() {
        let def = CdnFileDefinition {
            author: "test".into(),
            server_assigned_unique_id: None,
            hash: "abc".into(),
            munged_name: "file_123".into(),
            original_file_name: "file.7z".into(),
            size: 3000,
            parts: vec![
                CdnPart { hash: "a".into(), index: 0, offset: 0, size: 1000 },
                CdnPart { hash: "b".into(), index: 1, offset: 1000, size: 1000 },
                CdnPart { hash: "c".into(), index: 2, offset: 2000, size: 1000 },
            ],
        };

        // Base URL already contains the file identifier (like Tuxborn.wabbajack_uuid)
        let urls = WabbajackCdnDownloader::get_part_urls("https://cdn.example.com/file_123", &def);
        assert_eq!(urls.len(), 3);
        assert_eq!(urls[0], "https://cdn.example.com/file_123/parts/0");
        assert_eq!(urls[1], "https://cdn.example.com/file_123/parts/1");
        assert_eq!(urls[2], "https://cdn.example.com/file_123/parts/2");
    }

    #[tokio::test]
    #[ignore] // Requires network - run with: cargo test test_cdn_download_tuxborn -- --ignored --nocapture
    async fn test_cdn_download_tuxborn() {
        use std::time::Instant;

        let downloader = WabbajackCdnDownloader::new().unwrap();
        let base_url = "https://authored-files.wabbajack.org/Tuxborn.wabbajack_b49465c4-f49a-4f3e-9e3d-ddfe8dbac5e2";

        // First, just fetch the definition to see the file info
        let definition = downloader.get_definition(base_url).await.unwrap();
        println!("File: {} by {}", definition.original_file_name, definition.author);
        println!("Size: {} bytes ({:.2} GB)", definition.size, definition.size as f64 / 1_073_741_824.0);
        println!("Parts: {}", definition.parts.len());

        // Download first 50 parts (100MB) as a speed test
        let test_parts = 50;
        let test_size: u64 = definition.parts.iter().take(test_parts).map(|p| p.size as u64).sum();
        println!("\nDownloading first {} parts ({:.2} MB) for speed test...", test_parts, test_size as f64 / 1_048_576.0);

        let output_path = std::path::Path::new("/tmp/tuxborn_speed_test.partial");
        let start = Instant::now();

        // Create a partial definition with only the first N parts
        let partial_def = CdnFileDefinition {
            author: definition.author.clone(),
            server_assigned_unique_id: definition.server_assigned_unique_id,
            hash: definition.hash.clone(),
            munged_name: definition.munged_name.clone(),
            original_file_name: definition.original_file_name.clone(),
            parts: definition.parts.iter().take(test_parts).cloned().collect(),
            size: test_size,
        };

        // Download using our parallel method (manually call the parts)
        let part_urls = WabbajackCdnDownloader::get_part_urls(base_url, &partial_def);

        // Pre-allocate output file
        let file = tokio::fs::File::create(output_path).await.unwrap();
        file.set_len(test_size).await.unwrap();
        drop(file);

        // Download parts in parallel
        let results: Vec<Result<()>> = futures::stream::iter(partial_def.parts.iter().zip(part_urls.iter()))
            .map(|(part, url)| {
                let url = url.clone();
                let output_path = output_path.to_path_buf();
                let part_offset = part.offset as u64;
                let part_size = part.size;
                let client = downloader.client.clone();

                async move {
                    let bytes = WabbajackCdnDownloader::download_part_static(&client, &url).await?;
                    assert_eq!(bytes.len(), part_size);

                    let mut file = tokio::fs::OpenOptions::new()
                        .write(true)
                        .open(&output_path)
                        .await?;
                    file.seek(std::io::SeekFrom::Start(part_offset)).await?;
                    file.write_all(&bytes).await?;
                    Ok(())
                }
            })
            .buffer_unordered(16)
            .collect()
            .await;

        for r in results {
            r.unwrap();
        }

        let elapsed = start.elapsed();
        let speed_mbs = test_size as f64 / elapsed.as_secs_f64() / 1_048_576.0;

        println!("Downloaded: {:.2} MB", test_size as f64 / 1_048_576.0);
        println!("Time: {:.2}s", elapsed.as_secs_f64());
        println!("Speed: {:.2} MB/s", speed_mbs);

        // Cleanup
        let _ = tokio::fs::remove_file(output_path).await;

        // Assert reasonable speed (at least 20 MB/s)
        assert!(speed_mbs > 20.0, "Download speed too slow: {:.2} MB/s", speed_mbs);
    }
}
