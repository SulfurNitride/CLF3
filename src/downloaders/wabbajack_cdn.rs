//! Wabbajack CDN multi-part downloader
//!
//! Wabbajack CDN stores large files in multiple parts (chunks).
//! This module fetches the definition file and downloads parts in parallel.

use anyhow::{bail, Context, Result};
use flate2::read::GzDecoder;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::io::Read;
use std::path::Path;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
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
    pub fn get_part_urls(base_url: &str, definition: &CdnFileDefinition) -> Vec<String> {
        let url = Self::remap_url(base_url);
        definition
            .parts
            .iter()
            .map(|part| {
                format!(
                    "{}/{}/parts/{}",
                    url.trim_end_matches('/'),
                    definition.munged_name,
                    part.index
                )
            })
            .collect()
    }

    /// Download a CDN file (all parts) to the output path
    /// Tries direct download first (more reliable), falls back to CDN multi-part
    pub async fn download(
        &self,
        base_url: &str,
        output_path: &Path,
        expected_size: u64,
    ) -> Result<u64> {
        // Try direct download first (more reliable for some files)
        match self.download_direct(base_url, output_path, expected_size).await {
            Ok(size) => return Ok(size),
            Err(direct_err) => {
                debug!("Direct download failed ({}), trying CDN multi-part", direct_err);
            }
        }

        // Fall back to CDN multi-part download
        let definition = self.get_definition(base_url).await?;

        info!(
            "CDN file: {} ({} parts, {} bytes)",
            definition.original_file_name,
            definition.parts.len(),
            definition.size
        );

        // Verify expected size matches
        if definition.size != expected_size {
            bail!(
                "CDN definition size mismatch: expected {}, got {}",
                expected_size,
                definition.size
            );
        }

        let part_urls = Self::get_part_urls(base_url, &definition);

        // Create output directory
        if let Some(parent) = output_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // Download all parts in order
        let mut file = File::create(output_path)
            .await
            .with_context(|| format!("Failed to create {}", output_path.display()))?;

        let mut total_bytes = 0u64;

        for (i, url) in part_urls.iter().enumerate() {
            let part = &definition.parts[i];
            debug!("Downloading part {}/{}: {} bytes", i + 1, part_urls.len(), part.size);

            let bytes = super::with_retry(&format!("CDN part {}", i + 1), super::MAX_RETRIES, || {
                self.download_part(url)
            })
            .await?;

            if bytes.len() != part.size {
                bail!(
                    "Part {} size mismatch: expected {}, got {}",
                    i,
                    part.size,
                    bytes.len()
                );
            }

            file.write_all(&bytes).await?;
            total_bytes += bytes.len() as u64;
        }

        file.flush().await?;

        if total_bytes != expected_size {
            bail!(
                "Total size mismatch: expected {}, got {}",
                expected_size,
                total_bytes
            );
        }

        info!("Downloaded {} bytes to {}", total_bytes, output_path.display());
        Ok(total_bytes)
    }

    /// Download a single part
    async fn download_part(&self, url: &str) -> Result<Vec<u8>> {
        let response = self
            .client
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

        let urls = WabbajackCdnDownloader::get_part_urls("https://cdn.example.com/base", &def);
        assert_eq!(urls.len(), 3);
        assert_eq!(urls[0], "https://cdn.example.com/base/file_123/parts/0");
        assert_eq!(urls[1], "https://cdn.example.com/base/file_123/parts/1");
        assert_eq!(urls[2], "https://cdn.example.com/base/file_123/parts/2");
    }
}
