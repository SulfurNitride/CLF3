//! Yandex Disk public share downloader
//!
//! Resolves `disk.yandex.ru/d/...` / `yadi.sk/...` public share URLs into
//! direct download links via the public cloud API. No authentication needed.

use anyhow::{bail, Context, Result};
use reqwest::Client;
use serde::Deserialize;
use tracing::{debug, info};

const DOWNLOAD_ENDPOINT: &str = "https://cloud-api.yandex.net/v1/disk/public/resources/download";
const LIST_ENDPOINT: &str = "https://cloud-api.yandex.net/v1/disk/public/resources";

pub struct YandexDownloader {
    client: Client,
}

#[derive(Debug, Deserialize)]
struct DownloadResponse {
    href: Option<String>,
    message: Option<String>,
    description: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ListResponse {
    #[serde(rename = "type")]
    kind: Option<String>,
    name: Option<String>,
    path: Option<String>,
    #[serde(rename = "_embedded")]
    embedded: Option<Embedded>,
}

#[derive(Debug, Deserialize)]
struct Embedded {
    items: Vec<ListItem>,
}

#[derive(Debug, Deserialize, Clone)]
struct ListItem {
    #[serde(rename = "type")]
    kind: String,
    name: String,
    path: String,
}

impl YandexDownloader {
    pub fn new() -> Result<Self> {
        let client = Client::builder()
            .user_agent("Mozilla/5.0 (X11; Linux x86_64) clf3/1.0")
            .build()
            .context("Failed to create Yandex HTTP client")?;
        Ok(Self { client })
    }

    /// Resolve a Yandex Disk public share URL to a direct download URL.
    ///
    /// Strategy:
    /// 1. Try resolving without a `path` (works for single-file shares).
    /// 2. On failure, list the share contents and find a file whose name
    ///    matches `expected_name` (case-insensitive). Recurses into
    ///    subfolders one level deep.
    /// 3. Resolve again using the discovered path.
    ///
    /// The returned URL is time-limited and should be fetched promptly.
    pub async fn get_download_url(
        &self,
        public_url: &str,
        expected_name: Option<&str>,
    ) -> Result<String> {
        debug!("Resolving Yandex Disk URL: {}", public_url);

        // Single-file share: works without path.
        if let Ok(href) = self.try_resolve(public_url, None).await {
            return Ok(href);
        }

        let name = expected_name
            .context("Yandex bare resolve failed and no expected_name provided to search folder")?;

        // Folder share — list contents, find matching file path.
        let path = self
            .find_file_path_by_name(public_url, name)
            .await
            .with_context(|| {
                format!(
                    "Could not locate '{}' inside Yandex folder: {}",
                    name, public_url
                )
            })?;

        info!("Yandex folder file resolved: {} -> {}", name, path);

        self.try_resolve(public_url, Some(&path))
            .await
            .with_context(|| {
                format!(
                    "Yandex download resolve failed for path={}: {}",
                    path, public_url
                )
            })
    }

    /// List a public folder share and find a file whose name matches
    /// `expected_name` (case-insensitive). Searches the root and one level
    /// of subfolders. Returns the disk-relative path suitable for use in
    /// the `path=` parameter of the download endpoint.
    async fn find_file_path_by_name(
        &self,
        public_url: &str,
        expected_name: &str,
    ) -> Result<String> {
        let expected_lower = expected_name.to_lowercase();

        let root = self.list_folder(public_url, None).await?;
        let root_items = root.embedded.map(|e| e.items).unwrap_or_default();

        // First pass — root items.
        if let Some(item) = root_items
            .iter()
            .find(|i| i.kind == "file" && i.name.to_lowercase() == expected_lower)
        {
            return Ok(item.path.clone());
        }

        // Second pass — descend into subfolders one level.
        for sub in root_items.iter().filter(|i| i.kind == "dir") {
            let listing = match self.list_folder(public_url, Some(&sub.path)).await {
                Ok(l) => l,
                Err(e) => {
                    debug!("Failed to list Yandex subfolder {}: {}", sub.path, e);
                    continue;
                }
            };
            if let Some(items) = listing.embedded.map(|e| e.items) {
                if let Some(item) = items
                    .iter()
                    .find(|i| i.kind == "file" && i.name.to_lowercase() == expected_lower)
                {
                    return Ok(item.path.clone());
                }
            }
        }

        // Last resort — log what was actually present so the user can see.
        let names: Vec<String> = root_items.iter().map(|i| i.name.clone()).collect();
        bail!(
            "No matching file in Yandex share root (looking for '{}', found: {:?})",
            expected_name,
            names
        );
    }

    async fn list_folder(&self, public_url: &str, path: Option<&str>) -> Result<ListResponse> {
        let mut request_url = format!(
            "{}?public_key={}&limit=200",
            LIST_ENDPOINT,
            percent_encode(public_url)
        );
        if let Some(p) = path {
            request_url.push_str("&path=");
            request_url.push_str(&percent_encode(p));
        }

        let resp = self
            .client
            .get(&request_url)
            .send()
            .await
            .with_context(|| format!("Yandex list request failed: {}", public_url))?;

        let status = resp.status();
        let body_text = resp
            .text()
            .await
            .context("Failed to read Yandex list body")?;

        if !status.is_success() {
            bail!("Yandex list returned {}: {}", status, body_text);
        }

        let parsed: ListResponse = serde_json::from_str(&body_text)
            .with_context(|| format!("Failed to parse Yandex list response: {}", body_text))?;
        Ok(parsed)
    }

    async fn try_resolve(&self, public_url: &str, path: Option<&str>) -> Result<String> {
        let mut request_url = format!(
            "{}?public_key={}",
            DOWNLOAD_ENDPOINT,
            percent_encode(public_url)
        );
        if let Some(p) = path {
            request_url.push_str("&path=");
            request_url.push_str(&percent_encode(p));
        }

        let resp = self
            .client
            .get(&request_url)
            .send()
            .await
            .with_context(|| format!("Yandex API request failed: {}", public_url))?;

        let status = resp.status();
        let body_text = resp
            .text()
            .await
            .context("Failed to read Yandex API response body")?;

        let body: DownloadResponse = serde_json::from_str(&body_text).with_context(|| {
            format!(
                "Failed to parse Yandex API response (status={}): {}",
                status, body_text
            )
        })?;

        if let Some(href) = body.href {
            return Ok(href);
        }

        let msg = body.description.or(body.message).unwrap_or_default();
        bail!(
            "Yandex API returned no href (status={}, message='{}')",
            status,
            msg
        );
    }
}

/// Percent-encode all non-unreserved characters per RFC 3986.
fn percent_encode(input: &str) -> String {
    input
        .bytes()
        .flat_map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![b as char]
            }
            _ => format!("%{:02X}", b).chars().collect(),
        })
        .collect()
}

impl Default for YandexDownloader {
    fn default() -> Self {
        Self::new().expect("Failed to create Yandex downloader")
    }
}

/// Check if a URL is a Yandex Disk public share URL.
pub fn is_yandex_url(url: &str) -> bool {
    let lower = url.to_lowercase();
    lower.contains("disk.yandex.")
        || lower.contains("yadi.sk/")
        || lower.contains("disk.360.yandex.")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_yandex_url() {
        assert!(is_yandex_url("https://disk.yandex.ru/d/ogrEiFwGFnNcLQ"));
        assert!(is_yandex_url("https://disk.yandex.com/d/abc"));
        assert!(is_yandex_url("https://yadi.sk/d/xyz"));
        assert!(!is_yandex_url("https://www.nexusmods.com/"));
    }

    /// Integration test — hits the real Yandex API.
    /// Run: cargo test --lib downloaders::yandex::tests::test_live_resolve -- --ignored
    #[tokio::test]
    #[ignore]
    async fn test_live_resolve() {
        let dl = YandexDownloader::new().unwrap();
        let url = dl
            .get_download_url(
                "https://disk.yandex.ru/d/ogrEiFwGFnNcLQ",
                Some("EyesMod3_FullUncompressed2K.7z"),
            )
            .await
            .expect("Yandex resolve failed");
        assert!(
            url.starts_with("https://"),
            "Expected https URL, got: {}",
            url
        );
        println!("Resolved URL: {}", url);
    }
}
