//! Google Drive downloader
//!
//! Handles Google Drive's confirmation page for large files.
//! Based on the gdown Python library approach.

use anyhow::{bail, Context, Result};
use regex::Regex;
use reqwest::Client;
use scraper::{Html, Selector};
use std::collections::HashMap;
use tracing::debug;

/// Google Drive downloader
pub struct GoogleDriveDownloader {
    client: Client,
}

impl GoogleDriveDownloader {
    pub fn new() -> Result<Self> {
        let client = Client::builder()
            .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            .cookie_store(true) // Needed for Google's confirmation flow
            .redirect(reqwest::redirect::Policy::limited(10))
            .build()
            .context("Failed to create HTTP client")?;

        Ok(Self { client })
    }

    /// Get direct download URL for a Google Drive file
    ///
    /// Google Drive serves a confirmation page for large files.
    /// This method handles that flow and returns the actual download URL.
    pub async fn get_download_url(&self, file_id: &str, expected_size: u64) -> Result<String> {
        // Try the drive.usercontent.google.com endpoint first (newer)
        let initial_url = format!(
            "https://drive.usercontent.google.com/download?id={}&export=download&confirm=t",
            file_id
        );

        debug!("Fetching Google Drive page: {}", initial_url);

        let response = self
            .client
            .get(&initial_url)
            .send()
            .await
            .context("Failed to connect to Google Drive")?;

        // Check if we got redirected to a download
        let final_url = response.url().to_string();

        // Check content-length
        if let Some(len) = response.content_length() {
            if len == expected_size {
                debug!("Got direct download (size matches)");
                return Ok(final_url);
            }
            debug!(
                "Content-Length {} doesn't match expected {}",
                len, expected_size
            );
        }

        // Check content-type - if it's not HTML, we probably have the file
        if let Some(ct) = response.headers().get("content-type") {
            let ct_str = ct.to_str().unwrap_or("");
            if !ct_str.contains("text/html") {
                debug!("Non-HTML content-type: {}, assuming download", ct_str);
                return Ok(final_url);
            }
        }

        // We got HTML - parse for the actual download link
        let html = response.text().await.context("Failed to read response")?;

        // Debug: log first 500 chars of response
        debug!(
            "Got HTML response (first 500 chars): {}",
            &html[..html.len().min(500)]
        );

        parse_confirmation_page(&html, file_id)
    }

    /// Download a file from Google Drive (uses same client/cookies as URL fetch)
    pub async fn download_to_file(
        &self,
        file_id: &str,
        output_path: &std::path::Path,
        expected_size: u64,
        pb: Option<&indicatif::ProgressBar>,
    ) -> Result<()> {
        use futures::StreamExt;
        use tokio::io::AsyncWriteExt;

        let url = self.get_download_url(file_id, expected_size).await?;

        debug!("Downloading from: {}", url);

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .context("Failed to connect to Google Drive")?
            .error_for_status()
            .context("Google Drive download failed")?;

        // Create parent directories
        if let Some(parent) = output_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let mut file = tokio::fs::File::create(output_path).await?;
        let mut stream = response.bytes_stream();
        let mut downloaded: u64 = 0;

        while let Some(chunk_result) = stream.next().await {
            let chunk = chunk_result.context("Error reading response")?;
            file.write_all(&chunk).await?;
            downloaded += chunk.len() as u64;
            if let Some(pb) = pb {
                pb.set_position(downloaded);
            }
        }

        file.flush().await?;

        // Verify size
        let meta = tokio::fs::metadata(output_path).await?;
        if meta.len() != expected_size {
            anyhow::bail!(
                "Size mismatch: expected {} bytes, got {}",
                expected_size,
                meta.len()
            );
        }

        Ok(())
    }
}

impl Default for GoogleDriveDownloader {
    fn default() -> Self {
        Self::new().expect("Failed to create Google Drive downloader")
    }
}

/// Parse Google Drive confirmation page to extract download URL
///
/// Based on gdown's approach - tries multiple extraction methods.
fn parse_confirmation_page(html: &str, file_id: &str) -> Result<String> {
    use std::sync::OnceLock;

    static UUID_RE: OnceLock<Regex> = OnceLock::new();
    static DOWNLOAD_RE: OnceLock<Regex> = OnceLock::new();
    static JSON_RE: OnceLock<Regex> = OnceLock::new();
    static CONFIRM_RE: OnceLock<Regex> = OnceLock::new();
    static ERROR_RE: OnceLock<Regex> = OnceLock::new();

    // Method 1: Look for uuid parameter in the page (new Google Drive format)
    let uuid_re = UUID_RE.get_or_init(|| Regex::new(r#"uuid[&=]([a-f0-9-]+)"#).unwrap());
    if let Some(caps) = uuid_re.captures(html) {
        let uuid = caps.get(1).unwrap().as_str();
        let url = format!(
            "https://drive.usercontent.google.com/download?id={}&export=download&confirm=t&uuid={}",
            file_id, uuid
        );
        debug!("Found UUID token: {}", uuid);
        return Ok(url);
    }

    // Method 2: Look for href="/uc?export=download..." pattern (old format)
    let download_re =
        DOWNLOAD_RE.get_or_init(|| Regex::new(r#"href="(/uc\?export=download[^"]+)"#).unwrap());
    if let Some(caps) = download_re.captures(html) {
        let path = caps.get(1).unwrap().as_str();
        let url = format!("https://docs.google.com{}", path.replace("&amp;", "&"));
        debug!("Found download URL via regex");
        return Ok(url);
    }

    // Method 3: Look for form action with download URL
    let document = Html::parse_document(html);
    if let Ok(form_selector) = Selector::parse("form") {
        for form in document.select(&form_selector) {
            if let Some(action) = form.value().attr("action") {
                if action.contains("download") || action.contains("uc?") {
                    let mut url = action.replace("&amp;", "&");

                    // Collect hidden form inputs
                    if let Ok(input_selector) = Selector::parse("input[type=\"hidden\"]") {
                        let params: HashMap<String, String> = form
                            .select(&input_selector)
                            .filter_map(|input| {
                                let name = input.value().attr("name")?;
                                let value = input.value().attr("value")?;
                                Some((name.to_string(), value.to_string()))
                            })
                            .collect();

                        if !params.is_empty() {
                            let query = serde_urlencoded::to_string(&params).unwrap_or_default();
                            if url.contains('?') {
                                url = format!("{}&{}", url, query);
                            } else {
                                url = format!("{}?{}", url, query);
                            }
                        }
                    }

                    debug!("Found download URL via form parsing: {}", url);
                    return Ok(url);
                }
            }
        }
    }

    // Method 4: Look for embedded JSON with downloadUrl
    let json_re = JSON_RE.get_or_init(|| Regex::new(r#""downloadUrl":"([^"]+)""#).unwrap());
    if let Some(caps) = json_re.captures(html) {
        let url = caps
            .get(1)
            .unwrap()
            .as_str()
            .replace(r"\u003d", "=")
            .replace(r"\u003f", "?")
            .replace(r"\u0026", "&");
        debug!("Found download URL via JSON extraction");
        return Ok(url);
    }

    // Method 5: Try confirm=t with the file ID directly
    let confirm_re = CONFIRM_RE.get_or_init(|| Regex::new(r#"confirm=([a-zA-Z0-9_-]+)"#).unwrap());
    if let Some(caps) = confirm_re.captures(html) {
        let confirm = caps.get(1).unwrap().as_str();
        let url = format!(
            "https://drive.usercontent.google.com/download?id={}&export=download&confirm={}",
            file_id, confirm
        );
        debug!("Found confirm token: {}", confirm);
        return Ok(url);
    }

    // Check for error messages
    if html.contains("Google Drive - Virus scan warning")
        || html.contains("can't scan this file for viruses")
    {
        // This is the virus warning page - we need to bypass it
        // Try constructing the URL directly with confirm=t
        let url = format!(
            "https://drive.usercontent.google.com/download?id={}&export=download&confirm=t",
            file_id
        );
        debug!("Got virus warning page, trying direct confirm=t");
        return Ok(url);
    }

    let error_re =
        ERROR_RE.get_or_init(|| Regex::new(r#"<p class="uc-error-subcaption">(.*?)</p>"#).unwrap());
    if let Some(caps) = error_re.captures(html) {
        let error_msg = caps.get(1).unwrap().as_str();
        bail!("Google Drive error: {}", error_msg);
    }

    // Log the HTML for debugging
    eprintln!(
        "DEBUG: Could not parse GDrive page. First 2000 chars:\n{}",
        &html[..html.len().min(2000)]
    );

    bail!("Could not extract download URL from Google Drive confirmation page")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_href_pattern() {
        let html = r#"<a href="/uc?export=download&amp;id=ABC123&amp;confirm=t">Download</a>"#;
        let url = parse_confirmation_page(html, "ABC123").unwrap();
        assert!(url.contains("export=download"));
    }

    #[test]
    fn test_parse_json_pattern() {
        let html = r#"{"downloadUrl":"https://example.com/download\u003fid\u003dABC"}"#;
        let url = parse_confirmation_page(html, "ABC").unwrap();
        assert_eq!(url, "https://example.com/download?id=ABC");
    }

    #[test]
    fn test_parse_error_message() {
        let html = r#"<p class="uc-error-subcaption">File not found</p>"#;
        let result = parse_confirmation_page(html, "test123");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("File not found"));
    }

    #[test]
    fn test_parse_uuid_pattern() {
        let html =
            r#"<form action="https://drive.usercontent.google.com/download?uuid=abc-123-def">"#;
        let url = parse_confirmation_page(html, "FILE123").unwrap();
        assert!(url.contains("uuid=abc-123-def") || url.contains("FILE123"));
    }
}
