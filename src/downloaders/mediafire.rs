//! MediaFire downloader
//!
//! Parses MediaFire download pages to extract direct download links.

use anyhow::{bail, Context, Result};
use reqwest::Client;
use scraper::{Html, Selector};
use tracing::debug;

/// MediaFire downloader
pub struct MediaFireDownloader {
    client: Client,
}

impl MediaFireDownloader {
    pub fn new() -> Result<Self> {
        // MediaFire requires a browser-like User-Agent
        let client = Client::builder()
            .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            .build()
            .context("Failed to create HTTP client")?;

        Ok(Self { client })
    }

    /// Get direct download URL from a MediaFire page
    pub async fn get_download_url(&self, page_url: &str) -> Result<String> {
        debug!("Fetching MediaFire page: {}", page_url);

        let response = self
            .client
            .get(page_url)
            .send()
            .await
            .context("Failed to connect to MediaFire")?;

        // MediaFire sometimes returns redirects, follow them
        let final_url = response.url().to_string();
        if final_url != page_url {
            debug!("Followed redirect to: {}", final_url);
        }

        let html = response
            .text()
            .await
            .context("Failed to read MediaFire response")?;

        parse_mediafire_page(&html)
    }
}

impl Default for MediaFireDownloader {
    fn default() -> Self {
        Self::new().expect("Failed to create MediaFire downloader")
    }
}

/// Parse MediaFire page to extract download URL
fn parse_mediafire_page(html: &str) -> Result<String> {
    let document = Html::parse_document(html);

    // Method 1: Look for the download button with aria-label
    if let Ok(selector) = Selector::parse("input.popsok[aria-label='Download file']") {
        if let Some(input) = document.select(&selector).next() {
            if let Some(href) = input.value().attr("href") {
                debug!("Found download URL via input.popsok selector");
                return Ok(href.to_string());
            }
        }
    }

    // Method 2: Look for download button anchor
    if let Ok(selector) = Selector::parse("a.downloadButton, a#downloadButton") {
        if let Some(a) = document.select(&selector).next() {
            if let Some(href) = a.value().attr("href") {
                debug!("Found download URL via downloadButton selector");
                return Ok(href.to_string());
            }
        }
    }

    // Method 3: Look for window.location.href redirect
    if let Some(start) = html.find("window.location.href = '") {
        let rest = &html[start + 24..];
        if let Some(end) = rest.find('\'') {
            let url = &rest[..end];
            if url.starts_with("http") {
                debug!("Found download URL via window.location.href");
                return Ok(url.to_string());
            }
        }
    }

    // Method 4: Look for direct download link in aria-label
    if let Ok(selector) = Selector::parse("[aria-label='Download file']") {
        if let Some(elem) = document.select(&selector).next() {
            if let Some(href) = elem.value().attr("href") {
                debug!("Found download URL via aria-label selector");
                return Ok(href.to_string());
            }
        }
    }

    // Check for error states
    if html.contains("Invalid or Deleted File") {
        bail!("MediaFire file was deleted or is invalid");
    }
    if html.contains("This file is no longer available") {
        bail!("MediaFire file is no longer available");
    }

    bail!("Could not extract download URL from MediaFire page")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_download_button() {
        let html = r#"<a class="downloadButton" href="https://download.mediafire.com/abc123/file.zip">Download</a>"#;
        let url = parse_mediafire_page(html).unwrap();
        assert_eq!(url, "https://download.mediafire.com/abc123/file.zip");
    }

    #[test]
    fn test_parse_window_location() {
        let html = r#"<script>window.location.href = 'https://download.mediafire.com/xyz/file.rar';</script>"#;
        let url = parse_mediafire_page(html).unwrap();
        assert_eq!(url, "https://download.mediafire.com/xyz/file.rar");
    }

    #[test]
    fn test_deleted_file() {
        let html = r#"<div>Invalid or Deleted File</div>"#;
        let result = parse_mediafire_page(html);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("deleted"));
    }
}
