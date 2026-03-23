//! LoversLab automated downloader
//!
//! Logs into LoversLab (IPS4 forum) with email/password, scrapes the
//! download page for each file, matches the expected filename, and
//! downloads the correct attachment.  Downloads are sequential (one at
//! a time) to avoid rate-limiting.

use anyhow::{bail, Context, Result};
use reqwest::cookie::Jar;
use reqwest::Client;
use scraper::{Html, Selector};
use std::path::Path;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tracing::{debug, info, warn};

const BASE_URL: &str = "https://www.loverslab.com";
const USER_AGENT: &str =
    "Mozilla/5.0 (X11; Linux x86_64; rv:135.0) Gecko/20100101 Firefox/135.0";

/// A logged-in LoversLab session that can download files.
pub struct LoversLabDownloader {
    client: Client,
    /// Same cookies but doesn't follow redirects — used to capture Location headers
    no_redirect_client: Client,
}

/// Info about a single downloadable file on a LoversLab download page.
#[derive(Debug, Clone)]
struct FileEntry {
    name: String,
    download_url: String,
}

impl LoversLabDownloader {
    /// Create a new downloader by logging in with the given credentials.
    /// Returns `None` if credentials are empty.
    pub async fn login(email: &str, password: &str) -> Result<Self> {
        if email.is_empty() || password.is_empty() {
            bail!("LoversLab credentials not configured");
        }

        info!("Logging into LoversLab...");

        let cookie_jar = Arc::new(Jar::default());

        let client = Client::builder()
            .user_agent(USER_AGENT)
            .cookie_provider(cookie_jar.clone())
            .redirect(reqwest::redirect::Policy::limited(10))
            .build()
            .context("Failed to create LoversLab HTTP client")?;

        let no_redirect_client = Client::builder()
            .user_agent(USER_AGENT)
            .cookie_provider(cookie_jar)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .context("Failed to create LoversLab no-redirect client")?;

        // Step 1: GET the login page to obtain csrfKey + session cookies
        let login_page = client
            .get(format!("{}/login/", BASE_URL))
            .send()
            .await
            .context("Failed to load LoversLab login page")?
            .text()
            .await
            .context("Failed to read login page")?;

        let csrf_key = extract_csrf_key(&login_page)
            .context("Could not find csrfKey on LoversLab login page")?;

        debug!("Got LoversLab csrfKey: {}...", &csrf_key[..8.min(csrf_key.len())]);

        // Step 2: POST login form
        let params = [
            ("csrfKey", csrf_key.as_str()),
            ("auth", email),
            ("password", password),
            ("remember_me", "1"),
            ("_processLogin", "usernamepassword"),
        ];

        let login_resp = client
            .post(format!("{}/login/", BASE_URL))
            .form(&params)
            .send()
            .await
            .context("LoversLab login request failed")?;

        let status = login_resp.status();
        let body = login_resp.text().await.unwrap_or_default();

        // A successful login redirects (302/303) back to the homepage.
        // If we get the login form again with an error, login failed.
        if body.contains("Your account has been locked")
            || body.contains("You have been banned")
        {
            bail!("LoversLab account is locked or banned");
        }

        // IPS4 shows a specific error div on bad credentials
        if body.contains("ipsMessage ipsMessage_error")
            || body.contains("Login method does not exist")
            || body.contains("email address or password is incorrect")
        {
            bail!("LoversLab login failed: incorrect email or password");
        }

        // Verify we're actually logged in by checking for a logout link
        if !body.contains("/logout/") && !status.is_redirection() {
            // Try to detect what went wrong
            warn!(
                "LoversLab login may have failed (status={}, body_len={})",
                status,
                body.len()
            );
            // Don't bail — the redirect may have consumed the body, and
            // cookie-based auth might still work on subsequent requests.
        }

        info!("LoversLab login successful");

        Ok(Self {
            client,
            no_redirect_client,
        })
    }

    /// Download a LoversLab file to `output_path`.
    ///
    /// * `page_url` — the LoversLab page URL from the modlist (may or may not have `?do=download`)
    /// * `expected_name` — the filename we're looking for (from the archive name / prompt)
    /// * `output_path` — where to save the downloaded file
    pub async fn download(
        &self,
        page_url: &str,
        expected_name: &str,
        output_path: &Path,
    ) -> Result<()> {
        // Forum topic URLs have attachments embedded in posts, not download pages
        if page_url.contains("/topic/") {
            return self
                .download_forum_attachment(page_url, expected_name, output_path)
                .await;
        }

        // Ensure the URL has ?do=download so we get the file list page
        let download_page_url = ensure_download_url(page_url);

        info!(
            "Fetching LoversLab download page: {}",
            download_page_url
        );

        // Use no-redirect client to capture Location headers (Mega redirects have #key in fragment)
        let initial_resp = self
            .no_redirect_client
            .get(&download_page_url)
            .send()
            .await
            .with_context(|| format!("Failed to fetch LL page: {}", download_page_url))?;

        // Handle redirects manually
        if initial_resp.status().is_redirection() {
            if let Some(location) = initial_resp.headers().get("location") {
                let location_str = location.to_str().unwrap_or("");
                if location_str.contains("mega.nz") {
                    info!(
                        "LL redirects to Mega (with key): {} -> {}",
                        download_page_url, location_str
                    );
                    return super::mega_native::download_mega_file(location_str, output_path)
                        .await
                        .with_context(|| {
                            format!(
                                "Mega download failed for {} (redirected from LL: {})",
                                expected_name, download_page_url
                            )
                        });
                }
            }
            // Non-Mega redirect — follow with the normal client
        }

        // Fetch the page with the normal client (follows redirects, has full cookie support)
        let response = self
            .client
            .get(&download_page_url)
            .send()
            .await
            .with_context(|| format!("Failed to fetch LL page: {}", download_page_url))?;

        // Check if the response is already a file download (not HTML)
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();

        if !content_type.contains("text/html") && !content_type.is_empty() {
            info!(
                "LL returned direct download (content-type: {}): {}",
                content_type, expected_name
            );
            return Self::stream_response_to_file(response, output_path).await;
        }

        let page_html = response
            .text()
            .await
            .context("Failed to read LL download page")?;

        // Check if we got redirected to login (session expired)
        if page_html.contains("id=\"elSignIn_submit\"") || page_html.contains("_processLogin") {
            bail!("LoversLab session expired — re-login required");
        }

        // Check if the page contains a Mega link instead of a direct download
        if let Some(mega_url) = extract_mega_url(&page_html) {
            info!(
                "LL page contains Mega link: {} -> {}",
                download_page_url, mega_url
            );
            return super::mega_native::download_mega_file(&mega_url, output_path)
                .await
                .with_context(|| {
                    format!(
                        "Mega download failed for {} (linked from LL: {})",
                        expected_name, download_page_url
                    )
                });
        }

        let csrf_key = match extract_csrf_key(&page_html) {
            Some(key) => key,
            None => {
                // Log diagnostic info to help debug
                let page_len = page_html.len();
                let has_cloudflare = page_html.contains("Just a moment");
                let has_login = page_html.contains("elSignIn");
                let title = page_html
                    .find("<title>")
                    .and_then(|start| {
                        let rest = &page_html[start + 7..];
                        rest.find("</title>").map(|end| &rest[..end])
                    })
                    .unwrap_or("unknown");
                warn!(
                    "LL page missing csrfKey for {}: title='{}', len={}, cloudflare={}, login={}",
                    expected_name, title, page_len, has_cloudflare, has_login
                );
                bail!(
                    "Could not find csrfKey on LL download page (page title: '{}', {} bytes)",
                    title, page_len
                );
            }
        };

        // Parse the file list
        let entries = parse_file_list(&page_html, &csrf_key)?;

        if entries.is_empty() {
            // Might be a single-file download — check if there's a direct download link
            if let Some(direct_url) = find_single_download_link(&page_html, &csrf_key) {
                info!("Single-file download detected for {}", expected_name);
                return self.download_file(&direct_url, output_path).await;
            }
            bail!(
                "No downloadable files found on LL page: {}",
                download_page_url
            );
        }

        info!(
            "Found {} files on LL page, looking for: {}",
            entries.len(),
            expected_name
        );

        // Match the expected filename against the available files
        let matched = match_filename(expected_name, &entries)
            .with_context(|| {
                let available: Vec<_> = entries.iter().map(|e| e.name.as_str()).collect();
                format!(
                    "Could not match '{}' against available files: {:?}",
                    expected_name, available
                )
            })?;

        info!(
            "Matched '{}' -> '{}' ({})",
            expected_name, matched.name, matched.download_url
        );

        self.download_file(&matched.download_url, output_path).await
    }

    /// Download a file from a resolved URL to disk, streaming to avoid truncation.
    async fn download_file(&self, url: &str, output_path: &Path) -> Result<()> {
        let resp = self
            .client
            .get(url)
            .send()
            .await
            .with_context(|| format!("Failed to download from LL: {}", url))?;

        if !resp.status().is_success() {
            bail!(
                "LoversLab download failed with status {}: {}",
                resp.status(),
                url
            );
        }

        Self::stream_response_to_file(resp, output_path).await
    }

    /// Stream an HTTP response body to a file, verifying Content-Length.
    async fn stream_response_to_file(
        resp: reqwest::Response,
        output_path: &Path,
    ) -> Result<()> {
        use futures::StreamExt;

        let expected_len = resp.content_length();

        if let Some(parent) = output_path.parent() {
            tokio::fs::create_dir_all(parent).await.ok();
        }

        let mut file = File::create(output_path)
            .await
            .with_context(|| format!("Failed to create file: {}", output_path.display()))?;

        let mut stream = resp.bytes_stream();
        let mut written: u64 = 0;

        while let Some(chunk) = stream.next().await {
            let chunk = chunk.context("Error reading LL download stream")?;
            tokio::io::AsyncWriteExt::write_all(&mut file, &chunk)
                .await
                .context("Failed to write chunk to disk")?;
            written += chunk.len() as u64;
        }

        tokio::io::AsyncWriteExt::flush(&mut file).await.ok();

        // Verify we got the complete file
        if let Some(expected) = expected_len {
            if written != expected {
                let _ = tokio::fs::remove_file(output_path).await;
                bail!(
                    "LL download incomplete: got {} bytes, expected {} (connection dropped)",
                    written,
                    expected
                );
            }
        }

        info!(
            "Downloaded {} ({} bytes)",
            output_path.display(),
            written
        );

        Ok(())
    }

    /// Download a file from a LoversLab forum topic page.
    /// Forum posts have attachments as `ipsAttachLink` elements with direct download URLs.
    async fn download_forum_attachment(
        &self,
        topic_url: &str,
        expected_name: &str,
        output_path: &Path,
    ) -> Result<()> {
        info!("Fetching LL forum topic for attachment: {}", topic_url);

        let page_html = self
            .client
            .get(topic_url)
            .send()
            .await
            .with_context(|| format!("Failed to fetch LL topic: {}", topic_url))?
            .text()
            .await
            .context("Failed to read LL topic page")?;

        // Parse ipsAttachLink elements: <a class="ipsAttachLink" href="...attachment.php?id=X&key=Y">filename</a>
        let attachments = parse_forum_attachments(&page_html)?;

        if attachments.is_empty() {
            bail!("No attachments found on LL forum topic: {}", topic_url);
        }

        info!(
            "Found {} attachments on forum page, looking for: {}",
            attachments.len(),
            expected_name
        );

        let matched = match_filename(expected_name, &attachments).with_context(|| {
            let available: Vec<_> = attachments.iter().map(|e| e.name.as_str()).collect();
            format!(
                "Could not match '{}' against forum attachments: {:?}",
                expected_name, available
            )
        })?;

        info!(
            "Matched forum attachment '{}' -> '{}' ({})",
            expected_name, matched.name, matched.download_url
        );

        self.download_file(&matched.download_url, output_path).await
    }
}

/// Extract csrfKey from page HTML.
/// Looks for the JS variable `csrfKey: "..."` or hidden input.
fn extract_csrf_key(html: &str) -> Option<String> {
    // Method 1: JS variable — csrfKey: "abc123",
    if let Some(start) = html.find("csrfKey: \"") {
        let rest = &html[start + 10..];
        if let Some(end) = rest.find('"') {
            return Some(rest[..end].to_string());
        }
    }

    // Method 2: Hidden form input
    let needle = "name=\"csrfKey\" value=\"";
    if let Some(start) = html.find(needle) {
        let rest = &html[start + needle.len()..];
        if let Some(end) = rest.find('"') {
            return Some(rest[..end].to_string());
        }
    }

    None
}

/// Parse the file list from a LoversLab `?do=download` page.
///
/// Each file entry is an `<li class='ipsDataItem'>` containing:
/// - `<h4 class='ipsDataItem_title'><span>filename</span></h4>`
/// - `<a href='...?do=download&r=ID&confirm=1&t=1&csrfKey=...' data-action="download">`
fn parse_file_list(html: &str, _csrf_key: &str) -> Result<Vec<FileEntry>> {
    let document = Html::parse_document(html);
    let item_selector = Selector::parse("li.ipsDataItem")
        .map_err(|e| anyhow::anyhow!("Bad selector: {:?}", e))?;
    let title_selector = Selector::parse("h4.ipsDataItem_title span")
        .map_err(|e| anyhow::anyhow!("Bad selector: {:?}", e))?;
    let link_selector = Selector::parse("a[data-action='download']")
        .map_err(|e| anyhow::anyhow!("Bad selector: {:?}", e))?;

    let mut entries = Vec::new();

    for item in document.select(&item_selector) {
        let name = match item.select(&title_selector).next() {
            Some(el) => el.text().collect::<String>().trim().to_string(),
            None => continue,
        };

        let url = match item.select(&link_selector).next() {
            Some(el) => match el.value().attr("href") {
                Some(href) => {
                    // HTML entities are decoded by scraper, but ensure full URL
                    let href = href.replace("&amp;", "&");
                    if href.starts_with("http") {
                        href
                    } else {
                        format!("{}{}", BASE_URL, href)
                    }
                }
                None => continue,
            },
            None => continue,
        };

        if !name.is_empty() {
            entries.push(FileEntry {
                name,
                download_url: url,
            });
        }
    }

    Ok(entries)
}

/// For single-file download pages, look for a direct download link.
fn find_single_download_link(html: &str, _csrf_key: &str) -> Option<String> {
    let document = Html::parse_document(html);

    // Look for a download button/link with data-action="download"
    let selector = Selector::parse("a[data-action='download']").ok()?;
    let link = document.select(&selector).next()?;
    let href = link.value().attr("href")?;

    let href = href.replace("&amp;", "&");
    if href.starts_with("http") {
        Some(href)
    } else {
        Some(format!("{}{}", BASE_URL, href))
    }
}

/// Ensure the URL points to the download file list page.
fn ensure_download_url(url: &str) -> String {
    // Strip fragment (#...) from URL first — fragments break the download page
    let url = if let Some(hash_pos) = url.find('#') {
        &url[..hash_pos]
    } else {
        url
    };
    let url = url.trim_end_matches('/');

    // Strip any existing query params that aren't relevant
    let base = if let Some(pos) = url.find("?do=download") {
        // Keep just the base + ?do=download, strip r=, confirm=, csrfKey= etc
        let base_end = pos + "?do=download".len();
        &url[..base_end]
    } else if url.contains('?') {
        // Has other params but not do=download
        return format!("{}&do=download", url);
    } else {
        // No query params at all — add ?do=download
        return if url.ends_with('/') {
            format!("{}?do=download", url)
        } else {
            format!("{}/?do=download", url)
        };
    };
    base.to_string()
}

/// Match an expected filename against the available file entries.
///
/// Tries exact match first, then progressively fuzzier matching:
/// 1. Exact match (case-insensitive)
/// 2. Match after stripping trailing " (1)", " (2)" etc. from expected name
/// 3. Match checking if expected name starts with / contains file entry name
/// 4. Best substring match
fn match_filename<'a>(expected: &str, entries: &'a [FileEntry]) -> Option<&'a FileEntry> {
    let expected_lower = expected.to_lowercase();

    // Strip trailing " (N)" suffix that Wabbajack adds for duplicate filenames
    let expected_clean = strip_duplicate_suffix(&expected_lower);

    // Normalize whitespace for comparison (collapse multiple spaces into one)
    let normalize_ws = |s: &str| -> String {
        s.split_whitespace().collect::<Vec<_>>().join(" ")
    };
    let expected_norm = normalize_ws(&expected_clean);

    // 1. Exact match (with whitespace normalization)
    if let Some(entry) = entries.iter().find(|e| {
        let entry_lower = e.name.to_lowercase();
        entry_lower == expected_lower
            || entry_lower == expected_clean
            || normalize_ws(&entry_lower) == expected_norm
    }) {
        return Some(entry);
    }

    // 2. Check if entry name is contained within expected name or vice versa
    if let Some(entry) = entries.iter().find(|e| {
        let entry_lower = e.name.to_lowercase();
        expected_clean.contains(&entry_lower) || entry_lower.contains(&expected_clean)
    }) {
        return Some(entry);
    }

    // 4. Best match by longest common prefix (after stripping extension)
    let expected_stem = strip_extension(&expected_clean);
    let mut best: Option<(usize, &FileEntry)> = None;
    for entry in entries {
        let entry_stem = strip_extension(&entry.name.to_lowercase());
        let common = common_prefix_len(&expected_stem, &entry_stem);
        if common > 0 {
            if best.map_or(true, |(best_len, _)| common > best_len) {
                best = Some((common, entry));
            }
        }
    }

    best.map(|(_, entry)| entry)
}

/// Strip trailing ` (1)`, ` (2)`, etc. from a filename.
fn strip_duplicate_suffix(name: &str) -> String {
    // Match pattern like "filename (1).ext" -> "filename.ext"
    if let Some(paren_start) = name.rfind(" (") {
        let after_paren = &name[paren_start + 2..];
        if let Some(paren_end) = after_paren.find(')') {
            let between = &after_paren[..paren_end];
            if between.chars().all(|c| c.is_ascii_digit()) {
                // It's a duplicate suffix like " (1)" — remove it
                let before = &name[..paren_start];
                let after = &after_paren[paren_end + 1..];
                return format!("{}{}", before, after);
            }
        }
    }
    name.to_string()
}

/// Strip file extension from a name.
fn strip_extension(name: &str) -> String {
    if let Some(dot) = name.rfind('.') {
        name[..dot].to_string()
    } else {
        name.to_string()
    }
}

/// Length of the common prefix between two strings.
fn common_prefix_len(a: &str, b: &str) -> usize {
    a.chars()
        .zip(b.chars())
        .take_while(|(ca, cb)| ca == cb)
        .count()
}

/// Extract a Mega URL from page HTML if present.
fn extract_mega_url(html: &str) -> Option<String> {
    // Look for mega.nz links in href attributes
    if let Some(start) = html.find("https://mega.nz/") {
        let rest = &html[start..];
        // Find the end of the URL (quote, space, or angle bracket)
        let end = rest
            .find(|c: char| c == '"' || c == '\'' || c == ' ' || c == '<' || c == '>')
            .unwrap_or(rest.len());
        let url = &rest[..end];
        if url.len() > 20 {
            return Some(url.to_string());
        }
    }
    // Also check for old-style mega.nz/#! links
    if let Some(start) = html.find("https://mega.nz/#!") {
        let rest = &html[start..];
        let end = rest
            .find(|c: char| c == '"' || c == '\'' || c == ' ' || c == '<' || c == '>')
            .unwrap_or(rest.len());
        let url = &rest[..end];
        if url.len() > 20 {
            return Some(url.to_string());
        }
    }
    None
}

/// Parse forum post attachments (ipsAttachLink elements).
fn parse_forum_attachments(html: &str) -> Result<Vec<FileEntry>> {
    let document = Html::parse_document(html);
    let selector = Selector::parse("a.ipsAttachLink")
        .map_err(|e| anyhow::anyhow!("Bad selector: {:?}", e))?;

    let mut entries = Vec::new();

    for link in document.select(&selector) {
        let name = link.text().collect::<String>().trim().to_string();
        let url = match link.value().attr("href") {
            Some(href) => {
                let href = href.replace("&amp;", "&");
                if href.starts_with("http") {
                    href
                } else {
                    format!("{}{}", BASE_URL, href)
                }
            }
            None => continue,
        };

        if !name.is_empty() && url.contains("attachment.php") {
            entries.push(FileEntry {
                name,
                download_url: url,
            });
        }
    }

    Ok(entries)
}

/// Check if a URL is a LoversLab URL.
pub fn is_loverslab_url(url: &str) -> bool {
    url.contains("loverslab.com/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_loverslab_url() {
        assert!(is_loverslab_url(
            "https://www.loverslab.com/files/file/12760-creature-overhaul/?do=download"
        ));
        assert!(is_loverslab_url(
            "https://loverslab.com/files/file/5878-devious-devices-se/"
        ));
        assert!(!is_loverslab_url("https://www.nexusmods.com/skyrim/mods/123"));
    }

    #[test]
    fn test_extract_csrf_key_js() {
        let html = r#"var ips = { csrfKey: "53eb28fff7deedb14701406491369816", };"#;
        assert_eq!(
            extract_csrf_key(html).unwrap(),
            "53eb28fff7deedb14701406491369816"
        );
    }

    #[test]
    fn test_extract_csrf_key_hidden_input() {
        let html = r#"<input type="hidden" name="csrfKey" value="abc123def456">"#;
        assert_eq!(extract_csrf_key(html).unwrap(), "abc123def456");
    }

    #[test]
    fn test_ensure_download_url() {
        assert_eq!(
            ensure_download_url("https://www.loverslab.com/files/file/123-mod/"),
            "https://www.loverslab.com/files/file/123-mod/?do=download"
        );
        assert_eq!(
            ensure_download_url("https://www.loverslab.com/files/file/123-mod/?do=download"),
            "https://www.loverslab.com/files/file/123-mod/?do=download"
        );
        assert_eq!(
            ensure_download_url(
                "https://www.loverslab.com/files/file/123-mod/?do=download&r=456&confirm=1"
            ),
            "https://www.loverslab.com/files/file/123-mod/?do=download"
        );
    }

    #[test]
    fn test_strip_duplicate_suffix() {
        assert_eq!(
            strip_duplicate_suffix("co more creatures 1.8.2 (with hostile creatures) (1).rar"),
            "co more creatures 1.8.2 (with hostile creatures).rar"
        );
        assert_eq!(
            strip_duplicate_suffix("file (2).7z"),
            "file.7z"
        );
        // Don't strip non-numeric parens
        assert_eq!(
            strip_duplicate_suffix("file (with stuff).7z"),
            "file (with stuff).7z"
        );
    }

    #[test]
    fn test_match_filename_exact() {
        let entries = vec![
            FileEntry {
                name: "Mod v1.0.rar".to_string(),
                download_url: "http://example.com/1".to_string(),
            },
            FileEntry {
                name: "Mod v2.0.rar".to_string(),
                download_url: "http://example.com/2".to_string(),
            },
        ];
        let result = match_filename("Mod v2.0.rar", &entries).unwrap();
        assert_eq!(result.name, "Mod v2.0.rar");
    }

    #[test]
    fn test_match_filename_with_duplicate_suffix() {
        let entries = vec![
            FileEntry {
                name: "CO More Creatures 1.8.2.rar".to_string(),
                download_url: "http://example.com/1".to_string(),
            },
            FileEntry {
                name: "CO More Creatures 1.8.2 (With Hostile Creatures).rar".to_string(),
                download_url: "http://example.com/2".to_string(),
            },
        ];
        let result = match_filename(
            "CO More Creatures 1.8.2 (With Hostile Creatures) (1).rar",
            &entries,
        )
        .unwrap();
        assert_eq!(
            result.name,
            "CO More Creatures 1.8.2 (With Hostile Creatures).rar"
        );
    }

    #[test]
    fn test_parse_file_list() {
        let html = r#"
            <ul>
                <li class='ipsDataItem'>
                    <div class='ipsDataItem_main'>
                        <h4 class='ipsDataItem_title ipsContained_container'>
                            <span class='ipsType_break ipsContained'>TestFile v1.0.rar</span>
                        </h4>
                    </div>
                    <div class='ipsDataItem_generic'>
                        <a href='https://www.loverslab.com/files/file/123-test/?do=download&amp;r=456&amp;confirm=1&amp;t=1&amp;csrfKey=abc123' data-action="download">Download</a>
                    </div>
                </li>
                <li class='ipsDataItem'>
                    <div class='ipsDataItem_main'>
                        <h4 class='ipsDataItem_title ipsContained_container'>
                            <span class='ipsType_break ipsContained'>TestFile v2.0.rar</span>
                        </h4>
                    </div>
                    <div class='ipsDataItem_generic'>
                        <a href='https://www.loverslab.com/files/file/123-test/?do=download&amp;r=789&amp;confirm=1&amp;t=1&amp;csrfKey=abc123' data-action="download">Download</a>
                    </div>
                </li>
            </ul>
        "#;

        let entries = parse_file_list(html, "abc123").unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].name, "TestFile v1.0.rar");
        assert_eq!(entries[1].name, "TestFile v2.0.rar");
        assert!(entries[0].download_url.contains("r=456"));
        assert!(entries[1].download_url.contains("r=789"));
    }

    /// Integration test: login + download a small file from LoversLab, verify hash.
    /// Run with: LOVERSLAB_EMAIL=x LOVERSLAB_PASSWORD=y cargo test --lib downloaders::loverslab::tests::test_live_download -- --ignored
    #[tokio::test]
    #[ignore]
    async fn test_live_download() {
        let email = std::env::var("LOVERSLAB_EMAIL").expect("LOVERSLAB_EMAIL not set");
        let password = std::env::var("LOVERSLAB_PASSWORD").expect("LOVERSLAB_PASSWORD not set");

        let ll = LoversLabDownloader::login(&email, &password)
            .await
            .expect("Login failed");

        // ES DDI Patch.7z — 1,686 bytes, single file on a multi-file page
        let url = "https://www.loverslab.com/files/file/2438-estrus-chaurus-spider-addon-%E2%95%B2-%E2%80%A2%CC%80-%CF%89-%E2%80%A2%CC%81-%E2%95%B1/?do=download&r=656247";
        let output = std::env::temp_dir().join("clf3_ll_test_es_ddi_patch.7z");

        let _ = std::fs::remove_file(&output);

        ll.download(url, "ES DDI Patch.7z", &output)
            .await
            .expect("Download failed");

        assert!(output.exists(), "Output file should exist");
        let meta = std::fs::metadata(&output).unwrap();
        assert_eq!(meta.len(), 1686, "File size should match");

        // Verify hash matches what the modlist expects
        let expected_hash = "qcloPquVeKc=";
        let (matches, actual) =
            crate::hash::verify_file_hash_detailed(&output, expected_hash).unwrap();
        println!(
            "ES DDI Patch.7z: {} bytes, hash={} expected={} match={}",
            meta.len(), actual, expected_hash, matches
        );
        assert!(matches, "Hash mismatch: got {} expected {}", actual, expected_hash);

        let _ = std::fs::remove_file(&output);
    }

    /// Integration test: multi-file page where we must match by filename, verify hash.
    /// Run with: LOVERSLAB_EMAIL=x LOVERSLAB_PASSWORD=y cargo test --lib downloaders::loverslab::tests::test_live_multi_file -- --ignored
    #[tokio::test]
    #[ignore]
    async fn test_live_multi_file() {
        let email = std::env::var("LOVERSLAB_EMAIL").expect("LOVERSLAB_EMAIL not set");
        let password = std::env::var("LOVERSLAB_PASSWORD").expect("LOVERSLAB_PASSWORD not set");

        let ll = LoversLabDownloader::login(&email, &password)
            .await
            .expect("Login failed");

        // Creature Overhaul — 6 files on the page, we want the "(With Hostile Creatures)" variant
        // URL has NO r= parameter, just the page — downloader must scrape + match
        let url = "https://www.loverslab.com/files/file/12760-creature-overhaul/?do=download";
        let expected_name = "CO More Creatures 1.8.2 (With Hostile Creatures) (1).rar";
        let output = std::env::temp_dir().join("clf3_ll_test_co_hostile.rar");

        let _ = std::fs::remove_file(&output);

        ll.download(url, expected_name, &output)
            .await
            .expect("Download failed");

        assert!(output.exists(), "Output file should exist");
        let meta = std::fs::metadata(&output).unwrap();
        assert_eq!(meta.len(), 208053, "File size should match");

        // Verify hash matches what the modlist expects
        let expected_hash = "UeMYfB43FU8=";
        let (matches, actual) =
            crate::hash::verify_file_hash_detailed(&output, expected_hash).unwrap();
        println!(
            "CO Hostile Creatures: {} bytes, hash={} expected={} match={}",
            meta.len(), actual, expected_hash, matches
        );
        assert!(matches, "Hash mismatch: got {} expected {}", actual, expected_hash);

        let _ = std::fs::remove_file(&output);
    }

}
