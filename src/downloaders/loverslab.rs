//! LoversLab automated downloader
//!
//! Uses LoversLab's authenticated IPS Downloads controller with numeric file
//! and resource IDs. Only the AJAX file chooser (or forum attachments) needs
//! HTML parsing; display slugs and page titles are not part of resolution.
//! The installer serializes downloads and verifies the modlist archive hash.

use super::http::ProgressCallback;
use anyhow::{bail, Context, Result};
use reqwest::cookie::Jar;
use reqwest::Client;
use reqwest::Url;
use scraper::{Html, Selector};
use std::path::Path;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tracing::info;

const BASE_URL: &str = "https://www.loverslab.com";
const USER_AGENT: &str = "Mozilla/5.0 (X11; Linux x86_64; rv:135.0) Gecko/20100101 Firefox/135.0";

/// Resolution needs a user-selected file rather than another automatic retry.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ManualDownloadRequired(pub String);

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
    wait: bool,
}

impl LoversLabDownloader {
    /// Create a new downloader by logging in with the given credentials.
    /// Fails if credentials are empty or the server does not confirm a login.
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
        if body.contains("Your account has been locked") || body.contains("You have been banned") {
            bail!("LoversLab account is locked or banned");
        }

        // IPS4 shows a specific error div on bad credentials
        if body.contains("ipsMessage ipsMessage_error")
            || body.contains("Login method does not exist")
            || body.contains("email address or password is incorrect")
        {
            bail!("LoversLab login failed: incorrect email or password");
        }

        // A challenge/error page must not be mistaken for a verified account.
        if !status.is_success() || !body.contains("/logout/") {
            bail!(
                "LoversLab login was not confirmed (HTTP {}); interactive sign-in may be required",
                status
            );
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
        self.download_with_callback(page_url, expected_name, output_path, None)
            .await
    }

    pub async fn download_with_callback(
        &self,
        page_url: &str,
        expected_name: &str,
        output_path: &Path,
        progress: Option<&ProgressCallback>,
    ) -> Result<()> {
        if !is_loverslab_url(page_url) {
            bail!("Expected a LoversLab URL");
        }
        if page_url.contains("/topic/") || page_url.contains("/blogs/entry/") {
            return self
                .download_forum_attachment(page_url, expected_name, output_path, progress)
                .await;
        }

        let download_url =
            controller_download_url(page_url, false).context("Unsupported LoversLab file URL")?;
        let resource_id = query_value(&download_url, "r");
        info!(
            "Resolving LoversLab file {} for {}",
            query_value(&download_url, "id").unwrap_or_default(),
            expected_name
        );

        // The same request the site's file chooser makes. Single-file pages
        // can return an attachment immediately; do not fetch them twice.
        let response = match self.request_download(&download_url, true).await? {
            DownloadResponse::Mega(url) => {
                return super::mega_native::download_mega_file_with_callback(
                    url.as_str(),
                    output_path,
                    progress,
                )
                .await
            }
            DownloadResponse::Http(response) => response,
        };
        if is_file_response(response.headers()) {
            return Self::stream_response_to_file(response, output_path, progress).await;
        }
        let html = response
            .text()
            .await
            .context("Failed to read LL file chooser")?;
        if html.contains("_processLogin") || html.contains("id=\"elSignIn_submit\"") {
            bail!("LoversLab session expired — re-login required");
        }
        let entries = parse_file_list(&html)?;
        if let Some(matched) = select_file_entry(expected_name, resource_id.as_deref(), &entries) {
            return self.download_entry(matched, output_path, progress).await;
        }
        // A pinned resource must never silently switch to another attachment.
        if resource_id.is_some() {
            if entries.is_empty() {
                if let Some(entry) = find_single_download_link(&html) {
                    let selected = Url::parse(&entry.download_url)
                        .ok()
                        .and_then(|url| query_value(&url, "r"));
                    if selected == resource_id {
                        return self.download_entry(&entry, output_path, progress).await;
                    }
                }
            }
            return Err(ManualDownloadRequired(format!(
                "LL resource for '{}' was not found",
                expected_name
            ))
            .into());
        }
        if entries.is_empty() {
            if let Some(entry) = find_single_download_link(&html) {
                return self.download_entry(&entry, output_path, progress).await;
            }
        }

        // Authors can attach additional files inside a download description.
        // These do not appear in the Downloads chooser (Tahrovin's SCOE pack
        // is one example). Query the same numeric record and match attachments.
        let mut detail_url = download_url.clone();
        let detail_query: Vec<_> = detail_url
            .query_pairs()
            .filter(|(key, _)| matches!(key.as_ref(), "app" | "module" | "controller" | "id"))
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        detail_url.set_query(None);
        detail_url.query_pairs_mut().extend_pairs(detail_query);
        self.download_forum_attachment(detail_url.as_str(), expected_name, output_path, progress)
            .await
    }

    async fn download_entry(
        &self,
        entry: &FileEntry,
        output_path: &Path,
        progress: Option<&ProgressCallback>,
    ) -> Result<()> {
        let url = controller_download_url(&entry.download_url, true)
            .or_else(|| Url::parse(&entry.download_url).ok())
            .context("Invalid LoversLab download action")?;
        if entry.wait {
            // IPS's downloads.front.view.download controller first requests a
            // JSON countdown, then navigates to the same URL after it expires.
            let response = match self.request_download(&url, true).await? {
                DownloadResponse::Http(response) => response,
                DownloadResponse::Mega(_) => {
                    bail!("Unexpected redirect while starting LL download countdown")
                }
            };
            let countdown: DownloadCountdown = response
                .json()
                .await
                .map_err(|e| e.without_url())
                .context("Invalid LL download countdown")?;
            let seconds = countdown.download.saturating_sub(countdown.current_time);
            if seconds > 300 {
                bail!("LoversLab requires a {} second wait; retry later", seconds);
            }
            info!("Waiting {} seconds for LoversLab download", seconds);
            tokio::time::sleep(std::time::Duration::from_secs(seconds)).await;
        }
        self.download_file(url.as_str(), output_path, progress)
            .await
    }

    /// Follow each redirect once, preserving external fragments (Mega keys).
    /// Cookies remain scoped by the shared cookie jar; AJAX headers stay on LL.
    async fn request_download(&self, initial: &Url, ajax: bool) -> Result<DownloadResponse> {
        let mut url = initial.clone();
        for _ in 0..10 {
            if !matches!(url.scheme(), "http" | "https") {
                bail!("Unsupported LoversLab download redirect scheme");
            }
            if matches!(url.domain(), Some("mega.nz" | "www.mega.nz")) {
                return Ok(DownloadResponse::Mega(url));
            }
            let mut request = self.no_redirect_client.get(url.clone());
            if ajax && is_loverslab_url(url.as_str()) {
                request = request.header("X-Requested-With", "XMLHttpRequest");
            }
            let response = request
                .send()
                .await
                .map_err(|e| e.without_url())
                .context("LoversLab download request failed")?;
            if response.status().is_redirection() {
                let location = response
                    .headers()
                    .get(reqwest::header::LOCATION)
                    .and_then(|value| value.to_str().ok())
                    .context("LL download redirect has no Location")?;
                url = url.join(location).context("Invalid LL download redirect")?;
                continue;
            }
            if !response.status().is_success() {
                bail!(
                    "LoversLab download request failed (HTTP {})",
                    response.status()
                );
            }
            return Ok(DownloadResponse::Http(response));
        }
        bail!("Too many LoversLab download redirects")
    }

    async fn download_file(
        &self,
        url: &str,
        output_path: &Path,
        progress: Option<&ProgressCallback>,
    ) -> Result<()> {
        let url = Url::parse(url).context("Invalid LL attachment URL")?;
        match self.request_download(&url, false).await? {
            DownloadResponse::Mega(url) => {
                super::mega_native::download_mega_file_with_callback(
                    url.as_str(),
                    output_path,
                    progress,
                )
                .await
            }
            DownloadResponse::Http(response) => {
                Self::stream_response_to_file(response, output_path, progress).await
            }
        }
    }

    /// Stream an HTTP response body to a file, verifying Content-Length.
    async fn stream_response_to_file(
        resp: reqwest::Response,
        output_path: &Path,
        progress: Option<&ProgressCallback>,
    ) -> Result<()> {
        use futures::StreamExt;

        if !resp.status().is_success() || !is_file_response(resp.headers()) {
            bail!(
                "LoversLab returned a page or error instead of a file (HTTP {})",
                resp.status()
            );
        }
        let expected_len = resp.content_length();
        let started = std::time::Instant::now();
        let mut last_report = started;
        if let Some(callback) = progress {
            callback(0, expected_len.unwrap_or(0), 0.0);
        }

        if let Some(parent) = output_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .context("Failed to create LL download directory")?;
        }

        let mut file = File::create(output_path)
            .await
            .with_context(|| format!("Failed to create file: {}", output_path.display()))?;

        let result: Result<u64> = async {
            let mut stream = resp.bytes_stream();
            let mut written: u64 = 0;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk
                    .map_err(|e| e.without_url())
                    .context("Error reading LL download stream")?;
                file.write_all(&chunk)
                    .await
                    .context("Failed to write chunk to disk")?;
                written += chunk.len() as u64;
                if last_report.elapsed() >= std::time::Duration::from_millis(250) {
                    if let Some(callback) = progress {
                        callback(
                            written,
                            expected_len.unwrap_or(0),
                            written as f64 / started.elapsed().as_secs_f64().max(0.001),
                        );
                    }
                    last_report = std::time::Instant::now();
                }
            }
            file.flush().await.context("Failed to flush LL download")?;
            if let Some(expected) = expected_len {
                if written != expected {
                    bail!(
                        "LL download incomplete: got {} bytes, expected {}",
                        written,
                        expected
                    );
                }
            }
            Ok(written)
        }
        .await;
        drop(file);
        if result.is_err() {
            let _ = tokio::fs::remove_file(output_path).await;
        }
        let written = result?;
        if let Some(callback) = progress {
            callback(
                written,
                expected_len.unwrap_or(written),
                written as f64 / started.elapsed().as_secs_f64().max(0.001),
            );
        }
        info!("Downloaded {} ({} bytes)", output_path.display(), written);

        Ok(())
    }

    /// Discover exact attachments in a forum, blog, or file description.
    async fn download_forum_attachment(
        &self,
        topic_url: &str,
        expected_name: &str,
        output_path: &Path,
        progress: Option<&ProgressCallback>,
    ) -> Result<()> {
        info!(
            "Looking for LL description/forum attachment: {}",
            expected_name
        );

        let page_html = self
            .client
            .get(topic_url)
            .send()
            .await
            .map_err(|e| e.without_url())
            .context("Failed to fetch LL topic")?
            .error_for_status()
            .map_err(|e| e.without_url())?
            .text()
            .await
            .context("Failed to read LL topic page")?;

        // Parse ipsAttachLink elements: <a class="ipsAttachLink" href="...attachment.php?id=X&key=Y">filename</a>
        let attachments = parse_forum_attachments(&page_html)?;

        let matched = match_filename(expected_name, &attachments).ok_or_else(|| {
            ManualDownloadRequired(format!(
                "Requested LL file '{}' was not found uniquely in the chooser or page attachments",
                expected_name
            ))
        })?;

        info!("Matched LL forum attachment: {}", matched.name);

        self.download_file(&matched.download_url, output_path, progress)
            .await
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

    let document = Html::parse_document(html);
    let selector = Selector::parse("input[name='csrfKey']").ok()?;
    document
        .select(&selector)
        .next()?
        .value()
        .attr("value")
        .map(str::to_string)
}

/// Parse the file list from a LoversLab `?do=download` page.
///
/// Each file entry is an `<li class='ipsDataItem'>` containing:
/// - `<h4 class='ipsDataItem_title'><span>filename</span></h4>`
/// - `<a href='...?do=download&r=ID&confirm=1&t=1&csrfKey=...' data-action="download">`
fn parse_file_list(html: &str) -> Result<Vec<FileEntry>> {
    let document = Html::parse_document(html);
    let item_selector =
        Selector::parse("li.ipsDataItem").map_err(|e| anyhow::anyhow!("Bad selector: {:?}", e))?;
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

        let Some(link) = item.select(&link_selector).next() else {
            continue;
        };
        // scraper already decodes entities in the server's action URL.
        let Some(href) = link.value().attr("href") else {
            continue;
        };
        let url = Url::parse(BASE_URL)?.join(href)?;

        if !name.is_empty() {
            entries.push(FileEntry {
                name,
                download_url: url.to_string(),
                wait: link
                    .value()
                    .attr("data-wait")
                    .is_some_and(|value| !value.is_empty()),
            });
        }
    }

    Ok(entries)
}

/// A confirmation dialog with a single explicit download action.
fn find_single_download_link(html: &str) -> Option<FileEntry> {
    let document = Html::parse_document(html);
    let selector = Selector::parse("a[data-action='download']").ok()?;
    let mut links = document.select(&selector);
    let link = links.next()?;
    if links.next().is_some() {
        return None;
    }
    Some(FileEntry {
        name: String::new(),
        download_url: Url::parse(BASE_URL)
            .ok()?
            .join(link.value().attr("href")?)
            .ok()?
            .to_string(),
        wait: link
            .value()
            .attr("data-wait")
            .is_some_and(|value| !value.is_empty()),
    })
}

enum DownloadResponse {
    Http(reqwest::Response),
    Mega(Url),
}

#[derive(serde::Deserialize)]
struct DownloadCountdown {
    download: u64,
    #[serde(rename = "currentTime")]
    current_time: u64,
}

fn query_value(url: &Url, key: &str) -> Option<String> {
    url.query_pairs()
        .find_map(|(k, v)| (k == key).then(|| v.into_owned()))
}

/// IPS's non-friendly route avoids all dependence on a file's display slug.
/// Preserve only the server's download action parameters. The initial lookup
/// deliberately drops any stale CSRF/confirmation parameters in a modlist.
fn controller_download_url(input: &str, confirmed: bool) -> Option<Url> {
    let source = Url::parse(input).ok()?;
    if !is_loverslab_url(input) {
        return None;
    }
    let file_id = if let Some(rest) = source.path().strip_prefix("/files/file/") {
        rest.chars()
            .take_while(char::is_ascii_digit)
            .collect::<String>()
    } else if source.path() == "/index.php"
        && query_value(&source, "app").as_deref() == Some("downloads")
        && query_value(&source, "module").as_deref() == Some("downloads")
        && query_value(&source, "controller").as_deref() == Some("view")
    {
        query_value(&source, "id")?
    } else {
        return None;
    };
    if file_id.is_empty() || !file_id.bytes().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let mut url = Url::parse(&format!("{BASE_URL}/index.php")).ok()?;
    url.query_pairs_mut()
        .append_pair("app", "downloads")
        .append_pair("module", "downloads")
        .append_pair("controller", "view")
        .append_pair("id", &file_id)
        .append_pair("do", "download");
    // Literal '?' replacements in old modlist slugs can precede the real
    // query delimiter, so find r= only at a query boundary, before fragments.
    let resource = input
        .split('#')
        .next()?
        .split(['?', '&'])
        .find_map(|part| part.strip_prefix("r="));
    if let Some(resource) = resource {
        if resource.is_empty() || !resource.bytes().all(|c| c.is_ascii_digit()) {
            return None;
        }
        url.query_pairs_mut().append_pair("r", resource);
    }
    if confirmed {
        for key in ["confirm", "t", "csrfKey"] {
            if let Some(value) = query_value(&source, key) {
                url.query_pairs_mut().append_pair(key, &value);
            }
        }
    }
    Some(url)
}

/// Reject error pages/JSON even when they use HTTP 200. The response must be
/// an attachment or a recognized archive/binary type before opening the file.
fn is_file_response(headers: &reqwest::header::HeaderMap) -> bool {
    let content_type = headers
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if content_type.starts_with("text/")
        || content_type.contains("json")
        || content_type.contains("xml")
        || content_type.contains("javascript")
    {
        return false;
    }
    let attachment = headers
        .get(reqwest::header::CONTENT_DISPOSITION)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(';')
                .next()
                .unwrap_or_default()
                .trim()
                .eq_ignore_ascii_case("attachment")
        });
    attachment
        || matches!(
            content_type.as_str(),
            "application/octet-stream"
                | "application/zip"
                | "application/x-zip-compressed"
                | "application/x-7z-compressed"
                | "application/x-rar-compressed"
                | "application/vnd.rar"
                | "application/x-rar"
                | "application/rar"
                | "application/download"
                | "application/force-download"
        )
}

/// Match exact filenames, allowing only case/whitespace and Wabbajack's
/// duplicate suffix. Never guess a different version from a shared prefix.
fn select_file_entry<'a>(
    expected: &str,
    resource_id: Option<&str>,
    entries: &'a [FileEntry],
) -> Option<&'a FileEntry> {
    let Some(resource_id) = resource_id else {
        return match_filename(expected, entries);
    };
    let mut matches = entries.iter().filter(|entry| {
        Url::parse(&entry.download_url)
            .ok()
            .and_then(|url| query_value(&url, "r"))
            .as_deref()
            == Some(resource_id)
    });
    let entry = matches.next()?;
    matches.next().is_none().then_some(entry)
}

fn match_filename<'a>(expected: &str, entries: &'a [FileEntry]) -> Option<&'a FileEntry> {
    let normalize = |name: &str| {
        name.split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_lowercase()
    };
    let expected = normalize(expected);
    let clean = strip_duplicate_suffix(&expected);
    for target in [&expected, &clean] {
        let mut matches = entries
            .iter()
            .filter(|entry| normalize(&entry.name) == *target);
        if let Some(entry) = matches.next() {
            return matches.next().is_none().then_some(entry);
        }
    }
    None
}

/// Strip trailing ` (1)`, ` (2)`, etc. from a filename.
fn strip_duplicate_suffix(name: &str) -> String {
    // Match pattern like "filename (1).ext" -> "filename.ext"
    if let Some(paren_start) = name.rfind(" (") {
        let after_paren = &name[paren_start + 2..];
        if let Some(paren_end) = after_paren.find(')') {
            let between = &after_paren[..paren_end];
            if !between.is_empty()
                && between.chars().all(|c| c.is_ascii_digit())
                && (after_paren[paren_end + 1..].starts_with('.')
                    || after_paren[paren_end + 1..].is_empty())
            {
                // It's a duplicate suffix like " (1)" — remove it
                let before = &name[..paren_start];
                let after = &after_paren[paren_end + 1..];
                return format!("{}{}", before, after);
            }
        }
    }
    name.to_string()
}

/// Parse forum post attachments (ipsAttachLink elements).
fn parse_forum_attachments(html: &str) -> Result<Vec<FileEntry>> {
    let document = Html::parse_document(html);
    let selector =
        Selector::parse("a.ipsAttachLink").map_err(|e| anyhow::anyhow!("Bad selector: {:?}", e))?;

    let mut entries = Vec::new();

    for link in document.select(&selector) {
        let name = link.text().collect::<String>().trim().to_string();
        let Some(href) = link.value().attr("href") else {
            continue;
        };
        let url = Url::parse(BASE_URL)?.join(href)?.to_string();

        if !name.is_empty() && url.contains("attachment.php") {
            entries.push(FileEntry {
                name,
                download_url: url,
                wait: false,
            });
        }
    }

    Ok(entries)
}

/// Check if a URL is a LoversLab URL.
pub fn is_loverslab_url(url: &str) -> bool {
    Url::parse(url).is_ok_and(|url| {
        matches!(url.scheme(), "http" | "https")
            && matches!(url.domain(), Some("loverslab.com" | "www.loverslab.com"))
            && url.username().is_empty()
            && url.password().is_none()
            && url.port().is_none()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn controller_uses_ids_even_with_damaged_slugs() {
        let url = controller_download_url(
            "https://www.loverslab.com/files/file/2438-broken-???-slug/?do=download&r=656247&confirm=1&csrfKey=stale#ignored",
            false,
        ).unwrap();
        assert_eq!(url.path(), "/index.php");
        assert_eq!(query_value(&url, "id").as_deref(), Some("2438"));
        assert_eq!(query_value(&url, "r").as_deref(), Some("656247"));
        assert!(query_value(&url, "confirm").is_none());
        assert!(query_value(&url, "csrfKey").is_none());
        assert_eq!(controller_download_url(url.as_str(), false), Some(url));
    }

    #[test]
    fn controller_preserves_server_action_and_rejects_unrelated_urls() {
        let url = controller_download_url(
            "https://www.loverslab.com/files/file/13011-display/?do=download&r=959512&confirm=1&t=1&csrfKey=fresh",
            true,
        ).unwrap();
        assert_eq!(query_value(&url, "csrfKey").as_deref(), Some("fresh"));
        assert_eq!(query_value(&url, "confirm").as_deref(), Some("1"));
        assert_eq!(query_value(&url, "r").as_deref(), Some("959512"));
        for input in [
            "https://example.com/loverslab.com/files/file/123/",
            "https://loverslab.com.example.com/files/file/123/",
            "https://www.loverslab.com/files/file/no-id/",
            "https://www.loverslab.com/files/file/123/?r=invalid",
            "https://www.loverslab.com/topic/123/",
        ] {
            assert!(controller_download_url(input, false).is_none(), "{input}");
        }
        let url = controller_download_url(
            "https://www.loverslab.com/files/file/123/?other=456#r=789",
            false,
        )
        .unwrap();
        assert!(query_value(&url, "r").is_none());
    }

    #[test]
    fn missing_versions_and_ambiguous_names_are_not_guessed() {
        let entries = vec![FileEntry {
            name: "Mod v2.0.7z".into(),
            download_url: "https://example.com/file".into(),
            wait: false,
        }];
        assert!(match_filename("Mod v1.0.7z", &entries).is_none());
        assert!(match_filename("Mod v2.0.7z", &[entries[0].clone(), entries[0].clone()]).is_none());
        assert!(match_filename("  MOD   v2.0 (2).7z", &entries).is_some());
    }

    #[test]
    fn pinned_resource_cannot_fall_back_to_a_different_file() {
        let entries = vec![
            FileEntry {
                name: "Mod.7z".into(),
                download_url: "https://www.loverslab.com/files/file/123/?do=download&r=100".into(),
                wait: false,
            },
            FileEntry {
                name: "Older Mod.7z".into(),
                download_url: "https://www.loverslab.com/files/file/123/?do=download&r=200".into(),
                wait: false,
            },
        ];
        assert_eq!(
            select_file_entry("Mod.7z", Some("200"), &entries)
                .unwrap()
                .name,
            "Older Mod.7z"
        );
        assert!(select_file_entry("Mod.7z", Some("300"), &entries).is_none());
        assert_eq!(
            select_file_entry("Mod.7z", None, &entries).unwrap().name,
            "Mod.7z"
        );
    }

    #[test]
    fn chooser_needs_no_global_csrf_and_retains_countdown() {
        let html = r#"<li class='ipsDataItem'><h4 class='ipsDataItem_title'><span>Mod.7z</span></h4>
            <a data-action='download' data-wait='1' href='/files/file/123-mod/?do=download&amp;r=456&amp;csrfKey=fresh'>Download</a></li>"#;
        assert!(extract_csrf_key(html).is_none());
        let entries = parse_file_list(html).unwrap();
        assert_eq!(entries.len(), 1);
        assert!(entries[0].wait);
        assert!(entries[0].download_url.contains("&csrfKey=fresh"));
        assert!(find_single_download_link(html).unwrap().wait);
        assert!(find_single_download_link(&format!("{html}{html}")).is_none());
    }

    #[test]
    fn only_binary_responses_are_files() {
        use reqwest::header::{HeaderMap, HeaderValue, CONTENT_DISPOSITION, CONTENT_TYPE};
        let mut headers = HeaderMap::new();
        assert!(!is_file_response(&headers));
        for content_type in [
            "text/html",
            "application/json",
            "application/problem+json",
            "application/xhtml+xml",
        ] {
            headers.insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
            assert!(!is_file_response(&headers));
        }
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/x-7z-compressed"),
        );
        assert!(is_file_response(&headers));
        headers.remove(CONTENT_TYPE);
        headers.insert(
            CONTENT_DISPOSITION,
            HeaderValue::from_static("attachment; filename=mod.7z"),
        );
        assert!(is_file_response(&headers));
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("text/html"));
        assert!(!is_file_response(&headers));
    }

    fn test_client() -> LoversLabDownloader {
        LoversLabDownloader {
            client: Client::builder().no_proxy().build().unwrap(),
            no_redirect_client: Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap(),
        }
    }

    async fn server(
        responses: Vec<(&'static str, &'static str)>,
    ) -> (Url, tokio::task::JoinHandle<()>) {
        use tokio::io::AsyncReadExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!("http://{}/start", listener.local_addr().unwrap())).unwrap();
        let task = tokio::spawn(async move {
            for (path, response) in responses {
                let (mut socket, _) =
                    tokio::time::timeout(std::time::Duration::from_secs(5), listener.accept())
                        .await
                        .unwrap()
                        .unwrap();
                let mut request = Vec::new();
                while !request.windows(4).any(|part| part == b"\r\n\r\n") {
                    let mut buf = [0; 1024];
                    let read = socket.read(&mut buf).await.unwrap();
                    assert!(read > 0);
                    request.extend_from_slice(&buf[..read]);
                }
                assert!(
                    String::from_utf8_lossy(&request).starts_with(&format!("GET {path} HTTP/1.1"))
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        (url, task)
    }

    #[tokio::test]
    async fn follows_redirects_once_and_streams_the_original_response() {
        let (url, server) = server(vec![
            ("/start", "HTTP/1.1 302 Found\r\nLocation: /file\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"),
            ("/file", "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: 4\r\nConnection: close\r\n\r\ndata"),
        ]).await;
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("file.7z");
        test_client()
            .download_file(url.as_str(), &output, None)
            .await
            .unwrap();
        assert_eq!(std::fs::read(output).unwrap(), b"data");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn preserves_mega_fragment_after_intermediate_redirect() {
        let (url, server) = server(vec![
            ("/start", "HTTP/1.1 302 Found\r\nLocation: /next\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"),
            ("/next", "HTTP/1.1 302 Found\r\nLocation: https://mega.nz/file/example#secret-key\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"),
        ]).await;
        let DownloadResponse::Mega(url) = test_client().request_download(&url, true).await.unwrap()
        else {
            panic!("Expected Mega redirect");
        };
        assert_eq!(url.fragment(), Some("secret-key"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_pages_and_http_errors_without_overwriting_files() {
        for response in [
            "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: 5\r\nConnection: close\r\n\r\nlogin",
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
            "HTTP/1.1 429 Too Many Requests\r\nContent-Type: application/octet-stream\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        ] {
            let (url, server) = server(vec![("/start", response)]).await;
            let dir = tempfile::tempdir().unwrap();
            let output = dir.path().join("file.7z");
            std::fs::write(&output, b"existing").unwrap();
            assert!(test_client().download_file(url.as_str(), &output, None).await.is_err());
            assert_eq!(std::fs::read(output).unwrap(), b"existing");
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn removes_truncated_downloads() {
        let (url, server) = server(vec![("/start",
            "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: 100\r\nConnection: close\r\n\r\nshort",
        )]).await;
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("file.7z");
        assert!(test_client()
            .download_file(url.as_str(), &output, None)
            .await
            .is_err());
        assert!(!output.exists());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn countdown_handshake_precedes_transfer() {
        let (url, server) = server(vec![
            ("/start", "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"download\":100,\"currentTime\":100}"),
            ("/start", "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: 4\r\nConnection: close\r\n\r\ndata"),
        ]).await;
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("file.7z");
        let entry = FileEntry {
            name: "file.7z".into(),
            download_url: url.to_string(),
            wait: true,
        };
        test_client()
            .download_entry(&entry, &output, None)
            .await
            .unwrap();
        assert_eq!(std::fs::read(output).unwrap(), b"data");
        server.await.unwrap();
    }

    /// Verify small LL files, or one CLF3_LL_TEST_ARCHIVE, from a real modlist.
    #[tokio::test]
    #[ignore]
    async fn test_live_modlist_small_files() {
        let selected = std::env::var("CLF3_LL_TEST_ARCHIVE").ok();
        let path = std::env::var("CLF3_LL_TEST_MODLIST").expect("CLF3_LL_TEST_MODLIST not set");
        let mut zip = zip::ZipArchive::new(std::fs::File::open(path).unwrap()).unwrap();
        let modlist: serde_json::Value =
            serde_json::from_reader(zip.by_name("modlist").unwrap()).unwrap();
        let email = std::env::var("LOVERSLAB_EMAIL").expect("LOVERSLAB_EMAIL not set");
        let password = std::env::var("LOVERSLAB_PASSWORD").expect("LOVERSLAB_PASSWORD not set");
        let ll = LoversLabDownloader::login(&email, &password).await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let mut verified = 0;
        for archive in modlist["Archives"].as_array().unwrap() {
            let Some(url) = archive["State"]["Url"].as_str() else {
                continue;
            };
            let size = archive["Size"].as_u64().unwrap();
            let name = archive["Name"].as_str().unwrap();
            if !is_loverslab_url(url)
                || selected
                    .as_ref()
                    .map_or(size >= 25 * 1024, |selected| selected != name)
            {
                continue;
            }
            let output = dir.path().join(format!("archive-{verified}"));
            ll.download(url, name, &output)
                .await
                .unwrap_or_else(|e| panic!("{name}: {e:#}"));
            assert_eq!(std::fs::metadata(&output).unwrap().len(), size, "{name}");
            assert!(
                crate::hash::verify_file_hash(&output, archive["Hash"].as_str().unwrap()).unwrap(),
                "{name}"
            );
            println!("Verified {name}: {size} bytes, modlist hash matches");
            verified += 1;
        }
        assert!(verified > 0, "No small LL archives in modlist");
    }

    #[test]
    fn test_is_loverslab_url() {
        assert!(is_loverslab_url(
            "https://www.loverslab.com/files/file/12760-creature-overhaul/?do=download"
        ));
        assert!(is_loverslab_url(
            "https://loverslab.com/files/file/5878-devious-devices-se/"
        ));
        assert!(!is_loverslab_url(
            "https://www.nexusmods.com/skyrim/mods/123"
        ));
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
    fn test_strip_duplicate_suffix() {
        assert_eq!(
            strip_duplicate_suffix("co more creatures 1.8.2 (with hostile creatures) (1).rar"),
            "co more creatures 1.8.2 (with hostile creatures).rar"
        );
        assert_eq!(strip_duplicate_suffix("file (2).7z"), "file.7z");
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
                wait: false,
            },
            FileEntry {
                name: "Mod v2.0.rar".to_string(),
                download_url: "http://example.com/2".to_string(),
                wait: false,
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
                wait: false,
            },
            FileEntry {
                name: "CO More Creatures 1.8.2 (With Hostile Creatures).rar".to_string(),
                download_url: "http://example.com/2".to_string(),
                wait: false,
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

        let entries = parse_file_list(html).unwrap();
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
            meta.len(),
            actual,
            expected_hash,
            matches
        );
        assert!(
            matches,
            "Hash mismatch: got {} expected {}",
            actual, expected_hash
        );

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
            meta.len(),
            actual,
            expected_hash,
            matches
        );
        assert!(
            matches,
            "Hash mismatch: got {} expected {}",
            actual, expected_hash
        );

        let _ = std::fs::remove_file(&output);
    }
}
