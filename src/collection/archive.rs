//! Per-mod source dispatch for collection installs.
//!
//! Maps Vortex `source.type` values onto the existing per-source downloaders
//! in `crate::downloaders`. One call → one archive on disk, md5-verified.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use tracing::{info, warn};

use super::db::ModDbEntry;
use super::verify::compute_md5;
use crate::downloaders::{with_retry, HttpClient, NexusDownloader, MAX_RETRIES};
use crate::nxm_handler::NxmLink;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Shared state for the free-tier NXM flow: the listener receiver (mutex'd
/// so only one fetch waits on it at a time) and the browser command used to
/// open Nexus mod pages.
pub struct NxmContext {
    pub rx: Mutex<tokio::sync::mpsc::UnboundedReceiver<NxmLink>>,
    pub browser: String,
}

/// Outcome of a single mod's fetch attempt.
pub enum FetchOutcome {
    /// File already on disk, hash matched — nothing transferred.
    Cached(PathBuf),
    /// Newly downloaded and hash-verified.
    Downloaded(PathBuf),
    /// Source needs human action (browse URL, bundled archive, etc.).
    Manual {
        url: String,
        notes: String,
    },
    /// Vortex `bundle` source: payload was bundled into the collection ZIP
    /// under `<collection_root>/bundled/Bundled - <sanitized name>/`. Path
    /// is the resolved directory; install pipeline copies its contents into
    /// the mod folder.
    Bundled(PathBuf),
}

/// Sidecar file holding the last successful md5 verification of an archive.
/// Lets reruns skip a multi-GB rehash when size+mtime are unchanged.
#[derive(serde::Serialize, serde::Deserialize)]
struct VerifiedSidecar {
    size: u64,
    mtime_secs: i64,
    md5: String,
}

const VERIFIED_SUFFIX: &str = ".clf3-verified";

fn verified_sidecar_path(archive: &Path) -> std::path::PathBuf {
    let mut p = archive.as_os_str().to_owned();
    p.push(VERIFIED_SUFFIX);
    std::path::PathBuf::from(p)
}

fn read_verified_sidecar(archive: &Path) -> Option<VerifiedSidecar> {
    let bytes = std::fs::read(verified_sidecar_path(archive)).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn write_verified_sidecar(archive: &Path, md5: &str) {
    let Ok(meta) = std::fs::metadata(archive) else {
        return;
    };
    let mtime_secs = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let sidecar = VerifiedSidecar {
        size: meta.len(),
        mtime_secs,
        md5: md5.to_string(),
    };
    if let Ok(json) = serde_json::to_vec(&sidecar) {
        let _ = std::fs::write(verified_sidecar_path(archive), json);
    }
}

/// Returns true when `path` already holds the expected archive. Uses a
/// sidecar (size+mtime → cached md5) to skip rehashing unchanged files;
/// falls back to a streaming md5 when the sidecar is absent or stale.
fn archive_present(path: &Path, expected_md5: &str, expected_size: u64) -> bool {
    let Ok(meta) = std::fs::metadata(path) else {
        return false;
    };
    if meta.len() != expected_size {
        return false;
    }

    // Fast path: sidecar matches current size+mtime AND records the expected
    // md5. The mtime check guards against external edits between runs.
    if let Some(sidecar) = read_verified_sidecar(path) {
        let mtime_secs = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        if sidecar.size == meta.len()
            && sidecar.mtime_secs == mtime_secs
            && sidecar.md5.eq_ignore_ascii_case(expected_md5)
        {
            return true;
        }
    }

    match compute_md5(path) {
        Ok(actual) if actual.eq_ignore_ascii_case(expected_md5) => {
            write_verified_sidecar(path, &actual);
            true
        }
        _ => false,
    }
}

/// Fetch a single Vortex mod archive into `output_path`.
///
/// `game_domain` is the Nexus URL slug (e.g. `skyrimspecialedition`).
/// When `nxm_ctx` is `Some`, free-tier Nexus downloads use the NXM
/// browser-handoff flow; otherwise a Nexus Premium API key is required.
pub async fn fetch_one(
    mod_entry: &ModDbEntry,
    output_path: &Path,
    http: &HttpClient,
    nexus: &NexusDownloader,
    game_domain: &str,
    nxm_ctx: Option<&Arc<NxmContext>>,
    collection_root: &Path,
) -> Result<FetchOutcome> {
    let expected_size = mod_entry.file_size as u64;

    if archive_present(output_path, &mod_entry.md5, expected_size) {
        return Ok(FetchOutcome::Cached(output_path.to_path_buf()));
    }
    let _ = std::fs::remove_file(output_path);

    match mod_entry.source_type.as_str() {
        "nexus" => fetch_nexus(mod_entry, output_path, http, nexus, game_domain, nxm_ctx)
            .await
            .map(FetchOutcome::Downloaded),
        "direct" => fetch_direct(&mod_entry.source_url, output_path, http, expected_size)
            .await
            .map(|_| FetchOutcome::Downloaded(output_path.to_path_buf())),
        "browse" | "manual" => Ok(FetchOutcome::Manual {
            url: mod_entry.source_url.clone(),
            notes: format!("source_type={}", mod_entry.source_type),
        }),
        "bundle" => match find_bundled_dir(collection_root, &mod_entry.name) {
            Some(p) => Ok(FetchOutcome::Bundled(p)),
            None => Ok(FetchOutcome::Manual {
                url: String::new(),
                notes: format!(
                    "bundled payload missing under {}/bundled/",
                    collection_root.display()
                ),
            }),
        },
        other => bail!("unknown source_type for mod '{}': {}", mod_entry.name, other),
    }
    .and_then(|outcome| match outcome {
        FetchOutcome::Downloaded(p) => verify_downloaded(&p, &mod_entry.md5, expected_size)
            .map(|_| FetchOutcome::Downloaded(p)),
        other => Ok(other),
    })
}

/// Vortex's filename sanitizer (mirrors `util.sanitizeFilename` used in
/// `transformCollection.ts` for bundle directory naming): replace each of
/// `\/:*?"<>|` with `_` and trim trailing spaces/dots.
fn vortex_sanitize_filename(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' | '/' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => out.push('_'),
            _ => out.push(c),
        }
    }
    out.trim_end_matches([' ', '.']).to_string()
}

/// Locate `<collection_root>/bundled/Bundled - <sanitized mod name>/` —
/// falls back to a case-insensitive scan when the exact path doesn't exist
/// (Vortex's renderModName may include a version suffix that we don't have
/// at install time).
fn find_bundled_dir(collection_root: &Path, mod_name: &str) -> Option<PathBuf> {
    let bundled_root = collection_root.join("bundled");
    let want = format!("Bundled - {}", vortex_sanitize_filename(mod_name));
    let exact = bundled_root.join(&want);
    if exact.is_dir() {
        return Some(exact);
    }
    let want_lower = want.to_lowercase();
    let want_prefix_lower = format!("{}-", want_lower); // matches "Bundled - X-1.0"
    if let Ok(read) = std::fs::read_dir(&bundled_root) {
        for entry in read.flatten() {
            let name = entry.file_name();
            let name_lower = name.to_string_lossy().to_lowercase();
            if (name_lower == want_lower || name_lower.starts_with(&want_prefix_lower))
                && entry.path().is_dir()
            {
                return Some(entry.path());
            }
        }
    }
    None
}

/// Verify a freshly downloaded archive matches expected md5 + size.
/// Persists a `<archive>.clf3-verified` sidecar on success so the next
/// `archive_present` check can skip the rehash.
fn verify_downloaded(path: &Path, expected_md5: &str, expected_size: u64) -> Result<()> {
    let meta = std::fs::metadata(path)
        .with_context(|| format!("downloaded archive missing: {}", path.display()))?;
    if meta.len() != expected_size {
        bail!(
            "size mismatch: expected {} bytes, got {}",
            expected_size,
            meta.len()
        );
    }
    let actual = compute_md5(path)?;
    if !actual.eq_ignore_ascii_case(expected_md5) {
        bail!("md5 mismatch: expected {}, got {}", expected_md5, actual);
    }
    write_verified_sidecar(path, &actual);
    Ok(())
}

/// Premium Nexus: fetch CDN URL via API key, then HTTP download.
/// Free-tier: open the Nexus mod page in the user's browser, wait for the
/// nxm:// click to land in `nxm_ctx.rx`, then download via the URL embedded
/// in the link.
async fn fetch_nexus(
    mod_entry: &ModDbEntry,
    output_path: &Path,
    http: &HttpClient,
    nexus: &NexusDownloader,
    game_domain: &str,
    nxm_ctx: Option<&Arc<NxmContext>>,
) -> Result<PathBuf> {
    if !nexus.is_premium() {
        let ctx = nxm_ctx.ok_or_else(|| {
            anyhow::anyhow!(
                "free-tier Nexus install requires the NXM listener; \
                 install_collection_streaming() should have started one"
            )
        })?;
        return fetch_nexus_via_nxm(mod_entry, output_path, http, ctx, game_domain).await;
    }

    // CDN link fetch is cheap; we still retry it because Nexus 5xx are
    // common during peak load.
    let url = with_retry(&format!("nexus link {}", mod_entry.name), MAX_RETRIES, || async {
        nexus
            .get_download_link(
                game_domain,
                mod_entry.mod_id as u64,
                mod_entry.file_id as u64,
            )
            .await
            .map_err(anyhow::Error::from)
    })
    .await
    .with_context(|| {
        format!(
            "Nexus link fetch failed: {}/{}/{}",
            game_domain, mod_entry.mod_id, mod_entry.file_id
        )
    })?;

    info!(
        "Downloading [{}]: {} ({} bytes)",
        mod_entry.source_type, mod_entry.name, mod_entry.file_size
    );

    let url_ref = &url;
    let name_ref = &mod_entry.name;
    let expected_size = mod_entry.file_size as u64;
    with_retry(&format!("download {}", mod_entry.name), MAX_RETRIES, || async {
        crate::downloaders::download_file_with_progress(
            http,
            url_ref,
            output_path,
            Some(expected_size),
        )
        .await
        .with_context(|| format!("HTTP download failed for {}", name_ref))?;
        // Enforce size INSIDE the retry block. The Nexus CDN sometimes
        // closes the connection after a partial body and our HTTP layer
        // accepts EOF as "done". Without this check the post-retry
        // `verify_downloaded` would fail without ever retrying.
        check_full_download(output_path, expected_size)
    })
    .await?;

    Ok(output_path.to_path_buf())
}

/// Verify a freshly-completed download is fully on disk. Drops a partial
/// file before returning Err so the next retry starts from a clean slate.
fn check_full_download(path: &Path, expected_size: u64) -> Result<()> {
    if expected_size == 0 {
        return Ok(()); // Source didn't declare a size; size-check disabled.
    }
    let actual = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    if actual != expected_size {
        let _ = std::fs::remove_file(path);
        bail!(
            "truncated download: got {} bytes, expected {} ({})",
            actual,
            expected_size,
            path.display()
        );
    }
    Ok(())
}

/// Free-tier Nexus: open the Nexus mod-file page in the user's browser, wait
/// for the matching nxm:// link to come in over the listener socket, then
/// download via the URL inside that link.
async fn fetch_nexus_via_nxm(
    mod_entry: &ModDbEntry,
    output_path: &Path,
    http: &HttpClient,
    ctx: &Arc<NxmContext>,
    game_domain: &str,
) -> Result<PathBuf> {
    use serde::Deserialize;

    let mod_id = mod_entry.mod_id as u64;
    let file_id = mod_entry.file_id as u64;
    let want_key = format!("{}:{}:{}", game_domain, mod_id, file_id);
    let page_url = crate::nxm_handler::nexus_mod_url(game_domain, mod_id, file_id);

    info!(
        "[free-tier] Opening Nexus page for {}: {}",
        mod_entry.name, page_url
    );
    if let Err(e) = std::process::Command::new(&ctx.browser).arg(&page_url).spawn() {
        warn!(
            "browser '{}' failed to launch for NXM handoff: {e}; \
             paste this URL into a browser and click 'Download with Manager': {}",
            ctx.browser, page_url
        );
    }
    info!("[free-tier] Click 'Download with Manager' to continue.");

    // Drain the listener until we get a link matching this mod. Stray links
    // for other mods (user clicked ahead) are dropped — sequential mode (the
    // caller pins concurrency to 1) keeps that simple.
    let link = {
        let mut rx = ctx.rx.lock().await;
        loop {
            let next = rx
                .recv()
                .await
                .ok_or_else(|| anyhow::anyhow!("NXM listener closed"))?;
            if next.lookup_key() == want_key {
                break next;
            }
            warn!(
                "Got NXM link for unrelated file {}; expected {} — discarding",
                next.lookup_key(),
                want_key
            );
        }
    };

    // The /download_link.json endpoint returns one CDN URL per call; needs
    // the Apikey header for free-tier (the key + expires from the link are
    // already baked into the URL but Nexus still requires the header).
    #[derive(Deserialize)]
    struct CdnEntry {
        #[serde(rename = "URI")]
        uri: String,
    }

    let api_url = link.api_url();
    // The link already carries the per-click `key` + `expires`; the inner
    // reqwest client does not need an API key header for this endpoint.
    let resp = http
        .inner()
        .get(&api_url)
        .send()
        .await
        .with_context(|| format!("nxm download_link.json: {api_url}"))?;
    if !resp.status().is_success() {
        bail!(
            "nxm download_link.json HTTP {}: {}",
            resp.status(),
            resp.text().await.unwrap_or_default()
        );
    }
    let entries: Vec<CdnEntry> = resp
        .json()
        .await
        .context("parse nxm download_link.json response")?;
    let cdn_url = entries
        .first()
        .map(|e| e.uri.clone())
        .ok_or_else(|| anyhow::anyhow!("Nexus returned no CDN URLs"))?;

    info!(
        "Downloading [{}]: {} ({} bytes)",
        mod_entry.source_type, mod_entry.name, mod_entry.file_size
    );

    let cdn_url_ref = cdn_url.as_str();
    let name_ref = &mod_entry.name;
    let expected_size = mod_entry.file_size as u64;
    with_retry(&format!("download {}", mod_entry.name), MAX_RETRIES, || async {
        crate::downloaders::download_file_with_progress(
            http,
            cdn_url_ref,
            output_path,
            Some(expected_size),
        )
        .await
        .with_context(|| format!("HTTP download failed for {}", name_ref))
    })
    .await?;

    Ok(output_path.to_path_buf())
}

/// Direct URL: straight HTTP download.
async fn fetch_direct(
    url: &str,
    output_path: &Path,
    http: &HttpClient,
    expected_size: u64,
) -> Result<()> {
    if url.is_empty() {
        bail!("direct source has no URL");
    }
    if let Err(e) = crate::downloaders::download_file_with_progress(
        http,
        url,
        output_path,
        Some(expected_size),
    )
    .await
    {
        warn!("direct download failed: {:#}", e);
        return Err(e).context("direct HTTP download failed");
    }
    Ok(())
}

