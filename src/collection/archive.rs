//! Per-mod source dispatch for collection installs.
//!
//! Maps Vortex `source.type` values onto the existing per-source downloaders
//! in `crate::downloaders`. One call → one archive on disk, md5-verified.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use tracing::{info, warn};

use super::db::ModDbEntry;
use super::verify::compute_md5;
use crate::downloaders::{HttpClient, NexusDownloader};

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
}

/// Returns true when `path` already holds the expected archive.
fn archive_present(path: &Path, expected_md5: &str, expected_size: u64) -> bool {
    let Ok(meta) = std::fs::metadata(path) else {
        return false;
    };
    if meta.len() != expected_size {
        return false;
    }
    matches!(
        compute_md5(path),
        Ok(actual) if actual.eq_ignore_ascii_case(expected_md5)
    )
}

/// Fetch a single Vortex mod archive into `output_path`.
///
/// `game_domain` is the Nexus URL slug (e.g. `skyrimspecialedition`).
/// Premium-only Nexus path for now; free-tier NXM browser handoff is a
/// follow-up that will plug in alongside.
pub async fn fetch_one(
    mod_entry: &ModDbEntry,
    output_path: &Path,
    http: &HttpClient,
    nexus: &NexusDownloader,
    game_domain: &str,
) -> Result<FetchOutcome> {
    let expected_size = mod_entry.file_size as u64;

    if archive_present(output_path, &mod_entry.md5, expected_size) {
        return Ok(FetchOutcome::Cached(output_path.to_path_buf()));
    }
    let _ = std::fs::remove_file(output_path);

    match mod_entry.source_type.as_str() {
        "nexus" => fetch_nexus(mod_entry, output_path, http, nexus, game_domain)
            .await
            .map(FetchOutcome::Downloaded),
        "direct" => fetch_direct(&mod_entry.source_url, output_path, http, expected_size)
            .await
            .map(|_| FetchOutcome::Downloaded(output_path.to_path_buf())),
        "browse" | "manual" => Ok(FetchOutcome::Manual {
            url: mod_entry.source_url.clone(),
            notes: format!("source_type={}", mod_entry.source_type),
        }),
        "bundle" => Ok(FetchOutcome::Manual {
            url: String::new(),
            notes: "bundled archive — extract from collection.7z manually".into(),
        }),
        other => bail!("unknown source_type for mod '{}': {}", mod_entry.name, other),
    }
    .and_then(|outcome| match outcome {
        FetchOutcome::Downloaded(p) => verify_downloaded(&p, &mod_entry.md5, expected_size)
            .map(|_| FetchOutcome::Downloaded(p)),
        other => Ok(other),
    })
}

/// Verify a freshly downloaded archive matches expected md5 + size.
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
    Ok(())
}

/// Premium Nexus: fetch CDN URL via API key, then HTTP download.
async fn fetch_nexus(
    mod_entry: &ModDbEntry,
    output_path: &Path,
    http: &HttpClient,
    nexus: &NexusDownloader,
    game_domain: &str,
) -> Result<PathBuf> {
    if !nexus.is_premium() {
        bail!(
            "free-tier NXM browser handoff is not yet wired into the streaming installer; \
             use a Nexus Premium API key, or wait for the v2 NXM bridge"
        );
    }

    let url = nexus
        .get_download_link(
            game_domain,
            mod_entry.mod_id as u64,
            mod_entry.file_id as u64,
        )
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

    crate::downloaders::download_file_with_progress(
        http,
        &url,
        output_path,
        Some(mod_entry.file_size as u64),
    )
    .await
    .with_context(|| format!("HTTP download failed for {}", mod_entry.name))?;

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

