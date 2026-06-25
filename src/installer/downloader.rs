#![allow(dead_code)] // Used by lib crate
//! Download phase coordinator
//!
//! Coordinates downloading all archives from various sources with parallel downloads.
//! Supports direct API mode (premium) and manual browser mode (non-premium).

use crate::downloaders::{
    download_file_with_callback, GoogleDriveDownloader, HttpClient, LoversLabDownloader,
    MediaFireDownloader, NexusDownloader, ProgressCallback as HttpProgressCallback,
    WabbajackCdnDownloader, YandexDownloader,
};
use crate::hash::{verify_file_hash, verify_file_hash_detailed};
use crate::modlist::{ArchiveInfo, DownloadState, ModlistDb};

use super::config::{InstallConfig, ProgressEvent};
use super::progress::{ProgressHandle, ProgressReporter};

use anyhow::{bail, Context, Result};
use futures::stream::{self, StreamExt};
use rayon::prelude::*;
use regex::Regex;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

/// Build set of BSA temp_ids whose output already has a valid sidecar.
/// Used to skip downloading archives only needed for BSAs that are already built.
fn build_valid_bsa_set(
    db: &ModlistDb,
    config: &InstallConfig,
) -> std::collections::HashSet<String> {
    let mut valid = std::collections::HashSet::new();
    let bsa_directives = db
        .get_all_pending_directives_of_type("CreateBSA")
        .unwrap_or_default();
    for (_id, json) in bsa_directives {
        if let Ok(crate::modlist::Directive::CreateBSA(d)) = serde_json::from_str(&json) {
            let output_path = crate::paths::join_windows_path(&config.output_dir, &d.to);
            if crate::installer::sidecar::sidecar_valid(&output_path, &d.hash) {
                valid.insert(d.temp_id.to_string());
            }
        }
    }
    if !valid.is_empty() {
        config.reporter.log(&format!(
            "Found {} BSAs with valid sidecar cache (skipping their source downloads)",
            valid.len()
        ));
    }
    valid
}

/// Check if a directive's to_path is a BSA staging path for a valid BSA.
fn is_bsa_staging_path(to_path: &str, valid_temp_ids: &std::collections::HashSet<String>) -> bool {
    if !to_path.contains("TEMP_BSA_FILES") {
        return false;
    }
    let parts: Vec<&str> = to_path.split(['/', '\\']).collect();
    if let Some(idx) = parts.iter().position(|&p| p == "TEMP_BSA_FILES") {
        if let Some(temp_id) = parts.get(idx + 1) {
            return valid_temp_ids.contains(*temp_id);
        }
    }
    false
}

fn resolve_game_file_source_archive(
    config: &InstallConfig,
    archive: &ArchiveInfo,
) -> Option<PathBuf> {
    if !archive.state_json.contains("GameFileSourceDownloader") {
        return None;
    }

    let Ok(DownloadState::GameFileSource(state)) =
        serde_json::from_str::<DownloadState>(&archive.state_json)
    else {
        return None;
    };

    let game_file = &state.game_file;
    crate::paths::resolve_case_insensitive(&config.game_dir, game_file).or_else(|| {
        let data_path = format!("Data/{}", game_file);
        crate::paths::resolve_case_insensitive(&config.game_dir, &data_path)
    })
}

/// Max retries for network operations
const MAX_RETRIES: u32 = 3;
/// Delay between retries
const RETRY_DELAY: Duration = Duration::from_secs(2);
/// Max retries for rate limit errors (429)
const MAX_RATE_LIMIT_RETRIES: u32 = 10;
/// Initial delay for rate limit retry (increases exponentially)
const RATE_LIMIT_BASE_DELAY: Duration = Duration::from_secs(30);

/// Wabbajack proxy endpoint for Google Drive, Mega, MediaFire downloads
const PROXY_BASE_URL: &str = "https://build.wabbajack.org/proxy";

/// Wabbajack mirror endpoint (files stored on CDN by hash)
const MIRROR_BASE_URL: &str = "https://mirror.wabbajack.org";
/// Browser-like UA helps avoid ModDB anti-bot blocks on start/mirror pages.
const MODDB_USER_AGENT: &str =
    "Mozilla/5.0 (X11; Linux x86_64; rv:135.0) Gecko/20100101 Firefox/135.0";

/// Result tuple for parallel downloads: (name, path, result, optional nexus state update)
type DownloadResultTuple = (String, PathBuf, DownloadResult, Option<(String, i64)>);

/// Scan output directory and return HashMap of normalized_path -> file_size
fn scan_existing_outputs(output_dir: &Path) -> Result<HashMap<String, u64>> {
    let mut existing = HashMap::new();

    if !output_dir.exists() {
        return Ok(existing);
    }

    for entry in walkdir::WalkDir::new(output_dir)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        if entry.file_type().is_file() {
            if let Ok(rel_path) = entry.path().strip_prefix(output_dir) {
                let normalized = crate::paths::normalize_for_lookup(&rel_path.to_string_lossy());
                if let Ok(meta) = entry.metadata() {
                    existing.insert(normalized, meta.len());
                }
            }
        }
    }

    Ok(existing)
}

/// Download statistics
#[derive(Debug, Default, Clone)]
pub struct DownloadStats {
    pub downloaded: usize,
    pub skipped: usize,
    pub failed: usize,
    pub manual: usize,
    /// Details of failed downloads
    pub failed_downloads: Vec<FailedDownloadInfo>,
    /// Details of manual downloads needed
    pub manual_downloads: Vec<ManualDownloadInfo>,
}

/// Information about a manual download needed
#[derive(Debug, Clone, serde::Serialize)]
pub struct ManualDownloadInfo {
    pub name: String,
    pub url: String,
    pub prompt: Option<String>,
    pub expected_size: u64,
}

/// Information about a failed download
#[derive(Debug, Clone, serde::Serialize)]
pub struct FailedDownloadInfo {
    pub name: String,
    pub url: String,
    pub error: String,
    pub expected_size: u64,
}

/// Shared state for download coordination
struct DownloadContext {
    nexus: NexusDownloader,
    http: HttpClient,
    cdn: WabbajackCdnDownloader,
    gdrive: GoogleDriveDownloader,
    mediafire: MediaFireDownloader,
    yandex: YandexDownloader,
    /// LoversLab downloader (only present when credentials are configured)
    loverslab: Option<LoversLabDownloader>,
    /// Semaphore to enforce sequential LoversLab downloads (LL rate-limits concurrent requests)
    ll_semaphore: tokio::sync::Semaphore,
    config: InstallConfig,
    reporter: Arc<dyn ProgressReporter>,
    // Counters
    downloaded: AtomicUsize,
    skipped: AtomicUsize,
    failed: AtomicUsize,
    manual_downloads: Mutex<Vec<ManualDownloadInfo>>,
    failed_downloads: Mutex<Vec<FailedDownloadInfo>>,
    // Progress tracking for callbacks
    completed_archives: AtomicUsize,
    total_archives: usize,
}

impl DownloadContext {
    /// Create a progress handle for a download item.
    fn begin_download(&self, name: &str, total_bytes: u64) -> Arc<dyn ProgressHandle> {
        self.reporter.begin_item(name, Some(total_bytes))
    }
}

/// Helper: build a DownloadContext from config and pending count.
async fn build_context(config: &InstallConfig, total_archives: usize) -> Result<DownloadContext> {
    let loverslab = init_loverslab(config).await;
    Ok(DownloadContext {
        nexus: NexusDownloader::from_config(
            &config.nexus_api_key,
            config.nexus_oauth_token.as_deref(),
        )?,
        http: HttpClient::new()?,
        cdn: WabbajackCdnDownloader::new()?,
        gdrive: GoogleDriveDownloader::new()?,
        mediafire: MediaFireDownloader::new()?,
        yandex: YandexDownloader::new()?,
        loverslab,
        ll_semaphore: tokio::sync::Semaphore::new(1),
        config: config.clone(),
        reporter: config.reporter.clone(),
        downloaded: AtomicUsize::new(0),
        skipped: AtomicUsize::new(0),
        failed: AtomicUsize::new(0),
        manual_downloads: Mutex::new(Vec::new()),
        failed_downloads: Mutex::new(Vec::new()),
        completed_archives: AtomicUsize::new(0),
        total_archives,
    })
}

/// Download all pending archives (smart mode: only downloads archives needed for missing outputs)
pub async fn download_archives(db: &ModlistDb, config: &InstallConfig) -> Result<DownloadStats> {
    let reporter = &config.reporter;

    // Smart check: scan output directory to find what's missing
    reporter.log("Scanning output directory for existing files...");
    let existing_outputs = scan_existing_outputs(&config.output_dir)?;
    reporter.log(&format!(
        "Found {} existing output files",
        existing_outputs.len()
    ));

    // Build set of BSA temp_ids whose output is already valid (sidecar matches).
    // Directives writing into TEMP_BSA_FILES/{temp_id} can be skipped.
    let valid_bsa_temp_ids = build_valid_bsa_set(db, config);

    // Get all directives that need archives and check which outputs are missing
    let directive_outputs = db.get_directive_outputs_with_archives()?;
    let mut needed_archives: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut missing_count = 0;

    for (to_path, size, archive_hash) in &directive_outputs {
        // Skip directives that feed into a BSA that's already valid
        if is_bsa_staging_path(to_path, &valid_bsa_temp_ids) {
            continue;
        }

        let normalized = crate::paths::normalize_for_lookup(to_path);
        let output_exists = existing_outputs
            .get(&normalized)
            .map(|&existing_size| existing_size == *size as u64)
            .unwrap_or(false);

        if !output_exists {
            needed_archives.insert(archive_hash.clone());
            missing_count += 1;
        }
    }

    if needed_archives.is_empty() {
        reporter.log("All output files exist - no downloads needed!");
        return Ok(DownloadStats::default());
    }

    reporter.log(&format!(
        "Found {} missing outputs requiring {} archives",
        missing_count,
        needed_archives.len()
    ));

    // Get archive info for needed archives
    let needed_hashes: Vec<String> = needed_archives.into_iter().collect();
    let archives_to_check = db.get_archives_by_hashes(&needed_hashes)?;

    // Now check which of these archives are actually downloaded (with correct size)
    let mut already_downloaded = 0usize;
    let mut already_downloaded_size: u64 = 0;
    let mut need_download: Vec<ArchiveInfo> = Vec::new();
    let mut missing_named_count = 0usize;
    let mut missing_named_examples: Vec<String> = Vec::new();

    // First pass: check which archives exist — sidecar cache skips re-hashing
    let mut archives_to_verify: Vec<(ArchiveInfo, PathBuf)> = Vec::new();
    let mut sidecar_hits = 0usize;

    for archive in archives_to_check {
        if let Some(path) = resolve_game_file_source_archive(config, &archive) {
            db.mark_archive_downloaded(&archive.hash, path.to_string_lossy().as_ref())?;
            already_downloaded += 1;
            already_downloaded_size += archive.size as u64;
            continue;
        }

        let output_path = config.downloads_dir.join(&archive.name);
        if output_path.exists() {
            if let Ok(meta) = fs::metadata(&output_path) {
                if meta.len() != archive.size as u64 {
                    warn!(
                        "Size mismatch for '{}' (expected={}, actual={}) — will verify hash",
                        output_path.display(),
                        archive.size,
                        meta.len()
                    );
                }
                // Fast path: sidecar cache says hash+size+mtime match — skip re-hashing
                if super::sidecar::archive_hash_valid(&output_path, &archive.hash) {
                    db.mark_archive_downloaded(
                        &archive.hash,
                        output_path.to_string_lossy().as_ref(),
                    )?;
                    already_downloaded += 1;
                    already_downloaded_size += archive.size as u64;
                    sidecar_hits += 1;
                    continue;
                }
                // Slow path: need to actually hash the file
                archives_to_verify.push((archive, output_path));
                continue;
            }
        } else {
            // Important UX signal: copied archives must match the exact expected name/path.
            missing_named_count += 1;
            if missing_named_examples.len() < 5 {
                missing_named_examples.push(archive.name.clone());
            }
        }
        need_download.push(archive);
    }
    if sidecar_hits > 0 {
        reporter.log(&format!(
            "  {} archives verified via sidecar cache (skipped re-hashing)",
            sidecar_hits
        ));
    }

    if missing_named_count > 0 {
        warn!(
            "{} required archives were not found at exact expected path/name under '{}'. \
Copied files with different names will not be reused. Examples: {}",
            missing_named_count,
            config.downloads_dir.display(),
            missing_named_examples.join(", ")
        );
    }

    // Second pass: verify hashes of existing archives
    let mut corrupted_count = 0usize;
    if !archives_to_verify.is_empty() {
        let verify_total = archives_to_verify.len();
        reporter.log(&format!("Verifying {} existing archives...", verify_total));
        reporter.overall_set_total(verify_total as u64);
        reporter.overall_set_message("Verifying archives...");
        let verify_status = reporter.begin_status("Verify");
        verify_status.set_count(0, verify_total);
        let verify_counter = AtomicUsize::new(0);

        // Kick off readahead on all archives — tells the kernel to start loading
        // files into the page cache in the background. By the time the hash loop
        // reaches each file, it's likely already cached.
        #[cfg(target_os = "linux")]
        for (_, output_path) in &archives_to_verify {
            if let Ok(file) = std::fs::File::open(output_path) {
                use std::os::unix::io::AsRawFd;
                unsafe {
                    libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_WILLNEED);
                }
            }
        }

        // Verify hashes in parallel using rayon
        let verify_results: Vec<Result<(bool, String)>> = archives_to_verify
            .par_iter()
            .map(|(archive, output_path)| {
                let name = output_path
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy();
                reporter.overall_set_message(&format!("Verifying {}...", truncate_name(&name, 40)));
                let result = verify_file_hash_detailed(output_path, &archive.hash);
                reporter.overall_inc();
                let done = verify_counter.fetch_add(1, Ordering::Relaxed) + 1;
                verify_status.set_count(done, verify_total);
                result
            })
            .collect();
        verify_status.finish();

        // Process results sequentially (DB writes, file deletes, bookkeeping)
        for ((archive, output_path), result) in
            archives_to_verify.iter().zip(verify_results.into_iter())
        {
            match result {
                Ok((true, _actual_hash)) => {
                    // Hash matches - archive is valid, write sidecar for next run
                    db.mark_archive_downloaded(
                        &archive.hash,
                        output_path.to_string_lossy().as_ref(),
                    )?;
                    if let Err(e) = super::sidecar::write_archive_hash(output_path, &archive.hash) {
                        tracing::debug!(
                            "Failed to write archive sidecar for {}: {}",
                            output_path.display(),
                            e
                        );
                    }
                    already_downloaded += 1;
                    already_downloaded_size += archive.size as u64;
                }
                Ok((false, actual_hash))
                    if crate::installer::game_preflight::has_known_alt_variant(&archive.name) =>
                {
                    warn!(
                        "{} has different hash (known CC alt-variant, expected={}, actual={}) — accepting",
                        archive.name, archive.hash, actual_hash
                    );
                    db.mark_archive_downloaded(
                        &archive.hash,
                        output_path.to_string_lossy().as_ref(),
                    )?;
                    if let Err(e) = super::sidecar::write_archive_hash(output_path, &archive.hash) {
                        tracing::debug!(
                            "Failed to write archive sidecar for {}: {}",
                            output_path.display(),
                            e
                        );
                    }
                    already_downloaded += 1;
                    already_downloaded_size += archive.size as u64;
                }
                Ok((false, actual_hash)) => {
                    // Hash mismatch - corrupted, delete and re-download
                    reporter.log(&format!("  Corrupted (hash mismatch): {}", archive.name));
                    warn!(
                        "Rejecting local archive '{}' due to hash mismatch (expected={}, actual={})",
                        output_path.display(),
                        archive.hash,
                        actual_hash
                    );
                    if let Err(e) = fs::remove_file(output_path) {
                        warn!(
                            "Failed to remove corrupted file {}: {}",
                            output_path.display(),
                            e
                        );
                    }
                    corrupted_count += 1;
                    need_download.push(archive.clone());
                }
                Err(e) => {
                    // Error reading file - treat as corrupted
                    reporter.log(&format!("  Verify error for {}: {}", archive.name, e));
                    if let Err(e) = fs::remove_file(output_path) {
                        warn!(
                            "Failed to remove unreadable file {}: {}",
                            output_path.display(),
                            e
                        );
                    }
                    corrupted_count += 1;
                    need_download.push(archive.clone());
                }
            }
        }
        reporter.log(&format!(
            "  Verified {} archives ({} valid, {} corrupted)",
            verify_total, already_downloaded, corrupted_count
        ));
    }

    if corrupted_count > 0 {
        reporter.log(&format!(
            "Found {} corrupted archives - will re-download",
            corrupted_count
        ));
    }

    if already_downloaded > 0 {
        reporter.log(&format!(
            "Found {} archives already downloaded ({} bytes)",
            already_downloaded, already_downloaded_size
        ));
        // Report skipped archives to progress callback
        if let Some(ref callback) = config.progress_callback {
            callback(ProgressEvent::DownloadSkipped {
                count: already_downloaded,
                total_size: already_downloaded_size,
            });
        }
    }

    if need_download.is_empty() {
        reporter.log("All needed archives already downloaded!");
        return Ok(DownloadStats {
            downloaded: 0,
            skipped: already_downloaded,
            failed: 0,
            manual: 0,
            failed_downloads: Vec::new(),
            manual_downloads: Vec::new(),
        });
    }

    reporter.log(&format!(
        "Need to download {} archives",
        need_download.len()
    ));

    let pending = need_download;
    let concurrency = config.max_concurrent_downloads;

    // Setup progress
    reporter.overall_set_total(pending.len() as u64);
    reporter.overall_set_message("Starting downloads...");

    // Create shared context
    let total_archives = pending.len();
    let ctx = Arc::new(build_context(config, total_archives).await?);

    // Collect hashes and paths for DB updates (can't borrow db across await)
    let results: Vec<DownloadResultTuple> = stream::iter(pending)
        .map(|archive| {
            let ctx = Arc::clone(&ctx);
            async move {
                let output_path = ctx.config.downloads_dir.join(&archive.name);
                let (result, url_to_cache) = process_archive(&ctx, &archive, &output_path).await;
                (archive.hash.clone(), output_path, result, url_to_cache)
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;

    // Update database with results
    for (hash, output_path, result, url_to_cache) in results {
        // Cache URL if we got one (even if download failed, cache for retry)
        if let Some((url, expires)) = url_to_cache {
            let _ = db.cache_download_url(&hash, &url, expires);
        }

        match result {
            DownloadResult::Success => {
                db.mark_archive_downloaded(&hash, output_path.to_string_lossy().as_ref())?;
                // Write sidecar so future resumes skip re-hashing. download_archive()
                // already verified the hash before returning Success, so this is safe.
                if let Err(e) = super::sidecar::write_archive_hash(&output_path, &hash) {
                    tracing::debug!(
                        "Failed to write archive sidecar for {}: {}",
                        output_path.display(),
                        e
                    );
                }
            }
            DownloadResult::Skipped => {
                db.mark_archive_downloaded(&hash, output_path.to_string_lossy().as_ref())?;
            }
            DownloadResult::Manual | DownloadResult::Failed => {
                // Don't mark as downloaded
            }
        }
    }

    reporter.overall_finish();

    // Get the detailed lists for stats and printing
    let manual_downloads_list = ctx.manual_downloads.lock().await.clone();
    let failed_downloads_list = ctx.failed_downloads.lock().await.clone();

    // Collect stats (include pre-scan skipped count)
    let stats = DownloadStats {
        downloaded: ctx.downloaded.load(Ordering::Relaxed),
        skipped: ctx.skipped.load(Ordering::Relaxed) + already_downloaded,
        failed: ctx.failed.load(Ordering::Relaxed),
        manual: manual_downloads_list.len(),
        failed_downloads: failed_downloads_list.clone(),
        manual_downloads: manual_downloads_list.clone(),
    };

    // Print manual download instructions
    if !manual_downloads_list.is_empty() {
        reporter.log(&format!(
            "\n=== Manual Downloads Required ({}) ===",
            manual_downloads_list.len()
        ));
        reporter.log(&format!(
            "Please download the following files to: {}\n",
            config.downloads_dir.display()
        ));

        // Log to file for later reference
        warn!(
            "=== Manual Downloads Required ({}) ===",
            manual_downloads_list.len()
        );
        warn!("Download destination: {}", config.downloads_dir.display());

        for (i, md) in manual_downloads_list.iter().enumerate() {
            reporter.log(&format!("{}. {}", i + 1, md.name));
            reporter.log(&format!("   URL: {}", md.url));
            reporter.log(&format!("   Expected size: {} bytes", md.expected_size));
            if let Some(prompt) = &md.prompt {
                reporter.log(&format!("   Note: {}", prompt));
            }
            reporter.log("");

            // Log each manual download to file
            if let Some(prompt) = &md.prompt {
                warn!(
                    "[MANUAL] {}: {} (size: {} bytes, note: {})",
                    md.name, md.url, md.expected_size, prompt
                );
            } else {
                warn!(
                    "[MANUAL] {}: {} (size: {} bytes)",
                    md.name, md.url, md.expected_size
                );
            }
        }

        reporter.log("After downloading, run the command again to continue.\n");
    }

    // Print failed download instructions
    if !failed_downloads_list.is_empty() {
        reporter.log(&format!(
            "\n=== Failed Downloads ({}) ===",
            failed_downloads_list.len()
        ));
        reporter.log(&format!(
            "These downloads failed. Try manually downloading to: {}\n",
            config.downloads_dir.display()
        ));

        // Log to file for later reference
        warn!("=== Failed Downloads ({}) ===", failed_downloads_list.len());
        warn!("Download destination: {}", config.downloads_dir.display());

        for (i, fd) in failed_downloads_list.iter().enumerate() {
            reporter.log(&format!("{}. {}", i + 1, fd.name));
            reporter.log(&format!("   URL: {}", fd.url));
            reporter.log(&format!("   Error: {}", fd.error));
            reporter.log(&format!(
                "   Expected size: {} bytes ({:.2} MB)",
                fd.expected_size,
                fd.expected_size as f64 / 1024.0 / 1024.0
            ));
            reporter.log("");

            // Log each failed download to file
            warn!(
                "[FAILED] {}: {} (error: {}, size: {} bytes)",
                fd.name, fd.url, fd.error, fd.expected_size
            );
        }

        reporter.log("After downloading, run the command again to continue.\n");
    }

    // Print summary
    reporter.log("\n=== Download Summary ===");
    reporter.log(&format!("Downloaded: {}", stats.downloaded));
    reporter.log(&format!("Skipped:    {}", stats.skipped));
    reporter.log(&format!("Manual:     {}", stats.manual));
    reporter.log(&format!("Failed:     {}", stats.failed));

    // Print Nexus rate limits
    let limits = ctx.nexus.rate_limits();
    reporter.log(&format!(
        "\nNexus API: {}/{} hourly, {}/{} daily",
        limits.hourly_remaining, limits.hourly_limit, limits.daily_remaining, limits.daily_limit
    ));

    Ok(stats)
}

/// Event emitted per-archive during streaming downloads.
#[derive(Debug)]
pub enum ArchiveEvent {
    /// Archive is ready (downloaded + hash verified, or already existed).
    Ready {
        hash: String,
        name: String,
        path: PathBuf,
    },
    /// Archive download failed.
    Failed {
        hash: String,
        name: String,
        error: String,
    },
    /// Archive requires manual download — not a hard failure but blocks this archive.
    Manual { hash: String, name: String },
}

/// Download archives with per-archive completion events.
///
/// Like `download_archives`, but sends an `ArchiveEvent` through the provided
/// `std::sync::mpsc::SyncSender` as each archive finishes. Already-downloaded
/// archives emit `Ready` events before the download stream starts.
pub async fn download_archives_streaming(
    db: &ModlistDb,
    config: &InstallConfig,
    tx: &std::sync::mpsc::SyncSender<ArchiveEvent>,
    priority: Option<&HashMap<String, u32>>,
) -> Result<DownloadStats> {
    let reporter = &config.reporter;

    // Smart check: scan output directory to find what's missing
    reporter.log("Scanning output directory for existing files...");
    let existing_outputs = scan_existing_outputs(&config.output_dir)?;
    reporter.log(&format!(
        "Found {} existing output files",
        existing_outputs.len()
    ));

    // Build set of BSA temp_ids whose output is already valid (sidecar matches)
    let valid_bsa_temp_ids = build_valid_bsa_set(db, config);

    // Get all directives that need archives and check which outputs are missing
    let directive_outputs = db.get_directive_outputs_with_archives()?;
    let mut needed_archives: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut missing_count = 0;

    for (to_path, size, archive_hash) in &directive_outputs {
        if is_bsa_staging_path(to_path, &valid_bsa_temp_ids) {
            continue;
        }

        let normalized = crate::paths::normalize_for_lookup(to_path);
        let output_exists = existing_outputs
            .get(&normalized)
            .map(|&existing_size| existing_size == *size as u64)
            .unwrap_or(false);

        if !output_exists {
            needed_archives.insert(archive_hash.clone());
            missing_count += 1;
        }
    }

    if needed_archives.is_empty() {
        reporter.log("All output files exist - no downloads needed!");
        return Ok(DownloadStats::default());
    }

    reporter.log(&format!(
        "Found {} missing outputs requiring {} archives",
        missing_count,
        needed_archives.len()
    ));

    // Get archive info for needed archives
    let needed_hashes: Vec<String> = needed_archives.into_iter().collect();
    let archives_to_check = db.get_archives_by_hashes(&needed_hashes)?;

    // Check which archives are already downloaded
    let mut already_downloaded = 0usize;
    let mut already_downloaded_size: u64 = 0;
    let mut need_download: Vec<ArchiveInfo> = Vec::new();
    let mut archives_to_verify: Vec<(ArchiveInfo, PathBuf)> = Vec::new();
    let mut sidecar_verified: Vec<(ArchiveInfo, PathBuf)> = Vec::new();

    for archive in archives_to_check {
        if let Some(path) = resolve_game_file_source_archive(config, &archive) {
            db.mark_archive_downloaded(&archive.hash, path.to_string_lossy().as_ref())?;
            already_downloaded += 1;
            already_downloaded_size += archive.size as u64;
            let _ = tx.send(ArchiveEvent::Ready {
                hash: archive.hash.clone(),
                name: archive.name.clone(),
                path,
            });
            continue;
        }

        let output_path = config.downloads_dir.join(&archive.name);
        if output_path.exists() && fs::metadata(&output_path).is_ok() {
            // Fast path: sidecar cache says hash+size+mtime match
            if super::sidecar::archive_hash_valid(&output_path, &archive.hash) {
                db.mark_archive_downloaded(&archive.hash, output_path.to_string_lossy().as_ref())?;
                already_downloaded += 1;
                already_downloaded_size += archive.size as u64;
                sidecar_verified.push((archive, output_path));
                continue;
            }
            archives_to_verify.push((archive, output_path));
            continue;
        }
        need_download.push(archive);
    }

    // Emit sidecar-verified archives immediately (no hashing needed)
    if !sidecar_verified.is_empty() {
        reporter.log(&format!(
            "  {} archives verified via sidecar cache (skipped re-hashing)",
            sidecar_verified.len()
        ));
        if let Some(prio) = priority {
            sidecar_verified.sort_by(|a, b| {
                let pa = prio.get(&a.0.hash).copied().unwrap_or(0);
                let pb = prio.get(&b.0.hash).copied().unwrap_or(0);
                pb.cmp(&pa)
            });
        }
        for (archive, output_path) in sidecar_verified {
            let _ = tx.send(ArchiveEvent::Ready {
                hash: archive.hash.clone(),
                name: archive.name.clone(),
                path: output_path,
            });
        }
    }

    // Verify hashes of remaining archives (threaded)
    if !archives_to_verify.is_empty() {
        let verify_total = archives_to_verify.len();
        reporter.log(&format!("Verifying {} existing archives...", verify_total));
        reporter.overall_set_total(verify_total as u64);
        reporter.overall_set_message("Verifying archives...");
        let verify_status = reporter.begin_status("Verify");
        verify_status.set_count(0, verify_total);
        let verify_counter = AtomicUsize::new(0);

        // Readahead: tell kernel to start loading archive files into page cache
        #[cfg(target_os = "linux")]
        for (_, output_path) in &archives_to_verify {
            if let Ok(file) = std::fs::File::open(output_path) {
                use std::os::unix::io::AsRawFd;
                unsafe {
                    libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_WILLNEED);
                }
            }
        }

        let verify_results: Vec<Result<(bool, String)>> = archives_to_verify
            .par_iter()
            .map(|(archive, output_path)| {
                let name = output_path
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy();
                reporter.overall_set_message(&format!("Verifying {}...", truncate_name(&name, 40)));
                let result = verify_file_hash_detailed(output_path, &archive.hash);
                reporter.overall_inc();
                let done = verify_counter.fetch_add(1, Ordering::Relaxed) + 1;
                verify_status.set_count(done, verify_total);
                result
            })
            .collect();
        verify_status.finish();

        let mut verified_archives: Vec<(ArchiveInfo, PathBuf)> = Vec::new();
        for ((archive, output_path), result) in archives_to_verify.into_iter().zip(verify_results) {
            match result {
                Ok((true, _)) => {
                    db.mark_archive_downloaded(
                        &archive.hash,
                        output_path.to_string_lossy().as_ref(),
                    )?;
                    if let Err(e) = super::sidecar::write_archive_hash(&output_path, &archive.hash)
                    {
                        tracing::debug!(
                            "Failed to write archive sidecar for {}: {}",
                            output_path.display(),
                            e
                        );
                    }
                    already_downloaded += 1;
                    already_downloaded_size += archive.size as u64;
                    verified_archives.push((archive, output_path));
                }
                Ok((false, actual_hash))
                    if crate::installer::game_preflight::has_known_alt_variant(&archive.name) =>
                {
                    warn!(
                        "{} has different hash (known CC alt-variant, expected={}, actual={}) — accepting",
                        archive.name, archive.hash, actual_hash
                    );
                    db.mark_archive_downloaded(
                        &archive.hash,
                        output_path.to_string_lossy().as_ref(),
                    )?;
                    if let Err(e) = super::sidecar::write_archive_hash(&output_path, &archive.hash)
                    {
                        tracing::debug!(
                            "Failed to write archive sidecar for {}: {}",
                            output_path.display(),
                            e
                        );
                    }
                    already_downloaded += 1;
                    already_downloaded_size += archive.size as u64;
                    verified_archives.push((archive, output_path));
                }
                Ok((false, _)) | Err(_) => {
                    let _ = fs::remove_file(&output_path);
                    need_download.push(archive);
                }
            }
        }

        // Emit verified archives in priority order
        if let Some(prio) = priority {
            verified_archives.sort_by(|a, b| {
                let pa = prio.get(&a.0.hash).copied().unwrap_or(0);
                let pb = prio.get(&b.0.hash).copied().unwrap_or(0);
                pb.cmp(&pa)
            });
        }
        for (archive, output_path) in verified_archives {
            let _ = tx.send(ArchiveEvent::Ready {
                hash: archive.hash.clone(),
                name: archive.name.clone(),
                path: output_path,
            });
        }
    }

    if already_downloaded > 0 {
        reporter.log(&format!(
            "Found {} archives already downloaded ({} bytes)",
            already_downloaded, already_downloaded_size
        ));
        if let Some(ref callback) = config.progress_callback {
            callback(super::ProgressEvent::DownloadSkipped {
                count: already_downloaded,
                total_size: already_downloaded_size,
            });
        }
    }

    if need_download.is_empty() {
        reporter.log("All needed archives already downloaded!");
        return Ok(DownloadStats {
            downloaded: 0,
            skipped: already_downloaded,
            ..Default::default()
        });
    }

    // Sort downloads by priority (highest first) — BSA-feeding archives first
    if let Some(prio) = priority {
        need_download.sort_by(|a, b| {
            let pa = prio.get(&a.hash).copied().unwrap_or(0);
            let pb = prio.get(&b.hash).copied().unwrap_or(0);
            pb.cmp(&pa)
        });
    }

    reporter.log(&format!(
        "Need to download {} archives",
        need_download.len()
    ));

    let pending = need_download;
    let concurrency = config.max_concurrent_downloads;

    // Setup progress
    reporter.overall_set_total(pending.len() as u64);
    reporter.overall_set_message("Starting downloads...");

    let total_archives = pending.len();
    let ctx = Arc::new(build_context(config, total_archives).await?);

    // Stream downloads, emitting events as each completes
    stream::iter(pending)
        .map(|archive| {
            let ctx = Arc::clone(&ctx);
            async move {
                let output_path = ctx.config.downloads_dir.join(&archive.name);
                let (result, url_to_cache) = process_archive(&ctx, &archive, &output_path).await;
                (archive, output_path, result, url_to_cache)
            }
        })
        .buffer_unordered(concurrency)
        .for_each(|(archive, output_path, result, url_to_cache)| {
            // Cache URL if we got one
            if let Some((url, expires)) = url_to_cache {
                let _ = db.cache_download_url(&archive.hash, &url, expires);
            }

            match result {
                DownloadResult::Success | DownloadResult::Skipped => {
                    let _ = db.mark_archive_downloaded(
                        &archive.hash,
                        output_path.to_string_lossy().as_ref(),
                    );
                    // On Success path, download_archive() already verified the
                    // hash; on Skipped path, process_archive() only checked
                    // size, so this is best-effort. In either case a sidecar
                    // makes subsequent resumes O(1) via stat() instead of
                    // re-hashing every archive.
                    if matches!(result, DownloadResult::Success) {
                        if let Err(e) =
                            super::sidecar::write_archive_hash(&output_path, &archive.hash)
                        {
                            tracing::debug!(
                                "Failed to write archive sidecar for {}: {}",
                                output_path.display(),
                                e
                            );
                        }
                    }
                    let _ = tx.send(ArchiveEvent::Ready {
                        hash: archive.hash.clone(),
                        name: archive.name.clone(),
                        path: output_path,
                    });
                }
                DownloadResult::Manual => {
                    let _ = tx.send(ArchiveEvent::Manual {
                        hash: archive.hash.clone(),
                        name: archive.name.clone(),
                    });
                }
                DownloadResult::Failed => {
                    let _ = tx.send(ArchiveEvent::Failed {
                        hash: archive.hash.clone(),
                        name: archive.name.clone(),
                        error: "Download failed".to_string(),
                    });
                }
            }
            futures::future::ready(())
        })
        .await;

    reporter.overall_finish();

    let manual_downloads_list = ctx.manual_downloads.lock().await.clone();
    let failed_downloads_list = ctx.failed_downloads.lock().await.clone();

    let stats = DownloadStats {
        downloaded: ctx.downloaded.load(Ordering::Relaxed),
        skipped: ctx.skipped.load(Ordering::Relaxed) + already_downloaded,
        failed: ctx.failed.load(Ordering::Relaxed),
        manual: manual_downloads_list.len(),
        failed_downloads: failed_downloads_list,
        manual_downloads: manual_downloads_list,
    };

    // Print failed downloads with URLs for manual download
    if !stats.failed_downloads.is_empty() {
        reporter.log(&format!(
            "\n=== Failed Downloads ({}) ===",
            stats.failed_downloads.len()
        ));
        reporter.log(&format!(
            "Download these manually to: {}\n",
            config.downloads_dir.display()
        ));
        for (i, fd) in stats.failed_downloads.iter().enumerate() {
            reporter.log(&format!("{}. {}", i + 1, fd.name));
            reporter.log(&format!("   URL: {}", fd.url));
            reporter.log(&format!("   Error: {}", fd.error));
            reporter.log("");
        }
    }

    // Print summary
    reporter.log("\n=== Download Summary ===");
    reporter.log(&format!("Downloaded: {}", stats.downloaded));
    reporter.log(&format!("Skipped:    {}", stats.skipped));
    reporter.log(&format!("Manual:     {}", stats.manual));
    reporter.log(&format!("Failed:     {}", stats.failed));

    let limits = ctx.nexus.rate_limits();
    reporter.log(&format!(
        "\nNexus API: {}/{} hourly, {}/{} daily",
        limits.hourly_remaining, limits.hourly_limit, limits.daily_remaining, limits.daily_limit
    ));

    Ok(stats)
}

/// Result of processing a single archive
#[derive(Debug, Clone, Copy)]
enum DownloadResult {
    Success,
    Skipped,
    Manual,
    Failed,
}

/// Process a single archive (check, download, or mark manual)
/// Returns (result, optional (url, expires) to cache)
async fn process_archive(
    ctx: &DownloadContext,
    archive: &ArchiveInfo,
    output_path: &Path,
) -> (DownloadResult, Option<(String, i64)>) {
    // Check if file already exists with correct size
    if output_path.exists() {
        if let Ok(meta) = fs::metadata(output_path) {
            if meta.len() == archive.size as u64 {
                ctx.skipped.fetch_add(1, Ordering::Relaxed);
                ctx.reporter.overall_inc();
                update_overall_message(ctx);
                report_archive_complete(ctx, &archive.name);
                return (DownloadResult::Skipped, None);
            }
        }
    }

    // Parse the download state
    let state: DownloadState = match serde_json::from_str(&archive.state_json) {
        Ok(s) => s,
        Err(e) => {
            ctx.failed.fetch_add(1, Ordering::Relaxed);
            ctx.reporter.overall_inc();
            ctx.reporter
                .log(&format!("FAIL {} - parse error: {}", archive.name, e));
            return (DownloadResult::Failed, None);
        }
    };

    // Check for manual downloads first
    if let Some(manual_info) = check_manual(&state, archive, ctx.loverslab.is_some()) {
        ctx.manual_downloads.lock().await.push(manual_info);
        ctx.reporter.overall_inc();
        update_overall_message(ctx);
        report_archive_complete(ctx, &archive.name);
        return (DownloadResult::Manual, None);
    }

    // Create a progress handle for this download
    let handle = ctx.begin_download(&archive.name, archive.size as u64);

    // Download based on source type
    let source = source_type_name(&state);
    let result = download_archive(&state, archive, output_path, ctx, &handle).await;

    match result {
        Ok(url_to_cache) => {
            handle.finish();
            ctx.downloaded.fetch_add(1, Ordering::Relaxed);
            ctx.reporter.overall_inc();
            update_overall_message(ctx);
            report_archive_complete(ctx, &archive.name);
            (DownloadResult::Success, url_to_cache)
        }
        Err(e) => {
            ctx.failed.fetch_add(1, Ordering::Relaxed);
            ctx.reporter.overall_inc();
            let error_msg = root_cause(&e);
            let full_error = format!("{:#}", e);
            warn!("Download failed for {}: {}", archive.name, full_error);
            handle.finish_with_error(&format!(
                "FAIL [{}] {} - {}",
                source,
                truncate_name(&archive.name, 30),
                error_msg
            ));
            ctx.reporter.log(&format!(
                "FAIL [{}] {} - {}",
                source,
                truncate_name(&archive.name, 30),
                error_msg
            ));
            ctx.failed_downloads.lock().await.push(FailedDownloadInfo {
                name: archive.name.clone(),
                url: get_manual_url(&state),
                error: error_msg,
                expected_size: archive.size as u64,
            });
            report_archive_complete(ctx, &archive.name);
            (DownloadResult::Failed, None)
        }
    }
}

/// Update the overall progress bar message with current stats
fn update_overall_message(ctx: &DownloadContext) {
    let downloaded = ctx.downloaded.load(Ordering::Relaxed);
    let skipped = ctx.skipped.load(Ordering::Relaxed);
    let failed = ctx.failed.load(Ordering::Relaxed);
    ctx.reporter.overall_set_message(&format!(
        "OK:{} Skip:{} Fail:{}",
        downloaded, skipped, failed
    ));
}

/// Report archive completion via progress callback
fn report_archive_complete(ctx: &DownloadContext, name: &str) {
    // Increment completed count and get new value (1-based for display)
    let completed = ctx.completed_archives.fetch_add(1, Ordering::Relaxed) + 1;
    let total = ctx.total_archives;

    // Call progress callback if configured
    if let Some(ref callback) = ctx.config.progress_callback {
        callback(ProgressEvent::DownloadComplete {
            name: name.to_string(),
        });
        callback(ProgressEvent::ArchiveComplete {
            index: completed,
            total,
        });
    }
}

/// Check if this is a manual download type (only truly manual sources)
fn check_manual(
    state: &DownloadState,
    archive: &ArchiveInfo,
    has_loverslab: bool,
) -> Option<ManualDownloadInfo> {
    match state {
        DownloadState::Manual(manual_state) => {
            // ModDB "manual" links can often be resolved automatically.
            if is_moddb_url(&manual_state.url) {
                return None;
            }
            // LoversLab links are automated when credentials are configured.
            if has_loverslab && is_loverslab_url(&manual_state.url) {
                return None;
            }
            // MediaFire links can be resolved automatically.
            if is_mediafire_url(&manual_state.url) {
                return None;
            }
            // Mega links can be resolved via native API or proxy.
            if is_mega_url(&manual_state.url) {
                return None;
            }
            // Yandex Disk public shares resolve via the public cloud API.
            if is_yandex_url(&manual_state.url) {
                return None;
            }
            Some(ManualDownloadInfo {
                name: archive.name.clone(),
                url: manual_state.url.clone(),
                prompt: Some(manual_state.prompt.clone()),
                expected_size: archive.size as u64,
            })
        }
        // Mega is now handled via Wabbajack proxy, not manual
        _ => None,
    }
}

/// Try to log into LoversLab if credentials are configured.
/// Returns None (with a warning) if login fails — downloads fall back to manual.
async fn init_loverslab(config: &InstallConfig) -> Option<LoversLabDownloader> {
    if config.loverslab_email.is_empty() || config.loverslab_password.is_empty() {
        return None;
    }
    match LoversLabDownloader::login(&config.loverslab_email, &config.loverslab_password).await {
        Ok(ll) => {
            info!("LoversLab login successful — automated downloads enabled");
            Some(ll)
        }
        Err(e) => {
            warn!("LoversLab login failed (downloads will be manual): {}", e);
            None
        }
    }
}

fn is_loverslab_url(url: &str) -> bool {
    crate::downloaders::loverslab::is_loverslab_url(url)
}

fn is_mediafire_url(url: &str) -> bool {
    let lower = url.to_lowercase();
    lower.contains("mediafire.com/")
}

fn is_mega_url(url: &str) -> bool {
    let lower = url.to_lowercase();
    lower.contains("mega.nz/") || lower.contains("mega.co.nz/")
}

fn is_yandex_url(url: &str) -> bool {
    crate::downloaders::yandex::is_yandex_url(url)
}

fn is_moddb_url(url: &str) -> bool {
    url.contains("moddb.com")
}

fn is_moddb_start_url(url: &str) -> bool {
    url.contains("/addons/start/") || url.contains("/downloads/start/")
}

fn moddb_abs_url(url: &str) -> String {
    if url.starts_with("http://") || url.starts_with("https://") {
        url.to_string()
    } else {
        format!("https://www.moddb.com{}", url)
    }
}

fn extract_moddb_start_url(html: &str) -> Option<String> {
    let re = Regex::new(r#"href="([^"]*(?:/addons/start/\d+|/downloads/start/\d+)[^"]*)""#).ok()?;
    let capture = re.captures(html)?;
    Some(moddb_abs_url(capture.get(1)?.as_str()))
}

fn extract_moddb_mirror_url(html: &str) -> Option<String> {
    // Download-start pages include a mirror URL both in a link and in JS redirect.
    let re = Regex::new(r#"(?:(?:href|window\.location\.href)=")([^"]*/downloads/mirror/[^"]+)""#)
        .ok()?;
    let capture = re.captures(html)?;
    Some(moddb_abs_url(capture.get(1)?.as_str()))
}

async fn fetch_moddb_html(client: &HttpClient, url: &str, referer: Option<&str>) -> Result<String> {
    let mut request = client
        .inner()
        .get(url)
        .header(reqwest::header::USER_AGENT, MODDB_USER_AGENT)
        .header(reqwest::header::ACCEPT_LANGUAGE, "en-US,en;q=0.9")
        .header(
            reqwest::header::ACCEPT,
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        );
    if let Some(referer_url) = referer {
        request = request.header(reqwest::header::REFERER, referer_url);
    }

    let response = request
        .send()
        .await
        .with_context(|| format!("Failed to fetch ModDB page: {}", url))?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        bail!(
            "ModDB page error {}: {}",
            status.as_u16(),
            root_cause_text(&body)
        );
    }
    response
        .text()
        .await
        .with_context(|| format!("Failed to read ModDB page: {}", url))
}

fn root_cause_text(text: &str) -> String {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return "empty response".to_string();
    }
    let single_line = trimmed.replace('\n', " ");
    if single_line.len() > 120 {
        format!("{}...", &single_line[..117])
    } else {
        single_line
    }
}

async fn resolve_moddb_download_url(client: &HttpClient, source_url: &str) -> Result<String> {
    let source_url = moddb_abs_url(source_url);
    let start_url = if is_moddb_start_url(&source_url) {
        source_url.clone()
    } else {
        let page_html = fetch_moddb_html(client, &source_url, None).await?;
        extract_moddb_start_url(&page_html)
            .with_context(|| format!("Could not find ModDB start link on page: {}", source_url))?
    };

    let start_html = fetch_moddb_html(client, &start_url, Some(&source_url)).await?;
    let mirror_url = extract_moddb_mirror_url(&start_html).with_context(|| {
        format!(
            "Could not find ModDB mirror link on start page: {}",
            start_url
        )
    })?;

    // Follow redirects once with browser-like headers to produce a stable direct file URL.
    let response = client
        .inner()
        .get(&mirror_url)
        .header(reqwest::header::USER_AGENT, MODDB_USER_AGENT)
        .header(reqwest::header::REFERER, &start_url)
        .send()
        .await
        .with_context(|| format!("Failed to resolve ModDB mirror URL: {}", mirror_url))?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        bail!(
            "ModDB mirror resolution error {}: {}",
            status.as_u16(),
            root_cause_text(&body)
        );
    }

    Ok(response.url().to_string())
}

/// Truncate a filename for display
fn truncate_name(name: &str, max_len: usize) -> String {
    if name.len() <= max_len {
        name.to_string()
    } else {
        format!("{}...", &name[..max_len - 3])
    }
}

/// Get the source type name for error messages
fn source_type_name(state: &DownloadState) -> &'static str {
    match state {
        DownloadState::Nexus(_) => "Nexus",
        DownloadState::Http(_) => "HTTP",
        DownloadState::WabbajackCDN(_) => "CDN",
        DownloadState::GoogleDrive(_) => "GDrive",
        DownloadState::MediaFire(_) => "MediaFire",
        DownloadState::GameFileSource(_) => "GameFile",
        DownloadState::Manual(_) => "Manual",
        DownloadState::Mega(_) => "Mega",
    }
}

/// Get a user-friendly URL for manual download from the state
fn get_manual_url(state: &DownloadState) -> String {
    match state {
        DownloadState::Nexus(s) => {
            let domain = crate::downloaders::NexusDownloader::game_domain(&s.game_name);
            format!(
                "https://www.nexusmods.com/{}/mods/{}?tab=files",
                domain, s.mod_id
            )
        }
        DownloadState::Http(s) => s.url.clone(),
        DownloadState::WabbajackCDN(s) => s.url.clone(),
        DownloadState::GoogleDrive(s) => format!("https://drive.google.com/file/d/{}/view", s.id),
        DownloadState::MediaFire(s) => s.url.clone(),
        DownloadState::GameFileSource(s) => format!("Game file: {}", s.game_file),
        DownloadState::Manual(s) => s.url.clone(),
        DownloadState::Mega(s) => s.url.clone(),
    }
}

/// Extract the root cause error message (skip context chain)
fn root_cause(e: &anyhow::Error) -> String {
    // Get the deepest error in the chain
    let root = e.root_cause();
    let msg = root.to_string();
    // Truncate if too long
    if msg.len() > 80 {
        format!("{}...", &msg[..77])
    } else {
        msg
    }
}

/// Get the original source URL for proxy-able download states
fn get_proxy_source_url(state: &DownloadState) -> Option<String> {
    match state {
        DownloadState::GoogleDrive(s) => Some(format!(
            "https://drive.google.com/uc?id={}&export=download",
            s.id
        )),
        DownloadState::Mega(s) => Some(s.url.clone()),
        DownloadState::MediaFire(s) => Some(s.url.clone()),
        _ => None,
    }
}

/// Build the proxy URL for a given source URL
fn build_proxy_url(source_url: &str) -> String {
    // Manual percent-encoding of the source URL for the query parameter
    let encoded: String = source_url
        .bytes()
        .flat_map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![b as char]
            }
            _ => format!("%{:02X}", b).chars().collect(),
        })
        .collect();
    format!("{}?uri={}", PROXY_BASE_URL, encoded)
}

/// Download a file through the Wabbajack proxy
async fn download_via_proxy(
    client: &HttpClient,
    source_url: &str,
    output_path: &Path,
    expected_size: u64,
    _handle: &Arc<dyn ProgressHandle>,
    callback: Option<&HttpProgressCallback>,
) -> Result<()> {
    let proxy_url = build_proxy_url(source_url);
    debug!("Downloading via proxy: {}", proxy_url);
    download_file_with_callback(
        client,
        &proxy_url,
        output_path,
        Some(expected_size),
        callback,
    )
    .await?;
    Ok(())
}

/// Convert a base64 hash to hex string for mirror URL
fn hash_to_hex(base64_hash: &str) -> Result<String> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(base64_hash)
        .context("Invalid base64 hash for mirror lookup")?;
    Ok(bytes.iter().map(|b| format!("{:02X}", b)).collect())
}

/// Download a file from the Wabbajack mirror CDN (by hash)
async fn download_from_mirror(
    cdn: &WabbajackCdnDownloader,
    hash: &str,
    output_path: &Path,
    expected_size: u64,
) -> Result<()> {
    let hash_hex = hash_to_hex(hash)?;
    let mirror_url = format!("{}/{}", MIRROR_BASE_URL, hash_hex);
    debug!("Downloading from mirror: {}", mirror_url);
    cdn.download(&mirror_url, output_path, expected_size)
        .await?;
    Ok(())
}

/// Download a single archive based on its source type (with retry)
/// Returns Ok with optional (url, expires) to cache on success
async fn download_archive(
    state: &DownloadState,
    archive: &ArchiveInfo,
    output_path: &Path,
    ctx: &DownloadContext,
    handle: &Arc<dyn ProgressHandle>,
) -> Result<Option<(String, i64)>> {
    let mut attempt = 0u32;
    let mut rate_limit_retries = 0u32;
    let display_name = truncate_name(&archive.name, 40);
    let expected_size = archive.size as u64;
    let is_alt_variant = crate::installer::game_preflight::has_known_alt_variant(&archive.name);

    loop {
        attempt += 1;

        // Reset progress for retry
        if attempt > 1 {
            handle.set_bytes(0, expected_size, 0.0);
            handle.set_message(&display_name);
            // Remove partial file if exists
            let _ = std::fs::remove_file(output_path);
        }

        let result = download_archive_inner(state, archive, output_path, ctx, handle).await;

        match result {
            Ok(((), url_to_cache)) => {
                // Verify file size immediately after download
                match std::fs::metadata(output_path) {
                    Ok(meta) => {
                        let actual_size = meta.len();
                        if actual_size != expected_size && !is_alt_variant {
                            // Size mismatch - delete and retry
                            let _ = std::fs::remove_file(output_path);
                            if attempt < MAX_RETRIES {
                                ctx.reporter.log(&format!(
                                    "Size mismatch for {} (got {} expected {}), retrying...",
                                    truncate_name(&archive.name, 25),
                                    actual_size,
                                    expected_size
                                ));
                                tokio::time::sleep(RETRY_DELAY).await;
                                continue;
                            } else {
                                bail!(
                                    "Size mismatch after {} attempts: expected {} bytes, got {}",
                                    MAX_RETRIES,
                                    expected_size,
                                    actual_size
                                );
                            }
                        } else if actual_size != expected_size {
                            warn!(
                                "{} has different size (known CC alt-variant, got {} expected {}) — accepting",
                                archive.name, actual_size, expected_size
                            );
                        }
                    }
                    Err(e) => {
                        if attempt < MAX_RETRIES {
                            ctx.reporter.log(&format!(
                                "Cannot verify {} ({}), retrying...",
                                truncate_name(&archive.name, 25),
                                e
                            ));
                            tokio::time::sleep(RETRY_DELAY).await;
                            continue;
                        } else {
                            bail!("Cannot verify downloaded file: {}", e);
                        }
                    }
                }

                // Verify hash after size check passes
                handle.set_message(&format!(
                    "{} (verifying...)",
                    truncate_name(&archive.name, 30)
                ));

                match verify_file_hash(output_path, &archive.hash) {
                    Ok(true) => {
                        // Hash matches - success!
                    }
                    Ok(false) if is_alt_variant => {
                        // CC alt-variant (Steam vs Bethesda / patch drift) — accept it
                        warn!(
                            "{} has different hash (known CC alt-variant) — accepting",
                            archive.name
                        );
                    }
                    Ok(false) => {
                        // Hash mismatch - corrupted download, delete and retry
                        let _ = std::fs::remove_file(output_path);
                        if attempt < MAX_RETRIES {
                            ctx.reporter.log(&format!(
                                "Hash mismatch for {}, re-downloading...",
                                truncate_name(&archive.name, 35)
                            ));
                            tokio::time::sleep(RETRY_DELAY).await;
                            continue;
                        } else {
                            bail!(
                                "Hash verification failed after {} attempts for {}",
                                MAX_RETRIES,
                                archive.name
                            );
                        }
                    }
                    Err(e) => {
                        // Hash computation failed - treat as retry
                        if attempt < MAX_RETRIES {
                            ctx.reporter.log(&format!(
                                "Hash verify error for {} ({}), retrying...",
                                truncate_name(&archive.name, 25),
                                e
                            ));
                            tokio::time::sleep(RETRY_DELAY).await;
                            continue;
                        } else {
                            bail!("Hash verification error: {}", e);
                        }
                    }
                }

                return Ok(url_to_cache);
            }
            Err(e) => {
                let error_str = format!("{:#}", e);
                let is_rate_limit =
                    error_str.contains("429") || error_str.to_lowercase().contains("rate limit");

                if is_rate_limit {
                    rate_limit_retries += 1;
                    if rate_limit_retries <= MAX_RATE_LIMIT_RETRIES {
                        // Exponential backoff: 30s, 60s, 120s, 240s, 300s (capped)
                        let delay_secs = (RATE_LIMIT_BASE_DELAY.as_secs()
                            << (rate_limit_retries - 1).min(3))
                        .min(300);
                        handle.set_message(&format!("Rate limited, waiting {}s...", delay_secs));
                        ctx.reporter.log(&format!(
                            "Rate limit hit for {}, waiting {}s (retry {}/{})",
                            truncate_name(&archive.name, 25),
                            delay_secs,
                            rate_limit_retries,
                            MAX_RATE_LIMIT_RETRIES
                        ));
                        tokio::time::sleep(Duration::from_secs(delay_secs)).await;
                        continue;
                    }
                } else if attempt < MAX_RETRIES {
                    // Regular retry for network errors
                    tokio::time::sleep(RETRY_DELAY).await;
                    continue;
                }

                // Primary download exhausted retries - try proxy/mirror fallback
                // for proxyable sources (Google Drive, Mega, MediaFire)
                if let Some(source_url) = get_proxy_source_url(state) {
                    let progress_callback = make_progress_callback(
                        archive.name.clone(),
                        &ctx.config.progress_callback,
                        handle,
                    );
                    let callback_ref = progress_callback.as_ref();

                    // Try Wabbajack proxy
                    handle.set_bytes(0, expected_size, 0.0);
                    handle.set_message(&format!("{} (proxy)", truncate_name(&archive.name, 30)));
                    let _ = std::fs::remove_file(output_path);

                    info!(
                        "Primary download failed for {}, trying Wabbajack proxy",
                        archive.name
                    );
                    match download_via_proxy(
                        &ctx.http,
                        &source_url,
                        output_path,
                        expected_size,
                        handle,
                        callback_ref,
                    )
                    .await
                    {
                        Ok(()) => {
                            info!("Proxy download succeeded for {}", archive.name);
                            // Skip hash check here - the outer loop handles it
                            // but we need to return Ok to trigger verification
                            return Ok(None);
                        }
                        Err(proxy_err) => {
                            debug!("Proxy failed for {}: {}", archive.name, proxy_err);
                        }
                    }

                    // Try Wabbajack mirror (CDN by hash)
                    handle.set_bytes(0, expected_size, 0.0);
                    handle.set_message(&format!("{} (mirror)", truncate_name(&archive.name, 30)));
                    let _ = std::fs::remove_file(output_path);

                    info!("Proxy failed for {}, trying Wabbajack mirror", archive.name);
                    match download_from_mirror(&ctx.cdn, &archive.hash, output_path, expected_size)
                        .await
                    {
                        Ok(()) => {
                            info!("Mirror download succeeded for {}", archive.name);
                            return Ok(None);
                        }
                        Err(mirror_err) => {
                            debug!("Mirror failed for {}: {}", archive.name, mirror_err);
                        }
                    }
                }

                return Err(e);
            }
        }
    }
}

/// Create a progress callback that emits ProgressEvent::DownloadProgress
fn make_progress_callback(
    archive_name: String,
    callback: &Option<crate::installer::config::ProgressCallback>,
    handle: &Arc<dyn ProgressHandle>,
) -> Option<HttpProgressCallback> {
    // Always create a callback that updates the ProgressHandle (CLI bars).
    // Optionally also forward to the legacy GUI ProgressEvent callback.
    let gui_cb = callback.clone();
    let name = archive_name;
    let h = Arc::clone(handle);
    Some(Box::new(move |downloaded: u64, total: u64, speed: f64| {
        h.set_bytes(downloaded, total, speed);
        if let Some(ref cb) = gui_cb {
            cb(ProgressEvent::DownloadProgress {
                name: name.clone(),
                downloaded,
                total,
                speed,
            });
        }
    }) as HttpProgressCallback)
}

/// Inner download function (single attempt)
async fn download_archive_inner(
    state: &DownloadState,
    archive: &ArchiveInfo,
    output_path: &Path,
    ctx: &DownloadContext,
    handle: &Arc<dyn ProgressHandle>,
) -> Result<((), Option<(String, i64)>)> {
    // Create progress callback for GUI updates
    let progress_callback =
        make_progress_callback(archive.name.clone(), &ctx.config.progress_callback, handle);
    let callback_ref = progress_callback.as_ref();

    // Returns (result, optional url to cache)
    match state {
        DownloadState::Nexus(nexus_state) => {
            let domain = NexusDownloader::game_domain(&nexus_state.game_name);

            // Check if we have a valid cached URL
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;

            let (url, url_to_cache) = if let (Some(cached_url), Some(expires)) =
                (&archive.cached_url, archive.url_expires)
            {
                if expires > now + 300 {
                    // Valid for at least 5 more minutes
                    (cached_url.clone(), None)
                } else {
                    // Expired, fetch new URL
                    let url = ctx
                        .nexus
                        .get_download_link(domain, nexus_state.mod_id, nexus_state.file_id)
                        .await
                        .with_context(|| {
                            format!(
                                "Nexus: {}/mods/{}/files/{}",
                                domain, nexus_state.mod_id, nexus_state.file_id
                            )
                        })?;
                    // Cache for 4 hours
                    let expires = now + 4 * 3600;
                    (url.clone(), Some((url, expires)))
                }
            } else {
                // No cache, fetch new URL
                let url = ctx
                    .nexus
                    .get_download_link(domain, nexus_state.mod_id, nexus_state.file_id)
                    .await
                    .with_context(|| {
                        format!(
                            "Nexus: {}/mods/{}/files/{}",
                            domain, nexus_state.mod_id, nexus_state.file_id
                        )
                    })?;
                // Cache for 4 hours
                let expires = now + 4 * 3600;
                (url.clone(), Some((url, expires)))
            };

            // Download the file with progress
            download_file_with_callback(
                &ctx.http,
                &url,
                output_path,
                Some(archive.size as u64),
                callback_ref,
            )
            .await?;
            Ok(((), url_to_cache))
        }

        DownloadState::Http(http_state) => {
            download_file_with_callback(
                &ctx.http,
                &http_state.url,
                output_path,
                Some(archive.size as u64),
                callback_ref,
            )
            .await?;
            Ok(((), None))
        }

        DownloadState::WabbajackCDN(cdn_state) => {
            // CDN has its own progress tracking, we just update at the end
            ctx.cdn
                .download(&cdn_state.url, output_path, archive.size as u64)
                .await?;
            handle.set_bytes(archive.size as u64, archive.size as u64, 0.0);
            // Report final progress for GUI
            if let Some(ref cb) = ctx.config.progress_callback {
                cb(ProgressEvent::DownloadProgress {
                    name: archive.name.clone(),
                    downloaded: archive.size as u64,
                    total: archive.size as u64,
                    speed: 0.0, // CDN doesn't provide speed info
                });
            }
            Ok(((), None))
        }

        DownloadState::GoogleDrive(gd_state) => {
            // Try direct GDrive download first, fall back to proxy/mirror on quota errors
            match ctx
                .gdrive
                .download_to_file(&gd_state.id, output_path, archive.size as u64, None)
                .await
            {
                Ok(()) => {
                    handle.set_bytes(archive.size as u64, archive.size as u64, 0.0);
                    if let Some(ref cb) = ctx.config.progress_callback {
                        cb(ProgressEvent::DownloadProgress {
                            name: archive.name.clone(),
                            downloaded: archive.size as u64,
                            total: archive.size as u64,
                            speed: 0.0,
                        });
                    }
                }
                Err(_) => {
                    // GDrive failed (likely quota exceeded) — report for manual download
                    let url = format!("https://drive.google.com/file/d/{}/view", gd_state.id);
                    bail!(
                        "GDrive download failed (likely quota exceeded, download manually): {}",
                        url
                    );
                }
            }
            Ok(((), None))
        }

        DownloadState::MediaFire(mf_state) => {
            let url = ctx.mediafire.get_download_url(&mf_state.url).await?;
            download_file_with_callback(
                &ctx.http,
                &url,
                output_path,
                Some(archive.size as u64),
                callback_ref,
            )
            .await?;
            Ok(((), None))
        }

        DownloadState::GameFileSource(gf_state) => {
            copy_game_file(gf_state, archive, output_path, &ctx.config)?;
            handle.set_bytes(archive.size as u64, archive.size as u64, 0.0);
            // Report final progress for GUI (game file copies are instant)
            if let Some(ref cb) = ctx.config.progress_callback {
                cb(ProgressEvent::DownloadProgress {
                    name: archive.name.clone(),
                    downloaded: archive.size as u64,
                    total: archive.size as u64,
                    speed: 0.0,
                });
            }
            Ok(((), None))
        }

        DownloadState::Mega(mega_state) => {
            // Try native Mega download first, fall back to Wabbajack proxy
            info!("Mega download for {} - trying native API", archive.name);
            handle.set_message(&format!("Mega: {}", truncate_name(&archive.name, 30)));

            match crate::downloaders::mega_native::download_mega_file(&mega_state.url, output_path)
                .await
            {
                Ok(()) => {
                    info!("Mega native download succeeded for {}", archive.name);
                    if let Ok(meta) = std::fs::metadata(output_path) {
                        handle.set_bytes(meta.len(), meta.len(), 0.0);
                    }
                }
                Err(e) => {
                    warn!(
                        "Mega native download failed for {}: {} — trying proxy",
                        archive.name, e
                    );
                    download_via_proxy(
                        &ctx.http,
                        &mega_state.url,
                        output_path,
                        archive.size as u64,
                        handle,
                        callback_ref,
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "Mega download failed for {} ({}) — native and proxy both failed",
                            archive.name, mega_state.url
                        )
                    })?;
                }
            }
            Ok(((), None))
        }

        // Manual downloads are handled by check_manual()
        DownloadState::Manual(manual_state) => {
            if is_moddb_url(&manual_state.url) {
                let resolved_url = resolve_moddb_download_url(&ctx.http, &manual_state.url)
                    .await
                    .with_context(|| {
                        format!("Failed to resolve ModDB download URL: {}", manual_state.url)
                    })?;

                info!(
                    "Resolved ModDB URL for {}: {} -> {}",
                    archive.name, manual_state.url, resolved_url
                );

                download_file_with_callback(
                    &ctx.http,
                    &resolved_url,
                    output_path,
                    Some(archive.size as u64),
                    callback_ref,
                )
                .await?;
                Ok(((), None))
            } else if is_loverslab_url(&manual_state.url) {
                if let Some(ll) = &ctx.loverslab {
                    // Show waiting status while queued for LL semaphore
                    handle.set_message(&format!(
                        "LL (queued): {}",
                        truncate_name(&archive.name, 30)
                    ));

                    // Acquire semaphore to enforce sequential LL downloads
                    let _permit = ctx
                        .ll_semaphore
                        .acquire()
                        .await
                        .map_err(|_| anyhow::anyhow!("LoversLab semaphore closed"))?;

                    handle.set_message(&format!("LL: {}", truncate_name(&archive.name, 35)));

                    let result = ll
                        .download(&manual_state.url, &archive.name, output_path)
                        .await;

                    match result {
                        Ok(()) => Ok(((), None)),
                        Err(e) => {
                            let err_msg = format!("{:#}", e);
                            // If LL redirected to Mega (lost URL fragment), try proxy/mirror
                            let err_lower = err_msg.to_lowercase();
                            if err_lower.contains("invalid")
                                || err_lower.contains("public url")
                                || err_lower.contains("mega")
                            {
                                warn!(
                                    "LL download for {} redirected to Mega (key lost) — trying mirror",
                                    archive.name
                                );
                                download_from_mirror(
                                    &ctx.cdn,
                                    &archive.hash,
                                    output_path,
                                    archive.size as u64,
                                )
                                .await
                                .with_context(|| {
                                    format!(
                                        "LL/Mega download failed for {} — mirror also failed",
                                        archive.name
                                    )
                                })?;
                                Ok(((), None))
                            } else {
                                Err(e).with_context(|| {
                                    format!(
                                        "LoversLab download failed for {} ({})",
                                        archive.name, manual_state.url
                                    )
                                })
                            }
                        }
                    }
                } else {
                    bail!(
                        "LoversLab download required but credentials not configured: {}",
                        manual_state.url
                    )
                }
            } else if is_mediafire_url(&manual_state.url) {
                // MediaFire: resolve direct download URL from page
                let url = ctx
                    .mediafire
                    .get_download_url(&manual_state.url)
                    .await
                    .with_context(|| {
                        format!("Failed to resolve MediaFire URL: {}", manual_state.url)
                    })?;
                info!("Resolved MediaFire URL for {}: {}", archive.name, url);
                download_file_with_callback(
                    &ctx.http,
                    &url,
                    output_path,
                    Some(archive.size as u64),
                    callback_ref,
                )
                .await?;
                Ok(((), None))
            } else if is_mega_url(&manual_state.url) {
                info!(
                    "Mega manual download for {} - trying native API",
                    archive.name
                );
                handle.set_message(&format!("Mega: {}", truncate_name(&archive.name, 30)));

                match crate::downloaders::mega_native::download_mega_file(
                    &manual_state.url,
                    output_path,
                )
                .await
                {
                    Ok(()) => {
                        info!("Mega native download succeeded for {}", archive.name);
                        if let Ok(meta) = std::fs::metadata(output_path) {
                            handle.set_bytes(meta.len(), meta.len(), 0.0);
                        }
                        Ok(((), None))
                    }
                    Err(e) => {
                        warn!(
                            "Mega native download failed for {}: {} — trying proxy",
                            archive.name, e
                        );
                        download_via_proxy(
                            &ctx.http,
                            &manual_state.url,
                            output_path,
                            archive.size as u64,
                            handle,
                            callback_ref,
                        )
                        .await
                        .with_context(|| {
                            format!(
                                "Mega download failed for {} ({}) — native and proxy both failed",
                                archive.name, manual_state.url
                            )
                        })?;
                        Ok(((), None))
                    }
                }
            } else if is_yandex_url(&manual_state.url) {
                let url = ctx
                    .yandex
                    .get_download_url(&manual_state.url, Some(&archive.name))
                    .await
                    .with_context(|| {
                        format!("Failed to resolve Yandex Disk URL: {}", manual_state.url)
                    })?;
                info!("Resolved Yandex URL for {}: {}", archive.name, url);
                download_file_with_callback(
                    &ctx.http,
                    &url,
                    output_path,
                    Some(archive.size as u64),
                    callback_ref,
                )
                .await?;
                Ok(((), None))
            } else {
                bail!(
                    "Manual download required and no automation handler available: {}",
                    manual_state.url
                )
            }
        }
    }
}

/// Copy a file from the game installation directory
fn copy_game_file(
    state: &crate::modlist::GameFileSourceState,
    archive: &ArchiveInfo,
    output_path: &Path,
    config: &InstallConfig,
) -> Result<()> {
    let game_file_path = &state.game_file;

    // Check game directory and downloads directory
    let mut source_path: Option<PathBuf> = None;
    for (base, relative) in [
        (&config.game_dir, game_file_path.as_str()),
        (
            &config.game_dir,
            &format!("Data/{}", game_file_path) as &str,
        ),
    ] {
        if let Some(resolved) = crate::paths::resolve_case_insensitive(base, relative) {
            if resolved.exists() {
                source_path = Some(resolved);
                break;
            }
        }
    }
    if source_path.is_none() {
        let dl_path = config.downloads_dir.join(&archive.name);
        if dl_path.exists() {
            source_path = Some(dl_path);
        }
    }

    let source = source_path.with_context(|| {
        format!(
            "Game file not found: {} in {} or downloads dir",
            game_file_path,
            config.game_dir.display()
        )
    })?;

    // Skip copy if source and output are the same file
    if source == output_path {
        return Ok(());
    }

    // Create parent directory
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }

    // Copy the file
    let bytes_copied = fs::copy(&source, output_path).with_context(|| {
        format!(
            "Failed to copy {} to {}",
            source.display(),
            output_path.display()
        )
    })?;

    // Verify size — but tolerate the curated alt-variant set (Steam vs
    // Bethesda Creation Club builds drift by a handful of metadata bytes
    // while staying runtime-equivalent; see `ALT_VARIANT_FILE_BASENAMES`).
    if bytes_copied != archive.size as u64 {
        if crate::installer::game_preflight::has_known_alt_variant(game_file_path)
            || crate::installer::game_preflight::has_known_alt_variant(&archive.name)
        {
            warn!(
                "Game file {} size differs (expected {}, got {}) but is on the \
                 known alt-variant list — accepting",
                game_file_path, archive.size, bytes_copied
            );
        } else {
            bail!(
                "Size mismatch copying game file: expected {}, got {}",
                archive.size,
                bytes_copied
            );
        }
    }

    Ok(())
}

/// Download non-Nexus files directly.
async fn download_pending_archives_streaming(
    db: &ModlistDb,
    config: &InstallConfig,
    tx: &std::sync::mpsc::SyncSender<ArchiveEvent>,
    pending: Vec<ArchiveInfo>,
) -> Result<DownloadStats> {
    let reporter = &config.reporter;
    let concurrency = config.max_concurrent_downloads;

    reporter.overall_set_total(pending.len() as u64);
    reporter.overall_set_message("Starting automatic downloads...");

    let total_archives = pending.len();
    let ctx = Arc::new(build_context(config, total_archives).await?);

    stream::iter(pending)
        .map(|archive| {
            let ctx = Arc::clone(&ctx);
            async move {
                let output_path = ctx.config.downloads_dir.join(&archive.name);
                let (result, url_to_cache) = process_archive(&ctx, &archive, &output_path).await;
                (archive, output_path, result, url_to_cache)
            }
        })
        .buffer_unordered(concurrency)
        .for_each(|(archive, output_path, result, url_to_cache)| {
            if let Some((url, expires)) = url_to_cache {
                let _ = db.cache_download_url(&archive.hash, &url, expires);
            }

            match result {
                DownloadResult::Success | DownloadResult::Skipped => {
                    let _ = db.mark_archive_downloaded(
                        &archive.hash,
                        output_path.to_string_lossy().as_ref(),
                    );
                    if matches!(result, DownloadResult::Success) {
                        if let Err(e) =
                            super::sidecar::write_archive_hash(&output_path, &archive.hash)
                        {
                            tracing::debug!(
                                "Failed to write archive sidecar for {}: {}",
                                output_path.display(),
                                e
                            );
                        }
                    }
                    let _ = tx.send(ArchiveEvent::Ready {
                        hash: archive.hash.clone(),
                        name: archive.name.clone(),
                        path: output_path,
                    });
                }
                DownloadResult::Manual => {
                    let _ = tx.send(ArchiveEvent::Manual {
                        hash: archive.hash.clone(),
                        name: archive.name.clone(),
                    });
                }
                DownloadResult::Failed => {
                    let _ = tx.send(ArchiveEvent::Failed {
                        hash: archive.hash.clone(),
                        name: archive.name.clone(),
                        error: "Download failed".to_string(),
                    });
                }
            }
            futures::future::ready(())
        })
        .await;

    reporter.overall_finish();

    let manual_downloads_list = ctx.manual_downloads.lock().await.clone();
    let failed_downloads_list = ctx.failed_downloads.lock().await.clone();

    Ok(DownloadStats {
        downloaded: ctx.downloaded.load(Ordering::Relaxed),
        skipped: ctx.skipped.load(Ordering::Relaxed),
        failed: ctx.failed.load(Ordering::Relaxed),
        manual: manual_downloads_list.len(),
        failed_downloads: failed_downloads_list,
        manual_downloads: manual_downloads_list,
    })
}

async fn download_non_nexus_files(
    db: &ModlistDb,
    config: &InstallConfig,
    pending: Vec<ArchiveInfo>,
) -> Result<DownloadStats> {
    let reporter = &config.reporter;
    let concurrency = config.max_concurrent_downloads;

    // Setup progress
    reporter.overall_set_total(pending.len() as u64);
    reporter.overall_set_message("Starting downloads...");

    let total_archives = pending.len();
    let ctx = Arc::new(build_context(config, total_archives).await?);

    let results: Vec<DownloadResultTuple> = stream::iter(pending)
        .map(|archive| {
            let ctx = Arc::clone(&ctx);
            async move {
                let output_path = ctx.config.downloads_dir.join(&archive.name);
                let (result, url_to_cache) = process_archive(&ctx, &archive, &output_path).await;
                (archive.hash.clone(), output_path, result, url_to_cache)
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;

    for (hash, output_path, result, url_to_cache) in results {
        // Cache URL if we got one
        if let Some((url, expires)) = url_to_cache {
            let _ = db.cache_download_url(&hash, &url, expires);
        }

        match result {
            DownloadResult::Success | DownloadResult::Skipped => {
                db.mark_archive_downloaded(&hash, output_path.to_string_lossy().as_ref())?;
                if matches!(result, DownloadResult::Success) {
                    if let Err(e) = super::sidecar::write_archive_hash(&output_path, &hash) {
                        tracing::debug!(
                            "Failed to write archive sidecar for {}: {}",
                            output_path.display(),
                            e
                        );
                    }
                }
            }
            DownloadResult::Manual | DownloadResult::Failed => {}
        }
    }

    reporter.overall_finish();

    let manual_downloads_list = ctx.manual_downloads.lock().await.clone();
    let failed_downloads_list = ctx.failed_downloads.lock().await.clone();

    Ok(DownloadStats {
        downloaded: ctx.downloaded.load(Ordering::Relaxed),
        skipped: ctx.skipped.load(Ordering::Relaxed),
        failed: ctx.failed.load(Ordering::Relaxed),
        manual: manual_downloads_list.len(),
        failed_downloads: failed_downloads_list,
        manual_downloads: manual_downloads_list,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_moddb_start_url_from_addon_page() {
        let html = r#"<a href="/addons/start/88982" class="button">Download</a>"#;
        let start = extract_moddb_start_url(html).expect("missing start URL");
        assert_eq!(start, "https://www.moddb.com/addons/start/88982");
    }

    #[test]
    fn extract_moddb_mirror_url_from_start_page() {
        let html =
            r#"window.location.href="https://www.moddb.com/downloads/mirror/88982/130/abc123";"#;
        let mirror = extract_moddb_mirror_url(html).expect("missing mirror URL");
        assert_eq!(
            mirror,
            "https://www.moddb.com/downloads/mirror/88982/130/abc123"
        );
    }
}
