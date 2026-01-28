//! Download phase coordinator
//!
//! Coordinates downloading all archives from various sources with parallel downloads.
//! Supports both direct API mode (premium) and NXM browser mode (free/rate-limit bypass).

use crate::downloaders::{
    download_file_with_progress, GoogleDriveDownloader, HttpClient, MediaFireDownloader,
    NexusDownloader, WabbajackCdnDownloader,
};
use crate::modlist::{ArchiveInfo, DownloadState, ModlistDb, NexusState};
use crate::nxm_handler;

use super::config::InstallConfig;

use anyhow::{bail, Context, Result};
use futures::stream::{self, StreamExt};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

/// Max retries for network operations
const MAX_RETRIES: u32 = 3;
/// Delay between retries
const RETRY_DELAY: Duration = Duration::from_secs(2);
/// Max retries for rate limit errors (429)
const MAX_RATE_LIMIT_RETRIES: u32 = 10;
/// Initial delay for rate limit retry (increases exponentially)
const RATE_LIMIT_BASE_DELAY: Duration = Duration::from_secs(30);

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
#[derive(Debug, Default)]
pub struct DownloadStats {
    pub downloaded: usize,
    pub skipped: usize,
    pub failed: usize,
    pub manual: usize,
}

/// Information about a manual download needed
#[derive(Debug, Clone)]
pub struct ManualDownloadInfo {
    pub name: String,
    pub url: String,
    pub prompt: Option<String>,
    pub expected_size: u64,
}

/// Information about a failed download
#[derive(Debug, Clone)]
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
    config: InstallConfig,
    multi_progress: MultiProgress,
    overall_pb: ProgressBar,
    // Counters
    downloaded: AtomicUsize,
    skipped: AtomicUsize,
    failed: AtomicUsize,
    manual_downloads: Mutex<Vec<ManualDownloadInfo>>,
    failed_downloads: Mutex<Vec<FailedDownloadInfo>>,
}

/// Download all pending archives (smart mode: only downloads archives needed for missing outputs)
pub async fn download_archives(db: &ModlistDb, config: &InstallConfig) -> Result<DownloadStats> {
    // Smart check: scan output directory to find what's missing
    println!("Scanning output directory for existing files...");
    let existing_outputs = scan_existing_outputs(&config.output_dir)?;
    println!("Found {} existing output files", existing_outputs.len());

    // Get all directives that need archives and check which outputs are missing
    let directive_outputs = db.get_directive_outputs_with_archives()?;
    let mut needed_archives: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut missing_count = 0;

    for (to_path, size, archive_hash) in &directive_outputs {
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
        println!("All output files exist - no downloads needed!");
        return Ok(DownloadStats::default());
    }

    println!("Found {} missing outputs requiring {} archives", missing_count, needed_archives.len());

    // Get archive info for needed archives
    let needed_hashes: Vec<String> = needed_archives.into_iter().collect();
    let archives_to_check = db.get_archives_by_hashes(&needed_hashes)?;

    // Now check which of these archives are actually downloaded (with correct size)
    let mut already_downloaded = 0usize;
    let mut need_download: Vec<ArchiveInfo> = Vec::new();
    let mut truncated_count = 0usize;

    for archive in archives_to_check {
        let output_path = config.downloads_dir.join(&archive.name);
        if output_path.exists() {
            if let Ok(meta) = fs::metadata(&output_path) {
                if meta.len() == archive.size as u64 {
                    // Already downloaded with correct size - mark in DB
                    db.mark_archive_downloaded(&archive.hash, output_path.to_string_lossy().as_ref())?;
                    already_downloaded += 1;
                    continue;
                } else {
                    // File exists but wrong size - truncated/corrupted, delete and re-download
                    println!("  Truncated: {} ({}% complete)",
                        archive.name,
                        (meta.len() * 100) / (archive.size as u64).max(1));
                    let _ = fs::remove_file(&output_path);
                    truncated_count += 1;
                }
            }
        }
        need_download.push(archive);
    }

    if truncated_count > 0 {
        println!("Found {} truncated/corrupted archives - will re-download", truncated_count);
    }

    if already_downloaded > 0 {
        println!("Found {} archives already downloaded", already_downloaded);
    }

    if need_download.is_empty() {
        println!("All needed archives already downloaded!");
        return Ok(DownloadStats {
            downloaded: 0,
            skipped: already_downloaded,
            failed: 0,
            manual: 0,
        });
    }

    println!("Need to download {} archives", need_download.len());

    // Route to NXM mode if enabled
    if config.nxm_mode {
        let mut stats = download_archives_nxm(db, config, need_download).await?;
        stats.skipped += already_downloaded;
        return Ok(stats);
    }

    let pending = need_download;

    // Setup progress display
    let multi_progress = MultiProgress::new();
    let overall_pb = multi_progress.add(ProgressBar::new(pending.len() as u64));
    overall_pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) | {msg}")
            .unwrap()
            .progress_chars("=>-"),
    );
    overall_pb.enable_steady_tick(Duration::from_millis(100));
    overall_pb.set_message("Starting downloads...");

    // Create shared context
    let ctx = Arc::new(DownloadContext {
        nexus: NexusDownloader::new(&config.nexus_api_key)?,
        http: HttpClient::new()?,
        cdn: WabbajackCdnDownloader::new()?,
        gdrive: GoogleDriveDownloader::new()?,
        mediafire: MediaFireDownloader::new()?,
        config: config.clone(),
        multi_progress,
        overall_pb,
        downloaded: AtomicUsize::new(0),
        skipped: AtomicUsize::new(0),
        failed: AtomicUsize::new(0),
        manual_downloads: Mutex::new(Vec::new()),
        failed_downloads: Mutex::new(Vec::new()),
    });

    // Process downloads in parallel
    let concurrency = config.max_concurrent_downloads;

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
            }
            DownloadResult::Skipped => {
                db.mark_archive_downloaded(&hash, output_path.to_string_lossy().as_ref())?;
            }
            DownloadResult::Manual | DownloadResult::Failed => {
                // Don't mark as downloaded
            }
        }
    }

    ctx.overall_pb.finish_and_clear();

    // Collect stats (include pre-scan skipped count)
    let stats = DownloadStats {
        downloaded: ctx.downloaded.load(Ordering::Relaxed),
        skipped: ctx.skipped.load(Ordering::Relaxed) + already_downloaded,
        failed: ctx.failed.load(Ordering::Relaxed),
        manual: ctx.manual_downloads.lock().await.len(),
    };

    // Print manual download instructions
    let manual_downloads = ctx.manual_downloads.lock().await;
    if !manual_downloads.is_empty() {
        println!("\n=== Manual Downloads Required ({}) ===", manual_downloads.len());
        println!("Please download the following files to: {}\n", config.downloads_dir.display());

        for (i, md) in manual_downloads.iter().enumerate() {
            println!("{}. {}", i + 1, md.name);
            println!("   URL: {}", md.url);
            println!("   Expected size: {} bytes", md.expected_size);
            if let Some(prompt) = &md.prompt {
                println!("   Note: {}", prompt);
            }
            println!();
        }

        println!("After downloading, run the command again to continue.\n");
    }

    // Print failed download instructions
    let failed_downloads = ctx.failed_downloads.lock().await;
    if !failed_downloads.is_empty() {
        println!("\n=== Failed Downloads ({}) ===", failed_downloads.len());
        println!("These downloads failed. Try manually downloading to: {}\n", config.downloads_dir.display());

        for (i, fd) in failed_downloads.iter().enumerate() {
            println!("{}. {}", i + 1, fd.name);
            println!("   URL: {}", fd.url);
            println!("   Error: {}", fd.error);
            println!("   Expected size: {} bytes ({:.2} MB)", fd.expected_size, fd.expected_size as f64 / 1024.0 / 1024.0);
            println!();
        }

        println!("After downloading, run the command again to continue.\n");
    }

    // Print summary
    println!("\n=== Download Summary ===");
    println!("Downloaded: {}", stats.downloaded);
    println!("Skipped:    {}", stats.skipped);
    println!("Manual:     {}", stats.manual);
    println!("Failed:     {}", stats.failed);

    // Print Nexus rate limits
    let limits = ctx.nexus.rate_limits();
    println!(
        "\nNexus API: {}/{} hourly, {}/{} daily",
        limits.hourly_remaining, limits.hourly_limit,
        limits.daily_remaining, limits.daily_limit
    );

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
                ctx.overall_pb.inc(1);
                update_overall_message(ctx);
                return (DownloadResult::Skipped, None);
            }
        }
    }

    // Parse the download state
    let state: DownloadState = match serde_json::from_str(&archive.state_json) {
        Ok(s) => s,
        Err(e) => {
            ctx.failed.fetch_add(1, Ordering::Relaxed);
            ctx.overall_pb.inc(1);
            ctx.overall_pb.println(format!("FAIL {} - parse error: {}", archive.name, e));
            return (DownloadResult::Failed, None);
        }
    };

    // Check for manual downloads first
    if let Some(manual_info) = check_manual(&state, archive) {
        ctx.manual_downloads.lock().await.push(manual_info);
        ctx.overall_pb.inc(1);
        update_overall_message(ctx);
        return (DownloadResult::Manual, None);
    }

    // Create progress bar for this download
    let pb = ctx.multi_progress.insert_before(&ctx.overall_pb, ProgressBar::new(archive.size as u64));
    pb.set_style(
        ProgressStyle::default_bar()
            .template("  {spinner:.blue} {wide_msg} [{bar:30.white/dim}] {bytes}/{total_bytes} {bytes_per_sec}")
            .unwrap()
            .progress_chars("=>-"),
    );
    pb.enable_steady_tick(Duration::from_millis(100));

    // Truncate filename for display
    let display_name = truncate_name(&archive.name, 40);
    pb.set_message(display_name.clone());

    // Download based on source type
    let source = source_type_name(&state);
    let result = download_archive(&state, archive, output_path, ctx, &pb).await;

    match result {
        Ok(url_to_cache) => {
            pb.finish_and_clear();
            ctx.downloaded.fetch_add(1, Ordering::Relaxed);
            ctx.overall_pb.inc(1);
            update_overall_message(ctx);
            (DownloadResult::Success, url_to_cache)
        }
        Err(e) => {
            pb.finish_and_clear();
            ctx.failed.fetch_add(1, Ordering::Relaxed);
            ctx.overall_pb.inc(1);
            let error_msg = root_cause(&e);
            ctx.overall_pb.println(format!(
                "FAIL [{}] {} - {}",
                source, truncate_name(&archive.name, 30), error_msg
            ));
            // Record failed download with URL for manual download
            ctx.failed_downloads.lock().await.push(FailedDownloadInfo {
                name: archive.name.clone(),
                url: get_manual_url(&state),
                error: error_msg,
                expected_size: archive.size as u64,
            });
            (DownloadResult::Failed, None)
        }
    }
}

/// Update the overall progress bar message with current stats
fn update_overall_message(ctx: &DownloadContext) {
    let downloaded = ctx.downloaded.load(Ordering::Relaxed);
    let skipped = ctx.skipped.load(Ordering::Relaxed);
    let failed = ctx.failed.load(Ordering::Relaxed);
    ctx.overall_pb.set_message(format!(
        "OK:{} Skip:{} Fail:{}",
        downloaded, skipped, failed
    ));
}

/// Check if this is a manual download type
fn check_manual(state: &DownloadState, archive: &ArchiveInfo) -> Option<ManualDownloadInfo> {
    match state {
        DownloadState::Manual(manual_state) => Some(ManualDownloadInfo {
            name: archive.name.clone(),
            url: manual_state.url.clone(),
            prompt: Some(manual_state.prompt.clone()),
            expected_size: archive.size as u64,
        }),
        DownloadState::Mega(mega_state) => Some(ManualDownloadInfo {
            name: archive.name.clone(),
            url: mega_state.url.clone(),
            prompt: Some("Mega downloads require manual download".to_string()),
            expected_size: archive.size as u64,
        }),
        _ => None,
    }
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
            format!("https://www.nexusmods.com/{}/mods/{}?tab=files", domain, s.mod_id)
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

/// Download a single archive based on its source type (with retry)
/// Returns Ok with optional (url, expires) to cache on success
async fn download_archive(
    state: &DownloadState,
    archive: &ArchiveInfo,
    output_path: &Path,
    ctx: &DownloadContext,
    pb: &ProgressBar,
) -> Result<Option<(String, i64)>> {
    let mut attempt = 0u32;
    let mut rate_limit_retries = 0u32;
    let display_name = truncate_name(&archive.name, 40);
    let expected_size = archive.size as u64;

    loop {
        attempt += 1;

        // Reset progress bar for retry
        if attempt > 1 {
            pb.set_position(0);
            pb.set_message(display_name.clone());
            // Remove partial file if exists
            let _ = std::fs::remove_file(output_path);
        }

        let result = download_archive_inner(state, archive, output_path, ctx, pb).await;

        match result {
            Ok(((), url_to_cache)) => {
                // Verify file size immediately after download
                match std::fs::metadata(output_path) {
                    Ok(meta) => {
                        let actual_size = meta.len();
                        if actual_size != expected_size {
                            // Size mismatch - delete and retry
                            let _ = std::fs::remove_file(output_path);
                            if attempt < MAX_RETRIES {
                                ctx.overall_pb.println(format!(
                                    "Size mismatch for {} (got {} expected {}), retrying...",
                                    truncate_name(&archive.name, 25), actual_size, expected_size
                                ));
                                tokio::time::sleep(RETRY_DELAY).await;
                                continue;
                            } else {
                                bail!(
                                    "Size mismatch after {} attempts: expected {} bytes, got {}",
                                    MAX_RETRIES, expected_size, actual_size
                                );
                            }
                        }
                    }
                    Err(e) => {
                        if attempt < MAX_RETRIES {
                            ctx.overall_pb.println(format!(
                                "Cannot verify {} ({}), retrying...",
                                truncate_name(&archive.name, 25), e
                            ));
                            tokio::time::sleep(RETRY_DELAY).await;
                            continue;
                        } else {
                            bail!("Cannot verify downloaded file: {}", e);
                        }
                    }
                }
                return Ok(url_to_cache);
            }
            Err(e) => {
                let error_str = format!("{:#}", e);
                let is_rate_limit = error_str.contains("429") || error_str.to_lowercase().contains("rate limit");

                if is_rate_limit {
                    rate_limit_retries += 1;
                    if rate_limit_retries <= MAX_RATE_LIMIT_RETRIES {
                        // Exponential backoff: 30s, 60s, 120s, 240s, 300s (capped)
                        let delay_secs = (RATE_LIMIT_BASE_DELAY.as_secs() << (rate_limit_retries - 1).min(3)).min(300);
                        pb.set_message(format!("Rate limited, waiting {}s...", delay_secs));
                        ctx.overall_pb.println(format!(
                            "Rate limit hit for {}, waiting {}s (retry {}/{})",
                            truncate_name(&archive.name, 25), delay_secs, rate_limit_retries, MAX_RATE_LIMIT_RETRIES
                        ));
                        tokio::time::sleep(Duration::from_secs(delay_secs)).await;
                        continue;
                    }
                } else if attempt < MAX_RETRIES {
                    // Regular retry for network errors
                    tokio::time::sleep(RETRY_DELAY).await;
                    continue;
                }

                return Err(e);
            }
        }
    }
}

/// Inner download function (single attempt)
async fn download_archive_inner(
    state: &DownloadState,
    archive: &ArchiveInfo,
    output_path: &Path,
    ctx: &DownloadContext,
    pb: &ProgressBar,
) -> Result<((), Option<(String, i64)>)> {
    // Returns (result, optional url to cache)
    match state {
        DownloadState::Nexus(nexus_state) => {
            let domain = NexusDownloader::game_domain(&nexus_state.game_name);

            // Check if we have a valid cached URL
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;

            let (url, url_to_cache) = if let (Some(cached_url), Some(expires)) = (&archive.cached_url, archive.url_expires) {
                if expires > now + 300 {  // Valid for at least 5 more minutes
                    (cached_url.clone(), None)
                } else {
                    // Expired, fetch new URL
                    let url = ctx.nexus
                        .get_download_link(domain, nexus_state.mod_id, nexus_state.file_id)
                        .await
                        .with_context(|| format!(
                            "Nexus: {}/mods/{}/files/{}",
                            domain, nexus_state.mod_id, nexus_state.file_id
                        ))?;
                    // Cache for 4 hours
                    let expires = now + 4 * 3600;
                    (url.clone(), Some((url, expires)))
                }
            } else {
                // No cache, fetch new URL
                let url = ctx.nexus
                    .get_download_link(domain, nexus_state.mod_id, nexus_state.file_id)
                    .await
                    .with_context(|| format!(
                        "Nexus: {}/mods/{}/files/{}",
                        domain, nexus_state.mod_id, nexus_state.file_id
                    ))?;
                // Cache for 4 hours
                let expires = now + 4 * 3600;
                (url.clone(), Some((url, expires)))
            };

            // Download the file with progress
            download_file_with_progress(&ctx.http, &url, output_path, Some(archive.size as u64), Some(pb)).await?;
            Ok(((), url_to_cache))
        }

        DownloadState::Http(http_state) => {
            download_file_with_progress(&ctx.http, &http_state.url, output_path, Some(archive.size as u64), Some(pb)).await?;
            Ok(((), None))
        }

        DownloadState::WabbajackCDN(cdn_state) => {
            // CDN has its own progress tracking, we just update at the end
            ctx.cdn.download(&cdn_state.url, output_path, archive.size as u64).await?;
            pb.set_position(archive.size as u64);
            Ok(((), None))
        }

        DownloadState::GoogleDrive(gd_state) => {
            // Use gdrive's own client to maintain cookies through the confirmation flow
            ctx.gdrive.download_to_file(&gd_state.id, output_path, archive.size as u64, Some(pb)).await?;
            Ok(((), None))
        }

        DownloadState::MediaFire(mf_state) => {
            let url = ctx.mediafire.get_download_url(&mf_state.url).await?;
            download_file_with_progress(&ctx.http, &url, output_path, Some(archive.size as u64), Some(pb)).await?;
            Ok(((), None))
        }

        DownloadState::GameFileSource(gf_state) => {
            copy_game_file(gf_state, archive, output_path, &ctx.config)?;
            pb.set_position(archive.size as u64);
            Ok(((), None))
        }

        // Manual and Mega are handled by check_manual()
        DownloadState::Manual(_) | DownloadState::Mega(_) => {
            unreachable!("Manual downloads should be filtered out before this point")
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

    // Try different potential base locations with case-insensitive lookup
    let potential_bases = [
        (&config.game_dir, game_file_path.as_str()),
        (&config.game_dir, &format!("Data/{}", game_file_path)),
    ];

    let mut source_path: Option<PathBuf> = None;
    for (base, relative) in &potential_bases {
        if let Some(resolved) = crate::paths::resolve_case_insensitive(base, relative) {
            if resolved.exists() {
                source_path = Some(resolved);
                break;
            }
        }
    }

    let source = source_path.with_context(|| {
        format!(
            "Game file not found: {} in {}",
            game_file_path,
            config.game_dir.display()
        )
    })?;

    // Create parent directory
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }

    // Copy the file
    let bytes_copied = fs::copy(&source, output_path)
        .with_context(|| format!("Failed to copy {} to {}", source.display(), output_path.display()))?;

    // Verify size
    if bytes_copied != archive.size as u64 {
        bail!(
            "Size mismatch copying game file: expected {}, got {}",
            archive.size,
            bytes_copied
        );
    }

    Ok(())
}

// ============================================================================
// NXM Browser Mode
// ============================================================================

/// Pending Nexus download info for NXM mode
struct NexusPending {
    archive: ArchiveInfo,
    nexus_state: NexusState,
    output_path: PathBuf,
}

/// Download archives using NXM browser mode (bypasses API rate limits)
async fn download_archives_nxm(
    db: &ModlistDb,
    config: &InstallConfig,
    pending: Vec<ArchiveInfo>,
) -> Result<DownloadStats> {
    println!("\n=== NXM Browser Mode ===");
    println!("This mode opens browser tabs for Nexus downloads.");
    println!("Click 'Slow Download' or 'Download with Manager' for each file.\n");

    // Separate Nexus downloads from others
    let mut nexus_pending: Vec<NexusPending> = Vec::new();
    let mut other_pending: Vec<ArchiveInfo> = Vec::new();

    for archive in pending {
        let output_path = config.downloads_dir.join(&archive.name);

        // Skip if already exists with correct size
        if output_path.exists() {
            if let Ok(meta) = fs::metadata(&output_path) {
                if meta.len() == archive.size as u64 {
                    continue;
                }
            }
        }

        let state: DownloadState = match serde_json::from_str(&archive.state_json) {
            Ok(s) => s,
            Err(_) => continue,
        };

        match state {
            DownloadState::Nexus(nexus_state) => {
                nexus_pending.push(NexusPending {
                    archive,
                    nexus_state,
                    output_path,
                });
            }
            _ => {
                other_pending.push(archive);
            }
        }
    }

    println!("Nexus downloads (NXM mode): {}", nexus_pending.len());
    println!("Other downloads (direct):   {}", other_pending.len());

    // First, handle non-Nexus downloads with direct API
    let mut stats = DownloadStats::default();

    if !other_pending.is_empty() {
        println!("\n--- Downloading non-Nexus files ---");
        // Create a temporary config with nxm_mode disabled for direct downloads
        let mut direct_config = config.clone();
        direct_config.nxm_mode = false;

        // Process non-Nexus files directly
        let direct_stats = download_non_nexus_files(db, &direct_config, other_pending).await?;
        stats.downloaded += direct_stats.downloaded;
        stats.skipped += direct_stats.skipped;
        stats.failed += direct_stats.failed;
        stats.manual += direct_stats.manual;
    }

    // Now handle Nexus downloads via NXM
    if nexus_pending.is_empty() {
        println!("\nNo Nexus downloads needed in NXM mode.");
        return Ok(stats);
    }

    println!("\n--- Starting NXM server ---");

    // Start NXM server
    let (mut rx, _state) = nxm_handler::start_server(config.nxm_port).await?;

    // Create HTTP client for downloads
    let http = HttpClient::new()?;
    let nexus = NexusDownloader::new(&config.nexus_api_key)?;

    // Build lookup map: "game:mod_id:file_id" -> pending info
    let mut lookup: HashMap<String, NexusPending> = HashMap::new();
    for pending in nexus_pending {
        let domain = NexusDownloader::game_domain(&pending.nexus_state.game_name);
        let key = format!("{}:{}:{}", domain, pending.nexus_state.mod_id, pending.nexus_state.file_id);
        lookup.insert(key, pending);
    }

    let total_nexus = lookup.len();
    println!("Waiting for {} NXM links...\n", total_nexus);

    // Progress display
    let multi_progress = MultiProgress::new();
    let overall_pb = multi_progress.add(ProgressBar::new(total_nexus as u64));
    overall_pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} | {msg}")
            .unwrap()
            .progress_chars("=>-"),
    );
    overall_pb.enable_steady_tick(Duration::from_millis(100));
    overall_pb.set_message("Waiting for browser clicks...");

    // Open browser tabs for each pending download
    println!("Opening browser tabs...");
    for pending in lookup.values() {
        let domain = NexusDownloader::game_domain(&pending.nexus_state.game_name);
        let url = nxm_handler::nexus_mod_url(
            domain,
            pending.nexus_state.mod_id,
            pending.nexus_state.file_id,
        );

        // Open in browser
        let _ = std::process::Command::new(&config.browser)
            .arg(&url)
            .spawn();

        // Small delay to avoid overwhelming the browser
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    println!("\nClick 'Download with Manager' on each Nexus page.\n");

    // Process incoming NXM links
    let mut downloaded = 0;
    let mut failed = 0;

    while !lookup.is_empty() {
        // Wait for next NXM link (with timeout)
        let link = match tokio::time::timeout(Duration::from_secs(300), rx.recv()).await {
            Ok(Some(link)) => link,
            Ok(None) => {
                println!("NXM server closed unexpectedly");
                break;
            }
            Err(_) => {
                println!("Timeout waiting for NXM links. {} remaining.", lookup.len());
                break;
            }
        };

        // Find matching pending download
        let key = link.lookup_key();
        let Some(pending) = lookup.remove(&key) else {
            println!("Received NXM link for unknown file: {}", key);
            continue;
        };

        overall_pb.set_message(format!("Downloading: {}", truncate_name(&pending.archive.name, 30)));

        // Create progress bar for this download
        let pb = multi_progress.insert_before(&overall_pb, ProgressBar::new(pending.archive.size as u64));
        pb.set_style(
            ProgressStyle::default_bar()
                .template("  {spinner:.blue} {wide_msg} [{bar:30.white/dim}] {bytes}/{total_bytes} {bytes_per_sec}")
                .unwrap()
                .progress_chars("=>-"),
        );
        pb.enable_steady_tick(Duration::from_millis(100));
        pb.set_message(truncate_name(&pending.archive.name, 40));

        // Get download URL using NXM key (bypasses rate limit)
        let download_result = async {
            // Call Nexus API with the NXM key
            let api_url = link.api_url();
            let response = reqwest::Client::new()
                .get(&api_url)
                .header("apikey", &config.nexus_api_key)
                .send()
                .await
                .context("Failed to get download link")?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                bail!("Nexus API error {}: {}", status, body);
            }

            let links: Vec<serde_json::Value> = response.json().await?;
            let url = links
                .first()
                .and_then(|l| l.get("URI"))
                .and_then(|u| u.as_str())
                .context("No download URL in response")?;

            // Download the file
            download_file_with_progress(&http, url, &pending.output_path, Some(pending.archive.size as u64), Some(&pb)).await?;

            Ok::<_, anyhow::Error>(())
        }.await;

        pb.finish_and_clear();

        match download_result {
            Ok(()) => {
                downloaded += 1;
                db.mark_archive_downloaded(
                    &pending.archive.hash,
                    pending.output_path.to_string_lossy().as_ref(),
                )?;
                overall_pb.inc(1);
            }
            Err(e) => {
                failed += 1;
                overall_pb.println(format!("FAIL {} - {}", pending.archive.name, e));
                overall_pb.inc(1);
            }
        }

        overall_pb.set_message(format!("OK:{} Fail:{} Remaining:{}", downloaded, failed, lookup.len()));
    }

    overall_pb.finish_and_clear();

    // Add remaining as failed
    failed += lookup.len();
    stats.downloaded += downloaded;
    stats.failed += failed;

    // Print Nexus rate limits
    let limits = nexus.rate_limits();
    println!(
        "\nNexus API: {}/{} hourly, {}/{} daily",
        limits.hourly_remaining, limits.hourly_limit,
        limits.daily_remaining, limits.daily_limit
    );

    // Print summary
    println!("\n=== Download Summary ===");
    println!("Downloaded: {}", stats.downloaded);
    println!("Skipped:    {}", stats.skipped);
    println!("Manual:     {}", stats.manual);
    println!("Failed:     {}", stats.failed);

    Ok(stats)
}

/// Download non-Nexus files directly (used in NXM mode for CDN, HTTP, etc.)
async fn download_non_nexus_files(
    db: &ModlistDb,
    config: &InstallConfig,
    pending: Vec<ArchiveInfo>,
) -> Result<DownloadStats> {
    let multi_progress = MultiProgress::new();
    let overall_pb = multi_progress.add(ProgressBar::new(pending.len() as u64));
    overall_pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) | {msg}")
            .unwrap()
            .progress_chars("=>-"),
    );
    overall_pb.enable_steady_tick(Duration::from_millis(100));
    overall_pb.set_message("Starting downloads...");

    let ctx = Arc::new(DownloadContext {
        nexus: NexusDownloader::new(&config.nexus_api_key)?,
        http: HttpClient::new()?,
        cdn: WabbajackCdnDownloader::new()?,
        gdrive: GoogleDriveDownloader::new()?,
        mediafire: MediaFireDownloader::new()?,
        config: config.clone(),
        multi_progress,
        overall_pb,
        downloaded: AtomicUsize::new(0),
        skipped: AtomicUsize::new(0),
        failed: AtomicUsize::new(0),
        manual_downloads: Mutex::new(Vec::new()),
        failed_downloads: Mutex::new(Vec::new()),
    });

    let concurrency = config.max_concurrent_downloads;

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
            }
            DownloadResult::Manual | DownloadResult::Failed => {}
        }
    }

    ctx.overall_pb.finish_and_clear();

    let manual_count = ctx.manual_downloads.lock().await.len();

    Ok(DownloadStats {
        downloaded: ctx.downloaded.load(Ordering::Relaxed),
        skipped: ctx.skipped.load(Ordering::Relaxed),
        failed: ctx.failed.load(Ordering::Relaxed),
        manual: manual_count,
    })
}
