//! Download phase coordinator for collection installations.
//!
//! Handles parallel downloads from Nexus with:
//! - Rate limit tracking and exponential backoff
//! - NXM browser mode fallback
//! - Progress display with multi-progress bars
//! - Auto-retry on failures
//! - Size verification

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use futures::stream::{self, StreamExt};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rayon::prelude::*;
use tokio::sync::Mutex;
use tracing::info;

use crate::downloaders::{download_file_with_progress, HttpClient, NexusDownloader};

use super::db::{CollectionDb, ModDbEntry, ModStatus};
use super::verify::compute_md5;

/// Max retries for network operations
const MAX_RETRIES: u32 = 3;
/// Delay between retries
const RETRY_DELAY: Duration = Duration::from_secs(2);
/// Max retries for rate limit errors (429)
const MAX_RATE_LIMIT_RETRIES: u32 = 10;
/// Initial delay for rate limit retry (increases exponentially)
const RATE_LIMIT_BASE_DELAY: Duration = Duration::from_secs(30);

/// Download statistics
#[derive(Debug, Default, Clone)]
pub struct DownloadStats {
    pub downloaded: usize,
    pub skipped: usize,
    pub failed: usize,
    pub manual: usize,
}

/// Information about a failed download
#[derive(Debug, Clone)]
pub struct FailedDownloadInfo {
    pub name: String,
    pub mod_id: i64,
    pub file_id: i64,
    pub error: String,
    pub expected_size: u64,
}

/// Shared state for download coordination
struct DownloadContext {
    nexus: NexusDownloader,
    http: HttpClient,
    game_domain: String,
    downloads_dir: PathBuf,
    multi_progress: MultiProgress,
    overall_pb: ProgressBar,
    // Counters
    downloaded: AtomicUsize,
    skipped: AtomicUsize,
    failed: AtomicUsize,
    failed_downloads: Mutex<Vec<FailedDownloadInfo>>,
}

/// Download all pending mods for a collection
pub async fn download_mods(
    db: &CollectionDb,
    downloads_dir: &Path,
    nexus_api_key: &str,
    game_domain: &str,
    concurrency: usize,
    nxm_mode: bool,
) -> Result<DownloadStats> {
    // Get mods that need downloading
    let pending_mods = db.get_mods_by_status(ModStatus::Pending)?;

    if pending_mods.is_empty() {
        info!("No mods need downloading");
        return Ok(DownloadStats::default());
    }

    // Check what's already downloaded (verify with MD5 if available) - PARALLEL
    println!("Checking {} existing downloads (parallel)...", pending_mods.len());

    // Setup progress bar
    let check_pb = ProgressBar::new(pending_mods.len() as u64);
    check_pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] Checking [{bar:40.cyan/blue}] {pos}/{len} | {msg}")
            .unwrap()
            .progress_chars("=>-"),
    );
    check_pb.enable_steady_tick(Duration::from_millis(100));

    let already_count = AtomicUsize::new(0);
    let corrupted_count = AtomicUsize::new(0);
    let check_pb = Arc::new(check_pb);

    // Check in parallel, collect results
    #[derive(Debug)]
    enum CheckResult {
        AlreadyDownloaded { id: i64, path: String },
        NeedDownload(ModDbEntry),
        Corrupted(ModDbEntry),
    }

    let results: Vec<CheckResult> = pending_mods
        .into_par_iter()
        .map(|mod_entry| {
            // Try multiple filename patterns - folder_name (unique), then logical_filename (legacy)
            let candidates = get_filename_candidates(&mod_entry);
            let output_path = candidates.iter()
                .map(|f| downloads_dir.join(f))
                .find(|p| p.exists());

            let result = if let Some(output_path) = output_path {
                if let Ok(meta) = fs::metadata(&output_path) {
                    let actual_size = meta.len();

                    // Skip empty files
                    if actual_size == 0 {
                        let _ = fs::remove_file(&output_path);
                        CheckResult::NeedDownload(mod_entry)
                    } else if !mod_entry.md5.is_empty() {
                        // If we have an MD5 hash, verify it
                        match compute_md5(&output_path) {
                            Ok(actual_md5) => {
                                if actual_md5.eq_ignore_ascii_case(&mod_entry.md5) {
                                    // MD5 matches - file is good
                                    already_count.fetch_add(1, Ordering::Relaxed);
                                    CheckResult::AlreadyDownloaded {
                                        id: mod_entry.id,
                                        path: output_path.to_string_lossy().to_string(),
                                    }
                                } else {
                                    // MD5 mismatch - corrupted or wrong version
                                    let _ = fs::remove_file(&output_path);
                                    corrupted_count.fetch_add(1, Ordering::Relaxed);
                                    CheckResult::Corrupted(mod_entry)
                                }
                            }
                            Err(_) => {
                                // Can't read file - re-download
                                let _ = fs::remove_file(&output_path);
                                corrupted_count.fetch_add(1, Ordering::Relaxed);
                                CheckResult::Corrupted(mod_entry)
                            }
                        }
                    } else {
                        // No MD5 to verify - accept if file has reasonable size (>1KB)
                        if actual_size > 1000 {
                            already_count.fetch_add(1, Ordering::Relaxed);
                            CheckResult::AlreadyDownloaded {
                                id: mod_entry.id,
                                path: output_path.to_string_lossy().to_string(),
                            }
                        } else {
                            let _ = fs::remove_file(&output_path);
                            corrupted_count.fetch_add(1, Ordering::Relaxed);
                            CheckResult::Corrupted(mod_entry)
                        }
                    }
                } else {
                    CheckResult::NeedDownload(mod_entry)
                }
            } else {
                CheckResult::NeedDownload(mod_entry)
            };

            check_pb.inc(1);
            check_pb.set_message(format!(
                "OK:{} Corrupt:{}",
                already_count.load(Ordering::Relaxed),
                corrupted_count.load(Ordering::Relaxed)
            ));

            result
        })
        .collect();

    check_pb.finish_and_clear();

    // Process results sequentially (database updates)
    let mut need_download: Vec<ModDbEntry> = Vec::new();
    let mut already_downloaded = 0usize;

    for result in results {
        match result {
            CheckResult::AlreadyDownloaded { id, path } => {
                db.mark_mod_downloaded(id, &path)?;
                already_downloaded += 1;
            }
            CheckResult::NeedDownload(mod_entry) => {
                need_download.push(mod_entry);
            }
            CheckResult::Corrupted(mod_entry) => {
                need_download.push(mod_entry);
            }
        }
    }

    let corrupted = corrupted_count.load(Ordering::Relaxed);
    if corrupted > 0 {
        println!("Found {} corrupted/mismatched archives - will re-download", corrupted);
    }

    if already_downloaded > 0 {
        println!("Found {} archives already downloaded", already_downloaded);
    }

    if need_download.is_empty() {
        info!("All needed archives already downloaded!");
        return Ok(DownloadStats {
            downloaded: 0,
            skipped: already_downloaded,
            failed: 0,
            manual: 0,
        });
    }

    info!("Need to download {} mods", need_download.len());

    // Create Nexus client and validate API key + Premium status
    let nexus = NexusDownloader::new(nexus_api_key)?;

    // Validate and check Premium status before downloading
    println!("Validating Nexus API key...");
    match nexus.validate().await {
        Ok(user_info) => {
            if user_info.is_premium {
                println!(
                    "✓ Logged in as '{}' (Premium) - Direct API downloads enabled (20,000/day limit)",
                    user_info.name
                );
            } else {
                println!(
                    "⚠ Logged in as '{}' (Free) - Limited downloads, may need NXM mode for large collections",
                    user_info.name
                );
            }
        }
        Err(e) => {
            println!("⚠ Could not validate API key: {}", e);
            println!("  Downloads may fail if the API key is invalid.");
        }
    }

    // Show current rate limits
    let limits = nexus.rate_limits();
    println!(
        "Rate limits: {}/{} hourly, {}/{} daily",
        limits.hourly_remaining, limits.hourly_limit,
        limits.daily_remaining, limits.daily_limit
    );

    // Route to NXM mode if enabled
    if nxm_mode {
        let mut stats = download_mods_nxm(db, downloads_dir, game_domain, need_download).await?;
        stats.skipped += already_downloaded;
        return Ok(stats);
    }

    // Setup progress display
    let multi_progress = MultiProgress::new();
    let overall_pb = multi_progress.add(ProgressBar::new(need_download.len() as u64));
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
        nexus,
        http: HttpClient::new()?,
        game_domain: game_domain.to_string(),
        downloads_dir: downloads_dir.to_path_buf(),
        multi_progress,
        overall_pb,
        downloaded: AtomicUsize::new(0),
        skipped: AtomicUsize::new(0),
        failed: AtomicUsize::new(0),
        failed_downloads: Mutex::new(Vec::new()),
    });

    // Process downloads in parallel
    let results: Vec<(i64, PathBuf, DownloadResult)> = stream::iter(need_download)
        .map(|mod_entry| {
            let ctx = Arc::clone(&ctx);
            async move {
                let filename = get_filename(&mod_entry);
                let output_path = ctx.downloads_dir.join(&filename);
                let result = process_mod_download(&ctx, &mod_entry, &output_path).await;
                (mod_entry.id, output_path, result)
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;

    // Update database with results
    for (mod_id, output_path, result) in results {
        match result {
            DownloadResult::Success | DownloadResult::Skipped => {
                db.mark_mod_downloaded(mod_id, output_path.to_string_lossy().as_ref())?;
            }
            DownloadResult::Failed => {
                // Don't mark as downloaded - leave in pending state
            }
        }
    }

    ctx.overall_pb.finish_and_clear();

    // Collect stats
    let stats = DownloadStats {
        downloaded: ctx.downloaded.load(Ordering::Relaxed),
        skipped: ctx.skipped.load(Ordering::Relaxed) + already_downloaded,
        failed: ctx.failed.load(Ordering::Relaxed),
        manual: 0,
    };

    // Print failed download instructions
    let failed_downloads = ctx.failed_downloads.lock().await;
    if !failed_downloads.is_empty() {
        println!("\n=== Failed Downloads ({}) ===", failed_downloads.len());
        println!("These downloads failed. Try manually downloading to: {}\n", downloads_dir.display());

        for (i, fd) in failed_downloads.iter().enumerate() {
            let url = format!(
                "https://www.nexusmods.com/{}/mods/{}?tab=files&file_id={}",
                game_domain, fd.mod_id, fd.file_id
            );
            println!("{}. {}", i + 1, fd.name);
            println!("   URL: {}", url);
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

/// Get the primary filename for a mod download (used when saving new downloads)
fn get_filename(mod_entry: &ModDbEntry) -> String {
    // Use folder_name first - it's unique because it contains mod_id-file_id suffix
    // logical_filename can be generic (e.g., "Main File") and cause collisions
    let base = if !mod_entry.folder_name.is_empty() {
        &mod_entry.folder_name
    } else if !mod_entry.logical_filename.is_empty() {
        &mod_entry.logical_filename
    } else if !mod_entry.name.is_empty() {
        &mod_entry.name
    } else {
        // Last resort: use mod_id and file_id
        return format!("mod_{}_file_{}", mod_entry.mod_id, mod_entry.file_id);
    };

    // Return as-is - don't force any extension
    // The actual file format will be detected by magic bytes during extraction
    base.clone()
}

/// Get candidate filenames to check for existing downloads
/// Returns multiple names to handle legacy downloads that may use different naming
fn get_filename_candidates(mod_entry: &ModDbEntry) -> Vec<String> {
    let mut candidates = Vec::new();

    // Primary: folder_name (unique with mod_id-file_id)
    if !mod_entry.folder_name.is_empty() {
        candidates.push(mod_entry.folder_name.clone());
    }

    // Fallback: logical_filename (what Nexus calls the file - may be generic)
    if !mod_entry.logical_filename.is_empty()
        && (candidates.is_empty() || mod_entry.logical_filename != mod_entry.folder_name)
    {
        candidates.push(mod_entry.logical_filename.clone());
    }

    // Last resort: mod name
    if candidates.is_empty() && !mod_entry.name.is_empty() {
        candidates.push(mod_entry.name.clone());
    }

    // Ultimate fallback
    if candidates.is_empty() {
        candidates.push(format!("mod_{}_file_{}", mod_entry.mod_id, mod_entry.file_id));
    }

    candidates
}

/// Result of processing a single download
#[derive(Debug, Clone, Copy)]
enum DownloadResult {
    Success,
    Skipped,
    Failed,
}

/// Process a single mod download
async fn process_mod_download(
    ctx: &DownloadContext,
    mod_entry: &ModDbEntry,
    output_path: &Path,
) -> DownloadResult {
    // Check if file already exists with correct size
    if output_path.exists() {
        if let Ok(meta) = fs::metadata(output_path) {
            if meta.len() == mod_entry.file_size as u64 {
                ctx.skipped.fetch_add(1, Ordering::Relaxed);
                ctx.overall_pb.inc(1);
                update_overall_message(ctx);
                return DownloadResult::Skipped;
            }
        }
    }

    // Create progress bar for this download
    let pb = ctx.multi_progress.insert_before(&ctx.overall_pb, ProgressBar::new(mod_entry.file_size as u64));
    pb.set_style(
        ProgressStyle::default_bar()
            .template("  {spinner:.blue} {wide_msg} [{bar:30.white/dim}] {bytes}/{total_bytes} {bytes_per_sec}")
            .unwrap()
            .progress_chars("=>-"),
    );
    pb.enable_steady_tick(Duration::from_millis(100));

    // Truncate filename for display
    let display_name = truncate_name(&mod_entry.name, 40);
    pb.set_message(display_name.clone());

    // Choose download method based on source type
    let result = if mod_entry.source_type == "direct" && !mod_entry.source_url.is_empty() {
        // Direct download from URL (e.g., GitHub releases)
        pb.set_message(format!("{} (direct)", display_name));
        download_direct_url(ctx, mod_entry, output_path, &pb).await
    } else {
        // Download from Nexus
        download_nexus_mod(ctx, mod_entry, output_path, &pb).await
    };

    let source_label = if mod_entry.source_type == "direct" { "Direct" } else { "Nexus" };

    match result {
        Ok(()) => {
            pb.finish_and_clear();
            ctx.downloaded.fetch_add(1, Ordering::Relaxed);
            ctx.overall_pb.inc(1);
            update_overall_message(ctx);
            DownloadResult::Success
        }
        Err(e) => {
            pb.finish_and_clear();
            ctx.failed.fetch_add(1, Ordering::Relaxed);
            ctx.overall_pb.inc(1);
            let error_msg = root_cause(&e);
            ctx.overall_pb.println(format!(
                "FAIL [{}] {} - {}",
                source_label, truncate_name(&mod_entry.name, 30), error_msg
            ));
            // Record failed download
            ctx.failed_downloads.lock().await.push(FailedDownloadInfo {
                name: mod_entry.name.clone(),
                mod_id: mod_entry.mod_id,
                file_id: mod_entry.file_id,
                error: error_msg,
                expected_size: mod_entry.file_size as u64,
            });
            DownloadResult::Failed
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

/// Truncate a name for display
fn truncate_name(name: &str, max_len: usize) -> String {
    if name.len() <= max_len {
        name.to_string()
    } else {
        format!("{}...", &name[..max_len - 3])
    }
}

/// Extract the root cause error message
fn root_cause(e: &anyhow::Error) -> String {
    let root = e.root_cause();
    let msg = root.to_string();
    if msg.len() > 80 {
        format!("{}...", &msg[..77])
    } else {
        msg
    }
}

/// Download a mod from Nexus (with retry)
async fn download_nexus_mod(
    ctx: &DownloadContext,
    mod_entry: &ModDbEntry,
    output_path: &Path,
    pb: &ProgressBar,
) -> Result<()> {
    let mut attempt = 0u32;
    let mut rate_limit_retries = 0u32;
    let display_name = truncate_name(&mod_entry.name, 40);
    let expected_size = mod_entry.file_size as u64;

    loop {
        attempt += 1;

        // Reset progress bar for retry
        if attempt > 1 {
            pb.set_position(0);
            pb.set_message(display_name.clone());
            // Remove partial file if exists
            let _ = std::fs::remove_file(output_path);
        }

        let result = download_nexus_mod_inner(ctx, mod_entry, output_path, pb).await;

        match result {
            Ok(()) => {
                // Verify file was created
                match std::fs::metadata(output_path) {
                    Ok(meta) => {
                        let actual_size = meta.len();

                        // Check for obvious failures (empty file or error page)
                        if actual_size == 0 {
                            let _ = std::fs::remove_file(output_path);
                            if attempt < MAX_RETRIES {
                                tokio::time::sleep(RETRY_DELAY).await;
                                continue;
                            } else {
                                bail!("Downloaded empty file after {} attempts", MAX_RETRIES);
                            }
                        }

                        // Check for suspiciously small files (likely error pages)
                        if actual_size < 1000 && expected_size > 10000 {
                            let _ = std::fs::remove_file(output_path);
                            if attempt < MAX_RETRIES {
                                tokio::time::sleep(RETRY_DELAY).await;
                                continue;
                            } else {
                                bail!("Downloaded file too small ({} bytes)", actual_size);
                            }
                        }

                        // Verify MD5 hash if we have one
                        let expected_md5 = &mod_entry.md5;
                        if !expected_md5.is_empty() {
                            match compute_md5(output_path) {
                                Ok(actual_md5) => {
                                    if !actual_md5.eq_ignore_ascii_case(expected_md5) {
                                        // MD5 mismatch - delete and retry
                                        let _ = std::fs::remove_file(output_path);
                                        if attempt < MAX_RETRIES {
                                            ctx.overall_pb.println(format!(
                                                "MD5 mismatch for {}, retrying...",
                                                truncate_name(&mod_entry.name, 30)
                                            ));
                                            tokio::time::sleep(RETRY_DELAY).await;
                                            continue;
                                        } else {
                                            bail!(
                                                "MD5 mismatch after {} attempts: expected {}, got {}",
                                                MAX_RETRIES, expected_md5, actual_md5
                                            );
                                        }
                                    }
                                }
                                Err(e) => {
                                    // Can't compute MD5 - treat as failure
                                    let _ = std::fs::remove_file(output_path);
                                    if attempt < MAX_RETRIES {
                                        tokio::time::sleep(RETRY_DELAY).await;
                                        continue;
                                    } else {
                                        bail!("Cannot verify MD5: {}", e);
                                    }
                                }
                            }
                        }
                    }
                    Err(_e) => {
                        // File doesn't exist after download - retry
                        if attempt < MAX_RETRIES {
                            tokio::time::sleep(RETRY_DELAY).await;
                            continue;
                        } else {
                            bail!("Download failed after {} attempts", MAX_RETRIES);
                        }
                    }
                }
                return Ok(());
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
                            truncate_name(&mod_entry.name, 25), delay_secs, rate_limit_retries, MAX_RATE_LIMIT_RETRIES
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
async fn download_nexus_mod_inner(
    ctx: &DownloadContext,
    mod_entry: &ModDbEntry,
    output_path: &Path,
    pb: &ProgressBar,
) -> Result<()> {
    // Get download link from Nexus (cast i64 to u64 for API)
    let url: String = ctx.nexus
        .get_download_link(
            &ctx.game_domain,
            mod_entry.mod_id as u64,
            mod_entry.file_id as u64,
        )
        .await
        .with_context(|| format!(
            "Nexus: {}/mods/{}/files/{}",
            ctx.game_domain, mod_entry.mod_id, mod_entry.file_id
        ))?;

    // Download the file - don't pass expected size to avoid internal mismatch errors
    // We'll verify size ourselves in the retry loop
    download_file_with_progress(&ctx.http, &url, output_path, None, Some(pb)).await?;

    Ok(())
}

// ============================================================================
// Direct URL Downloads (GitHub, etc.)
// ============================================================================

/// Download a file directly from a URL (for non-Nexus sources like GitHub)
async fn download_direct_url(
    ctx: &DownloadContext,
    mod_entry: &ModDbEntry,
    output_path: &Path,
    pb: &ProgressBar,
) -> Result<()> {
    let url = &mod_entry.source_url;
    let expected_size = mod_entry.file_size as u64;
    let display_name = truncate_name(&mod_entry.name, 40);

    let mut attempt = 0u32;

    loop {
        attempt += 1;

        // Reset progress bar for retry
        if attempt > 1 {
            pb.set_position(0);
            pb.set_message(format!("{} (retry {})", display_name, attempt));
            let _ = std::fs::remove_file(output_path);
        }

        // Download directly from URL
        let result = download_file_with_progress(&ctx.http, url, output_path, Some(expected_size), Some(pb)).await;

        match result {
            Ok(_bytes_downloaded) => {
                // Verify file was created with correct size
                if let Ok(meta) = std::fs::metadata(output_path) {
                    if meta.len() == expected_size {
                        return Ok(());
                    } else if attempt < MAX_RETRIES {
                        // Size mismatch, retry
                        let _ = std::fs::remove_file(output_path);
                        tokio::time::sleep(RETRY_DELAY).await;
                        continue;
                    } else {
                        bail!("Size mismatch: expected {} bytes, got {} bytes", expected_size, meta.len());
                    }
                } else if attempt < MAX_RETRIES {
                    tokio::time::sleep(RETRY_DELAY).await;
                    continue;
                } else {
                    bail!("Download failed - file not created after {} attempts", MAX_RETRIES);
                }
            }
            Err(e) => {
                if attempt < MAX_RETRIES {
                    tokio::time::sleep(RETRY_DELAY).await;
                    continue;
                }
                return Err(e);
            }
        }
    }
}

// ============================================================================
// NXM Browser Mode (TODO: Implement when needed)
// ============================================================================

/// Download mods using NXM browser mode (for non-Premium users)
///
/// This mode opens browser tabs for each mod and waits for the user to click
/// "Download with Manager" which sends an nxm:// link back to our handler.
/// The NXM link contains key/expires tokens that bypass API rate limits.
///
/// NOTE: Premium users should NOT need this mode - they can use direct API
/// downloads with 20,000 daily limit. If you're Premium and hitting issues,
/// please report the specific error message.
async fn download_mods_nxm(
    _db: &CollectionDb,
    _downloads_dir: &Path,
    _game_domain: &str,
    pending: Vec<ModDbEntry>,
) -> Result<DownloadStats> {
    // NXM browser mode for non-Premium users
    println!("\n=== NXM Browser Mode ===");
    println!("This mode is for NON-Premium Nexus users.");
    println!("Premium users should use direct API mode (remove --nxm flag).\n");
    println!("To use NXM mode, we would need to:");
    println!("  1. Open {} browser tabs for Nexus mod pages", pending.len());
    println!("  2. You click 'Download with Manager' on each page");
    println!("  3. The NXM links are captured and downloads start automatically\n");

    // TODO: Implement NXM browser mode similar to Wabbajack's installer/downloader.rs
    // Key steps:
    // 1. Start NXM server with nxm_handler::start_server(port)
    // 2. Open browser tabs with nxm_handler::nexus_mod_url()
    // 3. Wait for NXM links via the receiver channel
    // 4. For each link, call link.api_url() to get Nexus API endpoint
    // 5. Fetch actual download URL from that endpoint
    // 6. Download the file
    //
    // For now, NXM mode is not fully implemented for collections.
    anyhow::bail!(
        "NXM browser mode not yet fully implemented for collections.\n\
        If you're a Premium user, remove the --nxm flag and use direct API mode.\n\
        If you're hitting rate limits as a Premium user, please report this issue with:\n\
        - The specific error message you see\n\
        - Your rate limit values (shown at download start)"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_filename() {
        let mod_entry = ModDbEntry {
            id: 1,
            name: "Test Mod".to_string(),
            folder_name: "TestMod".to_string(),
            logical_filename: "test_mod-1.0.zip".to_string(),
            md5: "abc123".to_string(),
            file_size: 1000,
            mod_id: 123,
            file_id: 456,
            source_type: "nexus".to_string(),
            source_url: "".to_string(),
            deploy_type: "".to_string(),
            phase: 0,
            status: "pending".to_string(),
            local_path: None,
            choices_json: None,
            error_message: None,
            fomod_validated: false,
            fomod_valid: false,
            fomod_error: None,
            fomod_module_name: None,
            hashes_json: None,
        };

        // folder_name is used first (contains mod_id-file_id for uniqueness)
        assert_eq!(get_filename(&mod_entry), "TestMod");

        let mod_entry_no_folder = ModDbEntry {
            folder_name: "".to_string(),
            ..mod_entry
        };
        // When folder_name is empty, falls back to logical_filename
        assert_eq!(get_filename(&mod_entry_no_folder), "test_mod-1.0.zip");
    }

    #[test]
    fn test_truncate_name() {
        assert_eq!(truncate_name("short", 10), "short");
        assert_eq!(truncate_name("this is a very long name", 10), "this is...");
    }
}
