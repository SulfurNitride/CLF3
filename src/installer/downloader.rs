//! Download phase coordinator
//!
//! Coordinates downloading all archives from various sources with parallel downloads.
//! Supports both direct API mode (premium) and NXM browser mode (free/rate-limit bypass).

use crate::downloaders::{
    download_file_with_callback, GoogleDriveDownloader, HttpClient, MediaFireDownloader,
    NexusDownloader, ProgressCallback as HttpProgressCallback, WabbajackCdnDownloader,
};
use crate::hash::verify_file_hash;
use crate::modlist::{ArchiveInfo, DownloadState, ModlistDb, NexusState};
use crate::nxm_handler;

use super::config::{InstallConfig, ProgressEvent};

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
use tracing::{debug, info, warn};

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
    // Progress tracking for callbacks
    completed_archives: AtomicUsize,
    total_archives: usize,
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

    println!(
        "Found {} missing outputs requiring {} archives",
        missing_count,
        needed_archives.len()
    );

    // Get archive info for needed archives
    let needed_hashes: Vec<String> = needed_archives.into_iter().collect();
    let archives_to_check = db.get_archives_by_hashes(&needed_hashes)?;

    // Now check which of these archives are actually downloaded (with correct size)
    let mut already_downloaded = 0usize;
    let mut already_downloaded_size: u64 = 0;
    let mut need_download: Vec<ArchiveInfo> = Vec::new();
    let mut truncated_count = 0usize;

    // First pass: check which archives exist with correct size
    let mut archives_to_verify: Vec<(ArchiveInfo, PathBuf)> = Vec::new();

    for archive in archives_to_check {
        let output_path = config.downloads_dir.join(&archive.name);
        if output_path.exists() {
            if let Ok(meta) = fs::metadata(&output_path) {
                if meta.len() == archive.size as u64 {
                    // Size matches - queue for hash verification
                    archives_to_verify.push((archive, output_path));
                    continue;
                } else {
                    // File exists but wrong size - truncated/corrupted, delete and re-download
                    println!(
                        "  Truncated: {} ({}% complete)",
                        archive.name,
                        (meta.len() * 100) / (archive.size as u64).max(1)
                    );
                    if let Err(e) = fs::remove_file(&output_path) {
                        warn!(
                            "Failed to remove truncated file {}: {}",
                            output_path.display(),
                            e
                        );
                    }
                    truncated_count += 1;
                }
            }
        }
        need_download.push(archive);
    }

    // Second pass: verify hashes of existing archives
    let mut corrupted_count = 0usize;
    if !archives_to_verify.is_empty() {
        println!(
            "Verifying {} existing archives...",
            archives_to_verify.len()
        );

        for (i, (archive, output_path)) in archives_to_verify.iter().enumerate() {
            // Show progress
            print!(
                "\r  Verifying {}/{}: {}...",
                i + 1,
                archives_to_verify.len(),
                truncate_name(&archive.name, 40)
            );
            let _ = std::io::Write::flush(&mut std::io::stdout());

            match verify_file_hash(&output_path, &archive.hash) {
                Ok(true) => {
                    // Hash matches - archive is valid
                    db.mark_archive_downloaded(
                        &archive.hash,
                        output_path.to_string_lossy().as_ref(),
                    )?;
                    already_downloaded += 1;
                    already_downloaded_size += archive.size as u64;
                }
                Ok(false) => {
                    // Hash mismatch - corrupted, delete and re-download
                    println!(
                        "\r  Corrupted (hash mismatch): {}                    ",
                        archive.name
                    );
                    if let Err(e) = fs::remove_file(&output_path) {
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
                    println!(
                        "\r  Verify error for {}: {}                    ",
                        archive.name, e
                    );
                    if let Err(e) = fs::remove_file(&output_path) {
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
        println!(
            "\r  Verified {} archives ({} valid, {} corrupted)                    ",
            archives_to_verify.len(),
            already_downloaded,
            corrupted_count
        );
    }

    if truncated_count > 0 || corrupted_count > 0 {
        println!(
            "Found {} truncated, {} corrupted archives - will re-download",
            truncated_count, corrupted_count
        );
    }

    if already_downloaded > 0 {
        println!(
            "Found {} archives already downloaded ({} bytes)",
            already_downloaded, already_downloaded_size
        );
        // Report skipped archives to progress callback
        if let Some(ref callback) = config.progress_callback {
            callback(ProgressEvent::DownloadSkipped {
                count: already_downloaded,
                total_size: already_downloaded_size,
            });
        }
    }

    if need_download.is_empty() {
        println!("All needed archives already downloaded!");
        return Ok(DownloadStats {
            downloaded: 0,
            skipped: already_downloaded,
            failed: 0,
            manual: 0,
            failed_downloads: Vec::new(),
            manual_downloads: Vec::new(),
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
    let total_archives = pending.len();
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
        completed_archives: AtomicUsize::new(0),
        total_archives,
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
        println!(
            "\n=== Manual Downloads Required ({}) ===",
            manual_downloads_list.len()
        );
        println!(
            "Please download the following files to: {}\n",
            config.downloads_dir.display()
        );

        // Log to file for later reference
        warn!(
            "=== Manual Downloads Required ({}) ===",
            manual_downloads_list.len()
        );
        warn!("Download destination: {}", config.downloads_dir.display());

        for (i, md) in manual_downloads_list.iter().enumerate() {
            println!("{}. {}", i + 1, md.name);
            println!("   URL: {}", md.url);
            println!("   Expected size: {} bytes", md.expected_size);
            if let Some(prompt) = &md.prompt {
                println!("   Note: {}", prompt);
            }
            println!();

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

        println!("After downloading, run the command again to continue.\n");
    }

    // Print failed download instructions
    if !failed_downloads_list.is_empty() {
        println!(
            "\n=== Failed Downloads ({}) ===",
            failed_downloads_list.len()
        );
        println!(
            "These downloads failed. Try manually downloading to: {}\n",
            config.downloads_dir.display()
        );

        // Log to file for later reference
        warn!("=== Failed Downloads ({}) ===", failed_downloads_list.len());
        warn!("Download destination: {}", config.downloads_dir.display());

        for (i, fd) in failed_downloads_list.iter().enumerate() {
            println!("{}. {}", i + 1, fd.name);
            println!("   URL: {}", fd.url);
            println!("   Error: {}", fd.error);
            println!(
                "   Expected size: {} bytes ({:.2} MB)",
                fd.expected_size,
                fd.expected_size as f64 / 1024.0 / 1024.0
            );
            println!();

            // Log each failed download to file
            warn!(
                "[FAILED] {}: {} (error: {}, size: {} bytes)",
                fd.name, fd.url, fd.error, fd.expected_size
            );
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
        limits.hourly_remaining, limits.hourly_limit, limits.daily_remaining, limits.daily_limit
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
            ctx.overall_pb.inc(1);
            ctx.overall_pb
                .println(format!("FAIL {} - parse error: {}", archive.name, e));
            return (DownloadResult::Failed, None);
        }
    };

    // Check for manual downloads first
    if let Some(manual_info) = check_manual(&state, archive) {
        ctx.manual_downloads.lock().await.push(manual_info);
        ctx.overall_pb.inc(1);
        update_overall_message(ctx);
        report_archive_complete(ctx, &archive.name);
        return (DownloadResult::Manual, None);
    }

    // Create progress bar for this download
    let pb = ctx
        .multi_progress
        .insert_before(&ctx.overall_pb, ProgressBar::new(archive.size as u64));
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
            report_archive_complete(ctx, &archive.name);
            (DownloadResult::Success, url_to_cache)
        }
        Err(e) => {
            pb.finish_and_clear();
            ctx.failed.fetch_add(1, Ordering::Relaxed);
            ctx.overall_pb.inc(1);
            let error_msg = root_cause(&e);
            ctx.overall_pb.println(format!(
                "FAIL [{}] {} - {}",
                source,
                truncate_name(&archive.name, 30),
                error_msg
            ));
            // Record failed download with URL for manual download
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
    ctx.overall_pb.set_message(format!(
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
fn check_manual(state: &DownloadState, archive: &ArchiveInfo) -> Option<ManualDownloadInfo> {
    match state {
        DownloadState::Manual(manual_state) => Some(ManualDownloadInfo {
            name: archive.name.clone(),
            url: manual_state.url.clone(),
            prompt: Some(manual_state.prompt.clone()),
            expected_size: archive.size as u64,
        }),
        // Mega is now handled via Wabbajack proxy, not manual
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
    pb: &ProgressBar,
    callback: Option<&HttpProgressCallback>,
) -> Result<()> {
    let proxy_url = build_proxy_url(source_url);
    debug!("Downloading via proxy: {}", proxy_url);
    download_file_with_callback(
        client,
        &proxy_url,
        output_path,
        Some(expected_size),
        Some(pb),
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
                        }
                    }
                    Err(e) => {
                        if attempt < MAX_RETRIES {
                            ctx.overall_pb.println(format!(
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
                pb.set_message(format!(
                    "{} (verifying...)",
                    truncate_name(&archive.name, 30)
                ));
                match verify_file_hash(output_path, &archive.hash) {
                    Ok(true) => {
                        // Hash matches - success!
                    }
                    Ok(false) => {
                        // Hash mismatch - corrupted download, delete and retry
                        let _ = std::fs::remove_file(output_path);
                        if attempt < MAX_RETRIES {
                            ctx.overall_pb.println(format!(
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
                            ctx.overall_pb.println(format!(
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
                        pb.set_message(format!("Rate limited, waiting {}s...", delay_secs));
                        ctx.overall_pb.println(format!(
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
                    );
                    let callback_ref = progress_callback.as_ref();

                    // Try Wabbajack proxy
                    pb.set_position(0);
                    pb.set_message(format!(
                        "{} (proxy)",
                        truncate_name(&archive.name, 30)
                    ));
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
                        pb,
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
                            debug!(
                                "Proxy failed for {}: {}",
                                archive.name, proxy_err
                            );
                        }
                    }

                    // Try Wabbajack mirror (CDN by hash)
                    pb.set_position(0);
                    pb.set_message(format!(
                        "{} (mirror)",
                        truncate_name(&archive.name, 30)
                    ));
                    let _ = std::fs::remove_file(output_path);

                    info!(
                        "Proxy failed for {}, trying Wabbajack mirror",
                        archive.name
                    );
                    match download_from_mirror(
                        &ctx.cdn,
                        &archive.hash,
                        output_path,
                        expected_size,
                    )
                    .await
                    {
                        Ok(()) => {
                            info!("Mirror download succeeded for {}", archive.name);
                            return Ok(None);
                        }
                        Err(mirror_err) => {
                            debug!(
                                "Mirror failed for {}: {}",
                                archive.name, mirror_err
                            );
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
) -> Option<HttpProgressCallback> {
    callback.as_ref().map(|cb| {
        let cb = cb.clone();
        let name = archive_name;
        Box::new(move |downloaded: u64, total: u64, speed: f64| {
            cb(ProgressEvent::DownloadProgress {
                name: name.clone(),
                downloaded,
                total,
                speed,
            });
        }) as HttpProgressCallback
    })
}

/// Inner download function (single attempt)
async fn download_archive_inner(
    state: &DownloadState,
    archive: &ArchiveInfo,
    output_path: &Path,
    ctx: &DownloadContext,
    pb: &ProgressBar,
) -> Result<((), Option<(String, i64)>)> {
    // Create progress callback for GUI updates
    let progress_callback =
        make_progress_callback(archive.name.clone(), &ctx.config.progress_callback);
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
                Some(pb),
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
                Some(pb),
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
            pb.set_position(archive.size as u64);
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
            // Use gdrive's own client to maintain cookies through the confirmation flow
            ctx.gdrive
                .download_to_file(&gd_state.id, output_path, archive.size as u64, Some(pb))
                .await?;
            // Report final progress for GUI
            if let Some(ref cb) = ctx.config.progress_callback {
                cb(ProgressEvent::DownloadProgress {
                    name: archive.name.clone(),
                    downloaded: archive.size as u64,
                    total: archive.size as u64,
                    speed: 0.0, // GDrive doesn't provide speed info through this path
                });
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
                Some(pb),
                callback_ref,
            )
            .await?;
            Ok(((), None))
        }

        DownloadState::GameFileSource(gf_state) => {
            copy_game_file(gf_state, archive, output_path, &ctx.config)?;
            pb.set_position(archive.size as u64);
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
            // Mega has no native client - download via Wabbajack proxy directly
            info!("Mega download for {} - using Wabbajack proxy", archive.name);
            download_via_proxy(
                &ctx.http,
                &mega_state.url,
                output_path,
                archive.size as u64,
                pb,
                callback_ref,
            )
            .await
            .with_context(|| {
                format!(
                    "Mega proxy download failed for {} ({})",
                    archive.name, mega_state.url
                )
            })?;
            Ok(((), None))
        }

        // Manual downloads are handled by check_manual()
        DownloadState::Manual(_) => {
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
    let bytes_copied = fs::copy(&source, output_path).with_context(|| {
        format!(
            "Failed to copy {} to {}",
            source.display(),
            output_path.display()
        )
    })?;

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

    // Auto-register as nxm:// handler so browser clicks work
    let exe = std::env::current_exe()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();
    if let Err(e) = nxm_handler::register_handler(&exe) {
        warn!(
            "Failed to register NXM handler (browser clicks may not work): {}",
            e
        );
    }

    // Start NXM server
    let (mut rx, _sock_path) = nxm_handler::start_listener().await?;

    // Create HTTP client for downloads
    let http = Arc::new(HttpClient::new()?);
    let nexus = NexusDownloader::new(&config.nexus_api_key)?;

    // Build lookup map: "game:mod_id:file_id" -> pending info
    let mut lookup: HashMap<String, NexusPending> = HashMap::new();
    for pending in nexus_pending {
        let domain = NexusDownloader::game_domain(&pending.nexus_state.game_name);
        let key = format!(
            "{}:{}:{}",
            domain, pending.nexus_state.mod_id, pending.nexus_state.file_id
        );
        lookup.insert(key, pending);
    }

    let total_nexus = lookup.len();
    println!("Waiting for {} NXM links...\n", total_nexus);

    // Progress display
    let multi_progress = MultiProgress::new();
    let overall_pb = multi_progress.add(ProgressBar::new(total_nexus as u64));
    overall_pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} | {msg}",
            )
            .unwrap()
            .progress_chars("=>-"),
    );
    overall_pb.enable_steady_tick(Duration::from_millis(100));
    overall_pb.set_message("Waiting for browser clicks...");

    // Pipelined NXM download: open ONE tab at a time, wait for user to click
    // "Download with Manager", start the download in background, then open the
    // next tab. Concurrent downloads capped at CPU thread count.
    let max_concurrent_downloads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);

    // Queue of keys still needing a tab opened
    let mut tab_queue: Vec<String> = lookup.keys().cloned().collect();
    let active_downloads = Arc::new(AtomicUsize::new(0));
    let mut tab_open = false; // true when we have exactly one tab waiting for NXM click
    let mut downloaded = 0usize;
    let mut failed = 0usize;
    let mut nxm_completed = 0usize; // tracks completed for progress callbacks

    // Channel for background download tasks to report results: (hash, name, path, result)
    let (result_tx, mut result_rx) =
        tokio::sync::mpsc::unbounded_channel::<(String, String, PathBuf, Result<()>)>();

    let open_tab = |pending: &NexusPending, browser: &str| {
        let domain = NexusDownloader::game_domain(&pending.nexus_state.game_name);
        let url = nxm_handler::nexus_mod_url(
            domain,
            pending.nexus_state.mod_id,
            pending.nexus_state.file_id,
        );
        let _ = std::process::Command::new(browser).arg(&url).spawn();
    };

    // Open the first tab
    if let Some(key) = tab_queue.pop() {
        if let Some(pending) = lookup.get(&key) {
            open_tab(pending, &config.browser);
            tab_open = true;
        }
    }

    println!("Click 'Download with Manager' on each Nexus page.\n");

    // Main event loop: process NXM links and download completions
    loop {
        if lookup.is_empty() && active_downloads.load(Ordering::Relaxed) == 0 {
            break;
        }

        tokio::select! {
            // Handle incoming NXM links
            nxm_result = rx.recv() => {
                let link = match nxm_result {
                    Some(link) => link,
                    None => {
                        println!("NXM server closed unexpectedly");
                        break;
                    }
                };

                let key = link.lookup_key();
                let Some(pending) = lookup.remove(&key) else {
                    println!("Received NXM link for unknown file: {}", key);
                    continue;
                };

                tab_open = false;

                // Open next tab immediately (user clicks while download runs)
                if active_downloads.load(Ordering::Relaxed) < max_concurrent_downloads {
                    if let Some(next_key) = tab_queue.pop() {
                        if let Some(next_pending) = lookup.get(&next_key) {
                            open_tab(next_pending, &config.browser);
                            tab_open = true;
                        }
                    }
                }

                // Spawn download as background task
                let http = Arc::clone(&http);
                let api_key = config.nexus_api_key.clone();
                let output_path = pending.output_path.clone();
                let archive_size = pending.archive.size as u64;
                let archive_name = pending.archive.name.clone();
                let archive_hash = pending.archive.hash.clone();
                let progress_callback = make_progress_callback(
                    archive_name.clone(),
                    &config.progress_callback,
                );
                let pb = multi_progress.insert_before(&overall_pb, ProgressBar::new(archive_size));
                pb.set_style(
                    ProgressStyle::default_bar()
                        .template("  {spinner:.blue} {wide_msg} [{bar:30.white/dim}] {bytes}/{total_bytes} {bytes_per_sec}")
                        .unwrap()
                        .progress_chars("=>-"),
                );
                pb.enable_steady_tick(Duration::from_millis(100));
                pb.set_message(truncate_name(&archive_name, 40));

                let active = Arc::clone(&active_downloads);
                active.fetch_add(1, Ordering::Relaxed);
                let tx = result_tx.clone();

                tokio::spawn(async move {
                    let result = async {
                        let api_url = link.api_url();
                        let response = reqwest::Client::new()
                            .get(&api_url)
                            .header("apikey", &api_key)
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

                        download_file_with_callback(&http, url, &output_path, Some(archive_size), Some(&pb), progress_callback.as_ref()).await?;
                        Ok::<_, anyhow::Error>(())
                    }.await;

                    pb.finish_and_clear();
                    active.fetch_sub(1, Ordering::Relaxed);
                    let _ = tx.send((archive_hash, archive_name, output_path, result));
                });

                overall_pb.set_message(format!(
                    "OK:{} Fail:{} Active:{} Remaining:{}",
                    downloaded, failed,
                    active_downloads.load(Ordering::Relaxed),
                    lookup.len()
                ));
            }

            // Handle completed downloads
            Some((hash, name, output_path, result)) = result_rx.recv() => {
                match result {
                    Ok(()) => {
                        overall_pb.set_message(format!("Verifying {}...", truncate_name(&output_path.file_name().unwrap_or_default().to_string_lossy(), 30)));
                        match verify_file_hash(&output_path, &hash) {
                            Ok(true) => {
                                downloaded += 1;
                                db.mark_archive_downloaded(
                                    &hash,
                                    output_path.to_string_lossy().as_ref(),
                                )?;
                            }
                            Ok(false) => {
                                overall_pb.println(format!("FAIL {} - hash mismatch", output_path.display()));
                                let _ = fs::remove_file(&output_path);
                                failed += 1;
                            }
                            Err(e) => {
                                overall_pb.println(format!("FAIL {} - verify error: {}", output_path.display(), e));
                                let _ = fs::remove_file(&output_path);
                                failed += 1;
                            }
                        }
                        overall_pb.inc(1);
                    }
                    Err(e) => {
                        failed += 1;
                        overall_pb.println(format!("FAIL {} - {}", output_path.display(), e));
                        overall_pb.inc(1);
                    }
                }

                // Report progress to GUI callback
                nxm_completed += 1;
                if let Some(ref callback) = config.progress_callback {
                    callback(ProgressEvent::DownloadComplete { name });
                    callback(ProgressEvent::ArchiveComplete {
                        index: nxm_completed,
                        total: total_nexus,
                    });
                }

                // A download slot freed up - open next tab if we don't have one waiting
                if !tab_open && active_downloads.load(Ordering::Relaxed) < max_concurrent_downloads {
                    if let Some(next_key) = tab_queue.pop() {
                        if let Some(next_pending) = lookup.get(&next_key) {
                            open_tab(next_pending, &config.browser);
                            tab_open = true;
                        }
                    }
                }

                overall_pb.set_message(format!(
                    "OK:{} Fail:{} Active:{} Remaining:{}",
                    downloaded, failed,
                    active_downloads.load(Ordering::Relaxed),
                    lookup.len()
                ));
            }

            // Timeout if nothing happens for 5 minutes
            _ = tokio::time::sleep(Duration::from_secs(300)) => {
                if tab_open || !lookup.is_empty() {
                    println!("Timeout waiting for NXM links. {} remaining.", lookup.len());
                    break;
                }
            }
        }
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
        limits.hourly_remaining, limits.hourly_limit, limits.daily_remaining, limits.daily_limit
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

    let total_archives = pending.len();
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
        completed_archives: AtomicUsize::new(0),
        total_archives,
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
