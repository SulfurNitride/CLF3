//! Producer-consumer pipeline for FromArchive extraction.
//!
//! # Architecture
//!
//! Uses a bounded channel to decouple extraction from file moving:
//!
//! ```text
//! Extractor Workers (N/2 threads)     Mover Workers (N/2 threads)
//! ┌─────────────────────────────┐     ┌─────────────────────────────┐
//! │ - Run 7z extraction         │     │ - Receive file paths        │
//! │ - Extract to temp dir       │────▶│ - Verify size via metadata  │
//! │ - Send paths via channel    │     │ - fs::rename to final dest  │
//! └─────────────────────────────┘     └─────────────────────────────┘
//!
//! Archives are tiered by size for optimal resource usage:
//! - Small (≤512MB):  Full parallel - 1 thread each, many archives at once
//! - Medium (512MB-2GB): Half parallel - 2 threads each
//! - Large (≥2GB):    Sequential - all threads for one archive
//!
//! BSA/BA2: Read directly using bsa module (no extraction needed)
//! ```
//!
//! Where N = system thread count (e.g., 8 or 16).
//!
//! Uses full archive extraction followed by fs::rename for instant file moves
//! (no copying on same filesystem). Temp dir auto-deletes unneeded files.

use crate::archive::sevenzip;
use crate::bsa;
use crate::installer::handlers::from_archive::{detect_archive_type, ArchiveType as NestedArchiveType};
use crate::modlist::{FromArchiveDirective, ModlistDb};
use crate::paths;

use super::processor::ProcessContext;

use anyhow::Result;
use crossbeam_channel::{bounded, Receiver, Sender};
use indicatif::ProgressBar;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use tracing::{error, warn, info};

/// Message sent from extractor to mover workers.
struct MoveJob {
    /// Path to the extracted file in temp directory
    source_path: PathBuf,
    /// Final output path
    output_path: PathBuf,
    /// Expected file size for verification
    expected_size: u64,
    /// Directive ID for error reporting
    directive_id: i64,
    /// Keep temp directory alive until this job is processed
    _temp_dir: Arc<tempfile::TempDir>,
    /// If true, this source has multiple destinations - must reflink/copy instead of rename
    is_shared_source: bool,
}

/// Archive size tier for determining parallelism strategy.
///
/// Total threads stay constant across tiers - only the distribution changes:
/// - Small: many archives, few threads each
/// - Medium: fewer archives, more threads each
/// - Large: one archive, all threads
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArchiveSizeTier {
    /// ≤512MB: Full parallel (all archives × 1 thread each)
    Small,
    /// 512MB-2GB: Half parallel (half archives × 2 threads each)
    Medium,
    /// ≥2GB: Sequential (1 archive × all threads)
    Large,
}

impl ArchiveSizeTier {
    fn from_size(size: u64) -> Self {
        const HALF_GB: u64 = 512 * 1024 * 1024;
        const TWO_GB: u64 = 2 * 1024 * 1024 * 1024;

        if size <= HALF_GB {
            ArchiveSizeTier::Small
        } else if size < TWO_GB {
            ArchiveSizeTier::Medium
        } else {
            ArchiveSizeTier::Large
        }
    }

    /// Get the number of 7z threads to use for this tier.
    /// Total threads stay constant, distributed across fewer archives for larger files.
    fn threads_per_archive(&self) -> usize {
        match self {
            ArchiveSizeTier::Small => 1,   // Many archives, 1 thread each
            ArchiveSizeTier::Medium => 2,  // Fewer archives, 2 threads each
            ArchiveSizeTier::Large => 0,   // 0 = all threads (sequential processing)
        }
    }
}

/// Configuration for extraction (simplified).
#[derive(Debug, Clone)]
pub struct StreamingConfig {
    /// Number of parallel file operations (default: all CPUs)
    pub parallelism: usize,
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self {
            parallelism: rayon::current_num_threads(),
        }
    }
}

/// Statistics from the extraction pipeline.
#[derive(Debug, Default)]
pub struct StreamingStats {
    pub extracted: usize,
    pub written: usize,
    pub skipped: usize,
    pub failed: usize,
}

/// Get the large archive threshold based on system RAM.
/// Returns 1GB if system has <16GB RAM, otherwise 2GB.
pub fn get_large_archive_threshold() -> u64 {
    use std::sync::OnceLock;
    static THRESHOLD: OnceLock<u64> = OnceLock::new();

    *THRESHOLD.get_or_init(|| {
        let sys = sysinfo::System::new_with_specifics(
            sysinfo::RefreshKind::nothing().with_memory(sysinfo::MemoryRefreshKind::everything()),
        );
        let total_ram = sys.total_memory();
        let sixteen_gb = 16 * 1024 * 1024 * 1024;

        if total_ram < sixteen_gb {
            1024 * 1024 * 1024 // 1 GB
        } else {
            2048 * 1024 * 1024 // 2 GB
        }
    })
}

/// Clean up leftover temp directories from previous interrupted runs.
pub fn cleanup_temp_dirs(downloads_dir: &std::path::Path) -> usize {
    let mut cleaned = 0;
    if let Ok(entries) = fs::read_dir(downloads_dir) {
        for entry in entries.filter_map(|e| e.ok()) {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            // tempfile creates dirs like .tmpXXXXXX
            if name_str.starts_with(".tmp") && entry.path().is_dir() {
                if fs::remove_dir_all(entry.path()).is_ok() {
                    cleaned += 1;
                }
            }
        }
    }
    if cleaned > 0 {
        eprintln!("Cleaned up {} leftover temp directories", cleaned);
    }
    cleaned
}

/// Progress callback type for streaming extraction.
/// Called with the current count of written files.
pub type ProgressCallback = Arc<dyn Fn(usize) + Send + Sync>;

/// Main entry point for streaming extraction with producer-consumer pipeline.
///
/// Processes archives using a decoupled extraction/move pipeline:
/// - Extractor workers (N/2 threads): Run 7z, produce file paths via channel
/// - Mover workers (N/2 threads): Consume paths, copy to final destination
pub fn process_from_archive_streaming(
    db: &ModlistDb,
    ctx: &ProcessContext,
    _config: StreamingConfig,
    pb: &ProgressBar,
    progress_callback: Option<ProgressCallback>,
) -> Result<StreamingStats> {
    // Clean up any leftover temp dirs from previous interrupted runs
    cleanup_temp_dirs(&ctx.config.downloads_dir);

    pb.set_message("Loading FromArchive directives...");
    let all_raw = db.get_all_pending_directives_of_type("FromArchive")?;

    if all_raw.is_empty() {
        return Ok(StreamingStats::default());
    }

    pb.set_message("Pre-filtering completed files...");

    // Parse, pre-filter against existing files, and group by archive
    // This avoids checking each file during extraction
    // Tuple: (directive_id, directive, resolved_path, file_in_bsa_if_nested)
    let mut by_archive: HashMap<String, Vec<(i64, FromArchiveDirective, Option<String>, Option<String>)>> = HashMap::new();
    let mut whole_file_directives: Vec<(i64, FromArchiveDirective)> = Vec::new();
    let mut parse_failures = 0;
    let mut pre_skipped = 0usize;

    for (id, json) in all_raw {
        match serde_json::from_str::<crate::modlist::Directive>(&json) {
            Ok(crate::modlist::Directive::FromArchive(d)) => {
                // PRE-FILTER: Skip if output already exists with correct size
                let normalized_to = paths::normalize_for_lookup(&d.to);
                if let Some(&existing_size) = ctx.existing_files.get(&normalized_to) {
                    if existing_size == d.size {
                        pre_skipped += 1;
                        continue; // Skip this directive entirely
                    }
                }

                if d.archive_hash_path.len() == 1 {
                    // Whole file - archive IS the file
                    whole_file_directives.push((id, d));
                } else if let Some(hash) = d.archive_hash_path.first() {
                    // Look up the correct path from the archive index
                    let resolved_path = if d.archive_hash_path.len() >= 2 {
                        let requested_path = &d.archive_hash_path[1];
                        db.lookup_archive_file(hash, requested_path).ok().flatten()
                    } else {
                        None
                    };

                    // Check if this is a nested BSA directive (len >= 3)
                    let file_in_bsa = if d.archive_hash_path.len() >= 3 {
                        Some(d.archive_hash_path[2].clone())
                    } else {
                        None
                    };

                    by_archive.entry(hash.clone()).or_default().push((id, d, resolved_path, file_in_bsa));
                }
            }
            _ => {
                parse_failures += 1;
            }
        }
    }

    if pre_skipped > 0 {
        eprintln!("Pre-filtered {} already-complete files", pre_skipped);
    }
    if parse_failures > 0 {
        warn!("WARN: {} directives failed to parse", parse_failures);
    }

    // Create a wrapper callback that adds pre_skipped offset to the count
    // This ensures the GUI sees the correct total progress including pre-filtered files
    let adjusted_callback: Option<ProgressCallback> = progress_callback.map(|cb| {
        let offset = pre_skipped;
        Arc::new(move |count: usize| {
            cb(offset + count);
        }) as ProgressCallback
    });

    // Group archives by size tier and type
    let archives: Vec<_> = by_archive.into_iter().collect();
    let mut small_archives = Vec::new();   // ≤512MB - full parallel (1 thread each)
    let mut medium_archives = Vec::new();  // 512MB-2GB - half parallel (2 threads each)
    let mut large_archives = Vec::new();   // ≥2GB - sequential (all threads)
    let mut bsa_archives = Vec::new();     // BSA/BA2 handled separately

    for (archive_hash, directives) in archives {
        let archive_path = match ctx.get_archive_path(&archive_hash) {
            Some(p) => p.clone(),
            None => continue,
        };

        let archive_type = detect_archive_type(&archive_path)
            .unwrap_or(NestedArchiveType::Unknown);

        // BSA/BA2 handled separately (no extraction needed)
        if matches!(archive_type, NestedArchiveType::Tes3Bsa | NestedArchiveType::Bsa | NestedArchiveType::Ba2) {
            bsa_archives.push((archive_hash, directives, archive_path));
            continue;
        }

        // Categorize by size tier
        let archive_size = fs::metadata(&archive_path)
            .map(|m| m.len())
            .unwrap_or(0);

        match ArchiveSizeTier::from_size(archive_size) {
            ArchiveSizeTier::Small => small_archives.push((archive_hash, directives, archive_path, archive_size)),
            ArchiveSizeTier::Medium => medium_archives.push((archive_hash, directives, archive_path, archive_size)),
            ArchiveSizeTier::Large => large_archives.push((archive_hash, directives, archive_path, archive_size)),
        }
    }

    // Sort each tier by size (smallest first) so quick archives complete first
    small_archives.sort_by_key(|(_, _, _, size)| *size);
    medium_archives.sort_by_key(|(_, _, _, size)| *size);
    large_archives.sort_by_key(|(_, _, _, size)| *size);

    let total_archives = small_archives.len() + medium_archives.len() + large_archives.len() + bsa_archives.len();
    let total_files: usize = small_archives.iter()
        .map(|(_, d, _, _)| d.len())
        .chain(medium_archives.iter().map(|(_, d, _, _)| d.len()))
        .chain(large_archives.iter().map(|(_, d, _, _)| d.len()))
        .chain(bsa_archives.iter().map(|(_, d, _)| d.len()))
        .sum();

    eprintln!(
        "Processing {} archives ({} files): {} small + {} medium + {} large + {} BSA",
        total_archives, total_files,
        small_archives.len(), medium_archives.len(), large_archives.len(), bsa_archives.len()
    );

    // Stats
    let extracted = Arc::new(AtomicUsize::new(0));
    let written = Arc::new(AtomicUsize::new(0));
    let skipped = Arc::new(AtomicUsize::new(0));
    let failed = Arc::new(AtomicUsize::new(0));
    let logged_failures = Arc::new(AtomicUsize::new(0));

    // Clear old progress bar line and reset for archive processing
    pb.finish_and_clear();
    pb.reset();
    pb.set_length(total_archives as u64);
    pb.set_position(0);
    pb.enable_steady_tick(std::time::Duration::from_millis(100));

    // Process whole-file directives first (simple copy)
    if !whole_file_directives.is_empty() {
        pb.set_message(format!("Copying {} whole-file directives...", whole_file_directives.len()));
        process_whole_file_directives(
            &whole_file_directives,
            ctx,
            &extracted,
            &skipped,
            &failed,
        );
    }

    // Calculate thread distribution for producer-consumer pipeline
    let total_threads = rayon::current_num_threads();
    let extractor_threads = (total_threads / 2).max(1);
    let mover_threads = (total_threads - extractor_threads).max(1);

    eprintln!("Using producer-consumer pipeline: {} extractor + {} mover threads",
              extractor_threads, mover_threads);

    // Process SMALL archives using producer-consumer pipeline
    if !small_archives.is_empty() {
        pb.set_message(format!("Processing {} small archives (producer-consumer)...", small_archives.len()));

        process_archives_with_pipeline(
            small_archives,
            ctx,
            &extracted,
            &written,
            &skipped,
            &failed,
            &logged_failures,
            pb,
            Some(ArchiveSizeTier::Small.threads_per_archive()),
            extractor_threads,
            mover_threads,
            adjusted_callback.clone(),
        );
    }

    // Process MEDIUM archives with pipeline
    if !medium_archives.is_empty() {
        let half_threads = (rayon::current_num_threads() / 2).max(1);
        pb.set_message(format!("Processing {} medium archives ({} at a time)...", medium_archives.len(), half_threads));

        // Process medium archives in chunks with pipeline
        for chunk in medium_archives.chunks(half_threads) {
            process_archives_with_pipeline(
                chunk.to_vec(),
                ctx,
                &extracted,
                &written,
                &skipped,
                &failed,
                &logged_failures,
                pb,
                Some(ArchiveSizeTier::Medium.threads_per_archive()),
                extractor_threads,
                mover_threads,
                adjusted_callback.clone(),
            );
        }
    }

    // Process LARGE archives SEQUENTIALLY with pipeline (all threads)
    for archive in large_archives {
        let archive_name = archive.2.file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "large".to_string());

        pb.set_message(format!("{} ({} files) [LARGE]", archive_name, archive.1.len()));

        process_archives_with_pipeline(
            vec![archive],
            ctx,
            &extracted,
            &written,
            &skipped,
            &failed,
            &logged_failures,
            pb,
            None, // All threads for large archives
            extractor_threads,
            mover_threads,
            adjusted_callback.clone(),
        );
    }

    // Process BSA/BA2 archives (read directly, no extraction needed)
    for (_archive_hash, directives, archive_path) in bsa_archives {
        let archive_name = archive_path.file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "bsa".to_string());

        pb.set_message(format!("{} ({} files) [BSA]", archive_name, directives.len()));

        // Process BSA directly (no extraction to temp needed)
        process_bsa_archive(
            &archive_path,
            &directives,
            ctx,
            &extracted,
            &written,
            &skipped,
            &failed,
            &logged_failures,
            adjusted_callback.clone(),
        );

        pb.inc(1);
        pb.set_message(format!(
            "OK:{} Skip:{} Fail:{}",
            written.load(Ordering::Relaxed),
            skipped.load(Ordering::Relaxed),
            failed.load(Ordering::Relaxed)
        ));
    }

    let stats = StreamingStats {
        extracted: extracted.load(Ordering::Relaxed),
        written: written.load(Ordering::Relaxed),
        // Include pre_skipped in the skipped count for accurate totals
        skipped: skipped.load(Ordering::Relaxed) + parse_failures + pre_skipped,
        failed: failed.load(Ordering::Relaxed),
    };

    eprintln!(
        "Complete: {} extracted, {} written, {} skipped ({} pre-filtered), {} failed",
        stats.extracted, stats.written, stats.skipped, pre_skipped, stats.failed
    );

    Ok(stats)
}

/// Process archives using the producer-consumer pipeline.
///
/// Extractor workers extract files to temp dirs and send paths to channel.
/// Mover workers receive paths and copy files to final destinations.
#[allow(clippy::too_many_arguments)]
fn process_archives_with_pipeline(
    archives: Vec<(String, Vec<(i64, FromArchiveDirective, Option<String>, Option<String>)>, PathBuf, u64)>,
    ctx: &ProcessContext,
    extracted: &Arc<AtomicUsize>,
    written: &Arc<AtomicUsize>,
    skipped: &Arc<AtomicUsize>,
    failed: &Arc<AtomicUsize>,
    logged_failures: &Arc<AtomicUsize>,
    pb: &ProgressBar,
    threads_per_archive: Option<usize>,
    _extractor_threads: usize,
    mover_threads: usize,
    progress_callback: Option<ProgressCallback>,
) {
    const MAX_LOGGED_FAILURES: usize = 100;

    // Bounded channel for backpressure - prevents memory buildup
    // Buffer size based on mover capacity
    let (tx, rx): (Sender<MoveJob>, Receiver<MoveJob>) = bounded(mover_threads * 64);

    // Shared state for mover workers
    let written_clone = Arc::clone(written);
    let skipped_clone = Arc::clone(skipped);
    let failed_clone = Arc::clone(failed);
    let logged_failures_clone = Arc::clone(logged_failures);
    let progress_callback_clone = progress_callback.clone();

    // Spawn mover workers in a separate thread pool
    let mover_handle = thread::spawn(move || {
        // Use a thread pool for movers
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(mover_threads)
            .build()
            .expect("Failed to create mover thread pool");

        pool.install(|| {
            // Process jobs from channel until it's closed
            rx.iter().par_bridge().for_each(|job: MoveJob| {
                // Verify size via metadata (no RAM needed)
                let src_size = match fs::metadata(&job.source_path) {
                    Ok(m) => m.len(),
                    Err(e) => {
                        let count = logged_failures_clone.fetch_add(1, Ordering::Relaxed);
                        if count < MAX_LOGGED_FAILURES {
                            error!("FAIL [{}]: metadata error: {}", job.directive_id, e);
                        }
                        failed_clone.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                };

                if src_size != job.expected_size {
                    let count = logged_failures_clone.fetch_add(1, Ordering::Relaxed);
                    if count < MAX_LOGGED_FAILURES {
                        error!("FAIL [{}]: size mismatch: expected {} got {}",
                                  job.directive_id, job.expected_size, src_size);
                    }
                    failed_clone.fetch_add(1, Ordering::Relaxed);
                    return;
                }

                // Create output directory
                if let Err(e) = paths::ensure_parent_dirs(&job.output_path) {
                    let count = logged_failures_clone.fetch_add(1, Ordering::Relaxed);
                    if count < MAX_LOGGED_FAILURES {
                        error!("FAIL [{}]: cannot create dirs: {}", job.directive_id, e);
                    }
                    failed_clone.fetch_add(1, Ordering::Relaxed);
                    return;
                }

                // Check if output already exists (race condition: another job may have created it)
                if let Ok(meta) = fs::metadata(&job.output_path) {
                    if meta.len() == job.expected_size {
                        // Already exists with correct size - treat as skip/success
                        skipped_clone.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                    // Wrong size - remove and re-copy
                    let _ = fs::remove_file(&job.output_path);
                }

                if job.is_shared_source {
                    // Source has multiple destinations - use reflink (instant CoW) with copy fallback
                    // Remove any existing file first (reflink fails if dest exists)
                    let _ = fs::remove_file(&job.output_path);
                    if let Err(e) = reflink_copy::reflink_or_copy(&job.source_path, &job.output_path) {
                        let count = logged_failures_clone.fetch_add(1, Ordering::Relaxed);
                        if count < MAX_LOGGED_FAILURES {
                            error!("FAIL [{}]: reflink/copy error: {}", job.directive_id, e);
                        }
                        failed_clone.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                } else {
                    // Single destination - use rename (instant move)
                    // Remove destination first if it exists (rename behavior varies by OS)
                    let _ = fs::remove_file(&job.output_path);
                    if fs::rename(&job.source_path, &job.output_path).is_err() {
                        // Fallback to reflink/copy if rename fails (cross-filesystem)
                        if let Err(e) = reflink_copy::reflink_or_copy(&job.source_path, &job.output_path) {
                            let count = logged_failures_clone.fetch_add(1, Ordering::Relaxed);
                            if count < MAX_LOGGED_FAILURES {
                                error!("FAIL [{}]: rename then reflink/copy failed: {}", job.directive_id, e);
                            }
                            failed_clone.fetch_add(1, Ordering::Relaxed);
                            return;
                        }
                        let _ = fs::remove_file(&job.source_path);
                    }
                }

                let new_count = written_clone.fetch_add(1, Ordering::Relaxed) + 1;
                // Call progress callback if provided
                if let Some(ref callback) = progress_callback_clone {
                    callback(new_count);
                }
            });
        });
    });

    // Process archives in parallel (extractor side)
    let pb_ref = pb;
    let ctx_ref = ctx;
    let extracted_ref = extracted;
    let skipped_ref = skipped;
    let failed_ref = failed;
    let logged_failures_ref = logged_failures;
    let output_dir = ctx.config.output_dir.clone();

    archives.par_iter().for_each(|(_archive_hash, directives, archive_path, _size)| {
        process_single_archive_with_channel(
            archive_path,
            directives,
            ctx_ref,
            extracted_ref,
            skipped_ref,
            failed_ref,
            logged_failures_ref,
            threads_per_archive,
            &tx,
            &output_dir,
        );

        pb_ref.inc(1);
        pb_ref.set_message(format!(
            "OK:{} Skip:{} Fail:{}",
            written.load(Ordering::Relaxed),
            skipped_ref.load(Ordering::Relaxed),
            failed_ref.load(Ordering::Relaxed)
        ));
    });

    // Drop sender to signal movers to finish
    drop(tx);

    // Wait for movers to complete
    mover_handle.join().expect("Mover thread panicked");
}

/// Process a single archive: extract to temp, send file paths to channel.
///
/// Uses full archive extraction (single fast `7z x` call) followed by fs::rename
/// for instant file moves. This is faster than streaming individual files.
#[allow(clippy::too_many_arguments)]
fn process_single_archive_with_channel(
    archive_path: &PathBuf,
    directives: &[(i64, FromArchiveDirective, Option<String>, Option<String>)],
    ctx: &ProcessContext,
    extracted: &Arc<AtomicUsize>,
    skipped: &Arc<AtomicUsize>,
    failed: &Arc<AtomicUsize>,
    logged_failures: &Arc<AtomicUsize>,
    threads: Option<usize>,
    tx: &Sender<MoveJob>,
    output_dir: &PathBuf,
) {
    const MAX_LOGGED_FAILURES: usize = 100;

    // Filter directives that need processing (skip existing files)
    let to_process: Vec<_> = directives
        .iter()
        .filter(|(_, d, _, _)| {
            let normalized_to = paths::normalize_for_lookup(&d.to);
            if let Some(&existing_size) = ctx.existing_files.get(&normalized_to) {
                if existing_size == d.size {
                    skipped.fetch_add(1, Ordering::Relaxed);
                    return false;
                }
            }
            true
        })
        .collect();

    if to_process.is_empty() {
        return;
    }

    // Create temp dir for extraction in output_dir (faster if output is local, downloads on NAS)
    let temp_dir = match tempfile::tempdir_in(&ctx.config.output_dir) {
        Ok(d) => Arc::new(d),
        Err(e) => {
            error!("FAIL: Cannot create temp dir: {}", e);
            failed.fetch_add(to_process.len(), Ordering::Relaxed);
            return;
        }
    };

    // Separate simple directives from nested BSA directives
    let mut simple_directives: Vec<_> = Vec::new();
    let mut nested_bsa_directives: Vec<_> = Vec::new();

    for item in &to_process {
        let (id, directive, resolved, file_in_bsa) = item;
        if file_in_bsa.is_some() {
            nested_bsa_directives.push((*id, directive, resolved.clone(), file_in_bsa.clone().unwrap()));
        } else {
            simple_directives.push((*id, directive, resolved.clone()));
        }
    }

    // FULL EXTRACTION: Extract entire archive to temp dir (one fast 7z x call)
    // This is faster than streaming individual files with separate 7z calls
    if let Err(e) = sevenzip::extract_all_with_threads(archive_path, temp_dir.path(), threads) {
        let count = logged_failures.fetch_add(1, Ordering::Relaxed);
        if count < MAX_LOGGED_FAILURES {
            error!("FAIL: Cannot extract {}: {}", archive_path.display(), e);
        }
        failed.fetch_add(to_process.len(), Ordering::Relaxed);
        return;
    }

    // Build map of extracted files (parallel walkdir)
    let extracted_map = build_extracted_file_map(temp_dir.path());

    // Count how many destinations each source file has
    // Files with multiple destinations need reflink/copy instead of rename
    let mut source_use_count: HashMap<String, usize> = HashMap::new();
    for (_, directive, resolved) in &simple_directives {
        let path_in_archive = resolved.as_deref()
            .or_else(|| directive.archive_hash_path.get(1).map(|s| s.as_str()))
            .unwrap_or("");
        let normalized = paths::normalize_for_lookup(path_in_archive);
        *source_use_count.entry(normalized).or_insert(0) += 1;
    }

    // Also count BSA files used by nested directives - they must not be renamed
    // until all nested extractions are done
    for (_, directive, resolved, _) in &nested_bsa_directives {
        let bsa_path = resolved.as_deref()
            .or_else(|| directive.archive_hash_path.get(1).map(|s| s.as_str()))
            .unwrap_or("");
        let normalized = paths::normalize_for_lookup(bsa_path);
        // Mark as shared so it uses reflink/copy instead of rename
        *source_use_count.entry(normalized).or_insert(0) += 1;
    }

    // Process nested BSA directives FIRST (before movers can rename BSA files)
    process_nested_bsa_directives(
        &nested_bsa_directives,
        &extracted_map,
        output_dir,
        extracted,
        failed,
        logged_failures,
    );

    // Send simple directives to mover workers via channel
    for (id, directive, resolved) in simple_directives {
        let path_in_archive = resolved.as_deref()
            .or_else(|| directive.archive_hash_path.get(1).map(|s| s.as_str()))
            .unwrap_or("");
        let normalized = paths::normalize_for_lookup(path_in_archive);

        let src_path = match extracted_map.get(&normalized) {
            Some(p) => p.clone(),
            None => {
                let count = logged_failures.fetch_add(1, Ordering::Relaxed);
                if count < MAX_LOGGED_FAILURES {
                    error!("FAIL [{}]: not found in archive: {}", id, path_in_archive);
                }
                failed.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };

        let final_output_path = paths::join_windows_path(output_dir, &directive.to);

        // If this source has multiple destinations, use reflink/copy
        let is_shared_source = source_use_count.get(&normalized).copied().unwrap_or(0) > 1;

        let job = MoveJob {
            source_path: src_path,
            output_path: final_output_path,
            expected_size: directive.size,
            directive_id: id,
            _temp_dir: Arc::clone(&temp_dir),
            is_shared_source,
        };

        if tx.send(job).is_err() {
            failed.fetch_add(1, Ordering::Relaxed);
            continue;
        }

        extracted.fetch_add(1, Ordering::Relaxed);
    }
}

/// Process nested archive directives - extracts files from nested archives (BSA, ZIP, 7z, etc.).
#[allow(clippy::too_many_arguments)]
fn process_nested_bsa_directives(
    nested_bsa_directives: &[(i64, &FromArchiveDirective, Option<String>, String)],
    extracted_map: &HashMap<String, PathBuf>,
    output_dir: &PathBuf,
    extracted: &Arc<AtomicUsize>,
    failed: &Arc<AtomicUsize>,
    logged_failures: &Arc<AtomicUsize>,
) {
    const MAX_LOGGED_FAILURES: usize = 100;

    if nested_bsa_directives.is_empty() {
        return;
    }

    // Group by nested archive file
    let mut by_archive: HashMap<String, Vec<(i64, &FromArchiveDirective, String)>> = HashMap::new();
    for (id, directive, resolved, file_in_archive) in nested_bsa_directives {
        let archive_path = resolved.as_deref()
            .or_else(|| directive.archive_hash_path.get(1).map(|s| s.as_str()))
            .unwrap_or("")
            .to_string();
        by_archive.entry(archive_path).or_default().push((*id, *directive, file_in_archive.clone()));
    }

    for (archive_path_in_outer, directives) in by_archive {
        let normalized_archive = paths::normalize_for_lookup(&archive_path_in_outer);

        let archive_disk_path = match extracted_map.get(&normalized_archive) {
            Some(p) => p.clone(),
            None => {
                let count = logged_failures.fetch_add(1, Ordering::Relaxed);
                if count < MAX_LOGGED_FAILURES {
                    error!("FAIL: Nested archive not found: {}", archive_path_in_outer);
                }
                failed.fetch_add(directives.len(), Ordering::Relaxed);
                continue;
            }
        };

        // Detect archive type to use correct extraction method
        let archive_type = match detect_archive_type(&archive_disk_path) {
            Ok(t) => t,
            Err(e) => {
                let count = logged_failures.fetch_add(1, Ordering::Relaxed);
                if count < MAX_LOGGED_FAILURES {
                    error!("FAIL: Cannot detect archive type for {}: {}", archive_path_in_outer, e);
                }
                failed.fetch_add(directives.len(), Ordering::Relaxed);
                continue;
            }
        };

        directives.par_iter().for_each(|(id, directive, file_in_archive)| {
            // Extract using appropriate method based on archive type
            let data = match archive_type {
                NestedArchiveType::Tes3Bsa | NestedArchiveType::Bsa | NestedArchiveType::Ba2 => {
                    // BSA/BA2 extraction using bsa module
                    bsa::extract_archive_file(&archive_disk_path, file_in_archive)
                }
                NestedArchiveType::Zip | NestedArchiveType::SevenZ | NestedArchiveType::Rar | NestedArchiveType::Unknown => {
                    // Regular archive extraction using 7z
                    sevenzip::extract_file_case_insensitive(&archive_disk_path, file_in_archive)
                }
            };

            let data = match data {
                Ok(d) => d,
                Err(e) => {
                    let count = logged_failures.fetch_add(1, Ordering::Relaxed);
                    if count < MAX_LOGGED_FAILURES {
                        error!("FAIL [{}]: Nested extract error from {:?} archive: {}", id, archive_type, e);
                    }
                    failed.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };

            if data.len() as u64 != directive.size {
                let count = logged_failures.fetch_add(1, Ordering::Relaxed);
                if count < MAX_LOGGED_FAILURES {
                    error!("FAIL [{}]: Size mismatch: expected {} got {}", id, directive.size, data.len());
                }
                failed.fetch_add(1, Ordering::Relaxed);
                return;
            }

            let output_path = paths::join_windows_path(output_dir, &directive.to);
            if let Err(_e) = paths::ensure_parent_dirs(&output_path) {
                failed.fetch_add(1, Ordering::Relaxed);
                return;
            }

            if let Err(e) = fs::write(&output_path, &data) {
                failed.fetch_add(1, Ordering::Relaxed);
                error!("FAIL [{}]: write error: {}", id, e);
                return;
            }

            extracted.fetch_add(1, Ordering::Relaxed);
        });
    }
}

/// Process whole-file directives (archive IS the file, just copy it).
fn process_whole_file_directives(
    directives: &[(i64, FromArchiveDirective)],
    ctx: &ProcessContext,
    extracted: &Arc<AtomicUsize>,
    skipped: &Arc<AtomicUsize>,
    failed: &Arc<AtomicUsize>,
) {
    directives.par_iter().for_each(|(id, directive)| {
        let archive_hash = &directive.archive_hash_path[0];

        // Check if output already exists with correct size
        let normalized_to = paths::normalize_for_lookup(&directive.to);
        if let Some(&existing_size) = ctx.existing_files.get(&normalized_to) {
            if existing_size == directive.size {
                skipped.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }

        let archive_path = match ctx.get_archive_path(archive_hash) {
            Some(p) => p,
            None => {
                failed.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };

        // Verify size matches
        let archive_size = match fs::metadata(archive_path) {
            Ok(m) => m.len(),
            Err(_) => {
                failed.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };

        if archive_size != directive.size {
            // Not actually a whole-file directive - size mismatch
            failed.fetch_add(1, Ordering::Relaxed);
            error!("FAIL [{}]: whole-file size mismatch {} vs {}", id, archive_size, directive.size);
            return;
        }

        // Copy the file
        let output_path = paths::join_windows_path(&ctx.config.output_dir, &directive.to);
        if let Err(e) = paths::ensure_parent_dirs(&output_path) {
            failed.fetch_add(1, Ordering::Relaxed);
            error!("FAIL [{}]: cannot create parent dirs: {}", id, e);
            return;
        }

        if let Err(e) = fs::copy(archive_path, &output_path) {
            failed.fetch_add(1, Ordering::Relaxed);
            error!("FAIL [{}]: copy failed: {}", id, e);
            return;
        }

        extracted.fetch_add(1, Ordering::Relaxed);
    });
}

/// Process a BSA/BA2 archive directly (no extraction needed).
/// BSA files are read directly using the bsa module.
#[allow(clippy::too_many_arguments)]
fn process_bsa_archive(
    archive_path: &PathBuf,
    directives: &[(i64, FromArchiveDirective, Option<String>, Option<String>)],
    ctx: &ProcessContext,
    extracted: &Arc<AtomicUsize>,
    written: &Arc<AtomicUsize>,
    skipped: &Arc<AtomicUsize>,
    failed: &Arc<AtomicUsize>,
    logged_failures: &Arc<AtomicUsize>,
    progress_callback: Option<ProgressCallback>,
) {
    const MAX_LOGGED_FAILURES: usize = 100;

    // Filter directives that need processing (skip existing files)
    let to_process: Vec<_> = directives
        .iter()
        .filter(|(_, d, _, _)| {
            let normalized_to = paths::normalize_for_lookup(&d.to);
            if let Some(&existing_size) = ctx.existing_files.get(&normalized_to) {
                if existing_size == d.size {
                    skipped.fetch_add(1, Ordering::Relaxed);
                    return false;
                }
            }
            true
        })
        .collect();

    if to_process.is_empty() {
        return;
    }

    let output_dir = &ctx.config.output_dir;

    // Process files from BSA in parallel
    to_process.par_iter().for_each(|(id, directive, resolved, _file_in_bsa)| {
        // For BSA archives, the file path is in archive_hash_path[1]
        let file_path_in_bsa = resolved.as_deref()
            .or_else(|| directive.archive_hash_path.get(1).map(|s| s.as_str()))
            .unwrap_or("");

        // Extract from BSA
        let data = match bsa::extract_archive_file(archive_path, file_path_in_bsa) {
            Ok(d) => d,
            Err(e) => {
                let count = logged_failures.fetch_add(1, Ordering::Relaxed);
                if count < MAX_LOGGED_FAILURES {
                    error!("FAIL [{}]: BSA read error: {}", id, e);
                }
                failed.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };

        // Verify size
        if data.len() as u64 != directive.size {
            let count = logged_failures.fetch_add(1, Ordering::Relaxed);
            if count < MAX_LOGGED_FAILURES {
                error!("FAIL [{}]: BSA size mismatch: expected {} got {}", id, directive.size, data.len());
            }
            failed.fetch_add(1, Ordering::Relaxed);
            return;
        }

        // Write to output
        let output_path = paths::join_windows_path(output_dir, &directive.to);
        if let Err(e) = paths::ensure_parent_dirs(&output_path) {
            failed.fetch_add(1, Ordering::Relaxed);
            error!("FAIL [{}]: cannot create dirs: {}", id, e);
            return;
        }

        if let Err(e) = fs::write(&output_path, &data) {
            failed.fetch_add(1, Ordering::Relaxed);
            error!("FAIL [{}]: write error: {}", id, e);
            return;
        }

        extracted.fetch_add(1, Ordering::Relaxed);
        let new_count = written.fetch_add(1, Ordering::Relaxed) + 1;
        // Call progress callback if provided
        if let Some(ref callback) = progress_callback {
            callback(new_count);
        }
    });
}

/// Build a map of all files in a directory: normalized path -> actual file path on disk.
///
/// Handles CP437-encoded filenames that 7z may extract from legacy Windows archives.
/// Creates entries for both the original path and CP437-to-UTF8 converted path.
fn build_extracted_file_map(dir: &std::path::Path) -> HashMap<String, PathBuf> {
    let entries: Vec<_> = walkdir::WalkDir::new(dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .collect();

    let mut map: HashMap<String, PathBuf> = HashMap::new();

    for entry in &entries {
        let rel_path = entry.path()
            .strip_prefix(dir)
            .unwrap_or(entry.path());

        // Get the path as bytes (OsStr -> bytes on Unix)
        #[cfg(unix)]
        let path_bytes: Vec<u8> = {
            use std::os::unix::ffi::OsStrExt;
            rel_path.as_os_str().as_bytes().to_vec()
        };
        #[cfg(not(unix))]
        let path_bytes: Vec<u8> = rel_path.to_string_lossy().as_bytes().to_vec();

        // Check if path contains non-ASCII bytes (potential CP437)
        let has_high_bytes = path_bytes.iter().any(|&b| b >= 0x80);

        // Add the regular normalized path
        let normalized = paths::normalize_for_lookup(&rel_path.to_string_lossy());
        map.insert(normalized, entry.path().to_path_buf());

        // If path contains high bytes, also add CP437-converted version
        if has_high_bytes {
            let utf8_path = paths::cp437_to_utf8(&path_bytes);
            let normalized_cp437 = paths::normalize_for_lookup(&utf8_path);
            map.insert(normalized_cp437, entry.path().to_path_buf());
        }
    }

    map
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_streaming_config_default() {
        let config = StreamingConfig::default();
        assert!(config.parallelism > 0);
    }

    #[test]
    fn test_large_archive_threshold() {
        let threshold = get_large_archive_threshold();
        // Should be either 1GB or 2GB depending on system RAM
        let one_gb = 1024 * 1024 * 1024;
        let two_gb = 2048 * 1024 * 1024;
        assert!(threshold == one_gb || threshold == two_gb,
            "Threshold should be 1GB or 2GB, got {}", threshold);
    }

    #[test]
    fn test_threshold_is_cached() {
        // Call twice - should return same value (cached)
        let first = get_large_archive_threshold();
        let second = get_large_archive_threshold();
        assert_eq!(first, second, "Threshold should be cached and consistent");
    }

    #[test]
    fn test_archive_size_tier() {
        const HALF_GB: u64 = 512 * 1024 * 1024;
        const TWO_GB: u64 = 2 * 1024 * 1024 * 1024;

        // Small: <=512MB
        assert_eq!(ArchiveSizeTier::from_size(0), ArchiveSizeTier::Small);
        assert_eq!(ArchiveSizeTier::from_size(HALF_GB), ArchiveSizeTier::Small);

        // Medium: 512MB-2GB
        assert_eq!(ArchiveSizeTier::from_size(HALF_GB + 1), ArchiveSizeTier::Medium);
        assert_eq!(ArchiveSizeTier::from_size(TWO_GB - 1), ArchiveSizeTier::Medium);

        // Large: >=2GB
        assert_eq!(ArchiveSizeTier::from_size(TWO_GB), ArchiveSizeTier::Large);
        assert_eq!(ArchiveSizeTier::from_size(TWO_GB + 1), ArchiveSizeTier::Large);
    }

    #[test]
    fn test_tier_threads() {
        assert_eq!(ArchiveSizeTier::Small.threads_per_archive(), 1);
        assert_eq!(ArchiveSizeTier::Medium.threads_per_archive(), 2);
        assert_eq!(ArchiveSizeTier::Large.threads_per_archive(), 0); // 0 = all threads
    }
}
