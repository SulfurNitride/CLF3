//! Fused install + patch pipeline for FromArchive and PatchedFromArchive directives.
//!
//! # Architecture
//!
//! ```text
//! Phase A: Extract + Patch (all N threads, per archive)
//!   Extract archive → temp dir
//!   FromArchive files: ready in temp
//!   PatchedFromArchive files: apply delta patch → write to temp
//!   Build manifest of staged files
//!
//! Phase B: Finalize (all N threads, per archive, immediately after Phase A)
//!   Reflink/copy staged files → final destinations
//!   Verify file sizes
//!   All verified → delete temp dir
//!   Failures → log, keep temp for diagnostics
//!   Next archive
//! ```
//!
//! Archives are tiered by size for optimal resource usage:
//! - Small (≤512MB):  Full parallel - 1 thread each, many archives at once
//! - Medium (512MB-2GB): Bounded parallel (1 thread per archive)
//! - Large (≥2GB):    Bounded parallel (1 thread per archive)
//!
//! BSA/BA2: Read directly using bsa module (no extraction needed) for pure FromArchive.
//! BSA/BA2 with PatchedFromArchive sources go through the fused pipeline.
//!
//! Peak temp disk = one archive's worth at a time (finalize immediately after each archive).

use crate::archive::sevenzip;
use crate::bsa;
use crate::installer::handlers::from_archive::{
    detect_archive_type, ArchiveType as NestedArchiveType,
};
use crate::modlist::{Directive, FromArchiveDirective, ModlistDb, PatchedFromArchiveDirective};
use crate::paths;

use super::extract_strategy::should_use_selective_extraction;
use super::processor::{
    apply_patch_to_temp, build_patch_basis_key, build_patch_basis_key_from_archive_hash_path,
    output_exists, preload_patch_blobs_by_name, restore_patched_output_from_cache,
    should_preload_patch_blobs, store_patched_output_in_cache, ProcessContext,
};

use anyhow::Result;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tracing::{debug, error, warn};

/// A file staged in a temp directory, ready to be finalized to its output path.
struct StagedFile {
    /// Path to the file in the temp directory
    temp_path: PathBuf,
    /// Final output path
    output_path: PathBuf,
    /// Expected file size for verification
    expected_size: u64,
    /// Directive ID for error reporting
    directive_id: i64,
}

/// Result of processing a single archive (extract + patch).
struct ArchiveResult {
    /// All files staged for finalization
    staged_files: Vec<StagedFile>,
    /// Temp directory holding staged files (dropped after finalization to clean up)
    temp_dir: tempfile::TempDir,
    /// Additional temp directories for nested archive extraction (dropped after finalization)
    nested_temp_dirs: Vec<tempfile::TempDir>,
    /// Number of directives skipped (cache hits, already exist)
    skipped_count: usize,
    /// Number of FromArchive files extracted
    extracted_count: usize,
    /// Number of PatchedFromArchive files patched
    patched_count: usize,
    /// Number of directives that failed
    failed_count: usize,
}

/// Unified directive enum for grouping both types by archive.
enum ArchiveDirective {
    FromArchive {
        id: i64,
        directive: FromArchiveDirective,
        resolved_path: Option<String>,
        file_in_bsa: Option<String>,
    },
    Patched {
        id: i64,
        directive: PatchedFromArchiveDirective,
    },
}

/// Statistics from finalization of one archive.
struct FinalizeStats {
    written: usize,
    skipped: usize,
    failed: usize,
}


/// Configuration for extraction (simplified).
#[derive(Debug, Clone, Default)]
pub struct StreamingConfig;

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
#[allow(dead_code)] // Used in tests
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

/// Main entry point for fused install + patch pipeline.
///
/// Processes both FromArchive and PatchedFromArchive directives in a single pass
/// per archive. Each archive is extracted once, then both file copies and patches
/// are staged to temp, and finalized (reflink/copy) to their output paths.
pub fn process_fused_streaming(
    db: &ModlistDb,
    ctx: &ProcessContext,
    _config: StreamingConfig,
    pb: &ProgressBar,
    progress_callback: Option<ProgressCallback>,
) -> Result<StreamingStats> {
    const MAX_LOGGED_FAILURES: usize = 100;

    // Clean up any leftover temp dirs from previous interrupted runs
    cleanup_temp_dirs(&ctx.config.downloads_dir);

    // === Load FromArchive directives ===
    pb.set_message("Loading FromArchive directives...");
    let from_archive_raw = db.get_all_pending_directives_of_type("FromArchive")?;

    // === Load PatchedFromArchive directives ===
    pb.set_message("Loading PatchedFromArchive directives...");
    let patched_raw = db.get_all_pending_directives_of_type("PatchedFromArchive")?;

    if from_archive_raw.is_empty() && patched_raw.is_empty() {
        return Ok(StreamingStats::default());
    }

    pb.set_message("Pre-filtering completed files...");

    // Parse FromArchive directives, pre-filter, group by archive
    let mut by_archive: HashMap<String, Vec<ArchiveDirective>> = HashMap::new();
    let mut whole_file_directives: Vec<(i64, FromArchiveDirective)> = Vec::new();
    let mut parse_failures = 0;
    let mut pre_skipped = 0usize;

    for (id, json) in from_archive_raw {
        match serde_json::from_str::<Directive>(&json) {
            Ok(Directive::FromArchive(d)) => {
                // PRE-FILTER: Skip if output already exists with correct size
                let normalized_to = paths::normalize_for_lookup(&d.to);
                if let Some(&existing_size) = ctx.existing_files.get(&normalized_to) {
                    if existing_size == d.size {
                        pre_skipped += 1;
                        continue;
                    }
                }

                if d.archive_hash_path.len() == 1 {
                    // Whole file - archive IS the file
                    whole_file_directives.push((id, d));
                } else if let Some(hash) = d.archive_hash_path.first() {
                    let resolved_path = if d.archive_hash_path.len() >= 2 {
                        let requested_path = &d.archive_hash_path[1];
                        db.lookup_archive_file(hash, requested_path).ok().flatten()
                    } else {
                        None
                    };

                    let file_in_bsa = if d.archive_hash_path.len() >= 3 {
                        Some(d.archive_hash_path[2].clone())
                    } else {
                        None
                    };

                    by_archive
                        .entry(hash.clone())
                        .or_default()
                        .push(ArchiveDirective::FromArchive {
                            id,
                            directive: d,
                            resolved_path,
                            file_in_bsa,
                        });
                }
            }
            _ => {
                parse_failures += 1;
            }
        }
    }

    // Parse PatchedFromArchive directives, pre-filter, group by archive
    // Patched directives that are pre-skipped are already counted in the shared pre_skipped counter

    for (id, json) in patched_raw {
        match serde_json::from_str::<Directive>(&json) {
            Ok(Directive::PatchedFromArchive(d)) => {
                let normalized_to = paths::normalize_for_lookup(&d.to);
                if let Some(&existing_size) = ctx.existing_files.get(&normalized_to) {
                    if existing_size == d.size {
                        // counted in shared pre_skipped
                        pre_skipped += 1;
                        continue;
                    }
                }

                if let Some(hash) = d.archive_hash_path.first() {
                    by_archive
                        .entry(hash.clone())
                        .or_default()
                        .push(ArchiveDirective::Patched { id, directive: d });
                }
            }
            _ => {
                // patched parse failures are counted in the shared parse_failures counter
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
    let adjusted_callback: Option<ProgressCallback> = progress_callback.map(|cb| {
        let offset = pre_skipped;
        Arc::new(move |count: usize| {
            cb(offset + count);
        }) as ProgressCallback
    });

    // Classify archives by type
    let archives: Vec<_> = by_archive.into_iter().collect();
    let archive_count = archives.len();

    // Unified list of all archives to process, with type tag
    enum ArchiveKind {
        /// Extract via 7z/ZIP/RAR, then finalize
        Extract,
        /// Read directly via BSA/BA2 module (no extraction)
        BsaDirect,
    }

    let mut all_archives: Vec<(String, Vec<ArchiveDirective>, PathBuf, ArchiveKind)> =
        Vec::with_capacity(archive_count);
    let mut extract_count = 0usize;
    let mut bsa_only_count = 0usize;

    for (archive_hash, directives) in archives {
        let archive_path = match ctx.get_archive_path(&archive_hash) {
            Some(p) => p.clone(),
            None => continue,
        };

        let archive_type = detect_archive_type(&archive_path).unwrap_or(NestedArchiveType::Unknown);

        // BSA/BA2 with ONLY FromArchive directives → fast direct-read path
        let has_patched = directives
            .iter()
            .any(|d| matches!(d, ArchiveDirective::Patched { .. }));

        if !has_patched
            && matches!(
                archive_type,
                NestedArchiveType::Tes3Bsa | NestedArchiveType::Bsa | NestedArchiveType::Ba2
            )
        {
            bsa_only_count += 1;
            all_archives.push((archive_hash, directives, archive_path, ArchiveKind::BsaDirect));
        } else {
            extract_count += 1;
            all_archives.push((archive_hash, directives, archive_path, ArchiveKind::Extract));
        }
    }

    // Sort by directive count descending so big archives start first
    all_archives.sort_by(|a, b| b.1.len().cmp(&a.1.len()));

    let total_archives = all_archives.len();
    let total_files: usize = all_archives.iter().map(|(_, d, _, _)| d.len()).sum();

    eprintln!(
        "Processing {} archives ({} files): {} extract + {} BSA-only",
        total_archives, total_files, extract_count, bsa_only_count
    );

    // Stats
    let extracted = Arc::new(AtomicUsize::new(0));
    let written = Arc::new(AtomicUsize::new(0));
    let skipped = Arc::new(AtomicUsize::new(0));
    let failed = Arc::new(AtomicUsize::new(0));
    let logged_failures = Arc::new(AtomicUsize::new(0));
    let completed_archives = Arc::new(AtomicUsize::new(0));

    // Multi-progress display: overall bar + per-archive spinner bars
    let mp = MultiProgress::new();
    pb.finish_and_clear();
    let overall_pb = mp.add(ProgressBar::new(total_archives as u64));
    overall_pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} archives | {msg}")
            .unwrap()
            .progress_chars("=>-"),
    );
    overall_pb.enable_steady_tick(std::time::Duration::from_millis(100));
    overall_pb.set_message("Starting...");

    let archive_bar_style = ProgressStyle::default_spinner()
        .template("  {spinner:.blue} {wide_msg}")
        .unwrap();

    // Process whole-file directives first (simple copy)
    if !whole_file_directives.is_empty() {
        overall_pb.set_message(format!(
            "Copying {} whole-file directives...",
            whole_file_directives.len()
        ));
        process_whole_file_directives(&whole_file_directives, ctx, &extracted, &skipped, &failed);
    }

    // Process ALL archives in a single parallel pool (rayon manages thread count)
    all_archives.par_iter().for_each(
        |(archive_hash, directives, archive_path, kind)| {
            let archive_name = archive_path
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_else(|| archive_hash.clone());
            let display_name = truncate_name(&archive_name, 50);

            let is_bsa_direct = matches!(kind, ArchiveKind::BsaDirect);
            let label = if is_bsa_direct { "reading" } else { "extracting" };

            // Per-archive progress bar
            let archive_pb = mp.insert_before(&overall_pb, ProgressBar::new_spinner());
            archive_pb.set_style(archive_bar_style.clone());
            archive_pb.enable_steady_tick(std::time::Duration::from_millis(100));
            archive_pb.set_message(format!(
                "{} ({} files) {}...",
                display_name,
                directives.len(),
                label,
            ));

            if is_bsa_direct {
                // BSA/BA2 direct-read path
                let bsa_directives: Vec<(
                    i64,
                    FromArchiveDirective,
                    Option<String>,
                    Option<String>,
                )> = directives
                    .iter()
                    .filter_map(|d| match d {
                        ArchiveDirective::FromArchive {
                            id,
                            directive,
                            resolved_path,
                            file_in_bsa,
                        } => Some((
                            *id,
                            directive.clone(),
                            resolved_path.clone(),
                            file_in_bsa.clone(),
                        )),
                        _ => None,
                    })
                    .collect();

                process_bsa_archive(
                    archive_path,
                    &bsa_directives,
                    ctx,
                    &extracted,
                    &written,
                    &skipped,
                    &failed,
                    &logged_failures,
                    adjusted_callback.clone(),
                );
            } else {
                // Extract + Patch fused pipeline
                let result = process_single_archive_fused(
                    archive_path,
                    archive_hash,
                    directives,
                    ctx,
                    Some(1),
                );

                match result {
                    Ok(archive_result) => {
                        extracted.fetch_add(
                            archive_result.extracted_count + archive_result.patched_count,
                            Ordering::Relaxed,
                        );
                        skipped.fetch_add(archive_result.skipped_count, Ordering::Relaxed);
                        failed.fetch_add(archive_result.failed_count, Ordering::Relaxed);

                        archive_pb.set_message(format!(
                            "{} ({} files) finalizing...",
                            display_name,
                            archive_result.staged_files.len()
                        ));

                        let fin_stats = finalize_archive(
                            archive_result,
                            &ctx.config.output_dir,
                            &logged_failures,
                            &adjusted_callback,
                        );
                        written.fetch_add(fin_stats.written, Ordering::Relaxed);
                        skipped.fetch_add(fin_stats.skipped, Ordering::Relaxed);
                        failed.fetch_add(fin_stats.failed, Ordering::Relaxed);
                    }
                    Err(e) => {
                        let count = logged_failures.fetch_add(1, Ordering::Relaxed);
                        if count < MAX_LOGGED_FAILURES {
                            error!("FAIL: Archive {}: {:#}", archive_name, e);
                        }
                        failed.fetch_add(directives.len(), Ordering::Relaxed);
                    }
                }
            }

            archive_pb.finish_and_clear();
            completed_archives.fetch_add(1, Ordering::Relaxed);
            let done = completed_archives.load(Ordering::Relaxed);
            overall_pb.set_position(done as u64);
            overall_pb.set_message(format!(
                "OK:{} Skip:{} Fail:{}",
                written.load(Ordering::Relaxed),
                skipped.load(Ordering::Relaxed),
                failed.load(Ordering::Relaxed)
            ));
        },
    );

    overall_pb.finish_and_clear();

    let stats = StreamingStats {
        extracted: extracted.load(Ordering::Relaxed),
        written: written.load(Ordering::Relaxed),
        skipped: skipped.load(Ordering::Relaxed) + parse_failures + pre_skipped,
        failed: failed.load(Ordering::Relaxed),
    };

    eprintln!(
        "Complete: {} extracted, {} written, {} skipped ({} pre-filtered), {} failed",
        stats.extracted, stats.written, stats.skipped, pre_skipped, stats.failed
    );

    Ok(stats)
}

/// Process a single archive: extract to temp, apply patches, return staged files.
///
/// Handles both FromArchive and PatchedFromArchive directives in one extraction pass.
fn process_single_archive_fused(
    archive_path: &Path,
    _archive_hash: &str,
    directives: &[ArchiveDirective],
    ctx: &ProcessContext,
    threads_per_archive: Option<usize>,
) -> Result<ArchiveResult> {
    const MAX_LOGGED_FAILURES: usize = 100;

    let temp_dir = tempfile::tempdir_in(&ctx.config.output_dir)?;
    let output_dir = &ctx.config.output_dir;

    // Separate directive types
    let mut from_simple: Vec<(i64, &FromArchiveDirective, Option<&str>)> = Vec::new();
    let mut from_nested: Vec<(i64, &FromArchiveDirective, Option<&str>, &str)> = Vec::new();
    let mut patched_directives: Vec<(i64, &PatchedFromArchiveDirective)> = Vec::new();

    for d in directives {
        match d {
            ArchiveDirective::FromArchive {
                id,
                directive,
                resolved_path,
                file_in_bsa,
            } => {
                let resolved = resolved_path.as_deref();
                if let Some(ref bsa_file) = file_in_bsa {
                    from_nested.push((*id, directive, resolved, bsa_file));
                } else {
                    from_simple.push((*id, directive, resolved));
                }
            }
            ArchiveDirective::Patched { id, directive } => {
                patched_directives.push((*id, directive));
            }
        }
    }

    // Filter patched directives: check cache and existing outputs first
    let mut patched_to_process: Vec<(i64, &PatchedFromArchiveDirective)> = Vec::new();
    let mut patched_skipped = 0usize;
    let mut patched_cache_completed = 0usize;

    for (id, directive) in &patched_directives {
        if output_exists(ctx, &directive.to, directive.size) {
            patched_skipped += 1;
            continue;
        }

        match restore_patched_output_from_cache(ctx, directive) {
            Ok(true) => {
                patched_cache_completed += 1;
                continue;
            }
            Ok(false) => {}
            Err(e) => {
                warn!(
                    "Patch cache restore failed for '{}': {} (falling back)",
                    directive.to, e
                );
            }
        }

        patched_to_process.push((*id, directive));
    }

    // Build verified local basis for patched directives
    let mut verified_local_basis: HashMap<String, PathBuf> = HashMap::new();
    let mut needs_extraction: Vec<(i64, &PatchedFromArchiveDirective)> = Vec::new();

    for (id, directive) in &patched_to_process {
        let basis_key =
            build_patch_basis_key_from_archive_hash_path(&directive.archive_hash_path);
        if let Some(key) = basis_key.as_deref() {
            if let Some(local_path) =
                ctx.resolve_verified_patch_basis_path(key, Some(&directive.from_hash))
            {
                verified_local_basis.insert(key.to_string(), local_path);
                // Still need to process this directive but source comes from local basis
                needs_extraction.push((*id, directive));
                continue;
            }
        }
        needs_extraction.push((*id, directive));
    }
    // All patched_to_process need processing; needs_extraction is the same list
    // The verified_local_basis just provides an alternative source path
    drop(needs_extraction);

    // Collect all paths needed from the archive (union of FromArchive + PatchedFromArchive)
    let mut needed_paths = Vec::new();
    let mut seen_needed = HashSet::new();

    for (_, directive, resolved) in &from_simple {
        let path = resolved
            .or_else(|| directive.archive_hash_path.get(1).map(|s| s.as_str()))
            .unwrap_or("");
        if !path.is_empty() && seen_needed.insert(normalize_archive_lookup_path(path)) {
            needed_paths.push(path.to_string());
        }
    }

    for (_, directive, resolved, _) in &from_nested {
        let path = resolved
            .or_else(|| directive.archive_hash_path.get(1).map(|s| s.as_str()))
            .unwrap_or("");
        if !path.is_empty() && seen_needed.insert(normalize_archive_lookup_path(path)) {
            needed_paths.push(path.to_string());
        }
    }

    // For patched directives, include their source paths (unless served by local basis)
    for (_, directive) in &patched_to_process {
        let basis_key =
            build_patch_basis_key_from_archive_hash_path(&directive.archive_hash_path);
        if basis_key
            .as_deref()
            .is_some_and(|k| verified_local_basis.contains_key(k))
        {
            continue; // Source comes from local basis, no extraction needed
        }

        if directive.archive_hash_path.len() >= 2 {
            let path = &directive.archive_hash_path[1];
            if seen_needed.insert(normalize_archive_lookup_path(path)) {
                needed_paths.push(path.clone());
            }
        }
    }

    // Detect archive type for extraction strategy
    let archive_type =
        detect_archive_type(archive_path).unwrap_or(NestedArchiveType::Unknown);
    let is_bsa = matches!(
        archive_type,
        NestedArchiveType::Tes3Bsa | NestedArchiveType::Bsa | NestedArchiveType::Ba2
    );

    // Concurrent: extraction + patch delta preloading
    let preload_enabled = should_preload_patch_blobs() && !patched_to_process.is_empty();
    let wabbajack_path = ctx.config.wabbajack_path.clone();
    let extract_dir = temp_dir.path().to_path_buf();

    let (mut extracted_map, preloaded_patches) = std::thread::scope(|s| {
        // Preload patch deltas in background
        let preload_handle = s.spawn(|| {
            if preload_enabled {
                let patch_names: HashSet<String> = patched_to_process
                    .iter()
                    .map(|(_, d)| d.patch_id.to_string())
                    .collect();
                preload_patch_blobs_by_name(&wabbajack_path, &patch_names).unwrap_or_else(|e| {
                    debug!("Patch preload failed (falling back to shared reader): {}", e);
                    HashMap::new()
                })
            } else {
                HashMap::new()
            }
        });

        // Extract archive
        if is_bsa {
            // BSA/BA2: extract each needed file individually (preserving path structure)
            extract_bsa_files_to_temp(archive_path, &needed_paths, &extract_dir);
        } else if !needed_paths.is_empty() {
            if let Err(e) = extract_archive_to_temp(
                archive_path,
                &needed_paths,
                &extract_dir,
                threads_per_archive,
            ) {
                error!("FAIL: Cannot extract {}: {}", archive_path.display(), e);
            }
        }

        let map = build_extracted_file_map(&extract_dir);
        let patches = preload_handle.join().unwrap_or_default();
        (map, patches)
    });

    // Extract nested BSA/BA2 sources for PatchedFromArchive directives with archive_hash_path.len() >= 3
    // Group by BSA/BA2 container so we open each archive only once (parallel batch extraction)
    let mut nested_temp_dirs: Vec<tempfile::TempDir> = Vec::new();
    {
        // Collect needed files grouped by their BSA/BA2 container path
        let mut by_bsa_container: HashMap<PathBuf, Vec<(String, String)>> = HashMap::new();

        for (_, directive) in &patched_to_process {
            if directive.archive_hash_path.len() < 3 {
                continue;
            }
            let basis_key =
                build_patch_basis_key_from_archive_hash_path(&directive.archive_hash_path);
            if basis_key
                .as_deref()
                .is_some_and(|k| verified_local_basis.contains_key(k))
            {
                continue;
            }

            let bsa_path = &directive.archive_hash_path[1];
            let file_in_bsa = &directive.archive_hash_path[2];
            let combined_key = format!("{}/{}", bsa_path, file_in_bsa);
            let combined_normalized = paths::normalize_for_lookup(&combined_key);

            if extracted_map.contains_key(&combined_normalized) {
                continue;
            }

            let bsa_normalized = normalize_archive_lookup_path(bsa_path);
            if let Some(disk_path) = extracted_map.get(&bsa_normalized) {
                by_bsa_container
                    .entry(disk_path.clone())
                    .or_default()
                    .push((file_in_bsa.clone(), combined_normalized));
            } else {
                warn!(
                    "Nested BSA container not found for patch source: {}",
                    bsa_path
                );
            }
        }

        // Batch extract from each BSA/BA2 container (opens archive once, parallel decompression)
        for (bsa_disk_path, needed_files) in &by_bsa_container {
            let wanted: HashSet<String> = needed_files
                .iter()
                .map(|(file_in_bsa, _)| file_in_bsa.replace('\\', "/").to_lowercase())
                .collect();

            let nested_tmp = match tempfile::tempdir_in(output_dir) {
                Ok(t) => t,
                Err(e) => {
                    error!("FAIL: create temp dir for nested BSA batch: {}", e);
                    continue;
                }
            };

            // Build reverse lookup: normalized BSA path -> combined_normalized key
            let mut path_to_combined: HashMap<String, String> = HashMap::new();
            for (file_in_bsa, combined_normalized) in needed_files {
                let normalized = file_in_bsa.replace('\\', "/").to_lowercase();
                path_to_combined.insert(normalized, combined_normalized.clone());
            }

            // Collect extracted file mappings via Mutex (callback is Fn, not FnMut)
            let extracted_entries: std::sync::Mutex<Vec<(String, PathBuf)>> =
                std::sync::Mutex::new(Vec::new());
            let tmp_path = nested_tmp.path().to_path_buf();

            if let Err(e) = bsa::extract_archive_batch(bsa_disk_path, &wanted, |path, data| {
                let normalized = path.replace('\\', "/").to_lowercase();
                let temp_file = tmp_path.join(path);
                if let Some(parent) = temp_file.parent() {
                    let _ = fs::create_dir_all(parent);
                }
                fs::write(&temp_file, &data)
                    .map_err(|e| anyhow::anyhow!("write nested BSA extract: {}", e))?;

                if let Some(combined_key) = path_to_combined.get(&normalized) {
                    extracted_entries
                        .lock()
                        .expect("extracted_entries mutex")
                        .push((combined_key.clone(), temp_file));
                }
                Ok(())
            }) {
                error!(
                    "FAIL: batch extract from nested BSA/BA2 {}: {}",
                    bsa_disk_path.display(),
                    e
                );
            }

            // Merge extracted entries into the main map
            for (key, path) in extracted_entries.into_inner().expect("extracted_entries mutex") {
                extracted_map.insert(key, path);
            }

            nested_temp_dirs.push(nested_tmp);
        }
    }

    let mut staged_files = Vec::new();
    let total_skipped = patched_skipped;
    let mut extracted_count = 0usize;
    let mut patched_count = patched_cache_completed;
    let mut failed_count = 0usize;

    // Count how many destinations each source has (for shared source detection)
    let mut source_use_count: HashMap<String, usize> = HashMap::new();
    for (_, directive, resolved) in &from_simple {
        let path = resolved
            .or_else(|| directive.archive_hash_path.get(1).map(|s| s.as_str()))
            .unwrap_or("");
        let normalized = normalize_archive_lookup_path(path);
        *source_use_count.entry(normalized).or_insert(0) += 1;
    }
    // BSA containers used by nested directives should not be renamed
    for (_, directive, resolved, _) in &from_nested {
        let path = resolved
            .or_else(|| directive.archive_hash_path.get(1).map(|s| s.as_str()))
            .unwrap_or("");
        let normalized = normalize_archive_lookup_path(path);
        *source_use_count.entry(normalized).or_insert(0) += 1;
    }

    // === Process nested BSA directives FIRST (before anything can rename BSA files) ===
    if !from_nested.is_empty() {
        let (nested_staged, nested_bsa_temp_dirs) = process_nested_bsa_directives_staged(
            &from_nested,
            &extracted_map,
            ctx,
            output_dir,
        );
        nested_temp_dirs.extend(nested_bsa_temp_dirs);
        for result in nested_staged {
            match result {
                Ok(sf) => {
                    staged_files.push(sf);
                    extracted_count += 1;
                }
                Err(msg) => {
                    error!("{}", msg);
                    failed_count += 1;
                }
            }
        }
    }

    // === Process FromArchive simple directives ===
    for (id, directive, resolved) in &from_simple {
        let path_in_archive = resolved
            .or_else(|| directive.archive_hash_path.get(1).map(|s| s.as_str()))
            .unwrap_or("");
        let normalized = normalize_archive_lookup_path(path_in_archive);

        let src_path = match extracted_map.get(&normalized) {
            Some(p) => p.clone(),
            None => {
                let count = failed_count;
                if count < MAX_LOGGED_FAILURES {
                    error!("FAIL [{}]: not found in archive: {}", id, path_in_archive);
                }
                failed_count += 1;
                continue;
            }
        };

        let final_output_path = paths::join_windows_path(output_dir, &directive.to);

        // Record patch basis
        if !path_in_archive.is_empty() {
            if let Some(ahash) = directive.archive_hash_path.first() {
                let basis_key = build_patch_basis_key(ahash, Some(path_in_archive), None);
                ctx.record_patch_basis_candidate_path_dual(
                    &basis_key,
                    &directive.archive_hash_path,
                    &src_path,
                    &final_output_path,
                    directive.size,
                );
            }
        }

        staged_files.push(StagedFile {
            temp_path: src_path,
            output_path: final_output_path,
            expected_size: directive.size,
            directive_id: *id,
        });
        extracted_count += 1;
    }

    // === Process PatchedFromArchive directives ===
    // Use parallel processing for patch application
    let patch_results: Vec<_> = patched_to_process
        .par_iter()
        .map(|(id, directive)| {
            let patch_name = directive.patch_id.to_string();
            let preloaded_delta = preloaded_patches.get(&patch_name).map(|v| v.as_slice());

            // Find source path
            let source_path = resolve_patch_source(
                directive,
                &verified_local_basis,
                &extracted_map,
                archive_path,
            );

            let source_path = match source_path {
                Ok(p) => p,
                Err(e) => {
                    return Err((*id, format!("Source not found for patch: {:#}", e)));
                }
            };

            // Apply patch to temp file
            let temp_output = temp_dir
                .path()
                .join(format!("patched_{}.tmp", id));
            if let Err(e) = apply_patch_to_temp(
                ctx,
                &source_path,
                &patch_name,
                preloaded_delta,
                &temp_output,
                directive.size,
            ) {
                return Err((*id, format!("Patch apply failed: {:#}", e)));
            }

            // Store in patch cache (from temp — cache verifies hash on restore)
            if let Err(e) = store_patched_output_in_cache(ctx, directive, &temp_output) {
                warn!("Failed to cache patch output '{}': {}", directive.to, e);
            }

            let final_output_path = paths::join_windows_path(output_dir, &directive.to);
            Ok(StagedFile {
                temp_path: temp_output,
                output_path: final_output_path,
                expected_size: directive.size,
                directive_id: *id,
            })
        })
        .collect();

    for result in patch_results {
        match result {
            Ok(sf) => {
                staged_files.push(sf);
                patched_count += 1;
            }
            Err((id, msg)) => {
                if failed_count < MAX_LOGGED_FAILURES {
                    error!("FAIL [{}]: {}", id, msg);
                }
                failed_count += 1;
            }
        }
    }

    Ok(ArchiveResult {
        staged_files,
        temp_dir,
        nested_temp_dirs,
        skipped_count: total_skipped,
        extracted_count,
        patched_count,
        failed_count,
    })
}

/// Resolve the source path for a PatchedFromArchive directive.
///
/// Priority: verified local basis → extracted file map → archive itself (len==1)
fn resolve_patch_source(
    directive: &PatchedFromArchiveDirective,
    verified_local_basis: &HashMap<String, PathBuf>,
    extracted_map: &HashMap<String, PathBuf>,
    archive_path: &Path,
) -> Result<PathBuf> {
    let basis_key =
        build_patch_basis_key_from_archive_hash_path(&directive.archive_hash_path);

    if let Some(key) = basis_key.as_deref() {
        if let Some(path) = verified_local_basis.get(key) {
            return Ok(path.clone());
        }
    }

    if directive.archive_hash_path.len() == 1 {
        // Source is the archive file itself
        return Ok(archive_path.to_path_buf());
    }

    if directive.archive_hash_path.len() == 2 {
        let path = &directive.archive_hash_path[1];
        let normalized = normalize_archive_lookup_path(path);
        return extracted_map
            .get(&normalized)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Source not found in extracted files: {}", path));
    }

    if directive.archive_hash_path.len() >= 3 {
        let bsa_path = &directive.archive_hash_path[1];
        let file_in_bsa = &directive.archive_hash_path[2];
        // Try combined key first (inserted by nested BSA extraction above)
        let combined_key = format!("{}/{}", bsa_path, file_in_bsa);
        let combined_normalized = paths::normalize_for_lookup(&combined_key);
        if let Some(path) = extracted_map.get(&combined_normalized) {
            return Ok(path.clone());
        }
        // Also try just the inner file path (in case it was extracted differently)
        let inner_normalized = normalize_archive_lookup_path(file_in_bsa);
        return extracted_map.get(&inner_normalized).cloned().ok_or_else(|| {
            anyhow::anyhow!(
                "Source not found in BSA/BA2: {} / {}",
                bsa_path,
                file_in_bsa
            )
        });
    }

    anyhow::bail!("Invalid archive_hash_path")
}

/// Extract BSA/BA2 files to temp directory in parallel, preserving path structure.
/// Opens the archive once and decompresses all matching files in parallel.
fn extract_bsa_files_to_temp(archive_path: &Path, needed_paths: &[String], temp_dir: &Path) {
    let wanted: HashSet<String> = needed_paths
        .iter()
        .map(|p| p.replace('\\', "/").to_lowercase())
        .collect();

    if let Err(e) = bsa::extract_archive_batch(archive_path, &wanted, |path, data| {
        let temp_file = temp_dir.join(path);
        if let Some(parent) = temp_file.parent() {
            let _ = fs::create_dir_all(parent);
        }
        fs::write(&temp_file, &data).map_err(|e| {
            anyhow::anyhow!("BSA extract write error for {}: {}", path, e)
        })
    }) {
        error!(
            "FAIL: BSA batch extraction error for {}: {}",
            archive_path.display(),
            e
        );
    }
}

/// Extract archive (7z/ZIP/RAR) to temp directory with fallback chain.
fn extract_archive_to_temp(
    archive_path: &Path,
    needed_paths: &[String],
    temp_dir: &Path,
    threads: Option<usize>,
) -> Result<()> {
    let selective = should_use_selective_extraction(archive_path, needed_paths.len());
    debug!(
        "Extraction strategy: archive={}, needed_files={}, mode={}",
        archive_path.display(),
        needed_paths.len(),
        if selective { "selective" } else { "full" }
    );

    if selective {
        sevenzip::extract_files_case_insensitive(archive_path, needed_paths, temp_dir)
            .map(|_| ())
            .or_else(|e| {
                warn!(
                    "Selective extraction failed for {}, falling back to full: {}",
                    archive_path.display(),
                    e
                );
                sevenzip::extract_all_with_threads(archive_path, temp_dir, threads).map(|_| ())
            })
    } else {
        sevenzip::extract_all_with_threads(archive_path, temp_dir, threads)
            .map(|_| ())
            .or_else(|full_err| {
                if needed_paths.is_empty() {
                    return Err(full_err);
                }
                warn!(
                    "Full extraction failed for {}, retrying selective of {} files: {}",
                    archive_path.display(),
                    needed_paths.len(),
                    full_err
                );
                sevenzip::extract_files_case_insensitive(archive_path, needed_paths, temp_dir)
                    .map(|_| ())
                    .map_err(|selective_err| {
                        anyhow::anyhow!(
                            "full extraction failed: {}; selective fallback failed: {}",
                            full_err,
                            selective_err
                        )
                    })
            })
    }
}

/// Finalize staged files: reflink/copy to output, verify size, cleanup temp.
fn finalize_archive(
    result: ArchiveResult,
    _output_dir: &Path,
    logged_failures: &Arc<AtomicUsize>,
    progress_callback: &Option<ProgressCallback>,
) -> FinalizeStats {
    const MAX_LOGGED_FAILURES: usize = 100;

    let written = AtomicUsize::new(0);
    let skipped_atomic = AtomicUsize::new(0);
    let failed_atomic = AtomicUsize::new(0);

    result.staged_files.par_iter().for_each(|sf| {
        // Create output directory
        if let Err(e) = paths::ensure_parent_dirs(&sf.output_path) {
            let count = logged_failures.fetch_add(1, Ordering::Relaxed);
            if count < MAX_LOGGED_FAILURES {
                error!(
                    "FAIL [{}]: cannot create dirs for {}: {}",
                    sf.directive_id,
                    sf.output_path.display(),
                    e
                );
            }
            failed_atomic.fetch_add(1, Ordering::Relaxed);
            return;
        }

        // Check if output already exists with correct size (race condition)
        if let Ok(meta) = fs::metadata(&sf.output_path) {
            if meta.len() == sf.expected_size {
                skipped_atomic.fetch_add(1, Ordering::Relaxed);
                return;
            }
            // Wrong size - remove and re-copy
            let _ = fs::remove_file(&sf.output_path);
        }

        // Verify source exists and has expected size
        let src_size = match fs::metadata(&sf.temp_path) {
            Ok(m) => m.len(),
            Err(e) => {
                let count = logged_failures.fetch_add(1, Ordering::Relaxed);
                if count < MAX_LOGGED_FAILURES {
                    error!(
                        "FAIL [{}]: staged file missing: {}: {}",
                        sf.directive_id,
                        sf.temp_path.display(),
                        e
                    );
                }
                failed_atomic.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };

        if src_size != sf.expected_size {
            let count = logged_failures.fetch_add(1, Ordering::Relaxed);
            if count < MAX_LOGGED_FAILURES {
                error!(
                    "FAIL [{}]: size mismatch: expected {} got {}",
                    sf.directive_id, sf.expected_size, src_size
                );
            }
            failed_atomic.fetch_add(1, Ordering::Relaxed);
            return;
        }

        // Always copy (not rename) to preserve temp source for other directives
        // sharing the same extracted file. Temp dir cleanup handles the originals.
        let _ = fs::remove_file(&sf.output_path);
        if let Err(e) = reflink_copy::reflink_or_copy(&sf.temp_path, &sf.output_path) {
            // Check if another thread already created it (race condition)
            if let Ok(meta) = fs::metadata(&sf.output_path) {
                if meta.len() == sf.expected_size {
                    skipped_atomic.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            }
            let count = logged_failures.fetch_add(1, Ordering::Relaxed);
            if count < MAX_LOGGED_FAILURES {
                error!(
                    "FAIL [{}]: copy failed: {}",
                    sf.directive_id, e
                );
            }
            failed_atomic.fetch_add(1, Ordering::Relaxed);
            return;
        }

        let new_count = written.fetch_add(1, Ordering::Relaxed) + 1;
        if let Some(ref callback) = progress_callback {
            callback(new_count);
        }
    });

    // Explicitly drop temp dirs after all staged files have been copied out.
    // This cleans up the extraction artifacts.
    drop(result.temp_dir);
    drop(result.nested_temp_dirs);

    FinalizeStats {
        written: written.load(Ordering::Relaxed),
        skipped: skipped_atomic.load(Ordering::Relaxed),
        failed: failed_atomic.load(Ordering::Relaxed),
    }
}

/// Process nested archive directives, returning staged files and temp dirs to keep alive.
///
/// Extracts files from nested archives (BSA, ZIP, 7z, etc.) that were themselves
/// extracted from the outer archive.
fn process_nested_bsa_directives_staged(
    nested_bsa_directives: &[(i64, &FromArchiveDirective, Option<&str>, &str)],
    extracted_map: &HashMap<String, PathBuf>,
    ctx: &ProcessContext,
    output_dir: &Path,
) -> (Vec<Result<StagedFile, String>>, Vec<tempfile::TempDir>) {
    if nested_bsa_directives.is_empty() {
        return (Vec::new(), Vec::new());
    }

    let mut kept_temp_dirs: Vec<tempfile::TempDir> = Vec::new();

    // Group by nested archive file
    let mut by_archive: HashMap<String, Vec<(i64, &FromArchiveDirective, &str)>> = HashMap::new();
    for (id, directive, resolved, file_in_archive) in nested_bsa_directives {
        let archive_path = resolved
            .or_else(|| directive.archive_hash_path.get(1).map(|s| s.as_str()))
            .unwrap_or("")
            .to_string();
        by_archive
            .entry(archive_path)
            .or_default()
            .push((*id, *directive, *file_in_archive));
    }

    let results = std::sync::Mutex::new(Vec::new());

    for (archive_path_in_outer, directives) in by_archive {
        let normalized_archive = normalize_archive_lookup_path(&archive_path_in_outer);

        let archive_disk_path = match extracted_map.get(&normalized_archive) {
            Some(p) => p.clone(),
            None => {
                for (id, _, _) in &directives {
                    results.lock().expect("results mutex").push(Err(format!(
                        "FAIL [{}]: Nested archive not found: {}",
                        id, archive_path_in_outer
                    )));
                }
                continue;
            }
        };

        let archive_type = match detect_archive_type(&archive_disk_path) {
            Ok(t) => t,
            Err(e) => {
                for (id, _, _) in &directives {
                    results.lock().expect("results mutex").push(Err(format!(
                        "FAIL [{}]: Cannot detect archive type for {}: {}",
                        id, archive_path_in_outer, e
                    )));
                }
                continue;
            }
        };

        // For non-BSA nested archives, extract all needed files in one batched call
        if matches!(
            archive_type,
            NestedArchiveType::Zip
                | NestedArchiveType::SevenZ
                | NestedArchiveType::Rar
                | NestedArchiveType::Unknown
        ) {
            let mut unique_files = Vec::new();
            let mut seen = HashSet::new();
            for (_, _, file_in_archive) in &directives {
                let normalized = normalize_archive_lookup_path(file_in_archive);
                if seen.insert(normalized) {
                    unique_files.push(file_in_archive.to_string());
                }
            }

            match tempfile::tempdir_in(output_dir) {
                Ok(nested_tmp_dir) => {
                    let batch_result = sevenzip::extract_files_case_insensitive(
                        &archive_disk_path,
                        &unique_files,
                        nested_tmp_dir.path(),
                    );
                    match batch_result {
                        Ok(_) => {
                            let nested_map =
                                build_extracted_file_map(nested_tmp_dir.path());

                            // Process each directive
                            for (id, directive, file_in_archive) in &directives {
                                let normalized_file =
                                    normalize_archive_lookup_path(file_in_archive);
                                let extracted_path = match nested_map.get(&normalized_file) {
                                    Some(p) => p,
                                    None => {
                                        results.lock().expect("results mutex").push(Err(format!(
                                            "FAIL [{}]: Nested file not found: {}",
                                            id, file_in_archive
                                        )));
                                        continue;
                                    }
                                };

                                let src_size = match fs::metadata(extracted_path) {
                                    Ok(meta) => meta.len(),
                                    Err(e) => {
                                        results.lock().expect("results mutex").push(Err(format!(
                                            "FAIL [{}]: Nested metadata error: {}",
                                            id, e
                                        )));
                                        continue;
                                    }
                                };

                                if src_size != directive.size {
                                    results.lock().expect("results mutex").push(Err(format!(
                                        "FAIL [{}]: Size mismatch: expected {} got {}",
                                        id, directive.size, src_size
                                    )));
                                    continue;
                                }

                                let final_path =
                                    paths::join_windows_path(output_dir, &directive.to);

                                // Record patch basis
                                if let Some(ahash) = directive.archive_hash_path.first() {
                                    let basis_key = build_patch_basis_key(
                                        ahash,
                                        Some(&archive_path_in_outer),
                                        Some(file_in_archive),
                                    );
                                    ctx.record_patch_basis_candidate_path_dual(
                                        &basis_key,
                                        &directive.archive_hash_path,
                                        extracted_path,
                                        &final_path,
                                        directive.size,
                                    );
                                }

                                results.lock().expect("results mutex").push(Ok(StagedFile {
                                    temp_path: extracted_path.clone(),
                                    output_path: final_path,
                                    expected_size: directive.size,
                                    directive_id: *id,
                                }));
                            }

                            // Keep temp dir alive until finalize_archive copies files out
                            kept_temp_dirs.push(nested_tmp_dir);
                        }
                        Err(e) => {
                            for (id, _, _) in &directives {
                                results.lock().expect("results mutex").push(Err(format!(
                                    "FAIL [{}]: Nested batch extract error from {:?} archive {}: {}",
                                    id, archive_type, archive_disk_path.display(), e
                                )));
                            }
                        }
                    }
                }
                Err(e) => {
                    for (id, _, _) in &directives {
                        results.lock().expect("results mutex").push(Err(format!(
                            "FAIL [{}]: Cannot create temp dir for nested extraction: {}",
                            id, e
                        )));
                    }
                }
            }
            continue;
        }

        // BSA/BA2: batch extract in parallel (open once, decompress all files at once)
        let mut wanted_paths: HashSet<String> = HashSet::new();
        let mut path_to_directives: HashMap<String, Vec<(i64, &FromArchiveDirective, &str)>> =
            HashMap::new();

        for (id, directive, file_in_archive) in &directives {
            let normalized = file_in_archive.replace('\\', "/").to_lowercase();
            wanted_paths.insert(normalized.clone());
            path_to_directives
                .entry(normalized)
                .or_default()
                .push((*id, *directive, *file_in_archive));
        }

        if let Err(e) = bsa::extract_archive_batch(
            &archive_disk_path,
            &wanted_paths,
            |path, data| {
                let lookup = path.replace('\\', "/").to_lowercase();
                let Some(directive_list) = path_to_directives.get(&lookup) else {
                    return Ok(());
                };

                for &(id, directive, file_in_archive) in directive_list {
                    if data.len() as u64 != directive.size {
                        results.lock().expect("results mutex").push(Err(format!(
                            "FAIL [{}]: Size mismatch: expected {} got {}",
                            id, directive.size, data.len()
                        )));
                        continue;
                    }

                    let out_path = paths::join_windows_path(output_dir, &directive.to);
                    if let Err(_e) = paths::ensure_parent_dirs(&out_path) {
                        results.lock().expect("results mutex").push(Err(format!(
                            "FAIL [{}]: Cannot create parent dirs",
                            id
                        )));
                        continue;
                    }

                    if let Err(e) = fs::write(&out_path, &data) {
                        results.lock().expect("results mutex").push(Err(format!("FAIL [{}]: write error: {}", id, e)));
                        continue;
                    }

                    if let Some(ahash) = directive.archive_hash_path.first() {
                        let basis_key = build_patch_basis_key(
                            ahash,
                            Some(&archive_path_in_outer),
                            Some(file_in_archive),
                        );
                        ctx.record_patch_basis_candidate_bytes_dual(
                            &basis_key,
                            &directive.archive_hash_path,
                            &out_path,
                            &data,
                        );
                    }

                    results.lock().expect("results mutex").push(Ok(StagedFile {
                        temp_path: out_path.clone(),
                        output_path: out_path,
                        expected_size: directive.size,
                        directive_id: id,
                    }));
                }
                Ok(())
            },
        ) {
            // Batch extraction failed entirely — report for all directives
            for (id, _, _) in &directives {
                results.lock().expect("results mutex").push(Err(format!(
                    "FAIL [{}]: Nested BSA batch extract error from {:?}: {}",
                    id, archive_type, e
                )));
            }
        }
    }

    (results.into_inner().expect("results mutex"), kept_temp_dirs)
}

fn normalize_archive_lookup_path(path: &str) -> String {
    let base = path.split('#').next().unwrap_or(path);
    paths::normalize_for_lookup(base)
}

/// Truncate a name to max_len characters, adding "..." if needed.
fn truncate_name(name: &str, max_len: usize) -> String {
    if name.len() <= max_len {
        name.to_string()
    } else {
        format!("{}...", &name[..max_len.saturating_sub(3)])
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

        let archive_size = match fs::metadata(archive_path) {
            Ok(m) => m.len(),
            Err(_) => {
                failed.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };

        if archive_size != directive.size {
            failed.fetch_add(1, Ordering::Relaxed);
            error!(
                "FAIL [{}]: whole-file size mismatch {} vs {}",
                id, archive_size, directive.size
            );
            return;
        }

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
        let basis_key = build_patch_basis_key(archive_hash, None, None);
        ctx.record_patch_basis_candidate_path_dual(
            &basis_key,
            &directive.archive_hash_path,
            archive_path,
            &output_path,
            directive.size,
        );

        extracted.fetch_add(1, Ordering::Relaxed);
    });
}

/// Process a BSA/BA2 archive directly using batch extraction.
/// Opens the archive ONCE and decompresses all files in parallel via callback.
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

    // Build lookup: normalized BSA path -> list of directives needing that file
    let mut path_to_directives: HashMap<String, Vec<&(i64, FromArchiveDirective, Option<String>, Option<String>)>> =
        HashMap::new();
    let mut wanted_paths: HashSet<String> = HashSet::new();

    for item in &to_process {
        let (_, directive, resolved, _) = item;
        let file_path_in_bsa = resolved
            .as_deref()
            .or_else(|| directive.archive_hash_path.get(1).map(|s| s.as_str()))
            .unwrap_or("");

        let normalized = file_path_in_bsa.replace('\\', "/").to_lowercase();
        wanted_paths.insert(normalized.clone());
        path_to_directives
            .entry(normalized)
            .or_default()
            .push(item);
    }

    // Extract all files in parallel via batch callback
    let path_to_directives = &path_to_directives;
    if let Err(e) = bsa::extract_archive_batch(archive_path, &wanted_paths, |path, data| {
        let lookup = path.replace('\\', "/").to_lowercase();
        let Some(directive_list) = path_to_directives.get(&lookup) else {
            return Ok(());
        };

        for &(id, ref directive, _, _) in directive_list {
            if data.len() as u64 != directive.size {
                let count = logged_failures.fetch_add(1, Ordering::Relaxed);
                if count < MAX_LOGGED_FAILURES {
                    error!(
                        "FAIL [{}]: BSA size mismatch: expected {} got {}",
                        id, directive.size, data.len()
                    );
                }
                failed.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            let out_path = paths::join_windows_path(output_dir, &directive.to);
            if let Err(e) = paths::ensure_parent_dirs(&out_path) {
                failed.fetch_add(1, Ordering::Relaxed);
                error!("FAIL [{}]: cannot create dirs for {}: {}", id, out_path.display(), e);
                continue;
            }

            if let Err(e) = fs::write(&out_path, &data) {
                failed.fetch_add(1, Ordering::Relaxed);
                error!("FAIL [{}]: write error: {}", id, e);
                continue;
            }

            if let Some(archive_hash) = directive.archive_hash_path.first() {
                let file_path_in_bsa = directive
                    .archive_hash_path
                    .get(1)
                    .map(|s| s.as_str())
                    .unwrap_or("");
                let basis_key =
                    build_patch_basis_key(archive_hash, Some(file_path_in_bsa), None);
                ctx.record_patch_basis_candidate_bytes_dual(
                    &basis_key,
                    &directive.archive_hash_path,
                    &out_path,
                    &data,
                );
            }

            extracted.fetch_add(1, Ordering::Relaxed);
            let new_count = written.fetch_add(1, Ordering::Relaxed) + 1;
            if let Some(ref callback) = progress_callback {
                callback(new_count);
            }
        }
        Ok(())
    }) {
        let count = logged_failures.fetch_add(1, Ordering::Relaxed);
        if count < MAX_LOGGED_FAILURES {
            error!(
                "FAIL: BSA batch extraction error for {}: {}",
                archive_path.display(),
                e
            );
        }
        failed.fetch_add(to_process.len(), Ordering::Relaxed);
    }
}

/// Build a map of all files in a directory: normalized path -> actual file path on disk.
///
/// Handles CP437-encoded filenames that 7z may extract from legacy Windows archives.
fn build_extracted_file_map(dir: &std::path::Path) -> HashMap<String, PathBuf> {
    let entries: Vec<_> = walkdir::WalkDir::new(dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .collect();

    let mut map: HashMap<String, PathBuf> = HashMap::new();

    for entry in &entries {
        let rel_path = entry.path().strip_prefix(dir).unwrap_or(entry.path());

        #[cfg(unix)]
        let path_bytes: Vec<u8> = {
            use std::os::unix::ffi::OsStrExt;
            rel_path.as_os_str().as_bytes().to_vec()
        };
        #[cfg(not(unix))]
        let path_bytes: Vec<u8> = rel_path.to_string_lossy().as_bytes().to_vec();

        let has_high_bytes = path_bytes.iter().any(|&b| b >= 0x80);

        let normalized = paths::normalize_for_lookup(&rel_path.to_string_lossy());
        map.insert(normalized, entry.path().to_path_buf());

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
    fn test_large_archive_threshold() {
        let threshold = get_large_archive_threshold();
        let one_gb = 1024 * 1024 * 1024;
        let two_gb = 2048 * 1024 * 1024;
        assert!(
            threshold == one_gb || threshold == two_gb,
            "Threshold should be 1GB or 2GB, got {}",
            threshold
        );
    }

    #[test]
    fn test_threshold_is_cached() {
        let first = get_large_archive_threshold();
        let second = get_large_archive_threshold();
        assert_eq!(first, second, "Threshold should be cached and consistent");
    }

}
