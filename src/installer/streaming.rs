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
//! Archives are processed in parallel via rayon's work-stealing pool.
//! BSA/BA2 direct-reads use rayon internally for parallel decompression.
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
use crate::modlist::{
    Directive, FromArchiveDirective, ModlistDb, PatchedFromArchiveDirective,
    TransformedTextureDirective,
};
use crate::paths;

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

impl ArchiveDirective {
    fn id(&self) -> i64 {
        match self {
            Self::FromArchive { id, .. } | Self::Patched { id, .. } => *id,
        }
    }
}

/// Statistics from finalization of one archive.
struct FinalizeStats {
    written: usize,
    skipped: usize,
    failed: usize,
}


/// A DDS texture job sent from extraction threads to the DDS handler thread.
/// During extraction, `data` holds the raw bytes. The collector thread spills
/// them to temp files on disk so they don't accumulate in RAM.
struct DdsJob {
    /// Directive ID
    id: i64,
    /// The directive with format/dimension info
    directive: TransformedTextureDirective,
    /// Raw source texture data (DDS) — present when sent through channel
    data: Vec<u8>,
}

/// A DDS job with data spilled to a temp file on disk.
/// Keeps only metadata in RAM (~100 bytes vs megabytes per texture).
struct SpilledDdsJob {
    id: i64,
    directive: TransformedTextureDirective,
    /// Path to temp file containing the raw DDS data
    data_path: PathBuf,
}

impl SpilledDdsJob {
    /// Read the texture data back from disk.
    fn read_data(&self) -> std::io::Result<Vec<u8>> {
        fs::read(&self.data_path)
    }
}


/// Configuration for fused extraction/patch scheduling.
#[derive(Debug, Clone, Default)]
pub struct StreamingConfig {
    /// Max worker threads for non-BSA archive extraction.
    /// `None` => auto-detect from CPU count.
    pub max_extract_workers: Option<usize>,
    /// Max number of 7z archives processed concurrently.
    /// `None` => default 1.
    pub max_parallel_7z_archives: Option<usize>,
    /// Max number of BSA/BA2 archives processed concurrently.
    /// Each BSA extraction uses rayon internally, so low values are usually better.
    /// `None` => default 1.
    pub max_parallel_bsa_archives: Option<usize>,
}

/// Statistics from the extraction pipeline.
#[derive(Debug, Default)]
pub struct StreamingStats {
    pub extracted: usize,
    pub written: usize,
    pub skipped: usize,
    pub failed: usize,
    /// Archive hashes that had extraction/finalization failures.
    /// Re-verified after extraction; corrupted ones are marked for re-download.
    #[allow(dead_code)] // Available for callers to inspect
    pub failed_archive_hashes: Vec<String>,
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
            if name_str.starts_with(".tmp") && entry.path().is_dir()
                && fs::remove_dir_all(entry.path()).is_ok() {
                    cleaned += 1;
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
    config: StreamingConfig,
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

    // === Load TransformedTexture directives for inline DDS processing ===
    // Add texture-only archives to by_archive so they get extracted during install phase.
    // Without this, archives referenced only by TransformedTexture are never extracted.
    // Build lookup: (archive_hash, normalized_source_path) → Vec<(id, directive)>
    // During extraction, when we encounter these source files, we feed them to
    // a dedicated DDS handler thread instead of waiting for a separate phase.
    let texture_raw = db.get_all_pending_directives_of_type("TransformedTexture")?;
    let mut texture_by_archive: HashMap<String, Vec<(i64, TransformedTextureDirective)>> =
        HashMap::new();
    for (id, json) in texture_raw {
        if let Ok(Directive::TransformedTexture(d)) = serde_json::from_str::<Directive>(&json) {
            // Skip textures whose output already exists
            let output_path = paths::join_windows_path(&ctx.config.output_dir, &d.to);
            if output_path.exists() {
                continue;
            }
            if let Some(hash) = d.archive_hash_path.first() {
                texture_by_archive
                    .entry(hash.clone())
                    .or_default()
                    .push((id, d));
            }
        }
    }
    let texture_count: usize = texture_by_archive.values().map(|v| v.len()).sum();

    // Ensure archives that ONLY have TransformedTexture directives are included in
    // the extraction pass. Without this, these archives are never extracted and the
    // texture source files can't be captured during install phase.
    let mut tex_only_count = 0usize;
    for archive_hash in texture_by_archive.keys() {
        if !by_archive.contains_key(archive_hash) {
            by_archive.entry(archive_hash.clone()).or_default();
            tex_only_count += 1;
        }
    }
    if tex_only_count > 0 {
        eprintln!(
            "Added {} texture-only archives to extraction pass",
            tex_only_count
        );
    }

    // Build per-archive texture lookups:
    // - depth2_lookup: archive_hash → { source_path → directives } (direct files in archive)
    // - depth3_lookup: archive_hash → { nested_bsa_path → { file_in_bsa → directives } }
    type TextureLookup = HashMap<String, Vec<(i64, TransformedTextureDirective)>>;
    type NestedTextureLookup = HashMap<String, TextureLookup>; // bsa_name → { file_path → directives }
    let texture_depth2: Arc<HashMap<String, TextureLookup>> = {
        let mut by_hash: HashMap<String, TextureLookup> = HashMap::new();
        for (hash, directives) in &texture_by_archive {
            let lookup = by_hash.entry(hash.clone()).or_default();
            for (id, d) in directives {
                if d.archive_hash_path.len() == 2 {
                    let source = d.archive_hash_path[1].replace('\\', "/").to_lowercase();
                    lookup.entry(source).or_default().push((*id, d.clone()));
                }
            }
        }
        Arc::new(by_hash)
    };
    let texture_depth3: Arc<HashMap<String, NestedTextureLookup>> = {
        let mut by_hash: HashMap<String, NestedTextureLookup> = HashMap::new();
        for (hash, directives) in &texture_by_archive {
            for (id, d) in directives {
                if d.archive_hash_path.len() >= 3 {
                    let bsa_name = d.archive_hash_path[1].replace('\\', "/").to_lowercase();
                    let file_in_bsa = d.archive_hash_path[2].replace('\\', "/").to_lowercase();
                    by_hash
                        .entry(hash.clone())
                        .or_default()
                        .entry(bsa_name)
                        .or_default()
                        .entry(file_in_bsa)
                        .or_default()
                        .push((*id, d.clone()));
                }
            }
        }
        Arc::new(by_hash)
    };

    // DDS processing channel — extraction threads send texture data here,
    // dedicated DDS handler thread processes them concurrently with extraction.
    let (dds_tx, dds_rx) = std::sync::mpsc::sync_channel::<DdsJob>(32);

    if texture_count > 0 {
        let d2_count: usize = texture_depth2.values().map(|v| v.values().map(|d| d.len()).sum::<usize>()).sum();
        let d3_count: usize = texture_depth3.values()
            .map(|bsas| bsas.values().map(|files| files.values().map(|d| d.len()).sum::<usize>()).sum::<usize>()).sum();
        let d2_archives: usize = texture_depth2.values().filter(|v| !v.is_empty()).count();
        let d3_archives: usize = texture_depth3.values().filter(|v| !v.is_empty()).count();

        eprintln!(
            "DDS inline: {} textures ({} depth-2 in {} archives, {} depth-3 in {} archives)",
            texture_count, d2_count, d2_archives, d3_count, d3_archives
        );
    }

    // Global counter for GUI progress — shared across all archives
    let global_written = Arc::new(AtomicUsize::new(0));

    // Create a wrapper callback that uses global counter + pre_skipped offset
    let adjusted_callback: Option<ProgressCallback> = progress_callback.map(|cb| {
        let offset = pre_skipped;
        let counter = global_written.clone();
        Arc::new(move |_per_archive_count: usize| {
            let total = counter.fetch_add(1, Ordering::Relaxed) + 1;
            cb(offset + total);
        }) as ProgressCallback
    });

    // Classify archives by type
    let mut archives: Vec<_> = by_archive.into_iter().collect();
    // Match Wabbajack's effective behavior: stable progression in directive insertion order.
    archives.sort_by_key(|(_, directives)| directives.iter().map(ArchiveDirective::id).min().unwrap_or(i64::MAX));
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

        // BSA/BA2 → fast direct-read path (reads files by name, no temp extraction)
        // Works for both pure FromArchive and mixed FromArchive+Patched directives.
        if matches!(
            archive_type,
            NestedArchiveType::Tes3Bsa | NestedArchiveType::Bsa | NestedArchiveType::Ba2
        ) {
            bsa_only_count += 1;
            all_archives.push((archive_hash, directives, archive_path, ArchiveKind::BsaDirect));
        } else {
            extract_count += 1;
            all_archives.push((archive_hash, directives, archive_path, ArchiveKind::Extract));
        }
    }

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
    let failed_archive_hashes: Arc<std::sync::Mutex<HashSet<String>>> =
        Arc::new(std::sync::Mutex::new(HashSet::new()));

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

    // Split into BSA-direct (cheap) and extract (expensive).
    let (bsa_archives, extract_archives): (Vec<_>, Vec<_>) = all_archives
        .into_iter()
        .partition(|(_, _, _, kind)| matches!(kind, ArchiveKind::BsaDirect));

    // Split extract archives into 7z and non-7z so 7z process concurrency can be capped.
    let (sevenzip_archives, other_extract_archives): (Vec<_>, Vec<_>) = extract_archives
        .into_iter()
        .partition(|(_, _, path, _)| {
            detect_archive_type(path)
                .map(|t| matches!(t, NestedArchiveType::SevenZ))
                .unwrap_or(false)
        });

    let extract_workers = config.max_extract_workers.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|n| n.get().max(2))
            .unwrap_or(4)
    });
    let sevenzip_archive_workers = config
        .max_parallel_7z_archives
        .unwrap_or(extract_workers)
        .max(1);
    let bsa_archive_workers = config.max_parallel_bsa_archives.unwrap_or(1).max(1);

    // Non-7z extraction pool.
    let extract_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(extract_workers)
        .thread_name(|i| format!("extract-{}", i))
        .build()
        .expect("Failed to build extraction thread pool");

    // 7z extraction pool (external 7z processes). Keep concurrency low to control memory.
    let sevenzip_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(sevenzip_archive_workers)
        .thread_name(|i| format!("extract-7z-{}", i))
        .build()
        .expect("Failed to build 7z extraction thread pool");

    let bsa_archive_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(bsa_archive_workers)
        .thread_name(|i| format!("bsa-archive-{}", i))
        .build()
        .expect("Failed to build BSA archive pool");

    if !other_extract_archives.is_empty() {
        eprintln!(
            "  Extract queue (non-7z): {} archives ({} concurrent workers)",
            other_extract_archives.len(),
            extract_workers
        );
    }
    if !sevenzip_archives.is_empty() {
        eprintln!(
            "  Extract queue (7z): {} archives ({} concurrent workers)",
            sevenzip_archives.len(),
            sevenzip_archive_workers
        );
    }
    if !bsa_archives.is_empty() {
        eprintln!(
            "  BSA queue: {} archives ({} concurrent archive workers)",
            bsa_archives.len(),
            bsa_archive_workers
        );
    }

    // Collect DDS jobs during extraction by spilling texture data to temp files.
    // Only metadata is kept in RAM (~100 bytes each). Data is read back when processing.
    let dds_spill_dir = ctx.config.output_dir.join(".clf3_dds_spill");
    let _ = fs::create_dir_all(&dds_spill_dir);
    let collected_dds_jobs: Arc<std::sync::Mutex<Vec<SpilledDdsJob>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));

    // Process BSA-direct archives in main rayon pool (unlimited, they're fast)
    // and extract archives in bounded pool — concurrently via std::thread::scope.
    std::thread::scope(|thread_scope| {
        // Spawn collector thread — receives DDS jobs and spills data to disk
        let collected_jobs = collected_dds_jobs.clone();
        let spill_dir = dds_spill_dir.clone();
        let collector_handle = if texture_count > 0 {
            let rx = dds_rx;
            Some(thread_scope.spawn(move || {
                let mut jobs = Vec::new();
                let mut idx = 0u64;
                while let Ok(job) = rx.recv() {
                    // Write data to temp file, keep only path in memory
                    let data_path = spill_dir.join(format!("dds_{}.tmp", idx));
                    if let Err(e) = fs::write(&data_path, &job.data) {
                        warn!("DDS spill write failed: {}", e);
                        continue;
                    }
                    jobs.push(SpilledDdsJob {
                        id: job.id,
                        directive: job.directive,
                        data_path,
                    });
                    idx += 1;
                }
                let count = jobs.len();
                collected_jobs.lock().expect("dds jobs lock").extend(jobs);
                count
            }))
        } else {
            drop(dds_rx);
            None
        };

        // Clone sender for BSA and extract threads; original dropped after both finish
        // to signal DDS handler to stop.
        let bsa_dds_tx = dds_tx.clone();
        let extract_dds_tx = dds_tx.clone();
        drop(dds_tx); // Only clones remain — when both threads finish, channel closes

        let bsa_tex_d2 = texture_depth2.clone();
        let bsa_tex_d3 = texture_depth3.clone();
        let ext_tex_d2 = texture_depth2.clone();
        let ext_tex_d3 = texture_depth3.clone();

        // BSA-direct: spawn into main rayon pool
        let bsa_handle = {
            let extracted = extracted.clone();
            let written = written.clone();
            let skipped = skipped.clone();
            let failed = failed.clone();
            let logged_failures = logged_failures.clone();
            let completed_archives = completed_archives.clone();
            let overall_pb = overall_pb.clone();
            let mp = mp.clone();
            let archive_bar_style = archive_bar_style.clone();
            let adjusted_callback = adjusted_callback.clone();
            thread_scope.spawn(move || {
                let dds_tx = bsa_dds_tx;
                let tex_d2 = bsa_tex_d2;
                let tex_d3 = bsa_tex_d3;
                bsa_archive_pool.scope(|s| {
                    for (archive_hash, directives, archive_path, _kind) in &bsa_archives {
                        let extracted = &extracted;
                        let written = &written;
                        let skipped = &skipped;
                        let failed = &failed;
                        let logged_failures = &logged_failures;
                        let completed_archives = &completed_archives;
                        let overall_pb = &overall_pb;
                        let archive_bar_style = &archive_bar_style;
                        let mp = &mp;
                        let adjusted_callback = &adjusted_callback;
                        let dds_tx = &dds_tx;
                        let tex_d2 = &tex_d2;
                        let _tex_d3 = &tex_d3; // BSA-direct: no nested BSAs

                    s.spawn(move |_| {
                        let archive_name = archive_path
                            .file_name()
                            .map(|n| n.to_string_lossy().to_string())
                            .unwrap_or_else(|| archive_hash.clone());
                        let display_name = truncate_name(&archive_name, 50);

                        let archive_pb = mp.insert_before(overall_pb, ProgressBar::new_spinner());
                        archive_pb.set_style(archive_bar_style.clone());
                        archive_pb.enable_steady_tick(std::time::Duration::from_millis(100));
                        archive_pb.set_message(format!(
                            "{} ({} files) reading...",
                            display_name,
                            directives.len(),
                        ));

                        // BSA/BA2 direct-read path — handles both FromArchive and Patched
                        let bsa_from: Vec<(
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

                        let bsa_patched: Vec<(i64, &PatchedFromArchiveDirective)> = directives
                            .iter()
                            .filter_map(|d| match d {
                                ArchiveDirective::Patched { id, directive } => {
                                    Some((*id, directive))
                                }
                                _ => None,
                            })
                            .collect();

                        if !bsa_from.is_empty() {
                            process_bsa_archive(
                                archive_path,
                                &bsa_from,
                                ctx,
                                extracted,
                                written,
                                skipped,
                                failed,
                                logged_failures,
                                adjusted_callback.clone(),
                            );
                        }

                        if !bsa_patched.is_empty() {
                            process_bsa_patched_directives(
                                archive_path,
                                archive_hash,
                                &bsa_patched,
                                ctx,
                                extracted,
                                written,
                                skipped,
                                failed,
                                logged_failures,
                                adjusted_callback.clone(),
                            );
                        }

                        // Extract texture source files from this BSA and send to DDS handler
                        if let Some(d2) = tex_d2.get(archive_hash) {
                            if !d2.is_empty() {
                                extract_textures_from_bsa(
                                    archive_path, d2, dds_tx,
                                );
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
                    });
                }
            });
        })
        };

        // Shared closure for processing a single archive in any pool.
        // Returns a function that can be called from rayon::scope spawns.
        let process_archive = |archive_hash: &String,
                               directives: &Vec<ArchiveDirective>,
                               archive_path: &PathBuf,
                               extracted: &AtomicUsize,
                               written: &AtomicUsize,
                               skipped: &AtomicUsize,
                               failed: &AtomicUsize,
                               logged_failures: &Arc<AtomicUsize>,
                               completed_archives: &AtomicUsize,
                               overall_pb: &ProgressBar,
                               archive_bar_style: &ProgressStyle,
                               mp: &MultiProgress,
                               adjusted_callback: &Option<ProgressCallback>,
                               dds_tx: &std::sync::mpsc::SyncSender<DdsJob>,
                               tex_d2: &HashMap<String, TextureLookupInner>,
                               tex_d3: &HashMap<String, NestedTextureLookupInner>,
                               failed_archive_hashes: &std::sync::Mutex<HashSet<String>>| {
            let archive_name = archive_path
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_else(|| archive_hash.clone());
            let display_name = truncate_name(&archive_name, 50);

            let archive_pb = mp.insert_before(overall_pb, ProgressBar::new_spinner());
            archive_pb.set_style(archive_bar_style.clone());
            archive_pb.enable_steady_tick(std::time::Duration::from_millis(100));
            archive_pb.set_message(format!(
                "{} ({} files) extracting...",
                display_name,
                directives.len(),
            ));

            let mut extra_paths: Vec<String> = Vec::new();
            if let Some(d2) = tex_d2.get(archive_hash) {
                for source_path in d2.keys() {
                    extra_paths.push(source_path.clone());
                }
            }
            if let Some(d3) = tex_d3.get(archive_hash) {
                for bsa_name in d3.keys() {
                    extra_paths.push(bsa_name.clone());
                }
            }

            let result = process_single_archive_fused(
                archive_path,
                archive_hash,
                directives,
                ctx,
                Some(1),
                &extra_paths,
            );

            match result {
                Ok(archive_result) => {
                    extracted.fetch_add(
                        archive_result.extracted_count + archive_result.patched_count,
                        Ordering::Relaxed,
                    );
                    skipped.fetch_add(archive_result.skipped_count, Ordering::Relaxed);
                    failed.fetch_add(archive_result.failed_count, Ordering::Relaxed);

                    if archive_result.failed_count > 0 {
                        failed_archive_hashes
                            .lock().expect("failed hashes lock")
                            .insert(archive_hash.clone());
                    }

                    if let Some(d2) = tex_d2.get(archive_hash) {
                        if !d2.is_empty() {
                            extract_textures_from_temp_dir(
                                archive_result.temp_dir.path(),
                                d2,
                                dds_tx,
                            );
                        }
                    }
                    if let Some(d3) = tex_d3.get(archive_hash) {
                        if !d3.is_empty() {
                            extract_textures_from_nested_bsas(
                                archive_result.temp_dir.path(),
                                d3,
                                dds_tx,
                            );
                        }
                    }

                    archive_pb.set_message(format!(
                        "{} ({} files) finalizing...",
                        display_name,
                        archive_result.staged_files.len()
                    ));

                    let fin_stats = finalize_archive(
                        archive_result,
                        &ctx.config.output_dir,
                        logged_failures,
                        adjusted_callback,
                    );
                    written.fetch_add(fin_stats.written, Ordering::Relaxed);
                    skipped.fetch_add(fin_stats.skipped, Ordering::Relaxed);
                    failed.fetch_add(fin_stats.failed, Ordering::Relaxed);

                    if fin_stats.failed > 0 {
                        failed_archive_hashes
                            .lock().expect("failed hashes lock")
                            .insert(archive_hash.clone());
                    }
                }
                Err(e) => {
                    let count = logged_failures.fetch_add(1, Ordering::Relaxed);
                    if count < MAX_LOGGED_FAILURES {
                        error!("FAIL: Archive {}: {:#}", archive_name, e);
                    }
                    failed.fetch_add(directives.len(), Ordering::Relaxed);
                    failed_archive_hashes
                        .lock().expect("failed hashes lock")
                        .insert(archive_hash.clone());
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
        };

        // 7z archives: dedicated capped pool.
        let sevenzip_handle = {
            let extracted = extracted.clone();
            let written = written.clone();
            let skipped = skipped.clone();
            let failed = failed.clone();
            let logged_failures = logged_failures.clone();
            let completed_archives = completed_archives.clone();
            let overall_pb = overall_pb.clone();
            let mp = mp.clone();
            let archive_bar_style = archive_bar_style.clone();
            let adjusted_callback = adjusted_callback.clone();
            let failed_hashes = failed_archive_hashes.clone();
            let dds_tx = extract_dds_tx.clone();
            let tex_d2 = ext_tex_d2.clone();
            let tex_d3 = ext_tex_d3.clone();
            thread_scope.spawn(move || {
                sevenzip_pool.scope(|s| {
                    for (archive_hash, directives, archive_path, _kind) in &sevenzip_archives {
                        let extracted = &extracted;
                        let written = &written;
                        let skipped = &skipped;
                        let failed = &failed;
                        let logged_failures = &logged_failures;
                        let completed_archives = &completed_archives;
                        let overall_pb = &overall_pb;
                        let archive_bar_style = &archive_bar_style;
                        let mp = &mp;
                        let adjusted_callback = &adjusted_callback;
                        let dds_tx = &dds_tx;
                        let tex_d2 = &tex_d2;
                        let tex_d3 = &tex_d3;
                        let failed_hashes = &failed_hashes;

                        s.spawn(move |_| {
                            process_archive(
                                archive_hash, directives, archive_path,
                                extracted, written, skipped, failed,
                                logged_failures, completed_archives,
                                overall_pb, archive_bar_style, mp,
                                adjusted_callback, dds_tx, tex_d2, tex_d3,
                                failed_hashes,
                            );
                        });
                    }
                });
            })
        };

        // Non-7z extract archives.
        let extract_handle = {
            let extracted = extracted.clone();
            let written = written.clone();
            let skipped = skipped.clone();
            let failed = failed.clone();
            let logged_failures = logged_failures.clone();
            let completed_archives = completed_archives.clone();
            let overall_pb = overall_pb.clone();
            let mp = mp.clone();
            let archive_bar_style = archive_bar_style.clone();
            let adjusted_callback = adjusted_callback.clone();
            let failed_hashes = failed_archive_hashes.clone();
            thread_scope.spawn(move || {
                let dds_tx = extract_dds_tx;
                let tex_d2 = ext_tex_d2;
                let tex_d3 = ext_tex_d3;
                extract_pool.scope(|s| {
                    for (archive_hash, directives, archive_path, _kind) in &other_extract_archives {
                        let extracted = &extracted;
                        let written = &written;
                        let skipped = &skipped;
                        let failed = &failed;
                        let logged_failures = &logged_failures;
                        let completed_archives = &completed_archives;
                        let overall_pb = &overall_pb;
                        let archive_bar_style = &archive_bar_style;
                        let mp = &mp;
                        let adjusted_callback = &adjusted_callback;
                        let dds_tx = &dds_tx;
                        let tex_d2 = &tex_d2;
                        let tex_d3 = &tex_d3;
                        let failed_hashes = &failed_hashes;

                        s.spawn(move |_| {
                            process_archive(
                                archive_hash, directives, archive_path,
                                extracted, written, skipped, failed,
                                logged_failures, completed_archives,
                                overall_pb, archive_bar_style, mp,
                                adjusted_callback, dds_tx, tex_d2, tex_d3,
                                failed_hashes,
                            );
                        });
                    }
                });
            })
        };

        bsa_handle.join().expect("BSA processing thread panicked");
        sevenzip_handle.join().expect("7z extract processing thread panicked");
        extract_handle.join().expect("Extract processing thread panicked");
        // All dds_tx senders (bsa + extract) are now dropped,
        // so the channel is closed and the collector will finish draining.
        let collected_count = if let Some(handle) = collector_handle {
            handle.join().expect("DDS collector thread panicked")
        } else {
            0
        };
        if collected_count > 0 {
            eprintln!("Collected {} texture jobs during extraction, processing in parallel...", collected_count);
        }
    });

    overall_pb.finish_and_clear();

    // Process collected DDS textures in parallel using rayon (all CPU cores).
    // Data was spilled to temp files during extraction — read back on demand.
    let dds_jobs = std::mem::take(&mut *collected_dds_jobs.lock().expect("dds jobs lock"));
    if !dds_jobs.is_empty() {
        use crate::textures::{OutputFormat, TextureJob, process_texture_batch, process_texture_with_fallback, init_gpu};
        use crate::installer::handlers::texture::is_fallback_mode;

        let total_tex = dds_jobs.len();

        // Group by format
        let mut by_format: HashMap<String, Vec<SpilledDdsJob>> = HashMap::new();
        for job in dds_jobs {
            let fmt_str = job.directive.image_state.format.to_uppercase();
            by_format.entry(fmt_str).or_default().push(job);
        }

        // Print format breakdown
        let mut format_summary: Vec<_> = by_format.iter().map(|(f, j)| (f.clone(), j.len())).collect();
        format_summary.sort_by(|a, b| b.1.cmp(&a.1));
        let summary_str = format_summary.iter()
            .map(|(f, n)| format!("{}: {}", f, n))
            .collect::<Vec<_>>()
            .join(", ");
        eprintln!("Processing {} textures in parallel [{}]", total_tex, summary_str);

        // Initialize GPU for BC7
        let _ = init_gpu();

        let dds_ok = AtomicUsize::new(0);
        let dds_fail = AtomicUsize::new(0);

        // Process each format group. BC7 uses GPU batch pipeline, everything else uses CPU par_iter.
        for (fmt_str, jobs) in by_format {
            let fmt = OutputFormat::from_str(&fmt_str)
                .unwrap_or(if is_fallback_mode() { OutputFormat::BC1 } else { OutputFormat::BC7 });

            if fmt == OutputFormat::BC7 {
                // BC7: parallel CPU prep → GPU batch encode
                eprintln!("  BC7: {} textures (GPU+CPU pipeline)...", jobs.len());

                let tex_jobs: Vec<TextureJob> = jobs.iter().filter_map(|j| {
                    j.read_data().ok().map(|data| TextureJob {
                        data,
                        width: j.directive.image_state.width,
                        height: j.directive.image_state.height,
                        format: OutputFormat::BC7,
                        id: Some(format!("{}", j.id)),
                    })
                }).collect();

                let results = process_texture_batch(tex_jobs);

                for (job, (_job_id, result)) in jobs.iter().zip(results.into_iter()) {
                    match result {
                        Ok(processed) => {
                            let out = paths::join_windows_path(&ctx.config.output_dir, &job.directive.to);
                            if paths::ensure_parent_dirs(&out).is_ok() && fs::write(&out, &processed.data).is_ok() {
                                dds_ok.fetch_add(1, Ordering::Relaxed);
                                ctx.textures_processed_during_install.lock().expect("tex lock").insert(job.id);
                            } else {
                                dds_fail.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Err(e) => {
                            dds_fail.fetch_add(1, Ordering::Relaxed);
                            warn!("DDS BC7 fail [{}]: {:#}", job.id, e);
                        }
                    }
                }
            } else {
                // Non-BC7: full CPU parallelism (like Radium)
                eprintln!("  {}: {} textures (CPU parallel)...", fmt.name(), jobs.len());

                jobs.par_iter().for_each(|job| {
                    let result: anyhow::Result<()> = (|| {
                        let data = job.read_data()?;
                        let (tex, _) = process_texture_with_fallback(
                            &data, job.directive.image_state.width,
                            job.directive.image_state.height, fmt,
                        )?;
                        let out = paths::join_windows_path(&ctx.config.output_dir, &job.directive.to);
                        paths::ensure_parent_dirs(&out)?;
                        fs::write(&out, &tex.data)?;
                        Ok(())
                    })();
                    match result {
                        Ok(()) => {
                            dds_ok.fetch_add(1, Ordering::Relaxed);
                            ctx.textures_processed_during_install.lock().expect("tex lock").insert(job.id);
                        }
                        Err(e) => {
                            dds_fail.fetch_add(1, Ordering::Relaxed);
                            warn!("DDS {} fail [{}]: {}", fmt.name(), job.id, e);
                        }
                    }
                });
            }
        }

        let ok = dds_ok.load(Ordering::Relaxed);
        let fail = dds_fail.load(Ordering::Relaxed);
        eprintln!("DDS processing complete: {} OK, {} failed", ok, fail);
    }

    // Clean up DDS spill directory
    if let Err(e) = fs::remove_dir_all(&dds_spill_dir) {
        debug!("DDS spill cleanup: {}", e);
    }

    // Re-verify archives that had extraction failures — detect corrupted downloads.
    let bad_hashes = failed_archive_hashes
        .lock()
        .expect("failed hashes lock")
        .clone();

    if !bad_hashes.is_empty() {
        eprintln!(
            "\n{} archive(s) had failures — re-verifying hashes...",
            bad_hashes.len()
        );
        let mut corrupted = Vec::new();
        for hash in &bad_hashes {
            if let Some(archive_path) = ctx.get_archive_path(hash) {
                match crate::hash::compute_file_hash(archive_path) {
                    Ok(actual_hash) => {
                        if actual_hash != *hash {
                            let name = archive_path
                                .file_name()
                                .map(|n| n.to_string_lossy().to_string())
                                .unwrap_or_else(|| hash.clone());
                            eprintln!(
                                "  CORRUPTED: {} (expected {}, got {})",
                                name, hash, actual_hash
                            );
                            corrupted.push(hash.clone());
                            // Reset download status so next run re-downloads
                            if let Some(archive_name) = archive_path.file_name() {
                                let name_str = archive_name.to_string_lossy();
                                if let Err(e) = db.reset_archive_download_status(&name_str) {
                                    warn!("Failed to reset download status for {}: {}", name_str, e);
                                } else {
                                    eprintln!(
                                        "  → Marked {} for re-download on next run",
                                        name_str
                                    );
                                }
                            }
                        } else {
                            // Hash matches — failures were from content issues, not corruption
                            let name = archive_path
                                .file_name()
                                .map(|n| n.to_string_lossy().to_string())
                                .unwrap_or_else(|| hash.clone());
                            eprintln!("  OK (hash valid): {} — content mismatch in modlist?", name);
                        }
                    }
                    Err(e) => {
                        eprintln!("  ERROR: could not re-hash {}: {}", hash, e);
                    }
                }
            }
        }
        if !corrupted.is_empty() {
            eprintln!(
                "\n{} corrupted archive(s) marked for re-download. Re-run to fix.",
                corrupted.len()
            );
        }
        // Only keep the hashes that had failures (corrupted or content mismatch)
        // in the returned stats for the caller to act on.
    }

    let stats = StreamingStats {
        extracted: extracted.load(Ordering::Relaxed),
        written: written.load(Ordering::Relaxed),
        skipped: skipped.load(Ordering::Relaxed) + parse_failures + pre_skipped,
        failed: failed.load(Ordering::Relaxed),
        failed_archive_hashes: bad_hashes.into_iter().collect(),
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
    extra_needed_paths: &[String],
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

    // Add extra needed paths (e.g. texture source files, nested BSAs for textures)
    for path in extra_needed_paths {
        if seen_needed.insert(normalize_archive_lookup_path(path)) {
            needed_paths.push(path.clone());
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

            if let Some(disk_path) = find_archive_in_extracted_map(&extracted_map, bsa_path) {
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

            if bsa::detect_format(bsa_disk_path).is_some() {
                // BSA/BA2: use native batch extraction
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
            } else {
                // Non-BSA container (e.g. .fomod which is a ZIP): use generic archive extraction
                let wanted_files: Vec<String> = wanted.iter().cloned().collect();
                match sevenzip::extract_files_case_insensitive(bsa_disk_path, &wanted_files, &tmp_path) {
                    Ok(_) => {
                        // Scan extracted files and register them
                        for (file_in_bsa, combined_normalized) in needed_files {
                            let normalized = file_in_bsa.replace('\\', "/").to_lowercase();
                            // Try to find the extracted file (case-insensitive scan)
                            let candidate = tmp_path.join(&normalized);
                            let found = if candidate.exists() {
                                Some(candidate)
                            } else {
                                // Walk the temp dir for a case-insensitive match
                                find_file_case_insensitive(&tmp_path, &normalized)
                            };
                            if let Some(temp_file) = found {
                                extracted_entries
                                    .lock()
                                    .expect("extracted_entries mutex")
                                    .push((combined_normalized.clone(), temp_file));
                            }
                        }
                    }
                    Err(e) => {
                        error!(
                            "FAIL: extract from nested archive {}: {}",
                            bsa_disk_path.display(),
                            e
                        );
                    }
                }
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
        for bsa_candidate in archive_lookup_candidates(bsa_path) {
            let combined_key = format!("{}/{}", bsa_candidate, file_in_bsa);
            let combined_normalized = paths::normalize_for_lookup(&combined_key);
            if let Some(path) = extracted_map.get(&combined_normalized) {
                return Ok(path.clone());
            }
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
    if needed_paths.is_empty() {
        debug!(
            "Extraction strategy: archive={}, needed_files=0, mode=full",
            archive_path.display()
        );
        return sevenzip::extract_all_with_threads(archive_path, temp_dir, threads).map(|_| ());
    }

    debug!(
        "Extraction strategy: archive={}, needed_files={}, mode=selective-first",
        archive_path.display(),
        needed_paths.len()
    );

    sevenzip::extract_files_case_insensitive(archive_path, needed_paths, temp_dir)
        .map(|_| ())
        .or_else(|selective_err| {
            warn!(
                "Selective extraction failed for {}, falling back to full: {}",
                archive_path.display(),
                selective_err
            );
            sevenzip::extract_all_with_threads(archive_path, temp_dir, threads)
                .map(|_| ())
                .map_err(|full_err| {
                    anyhow::anyhow!(
                        "selective extraction failed: {}; full fallback failed: {}",
                        selective_err,
                        full_err
                    )
                })
        })
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
        let archive_disk_path = match find_archive_in_extracted_map(extracted_map, &archive_path_in_outer) {
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
    paths::normalize_for_lookup(path)
}

fn archive_lookup_candidates(path: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();

    let normalized = normalize_archive_lookup_path(path);
    if seen.insert(normalized.clone()) {
        out.push(normalized.clone());
    }

    let trimmed = normalized
        .trim_start_matches("./")
        .trim_start_matches('/')
        .to_string();
    if seen.insert(trimmed.clone()) {
        out.push(trimmed.clone());
    }

    if let Some(stripped) = trimmed.strip_prefix("data/") {
        let stripped = stripped.to_string();
        if seen.insert(stripped.clone()) {
            out.push(stripped);
        }
    }

    out
}

fn find_archive_in_extracted_map<'a>(
    extracted_map: &'a HashMap<String, PathBuf>,
    archive_path: &str,
) -> Option<&'a PathBuf> {
    for candidate in archive_lookup_candidates(archive_path) {
        if let Some(path) = extracted_map.get(&candidate) {
            return Some(path);
        }
    }
    None
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

/// Extract texture source files from a BSA/BA2 archive and send to DDS handler channel.
/// Called during the BSA-direct extraction path.
type TextureLookupInner = HashMap<String, Vec<(i64, TransformedTextureDirective)>>;

fn extract_textures_from_bsa(
    archive_path: &Path,
    tex_lookup: &TextureLookupInner,
    dds_tx: &std::sync::mpsc::SyncSender<DdsJob>,
) {
    // Build wanted paths set from texture lookup
    let wanted: HashSet<String> = tex_lookup.keys().cloned().collect();
    if wanted.is_empty() {
        return;
    }

    let tex_lookup = tex_lookup;
    let _ = bsa::extract_archive_batch(archive_path, &wanted, |path, data| {
        let lookup = path.replace('\\', "/").to_lowercase();
        if let Some(directives) = tex_lookup.get(&lookup) {
            for (id, directive) in directives {
                // Send to DDS handler — clone data for each directive using this source
                let _ = dds_tx.send(DdsJob {
                    id: *id,
                    directive: directive.clone(),
                    data: data.clone(),
                });
            }
        }
        Ok(())
    });
}

/// Extract texture source files from an already-extracted temp directory
/// and send to DDS handler channel. Called for 7z/RAR/ZIP archives.
fn extract_textures_from_temp_dir(
    temp_dir: &Path,
    tex_lookup: &TextureLookupInner,
    dds_tx: &std::sync::mpsc::SyncSender<DdsJob>,
) {
    // Build a map of files in the temp dir
    let file_map = build_extracted_file_map(temp_dir);

    for (normalized_path, directives) in tex_lookup {
        // Try to find the file in the temp dir
        let found = file_map.get(normalized_path)
            .or_else(|| {
                // Also try backslash variant
                let bs = normalized_path.replace('/', "\\");
                file_map.get(&bs)
            });

        if let Some(file_path) = found {
            if let Ok(data) = fs::read(file_path) {
                for (id, directive) in directives {
                    let _ = dds_tx.send(DdsJob {
                        id: *id,
                        directive: directive.clone(),
                        data: data.clone(),
                    });
                }
            }
        }
    }
}

/// Extract texture source files from nested BSAs within an extracted temp directory.
/// For depth-3 paths: archive → nested BSA → file inside BSA.
/// Finds the nested BSA in the temp dir, opens it, and extracts the needed texture files.
type NestedTextureLookupInner = HashMap<String, TextureLookupInner>;

fn extract_textures_from_nested_bsas(
    temp_dir: &Path,
    nested_lookup: &NestedTextureLookupInner,
    dds_tx: &std::sync::mpsc::SyncSender<DdsJob>,
) {
    let file_map = build_extracted_file_map(temp_dir);

    for (bsa_name, tex_lookup) in nested_lookup {
        // Find the nested BSA file in the extracted temp dir
        let bsa_path = file_map.get(bsa_name)
            .or_else(|| {
                let bs = bsa_name.replace('/', "\\");
                file_map.get(&bs)
            });

        let Some(bsa_disk_path) = bsa_path else {
            debug!("Nested BSA not found in temp dir: {}", bsa_name);
            continue;
        };

        // Build wanted paths from the texture lookup
        let wanted: HashSet<String> = tex_lookup.keys().cloned().collect();
        if wanted.is_empty() {
            continue;
        }

        // Extract texture files from the nested BSA
        let tex_lookup = tex_lookup;
        let _ = bsa::extract_archive_batch(bsa_disk_path, &wanted, |path, data| {
            let lookup = path.replace('\\', "/").to_lowercase();
            if let Some(directives) = tex_lookup.get(&lookup) {
                for (id, directive) in directives {
                    let _ = dds_tx.send(DdsJob {
                        id: *id,
                        directive: directive.clone(),
                        data: data.clone(),
                    });
                }
            }
            Ok(())
        });
    }
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

/// Process PatchedFromArchive directives where the basis file is inside a BSA/BA2.
/// Reads basis directly from BSA, writes to temp, applies patch, writes final output.
/// Much faster than extracting the entire BSA via 7z.
fn process_bsa_patched_directives(
    archive_path: &PathBuf,
    _archive_hash: &str,
    directives: &[(i64, &PatchedFromArchiveDirective)],
    ctx: &ProcessContext,
    extracted: &Arc<AtomicUsize>,
    written: &Arc<AtomicUsize>,
    skipped: &Arc<AtomicUsize>,
    failed: &Arc<AtomicUsize>,
    logged_failures: &Arc<AtomicUsize>,
    progress_callback: Option<ProgressCallback>,
) {
    const MAX_LOGGED_FAILURES: usize = 100;
    let output_dir = &ctx.config.output_dir;

    // Filter already-done directives
    let to_process: Vec<_> = directives
        .iter()
        .filter(|(_, d)| !output_exists(ctx, &d.to, d.size))
        .collect();

    if to_process.is_empty() {
        skipped.fetch_add(directives.len(), Ordering::Relaxed);
        return;
    }

    // Collect needed BSA paths for batch read
    let mut wanted_paths: HashSet<String> = HashSet::new();
    let mut path_to_directives: HashMap<String, Vec<&(i64, &PatchedFromArchiveDirective)>> =
        HashMap::new();

    for item in &to_process {
        let (_, directive) = item;
        // archive_hash_path: [archive_hash, file_in_bsa] or [archive_hash, bsa_path, file_in_bsa]
        let file_path_in_bsa = if directive.archive_hash_path.len() >= 2 {
            &directive.archive_hash_path[1]
        } else {
            continue;
        };
        let normalized = file_path_in_bsa.replace('\\', "/").to_lowercase();
        wanted_paths.insert(normalized.clone());
        path_to_directives
            .entry(normalized)
            .or_default()
            .push(item);
    }

    // Read basis files from BSA in one batch, apply patches inline
    let path_to_directives = &path_to_directives;
    if let Err(e) = bsa::extract_archive_batch(archive_path, &wanted_paths, |path, data| {
        let lookup = path.replace('\\', "/").to_lowercase();
        let Some(directive_list) = path_to_directives.get(&lookup) else {
            return Ok(());
        };

        for &(id, directive) in directive_list {
            // Write basis to temp file for mmap-based patching
            let temp_dir = match tempfile::tempdir_in(output_dir) {
                Ok(d) => d,
                Err(e) => {
                    let count = logged_failures.fetch_add(1, Ordering::Relaxed);
                    if count < MAX_LOGGED_FAILURES {
                        tracing::error!("FAIL [{}]: cannot create temp dir: {}", id, e);
                    }
                    failed.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            };

            let basis_temp = temp_dir.path().join("basis");
            if let Err(e) = fs::write(&basis_temp, &data) {
                let count = logged_failures.fetch_add(1, Ordering::Relaxed);
                if count < MAX_LOGGED_FAILURES {
                    tracing::error!("FAIL [{}]: cannot write basis temp: {}", id, e);
                }
                failed.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            let final_output = paths::join_windows_path(output_dir, &directive.to);
            if let Err(e) = paths::ensure_parent_dirs(&final_output) {
                failed.fetch_add(1, Ordering::Relaxed);
                tracing::error!("FAIL [{}]: cannot create dirs: {}", id, e);
                continue;
            }

            // Apply patch: basis + delta → final output
            let patch_name = directive.patch_id.to_string();
            match apply_patch_to_temp(
                ctx,
                &basis_temp,
                &patch_name,
                None,
                &final_output,
                directive.size,
            ) {
                Ok(()) => {
                    // Record patch basis for future runs
                    if let Some(ahash) = directive.archive_hash_path.first() {
                        let file_in = directive
                            .archive_hash_path
                            .get(1)
                            .map(|s| s.as_str())
                            .unwrap_or("");
                        let basis_key =
                            build_patch_basis_key(ahash, Some(file_in), None);
                        ctx.record_patch_basis_candidate_bytes_dual(
                            &basis_key,
                            &directive.archive_hash_path,
                            &final_output,
                            &data,
                        );
                    }

                    extracted.fetch_add(1, Ordering::Relaxed);
                    let new_count = written.fetch_add(1, Ordering::Relaxed) + 1;
                    if let Some(ref callback) = progress_callback {
                        callback(new_count);
                    }
                }
                Err(e) => {
                    let count = logged_failures.fetch_add(1, Ordering::Relaxed);
                    if count < MAX_LOGGED_FAILURES {
                        tracing::error!("FAIL [{}]: patch apply failed: {}", id, e);
                    }
                    failed.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        Ok(())
    }) {
        let count = logged_failures.fetch_add(1, Ordering::Relaxed);
        if count < MAX_LOGGED_FAILURES {
            tracing::error!(
                "FAIL: BSA patch batch error for {}: {}",
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

/// Walk a directory tree to find a file matching the given relative path (case-insensitive).
fn find_file_case_insensitive(base: &Path, normalized_rel: &str) -> Option<PathBuf> {
    for entry in walkdir::WalkDir::new(base)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
    {
        if let Ok(rel) = entry.path().strip_prefix(base) {
            let rel_normalized = rel.to_string_lossy().replace('\\', "/").to_lowercase();
            if rel_normalized == normalized_rel {
                return Some(entry.path().to_path_buf());
            }
        }
    }
    None
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
