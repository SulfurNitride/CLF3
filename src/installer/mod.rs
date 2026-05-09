#![allow(dead_code)] // Used by lib crate
//! Installation orchestrator
//!
//! Coordinates the phases of modlist installation:
//! 1. Game Check    — validate game dir, detect game type
//! 2. Download      — fetch all required archives
//! 3. Validate      — verify archive sizes match expected
//! 4. Install+Patch — FromArchive + PatchedFromArchive + InlineFile + RemappedInlineFile
//! 5. DDS Transform — TransformedTexture directives
//! 6. BSA Build     — CreateBSA directives
//! 7. Cleanup       — extra files + BSA temp dirs

pub mod bsa_reuse;
pub mod config;
pub mod config_cache;
pub mod downloader;
pub mod game_preflight;
pub mod handlers;
pub mod pipeline;
pub mod prevalidation;
pub mod processor;
pub mod progress;
pub mod progress_cli;
pub mod sidecar;
pub mod streaming;

pub use config::{ExtractStrategy, InstallConfig};
#[allow(unused_imports)] // Used by lib crate (GUI)
pub use config_cache::{ConfigCache, ModlistConfig};
#[allow(unused_imports)] // NullReporter used by lib crate (GUI)
pub use progress::{
    NullReporter, Phase, ProgressEvent, ProgressHandle, ProgressMode, ProgressReporter,
    ProgressUnit, TaskId, TaskKind, TaskOutcome, TaskStage, TaskStarted, TaskUpdate,
};
pub use progress_cli::CliReporter;

use crate::modlist::{import_wabbajack_to_db, ModlistDb};
use anyhow::{bail, Context, Result};
use std::fs;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tracing::{info, warn};

/// Tracks peak RSS and logs whenever a new high-water mark is hit.
/// Thread-safe — share via Arc or &reference.
pub(crate) struct PeakRssTracker {
    /// Highest RSS seen so far, in KB.
    peak_kb: AtomicU64,
    /// Timestamp of install start, for relative timing.
    start: Instant,
}

impl PeakRssTracker {
    pub fn new() -> Self {
        Self {
            peak_kb: AtomicU64::new(0),
            start: Instant::now(),
        }
    }

    /// Sample current RSS. If it exceeds the previous peak, log and update.
    /// `label` describes what's happening (e.g. "extract Mod.7z", "build BSA").
    pub fn check(&self, label: &str, reporter: &dyn progress::ProgressReporter) {
        let rss_kb = current_rss_kb().unwrap_or(0);
        let prev_peak = self.peak_kb.fetch_max(rss_kb, Ordering::Relaxed);
        if rss_kb > prev_peak && rss_kb > prev_peak + 50_000 {
            // New peak, at least 50MB higher than previous — worth logging
            let elapsed = self.start.elapsed().as_secs_f64();
            reporter.log(&format!(
                "[RSS-PEAK] {}MB (+{}MB) at {:.1}s — {}",
                rss_kb / 1024,
                (rss_kb - prev_peak) / 1024,
                elapsed,
                label,
            ));
        }
    }

    /// Return the peak RSS seen so far, in KB.
    pub fn peak_kb(&self) -> u64 {
        self.peak_kb.load(Ordering::Relaxed)
    }
}

pub(crate) fn current_rss_kb() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if line.starts_with("VmRSS:") {
            return line
                .split_whitespace()
                .nth(1)
                .and_then(|v| v.parse::<u64>().ok());
        }
    }
    None
}

/// Memory usage stats from /proc/self/status.
struct MemoryStats {
    /// Current RSS in KB
    current_rss_kb: u64,
    /// Peak RSS (VmHWM) in KB — kernel-tracked high water mark
    peak_rss_kb: u64,
}

fn read_memory_stats() -> Option<MemoryStats> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    let mut current = 0u64;
    let mut peak = 0u64;
    for line in status.lines() {
        if line.starts_with("VmRSS:") {
            current = line.split_whitespace().nth(1)?.parse().ok()?;
        } else if line.starts_with("VmHWM:") {
            peak = line.split_whitespace().nth(1)?.parse().ok()?;
        }
    }
    Some(MemoryStats {
        current_rss_kb: current,
        peak_rss_kb: peak,
    })
}

/// Log a post-install performance summary.
fn log_install_summary(
    stats: &InstallStats,
    total_start: Instant,
    reporter: &Arc<dyn progress::ProgressReporter>,
) {
    let total_secs = total_start.elapsed().as_secs_f64();
    let total_min = total_secs / 60.0;

    reporter.log("--- Performance Summary ---");
    reporter.log(&format!(
        "Total time: {:.1}m ({:.0}s)",
        total_min, total_secs
    ));

    for (phase, duration) in &stats.phase_durations {
        if *duration >= 0.1 {
            reporter.log(&format!("  {:<30} {:>6.1}s", phase, duration));
        }
    }

    let total_directives =
        stats.directives_completed + stats.directives_skipped + stats.directives_failed;
    if total_directives > 0 && total_secs > 0.0 {
        reporter.log(&format!(
            "Directives: {} completed, {} skipped, {} failed ({:.0}/s)",
            stats.directives_completed,
            stats.directives_skipped,
            stats.directives_failed,
            stats.directives_completed as f64 / total_secs,
        ));
    }

    if stats.archives_downloaded > 0 {
        // Find the download+extract phase duration
        let extract_secs = stats
            .phase_durations
            .iter()
            .find(|(name, _)| name.contains("Download") || name.contains("Pipelined"))
            .map(|(_, d)| *d)
            .unwrap_or(total_secs);
        if extract_secs > 0.0 {
            reporter.log(&format!(
                "Archives: {} downloaded, {} skipped ({:.1} archives/min)",
                stats.archives_downloaded,
                stats.archives_skipped,
                stats.archives_downloaded as f64 / (extract_secs / 60.0),
            ));
        }
    }

    // Memory usage
    if let Some(mem) = read_memory_stats() {
        let mut mem_line = format!(
            "Memory: current {} MB, peak {} MB",
            mem.current_rss_kb / 1024,
            mem.peak_rss_kb / 1024,
        );
        // Child process peak RSS (7z binary etc.) via getrusage
        #[cfg(target_os = "linux")]
        {
            let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
            if unsafe { libc::getrusage(libc::RUSAGE_CHILDREN, &mut usage) } == 0 {
                let child_peak_kb = usage.ru_maxrss as u64; // already in KB on Linux
                if child_peak_kb > 0 {
                    mem_line.push_str(&format!(", child peak {} MB", child_peak_kb / 1024));
                }
            }
        }
        reporter.log(&mem_line);
    }
}

fn log_phase_metrics(phase: &str, started: Instant) {
    let elapsed_ms = started.elapsed().as_millis();
    let rss_kb = current_rss_kb().unwrap_or(0);
    info!(
        "Phase done: phase='{}' elapsed_ms={} rss_kb={}",
        phase, elapsed_ms, rss_kb
    );
}

#[cfg(target_os = "linux")]
fn trim_allocator_rss(reason: &str) {
    // Return free pages to the OS from both allocators:
    // - mi_collect: mimalloc (Rust allocations via #[global_allocator])
    // - malloc_trim: glibc (C library allocations: SQLite, libcurl, etc.)
    // Broadcast to all rayon threads so their thread-local mimalloc heaps get collected too.
    rayon::broadcast(|_| unsafe {
        libmimalloc_sys::mi_collect(true);
    });
    unsafe {
        libmimalloc_sys::mi_collect(true);
    }
    unsafe {
        libc::malloc_trim(0);
    }
    info!("allocator collect after {}", reason);
}

#[cfg(not(target_os = "linux"))]
fn trim_allocator_rss(_reason: &str) {
    rayon::broadcast(|_| unsafe {
        libmimalloc_sys::mi_collect(true);
    });
    unsafe {
        libmimalloc_sys::mi_collect(true);
    }
}

/// Installation statistics
#[derive(Debug, Default, Clone)]
pub struct InstallStats {
    pub archives_downloaded: usize,
    pub archives_skipped: usize,
    pub archives_failed: usize,
    pub archives_manual: usize,
    pub directives_completed: usize,
    pub directives_skipped: usize,
    pub directives_failed: usize,
    /// Details of failed downloads
    pub failed_downloads: Vec<downloader::FailedDownloadInfo>,
    /// Details of manual downloads needed
    pub manual_downloads: Vec<downloader::ManualDownloadInfo>,
    /// Phase durations in seconds
    pub phase_durations: Vec<(String, f64)>,
}

/// Main installer orchestrator
pub struct Installer {
    config: InstallConfig,
    db: ModlistDb,
}

impl Installer {
    /// Create a new installer with the given configuration
    pub fn new(config: InstallConfig) -> Result<Self> {
        // Validate config
        config.validate()?;

        // Create output and downloads directories if needed
        fs::create_dir_all(&config.output_dir).with_context(|| {
            format!(
                "Failed to create output directory: {}",
                config.output_dir.display()
            )
        })?;
        fs::create_dir_all(&config.downloads_dir).with_context(|| {
            format!(
                "Failed to create downloads directory: {}",
                config.downloads_dir.display()
            )
        })?;

        // Parse wabbajack and import to database
        config
            .reporter
            .log(&format!("Parsing: {}", config.wabbajack_path.display()));
        let db = import_wabbajack_to_db(&config.wabbajack_path, &config.db_path())?;

        // Show modlist info
        if let (Some(name), Some(version)) = (db.get_metadata("name")?, db.get_metadata("version")?)
        {
            config
                .reporter
                .log(&format!("Modlist: {} v{}", name, version));
        }

        let stats = db.get_directive_stats()?;
        config.reporter.log(&format!(
            "Directives: {} total ({} pending)\n",
            stats.total, stats.pending
        ));

        Ok(Self { config, db })
    }

    fn reporter(&self) -> &Arc<dyn ProgressReporter> {
        &self.config.reporter
    }

    /// Download all required archives
    async fn download_phase(&mut self) -> Result<downloader::DownloadStats> {
        downloader::download_archives(&self.db, &self.config).await
    }

    /// Validate downloaded archives that haven't been verified yet.
    /// Archives with download_status='completed' were already hash-verified
    /// during the download phase — skip them to avoid redundant I/O.
    fn validate_archives(&self) -> Result<Vec<(String, String)>> {
        use crate::hash::compute_file_hash;
        use rayon::prelude::*;
        use std::sync::Mutex;

        let archives = self.db.get_all_archives()?;

        // Skip archives already hash-verified in download phase
        let archives_to_check: Vec<_> = archives
            .iter()
            .filter(|a| a.download_status != "completed")
            .filter(|a| self.config.downloads_dir.join(&a.name).exists())
            .collect();

        if archives_to_check.is_empty() {
            self.reporter().log("No downloaded archives to validate");
            return Ok(Vec::new());
        }

        let reporter = self.reporter().clone();
        reporter.overall_set_total(archives_to_check.len() as u64);
        reporter.overall_set_message("Verifying archives (size + hash)...");
        let verify_status = reporter.begin_status("Verify");
        verify_status.set_count(0, archives_to_check.len());
        let verify_counter = AtomicUsize::new(0);

        let errors = Mutex::new(Vec::new());
        let downloads_dir = &self.config.downloads_dir;

        archives_to_check.par_iter().for_each(|archive| {
            let file_path = downloads_dir.join(&archive.name);

            match fs::metadata(&file_path) {
                Ok(meta) => {
                    let actual_size = meta.len();
                    let expected_size = archive.size as u64;
                    if actual_size != expected_size {
                        errors.lock().expect("errors mutex").push((
                            archive.name.clone(),
                            format!(
                                "Size mismatch: expected {} bytes, got {} ({}%)",
                                expected_size,
                                actual_size,
                                (actual_size * 100) / expected_size.max(1)
                            ),
                        ));
                        reporter.overall_inc();
                        let done = verify_counter.fetch_add(1, Ordering::Relaxed) + 1;
                        verify_status.set_count(done, archives_to_check.len());
                        return;
                    }

                    // Size OK — verify hash
                    match compute_file_hash(&file_path) {
                        Ok(actual_hash) => {
                            if actual_hash != archive.hash {
                                errors.lock().expect("errors mutex").push((
                                    archive.name.clone(),
                                    format!(
                                        "Hash mismatch: expected {}, got {}",
                                        archive.hash, actual_hash
                                    ),
                                ));
                            }
                        }
                        Err(e) => {
                            errors.lock().expect("errors mutex").push((
                                archive.name.clone(),
                                format!("Hash computation failed: {}", e),
                            ));
                        }
                    }
                }
                Err(e) => {
                    errors
                        .lock()
                        .expect("errors mutex")
                        .push((archive.name.clone(), format!("Cannot read file: {}", e)));
                }
            }

            reporter.overall_inc();
            let done = verify_counter.fetch_add(1, Ordering::Relaxed) + 1;
            verify_status.set_count(done, archives_to_check.len());
        });

        verify_status.finish();
        reporter.overall_finish();
        let errors = errors.into_inner().expect("errors mutex");
        self.reporter().log(&format!(
            "Verified {} archives ({} valid, {} corrupted)",
            archives_to_check.len(),
            archives_to_check.len() - errors.len(),
            errors.len()
        ));

        Ok(errors)
    }

    /// Run pipelined installation: download and extract in parallel.
    ///
    /// Instead of downloading all archives first, this processes each archive
    /// as soon as it finishes downloading. Texture and BSA phases still run
    /// sequentially after all extraction completes (Phase 1 MVP).
    pub async fn run_pipelined(&mut self) -> Result<InstallStats> {
        let mut stats = InstallStats::default();
        let total_start = Instant::now();

        // === Phase 1: Game Check ===
        let game_check_start = Instant::now();
        self.reporter().phase_start(Phase::GameCheck);
        self.reporter().log(&format!(
            "Game directory: {}",
            self.config.game_dir.display()
        ));
        if !self.config.game_dir.exists() {
            bail!(
                "Game directory does not exist: {}",
                self.config.game_dir.display()
            );
        }
        self.reporter().log("Game directory validated");

        // Hash every GameFileSource archive against the chosen game directory
        // BEFORE spending bandwidth. Catches: game updated since modlist
        // authored, missing DLC, wrong store variant (Steam vs GOG), etc.
        //
        // Runs parallel via rayon; typical cost <2s for Bethesda modlists.
        let preflight = game_preflight::check_game_files_from_db(&self.db, &self.config.game_dir)?;
        if preflight.total == 0 {
            self.reporter()
                .log("No game files required by this modlist — skipping hash preflight");
        } else {
            self.reporter()
                .log(&format!("Verifying {} game files...", preflight.total));
            if preflight.all_ok() {
                self.reporter()
                    .log(&format!("All {} game files verified", preflight.total));
            } else {
                // Dump per-file diagnostics so user knows which files are bad.
                for line in preflight.format_summary().lines() {
                    self.reporter().log(line);
                }
                bail!(
                    "Game file preflight failed: {} missing, {} hash mismatch. \
                     Game likely updated or wrong store version — no downloads started. \
                     Fix game files and re-run.",
                    preflight.missing().len(),
                    preflight.mismatched().len()
                );
            }
        }

        log_phase_metrics("Game Check", game_check_start);

        // === Phase 2: Pipelined Download + Extract ===
        let pipeline_start = Instant::now();
        self.reporter().phase_start(Phase::Downloading);

        // Create the directive processor early (needs DB + config)
        let mut dp = processor::DirectiveProcessor::new(&self.db, &self.config)?;

        // Pre-validation: classify all directives as valid/needs-work
        let prevalidation_result = prevalidation::run_prevalidation(
            &self.db,
            &dp.ctx.existing_files,
            &self.config.output_dir,
            &self.config.reporter,
        )?;
        prevalidation_result.log_summary(&self.config.reporter);
        dp.ctx.prevalidation_stats = prevalidation_result.stats_as_tuples();
        dp.ctx.extra_files_for_cleanup = prevalidation_result.extra_files;
        dp.ctx.skip_set = prevalidation_result.skip_set;

        // Pre-load and group all directives by archive hash (no index needed yet)
        let grouped = pipeline::load_and_group_directives(&self.db, &dp.ctx)?;
        let total_from = grouped
            .from_archive
            .values()
            .map(|v| v.len())
            .sum::<usize>();
        let total_patched = grouped.patched.values().map(|v| v.len()).sum::<usize>();
        let total_textures = grouped.textures.values().map(|v| v.len()).sum::<usize>();
        let total_whole = grouped.whole_file.len();
        self.reporter().log(&format!(
            "Grouped directives: {} FromArchive, {} Patched, {} Textures, {} whole-file ({} pre-skipped)",
            total_from, total_patched, total_textures, total_whole, grouped.pre_skipped
        ));
        let (direct, conflict, patch) = grouped.tier_counts;
        self.reporter().log(&format!(
            "Extraction tiers: {} direct, {} conflict, {} patch",
            direct, conflict, patch
        ));

        // BSA partial reuse: extract unchanged files from existing BSAs
        let reuse_stats = bsa_reuse::pre_extract_reusable_bsa_files(
            &self.db,
            &mut dp.ctx,
            &self.config.reporter,
        )?;
        if reuse_stats.files_reused > 0 {
            self.reporter().log(&format!(
                "BSA reuse: {} files pre-extracted from {} existing BSAs ({} changed, need download)",
                reuse_stats.files_reused, reuse_stats.bsas_with_reuse, reuse_stats.files_changed
            ));
        }

        // Create channel for archive events
        let (tx, rx) = std::sync::mpsc::sync_channel::<downloader::ArchiveEvent>(32);

        let streaming_config = streaming::StreamingConfig {
            max_extract_workers: Some(self.config.max_install_workers),
            max_parallel_7z_archives: Some(self.config.max_parallel_7z_archives),
            max_parallel_bsa_archives: Some(self.config.max_parallel_bsa_archives),
        };

        // Run download + extraction concurrently:
        // - Main thread: runs the processing loop (has &self.db, which is !Send)
        // - Download thread: opens its own DB connection for download status updates
        let db_path = self.config.db_path();
        let config_clone = self.config.clone();
        let priority_map = grouped.priority.clone();

        // Spawn the download on a dedicated thread with its own tokio runtime + DB connection
        let download_handle = std::thread::spawn(move || {
            let download_db = crate::modlist::ModlistDb::open_shared(&db_path)
                .expect("Failed to open download DB connection");
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to build tokio runtime for download thread");
            rt.block_on(async {
                downloader::download_archives_streaming(
                    &download_db,
                    &config_clone,
                    &tx,
                    Some(&priority_map),
                )
                .await
            })
        });

        // Run the processing loop on the main thread (owns &self.db).
        //
        // Two strategies exist:
        //
        // - Streaming (default): `run_processing_loop` processes each archive
        //   as soon as the download thread hands it over. Download and extract
        //   run fully in parallel — best for large modlists where the
        //   download dominates wall-clock time. BSAs are built incrementally
        //   via `BsaReadinessTracker` as their source archives complete.
        //
        // - Phased: `run_processing_loop_phased` drains all events first,
        //   then runs 4 sequential phases (complex extract → DDS → BSA build
        //   → simple extract) at full CPU. Better on small modlists where
        //   download is short and CPU-heavy phases dominate.
        //
        // Selection comes from `config.extract_strategy` via the `--extract`
        // CLI flag.
        let streaming_stats = match self.config.extract_strategy {
            ExtractStrategy::Streaming => {
                self.reporter()
                    .log("Using streaming extraction (download and extract run concurrently)");
                pipeline::run_processing_loop(
                    &self.db,
                    &dp.ctx,
                    &rx,
                    &grouped,
                    streaming_config,
                    &self.config.reporter,
                )?
            }
            ExtractStrategy::Phased => {
                self.reporter()
                    .log("Using phased extraction (download completes, then 4 sequential phases)");
                pipeline::run_processing_loop_phased(
                    &self.db,
                    &dp.ctx,
                    &rx,
                    &grouped,
                    streaming_config,
                    &self.config.reporter,
                )?
            }
        };

        // Wait for download thread to finish
        let download_stats = download_handle
            .join()
            .map_err(|_| anyhow::anyhow!("Download thread panicked"))??;

        stats.archives_downloaded = download_stats.downloaded;
        stats.archives_skipped = download_stats.skipped;
        stats.archives_failed = download_stats.failed;
        stats.archives_manual = download_stats.manual;
        stats.failed_downloads = download_stats.failed_downloads;
        stats.manual_downloads = download_stats.manual_downloads;

        stats.directives_completed = streaming_stats.extracted + streaming_stats.written;
        stats.directives_skipped = streaming_stats.skipped;
        stats.directives_failed = streaming_stats.failed;

        if stats.archives_manual > 0 || stats.archives_failed > 0 {
            log_phase_metrics("Pipelined Download+Extract", pipeline_start);
            return Ok(stats);
        }

        if streaming_stats.failed > 0 {
            warn!(
                "{} directive(s) failed during extraction — continuing with remaining phases",
                streaming_stats.failed
            );
        }
        let pipeline_secs = pipeline_start.elapsed().as_secs_f64();
        log_phase_metrics("Pipelined Download+Extract", pipeline_start);
        stats
            .phase_durations
            .push(("Download+Extract".into(), pipeline_secs));

        // === Phase 3: InlineFile + RemappedInlineFile ===
        let inline_start = Instant::now();
        self.reporter().phase_start(Phase::Installing);
        dp.inline_phase()?;
        trim_allocator_rss("inline files");
        log_phase_metrics("Inline Files", inline_start);
        stats
            .phase_durations
            .push(("Inline Files".into(), inline_start.elapsed().as_secs_f64()));

        // === Phase 4: DDS Transformations ===
        let dds_start = Instant::now();
        self.reporter().phase_start(Phase::DdsTransform);
        dp.texture_phase()?;
        trim_allocator_rss("texture phase");
        log_phase_metrics("DDS Transform", dds_start);
        stats
            .phase_durations
            .push(("DDS Transform".into(), dds_start.elapsed().as_secs_f64()));

        // === Phase 5: BSA Building ===
        let bsa_start = Instant::now();
        self.reporter().phase_start(Phase::BsaBuild);
        dp.bsa_phase()?;
        trim_allocator_rss("bsa build phase");
        log_phase_metrics("BSA Build", bsa_start);
        stats
            .phase_durations
            .push(("BSA Build".into(), bsa_start.elapsed().as_secs_f64()));

        // === Phase 6: Cleanup ===
        let cleanup_start = Instant::now();
        self.reporter().phase_start(Phase::Cleanup);
        dp.cleanup_phase()?;
        trim_allocator_rss("cleanup phase");
        log_phase_metrics("Cleanup", cleanup_start);
        stats
            .phase_durations
            .push(("Cleanup".into(), cleanup_start.elapsed().as_secs_f64()));

        let process_stats = dp.finish();
        stats.directives_completed += process_stats.completed;
        stats.directives_skipped += process_stats.skipped;
        stats.directives_failed += process_stats.failed;

        if stats.directives_failed > 0 {
            self.reporter().log(&format!(
                "Directive processing incomplete. {} failures.",
                stats.directives_failed
            ));
        } else {
            self.reporter().log("Installation complete!");
        }

        log_install_summary(&stats, total_start, &self.config.reporter);

        Ok(stats)
    }
}
