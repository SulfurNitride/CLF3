//! Installation orchestrator
//!
//! Coordinates the phases of modlist installation:
//! 1. Game Check    — validate game dir, detect game type
//! 2. Download      — fetch all required archives
//! 3. Validate      — verify archive sizes match expected
//! 4. Install       — FromArchive + InlineFile + RemappedInlineFile
//! 5. Patch         — PatchedFromArchive directives
//! 6. DDS Transform — TransformedTexture directives
//! 7. BSA Build     — CreateBSA directives
//! 8. Cleanup       — extra files + BSA temp dirs

pub mod config;
pub mod config_cache;
pub mod downloader;
pub mod extract_strategy;
pub mod handlers;
pub mod processor;
pub mod streaming;

pub use config::{InstallConfig, ProgressCallback, ProgressEvent};
#[allow(unused_imports)] // Used by lib crate (GUI)
pub use config_cache::{ConfigCache, ModlistConfig};

use crate::modlist::{import_wabbajack_to_db, ModlistDb};
use anyhow::{bail, Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use std::fs;
use std::time::{Duration, Instant};
use tracing::{info, warn};

fn current_rss_kb() -> Option<u64> {
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

fn log_phase_metrics(phase: &str, started: Instant) {
    let elapsed_ms = started.elapsed().as_millis();
    let rss_kb = current_rss_kb().unwrap_or(0);
    info!(
        "Phase done: phase='{}' elapsed_ms={} rss_kb={}",
        phase, elapsed_ms, rss_kb
    );
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
        fs::create_dir_all(&config.output_dir)
            .with_context(|| format!("Failed to create output directory: {}", config.output_dir.display()))?;
        fs::create_dir_all(&config.downloads_dir)
            .with_context(|| format!("Failed to create downloads directory: {}", config.downloads_dir.display()))?;

        // Parse wabbajack and import to database
        println!("Parsing: {}", config.wabbajack_path.display());
        let db = import_wabbajack_to_db(&config.wabbajack_path, &config.db_path())?;

        // Show modlist info
        if let (Some(name), Some(version)) = (db.get_metadata("name")?, db.get_metadata("version")?) {
            println!("Modlist: {} v{}", name, version);
        }

        let stats = db.get_directive_stats()?;
        println!("Directives: {} total ({} pending)\n", stats.total, stats.pending);

        Ok(Self { config, db })
    }

    /// Report a progress event to the callback if one is set
    fn report_progress(&self, event: ProgressEvent) {
        if let Some(ref callback) = self.config.progress_callback {
            callback(event);
        }
    }

    /// Download all required archives
    async fn download_phase(&mut self) -> Result<downloader::DownloadStats> {
        downloader::download_archives(&self.db, &self.config).await
    }

    /// Validate ALL downloaded archives (check sizes match expected)
    /// This catches truncated/corrupted downloads from interrupted sessions
    fn validate_archives(&self) -> Result<Vec<(String, String)>> {
        // Validate ALL archives that exist in downloads folder
        let archives = self.db.get_all_archives()?;
        let mut errors: Vec<(String, String)> = Vec::new();

        // Only check archives that actually exist on disk
        let archives_to_check: Vec<_> = archives
            .iter()
            .filter(|a| self.config.downloads_dir.join(&a.name).exists())
            .collect();

        if archives_to_check.is_empty() {
            println!("No downloaded archives to validate");
            return Ok(Vec::new());
        }

        let pb = ProgressBar::new(archives_to_check.len() as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} | {msg}")
                .unwrap()
                .progress_chars("=>-"),
        );
        pb.enable_steady_tick(Duration::from_millis(100));
        pb.set_message("Validating downloaded archives...");

        for archive in archives_to_check {
            let file_path = self.config.downloads_dir.join(&archive.name);

            match fs::metadata(&file_path) {
                Ok(meta) => {
                    let actual_size = meta.len();
                    let expected_size = archive.size as u64;
                    if actual_size != expected_size {
                        errors.push((
                            archive.name.clone(),
                            format!("Size mismatch: expected {} bytes, got {} ({}%)",
                                expected_size, actual_size,
                                (actual_size * 100) / expected_size.max(1)),
                        ));
                    }
                }
                Err(e) => {
                    errors.push((archive.name.clone(), format!("Cannot read file: {}", e)));
                }
            }

            pb.inc(1);
        }

        pb.finish_and_clear();
        println!("Validated {} archives", archives.len());

        Ok(errors)
    }

    /// Run the full installation using streaming pipeline.
    ///
    /// Phases:
    /// 1. Game Check → 2. Download → 3. Validate → 4. Install →
    /// 5. Patch → 6. DDS Transform → 7. BSA Build → 8. Cleanup
    pub async fn run_streaming(
        &mut self,
        _extraction_workers: usize,
        _mover_workers: usize,
    ) -> Result<InstallStats> {
        let mut stats = InstallStats::default();

        // === Phase 1: Game Check ===
        let game_check_start = Instant::now();
        println!("=== Phase 1: Game Check ===\n");
        self.report_progress(ProgressEvent::PhaseChange {
            phase: "Game Check".to_string(),
        });
        println!("Game directory: {}", self.config.game_dir.display());
        if !self.config.game_dir.exists() {
            bail!("Game directory does not exist: {}", self.config.game_dir.display());
        }
        println!("Game directory validated\n");
        log_phase_metrics("Game Check", game_check_start);

        // === Phase 2: Download Archives ===
        let download_start = Instant::now();
        println!("=== Phase 2: Download Archives ===\n");
        self.report_progress(ProgressEvent::PhaseChange {
            phase: "Downloading".to_string(),
        });
        let download_stats = self.download_phase().await?;
        stats.archives_downloaded = download_stats.downloaded;
        stats.archives_skipped = download_stats.skipped;
        stats.archives_failed = download_stats.failed;
        stats.archives_manual = download_stats.manual;
        stats.failed_downloads = download_stats.failed_downloads;
        stats.manual_downloads = download_stats.manual_downloads;

        if stats.archives_manual > 0 || stats.archives_failed > 0 {
            log_phase_metrics("Downloading", download_start);
            return Ok(stats);
        }
        log_phase_metrics("Downloading", download_start);

        // === Phase 3: Validate Archives ===
        let validate_start = Instant::now();
        println!("\n=== Phase 3: Validate Archives ===\n");
        self.report_progress(ProgressEvent::PhaseChange {
            phase: "Validating".to_string(),
        });
        let mut validation_attempts = 0;
        const MAX_VALIDATION_ATTEMPTS: usize = 3;

        loop {
            validation_attempts += 1;
            let validation_errors = self.validate_archives()?;

            if validation_errors.is_empty() {
                println!("All archives validated successfully!\n");
                break;
            }

            if validation_attempts >= MAX_VALIDATION_ATTEMPTS {
                println!("\nValidation failed after {} attempts! {} archives still have issues:",
                    MAX_VALIDATION_ATTEMPTS, validation_errors.len());
                for (name, error) in &validation_errors {
                    println!("  - {}: {}", name, error);
                }
                bail!("Archive validation failed");
            }

            println!("\nFound {} corrupted archives, auto-fixing...", validation_errors.len());
            for (name, error) in &validation_errors {
                println!("  - {}: {}", name, error);
                let file_path = self.config.downloads_dir.join(name);
                if file_path.exists() {
                    if let Err(e) = fs::remove_file(&file_path) {
                        warn!("Failed to remove corrupted file {}: {}", file_path.display(), e);
                    }
                }
                if let Err(e) = self.db.reset_archive_download_status(name) {
                    warn!("Failed to reset download status for {}: {}", name, e);
                }
            }

            println!("\nRe-downloading {} files...\n", validation_errors.len());
            let redownload_stats = self.download_phase().await?;

            if redownload_stats.failed > 0 {
                println!("Re-download had {} failures", redownload_stats.failed);
            }
        }
        log_phase_metrics("Validating", validate_start);

        // Create the directive processor for phases 4-8
        let dp = processor::DirectiveProcessor::new(&self.db, &self.config)?;

        // Pre-flight: check archives and index
        let preflight_start = Instant::now();
        dp.preflight_check()?;
        dp.index_archives()?;
        log_phase_metrics("Preflight/Index", preflight_start);

        // === Phase 4: Install Files ===
        let install_start = Instant::now();
        println!("\n=== Phase 4: Install Files ===\n");
        self.report_progress(ProgressEvent::PhaseChange {
            phase: "Installing".to_string(),
        });
        self.report_progress(ProgressEvent::Status {
            message: "Installing files...".to_string(),
        });
        dp.install_phase()?;
        log_phase_metrics("Installing", install_start);

        // === Phase 5: Apply Patches ===
        let patch_start = Instant::now();
        println!("\n=== Phase 5: Apply Patches ===\n");
        self.report_progress(ProgressEvent::PhaseChange {
            phase: "Patching".to_string(),
        });
        dp.patch_phase()?;
        log_phase_metrics("Patching", patch_start);

        // === Phase 6: DDS Transformations ===
        let dds_start = Instant::now();
        println!("\n=== Phase 6: DDS Transformations ===\n");
        self.report_progress(ProgressEvent::PhaseChange {
            phase: "DDS Transform".to_string(),
        });
        dp.texture_phase()?;
        log_phase_metrics("DDS Transform", dds_start);

        // === Phase 7: BSA Building ===
        let bsa_start = Instant::now();
        println!("\n=== Phase 7: BSA Building ===\n");
        self.report_progress(ProgressEvent::PhaseChange {
            phase: "BSA Build".to_string(),
        });
        dp.bsa_phase()?;
        log_phase_metrics("BSA Build", bsa_start);

        // === Phase 8: Cleanup ===
        let cleanup_start = Instant::now();
        println!("\n=== Phase 8: Cleanup ===\n");
        self.report_progress(ProgressEvent::PhaseChange {
            phase: "Cleanup".to_string(),
        });
        dp.cleanup_phase()?;
        log_phase_metrics("Cleanup", cleanup_start);

        let process_stats = dp.finish();
        stats.directives_completed = process_stats.completed;
        stats.directives_skipped = process_stats.skipped;
        stats.directives_failed = process_stats.failed;

        if stats.directives_failed > 0 {
            println!("\nDirective processing incomplete. {} failures.", stats.directives_failed);
        } else {
            println!("\nInstallation complete!");
        }

        Ok(stats)
    }
}
