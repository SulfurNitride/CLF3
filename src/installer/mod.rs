//! Installation orchestrator
//!
//! Coordinates the phases of modlist installation:
//! 1. Download all archives
//! 2. Validate downloaded archives (size check)
//! 3. Process directives

pub mod config;
pub mod downloader;
pub mod handlers;
pub mod processor;
pub mod streaming;
pub mod verify;

pub use config::InstallConfig;

use crate::modlist::{import_wabbajack_to_db, ModlistDb};
use anyhow::{bail, Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use std::fs;
use std::time::Duration;

/// Installation statistics
#[derive(Debug, Default)]
pub struct InstallStats {
    pub archives_downloaded: usize,
    pub archives_skipped: usize,
    pub archives_failed: usize,
    pub archives_manual: usize,
    pub directives_completed: usize,
    pub directives_failed: usize,
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

    /// Run the full installation
    pub async fn run(&mut self) -> Result<InstallStats> {
        let mut stats = InstallStats::default();

        // Phase 1: Downloads
        println!("=== Phase 1: Download Archives ===\n");
        let download_stats = self.download_phase().await?;
        stats.archives_downloaded = download_stats.downloaded;
        stats.archives_skipped = download_stats.skipped;
        stats.archives_failed = download_stats.failed;
        stats.archives_manual = download_stats.manual;

        // If there are manual downloads needed, stop here
        if stats.archives_manual > 0 || stats.archives_failed > 0 {
            return Ok(stats);
        }

        // Phase 2: Validate all downloaded archives (and auto-fix if needed)
        println!("\n=== Phase 2: Validate Archives ===\n");
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

            // Auto-fix: delete bad files and re-download
            println!("\nFound {} corrupted archives, auto-fixing...", validation_errors.len());
            for (name, error) in &validation_errors {
                println!("  - {}: {}", name, error);
                let file_path = self.config.downloads_dir.join(name);
                if file_path.exists() {
                    let _ = fs::remove_file(&file_path);
                }
                // Reset download status in DB
                let _ = self.db.reset_archive_download_status(name);
            }

            // Re-run download phase for the corrupted files
            println!("\nRe-downloading {} files...\n", validation_errors.len());
            let redownload_stats = self.download_phase().await?;

            if redownload_stats.failed > 0 {
                println!("Re-download had {} failures", redownload_stats.failed);
            }
        }

        // Phase 3: Process directives
        println!("=== Phase 3: Process Directives ===\n");
        let process_stats = processor::process_directives(&self.db, &self.config)?;
        stats.directives_completed = process_stats.completed;
        stats.directives_failed = process_stats.failed;

        if stats.directives_failed > 0 {
            println!("\nDirective processing incomplete. {} failures.", stats.directives_failed);
        } else {
            println!("\nAll directives processed successfully!");
        }

        Ok(stats)
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
    /// This is an alternative to `run()` that uses a streaming architecture
    /// with separate extraction and mover worker pools for better performance
    /// on large modlists.
    ///
    /// Arguments:
    /// - `extraction_workers`: Number of workers for extracting files from archives (default: 8)
    /// - `mover_workers`: Number of workers for writing files to disk (default: 8)
    pub async fn run_streaming(
        &mut self,
        extraction_workers: usize,
        mover_workers: usize,
    ) -> Result<InstallStats> {
        let mut stats = InstallStats::default();

        // Phase 1: Downloads (same as regular mode)
        println!("=== Phase 1: Download Archives ===\n");
        let download_stats = self.download_phase().await?;
        stats.archives_downloaded = download_stats.downloaded;
        stats.archives_skipped = download_stats.skipped;
        stats.archives_failed = download_stats.failed;
        stats.archives_manual = download_stats.manual;

        // If there are manual downloads needed, stop here
        if stats.archives_manual > 0 || stats.archives_failed > 0 {
            return Ok(stats);
        }

        // Phase 2: Validate all downloaded archives (same as regular mode)
        println!("\n=== Phase 2: Validate Archives ===\n");
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

            // Auto-fix: delete bad files and re-download
            println!("\nFound {} corrupted archives, auto-fixing...", validation_errors.len());
            for (name, error) in &validation_errors {
                println!("  - {}: {}", name, error);
                let file_path = self.config.downloads_dir.join(name);
                if file_path.exists() {
                    let _ = fs::remove_file(&file_path);
                }
                let _ = self.db.reset_archive_download_status(name);
            }

            println!("\nRe-downloading {} files...\n", validation_errors.len());
            let redownload_stats = self.download_phase().await?;

            if redownload_stats.failed > 0 {
                println!("Re-download had {} failures", redownload_stats.failed);
            }
        }

        // Phase 3: Process directives using STREAMING pipeline
        println!("=== Phase 3: Process Directives (Streaming) ===\n");
        let process_stats = processor::process_directives_streaming(
            &self.db,
            &self.config,
            extraction_workers,
            mover_workers,
        )?;
        stats.directives_completed = process_stats.completed;
        stats.directives_failed = process_stats.failed;

        if stats.directives_failed > 0 {
            println!("\nDirective processing incomplete. {} failures.", stats.directives_failed);
        } else {
            println!("\nAll directives processed successfully!");
        }

        Ok(stats)
    }
}
