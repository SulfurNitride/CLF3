//! Collection installation orchestrator.
//!
//! Handles the full installation pipeline for Nexus Collections:
//! 1. Parse collection JSON and import to database
//! 2. Setup MO2 instance (download MO2, create Stock Game)
//! 3. Download all mod archives
//! 4. Validate archives (size/hash check)
//! 5. Extract and route files per mod
//! 6. Process FOMOD installers with saved choices
//! 7. Generate modlist.txt (mod load order)
//! 8. Generate plugins.txt (plugin load order)

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use tracing::{error, info, warn};

use crate::file_router::FileRouter;
use crate::games::GameType;
use crate::mo2::Mo2Instance;

use super::db::{CollectionDb, ModDbEntry, ModStats, ModStatus};

/// Progress callback for reporting installation status
pub type ProgressCallback = Arc<dyn Fn(InstallProgress) + Send + Sync>;

/// Installation progress information
#[derive(Debug, Clone)]
pub enum InstallProgress {
    /// Starting a phase
    PhaseStarted {
        phase: InstallPhase,
        message: String,
    },
    /// Phase completed
    PhaseCompleted {
        phase: InstallPhase,
    },
    /// Download progress
    Downloading {
        mod_name: String,
        current: u64,
        total: u64,
        mod_index: usize,
        mod_count: usize,
    },
    /// Extraction progress
    Extracting {
        mod_name: String,
        current: usize,
        total: usize,
    },
    /// Installation progress
    Installing {
        mod_name: String,
        current: usize,
        total: usize,
    },
    /// Error occurred
    Error {
        message: String,
    },
    /// Overall stats update
    Stats {
        stats: ModStats,
    },
}

/// Installation phases
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstallPhase {
    /// Phase A: Parse collection JSON, determine game type
    ParseCollection,
    /// Phase B: Setup MO2 instance (download MO2, create Stock Game)
    SetupMo2,
    /// Phase C: Download all mod archives
    DownloadMods,
    /// Phase D: Validate archives (size/hash check)
    ValidateArchives,
    /// Phase E: Extract and route files per mod
    ExtractFiles,
    /// Phase F: Process FOMOD installers with saved choices
    ProcessFomods,
    /// Phase G: Generate modlist.txt (mod load order)
    GenerateModlist,
    /// Phase H: Generate plugins.txt (plugin load order)
    GeneratePlugins,
}

impl InstallPhase {
    pub fn name(&self) -> &'static str {
        match self {
            InstallPhase::ParseCollection => "Parse Collection",
            InstallPhase::SetupMo2 => "Setup MO2",
            InstallPhase::DownloadMods => "Download Mods",
            InstallPhase::ValidateArchives => "Validate Archives",
            InstallPhase::ExtractFiles => "Extract Files",
            InstallPhase::ProcessFomods => "Process FOMODs",
            InstallPhase::GenerateModlist => "Generate Mod List",
            InstallPhase::GeneratePlugins => "Generate Plugin List",
        }
    }
}

/// Configuration for collection installation
#[derive(Debug, Clone)]
pub struct InstallerConfig {
    /// Path to the collection JSON file
    pub collection_path: PathBuf,
    /// Output directory for MO2 instance
    pub output_dir: PathBuf,
    /// Path to the game installation
    pub game_path: PathBuf,
    /// Game type (auto-detected from collection if not specified)
    pub game_type: Option<GameType>,
    /// Nexus API key for downloads
    pub nexus_api_key: String,
    /// Number of concurrent operations (CPU thread count)
    pub concurrent_downloads: usize,
    /// Downloads directory (defaults to output_dir/downloads)
    pub downloads_dir: Option<PathBuf>,
}

impl Default for InstallerConfig {
    fn default() -> Self {
        let thread_count = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);

        Self {
            collection_path: PathBuf::new(),
            output_dir: PathBuf::new(),
            game_path: PathBuf::new(),
            game_type: None,
            nexus_api_key: String::new(),
            concurrent_downloads: thread_count,
            downloads_dir: None,
        }
    }
}

/// Collection installer orchestrator
pub struct CollectionInstaller {
    config: InstallerConfig,
    db: CollectionDb,
    mo2: Option<Mo2Instance>,
    file_router: Option<FileRouter>,
    progress_callback: Option<ProgressCallback>,
    game_type: GameType,
    masterlist_path: Option<PathBuf>,
}

impl CollectionInstaller {
    /// Create a new collection installer
    pub fn new(config: InstallerConfig, db_path: &Path) -> Result<Self> {
        let db = CollectionDb::open(db_path).context("Failed to open collection database")?;

        Ok(Self {
            config,
            db,
            mo2: None,
            file_router: None,
            progress_callback: None,
            game_type: GameType::SkyrimSE, // Default, will be updated during parse
            masterlist_path: None,
        })
    }

    /// Create installer with in-memory database (for testing)
    pub fn new_in_memory(config: InstallerConfig) -> Result<Self> {
        let db = CollectionDb::in_memory()?;

        Ok(Self {
            config,
            masterlist_path: None,
            db,
            mo2: None,
            file_router: None,
            progress_callback: None,
            game_type: GameType::SkyrimSE,
        })
    }

    /// Set progress callback
    pub fn set_progress_callback(&mut self, callback: ProgressCallback) {
        self.progress_callback = Some(callback);
    }

    /// Report progress
    fn report_progress(&self, progress: InstallProgress) {
        if let Some(ref callback) = self.progress_callback {
            callback(progress);
        }
    }

    /// Get the database reference
    pub fn db(&self) -> &CollectionDb {
        &self.db
    }

    /// Get mutable database reference
    pub fn db_mut(&mut self) -> &mut CollectionDb {
        &mut self.db
    }

    /// Get current mod statistics
    pub fn get_stats(&self) -> Result<ModStats> {
        self.db.get_mod_stats()
    }

    /// Get the MO2 instance (if setup)
    pub fn mo2(&self) -> Option<&Mo2Instance> {
        self.mo2.as_ref()
    }

    /// Get the file router (if setup)
    pub fn file_router(&self) -> Option<&FileRouter> {
        self.file_router.as_ref()
    }

    /// Run the full installation pipeline
    pub async fn install(&mut self) -> Result<()> {
        // Phase A: Parse collection
        self.phase_parse_collection()?;

        // Phase A.5: Download LOOT masterlist (REQUIRED for proper plugin sorting)
        // Do this early so we fail fast if network is unavailable
        self.phase_download_masterlist().await?;

        // Phase B: Setup MO2 (always runs - directory cleaned by CLI if needed)
        self.phase_setup_mo2().await?;

        // Phase C: Download mods (smart checking skips existing valid files)
        self.phase_download_mods().await?;

        // Phase D: Validate archives (with auto-fix retry loop)
        self.phase_validate_archives().await?;

        // Phase D.5: FOMOD preflight validation
        self.phase_validate_fomods()?;

        // Phase E: Extract files (skips FOMOD mods - they're handled in Phase F)
        self.phase_extract_files().await?;

        // Phase F: Process FOMODs (using pre-validated configs)
        self.phase_process_fomods()?;

        // Phase G: Generate modlist.txt
        self.phase_generate_modlist()?;

        // Phase H: Generate plugins.txt
        self.phase_generate_plugins()?;

        info!("Collection installation complete!");
        Ok(())
    }

    /// Phase A: Parse collection JSON and import to database
    pub fn phase_parse_collection(&mut self) -> Result<()> {
        self.report_progress(InstallProgress::PhaseStarted {
            phase: InstallPhase::ParseCollection,
            message: "Parsing collection JSON...".to_string(),
        });

        // Load collection
        let collection = super::load_collection(&self.config.collection_path)?;

        info!(
            "Loaded collection: {} by {} ({} mods)",
            collection.get_name(),
            collection.get_author(),
            collection.mod_count()
        );

        // Detect game type from domain
        self.game_type = self
            .config
            .game_type
            .or_else(|| GameType::from_nexus_domain(collection.get_domain_name()))
            .unwrap_or(GameType::SkyrimSE);

        info!("Game type: {:?}", self.game_type);

        // Setup file router
        self.file_router = Some(FileRouter::new(self.game_type));

        // Import to database
        self.db.import_collection(&collection)?;

        self.report_progress(InstallProgress::PhaseCompleted {
            phase: InstallPhase::ParseCollection,
        });

        Ok(())
    }

    /// Phase A.5: Download LOOT masterlist
    ///
    /// Downloads the masterlist from GitHub. This is required for proper plugin sorting.
    /// We do this early to fail fast if there's a network issue.
    pub async fn phase_download_masterlist(&mut self) -> Result<()> {
        use crate::loot::PluginSorter;

        println!("Downloading LOOT masterlist (required for plugin sorting)...");
        let _ = std::io::Write::flush(&mut std::io::stdout());
        info!("Downloading LOOT masterlist (required for plugin sorting)...");

        // Create a temporary sorter just to get the cache directory and download
        // We use a dummy path since we just need the download functionality
        let temp_path = std::env::temp_dir();
        let sorter = PluginSorter::new(self.game_type, &temp_path, &temp_path)
            .context("Failed to initialize LOOT for masterlist download")?;

        let cache_dir = sorter.default_masterlist_cache_dir();
        let _masterlist_path = cache_dir.join("masterlist.yaml");

        // Always download fresh to ensure we have the latest
        match sorter.download_masterlist(&cache_dir).await {
            Ok(path) => {
                println!("LOOT masterlist downloaded successfully");
                info!("LOOT masterlist downloaded to: {}", path.display());
                self.masterlist_path = Some(path);
                Ok(())
            }
            Err(e) => {
                eprintln!("CRITICAL: Failed to download LOOT masterlist: {:#}", e);
                eprintln!("Plugin load order cannot be sorted correctly without the masterlist.");
                eprintln!("Please check your internet connection and try again.");
                error!("CRITICAL: Failed to download LOOT masterlist: {:#}", e);
                Err(e.context("Failed to download LOOT masterlist - cannot continue without it"))
            }
        }
    }

    /// Phase B: Setup MO2 instance
    pub async fn phase_setup_mo2(&mut self) -> Result<()> {
        self.report_progress(InstallProgress::PhaseStarted {
            phase: InstallPhase::SetupMo2,
            message: "Setting up MO2 instance...".to_string(),
        });

        // Create MO2 instance paths
        let mo2 = Mo2Instance::new(self.config.output_dir.clone());

        // Create directories
        mo2.create_directories()?;

        // Download MO2 if not present
        if !mo2.executable.exists() {
            info!("Downloading MO2...");
            crate::mo2::download_mo2(&self.config.output_dir, Some(|current, total| {
                // Progress callback for download
                info!("MO2 download: {}/{} bytes", current, total);
            }))
            .await?;
        }

        // Create Stock Game folder (check for game exe, not just directory existence)
        let stock_game_exe = mo2.stock_game_dir.join(self.game_type.executable());
        if !stock_game_exe.exists() {
            info!("Creating Stock Game folder (copying game files)...");
            println!("Copying game files to Stock Game folder...");
            let files_copied = crate::mo2::create_stock_game(
                &self.config.game_path,
                &mo2.stock_game_dir,
                None::<fn(crate::mo2::CopyProgress)>, // Progress is logged via tracing
            )?;
            println!("Copied {} game files to Stock Game", files_copied);
        } else {
            info!("Stock Game folder already exists with game files");
        }

        // Generate INI files
        let ini_config = crate::mo2::IniConfig {
            game_type: self.game_type,
            stock_game_path: mo2.stock_game_dir.to_string_lossy().to_string(),
            profile_name: "Default".to_string(),
            version: "2.5.2".to_string(),
        };

        crate::mo2::generate_ini(&ini_config, &mo2.ini_path)?;
        crate::mo2::create_profile(&mo2.profiles_dir, "Default", self.game_type)?;

        self.mo2 = Some(mo2);

        self.report_progress(InstallProgress::PhaseCompleted {
            phase: InstallPhase::SetupMo2,
        });

        Ok(())
    }

    /// Phase C: Download all mod archives
    ///
    /// Uses parallel downloads with:
    /// - Rate limit tracking and exponential backoff
    /// - NXM browser mode fallback (when nxm_mode enabled)
    /// - Progress display with multi-progress bars
    /// - Auto-retry on failures
    /// - Size verification
    pub async fn phase_download_mods(&mut self) -> Result<()> {
        self.report_progress(InstallProgress::PhaseStarted {
            phase: InstallPhase::DownloadMods,
            message: "Downloading mod archives...".to_string(),
        });

        let downloads_dir = self
            .config
            .downloads_dir
            .clone()
            .unwrap_or_else(|| self.config.output_dir.join("downloads"));

        std::fs::create_dir_all(&downloads_dir)?;

        // Get the game domain for Nexus API
        let game_domain = self.game_type.nexus_domain();

        // Use the download module for parallel downloads
        let stats = super::download::download_mods(
            &self.db,
            &downloads_dir,
            &self.config.nexus_api_key,
            game_domain,
            self.config.concurrent_downloads,
            false, // TODO: Add nxm_mode to InstallerConfig
        )
        .await?;

        info!(
            "Download phase complete: {} downloaded, {} skipped, {} failed",
            stats.downloaded, stats.skipped, stats.failed
        );

        // Report stats
        self.report_progress(InstallProgress::Stats {
            stats: ModStats {
                total: stats.downloaded + stats.skipped + stats.failed,
                pending: 0,
                downloading: 0,
                downloaded: stats.downloaded + stats.skipped,
                extracting: 0,
                extracted: 0,
                installing: 0,
                installed: 0,
                failed: stats.failed,
            },
        });

        self.report_progress(InstallProgress::PhaseCompleted {
            phase: InstallPhase::DownloadMods,
        });

        Ok(())
    }

    /// Phase D: Validate downloaded archives with auto-fix and retry
    pub async fn phase_validate_archives(&mut self) -> Result<()> {
        self.report_progress(InstallProgress::PhaseStarted {
            phase: InstallPhase::ValidateArchives,
            message: "Validating downloaded archives...".to_string(),
        });

        let downloads_dir = self
            .config
            .downloads_dir
            .clone()
            .unwrap_or_else(|| self.config.output_dir.join("downloads"));

        let verify_options = super::verify::VerifyOptions {
            check_size: true,
            check_hash: true, // Enable MD5 verification
        };

        const MAX_VALIDATION_ATTEMPTS: usize = 3;
        let mut validation_attempts = 0;

        loop {
            validation_attempts += 1;

            // Verify all downloaded archives
            let results = super::verify::verify_all_downloads(&self.db, &downloads_dir, &verify_options)?;

            // Separate valid and invalid
            let (valid, invalid): (Vec<_>, Vec<_>) = results
                .into_iter()
                .partition(|(_, result)| result.is_valid());

            info!(
                "Validation attempt {}: {} valid, {} invalid",
                validation_attempts,
                valid.len(),
                invalid.len()
            );

            // Valid mods stay as Downloaded - extraction phase will process them
            // (Don't change status here - they're still waiting to be extracted)

            // If all valid, we're done
            if invalid.is_empty() {
                info!("All archives validated successfully!");
                break;
            }

            // Check if we've exceeded max attempts
            if validation_attempts >= MAX_VALIDATION_ATTEMPTS {
                warn!(
                    "Validation failed after {} attempts! {} archives still have issues:",
                    MAX_VALIDATION_ATTEMPTS,
                    invalid.len()
                );
                for (mod_entry, result) in &invalid {
                    if let Some(msg) = result.error_message() {
                        warn!("  - {}: {}", mod_entry.name, msg);
                    }
                    self.db.mark_mod_failed(
                        mod_entry.id,
                        &result.error_message().unwrap_or_else(|| "Unknown error".to_string()),
                    )?;
                }
                // Don't bail - let the user see results and decide
                self.report_progress(InstallProgress::Error {
                    message: format!(
                        "Validation failed for {} archives after {} attempts",
                        invalid.len(),
                        MAX_VALIDATION_ATTEMPTS
                    ),
                });
                break;
            }

            // Auto-fix: delete corrupted files and reset status for re-download
            info!(
                "Found {} corrupted archives, auto-fixing (attempt {}/{})...",
                invalid.len(),
                validation_attempts,
                MAX_VALIDATION_ATTEMPTS
            );

            for (mod_entry, result) in &invalid {
                if let Some(msg) = result.error_message() {
                    info!("  - {}: {}", mod_entry.name, msg);
                }
            }

            // Delete and reset status
            let fixed_count = super::verify::auto_fix_corrupted(&self.db, &downloads_dir, &invalid)?;
            info!("Reset {} archives for re-download", fixed_count);

            // Re-run download phase for the corrupted files
            if fixed_count > 0 {
                info!("Re-downloading {} files...", fixed_count);
                self.phase_download_mods().await?;
            }
        }

        self.report_progress(InstallProgress::PhaseCompleted {
            phase: InstallPhase::ValidateArchives,
        });

        Ok(())
    }

    /// Phase D.5: FOMOD preflight validation
    ///
    /// Validates all FOMOD ModuleConfig.xml files before extraction.
    /// This catches parsing errors early and provides detailed error traces.
    pub fn phase_validate_fomods(&mut self) -> Result<()> {
        self.report_progress(InstallProgress::PhaseStarted {
            phase: InstallPhase::ProcessFomods, // Reuse phase for progress
            message: "Validating FOMOD configurations...".to_string(),
        });

        // Create local temp directory (not /tmp)
        let temp_base_dir = self.config.output_dir.join(".fomod_temp");

        // Run preflight validation
        let stats = super::extract::validate_fomod_configs(&self.db, &temp_base_dir)?;

        // Check for invalid FOMMODs
        if stats.invalid > 0 {
            let invalid_mods = self.db.get_invalid_fomod_mods()?;

            println!();
            println!("==========================================");
            println!("FOMOD VALIDATION FAILED");
            println!("==========================================");
            println!("{} FOMOD(s) failed validation:", stats.invalid);
            println!();

            for mod_entry in &invalid_mods {
                println!("  MOD: {}", mod_entry.name);
                println!("  FOLDER: {}", mod_entry.folder_name);
                if let Some(ref error) = mod_entry.fomod_error {
                    for line in error.lines() {
                        println!("    {}", line);
                    }
                }
                println!();
            }

            println!("==========================================");
            println!();
            println!("Options:");
            println!("  [S]kip failed mods and continue installation");
            println!("  [A]bort installation");
            println!();
            print!("What would you like to do? [s/A] ");

            use std::io::Write;
            std::io::stdout().flush().ok();

            let mut input = String::new();
            std::io::stdin().read_line(&mut input).ok();

            if input.trim().eq_ignore_ascii_case("s") {
                warn!("Skipping {} failed FOMOD mod(s) and continuing...", stats.invalid);
            } else {
                self.report_progress(InstallProgress::Error {
                    message: format!("FOMOD validation failed for {} mod(s)", stats.invalid),
                });
                anyhow::bail!("Installation aborted due to FOMOD validation failures.");
            }
        }

        info!(
            "FOMOD validation: {} valid, {} invalid, {} skipped",
            stats.valid, stats.invalid, stats.skipped
        );

        self.report_progress(InstallProgress::PhaseCompleted {
            phase: InstallPhase::ProcessFomods,
        });

        Ok(())
    }

    /// Phase E: Extract and route files
    pub async fn phase_extract_files(&mut self) -> Result<()> {
        self.report_progress(InstallProgress::PhaseStarted {
            phase: InstallPhase::ExtractFiles,
            message: "Extracting mod files...".to_string(),
        });

        let mo2 = self.mo2.as_ref().context("MO2 instance not set up")?;
        let file_router = self
            .file_router
            .as_ref()
            .context("File router not initialized")?;

        let downloads_dir = self
            .config
            .downloads_dir
            .clone()
            .unwrap_or_else(|| self.config.output_dir.join("downloads"));

        // Create local temp directory (not /tmp)
        let temp_base_dir = self.config.output_dir.join(".fomod_temp");

        // First, index all archives (builds file listings in database)
        info!("Indexing archive contents...");
        let indexed = super::extract::index_all_archives(&self.db, &downloads_dir)?;
        if indexed > 0 {
            info!("Indexed {} archives", indexed);
        }

        // Extract all mods in parallel with progress display
        let stats = super::extract::extract_all_mods(
            &self.db,
            &downloads_dir,
            &mo2.mods_dir,
            &mo2.stock_game_dir,
            file_router,
            &temp_base_dir,
        )?;

        // Update mod statuses based on results
        let mods = self.db.get_mods_by_status(ModStatus::Extracted)?;
        for mod_entry in &mods {
            // Check if mod folder was created
            let mod_dir = mo2.mods_dir.join(&mod_entry.folder_name);
            if mod_dir.exists() {
                self.db
                    .update_mod_status(mod_entry.id, ModStatus::Installed)?;
            }
        }

        info!(
            "Extraction complete: {} extracted, {} failed",
            stats.extracted, stats.failed
        );

        self.report_progress(InstallProgress::PhaseCompleted {
            phase: InstallPhase::ExtractFiles,
        });

        Ok(())
    }

    /// Phase F: Process FOMOD installers (parallel)
    pub fn phase_process_fomods(&mut self) -> Result<()> {
        use rayon::prelude::*;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        self.report_progress(InstallProgress::PhaseStarted {
            phase: InstallPhase::ProcessFomods,
            message: "Processing FOMOD installers...".to_string(),
        });

        let mo2 = self.mo2.as_ref().context("MO2 instance not set up")?;

        // Create local temp directory (not /tmp)
        let temp_base_dir = self.config.output_dir.join(".fomod_temp");
        std::fs::create_dir_all(&temp_base_dir)?;

        // Get FOMOD mods that have been validated but need processing
        let all_mods = self.db.get_mods_by_status(ModStatus::Extracted)?;
        let fomod_mods: Vec<_> = all_mods.into_iter().filter(|m| m.has_fomod()).collect();

        if fomod_mods.is_empty() {
            info!("No FOMOD installers to process");
            self.report_progress(InstallProgress::PhaseCompleted {
                phase: InstallPhase::ProcessFomods,
            });
            return Ok(());
        }

        let num_threads = rayon::current_num_threads();
        println!("Processing {} FOMOD installers ({} threads)...", fomod_mods.len(), num_threads);

        // Setup multi-progress display
        let mp = indicatif::MultiProgress::new();
        let overall_pb = mp.add(indicatif::ProgressBar::new(fomod_mods.len() as u64));
        overall_pb.set_style(
            indicatif::ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] FOMOD [{bar:40.cyan/blue}] {pos}/{len} | {msg}")
                .unwrap()
                .progress_chars("=>-"),
        );
        overall_pb.enable_steady_tick(std::time::Duration::from_millis(100));

        let processed = AtomicUsize::new(0);
        let skipped = AtomicUsize::new(0);
        let failed = AtomicUsize::new(0);

        let mp = Arc::new(mp);
        let overall_pb = Arc::new(overall_pb);
        let mods_dir = mo2.mods_dir.clone();

        // Process FOMODs in parallel - collect results
        #[derive(Debug)]
        enum FomodResult {
            Success { id: i64, files: usize, folders: usize },
            Skipped { id: i64, reason: String },
            Failed { id: i64, name: String, error: String },
        }

        let results: Vec<FomodResult> = fomod_mods
            .par_iter()
            .map(|mod_entry| {
                // Skip mods that failed preflight validation
                if mod_entry.fomod_validated && !mod_entry.fomod_valid {
                    skipped.fetch_add(1, Ordering::Relaxed);
                    overall_pb.inc(1);
                    overall_pb.set_message(format!(
                        "OK:{} Skip:{} Fail:{}",
                        processed.load(Ordering::Relaxed),
                        skipped.load(Ordering::Relaxed),
                        failed.load(Ordering::Relaxed)
                    ));
                    return FomodResult::Skipped {
                        id: mod_entry.id,
                        reason: mod_entry.fomod_error.clone().unwrap_or_else(|| "preflight failed".to_string()),
                    };
                }

                // Create mod-specific progress spinner
                let mod_pb = mp.insert_before(&overall_pb, indicatif::ProgressBar::new_spinner());
                mod_pb.set_style(
                    indicatif::ProgressStyle::default_spinner()
                        .template("  {spinner:.blue} {wide_msg}")
                        .unwrap(),
                );
                mod_pb.enable_steady_tick(std::time::Duration::from_millis(100));

                let display_name = if mod_entry.name.len() > 50 {
                    format!("{}...", &mod_entry.name[..47])
                } else {
                    mod_entry.name.clone()
                };
                mod_pb.set_message(display_name.clone());

                // Each mod gets its own temp subfolder for isolation
                let mod_temp_dir = temp_base_dir.join(format!(".fomod_{}", mod_entry.id));
                let _ = std::fs::create_dir_all(&mod_temp_dir);

                let result = match process_single_fomod_static(mod_entry, &mods_dir, &mod_temp_dir) {
                    Ok(stats) => {
                        processed.fetch_add(1, Ordering::Relaxed);
                        mod_pb.finish_and_clear();
                        FomodResult::Success {
                            id: mod_entry.id,
                            files: stats.files_installed,
                            folders: stats.folders_installed,
                        }
                    }
                    Err(e) => {
                        let mut error_chain = format!("{}", e);
                        let mut source = e.source();
                        while let Some(s) = source {
                            error_chain.push_str(&format!("\n  Caused by: {}", s));
                            source = s.source();
                        }
                        failed.fetch_add(1, Ordering::Relaxed);
                        mod_pb.finish_and_clear();
                        overall_pb.println(format!("FAIL: {} - {}", mod_entry.name, e));
                        FomodResult::Failed {
                            id: mod_entry.id,
                            name: mod_entry.name.clone(),
                            error: error_chain,
                        }
                    }
                };

                // Cleanup mod-specific temp dir
                let _ = std::fs::remove_dir_all(&mod_temp_dir);

                overall_pb.inc(1);
                overall_pb.set_message(format!(
                    "OK:{} Skip:{} Fail:{}",
                    processed.load(Ordering::Relaxed),
                    skipped.load(Ordering::Relaxed),
                    failed.load(Ordering::Relaxed)
                ));

                result
            })
            .collect();

        overall_pb.finish_and_clear();

        // Update database with results (sequential)
        let mut success_count = 0;
        let mut skip_count = 0;
        let mut fail_count = 0;
        let mut failures: Vec<(String, String)> = Vec::new();

        for result in results {
            match result {
                FomodResult::Success { id, files, folders } => {
                    self.db.update_mod_status(id, ModStatus::Installed)?;
                    info!("FOMOD id={}: installed {} files, {} folders", id, files, folders);
                    success_count += 1;
                }
                FomodResult::Skipped { id, reason } => {
                    info!("FOMOD id={} skipped: {}", id, reason);
                    skip_count += 1;
                }
                FomodResult::Failed { id, name, error } => {
                    self.db.mark_mod_failed(id, &error)?;
                    failures.push((name, error));
                    fail_count += 1;
                }
            }
        }

        println!(
            "FOMOD processing complete: {} processed, {} skipped, {} failed",
            success_count, skip_count, fail_count
        );

        // Report failures at the end
        if !failures.is_empty() {
            println!("\n{} FOMOD installations failed:", failures.len());
            for (name, _error) in &failures {
                println!("  - {}", name);
            }
            println!();
        }

        self.report_progress(InstallProgress::PhaseCompleted {
            phase: InstallPhase::ProcessFomods,
        });

        Ok(())
    }

    /// Process a single FOMOD mod
    fn process_single_fomod(
        &self,
        mod_entry: &ModDbEntry,
        mods_dir: &Path,
        temp_base_dir: &Path,
    ) -> Result<super::fomod::FomodStats> {
        use super::fomod::{execute_fomod, find_module_config, parse_fomod};

        // Get choices from database
        let choices = mod_entry
            .get_choices()
            .context("FOMOD mod has no choices")?;

        // Get archive path
        let archive_path = mod_entry
            .local_path
            .as_ref()
            .map(PathBuf::from)
            .context("FOMOD mod has no local archive path")?;

        if !archive_path.exists() {
            anyhow::bail!("Archive not found: {}", archive_path.display());
        }

        // Extract to temp directory (in local folder, not /tmp)
        let temp_dir = tempfile::tempdir_in(temp_base_dir).context("Failed to create temp directory")?;
        info!(
            "Extracting {} to temp for FOMOD processing...",
            mod_entry.name
        );

        super::extract::extract_archive_to_dir_pub(&archive_path, temp_dir.path())?;

        // Find ModuleConfig.xml in extracted archive
        let (config_path, data_root) = find_module_config(temp_dir.path())
            .with_context(|| format!("No FOMOD config found in {}", mod_entry.name))?;

        info!("Found FOMOD config at: {}", config_path.display());

        // Parse FOMOD config
        let config = parse_fomod(&config_path)
            .with_context(|| format!("Failed to parse FOMOD config for {}", mod_entry.name))?;

        info!(
            "FOMOD {}: {} required files, {} steps, {} conditional patterns",
            config.module_name,
            config.required_files.len(),
            config.install_steps.len(),
            config.conditional_installs.len()
        );

        // Create mod destination directory
        let mod_dest = mods_dir.join(&mod_entry.folder_name);
        std::fs::create_dir_all(&mod_dest)?;

        // Execute FOMOD with choices
        let stats = execute_fomod(&data_root, &mod_dest, &config, &choices)
            .with_context(|| format!("FOMOD execution failed for {}", mod_entry.name))?;

        // Temp directory is automatically cleaned up when dropped
        Ok(stats)
    }
}

/// Static FOMOD processor for parallel execution
/// Each call creates its own temp directory for isolation
fn process_single_fomod_static(
    mod_entry: &ModDbEntry,
    mods_dir: &Path,
    temp_base_dir: &Path,
) -> Result<super::fomod::FomodStats> {
    use super::fomod::{execute_fomod, find_module_config, parse_fomod};

    // Get choices from database
    let choices = mod_entry
        .get_choices()
        .context("FOMOD mod has no choices")?;

    // Get archive path
    let archive_path = mod_entry
        .local_path
        .as_ref()
        .map(PathBuf::from)
        .context("FOMOD mod has no local archive path")?;

    if !archive_path.exists() {
        anyhow::bail!("Archive not found: {}", archive_path.display());
    }

    // Extract to temp directory (unique per mod)
    let temp_dir = tempfile::tempdir_in(temp_base_dir).context("Failed to create temp directory")?;

    super::extract::extract_archive_to_dir_pub(&archive_path, temp_dir.path())?;

    // Find ModuleConfig.xml in extracted archive
    let (config_path, data_root) = find_module_config(temp_dir.path())
        .with_context(|| format!("No FOMOD config found in {}", mod_entry.name))?;

    // Parse FOMOD config
    let config = parse_fomod(&config_path)
        .with_context(|| format!("Failed to parse FOMOD config for {}", mod_entry.name))?;

    // Create mod destination directory
    let mod_dest = mods_dir.join(&mod_entry.folder_name);
    std::fs::create_dir_all(&mod_dest)?;

    // Execute FOMOD with choices
    let stats = execute_fomod(&data_root, &mod_dest, &config, &choices)
        .with_context(|| format!("FOMOD execution failed for {}", mod_entry.name))?;

    Ok(stats)
}

impl CollectionInstaller {
    /// Phase G: Generate modlist.txt
    ///
    /// Uses ensemble sorting combining 4 methods:
    /// 1. DFS topological sort (from sinks, respects before/after)
    /// 2. Kahn's algorithm (topological sort with plugin tie-breaking)
    /// 3. Plugin order (LOOT-sorted plugin positions)
    /// 4. Collection order (original order from collection JSON)
    pub fn phase_generate_modlist(&mut self) -> Result<()> {
        use super::modlist::{ModInfo, ModListGenerator, ModRule as ModListRule};

        self.report_progress(InstallProgress::PhaseStarted {
            phase: InstallPhase::GenerateModlist,
            message: "Generating mod load order (ensemble sorting)...".to_string(),
        });

        let mo2 = self.mo2.as_ref().context("MO2 instance not set up")?;

        // Get all mods and filter to those that have been extracted (have a folder)
        // This handles the case where extraction succeeded but status wasn't updated
        let all_mods = self.db.get_all_mods()?;
        let db_mods: Vec<_> = all_mods
            .into_iter()
            .filter(|m| {
                // Include if status is Extracted/Installed OR folder exists
                let status_ok = matches!(m.status.as_str(), "extracted" | "installed");
                let folder_exists = mo2.mods_dir.join(&m.folder_name).exists();
                status_ok || folder_exists
            })
            .collect();
        let db_rules = self.db.get_mod_rules()?;

        info!(
            "Generating modlist.txt for {} mods with {} rules (ensemble sorting)",
            db_mods.len(),
            db_rules.len()
        );

        // Convert ModDbEntry to ModInfo for the generator
        let mods: Vec<ModInfo> = db_mods
            .iter()
            .map(|m| ModInfo {
                name: m.name.clone(),
                logical_filename: m.logical_filename.clone(),
                folder_name: m.folder_name.clone(),
                md5: m.md5.clone(),
            })
            .collect();

        // Convert ModRuleDbEntry to ModListRule for the generator
        let rules: Vec<ModListRule> = db_rules
            .iter()
            .map(|r| ModListRule {
                rule_type: r.rule_type.clone(),
                source_logical_name: r.source_filename.clone(),
                source_md5: r.source_md5.clone(),
                reference_logical_name: r.reference_filename.clone(),
                reference_md5: r.reference_md5.clone(),
            })
            .collect();

        // Get LOOT-sorted plugins for plugin position tie-breaking
        let db_plugins = self.db.get_plugins()?;
        let enabled_plugins: Vec<String> = db_plugins
            .iter()
            .filter(|p| p.enabled)
            .map(|p| p.name.clone())
            .collect();

        let sorted_plugins = self.sort_plugins_with_loot(
            &mo2.stock_game_dir,
            &mo2.mods_dir,
            &enabled_plugins,
        );

        // Generate mod order using ensemble sorting
        let mod_order = ModListGenerator::generate_mod_order_combined(
            &mods,
            &rules,
            &sorted_plugins,
            &mo2.mods_dir,
        );

        // Write modlist.txt
        let modlist_path = mo2.profiles_dir.join("Default").join("modlist.txt");
        ModListGenerator::write_modlist(&modlist_path, &mod_order)?;

        self.report_progress(InstallProgress::PhaseCompleted {
            phase: InstallPhase::GenerateModlist,
        });

        Ok(())
    }

    /// Phase H: Generate plugins.txt
    ///
    /// Uses LOOT for optimal plugin sorting based on:
    /// - Plugin master dependencies
    /// - LOOT masterlist rules
    /// Falls back to collection order if LOOT fails.
    pub fn phase_generate_plugins(&mut self) -> Result<()> {
        self.report_progress(InstallProgress::PhaseStarted {
            phase: InstallPhase::GeneratePlugins,
            message: "Generating plugin load order...".to_string(),
        });

        let mo2 = self.mo2.as_ref().context("MO2 instance not set up")?;

        // Get plugins from database
        let db_plugins = self.db.get_plugins()?;

        info!("Generating plugins.txt for {} plugins", db_plugins.len());

        // Base game plugins (always first, always enabled)
        let base_plugins = match self.game_type {
            GameType::SkyrimSE => vec![
                "Skyrim.esm",
                "Update.esm",
                "Dawnguard.esm",
                "HearthFires.esm",
                "Dragonborn.esm",
            ],
        };

        // Collect enabled plugin names for sorting
        let enabled_plugins: Vec<String> = db_plugins
            .iter()
            .filter(|p| p.enabled)
            .map(|p| p.name.clone())
            .collect();

        // Try LOOT sorting, fall back to collection order
        let sorted_plugins = self.sort_plugins_with_loot(&mo2.stock_game_dir, &mo2.mods_dir, &enabled_plugins);

        // Build plugins.txt (don't include base game ESMs - MO2 handles those)
        let plugins_path = mo2.profiles_dir.join("Default").join("plugins.txt");
        let mut content = String::from("# This file was automatically generated by NexusBridge\n");

        // Add sorted plugins (all enabled since we filtered above)
        // Skip base game plugins - MO2 adds them automatically
        for plugin in &sorted_plugins {
            let plugin_lower = plugin.to_lowercase();
            let is_base = base_plugins.iter().any(|b| b.to_lowercase() == plugin_lower);
            if !is_base {
                content.push_str(&format!("*{}\n", plugin));
            }
        }

        // Add disabled plugins at the end
        for plugin in &db_plugins {
            if !plugin.enabled {
                content.push_str(&format!("{}\n", plugin.name));
            }
        }

        std::fs::write(&plugins_path, &content)?;
        info!("Wrote plugins.txt to {}", plugins_path.display());

        // Also write loadorder.txt (without base game ESMs)
        let loadorder_path = mo2.profiles_dir.join("Default").join("loadorder.txt");
        let mut loadorder = String::new();

        for plugin in &sorted_plugins {
            let plugin_lower = plugin.to_lowercase();
            let is_base = base_plugins.iter().any(|b| b.to_lowercase() == plugin_lower);
            if !is_base {
                loadorder.push_str(&format!("{}\n", plugin));
            }
        }
        // Disabled plugins also go in loadorder.txt
        for plugin in &db_plugins {
            if !plugin.enabled {
                loadorder.push_str(&format!("{}\n", plugin.name));
            }
        }

        std::fs::write(&loadorder_path, loadorder)?;
        info!("Wrote loadorder.txt to {}", loadorder_path.display());

        self.report_progress(InstallProgress::PhaseCompleted {
            phase: InstallPhase::GeneratePlugins,
        });

        Ok(())
    }

    /// Sort plugins using LOOT, with fallback to original order.
    fn sort_plugins_with_loot(
        &self,
        game_path: &Path,
        mods_dir: &Path,
        plugin_names: &[String],
    ) -> Vec<String> {
        use crate::loot::PluginSorter;

        if plugin_names.is_empty() {
            return Vec::new();
        }

        info!("Attempting LOOT sorting for {} plugins...", plugin_names.len());

        // Try to create LOOT sorter
        let mut sorter: PluginSorter = match PluginSorter::new(self.game_type, game_path, mods_dir) {
            Ok(s) => s,
            Err(e) => {
                warn!("Failed to initialize LOOT: {}", e);
                warn!("Falling back to collection order");
                return plugin_names.to_vec();
            }
        };

        // Load the pre-downloaded masterlist
        let masterlist_path = match &self.masterlist_path {
            Some(path) => path.clone(),
            None => {
                error!("LOOT masterlist was not downloaded! This should not happen.");
                error!("Falling back to collection order - PLUGINS WILL BE IN WRONG ORDER!");
                return plugin_names.to_vec();
            }
        };

        if let Err(e) = sorter.load_masterlist(&masterlist_path) {
            error!("Failed to load LOOT masterlist from {}: {:#}", masterlist_path.display(), e);
            error!("Falling back to collection order - PLUGINS WILL BE IN WRONG ORDER!");
            return plugin_names.to_vec();
        }
        println!("LOOT masterlist loaded from: {}", masterlist_path.display());

        // Debug: show first few plugins before sorting
        println!("DEBUG: First 5 plugins BEFORE LOOT sort:");
        for (i, p) in plugin_names.iter().take(5).enumerate() {
            println!("  {}: {}", i, p);
        }

        // Try to sort
        let sorted: Vec<String> = match sorter.sort_all(plugin_names) {
            Ok(s) => s,
            Err(e) => {
                warn!("LOOT sorting failed: {}", e);
                warn!("Falling back to collection order");
                return plugin_names.to_vec();
            }
        };

        // Debug: show first few plugins after sorting
        println!("DEBUG: First 5 plugins AFTER LOOT sort:");
        for (i, p) in sorted.iter().take(5).enumerate() {
            println!("  {}: {}", i, p);
        }

        info!("LOOT sorted {} plugins successfully", sorted.len());
        sorted
    }

    /// Resume an interrupted installation
    pub async fn resume(&mut self) -> Result<()> {
        // Reset any stuck mods
        let reset_count = self.db.reset_stuck_mods()?;
        if reset_count > 0 {
            info!("Reset {} stuck mods", reset_count);
        }

        // Get current stats to determine where to resume
        let stats = self.db.get_mod_stats()?;
        info!(
            "Resuming installation: {}/{} installed, {} failed",
            stats.installed, stats.total, stats.failed
        );

        // Resume from appropriate phase
        if stats.pending > 0 || stats.downloading > 0 {
            self.phase_download_mods().await?;
        }

        if stats.downloaded > 0 {
            self.phase_validate_archives().await?;
        }

        if stats.extracted > 0 || stats.extracting > 0 {
            self.phase_extract_files().await?;
        }

        // Always regenerate load order files
        self.phase_generate_modlist()?;
        self.phase_generate_plugins()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_installer_config_default() {
        let config = InstallerConfig::default();
        // Should default to CPU thread count
        assert!(config.concurrent_downloads > 0);
    }

    #[tokio::test]
    async fn test_parse_collection_phase() {
        let temp_dir = TempDir::new().unwrap();

        // Create a test collection JSON
        let collection_json = r#"{
            "collectionName": "Test Collection",
            "domainName": "skyrimspecialedition",
            "mods": [
                {
                    "name": "Test Mod",
                    "folderName": "TestMod",
                    "logicalFilename": "TestMod.zip",
                    "source": {
                        "type": "nexus",
                        "modId": 1234,
                        "fileId": 5678
                    }
                }
            ],
            "plugins": [
                {"name": "TestMod.esp", "enabled": true}
            ]
        }"#;

        let collection_path = temp_dir.path().join("collection.json");
        std::fs::write(&collection_path, collection_json).unwrap();

        let config = InstallerConfig {
            collection_path,
            output_dir: temp_dir.path().to_path_buf(),
            ..Default::default()
        };

        let mut installer = CollectionInstaller::new_in_memory(config).unwrap();
        installer.phase_parse_collection().unwrap();

        let stats = installer.get_stats().unwrap();
        assert_eq!(stats.total, 1);
        assert_eq!(stats.pending, 1);

        // Check metadata was imported
        assert_eq!(
            installer.db().get_metadata("name").unwrap(),
            Some("Test Collection".to_string())
        );
    }

    #[test]
    fn test_install_phase_names() {
        assert_eq!(InstallPhase::ParseCollection.name(), "Parse Collection");
        assert_eq!(InstallPhase::DownloadMods.name(), "Download Mods");
        assert_eq!(InstallPhase::GeneratePlugins.name(), "Generate Plugin List");
    }
}
