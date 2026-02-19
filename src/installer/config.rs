//! Installation configuration
//!
//! Defines the configuration structure for modlist installation.

use std::path::PathBuf;
use std::sync::Arc;

/// Progress callback type for reporting download/installation progress
pub type ProgressCallback = Arc<dyn Fn(ProgressEvent) + Send + Sync>;

/// Events reported during installation for progress tracking
#[derive(Debug, Clone)]
#[allow(dead_code)] // Fields are read in the GUI (lib crate) but not by the binary crate
pub enum ProgressEvent {
    /// Download progress update
    DownloadProgress {
        name: String,
        downloaded: u64,
        total: u64,
        /// Bytes per second
        speed: f64,
    },
    /// A download has completed
    DownloadComplete { name: String },
    /// An archive has been processed (downloaded or skipped)
    ArchiveComplete {
        /// 1-based index of the completed archive
        index: usize,
        /// Total number of archives
        total: usize,
    },
    /// Archives were skipped (already downloaded)
    DownloadSkipped {
        /// Number of archives skipped
        count: usize,
        /// Total size of skipped archives in bytes
        total_size: u64,
    },
    /// Installation phase changed (e.g., "Downloading", "Extracting", "Installing")
    PhaseChange { phase: String },
    /// A directive completed
    DirectiveComplete { index: usize, total: usize },
    /// Status message update
    Status { message: String },
    /// Directive processing phase started (e.g., FromArchive, PatchedFromArchive)
    DirectivePhaseStarted {
        /// Type of directive being processed
        directive_type: String,
        /// Total directives of this type to process
        total: usize,
    },
}

/// Configuration for a modlist installation
#[derive(Clone)]
pub struct InstallConfig {
    /// Path to the .wabbajack file
    pub wabbajack_path: PathBuf,

    /// Installation target directory (where mods go)
    pub output_dir: PathBuf,

    /// Directory for downloaded archives
    pub downloads_dir: PathBuf,

    /// Game installation directory (for GameFileSource)
    pub game_dir: PathBuf,

    /// Nexus API key (required for download links)
    pub nexus_api_key: String,

    /// Maximum concurrent downloads
    pub max_concurrent_downloads: usize,

    /// Use NXM browser mode instead of direct API
    pub nxm_mode: bool,

    /// Browser command to open Nexus pages
    pub browser: String,

    /// Optional directory to persist patched outputs by hash for reuse
    pub patch_cache_dir: Option<PathBuf>,

    /// Optional callback for progress reporting
    pub progress_callback: Option<ProgressCallback>,
}

impl std::fmt::Debug for InstallConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstallConfig")
            .field("wabbajack_path", &self.wabbajack_path)
            .field("output_dir", &self.output_dir)
            .field("downloads_dir", &self.downloads_dir)
            .field("game_dir", &self.game_dir)
            .field("nexus_api_key", &"[REDACTED]")
            .field("max_concurrent_downloads", &self.max_concurrent_downloads)
            .field("nxm_mode", &self.nxm_mode)
            .field("browser", &self.browser)
            .field("patch_cache_dir", &self.patch_cache_dir)
            .field(
                "progress_callback",
                &self.progress_callback.as_ref().map(|_| "<callback>"),
            )
            .finish()
    }
}

impl InstallConfig {
    /// Get the path to the state database
    /// Uses local cache directory (~/.cache/clf3/) to avoid network filesystem issues
    pub fn db_path(&self) -> PathBuf {
        // Use modlist name for the database filename
        let modlist_name = self
            .wabbajack_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown");

        // Store in local cache directory to avoid CIFS/NFS locking issues
        let cache_dir = dirs::cache_dir()
            .unwrap_or_else(|| PathBuf::from("/tmp"))
            .join("clf3");

        // Create cache directory if it doesn't exist
        let _ = std::fs::create_dir_all(&cache_dir);

        cache_dir.join(format!("{}.db", modlist_name))
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<(), ConfigError> {
        if !self.wabbajack_path.exists() {
            return Err(ConfigError::WabbajackNotFound(self.wabbajack_path.clone()));
        }

        if !self.game_dir.exists() {
            return Err(ConfigError::GameDirNotFound(self.game_dir.clone()));
        }

        if self.nexus_api_key.is_empty() {
            return Err(ConfigError::MissingNexusKey);
        }

        Ok(())
    }
}

/// Configuration errors
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("Wabbajack file not found: {0}")]
    WabbajackNotFound(PathBuf),

    #[error("Game directory not found: {0}")]
    GameDirNotFound(PathBuf),

    #[error("Nexus API key is required (premium account needed)")]
    MissingNexusKey,
}
