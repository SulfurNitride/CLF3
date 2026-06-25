//! Installation configuration
//!
//! Defines the configuration structure for modlist installation.

use super::progress::ProgressReporter;
use serde::Serialize;
use std::path::PathBuf;
use std::sync::Arc;

/// How the install pipeline schedules download vs. extraction work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtractStrategy {
    /// Extract archives incrementally as each one finishes downloading.
    /// Network and CPU run concurrently — best when the download dominates
    /// wall-clock (large modlists, slow connections).
    Streaming,
    /// Wait for all downloads to finish, then run 4 sequential extraction
    /// phases at full CPU. Best for small modlists where download is short
    /// and the CPU-heavy phases (DDS, BSA build) dominate.
    Phased,
}

impl Default for ExtractStrategy {
    fn default() -> Self {
        ExtractStrategy::Streaming
    }
}

/// Progress callback type for reporting download/installation progress
pub type ProgressCallback = Arc<dyn Fn(ProgressEvent) + Send + Sync>;

/// Events reported during installation for progress tracking
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
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

    /// Nexus API key (required for Nexus download links unless an OAuth token is provided)
    pub nexus_api_key: String,

    /// Nexus OAuth bearer token. Takes precedence over nexus_api_key when present.
    pub nexus_oauth_token: Option<String>,

    /// Maximum concurrent downloads
    pub max_concurrent_downloads: usize,

    /// Maximum parallel workers for archive extraction/install phase
    pub max_install_workers: usize,

    /// Maximum number of BSA/BA2 archives processed concurrently.
    /// Keep this low because each BSA extraction uses rayon internally.
    pub max_parallel_bsa_archives: usize,

    /// Maximum number of 7z archives processed concurrently.
    /// Each 7z archive runs in its own external 7z process.
    pub max_parallel_7z_archives: usize,

    /// Optional directory to persist patched outputs by hash for reuse
    pub patch_cache_dir: Option<PathBuf>,

    /// Optional callback for progress reporting (legacy — being replaced by reporter)
    pub progress_callback: Option<ProgressCallback>,

    /// Unified progress reporter (CLI or GUI implementation)
    pub reporter: Arc<dyn ProgressReporter>,

    /// LoversLab email for automated downloads (empty = manual)
    pub loverslab_email: String,

    /// LoversLab password for automated downloads (empty = manual)
    pub loverslab_password: String,

    /// Extraction scheduling strategy. Defaults to `Streaming` so large
    /// modlists overlap CPU extract work with network downloads.
    pub extract_strategy: ExtractStrategy,

    /// Optional gallery `machine_name` for this modlist. When set, the
    /// installer writes a `.clf3-install.json` manifest and updates the
    /// settings record on success so `clf3 modlist check` can find it.
    pub machine_name: Option<String>,

    /// Optional URL the .wabbajack file was originally downloaded from.
    /// Recorded in the post-install manifest so `clf3 modlist update` can
    /// fall back to it when the gallery entry has moved.
    pub wabbajack_url: Option<String>,
}

impl std::fmt::Debug for InstallConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstallConfig")
            .field("wabbajack_path", &self.wabbajack_path)
            .field("output_dir", &self.output_dir)
            .field("downloads_dir", &self.downloads_dir)
            .field("game_dir", &self.game_dir)
            .field("nexus_api_key", &"[REDACTED]")
            .field(
                "nexus_oauth_token",
                &self.nexus_oauth_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field("max_concurrent_downloads", &self.max_concurrent_downloads)
            .field("max_install_workers", &self.max_install_workers)
            .field("max_parallel_bsa_archives", &self.max_parallel_bsa_archives)
            .field("max_parallel_7z_archives", &self.max_parallel_7z_archives)
            .field("patch_cache_dir", &self.patch_cache_dir)
            .field(
                "progress_callback",
                &self.progress_callback.as_ref().map(|_| "<callback>"),
            )
            .field("reporter", &"<reporter>")
            .field("loverslab_email", &self.loverslab_email)
            .field("loverslab_password", &"[REDACTED]")
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

        let has_api_key = !self.nexus_api_key.trim().is_empty();
        let has_oauth_token = self
            .nexus_oauth_token
            .as_deref()
            .map(|token| !token.trim().is_empty())
            .unwrap_or(false);
        if !has_api_key && !has_oauth_token {
            return Err(ConfigError::MissingNexusKey);
        }
        if self.max_concurrent_downloads == 0 {
            return Err(ConfigError::InvalidConcurrency(
                "max_concurrent_downloads must be >= 1",
            ));
        }
        if self.max_install_workers == 0 {
            return Err(ConfigError::InvalidConcurrency(
                "max_install_workers must be >= 1",
            ));
        }
        if self.max_parallel_bsa_archives == 0 {
            return Err(ConfigError::InvalidConcurrency(
                "max_parallel_bsa_archives must be >= 1",
            ));
        }
        if self.max_parallel_7z_archives == 0 {
            return Err(ConfigError::InvalidConcurrency(
                "max_parallel_7z_archives must be >= 1",
            ));
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

    #[error("Nexus API key or OAuth token is required (premium account needed)")]
    MissingNexusKey,

    #[error("Invalid concurrency setting: {0}")]
    InvalidConcurrency(&'static str),
}
