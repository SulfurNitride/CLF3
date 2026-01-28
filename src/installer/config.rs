//! Installation configuration
//!
//! Defines the configuration structure for modlist installation.

use std::path::PathBuf;

/// Configuration for a modlist installation
#[derive(Debug, Clone)]
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

    /// Port for NXM handler server
    pub nxm_port: u16,

    /// Browser command to open Nexus pages
    pub browser: String,
}

impl InstallConfig {
    /// Get the path to the state database
    pub fn db_path(&self) -> PathBuf {
        self.downloads_dir.join(".clf3_state.db")
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
