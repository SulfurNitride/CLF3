//! Settings management for CLF3 GUI
//!
//! Stores user preferences in ~/.config/clf3/settings.json

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// User settings for CLF3
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Settings {
    /// Default installation directory
    #[serde(default)]
    pub default_install_dir: String,

    /// Default downloads directory
    #[serde(default)]
    pub default_downloads_dir: String,

    /// Nexus Mods API key
    #[serde(default)]
    pub nexus_api_key: String,

    /// Selected GPU index (None = auto-select)
    #[serde(default)]
    pub gpu_index: Option<usize>,

    /// GPU name for display (informational only)
    #[serde(default)]
    pub gpu_name: String,
}

impl Settings {
    /// Get the config directory path (~/.config/clf3)
    fn config_dir() -> Result<PathBuf> {
        let config_dir = dirs::config_dir()
            .context("Could not determine config directory")?
            .join("clf3");

        Ok(config_dir)
    }

    /// Get the settings file path
    fn settings_path() -> Result<PathBuf> {
        Ok(Self::config_dir()?.join("settings.json"))
    }

    /// Load settings from disk, or return defaults if not found
    pub fn load() -> Self {
        match Self::try_load() {
            Ok(settings) => settings,
            Err(e) => {
                eprintln!("Could not load settings: {}. Using defaults.", e);
                Self::default()
            }
        }
    }

    /// Try to load settings, returning error on failure
    fn try_load() -> Result<Self> {
        let path = Self::settings_path()?;

        if !path.exists() {
            return Ok(Self::default());
        }

        let content = std::fs::read_to_string(&path)
            .with_context(|| format!("Failed to read {:?}", path))?;

        let settings: Self = serde_json::from_str(&content)
            .with_context(|| format!("Failed to parse {:?}", path))?;

        Ok(settings)
    }

    /// Save settings to disk
    pub fn save(&self) -> Result<()> {
        let config_dir = Self::config_dir()?;
        std::fs::create_dir_all(&config_dir)
            .with_context(|| format!("Failed to create {:?}", config_dir))?;

        let path = Self::settings_path()?;
        let content = serde_json::to_string_pretty(self)
            .context("Failed to serialize settings")?;

        std::fs::write(&path, content)
            .with_context(|| format!("Failed to write {:?}", path))?;

        Ok(())
    }

    /// Check if any defaults are set
    pub fn has_defaults(&self) -> bool {
        !self.default_install_dir.is_empty()
            || !self.default_downloads_dir.is_empty()
            || !self.nexus_api_key.is_empty()
    }
}

/// Get available GPUs for selection
pub fn get_available_gpus() -> Vec<(usize, String)> {
    crate::textures::list_gpus()
        .into_iter()
        .map(|gpu| (gpu.adapter_index, format!("{} ({}, {})", gpu.name, gpu.backend, gpu.device_type)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_settings_default() {
        let settings = Settings::default();
        assert!(settings.default_install_dir.is_empty());
        assert!(settings.nexus_api_key.is_empty());
        assert!(settings.gpu_index.is_none());
    }

    #[test]
    fn test_settings_serialize() {
        let settings = Settings {
            default_install_dir: "/home/user/Games".into(),
            default_downloads_dir: "/home/user/Downloads".into(),
            nexus_api_key: "test_key".into(),
            gpu_index: Some(0),
            gpu_name: "Test GPU".into(),
        };

        let json = serde_json::to_string(&settings).unwrap();
        let loaded: Settings = serde_json::from_str(&json).unwrap();

        assert_eq!(loaded.default_install_dir, settings.default_install_dir);
        assert_eq!(loaded.gpu_index, Some(0));
    }
}
