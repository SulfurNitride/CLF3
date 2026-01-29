//! Mod Organizer 2 portable instance creation.
//!
//! This module handles:
//! - Downloading MO2 from GitHub releases
//! - Extracting the portable installation
//! - Creating Stock Game folder (copy of game files)
//! - Configuring ModOrganizer.ini for the game

mod downloader;
mod ini;
mod stock_game;

pub use downloader::{download_mo2, fetch_latest_mo2_release, GithubAsset, GithubRelease};
pub use ini::{create_profile, generate_ini, IniConfig};
pub use stock_game::{create_stock_game, verify_stock_game, CopyProgress};

use std::path::PathBuf;

/// MO2 portable instance paths.
#[derive(Debug, Clone)]
pub struct Mo2Instance {
    /// Root directory of the MO2 installation.
    pub root: PathBuf,
    /// Path to ModOrganizer.exe.
    pub executable: PathBuf,
    /// Path to ModOrganizer.ini.
    pub ini_path: PathBuf,
    /// Path to the mods directory.
    pub mods_dir: PathBuf,
    /// Path to the downloads directory.
    pub downloads_dir: PathBuf,
    /// Path to the profiles directory.
    pub profiles_dir: PathBuf,
    /// Path to the overwrite directory.
    pub overwrite_dir: PathBuf,
    /// Path to the Stock Game folder.
    pub stock_game_dir: PathBuf,
}

impl Mo2Instance {
    /// Creates path references for an MO2 instance at the given root.
    pub fn new(root: PathBuf) -> Self {
        Self {
            executable: root.join("ModOrganizer.exe"),
            ini_path: root.join("ModOrganizer.ini"),
            mods_dir: root.join("mods"),
            downloads_dir: root.join("downloads"),
            profiles_dir: root.join("profiles"),
            overwrite_dir: root.join("overwrite"),
            stock_game_dir: root.join("Stock Game"),
            root,
        }
    }

    /// Returns the path for a specific mod folder.
    pub fn mod_path(&self, mod_name: &str) -> PathBuf {
        self.mods_dir.join(mod_name)
    }

    /// Returns the path for a specific profile.
    pub fn profile_path(&self, profile_name: &str) -> PathBuf {
        self.profiles_dir.join(profile_name)
    }

    /// Returns the modlist.txt path for a profile.
    pub fn modlist_path(&self, profile_name: &str) -> PathBuf {
        self.profile_path(profile_name).join("modlist.txt")
    }

    /// Returns the plugins.txt path for a profile.
    pub fn plugins_path(&self, profile_name: &str) -> PathBuf {
        self.profile_path(profile_name).join("plugins.txt")
    }

    /// Returns the loadorder.txt path for a profile.
    pub fn loadorder_path(&self, profile_name: &str) -> PathBuf {
        self.profile_path(profile_name).join("loadorder.txt")
    }

    /// Creates all necessary directories for the instance.
    pub fn create_directories(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.mods_dir)?;
        std::fs::create_dir_all(&self.downloads_dir)?;
        std::fs::create_dir_all(&self.profiles_dir)?;
        std::fs::create_dir_all(&self.overwrite_dir)?;
        std::fs::create_dir_all(&self.stock_game_dir)?;
        Ok(())
    }

    /// Checks if the instance appears to be valid (has ModOrganizer.exe).
    pub fn is_valid(&self) -> bool {
        self.executable.exists()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mo2_instance_paths() {
        let instance = Mo2Instance::new(PathBuf::from("/test/mo2"));

        assert_eq!(instance.executable, PathBuf::from("/test/mo2/ModOrganizer.exe"));
        assert_eq!(instance.ini_path, PathBuf::from("/test/mo2/ModOrganizer.ini"));
        assert_eq!(instance.mods_dir, PathBuf::from("/test/mo2/mods"));
        assert_eq!(instance.downloads_dir, PathBuf::from("/test/mo2/downloads"));
        assert_eq!(instance.stock_game_dir, PathBuf::from("/test/mo2/Stock Game"));
    }

    #[test]
    fn test_mod_and_profile_paths() {
        let instance = Mo2Instance::new(PathBuf::from("/test/mo2"));

        assert_eq!(
            instance.mod_path("SkyUI"),
            PathBuf::from("/test/mo2/mods/SkyUI")
        );
        assert_eq!(
            instance.profile_path("Default"),
            PathBuf::from("/test/mo2/profiles/Default")
        );
        assert_eq!(
            instance.modlist_path("Default"),
            PathBuf::from("/test/mo2/profiles/Default/modlist.txt")
        );
        assert_eq!(
            instance.plugins_path("Default"),
            PathBuf::from("/test/mo2/profiles/Default/plugins.txt")
        );
    }
}
