//! LOOT integration for plugin sorting.
//!
//! Uses libloot to sort plugin load order based on:
//! - Plugin master dependencies
//! - LOOT masterlist rules
//! - User-defined metadata
//!
//! # Example
//!
//! ```ignore
//! use clf3::loot::PluginSorter;
//! use clf3::games::GameType;
//!
//! let sorter = PluginSorter::new(GameType::SkyrimSE, &game_path, &mods_dir)?;
//! let sorted = sorter.sort_plugins(&plugin_names)?;
//! ```

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use tracing::{debug, info, warn};
use walkdir::WalkDir;

use crate::games::GameType;

/// URLs for LOOT masterlists by game type
const SKYRIMSE_MASTERLIST_URL: &str =
    "https://raw.githubusercontent.com/loot/skyrimse/refs/heads/v0.26/masterlist.yaml";

/// Plugin sorter using libloot.
pub struct PluginSorter {
    game: libloot::Game,
    game_type: GameType,
    mods_dir: PathBuf,
    game_data_dir: PathBuf,
    masterlist_loaded: bool,
}

impl PluginSorter {
    /// Create a new plugin sorter for the given game.
    ///
    /// # Arguments
    /// * `game_type` - The game type (e.g., SkyrimSE)
    /// * `game_path` - Path to the game installation (e.g., Stock Game folder)
    /// * `mods_dir` - Path to MO2 mods directory
    pub fn new(game_type: GameType, game_path: &Path, mods_dir: &Path) -> Result<Self> {
        let libloot_game_type = match game_type {
            GameType::SkyrimSE => libloot::GameType::SkyrimSE,
            // Add more game types as needed
        };

        // Try to find local app data path for the game
        let local_path = find_local_app_data(game_type);

        let game = if let Some(ref local) = local_path {
            info!("Using local app data: {}", local.display());
            libloot::Game::with_local_path(libloot_game_type, game_path, local)
                .context("Failed to create LOOT game handle with local path")?
        } else {
            info!("No local app data found, using default");
            libloot::Game::new(libloot_game_type, game_path)
                .context("Failed to create LOOT game handle")?
        };

        let game_data_dir = game_path.join("Data");

        Ok(Self {
            game,
            game_type,
            mods_dir: mods_dir.to_path_buf(),
            game_data_dir,
            masterlist_loaded: false,
        })
    }

    /// Load the LOOT masterlist for proper plugin sorting.
    ///
    /// The masterlist contains sorting rules for thousands of plugins.
    /// Without it, LOOT only sorts based on master dependencies.
    ///
    /// # Arguments
    /// * `masterlist_path` - Path to the masterlist.yaml file
    pub fn load_masterlist(&mut self, masterlist_path: &Path) -> Result<()> {
        if !masterlist_path.exists() {
            return Err(anyhow!(
                "Masterlist not found at: {}",
                masterlist_path.display()
            ));
        }

        info!("Loading LOOT masterlist from: {}", masterlist_path.display());

        let database = self.game.database();
        let mut db = database
            .write()
            .map_err(|e| anyhow!("Failed to acquire database lock: {}", e))?;

        db.load_masterlist(masterlist_path)
            .context("Failed to load masterlist")?;

        self.masterlist_loaded = true;
        info!("LOOT masterlist loaded successfully");
        Ok(())
    }

    /// Download the masterlist from GitHub and save it to the specified path.
    ///
    /// Returns the path where the masterlist was saved.
    pub async fn download_masterlist(&self, cache_dir: &Path) -> Result<PathBuf> {
        let url = match self.game_type {
            GameType::SkyrimSE => SKYRIMSE_MASTERLIST_URL,
        };

        std::fs::create_dir_all(cache_dir)?;
        let masterlist_path = cache_dir.join("masterlist.yaml");

        info!("Downloading LOOT masterlist from: {}", url);

        let response = reqwest::get(url).await.context("Failed to download masterlist")?;

        if !response.status().is_success() {
            return Err(anyhow!(
                "Failed to download masterlist: HTTP {}",
                response.status()
            ));
        }

        let content = response
            .text()
            .await
            .context("Failed to read masterlist response")?;

        std::fs::write(&masterlist_path, &content).context("Failed to save masterlist")?;

        info!(
            "Masterlist downloaded and saved to: {}",
            masterlist_path.display()
        );
        Ok(masterlist_path)
    }

    /// Get the default cache directory for masterlists.
    pub fn default_masterlist_cache_dir(&self) -> PathBuf {
        let cache_base = dirs::cache_dir().unwrap_or_else(|| PathBuf::from(".cache"));
        let game_folder = match self.game_type {
            GameType::SkyrimSE => "skyrimse",
        };
        cache_base.join("clf3").join("loot").join(game_folder)
    }

    /// Load masterlist, downloading if necessary.
    ///
    /// Checks for a cached masterlist first, downloads if not found or if force_download is true.
    pub async fn ensure_masterlist(&mut self, force_download: bool) -> Result<()> {
        let cache_dir = self.default_masterlist_cache_dir();
        let masterlist_path = cache_dir.join("masterlist.yaml");

        // Check if we need to download
        let needs_download = force_download || !masterlist_path.exists();

        if needs_download {
            self.download_masterlist(&cache_dir).await?;
        } else {
            debug!(
                "Using cached masterlist at: {}",
                masterlist_path.display()
            );
        }

        self.load_masterlist(&masterlist_path)
    }

    /// Set additional data paths from MO2 mod folders.
    ///
    /// This allows LOOT to find plugins in mod folders, simulating
    /// MO2's virtual filesystem.
    pub fn set_mod_paths(&mut self) -> Result<()> {
        let mut mod_paths = Vec::new();

        if self.mods_dir.exists() {
            for entry in std::fs::read_dir(&self.mods_dir)? {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    mod_paths.push(entry.path());
                }
            }
        }

        if !mod_paths.is_empty() {
            info!("Setting {} additional data paths for LOOT", mod_paths.len());
            self.game
                .set_additional_data_paths(mod_paths)
                .context("Failed to set additional data paths")?;
        }

        Ok(())
    }

    /// Find plugin files and load their headers.
    ///
    /// Searches for plugins in:
    /// 1. MO2 mods directory (first match wins, like MO2 priority)
    /// 2. Game Data folder
    pub fn load_plugins(&mut self, plugin_names: &[String]) -> Result<Vec<String>> {
        let mut plugin_paths = Vec::new();
        let mut found_plugins: HashSet<String> = HashSet::new();
        let mut loaded_names = Vec::new();

        for plugin_name in plugin_names {
            let name_lower = plugin_name.to_lowercase();

            // Skip if already found
            if found_plugins.contains(&name_lower) {
                continue;
            }

            // Search in mods directory first (MO2 priority order)
            let mut found = false;
            if self.mods_dir.exists() {
                for entry in std::fs::read_dir(&self.mods_dir)? {
                    let entry = entry?;
                    if !entry.file_type()?.is_dir() {
                        continue;
                    }

                    let plugin_path = entry.path().join(plugin_name);
                    if plugin_path.exists() {
                        plugin_paths.push(plugin_path);
                        found_plugins.insert(name_lower.clone());
                        loaded_names.push(plugin_name.clone());
                        found = true;
                        break;
                    }
                }
            }

            // Try game Data folder if not found in mods
            if !found {
                let game_plugin_path = self.game_data_dir.join(plugin_name);
                if game_plugin_path.exists() {
                    plugin_paths.push(game_plugin_path);
                    found_plugins.insert(name_lower);
                    loaded_names.push(plugin_name.clone());
                }
            }
        }

        println!(
            "DEBUG: Found {} of {} plugins, loading headers...",
            plugin_paths.len(),
            plugin_names.len()
        );
        info!(
            "Loading {} plugin headers for LOOT sorting...",
            plugin_paths.len()
        );

        // Convert to Path references for libloot
        let path_refs: Vec<&Path> = plugin_paths.iter().map(|p| p.as_path()).collect();

        self.game
            .load_plugin_headers(&path_refs)
            .context("Failed to load plugin headers")?;

        Ok(loaded_names)
    }

    /// Sort plugins using LOOT.
    ///
    /// Returns the sorted plugin names.
    pub fn sort_plugins(&self, plugin_names: &[String]) -> Result<Vec<String>> {
        let name_refs: Vec<&str> = plugin_names.iter().map(|s| s.as_str()).collect();

        let sorted = self
            .game
            .sort_plugins(&name_refs)
            .context("LOOT plugin sorting failed")?;

        info!("LOOT sorted {} plugins", sorted.len());
        Ok(sorted)
    }

    /// Full sorting workflow: set paths, load plugins, sort.
    ///
    /// This is a convenience method that combines all steps.
    ///
    /// **Requires**: Call `load_masterlist()` or `ensure_masterlist()` first.
    /// Without the masterlist, sorting will fail.
    pub fn sort_all(&mut self, plugin_names: &[String]) -> Result<Vec<String>> {
        println!("DEBUG sort_all: masterlist_loaded = {}", self.masterlist_loaded);
        if !self.masterlist_loaded {
            return Err(anyhow!(
                "LOOT masterlist not loaded! Call load_masterlist() or ensure_masterlist() first. \
                Without the masterlist, plugins cannot be sorted correctly."
            ));
        }

        // Set mod paths for LOOT
        println!("DEBUG sort_all: setting mod paths...");
        self.set_mod_paths()?;

        // Load plugins that exist
        println!("DEBUG sort_all: loading {} plugins...", plugin_names.len());
        let loaded = self.load_plugins(plugin_names)?;
        println!("DEBUG sort_all: loaded {} plugins", loaded.len());

        if loaded.is_empty() {
            warn!("No plugins found to sort");
            return Ok(Vec::new());
        }

        // Sort loaded plugins
        println!("DEBUG sort_all: sorting...");
        let result = self.sort_plugins(&loaded)?;
        println!("DEBUG sort_all: sorted {} plugins", result.len());
        Ok(result)
    }

    /// Check if the masterlist has been loaded.
    pub fn is_masterlist_loaded(&self) -> bool {
        self.masterlist_loaded
    }
}

/// Find the local app data path for a game.
///
/// For Steam/Proton on Linux, this is typically in the Proton prefix.
fn find_local_app_data(game_type: GameType) -> Option<PathBuf> {
    let home = std::env::var("HOME").ok()?;

    match game_type {
        GameType::SkyrimSE => {
            // Steam Proton prefix for Skyrim SE (Steam App ID 489830)
            let proton_paths = [
                // Standard Steam location
                format!(
                    "{}/.steam/steam/steamapps/compatdata/489830/pfx/drive_c/users/steamuser/AppData/Local/Skyrim Special Edition",
                    home
                ),
                // Alternative Steam location
                format!(
                    "{}/.local/share/Steam/steamapps/compatdata/489830/pfx/drive_c/users/steamuser/AppData/Local/Skyrim Special Edition",
                    home
                ),
                // Flatpak Steam
                format!(
                    "{}/.var/app/com.valvesoftware.Steam/.local/share/Steam/steamapps/compatdata/489830/pfx/drive_c/users/steamuser/AppData/Local/Skyrim Special Edition",
                    home
                ),
            ];

            for path in &proton_paths {
                let p = PathBuf::from(path);
                if p.exists() {
                    return Some(p);
                }
            }

            None
        }
    }
}

/// Discover all plugins in a mods directory.
///
/// Returns a list of unique plugin filenames found across all mod folders.
pub fn discover_plugins(mods_dir: &Path) -> Result<Vec<String>> {
    let mut plugins: HashSet<String> = HashSet::new();

    for entry in WalkDir::new(mods_dir)
        .min_depth(2)
        .max_depth(2)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        if !entry.file_type().is_file() {
            continue;
        }

        let path = entry.path();
        if let Some(ext) = path.extension() {
            let ext_lower = ext.to_string_lossy().to_lowercase();
            if ext_lower == "esp" || ext_lower == "esm" || ext_lower == "esl" {
                if let Some(name) = path.file_name() {
                    plugins.insert(name.to_string_lossy().to_string());
                }
            }
        }
    }

    let mut result: Vec<String> = plugins.into_iter().collect();
    result.sort();
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_local_app_data_nonexistent() {
        // This will return None on most systems without the game installed
        let result = find_local_app_data(GameType::SkyrimSE);
        // Just verify it doesn't panic
        let _ = result;
    }

    #[test]
    fn test_discover_plugins_empty() {
        let temp = tempfile::tempdir().unwrap();
        let result = discover_plugins(temp.path()).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_discover_plugins() {
        let temp = tempfile::tempdir().unwrap();

        // Create mod folder structure
        let mod1 = temp.path().join("Mod1");
        std::fs::create_dir_all(&mod1).unwrap();
        std::fs::write(mod1.join("test1.esp"), "").unwrap();
        std::fs::write(mod1.join("test2.esm"), "").unwrap();

        let mod2 = temp.path().join("Mod2");
        std::fs::create_dir_all(&mod2).unwrap();
        std::fs::write(mod2.join("test3.esl"), "").unwrap();
        std::fs::write(mod2.join("readme.txt"), "").unwrap(); // Should be ignored

        let result = discover_plugins(temp.path()).unwrap();
        assert_eq!(result.len(), 3);
        assert!(result.contains(&"test1.esp".to_string()));
        assert!(result.contains(&"test2.esm".to_string()));
        assert!(result.contains(&"test3.esl".to_string()));
    }
}
