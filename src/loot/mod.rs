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
// Masterlist branch tracks libloot's ABI version, NOT the LOOT app version.
// Our `libloot` crate dep is 0.29.x, so the matching masterlist branch is
// v0.29. Loading a v0.26 masterlist with libloot 0.29 produces partially-
// applied rules and bad tie-breaking.
const SKYRIMSE_MASTERLIST_URL: &str =
    "https://raw.githubusercontent.com/loot/skyrimse/refs/heads/v0.29/masterlist.yaml";

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
    /// Checks for a cached masterlist first, downloads if missing, if
    /// `force_download` is set, or if the cached copy is older than
    /// `MASTERLIST_MAX_AGE`. The age check matters because LOOT's
    /// masterlist gets multiple updates a week — a stale cache produces
    /// noticeably different load orders than a fresh Fluorine install.
    pub async fn ensure_masterlist(&mut self, force_download: bool) -> Result<()> {
        const MASTERLIST_MAX_AGE: std::time::Duration =
            std::time::Duration::from_secs(7 * 24 * 60 * 60); // 7 days

        let cache_dir = self.default_masterlist_cache_dir();
        let masterlist_path = cache_dir.join("masterlist.yaml");

        let stale = match std::fs::metadata(&masterlist_path).and_then(|m| m.modified()) {
            Ok(mtime) => match mtime.elapsed() {
                Ok(age) => age > MASTERLIST_MAX_AGE,
                // Clock skew (mtime in the future) — treat as fresh.
                Err(_) => false,
            },
            Err(_) => true, // Missing file — needs download.
        };

        let needs_download = force_download || !masterlist_path.exists() || stale;

        if needs_download {
            if stale && masterlist_path.exists() {
                info!("LOOT masterlist is stale (>7 days old) — refreshing");
            }
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
/// Looks at `mods_dir/<mod>/<plugin>` only (depth exactly 2). Mirrors what
/// MO2 / Fluorine's VFS actually deploys: plugins below the mod root
/// (e.g. inside an `Optional/` subdir an FOMOD installer left behind) are
/// NOT load-order participants, so listing them in plugins.txt only earns
/// us "Plugin not found" warnings. `flatten_wrapper_dir` already pulled
/// any `Data/` wrapper up before we got here.
pub fn discover_plugins(mods_dir: &Path) -> Result<Vec<String>> {
    let plugins = discover_plugins_with_owner(mods_dir)?;
    let mut names: Vec<String> = plugins
        .into_iter()
        .map(|(name, _owner)| name)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    names.sort();
    Ok(names)
}

/// Same as `discover_plugins`, but also reports which mod folder each
/// plugin came from. Lets callers filter out plugins that ship in mods
/// disabled in `modlist.txt` (Vortex `recommends`-mapped optional mods)
/// before writing `plugins.txt` — those plugins can't load anyway because
/// the mod isn't part of the VFS.
pub fn discover_plugins_with_owner(mods_dir: &Path) -> Result<Vec<(String, String)>> {
    let mut out = Vec::new();
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
        let Some(ext) = path.extension() else { continue };
        let ext_lower = ext.to_string_lossy().to_lowercase();
        if !matches!(ext_lower.as_str(), "esp" | "esm" | "esl") {
            continue;
        }
        let Some(name) = path.file_name() else { continue };
        let Some(owner) = path
            .parent()
            .and_then(|p| p.file_name())
            .map(|n| n.to_string_lossy().into_owned())
        else {
            continue;
        };
        out.push((name.to_string_lossy().into_owned(), owner));
    }
    Ok(out)
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

    /// Run our LOOT path on the same plugin set Fluorine sorted, then diff
    /// the two outputs. Validates whether our PluginSorter produces the
    /// same order Fluorine's bundled libloot does — separates "stale
    /// masterlist" (already fixed) from "we're calling libloot wrong".
    ///
    /// Skipped unless the user's IAP install is present. Run with:
    ///   cargo test -- --ignored loot_diff_against_fluorine_plugins_txt
    #[test]
    #[ignore]
    fn loot_diff_against_fluorine_plugins_txt() {
        let mods_dir = std::path::Path::new("/home/luke/Games/IAP/mods");
        let game_path = std::path::Path::new(
            "/home/luke/.local/share/Steam/steamapps/common/Skyrim Special Edition",
        );
        let fluorine_plugins =
            std::path::Path::new("/home/luke/Games/IAP/profiles/Default/plugins.txt");
        let our_plugins =
            std::path::Path::new("/home/luke/Games/IAP/profiles/Default/plugins copy.txt");

        if !mods_dir.is_dir() || !game_path.is_dir() {
            eprintln!("[skip] IAP install not present at {}", mods_dir.display());
            return;
        }

        // Read the plugin set from "copy.txt" (the input set Fluorine saw).
        let read_set = |p: &std::path::Path| -> Vec<String> {
            std::fs::read_to_string(p)
                .unwrap()
                .lines()
                .filter_map(|l| {
                    let l = l.trim();
                    if l.is_empty() || l.starts_with('#') {
                        return None;
                    }
                    let name = l.trim_start_matches(['*', '+']);
                    Some(name.to_string())
                })
                .collect()
        };
        let input_set = read_set(our_plugins);
        let fluorine_order = read_set(fluorine_plugins);
        eprintln!(
            "Input set: {} plugins | Fluorine order: {} plugins",
            input_set.len(),
            fluorine_order.len()
        );

        // Run our LOOT path end to end, exactly like the install pipeline does.
        let rt = tokio::runtime::Runtime::new().unwrap();
        let sorted = rt.block_on(async {
            let mut sorter =
                PluginSorter::new(GameType::SkyrimSE, game_path, mods_dir).unwrap();
            // Force a refresh so we're testing against current rules, not a
            // cached masterlist.
            sorter.ensure_masterlist(true).await.unwrap();
            sorter.set_mod_paths().unwrap();
            let loaded = sorter.load_plugins(&input_set).unwrap();
            sorter.sort_plugins(&loaded).unwrap()
        });
        eprintln!("Our LOOT sorted: {} plugins", sorted.len());

        // Compute first divergence between our order and Fluorine's order
        // (case-insensitive — Fluorine stores some entries lowercased).
        let lc = |v: &[String]| -> Vec<String> { v.iter().map(|s| s.to_lowercase()).collect() };
        let ours_lc = lc(&sorted);
        let theirs_lc = lc(&fluorine_order);

        let mut first_diff = None;
        let len = ours_lc.len().min(theirs_lc.len());
        for i in 0..len {
            if ours_lc[i] != theirs_lc[i] {
                first_diff = Some(i);
                break;
            }
        }

        // Count divergent positions for a quick "how close are we" metric.
        let same_position = (0..len).filter(|&i| ours_lc[i] == theirs_lc[i]).count();
        eprintln!(
            "Positional match: {}/{} ({:.1}%)",
            same_position,
            len,
            100.0 * same_position as f64 / len as f64
        );

        match first_diff {
            None if ours_lc.len() == theirs_lc.len() => {
                eprintln!("MATCH — our LOOT produces same order as Fluorine.");
            }
            None => {
                eprintln!(
                    "PREFIX MATCH but length differs (ours {} vs theirs {})",
                    ours_lc.len(),
                    theirs_lc.len()
                );
            }
            Some(idx) => {
                eprintln!("First divergence at index {idx}:");
                let lo = idx.saturating_sub(3);
                let hi = (idx + 4).min(len);
                for i in lo..hi {
                    let mark = if i == idx { ">>>" } else { "   " };
                    eprintln!(
                        "  {} [{}] ours={:<60}  theirs={}",
                        mark, i, sorted[i], fluorine_order[i]
                    );
                }
                // Don't panic — this is a diagnostic test, not a regression.
            }
        }
    }

    /// Same as above but pass the plugin set to LOOT in **Fluorine-order**
    /// instead of alphabetical. Tells us whether libloot's tie-breaking is
    /// input-order-sensitive — i.e. whether matching Fluorine's pre-sort
    /// input order alone closes the gap.
    #[test]
    #[ignore]
    fn loot_diff_with_fluorine_input_order() {
        let mods_dir = std::path::Path::new("/home/luke/Games/IAP/mods");
        let game_path = std::path::Path::new(
            "/home/luke/.local/share/Steam/steamapps/common/Skyrim Special Edition",
        );
        let fluorine_plugins =
            std::path::Path::new("/home/luke/Games/IAP/profiles/Default/plugins.txt");

        if !mods_dir.is_dir() || !game_path.is_dir() {
            eprintln!("[skip] IAP install not present");
            return;
        }

        let read_set = |p: &std::path::Path| -> Vec<String> {
            std::fs::read_to_string(p)
                .unwrap()
                .lines()
                .filter_map(|l| {
                    let l = l.trim();
                    if l.is_empty() || l.starts_with('#') {
                        return None;
                    }
                    Some(l.trim_start_matches(['*', '+']).to_string())
                })
                .collect()
        };
        let fluorine_order = read_set(fluorine_plugins);

        let rt = tokio::runtime::Runtime::new().unwrap();
        let sorted = rt.block_on(async {
            let mut sorter =
                PluginSorter::new(GameType::SkyrimSE, game_path, mods_dir).unwrap();
            sorter.ensure_masterlist(true).await.unwrap();
            sorter.set_mod_paths().unwrap();
            // Feed plugins in Fluorine's existing plugins.txt order.
            let loaded = sorter.load_plugins(&fluorine_order).unwrap();
            sorter.sort_plugins(&loaded).unwrap()
        });

        let lc = |v: &[String]| -> Vec<String> { v.iter().map(|s| s.to_lowercase()).collect() };
        let ours = lc(&sorted);
        let theirs = lc(&fluorine_order);
        let len = ours.len().min(theirs.len());
        let same = (0..len).filter(|&i| ours[i] == theirs[i]).count();
        eprintln!(
            "Fluorine-order input → positional match: {}/{} ({:.1}%)",
            same, len, 100.0 * same as f64 / len as f64
        );
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
