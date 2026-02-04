//! Config cache for remembering user settings per modlist
//!
//! Stores user configurations (paths, settings) keyed by modlist identity,
//! so when the user loads the same modlist again, paths are auto-filled.

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tracing::info;

/// Cached configuration for a modlist installation
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ModlistConfig {
    /// Installation directory
    pub install_dir: Option<String>,
    /// Downloads directory
    pub downloads_dir: Option<String>,
    /// Fallout New Vegas path
    pub fnv_path: Option<String>,
    /// Fallout 3 path (for TTW)
    pub fo3_path: Option<String>,
    /// TTW MPI file path
    pub ttw_mpi_path: Option<String>,
    /// TTW installer binary path
    pub ttw_installer_path: Option<String>,
    /// Selected profile name
    pub profile_name: Option<String>,
    /// Whether TTW was detected as required
    pub ttw_required: Option<bool>,
    /// Last successful install timestamp
    pub last_install: Option<i64>,
}

impl ModlistConfig {
    /// Check if this config has the essential TTW paths
    pub fn has_ttw_paths(&self) -> bool {
        self.ttw_mpi_path.is_some() && self.fo3_path.is_some()
    }

    /// Check if this config has basic paths set
    pub fn has_basic_paths(&self) -> bool {
        self.install_dir.is_some() && self.downloads_dir.is_some()
    }
}

/// Database for caching modlist configurations
pub struct ConfigCache {
    conn: Connection,
}

impl ConfigCache {
    /// Open or create the config cache database
    pub fn open() -> Result<Self> {
        let cache_path = Self::cache_path()?;

        // Ensure parent directory exists
        if let Some(parent) = cache_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let conn = Connection::open(&cache_path)
            .with_context(|| format!("Failed to open config cache: {}", cache_path.display()))?;

        let cache = Self { conn };
        cache.init_schema()?;

        Ok(cache)
    }

    /// Get the cache database path
    fn cache_path() -> Result<PathBuf> {
        let config_dir = dirs::config_dir()
            .context("Could not determine config directory")?
            .join("clf3");
        Ok(config_dir.join("modlist_configs.db"))
    }

    /// Initialize database schema
    fn init_schema(&self) -> Result<()> {
        self.conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS modlist_configs (
                -- Primary key: combination of name + version for reliable matching
                modlist_key TEXT PRIMARY KEY,

                -- Modlist identity
                modlist_name TEXT NOT NULL,
                modlist_version TEXT NOT NULL,
                modlist_hash TEXT,
                game_type TEXT,

                -- User configuration (JSON)
                config_json TEXT NOT NULL,

                -- Timestamps
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_modlist_name
                ON modlist_configs(modlist_name);

            CREATE INDEX IF NOT EXISTS idx_modlist_hash
                ON modlist_configs(modlist_hash);
            "#,
        )?;

        Ok(())
    }

    /// Generate a key for a modlist (name + version)
    fn modlist_key(name: &str, version: &str) -> String {
        format!("{}::{}", name.to_lowercase(), version)
    }

    /// Get cached config for a modlist
    pub fn get_config(&self, name: &str, version: &str) -> Result<Option<ModlistConfig>> {
        let key = Self::modlist_key(name, version);

        let config_json: Option<String> = self
            .conn
            .query_row(
                "SELECT config_json FROM modlist_configs WHERE modlist_key = ?1",
                params![key],
                |row| row.get(0),
            )
            .optional()?;

        match config_json {
            Some(json) => {
                let config: ModlistConfig = serde_json::from_str(&json)
                    .with_context(|| format!("Failed to parse cached config for {}", name))?;
                info!("Loaded cached config for modlist: {} v{}", name, version);
                Ok(Some(config))
            }
            None => Ok(None),
        }
    }

    /// Get cached config by modlist hash (fallback lookup)
    pub fn get_config_by_hash(&self, hash: &str) -> Result<Option<ModlistConfig>> {
        let config_json: Option<String> = self
            .conn
            .query_row(
                "SELECT config_json FROM modlist_configs WHERE modlist_hash = ?1",
                params![hash],
                |row| row.get(0),
            )
            .optional()?;

        match config_json {
            Some(json) => {
                let config: ModlistConfig = serde_json::from_str(&json)?;
                info!("Loaded cached config by hash: {}", hash);
                Ok(Some(config))
            }
            None => Ok(None),
        }
    }

    /// Save config for a modlist
    pub fn save_config(
        &self,
        name: &str,
        version: &str,
        game_type: &str,
        hash: Option<&str>,
        config: &ModlistConfig,
    ) -> Result<()> {
        let key = Self::modlist_key(name, version);
        let config_json = serde_json::to_string(config)?;
        let now = chrono::Utc::now().timestamp();

        self.conn.execute(
            r#"
            INSERT INTO modlist_configs
                (modlist_key, modlist_name, modlist_version, modlist_hash, game_type,
                 config_json, created_at, updated_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)
            ON CONFLICT(modlist_key) DO UPDATE SET
                modlist_hash = COALESCE(?4, modlist_hash),
                game_type = ?5,
                config_json = ?6,
                updated_at = ?7
            "#,
            params![key, name, version, hash, game_type, config_json, now],
        )?;

        info!("Saved config for modlist: {} v{}", name, version);
        Ok(())
    }

    /// Update specific fields in a cached config
    pub fn update_config<F>(&self, name: &str, version: &str, updater: F) -> Result<()>
    where
        F: FnOnce(&mut ModlistConfig),
    {
        let mut config = self.get_config(name, version)?.unwrap_or_default();
        updater(&mut config);

        // Get existing game_type
        let key = Self::modlist_key(name, version);
        let game_type: Option<String> = self
            .conn
            .query_row(
                "SELECT game_type FROM modlist_configs WHERE modlist_key = ?1",
                params![key],
                |row| row.get(0),
            )
            .optional()?
            .flatten();

        self.save_config(name, version, &game_type.unwrap_or_default(), None, &config)
    }

    /// List all cached modlists
    pub fn list_modlists(&self) -> Result<Vec<(String, String, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT modlist_name, modlist_version, game_type FROM modlist_configs ORDER BY updated_at DESC"
        )?;

        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?.unwrap_or_default(),
            ))
        })?;

        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }

        Ok(result)
    }

    /// Delete cached config for a modlist
    pub fn delete_config(&self, name: &str, version: &str) -> Result<()> {
        let key = Self::modlist_key(name, version);
        self.conn.execute(
            "DELETE FROM modlist_configs WHERE modlist_key = ?1",
            params![key],
        )?;
        Ok(())
    }

    /// Clear all cached configs
    pub fn clear_all(&self) -> Result<()> {
        self.conn.execute("DELETE FROM modlist_configs", [])?;
        info!("Cleared all cached modlist configs");
        Ok(())
    }
}

/// Precheck result for a modlist
#[derive(Debug, Clone)]
pub struct ModlistPrecheck {
    /// Modlist name
    pub name: String,
    /// Modlist version
    pub version: String,
    /// Game type (e.g., "FalloutNewVegas")
    pub game_type: String,
    /// Whether TTW is required
    pub ttw_required: bool,
    /// TTW markers found (if TTW required)
    pub ttw_markers: Vec<String>,
    /// Cached config if available
    pub cached_config: Option<ModlistConfig>,
    /// What's missing for installation
    pub missing: Vec<String>,
}

impl ModlistPrecheck {
    /// Check if ready to install (nothing missing)
    pub fn is_ready(&self) -> bool {
        self.missing.is_empty()
    }

    /// Get a user-friendly summary
    pub fn summary(&self) -> String {
        let mut lines = vec![
            format!("Modlist: {} v{}", self.name, self.version),
            format!("Game: {}", self.game_type),
        ];

        if self.ttw_required {
            lines.push("⚠️  TTW Required: Yes".to_string());
            if !self.ttw_markers.is_empty() {
                lines.push(format!("   Detected via: {}", self.ttw_markers.join(", ")));
            }
        }

        if self.cached_config.is_some() {
            lines.push("✓ Previous configuration found".to_string());
        }

        if !self.missing.is_empty() {
            lines.push(format!("\nMissing: {}", self.missing.join(", ")));
        }

        lines.join("\n")
    }
}

/// Perform precheck on a modlist
pub fn precheck_modlist(
    modlist: &crate::modlist::Modlist,
    cache: &ConfigCache,
) -> Result<ModlistPrecheck> {
    // Check TTW requirement
    let ttw_result = modlist.requires_ttw();

    // Try to load cached config
    let cached_config = cache.get_config(&modlist.name, &modlist.version)?;

    // Determine what's missing
    let mut missing = Vec::new();

    if cached_config.as_ref().map(|c| c.install_dir.is_none()).unwrap_or(true) {
        missing.push("Install directory".to_string());
    }

    if cached_config.as_ref().map(|c| c.downloads_dir.is_none()).unwrap_or(true) {
        missing.push("Downloads directory".to_string());
    }

    if ttw_result.required {
        if cached_config.as_ref().map(|c| c.ttw_mpi_path.is_none()).unwrap_or(true) {
            missing.push("TTW MPI file".to_string());
        }
        if cached_config.as_ref().map(|c| c.fo3_path.is_none()).unwrap_or(true) {
            missing.push("Fallout 3 path".to_string());
        }
    }

    Ok(ModlistPrecheck {
        name: modlist.name.clone(),
        version: modlist.version.clone(),
        game_type: modlist.game_type.clone(),
        ttw_required: ttw_result.required,
        ttw_markers: ttw_result.markers_found,
        cached_config,
        missing,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_cache() -> ConfigCache {
        // Use in-memory database for tests
        let conn = Connection::open_in_memory().unwrap();
        let cache = ConfigCache { conn };
        cache.init_schema().unwrap();
        cache
    }

    #[test]
    fn test_save_and_load_config() {
        let cache = test_cache();

        let config = ModlistConfig {
            install_dir: Some("/home/user/modlist".to_string()),
            downloads_dir: Some("/home/user/downloads".to_string()),
            ttw_mpi_path: Some("/path/to/ttw.mpi".to_string()),
            fo3_path: Some("/path/to/fo3".to_string()),
            ..Default::default()
        };

        cache
            .save_config("Begin Again", "5.2.1", "FalloutNewVegas", None, &config)
            .unwrap();

        let loaded = cache.get_config("Begin Again", "5.2.1").unwrap();
        assert!(loaded.is_some());

        let loaded = loaded.unwrap();
        assert_eq!(loaded.install_dir, Some("/home/user/modlist".to_string()));
        assert!(loaded.has_ttw_paths());
    }

    #[test]
    fn test_config_not_found() {
        let cache = test_cache();
        let loaded = cache.get_config("NonExistent", "1.0.0").unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn test_update_config() {
        let cache = test_cache();

        // Initial save
        let config = ModlistConfig {
            install_dir: Some("/old/path".to_string()),
            ..Default::default()
        };
        cache
            .save_config("Test", "1.0", "Skyrim", None, &config)
            .unwrap();

        // Update
        cache
            .update_config("Test", "1.0", |c| {
                c.install_dir = Some("/new/path".to_string());
                c.ttw_mpi_path = Some("/path/to/mpi".to_string());
            })
            .unwrap();

        let loaded = cache.get_config("Test", "1.0").unwrap().unwrap();
        assert_eq!(loaded.install_dir, Some("/new/path".to_string()));
        assert_eq!(loaded.ttw_mpi_path, Some("/path/to/mpi".to_string()));
    }

    #[test]
    fn test_list_modlists() {
        let cache = test_cache();

        cache
            .save_config("Modlist A", "1.0", "Skyrim", None, &ModlistConfig::default())
            .unwrap();
        cache
            .save_config("Modlist B", "2.0", "Fallout4", None, &ModlistConfig::default())
            .unwrap();

        let list = cache.list_modlists().unwrap();
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn test_modlist_key_case_insensitive() {
        let cache = test_cache();

        let config = ModlistConfig {
            install_dir: Some("/path".to_string()),
            ..Default::default()
        };
        cache
            .save_config("Begin Again", "1.0", "FNV", None, &config)
            .unwrap();

        // Should find with different case
        let loaded = cache.get_config("begin again", "1.0").unwrap();
        assert!(loaded.is_some());
    }
}
