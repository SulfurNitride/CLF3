//! SQLite storage for collection installation data.
//!
//! Stores collection data in SQLite for:
//! - Low memory usage (mods don't stay in RAM)
//! - Resumable installations
//! - Progress tracking
//! - File verification via MD5 hashes

use anyhow::{Context, Result};
use rusqlite::{params, Connection};
use std::path::Path;
use tracing::info;

use super::types::*;

/// Installation status for mods
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModStatus {
    Pending,
    Downloading,
    Downloaded,
    Extracting,
    Extracted,
    Installing,
    Installed,
    Failed,
}

impl ModStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            ModStatus::Pending => "pending",
            ModStatus::Downloading => "downloading",
            ModStatus::Downloaded => "downloaded",
            ModStatus::Extracting => "extracting",
            ModStatus::Extracted => "extracted",
            ModStatus::Installing => "installing",
            ModStatus::Installed => "installed",
            ModStatus::Failed => "failed",
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Self {
        match s {
            "pending" => ModStatus::Pending,
            "downloading" => ModStatus::Downloading,
            "downloaded" => ModStatus::Downloaded,
            "extracting" => ModStatus::Extracting,
            "extracted" => ModStatus::Extracted,
            "installing" => ModStatus::Installing,
            "installed" => ModStatus::Installed,
            "failed" => ModStatus::Failed,
            _ => ModStatus::Pending,
        }
    }
}

/// SQLite-backed collection storage
pub struct CollectionDb {
    conn: Connection,
}

impl CollectionDb {
    /// Open or create a collection database
    pub fn open(db_path: &Path) -> Result<Self> {
        let conn = Connection::open(db_path)
            .with_context(|| format!("Failed to open database: {}", db_path.display()))?;

        // Configure for performance
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA cache_size = 10000;
             PRAGMA temp_store = MEMORY;
             PRAGMA mmap_size = 268435456;",
        )
        .context("Failed to configure SQLite pragmas")?;

        let db = Self { conn };
        db.create_tables()?;

        Ok(db)
    }

    /// Create an in-memory database (for testing)
    pub fn in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory().context("Failed to create in-memory database")?;

        let db = Self { conn };
        db.create_tables()?;

        Ok(db)
    }

    fn create_tables(&self) -> Result<()> {
        self.conn
            .execute_batch(
                r#"
            -- Collection metadata
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            -- Mods (downloads)
            CREATE TABLE IF NOT EXISTS mods (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                folder_name TEXT NOT NULL,
                logical_filename TEXT NOT NULL,
                md5 TEXT NOT NULL,
                file_size INTEGER NOT NULL,
                mod_id INTEGER NOT NULL,
                file_id INTEGER NOT NULL,
                source_type TEXT NOT NULL DEFAULT 'nexus',
                source_url TEXT NOT NULL DEFAULT '',
                deploy_type TEXT NOT NULL DEFAULT '',
                phase INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'pending',
                local_path TEXT,
                choices_json TEXT,
                error_message TEXT,
                -- FOMOD validation fields
                fomod_validated INTEGER NOT NULL DEFAULT 0,
                fomod_valid INTEGER NOT NULL DEFAULT 0,
                fomod_error TEXT,
                fomod_module_name TEXT,
                -- Expected file paths from collection hashes (for FOMOD without choices)
                hashes_json TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_mods_md5 ON mods(md5);
            CREATE INDEX IF NOT EXISTS idx_mods_status ON mods(status);
            CREATE INDEX IF NOT EXISTS idx_mods_phase ON mods(phase);
            CREATE INDEX IF NOT EXISTS idx_mods_folder ON mods(folder_name);

            -- Mod rules (ordering constraints)
            CREATE TABLE IF NOT EXISTS mod_rules (
                id INTEGER PRIMARY KEY,
                rule_type TEXT NOT NULL,
                source_md5 TEXT NOT NULL,
                source_filename TEXT NOT NULL,
                reference_md5 TEXT NOT NULL,
                reference_filename TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_rules_source ON mod_rules(source_md5);
            CREATE INDEX IF NOT EXISTS idx_rules_reference ON mod_rules(reference_md5);

            -- Plugins (load order)
            CREATE TABLE IF NOT EXISTS plugins (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                enabled INTEGER NOT NULL DEFAULT 1,
                load_order INTEGER
            );

            CREATE INDEX IF NOT EXISTS idx_plugins_order ON plugins(load_order);

            -- Archive file listings (for path lookup within archives)
            CREATE TABLE IF NOT EXISTS archive_files (
                id INTEGER PRIMARY KEY,
                mod_md5 TEXT NOT NULL,
                file_path TEXT NOT NULL,
                normalized_path TEXT NOT NULL,
                file_size INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_archive_files_md5 ON archive_files(mod_md5);
            CREATE INDEX IF NOT EXISTS idx_archive_files_normalized ON archive_files(mod_md5, normalized_path);

            -- Installation log (track what files were installed where)
            CREATE TABLE IF NOT EXISTS installed_files (
                id INTEGER PRIMARY KEY,
                mod_id INTEGER NOT NULL,
                source_path TEXT NOT NULL,
                dest_path TEXT NOT NULL,
                file_size INTEGER NOT NULL,
                installed_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (mod_id) REFERENCES mods(id)
            );

            CREATE INDEX IF NOT EXISTS idx_installed_mod ON installed_files(mod_id);
            CREATE INDEX IF NOT EXISTS idx_installed_dest ON installed_files(dest_path);
            "#,
            )
            .context("Failed to create tables")?;

        Ok(())
    }

    /// Store collection metadata
    pub fn set_metadata(&self, key: &str, value: &str) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES (?1, ?2)",
            params![key, value],
        )?;
        Ok(())
    }

    /// Get collection metadata
    pub fn get_metadata(&self, key: &str) -> Result<Option<String>> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT value FROM metadata WHERE key = ?1")?;

        let result = stmt.query_row([key], |row| row.get(0));

        match result {
            Ok(value) => Ok(Some(value)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e).context("Failed to query metadata"),
        }
    }

    /// Clear all data from the database
    pub fn clear_all_data(&mut self) -> Result<()> {
        self.conn
            .execute_batch(
                "DELETE FROM installed_files;
             DELETE FROM archive_files;
             DELETE FROM plugins;
             DELETE FROM mod_rules;
             DELETE FROM mods;
             DELETE FROM metadata;",
            )
            .context("Failed to clear database")?;
        Ok(())
    }

    /// Import a parsed collection into the database
    pub fn import_collection(&mut self, collection: &Collection) -> Result<()> {
        let tx = self.conn.transaction()?;

        // Store metadata (using getters to handle both info wrapper and legacy formats)
        tx.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES ('name', ?1)",
            [collection.get_name()],
        )?;
        tx.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES ('author', ?1)",
            [collection.get_author()],
        )?;
        tx.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES ('version', ?1)",
            [&collection.version],
        )?;
        tx.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES ('domain_name', ?1)",
            [collection.get_domain_name()],
        )?;

        info!("Importing {} mods", collection.mods.len());

        // Import mods
        {
            let mut stmt = tx.prepare(
                "INSERT INTO mods (name, folder_name, logical_filename, md5, file_size, mod_id, file_id, source_type, source_url, deploy_type, phase, choices_json, hashes_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)"
            )?;

            for mod_entry in &collection.mods {
                let choices_json = mod_entry
                    .choices
                    .as_ref()
                    .map(|c| serde_json::to_string(c).unwrap_or_default());

                // Store hashes as JSON for hash-based FOMOD fallback
                let hashes_json = if !mod_entry.hashes.is_empty() {
                    Some(serde_json::to_string(&mod_entry.hashes).unwrap_or_default())
                } else {
                    None
                };

                // Use MD5 from source (hashes array is per-file, not archive MD5)
                let md5 = if !mod_entry.source.md5.is_empty() {
                    &mod_entry.source.md5
                } else {
                    "" // Will need to be computed later
                };

                // Use source.logical_filename if top-level logical_filename is empty
                let logical_filename = if mod_entry.logical_filename.is_empty() {
                    &mod_entry.source.logical_filename
                } else {
                    &mod_entry.logical_filename
                };

                // Construct folder_name as "logicalFilename-modId-fileId" like CInstaller does
                // CInstaller uses: logicalFilename.empty() ? name : logicalFilename
                // This ensures unique folder names and matches the expected format
                let folder_name = if !mod_entry.folder_name.is_empty() {
                    mod_entry.folder_name.clone()
                } else if mod_entry.source.mod_id > 0 && mod_entry.source.file_id > 0 {
                    // Use logical_filename as base (like CInstaller), fall back to name
                    let base_name = if !mod_entry.source.logical_filename.is_empty() {
                        &mod_entry.source.logical_filename
                    } else if !mod_entry.logical_filename.is_empty() {
                        &mod_entry.logical_filename
                    } else {
                        &mod_entry.name
                    };
                    // Construct folder name with Nexus IDs: "logicalFilename-modId-fileId"
                    format!(
                        "{}-{}-{}",
                        base_name, mod_entry.source.mod_id, mod_entry.source.file_id
                    )
                } else {
                    mod_entry.name.clone()
                };

                // Get deploy type from details (dinput, enb, etc.)
                let deploy_type = mod_entry
                    .details
                    .as_ref()
                    .map(|d| d.mod_type.as_str())
                    .unwrap_or("");

                stmt.execute(params![
                    mod_entry.name,
                    folder_name,
                    logical_filename,
                    md5,
                    mod_entry.source.file_size,
                    mod_entry.source.mod_id,
                    mod_entry.source.file_id,
                    mod_entry.source.source_type,
                    mod_entry.source.url,
                    deploy_type,
                    mod_entry.phase,
                    choices_json,
                    hashes_json,
                ])?;
            }
        }

        info!("Importing {} mod rules", collection.mod_rules.len());

        // Import mod rules
        {
            let mut stmt = tx.prepare(
                "INSERT INTO mod_rules (rule_type, source_md5, source_filename, reference_md5, reference_filename)
                 VALUES (?1, ?2, ?3, ?4, ?5)"
            )?;

            for rule in &collection.mod_rules {
                stmt.execute(params![
                    rule.rule_type,
                    rule.source.file_md5,
                    rule.source.logical_file_name,
                    rule.reference.file_md5,
                    rule.reference.logical_file_name,
                ])?;
            }
        }

        info!("Importing {} plugins", collection.plugins.len());

        // Import plugins with load order (use OR REPLACE to handle duplicates)
        {
            let mut stmt = tx.prepare(
                "INSERT OR REPLACE INTO plugins (name, enabled, load_order) VALUES (?1, ?2, ?3)",
            )?;

            for (i, plugin) in collection.plugins.iter().enumerate() {
                stmt.execute(params![plugin.name, plugin.enabled, i as i64])?;
            }
        }

        tx.commit()?;

        info!("Import complete");
        Ok(())
    }

    /// Get mod statistics by status
    pub fn get_mod_stats(&self) -> Result<ModStats> {
        let mut stmt = self
            .conn
            .prepare("SELECT status, COUNT(*) FROM mods GROUP BY status")?;

        let mut stats = ModStats::default();
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as usize))
        })?;

        for row in rows {
            let (status, count) = row?;
            match status.as_str() {
                "pending" => stats.pending = count,
                "downloading" => stats.downloading = count,
                "downloaded" => stats.downloaded = count,
                "extracting" => stats.extracting = count,
                "extracted" => stats.extracted = count,
                "installing" => stats.installing = count,
                "installed" => stats.installed = count,
                "failed" => stats.failed = count,
                _ => {}
            }
        }

        stats.total = stats.pending
            + stats.downloading
            + stats.downloaded
            + stats.extracting
            + stats.extracted
            + stats.installing
            + stats.installed
            + stats.failed;
        Ok(stats)
    }

    /// Get all mods that need downloading
    pub fn get_pending_downloads(&self) -> Result<Vec<ModDbEntry>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, name, folder_name, logical_filename, md5, file_size, mod_id, file_id, source_type, source_url, deploy_type, phase, status, local_path, choices_json, error_message, fomod_validated, fomod_valid, fomod_error, fomod_module_name, hashes_json
             FROM mods WHERE status = 'pending' ORDER BY phase, id",
        )?;

        self.query_mods(&mut stmt, [])
    }

    /// Get all mods with a specific status
    pub fn get_mods_by_status(&self, status: ModStatus) -> Result<Vec<ModDbEntry>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, name, folder_name, logical_filename, md5, file_size, mod_id, file_id, source_type, source_url, deploy_type, phase, status, local_path, choices_json, error_message, fomod_validated, fomod_valid, fomod_error, fomod_module_name, hashes_json
             FROM mods WHERE status = ?1 ORDER BY phase, id",
        )?;

        self.query_mods(&mut stmt, [status.as_str()])
    }

    /// Get a mod by its database ID
    pub fn get_mod_by_id(&self, id: i64) -> Result<Option<ModDbEntry>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT id, name, folder_name, logical_filename, md5, file_size, mod_id, file_id, source_type, source_url, deploy_type, phase, status, local_path, choices_json, error_message, fomod_validated, fomod_valid, fomod_error, fomod_module_name, hashes_json
             FROM mods WHERE id = ?1",
        )?;

        let result = stmt.query_row([id], |row| {
            Ok(ModDbEntry {
                id: row.get(0)?,
                name: row.get(1)?,
                folder_name: row.get(2)?,
                logical_filename: row.get(3)?,
                md5: row.get(4)?,
                file_size: row.get(5)?,
                mod_id: row.get(6)?,
                file_id: row.get(7)?,
                source_type: row.get(8)?,
                source_url: row.get(9)?,
                deploy_type: row.get(10)?,
                phase: row.get(11)?,
                status: row.get(12)?,
                local_path: row.get(13)?,
                choices_json: row.get(14)?,
                error_message: row.get(15)?,
                fomod_validated: row.get(16)?,
                fomod_valid: row.get(17)?,
                fomod_error: row.get(18)?,
                fomod_module_name: row.get(19)?,
                hashes_json: row.get(20)?,
            })
        });

        match result {
            Ok(entry) => Ok(Some(entry)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e).context("Failed to query mod"),
        }
    }

    /// Get a mod by its MD5 hash
    pub fn get_mod_by_md5(&self, md5: &str) -> Result<Option<ModDbEntry>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT id, name, folder_name, logical_filename, md5, file_size, mod_id, file_id, source_type, source_url, deploy_type, phase, status, local_path, choices_json, error_message, fomod_validated, fomod_valid, fomod_error, fomod_module_name, hashes_json
             FROM mods WHERE md5 = ?1",
        )?;

        let result = stmt.query_row([md5], |row| {
            Ok(ModDbEntry {
                id: row.get(0)?,
                name: row.get(1)?,
                folder_name: row.get(2)?,
                logical_filename: row.get(3)?,
                md5: row.get(4)?,
                file_size: row.get(5)?,
                mod_id: row.get(6)?,
                file_id: row.get(7)?,
                source_type: row.get(8)?,
                source_url: row.get(9)?,
                deploy_type: row.get(10)?,
                phase: row.get(11)?,
                status: row.get(12)?,
                local_path: row.get(13)?,
                choices_json: row.get(14)?,
                error_message: row.get(15)?,
                fomod_validated: row.get(16)?,
                fomod_valid: row.get(17)?,
                fomod_error: row.get(18)?,
                fomod_module_name: row.get(19)?,
                hashes_json: row.get(20)?,
            })
        });

        match result {
            Ok(entry) => Ok(Some(entry)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e).context("Failed to query mod"),
        }
    }

    /// Helper to query mods
    fn query_mods<P: rusqlite::Params>(
        &self,
        stmt: &mut rusqlite::Statement,
        params: P,
    ) -> Result<Vec<ModDbEntry>> {
        let rows = stmt.query_map(params, |row| {
            Ok(ModDbEntry {
                id: row.get(0)?,
                name: row.get(1)?,
                folder_name: row.get(2)?,
                logical_filename: row.get(3)?,
                md5: row.get(4)?,
                file_size: row.get(5)?,
                mod_id: row.get(6)?,
                file_id: row.get(7)?,
                source_type: row.get(8)?,
                source_url: row.get(9)?,
                deploy_type: row.get(10)?,
                phase: row.get(11)?,
                status: row.get(12)?,
                local_path: row.get(13)?,
                choices_json: row.get(14)?,
                error_message: row.get(15)?,
                fomod_validated: row.get(16)?,
                fomod_valid: row.get(17)?,
                fomod_error: row.get(18)?,
                fomod_module_name: row.get(19)?,
                hashes_json: row.get(20)?,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Update mod status
    pub fn update_mod_status(&self, id: i64, status: ModStatus) -> Result<()> {
        self.conn.execute(
            "UPDATE mods SET status = ?2, updated_at = datetime('now') WHERE id = ?1",
            params![id, status.as_str()],
        )?;
        Ok(())
    }

    /// Mark mod as downloaded with local path
    pub fn mark_mod_downloaded(&self, id: i64, local_path: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE mods SET status = 'downloaded', local_path = ?2, updated_at = datetime('now') WHERE id = ?1",
            params![id, local_path],
        )?;
        Ok(())
    }

    /// Mark mod as failed with error message
    pub fn mark_mod_failed(&self, id: i64, error: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE mods SET status = 'failed', error_message = ?2, updated_at = datetime('now') WHERE id = ?1",
            params![id, error],
        )?;
        Ok(())
    }

    /// Update mod MD5 hash (after computing from downloaded file)
    pub fn update_mod_md5(&self, id: i64, md5: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE mods SET md5 = ?2, updated_at = datetime('now') WHERE id = ?1",
            params![id, md5],
        )?;
        Ok(())
    }

    /// Mark FOMOD as validated successfully
    pub fn mark_fomod_valid(&self, id: i64, module_name: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE mods SET fomod_validated = 1, fomod_valid = 1, fomod_module_name = ?2, fomod_error = NULL, updated_at = datetime('now') WHERE id = ?1",
            params![id, module_name],
        )?;
        Ok(())
    }

    /// Mark FOMOD as invalid with error message
    pub fn mark_fomod_invalid(&self, id: i64, error: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE mods SET fomod_validated = 1, fomod_valid = 0, fomod_error = ?2, updated_at = datetime('now') WHERE id = ?1",
            params![id, error],
        )?;
        Ok(())
    }

    /// Get FOMOD mods that need validation
    pub fn get_fomod_mods_needing_validation(&self) -> Result<Vec<ModDbEntry>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, name, folder_name, logical_filename, md5, file_size, mod_id, file_id, source_type, source_url, deploy_type, phase, status, local_path, choices_json, error_message, fomod_validated, fomod_valid, fomod_error, fomod_module_name, hashes_json
             FROM mods WHERE choices_json IS NOT NULL AND fomod_validated = 0 AND local_path IS NOT NULL ORDER BY phase, id",
        )?;

        self.query_mods(&mut stmt, [])
    }

    /// Get FOMOD mods that failed validation
    pub fn get_invalid_fomod_mods(&self) -> Result<Vec<ModDbEntry>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, name, folder_name, logical_filename, md5, file_size, mod_id, file_id, source_type, source_url, deploy_type, phase, status, local_path, choices_json, error_message, fomod_validated, fomod_valid, fomod_error, fomod_module_name, hashes_json
             FROM mods WHERE fomod_validated = 1 AND fomod_valid = 0 ORDER BY phase, id",
        )?;

        self.query_mods(&mut stmt, [])
    }

    /// Reset stuck mods (downloading/extracting/installing) back to previous state
    pub fn reset_stuck_mods(&self) -> Result<usize> {
        let count = self.conn.execute(
            "UPDATE mods SET status = CASE
                WHEN status = 'downloading' THEN 'pending'
                WHEN status = 'extracting' THEN 'downloaded'
                WHEN status = 'installing' THEN 'extracted'
                ELSE status
             END,
             updated_at = datetime('now')
             WHERE status IN ('downloading', 'extracting', 'installing')",
            [],
        )?;
        Ok(count)
    }

    /// Get all mods (preserves original collection order by id)
    pub fn get_all_mods(&self) -> Result<Vec<ModDbEntry>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, name, folder_name, logical_filename, md5, file_size, mod_id, file_id, source_type, source_url, deploy_type, phase, status, local_path, choices_json, error_message, fomod_validated, fomod_valid, fomod_error, fomod_module_name, hashes_json
             FROM mods ORDER BY id",
        )?;

        self.query_mods(&mut stmt, [])
    }

    // ========== Archive File Indexing ==========

    /// Check if a mod's archive has been indexed
    pub fn is_archive_indexed(&self, mod_md5: &str) -> Result<bool> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM archive_files WHERE mod_md5 = ?1",
            params![mod_md5],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Index archive files for a mod
    pub fn index_archive_files(&self, mod_md5: &str, files: &[ArchiveFileEntry]) -> Result<()> {
        // Clear existing entries
        self.conn.execute(
            "DELETE FROM archive_files WHERE mod_md5 = ?1",
            params![mod_md5],
        )?;

        let mut stmt = self.conn.prepare_cached(
            "INSERT INTO archive_files (mod_md5, file_path, normalized_path, file_size) VALUES (?1, ?2, ?3, ?4)",
        )?;

        for entry in files {
            let normalized = normalize_path(&entry.file_path);
            stmt.execute(params![
                mod_md5,
                entry.file_path,
                normalized,
                entry.file_size as i64
            ])?;
        }

        Ok(())
    }

    /// Look up actual file path from normalized path
    pub fn lookup_archive_file(&self, mod_md5: &str, path: &str) -> Result<Option<String>> {
        let normalized = normalize_path(path);

        let result = self.conn.query_row(
            "SELECT file_path FROM archive_files WHERE mod_md5 = ?1 AND normalized_path = ?2",
            params![mod_md5, normalized],
            |row| row.get(0),
        );

        match result {
            Ok(path) => Ok(Some(path)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e).context("Failed to lookup archive file"),
        }
    }

    /// Get all files in an archive
    pub fn get_archive_files(&self, mod_md5: &str) -> Result<Vec<ArchiveFileEntry>> {
        let mut stmt = self.conn.prepare(
            "SELECT file_path, file_size FROM archive_files WHERE mod_md5 = ?1",
        )?;

        let rows = stmt.query_map([mod_md5], |row| {
            Ok(ArchiveFileEntry {
                file_path: row.get(0)?,
                file_size: row.get::<_, i64>(1)? as u64,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    // ========== Installed Files Tracking ==========

    /// Record an installed file
    pub fn record_installed_file(
        &self,
        mod_id: i64,
        source_path: &str,
        dest_path: &str,
        file_size: u64,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO installed_files (mod_id, source_path, dest_path, file_size) VALUES (?1, ?2, ?3, ?4)",
            params![mod_id, source_path, dest_path, file_size as i64],
        )?;
        Ok(())
    }

    /// Get all installed files for a mod
    pub fn get_installed_files(&self, mod_id: i64) -> Result<Vec<InstalledFileEntry>> {
        let mut stmt = self.conn.prepare(
            "SELECT source_path, dest_path, file_size FROM installed_files WHERE mod_id = ?1",
        )?;

        let rows = stmt.query_map([mod_id], |row| {
            Ok(InstalledFileEntry {
                source_path: row.get(0)?,
                dest_path: row.get(1)?,
                file_size: row.get::<_, i64>(2)? as u64,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    // ========== Plugins ==========

    /// Get all plugins in load order
    pub fn get_plugins(&self) -> Result<Vec<PluginDbEntry>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, name, enabled, load_order FROM plugins ORDER BY load_order",
        )?;

        let rows = stmt.query_map([], |row| {
            Ok(PluginDbEntry {
                id: row.get(0)?,
                name: row.get(1)?,
                enabled: row.get(2)?,
                load_order: row.get(3)?,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Update plugin load order
    pub fn update_plugin_order(&self, name: &str, load_order: i64) -> Result<()> {
        self.conn.execute(
            "UPDATE plugins SET load_order = ?2 WHERE name = ?1",
            params![name, load_order],
        )?;
        Ok(())
    }

    // ========== Mod Rules ==========

    /// Get all mod rules in insertion order
    pub fn get_mod_rules(&self) -> Result<Vec<ModRuleDbEntry>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, rule_type, source_md5, source_filename, reference_md5, reference_filename FROM mod_rules ORDER BY id",
        )?;

        let rows = stmt.query_map([], |row| {
            Ok(ModRuleDbEntry {
                id: row.get(0)?,
                rule_type: row.get(1)?,
                source_md5: row.get(2)?,
                source_filename: row.get(3)?,
                reference_md5: row.get(4)?,
                reference_filename: row.get(5)?,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }
}

/// Mod entry from database
#[derive(Debug, Clone)]
pub struct ModDbEntry {
    pub id: i64,
    pub name: String,
    pub folder_name: String,
    pub logical_filename: String,
    pub md5: String,
    pub file_size: i64,
    pub mod_id: i64,
    pub file_id: i64,
    pub source_type: String,
    /// Direct download URL (for non-Nexus sources)
    pub source_url: String,
    /// Deploy type from collection (dinput, enb, etc.) - determines root vs mod folder
    pub deploy_type: String,
    pub phase: i32,
    pub status: String,
    pub local_path: Option<String>,
    pub choices_json: Option<String>,
    pub error_message: Option<String>,
    // FOMOD validation fields
    pub fomod_validated: bool,
    pub fomod_valid: bool,
    pub fomod_error: Option<String>,
    pub fomod_module_name: Option<String>,
    /// Expected file paths from collection hashes (for FOMOD without choices)
    pub hashes_json: Option<String>,
}

impl ModDbEntry {
    /// Returns true if this mod should be deployed to game root (dinput, enb, etc.)
    pub fn is_root_mod(&self) -> bool {
        matches!(self.deploy_type.as_str(), "dinput" | "enb")
    }
}

impl ModDbEntry {
    /// Get the FOMOD choices if any
    pub fn get_choices(&self) -> Option<FomodChoices> {
        self.choices_json
            .as_ref()
            .and_then(|json| serde_json::from_str(json).ok())
    }

    /// Check if this mod has FOMOD choices
    pub fn has_fomod(&self) -> bool {
        self.choices_json.is_some()
    }

    /// Check if this mod has expected file hashes (for hash-based installation)
    pub fn has_hashes(&self) -> bool {
        self.hashes_json.as_ref().is_some_and(|j| !j.is_empty() && j != "[]")
    }

    /// Get expected file paths from hashes (normalized to forward slashes, lowercase)
    pub fn get_expected_paths(&self) -> Vec<String> {
        self.hashes_json
            .as_ref()
            .and_then(|json| {
                serde_json::from_str::<Vec<crate::collection::FileHash>>(json).ok()
            })
            .map(|hashes| {
                hashes
                    .into_iter()
                    .map(|h| h.path.replace('\\', "/"))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get the status as enum
    pub fn status_enum(&self) -> ModStatus {
        ModStatus::from_str(&self.status)
    }
}

/// Statistics for mods
#[derive(Debug, Default, Clone)]
pub struct ModStats {
    pub total: usize,
    pub pending: usize,
    pub downloading: usize,
    pub downloaded: usize,
    pub extracting: usize,
    pub extracted: usize,
    pub installing: usize,
    pub installed: usize,
    pub failed: usize,
}

impl ModStats {
    pub fn progress_percent(&self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            (self.installed as f64 / self.total as f64) * 100.0
        }
    }

    pub fn download_progress_percent(&self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            ((self.downloaded + self.extracted + self.installed) as f64 / self.total as f64) * 100.0
        }
    }
}

/// Archive file entry for indexing
#[derive(Debug, Clone)]
pub struct ArchiveFileEntry {
    pub file_path: String,
    pub file_size: u64,
}

/// Installed file entry
#[derive(Debug, Clone)]
pub struct InstalledFileEntry {
    pub source_path: String,
    pub dest_path: String,
    pub file_size: u64,
}

/// Plugin database entry
#[derive(Debug, Clone)]
pub struct PluginDbEntry {
    pub id: i64,
    pub name: String,
    pub enabled: bool,
    pub load_order: Option<i64>,
}

/// Mod rule database entry
#[derive(Debug, Clone)]
pub struct ModRuleDbEntry {
    pub id: i64,
    pub rule_type: String,
    pub source_md5: String,
    pub source_filename: String,
    pub reference_md5: String,
    pub reference_filename: String,
}

/// Normalize a path for case-insensitive lookup
fn normalize_path(path: &str) -> String {
    path.to_lowercase().replace('\\', "/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_db() {
        let db = CollectionDb::in_memory().unwrap();
        db.set_metadata("test_key", "test_value").unwrap();
        let value = db.get_metadata("test_key").unwrap();
        assert_eq!(value, Some("test_value".to_string()));
    }

    #[test]
    fn test_mod_status() {
        assert_eq!(ModStatus::Pending.as_str(), "pending");
        assert_eq!(ModStatus::from_str("installed"), ModStatus::Installed);
        assert_eq!(ModStatus::from_str("unknown"), ModStatus::Pending);
    }

    #[test]
    fn test_import_collection() {
        let mut db = CollectionDb::in_memory().unwrap();

        let collection = Collection {
            name: "Test Collection".to_string(),
            author: "Tester".to_string(),
            domain_name: "skyrimspecialedition".to_string(),
            description: "A test collection".to_string(),
            version: "1.0".to_string(),
            mods: vec![CollectionMod {
                name: "Test Mod".to_string(),
                folder_name: "TestMod".to_string(),
                logical_filename: "TestMod-1-0.zip".to_string(),
                version: "1.0".to_string(),
                source: ModSource {
                    source_type: "nexus".to_string(),
                    mod_id: 12345,
                    file_id: 67890,
                    file_size: 1024,
                    md5: "abc123".to_string(),
                    ..Default::default()
                },
                phase: 0,
                ..Default::default()
            }],
            mod_rules: vec![],
            plugins: vec![PluginInfo {
                name: "TestMod.esp".to_string(),
                enabled: true,
            }],
            ..Default::default()
        };

        db.import_collection(&collection).unwrap();

        // Check metadata
        assert_eq!(
            db.get_metadata("name").unwrap(),
            Some("Test Collection".to_string())
        );

        // Check mods
        let mods = db.get_all_mods().unwrap();
        assert_eq!(mods.len(), 1);
        assert_eq!(mods[0].name, "Test Mod");
        assert_eq!(mods[0].md5, "abc123");

        // Check plugins
        let plugins = db.get_plugins().unwrap();
        assert_eq!(plugins.len(), 1);
        assert_eq!(plugins[0].name, "TestMod.esp");
    }

    #[test]
    fn test_mod_status_updates() {
        let mut db = CollectionDb::in_memory().unwrap();

        let collection = Collection {
            name: "Test".to_string(),
            mods: vec![CollectionMod {
                name: "Mod1".to_string(),
                folder_name: "Mod1".to_string(),
                logical_filename: "Mod1.zip".to_string(),
                source: ModSource {
                    source_type: "nexus".to_string(),
                    mod_id: 1,
                    file_id: 1,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        db.import_collection(&collection).unwrap();

        let mods = db.get_all_mods().unwrap();
        let mod_id = mods[0].id;

        // Test status updates
        db.mark_mod_downloaded(mod_id, "/path/to/file.zip").unwrap();
        let updated = db.get_mod_by_id(mod_id).unwrap().unwrap();
        assert_eq!(updated.status, "downloaded");
        assert_eq!(updated.local_path, Some("/path/to/file.zip".to_string()));

        // Test failed status
        db.mark_mod_failed(mod_id, "Download error").unwrap();
        let failed = db.get_mod_by_id(mod_id).unwrap().unwrap();
        assert_eq!(failed.status, "failed");
        assert_eq!(failed.error_message, Some("Download error".to_string()));
    }

    #[test]
    fn test_archive_indexing() {
        let db = CollectionDb::in_memory().unwrap();

        let files = vec![
            ArchiveFileEntry {
                file_path: "Data/Textures/test.dds".to_string(),
                file_size: 1024,
            },
            ArchiveFileEntry {
                file_path: "Data/Meshes/test.nif".to_string(),
                file_size: 2048,
            },
        ];

        db.index_archive_files("abc123", &files).unwrap();

        assert!(db.is_archive_indexed("abc123").unwrap());
        assert!(!db.is_archive_indexed("nonexistent").unwrap());

        // Case-insensitive lookup
        let result = db
            .lookup_archive_file("abc123", "data/textures/test.dds")
            .unwrap();
        assert_eq!(result, Some("Data/Textures/test.dds".to_string()));
    }
}
