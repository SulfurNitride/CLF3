//! SQLite storage for modlist data
//!
//! Stores parsed modlist data in SQLite for:
//! - Low memory usage (370k+ directives don't stay in RAM)
//! - Resumable installations
//! - Progress tracking
//! - Efficient querying

// Many methods not yet used until installation pipeline is built
#![allow(dead_code)]

use anyhow::{Context, Result};
use rusqlite::{params, Connection};
use std::path::Path;
use tracing::info;

use super::types::*;

/// Installation status for directives
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectiveStatus {
    Pending,
    Processing,
    Completed,
    Failed,
}

impl DirectiveStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            DirectiveStatus::Pending => "pending",
            DirectiveStatus::Processing => "processing",
            DirectiveStatus::Completed => "completed",
            DirectiveStatus::Failed => "failed",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "pending" => DirectiveStatus::Pending,
            "processing" => DirectiveStatus::Processing,
            "completed" => DirectiveStatus::Completed,
            "failed" => DirectiveStatus::Failed,
            _ => DirectiveStatus::Pending,
        }
    }
}

/// SQLite-backed modlist storage
pub struct ModlistDb {
    conn: Connection,
}

impl ModlistDb {
    /// Open or create a modlist database
    pub fn open(db_path: &Path) -> Result<Self> {
        let conn = Connection::open(db_path)
            .with_context(|| format!("Failed to open database: {}", db_path.display()))?;

        // Configure for performance
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA cache_size = 10000;
             PRAGMA temp_store = MEMORY;
             PRAGMA mmap_size = 268435456;"
        ).context("Failed to configure SQLite pragmas")?;

        let db = Self { conn };
        db.create_tables()?;

        Ok(db)
    }

    /// Create an in-memory database (for testing)
    pub fn in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()
            .context("Failed to create in-memory database")?;

        let db = Self { conn };
        db.create_tables()?;

        Ok(db)
    }

    fn create_tables(&self) -> Result<()> {
        self.conn.execute_batch(
            r#"
            -- Modlist metadata
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            -- Archives (downloads)
            CREATE TABLE IF NOT EXISTS archives (
                id INTEGER PRIMARY KEY,
                hash TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                size INTEGER NOT NULL,
                meta TEXT NOT NULL,
                state_json TEXT NOT NULL,
                download_status TEXT NOT NULL DEFAULT 'pending',
                extraction_status TEXT NOT NULL DEFAULT 'pending',
                local_path TEXT,
                cached_url TEXT,
                url_expires INTEGER
            );

            CREATE INDEX IF NOT EXISTS idx_archives_hash ON archives(hash);
            CREATE INDEX IF NOT EXISTS idx_archives_status ON archives(download_status);
            CREATE INDEX IF NOT EXISTS idx_archives_extraction ON archives(extraction_status);

            -- Directives
            CREATE TABLE IF NOT EXISTS directives (
                id INTEGER PRIMARY KEY,
                directive_type TEXT NOT NULL,
                to_path TEXT NOT NULL,
                hash TEXT NOT NULL,
                size INTEGER NOT NULL,
                data_json TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                error_message TEXT,
                archive_hash TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_directives_type ON directives(directive_type);
            CREATE INDEX IF NOT EXISTS idx_directives_status ON directives(status);
            CREATE INDEX IF NOT EXISTS idx_directives_archive ON directives(archive_hash);
            CREATE INDEX IF NOT EXISTS idx_directives_to_path ON directives(to_path);

            -- Archive file listings (for path lookup)
            CREATE TABLE IF NOT EXISTS archive_files (
                id INTEGER PRIMARY KEY,
                archive_hash TEXT NOT NULL,
                file_path TEXT NOT NULL,
                normalized_path TEXT NOT NULL,
                file_size INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_archive_files_hash ON archive_files(archive_hash);
            CREATE INDEX IF NOT EXISTS idx_archive_files_normalized ON archive_files(archive_hash, normalized_path);
            "#
        ).context("Failed to create tables")?;

        Ok(())
    }

    /// Store modlist metadata
    pub fn set_metadata(&self, key: &str, value: &str) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES (?1, ?2)",
            params![key, value],
        )?;
        Ok(())
    }

    /// Get modlist metadata
    pub fn get_metadata(&self, key: &str) -> Result<Option<String>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT value FROM metadata WHERE key = ?1"
        )?;

        let result = stmt.query_row([key], |row| row.get(0));

        match result {
            Ok(value) => Ok(Some(value)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e).context("Failed to query metadata"),
        }
    }

    /// Clear all data from the database (for re-import when wabbajack file changes)
    pub fn clear_all_data(&mut self) -> Result<()> {
        self.conn.execute_batch(
            "DELETE FROM directives;
             DELETE FROM archives;
             DELETE FROM metadata;"
        ).context("Failed to clear database")?;
        Ok(())
    }

    /// Import a parsed modlist into the database
    pub fn import_modlist(&mut self, modlist: &Modlist) -> Result<()> {
        let tx = self.conn.transaction()?;

        // Store metadata
        tx.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES ('name', ?1)",
            [&modlist.name],
        )?;
        tx.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES ('author', ?1)",
            [&modlist.author],
        )?;
        tx.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES ('version', ?1)",
            [&modlist.version],
        )?;
        tx.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES ('game_type', ?1)",
            [&modlist.game_type],
        )?;
        tx.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES ('wabbajack_version', ?1)",
            [&modlist.wabbajack_version],
        )?;

        info!("Importing {} archives", modlist.archives.len());

        // Import archives
        {
            let mut stmt = tx.prepare(
                "INSERT OR REPLACE INTO archives (hash, name, size, meta, state_json)
                 VALUES (?1, ?2, ?3, ?4, ?5)"
            )?;

            for archive in &modlist.archives {
                let state_json = serde_json::to_string(&archive.state)?;
                stmt.execute(params![
                    &archive.hash,
                    &archive.name,
                    archive.size as i64,
                    &archive.meta,
                    &state_json,
                ])?;
            }
        }

        info!("Importing {} directives", modlist.directives.len());

        // Import directives
        {
            let mut stmt = tx.prepare(
                "INSERT INTO directives (directive_type, to_path, hash, size, data_json, archive_hash)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)"
            )?;

            for directive in &modlist.directives {
                let data_json = serde_json::to_string(directive)?;
                let archive_hash = Self::extract_archive_hash(directive);

                stmt.execute(params![
                    directive.directive_type(),
                    directive.to_path(),
                    Self::extract_hash(directive),
                    directive.size() as i64,
                    &data_json,
                    archive_hash,
                ])?;
            }
        }

        tx.commit()?;

        info!("Import complete");
        Ok(())
    }

    fn extract_hash(directive: &Directive) -> &str {
        match directive {
            Directive::FromArchive(d) => &d.hash,
            Directive::PatchedFromArchive(d) => &d.hash,
            Directive::InlineFile(d) => &d.hash,
            Directive::RemappedInlineFile(d) => &d.hash,
            Directive::TransformedTexture(d) => &d.hash,
            Directive::CreateBSA(d) => &d.hash,
        }
    }

    fn extract_archive_hash(directive: &Directive) -> Option<&str> {
        match directive {
            Directive::FromArchive(d) => d.archive_hash_path.first().map(|s| s.as_str()),
            Directive::PatchedFromArchive(d) => d.archive_hash_path.first().map(|s| s.as_str()),
            Directive::TransformedTexture(d) => d.archive_hash_path.first().map(|s| s.as_str()),
            _ => None,
        }
    }

    /// Get directive counts by status
    pub fn get_directive_stats(&self) -> Result<DirectiveStats> {
        let mut stmt = self.conn.prepare(
            "SELECT status, COUNT(*) FROM directives GROUP BY status"
        )?;

        let mut stats = DirectiveStats::default();
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as usize))
        })?;

        for row in rows {
            let (status, count) = row?;
            match status.as_str() {
                "pending" => stats.pending = count,
                "processing" => stats.processing = count,
                "completed" => stats.completed = count,
                "failed" => stats.failed = count,
                _ => {}
            }
        }

        stats.total = stats.pending + stats.processing + stats.completed + stats.failed;
        Ok(stats)
    }

    /// Get directive counts by type
    pub fn get_directive_type_counts(&self) -> Result<Vec<(String, usize)>> {
        let mut stmt = self.conn.prepare(
            "SELECT directive_type, COUNT(*) FROM directives GROUP BY directive_type ORDER BY COUNT(*) DESC"
        )?;

        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as usize))
        })?;

        let mut counts = Vec::new();
        for row in rows {
            counts.push(row?);
        }
        Ok(counts)
    }

    /// Get pending directives of a specific type (limited)
    pub fn get_pending_directives(&self, directive_type: &str, limit: usize) -> Result<Vec<(i64, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, data_json FROM directives
             WHERE directive_type = ?1 AND status = 'pending'
             LIMIT ?2"
        )?;

        let rows = stmt.query_map(params![directive_type, limit as i64], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Get ALL pending directives of a specific type (no limit)
    pub fn get_all_pending_directives_of_type(&self, directive_type: &str) -> Result<Vec<(i64, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, data_json FROM directives
             WHERE directive_type = ?1 AND status = 'pending'"
        )?;

        let rows = stmt.query_map([directive_type], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Get all output paths from all directives (for cleanup)
    pub fn get_all_output_paths(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare(
            "SELECT to_path FROM directives WHERE to_path IS NOT NULL AND to_path != ''"
        )?;

        let rows = stmt.query_map([], |row| {
            row.get::<_, String>(0)
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Get all pending directives for a specific archive hash
    pub fn get_directives_for_archive(&self, archive_hash: &str) -> Result<Vec<(i64, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, data_json FROM directives
             WHERE archive_hash = ?1 AND status = 'pending'"
        )?;

        let rows = stmt.query_map([archive_hash], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Mark directive as processing
    pub fn mark_directive_processing(&self, id: i64) -> Result<()> {
        self.conn.execute(
            "UPDATE directives SET status = 'processing', updated_at = datetime('now') WHERE id = ?1",
            [id],
        )?;
        Ok(())
    }

    /// Mark directive as completed
    pub fn mark_directive_completed(&self, id: i64) -> Result<()> {
        self.conn.execute(
            "UPDATE directives SET status = 'completed', updated_at = datetime('now') WHERE id = ?1",
            [id],
        )?;
        Ok(())
    }

    /// Mark directive as failed
    pub fn mark_directive_failed(&self, id: i64, error: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE directives SET status = 'failed', error_message = ?2, updated_at = datetime('now') WHERE id = ?1",
            params![id, error],
        )?;
        Ok(())
    }

    /// Reset processing directives back to pending (for resume after crash)
    pub fn reset_processing_to_pending(&self) -> Result<usize> {
        let count = self.conn.execute(
            "UPDATE directives SET status = 'pending', updated_at = datetime('now') WHERE status = 'processing'",
            [],
        )?;
        Ok(count)
    }

    /// Get archive info by hash
    pub fn get_archive(&self, hash: &str) -> Result<Option<ArchiveInfo>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT hash, name, size, meta, state_json, download_status, extraction_status, local_path, cached_url, url_expires
             FROM archives WHERE hash = ?1"
        )?;

        let result = stmt.query_row([hash], |row| {
            Ok(ArchiveInfo {
                hash: row.get(0)?,
                name: row.get(1)?,
                size: row.get(2)?,
                meta: row.get(3)?,
                state_json: row.get(4)?,
                download_status: row.get(5)?,
                extraction_status: row.get(6)?,
                local_path: row.get(7)?,
                cached_url: row.get(8)?,
                url_expires: row.get(9)?,
            })
        });

        match result {
            Ok(info) => Ok(Some(info)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e).context("Failed to query archive"),
        }
    }

    /// Mark archive as downloaded
    pub fn mark_archive_downloaded(&self, hash: &str, local_path: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE archives SET download_status = 'completed', local_path = ?2 WHERE hash = ?1",
            params![hash, local_path],
        )?;
        Ok(())
    }

    /// Get all archives that need downloading
    pub fn get_pending_downloads(&self) -> Result<Vec<ArchiveInfo>> {
        let mut stmt = self.conn.prepare(
            "SELECT hash, name, size, meta, state_json, download_status, extraction_status, local_path, cached_url, url_expires
             FROM archives WHERE download_status = 'pending'"
        )?;

        let rows = stmt.query_map([], |row| {
            Ok(ArchiveInfo {
                hash: row.get(0)?,
                name: row.get(1)?,
                size: row.get(2)?,
                meta: row.get(3)?,
                state_json: row.get(4)?,
                download_status: row.get(5)?,
                extraction_status: row.get(6)?,
                local_path: row.get(7)?,
                cached_url: row.get(8)?,
                url_expires: row.get(9)?,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Get all archives (for validation)
    pub fn get_all_archives(&self) -> Result<Vec<ArchiveInfo>> {
        let mut stmt = self.conn.prepare(
            "SELECT hash, name, size, meta, state_json, download_status, extraction_status, local_path, cached_url, url_expires
             FROM archives"
        )?;

        let rows = stmt.query_map([], |row| {
            Ok(ArchiveInfo {
                hash: row.get(0)?,
                name: row.get(1)?,
                size: row.get(2)?,
                meta: row.get(3)?,
                state_json: row.get(4)?,
                download_status: row.get(5)?,
                extraction_status: row.get(6)?,
                local_path: row.get(7)?,
                cached_url: row.get(8)?,
                url_expires: row.get(9)?,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Cache a download URL for an archive
    pub fn cache_download_url(&self, hash: &str, url: &str, expires: i64) -> Result<()> {
        self.conn.execute(
            "UPDATE archives SET cached_url = ?2, url_expires = ?3 WHERE hash = ?1",
            params![hash, url, expires],
        )?;
        Ok(())
    }

    /// Reset archive download status to pending (for re-download after corruption)
    pub fn reset_archive_download_status(&self, name: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE archives SET download_status = 'pending', local_path = NULL WHERE name = ?1",
            [name],
        )?;
        Ok(())
    }

    /// Get cached download URL if still valid
    pub fn get_cached_url(&self, hash: &str) -> Result<Option<String>> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let mut stmt = self.conn.prepare_cached(
            "SELECT cached_url FROM archives WHERE hash = ?1 AND cached_url IS NOT NULL AND url_expires > ?2"
        )?;

        match stmt.query_row(params![hash, now], |row| row.get(0)) {
            Ok(url) => Ok(Some(url)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e).context("Failed to get cached URL"),
        }
    }

    // ========== Extraction Status Methods ==========

    /// Mark archive extraction as started
    pub fn mark_archive_extracting(&self, hash: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE archives SET extraction_status = 'extracting' WHERE hash = ?1",
            [hash],
        )?;
        Ok(())
    }

    /// Mark archive extraction as completed
    pub fn mark_archive_extracted(&self, hash: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE archives SET extraction_status = 'extracted' WHERE hash = ?1",
            [hash],
        )?;
        Ok(())
    }

    /// Mark archive as not needing extraction (ZIP/BSA - direct access)
    pub fn mark_archive_no_extraction_needed(&self, hash: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE archives SET extraction_status = 'not_needed' WHERE hash = ?1",
            [hash],
        )?;
        Ok(())
    }

    /// Mark archive extraction as failed
    pub fn mark_archive_extraction_failed(&self, hash: &str, error: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE archives SET extraction_status = 'failed' WHERE hash = ?1",
            [hash],
        )?;
        // Could also store error message if needed
        let _ = error;
        Ok(())
    }

    /// Get extraction status for an archive
    pub fn get_archive_extraction_status(&self, hash: &str) -> Result<Option<String>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT extraction_status FROM archives WHERE hash = ?1"
        )?;

        match stmt.query_row([hash], |row| row.get::<_, String>(0)) {
            Ok(status) => Ok(Some(status)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e).context("Failed to get extraction status"),
        }
    }

    /// Get all archives needing extraction (7z/RAR files that haven't been extracted)
    pub fn get_archives_needing_extraction(&self) -> Result<Vec<ArchiveInfo>> {
        let mut stmt = self.conn.prepare(
            "SELECT hash, name, size, meta, state_json, download_status, extraction_status, local_path, cached_url, url_expires
             FROM archives
             WHERE download_status = 'completed'
               AND extraction_status = 'pending'
               AND (name LIKE '%.7z' OR name LIKE '%.rar')"
        )?;

        let rows = stmt.query_map([], |row| {
            Ok(ArchiveInfo {
                hash: row.get(0)?,
                name: row.get(1)?,
                size: row.get(2)?,
                meta: row.get(3)?,
                state_json: row.get(4)?,
                download_status: row.get(5)?,
                extraction_status: row.get(6)?,
                local_path: row.get(7)?,
                cached_url: row.get(8)?,
                url_expires: row.get(9)?,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Get count of archives by extraction status
    pub fn get_extraction_status_counts(&self) -> Result<(usize, usize, usize, usize)> {
        let mut stmt = self.conn.prepare(
            "SELECT extraction_status, COUNT(*) FROM archives GROUP BY extraction_status"
        )?;

        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as usize))
        })?;

        let mut pending = 0;
        let mut extracting = 0;
        let mut extracted = 0;
        let mut not_needed = 0;

        for row in rows {
            let (status, count) = row?;
            match status.as_str() {
                "pending" => pending = count,
                "extracting" => extracting = count,
                "extracted" => extracted = count,
                "not_needed" => not_needed = count,
                _ => {}
            }
        }

        Ok((pending, extracting, extracted, not_needed))
    }
}

/// Archive info from database
#[derive(Debug)]
pub struct ArchiveInfo {
    pub hash: String,
    pub name: String,
    pub size: i64,
    pub meta: String,
    pub state_json: String,
    pub download_status: String,
    pub extraction_status: String,
    pub local_path: Option<String>,
    pub cached_url: Option<String>,
    pub url_expires: Option<i64>,
}

/// Statistics for directives
#[derive(Debug, Default)]
pub struct DirectiveStats {
    pub total: usize,
    pub pending: usize,
    pub processing: usize,
    pub completed: usize,
    pub failed: usize,
}

impl DirectiveStats {
    pub fn progress_percent(&self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            (self.completed as f64 / self.total as f64) * 100.0
        }
    }
}

/// Archive file entry for indexing
pub struct ArchiveFileEntry {
    pub file_path: String,
    pub file_size: u64,
}

impl ModlistDb {
    /// Check if an archive has been indexed
    pub fn is_archive_indexed(&self, archive_hash: &str) -> Result<bool> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM archive_files WHERE archive_hash = ?1",
            params![archive_hash],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Insert archive file listings (batch)
    pub fn index_archive_files(&self, archive_hash: &str, files: &[ArchiveFileEntry]) -> Result<()> {
        // Clear existing entries for this archive
        self.conn.execute(
            "DELETE FROM archive_files WHERE archive_hash = ?1",
            params![archive_hash],
        )?;

        let mut stmt = self.conn.prepare_cached(
            "INSERT INTO archive_files (archive_hash, file_path, normalized_path, file_size) VALUES (?1, ?2, ?3, ?4)"
        )?;

        for entry in files {
            let normalized = normalize_path(&entry.file_path);
            stmt.execute(params![archive_hash, entry.file_path, normalized, entry.file_size as i64])?;
        }

        Ok(())
    }

    /// Look up actual file path from normalized path
    pub fn lookup_archive_file(&self, archive_hash: &str, path: &str) -> Result<Option<String>> {
        let normalized = normalize_path(path);

        let result = self.conn.query_row(
            "SELECT file_path FROM archive_files WHERE archive_hash = ?1 AND normalized_path = ?2",
            params![archive_hash, normalized],
            |row| row.get(0),
        );

        match result {
            Ok(path) => Ok(Some(path)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e).context("Failed to lookup archive file"),
        }
    }

    /// Get count of indexed files for an archive
    pub fn get_archive_file_count(&self, archive_hash: &str) -> Result<usize> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM archive_files WHERE archive_hash = ?1",
            params![archive_hash],
            |row| row.get(0),
        )?;
        Ok(count as usize)
    }

    /// Get all directive output info for smart download checking
    /// Returns (to_path, size, archive_hash) for all directives that need archives
    pub fn get_directive_outputs_with_archives(&self) -> Result<Vec<(String, i64, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT to_path, size, archive_hash FROM directives
             WHERE archive_hash IS NOT NULL AND archive_hash != ''"
        )?;

        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Get archives by their hashes
    pub fn get_archives_by_hashes(&self, hashes: &[String]) -> Result<Vec<ArchiveInfo>> {
        if hashes.is_empty() {
            return Ok(Vec::new());
        }

        // Build query with IN clause
        let placeholders: Vec<String> = (1..=hashes.len()).map(|i| format!("?{}", i)).collect();
        let query = format!(
            "SELECT hash, name, size, meta, state_json, download_status, extraction_status, local_path, cached_url, url_expires
             FROM archives WHERE hash IN ({})",
            placeholders.join(", ")
        );

        let mut stmt = self.conn.prepare(&query)?;

        let rows = stmt.query_map(rusqlite::params_from_iter(hashes.iter()), |row| {
            Ok(ArchiveInfo {
                hash: row.get(0)?,
                name: row.get(1)?,
                size: row.get(2)?,
                meta: row.get(3)?,
                state_json: row.get(4)?,
                download_status: row.get(5)?,
                extraction_status: row.get(6)?,
                local_path: row.get(7)?,
                cached_url: row.get(8)?,
                url_expires: row.get(9)?,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }
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
        let db = ModlistDb::in_memory().unwrap();
        db.set_metadata("test_key", "test_value").unwrap();
        let value = db.get_metadata("test_key").unwrap();
        assert_eq!(value, Some("test_value".to_string()));
    }
}
