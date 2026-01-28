//! SQLite-based BSA extraction cache
//!
//! Stores extracted files on disk instead of RAM, dramatically reducing
//! memory usage for large modlist installations.
//!
//! All file paths are normalized for case-insensitive lookup:
//! - Backslashes converted to forward slashes
//! - Lowercased for storage key
//! - Original casing preserved in a separate column for retrieval

use anyhow::{Context, Result};
use rusqlite::{params, Connection};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use tracing::{debug, info, warn};

/// Normalize a path for case-insensitive lookup
/// Converts backslashes to forward slashes and lowercases
fn normalize_path(path: &str) -> String {
    path.replace('\\', "/").to_lowercase()
}

/// SQLite-based cache for BSA file extraction
///
/// Stores extracted files on disk in a temp SQLite database,
/// keeping RAM usage minimal even for large modlists.
pub struct BsaCache {
    conn: Mutex<Connection>,
    db_path: PathBuf,
    total_bytes: AtomicUsize,
    /// If true, delete the database file on drop (for temp caches)
    delete_on_drop: bool,
}

impl BsaCache {
    /// Create a new cache in a temp file
    pub fn new() -> Result<Self> {
        let db_path = std::env::temp_dir().join(format!("clf3_bsa_cache_{}.db", std::process::id()));

        let conn = Connection::open(&db_path)
            .with_context(|| format!("Failed to create cache at {}", db_path.display()))?;

        // Configure for performance with minimal memory footprint
        conn.execute_batch(
            "PRAGMA journal_mode = OFF;
             PRAGMA synchronous = OFF;
             PRAGMA cache_size = 1000;
             PRAGMA temp_store = FILE;
             PRAGMA locking_mode = EXCLUSIVE;
             PRAGMA mmap_size = 0;",
        )
        .context("Failed to configure SQLite")?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS bsa_cache (
                bsa_path TEXT NOT NULL,
                file_path_normalized TEXT NOT NULL,
                file_path_original TEXT NOT NULL,
                data BLOB NOT NULL,
                PRIMARY KEY (bsa_path, file_path_normalized)
            )",
            [],
        )
        .context("Failed to create cache table")?;

        info!("Created BSA cache at {}", db_path.display());

        Ok(Self {
            conn: Mutex::new(conn),
            db_path,
            total_bytes: AtomicUsize::new(0),
            delete_on_drop: true, // Temp caches get deleted
        })
    }

    /// Create cache at a specific path (persistent - survives program exit)
    pub fn at_path(db_path: &Path) -> Result<Self> {
        let conn = Connection::open(db_path)
            .with_context(|| format!("Failed to create cache at {}", db_path.display()))?;

        conn.execute_batch(
            "PRAGMA journal_mode = OFF;
             PRAGMA synchronous = OFF;
             PRAGMA cache_size = 1000;
             PRAGMA temp_store = FILE;
             PRAGMA locking_mode = EXCLUSIVE;
             PRAGMA mmap_size = 0;",
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS bsa_cache (
                bsa_path TEXT NOT NULL,
                file_path_normalized TEXT NOT NULL,
                file_path_original TEXT NOT NULL,
                data BLOB NOT NULL,
                PRIMARY KEY (bsa_path, file_path_normalized)
            )",
            [],
        )?;

        Ok(Self {
            conn: Mutex::new(conn),
            db_path: db_path.to_path_buf(),
            total_bytes: AtomicUsize::new(0),
            delete_on_drop: false, // Persistent caches survive program exit
        })
    }

    /// Insert a single file
    /// Path is normalized for case-insensitive lookup, original casing preserved
    pub fn insert(&self, bsa_path: &Path, file_path: &str, data: &[u8]) -> Result<usize> {
        let bsa_str = bsa_path.to_string_lossy().to_string();
        let normalized = normalize_path(file_path);
        let conn = self.conn.lock().unwrap();

        conn.execute(
            "INSERT OR REPLACE INTO bsa_cache (bsa_path, file_path_normalized, file_path_original, data) VALUES (?1, ?2, ?3, ?4)",
            params![&bsa_str, &normalized, file_path, data],
        )
        .with_context(|| format!("Failed to cache {}:{}", bsa_str, file_path))?;

        self.total_bytes.fetch_add(data.len(), Ordering::Relaxed);
        Ok(data.len())
    }

    /// Insert multiple files in a single transaction
    /// Paths are normalized for case-insensitive lookup, original casing preserved
    pub fn insert_batch(&self, bsa_path: &Path, files: &[(&str, &[u8])]) -> Result<(usize, usize)> {
        let bsa_str = bsa_path.to_string_lossy().to_string();
        let mut conn = self.conn.lock().unwrap();

        let tx = conn.transaction().context("Failed to start transaction")?;

        let mut count = 0;
        let mut bytes = 0;

        {
            let mut stmt = tx
                .prepare_cached(
                    "INSERT OR REPLACE INTO bsa_cache (bsa_path, file_path_normalized, file_path_original, data) VALUES (?1, ?2, ?3, ?4)",
                )
                .context("Failed to prepare insert")?;

            for (file_path, data) in files {
                let normalized = normalize_path(file_path);
                stmt.execute(params![&bsa_str, &normalized, file_path, data])
                    .with_context(|| format!("Failed to cache {}", file_path))?;
                count += 1;
                bytes += data.len();
            }
        }

        tx.commit().context("Failed to commit")?;
        self.total_bytes.fetch_add(bytes, Ordering::Relaxed);

        debug!("Cached {} files ({} bytes) from {}", count, bytes, bsa_str);
        Ok((count, bytes))
    }

    /// Insert files using a streaming callback
    /// The callback yields (file_path, data) pairs one at a time
    /// Paths are normalized for case-insensitive lookup, original casing preserved
    pub fn insert_streaming<F>(&self, bsa_path: &Path, mut producer: F) -> Result<(usize, usize)>
    where
        F: FnMut(&mut dyn FnMut(String, Vec<u8>) -> Result<()>) -> Result<()>,
    {
        let bsa_str = bsa_path.to_string_lossy().to_string();
        let mut conn = self.conn.lock().unwrap();

        let tx = conn.transaction().context("Failed to start transaction")?;

        let mut count = 0;
        let mut bytes = 0;

        {
            let mut stmt = tx
                .prepare_cached(
                    "INSERT OR REPLACE INTO bsa_cache (bsa_path, file_path_normalized, file_path_original, data) VALUES (?1, ?2, ?3, ?4)",
                )
                .context("Failed to prepare insert")?;

            let mut inserter = |file_path: String, data: Vec<u8>| -> Result<()> {
                bytes += data.len();
                let normalized = normalize_path(&file_path);
                stmt.execute(params![&bsa_str, &normalized, &file_path, &data])
                    .with_context(|| format!("Failed to cache {}", file_path))?;
                count += 1;
                Ok(())
            };

            producer(&mut inserter)?;
        }

        tx.commit().context("Failed to commit")?;
        self.total_bytes.fetch_add(bytes, Ordering::Relaxed);

        Ok((count, bytes))
    }

    /// Get a file from cache (case-insensitive lookup)
    pub fn get(&self, bsa_path: &Path, file_path: &str) -> Result<Option<Vec<u8>>> {
        let bsa_str = bsa_path.to_string_lossy().to_string();
        let normalized = normalize_path(file_path);
        let conn = self.conn.lock().unwrap();

        let mut stmt = conn
            .prepare_cached("SELECT data FROM bsa_cache WHERE bsa_path = ?1 AND file_path_normalized = ?2")
            .context("Failed to prepare select")?;

        match stmt.query_row(params![&bsa_str, &normalized], |row| {
            row.get::<_, Vec<u8>>(0)
        }) {
            Ok(data) => Ok(Some(data)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e).context("Failed to query cache"),
        }
    }

    /// Get multiple files from cache in parallel
    pub fn get_batch(&self, bsa_path: &Path, file_paths: &[&str]) -> Result<Vec<(String, Vec<u8>)>> {
        // For thread safety, we fetch sequentially but could batch
        let results: Vec<_> = file_paths
            .iter()
            .filter_map(|path| {
                self.get(bsa_path, path)
                    .ok()
                    .flatten()
                    .map(|data| (path.to_string(), data))
            })
            .collect();

        Ok(results)
    }

    /// Check if a file exists in cache (case-insensitive lookup)
    pub fn contains(&self, bsa_path: &Path, file_path: &str) -> Result<bool> {
        let bsa_str = bsa_path.to_string_lossy().to_string();
        let normalized = normalize_path(file_path);
        let conn = self.conn.lock().unwrap();

        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM bsa_cache WHERE bsa_path = ?1 AND file_path_normalized = ?2",
            params![&bsa_str, &normalized],
            |row| row.get(0),
        )?;

        Ok(count > 0)
    }

    /// Remove a file from cache (case-insensitive lookup)
    pub fn remove(&self, bsa_path: &Path, file_path: &str) -> Result<bool> {
        let bsa_str = bsa_path.to_string_lossy().to_string();
        let normalized = normalize_path(file_path);
        let conn = self.conn.lock().unwrap();

        let deleted = conn.execute(
            "DELETE FROM bsa_cache WHERE bsa_path = ?1 AND file_path_normalized = ?2",
            params![&bsa_str, &normalized],
        )?;

        Ok(deleted > 0)
    }

    /// Clear all cached data
    pub fn clear(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM bsa_cache", [])?;
        self.total_bytes.store(0, Ordering::Relaxed);
        Ok(())
    }

    /// Get total bytes stored
    pub fn total_bytes(&self) -> usize {
        self.total_bytes.load(Ordering::Relaxed)
    }

    /// Get number of cached files
    pub fn file_count(&self) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM bsa_cache", [], |row| row.get(0))?;
        Ok(count as usize)
    }

    /// Get database file size on disk
    pub fn db_size(&self) -> u64 {
        std::fs::metadata(&self.db_path)
            .map(|m| m.len())
            .unwrap_or(0)
    }

    /// Get the database path
    pub fn path(&self) -> &Path {
        &self.db_path
    }
}

impl Drop for BsaCache {
    fn drop(&mut self) {
        // Only delete temp caches, not persistent ones
        if self.delete_on_drop {
            if let Err(e) = std::fs::remove_file(&self.db_path) {
                warn!(
                    "Failed to remove cache file {}: {}",
                    self.db_path.display(),
                    e
                );
            } else {
                info!("Cleaned up BSA cache: {}", self.db_path.display());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_insert_and_get() -> Result<()> {
        let dir = tempdir()?;
        let cache = BsaCache::at_path(&dir.path().join("test.db"))?;

        let bsa_path = Path::new("/test/archive.bsa");
        let file_path = "textures/test.dds";
        let data = vec![1, 2, 3, 4, 5];

        cache.insert(bsa_path, file_path, &data)?;

        let retrieved = cache.get(bsa_path, file_path)?;
        assert_eq!(retrieved, Some(data));

        Ok(())
    }

    #[test]
    fn test_batch_insert() -> Result<()> {
        let dir = tempdir()?;
        let cache = BsaCache::at_path(&dir.path().join("test.db"))?;

        let bsa_path = Path::new("/test/archive.bsa");
        let files: Vec<(&str, &[u8])> = vec![
            ("file1.txt", &[1, 2, 3]),
            ("file2.txt", &[4, 5, 6]),
            ("file3.txt", &[7, 8, 9]),
        ];

        let (count, bytes) = cache.insert_batch(bsa_path, &files)?;
        assert_eq!(count, 3);
        assert_eq!(bytes, 9);
        assert_eq!(cache.file_count()?, 3);

        Ok(())
    }

    #[test]
    fn test_contains_and_remove() -> Result<()> {
        let dir = tempdir()?;
        let cache = BsaCache::at_path(&dir.path().join("test.db"))?;

        let bsa_path = Path::new("/test/archive.bsa");
        cache.insert(bsa_path, "test.txt", &[1, 2, 3])?;

        assert!(cache.contains(bsa_path, "test.txt")?);
        assert!(!cache.contains(bsa_path, "missing.txt")?);

        cache.remove(bsa_path, "test.txt")?;
        assert!(!cache.contains(bsa_path, "test.txt")?);

        Ok(())
    }

    #[test]
    fn test_case_insensitive_lookup() -> Result<()> {
        let dir = tempdir()?;
        let cache = BsaCache::at_path(&dir.path().join("test.db"))?;

        let bsa_path = Path::new("/test/archive.bsa");
        let data = vec![1, 2, 3, 4, 5];

        // Insert with one casing
        cache.insert(bsa_path, "Textures\\Armor\\Steel.dds", &data)?;

        // Retrieve with different casings - all should work
        assert!(cache.get(bsa_path, "textures/armor/steel.dds")?.is_some());
        assert!(cache.get(bsa_path, "TEXTURES\\ARMOR\\STEEL.DDS")?.is_some());
        assert!(cache.get(bsa_path, "Textures/Armor/Steel.dds")?.is_some());

        // Contains should also be case-insensitive
        assert!(cache.contains(bsa_path, "textures\\armor\\steel.dds")?);

        Ok(())
    }
}
