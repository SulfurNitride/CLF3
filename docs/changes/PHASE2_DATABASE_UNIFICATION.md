# Phase 2: Database Layer Unification

## Overview

Both `modlist/db.rs` and `collection/db.rs` share significant code patterns that can be unified into a shared database module. This document catalogs the duplications and proposes a unification strategy.

## Duplicated Code Analysis

### 1. SQLite PRAGMA Configuration

**modlist/db.rs: lines 60-67 (8 lines)**
```rust
conn.execute_batch(
    "PRAGMA journal_mode = WAL;
     PRAGMA synchronous = NORMAL;
     PRAGMA cache_size = 10000;
     PRAGMA temp_store = MEMORY;
     PRAGMA mmap_size = 268435456;"
).context("Failed to configure SQLite pragmas")?;
```

**collection/db.rs: lines 71-78 (8 lines)**
```rust
conn.execute_batch(
    "PRAGMA journal_mode = WAL;
     PRAGMA synchronous = NORMAL;
     PRAGMA cache_size = 10000;
     PRAGMA temp_store = MEMORY;
     PRAGMA mmap_size = 268435456;",
)
.context("Failed to configure SQLite pragmas")?;
```

**Identical: YES** (only formatting difference - trailing comma)

---

### 2. Connection Methods

#### open() method

**modlist/db.rs: lines 55-73 (19 lines)**
```rust
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
```

**collection/db.rs: lines 65-84 (20 lines)**
```rust
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
```

**Identical: YES** (same logic, only formatting differences)

#### in_memory() method

**modlist/db.rs: lines 75-84 (10 lines)**
```rust
pub fn in_memory() -> Result<Self> {
    let conn = Connection::open_in_memory()
        .context("Failed to create in-memory database")?;

    let db = Self { conn };
    db.create_tables()?;

    Ok(db)
}
```

**collection/db.rs: lines 86-94 (9 lines)**
```rust
pub fn in_memory() -> Result<Self> {
    let conn = Connection::open_in_memory().context("Failed to create in-memory database")?;

    let db = Self { conn };
    db.create_tables()?;

    Ok(db)
}
```

**Identical: YES** (same logic, only formatting differences)

---

### 3. Metadata Methods

#### set_metadata()

**modlist/db.rs: lines 151-158 (8 lines)**
```rust
pub fn set_metadata(&self, key: &str, value: &str) -> Result<()> {
    self.conn.execute(
        "INSERT OR REPLACE INTO metadata (key, value) VALUES (?1, ?2)",
        params![key, value],
    )?;
    Ok(())
}
```

**collection/db.rs: lines 195-202 (8 lines)**
```rust
pub fn set_metadata(&self, key: &str, value: &str) -> Result<()> {
    self.conn.execute(
        "INSERT OR REPLACE INTO metadata (key, value) VALUES (?1, ?2)",
        params![key, value],
    )?;
    Ok(())
}
```

**Identical: YES**

#### get_metadata()

**modlist/db.rs: lines 160-173 (14 lines)**
```rust
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
```

**collection/db.rs: lines 204-217 (14 lines)**
```rust
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
```

**Identical: YES** (same logic, minor formatting differences)

---

### 4. normalize_path() Function

**modlist/db.rs: lines 842-844 (3 lines)**
```rust
fn normalize_path(path: &str) -> String {
    path.to_lowercase().replace('\\', "/")
}
```

**collection/db.rs: lines 972-974 (3 lines)**
```rust
fn normalize_path(path: &str) -> String {
    path.to_lowercase().replace('\\', "/")
}
```

**Identical: YES** (character-for-character identical)

---

### 5. Archive Indexing Methods

#### ArchiveFileEntry struct

**modlist/db.rs: lines 715-719 (5 lines)**
```rust
pub struct ArchiveFileEntry {
    pub file_path: String,
    pub file_size: u64,
}
```

**collection/db.rs: lines 936-940 (5 lines)**
```rust
#[derive(Debug, Clone)]
pub struct ArchiveFileEntry {
    pub file_path: String,
    pub file_size: u64,
}
```

**Nearly Identical**: collection adds `#[derive(Debug, Clone)]`

#### is_archive_indexed()

**modlist/db.rs: lines 722-730 (9 lines)**
```rust
pub fn is_archive_indexed(&self, archive_hash: &str) -> Result<bool> {
    let count: i64 = self.conn.query_row(
        "SELECT COUNT(*) FROM archive_files WHERE archive_hash = ?1",
        params![archive_hash],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}
```

**collection/db.rs: lines 657-665 (9 lines)**
```rust
pub fn is_archive_indexed(&self, mod_md5: &str) -> Result<bool> {
    let count: i64 = self.conn.query_row(
        "SELECT COUNT(*) FROM archive_files WHERE mod_md5 = ?1",
        params![mod_md5],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}
```

**Nearly Identical**: Different column name (`archive_hash` vs `mod_md5`) and parameter name

#### index_archive_files()

**modlist/db.rs: lines 732-750 (19 lines)**
```rust
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
```

**collection/db.rs: lines 667-690 (24 lines)**
```rust
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
```

**Nearly Identical**: Different column name (`archive_hash` vs `mod_md5`), otherwise same logic

#### lookup_archive_file()

**modlist/db.rs: lines 752-767 (16 lines)**
```rust
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
```

**collection/db.rs: lines 692-707 (16 lines)**
```rust
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
```

**Nearly Identical**: Different column name (`archive_hash` vs `mod_md5`), otherwise character-for-character identical

---

### 6. Metadata Table Schema

Both databases have identical metadata table definitions:

**modlist/db.rs: lines 89-93**
```sql
CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
```

**collection/db.rs: lines 100-104**
```sql
CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
```

**Identical: YES**

---

### 7. Archive Files Table Schema

**modlist/db.rs: lines 134-144**
```sql
CREATE TABLE IF NOT EXISTS archive_files (
    id INTEGER PRIMARY KEY,
    archive_hash TEXT NOT NULL,
    file_path TEXT NOT NULL,
    normalized_path TEXT NOT NULL,
    file_size INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_archive_files_hash ON archive_files(archive_hash);
CREATE INDEX IF NOT EXISTS idx_archive_files_normalized ON archive_files(archive_hash, normalized_path);
```

**collection/db.rs: lines 163-173**
```sql
CREATE TABLE IF NOT EXISTS archive_files (
    id INTEGER PRIMARY KEY,
    mod_md5 TEXT NOT NULL,
    file_path TEXT NOT NULL,
    normalized_path TEXT NOT NULL,
    file_size INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_archive_files_md5 ON archive_files(mod_md5);
CREATE INDEX IF NOT EXISTS idx_archive_files_normalized ON archive_files(mod_md5, normalized_path);
```

**Nearly Identical**: Different ID column name (`archive_hash` vs `mod_md5`), otherwise identical structure

---

## Proposed New Structure

```
src/core/db/
    mod.rs           # Re-exports
    base.rs          # BaseDb with shared functionality
    traits.rs        # ArchiveIndexDb trait
    helpers.rs       # normalize_path and utilities
```

---

## Trait Definition (Proposed)

```rust
// src/core/db/helpers.rs

/// Normalize a path for case-insensitive lookup.
/// Converts to lowercase and normalizes path separators to forward slashes.
pub fn normalize_path(path: &str) -> String {
    path.to_lowercase().replace('\\', "/")
}

/// SQLite PRAGMA configuration for optimal performance.
pub const SQLITE_PRAGMAS: &str =
    "PRAGMA journal_mode = WAL;
     PRAGMA synchronous = NORMAL;
     PRAGMA cache_size = 10000;
     PRAGMA temp_store = MEMORY;
     PRAGMA mmap_size = 268435456;";
```

```rust
// src/core/db/traits.rs

use anyhow::Result;

/// Archive file entry for indexing archive contents.
#[derive(Debug, Clone)]
pub struct ArchiveFileEntry {
    pub file_path: String,
    pub file_size: u64,
}

/// Trait for databases that support archive file indexing.
///
/// This trait provides a common interface for indexing and looking up
/// files within archives, used by both ModlistDb and CollectionDb.
pub trait ArchiveIndexDb {
    /// Get the column name used for the archive identifier.
    /// Returns "archive_hash" for ModlistDb, "mod_md5" for CollectionDb.
    fn archive_id_column(&self) -> &'static str;

    /// Check if an archive has been indexed.
    fn is_archive_indexed(&self, archive_id: &str) -> Result<bool>;

    /// Index archive files for later lookup.
    fn index_archive_files(&self, archive_id: &str, files: &[ArchiveFileEntry]) -> Result<()>;

    /// Look up the actual file path from a normalized path.
    fn lookup_archive_file(&self, archive_id: &str, path: &str) -> Result<Option<String>>;

    /// Get count of indexed files for an archive.
    fn get_archive_file_count(&self, archive_id: &str) -> Result<usize>;
}
```

```rust
// src/core/db/base.rs

use anyhow::{Context, Result};
use rusqlite::{params, Connection};
use std::path::Path;

use super::helpers::{normalize_path, SQLITE_PRAGMAS};
use super::traits::{ArchiveFileEntry, ArchiveIndexDb};

/// Common database functionality shared between ModlistDb and CollectionDb.
pub struct BaseDb {
    pub(crate) conn: Connection,
}

impl BaseDb {
    /// Open a database file with optimized PRAGMA settings.
    pub fn open(db_path: &Path) -> Result<Connection> {
        let conn = Connection::open(db_path)
            .with_context(|| format!("Failed to open database: {}", db_path.display()))?;

        conn.execute_batch(SQLITE_PRAGMAS)
            .context("Failed to configure SQLite pragmas")?;

        Ok(conn)
    }

    /// Create an in-memory database.
    pub fn open_in_memory() -> Result<Connection> {
        Connection::open_in_memory()
            .context("Failed to create in-memory database")
    }

    /// Store metadata key-value pair.
    pub fn set_metadata(conn: &Connection, key: &str, value: &str) -> Result<()> {
        conn.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES (?1, ?2)",
            params![key, value],
        )?;
        Ok(())
    }

    /// Get metadata value by key.
    pub fn get_metadata(conn: &Connection, key: &str) -> Result<Option<String>> {
        let mut stmt = conn.prepare_cached(
            "SELECT value FROM metadata WHERE key = ?1"
        )?;

        let result = stmt.query_row([key], |row| row.get(0));

        match result {
            Ok(value) => Ok(Some(value)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e).context("Failed to query metadata"),
        }
    }

    /// Check if archive is indexed (generic implementation).
    pub fn is_archive_indexed_impl(
        conn: &Connection,
        id_column: &str,
        archive_id: &str,
    ) -> Result<bool> {
        let query = format!(
            "SELECT COUNT(*) FROM archive_files WHERE {} = ?1",
            id_column
        );
        let count: i64 = conn.query_row(&query, params![archive_id], |row| row.get(0))?;
        Ok(count > 0)
    }

    /// Index archive files (generic implementation).
    pub fn index_archive_files_impl(
        conn: &Connection,
        id_column: &str,
        archive_id: &str,
        files: &[ArchiveFileEntry],
    ) -> Result<()> {
        // Clear existing entries
        let delete_query = format!(
            "DELETE FROM archive_files WHERE {} = ?1",
            id_column
        );
        conn.execute(&delete_query, params![archive_id])?;

        let insert_query = format!(
            "INSERT INTO archive_files ({}, file_path, normalized_path, file_size) VALUES (?1, ?2, ?3, ?4)",
            id_column
        );
        let mut stmt = conn.prepare_cached(&insert_query)?;

        for entry in files {
            let normalized = normalize_path(&entry.file_path);
            stmt.execute(params![
                archive_id,
                entry.file_path,
                normalized,
                entry.file_size as i64
            ])?;
        }

        Ok(())
    }

    /// Lookup archive file (generic implementation).
    pub fn lookup_archive_file_impl(
        conn: &Connection,
        id_column: &str,
        archive_id: &str,
        path: &str,
    ) -> Result<Option<String>> {
        let normalized = normalize_path(path);
        let query = format!(
            "SELECT file_path FROM archive_files WHERE {} = ?1 AND normalized_path = ?2",
            id_column
        );

        let result = conn.query_row(&query, params![archive_id, normalized], |row| row.get(0));

        match result {
            Ok(path) => Ok(Some(path)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e).context("Failed to lookup archive file"),
        }
    }

    /// Get archive file count (generic implementation).
    pub fn get_archive_file_count_impl(
        conn: &Connection,
        id_column: &str,
        archive_id: &str,
    ) -> Result<usize> {
        let query = format!(
            "SELECT COUNT(*) FROM archive_files WHERE {} = ?1",
            id_column
        );
        let count: i64 = conn.query_row(&query, params![archive_id], |row| row.get(0))?;
        Ok(count as usize)
    }
}
```

---

## Migration Steps

### Step 1: Create src/core/db/ directory
```bash
mkdir -p src/core/db
```

### Step 2: Create helper module (src/core/db/helpers.rs)
- Move `normalize_path()` function
- Add `SQLITE_PRAGMAS` constant

### Step 3: Create traits module (src/core/db/traits.rs)
- Define `ArchiveFileEntry` struct (with Debug, Clone derives)
- Define `ArchiveIndexDb` trait

### Step 4: Create base module (src/core/db/base.rs)
- Create `BaseDb` struct with static helper methods
- Implement generic versions of shared functionality

### Step 5: Create mod.rs for re-exports (src/core/db/mod.rs)
```rust
mod base;
mod helpers;
mod traits;

pub use base::BaseDb;
pub use helpers::{normalize_path, SQLITE_PRAGMAS};
pub use traits::{ArchiveFileEntry, ArchiveIndexDb};
```

### Step 6: Update ModlistDb
- Import from `crate::core::db`
- Remove duplicated code
- Delegate to `BaseDb` methods
- Keep domain-specific code (directives, archives)

### Step 7: Update CollectionDb
- Import from `crate::core::db`
- Remove duplicated code
- Delegate to `BaseDb` methods
- Keep domain-specific code (mods, plugins, rules)

---

## Estimated Line Reduction

| Location | Before | After | Change |
|----------|--------|-------|--------|
| src/core/db/ (new) | 0 | ~150 | +150 |
| modlist/db.rs | 858 | ~780 | -78 |
| collection/db.rs | 1115 | ~1035 | -80 |
| **Net Change** | 1973 | ~1965 | **-8** |

**Note**: The net line reduction is small because we're moving shared code to a new module rather than eliminating it. The real benefit is:

1. **Single source of truth** - PRAGMA settings, path normalization, etc. only defined once
2. **Consistency guarantee** - Both DBs use identical logic for shared operations
3. **Easier maintenance** - Fix bugs or add features in one place
4. **Better testing** - Shared code can be tested independently
5. **Type safety** - Trait ensures consistent interface

---

## Code Sharing Summary

| Component | Lines Shared | Benefit |
|-----------|--------------|---------|
| PRAGMA configuration | 8 | Single constant, guaranteed consistency |
| open() pattern | 19 | Shared via BaseDb::open() |
| in_memory() pattern | 10 | Shared via BaseDb::open_in_memory() |
| set_metadata() | 8 | Shared via BaseDb::set_metadata() |
| get_metadata() | 14 | Shared via BaseDb::get_metadata() |
| normalize_path() | 3 | Single shared function |
| is_archive_indexed() | 9 | Generic impl in BaseDb |
| index_archive_files() | 19 | Generic impl in BaseDb |
| lookup_archive_file() | 16 | Generic impl in BaseDb |
| ArchiveFileEntry | 5 | Single shared struct |
| **Total Shared** | **~111** | |

---

## Verification Checklist

After migration:
- [ ] `cargo check` passes
- [ ] `cargo test` passes
- [ ] `cargo clippy` passes
- [ ] ModlistDb works identically (test with existing .wabbajack files)
- [ ] CollectionDb works identically (test with existing collection.json files)
- [ ] Path normalization is consistent
- [ ] Archive indexing works for both hash types
- [ ] Metadata get/set works for both DB types

---

## Dependencies

This phase depends on:
- Phase 1: Core module structure must exist (`src/core/mod.rs`)

This phase enables:
- Phase 3: Progress system unification (may use similar patterns)
- Phase 4: Downloader unification (may need shared DB access)
