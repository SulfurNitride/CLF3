# CLF3 Master Unification Plan

## Executive Summary

This document provides a comprehensive plan for unifying duplicated code between `src/collection/` and `src/installer/` modules in the CLF3 codebase.

### Key Metrics

| Metric | Current State | After Unification | Reduction |
|--------|---------------|-------------------|-----------|
| **Total Lines** | ~3,724 in target files | ~2,500 | ~1,224 lines (33%) |
| **Download Coordinators** | 2 implementations (1,753 lines) | 1 shared + 2 adapters (~900 lines) | ~853 lines |
| **Database Layers** | 2 implementations (1,971 lines) | 1 base trait + 2 impls (~1,200 lines) | ~771 lines |
| **ArchiveFileEntry** | 3 definitions | 1 canonical | 2 definitions |
| **normalize_path()** | 3 implementations | 1 canonical | 2 implementations |

### New Shared Modules

1. `src/core/db/base.rs` - ArchiveIndexDb trait and ArchiveFileEntry type
2. `src/core/download/coordinator.rs` - Generic download coordinator
3. `src/core/download/progress.rs` - Shared progress bar management
4. `src/core/download/output.rs` - Download result reporting

### Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Database schema incompatibility | Low | High | Git tags before each phase; DB versioning |
| Download behavior regression | Medium | High | Comprehensive test coverage; parallel testing |
| Progress bar display issues | Low | Low | Visual testing during development |
| Performance regression | Low | Medium | Benchmark before/after each phase |

---

## Phase 1: Type Unification (Day 1)

**Goal:** Eliminate duplicate type definitions and establish canonical shared types.

### Task 1.1: Unify ArchiveFileEntry

**Current State:**
- `src/collection/db.rs:937-941` - `ArchiveFileEntry { file_path: String, file_size: u64 }`
- `src/modlist/db.rs:715-718` - `ArchiveFileEntry { file_path: String, file_size: u64 }`
- `src/archive/listing.rs` (if exists) - May have third definition

**Action:**
1. Create `src/core/db/mod.rs`:
   ```rust
   //! Core database types shared between collection and modlist databases.

   mod base;
   pub use base::{ArchiveFileEntry, ArchiveIndexDb};
   ```

2. Create `src/core/db/base.rs`:
   ```rust
   //! Base types for archive indexing.

   /// Archive file entry for indexing.
   /// Used by both CollectionDb and ModlistDb for file lookups.
   #[derive(Debug, Clone)]
   pub struct ArchiveFileEntry {
       pub file_path: String,
       pub file_size: u64,
   }
   ```

3. Update imports in:
   - `src/collection/db.rs` - Remove local definition, import from `crate::core::db::ArchiveFileEntry`
   - `src/modlist/db.rs` - Remove local definition, import from `crate::core::db::ArchiveFileEntry`

**Verification:**
```bash
cargo build 2>&1 | grep -c "error"  # Should be 0
cargo test --lib 2>&1 | grep -E "(passed|failed)"
```

**Lines Saved:** ~10 lines

---

### Task 1.2: Remove Wrapper Functions

**Current State:**
- `DownloadStats` and `FailedDownloadInfo` are re-exported in `installer/downloader.rs:13`
- Already defined in `src/downloads/mod.rs`

**Action:**
1. Verify `src/downloads/mod.rs` is the canonical source
2. Remove re-export wrapper if redundant (keep if needed for API compatibility)
3. Update any direct users to import from `crate::downloads`

**Files to Check:**
- `src/installer/downloader.rs` line 13
- Any files importing from `crate::installer::downloader::{DownloadStats, FailedDownloadInfo}`

**Verification:**
```bash
grep -r "use.*downloader.*DownloadStats" src/
grep -r "use.*downloads.*DownloadStats" src/
```

**Lines Saved:** ~3 lines

---

### Task 1.3: Extract normalize_path()

**Current State:**
- `src/collection/db.rs:972-974` - `fn normalize_path(path: &str) -> String`
- `src/modlist/db.rs:841-843` - `fn normalize_path(path: &str) -> String`
- `src/paths.rs:27-29` - `pub fn normalize_for_lookup(path: &str) -> String`

All three are identical: `path.to_lowercase().replace('\\', "/")`

**Action:**
1. Verify `src/paths.rs::normalize_for_lookup()` is already public
2. Replace usages in both db.rs files:
   ```rust
   // Before:
   let normalized = normalize_path(&entry.file_path);

   // After:
   let normalized = crate::paths::normalize_for_lookup(&entry.file_path);
   ```
3. Remove local `normalize_path()` functions from both db.rs files

**Files to Modify:**
- `src/collection/db.rs` - Remove lines 972-974, update calls
- `src/modlist/db.rs` - Remove lines 841-843, update calls

**Verification:**
```bash
cargo test test_normalize -- --nocapture  # paths.rs test
cargo test test_archive_indexing -- --nocapture  # collection/db.rs test
```

**Lines Saved:** ~8 lines

---

## Phase 2: Database Unification (Days 2-3)

**Goal:** Create a shared trait for archive indexing operations used by both database implementations.

### Task 2.1: Create src/core/db/base.rs

**Action:** Extend the base.rs created in Task 1.1 with the ArchiveIndexDb trait.

```rust
//! Base types and traits for archive indexing.

use anyhow::Result;

/// Archive file entry for indexing.
#[derive(Debug, Clone)]
pub struct ArchiveFileEntry {
    pub file_path: String,
    pub file_size: u64,
}

/// Trait for databases that support archive file indexing.
///
/// Both CollectionDb and ModlistDb implement this trait to provide
/// consistent archive file lookup functionality.
pub trait ArchiveIndexDb {
    /// Check if an archive has been indexed.
    ///
    /// # Arguments
    /// * `archive_id` - Archive identifier (MD5 hash for collections, hash for modlists)
    fn is_archive_indexed(&self, archive_id: &str) -> Result<bool>;

    /// Index archive files for lookup.
    ///
    /// Stores file paths with normalized versions for case-insensitive lookup.
    fn index_archive_files(&self, archive_id: &str, files: &[ArchiveFileEntry]) -> Result<()>;

    /// Look up actual file path from normalized path.
    ///
    /// Returns the original case-preserved path if found.
    fn lookup_archive_file(&self, archive_id: &str, normalized_path: &str) -> Result<Option<String>>;

    /// Get all files in an archive.
    fn get_archive_files(&self, archive_id: &str) -> Result<Vec<ArchiveFileEntry>>;
}
```

**Verification:**
```bash
cargo build  # Trait compiles
```

---

### Task 2.2: Implement ArchiveIndexDb for CollectionDb

**Current Implementation (collection/db.rs:655-727):**
- `is_archive_indexed()` - line 658
- `index_archive_files()` - line 668
- `lookup_archive_file()` - line 693
- `get_archive_files()` - line 710

**Action:**
1. Add trait import: `use crate::core::db::{ArchiveFileEntry, ArchiveIndexDb};`
2. Implement the trait for CollectionDb
3. Remove the standalone `ArchiveFileEntry` struct definition

**Implementation:**
```rust
impl ArchiveIndexDb for CollectionDb {
    fn is_archive_indexed(&self, archive_id: &str) -> Result<bool> {
        // Existing implementation using mod_md5 column
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM archive_files WHERE mod_md5 = ?1",
            params![archive_id],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    fn index_archive_files(&self, archive_id: &str, files: &[ArchiveFileEntry]) -> Result<()> {
        // Existing implementation
        self.conn.execute("DELETE FROM archive_files WHERE mod_md5 = ?1", params![archive_id])?;
        let mut stmt = self.conn.prepare_cached(
            "INSERT INTO archive_files (mod_md5, file_path, normalized_path, file_size) VALUES (?1, ?2, ?3, ?4)"
        )?;
        for entry in files {
            let normalized = crate::paths::normalize_for_lookup(&entry.file_path);
            stmt.execute(params![archive_id, entry.file_path, normalized, entry.file_size as i64])?;
        }
        Ok(())
    }

    fn lookup_archive_file(&self, archive_id: &str, path: &str) -> Result<Option<String>> {
        let normalized = crate::paths::normalize_for_lookup(path);
        let result = self.conn.query_row(
            "SELECT file_path FROM archive_files WHERE mod_md5 = ?1 AND normalized_path = ?2",
            params![archive_id, normalized],
            |row| row.get(0),
        );
        match result {
            Ok(path) => Ok(Some(path)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e).context("Failed to lookup archive file"),
        }
    }

    fn get_archive_files(&self, archive_id: &str) -> Result<Vec<ArchiveFileEntry>> {
        let mut stmt = self.conn.prepare(
            "SELECT file_path, file_size FROM archive_files WHERE mod_md5 = ?1"
        )?;
        let rows = stmt.query_map([archive_id], |row| {
            Ok(ArchiveFileEntry {
                file_path: row.get(0)?,
                file_size: row.get::<_, i64>(1)? as u64,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().context("Failed to get archive files")
    }
}
```

**Note:** CollectionDb uses `mod_md5` as the archive identifier column.

---

### Task 2.3: Implement ArchiveIndexDb for ModlistDb

**Current Implementation (modlist/db.rs:721-777):**
- `is_archive_indexed()` - line 723
- `index_archive_files()` - line 733
- `lookup_archive_file()` - line 752

**Action:** Same pattern as CollectionDb but using `archive_hash` column.

**Implementation:**
```rust
impl ArchiveIndexDb for ModlistDb {
    fn is_archive_indexed(&self, archive_id: &str) -> Result<bool> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM archive_files WHERE archive_hash = ?1",
            params![archive_id],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    fn index_archive_files(&self, archive_id: &str, files: &[ArchiveFileEntry]) -> Result<()> {
        self.conn.execute("DELETE FROM archive_files WHERE archive_hash = ?1", params![archive_id])?;
        let mut stmt = self.conn.prepare_cached(
            "INSERT INTO archive_files (archive_hash, file_path, normalized_path, file_size) VALUES (?1, ?2, ?3, ?4)"
        )?;
        for entry in files {
            let normalized = crate::paths::normalize_for_lookup(&entry.file_path);
            stmt.execute(params![archive_id, entry.file_path, normalized, entry.file_size as i64])?;
        }
        Ok(())
    }

    fn lookup_archive_file(&self, archive_id: &str, path: &str) -> Result<Option<String>> {
        let normalized = crate::paths::normalize_for_lookup(path);
        let result = self.conn.query_row(
            "SELECT file_path FROM archive_files WHERE archive_hash = ?1 AND normalized_path = ?2",
            params![archive_id, normalized],
            |row| row.get(0),
        );
        match result {
            Ok(path) => Ok(Some(path)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e).context("Failed to lookup archive file"),
        }
    }

    fn get_archive_files(&self, archive_id: &str) -> Result<Vec<ArchiveFileEntry>> {
        // ModlistDb doesn't have this method currently - add it
        let mut stmt = self.conn.prepare(
            "SELECT file_path, file_size FROM archive_files WHERE archive_hash = ?1"
        )?;
        let rows = stmt.query_map([archive_id], |row| {
            Ok(ArchiveFileEntry {
                file_path: row.get(0)?,
                file_size: row.get::<_, i64>(1)? as u64,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().context("Failed to get archive files")
    }
}
```

**Note:** ModlistDb uses `archive_hash` as the archive identifier column.

---

### Task 2.4: Create Generic Archive Indexing Utility

**Action:** Create a shared function that can index archives using any ArchiveIndexDb implementation.

Create `src/core/db/indexing.rs`:
```rust
//! Shared archive indexing utilities.

use std::path::Path;
use anyhow::Result;
use rayon::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::base::{ArchiveFileEntry, ArchiveIndexDb};
use crate::archive::list_archive_files;

/// Index multiple archives in parallel.
///
/// Returns count of (successful, failed) indexing operations.
pub fn index_archives_parallel<D: ArchiveIndexDb + Sync>(
    db: &D,
    archives: &[(String, &Path)],  // (archive_id, path)
) -> Result<(usize, usize)> {
    let successful = AtomicUsize::new(0);
    let failed = AtomicUsize::new(0);

    archives.par_iter().for_each(|(archive_id, path)| {
        if db.is_archive_indexed(archive_id).unwrap_or(false) {
            successful.fetch_add(1, Ordering::Relaxed);
            return;
        }

        match list_archive_files(path) {
            Ok(files) => {
                let entries: Vec<ArchiveFileEntry> = files
                    .into_iter()
                    .map(|f| ArchiveFileEntry {
                        file_path: f.path,
                        file_size: f.size,
                    })
                    .collect();

                if db.index_archive_files(archive_id, &entries).is_ok() {
                    successful.fetch_add(1, Ordering::Relaxed);
                } else {
                    failed.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(_) => {
                failed.fetch_add(1, Ordering::Relaxed);
            }
        }
    });

    Ok((successful.load(Ordering::Relaxed), failed.load(Ordering::Relaxed)))
}
```

**Verification:**
```bash
cargo test test_archive_indexing
cargo test test_create_db
```

**Lines Saved in Phase 2:** ~200 lines (duplicate method implementations)

---

## Phase 3: Download Unification (Days 4-5)

**Goal:** Unify the download coordination logic between collection and installer modules.

### Task 3.1: Create src/core/download/progress.rs

**Current State:**
- `collection/download.rs:439-447` - `update_overall_message()`
- `installer/downloader.rs:392-400` - `update_overall_message()`

Both are identical.

**Action:** Create shared progress management.

```rust
//! Download progress display management.

use std::sync::atomic::{AtomicUsize, Ordering};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::time::Duration;

/// Shared download progress state.
pub struct DownloadProgress {
    pub multi_progress: MultiProgress,
    pub overall_pb: ProgressBar,
    pub downloaded: AtomicUsize,
    pub skipped: AtomicUsize,
    pub failed: AtomicUsize,
}

impl DownloadProgress {
    /// Create a new progress display for the given total count.
    pub fn new(total: usize) -> Self {
        let multi_progress = MultiProgress::new();
        let overall_pb = multi_progress.add(ProgressBar::new(total as u64));
        overall_pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) | {msg}")
                .unwrap()
                .progress_chars("=>-"),
        );
        overall_pb.enable_steady_tick(Duration::from_millis(100));
        overall_pb.set_message("Starting downloads...");

        Self {
            multi_progress,
            overall_pb,
            downloaded: AtomicUsize::new(0),
            skipped: AtomicUsize::new(0),
            failed: AtomicUsize::new(0),
        }
    }

    /// Update the overall progress bar message with current stats.
    pub fn update_message(&self) {
        let downloaded = self.downloaded.load(Ordering::Relaxed);
        let skipped = self.skipped.load(Ordering::Relaxed);
        let failed = self.failed.load(Ordering::Relaxed);
        self.overall_pb.set_message(format!(
            "OK:{} Skip:{} Fail:{}",
            downloaded, skipped, failed
        ));
    }

    /// Create a progress bar for an individual download.
    pub fn create_item_bar(&self, size: u64) -> ProgressBar {
        let pb = self.multi_progress.insert_before(&self.overall_pb, ProgressBar::new(size));
        pb.set_style(
            ProgressStyle::default_bar()
                .template("  {spinner:.blue} {wide_msg} [{bar:30.white/dim}] {bytes}/{total_bytes} {bytes_per_sec}")
                .unwrap()
                .progress_chars("=>-"),
        );
        pb.enable_steady_tick(Duration::from_millis(100));
        pb
    }

    /// Increment downloaded count and update progress.
    pub fn record_success(&self) {
        self.downloaded.fetch_add(1, Ordering::Relaxed);
        self.overall_pb.inc(1);
        self.update_message();
    }

    /// Increment skipped count and update progress.
    pub fn record_skip(&self) {
        self.skipped.fetch_add(1, Ordering::Relaxed);
        self.overall_pb.inc(1);
        self.update_message();
    }

    /// Increment failed count and update progress.
    pub fn record_failure(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
        self.overall_pb.inc(1);
        self.update_message();
    }

    /// Print a message to the progress display.
    pub fn println(&self, msg: impl AsRef<str>) {
        self.overall_pb.println(msg.as_ref());
    }

    /// Finish and clear the progress display.
    pub fn finish(&self) {
        self.overall_pb.finish_and_clear();
    }

    /// Get current statistics.
    pub fn stats(&self) -> (usize, usize, usize) {
        (
            self.downloaded.load(Ordering::Relaxed),
            self.skipped.load(Ordering::Relaxed),
            self.failed.load(Ordering::Relaxed),
        )
    }
}
```

---

### Task 3.2: Create src/core/download/coordinator.rs

**Action:** Create a generic download coordinator that can be used by both collection and installer.

```rust
//! Generic download coordinator.

use std::path::Path;
use std::sync::Arc;
use anyhow::Result;
use futures::stream::{self, StreamExt};
use tokio::sync::Mutex;

use super::progress::DownloadProgress;
use crate::core::download::DownloadResult;
use crate::downloads::{DownloadStats, FailedDownloadInfo, ManualDownloadInfo};

/// Trait for items that can be downloaded.
pub trait Downloadable: Send + Sync + 'static {
    /// Unique identifier for this download.
    fn id(&self) -> String;

    /// Display name for progress.
    fn name(&self) -> &str;

    /// Expected file size in bytes.
    fn expected_size(&self) -> u64;

    /// Output path for the downloaded file.
    fn output_path(&self, downloads_dir: &Path) -> std::path::PathBuf;
}

/// Trait for download sources/handlers.
#[async_trait::async_trait]
pub trait DownloadHandler<T: Downloadable>: Send + Sync {
    /// Check if item already exists with correct size/hash.
    async fn check_exists(&self, item: &T) -> bool;

    /// Download a single item.
    /// Returns DownloadResult and optional failed/manual download info.
    async fn download(
        &self,
        item: &T,
        progress: &DownloadProgress,
    ) -> (DownloadResult, Option<FailedDownloadInfo>, Option<ManualDownloadInfo>);
}

/// Generic download coordinator.
pub struct DownloadCoordinator<T: Downloadable, H: DownloadHandler<T>> {
    handler: Arc<H>,
    concurrency: usize,
    _marker: std::marker::PhantomData<T>,
}

impl<T: Downloadable, H: DownloadHandler<T>> DownloadCoordinator<T, H> {
    pub fn new(handler: H, concurrency: usize) -> Self {
        Self {
            handler: Arc::new(handler),
            concurrency,
            _marker: std::marker::PhantomData,
        }
    }

    /// Download all items in parallel.
    pub async fn download_all(&self, items: Vec<T>) -> Result<DownloadStats> {
        if items.is_empty() {
            return Ok(DownloadStats::default());
        }

        let progress = Arc::new(DownloadProgress::new(items.len()));
        let failed_downloads = Arc::new(Mutex::new(Vec::<FailedDownloadInfo>::new()));
        let manual_downloads = Arc::new(Mutex::new(Vec::<ManualDownloadInfo>::new()));

        let _results: Vec<DownloadResult> = stream::iter(items)
            .map(|item| {
                let handler = Arc::clone(&self.handler);
                let progress = Arc::clone(&progress);
                let failed = Arc::clone(&failed_downloads);
                let manual = Arc::clone(&manual_downloads);
                async move {
                    let (result, failed_info, manual_info) = handler.download(&item, &progress).await;

                    if let Some(info) = failed_info {
                        failed.lock().await.push(info);
                    }
                    if let Some(info) = manual_info {
                        manual.lock().await.push(info);
                    }

                    result
                }
            })
            .buffer_unordered(self.concurrency)
            .collect()
            .await;

        progress.finish();

        let (downloaded, skipped, failed_count) = progress.stats();
        let manual_count = manual_downloads.lock().await.len();

        Ok(DownloadStats {
            downloaded,
            skipped,
            failed: failed_count,
            manual: manual_count,
        })
    }
}
```

---

### Task 3.3: Create src/core/download/output.rs

**Action:** Create shared download result reporting.

```rust
//! Download result output and reporting.

use crate::downloads::{DownloadStats, FailedDownloadInfo, ManualDownloadInfo};
use std::path::Path;

/// Print download summary to stdout.
pub fn print_download_summary(stats: &DownloadStats) {
    println!("\n=== Download Summary ===");
    println!("Downloaded: {}", stats.downloaded);
    println!("Skipped:    {}", stats.skipped);
    println!("Manual:     {}", stats.manual);
    println!("Failed:     {}", stats.failed);
}

/// Print failed download instructions.
pub fn print_failed_downloads(failed: &[FailedDownloadInfo], downloads_dir: &Path) {
    if failed.is_empty() {
        return;
    }

    println!("\n=== Failed Downloads ({}) ===", failed.len());
    println!("These downloads failed. Try manually downloading to: {}\n", downloads_dir.display());

    for (i, fd) in failed.iter().enumerate() {
        println!("{}. {}", i + 1, fd.name);
        println!("   URL: {}", fd.url);
        println!("   Error: {}", fd.error);
        println!("   Expected size: {} bytes ({:.2} MB)", fd.expected_size, fd.expected_size as f64 / 1024.0 / 1024.0);
        println!();
    }

    println!("After downloading, run the command again to continue.\n");
}

/// Print manual download instructions.
pub fn print_manual_downloads(manual: &[ManualDownloadInfo], downloads_dir: &Path) {
    if manual.is_empty() {
        return;
    }

    println!("\n=== Manual Downloads Required ({}) ===", manual.len());
    println!("Please download the following files to: {}\n", downloads_dir.display());

    for (i, md) in manual.iter().enumerate() {
        println!("{}. {}", i + 1, md.name);
        println!("   URL: {}", md.url);
        println!("   Expected size: {} bytes", md.expected_size);
        if let Some(prompt) = &md.prompt {
            println!("   Note: {}", prompt);
        }
        println!();
    }

    println!("After downloading, run the command again to continue.\n");
}

/// Print Nexus API rate limits.
pub fn print_nexus_limits(hourly_remaining: u32, hourly_limit: u32, daily_remaining: u32, daily_limit: u32) {
    println!(
        "\nNexus API: {}/{} hourly, {}/{} daily",
        hourly_remaining, hourly_limit,
        daily_remaining, daily_limit
    );
}
```

---

### Task 3.4: Refactor collection/download.rs

**Action:** Update to use shared coordinator.

1. Remove local `DownloadContext` struct (use DownloadProgress)
2. Remove local `update_overall_message()` function
3. Implement `Downloadable` for `ModDbEntry`
4. Implement `DownloadHandler` for collection-specific logic
5. Use shared output functions

**Lines to Remove:**
- Lines 34-47: `DownloadContext` struct
- Lines 438-447: `update_overall_message()` function

**Lines Saved:** ~60 lines

---

### Task 3.5: Refactor installer/downloader.rs

**Action:** Same pattern as collection/download.rs.

1. Remove local `DownloadContext` struct
2. Remove local `update_overall_message()` function
3. Implement `Downloadable` for `ArchiveInfo`
4. Implement `DownloadHandler` for installer-specific logic (multi-source)
5. Keep NXM mode as installer-specific extension

**Lines to Remove:**
- Lines 64-80: `DownloadContext` struct
- Lines 391-400: `update_overall_message()` function

**Lines Saved:** ~60 lines

---

## Phase 4: Update core/mod.rs

**Action:** Update the core module to export all new types.

```rust
//! Core shared types for both Collection and Wabbajack installers.

pub mod db;
pub mod download;
pub mod error;
pub mod stats;

// Re-exports for convenience
pub use db::{ArchiveFileEntry, ArchiveIndexDb};
pub use download::DownloadResult;
pub use error::root_cause;
pub use stats::StatsSnapshot;
```

---

## Verification Checklist

### After Each Task

- [ ] `cargo build` completes without errors
- [ ] `cargo test --lib` passes all tests
- [ ] `cargo clippy` has no new warnings
- [ ] No regression in functionality (manual test if needed)

### After Phase 1 (Type Unification)

- [ ] Only one `ArchiveFileEntry` definition exists in `src/core/db/base.rs`
- [ ] Only one `normalize_path` implementation is used (`paths::normalize_for_lookup`)
- [ ] `cargo test test_normalize` passes
- [ ] `cargo test test_archive_indexing` passes

### After Phase 2 (Database Unification)

- [ ] `ArchiveIndexDb` trait is implemented by both `CollectionDb` and `ModlistDb`
- [ ] `cargo test` - all database tests pass
- [ ] Archive indexing works for both collection and modlist flows

### After Phase 3 (Download Unification)

- [ ] `DownloadProgress` is used by both download modules
- [ ] `cargo test` - all download-related tests pass
- [ ] Manual test: download a small collection
- [ ] Manual test: download archives for a modlist

### Final Verification

- [ ] All tests pass: `cargo test`
- [ ] No clippy warnings: `cargo clippy -- -D warnings`
- [ ] Line count reduced by at least 800 lines
- [ ] No functionality regression

**Line Count Check:**
```bash
# Before starting
wc -l src/collection/download.rs src/installer/downloader.rs src/collection/db.rs src/modlist/db.rs

# After completion (should show reduction)
wc -l src/collection/download.rs src/installer/downloader.rs src/collection/db.rs src/modlist/db.rs src/core/**/*.rs
```

---

## Rollback Plan

### Git Tags

Create tags before each phase:
```bash
git tag -a pre-unification-v1 -m "Before Phase 1: Type Unification"
git tag -a pre-unification-v2 -m "Before Phase 2: Database Unification"
git tag -a pre-unification-v3 -m "Before Phase 3: Download Unification"
```

### Rollback Procedure

If issues are discovered after a phase:

1. **Immediate Rollback:**
   ```bash
   git reset --hard pre-unification-vN
   ```

2. **Partial Rollback (keep some changes):**
   ```bash
   git diff pre-unification-vN -- src/specific/file.rs > changes.patch
   git reset --hard pre-unification-vN
   git apply changes.patch
   ```

3. **Database Rollback:**
   - If database schema changed, delete the SQLite database files
   - They will be regenerated on next run

### Smoke Test Script

Create `scripts/smoke_test.sh`:
```bash
#!/bin/bash
set -e

echo "Running smoke tests..."

# Build
cargo build --release

# Unit tests
cargo test --lib

# Clippy
cargo clippy -- -D warnings

# Integration test (if available)
# cargo test --test integration

echo "All smoke tests passed!"
```

---

## File Summary

### New Files to Create

| File | Purpose | Lines (est) |
|------|---------|-------------|
| `src/core/db/mod.rs` | DB module exports | 5 |
| `src/core/db/base.rs` | ArchiveFileEntry, ArchiveIndexDb trait | 50 |
| `src/core/db/indexing.rs` | Parallel indexing utility | 50 |
| `src/core/download/progress.rs` | DownloadProgress struct | 100 |
| `src/core/download/coordinator.rs` | Generic coordinator | 80 |
| `src/core/download/output.rs` | Result reporting | 60 |
| **Total new** | | ~345 |

### Files to Modify

| File | Changes | Lines Removed (est) |
|------|---------|---------------------|
| `src/core/mod.rs` | Add db submodule | 0 |
| `src/collection/db.rs` | Use trait, remove dups | 80 |
| `src/modlist/db.rs` | Use trait, remove dups | 60 |
| `src/collection/download.rs` | Use shared progress | 60 |
| `src/installer/downloader.rs` | Use shared progress | 60 |
| **Total removed** | | ~260 |

### Net Result

- **New code:** ~345 lines
- **Removed code:** ~260 lines + eliminated duplication
- **Effective reduction:** ~800-1,000 lines of duplicate logic consolidated

---

## Implementation Schedule

| Day | Phase | Tasks | Estimated Time |
|-----|-------|-------|----------------|
| 1 | Phase 1 | Tasks 1.1-1.3 | 2-3 hours |
| 2 | Phase 2 | Tasks 2.1-2.2 | 3-4 hours |
| 3 | Phase 2 | Tasks 2.3-2.4 | 2-3 hours |
| 4 | Phase 3 | Tasks 3.1-3.3 | 4-5 hours |
| 5 | Phase 3 | Tasks 3.4-3.5, Phase 4 | 3-4 hours |

**Total:** ~15-19 hours of development work

---

## Success Criteria

1. **Code Reduction:** At least 800 lines of duplicate code eliminated
2. **Test Coverage:** All existing tests continue to pass
3. **No Regressions:** Manual testing confirms identical behavior
4. **Clean Build:** `cargo clippy` passes without warnings
5. **Maintainability:** New shared types are well-documented and easy to extend
