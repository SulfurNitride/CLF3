# Phase 1.1: ArchiveFileEntry Unification

## Current State

| Location | File | Lines | Derives |
|----------|------|-------|---------|
| 1 (canonical) | `src/archive/listing.rs:15-20` | 6 lines | `#[derive(Debug, Clone)]` |
| 2 (duplicate) | `src/collection/db.rs:938-941` | 4 lines | `#[derive(Debug, Clone)]` |
| 3 (duplicate) | `src/modlist/db.rs:716-719` | 4 lines | None |

### Definition Comparison

**src/archive/listing.rs (canonical, lines 13-20):**
```rust
/// Entry representing a file within an archive.
#[derive(Debug, Clone)]
pub struct ArchiveFileEntry {
    /// Path of the file within the archive
    pub file_path: String,
    /// Size of the file in bytes (uncompressed)
    pub file_size: u64,
}
```

**src/collection/db.rs (duplicate, lines 936-941):**
```rust
/// Archive file entry for indexing
#[derive(Debug, Clone)]
pub struct ArchiveFileEntry {
    pub file_path: String,
    pub file_size: u64,
}
```

**src/modlist/db.rs (duplicate, lines 715-719):**
```rust
/// Archive file entry for indexing
pub struct ArchiveFileEntry {
    pub file_path: String,
    pub file_size: u64,
}
```

### Key Differences

1. **Derives**: `modlist/db.rs` version has NO derive macros - missing `Debug` and `Clone`
2. **Documentation**: `archive/listing.rs` has more detailed doc comments
3. **Fields**: All three have identical fields (`file_path: String`, `file_size: u64`)

## Files That Import Each Version

### archive/listing.rs version (via `crate::archive::ArchiveFileEntry`)
- `src/archive/mod.rs:22` - Re-exports as `pub use listing::ArchiveFileEntry`
- `src/collection/extract.rs:23` - Imports from `crate::archive`
- `src/collection/extract.rs:33` - Re-exports as `ExtractArchiveFileEntry` (compatibility alias)

### collection/db.rs version (via `crate::collection::db::ArchiveFileEntry`)
- `src/collection/mod.rs:34` - Re-exports as `pub use db::ArchiveFileEntry`
- `src/collection/extract.rs:67,71,79,83,93` - Uses `super::db::ArchiveFileEntry` in compat wrappers

### modlist/db.rs version (via `crate::modlist::ArchiveFileEntry`)
- `src/modlist/mod.rs:12` - Re-exports via `pub use db::*`
- `src/installer/processor.rs:12` - Imports `use crate::modlist::ArchiveFileEntry`
- `src/installer/processor.rs:155,223` - Constructs instances

## Compatibility Layer Analysis

### collection/extract.rs
Currently has wrapper functions that convert between types:
```rust
/// List all files in an archive using 7z (wrapper for backwards compatibility)
pub fn list_archive_files_compat(archive_path: &Path) -> Result<Vec<super::db::ArchiveFileEntry>> {
    let files = list_archive_files(archive_path)?;
    Ok(files
        .into_iter()
        .map(|f| super::db::ArchiveFileEntry {
            file_path: f.file_path,
            file_size: f.file_size,
        })
        .collect())
}
```

### installer/processor.rs
Has its own wrapper function:
```rust
/// List all files in an archive using 7z (wrapper for modlist ArchiveFileEntry type)
fn list_archive_files_compat(archive_path: &Path) -> Result<Vec<ArchiveFileEntry>> {
    let files = list_archive_files(archive_path)?;
    Ok(files
        .into_iter()
        .map(|f| ArchiveFileEntry {
            file_path: f.file_path,
            file_size: f.file_size,
        })
        .collect())
}
```

## Changes Required

### Step 1: Update modlist/db.rs Definition (add derives)
**File:** `src/modlist/db.rs`
**Action:** Add `#[derive(Debug, Clone)]` to ArchiveFileEntry (line 715)
**Reason:** Required for compatibility before removal

### Step 2: Remove Duplicate from collection/db.rs
**File:** `src/collection/db.rs`
**Action:** Remove lines 936-941 (struct definition)
**Lines to remove:**
```rust
/// Archive file entry for indexing
#[derive(Debug, Clone)]
pub struct ArchiveFileEntry {
    pub file_path: String,
    pub file_size: u64,
}
```

### Step 3: Update collection/mod.rs Exports
**File:** `src/collection/mod.rs`
**Action:** Change import/export
**Current (line 33-35):**
```rust
pub use db::{
    ArchiveFileEntry, CollectionDb, InstalledFileEntry, ModDbEntry, ModRuleDbEntry, ModStats,
    ModStatus, PluginDbEntry,
};
```
**Change to:**
```rust
pub use db::{
    CollectionDb, InstalledFileEntry, ModDbEntry, ModRuleDbEntry, ModStats,
    ModStatus, PluginDbEntry,
};
pub use crate::archive::ArchiveFileEntry;
```

### Step 4: Update collection/extract.rs
**File:** `src/collection/extract.rs`
**Action:** Remove compat wrappers and use archive type directly
**Changes:**
1. Line 33: Remove the re-export alias `ExtractArchiveFileEntry` (or keep for backwards compat)
2. Lines 67-88: Remove `list_archive_files_compat` and `list_zip_files_compat` functions
3. Lines 91-94: Change `IndexResult` to use `crate::archive::ArchiveFileEntry`

### Step 5: Update collection/db.rs Usage
**File:** `src/collection/db.rs`
**Action:** Add import at top of file
**Add:** `use crate::archive::ArchiveFileEntry;`
**Check functions:** `index_archive_files` (line 668), `get_archive_files` (line 710)

### Step 6: Remove Duplicate from modlist/db.rs
**File:** `src/modlist/db.rs`
**Action:** Remove lines 715-719
**Lines to remove:**
```rust
/// Archive file entry for indexing
pub struct ArchiveFileEntry {
    pub file_path: String,
    pub file_size: u64,
}
```

### Step 7: Update modlist/mod.rs or modlist/db.rs
**File:** `src/modlist/db.rs` (top of file)
**Action:** Add import
**Add:** `use crate::archive::ArchiveFileEntry;`

### Step 8: Update installer/processor.rs
**File:** `src/installer/processor.rs`
**Action:**
1. Change import on line 12 from `use crate::modlist::ArchiveFileEntry` to `use crate::archive::ArchiveFileEntry`
2. Remove `list_archive_files_compat` function (lines 150-160)
3. Update usages at lines 223 to use `crate::archive::ArchiveFileEntry` directly

## Verification Steps

- [ ] `cargo check` passes
- [ ] `cargo test` passes
- [ ] `cargo clippy` passes with no new warnings
- [ ] `grep -r "pub struct ArchiveFileEntry" src/` shows only 1 definition
- [ ] `grep -r "ArchiveFileEntry" src/` shows consistent usage

## Lines Removed

| File | Lines Removed | Description |
|------|---------------|-------------|
| `src/collection/db.rs` | 6 lines | Struct definition |
| `src/collection/extract.rs` | ~25 lines | Compat wrappers (list_archive_files_compat, list_zip_files_compat) |
| `src/modlist/db.rs` | 5 lines | Struct definition |
| `src/installer/processor.rs` | ~10 lines | list_archive_files_compat function |

**Summary:**
- Before: ~46 lines across 4 files (3 definitions + 2 compat wrappers)
- After: 8 lines in 1 file (src/archive/listing.rs)
- Net reduction: ~38 lines
- Compat wrappers eliminated: 2

## Risk Assessment

**Low Risk:**
- All three definitions have identical fields
- The canonical version already has `Debug` and `Clone` derives
- No behavioral differences between versions

**Notes:**
- The `modlist/db.rs` version lacks `Debug` and `Clone` derives, but since we're using the canonical version which has them, this is actually an improvement
- Removing compat wrappers eliminates unnecessary allocations from `.map().collect()` conversions
