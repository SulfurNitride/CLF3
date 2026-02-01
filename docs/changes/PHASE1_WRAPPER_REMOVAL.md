# Phase 1.2: Wrapper Function Removal

## Overview

This document analyzes the wrapper functions in `src/collection/extract.rs` that exist for "backwards compatibility" but add unnecessary indirection. These wrappers convert between identical `ArchiveFileEntry` types that exist in three separate locations.

## Type Duplication Analysis

There are **three identical** `ArchiveFileEntry` structs in the codebase:

| Location | Fields | Purpose |
|----------|--------|---------|
| `src/archive/listing.rs:15-20` | `file_path: String`, `file_size: u64` | Canonical shared type |
| `src/collection/db.rs:938-941` | `file_path: String`, `file_size: u64` | Collection database type |
| `src/modlist/db.rs:716-719` | `file_path: String`, `file_size: u64` | Modlist database type |

All three types have **identical fields** - the wrappers exist only to convert between these duplicated types.

---

## Wrapper Functions to Remove

### 1. `list_archive_files_compat` (lines 67-76)

**Location:** `/home/luke/Documents/Wabbajack Rust Update/clf3/src/collection/extract.rs`

**Code:**
```rust
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

**Purpose:** Converts `crate::archive::ArchiveFileEntry` to `super::db::ArchiveFileEntry`

**Callers:**
| File | Line | Context |
|------|------|---------|
| `src/collection/extract.rs` | 163 | `index_all_archives()` - non-ZIP archive listing |
| `src/collection/extract.rs` | 699 | `extract_all_mods()` - non-ZIP archive listing in phase 1 |

**Replacement:** Direct call to `crate::archive::list_archive_files` after unifying `ArchiveFileEntry` types

---

### 2. `list_zip_files_compat` (lines 79-88)

**Location:** `/home/luke/Documents/Wabbajack Rust Update/clf3/src/collection/extract.rs`

**Code:**
```rust
pub fn list_zip_files_compat(archive_path: &Path) -> Result<Vec<super::db::ArchiveFileEntry>> {
    let files = list_zip_files(archive_path)?;
    Ok(files
        .into_iter()
        .map(|f| super::db::ArchiveFileEntry {
            file_path: f.file_path,
            file_size: f.file_size,
        })
        .collect())
}
```

**Purpose:** Converts `crate::archive::ArchiveFileEntry` to `super::db::ArchiveFileEntry`

**Callers:**
| File | Line | Context |
|------|------|---------|
| `src/collection/extract.rs` | 162 | `index_all_archives()` - ZIP archive listing |
| `src/collection/extract.rs` | 698 | `extract_all_mods()` - ZIP archive listing in phase 1 |

**Replacement:** Direct call to `crate::archive::list_zip_files` after unifying `ArchiveFileEntry` types

---

### 3. `extract_archive_to_dir_pub` (lines 218-220)

**Location:** `/home/luke/Documents/Wabbajack Rust Update/clf3/src/collection/extract.rs`

**Code:**
```rust
pub fn extract_archive_to_dir_pub(archive_path: &Path, output_dir: &Path) -> Result<()> {
    extract_archive_to_dir(archive_path, output_dir)
}
```

**Purpose:** Trivial public wrapper around `crate::archive::extract_archive_to_dir`

**Callers:**
| File | Line | Context |
|------|------|---------|
| `src/collection/installer.rs` | 921 | `FomodProcessor::process_with_choices()` - FOMOD extraction |
| `src/collection/installer.rs` | 982 | `process_single_fomod_static()` - parallel FOMOD processing |

**Replacement:** Direct call to `crate::archive::extract_archive_to_dir`

---

### 4. `ExtractArchiveFileEntry` re-export (line 33)

**Location:** `/home/luke/Documents/Wabbajack Rust Update/clf3/src/collection/extract.rs`

**Code:**
```rust
pub use crate::archive::ArchiveFileEntry as ExtractArchiveFileEntry;
```

**Purpose:** Type alias for backwards compatibility

**Users:**
| File | Line | Context |
|------|------|---------|
| None found | - | Unused alias (no external references) |

**Replacement:** Remove entirely - not used anywhere

---

## Note: Separate Wrapper in `installer/processor.rs`

There is also a **private** `list_archive_files_compat` function in `src/installer/processor.rs:151-160` that converts to `crate::modlist::ArchiveFileEntry`. This is a separate issue related to the modlist database type duplication.

| File | Line | Context |
|------|------|---------|
| `src/installer/processor.rs` | 151-160 | Definition (private function) |
| `src/installer/processor.rs` | 231 | `index_archives()` - archive indexing for modlist |

---

## Changes Required Per File

| File | Change | Lines Affected |
|------|--------|----------------|
| `src/collection/extract.rs` | Remove `list_archive_files_compat` function | -10 lines (67-76) |
| `src/collection/extract.rs` | Remove `list_zip_files_compat` function | -10 lines (79-88) |
| `src/collection/extract.rs` | Remove `extract_archive_to_dir_pub` wrapper | -4 lines (216-220) |
| `src/collection/extract.rs` | Remove `ExtractArchiveFileEntry` re-export | -2 lines (32-33) |
| `src/collection/extract.rs` | Update calls at lines 162-163 to use direct functions | ~2 lines changed |
| `src/collection/extract.rs` | Update calls at lines 698-699 to use direct functions | ~2 lines changed |
| `src/collection/installer.rs` | Update line 921 to use `crate::archive::extract_archive_to_dir` | ~1 line changed |
| `src/collection/installer.rs` | Update line 982 to use `crate::archive::extract_archive_to_dir` | ~1 line changed |
| `src/collection/db.rs` | Remove duplicate `ArchiveFileEntry` struct (lines 936-941) | -6 lines |
| `src/collection/db.rs` | Add `use crate::archive::ArchiveFileEntry;` | +1 line |

---

## Dependencies

**Before removing wrappers, must first:**

1. Unify the `ArchiveFileEntry` type:
   - Keep canonical type in `src/archive/listing.rs`
   - Update `src/collection/db.rs` to use `crate::archive::ArchiveFileEntry`
   - Update `src/modlist/db.rs` to use `crate::archive::ArchiveFileEntry` (separate phase)

2. Update `IndexResult` struct in `extract.rs` (line 91-94):
   - Change `files: Vec<super::db::ArchiveFileEntry>` to `files: Vec<crate::archive::ArchiveFileEntry>`

3. Update `CollectionDb::index_archive_files` method signature:
   - Accept `&[crate::archive::ArchiveFileEntry]` instead of local type

---

## Verification Steps

After making changes:

- [ ] `cargo check` passes with no errors
- [ ] `cargo clippy` shows no new warnings
- [ ] All callers updated to use direct archive functions
- [ ] No orphaned imports remain
- [ ] `cargo test` passes (if tests exist)

---

## Lines Removed Summary

| Component | Lines Removed |
|-----------|--------------|
| `list_archive_files_compat` wrapper | 10 |
| `list_zip_files_compat` wrapper | 10 |
| `extract_archive_to_dir_pub` wrapper | 4 |
| `ExtractArchiveFileEntry` re-export | 2 |
| `ArchiveFileEntry` duplicate in `collection/db.rs` | 6 |
| **Total** | **~32 lines** |

---

## Risk Assessment

**Low Risk:**
- All wrapper functions are trivial pass-through or type conversion
- No business logic in any wrapper
- Types are structurally identical (same fields, same types)
- Clear 1:1 mapping between callers and replacements

**Testing:**
- Manual verification with `cargo check`
- Run any existing tests
- Test archive indexing and extraction functionality

---

## Implementation Order

1. First: Unify `ArchiveFileEntry` type in `collection/db.rs` to use `crate::archive::ArchiveFileEntry`
2. Second: Update `IndexResult` struct and `CollectionDb::index_archive_files` signature
3. Third: Remove `list_archive_files_compat` and `list_zip_files_compat`, update callers
4. Fourth: Remove `extract_archive_to_dir_pub`, update callers in `installer.rs`
5. Fifth: Remove unused `ExtractArchiveFileEntry` re-export
6. Final: Run `cargo check` and `cargo clippy` to verify
