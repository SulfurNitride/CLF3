# Session Notes - Streaming Pipeline Work

## Date: 2026-01-30

## Current Status
Streaming pipeline is working with several fixes applied. Key fix today: **misclassified whole-file directive recovery**.

## What's Working
- Basic streaming architecture with 8 extraction + 8 mover workers
- Bounded channels for backpressure
- Memory pressure monitoring via sysinfo
- ZIP random access extraction (using zip crate fallback)
- Sequential batch processing: ZIP → RAR → 7z
- Path normalization using `trim_matches('/')` for consistent lookups
- **NEW**: Misclassified directive recovery - when `archive_hash_path.len() == 1` but sizes don't match, we look up the file in the archive by size+filename
- Build compiles successfully

## Recent Fixes (This Session)

### Misclassified Whole-File Directive Recovery
Some modlists have directives with `archive_hash_path.len() == 1` (normally meaning "whole file = archive") but the expected file size doesn't match the archive size. This means the directive is malformed and missing the path.

**Recovery mechanism added:**
1. Check if archive size matches expected size
2. If mismatch, look up file by size + filename in archive index
3. If found, add to extraction queue for normal processing
4. New DB functions: `lookup_archive_file_by_size_and_name()`, `lookup_archive_file_by_size()`

This fixes the "A Lovely Letter Alternate Routes" mod which had 100+ failing files.

### rawzip Bug Fix
The rawzip extraction was using `uncompressed_size_hint()` from the central directory, but this can differ from the local file header. Fixed by using `local_entry.claim_verifier().size()` to get the actual uncompressed size from the local header.

## Known Issues

### 2. RAR Skip Function Not Yet Implemented
Should use `entry.skip()` for unneeded files instead of extracting everything.

### 3. 7z Solid Archive Detection
Not yet detecting solid vs non-solid 7z archives for optimal ordering.

## Key Files
- `src/installer/streaming.rs` - Main streaming pipeline (has recovery logic)
- `src/modlist/db.rs` - Archive file lookup/indexing (has new recovery functions)
- `src/archive/fast_zip.rs` - rawzip+libdeflater implementation (disabled)
- `src/paths.rs` - Path normalization functions

## TODO When Resuming
1. Debug rawzip extraction bug (returns wrong data)
2. Implement RAR skip() for unneeded files
3. Detect 7z compression type (solid vs non-solid)
4. Optimize extracted file verification

## Dependencies (Pure Rust - No External Binaries)
- `zip = "7.2"` (fallback, working)
- `rawzip + libdeflater` (implemented but buggy)
- `sevenz-rust = "0.6.1"`
- `unrar = "0.5.8"`
- `ba2 = "3.0.1"`
- `sysinfo = "0.38.0"`
