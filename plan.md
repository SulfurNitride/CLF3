# Pipeline Architecture Plan — Priority-Based Install

## Current State (as of 2026-03-19)

### What's Working
- Pipelined download+extract: archives processed as they download
- Parallel extraction: `thread_scope.spawn` with concurrency limiter (`max_concurrent` = CPU count)
- BSA readiness tracking: 53/70 BSAs built early during pipeline (not deferred to bsa_phase)
- DDS source extraction during pipeline (captured via spill channel, transformed after all archives)
- `open_shared()` for SQLite multi-connection (no WAL deletion)
- 7z runs mmt=1 per process, up to max_concurrent processes simultaneously
- Pipelined mode is now the default for both CLI and GUI
- **CLI progress reporting**: `[N/total] Extracted: archive_name (directives, written, failed)` per-archive
- **Priority download ordering**: archives scored by impact (BSA deps +100, patches +50, textures +25, simple +1), sorted highest-first for both downloads and already-downloaded emissions
- All 203 tests pass

### What's Done
- [x] Phase 1: CLI Progress Reporting
- [x] Phase 3: Priority Download Ordering

### Known Issues to Fix
1. **150 extraction failures** — need to investigate after architecture work
2. **DDS textures batch at end** — source data extracted during pipeline but transformed after all archives finish via `process_spilled_dds_jobs()`. Should transform inline per-archive.
3. **GUI progress broken** — `DirectiveComplete` callback has `total: 0` causing `inf%`. Phase never changes from "Downloading archives...". Low priority (CLI-first future).

## Architecture: Priority Pipeline

### Concept
Build a dependency graph at startup. Score each archive by "impact" (does it feed BSAs? patches? textures?). Download highest-impact archives first. Process everything (extract, DDS transform, BSA build) inline as archives complete — no deferred phases.

### Phase 1: CLI Progress Reporting (DONE)
**Files**: `src/installer/pipeline.rs`

- Added `archives_completed` AtomicUsize counter and `total_archives` from `GroupedDirectives`
- Each extraction thread prints on completion:
  ```
  [45/92] Extracted: archive_name.7z (234 directives, 1500 written, 0 failed)
  ```
- Uses `eprintln!` to avoid conflicting with download progress bars.

### Phase 2: Inline DDS Texture Processing (TODO)
**Files**: `src/installer/pipeline.rs`, `src/installer/streaming.rs`

Current flow:
```
extract archive → capture DDS source → send to spill channel → ... all archives done ... → batch transform all DDS
```

New flow:
```
extract archive → for each DDS source in this archive → transform immediately → write output
```

Implementation:
1. In `extract_prepared_archive()`, after extraction and finalization, process this archive's textures immediately instead of sending to the DDS channel.
2. For 7z/ZIP archives: after `process_single_archive_fused` + `finalize_archive`, read DDS source from temp dir, transform, write output.
3. For BSA archives: after `extract_textures_from_bsa`, transform immediately instead of sending to channel.
4. Remove or make optional the DDS spill channel + collector thread.
5. Track completed texture IDs in `ctx.textures_processed_during_install` so `texture_phase()` skips them.
6. GPU init (`init_gpu()`) must happen once at pipeline start, not per-archive.

Key consideration: BC7 textures benefit from GPU batch processing. Options:
- (a) Process non-BC7 inline, batch BC7 at end → partial improvement
- (b) Process all inline, GPU encode one-at-a-time → simpler, slightly less GPU efficient
- (c) Accumulate BC7 in small batches (e.g., per-archive), flush each batch → good balance

### Phase 3: Priority Download Ordering (DONE)
**Files**: `src/installer/pipeline.rs`, `src/installer/downloader.rs`

- Priority scores computed in `load_and_group_directives()`:
  - Archive feeds a CreateBSA staging dir → +100
  - Archive has PatchedFromArchive directives → +50 + count
  - Archive has TransformedTexture directives → +25 + count
  - Archive has FromArchive (simple extract) → +1 + count
- `GroupedDirectives.priority: HashMap<String, u32>` passed to download thread
- `download_archives_streaming()` accepts `Option<&HashMap<String, u32>>` priority map
- Both `need_download` and already-downloaded `Ready` events sorted by priority (highest first)

### Phase 4: Investigate 150 Failures
After architecture work. Run with `RUST_LOG=debug`, check for:
- Path resolution failures (lookup_archive_file returning None)
- Extraction errors (7z exit codes, corrupt archives)
- File write failures (permissions, disk space)
- Patch basis misses

## Key Files Reference

| File | Purpose |
|------|---------|
| `src/installer/pipeline.rs` | Pipeline coordinator, BSA tracker, extraction dispatch, `GroupedDirectives`, `BsaReadinessTracker` |
| `src/installer/streaming.rs` | `process_single_archive_fused`, `finalize_archive`, `process_spilled_dds_jobs`, `extract_textures_from_*` |
| `src/installer/downloader.rs` | `download_archives_streaming`, `ArchiveEvent` enum |
| `src/installer/mod.rs` | `run_pipelined()` entry point, phase orchestration |
| `src/installer/processor.rs` | `ProcessContext`, `index_single_archive`, `DirectiveProcessor`, `bsa_phase`, `texture_phase` |
| `src/installer/handlers/create_bsa.rs` | `handle_create_bsa`, `output_bsa_valid` |
| `src/modlist/db.rs` | `ModlistDb::open`, `open_shared` |

## Threading Model

```
Main thread:     recv ArchiveEvent → prepare_archive (DB work) → spawn extraction thread
                 also drains done_rx for BSA readiness checking

Extraction threads (up to max_concurrent = CPU count):
                 extract_prepared_archive → finalize → [future: inline DDS] → signal done_tx
                 each 7z runs mmt=1, parallelism comes from many concurrent archives

Download thread: own tokio runtime + own DB connection (open_shared)
                 emits ArchiveEvent::Ready per completed download
                 archives sorted by priority (BSA deps first)

DDS collector:   (currently) spills texture source data to disk for batch processing
                 (future: Phase 2) removed — textures processed inline per-archive
```

## Remaining Implementation Order
1. **Phase 2** (inline DDS) — biggest performance win, removes deferred texture phase
2. **Phase 4** (failure investigation) — after architecture stabilizes
