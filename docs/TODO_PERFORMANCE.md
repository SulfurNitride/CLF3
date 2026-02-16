# Performance TODO

Last updated: 2026-02-16

## ROI status (Claude review)
- [x] ROI 1: Parallelize patch application within each archive.
- [x] ROI 2: Route nested non-BSA outputs through mover queue (rename/reflink path).
- [x] ROI 3: Unify extraction strategy logic and remove dead patch channel path.

## Completed in this pass
- [x] Deduplicate selective-vs-full extraction strategy into shared module (`src/installer/extract_strategy.rs`).
- [x] Remove dead patch-phase dummy channel plumbing in `process_patched_from_archive`.
- [x] Parallelize patch apply within each archive after source extraction/preload.
- [x] Add patch-phase timing metrics (extract vs apply vs total).
- [x] Route nested non-BSA extracted files through mover queue (`MoveJob`) instead of read-into-RAM + direct write.
- [x] Add mover queue high-water mark metric.
- [x] Add phase timing + RSS logging at installer orchestration boundaries.

## Next follow-up improvements
- [ ] Make nested BSA/BA2 extraction path stream-to-temp file (avoid keeping extracted blob in memory before queueing move jobs).
- [ ] Add per-archive counters for selective fallback rate and include in final phase summaries.
- [ ] Add structured metrics output (JSON line per phase/archive) for easy regression tracking.
- [ ] Consider ratio-based extraction heuristic (needed/total entries), not only absolute count threshold.
- [ ] Tune mover/extractor worker split dynamically by observed queue saturation and archive mix.

## Verification checklist
- [ ] Run a full install benchmark with `RUST_LOG=info` and capture phase timings.
- [ ] Compare patch-phase wall clock before/after on a patch-heavy modlist.
- [ ] Verify no temp-dir lifetime regressions with nested archive extraction.
- [ ] Validate output parity on a known modlist install (file count + size checks).
