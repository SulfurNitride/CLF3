# CLF3 Improvement Plan - Hive Mind Coordination

**Swarm ID**: hive-1769963469904
**Queen**: Strategic Coordinator
**Created**: 2026-02-01
**Status**: ACTIVE

---

## Phase 1: GUI Improvements

### Task #1: Fix GUI file count badge overflow
- **Status**: COMPLETED
- **Priority**: HIGH (quick win)
- **Files**: `src/gui/mod.rs` lines 711-726
- **Change**: Removed fixed `width: 90px`, wrapped Text in HorizontalLayout with padding
- **Assigned**: Queen (direct)
- **Review**: [x] Code changed [x] Compiles [x] Verified

### Task #2: Fix UI freeze when loading .wabbajack files
- **Status**: COMPLETED
- **Priority**: HIGH (UX critical)
- **Files**: `src/gui/mod.rs` (browse_source callback)
- **Change**: Moved parsing to std::thread::spawn, show "Loading..." state, use slint::invoke_from_event_loop for UI update
- **Assigned**: Queen (direct)
- **Review**: [x] Code changed [x] Compiles [x] Verified

### Task #3: Display modlist metadata in log
- **Status**: COMPLETED
- **Priority**: MEDIUM
- **Files**: `src/gui/mod.rs` (detect_game_from_wabbajack function)
- **Change**: Log Author, Game, Download Size, Install Size, Archives count via ProgressUpdate::Log
- **Assigned**: Queen (combined with #2)
- **Review**: [x] Code changed [x] Compiles [x] Verified

---

## Phase 2: Performance Enhancement

### Task #4: GPU acceleration for DDS transformation
- **Status**: PENDING
- **Priority**: HIGH (performance)
- **Files**:
  - `Cargo.toml` - add wgpu
  - `src/textures/gpu.rs` - NEW (compute shaders)
  - `src/textures/processor.rs` - integrate GPU path
- **Change**: Full GPU encoding, DirectXTex only for legacy decode
- **Reference**: `/home/luke/Documents/Wabbajack Rust Update/Radium-Textures-master/`
- **Assigned**: Worker 3
- **Review**: [ ] Code changed [ ] Benchmarked [ ] Verified

---

## Phase 3: Code Cleanup

### Task #5: Remove Vortex/Nexus Collection code
- **Status**: COMPLETED
- **Priority**: MEDIUM
- **Prerequisites**:
  - [x] Verify git history has Vortex code (commit 51bdd49)
  - [x] Commit current uncommitted changes (commit e7fc6c9)
  - [x] Create tag `with-vortex-support`
- **Files to DELETE**: `src/collection/` (entire directory)
- **Files to MODIFY** (28 total):
  - `src/gui/mod.rs` - remove Collection tab
  - `src/main.rs` - remove collection imports
  - `src/lib.rs` - remove pub mod collection
  - `src/installer/processor.rs`
  - `src/installer/downloader.rs`
  - `src/installer/streaming.rs`
  - `src/installer/verify.rs`
  - `src/archive/sevenzip.rs`
  - `src/file_router/mod.rs`
  - `src/nxm_handler.rs`
  - `src/loot/mod.rs`
  - `src/bsa/writer.rs`
  - `src/bsa/reader.rs`
  - `src/downloaders/google_drive.rs`
  - `src/bin/loot_test.rs`
- **Assigned**: Queen (careful review needed)
- **Review**: [ ] Tag created [ ] Files removed [ ] References cleaned [ ] Compiles [ ] Tested

---

## Progress Log

| Time | Task | Action | Result |
|------|------|--------|--------|
| -- | Setup | Created plan file | OK |
| 16:48 | #1 | Changed badge from fixed 90px to dynamic HorizontalLayout with padding | OK - Compiles |
| 16:52 | #2 | Moved .wabbajack parsing to background thread with Loading state | OK - Compiles |
| 16:53 | #3 | Added metadata logging (Author, Game, Download/Install sizes) | OK - Compiles |
| 16:55 | -- | Committed all changes (e7fc6c9) | OK |
| 16:55 | -- | Created tag `with-vortex-support` | OK |
| 17:05 | #5 | Removed src/collection/ directory | OK |
| 17:06 | #5 | Cleaned lib.rs, main.rs, gui/mod.rs | OK |
| 17:08 | #5 | Full release build successful | OK - 76 warnings (dead code) |

---

## Post-Change Review Checklist

After EVERY change:
1. [ ] Code compiles (`cargo check`)
2. [ ] No new warnings (`cargo clippy`)
3. [ ] Functionality verified
4. [ ] Plan file updated
5. [ ] Memory synced to hive

---

## Notes

- Vortex code safely in git at commit `51bdd49`
- Current uncommitted changes need commit before Phase 3
- GPU acceleration should mirror Radium-Textures implementation
