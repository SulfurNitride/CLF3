# CLF3 Missing Content Fix Plan

## Problem Summary

Fallout Anomaly installation shows only 116GB output instead of expected 199.3GB (~83GB missing).

## Root Cause Analysis

### Database State
- Total directives: 76,941
- All marked as "pending" (0 completed) - database state tracking issue
- Actual files created: 36,487

### Directive Breakdown
- FromArchive: 72,877 total
  - mods folder: 34,156
  - TEMP_BSA_FILES: 36,386 (staging for BA2 creation)
  - Stock Folder: 143
  - Other (MO2 files): ~2,200
- CreateBSA: 20 (builds BA2 archives from TEMP_BSA_FILES)
- Other types: ~4,000

### Critical Finding
The 20 CreateBSA-generated BA2 archives are **MISSING**. These should contain the 36,386 staged files and account for the ~83GB gap.

Example missing BA2s:
- mods/Fallout 4 HD Overhaul 2k/DLCCoast - Textures.ba2
- mods/Anomaly PRP Master/Anomaly_PRP_Master - Main.ba2
- mods/Halffaces Merged Pack/Halffaces Merged Texture Pack - Main.ba2
- Stock Folder/Data/Fallout4 - Interface.ba2

## Code Issues Found

### Issue 1: Hard Stop on FromArchive Failures (CRITICAL)
**File:** `src/installer/processor.rs` lines 866-872

```rust
if streaming_stats.failed > 0 {
    anyhow::bail!(
        "FromArchive phase failed: {} files failed to extract. Check logs for details.",
        streaming_stats.failed
    );
}
```

If ANY FromArchive directive fails (even 1 out of 72,877), the entire process bails and CreateBSA never runs. This is too aggressive.

### Issue 2: check_failures Uses Accumulated Counter
**File:** `src/installer/processor.rs` lines 875-884

The `failed` counter accumulates across ALL phases. Each `check_failures` call checks if `failed > 0`, meaning any failure in an earlier phase causes all subsequent phases to fail.

### Issue 3: Database Status Not Updated
Directives show as "pending" even after processing. Either:
- Status update is failing silently
- Status update happens in a transaction that gets rolled back on error
- Status update code path is not being reached

## Fixes Required

### Fix 1: Don't Bail on FromArchive Failures
Change the hard stop to a warning. Allow CreateBSA to run even if some FromArchive files failed (the staging files might still exist).

```rust
// WARNING instead of BAIL
if streaming_stats.failed > 0 {
    eprintln!(
        "WARNING: FromArchive phase had {} failures. Continuing with CreateBSA...",
        streaming_stats.failed
    );
}
```

### Fix 2: Reset Failed Counter Between Phases
Each phase should check only ITS OWN failures, not accumulated failures from previous phases.

### Fix 3: Ensure CreateBSA Runs Regardless
CreateBSA should attempt to run even if previous phases had failures. It will naturally fail if staging files are missing, but at least it will try.

### Fix 4: Improve Error Reporting
Log which specific BA2s failed to create and why.

## Already Fixed (This Session)

1. **Hash verification** - Added xxHash64 verification for archives (pre-check and post-check)
2. **Race condition** - Handle "File exists" errors as skips instead of failures
3. **BSA temp cleanup** - Added `cleanup_bsa_temp_dirs()` calls after processing

## Testing Plan

1. Run Anomaly install with verbose logging
2. Check if TEMP_BSA_FILES get created
3. Check if CreateBSA phase runs
4. Verify BA2 archives are created
5. Compare final output size to expected 199.3GB

## Files to Modify

- `src/installer/processor.rs` - Fix hard stop, reset counters, improve error handling
- `src/installer/handlers/create_bsa.rs` - Better error messages
- `src/modlist/db.rs` - Verify status update is working
