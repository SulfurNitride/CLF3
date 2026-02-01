# Phase 3: Download Coordinator Unification

## Overview

This document analyzes the duplicated code between the Collection downloader (`src/collection/download.rs`) and the Installer downloader (`src/installer/downloader.rs`), and proposes a unified structure to reduce duplication while maintaining flexibility for source-specific behavior.

## Duplicated Code Analysis

### 1. DownloadContext Struct

Both files have nearly identical `DownloadContext` structs with progress tracking fields:

**collection/download.rs: lines 35-47**
```rust
struct DownloadContext {
    nexus: NexusDownloader,
    http: HttpClient,
    game_domain: String,
    downloads_dir: PathBuf,
    multi_progress: MultiProgress,
    overall_pb: ProgressBar,
    // Counters
    downloaded: AtomicUsize,
    skipped: AtomicUsize,
    failed: AtomicUsize,
    failed_downloads: Mutex<Vec<FailedDownloadInfo>>,
}
```

**installer/downloader.rs: lines 65-80**
```rust
struct DownloadContext {
    nexus: NexusDownloader,
    http: HttpClient,
    cdn: WabbajackCdnDownloader,
    gdrive: GoogleDriveDownloader,
    mediafire: MediaFireDownloader,
    config: InstallConfig,
    multi_progress: MultiProgress,
    overall_pb: ProgressBar,
    // Counters
    downloaded: AtomicUsize,
    skipped: AtomicUsize,
    failed: AtomicUsize,
    manual_downloads: Mutex<Vec<ManualDownloadInfo>>,
    failed_downloads: Mutex<Vec<FailedDownloadInfo>>,
}
```

**Common fields:**
- `nexus: NexusDownloader`
- `http: HttpClient`
- `multi_progress: MultiProgress`
- `overall_pb: ProgressBar`
- `downloaded: AtomicUsize`
- `skipped: AtomicUsize`
- `failed: AtomicUsize`
- `failed_downloads: Mutex<Vec<FailedDownloadInfo>>`

**Different fields:**
- Collection: `game_domain: String`, `downloads_dir: PathBuf`
- Installer: `cdn`, `gdrive`, `mediafire` downloaders, `config: InstallConfig`, `manual_downloads`

**Identical: NO** - but 8 of 11 fields (72%) are identical

---

### 2. update_overall_message() Function

**collection/download.rs: lines 439-447**
```rust
fn update_overall_message(ctx: &DownloadContext) {
    let downloaded = ctx.downloaded.load(Ordering::Relaxed);
    let skipped = ctx.skipped.load(Ordering::Relaxed);
    let failed = ctx.failed.load(Ordering::Relaxed);
    ctx.overall_pb.set_message(format!(
        "OK:{} Skip:{} Fail:{}",
        downloaded, skipped, failed
    ));
}
```

**installer/downloader.rs: lines 392-400**
```rust
fn update_overall_message(ctx: &DownloadContext) {
    let downloaded = ctx.downloaded.load(Ordering::Relaxed);
    let skipped = ctx.skipped.load(Ordering::Relaxed);
    let failed = ctx.failed.load(Ordering::Relaxed);
    ctx.overall_pb.set_message(format!(
        "OK:{} Skip:{} Fail:{}",
        downloaded, skipped, failed
    ));
}
```

**Identical: YES** - 100% identical (9 lines each)

---

### 3. Progress Bar Style Templates

**Overall bar template (both files):**
```rust
"{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) | {msg}"
```

**Per-file bar template (both files):**
```rust
"  {spinner:.blue} {wide_msg} [{bar:30.white/dim}] {bytes}/{total_bytes} {bytes_per_sec}"
```

**Locations:**
- collection/download.rs: lines 218-222 (overall), lines 384-388 (per-file)
- installer/downloader.rs: lines 177-182 (overall), lines 346-351 (per-file)
- installer/downloader.rs: lines 795-800 (NXM overall), lines 854-859 (NXM per-file)

**Identical: YES** - Same templates used in 6 different locations

---

### 4. Retry Logic Pattern

Both files implement identical retry logic with rate limit handling:

**collection/download.rs: lines 452-577** (download_nexus_mod)
**installer/downloader.rs: lines 455-543** (download_archive)

Pattern structure:
```rust
let mut attempt = 0u32;
let mut rate_limit_retries = 0u32;

loop {
    attempt += 1;

    // Reset progress bar for retry
    if attempt > 1 {
        pb.set_position(0);
        pb.set_message(display_name.clone());
        let _ = std::fs::remove_file(output_path);
    }

    let result = download_inner(...).await;

    match result {
        Ok(()) => {
            // Verify file (size/MD5)
            // Retry if verification fails
            return Ok(...);
        }
        Err(e) => {
            if is_rate_limit_error(&e) {
                rate_limit_retries += 1;
                if rate_limit_retries <= MAX_RATE_LIMIT_RETRIES {
                    // Exponential backoff: 30s, 60s, 120s, 240s, 300s (capped)
                    let delay_secs = (RATE_LIMIT_BASE_DELAY.as_secs() << (rate_limit_retries - 1).min(3)).min(300);
                    pb.set_message(format!("Rate limited, waiting {}s...", delay_secs));
                    tokio::time::sleep(Duration::from_secs(delay_secs)).await;
                    continue;
                }
            } else if attempt < MAX_RETRIES {
                tokio::time::sleep(RETRY_DELAY).await;
                continue;
            }
            return Err(e);
        }
    }
}
```

**Pattern similarity: ~90%** - Core retry/backoff logic is identical

---

### 5. Print Functions for Failed/Manual Downloads

**Failed downloads printing:**

**collection/download.rs: lines 278-292**
```rust
let failed_downloads = ctx.failed_downloads.lock().await;
if !failed_downloads.is_empty() {
    println!("\n=== Failed Downloads ({}) ===", failed_downloads.len());
    println!("These downloads failed. Try manually downloading to: {}\n", downloads_dir.display());

    for (i, fd) in failed_downloads.iter().enumerate() {
        println!("{}. {}", i + 1, fd.name);
        println!("   URL: {}", fd.url);
        println!("   Error: {}", fd.error);
        println!("   Expected size: {} bytes ({:.2} MB)", fd.expected_size, fd.expected_size as f64 / 1024.0 / 1024.0);
        println!();
    }

    println!("After downloading, run the command again to continue.\n");
}
```

**installer/downloader.rs: lines 270-284**
```rust
let failed_downloads = ctx.failed_downloads.lock().await;
if !failed_downloads.is_empty() {
    println!("\n=== Failed Downloads ({}) ===", failed_downloads.len());
    println!("These downloads failed. Try manually downloading to: {}\n", config.downloads_dir.display());

    for (i, fd) in failed_downloads.iter().enumerate() {
        println!("{}. {}", i + 1, fd.name);
        println!("   URL: {}", fd.url);
        println!("   Error: {}", fd.error);
        println!("   Expected size: {} bytes ({:.2} MB)", fd.expected_size, fd.expected_size as f64 / 1024.0 / 1024.0);
        println!();
    }

    println!("After downloading, run the command again to continue.\n");
}
```

**Identical: YES** - Only difference is `downloads_dir` vs `config.downloads_dir`

---

### 6. Summary Stats Printing

**collection/download.rs: lines 294-306**
```rust
println!("\n=== Download Summary ===");
println!("Downloaded: {}", stats.downloaded);
println!("Skipped:    {}", stats.skipped);
println!("Failed:     {}", stats.failed);

let limits = ctx.nexus.rate_limits();
println!(
    "\nNexus API: {}/{} hourly, {}/{} daily",
    limits.hourly_remaining, limits.hourly_limit,
    limits.daily_remaining, limits.daily_limit
);
```

**installer/downloader.rs: lines 286-299**
```rust
println!("\n=== Download Summary ===");
println!("Downloaded: {}", stats.downloaded);
println!("Skipped:    {}", stats.skipped);
println!("Manual:     {}", stats.manual);
println!("Failed:     {}", stats.failed);

let limits = ctx.nexus.rate_limits();
println!(
    "\nNexus API: {}/{} hourly, {}/{} daily",
    limits.hourly_remaining, limits.hourly_limit,
    limits.daily_remaining, limits.daily_limit
);
```

**Nearly Identical: YES** - Installer adds "Manual" line

---

### 7. File Existence Check Pattern

**collection/download.rs: lines 369-379** (in process_mod_download)
```rust
if output_path.exists() {
    if let Ok(meta) = fs::metadata(output_path) {
        if meta.len() == mod_entry.file_size as u64 {
            ctx.skipped.fetch_add(1, Ordering::Relaxed);
            ctx.overall_pb.inc(1);
            update_overall_message(ctx);
            return DownloadResult::Skipped;
        }
    }
}
```

**installer/downloader.rs: lines 313-323** (in process_archive)
```rust
if output_path.exists() {
    if let Ok(meta) = fs::metadata(output_path) {
        if meta.len() == archive.size as u64 {
            ctx.skipped.fetch_add(1, Ordering::Relaxed);
            ctx.overall_pb.inc(1);
            update_overall_message(ctx);
            return (DownloadResult::Skipped, None);
        }
    }
}
```

**Identical: YES** - Same logic, only return type differs

---

## Proposed New Structure

```
src/core/download/
├── mod.rs           # Re-exports (exists, expand)
├── context.rs       # (existing) DownloadResult enum
├── coordinator.rs   # NEW: DownloadCoordinatorState trait + helpers
├── progress.rs      # NEW: ProgressFactory for bar creation
├── output.rs        # NEW: OutputPrinter for summary/failed printing
└── retry.rs         # NEW: with_retry wrapper (optional)
```

---

## Shared Code Definitions

### coordinator.rs - Shared Download State Trait

```rust
//! Shared download coordinator state and helpers.

use std::sync::atomic::{AtomicUsize, Ordering};
use indicatif::{MultiProgress, ProgressBar};
use tokio::sync::Mutex;

use crate::downloads::FailedDownloadInfo;

/// Trait for download coordinators to share common progress tracking.
pub trait DownloadCoordinator {
    fn multi_progress(&self) -> &MultiProgress;
    fn overall_pb(&self) -> &ProgressBar;
    fn downloaded(&self) -> &AtomicUsize;
    fn skipped(&self) -> &AtomicUsize;
    fn failed(&self) -> &AtomicUsize;
    fn failed_downloads(&self) -> &Mutex<Vec<FailedDownloadInfo>>;
}

/// Update the overall progress bar message with current stats.
pub fn update_overall_message<C: DownloadCoordinator>(ctx: &C) {
    let downloaded = ctx.downloaded().load(Ordering::Relaxed);
    let skipped = ctx.skipped().load(Ordering::Relaxed);
    let failed = ctx.failed().load(Ordering::Relaxed);
    ctx.overall_pb().set_message(format!(
        "OK:{} Skip:{} Fail:{}",
        downloaded, skipped, failed
    ));
}

/// Record a skipped download (file already exists).
pub fn record_skip<C: DownloadCoordinator>(ctx: &C) {
    ctx.skipped().fetch_add(1, Ordering::Relaxed);
    ctx.overall_pb().inc(1);
    update_overall_message(ctx);
}

/// Record a successful download.
pub fn record_success<C: DownloadCoordinator>(ctx: &C) {
    ctx.downloaded().fetch_add(1, Ordering::Relaxed);
    ctx.overall_pb().inc(1);
    update_overall_message(ctx);
}

/// Record a failed download.
pub fn record_failure<C: DownloadCoordinator>(ctx: &C) {
    ctx.failed().fetch_add(1, Ordering::Relaxed);
    ctx.overall_pb().inc(1);
}
```

### progress.rs - Progress Bar Factory

```rust
//! Progress bar creation utilities for download coordinators.

use std::time::Duration;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

/// Standard template for overall download progress bar.
pub const OVERALL_BAR_TEMPLATE: &str =
    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) | {msg}";

/// Standard template for per-file download progress bar.
pub const FILE_BAR_TEMPLATE: &str =
    "  {spinner:.blue} {wide_msg} [{bar:30.white/dim}] {bytes}/{total_bytes} {bytes_per_sec}";

/// Standard progress characters for all bars.
pub const PROGRESS_CHARS: &str = "=>-";

/// Create an overall progress bar for download coordination.
pub fn create_overall_bar(mp: &MultiProgress, total: u64) -> ProgressBar {
    let pb = mp.add(ProgressBar::new(total));
    pb.set_style(
        ProgressStyle::default_bar()
            .template(OVERALL_BAR_TEMPLATE)
            .unwrap()
            .progress_chars(PROGRESS_CHARS),
    );
    pb.enable_steady_tick(Duration::from_millis(100));
    pb.set_message("Starting downloads...");
    pb
}

/// Create a per-file progress bar for individual downloads.
pub fn create_file_bar(mp: &MultiProgress, overall: &ProgressBar, size: u64) -> ProgressBar {
    let pb = mp.insert_before(overall, ProgressBar::new(size));
    pb.set_style(
        ProgressStyle::default_bar()
            .template(FILE_BAR_TEMPLATE)
            .unwrap()
            .progress_chars(PROGRESS_CHARS),
    );
    pb.enable_steady_tick(Duration::from_millis(100));
    pb
}
```

### output.rs - Output Printing Utilities

```rust
//! Output printing utilities for download summaries.

use std::path::Path;

use crate::downloads::{DownloadStats, FailedDownloadInfo, ManualDownloadInfo};
use crate::downloaders::NexusDownloader;

/// Print failed downloads section.
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
        println!("   Expected size: {} bytes ({:.2} MB)",
            fd.expected_size, fd.expected_size as f64 / 1024.0 / 1024.0);
        println!();
    }

    println!("After downloading, run the command again to continue.\n");
}

/// Print manual downloads section.
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

/// Print download summary with stats.
pub fn print_summary(stats: &DownloadStats, include_manual: bool) {
    println!("\n=== Download Summary ===");
    println!("Downloaded: {}", stats.downloaded);
    println!("Skipped:    {}", stats.skipped);
    if include_manual {
        println!("Manual:     {}", stats.manual);
    }
    println!("Failed:     {}", stats.failed);
}

/// Print Nexus API rate limits.
pub fn print_nexus_limits(nexus: &NexusDownloader) {
    let limits = nexus.rate_limits();
    println!(
        "\nNexus API: {}/{} hourly, {}/{} daily",
        limits.hourly_remaining, limits.hourly_limit,
        limits.daily_remaining, limits.daily_limit
    );
}
```

### retry.rs - Retry Wrapper (Optional)

```rust
//! Retry logic for downloads with rate limit handling.

use std::path::Path;
use std::time::Duration;

use anyhow::{bail, Result};
use indicatif::ProgressBar;

use crate::downloads::{is_rate_limit_error, MAX_RETRIES, RETRY_DELAY, MAX_RATE_LIMIT_RETRIES, RATE_LIMIT_BASE_DELAY};
use crate::progress::truncate_name;

/// Configuration for retry behavior.
pub struct RetryConfig<'a> {
    pub display_name: &'a str,
    pub output_path: &'a Path,
    pub pb: &'a ProgressBar,
    pub overall_pb: Option<&'a ProgressBar>,
}

/// Execute a download with retry logic and rate limit handling.
///
/// The `download_fn` closure should perform the actual download and return
/// `Ok(T)` on success or `Err` on failure.
pub async fn with_retry<T, F, Fut>(
    config: RetryConfig<'_>,
    download_fn: F,
) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut attempt = 0u32;
    let mut rate_limit_retries = 0u32;

    loop {
        attempt += 1;

        // Reset progress bar for retry
        if attempt > 1 {
            config.pb.set_position(0);
            config.pb.set_message(config.display_name.to_string());
            let _ = std::fs::remove_file(config.output_path);
        }

        let result = download_fn().await;

        match result {
            Ok(value) => return Ok(value),
            Err(e) => {
                if is_rate_limit_error(&e) {
                    rate_limit_retries += 1;
                    if rate_limit_retries <= MAX_RATE_LIMIT_RETRIES {
                        let delay_secs = (RATE_LIMIT_BASE_DELAY.as_secs() << (rate_limit_retries - 1).min(3)).min(300);
                        config.pb.set_message(format!("Rate limited, waiting {}s...", delay_secs));
                        if let Some(overall) = config.overall_pb {
                            overall.println(format!(
                                "Rate limit hit for {}, waiting {}s (retry {}/{})",
                                truncate_name(config.display_name, 25),
                                delay_secs,
                                rate_limit_retries,
                                MAX_RATE_LIMIT_RETRIES
                            ));
                        }
                        tokio::time::sleep(Duration::from_secs(delay_secs)).await;
                        continue;
                    }
                } else if attempt < MAX_RETRIES {
                    tokio::time::sleep(RETRY_DELAY).await;
                    continue;
                }

                return Err(e);
            }
        }
    }
}
```

---

## Migration Steps

### Step 1: Create progress.rs with ProgressFactory
- Extract `OVERALL_BAR_TEMPLATE`, `FILE_BAR_TEMPLATE`, `PROGRESS_CHARS` constants
- Create `create_overall_bar()` and `create_file_bar()` functions
- Update `src/core/download/mod.rs` to export new module

### Step 2: Create coordinator.rs with shared state
- Define `DownloadCoordinator` trait
- Implement `update_overall_message()`, `record_skip()`, `record_success()`, `record_failure()`
- These are generic over any context implementing the trait

### Step 3: Create output.rs with print functions
- Move `print_failed_downloads()` (parameterized with downloads_dir)
- Move `print_manual_downloads()` (installer-only, but available to both)
- Move `print_summary()` with `include_manual` flag
- Move `print_nexus_limits()`

### Step 4: Update collection/download.rs
- Import new modules
- Implement `DownloadCoordinator` trait for `DownloadContext`
- Replace inline progress bar creation with `create_overall_bar()`, `create_file_bar()`
- Replace `update_overall_message()` with generic version
- Replace print blocks with `print_failed_downloads()`, `print_summary()`, `print_nexus_limits()`

### Step 5: Update installer/downloader.rs
- Import new modules
- Implement `DownloadCoordinator` trait for `DownloadContext`
- Replace inline progress bar creation with factory functions
- Replace `update_overall_message()` with generic version
- Replace print blocks with shared functions
- Also update NXM mode section (lines 793-862)

### Step 6: (Optional) Extract retry logic
- If retry patterns diverge too much, keep separate
- If they stay similar, use `with_retry()` wrapper

---

## Estimated Line Reduction

| Location | Current Lines | After Unification |
|----------|---------------|-------------------|
| **New shared code** | - | +120 lines |
| collection/download.rs | 743 | ~680 lines |
| installer/downloader.rs | 1012 | ~900 lines |

**Breakdown:**
- New `progress.rs`: +35 lines
- New `coordinator.rs`: +45 lines
- New `output.rs`: +55 lines
- Removed from collection/download.rs: ~63 lines (progress bar setup x2, update_overall, print blocks)
- Removed from installer/downloader.rs: ~112 lines (progress bar setup x4, update_overall, print blocks x2)

**Net reduction: ~55 lines**

More importantly:
- Single source of truth for progress bar styles
- Single source of truth for output formatting
- Easier to maintain consistent UX across both downloaders
- Trait-based design allows future downloaders to reuse infrastructure

---

## Verification Checklist

After implementation:

- [ ] `cargo check` passes
- [ ] `cargo test` passes (all existing tests)
- [ ] `cargo clippy` has no new warnings
- [ ] Collection downloader works identically:
  - [ ] Progress bars display correctly
  - [ ] Failed download summary prints correctly
  - [ ] Stats summary prints correctly
  - [ ] Nexus rate limits display correctly
- [ ] Installer downloader works identically:
  - [ ] Progress bars display correctly (API mode)
  - [ ] Progress bars display correctly (NXM mode)
  - [ ] Manual download summary prints correctly
  - [ ] Failed download summary prints correctly
  - [ ] Stats summary prints correctly
  - [ ] Nexus rate limits display correctly
- [ ] No regression in retry behavior
- [ ] No regression in rate limit backoff

---

## Future Considerations

1. **Retry Logic Unification**: The retry loops in both downloaders are very similar but handle different verification (MD5 vs size-only). Could potentially unify with a verification callback.

2. **URL Caching**: Only the installer caches download URLs. Could add to collections if beneficial.

3. **Manual Downloads**: Collections don't support manual downloads currently. The shared `ManualDownloadInfo` is already in `src/downloads/mod.rs` for future use.

4. **NXM Mode for Collections**: Currently stub. Could share NXM infrastructure from installer when implemented.
