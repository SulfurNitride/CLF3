# Streaming Archive Extraction Pipeline Architecture

## Executive Summary

This document describes the streaming pipeline architecture for archive extraction, which replaces the **chunk-based batch model** with a **continuous streaming pipeline**. The implementation exists in:

- `/home/luke/Documents/Wabbajack Rust Update/clf3/src/installer/extraction_pipeline.rs` - Extraction workers
- `/home/luke/Documents/Wabbajack Rust Update/clf3/src/installer/mover.rs` - File mover workers

The goal is to eliminate blocking where slow archives (large 7z files) hold up fast archives (small ZIPs), achieving better CPU utilization and faster overall completion times.

## Problem Statement

### Current Architecture (Chunk-Based)

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Current Flow                                 │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Archives: [A, B, C, D, E, F, G, H]  (sorted by priority)           │
│                                                                      │
│  Chunk 1: [A, B, C, D]  ──► par_iter ──► WAIT FOR ALL ──┐           │
│                                                          │           │
│  Chunk 2: [E, F, G, H]  ◄────────────────────────────────┘           │
│                          ──► par_iter ──► WAIT FOR ALL ──► Done     │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

**Problems:**
1. If archive `C` is a 10GB solid 7z (takes 5 minutes), archives `E, F, G, H` wait idle
2. Fast ZIPs complete in seconds but their slots sit empty waiting for slow archives
3. Poor CPU utilization during chunk transitions
4. No overlap between extraction and file moving phases

### Line Numbers in processor.rs

- Lines 1045-1062: Chunk creation with `chunks(num_threads)`
- Lines 1064-1348: `par_iter` within each chunk - all must complete before next chunk
- Lines 1123-1142: Extract-then-move pattern (sequential per archive)

## Proposed Architecture (Streaming Pipeline)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Streaming Pipeline                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────┐     ┌──────────────────┐     ┌─────────────────┐          │
│  │   Archive    │     │   Extraction     │     │   File Mover    │          │
│  │    Queue     │────►│    Workers       │────►│    Workers      │          │
│  │  (Priority)  │     │   (N threads)    │     │   (M threads)   │          │
│  └──────────────┘     └──────────────────┘     └─────────────────┘          │
│         │                     │                        │                     │
│         │                     │                        │                     │
│         ▼                     ▼                        ▼                     │
│  ┌──────────────┐     ┌──────────────────┐     ┌─────────────────┐          │
│  │ Sorted by:   │     │ As soon as ONE   │     │ As soon as ONE  │          │
│  │ 1. Type      │     │ archive done,    │     │ dir ready,      │          │
│  │ 2. Size      │     │ send to mover    │     │ start moving    │          │
│  │ 3. Files     │     │ queue + pull     │     │ + cleanup       │          │
│  └──────────────┘     │ next archive     │     └─────────────────┘          │
│                       └──────────────────┘                                   │
│                                                                              │
│  ═══════════════════════════════════════════════════════════════════════    │
│  CONTINUOUS FLOW: No waiting between archives. Fast archives fly through.   │
│  ═══════════════════════════════════════════════════════════════════════    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Design Components

### 1. Data Structures

#### 1.1 Archive Work Item

```rust
/// Represents an archive ready for extraction
#[derive(Debug)]
pub struct ExtractionJob {
    /// Unique job ID for tracking
    pub job_id: u64,
    /// Archive hash (for temp dir naming)
    pub archive_hash: String,
    /// Path to the archive file
    pub archive_path: PathBuf,
    /// Archive type for extraction strategy
    pub archive_type: ArchiveType,
    /// All directives that need files from this archive
    pub directives: Vec<(i64, FromArchiveDirective, Option<String>)>,
    /// Priority score (lower = higher priority)
    pub priority: u32,
    /// Estimated extraction time (for scheduling)
    pub estimated_time_ms: u64,
}

impl ExtractionJob {
    /// Calculate priority based on archive type and size
    pub fn calculate_priority(archive_type: ArchiveType, file_count: usize, size_bytes: u64) -> u32 {
        let type_priority = match archive_type {
            ArchiveType::Zip => 0,        // Fastest
            ArchiveType::Rar => 100,      // Medium
            ArchiveType::SevenZip => 200, // Slowest
            ArchiveType::Bsa => 50,       // Fast random access
            ArchiveType::Unknown => 300,
        };

        // Smaller archives get higher priority within same type
        let size_priority = (size_bytes / (1024 * 1024)) as u32; // MB as priority

        type_priority + size_priority.min(99)
    }
}
```

#### 1.2 Extracted Archive Ready for Moving

```rust
/// Result of extraction, ready for file moving phase
#[derive(Debug)]
pub struct ExtractedArchive {
    /// Original job reference
    pub job_id: u64,
    /// Archive hash (for cleanup)
    pub archive_hash: String,
    /// Path to temp directory with extracted files
    pub temp_dir: PathBuf,
    /// Index of normalized_path -> actual_path for fast lookup
    pub file_index: HashMap<String, PathBuf>,
    /// Directives to process (file moves)
    pub directives: Vec<(i64, FromArchiveDirective, Option<String>)>,
    /// Archive name for progress display
    pub archive_name: String,
}
```

#### 1.3 File Move Job

```rust
/// Individual file move operation
#[derive(Debug)]
pub struct FileMoveJob {
    /// Source path in extracted temp dir
    pub source: PathBuf,
    /// Destination path
    pub destination: PathBuf,
    /// Expected file size for validation
    pub expected_size: u64,
    /// Directive ID for tracking
    pub directive_id: i64,
    /// Archive name for error reporting
    pub archive_name: String,
    /// Whether this is a BSA needed for nested extraction (copy, don't move)
    pub requires_copy: bool,
}
```

#### 1.4 Pipeline Channels

```rust
use crossbeam_channel::{bounded, Sender, Receiver};

/// Channel capacity constants
const EXTRACTION_QUEUE_SIZE: usize = 64;      // Pending extraction jobs
const EXTRACTED_QUEUE_SIZE: usize = 16;        // Extracted archives ready for moving
const FILE_MOVE_QUEUE_SIZE: usize = 1024;      // Individual file move operations

/// Pipeline channels for inter-thread communication
pub struct PipelineChannels {
    /// Archives waiting to be extracted
    pub extraction_tx: Sender<ExtractionJob>,
    pub extraction_rx: Receiver<ExtractionJob>,

    /// Extracted archives ready for file moving
    pub extracted_tx: Sender<ExtractedArchive>,
    pub extracted_rx: Receiver<ExtractedArchive>,

    /// Completion notifications (for progress tracking)
    pub completion_tx: Sender<CompletionEvent>,
    pub completion_rx: Receiver<CompletionEvent>,
}

#[derive(Debug)]
pub enum CompletionEvent {
    FileCompleted { directive_id: i64, success: bool },
    ArchiveCompleted { job_id: u64, archive_name: String },
    ExtractionFailed { job_id: u64, archive_name: String, error: String },
}

impl PipelineChannels {
    pub fn new() -> Self {
        let (extraction_tx, extraction_rx) = bounded(EXTRACTION_QUEUE_SIZE);
        let (extracted_tx, extracted_rx) = bounded(EXTRACTED_QUEUE_SIZE);
        let (completion_tx, completion_rx) = bounded(FILE_MOVE_QUEUE_SIZE * 2);

        Self {
            extraction_tx,
            extraction_rx,
            extracted_tx,
            extracted_rx,
            completion_tx,
            completion_rx,
        }
    }
}
```

### 2. Extraction Workers

```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// Configuration for extraction worker pool
pub struct ExtractionWorkerConfig {
    /// Number of extraction workers
    pub num_workers: usize,
    /// Base output directory (for temp dirs)
    pub output_dir: PathBuf,
    /// Shutdown signal
    pub shutdown: Arc<AtomicBool>,
}

/// Extraction worker that pulls from queue and extracts archives
pub fn extraction_worker(
    worker_id: usize,
    config: Arc<ExtractionWorkerConfig>,
    channels: Arc<PipelineChannels>,
    active_extractions: Arc<AtomicUsize>,
) {
    loop {
        // Check shutdown
        if config.shutdown.load(Ordering::Relaxed) {
            break;
        }

        // Pull next job (blocks until available or timeout)
        let job = match channels.extraction_rx.recv_timeout(Duration::from_millis(100)) {
            Ok(job) => job,
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
        };

        active_extractions.fetch_add(1, Ordering::Relaxed);

        // Perform extraction
        let result = extract_archive_to_temp(&job, &config.output_dir);

        active_extractions.fetch_sub(1, Ordering::Relaxed);

        match result {
            Ok(extracted) => {
                // Send to mover queue
                if channels.extracted_tx.send(extracted).is_err() {
                    break; // Channel closed
                }
            }
            Err(e) => {
                // Report failure
                let _ = channels.completion_tx.send(CompletionEvent::ExtractionFailed {
                    job_id: job.job_id,
                    archive_name: job.archive_path.file_name()
                        .unwrap_or_default()
                        .to_string_lossy()
                        .to_string(),
                    error: format!("{:#}", e),
                });
            }
        }
    }
}

/// Extract archive to temp directory and build file index
fn extract_archive_to_temp(
    job: &ExtractionJob,
    output_dir: &Path,
) -> Result<ExtractedArchive> {
    let archive_name = job.archive_path
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();

    // Get stable temp directory path
    let temp_dir = get_extraction_temp_dir(output_dir, &job.archive_hash);

    // Check if already extracted (resume support)
    let needs_extraction = !temp_dir.exists() || !has_files(&temp_dir);

    if needs_extraction {
        // Create temp dir
        fs::create_dir_all(&temp_dir)?;

        // Extract based on archive type
        extract_archive_to_dir(&job.archive_path, &temp_dir)?;
    }

    // Build file index for fast lookup
    let file_index = build_extracted_file_index(&temp_dir);

    Ok(ExtractedArchive {
        job_id: job.job_id,
        archive_hash: job.archive_hash.clone(),
        temp_dir,
        file_index,
        directives: job.directives.clone(),
        archive_name,
    })
}
```

### 3. Mover Workers

```rust
/// Configuration for file mover worker pool
pub struct MoverWorkerConfig {
    /// Number of mover workers
    pub num_workers: usize,
    /// Process context for path resolution
    pub ctx: Arc<ProcessContext>,
    /// Shutdown signal
    pub shutdown: Arc<AtomicBool>,
}

/// File mover worker that processes extracted archives
pub fn mover_worker(
    worker_id: usize,
    config: Arc<MoverWorkerConfig>,
    channels: Arc<PipelineChannels>,
    stats: Arc<PipelineStats>,
) {
    loop {
        if config.shutdown.load(Ordering::Relaxed) {
            break;
        }

        // Pull next extracted archive
        let extracted = match channels.extracted_rx.recv_timeout(Duration::from_millis(100)) {
            Ok(e) => e,
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
        };

        // Process all files from this archive
        process_extracted_archive(&config.ctx, &extracted, &channels, &stats);

        // Cleanup temp directory after all files moved
        cleanup_extraction_temp_dir(&config.ctx.config.output_dir, &extracted.archive_hash);

        // Notify completion
        let _ = channels.completion_tx.send(CompletionEvent::ArchiveCompleted {
            job_id: extracted.job_id,
            archive_name: extracted.archive_name.clone(),
        });
    }
}

/// Process all files from an extracted archive
fn process_extracted_archive(
    ctx: &ProcessContext,
    extracted: &ExtractedArchive,
    channels: &PipelineChannels,
    stats: &PipelineStats,
) {
    // Identify BSA paths needed for nested extraction
    let bsa_paths_for_nested: HashSet<String> = extracted.directives
        .iter()
        .filter(|(_, d, _)| d.archive_hash_path.len() > 2)
        .map(|(_, d, _)| paths::normalize_for_lookup(&d.archive_hash_path[1]))
        .collect();

    // Track moved files for multi-directive sources
    let mut moved_files: HashMap<String, PathBuf> = HashMap::new();

    for (id, directive, resolved_path) in &extracted.directives {
        // Skip if output already exists
        if output_exists(ctx, &directive.to, directive.size) {
            stats.skipped.fetch_add(1, Ordering::Relaxed);
            let _ = channels.completion_tx.send(CompletionEvent::FileCompleted {
                directive_id: *id,
                success: true,
            });
            continue;
        }

        // Handle different directive types
        let result = match directive.archive_hash_path.len() {
            1 => {
                // Whole archive copy - should have been handled separately
                Err(anyhow::anyhow!("Whole archive directive in mover"))
            }
            2 => {
                // Simple extraction
                let path_in_archive = resolved_path.clone()
                    .unwrap_or_else(|| directive.archive_hash_path[1].clone());
                process_simple_extraction(
                    ctx,
                    directive,
                    &path_in_archive,
                    &extracted.file_index,
                    &bsa_paths_for_nested,
                    &mut moved_files,
                )
            }
            _ => {
                // Nested BSA extraction
                process_nested_bsa(ctx, directive, &extracted.file_index)
            }
        };

        match result {
            Ok(()) => {
                stats.completed.fetch_add(1, Ordering::Relaxed);
                let _ = channels.completion_tx.send(CompletionEvent::FileCompleted {
                    directive_id: *id,
                    success: true,
                });
            }
            Err(e) => {
                stats.failed.fetch_add(1, Ordering::Relaxed);
                stats.record_failure(&extracted.archive_name, &e.to_string());
                let _ = channels.completion_tx.send(CompletionEvent::FileCompleted {
                    directive_id: *id,
                    success: false,
                });
            }
        }
    }
}
```

### 4. Pipeline Coordinator

```rust
use rayon::ThreadPoolBuilder;

/// Main pipeline statistics
pub struct PipelineStats {
    pub completed: AtomicUsize,
    pub skipped: AtomicUsize,
    pub failed: AtomicUsize,
    pub total: AtomicUsize,
    pub archives_completed: AtomicUsize,
    pub archives_total: AtomicUsize,
    failures: RwLock<HashMap<String, (usize, String)>>,
}

/// Pipeline coordinator that manages the streaming extraction
pub struct ExtractionPipeline {
    channels: Arc<PipelineChannels>,
    stats: Arc<PipelineStats>,
    config: PipelineConfig,
    shutdown: Arc<AtomicBool>,
}

pub struct PipelineConfig {
    pub extraction_workers: usize,
    pub mover_workers: usize,
    pub output_dir: PathBuf,
    pub ctx: Arc<ProcessContext>,
}

impl ExtractionPipeline {
    pub fn new(config: PipelineConfig) -> Self {
        Self {
            channels: Arc::new(PipelineChannels::new()),
            stats: Arc::new(PipelineStats::new()),
            config,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Run the extraction pipeline
    pub fn run(&self, jobs: Vec<ExtractionJob>) -> Result<PipelineStats> {
        let total_archives = jobs.len();
        let total_files: usize = jobs.iter().map(|j| j.directives.len()).sum();

        self.stats.archives_total.store(total_archives, Ordering::Relaxed);
        self.stats.total.store(total_files, Ordering::Relaxed);

        // Spawn extraction workers
        let extraction_handles = self.spawn_extraction_workers();

        // Spawn mover workers
        let mover_handles = self.spawn_mover_workers();

        // Spawn progress reporter
        let progress_handle = self.spawn_progress_reporter();

        // Feed jobs into the pipeline (sorted by priority)
        let mut sorted_jobs = jobs;
        sorted_jobs.sort_by_key(|j| j.priority);

        for job in sorted_jobs {
            if self.channels.extraction_tx.send(job).is_err() {
                break; // Pipeline shut down
            }
        }

        // Signal no more jobs
        drop(self.channels.extraction_tx.clone()); // Close sender

        // Wait for all workers to complete
        for handle in extraction_handles {
            handle.join().ok();
        }

        // Signal extractors done, close extracted channel
        drop(self.channels.extracted_tx.clone());

        for handle in mover_handles {
            handle.join().ok();
        }

        // Signal completion channel
        drop(self.channels.completion_tx.clone());

        progress_handle.join().ok();

        Ok(Arc::try_unwrap(self.stats.clone()).unwrap_or_else(|arc| (*arc).clone()))
    }

    fn spawn_extraction_workers(&self) -> Vec<std::thread::JoinHandle<()>> {
        let extraction_config = Arc::new(ExtractionWorkerConfig {
            num_workers: self.config.extraction_workers,
            output_dir: self.config.output_dir.clone(),
            shutdown: self.shutdown.clone(),
        });

        let active_extractions = Arc::new(AtomicUsize::new(0));

        (0..self.config.extraction_workers)
            .map(|id| {
                let config = extraction_config.clone();
                let channels = self.channels.clone();
                let active = active_extractions.clone();

                std::thread::Builder::new()
                    .name(format!("extractor-{}", id))
                    .spawn(move || extraction_worker(id, config, channels, active))
                    .expect("Failed to spawn extraction worker")
            })
            .collect()
    }

    fn spawn_mover_workers(&self) -> Vec<std::thread::JoinHandle<()>> {
        let mover_config = Arc::new(MoverWorkerConfig {
            num_workers: self.config.mover_workers,
            ctx: self.config.ctx.clone(),
            shutdown: self.shutdown.clone(),
        });

        (0..self.config.mover_workers)
            .map(|id| {
                let config = mover_config.clone();
                let channels = self.channels.clone();
                let stats = self.stats.clone();

                std::thread::Builder::new()
                    .name(format!("mover-{}", id))
                    .spawn(move || mover_worker(id, config, channels, stats))
                    .expect("Failed to spawn mover worker")
            })
            .collect()
    }

    fn spawn_progress_reporter(&self) -> std::thread::JoinHandle<()> {
        let channels = self.channels.clone();
        let stats = self.stats.clone();
        let shutdown = self.shutdown.clone();

        std::thread::Builder::new()
            .name("progress-reporter".to_string())
            .spawn(move || progress_reporter(channels, stats, shutdown))
            .expect("Failed to spawn progress reporter")
    }
}
```

### 5. Error Handling Strategy

```rust
/// Error handling without blocking the pipeline
pub enum ExtractionError {
    /// Archive corrupted or unreadable - skip entire archive
    ArchiveCorrupt { archive: String, reason: String },

    /// Single file extraction failed - continue with others
    FileFailed { archive: String, file: String, reason: String },

    /// Disk full - pause pipeline
    DiskFull { path: PathBuf },

    /// Permission denied - log and continue
    PermissionDenied { path: PathBuf },
}

impl ExtractionPipeline {
    /// Handle extraction error without blocking
    fn handle_error(&self, error: ExtractionError) {
        match error {
            ExtractionError::ArchiveCorrupt { archive, reason } => {
                // Log and mark all directives as failed
                self.stats.record_failure(&archive, &reason);
                // Archive already skipped, nothing to do
            }

            ExtractionError::FileFailed { archive, file, reason } => {
                // Log individual file failure
                self.stats.record_failure(&archive, &format!("{}: {}", file, reason));
                // Continue processing other files
            }

            ExtractionError::DiskFull { path } => {
                // This is critical - pause and notify user
                self.shutdown.store(true, Ordering::Relaxed);
                eprintln!("ERROR: Disk full at {}. Pipeline paused.", path.display());
            }

            ExtractionError::PermissionDenied { path } => {
                // Log and continue
                eprintln!("WARN: Permission denied: {}", path.display());
            }
        }
    }
}
```

### 6. Progress Tracking

```rust
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

/// Progress reporter that consumes completion events
fn progress_reporter(
    channels: Arc<PipelineChannels>,
    stats: Arc<PipelineStats>,
    shutdown: Arc<AtomicBool>,
) {
    let mp = MultiProgress::new();

    // Overall progress bar
    let overall_pb = mp.add(ProgressBar::new(stats.total.load(Ordering::Relaxed) as u64));
    overall_pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} files | {msg}")
            .unwrap()
            .progress_chars("=>-"),
    );
    overall_pb.enable_steady_tick(Duration::from_millis(100));

    // Archive progress bar
    let archive_pb = mp.add(ProgressBar::new(stats.archives_total.load(Ordering::Relaxed) as u64));
    archive_pb.set_style(
        ProgressStyle::default_bar()
            .template("  {spinner:.blue} Archives: [{bar:30.white/dim}] {pos}/{len}")
            .unwrap()
            .progress_chars("=>-"),
    );

    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        match channels.completion_rx.recv_timeout(Duration::from_millis(100)) {
            Ok(event) => {
                match event {
                    CompletionEvent::FileCompleted { directive_id: _, success } => {
                        overall_pb.inc(1);
                        let completed = stats.completed.load(Ordering::Relaxed);
                        let skipped = stats.skipped.load(Ordering::Relaxed);
                        let failed = stats.failed.load(Ordering::Relaxed);
                        overall_pb.set_message(format!(
                            "OK: {} | Skip: {} | Fail: {}",
                            completed, skipped, failed
                        ));
                    }
                    CompletionEvent::ArchiveCompleted { job_id: _, archive_name } => {
                        stats.archives_completed.fetch_add(1, Ordering::Relaxed);
                        archive_pb.inc(1);
                        archive_pb.set_message(format!("Completed: {}", archive_name));
                    }
                    CompletionEvent::ExtractionFailed { job_id: _, archive_name, error } => {
                        overall_pb.println(format!("FAIL: {}: {}", archive_name, error));
                    }
                }
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
        }
    }

    overall_pb.finish_with_message("Complete");
    archive_pb.finish_and_clear();
}
```

## Worker Allocation Strategy

### Recommended Thread Distribution

```rust
/// Calculate optimal worker distribution
pub fn calculate_worker_distribution() -> (usize, usize) {
    let total_threads = rayon::current_num_threads();

    // Extraction is CPU-bound (decompression)
    // Moving is I/O-bound (disk operations)
    //
    // For SSDs: More movers since I/O is fast
    // For HDDs: Fewer movers to avoid seek thrashing

    // Default: 60% extractors, 40% movers
    let extraction_workers = (total_threads * 6 / 10).max(2);
    let mover_workers = (total_threads - extraction_workers).max(2);

    (extraction_workers, mover_workers)
}

/// Adaptive distribution based on archive types
pub fn adaptive_worker_distribution(jobs: &[ExtractionJob]) -> (usize, usize) {
    let total_threads = rayon::current_num_threads();

    // Count archive types
    let zip_count = jobs.iter().filter(|j| j.archive_type == ArchiveType::Zip).count();
    let sevenz_count = jobs.iter().filter(|j| j.archive_type == ArchiveType::SevenZip).count();
    let total = jobs.len();

    if total == 0 {
        return calculate_worker_distribution();
    }

    // If mostly ZIPs (fast extraction), allocate more to moving
    let zip_ratio = zip_count as f32 / total as f32;

    if zip_ratio > 0.7 {
        // Many ZIPs: 40% extractors, 60% movers
        let extraction_workers = (total_threads * 4 / 10).max(2);
        let mover_workers = (total_threads - extraction_workers).max(2);
        (extraction_workers, mover_workers)
    } else if sevenz_count as f32 / total as f32 > 0.5 {
        // Many 7z: 70% extractors, 30% movers (extraction is bottleneck)
        let extraction_workers = (total_threads * 7 / 10).max(2);
        let mover_workers = (total_threads - extraction_workers).max(2);
        (extraction_workers, mover_workers)
    } else {
        calculate_worker_distribution()
    }
}
```

## Integration with Existing Code

### Modification to process_from_archive_fast

```rust
/// Process FromArchive directives using streaming pipeline
fn process_from_archive_streaming(
    db: &ModlistDb,
    ctx: &ProcessContext,
    pb: &ProgressBar,
    completed: &AtomicUsize,
    skipped: &AtomicUsize,
    failed: &AtomicUsize,
) -> Result<()> {
    // ... existing directive loading and grouping code (lines 917-998) ...

    // Convert to ExtractionJobs
    let jobs: Vec<ExtractionJob> = other_archives
        .into_iter()
        .enumerate()
        .map(|(idx, (archive_hash, archive_path, directives))| {
            let archive_type = detect_archive_type(&archive_path);
            let size = fs::metadata(&archive_path).map(|m| m.len()).unwrap_or(0);

            ExtractionJob {
                job_id: idx as u64,
                archive_hash,
                archive_path,
                archive_type,
                directives,
                priority: ExtractionJob::calculate_priority(archive_type, directives.len(), size),
                estimated_time_ms: estimate_extraction_time(archive_type, size),
            }
        })
        .collect();

    // Calculate worker distribution
    let (extraction_workers, mover_workers) = adaptive_worker_distribution(&jobs);

    pb.println(format!(
        "Starting streaming pipeline: {} extractors, {} movers",
        extraction_workers, mover_workers
    ));

    // Create and run pipeline
    let pipeline = ExtractionPipeline::new(PipelineConfig {
        extraction_workers,
        mover_workers,
        output_dir: ctx.config.output_dir.clone(),
        ctx: Arc::new(ctx.clone()),
    });

    let stats = pipeline.run(jobs)?;

    // Transfer stats to caller's counters
    completed.fetch_add(stats.completed.load(Ordering::Relaxed), Ordering::Relaxed);
    skipped.fetch_add(stats.skipped.load(Ordering::Relaxed), Ordering::Relaxed);
    failed.fetch_add(stats.failed.load(Ordering::Relaxed), Ordering::Relaxed);

    // ... existing BSA processing (Phase 2, lines 1360-1373) ...

    Ok(())
}
```

## Performance Comparison

### Theoretical Analysis

**Chunk-Based (Current):**
```
Time = sum(max(chunk_times)) for each chunk
     = max(A,B,C,D) + max(E,F,G,H) + ...
```

**Streaming Pipeline (Proposed):**
```
Time = max(total_extraction_time / N, total_move_time / M)
     where N = extraction workers, M = mover workers
```

### Expected Improvements

| Scenario | Current | Streaming | Improvement |
|----------|---------|-----------|-------------|
| Mixed archives (50% ZIP, 50% 7z) | ~100% | ~60-70% | 30-40% faster |
| Mostly fast (80% ZIP) | ~100% | ~50-60% | 40-50% faster |
| Mostly slow (80% 7z) | ~100% | ~90-95% | 5-10% faster |
| All same type | ~100% | ~95-100% | Minimal |

### Why Streaming Helps

1. **No idle time**: When a slow 7z is extracting, fast ZIPs continue flowing
2. **Better CPU utilization**: Extraction and moving happen concurrently
3. **Smoother progress**: Files complete continuously instead of in bursts
4. **Memory efficiency**: Only buffered archives in queue, not all at once

## Implementation Phases

### Phase 1: Core Infrastructure
- Implement `PipelineChannels` and data structures
- Basic extraction worker with single-threaded moving
- Test with small archives

### Phase 2: Parallel Movers
- Implement mover worker pool
- Add moved-file tracking for multi-directive sources
- Handle BSA-for-nested special case

### Phase 3: Progress and Error Handling
- Implement progress reporter
- Add comprehensive error handling
- Implement failure tracking

### Phase 4: Optimization
- Adaptive worker distribution
- Memory monitoring
- Queue depth tuning

### Phase 5: Integration
- Replace `process_from_archive_fast` chunk loop
- Maintain backward compatibility
- Add CLI flag for pipeline mode

## Dependencies

Add to `Cargo.toml`:
```toml
# For bounded channels with select
crossbeam-channel = "0.5"
```

## File Structure

```
src/
  installer/
    processor.rs        # Existing (modify)
    pipeline/
      mod.rs            # Pipeline module root
      channels.rs       # PipelineChannels
      extraction.rs     # ExtractionJob, extraction_worker
      mover.rs          # FileMoveJob, mover_worker
      coordinator.rs    # ExtractionPipeline
      progress.rs       # Progress tracking
      stats.rs          # PipelineStats
```

## Existing Implementation Status

The streaming pipeline has been partially implemented in the codebase:

### Implemented Components

**1. Extraction Pipeline (`src/installer/extraction_pipeline.rs`)**
- `ExtractionJob` - Input structure with archive hash, path, and directives
- `ExtractionResult` - Output with Success/Failed/Skipped outcomes
- `ExtractionOutcome` - Contains temp_dir, file_index, and timing info
- `ExtractionProgress` - Atomic counters for queued/in_progress/completed/failed
- `ExtractionPipelineConfig` - Worker count and buffer configuration
- `ExtractionPipeline` - Main coordinator with job submission and results channel
- `extraction_worker()` - Worker function that processes jobs
- `create_extraction_jobs()` - Helper to create jobs from grouped directives

**2. Mover Pipeline (`src/installer/mover.rs`)**
- `ExtractedArchive` - Archive with file_index and directives ready for moving
- `DirectiveInfo` - Parsed directive with resolved path
- `CategorizedDirectives` - Separated into simple_extract, nested_bsa, whole_file, skipped
- `MoverConfig` - Output/downloads dirs and existing files cache
- `MoverStats` - Atomic counters for completed/skipped/failed
- `process_extracted_archive()` - Main processing function
- `run_mover_worker()` - Worker function for channel consumption
- `spawn_mover_workers()` - Spawns worker pool with channels

### Integration Gap

The existing `process_from_archive_fast()` in `processor.rs` (lines 903-1379) still uses the **chunk-based model**:

```rust
// Current: Chunk-based (blocking)
for chunk in other_archives.chunks(num_threads) {
    // ... create progress bars for chunk
    chunk.par_iter().enumerate().for_each(|(idx, ...)| {
        // Extract and move within same thread
    });
    // BLOCKS HERE until all in chunk complete
}
```

### Integration Code

To integrate the streaming pipeline, replace the chunk loop in `process_from_archive_fast()`:

```rust
fn process_from_archive_streaming(
    db: &ModlistDb,
    ctx: &ProcessContext,
    pb: &ProgressBar,
    completed: &AtomicUsize,
    skipped: &AtomicUsize,
    failed: &AtomicUsize,
) -> Result<()> {
    // ... existing directive loading (lines 917-998) ...

    // Convert other_archives to ExtractionJobs
    let jobs = extraction_pipeline::create_extraction_jobs(
        other_archives.into_iter()
            .map(|(hash, path, directives)| (hash, path, directives))
            .collect()
    );

    if jobs.is_empty() {
        return Ok(());
    }

    // Configure pipeline
    let num_threads = rayon::current_num_threads();
    let extraction_workers = (num_threads / 2).max(2).min(8);
    let mover_workers = (num_threads - extraction_workers).max(2);

    let extraction_config = extraction_pipeline::ExtractionPipelineConfig::with_workers(
        extraction_workers,
        ctx.config.output_dir.clone(),
    );

    // Build existing files cache for skip detection
    let existing_files = Arc::new(mover::build_existing_files_cache(&ctx.config.output_dir));

    let mover_config = mover::MoverConfig {
        output_dir: ctx.config.output_dir.clone(),
        downloads_dir: ctx.config.downloads_dir.clone(),
        existing_files,
    };

    // Start extraction pipeline
    let extraction_pipeline = extraction_pipeline::ExtractionPipeline::new(extraction_config);

    // Start mover workers
    let (mover_tx, mover_stats, mover_shutdown, mover_handles) =
        mover::spawn_mover_workers(mover_workers, mover_config);

    // Submit all jobs to extraction pipeline
    let total_files: usize = jobs.iter().map(|j| j.directives.len()).sum();
    pb.set_length(total_files as u64);
    pb.set_message(format!("Processing {} archives...", jobs.len()));

    extraction_pipeline.submit_batch(jobs)?;
    extraction_pipeline.finish_submitting();

    // Bridge: extraction results -> mover input
    let result_rx = extraction_pipeline.result_receiver();
    let extraction_progress = extraction_pipeline.progress().clone();

    // Spawn bridge thread
    let bridge_handle = std::thread::spawn(move || {
        while let Ok(result) = result_rx.recv() {
            match result.outcome {
                extraction_pipeline::ExtractionOutcome::Success { .. } => {
                    if let Some(extracted) = result.into_extracted_archive() {
                        if mover_tx.send(extracted).is_err() {
                            break;
                        }
                    }
                }
                extraction_pipeline::ExtractionOutcome::Failed { error } => {
                    // Mark all directives as failed
                    for (id, _, _) in &result.directives {
                        // Record failure
                    }
                }
                extraction_pipeline::ExtractionOutcome::Skipped { .. } => {
                    // Handle BSA separately
                }
            }
        }
        // Close mover channel
        drop(mover_tx);
    });

    // Wait for extraction to complete
    extraction_pipeline.wait_for_completion();
    bridge_handle.join().ok();

    // Wait for movers to complete
    mover::shutdown_movers(mover_shutdown, mover_handles);

    // Transfer stats
    let (mover_completed, mover_skipped, mover_failed) = mover_stats.snapshot();
    completed.fetch_add(mover_completed, Ordering::Relaxed);
    skipped.fetch_add(mover_skipped, Ordering::Relaxed);
    failed.fetch_add(mover_failed, Ordering::Relaxed);

    Ok(())
}
```

### Remaining Work

1. **Progress UI Integration**: Connect pipeline stats to indicatif progress bars
2. **BSA Handling**: Route skipped BSA archives to existing sequential processor
3. **Error Aggregation**: Collect failures from both extraction and mover pipelines
4. **Feature Flag**: Add CLI flag to toggle between chunk and streaming modes
5. **Testing**: Verify with large modlists (10+ archives with mixed types)

## Conclusion

The streaming pipeline architecture eliminates the fundamental bottleneck of chunk-based processing where slow archives block fast ones. By using bounded channels and separate worker pools for extraction vs moving, we achieve:

1. **Continuous flow**: No waiting between archives
2. **Better parallelism**: Extraction and moving overlap
3. **Adaptive scaling**: Worker distribution based on archive mix
4. **Robust error handling**: Individual failures don't block pipeline
5. **Clear progress**: Real-time completion tracking

The core components (`extraction_pipeline.rs` and `mover.rs`) are implemented and tested. The remaining work is integrating them into `processor.rs` to replace the chunk-based loop.
