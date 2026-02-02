//! Directive processor - TTW-style batch processing
//!
//! Architecture:
//! - ZIP/7z/RAR: Extraction using 7z binary via crate::archive::sevenzip
//! - ZIP: zip crate is used for reading .wabbajack files (they are ZIP archives)
//! - BSA/BA2: Direct extraction via ba2 crate
//! - Nested BSA: Extract BSA from archive, then extract file from BSA
//! - Parallel processing using rayon

use crate::bsa::{self, BsaCache, list_files as list_bsa_files};
use crate::modlist::{ArchiveFileEntry, Directive, FromArchiveDirective, ModlistDb};
use crate::paths;

use super::config::InstallConfig;
use super::handlers;
use super::handlers::from_archive::{ArchiveType, detect_archive_type, extract_from_archive_with_temp};

use anyhow::{bail, Context, Result};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, Write};
use zip::ZipArchive;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

/// List files in an archive using pure Rust crates
fn list_archive_files_rust(archive_path: &Path) -> Result<Vec<ArchiveFileEntry>> {
    let archive_type = detect_archive_type(archive_path)?;

    match archive_type {
        ArchiveType::Zip => list_zip_files(archive_path),
        ArchiveType::SevenZ => list_7z_files(archive_path),
        ArchiveType::Rar => list_rar_files(archive_path),
        ArchiveType::Bsa | ArchiveType::Ba2 => {
            list_bsa_files(archive_path).map(|entries| {
                entries
                    .into_iter()
                    .map(|e| ArchiveFileEntry {
                        file_path: e.path,
                        file_size: e.size,
                    })
                    .collect()
            })
        }
        ArchiveType::Unknown => {
            list_zip_files(archive_path)
                .or_else(|_| list_7z_files(archive_path))
                .or_else(|_| list_rar_files(archive_path))
                .with_context(|| format!("Failed to list archive: {}", archive_path.display()))
        }
    }
}

/// List files in a ZIP archive
fn list_zip_files(archive_path: &Path) -> Result<Vec<ArchiveFileEntry>> {
    let file = File::open(archive_path)
        .with_context(|| format!("Failed to open ZIP: {}", archive_path.display()))?;
    let reader = BufReader::new(file);
    let mut archive = zip::ZipArchive::new(reader)
        .with_context(|| format!("Failed to read ZIP: {}", archive_path.display()))?;

    let mut files = Vec::new();
    for i in 0..archive.len() {
        let entry = archive.by_index_raw(i)?;
        if !entry.is_dir() {
            files.push(ArchiveFileEntry {
                file_path: entry.name().to_string(),
                file_size: entry.size(),
            });
        }
    }
    Ok(files)
}

/// List files in a 7z archive using 7z binary
fn list_7z_files(archive_path: &Path) -> Result<Vec<ArchiveFileEntry>> {
    let entries = crate::archive::sevenzip::list_archive(archive_path)
        .with_context(|| format!("Failed to list 7z: {}", archive_path.display()))?;

    Ok(entries
        .into_iter()
        .map(|e| ArchiveFileEntry {
            file_path: e.path,
            file_size: e.size,
        })
        .collect())
}

/// List files in a RAR archive using 7z binary
fn list_rar_files(archive_path: &Path) -> Result<Vec<ArchiveFileEntry>> {
    let entries = crate::archive::sevenzip::list_archive(archive_path)
        .with_context(|| format!("Failed to list RAR: {}", archive_path.display()))?;

    Ok(entries
        .into_iter()
        .map(|e| ArchiveFileEntry {
            file_path: e.path,
            file_size: e.size,
        })
        .collect())
}

/// Extract a single file from an archive (pure Rust implementation)
fn extract_single_file(
    archive_path: &Path,
    file_path: &str,
    temp_base_dir: &Path,
) -> Result<Vec<u8>> {
    extract_from_archive_with_temp(archive_path, file_path, temp_base_dir)
}

/// Extract multiple files from an archive (extracts whole archive, returns needed files)
fn extract_batch_rust(
    archive_path: &Path,
    file_paths: &[&str],
    temp_base_dir: &Path,
) -> Result<HashMap<String, Vec<u8>>> {
    if file_paths.is_empty() {
        return Ok(HashMap::new());
    }

    let archive_type = detect_archive_type(archive_path)?;

    // Create temp dir for extraction
    let temp_dir = tempfile::tempdir_in(temp_base_dir)
        .context("Failed to create temp directory")?;

    // Extract entire archive based on type
    match archive_type {
        ArchiveType::Zip => extract_zip_to_temp(archive_path, temp_dir.path())?,
        ArchiveType::SevenZ => extract_7z_to_temp(archive_path, temp_dir.path())?,
        ArchiveType::Rar => extract_rar_to_temp(archive_path, temp_dir.path())?,
        ArchiveType::Bsa | ArchiveType::Ba2 => {
            bail!("BSA/BA2 should not use batch extraction")
        }
        ArchiveType::Unknown => {
            extract_zip_to_temp(archive_path, temp_dir.path())
                .or_else(|_| extract_7z_to_temp(archive_path, temp_dir.path()))
                .or_else(|_| extract_rar_to_temp(archive_path, temp_dir.path()))
                .with_context(|| format!("Failed to extract: {}", archive_path.display()))?
        }
    }

    // Build set of normalized paths we need
    let needed: std::collections::HashSet<String> = file_paths
        .iter()
        .map(|p| paths::normalize_for_lookup(p))
        .collect();

    // Walk temp dir and collect files we need
    let mut results = HashMap::new();
    collect_needed_files(temp_dir.path(), temp_dir.path(), &needed, &mut results)?;

    Ok(results)
}

/// Extract entire ZIP to temp directory
fn extract_zip_to_temp(archive_path: &Path, temp_dir: &Path) -> Result<()> {
    let file = File::open(archive_path)?;
    let reader = BufReader::new(file);
    let mut archive = zip::ZipArchive::new(reader)?;

    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;
        let entry_path = match entry.enclosed_name() {
            Some(p) => p.to_path_buf(),
            None => continue,
        };

        let output_path = temp_dir.join(&entry_path);

        if entry.is_dir() {
            fs::create_dir_all(&output_path)?;
        } else {
            if let Some(parent) = output_path.parent() {
                fs::create_dir_all(parent)?;
            }
            let mut outfile = File::create(&output_path)?;
            std::io::copy(&mut entry, &mut outfile)?;
        }
    }
    Ok(())
}

/// Extract entire 7z to temp directory using 7z binary
fn extract_7z_to_temp(archive_path: &Path, temp_dir: &Path) -> Result<()> {
    crate::archive::sevenzip::extract_all(archive_path, temp_dir)
        .map(|_| ())
        .with_context(|| format!("Failed to extract 7z: {}", archive_path.display()))
}

/// Extract entire RAR to temp directory using 7z binary
///
/// The 7z binary handles RAR5 reference records correctly, unlike the unrar crate.
fn extract_rar_to_temp(archive_path: &Path, temp_dir: &Path) -> Result<()> {
    crate::archive::sevenzip::extract_all(archive_path, temp_dir)
        .map(|_| ())
        .with_context(|| format!("Failed to extract RAR: {}", archive_path.display()))
}

/// Recursively collect files from extracted archive
fn collect_needed_files(
    base: &Path,
    current: &Path,
    needed: &std::collections::HashSet<String>,
    results: &mut HashMap<String, Vec<u8>>,
) -> Result<()> {
    if let Ok(entries) = fs::read_dir(current) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_needed_files(base, &path, needed, results)?;
            } else if path.is_file() {
                // Get relative path from base
                if let Ok(rel) = path.strip_prefix(base) {
                    let rel_str = rel.to_string_lossy();
                    let normalized = paths::normalize_for_lookup(&rel_str);

                    if needed.contains(&normalized) {
                        if let Ok(data) = fs::read(&path) {
                            results.insert(normalized, data);
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

/// Index all archives that need indexing
fn index_archives(db: &ModlistDb, ctx: &ProcessContext) -> Result<()> {

    // Get all archives
    let archives = db.get_all_archives()?;

    // Filter to archives that need indexing and have local paths
    let to_index: Vec<_> = archives
        .iter()
        .filter(|a| {
            // Check if already indexed
            if db.is_archive_indexed(&a.hash).unwrap_or(false) {
                return false;
            }
            // Check if we have the file
            let path = match &a.local_path {
                Some(p) => PathBuf::from(p),
                None => ctx.config.downloads_dir.join(&a.name),
            };
            path.exists()
        })
        .collect();

    if to_index.is_empty() {
        return Ok(());
    }

    println!("Indexing {} archives...", to_index.len());

    let pb = ProgressBar::new(to_index.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} | {msg}")
            .unwrap()
            .progress_chars("=>-"),
    );
    pb.enable_steady_tick(Duration::from_millis(100));

    for archive in to_index {
        let archive_path = match &archive.local_path {
            Some(p) => PathBuf::from(p),
            None => ctx.config.downloads_dir.join(&archive.name),
        };

        pb.set_message(format!("Indexing {}...", archive.name));

        // Detect archive type using magic bytes
        let archive_type = match detect_archive_type(&archive_path) {
            Ok(t) => t,
            Err(_) => ArchiveType::Unknown,
        };

        // Skip non-archive files (game files like ESM, ESL, INI, BIK, etc.)
        let is_indexable = matches!(
            archive_type,
            ArchiveType::Zip | ArchiveType::SevenZ | ArchiveType::Rar | ArchiveType::Bsa | ArchiveType::Ba2
        );
        if !is_indexable {
            // Mark as indexed with empty file list (it's a single file, not a container)
            let _ = db.index_archive_files(&archive.hash, &[]);
            pb.inc(1);
            continue;
        }

        // Use pure Rust listing for all archive types
        let list_result = list_archive_files_rust(&archive_path);

        match list_result {
            Ok(files) => {
                let count = files.len();
                if let Err(e) = db.index_archive_files(&archive.hash, &files) {
                    pb.println(format!("WARN: Failed to index {}: {}", archive.name, e));
                } else {
                    pb.println(format!("OK: {} ({} files)", archive.name, count));
                }
            }
            Err(e) => {
                pb.println(format!("WARN: Failed to list {}: {}", archive.name, e));
            }
        }

        pb.inc(1);
    }

    pb.finish_and_clear();
    println!("Archive indexing complete");

    Ok(())
}

/// Thread-safe tracker for archive failures
struct FailureTracker {
    /// Maps archive name to failure count and sample error message
    failures: RwLock<HashMap<String, (usize, String)>>,
}

impl FailureTracker {
    fn new() -> Self {
        Self {
            failures: RwLock::new(HashMap::new()),
        }
    }

    fn record_failure(&self, archive_name: &str, error: &str) {
        let mut map = self.failures.write().unwrap();
        let entry = map
            .entry(archive_name.to_string())
            .or_insert_with(|| (0, error.to_string()));
        entry.0 += 1;
    }

    fn print_summary(&self, pb: &ProgressBar) {
        let map = self.failures.read().unwrap();
        if map.is_empty() {
            return;
        }

        // Sort by failure count descending
        let mut failures: Vec<_> = map.iter().collect();
        failures.sort_by(|a, b| b.1 .0.cmp(&a.1 .0));

        pb.println("\n--- Archives with failures (may need re-download) ---");
        for (name, (count, sample_error)) in failures.iter().take(20) {
            pb.println(format!("  {} failures: {}", count, name));
            pb.println(format!("    Sample error: {}", sample_error));
        }

        if failures.len() > 20 {
            pb.println(format!("  ... and {} more archives", failures.len() - 20));
        }
    }
}


/// Statistics from directive processing
#[derive(Debug, Default)]
pub struct ProcessStats {
    pub completed: usize,
    pub skipped: usize,
    pub failed: usize,
}

/// Helper for reporting progress to callback
pub struct ProgressReporter {
    callback: Option<super::ProgressCallback>,
    total_directives: usize,
    processed_count: AtomicUsize,
}

impl ProgressReporter {
    pub fn new(callback: Option<super::ProgressCallback>, total_directives: usize) -> Self {
        Self {
            callback,
            total_directives,
            processed_count: AtomicUsize::new(0),
        }
    }

    /// Report that a directive phase is starting
    pub fn phase_started(&self, directive_type: &str, count: usize) {
        if let Some(ref callback) = self.callback {
            callback(super::ProgressEvent::DirectivePhaseStarted {
                directive_type: directive_type.to_string(),
                total: count,
            });
        }
    }

    /// Report that a directive completed (increments internal counter)
    pub fn directive_completed(&self) {
        let current = self.processed_count.fetch_add(1, Ordering::Relaxed) + 1;
        if let Some(ref callback) = self.callback {
            callback(super::ProgressEvent::DirectiveComplete {
                index: current,
                total: self.total_directives,
            });
        }
    }

    /// Report progress with a specific count (for batch updates)
    pub fn report_count(&self, index: usize) {
        if let Some(ref callback) = self.callback {
            callback(super::ProgressEvent::DirectiveComplete {
                index,
                total: self.total_directives,
            });
        }
    }

    /// Report a status message
    pub fn status(&self, message: &str) {
        if let Some(ref callback) = self.callback {
            callback(super::ProgressEvent::Status {
                message: message.to_string(),
            });
        }
    }

    /// Get current processed count
    pub fn get_count(&self) -> usize {
        self.processed_count.load(Ordering::Relaxed)
    }

    /// Set the processed count (for syncing with atomic counters)
    pub fn set_count(&self, count: usize) {
        self.processed_count.store(count, Ordering::Relaxed);
    }

    /// Get a clone of the callback (for passing to other threads)
    pub fn get_callback(&self) -> Option<super::ProgressCallback> {
        self.callback.clone()
    }

    /// Get the total directive count
    pub fn get_total(&self) -> usize {
        self.total_directives
    }
}

/// Context for processing directives (thread-safe)
pub struct ProcessContext<'a> {
    /// Installation configuration
    pub config: &'a InstallConfig,
    /// Open wabbajack archive for inline files
    pub wabbajack: Mutex<ZipArchive<BufReader<File>>>,
    /// Archive cache - maps archive hash to file path
    pub archive_paths: HashMap<String, PathBuf>,
    /// SQLite cache for expensive extractions (7z/RAR)
    pub extraction_cache: BsaCache,
    /// Cache of existing output files: normalized_path -> size (built once at start)
    pub existing_files: HashMap<String, u64>,
}

impl<'a> ProcessContext<'a> {
    /// Create a new processing context
    pub fn new(config: &'a InstallConfig, db: &ModlistDb) -> Result<Self> {
        let file = File::open(&config.wabbajack_path)
            .with_context(|| format!("Failed to open {}", config.wabbajack_path.display()))?;
        let reader = BufReader::new(file);
        let wabbajack = ZipArchive::new(reader).context("Failed to read wabbajack as ZIP")?;

        // Build archive path lookup
        let mut archive_paths = HashMap::new();
        let archives = db.get_all_archives()?;
        for archive in archives {
            if let Some(local_path) = &archive.local_path {
                archive_paths.insert(archive.hash.clone(), PathBuf::from(local_path));
            } else {
                let path = config.downloads_dir.join(&archive.name);
                if path.exists() {
                    archive_paths.insert(archive.hash.clone(), path);
                }
            }
        }

        // SQLite cache for 7z/RAR extractions (persists across runs)
        // Use local cache directory to avoid CIFS/NFS locking issues
        let cache_dir = dirs::cache_dir()
            .unwrap_or_else(|| PathBuf::from("/tmp"))
            .join("clf3");
        let _ = std::fs::create_dir_all(&cache_dir);
        let cache_path = cache_dir.join("extraction_cache.db");
        let extraction_cache =
            BsaCache::at_path(&cache_path).context("Failed to create extraction cache")?;

        // Build cache of existing output files (walk once instead of stat per file)
        let mut existing_files = HashMap::new();
        if config.output_dir.exists() {
            for entry in walkdir::WalkDir::new(&config.output_dir)
                .into_iter()
                .filter_map(|e| e.ok())
            {
                if entry.file_type().is_file() {
                    if let Ok(rel_path) = entry.path().strip_prefix(&config.output_dir) {
                        let normalized = paths::normalize_for_lookup(&rel_path.to_string_lossy());
                        if let Ok(meta) = entry.metadata() {
                            existing_files.insert(normalized, meta.len());
                        }
                    }
                }
            }
        }

        Ok(Self {
            config,
            wabbajack: Mutex::new(wabbajack),
            archive_paths,
            extraction_cache,
            existing_files,
        })
    }

    pub fn get_archive_path(&self, hash: &str) -> Option<&PathBuf> {
        self.archive_paths.get(hash)
    }

    pub fn resolve_output_path(&self, to_path: &str) -> PathBuf {
        paths::join_windows_path(&self.config.output_dir, to_path)
    }

    pub fn read_wabbajack_file(&self, name: &str) -> Result<Vec<u8>> {
        let mut archive = self.wabbajack.lock().unwrap();
        let mut file = archive
            .by_name(name)
            .with_context(|| format!("File '{}' not found in wabbajack", name))?;
        let mut data = Vec::new();
        std::io::Read::read_to_end(&mut file, &mut data)?;
        Ok(data)
    }

    /// Get a file from the extraction cache (for pre-extracted 7z/RAR files)
    /// BSA files are stored in the working folder, regular files in SQLite
    pub fn get_cached_file(&self, archive_hash: &str, file_path: &str) -> Option<Vec<u8>> {
        // Check if this is a BSA file - if so, check working folder
        if is_bsa_file(file_path) {
            let bsa_working_path = get_bsa_working_path(&self.config.downloads_dir, archive_hash, file_path);
            if bsa_working_path.exists() {
                return fs::read(&bsa_working_path).ok();
            }
        }

        // Regular files are in SQLite cache
        let cache_key = Path::new(archive_hash);
        self.extraction_cache.get(cache_key, file_path).ok().flatten()
    }

    /// Get a BSA file from the working folder (for BSAs extracted from 7z/RAR)
    /// Returns the path to the BSA file on disk, if it exists
    pub fn get_cached_bsa_path(&self, archive_hash: &str, bsa_path: &str) -> Option<PathBuf> {
        let bsa_working_path = get_bsa_working_path(&self.config.downloads_dir, archive_hash, bsa_path);
        if bsa_working_path.exists() {
            Some(bsa_working_path)
        } else {
            None
        }
    }

    /// Get a nested BSA file from the extraction cache
    /// For files inside BSAs inside 7z/RAR archives
    pub fn get_cached_nested_bsa_file(&self, archive_hash: &str, bsa_path: &str, file_in_bsa: &str) -> Option<Vec<u8>> {
        let cache_key = Path::new(archive_hash);
        let combined_path = format!("{}/{}", bsa_path, file_in_bsa);
        self.extraction_cache.get(cache_key, &combined_path).ok().flatten()
    }
}

/// Check if output file already exists with correct size (uses cached file list)
fn output_exists(ctx: &ProcessContext, to_path: &str, expected_size: u64) -> bool {
    let normalized = paths::normalize_for_lookup(to_path);
    if let Some(&size) = ctx.existing_files.get(&normalized) {
        size == expected_size
    } else {
        false
    }
}

/// Check if output DDS file exists and has valid header
/// For textures where compression size isn't deterministic
fn output_dds_valid(ctx: &ProcessContext, to_path: &str) -> bool {
    let output_path = ctx.resolve_output_path(to_path);
    if !output_path.exists() {
        return false;
    }

    // Check DDS magic number "DDS " (0x20534444)
    if let Ok(file) = File::open(&output_path) {
        let mut reader = BufReader::new(file);
        let mut magic = [0u8; 4];
        if std::io::Read::read_exact(&mut reader, &mut magic).is_ok() {
            return &magic == b"DDS ";
        }
    }
    false
}

/// Pre-flight check: verify all archives needed for pending directives are present
/// Returns list of (archive_name, archive_hash) that are missing
fn check_missing_archives(db: &ModlistDb, ctx: &ProcessContext) -> Result<Vec<(String, String)>> {
    let mut needed_hashes: std::collections::HashSet<String> = std::collections::HashSet::new();

    // Check FromArchive directives (parse JSON to get archive hash like processing does)
    for (_, json) in db.get_all_pending_directives_of_type("FromArchive")? {
        if let Ok(Directive::FromArchive(d)) = serde_json::from_str::<Directive>(&json) {
            if !output_exists(ctx, &d.to, d.size) {
                if let Some(hash) = d.archive_hash_path.first() {
                    needed_hashes.insert(hash.clone());
                }
            }
        }
    }

    // Check PatchedFromArchive directives
    for (_, json) in db.get_all_pending_directives_of_type("PatchedFromArchive")? {
        if let Ok(Directive::PatchedFromArchive(d)) = serde_json::from_str::<Directive>(&json) {
            if !output_exists(ctx, &d.to, d.size) {
                if let Some(hash) = d.archive_hash_path.first() {
                    needed_hashes.insert(hash.clone());
                }
            }
        }
    }

    // Check TransformedTexture directives
    for (_, json) in db.get_all_pending_directives_of_type("TransformedTexture")? {
        if let Ok(Directive::TransformedTexture(d)) = serde_json::from_str::<Directive>(&json) {
            if !output_exists(ctx, &d.to, d.size) {
                if let Some(hash) = d.archive_hash_path.first() {
                    needed_hashes.insert(hash.clone());
                }
            }
        }
    }

    println!("Need {} unique archives for pending directives", needed_hashes.len());

    // Check which of those archives are missing
    let mut missing = Vec::new();
    let needed_list: Vec<_> = needed_hashes.into_iter().collect();
    let archives = db.get_archives_by_hashes(&needed_list)?;

    // Build a set of hashes we found in the DB
    let found_hashes: std::collections::HashSet<_> = archives.iter().map(|a| a.hash.clone()).collect();

    // Check archives that exist in DB but not on disk
    for archive in &archives {
        if ctx.get_archive_path(&archive.hash).is_none() {
            missing.push((archive.name.clone(), archive.hash.clone()));
        }
    }

    // Also flag hashes that aren't even in the archives table
    for hash in &needed_list {
        if !found_hashes.contains(hash) {
            missing.push(("(unknown archive)".to_string(), hash.clone()));
        }
    }

    Ok(missing)
}

/// Check if a file path looks like a BSA/BA2 archive
fn is_bsa_file(path: &str) -> bool {
    let lower = path.to_lowercase();
    lower.ends_with(".bsa") || lower.ends_with(".ba2")
}

/// Get the working folder for BSA files extracted from archives
/// Uses local cache directory to avoid CIFS/NFS issues
fn get_bsa_working_dir(_downloads_dir: &Path) -> PathBuf {
    let cache_dir = dirs::cache_dir()
        .unwrap_or_else(|| PathBuf::from("/tmp"))
        .join("clf3")
        .join("bsa_working");
    let _ = std::fs::create_dir_all(&cache_dir);
    cache_dir
}

/// Generate a stable filename for a BSA in the working folder
fn get_bsa_working_path(downloads_dir: &Path, archive_hash: &str, bsa_path: &str) -> PathBuf {
    let working_dir = get_bsa_working_dir(downloads_dir);
    let safe_name = format!(
        "{}_{}.bsa",
        archive_hash.replace(['/', '\\', '=', '+'], "_"),
        bsa_path.replace(['/', '\\', ' '], "_")
    );
    working_dir.join(safe_name)
}

/// Clean up BSA temp directories after installation completes.
/// Call this as post-install cleanup, not at startup.
pub fn cleanup_bsa_temp_dirs(config: &InstallConfig) -> Result<()> {
    // 1. Clean up TEMP_BSA_FILES staging directory
    let staging_root = config.output_dir.join("TEMP_BSA_FILES");
    if staging_root.exists() {
        eprintln!("Cleaning up orphaned BSA staging directories...");
        match std::fs::remove_dir_all(&staging_root) {
            Ok(()) => eprintln!("  Removed: {}", staging_root.display()),
            Err(e) => eprintln!("  WARN: Failed to remove {}: {}", staging_root.display(), e),
        }
    }

    // 2. Clean up Working_BSA cache
    let working_dir = get_bsa_working_dir(&config.downloads_dir);
    if working_dir.exists() {
        eprintln!("Cleaning up BSA working cache...");
        match std::fs::remove_dir_all(&working_dir) {
            Ok(()) => eprintln!("  Removed: {}", working_dir.display()),
            Err(e) => eprintln!("  WARN: Failed to remove {}: {}", working_dir.display(), e),
        }
    }

    Ok(())
}

/// Process all pending directives using streaming pipeline.
///
/// This is an alternative to `process_directives` that uses a streaming
/// architecture with separate extraction and mover worker pools.
///
/// Benefits:
/// - Configurable worker pools (default 8+8 workers)
/// - Memory-aware backpressure to prevent OOM
/// - Better I/O pipelining for large modlists
///
/// Use this when installing large modlists or on systems with limited RAM.
pub fn process_directives_streaming(
    db: &ModlistDb,
    config: &InstallConfig,
    _extraction_workers: usize,
    _mover_workers: usize,
) -> Result<ProcessStats> {
    use super::ProgressEvent;
    let ctx = ProcessContext::new(config, db)?;
    let progress_callback = &config.progress_callback;

    // Show directive counts
    let type_counts = db.get_directive_type_counts()?;
    println!("Directive types:");
    for (dtype, count) in &type_counts {
        println!("  {:>8}  {}", count, dtype);
    }
    println!();

    // Pre-flight check
    println!("--- Pre-flight: Checking archives ---\n");
    let missing_archives = check_missing_archives(db, &ctx)?;
    if !missing_archives.is_empty() {
        println!("ERROR: {} archives are missing! Re-run to download them:\n", missing_archives.len());
        for (name, hash) in missing_archives.iter().take(20) {
            println!("  - {} ({})", name, hash);
        }
        if missing_archives.len() > 20 {
            println!("  ... and {} more", missing_archives.len() - 20);
        }
        bail!("Missing archives - re-run the installer to download them");
    }
    println!("All needed archives present\n");

    // Phase 3a: Index archive contents
    println!("--- Phase 3a: Index archive contents ---\n");
    index_archives(db, &ctx)?;

    // Phase 3b: Process directives using streaming pipeline
    println!("\n--- Phase 3b: Process directives (streaming) ---\n");

    let stats = db.get_directive_stats()?;
    let total_pending = stats.pending;

    if total_pending == 0 {
        println!("No directives to process");
        return Ok(ProcessStats::default());
    }

    println!("Processing {} directives...\n", total_pending);

    let pb = ProgressBar::new(total_pending as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) | {msg}",
            )
            .unwrap()
            .progress_chars("=>-"),
    );
    pb.enable_steady_tick(Duration::from_millis(100));

    let completed = AtomicUsize::new(0);
    let skipped = AtomicUsize::new(0);
    let failed = AtomicUsize::new(0);

    // Create progress reporter for GUI updates
    let reporter = Arc::new(ProgressReporter::new(progress_callback.clone(), total_pending));

    // Signal phase change to Installing
    if let Some(ref callback) = progress_callback {
        callback(ProgressEvent::PhaseChange {
            phase: "Installing".to_string(),
        });
    }

    // Get directive counts by type for more accurate progress (type_counts is Vec<(String, usize)>)
    let get_count = |name: &str| -> usize {
        type_counts.iter()
            .find(|(dtype, _)| dtype == name)
            .map(|(_, count)| *count)
            .unwrap_or(0)
    };
    let from_archive_count = get_count("FromArchive");
    let inline_count = get_count("InlineFile");
    let remapped_count = get_count("RemappedInlineFile");
    let patched_count = get_count("PatchedFromArchive");
    let texture_count = get_count("TransformedTexture");
    let bsa_count = get_count("CreateBSA");

    // Use streaming pipeline for FromArchive directives
    let streaming_config = super::streaming::StreamingConfig::default();
    pb.set_message("Processing FromArchive (streaming)...");
    reporter.phase_started("FromArchive", from_archive_count);

    // Create progress callback for streaming - calls reporter for each written file
    let progress_callback: Option<super::streaming::ProgressCallback> = if let Some(callback) = reporter.get_callback() {
        let total = reporter.get_total();
        Some(std::sync::Arc::new(move |written_count| {
            callback(super::ProgressEvent::DirectiveComplete {
                index: written_count,
                total,
            });
        }))
    } else {
        None
    };

    let streaming_stats = super::streaming::process_from_archive_streaming(db, &ctx, streaming_config, &pb, progress_callback)?;
    completed.fetch_add(streaming_stats.written, Ordering::Relaxed);
    skipped.fetch_add(streaming_stats.skipped, Ordering::Relaxed);
    failed.fetch_add(streaming_stats.failed, Ordering::Relaxed);

    let current_count = completed.load(Ordering::Relaxed) + skipped.load(Ordering::Relaxed) + failed.load(Ordering::Relaxed);
    reporter.set_count(current_count);
    reporter.report_count(current_count);

    // Process other directives using standard methods
    pb.set_message("Processing InlineFile...");
    reporter.phase_started("InlineFile", inline_count);
    process_simple_directives(db, &ctx, "InlineFile", &pb, &completed, &skipped, &failed, &reporter)?;
    let current_count = completed.load(Ordering::Relaxed) + skipped.load(Ordering::Relaxed) + failed.load(Ordering::Relaxed);
    reporter.set_count(current_count);

    pb.set_message("Processing RemappedInlineFile...");
    reporter.phase_started("RemappedInlineFile", remapped_count);
    process_simple_directives(db, &ctx, "RemappedInlineFile", &pb, &completed, &skipped, &failed, &reporter)?;
    let current_count = completed.load(Ordering::Relaxed) + skipped.load(Ordering::Relaxed) + failed.load(Ordering::Relaxed);
    reporter.set_count(current_count);

    pb.set_message("Processing PatchedFromArchive...");
    reporter.phase_started("PatchedFromArchive", patched_count);
    process_patched_from_archive(db, &ctx, &pb, &completed, &skipped, &failed, &reporter)?;
    let current_count = completed.load(Ordering::Relaxed) + skipped.load(Ordering::Relaxed) + failed.load(Ordering::Relaxed);
    reporter.set_count(current_count);

    pb.set_message("Processing TransformedTexture...");
    reporter.phase_started("TransformedTexture", texture_count);
    process_transformed_texture(db, &ctx, &pb, &completed, &skipped, &failed, &reporter)?;
    let current_count = completed.load(Ordering::Relaxed) + skipped.load(Ordering::Relaxed) + failed.load(Ordering::Relaxed);
    reporter.set_count(current_count);

    pb.set_message("Processing CreateBSA...");
    reporter.phase_started("CreateBSA", bsa_count);
    process_create_bsa(db, &ctx, &pb, &completed, &skipped, &failed, &reporter)?;
    let current_count = completed.load(Ordering::Relaxed) + skipped.load(Ordering::Relaxed) + failed.load(Ordering::Relaxed);
    reporter.set_count(current_count);

    pb.finish_and_clear();

    let stats = ProcessStats {
        completed: completed.load(Ordering::Relaxed),
        skipped: skipped.load(Ordering::Relaxed),
        failed: failed.load(Ordering::Relaxed),
    };

    println!(
        "Processed {} directives ({} completed, {} skipped, {} failed)",
        stats.completed + stats.skipped + stats.failed,
        stats.completed,
        stats.skipped,
        stats.failed
    );

    // Phase 4: Clean up extra files
    println!("\n--- Phase 4: Cleanup extra files ---\n");
    cleanup_extra_files(db, &ctx)?;

    Ok(stats)
}

/// Process all pending directives
pub fn process_directives(db: &ModlistDb, config: &InstallConfig) -> Result<ProcessStats> {
    let ctx = ProcessContext::new(config, db)?;

    // Show directive counts
    let type_counts = db.get_directive_type_counts()?;
    println!("Directive types:");
    for (dtype, count) in &type_counts {
        println!("  {:>8}  {}", count, dtype);
    }
    println!();

    // Pre-flight check: verify all needed archives are downloaded
    println!("--- Pre-flight: Checking archives ---\n");
    let missing_archives = check_missing_archives(db, &ctx)?;
    if !missing_archives.is_empty() {
        println!("ERROR: {} archives are missing! Re-run to download them:\n", missing_archives.len());
        for (name, hash) in missing_archives.iter().take(20) {
            println!("  - {} ({})", name, hash);
        }
        if missing_archives.len() > 20 {
            println!("  ... and {} more", missing_archives.len() - 20);
        }
        bail!("Missing archives - re-run the installer to download them");
    }
    println!("All needed archives present\n");

    // Phase 3a: Index archive contents for path lookup
    println!("--- Phase 3a: Index archive contents ---\n");
    index_archives(db, &ctx)?;

    // Phase 3b: Process directives (on-demand extraction)
    println!("\n--- Phase 3b: Process directives ---\n");

    let stats = db.get_directive_stats()?;
    let total_pending = stats.pending;

    if total_pending == 0 {
        println!("No directives to process");
        return Ok(ProcessStats::default());
    }

    println!("Processing {} directives...\n", total_pending);

    let pb = ProgressBar::new(total_pending as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) | {msg}",
            )
            .unwrap()
            .progress_chars("=>-"),
    );
    pb.enable_steady_tick(Duration::from_millis(100));

    let completed = AtomicUsize::new(0);
    let skipped = AtomicUsize::new(0);
    let failed = AtomicUsize::new(0);

    // Create a dummy reporter (no callback for CLI mode)
    let reporter = Arc::new(ProgressReporter::new(None, total_pending));

    // Process FromArchive - grouped by archive for efficiency
    pb.set_message("Processing FromArchive...");
    process_from_archive_fast(db, &ctx, &pb, &completed, &skipped, &failed)?;

    // Process InlineFile
    pb.set_message("Processing InlineFile...");
    process_simple_directives(db, &ctx, "InlineFile", &pb, &completed, &skipped, &failed, &reporter)?;

    // Process RemappedInlineFile
    pb.set_message("Processing RemappedInlineFile...");
    process_simple_directives(
        db,
        &ctx,
        "RemappedInlineFile",
        &pb,
        &completed,
        &skipped,
        &failed,
        &reporter,
    )?;

    // PatchedFromArchive - with archive-level progress
    pb.set_message("Processing PatchedFromArchive...");
    process_patched_from_archive(db, &ctx, &pb, &completed, &skipped, &failed, &reporter)?;

    // TransformedTexture - with archive-level progress
    pb.set_message("Processing TransformedTexture...");
    process_transformed_texture(db, &ctx, &pb, &completed, &skipped, &failed, &reporter)?;

    // CreateBSA - must run LAST as it depends on staged files from other directives
    pb.set_message("Processing CreateBSA...");
    process_create_bsa(db, &ctx, &pb, &completed, &skipped, &failed, &reporter)?;

    pb.finish_and_clear();

    let stats = ProcessStats {
        completed: completed.load(Ordering::Relaxed),
        skipped: skipped.load(Ordering::Relaxed),
        failed: failed.load(Ordering::Relaxed),
    };

    println!(
        "Processed {} directives ({} completed, {} skipped, {} failed)",
        stats.completed + stats.skipped + stats.failed,
        stats.completed,
        stats.skipped,
        stats.failed
    );

    // Phase 4: Clean up files not in the modlist
    println!("\n--- Phase 4: Cleanup extra files ---\n");
    cleanup_extra_files(db, &ctx)?;

    Ok(stats)
}

/// Remove any files in the output directory that aren't part of the modlist
fn cleanup_extra_files(db: &ModlistDb, ctx: &ProcessContext) -> Result<()> {
    // Get all expected output paths from directives
    let expected_paths = db.get_all_output_paths()?;

    // Normalize paths for comparison
    let expected_set: std::collections::HashSet<String> = expected_paths
        .iter()
        .map(|p| paths::normalize_for_lookup(p))
        .collect();

    println!("Expected {} files in output directory", expected_set.len());

    // Walk the output directory
    let output_dir = &ctx.config.output_dir;
    if !output_dir.exists() {
        return Ok(());
    }

    // CRITICAL: Never delete from downloads directory, even if it's inside output
    let downloads_dir = ctx.config.downloads_dir.canonicalize().ok();

    let mut deleted_files = 0;
    let mut deleted_dirs = 0;
    let mut deleted_bytes: u64 = 0;

    // Collect files to delete (can't delete while iterating)
    let mut to_delete: Vec<PathBuf> = Vec::new();

    for entry in walkdir::WalkDir::new(output_dir)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        // SKIP the downloads directory entirely
        if let Some(ref dl_dir) = downloads_dir {
            if let Ok(canonical) = entry.path().canonicalize() {
                if canonical.starts_with(dl_dir) {
                    continue;
                }
            }
        }

        // Also skip by path prefix check (in case canonicalize fails)
        if entry.path().starts_with(&ctx.config.downloads_dir) {
            continue;
        }

        if entry.file_type().is_file() {
            // Get relative path from output dir
            if let Ok(rel_path) = entry.path().strip_prefix(output_dir) {
                let normalized = paths::normalize_for_lookup(&rel_path.to_string_lossy());

                if !expected_set.contains(&normalized) {
                    to_delete.push(entry.path().to_path_buf());
                }
            }
        }
    }

    // Delete extra files
    for path in &to_delete {
        if let Ok(meta) = fs::metadata(path) {
            deleted_bytes += meta.len();
        }
        if let Err(e) = fs::remove_file(path) {
            eprintln!("WARN: Failed to delete {}: {}", path.display(), e);
        } else {
            deleted_files += 1;
        }
    }

    // Clean up empty directories (bottom-up)
    let mut dirs_to_check: Vec<PathBuf> = Vec::new();
    for entry in walkdir::WalkDir::new(output_dir)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        if entry.file_type().is_dir() && entry.path() != output_dir {
            // CRITICAL: Never delete the downloads directory or anything inside it
            if let Some(ref dl_dir) = downloads_dir {
                if let Ok(canonical) = entry.path().canonicalize() {
                    if canonical.starts_with(dl_dir) || dl_dir.starts_with(&canonical) {
                        continue;
                    }
                }
            }
            if entry.path().starts_with(&ctx.config.downloads_dir) {
                continue;
            }
            // Also skip if this dir is a parent of downloads
            if ctx.config.downloads_dir.starts_with(entry.path()) {
                continue;
            }
            dirs_to_check.push(entry.path().to_path_buf());
        }
    }

    // Sort by depth (deepest first) to delete bottom-up
    dirs_to_check.sort_by_key(|p| std::cmp::Reverse(p.components().count()));

    for dir in dirs_to_check {
        // Double-check we're not deleting downloads dir (belt and suspenders)
        if dir == ctx.config.downloads_dir {
            continue;
        }
        if let Some(ref dl_dir) = downloads_dir {
            if let Ok(canonical) = dir.canonicalize() {
                if canonical.starts_with(dl_dir) || dl_dir.starts_with(&canonical) {
                    continue;
                }
            }
        }
        // Try to remove - will only succeed if empty
        if fs::remove_dir(&dir).is_ok() {
            deleted_dirs += 1;
        }
    }

    if deleted_files > 0 || deleted_dirs > 0 {
        println!(
            "Cleaned up {} extra files ({:.1} MB) and {} empty directories",
            deleted_files,
            deleted_bytes as f64 / 1024.0 / 1024.0,
            deleted_dirs
        );
    } else {
        println!("No extra files to clean up");
    }

    Ok(())
}

/// Process FromArchive directives efficiently
/// - Groups by archive
/// - Uses direct access for ZIP/BSA (fast)
/// - Uses cached extraction for 7z/RAR (slow but cached)
fn process_from_archive_fast(
    db: &ModlistDb,
    ctx: &ProcessContext,
    pb: &ProgressBar,
    completed: &AtomicUsize,
    skipped: &AtomicUsize,
    failed: &AtomicUsize,
) -> Result<()> {
    let failure_tracker = Arc::new(FailureTracker::new());

    pb.set_message("Loading FromArchive directives...");
    let all_raw = db.get_all_pending_directives_of_type("FromArchive")?;

    if all_raw.is_empty() {
        return Ok(());
    }

    pb.set_message(format!("Parsing {} directives...", all_raw.len()));

    // Parse and group by archive, resolving paths from index
    let mut by_archive: HashMap<String, Vec<(i64, FromArchiveDirective, Option<String>)>> = HashMap::new();
    let mut parse_failures = 0;
    let mut path_lookups = 0;

    for (id, json) in all_raw {
        match serde_json::from_str::<Directive>(&json) {
            Ok(Directive::FromArchive(d)) => {
                if let Some(hash) = d.archive_hash_path.first() {
                    // Look up the correct path from the archive index
                    let resolved_path = if d.archive_hash_path.len() >= 2 {
                        let requested_path = &d.archive_hash_path[1];
                        match db.lookup_archive_file(hash, requested_path) {
                            Ok(Some(actual_path)) => {
                                path_lookups += 1;
                                Some(actual_path)
                            }
                            _ => None, // Use original path if lookup fails
                        }
                    } else {
                        None
                    };
                    by_archive.entry(hash.clone()).or_default().push((id, d, resolved_path));
                }
            }
            _ => {
                parse_failures += 1;
                failed.fetch_add(1, Ordering::Relaxed);
                pb.inc(1);
            }
        }
    }

    if parse_failures > 0 {
        pb.println(format!("WARN: {} directives failed to parse", parse_failures));
    }
    if path_lookups > 0 {
        pb.println(format!("Resolved {} paths from archive index", path_lookups));
    }

    pb.println("Using 7z binary for archive extraction (ZIP, 7z, RAR)");

    // Separate BSA archives from others (BSA uses ba2 crate, everything else uses pure Rust)
    let mut bsa_archives: Vec<_> = Vec::new();
    let mut other_archives: Vec<_> = Vec::new();

    for (archive_hash, directives) in by_archive {
        let archive_path = match ctx.get_archive_path(&archive_hash) {
            Some(p) => p.clone(),
            None => {
                // Archive not found - but check if outputs already exist first
                let mut any_needed = false;
                for (_, directive, _) in &directives {
                    if output_exists(ctx, &directive.to, directive.size) {
                        skipped.fetch_add(1, Ordering::Relaxed);
                    } else {
                        failed.fetch_add(1, Ordering::Relaxed);
                        any_needed = true;
                    }
                    pb.inc(1);
                }
                if any_needed {
                    pb.println(format!("WARN: Archive not found: {}", archive_hash));
                }
                continue;
            }
        };

        // Separate BSA/BA2 files from others
        let archive_type = detect_archive_type(&archive_path).unwrap_or(ArchiveType::Unknown);
        match archive_type {
            ArchiveType::Bsa | ArchiveType::Ba2 => bsa_archives.push((archive_hash, archive_path, directives)),
            _ => other_archives.push((archive_hash, archive_path, directives)),
        }
    }

    let num_threads = rayon::current_num_threads();

    // Phase 1: Process regular archives in parallel using pure Rust crates
    if !other_archives.is_empty() {
        // Hide main progress bar during multi-progress phase
        pb.set_draw_target(indicatif::ProgressDrawTarget::hidden());

        // Create multi-progress display like the downloader
        let mp = MultiProgress::new();
        let overall_pb = mp.add(ProgressBar::new(other_archives.len() as u64));
        overall_pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} archives | {msg}")
                .unwrap()
                .progress_chars("=>-"),
        );
        overall_pb.enable_steady_tick(Duration::from_millis(100));
        overall_pb.set_message(format!("OK:0 Skip:0 Fail:0 ({} threads)", num_threads));

        // Wrap in Arc for sharing across threads
        let mp = Arc::new(mp);
        let overall_pb = Arc::new(overall_pb);
        let failure_tracker = failure_tracker.clone();

        other_archives.par_iter().for_each(|(archive_hash, archive_path, directives)| {
            let archive_name = archive_path.file_name().unwrap_or_default().to_string_lossy().to_string();
            let display_name = if archive_name.len() > 50 {
                format!("{}...", &archive_name[..47])
            } else {
                archive_name.clone()
            };

            // Create progress bar for this archive
            let archive_pb = mp.insert_before(&overall_pb, ProgressBar::new(directives.len() as u64));
            archive_pb.set_style(
                ProgressStyle::default_bar()
                    .template("  {spinner:.blue} {wide_msg} [{bar:30.white/dim}] {pos}/{len}")
                    .unwrap()
                    .progress_chars("=>-"),
            );
            archive_pb.enable_steady_tick(Duration::from_millis(100));
            archive_pb.set_message(format!("{} (scanning)", display_name));

            // Separate directives: skip existing, collect paths for batch extraction
            let mut to_extract: Vec<(i64, &FromArchiveDirective, String)> = Vec::new();
            let mut nested_bsa: Vec<(i64, &FromArchiveDirective, String, String)> = Vec::new();
            let mut whole_file: Vec<(i64, &FromArchiveDirective)> = Vec::new();

            for (id, directive, resolved_path) in directives {
                if output_exists(ctx, &directive.to, directive.size) {
                    skipped.fetch_add(1, Ordering::Relaxed);
                    archive_pb.inc(1);
                    pb.inc(1);
                    continue;
                }

                if directive.archive_hash_path.len() == 1 {
                    // Whole archive is the file
                    whole_file.push((*id, directive));
                } else if directive.archive_hash_path.len() == 2 {
                    // Simple extraction from archive
                    let path_in_archive = resolved_path.clone()
                        .unwrap_or_else(|| directive.archive_hash_path[1].clone());
                    to_extract.push((*id, directive, path_in_archive));
                } else {
                    // Nested BSA extraction
                    let bsa_path = resolved_path.clone()
                        .unwrap_or_else(|| directive.archive_hash_path[1].clone());
                    let file_in_bsa = directive.archive_hash_path[2].clone();
                    nested_bsa.push((*id, directive, bsa_path, file_in_bsa));
                }
            }

            // Handle whole file directives (just copy the archive)
            for (_id, directive) in &whole_file {
                match fs::read(archive_path) {
                    Ok(data) => {
                        if let Err(e) = write_output(ctx, directive, &data) {
                            failed.fetch_add(1, Ordering::Relaxed);
                            failure_tracker.record_failure(&archive_name, &e.to_string());
                        } else {
                            completed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(e) => {
                        failed.fetch_add(1, Ordering::Relaxed);
                        failure_tracker.record_failure(&archive_name, &format!("Read whole file: {}", e));
                    }
                }
                archive_pb.inc(1);
                pb.inc(1);
            }

            // Batch extract all needed files from this archive
            if !to_extract.is_empty() {
                archive_pb.set_message(format!("{} (extracting {} files)", display_name, to_extract.len()));

                // Create temp dir for batch extraction
                let temp_dir = match tempfile::tempdir_in(&ctx.config.downloads_dir) {
                    Ok(d) => d,
                    Err(e) => {
                        for _ in &to_extract {
                            failed.fetch_add(1, Ordering::Relaxed);
                            archive_pb.inc(1);
                            pb.inc(1);
                        }
                        failure_tracker.record_failure(&archive_name, &format!("Create temp dir: {}", e));
                        archive_pb.finish_and_clear();
                        overall_pb.inc(1);
                        return;
                    }
                };

                // Collect unique paths for extraction
                let paths: Vec<&str> = to_extract.iter().map(|(_, _, p)| p.as_str()).collect();

                // Batch extract using pure Rust
                let extracted = match extract_batch_rust(archive_path, &paths, temp_dir.path()) {
                    Ok(data) => data,
                    Err(e) => {
                        // Log the error and fall back to per-file extraction
                        overall_pb.println(format!("BATCH FAIL {}: {}", archive_name, e));
                        archive_pb.set_message(format!("{} (fallback mode)", display_name));
                        let mut fallback_data = HashMap::new();
                        let mut fallback_errors = 0;
                        for (id, _, path) in &to_extract {
                            match extract_single_file(archive_path, path, &ctx.config.downloads_dir) {
                                Ok(data) => {
                                    let key = paths::normalize_for_lookup(path);
                                    fallback_data.insert(key, data);
                                }
                                Err(e) => {
                                    fallback_errors += 1;
                                    if fallback_errors <= 3 {
                                        overall_pb.println(format!("FALLBACK ERR [{}]: {} - {:#}", id, path, e));
                                    }
                                }
                            }
                        }
                        if fallback_errors > 3 {
                            overall_pb.println(format!("... and {} more fallback errors", fallback_errors - 3));
                        }
                        fallback_data
                    }
                };

                archive_pb.set_message(format!("{} (writing)", display_name));

                // Process extracted files
                for (id, directive, path) in &to_extract {
                    let key = paths::normalize_for_lookup(path);

                    match extracted.get(&key) {
                        Some(data) => {
                            if let Err(e) = write_output(ctx, directive, data) {
                                failed.fetch_add(1, Ordering::Relaxed);
                                failure_tracker.record_failure(&archive_name, &e.to_string());
                                if failed.load(Ordering::Relaxed) <= 20 {
                                    overall_pb.println(format!("FAIL [{}] write: {}", id, e));
                                }
                            } else {
                                completed.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        None => {
                            failed.fetch_add(1, Ordering::Relaxed);
                            failure_tracker.record_failure(&archive_name, &format!("Not found: {}", path));
                            if failed.load(Ordering::Relaxed) <= 20 {
                                overall_pb.println(format!("FAIL [{}] not found in archive: {}", id, path));
                            }
                        }
                    }
                    archive_pb.inc(1);
                    pb.inc(1);
                }
            }

            // Handle nested BSA directives
            for (id, directive, bsa_path, file_in_bsa) in &nested_bsa {
                let result: Result<Vec<u8>> = (|| {
                    // Extract BSA to working folder (too large for SQLite cache)
                    let working_dir = ctx.config.downloads_dir.join("Working_BSA_Because_Its_Too_Big_For_SQL");
                    fs::create_dir_all(&working_dir)?;

                    let bsa_temp_name = format!("{}_{}.bsa",
                        archive_hash.replace(['/', '\\', '=', '+'], "_"),
                        bsa_path.replace(['/', '\\', ' '], "_"));
                    let bsa_temp_path = working_dir.join(&bsa_temp_name);

                    // Extract BSA if temp file doesn't exist
                    if !bsa_temp_path.exists() {
                        let data = extract_single_file(archive_path, bsa_path, &ctx.config.downloads_dir)
                            .with_context(|| format!("Failed to extract BSA '{}' from archive", bsa_path))?;
                        fs::write(&bsa_temp_path, &data)
                            .with_context(|| format!("Failed to write BSA temp file: {}", bsa_temp_path.display()))?;
                    }

                    // Extract from the BSA file
                    bsa::extract_file(&bsa_temp_path, file_in_bsa)
                        .with_context(|| format!("Failed to extract '{}' from BSA", file_in_bsa))
                })();

                match result {
                    Ok(data) => {
                        if let Err(e) = write_output(ctx, directive, &data) {
                            failed.fetch_add(1, Ordering::Relaxed);
                            failure_tracker.record_failure(&archive_name, &e.to_string());
                        } else {
                            completed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(e) => {
                        failed.fetch_add(1, Ordering::Relaxed);
                        failure_tracker.record_failure(&archive_name, &format!("Nested BSA: {:?}", e));
                        if failed.load(Ordering::Relaxed) <= 20 {
                            // Use {:#} to show full error chain
                            overall_pb.println(format!("FAIL [{}] nested BSA: {:#}", id, e));
                        }
                    }
                }
                archive_pb.inc(1);
                pb.inc(1);
            }

            // Finish this archive's progress bar
            archive_pb.finish_and_clear();
            overall_pb.inc(1);
            overall_pb.set_message(format!("OK:{} Skip:{} Fail:{} ({} threads)",
                completed.load(Ordering::Relaxed),
                skipped.load(Ordering::Relaxed),
                failed.load(Ordering::Relaxed),
                num_threads
            ));
        });

        overall_pb.finish_and_clear();

        // Print failure summary
        failure_tracker.print_summary(pb);

        // Restore main progress bar visibility
        pb.set_draw_target(indicatif::ProgressDrawTarget::stderr());
    }

    // Phase 2: Process BSA archives serially (ba2 crate not thread-safe)
    if !bsa_archives.is_empty() {
        pb.set_message(format!("Processing {} BSA archives...", bsa_archives.len()));

        for (i, (_archive_hash, archive_path, directives)) in bsa_archives.iter().enumerate() {
            let archive_name = archive_path.file_name().unwrap_or_default().to_string_lossy();
            pb.set_message(format!("[{}/{}] {} ({} files)", i + 1, bsa_archives.len(), archive_name, directives.len()));

            if let Err(e) = process_bsa_archive(ctx, archive_path, directives, pb, completed, skipped, failed, &failure_tracker) {
                pb.println(format!("ERROR: {}: {}", archive_name, e));
                failure_tracker.record_failure(&archive_name, &e.to_string());
            }
        }
    }

    // Print summary of failures by archive
    failure_tracker.print_summary(pb);

    Ok(())
}

/// Process a BSA archive - uses ba2 crate random access
#[allow(clippy::too_many_arguments)]
fn process_bsa_archive(
    ctx: &ProcessContext,
    archive_path: &Path,
    directives: &[(i64, FromArchiveDirective, Option<String>)],
    pb: &ProgressBar,
    completed: &AtomicUsize,
    skipped: &AtomicUsize,
    failed: &AtomicUsize,
    failure_tracker: &FailureTracker,
) -> Result<()> {
    let archive_name = archive_path.file_name().unwrap_or_default().to_string_lossy();
    for (id, directive, _resolved_path) in directives {
        if output_exists(ctx, &directive.to, directive.size) {
            skipped.fetch_add(1, Ordering::Relaxed);
            pb.inc(1);
            continue;
        }

        // Handle GameFileSource (single element = copy the BSA itself)
        if directive.archive_hash_path.len() == 1 {
            match fs::read(archive_path) {
                Ok(data) => {
                    if write_output(ctx, directive, &data).is_ok() {
                        completed.fetch_add(1, Ordering::Relaxed);
                    } else {
                        failed.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Err(_) => {
                    failed.fetch_add(1, Ordering::Relaxed);
                }
            }
            pb.inc(1);
            continue;
        }

        let file_path = directive.archive_hash_path.get(1).map(|s| s.as_str()).unwrap_or("");

        // Skip empty paths
        if file_path.is_empty() {
            let err_str = "Empty file path in BSA directive";
            failed.fetch_add(1, Ordering::Relaxed);
            failure_tracker.record_failure(&archive_name, err_str);
            if failed.load(Ordering::Relaxed) <= 10 {
                pb.println(format!("FAIL [{}] in '{}': {}", id, archive_name, err_str));
            }
            pb.inc(1);
            continue;
        }

        match bsa::extract_file(archive_path, file_path) {
            Ok(data) => {
                if let Err(e) = write_output(ctx, directive, &data) {
                    let err_str = e.to_string();
                    failed.fetch_add(1, Ordering::Relaxed);
                    failure_tracker.record_failure(&archive_name, &err_str);
                    if failed.load(Ordering::Relaxed) <= 10 {
                        pb.println(format!("FAIL [{}] in '{}': {}", id, archive_name, err_str));
                    }
                } else {
                    completed.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(e) => {
                let err_str = e.to_string();
                failed.fetch_add(1, Ordering::Relaxed);
                failure_tracker.record_failure(&archive_name, &err_str);
                if failed.load(Ordering::Relaxed) <= 10 {
                    pb.println(format!("FAIL [{}] in '{}': {}", id, archive_name, err_str));
                }
            }
        }
        pb.inc(1);
    }
    Ok(())
}

/// Write data to output file
fn write_output(ctx: &ProcessContext, directive: &FromArchiveDirective, data: &[u8]) -> Result<()> {
    if data.len() as u64 != directive.size {
        bail!(
            "Size mismatch: expected {} bytes, got {}",
            directive.size,
            data.len()
        );
    }

    let output_path = ctx.resolve_output_path(&directive.to);
    paths::ensure_parent_dirs(&output_path)?;

    let mut file = File::create(&output_path)?;
    file.write_all(data)?;

    Ok(())
}

/// Process PatchedFromArchive directives with producer-consumer pipeline.
///
/// Architecture:
/// - Patch applier workers (N/2 threads): Extract source, apply delta patch, produce patched data
/// - Writer workers (N/2 threads): Consume and write to disk
/// Process PatchedFromArchive directives with memory-optimized disk-based extraction.
///
/// This uses disk-based extraction and memory-mapped files to minimize RAM usage:
/// - Source files are extracted to temp directory, not loaded into RAM
/// - Patches are applied using memory-mapped source files
/// - Output is streamed directly to disk
///
/// Memory usage is bounded to ~O(delta_size) per patch instead of O(source + delta + output).
fn process_patched_from_archive(
    db: &ModlistDb,
    ctx: &ProcessContext,
    pb: &ProgressBar,
    completed: &AtomicUsize,
    skipped: &AtomicUsize,
    failed: &AtomicUsize,
    reporter: &Arc<ProgressReporter>,
) -> Result<()> {
    use crate::modlist::PatchedFromArchiveDirective;
    use crossbeam_channel::bounded;

    let failure_tracker = Arc::new(FailureTracker::new());

    // Clean up any leftover temp dirs from previous interrupted runs
    crate::installer::streaming::cleanup_temp_dirs(&ctx.config.downloads_dir);

    pb.set_message("Loading PatchedFromArchive directives...");
    let all_raw = db.get_all_pending_directives_of_type("PatchedFromArchive")?;

    if all_raw.is_empty() {
        pb.println("No PatchedFromArchive directives to process");
        return Ok(());
    }

    pb.set_message("Pre-filtering completed patches...");

    // Parse, pre-filter against existing files, and group by archive
    let mut by_archive: HashMap<String, Vec<(i64, PatchedFromArchiveDirective)>> = HashMap::new();
    let mut parse_failures = 0;
    let mut pre_skipped = 0usize;

    for (id, json) in all_raw {
        match serde_json::from_str::<Directive>(&json) {
            Ok(Directive::PatchedFromArchive(d)) => {
                // PRE-FILTER: Skip if output already exists with correct size
                let normalized_to = paths::normalize_for_lookup(&d.to);
                if let Some(&existing_size) = ctx.existing_files.get(&normalized_to) {
                    if existing_size == d.size {
                        pre_skipped += 1;
                        skipped.fetch_add(1, Ordering::Relaxed);
                        continue; // Skip this directive entirely
                    }
                }

                if let Some(hash) = d.archive_hash_path.first() {
                    by_archive.entry(hash.clone()).or_default().push((id, d));
                }
            }
            _ => {
                parse_failures += 1;
                failed.fetch_add(1, Ordering::Relaxed);
                pb.inc(1);
            }
        }
    }

    if pre_skipped > 0 {
        eprintln!("Pre-filtered {} already-complete patches", pre_skipped);
        // Report pre-skipped items to GUI
        for _ in 0..pre_skipped {
            reporter.directive_completed();
        }
    }
    if parse_failures > 0 {
        pb.println(format!("WARN: {} PatchedFromArchive directives failed to parse", parse_failures));
        // Report parse failures to GUI
        for _ in 0..parse_failures {
            reporter.directive_completed();
        }
    }

    // Build archive list with sizes for sorting
    let mut archives_with_size: Vec<(String, Vec<(i64, PatchedFromArchiveDirective)>, u64)> = by_archive
        .into_iter()
        .map(|(hash, directives)| {
            let size = ctx.get_archive_path(&hash)
                .and_then(|p| fs::metadata(p).ok())
                .map(|m| m.len())
                .unwrap_or(0);
            (hash, directives, size)
        })
        .collect();

    // Sort archives smallest to largest for better throughput
    archives_with_size.sort_by_key(|(_, _, size)| *size);

    let total_archives = archives_with_size.len();
    let total_directives: usize = archives_with_size.iter().map(|(_, d, _)| d.len()).sum();

    eprintln!(
        "Processing {} patches across {} archives (mmap-based, parallel)...",
        total_directives, total_archives
    );

    // Reset progress bar for patching phase
    pb.finish_and_clear();
    pb.reset();
    pb.set_length(total_directives as u64);
    pb.set_position(0);
    pb.enable_steady_tick(std::time::Duration::from_millis(100));

    // Create a dummy channel (not used, but keeps API compatible)
    // The new process_archive_patches writes directly to disk
    let (tx, _rx) = bounded::<(Vec<u8>, PathBuf, u64, i64)>(1);

    // Wrap counters in Arc for sharing
    let completed = Arc::new(AtomicUsize::new(completed.load(Ordering::Relaxed)));
    let failed = Arc::new(AtomicUsize::new(failed.load(Ordering::Relaxed)));
    let skipped = Arc::new(AtomicUsize::new(skipped.load(Ordering::Relaxed)));

    // Tier archives by size (same approach as FromArchive)
    const HALF_GB: u64 = 512 * 1024 * 1024;
    const TWO_GB: u64 = 2 * 1024 * 1024 * 1024;

    let mut small_archives = Vec::new();  // ≤512MB - full parallel
    let mut medium_archives = Vec::new(); // 512MB-2GB - limited parallel
    let mut large_archives = Vec::new();  // ≥2GB - sequential

    for (hash, directives, size) in archives_with_size {
        if size <= HALF_GB {
            small_archives.push((hash, directives, size));
        } else if size < TWO_GB {
            medium_archives.push((hash, directives, size));
        } else {
            large_archives.push((hash, directives, size));
        }
    }

    eprintln!(
        "Patching: {} small + {} medium + {} large archives",
        small_archives.len(), medium_archives.len(), large_archives.len()
    );

    // Process SMALL archives in full parallel
    small_archives.par_iter().for_each(|(archive_hash, directives, _size)| {
        process_archive_patches(
            ctx,
            archive_hash,
            directives,
            &tx,
            pb,
            &skipped,
            &failed,
            &failure_tracker,
            &completed,
            reporter,
        );
    });

    // Process MEDIUM archives with limited parallelism (half threads)
    let half_threads = (rayon::current_num_threads() / 2).max(1);
    for chunk in medium_archives.chunks(half_threads) {
        chunk.par_iter().for_each(|(archive_hash, directives, _size)| {
            process_archive_patches(
                ctx,
                archive_hash,
                directives,
                &tx,
                pb,
                &skipped,
                &failed,
                &failure_tracker,
                &completed,
                reporter,
            );
        });
    }

    // Process LARGE archives sequentially (one at a time, all threads for extraction)
    for (archive_hash, directives, _size) in &large_archives {
        process_archive_patches(
            ctx,
            archive_hash,
            directives,
            &tx,
            pb,
            &skipped,
            &failed,
            &failure_tracker,
            &completed,
            reporter,
        );
    }

    // Print failure summary
    failure_tracker.print_summary(pb);

    Ok(())
}

/// Process patches for a single archive using disk-based extraction (memory-optimized).
///
/// Instead of loading all source files into RAM, extracts to temp directory and uses
/// memory-mapped files for patching. Writes output directly to disk.
fn process_archive_patches(
    ctx: &ProcessContext,
    archive_hash: &str,
    directives: &[(i64, crate::modlist::PatchedFromArchiveDirective)],
    _tx: &crossbeam_channel::Sender<(Vec<u8>, PathBuf, u64, i64)>,
    pb: &ProgressBar,
    skipped: &Arc<AtomicUsize>,
    failed: &Arc<AtomicUsize>,
    failure_tracker: &Arc<FailureTracker>,
    completed: &Arc<AtomicUsize>,
    reporter: &Arc<ProgressReporter>,
) {
    let archive_path = match ctx.get_archive_path(archive_hash) {
        Some(p) => p.clone(),
        None => {
            // Archive not found - but check if outputs already exist first
            let mut any_needed = false;
            for (_, directive) in directives {
                if output_exists(ctx, &directive.to, directive.size) {
                    skipped.fetch_add(1, Ordering::Relaxed);
                } else {
                    failed.fetch_add(1, Ordering::Relaxed);
                    any_needed = true;
                }
                pb.inc(1);
                reporter.directive_completed();
            }
            if any_needed {
                pb.println(format!("WARN: Archive not found for patching: {}", archive_hash));
            }
            return;
        }
    };

    // Filter to directives that need processing
    let to_process: Vec<_> = directives
        .iter()
        .filter(|(_, d)| !output_exists(ctx, &d.to, d.size))
        .collect();

    // Count skipped and report progress
    let skip_count = directives.len() - to_process.len();
    if skip_count > 0 {
        skipped.fetch_add(skip_count, Ordering::Relaxed);
        pb.inc(skip_count as u64);
        for _ in 0..skip_count {
            reporter.directive_completed();
        }
    }

    if to_process.is_empty() {
        return;
    }

    // Collect unique source paths needed from this archive
    let mut simple_paths: HashMap<String, String> = HashMap::new();
    let mut nested_bsas: HashMap<String, Vec<String>> = HashMap::new();

    for (_, directive) in &to_process {
        if directive.archive_hash_path.len() == 2 {
            let path = &directive.archive_hash_path[1];
            let normalized = paths::normalize_for_lookup(path);
            simple_paths.insert(normalized, path.clone());
        } else if directive.archive_hash_path.len() >= 3 {
            let bsa_path = &directive.archive_hash_path[1];
            let file_in_bsa = &directive.archive_hash_path[2];
            nested_bsas.entry(bsa_path.clone()).or_default().push(file_in_bsa.clone());
            let normalized = paths::normalize_for_lookup(bsa_path);
            simple_paths.insert(normalized, bsa_path.clone());
        }
    }

    // Extract source files to temp directory (disk-based, not RAM)
    let temp_dir = match tempfile::tempdir_in(&ctx.config.downloads_dir) {
        Ok(dir) => dir,
        Err(e) => {
            for (_id, _) in &to_process {
                failed.fetch_add(1, Ordering::Relaxed);
                failure_tracker.record_failure("tempdir", &e.to_string());
                pb.inc(1);
                reporter.directive_completed();
            }
            return;
        }
    };

    let extracted_paths = extract_source_files_to_disk(
        ctx,
        &archive_path,
        &simple_paths,
        &nested_bsas,
        temp_dir.path(),
    );

    let archive_name = archive_path.file_name().unwrap_or_default().to_string_lossy().to_string();

    // Process each patch sequentially (to limit memory), write directly to disk
    for (id, directive) in &to_process {
        let result = apply_patch_streaming(
            ctx,
            &archive_path,
            directive,
            &extracted_paths,
        );

        match result {
            Ok(()) => {
                completed.fetch_add(1, Ordering::Relaxed);
                pb.inc(1);
                reporter.directive_completed();
            }
            Err(e) => {
                failed.fetch_add(1, Ordering::Relaxed);
                failure_tracker.record_failure(&archive_name, &e.to_string());
                if failed.load(Ordering::Relaxed) <= 10 {
                    pb.println(format!("FAIL [{}] patch: {:#}", id, e));
                }
                pb.inc(1);
                reporter.directive_completed();
            }
        }
    }

    // temp_dir is dropped here, cleaning up extracted files
}

/// Extract source files needed for patching to disk (memory-optimized).
///
/// Returns a map of normalized path -> temp file path on disk.
/// Files are extracted to the provided temp directory, not loaded into RAM.
fn extract_source_files_to_disk(
    _ctx: &ProcessContext,
    archive_path: &Path,
    simple_paths: &HashMap<String, String>,
    nested_bsas: &HashMap<String, Vec<String>>,
    temp_dir: &Path,
) -> HashMap<String, PathBuf> {
    let mut extracted: HashMap<String, PathBuf> = HashMap::new();

    if !simple_paths.is_empty() {
        let archive_type = detect_archive_type(archive_path).unwrap_or(ArchiveType::Unknown);

        match archive_type {
            ArchiveType::Zip => {
                if let Ok(file) = File::open(archive_path) {
                    if let Ok(mut archive) = zip::ZipArchive::new(BufReader::new(file)) {
                        for i in 0..archive.len() {
                            if let Ok(mut entry) = archive.by_index(i) {
                                let entry_name = entry.name().to_string();
                                let normalized = paths::normalize_for_lookup(&entry_name);
                                if simple_paths.contains_key(&normalized) {
                                    // Extract to temp file instead of RAM
                                    let temp_file_path = temp_dir.join(format!("src_{}.tmp", i));
                                    if let Ok(mut out_file) = File::create(&temp_file_path) {
                                        if std::io::copy(&mut entry, &mut out_file).is_ok() {
                                            extracted.insert(normalized, temp_file_path);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            ArchiveType::Bsa | ArchiveType::Ba2 => {
                // BSA files: extract each needed file to disk
                for (idx, (normalized, original)) in simple_paths.iter().enumerate() {
                    if let Ok(data) = bsa::extract_file(archive_path, original) {
                        let temp_file_path = temp_dir.join(format!("bsa_{}.tmp", idx));
                        if fs::write(&temp_file_path, &data).is_ok() {
                            extracted.insert(normalized.clone(), temp_file_path);
                        }
                    }
                }
            }
            ArchiveType::SevenZ | ArchiveType::Rar => {
                // Extract entire archive to temp (one fast 7z call)
                // This is much faster than streaming individual files (one 7z call per file)
                let extract_dir = temp_dir.join("archive_extract");
                let _ = fs::create_dir_all(&extract_dir);

                if crate::archive::sevenzip::extract_all(archive_path, &extract_dir).is_ok() {
                    // Walk extracted files and find what we need
                    for entry in walkdir::WalkDir::new(&extract_dir)
                        .into_iter()
                        .filter_map(|e| e.ok())
                        .filter(|e| e.file_type().is_file())
                    {
                        if let Ok(rel_path) = entry.path().strip_prefix(&extract_dir) {
                            let normalized = paths::normalize_for_lookup(&rel_path.to_string_lossy());
                            if simple_paths.contains_key(&normalized) {
                                extracted.insert(normalized, entry.path().to_path_buf());
                            }
                        }
                    }
                }
            }
            _ => {
                // Unknown format: try all extraction methods, extract entire archive to temp
                let extract_dir = temp_dir.join("archive_extract");
                let _ = fs::create_dir_all(&extract_dir);

                let extract_result = extract_zip_to_temp(archive_path, &extract_dir)
                    .or_else(|_| extract_7z_to_temp(archive_path, &extract_dir))
                    .or_else(|_| extract_rar_to_temp(archive_path, &extract_dir));

                if extract_result.is_ok() {
                    // Walk extracted files and collect paths for needed files
                    collect_needed_file_paths(
                        &extract_dir,
                        &extract_dir,
                        simple_paths,
                        &mut extracted,
                    );
                }
            }
        }
    }

    // For nested BSAs: extract files from BSAs we just extracted to disk
    for (bsa_path, files_in_bsa) in nested_bsas {
        let bsa_normalized = paths::normalize_for_lookup(bsa_path);
        // Clone the path to avoid borrow conflict
        let bsa_file_path = match extracted.get(&bsa_normalized) {
            Some(p) => p.clone(),
            None => continue,
        };
        // Extract files from the BSA to disk
        for (idx, file_path) in files_in_bsa.iter().enumerate() {
            if let Ok(file_data) = bsa::extract_file(&bsa_file_path, file_path) {
                let key = format!("{}/{}", bsa_path, file_path);
                let normalized_key = paths::normalize_for_lookup(&key);
                let temp_file_path = temp_dir.join(format!("nested_{}_{}.tmp",
                    bsa_normalized.replace(['/', '\\'], "_"), idx));
                if fs::write(&temp_file_path, &file_data).is_ok() {
                    extracted.insert(normalized_key, temp_file_path);
                }
            }
        }
    }

    extracted
}

/// Recursively collect file paths from extracted archive (disk-based version)
fn collect_needed_file_paths(
    base: &Path,
    current: &Path,
    needed: &HashMap<String, String>,
    results: &mut HashMap<String, PathBuf>,
) {
    let needed_normalized: std::collections::HashSet<String> =
        needed.keys().cloned().collect();

    if let Ok(entries) = fs::read_dir(current) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_needed_file_paths(base, &path, needed, results);
            } else if path.is_file() {
                // Get relative path from base
                if let Ok(rel) = path.strip_prefix(base) {
                    let rel_str = rel.to_string_lossy();
                    let normalized = paths::normalize_for_lookup(&rel_str);

                    if needed_normalized.contains(&normalized) {
                        results.insert(normalized, path.clone());
                    }
                }
            }
        }
    }
}

/// Apply a delta patch using memory-mapped files, streaming output directly to disk.
///
/// This is the memory-optimized version that:
/// 1. Memory-maps the source file (OS manages paging, not loaded to RAM)
/// 2. Reads delta from wabbajack (deltas are small, ~KB)
/// 3. Streams output directly to disk via buffered writer
fn apply_patch_streaming(
    ctx: &ProcessContext,
    archive_path: &Path,
    directive: &crate::modlist::PatchedFromArchiveDirective,
    extracted_paths: &HashMap<String, PathBuf>,
) -> Result<()> {
    use memmap2::Mmap;
    use std::io::{BufWriter, Cursor};
    use crate::octodiff::DeltaReader;

    // Get source file path from extracted cache
    let source_path = if directive.archive_hash_path.len() == 1 {
        // Whole archive file is the source
        archive_path.to_path_buf()
    } else if directive.archive_hash_path.len() == 2 {
        // Simple: file directly in archive
        let path = &directive.archive_hash_path[1];
        let normalized = paths::normalize_for_lookup(path);
        extracted_paths.get(&normalized)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Source file not found in extracted cache: {}", path))?
    } else if directive.archive_hash_path.len() >= 3 {
        // Nested BSA: file inside BSA inside archive
        let bsa_path = &directive.archive_hash_path[1];
        let file_in_bsa = &directive.archive_hash_path[2];
        let key = format!("{}/{}", bsa_path, file_in_bsa);
        let normalized = paths::normalize_for_lookup(&key);
        extracted_paths.get(&normalized)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Source file not found in BSA cache: {} / {}", bsa_path, file_in_bsa))?
    } else {
        anyhow::bail!("Invalid archive_hash_path");
    };

    // Memory-map source file (OS manages paging, not loaded to RAM)
    let source_file = File::open(&source_path)
        .with_context(|| format!("Failed to open source file: {}", source_path.display()))?;
    let source_mmap = unsafe {
        Mmap::map(&source_file)
            .with_context(|| format!("Failed to mmap source file: {}", source_path.display()))?
    };

    // Load delta patch from wabbajack (deltas are small, typically KB)
    let patch_name = directive.patch_id.to_string();
    let delta_data = ctx.read_wabbajack_file(&patch_name)?;

    // Create DeltaReader with mmap'd source
    let basis = Cursor::new(&source_mmap[..]);
    let delta = Cursor::new(delta_data);
    let mut reader = DeltaReader::new(basis, delta)?;

    // Stream directly to output file
    let output_path = ctx.resolve_output_path(&directive.to);
    paths::ensure_parent_dirs(&output_path)?;
    let output_file = File::create(&output_path)
        .with_context(|| format!("Failed to create output file: {}", output_path.display()))?;
    let mut writer = BufWriter::new(output_file);

    let written = std::io::copy(&mut reader, &mut writer)
        .with_context(|| format!("Failed to write patched file: {}", output_path.display()))?;

    // Verify size
    if written != directive.size {
        // Remove incomplete file
        let _ = fs::remove_file(&output_path);
        anyhow::bail!("Size mismatch: expected {}, got {}", directive.size, written);
    }

    Ok(())
}
/// Process TransformedTexture directives with progress display
fn process_transformed_texture(
    db: &ModlistDb,
    ctx: &ProcessContext,
    pb: &ProgressBar,
    completed: &AtomicUsize,
    skipped: &AtomicUsize,
    failed: &AtomicUsize,
    reporter: &Arc<ProgressReporter>,
) -> Result<()> {
    use crate::modlist::TransformedTextureDirective;

    let failure_tracker = Arc::new(FailureTracker::new());

    pb.set_message("Loading TransformedTexture directives...");
    let all_raw = db.get_all_pending_directives_of_type("TransformedTexture")?;

    if all_raw.is_empty() {
        pb.println("No TransformedTexture directives to process");
        return Ok(());
    }

    pb.set_message(format!("Parsing {} TransformedTexture directives...", all_raw.len()));

    // Parse and group by archive
    let mut by_archive: HashMap<String, Vec<(i64, TransformedTextureDirective)>> = HashMap::new();
    let mut parse_failures = 0;

    for (id, json) in all_raw {
        match serde_json::from_str::<Directive>(&json) {
            Ok(Directive::TransformedTexture(d)) => {
                if let Some(hash) = d.archive_hash_path.first() {
                    by_archive.entry(hash.clone()).or_default().push((id, d));
                }
            }
            _ => {
                parse_failures += 1;
                failed.fetch_add(1, Ordering::Relaxed);
                pb.inc(1);
            }
        }
    }

    if parse_failures > 0 {
        pb.println(format!("WARN: {} TransformedTexture directives failed to parse", parse_failures));
    }

    // Pre-scan for unsupported texture formats
    pb.set_message("Checking texture formats...");
    let unsupported = handlers::texture::find_unsupported_formats(by_archive.values().flatten());

    if !unsupported.is_empty() {
        // Suspend progress bar to show prompt
        pb.suspend(|| {
            if handlers::texture::prompt_unsupported_formats(&unsupported) {
                handlers::texture::enable_fallback_mode();
                println!("Continuing with BC1 fallback for unsupported formats...\n");
            } else {
                println!("\nAborting installation. Please report unsupported formats to developers.");
                std::process::exit(1);
            }
        });
    }

    let total_archives = by_archive.len();
    let total_directives: usize = by_archive.values().map(|v| v.len()).sum();
    eprintln!("Processing {} textures across {} archives...", total_directives, total_archives);

    // Reset progress bar for texture phase
    pb.finish_and_clear();
    pb.reset();
    pb.set_length(total_directives as u64);
    pb.set_position(0);
    pb.enable_steady_tick(std::time::Duration::from_millis(100));

    let failure_tracker = failure_tracker.clone();
    let reporter = reporter.clone();

    // Process archives in parallel
    let archives_vec: Vec<_> = by_archive.into_iter().collect();

    archives_vec.par_iter().for_each(|(archive_hash, directives)| {
        let archive_path = match ctx.get_archive_path(archive_hash) {
            Some(p) => p.clone(),
            None => {
                // Archive not found - but check if outputs already exist first
                let mut any_needed = false;
                for (_, directive) in directives {
                    if output_dds_valid(ctx, &directive.to) {
                        skipped.fetch_add(1, Ordering::Relaxed);
                    } else {
                        failed.fetch_add(1, Ordering::Relaxed);
                        any_needed = true;
                    }
                    pb.inc(1);
                    reporter.directive_completed();
                }
                if any_needed {
                    pb.println(format!("WARN: Archive not found for textures: {}", archive_hash));
                }
                return;
            }
        };

        let archive_name = archive_path.file_name().unwrap_or_default().to_string_lossy().to_string();

        // Filter to directives that need processing
        // Use DDS header validation instead of size comparison because texture
        // compression is not deterministic - output size may differ from expected
        let to_process: Vec<_> = directives
            .iter()
            .filter(|(_, d)| !output_dds_valid(ctx, &d.to))
            .collect();

        // Count skipped and report progress
        let skip_count = directives.len() - to_process.len();
        if skip_count > 0 {
            skipped.fetch_add(skip_count, Ordering::Relaxed);
            pb.inc(skip_count as u64);
            for _ in 0..skip_count {
                reporter.directive_completed();
            }
        }

        if to_process.is_empty() {
            return;
        }

        // Collect unique source paths needed from this archive
        // Simple paths (len == 2): file directly in archive
        // Nested BSA (len >= 3): BSA in archive, file in BSA
        let mut simple_paths: HashMap<String, String> = HashMap::new();
        let mut nested_bsas: HashMap<String, Vec<String>> = HashMap::new();

        for (_, directive) in &to_process {
            if directive.archive_hash_path.len() == 2 {
                let path = &directive.archive_hash_path[1];
                let normalized = paths::normalize_for_lookup(path);
                simple_paths.insert(normalized, path.clone());
            } else if directive.archive_hash_path.len() >= 3 {
                let bsa_path = &directive.archive_hash_path[1];
                let file_in_bsa = &directive.archive_hash_path[2];
                nested_bsas.entry(bsa_path.clone()).or_default().push(file_in_bsa.clone());
                // Also need to extract the BSA itself
                let normalized = paths::normalize_for_lookup(bsa_path);
                simple_paths.insert(normalized, bsa_path.clone());
            }
        }

        // Batch extract all needed files from this archive
        let mut extracted: HashMap<String, Vec<u8>> = HashMap::new();

        if !simple_paths.is_empty() {
            let archive_type = detect_archive_type(&archive_path).unwrap_or(ArchiveType::Unknown);
            let batch_result = match archive_type {
                ArchiveType::Zip => {
                    let mut result = HashMap::new();
                    if let Ok(file) = File::open(&archive_path) {
                        if let Ok(mut archive) = zip::ZipArchive::new(BufReader::new(file)) {
                            for i in 0..archive.len() {
                                if let Ok(mut entry) = archive.by_index(i) {
                                    let entry_name = entry.name().to_string();
                                    let normalized = paths::normalize_for_lookup(&entry_name);
                                    if simple_paths.contains_key(&normalized) {
                                        let mut data = Vec::with_capacity(entry.size() as usize);
                                        if std::io::Read::read_to_end(&mut entry, &mut data).is_ok() {
                                            result.insert(normalized, data);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    result
                }
                ArchiveType::Bsa | ArchiveType::Ba2 => {
                    let mut result = HashMap::new();
                    for (normalized, original) in &simple_paths {
                        if let Ok(data) = bsa::extract_file(&archive_path, original) {
                            result.insert(normalized.clone(), data);
                        }
                    }
                    result
                }
                _ => {
                    // 7z/RAR/other: batch extract using pure Rust
                    if let Ok(temp_dir) = tempfile::tempdir_in(&ctx.config.downloads_dir) {
                        let paths_vec: Vec<&str> = simple_paths.values().map(|s| s.as_str()).collect();
                        extract_batch_rust(&archive_path, &paths_vec, temp_dir.path())
                            .unwrap_or_default()
                    } else {
                        HashMap::new()
                    }
                }
            };
            extracted.extend(batch_result);
        }

        // For nested BSAs: extract files from BSAs we just extracted
        for (bsa_path, files_in_bsa) in &nested_bsas {
            let bsa_normalized = paths::normalize_for_lookup(bsa_path);
            if let Some(bsa_data) = extracted.get(&bsa_normalized) {
                // Write BSA to temp file and extract from it
                if let Ok(temp_bsa) = tempfile::Builder::new()
                    .prefix(".clf3_bsa_")
                    .suffix(".bsa")
                    .tempfile_in(&ctx.config.downloads_dir)
                {
                    if fs::write(temp_bsa.path(), bsa_data).is_ok() {
                        for file_path in files_in_bsa {
                            if let Ok(file_data) = bsa::extract_file(temp_bsa.path(), file_path) {
                                // Key: "bsa_path/file_path" normalized
                                let key = format!("{}/{}", bsa_path, file_path);
                                let normalized_key = paths::normalize_for_lookup(&key);
                                extracted.insert(normalized_key, file_data);
                            }
                        }
                    }
                }
            }
        }

        // Process each texture using the extracted data
        for (id, directive) in to_process {
            let result: Result<()> = (|| {
                // Get source data from extracted cache
                let source_data = if directive.archive_hash_path.len() == 2 {
                    // Simple: file directly in archive
                    let path = &directive.archive_hash_path[1];
                    let normalized = paths::normalize_for_lookup(path);
                    extracted.get(&normalized)
                        .ok_or_else(|| anyhow::anyhow!("Source texture not found: {}", path))?
                } else if directive.archive_hash_path.len() >= 3 {
                    // Nested BSA: file inside BSA inside archive
                    let bsa_path = &directive.archive_hash_path[1];
                    let file_in_bsa = &directive.archive_hash_path[2];
                    let key = format!("{}/{}", bsa_path, file_in_bsa);
                    let normalized = paths::normalize_for_lookup(&key);
                    extracted.get(&normalized)
                        .ok_or_else(|| anyhow::anyhow!("Source texture not found in BSA: {} / {}", bsa_path, file_in_bsa))?
                } else {
                    anyhow::bail!("Invalid archive_hash_path for texture");
                };

                // Process the texture
                handlers::handle_transformed_texture(ctx, directive, source_data)?;

                Ok(())
            })();

            match result {
                Ok(()) => {
                    completed.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    failed.fetch_add(1, Ordering::Relaxed);
                    failure_tracker.record_failure(&archive_name, &e.to_string());
                    if failed.load(Ordering::Relaxed) <= 10 {
                        pb.println(format!("FAIL [{}] texture: {:#}", id, e));
                    }
                }
            }
            pb.inc(1);
            reporter.directive_completed();
        }
    });

    // Print failure summary
    failure_tracker.print_summary(pb);

    Ok(())
}

/// Process CreateBSA directives - builds BSA archives from staged files
fn process_create_bsa(
    db: &ModlistDb,
    ctx: &ProcessContext,
    pb: &ProgressBar,
    completed: &AtomicUsize,
    skipped: &AtomicUsize,
    failed: &AtomicUsize,
    reporter: &Arc<ProgressReporter>,
) -> Result<()> {
    use crate::modlist::CreateBSADirective;

    pb.set_message("Loading CreateBSA directives...");
    let all_raw = db.get_all_pending_directives_of_type("CreateBSA")?;

    if all_raw.is_empty() {
        return Ok(());
    }

    eprintln!("Building {} BSA archives...", all_raw.len());

    // Reset progress bar for BSA build phase
    pb.finish_and_clear();
    pb.reset();
    pb.set_length(all_raw.len() as u64);
    pb.set_position(0);
    pb.enable_steady_tick(std::time::Duration::from_millis(100));

    // Parse all directives
    let mut directives: Vec<(i64, CreateBSADirective)> = all_raw
        .into_iter()
        .filter_map(|(id, json)| {
            match serde_json::from_str::<Directive>(&json) {
                Ok(Directive::CreateBSA(d)) => Some((id, d)),
                _ => {
                    failed.fetch_add(1, Ordering::Relaxed);
                    pb.inc(1);
                    None
                }
            }
        })
        .collect();

    // Sort by file count (smallest first) for faster progress feedback
    directives.sort_by_key(|(_, d)| d.file_states.len());

    // Simple threshold: ≤250 files parallel, >250 files sequential
    const PARALLEL_THRESHOLD: usize = 250;

    let (small, large): (Vec<_>, Vec<_>) = directives
        .into_iter()
        .partition(|(_, d)| d.file_states.len() <= PARALLEL_THRESHOLD);

    eprintln!(
        "Processing {} BSAs: {} small (≤{} files, 4 at a time), {} large (>{} files, sequential)",
        small.len() + large.len(),
        small.len(), PARALLEL_THRESHOLD,
        large.len(), PARALLEL_THRESHOLD
    );

    // Clone reporter for use in parallel iteration
    let reporter = reporter.clone();

    // Process SMALL BSAs in batches of 4
    for chunk in small.chunks(4) {
        chunk.par_iter().for_each(|(id, directive)| {
            if handlers::output_bsa_valid(ctx, directive) {
                skipped.fetch_add(1, Ordering::Relaxed);
                pb.inc(1);
                reporter.directive_completed();
                return;
            }

            let bsa_name = std::path::Path::new(&directive.to)
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_else(|| directive.to.clone());

            match handlers::handle_create_bsa(ctx, directive) {
                Ok(()) => {
                    completed.fetch_add(1, Ordering::Relaxed);
                    pb.println(format!("Created BSA: {}", bsa_name));
                }
                Err(e) => {
                    failed.fetch_add(1, Ordering::Relaxed);
                    pb.println(format!("FAIL [{}] create BSA {}: {:#}", id, bsa_name, e));
                }
            }
            pb.inc(1);
            reporter.directive_completed();
        });
    }

    // Process LARGE BSAs sequentially (one at a time, full thread usage)
    for (id, directive) in large {
        if handlers::output_bsa_valid(ctx, &directive) {
            skipped.fetch_add(1, Ordering::Relaxed);
            pb.inc(1);
            reporter.directive_completed();
            continue;
        }

        let bsa_name = std::path::Path::new(&directive.to)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| directive.to.clone());

        pb.set_message(format!("{} ({} files) [LARGE]", bsa_name, directive.file_states.len()));

        match handlers::handle_create_bsa(ctx, &directive) {
            Ok(()) => {
                completed.fetch_add(1, Ordering::Relaxed);
                pb.println(format!("Created BSA: {}", bsa_name));
            }
            Err(e) => {
                failed.fetch_add(1, Ordering::Relaxed);
                pb.println(format!("FAIL [{}] create BSA {}: {:#}", id, bsa_name, e));
            }
        }
        pb.inc(1);
        reporter.directive_completed();
    }

    Ok(())
}

/// Process simple directive types
fn process_simple_directives(
    db: &ModlistDb,
    ctx: &ProcessContext,
    dtype: &str,
    pb: &ProgressBar,
    completed: &AtomicUsize,
    skipped: &AtomicUsize,
    failed: &AtomicUsize,
    reporter: &Arc<ProgressReporter>,
) -> Result<()> {
    let directives = db.get_all_pending_directives_of_type(dtype)?;
    if directives.is_empty() {
        return Ok(());
    }

    // Parse
    let parsed: Vec<(i64, Result<Directive, String>)> = directives
        .into_iter()
        .map(|(id, json)| {
            let result = serde_json::from_str(&json).map_err(|e| format!("Parse error: {}", e));
            (id, result)
        })
        .collect();

    // Process in parallel
    let results: Vec<(i64, Result<bool, String>)> = parsed
        .into_par_iter()
        .map(|(id, parsed_result)| {
            let result = match parsed_result {
                Err(e) => Err(e),
                Ok(directive) => {
                    process_single_directive(ctx, &directive).map_err(|e| format!("{:#}", e))
                }
            };
            (id, result)
        })
        .collect();

    for (id, result) in results {
        match result {
            Ok(true) => {
                completed.fetch_add(1, Ordering::Relaxed);
            }
            Ok(false) => {
                skipped.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                failed.fetch_add(1, Ordering::Relaxed);
                if failed.load(Ordering::Relaxed) <= 10 {
                    pb.println(format!("FAIL [{}]: {}", id, e));
                }
            }
        }
        pb.inc(1);
        reporter.directive_completed();
    }

    Ok(())
}

/// Process a single directive - returns true if written, false if skipped
fn process_single_directive(ctx: &ProcessContext, directive: &Directive) -> Result<bool> {
    match directive {
        Directive::FromArchive(d) => {
            if output_exists(ctx, &d.to, d.size) {
                return Ok(false);
            }
            handlers::handle_from_archive(ctx, d)?;
            Ok(true)
        }
        Directive::PatchedFromArchive(d) => {
            if output_exists(ctx, &d.to, d.size) {
                return Ok(false);
            }
            handlers::handle_patched_from_archive(ctx, d)?;
            Ok(true)
        }
        Directive::InlineFile(d) => {
            if output_exists(ctx, &d.to, d.size) {
                return Ok(false);
            }
            handlers::handle_inline_file(ctx, d)?;
            Ok(true)
        }
        Directive::RemappedInlineFile(d) => {
            handlers::handle_remapped_inline_file(ctx, d)?;
            Ok(true)
        }
        Directive::TransformedTexture(_) => {
            bail!("TransformedTexture not yet implemented")
        }
        Directive::CreateBSA(_) => {
            bail!("CreateBSA not yet implemented")
        }
    }
}
