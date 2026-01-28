//! Directive processor - TTW-style batch processing
//!
//! Architecture:
//! - All archives: Use 7z binary for reliable extraction
//! - BSA: Direct extraction via ba2 crate
//! - Nested BSA: Extract BSA from archive, then extract file from BSA
//! - Parallel processing using rayon

use crate::bsa::{self, BsaCache, list_files as list_bsa_files};
use crate::modlist::{ArchiveFileEntry, Directive, FromArchiveDirective, ModlistDb};
use crate::paths;
use std::io::BufRead;

use super::config::InstallConfig;
use super::handlers;

use anyhow::{bail, Context, Result};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, Write};
use zip::ZipArchive;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

/// Get path to bundled 7zzs binary
fn get_7z_path() -> Result<PathBuf> {
    // Look next to executable
    let exe_path = std::env::current_exe().context("Failed to get executable path")?;
    let exe_dir = exe_path.parent().unwrap_or(Path::new("."));

    // Try various locations
    let candidates = [
        exe_dir.join("7zzs"),
        exe_dir.join("bin/7zzs"),
        PathBuf::from("bin/7zzs"),
        PathBuf::from("7zzs"),
    ];

    for path in &candidates {
        if path.exists() {
            return Ok(path.clone());
        }
    }

    // Try system PATH as fallback
    if let Ok(output) = Command::new("which").arg("7z").output() {
        if output.status.success() {
            let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
            return Ok(PathBuf::from(path));
        }
    }

    bail!("7zzs binary not found. Please place it next to the clf3 executable.")
}

/// Extract a single file from an archive using 7z (extracts whole archive to temp, finds file)
fn extract_with_7z(
    sevenz_path: &Path,
    archive_path: &Path,
    file_path: &str,
    temp_base_dir: &Path,
) -> Result<Vec<u8>> {
    // Create temp dir for extraction
    let temp_dir = tempfile::tempdir_in(temp_base_dir)
        .context("Failed to create temp directory for 7z extraction")?;

    // Extract entire archive
    let output = Command::new(sevenz_path)
        .arg("x")
        .arg("-y")
        .arg(format!("-o{}", temp_dir.path().display()))
        .arg(archive_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .with_context(|| format!("Failed to run 7z on {}", archive_path.display()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("7z extraction failed: {}", stderr.lines().next().unwrap_or("unknown error"));
    }

    // Find the file case-insensitively
    let target_normalized = paths::normalize_for_lookup(file_path);

    for entry in walkdir::WalkDir::new(temp_dir.path())
        .into_iter()
        .filter_map(|e| e.ok())
    {
        if entry.file_type().is_file() {
            if let Ok(rel_path) = entry.path().strip_prefix(temp_dir.path()) {
                let normalized = paths::normalize_for_lookup(&rel_path.to_string_lossy());
                if normalized == target_normalized {
                    return fs::read(entry.path())
                        .with_context(|| format!("Failed to read extracted file: {}", entry.path().display()));
                }
            }
        }
    }

    bail!("File '{}' not found in archive '{}'", file_path, archive_path.display())
}

/// Extract entire archive to temp dir, return map of normalized_path -> data for requested files
fn extract_batch_with_7z(
    sevenz_path: &Path,
    archive_path: &Path,
    file_paths: &[&str],
    temp_dir: &Path,
) -> Result<HashMap<String, Vec<u8>>> {
    if file_paths.is_empty() {
        return Ok(HashMap::new());
    }

    // Extract entire archive - simpler and avoids path matching issues
    let output = Command::new(sevenz_path)
        .arg("x")                    // Extract with full paths
        .arg("-y")                   // Yes to all
        .arg(format!("-o{}", temp_dir.display()))
        .arg(archive_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .with_context(|| format!("Failed to run 7z on {}", archive_path.display()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("7z extraction failed: {}", stderr.lines().next().unwrap_or("unknown error"));
    }

    // Build set of normalized paths we need
    let needed: std::collections::HashSet<String> = file_paths
        .iter()
        .map(|p| paths::normalize_for_lookup(p))
        .collect();

    // Walk temp dir and collect files we need
    let mut results = HashMap::new();
    collect_needed_files(temp_dir, temp_dir, &needed, &mut results)?;

    Ok(results)
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

/// List all files in an archive using 7z
fn list_archive_files(sevenz_path: &Path, archive_path: &Path) -> Result<Vec<ArchiveFileEntry>> {
    let output = Command::new(sevenz_path)
        .arg("l")           // List
        .arg("-slt")        // Technical listing (one field per line)
        .arg("-ba")         // Bare output (no headers)
        .arg(archive_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .with_context(|| format!("Failed to list archive: {}", archive_path.display()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("7z list failed: {}", stderr.lines().next().unwrap_or("unknown error"));
    }

    let mut files = Vec::new();
    let mut current_path: Option<String> = None;
    let mut current_size: u64 = 0;
    let mut is_folder = false;

    for line in output.stdout.lines() {
        let line = line?;
        if let Some(path) = line.strip_prefix("Path = ") {
            // Save previous entry if it was a file
            if let Some(prev_path) = current_path.take() {
                if !is_folder {
                    files.push(ArchiveFileEntry {
                        file_path: prev_path,
                        file_size: current_size,
                    });
                }
            }
            current_path = Some(path.to_string());
            current_size = 0;
            is_folder = false;
        } else if let Some(size) = line.strip_prefix("Size = ") {
            current_size = size.parse().unwrap_or(0);
        } else if let Some(folder) = line.strip_prefix("Folder = ") {
            is_folder = folder.trim() == "+";
        }
    }

    // Don't forget last entry
    if let Some(path) = current_path {
        if !is_folder {
            files.push(ArchiveFileEntry {
                file_path: path,
                file_size: current_size,
            });
        }
    }

    Ok(files)
}

/// Index all archives that need indexing
fn index_archives(db: &ModlistDb, ctx: &ProcessContext) -> Result<()> {
    let sevenz_path = get_7z_path()?;

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

        // Detect archive type to use appropriate listing method
        let archive_type = detect_archive_type(&archive_path);

        // Skip non-archive files (game files like ESM, ESL, INI, BIK, etc.)
        let is_indexable = matches!(archive_type.as_str(), "zip" | "7z" | "rar" | "bsa");
        if !is_indexable {
            // Mark as indexed with empty file list (it's a single file, not a container)
            let _ = db.index_archive_files(&archive.hash, &[]);
            pb.inc(1);
            continue;
        }

        let list_result = if archive_type == "bsa" {
            // Use BSA-specific listing
            list_bsa_files(&archive_path).map(|entries| {
                entries
                    .into_iter()
                    .map(|e| ArchiveFileEntry {
                        file_path: e.path,
                        file_size: e.size,
                    })
                    .collect()
            })
        } else {
            // Use 7z for ZIP/7z/RAR archives
            list_archive_files(&sevenz_path, &archive_path)
        };

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
        let cache_path = config.downloads_dir.join(".clf3_extraction_cache.db");
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

/// Detect archive type by magic bytes (more reliable than extension)
fn detect_archive_type(path: &Path) -> String {
    if let Ok(mut file) = File::open(path) {
        let mut magic = [0u8; 8];
        if std::io::Read::read(&mut file, &mut magic).is_ok() {
            // Check magic bytes
            if &magic[0..4] == b"PK\x03\x04" || &magic[0..4] == b"PK\x05\x06" {
                return "zip".to_string();
            }
            if &magic[0..6] == b"7z\xBC\xAF\x27\x1C" {
                return "7z".to_string();
            }
            if &magic[0..4] == b"Rar!" {
                return "rar".to_string();
            }
            if &magic[0..4] == b"BSA\x00" || &magic[0..4] == b"BTDX" {
                return "bsa".to_string();
            }
        }
    }
    // Fall back to extension
    path.extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase()
}


/// Check if a file path looks like a BSA/BA2 archive
fn is_bsa_file(path: &str) -> bool {
    let lower = path.to_lowercase();
    lower.ends_with(".bsa") || lower.ends_with(".ba2")
}

/// Get the working folder for BSA files extracted from archives
fn get_bsa_working_dir(downloads_dir: &Path) -> PathBuf {
    downloads_dir.join("Working_BSA_Because_Its_Too_Big_For_SQL")
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

    // Process FromArchive - grouped by archive for efficiency
    pb.set_message("Processing FromArchive...");
    process_from_archive_fast(db, &ctx, &pb, &completed, &skipped, &failed)?;

    // Process InlineFile
    pb.set_message("Processing InlineFile...");
    process_simple_directives(db, &ctx, "InlineFile", &pb, &completed, &skipped, &failed)?;

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
    )?;

    // PatchedFromArchive - with archive-level progress
    pb.set_message("Processing PatchedFromArchive...");
    process_patched_from_archive(db, &ctx, &pb, &completed, &skipped, &failed)?;

    // TransformedTexture - with archive-level progress
    pb.set_message("Processing TransformedTexture...");
    process_transformed_texture(db, &ctx, &pb, &completed, &skipped, &failed)?;

    // CreateBSA - must run LAST as it depends on staged files from other directives
    pb.set_message("Processing CreateBSA...");
    process_create_bsa(db, &ctx, &pb, &completed, &skipped, &failed)?;

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

    // Get 7z binary path
    let sevenz_path = get_7z_path()?;
    pb.println(format!("Using 7z: {}", sevenz_path.display()));

    // Separate BSA archives from others (BSA uses ba2 crate, everything else uses 7z)
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

        // Only separate BSA/BA2 files - everything else goes through 7z
        let archive_type = detect_archive_type(&archive_path);
        match archive_type.as_str() {
            "bsa" | "ba2" => bsa_archives.push((archive_hash, archive_path, directives)),
            _ => other_archives.push((archive_hash, archive_path, directives)),
        }
    }

    let num_threads = rayon::current_num_threads();

    // Phase 1: Process regular archives in parallel using 7z
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
        let sevenz_path = Arc::new(sevenz_path);
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

                // Batch extract
                let extracted = match extract_batch_with_7z(&sevenz_path, archive_path, &paths, temp_dir.path()) {
                    Ok(data) => data,
                    Err(e) => {
                        // Log the error and fall back to per-file extraction
                        overall_pb.println(format!("BATCH FAIL {}: {}", archive_name, e));
                        archive_pb.set_message(format!("{} (fallback mode)", display_name));
                        let mut fallback_data = HashMap::new();
                        for (_, _, path) in &to_extract {
                            if let Ok(data) = extract_with_7z(&sevenz_path, archive_path, path, &ctx.config.downloads_dir) {
                                let key = paths::normalize_for_lookup(path);
                                fallback_data.insert(key, data);
                            }
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
                        let data = extract_with_7z(&sevenz_path, archive_path, bsa_path, &ctx.config.downloads_dir)
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

/// Process PatchedFromArchive directives with progress display
fn process_patched_from_archive(
    db: &ModlistDb,
    ctx: &ProcessContext,
    pb: &ProgressBar,
    completed: &AtomicUsize,
    skipped: &AtomicUsize,
    failed: &AtomicUsize,
) -> Result<()> {
    use crate::modlist::PatchedFromArchiveDirective;

    let failure_tracker = Arc::new(FailureTracker::new());

    pb.set_message("Loading PatchedFromArchive directives...");
    let all_raw = db.get_all_pending_directives_of_type("PatchedFromArchive")?;

    if all_raw.is_empty() {
        pb.println("No PatchedFromArchive directives to process");
        return Ok(());
    }

    pb.set_message(format!("Parsing {} PatchedFromArchive directives...", all_raw.len()));

    // Parse and group by archive
    let mut by_archive: HashMap<String, Vec<(i64, PatchedFromArchiveDirective)>> = HashMap::new();
    let mut parse_failures = 0;

    for (id, json) in all_raw {
        match serde_json::from_str::<Directive>(&json) {
            Ok(Directive::PatchedFromArchive(d)) => {
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
        pb.println(format!("WARN: {} PatchedFromArchive directives failed to parse", parse_failures));
    }

    let total_archives = by_archive.len();
    let total_directives: usize = by_archive.values().map(|v| v.len()).sum();
    pb.println(format!("Processing {} patches across {} archives...", total_directives, total_archives));

    let failure_tracker = failure_tracker.clone();
    let archives_processed = AtomicUsize::new(0);

    // Process archives in parallel
    let archives_vec: Vec<_> = by_archive.into_iter().collect();

    let sevenz_path = get_7z_path().ok();

    archives_vec.par_iter().for_each(|(archive_hash, directives)| {
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
                }
                if any_needed {
                    pb.println(format!("WARN: Archive not found for patching: {}", archive_hash));
                }
                archives_processed.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };

        // Filter to directives that need processing
        let to_process: Vec<_> = directives
            .iter()
            .filter(|(_, d)| !output_exists(ctx, &d.to, d.size))
            .collect();

        // Count skipped
        let skip_count = directives.len() - to_process.len();
        if skip_count > 0 {
            skipped.fetch_add(skip_count, Ordering::Relaxed);
            pb.inc(skip_count as u64);
        }

        if to_process.is_empty() {
            archives_processed.fetch_add(1, Ordering::Relaxed);
            return;
        }

        // Collect unique source paths needed from this archive
        // Simple paths (len == 2): file directly in archive
        // Nested BSA (len >= 3): BSA in archive, file in BSA
        let mut simple_paths: HashMap<String, String> = HashMap::new(); // normalized -> original
        let mut nested_bsas: HashMap<String, Vec<String>> = HashMap::new(); // bsa_path -> [files_in_bsa]

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
            let archive_type = detect_archive_type(&archive_path);
            let batch_result = match archive_type.as_str() {
                "zip" => {
                    // ZIP: extract needed files directly (random access)
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
                "bsa" => {
                    // BSA: extract needed files directly
                    let mut result = HashMap::new();
                    for (normalized, original) in &simple_paths {
                        if let Ok(data) = bsa::extract_file(&archive_path, original) {
                            result.insert(normalized.clone(), data);
                        }
                    }
                    result
                }
                _ => {
                    // 7z/RAR/other: batch extract entire archive to temp, then read needed files
                    if let Some(ref sevenz) = sevenz_path {
                        if let Ok(temp_dir) = tempfile::tempdir_in(&ctx.config.downloads_dir) {
                            let paths_vec: Vec<&str> = simple_paths.values().map(|s| s.as_str()).collect();
                            extract_batch_with_7z(sevenz, &archive_path, &paths_vec, temp_dir.path())
                                .unwrap_or_default()
                        } else {
                            HashMap::new()
                        }
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

        let archive_name = archive_path.file_name().unwrap_or_default().to_string_lossy().to_string();

        // Process each patch using the extracted data
        for (id, directive) in to_process {
            let result: Result<()> = (|| {
                // Get source data from extracted cache
                let source_data = if directive.archive_hash_path.len() == 1 {
                    // Whole file is the source
                    fs::read(&archive_path)?
                } else if directive.archive_hash_path.len() == 2 {
                    // Simple: file directly in archive
                    let path = &directive.archive_hash_path[1];
                    let normalized = paths::normalize_for_lookup(path);
                    extracted.get(&normalized)
                        .cloned()
                        .ok_or_else(|| anyhow::anyhow!("Source file not found in extracted cache: {}", path))?
                } else if directive.archive_hash_path.len() >= 3 {
                    // Nested BSA: file inside BSA inside archive
                    let bsa_path = &directive.archive_hash_path[1];
                    let file_in_bsa = &directive.archive_hash_path[2];
                    let key = format!("{}/{}", bsa_path, file_in_bsa);
                    let normalized = paths::normalize_for_lookup(&key);
                    extracted.get(&normalized)
                        .cloned()
                        .ok_or_else(|| anyhow::anyhow!("Source file not found in BSA cache: {} / {}", bsa_path, file_in_bsa))?
                } else {
                    anyhow::bail!("Invalid archive_hash_path");
                };

                // Load delta patch from wabbajack
                let patch_name = directive.patch_id.to_string();
                let delta_data = ctx.read_wabbajack_file(&patch_name)?;

                // Apply delta patch
                use crate::octodiff::DeltaReader;
                use std::io::Cursor;
                let basis = Cursor::new(source_data);
                let delta = Cursor::new(delta_data);
                let mut reader = DeltaReader::new(basis, delta)?;
                let mut patched_data = Vec::with_capacity(directive.size as usize);
                std::io::Read::read_to_end(&mut reader, &mut patched_data)?;

                // Verify size
                if patched_data.len() as u64 != directive.size {
                    anyhow::bail!("Size mismatch: expected {}, got {}", directive.size, patched_data.len());
                }

                // Write output
                let output_path = ctx.resolve_output_path(&directive.to);
                paths::ensure_parent_dirs(&output_path)?;
                let mut file = File::create(&output_path)?;
                file.write_all(&patched_data)?;

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
                        pb.println(format!("FAIL [{}] patch: {:#}", id, e));
                    }
                }
            }
            pb.inc(1);
        }

        archives_processed.fetch_add(1, Ordering::Relaxed);
    });

    // Print failure summary
    failure_tracker.print_summary(pb);

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
    pb.println(format!("Processing {} textures across {} archives...", total_directives, total_archives));

    let failure_tracker = failure_tracker.clone();
    let sevenz_path = get_7z_path().ok();

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

        // Count skipped
        let skip_count = directives.len() - to_process.len();
        if skip_count > 0 {
            skipped.fetch_add(skip_count, Ordering::Relaxed);
            pb.inc(skip_count as u64);
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
            let archive_type = detect_archive_type(&archive_path);
            let batch_result = match archive_type.as_str() {
                "zip" => {
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
                "bsa" => {
                    let mut result = HashMap::new();
                    for (normalized, original) in &simple_paths {
                        if let Ok(data) = bsa::extract_file(&archive_path, original) {
                            result.insert(normalized.clone(), data);
                        }
                    }
                    result
                }
                _ => {
                    // 7z/RAR/other: batch extract
                    if let Some(ref sevenz) = sevenz_path {
                        if let Ok(temp_dir) = tempfile::tempdir_in(&ctx.config.downloads_dir) {
                            let paths_vec: Vec<&str> = simple_paths.values().map(|s| s.as_str()).collect();
                            extract_batch_with_7z(sevenz, &archive_path, &paths_vec, temp_dir.path())
                                .unwrap_or_default()
                        } else {
                            HashMap::new()
                        }
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
) -> Result<()> {
    use crate::modlist::CreateBSADirective;

    pb.set_message("Loading CreateBSA directives...");
    let all_raw = db.get_all_pending_directives_of_type("CreateBSA")?;

    if all_raw.is_empty() {
        return Ok(());
    }

    pb.println(format!("Building {} BSA archives...", all_raw.len()));

    // Parse all directives
    let directives: Vec<(i64, CreateBSADirective)> = all_raw
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

    // Process each BSA (not parallelized - they may share staging dirs)
    for (id, directive) in directives {
        // Check if output already exists and is valid
        if handlers::output_bsa_valid(ctx, &directive) {
            skipped.fetch_add(1, Ordering::Relaxed);
            pb.inc(1);
            continue;
        }

        let bsa_name = std::path::Path::new(&directive.to)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| directive.to.clone());

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
