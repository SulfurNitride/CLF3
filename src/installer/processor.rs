//! Directive processor - TTW-style batch processing
//!
//! Architecture:
//! - ZIP/7z/RAR: Extraction using 7z binary via crate::archive::sevenzip
//! - ZIP: zip crate is used for reading .wabbajack files (they are ZIP archives)
//! - BSA/BA2: Direct extraction via ba2 crate
//! - Nested BSA: Extract BSA from archive, then extract file from BSA
//! - Parallel processing using rayon

use crate::bsa::{self, list_files as list_bsa_files, BsaCache};
use crate::modlist::{ArchiveFileEntry, Directive, ModlistDb};
use crate::paths;

use super::config::InstallConfig;
use super::extract_strategy::should_use_selective_extraction;
use super::handlers;
use super::handlers::from_archive::{
    detect_archive_type, extract_from_archive_with_temp, ArchiveType,
};

use anyhow::{bail, Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use rusqlite::params;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tracing::{debug, warn};
use zip::ZipArchive;

#[derive(Debug, Clone)]
struct PatchBasisRecord {
    local_path: PathBuf,
    size: u64,
    quick_hash: u64,
}

/// Build a stable key for patch basis lookup:
/// `archive_hash|normalized_path_in_archive|normalized_nested_path`.
pub(crate) fn build_patch_basis_key(
    archive_hash: &str,
    path_in_archive: Option<&str>,
    nested_path: Option<&str>,
) -> String {
    let mut key = archive_hash.to_string();
    if let Some(path) = path_in_archive {
        key.push('|');
        key.push_str(&paths::normalize_for_lookup(path));
    }
    if let Some(path) = nested_path {
        key.push('|');
        key.push_str(&paths::normalize_for_lookup(path));
    }
    key
}

pub(crate) fn build_patch_basis_key_from_archive_hash_path(parts: &[String]) -> Option<String> {
    let archive_hash = parts.first()?;
    let path_in_archive = parts.get(1).map(|s| s.as_str());
    let nested_path = parts.get(2).map(|s| s.as_str());
    Some(build_patch_basis_key(
        archive_hash,
        path_in_archive,
        nested_path,
    ))
}

fn quick_file_hash(path: &Path) -> Result<u64> {
    use std::io::{Read, Seek, SeekFrom};

    let mut file = File::open(path)
        .with_context(|| format!("Failed to open file for quick hash: {}", path.display()))?;
    let len = file.metadata()?.len();
    const SAMPLE: usize = 64 * 1024;
    let mut hasher = xxhash_rust::xxh64::Xxh64::new(0);

    let mut head = vec![0u8; SAMPLE.min(len as usize)];
    if !head.is_empty() {
        file.read_exact(&mut head)?;
        hasher.update(&head);
    }

    if len > SAMPLE as u64 {
        let tail_len = SAMPLE.min(len as usize - head.len());
        if tail_len > 0 {
            file.seek(SeekFrom::End(-(tail_len as i64)))?;
            let mut tail = vec![0u8; tail_len];
            file.read_exact(&mut tail)?;
            hasher.update(&tail);
        }
    }

    Ok(hasher.digest())
}

fn quick_hash_bytes(data: &[u8]) -> u64 {
    const SAMPLE: usize = 64 * 1024;
    let mut hasher = xxhash_rust::xxh64::Xxh64::new(0);
    let head_len = data.len().min(SAMPLE);
    hasher.update(&data[..head_len]);
    if data.len() > SAMPLE {
        let tail_len = (data.len() - head_len).min(SAMPLE);
        hasher.update(&data[data.len() - tail_len..]);
    }
    hasher.digest()
}

fn should_preload_patch_blobs() -> bool {
    static SHOULD_PRELOAD: OnceLock<bool> = OnceLock::new();
    *SHOULD_PRELOAD.get_or_init(|| {
        if let Ok(v) = std::env::var("CLF3_PRELOAD_PATCH_BLOBS") {
            let value = v.trim().to_ascii_lowercase();
            return matches!(value.as_str(), "1" | "true" | "yes" | "on");
        }

        // Default ON: preload delta blobs per-archive so parallel apply doesn't
        // serialize on the shared wabbajack ZIP mutex.
        true
    })
}

/// List files in an archive using pure Rust crates
fn list_archive_files_rust(archive_path: &Path) -> Result<Vec<ArchiveFileEntry>> {
    let archive_type = detect_archive_type(archive_path)?;

    match archive_type {
        ArchiveType::Zip => list_zip_files(archive_path),
        ArchiveType::SevenZ => list_7z_files(archive_path),
        ArchiveType::Rar => list_rar_files(archive_path),
        ArchiveType::Tes3Bsa | ArchiveType::Bsa | ArchiveType::Ba2 => list_bsa_files(archive_path)
            .map(|entries| {
                entries
                    .into_iter()
                    .map(|e| ArchiveFileEntry {
                        file_path: e.path,
                        file_size: e.size,
                    })
                    .collect()
            }),
        ArchiveType::Unknown => list_zip_files(archive_path)
            .or_else(|_| list_7z_files(archive_path))
            .or_else(|_| list_rar_files(archive_path))
            .with_context(|| format!("Failed to list archive: {}", archive_path.display())),
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
    let temp_dir =
        tempfile::tempdir_in(temp_base_dir).context("Failed to create temp directory")?;

    // Extract entire archive based on type
    match archive_type {
        ArchiveType::Zip => extract_zip_to_temp(archive_path, temp_dir.path())?,
        ArchiveType::SevenZ => extract_7z_to_temp(archive_path, temp_dir.path())?,
        ArchiveType::Rar => extract_rar_to_temp(archive_path, temp_dir.path())?,
        ArchiveType::Tes3Bsa | ArchiveType::Bsa | ArchiveType::Ba2 => {
            bail!("BSA/BA2 should not use batch extraction")
        }
        ArchiveType::Unknown => extract_zip_to_temp(archive_path, temp_dir.path())
            .or_else(|_| extract_7z_to_temp(archive_path, temp_dir.path()))
            .or_else(|_| extract_rar_to_temp(archive_path, temp_dir.path()))
            .with_context(|| format!("Failed to extract: {}", archive_path.display()))?,
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

                    // Log BSA/BA2 files we find for debugging
                    if normalized.ends_with(".bsa") || normalized.ends_with(".ba2") {
                        if needed.contains(&normalized) {
                            tracing::warn!(
                                "[DEBUG] Found and MATCHED BSA/BA2: '{}' -> '{}'",
                                rel_str,
                                normalized
                            );
                        } else {
                            // Check if there's a similar path in needed set
                            let similar: Vec<_> = needed
                                .iter()
                                .filter(|n| n.contains("textures.ba2") || n.contains("main.ba2"))
                                .take(3)
                                .collect();
                            tracing::warn!("[DEBUG] Found BSA/BA2 NOT in needed: '{}' -> '{}', similar in needed: {:?}",
                                rel_str, normalized, similar);
                        }
                    }

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
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} | {msg}",
            )
            .unwrap()
            .progress_chars("=>-"),
    );
    pb.enable_steady_tick(Duration::from_millis(100));

    for archive in to_index {
        // Determine archive location based on type
        let archive_path = match &archive.local_path {
            Some(p) => PathBuf::from(p),
            None => {
                // Check if this is a GameFileSource archive (from game installation)
                if archive.state_json.contains("GameFileSourceDownloader") {
                    // Parse the state to get the game file path
                    if let Ok(state) =
                        serde_json::from_str::<crate::modlist::DownloadState>(&archive.state_json)
                    {
                        if let crate::modlist::DownloadState::GameFileSource(gf) = state {
                            // Look in game directory with case-insensitive path resolution
                            let game_file = &gf.game_file;
                            if let Some(resolved) = crate::paths::resolve_case_insensitive(
                                &ctx.config.game_dir,
                                game_file,
                            ) {
                                resolved
                            } else if let Some(resolved) = crate::paths::resolve_case_insensitive(
                                &ctx.config.game_dir,
                                &format!("Data/{}", game_file),
                            ) {
                                resolved
                            } else {
                                // Fallback to downloads dir (will fail, but with proper error)
                                ctx.config.downloads_dir.join(&archive.name)
                            }
                        } else {
                            ctx.config.downloads_dir.join(&archive.name)
                        }
                    } else {
                        ctx.config.downloads_dir.join(&archive.name)
                    }
                } else {
                    ctx.config.downloads_dir.join(&archive.name)
                }
            }
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
            ArchiveType::Zip
                | ArchiveType::SevenZ
                | ArchiveType::Rar
                | ArchiveType::Tes3Bsa
                | ArchiveType::Bsa
                | ArchiveType::Ba2
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
    processed_count: AtomicUsize,
    /// Current phase's total (for accurate per-phase progress reporting)
    phase_total: AtomicUsize,
    /// Current phase's processed count (reset per phase)
    phase_count: AtomicUsize,
}

impl ProgressReporter {
    pub fn new(callback: Option<super::ProgressCallback>, total_directives: usize) -> Self {
        Self {
            callback,
            processed_count: AtomicUsize::new(0),
            phase_total: AtomicUsize::new(total_directives),
            phase_count: AtomicUsize::new(0),
        }
    }

    /// Report that a directive phase is starting
    pub fn phase_started(&self, directive_type: &str, count: usize) {
        // Set phase-specific totals and reset phase counter
        self.phase_total.store(count, Ordering::Relaxed);
        self.phase_count.store(0, Ordering::Relaxed);
        if let Some(ref callback) = self.callback {
            callback(super::ProgressEvent::DirectivePhaseStarted {
                directive_type: directive_type.to_string(),
                total: count,
            });
        }
    }

    /// Report that a directive completed (increments internal counter)
    pub fn directive_completed(&self) {
        self.processed_count.fetch_add(1, Ordering::Relaxed);
        let phase_current = self.phase_count.fetch_add(1, Ordering::Relaxed) + 1;
        let phase_total = self.phase_total.load(Ordering::Relaxed);
        if let Some(ref callback) = self.callback {
            callback(super::ProgressEvent::DirectiveComplete {
                index: phase_current,
                total: phase_total,
            });
        }
    }

    /// Report progress with a specific count (for batch updates within current phase)
    pub fn report_count(&self, index: usize) {
        let phase_total = self.phase_total.load(Ordering::Relaxed);
        if let Some(ref callback) = self.callback {
            callback(super::ProgressEvent::DirectiveComplete {
                index,
                total: phase_total,
            });
        }
    }

    /// Set the processed count (for syncing with atomic counters)
    pub fn set_count(&self, count: usize) {
        self.processed_count.store(count, Ordering::Relaxed);
    }

    /// Get a clone of the callback (for passing to other threads)
    pub fn get_callback(&self) -> Option<super::ProgressCallback> {
        self.callback.clone()
    }

    /// Get the current phase's total count
    pub fn get_phase_total(&self) -> usize {
        self.phase_total.load(Ordering::Relaxed)
    }
}

/// Persistent SQLite-backed store for patch basis entries.
///
/// Stores `basis_key -> (local_path, size, quick_hash)` scoped per modlist.
/// Lives in the same `~/.cache/clf3/extraction_cache.db` used by `BsaCache`.
pub(crate) struct PatchBasisStore {
    conn: Mutex<rusqlite::Connection>,
    modlist_name: String,
}

impl PatchBasisStore {
    /// Open (or create) the patch_basis table in the given database.
    fn open(db_path: &Path, modlist_name: &str) -> Result<Self> {
        let conn = rusqlite::Connection::open(db_path)
            .with_context(|| format!("Failed to open patch basis DB at {}", db_path.display()))?;

        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA cache_size = 500;",
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS patch_basis (
                modlist_name TEXT NOT NULL,
                basis_key TEXT NOT NULL,
                local_path TEXT NOT NULL,
                size INTEGER NOT NULL,
                quick_hash INTEGER NOT NULL,
                PRIMARY KEY (modlist_name, basis_key)
            )",
            [],
        )
        .context("Failed to create patch_basis table")?;

        Ok(Self {
            conn: Mutex::new(conn),
            modlist_name: modlist_name.to_string(),
        })
    }

    /// Load all entries for this modlist, verify each (file exists + size + quick_hash),
    /// and return verified records. Stale entries are deleted from the DB.
    fn load_verified(&self) -> Result<HashMap<String, PatchBasisRecord>> {
        let conn = self.conn.lock().expect("patch basis DB lock poisoned");
        let mut stmt = conn.prepare(
            "SELECT basis_key, local_path, size, quick_hash FROM patch_basis WHERE modlist_name = ?1",
        )?;

        let rows: Vec<(String, String, i64, i64)> = stmt
            .query_map(params![&self.modlist_name], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })?
            .filter_map(|r| r.ok())
            .collect();

        let mut verified = HashMap::new();
        let mut stale_keys = Vec::new();

        for (key, local_path_str, size, quick_hash) in &rows {
            let local_path = PathBuf::from(local_path_str);
            let expected_size = *size as u64;
            let expected_hash = *quick_hash as u64;

            // Verify file still exists with matching size
            let meta = match fs::metadata(&local_path) {
                Ok(m) => m,
                Err(_) => {
                    stale_keys.push(key.clone());
                    continue;
                }
            };

            if meta.len() != expected_size {
                stale_keys.push(key.clone());
                continue;
            }

            // Verify quick hash
            match quick_file_hash(&local_path) {
                Ok(h) if h == expected_hash => {}
                _ => {
                    stale_keys.push(key.clone());
                    continue;
                }
            }

            verified.insert(
                key.clone(),
                PatchBasisRecord {
                    local_path,
                    size: expected_size,
                    quick_hash: expected_hash,
                },
            );
        }

        // Delete stale entries
        if !stale_keys.is_empty() {
            let mut del = conn.prepare(
                "DELETE FROM patch_basis WHERE modlist_name = ?1 AND basis_key = ?2",
            )?;
            for key in &stale_keys {
                let _ = del.execute(params![&self.modlist_name, key]);
            }
            debug!(
                "Patch basis: pruned {} stale entries, {} verified",
                stale_keys.len(),
                verified.len()
            );
        }

        Ok(verified)
    }

    /// Insert or update a single entry.
    fn upsert(&self, key: &str, record: &PatchBasisRecord) {
        let conn = self.conn.lock().expect("patch basis DB lock poisoned");
        let _ = conn.execute(
            "INSERT OR REPLACE INTO patch_basis (modlist_name, basis_key, local_path, size, quick_hash)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                &self.modlist_name,
                key,
                &*record.local_path.to_string_lossy(),
                record.size as i64,
                record.quick_hash as i64,
            ],
        );
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
    /// Patch basis keys that are actually needed for this run.
    needed_patch_basis_keys: RwLock<HashSet<String>>,
    /// Basis key -> local output path + quick verification metadata.
    patch_basis_db: RwLock<HashMap<String, PatchBasisRecord>>,
    /// Cache of expensive full file hashes by `path|size|mtime`.
    patch_basis_full_hash_cache: Mutex<HashMap<String, String>>,
    /// Persistent SQLite store for patch basis entries (scoped per modlist).
    patch_basis_store: Option<PatchBasisStore>,
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

        // Open persistent patch basis store and load verified entries
        let modlist_name = config
            .wabbajack_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string();
        let patch_basis_db_path = cache_dir.join("extraction_cache.db");
        let (patch_basis_store, preloaded_basis) =
            match PatchBasisStore::open(&patch_basis_db_path, &modlist_name) {
                Ok(store) => {
                    let entries = store.load_verified().unwrap_or_default();
                    if !entries.is_empty() {
                        println!(
                            "Loaded {} verified patch basis entries from cache",
                            entries.len()
                        );
                    }
                    (Some(store), entries)
                }
                Err(e) => {
                    warn!("Failed to open patch basis store: {}", e);
                    (None, HashMap::new())
                }
            };

        Ok(Self {
            config,
            wabbajack: Mutex::new(wabbajack),
            archive_paths,
            extraction_cache,
            existing_files,
            needed_patch_basis_keys: RwLock::new(HashSet::new()),
            patch_basis_db: RwLock::new(preloaded_basis),
            patch_basis_full_hash_cache: Mutex::new(HashMap::new()),
            patch_basis_store,
        })
    }

    pub fn get_archive_path(&self, hash: &str) -> Option<&PathBuf> {
        self.archive_paths.get(hash)
    }

    pub fn resolve_output_path(&self, to_path: &str) -> PathBuf {
        paths::join_windows_path(&self.config.output_dir, to_path)
    }

    pub fn read_wabbajack_file(&self, name: &str) -> Result<Vec<u8>> {
        let mut archive = self
            .wabbajack
            .lock()
            .expect("wabbajack archive lock poisoned");
        let mut file = archive
            .by_name(name)
            .with_context(|| format!("File '{}' not found in wabbajack", name))?;
        let mut data = Vec::new();
        std::io::Read::read_to_end(&mut file, &mut data)?;
        Ok(data)
    }

    pub fn set_needed_patch_basis_keys(&self, keys: HashSet<String>) {
        let mut needed = self
            .needed_patch_basis_keys
            .write()
            .expect("needed_patch_basis_keys lock poisoned");
        *needed = keys;
    }

    pub fn needed_patch_basis_count(&self) -> usize {
        self.needed_patch_basis_keys
            .read()
            .expect("needed_patch_basis_keys lock poisoned")
            .len()
    }

    pub fn patch_basis_db_count(&self) -> usize {
        self.patch_basis_db
            .read()
            .expect("patch_basis_db lock poisoned")
            .len()
    }

    fn is_needed_patch_basis_key(&self, key: &str) -> bool {
        self.needed_patch_basis_keys
            .read()
            .expect("needed_patch_basis_keys lock poisoned")
            .contains(key)
    }

    /// Record a patch basis candidate under both the given key AND the raw
    /// directive `archive_hash_path` components.  This ensures Phase 5 lookup
    /// succeeds regardless of whether it resolves paths the same way Phase 3 did.
    pub fn record_patch_basis_candidate_path_dual(
        &self,
        key: &str,
        archive_hash_path: &[String],
        source_path: &Path,
        local_output_path: &Path,
        expected_size: u64,
    ) {
        self.record_patch_basis_candidate_path(key, source_path, local_output_path, expected_size);
        if let Some(raw_key) = build_patch_basis_key_from_archive_hash_path(archive_hash_path) {
            if raw_key != key {
                self.record_patch_basis_candidate_path(
                    &raw_key,
                    source_path,
                    local_output_path,
                    expected_size,
                );
            }
        }
    }

    /// Record a patch basis candidate (bytes variant) under both the given key
    /// AND the raw directive `archive_hash_path` components.
    pub fn record_patch_basis_candidate_bytes_dual(
        &self,
        key: &str,
        archive_hash_path: &[String],
        local_output_path: &Path,
        data: &[u8],
    ) {
        self.record_patch_basis_candidate_bytes(key, local_output_path, data);
        if let Some(raw_key) = build_patch_basis_key_from_archive_hash_path(archive_hash_path) {
            if raw_key != key {
                self.record_patch_basis_candidate_bytes(&raw_key, local_output_path, data);
            }
        }
    }

    pub fn record_patch_basis_candidate_path(
        &self,
        key: &str,
        source_path: &Path,
        local_output_path: &Path,
        expected_size: u64,
    ) {
        if !self.is_needed_patch_basis_key(key) {
            return;
        }

        let src_size = match fs::metadata(source_path).map(|m| m.len()) {
            Ok(size) => size,
            Err(_) => return,
        };
        if src_size != expected_size {
            return;
        }

        let quick_hash = match quick_file_hash(source_path) {
            Ok(v) => v,
            Err(_) => return,
        };

        let record = PatchBasisRecord {
            local_path: local_output_path.to_path_buf(),
            size: expected_size,
            quick_hash,
        };

        // Write-through to SQLite
        if let Some(ref store) = self.patch_basis_store {
            store.upsert(key, &record);
        }

        self.patch_basis_db
            .write()
            .expect("patch_basis_db lock poisoned")
            .insert(key.to_string(), record);
    }

    pub fn record_patch_basis_candidate_bytes(
        &self,
        key: &str,
        local_output_path: &Path,
        data: &[u8],
    ) {
        if !self.is_needed_patch_basis_key(key) {
            return;
        }

        let record = PatchBasisRecord {
            local_path: local_output_path.to_path_buf(),
            size: data.len() as u64,
            quick_hash: quick_hash_bytes(data),
        };

        // Write-through to SQLite
        if let Some(ref store) = self.patch_basis_store {
            store.upsert(key, &record);
        }

        self.patch_basis_db
            .write()
            .expect("patch_basis_db lock poisoned")
            .insert(key.to_string(), record);
    }

    pub fn resolve_verified_patch_basis_path(
        &self,
        key: &str,
        expected_full_hash: Option<&str>,
    ) -> Option<PathBuf> {
        let record = self
            .patch_basis_db
            .read()
            .expect("patch_basis_db lock poisoned")
            .get(key)
            .cloned()?;

        let meta = fs::metadata(&record.local_path).ok()?;
        if meta.len() != record.size {
            return None;
        }

        let quick_hash = quick_file_hash(&record.local_path).ok()?;
        if quick_hash != record.quick_hash {
            return None;
        }

        if let Some(expected_hash) = expected_full_hash.filter(|h| !h.is_empty()) {
            let modified_ns = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let cache_key = format!(
                "{}|{}|{}",
                record.local_path.display(),
                record.size,
                modified_ns
            );

            let mut cache = self
                .patch_basis_full_hash_cache
                .lock()
                .expect("patch_basis_full_hash_cache lock poisoned");
            let actual_hash = if let Some(v) = cache.get(&cache_key) {
                v.clone()
            } else {
                let computed = match crate::hash::compute_file_hash(&record.local_path) {
                    Ok(v) => v,
                    Err(_) => return None,
                };
                cache.insert(cache_key, computed.clone());
                computed
            };
            if actual_hash != expected_hash {
                return None;
            }
        }

        Some(record.local_path)
    }

    /// Get a file from the extraction cache (for pre-extracted 7z/RAR files)
    /// BSA files are stored in the working folder, regular files in SQLite
    pub fn get_cached_file(&self, archive_hash: &str, file_path: &str) -> Option<Vec<u8>> {
        // Check if this is a BSA file - if so, check working folder
        if is_bsa_file(file_path) {
            let bsa_working_path =
                get_bsa_working_path(&self.config.downloads_dir, archive_hash, file_path);
            if bsa_working_path.exists() {
                return fs::read(&bsa_working_path).ok();
            }
        }

        // Regular files are in SQLite cache
        let cache_key = Path::new(archive_hash);
        self.extraction_cache
            .get(cache_key, file_path)
            .ok()
            .flatten()
    }

    /// Get a BSA file from the working folder (for BSAs extracted from 7z/RAR)
    /// Returns the path to the BSA file on disk, if it exists
    pub fn get_cached_bsa_path(&self, archive_hash: &str, bsa_path: &str) -> Option<PathBuf> {
        let bsa_working_path =
            get_bsa_working_path(&self.config.downloads_dir, archive_hash, bsa_path);
        if bsa_working_path.exists() {
            Some(bsa_working_path)
        } else {
            None
        }
    }

    /// Get a nested BSA file from the extraction cache
    /// For files inside BSAs inside 7z/RAR archives
    pub fn get_cached_nested_bsa_file(
        &self,
        archive_hash: &str,
        bsa_path: &str,
        file_in_bsa: &str,
    ) -> Option<Vec<u8>> {
        let cache_key = Path::new(archive_hash);
        let combined_path = format!("{}/{}", bsa_path, file_in_bsa);
        self.extraction_cache
            .get(cache_key, &combined_path)
            .ok()
            .flatten()
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

    println!(
        "Need {} unique archives for pending directives",
        needed_hashes.len()
    );

    // Check which of those archives are missing
    let mut missing = Vec::new();
    let needed_list: Vec<_> = needed_hashes.into_iter().collect();
    let archives = db.get_archives_by_hashes(&needed_list)?;

    // Build a set of hashes we found in the DB
    let found_hashes: std::collections::HashSet<_> =
        archives.iter().map(|a| a.hash.clone()).collect();

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
/// Stays in downloads_dir since BSA files can be very large (gigabytes)
fn get_bsa_working_dir(downloads_dir: &Path) -> PathBuf {
    downloads_dir.join(".clf3_bsa_working")
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

/// Holds shared state for phased directive processing.
pub struct DirectiveProcessor<'a> {
    pub ctx: ProcessContext<'a>,
    pub db: &'a ModlistDb,
    pub reporter: Arc<ProgressReporter>,
    completed: AtomicUsize,
    skipped: AtomicUsize,
    failed: AtomicUsize,
    phase_failures: Mutex<Vec<(String, usize)>>,
}

impl<'a> DirectiveProcessor<'a> {
    /// Create a new directive processor, printing directive counts.
    pub fn new(db: &'a ModlistDb, config: &'a InstallConfig) -> Result<Self> {
        let ctx = ProcessContext::new(config, db)?;

        // Show directive counts
        let type_counts = db.get_directive_type_counts()?;
        println!("Directive types:");
        for (dtype, count) in &type_counts {
            println!("  {:>8}  {}", count, dtype);
        }
        println!();

        let stats = db.get_directive_stats()?;
        let reporter = Arc::new(ProgressReporter::new(
            config.progress_callback.clone(),
            stats.pending,
        ));

        Ok(Self {
            ctx,
            db,
            reporter,
            completed: AtomicUsize::new(0),
            skipped: AtomicUsize::new(0),
            failed: AtomicUsize::new(0),
            phase_failures: Mutex::new(Vec::new()),
        })
    }

    /// Get count of a directive type
    fn get_type_count(&self, name: &str) -> Result<usize> {
        let type_counts = self.db.get_directive_type_counts()?;
        Ok(type_counts
            .iter()
            .find(|(dtype, _)| dtype == name)
            .map(|(_, count)| *count)
            .unwrap_or(0))
    }

    /// Create a progress bar
    fn make_progress_bar(&self, total: u64) -> ProgressBar {
        let pb = ProgressBar::new(total);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) | {msg}",
                )
                .unwrap()
                .progress_chars("=>-"),
        );
        pb.enable_steady_tick(Duration::from_millis(100));
        pb
    }

    /// Record failures from a phase
    fn record_phase_failures(&self, phase: &str, count: usize) {
        if count > 0 {
            eprintln!("WARNING: {} phase had {} failures", phase, count);
            self.phase_failures
                .lock()
                .expect("phase_failures lock poisoned")
                .push((phase.to_string(), count));
        }
    }

    /// Pre-flight check: verify all archives needed for pending directives are present
    pub fn preflight_check(&self) -> Result<()> {
        let missing_archives = check_missing_archives(self.db, &self.ctx)?;
        if !missing_archives.is_empty() {
            println!(
                "ERROR: {} archives are missing! Re-run to download them:\n",
                missing_archives.len()
            );
            warn!("=== Missing Archives ({}) ===", missing_archives.len());
            for (name, hash) in missing_archives.iter().take(20) {
                println!("  - {} ({})", name, hash);
                warn!("[MISSING] {} ({})", name, hash);
            }
            if missing_archives.len() > 20 {
                println!("  ... and {} more", missing_archives.len() - 20);
                for (name, hash) in missing_archives.iter().skip(20) {
                    warn!("[MISSING] {} ({})", name, hash);
                }
                warn!("... total {} missing archives", missing_archives.len());
            }
            bail!("Missing archives - re-run the installer to download them");
        }
        println!("All needed archives present\n");
        Ok(())
    }

    /// Index archive contents
    pub fn index_archives(&self) -> Result<()> {
        index_archives(self.db, &self.ctx)
    }

    /// Build the set of patch basis keys needed later in phase 5.
    /// This lets phase 4 record reusable local basis paths while files are written.
    pub fn prepare_patch_basis_db(&self) -> Result<()> {
        let all_raw = self
            .db
            .get_all_pending_directives_of_type("PatchedFromArchive")?;
        if all_raw.is_empty() {
            self.ctx.set_needed_patch_basis_keys(HashSet::new());
            return Ok(());
        }

        let mut keys: HashSet<String> = HashSet::new();
        let mut parse_failures = 0usize;
        for (_id, json) in all_raw {
            match serde_json::from_str::<Directive>(&json) {
                Ok(Directive::PatchedFromArchive(d)) => {
                    let Some(archive_hash) = d.archive_hash_path.first() else {
                        continue;
                    };
                    let resolved_path = if d.archive_hash_path.len() >= 2 {
                        self.db
                            .lookup_archive_file(archive_hash, &d.archive_hash_path[1])?
                            .unwrap_or_else(|| d.archive_hash_path[1].clone())
                    } else {
                        String::new()
                    };
                    let key = if d.archive_hash_path.len() >= 2 {
                        build_patch_basis_key(
                            archive_hash,
                            Some(&resolved_path),
                            d.archive_hash_path.get(2).map(|s| s.as_str()),
                        )
                    } else {
                        build_patch_basis_key(archive_hash, None, None)
                    };
                    keys.insert(key);

                    // Keep a fallback key based on raw path from directive too.
                    if let Some(raw_key) =
                        build_patch_basis_key_from_archive_hash_path(&d.archive_hash_path)
                    {
                        keys.insert(raw_key);
                    }
                }
                _ => parse_failures += 1,
            }
        }

        self.ctx.set_needed_patch_basis_keys(keys);
        println!(
            "Patch basis prep: tracking {} basis keys ({} parse failures)",
            self.ctx.needed_patch_basis_count(),
            parse_failures
        );
        Ok(())
    }

    /// Install phase: FromArchive + InlineFile + RemappedInlineFile
    pub fn install_phase(&self) -> Result<()> {
        use super::ProgressEvent;

        let stats = self.db.get_directive_stats()?;
        let total_pending = stats.pending;

        if total_pending == 0 {
            println!("No directives to process");
            return Ok(());
        }

        // Signal phase change to Installing
        if let Some(ref callback) = self.ctx.config.progress_callback {
            callback(ProgressEvent::PhaseChange {
                phase: "Installing".to_string(),
            });
        }

        let from_archive_count = self.get_type_count("FromArchive")?;
        let inline_count = self.get_type_count("InlineFile")?;
        let remapped_count = self.get_type_count("RemappedInlineFile")?;

        let install_total = from_archive_count + inline_count + remapped_count;
        let pb = self.make_progress_bar(install_total as u64);

        // FromArchive (streaming)
        let streaming_config = super::streaming::StreamingConfig::default();
        pb.set_message("Processing FromArchive (streaming)...");
        self.reporter
            .phase_started("FromArchive", from_archive_count);

        let progress_callback: Option<super::streaming::ProgressCallback> =
            if let Some(callback) = self.reporter.get_callback() {
                let total = self.reporter.get_phase_total();
                Some(std::sync::Arc::new(move |written_count| {
                    callback(super::ProgressEvent::DirectiveComplete {
                        index: written_count,
                        total,
                    });
                }))
            } else {
                None
            };

        let streaming_stats = super::streaming::process_from_archive_streaming(
            self.db,
            &self.ctx,
            streaming_config,
            &pb,
            progress_callback,
        )?;
        self.completed
            .fetch_add(streaming_stats.extracted, Ordering::Relaxed);
        self.skipped
            .fetch_add(streaming_stats.skipped, Ordering::Relaxed);
        self.failed
            .fetch_add(streaming_stats.failed, Ordering::Relaxed);
        self.sync_reporter();
        self.record_phase_failures("FromArchive", streaming_stats.failed);

        // InlineFile
        let failed_before = self.failed.load(Ordering::Relaxed);
        pb.set_message("Processing InlineFile...");
        self.reporter.phase_started("InlineFile", inline_count);
        process_simple_directives(
            self.db,
            &self.ctx,
            "InlineFile",
            &pb,
            &self.completed,
            &self.skipped,
            &self.failed,
            &self.reporter,
        )?;
        self.sync_reporter();
        self.record_phase_failures(
            "InlineFile",
            self.failed.load(Ordering::Relaxed) - failed_before,
        );

        // RemappedInlineFile
        let failed_before = self.failed.load(Ordering::Relaxed);
        pb.set_message("Processing RemappedInlineFile...");
        self.reporter
            .phase_started("RemappedInlineFile", remapped_count);
        process_simple_directives(
            self.db,
            &self.ctx,
            "RemappedInlineFile",
            &pb,
            &self.completed,
            &self.skipped,
            &self.failed,
            &self.reporter,
        )?;
        self.sync_reporter();
        self.record_phase_failures(
            "RemappedInlineFile",
            self.failed.load(Ordering::Relaxed) - failed_before,
        );

        pb.finish_and_clear();
        Ok(())
    }

    /// Patch phase: PatchedFromArchive
    pub fn patch_phase(&self) -> Result<()> {
        let patched_count = self.get_type_count("PatchedFromArchive")?;
        let pb = self.make_progress_bar(patched_count as u64);
        println!(
            "Patch basis DB: {} candidate local files for {} patch directives",
            self.ctx.patch_basis_db_count(),
            patched_count
        );

        let failed_before = self.failed.load(Ordering::Relaxed);
        pb.set_message("Processing PatchedFromArchive...");
        self.reporter
            .phase_started("PatchedFromArchive", patched_count);
        process_patched_from_archive(
            self.db,
            &self.ctx,
            &pb,
            &self.completed,
            &self.skipped,
            &self.failed,
            &self.reporter,
        )?;
        self.sync_reporter();
        self.record_phase_failures(
            "PatchedFromArchive",
            self.failed.load(Ordering::Relaxed) - failed_before,
        );

        pb.finish_and_clear();
        Ok(())
    }

    /// DDS Transform phase: TransformedTexture
    pub fn texture_phase(&self) -> Result<()> {
        let texture_count = self.get_type_count("TransformedTexture")?;
        let pb = self.make_progress_bar(texture_count as u64);

        let failed_before = self.failed.load(Ordering::Relaxed);
        pb.set_message("Processing TransformedTexture...");
        self.reporter
            .phase_started("TransformedTexture", texture_count);
        process_transformed_texture(
            self.db,
            &self.ctx,
            &pb,
            &self.completed,
            &self.skipped,
            &self.failed,
            &self.reporter,
        )?;
        self.sync_reporter();
        self.record_phase_failures(
            "TransformedTexture",
            self.failed.load(Ordering::Relaxed) - failed_before,
        );

        pb.finish_and_clear();
        Ok(())
    }

    /// BSA Build phase: CreateBSA
    pub fn bsa_phase(&self) -> Result<()> {
        let bsa_count = self.get_type_count("CreateBSA")?;
        let pb = self.make_progress_bar(bsa_count as u64);

        let failed_before = self.failed.load(Ordering::Relaxed);
        pb.set_message("Processing CreateBSA...");
        self.reporter.phase_started("CreateBSA", bsa_count);
        process_create_bsa(
            self.db,
            &self.ctx,
            &pb,
            &self.completed,
            &self.skipped,
            &self.failed,
            &self.reporter,
        )?;
        self.sync_reporter();
        self.record_phase_failures(
            "CreateBSA",
            self.failed.load(Ordering::Relaxed) - failed_before,
        );

        pb.finish_and_clear();
        Ok(())
    }

    /// Cleanup phase: extra files + BSA temp dirs
    pub fn cleanup_phase(&self) -> Result<()> {
        cleanup_extra_files(self.db, &self.ctx)?;
        cleanup_bsa_temp_dirs(&self.ctx.config)?;
        Ok(())
    }

    /// Sync the reporter with current counters
    fn sync_reporter(&self) {
        let current = self.completed.load(Ordering::Relaxed)
            + self.skipped.load(Ordering::Relaxed)
            + self.failed.load(Ordering::Relaxed);
        self.reporter.set_count(current);
        self.reporter.report_count(current);
    }

    /// Finalize and return process stats
    pub fn finish(self) -> ProcessStats {
        let stats = ProcessStats {
            completed: self.completed.load(Ordering::Relaxed),
            skipped: self.skipped.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
        };

        println!(
            "Processed {} directives ({} completed, {} skipped, {} failed)",
            stats.completed + stats.skipped + stats.failed,
            stats.completed,
            stats.skipped,
            stats.failed
        );

        let phase_failures = self
            .phase_failures
            .lock()
            .expect("phase_failures lock poisoned");
        if !phase_failures.is_empty() {
            eprintln!("\n=== FAILURE SUMMARY ===");
            for (phase, count) in phase_failures.iter() {
                eprintln!("  {}: {} failures", phase, count);
            }
            eprintln!("=======================\n");
        }

        stats
    }
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
        pb.println(format!(
            "WARN: {} PatchedFromArchive directives failed to parse",
            parse_failures
        ));
        // Report parse failures to GUI
        for _ in 0..parse_failures {
            reporter.directive_completed();
        }
    }

    // Build archive list with sizes for sorting
    let mut archives_with_size: Vec<(String, Vec<(i64, PatchedFromArchiveDirective)>, u64)> =
        by_archive
            .into_iter()
            .map(|(hash, directives)| {
                let size = ctx
                    .get_archive_path(&hash)
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
    // Track two units per directive:
    // 1) source preparation/extraction
    // 2) patch apply/write
    pb.set_length((total_directives as u64).saturating_mul(2));
    pb.set_position(0);
    pb.set_message("Applying patches (extracting sources + delta apply)...");
    pb.enable_steady_tick(std::time::Duration::from_millis(100));

    // Wrap counters in Arc for sharing
    let completed = Arc::new(AtomicUsize::new(completed.load(Ordering::Relaxed)));
    let failed = Arc::new(AtomicUsize::new(failed.load(Ordering::Relaxed)));
    let skipped = Arc::new(AtomicUsize::new(skipped.load(Ordering::Relaxed)));

    // Tier archives by size (same approach as FromArchive)
    const HALF_GB: u64 = 512 * 1024 * 1024;
    const TWO_GB: u64 = 2 * 1024 * 1024 * 1024;

    let mut small_archives = Vec::new(); // ≤512MB - full parallel
    let mut medium_archives = Vec::new(); // 512MB-2GB - limited parallel
    let mut large_archives = Vec::new(); // ≥2GB - sequential

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
        small_archives.len(),
        medium_archives.len(),
        large_archives.len()
    );
    debug!(
        "Patch scheduling: all_archives={} preload_blobs={}",
        small_archives.len() + medium_archives.len() + large_archives.len(),
        should_preload_patch_blobs()
    );

    // Process ALL archives concurrently via par_iter so extraction from one archive
    // overlaps with patch application from another. 7z extraction is inherently
    // single-threaded per archive, so concurrent archives keep all cores busy.
    // Each archive extracts to its own temp dir (disk-based, not RAM).
    let mut all_archives: Vec<_> = small_archives
        .iter()
        .chain(medium_archives.iter())
        .chain(large_archives.iter())
        .collect();
    // Sort by directive count descending so large-work archives start first,
    // giving them maximum time to extract while smaller archives finish quickly.
    all_archives.sort_by(|a, b| b.1.len().cmp(&a.1.len()));

    all_archives
        .par_iter()
        .for_each(|(archive_hash, directives, _size)| {
            process_archive_patches(
                ctx,
                archive_hash,
                directives,
                None,
                pb,
                &skipped,
                &failed,
                &failure_tracker,
                &completed,
                reporter,
            );
        });

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
    threads_per_archive: Option<usize>,
    pb: &ProgressBar,
    skipped: &Arc<AtomicUsize>,
    failed: &Arc<AtomicUsize>,
    failure_tracker: &Arc<FailureTracker>,
    completed: &Arc<AtomicUsize>,
    reporter: &Arc<ProgressReporter>,
) {
    let archive_start = Instant::now();
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
                pb.inc(2);
                reporter.directive_completed();
            }
            if any_needed {
                pb.println(format!(
                    "WARN: Archive not found for patching: {}",
                    archive_hash
                ));
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
        pb.inc((skip_count as u64).saturating_mul(2));
        for _ in 0..skip_count {
            reporter.directive_completed();
        }
    }

    if to_process.is_empty() {
        return;
    }

    // Prefer already-installed local basis files when they verify.
    let mut verified_local_basis: HashMap<String, PathBuf> = HashMap::new();

    // Collect unique source paths that still need extraction from archive.
    let mut simple_paths: HashMap<String, String> = HashMap::new();
    let mut nested_bsas: HashMap<String, Vec<String>> = HashMap::new();

    for (_, directive) in &to_process {
        let basis_key = build_patch_basis_key_from_archive_hash_path(&directive.archive_hash_path);
        if let Some(key) = basis_key.as_deref() {
            if let Some(local_path) =
                ctx.resolve_verified_patch_basis_path(key, Some(&directive.from_hash))
            {
                verified_local_basis.insert(key.to_string(), local_path);
                continue;
            }
        }

        if directive.archive_hash_path.len() == 2 {
            let path = &directive.archive_hash_path[1];
            let normalized = paths::normalize_for_lookup(path);
            simple_paths.insert(normalized, path.clone());
        } else if directive.archive_hash_path.len() >= 3 {
            let bsa_path = &directive.archive_hash_path[1];
            let file_in_bsa = &directive.archive_hash_path[2];
            nested_bsas
                .entry(bsa_path.clone())
                .or_default()
                .push(file_in_bsa.clone());
            let normalized = paths::normalize_for_lookup(bsa_path);
            simple_paths.insert(normalized, bsa_path.clone());
        }
    }

    // Extract source files to temp directory (disk-based, not RAM)
    let temp_dir = match tempfile::tempdir_in(&ctx.config.output_dir) {
        Ok(dir) => dir,
        Err(e) => {
            for (_id, _) in &to_process {
                failed.fetch_add(1, Ordering::Relaxed);
                failure_tracker.record_failure("tempdir", &e.to_string());
                pb.inc(2);
                reporter.directive_completed();
            }
            return;
        }
    };

    let archive_name = archive_path
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();
    pb.set_message(format!(
        "Extracting + preloading {} ({} directives)",
        archive_name,
        to_process.len()
    ));

    // Run extraction and delta preloading concurrently — extraction is often
    // single-threaded 7z decompression, so overlap it with reading delta blobs.
    let preload_enabled = should_preload_patch_blobs();
    let extraction_start = Instant::now();
    let (extracted_paths, preloaded_patches) = std::thread::scope(|s| {
        let preload_handle = s.spawn(|| {
            if preload_enabled {
                preload_patch_blobs(&ctx.config.wabbajack_path, &to_process).unwrap_or_else(|e| {
                    tracing::debug!(
                        "Patch preload failed for archive {} (falling back to shared reader): {}",
                        archive_name,
                        e
                    );
                    HashMap::new()
                })
            } else {
                HashMap::new()
            }
        });

        let extracted = if simple_paths.is_empty() && nested_bsas.is_empty() {
            HashMap::new()
        } else {
            extract_source_files_to_disk(
                ctx,
                &archive_path,
                &simple_paths,
                &nested_bsas,
                threads_per_archive,
                temp_dir.path(),
            )
        };

        let preloaded = preload_handle.join().unwrap_or_default();
        (extracted, preloaded)
    });
    let extraction_ms = extraction_start.elapsed().as_millis();
    // Stage 1 complete for all directives in this archive.
    pb.inc(to_process.len() as u64);

    pb.set_message(format!(
        "Patching archive {} ({} directives)",
        archive_name,
        to_process.len()
    ));

    // Apply all patches in parallel via rayon work-stealing. Since multiple archives
    // run concurrently via the outer par_iter, rayon naturally balances work across
    // all cores — threads freed from finished archives help with remaining ones.
    let apply_start = Instant::now();
    to_process.par_iter().for_each(|(id, directive)| {
        let patch_name = directive.patch_id.to_string();
        let preloaded_delta = preloaded_patches.get(&patch_name).map(|v| v.as_slice());
        let result = apply_patch_streaming(
            ctx,
            &archive_path,
            directive,
            &extracted_paths,
            &verified_local_basis,
            &patch_name,
            preloaded_delta,
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
    });
    let apply_ms = apply_start.elapsed().as_millis();
    tracing::debug!(
        "Patch archive {} done: directives={}, extracted_sources={}, preload={}, extract_ms={}, apply_ms={}, total_ms={}",
        archive_name,
        to_process.len(),
        extracted_paths.len(),
        preload_enabled,
        extraction_ms,
        apply_ms,
        archive_start.elapsed().as_millis()
    );

    // temp_dir is dropped here, cleaning up extracted files
}

/// Preload patch blobs for an archive using a local ZIP reader.
///
/// This avoids contention on the shared mutex-protected wabbajack reader.
fn preload_patch_blobs(
    wabbajack_path: &Path,
    directives: &[&(i64, crate::modlist::PatchedFromArchiveDirective)],
) -> Result<HashMap<String, Vec<u8>>> {
    let mut unique_patch_names = std::collections::HashSet::new();
    for (_, directive) in directives {
        unique_patch_names.insert(directive.patch_id.to_string());
    }

    if unique_patch_names.is_empty() {
        return Ok(HashMap::new());
    }

    let file = File::open(wabbajack_path)
        .with_context(|| format!("Failed to open {}", wabbajack_path.display()))?;
    let reader = BufReader::new(file);
    let mut archive = ZipArchive::new(reader).context("Failed to read wabbajack as ZIP")?;

    let mut blobs = HashMap::with_capacity(unique_patch_names.len());
    for patch_name in unique_patch_names {
        let mut patch_file = archive
            .by_name(&patch_name)
            .with_context(|| format!("Patch '{}' not found in wabbajack", patch_name))?;
        let mut data = Vec::new();
        std::io::Read::read_to_end(&mut patch_file, &mut data)?;
        blobs.insert(patch_name, data);
    }

    Ok(blobs)
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
    threads_per_archive: Option<usize>,
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
            ArchiveType::Tes3Bsa | ArchiveType::Bsa | ArchiveType::Ba2 => {
                // BSA files: extract each needed file to disk
                for (idx, (normalized, original)) in simple_paths.iter().enumerate() {
                    if let Ok(data) = bsa::extract_archive_file(archive_path, original) {
                        let temp_file_path = temp_dir.join(format!("bsa_{}.tmp", idx));
                        if fs::write(&temp_file_path, &data).is_ok() {
                            extracted.insert(normalized.clone(), temp_file_path);
                        }
                    }
                }
            }
            ArchiveType::SevenZ | ArchiveType::Rar => {
                // Extract only needed files when the request set is small.
                let extract_dir = temp_dir.join("archive_extract");
                let _ = fs::create_dir_all(&extract_dir);

                // Build set of needed BSA/BA2 container files from nested_bsas
                let needed_bsas: std::collections::HashSet<String> = nested_bsas
                    .keys()
                    .map(|k| paths::normalize_for_lookup(k))
                    .collect();

                let needed_paths: Vec<String> = simple_paths.values().cloned().collect();
                let selective = should_use_selective_extraction(archive_path, needed_paths.len());
                debug!(
                    "Patch source extraction strategy: archive={} needed_files={} mode={}",
                    archive_path.display(),
                    needed_paths.len(),
                    if selective { "selective" } else { "full" }
                );
                let extract_result = if selective {
                    crate::archive::sevenzip::extract_files_case_insensitive(
                        archive_path,
                        &needed_paths,
                        &extract_dir,
                    )
                    .map(|_| ())
                    .or_else(|_| {
                        crate::archive::sevenzip::extract_all_with_threads(
                            archive_path,
                            &extract_dir,
                            threads_per_archive,
                        )
                        .map(|_| ())
                    })
                } else {
                    crate::archive::sevenzip::extract_all_with_threads(
                        archive_path,
                        &extract_dir,
                        threads_per_archive,
                    )
                    .map(|_| ())
                };

                if extract_result.is_ok() {
                    // Walk extracted files and find what we need
                    for entry in walkdir::WalkDir::new(&extract_dir)
                        .into_iter()
                        .filter_map(|e| e.ok())
                        .filter(|e| e.file_type().is_file())
                    {
                        if let Ok(rel_path) = entry.path().strip_prefix(&extract_dir) {
                            let normalized =
                                paths::normalize_for_lookup(&rel_path.to_string_lossy());
                            // Collect both simple files AND BSA/BA2 container files
                            if simple_paths.contains_key(&normalized)
                                || needed_bsas.contains(&normalized)
                            {
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

                // Build set of needed BSA/BA2 container files from nested_bsas
                let needed_bsas: std::collections::HashSet<String> = nested_bsas
                    .keys()
                    .map(|k| paths::normalize_for_lookup(k))
                    .collect();

                let extract_result = extract_zip_to_temp(archive_path, &extract_dir)
                    .or_else(|_| extract_7z_to_temp(archive_path, &extract_dir))
                    .or_else(|_| extract_rar_to_temp(archive_path, &extract_dir));

                if extract_result.is_ok() {
                    // Walk extracted files and collect paths for needed files AND BSA containers
                    collect_needed_file_paths_with_bsas(
                        &extract_dir,
                        &extract_dir,
                        simple_paths,
                        &needed_bsas,
                        &mut extracted,
                    );
                }
            }
        }
    }

    // For nested archives: extract files from BSAs/ZIPs/etc we just extracted to disk
    // Note: "nested_bsas" is a misnomer - these can be any archive type (BSA, BA2, ZIP, FOMOD, etc.)
    for (nested_archive_path, files_in_archive) in nested_bsas {
        let archive_normalized = paths::normalize_for_lookup(nested_archive_path);
        // Clone the path to avoid borrow conflict
        let archive_file_path = match extracted.get(&archive_normalized) {
            Some(p) => p.clone(),
            None => continue,
        };
        // Extract files from the nested archive using the generic extractor
        // This handles BSA, BA2, ZIP (including .fomod), 7z, RAR, etc.
        for (idx, file_path) in files_in_archive.iter().enumerate() {
            if let Ok(file_data) =
                extract_from_archive_with_temp(&archive_file_path, file_path, temp_dir)
            {
                let key = format!("{}/{}", nested_archive_path, file_path);
                let normalized_key = paths::normalize_for_lookup(&key);
                let temp_file_path = temp_dir.join(format!(
                    "nested_{}_{}.tmp",
                    archive_normalized.replace(['/', '\\'], "_"),
                    idx
                ));
                if fs::write(&temp_file_path, &file_data).is_ok() {
                    extracted.insert(normalized_key, temp_file_path);
                }
            }
        }
    }

    extracted
}

/// Recursively collect file paths from extracted archive, including BSA container files
fn collect_needed_file_paths_with_bsas(
    base: &Path,
    current: &Path,
    needed: &HashMap<String, String>,
    needed_bsas: &std::collections::HashSet<String>,
    results: &mut HashMap<String, PathBuf>,
) {
    let needed_normalized: std::collections::HashSet<String> = needed.keys().cloned().collect();

    if let Ok(entries) = fs::read_dir(current) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_needed_file_paths_with_bsas(base, &path, needed, needed_bsas, results);
            } else if path.is_file() {
                // Get relative path from base
                if let Ok(rel) = path.strip_prefix(base) {
                    let rel_str = rel.to_string_lossy();
                    let normalized = paths::normalize_for_lookup(&rel_str);

                    // Collect both simple files AND BSA/BA2 container files
                    if needed_normalized.contains(&normalized) || needed_bsas.contains(&normalized)
                    {
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
    verified_local_basis: &HashMap<String, PathBuf>,
    patch_name: &str,
    preloaded_delta: Option<&[u8]>,
) -> Result<()> {
    use crate::octodiff::DeltaReader;
    use memmap2::Mmap;
    use std::io::{BufWriter, Cursor};

    // Get source file path:
    // 1) verified local basis from phase-4 output map
    // 2) extracted cache for this archive
    let source_path = if let Some(basis_key) =
        build_patch_basis_key_from_archive_hash_path(&directive.archive_hash_path)
    {
        if let Some(path) = verified_local_basis.get(&basis_key) {
            path.clone()
        } else if directive.archive_hash_path.len() == 1 {
            archive_path.to_path_buf()
        } else if directive.archive_hash_path.len() == 2 {
            let path = &directive.archive_hash_path[1];
            let normalized = paths::normalize_for_lookup(path);
            extracted_paths.get(&normalized).cloned().ok_or_else(|| {
                anyhow::anyhow!("Source file not found in extracted cache: {}", path)
            })?
        } else if directive.archive_hash_path.len() >= 3 {
            let bsa_path = &directive.archive_hash_path[1];
            let file_in_bsa = &directive.archive_hash_path[2];
            let key = format!("{}/{}", bsa_path, file_in_bsa);
            let normalized = paths::normalize_for_lookup(&key);
            extracted_paths.get(&normalized).cloned().ok_or_else(|| {
                anyhow::anyhow!(
                    "Source file not found in BSA cache: {} / {}",
                    bsa_path,
                    file_in_bsa
                )
            })?
        } else {
            anyhow::bail!("Invalid archive_hash_path");
        }
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

    // Use preloaded delta when available; fall back to shared reader if needed.
    let owned_delta;
    let delta_data: &[u8] = if let Some(data) = preloaded_delta {
        data
    } else {
        owned_delta = ctx.read_wabbajack_file(patch_name)?;
        owned_delta.as_slice()
    };

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
        anyhow::bail!(
            "Size mismatch: expected {}, got {}",
            directive.size,
            written
        );
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

    pb.set_message(format!(
        "Parsing {} TransformedTexture directives...",
        all_raw.len()
    ));

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
        pb.println(format!(
            "WARN: {} TransformedTexture directives failed to parse",
            parse_failures
        ));
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
                println!(
                    "\nAborting installation. Please report unsupported formats to developers."
                );
                std::process::exit(1);
            }
        });
    }

    let total_archives = by_archive.len();
    let total_directives: usize = by_archive.values().map(|v| v.len()).sum();
    eprintln!(
        "Processing {} textures across {} archives...",
        total_directives, total_archives
    );

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
                    // Log what BSA/BA2 we're looking for
                    let bsa_keys: Vec<_> = simple_paths.keys()
                        .filter(|k| k.ends_with(".ba2") || k.ends_with(".bsa"))
                        .collect();
                    if !bsa_keys.is_empty() {
                        tracing::debug!("[DEBUG] ZIP extraction - looking for BSA/BA2: {:?}", bsa_keys);
                    }
                    if let Ok(file) = File::open(&archive_path) {
                        if let Ok(mut archive) = zip::ZipArchive::new(BufReader::new(file)) {
                            for i in 0..archive.len() {
                                if let Ok(mut entry) = archive.by_index(i) {
                                    let entry_name = entry.name().to_string();
                                    let normalized = paths::normalize_for_lookup(&entry_name);
                                    // Debug log for BSA/BA2 files
                                    if normalized.ends_with(".ba2") || normalized.ends_with(".bsa") {
                                        if simple_paths.contains_key(&normalized) {
                                            tracing::debug!("[DEBUG] ZIP: Found and MATCHED BSA/BA2: '{}' -> '{}'", entry_name, normalized);
                                        } else {
                                            tracing::debug!("[DEBUG] ZIP: Found BSA/BA2 NOT in simple_paths: '{}' -> '{}'", entry_name, normalized);
                                        }
                                    }
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
                ArchiveType::Tes3Bsa | ArchiveType::Bsa | ArchiveType::Ba2 => {
                    let mut result = HashMap::new();
                    for (normalized, original) in &simple_paths {
                        if let Ok(data) = bsa::extract_archive_file(&archive_path, original) {
                            result.insert(normalized.clone(), data);
                        }
                    }
                    result
                }
                _ => {
                    // 7z/RAR/other: batch extract using pure Rust
                    if let Ok(temp_dir) = tempfile::tempdir_in(&ctx.config.output_dir) {
                        let paths_vec: Vec<&str> = simple_paths.values().map(|s| s.as_str()).collect();
                        // Filter to show only BA2/BSA paths for debugging
                        let bsa_paths: Vec<_> = paths_vec.iter()
                            .filter(|p| p.to_lowercase().ends_with(".ba2") || p.to_lowercase().ends_with(".bsa"))
                            .collect();
                        if !bsa_paths.is_empty() {
                            tracing::debug!("[DEBUG] Extracting from 7z/RAR, BSA/BA2 paths requested: {:?}", bsa_paths);
                        }
                        let result = match extract_batch_rust(&archive_path, &paths_vec, temp_dir.path()) {
                            Ok(r) => r,
                            Err(e) => {
                                tracing::debug!("[DEBUG] extract_batch_rust failed for {}: {}", archive_path.display(), e);
                                HashMap::new()
                            }
                        };
                        if !bsa_paths.is_empty() {
                            let extracted_bsas: Vec<_> = result.keys()
                                .filter(|k| k.ends_with(".ba2") || k.ends_with(".bsa"))
                                .collect();
                            tracing::debug!("[DEBUG] Extracted BSA/BA2 keys: {:?}", extracted_bsas);
                        }
                        result
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
            tracing::debug!("[DEBUG] Looking for BSA/BA2: original='{}', normalized='{}'", bsa_path, bsa_normalized);
            if let Some(bsa_data) = extracted.get(&bsa_normalized) {
                tracing::debug!("[DEBUG] Found BSA/BA2 in cache, size={} bytes, extracting {} files", bsa_data.len(), files_in_bsa.len());
                // Determine suffix from original path
                let suffix = if bsa_path.to_lowercase().ends_with(".ba2") { ".ba2" } else { ".bsa" };
                // Write BSA/BA2 to temp file and extract from it
                if let Ok(temp_bsa) = tempfile::Builder::new()
                    .prefix(".clf3_bsa_")
                    .suffix(suffix)
                    .tempfile_in(&ctx.config.downloads_dir)
                {
                    if fs::write(temp_bsa.path(), bsa_data).is_ok() {
                        for file_path in files_in_bsa {
                            match bsa::extract_archive_file(temp_bsa.path(), file_path) {
                                Ok(file_data) => {
                                    // Key: "bsa_path/file_path" normalized
                                    let key = format!("{}/{}", bsa_path, file_path);
                                    let normalized_key = paths::normalize_for_lookup(&key);
                                    extracted.insert(normalized_key, file_data);
                                }
                                Err(e) => {
                                    tracing::debug!("Failed to extract {} from {}: {}", file_path, bsa_path, e);
                                }
                            }
                        }
                    }
                }
            } else {
                tracing::debug!("BSA/BA2 not found in extracted archive: {} (normalized: {})", bsa_path, bsa_normalized);
                tracing::debug!("Available keys: {:?}", extracted.keys().take(10).collect::<Vec<_>>());
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
        .filter_map(
            |(id, json)| match serde_json::from_str::<Directive>(&json) {
                Ok(Directive::CreateBSA(d)) => Some((id, d)),
                _ => {
                    failed.fetch_add(1, Ordering::Relaxed);
                    pb.inc(1);
                    None
                }
            },
        )
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
        small.len(),
        PARALLEL_THRESHOLD,
        large.len(),
        PARALLEL_THRESHOLD
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

            // Determine archive type for display
            let archive_type = match &directive.state {
                crate::modlist::BSAState::BSA(_) => "BSA",
                crate::modlist::BSAState::BA2(_) => "BA2",
            };

            match handlers::handle_create_bsa(ctx, directive) {
                Ok(()) => {
                    completed.fetch_add(1, Ordering::Relaxed);
                    pb.println(format!("Created {}: {}", archive_type, bsa_name));
                }
                Err(e) => {
                    failed.fetch_add(1, Ordering::Relaxed);
                    pb.println(format!(
                        "FAIL [{}] create {} {}: {:#}",
                        id, archive_type, bsa_name, e
                    ));
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

        // Determine archive type for display
        let archive_type = match &directive.state {
            crate::modlist::BSAState::BSA(_) => "BSA",
            crate::modlist::BSAState::BA2(_) => "BA2",
        };

        pb.set_message(format!(
            "{} ({} files) [LARGE]",
            bsa_name,
            directive.file_states.len()
        ));

        match handlers::handle_create_bsa(ctx, &directive) {
            Ok(()) => {
                completed.fetch_add(1, Ordering::Relaxed);
                pb.println(format!("Created {}: {}", archive_type, bsa_name));
            }
            Err(e) => {
                failed.fetch_add(1, Ordering::Relaxed);
                pb.println(format!(
                    "FAIL [{}] create {} {}: {:#}",
                    id, archive_type, bsa_name, e
                ));
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_patch_basis_store_roundtrip() {
        let tmp = tempdir().unwrap();
        let db_path = tmp.path().join("test.db");
        let store = PatchBasisStore::open(&db_path, "test_modlist").unwrap();

        // Create a test file for verification
        let test_file = tmp.path().join("basis.bin");
        let data = b"hello world patch basis";
        fs::write(&test_file, data).unwrap();

        let record = PatchBasisRecord {
            local_path: test_file.clone(),
            size: data.len() as u64,
            quick_hash: quick_file_hash(&test_file).unwrap(),
        };

        // Upsert and reload
        store.upsert("key1", &record);
        let loaded = store.load_verified().unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded["key1"].local_path, test_file);
        assert_eq!(loaded["key1"].size, data.len() as u64);
    }

    #[test]
    fn test_patch_basis_store_stale_entries_pruned() {
        let tmp = tempdir().unwrap();
        let db_path = tmp.path().join("test.db");
        let store = PatchBasisStore::open(&db_path, "test_modlist").unwrap();

        // Insert entry pointing to non-existent file
        let record = PatchBasisRecord {
            local_path: PathBuf::from("/nonexistent/file.bin"),
            size: 100,
            quick_hash: 12345,
        };
        store.upsert("stale_key", &record);

        // Load should prune it
        let loaded = store.load_verified().unwrap();
        assert!(loaded.is_empty());

        // Re-open to verify it was actually deleted from DB
        let store2 = PatchBasisStore::open(&db_path, "test_modlist").unwrap();
        let loaded2 = store2.load_verified().unwrap();
        assert!(loaded2.is_empty());
    }

    #[test]
    fn test_patch_basis_store_modlist_isolation() {
        let tmp = tempdir().unwrap();
        let db_path = tmp.path().join("test.db");

        // Create a test file
        let test_file = tmp.path().join("shared.bin");
        fs::write(&test_file, b"shared data").unwrap();

        let record = PatchBasisRecord {
            local_path: test_file.clone(),
            size: 11,
            quick_hash: quick_file_hash(&test_file).unwrap(),
        };

        // Store under modlist A
        let store_a = PatchBasisStore::open(&db_path, "modlist_a").unwrap();
        store_a.upsert("key1", &record);

        // Store under modlist B with different key
        let store_b = PatchBasisStore::open(&db_path, "modlist_b").unwrap();
        store_b.upsert("key2", &record);

        // Each should only see its own entries
        let loaded_a = store_a.load_verified().unwrap();
        assert_eq!(loaded_a.len(), 1);
        assert!(loaded_a.contains_key("key1"));

        let loaded_b = store_b.load_verified().unwrap();
        assert_eq!(loaded_b.len(), 1);
        assert!(loaded_b.contains_key("key2"));
    }

    #[test]
    fn test_patch_basis_store_size_mismatch_pruned() {
        let tmp = tempdir().unwrap();
        let db_path = tmp.path().join("test.db");
        let store = PatchBasisStore::open(&db_path, "test_modlist").unwrap();

        // Create a test file
        let test_file = tmp.path().join("basis.bin");
        fs::write(&test_file, b"original").unwrap();

        let record = PatchBasisRecord {
            local_path: test_file.clone(),
            size: 999, // Wrong size
            quick_hash: 0,
        };
        store.upsert("bad_size", &record);

        let loaded = store.load_verified().unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_build_patch_basis_key() {
        let key = build_patch_basis_key("abc123", Some("Data\\Textures\\test.dds"), None);
        assert_eq!(key, "abc123|data/textures/test.dds");

        let key2 = build_patch_basis_key("abc123", Some("path.bsa"), Some("inner/file.nif"));
        assert_eq!(key2, "abc123|path.bsa|inner/file.nif");

        let key3 = build_patch_basis_key("abc123", None, None);
        assert_eq!(key3, "abc123");
    }

    #[test]
    fn test_build_patch_basis_key_from_archive_hash_path() {
        let parts = vec![
            "abc123".to_string(),
            "Data\\Textures\\test.dds".to_string(),
        ];
        let key = build_patch_basis_key_from_archive_hash_path(&parts).unwrap();
        assert_eq!(key, "abc123|data/textures/test.dds");

        let empty: Vec<String> = vec![];
        assert!(build_patch_basis_key_from_archive_hash_path(&empty).is_none());
    }
}
