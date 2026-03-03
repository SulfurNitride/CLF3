//! Directive processor - TTW-style batch processing
//!
//! Architecture:
//! - ZIP/7z/RAR: Extraction using 7z binary via crate::archive::sevenzip
//! - ZIP: zip crate is used for reading .wabbajack files (they are ZIP archives)
//! - BSA/BA2: Direct extraction via ba2 crate
//! - Nested BSA: Extract BSA from archive, then extract file from BSA
//! - Parallel processing using rayon

use crate::bsa::{self, BsaCache};
use crate::hash::verify_file_hash;
use crate::modlist::{ArchiveFileEntry, Directive, ModlistDb};
use crate::paths;

use super::config::InstallConfig;
use super::handlers;
use super::handlers::from_archive::{detect_archive_type, ArchiveType};

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
use std::time::Duration;
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

fn patch_cache_key(hash: &str) -> String {
    hash.as_bytes()
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect()
}

fn patch_cache_path(ctx: &ProcessContext, hash: &str) -> Option<PathBuf> {
    ctx.config
        .patch_cache_dir
        .as_ref()
        .map(|dir| dir.join(format!("{}.bin", patch_cache_key(hash))))
}

pub(crate) fn restore_patched_output_from_cache(
    ctx: &ProcessContext,
    directive: &crate::modlist::PatchedFromArchiveDirective,
) -> Result<bool> {
    let Some(cache_path) = patch_cache_path(ctx, &directive.hash) else {
        return Ok(false);
    };
    if !cache_path.exists() {
        return Ok(false);
    }

    let meta = fs::metadata(&cache_path)
        .with_context(|| format!("Failed to stat patch cache file: {}", cache_path.display()))?;
    if meta.len() != directive.size {
        return Ok(false);
    }
    if !verify_file_hash(&cache_path, &directive.hash)? {
        return Ok(false);
    }

    let output_path = ctx.resolve_output_path(&directive.to);
    paths::ensure_parent_dirs(&output_path)?;
    let _ = fs::remove_file(&output_path);
    reflink_copy::reflink_or_copy(&cache_path, &output_path).with_context(|| {
        format!(
            "Failed to restore patched output from cache {} -> {}",
            cache_path.display(),
            output_path.display()
        )
    })?;
    Ok(true)
}

pub(crate) fn store_patched_output_in_cache(
    ctx: &ProcessContext,
    directive: &crate::modlist::PatchedFromArchiveDirective,
    output_path: &Path,
) -> Result<()> {
    let Some(cache_path) = patch_cache_path(ctx, &directive.hash) else {
        return Ok(());
    };
    if let Some(parent) = cache_path.parent() {
        fs::create_dir_all(parent)?;
    }

    if cache_path.exists() {
        if let Ok(meta) = fs::metadata(&cache_path) {
            if meta.len() == directive.size
                && verify_file_hash(&cache_path, &directive.hash).unwrap_or(false) {
                    return Ok(());
                }
        }
        let _ = fs::remove_file(&cache_path);
    }

    reflink_copy::reflink_or_copy(output_path, &cache_path).with_context(|| {
        format!(
            "Failed to store patched output in cache {} -> {}",
            output_path.display(),
            cache_path.display()
        )
    })?;
    Ok(())
}

pub(crate) fn should_preload_patch_blobs() -> bool {
    static SHOULD_PRELOAD: OnceLock<bool> = OnceLock::new();
    *SHOULD_PRELOAD.get_or_init(|| {
        if let Ok(v) = std::env::var("CLF3_PRELOAD_PATCH_BLOBS") {
            let value = v.trim().to_ascii_lowercase();
            return matches!(value.as_str(), "1" | "true" | "yes" | "on");
        }

        // Default OFF: preloading all deltas into RAM per archive causes OOM when
        // multiple archives extract concurrently. On-demand reading from the shared
        // wabbajack ZIP is slower but uses negligible RAM.
        false
    })
}

/// List files in an archive using pure Rust crates
fn list_archive_files_rust(archive_path: &Path) -> Result<Vec<ArchiveFileEntry>> {
    let archive_type = detect_archive_type(archive_path)?;

    match archive_type {
        ArchiveType::Zip => list_zip_files(archive_path)
            .or_else(|e| {
                tracing::warn!("ZIP crate listing failed, falling back to 7z: {}", e);
                list_7z_files(archive_path)
            }),
        ArchiveType::SevenZ => list_7z_files(archive_path),
        ArchiveType::Rar => list_rar_files(archive_path),
        ArchiveType::Tes3Bsa | ArchiveType::Bsa | ArchiveType::Ba2 => {
            // Use the universal list_archive_files which auto-detects BA2 vs BSA format
            bsa::list_archive_files(archive_path).map(|entries| {
                entries
                    .into_iter()
                    .map(|e| ArchiveFileEntry {
                        file_path: e.path,
                        file_size: e.size,
                    })
                    .collect()
            })
        }
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
        conn.busy_timeout(Duration::from_secs(10))
            .context("Failed to set patch basis DB busy timeout")?;

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
    /// Set of texture directive IDs already processed during extraction (by DDS handler thread)
    pub textures_processed_during_install: Mutex<HashSet<i64>>,
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
        let patch_basis_db_path = cache_dir.join("patch_basis.db");
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
            textures_processed_during_install: Mutex::new(HashSet::new()),
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
pub(crate) fn output_exists(ctx: &ProcessContext, to_path: &str, expected_size: u64) -> bool {
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

    /// Install + Patch phase: FromArchive + PatchedFromArchive + InlineFile + RemappedInlineFile
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
        let patched_count = self.get_type_count("PatchedFromArchive")?;
        let inline_count = self.get_type_count("InlineFile")?;
        let remapped_count = self.get_type_count("RemappedInlineFile")?;

        let install_total = from_archive_count + patched_count + inline_count + remapped_count;
        let pb = self.make_progress_bar(install_total as u64);

        // FromArchive + PatchedFromArchive (fused streaming pipeline)
        let streaming_config = super::streaming::StreamingConfig {
            max_extract_workers: Some(self.ctx.config.max_install_workers),
            max_parallel_7z_archives: Some(self.ctx.config.max_parallel_7z_archives),
            max_parallel_bsa_archives: Some(self.ctx.config.max_parallel_bsa_archives),
        };
        pb.set_message("Processing FromArchive + PatchedFromArchive (fused)...");
        self.reporter
            .phase_started("FromArchive+Patch", from_archive_count + patched_count);

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

        println!(
            "Patch basis DB: {} candidate local files for {} patch directives",
            self.ctx.patch_basis_db_count(),
            patched_count
        );

        let streaming_stats = super::streaming::process_fused_streaming(
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
        self.record_phase_failures("FromArchive+Patch", streaming_stats.failed);

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
        cleanup_bsa_temp_dirs(self.ctx.config)?;
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

    /// Get current failure count (for early abort checks)
    pub fn failed_count(&self) -> usize {
        self.failed.load(Ordering::Relaxed)
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

/// Apply a delta patch to a temp file (reusable core extracted from apply_patch_streaming).
///
/// Memory-maps the source file, reads the delta, and streams output to `temp_output_path`.
/// Verifies that the written size matches `expected_size`.
pub(crate) fn apply_patch_to_temp(
    ctx: &ProcessContext,
    source_path: &Path,
    patch_name: &str,
    preloaded_delta: Option<&[u8]>,
    temp_output_path: &Path,
    expected_size: u64,
) -> Result<()> {
    use crate::octodiff::DeltaReader;
    use memmap2::Mmap;
    use std::io::{BufWriter, Cursor};

    // Memory-map source file (OS manages paging, not loaded to RAM)
    let source_file = File::open(source_path)
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

    // Stream to temp output file
    paths::ensure_parent_dirs(temp_output_path)?;
    let output_file = File::create(temp_output_path)
        .with_context(|| format!("Failed to create temp output: {}", temp_output_path.display()))?;
    let mut writer = BufWriter::new(output_file);

    let written = std::io::copy(&mut reader, &mut writer)
        .with_context(|| format!("Failed to write patched file: {}", temp_output_path.display()))?;

    if written != expected_size {
        let _ = fs::remove_file(temp_output_path);
        bail!(
            "Patch size mismatch: expected {}, got {}",
            expected_size,
            written
        );
    }

    Ok(())
}

/// Preload patch blobs by their names (patch IDs) from the wabbajack ZIP.
///
/// This avoids contention on the shared mutex-protected wabbajack reader
/// by opening a separate ZIP reader for bulk reading.
pub(crate) fn preload_patch_blobs_by_name(
    wabbajack_path: &Path,
    patch_names: &HashSet<String>,
) -> Result<HashMap<String, Vec<u8>>> {
    if patch_names.is_empty() {
        return Ok(HashMap::new());
    }

    let file = File::open(wabbajack_path)
        .with_context(|| format!("Failed to open {}", wabbajack_path.display()))?;
    let reader = BufReader::new(file);
    let mut archive = ZipArchive::new(reader).context("Failed to read wabbajack as ZIP")?;

    let mut blobs = HashMap::with_capacity(patch_names.len());
    for patch_name in patch_names {
        let mut patch_file = archive
            .by_name(patch_name)
            .with_context(|| format!("Patch '{}' not found in wabbajack", patch_name))?;
        let mut data = Vec::new();
        std::io::Read::read_to_end(&mut patch_file, &mut data)?;
        blobs.insert(patch_name.clone(), data);
    }

    Ok(blobs)
}

/// Extract archive to temp dir on disk (for 7z/RAR that don't support random access).
/// Only extracts files, doesn't read them into memory.
fn extract_to_temp_disk(archive_path: &Path, needed_paths: &[&str], temp_dir: &Path) -> Result<()> {
    let archive_type = detect_archive_type(archive_path)?;
    match archive_type {
        ArchiveType::SevenZ | ArchiveType::Rar | ArchiveType::Unknown => {
            if needed_paths.is_empty() {
                crate::archive::sevenzip::extract_all(archive_path, temp_dir)
                    .map(|_| ())
                    .with_context(|| format!("Failed to extract: {}", archive_path.display()))
            } else {
                let needed: Vec<String> = needed_paths.iter().map(|p| (*p).to_string()).collect();
                crate::archive::sevenzip::extract_files_case_insensitive(archive_path, &needed, temp_dir)
                    .map(|_| ())
                    .with_context(|| {
                        format!(
                            "Failed to selectively extract {} files from {}",
                            needed_paths.len(),
                            archive_path.display()
                        )
                    })
            }
        }
        _ => Ok(()),
    }
}

/// Find a file in a temp extraction directory by normalized path lookup.
fn find_file_in_temp_dir(temp_dir: &Path, file_path: &str) -> Option<PathBuf> {
    let normalized_target = paths::normalize_for_lookup(file_path);

    for entry in walkdir::WalkDir::new(temp_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
    {
        if let Ok(rel) = entry.path().strip_prefix(temp_dir) {
            let rel_str = rel.to_string_lossy();
            let normalized = paths::normalize_for_lookup(&rel_str);
            if normalized == normalized_target {
                return Some(entry.path().to_path_buf());
            }
        }
    }
    None
}

/// Read a single file from an archive without bulk extraction.
/// Works for ZIP (iterate to find entry) and BSA/BA2 (direct file access).
fn read_single_file_from_archive(
    archive_path: &Path,
    archive_type: ArchiveType,
    file_path: &str,
) -> Result<Vec<u8>> {
    let normalized_target = paths::normalize_for_lookup(file_path);

    match archive_type {
        ArchiveType::Zip => {
            let file = File::open(archive_path)
                .with_context(|| format!("Failed to open ZIP: {}", archive_path.display()))?;
            let mut archive = zip::ZipArchive::new(BufReader::new(file))
                .with_context(|| format!("Failed to read ZIP: {}", archive_path.display()))?;

            for i in 0..archive.len() {
                if let Ok(mut entry) = archive.by_index(i) {
                    let entry_name = entry.name().to_string();
                    let normalized = paths::normalize_for_lookup(&entry_name);
                    if normalized == normalized_target {
                        let mut data = Vec::with_capacity(entry.size() as usize);
                        std::io::Read::read_to_end(&mut entry, &mut data)?;
                        return Ok(data);
                    }
                }
            }
            anyhow::bail!("File not found in ZIP: {}", file_path)
        }
        ArchiveType::Tes3Bsa | ArchiveType::Bsa | ArchiveType::Ba2 => {
            bsa::extract_archive_file(archive_path, file_path)
        }
        _ => {
            anyhow::bail!("Cannot read individual files from archive type {:?}", archive_type)
        }
    }
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

    // Filter out textures already processed during install phase (DDS handler thread)
    let already_done = ctx.textures_processed_during_install
        .lock().expect("textures_processed lock")
        .clone();
    let pre_done = already_done.len();

    // Parse and group by archive
    let mut by_archive: HashMap<String, Vec<(i64, TransformedTextureDirective)>> = HashMap::new();
    let mut parse_failures = 0;

    for (id, json) in all_raw {
        if already_done.contains(&id) {
            completed.fetch_add(1, Ordering::Relaxed);
            pb.inc(1);
            continue;
        }
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

    if pre_done > 0 {
        eprintln!("{} textures already processed during install phase", pre_done);
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

    use crate::textures::{OutputFormat, TextureJob, process_texture_batch, process_texture_with_fallback};

    let total_archives = by_archive.len();
    let total_directives: usize = by_archive.values().map(|v| v.len()).sum();

    // ── Pass 1: Stage all texture source files to disk in parallel ──
    // Extract all needed source textures from all archives into a staging dir.
    // Archives are extracted in parallel so slow 7z/RAR don't block everything.
    let staging_dir = tempfile::tempdir_in(&ctx.config.output_dir)
        .context("Failed to create texture staging dir")?;

    pb.set_message("Staging texture source files...");
    pb.finish_and_clear();
    pb.reset();
    pb.set_length(total_archives as u64);
    pb.set_position(0);
    pb.enable_steady_tick(std::time::Duration::from_millis(100));

    eprintln!(
        "Staging {} textures from {} archives...",
        total_directives, total_archives
    );

    // Build flat list: (directive_id, directive, archive_hash, staged_path)
    // Each texture gets a unique file in staging_dir named by directive ID.
    struct StagedTexture {
        id: i64,
        directive: TransformedTextureDirective,
        staged_path: PathBuf,
    }

    let staged_textures: Mutex<Vec<StagedTexture>> = Mutex::new(Vec::new());
    let stage_skipped = AtomicUsize::new(0);
    let stage_failed = AtomicUsize::new(0);

    let archives_vec: Vec<_> = by_archive.into_iter().collect();

    // Extract archives in parallel — each archive writes needed textures to staging dir
    archives_vec.par_iter().for_each(|(archive_hash, directives)| {
        let archive_path = match ctx.get_archive_path(archive_hash) {
            Some(p) => p.clone(),
            None => {
                for (_, directive) in directives {
                    if output_dds_valid(ctx, &directive.to) {
                        stage_skipped.fetch_add(1, Ordering::Relaxed);
                    } else {
                        stage_failed.fetch_add(1, Ordering::Relaxed);
                    }
                }
                pb.inc(1);
                return;
            }
        };

        // Filter to directives that actually need processing
        let to_process: Vec<_> = directives
            .iter()
            .filter(|(_, d)| !output_dds_valid(ctx, &d.to))
            .collect();

        let skip_count = directives.len() - to_process.len();
        stage_skipped.fetch_add(skip_count, Ordering::Relaxed);

        if to_process.is_empty() {
            pb.inc(1);
            return;
        }

        let archive_name = archive_path.file_name().unwrap_or_default().to_string_lossy().to_string();
        let archive_type = detect_archive_type(&archive_path).unwrap_or(ArchiveType::Unknown);

        // For 7z/RAR: extract entire archive to temp dir, then copy needed files to staging
        let temp_extract = if matches!(archive_type, ArchiveType::SevenZ | ArchiveType::Rar | ArchiveType::Unknown) {
            let mut needed_paths = Vec::new();
            let mut seen_paths = HashSet::new();
            for (_, d) in &to_process {
                if let Some(path) = d.archive_hash_path.get(1) {
                    let normalized = paths::normalize_for_lookup(path);
                    if seen_paths.insert(normalized) {
                        needed_paths.push(path.as_str());
                    }
                }
            }
            match tempfile::tempdir_in(&ctx.config.output_dir) {
                Ok(td) => {
                    if let Err(e) = extract_to_temp_disk(&archive_path, &needed_paths, td.path()) {
                        pb.println(format!("WARN: Failed to extract {}: {}", archive_name, e));
                    }
                    Some(td)
                }
                Err(_) => None,
            }
        } else {
            None
        };

        // Handle nested BSAs: extract BSA files to temp
        let mut nested_bsa_temps: HashMap<String, tempfile::NamedTempFile> = HashMap::new();
        {
            let mut needed_bsas: HashMap<String, HashSet<String>> = HashMap::new();
            for (_, d) in &to_process {
                if d.archive_hash_path.len() >= 3 {
                    needed_bsas.entry(d.archive_hash_path[1].clone()).or_default()
                        .insert(d.archive_hash_path[2].clone());
                }
            }
            for bsa_path in needed_bsas.keys() {
                let bsa_data = if let Some(ref td) = temp_extract {
                    find_file_in_temp_dir(td.path(), bsa_path)
                        .and_then(|p| fs::read(&p).ok())
                } else {
                    read_single_file_from_archive(&archive_path, archive_type, bsa_path).ok()
                };
                if let Some(data) = bsa_data {
                    let suffix = if bsa_path.to_lowercase().ends_with(".ba2") { ".ba2" } else { ".bsa" };
                    if let Ok(temp_bsa) = tempfile::Builder::new()
                        .prefix(".clf3_bsa_").suffix(suffix)
                        .tempfile_in(&ctx.config.output_dir)
                    {
                        if fs::write(temp_bsa.path(), &data).is_ok() {
                            nested_bsa_temps.insert(bsa_path.clone(), temp_bsa);
                        }
                    }
                }
            }
        }

        // Stage each texture source file to disk
        for (id, directive) in &to_process {
            let source_data = if directive.archive_hash_path.len() == 2 {
                let path = &directive.archive_hash_path[1];
                if let Some(ref td) = temp_extract {
                    find_file_in_temp_dir(td.path(), path)
                        .and_then(|p| fs::read(&p).ok())
                } else {
                    read_single_file_from_archive(&archive_path, archive_type, path).ok()
                }
            } else if directive.archive_hash_path.len() >= 3 {
                let bsa_path = &directive.archive_hash_path[1];
                let file_in_bsa = &directive.archive_hash_path[2];
                nested_bsa_temps.get(bsa_path)
                    .and_then(|tb| bsa::extract_archive_file(tb.path(), file_in_bsa).ok())
            } else {
                None
            };

            if let Some(data) = source_data {
                let staged_path = staging_dir.path().join(format!("{}.dds", id));
                if fs::write(&staged_path, &data).is_ok() {
                    staged_textures.lock().expect("staged_textures").push(StagedTexture {
                        id: *id,
                        directive: directive.clone(),
                        staged_path,
                    });
                } else {
                    stage_failed.fetch_add(1, Ordering::Relaxed);
                }
            } else {
                stage_failed.fetch_add(1, Ordering::Relaxed);
            }
        }

        pb.inc(1);
        // temp_extract dropped here — archive temp dir cleaned up
    });

    let mut staged = staged_textures.into_inner().expect("staged_textures");
    let total_staged = staged.len();
    let total_skipped = stage_skipped.load(Ordering::Relaxed);
    let total_failed_stage = stage_failed.load(Ordering::Relaxed);
    skipped.fetch_add(total_skipped, Ordering::Relaxed);
    failed.fetch_add(total_failed_stage, Ordering::Relaxed);

    eprintln!(
        "Staged {} textures ({} skipped, {} failed to read)",
        total_staged, total_skipped, total_failed_stage
    );

    // ── Pass 2: Process all staged textures through GPU/CPU pipeline ──
    // Group by format like Radium — process each format section fully parallel.
    pb.finish_and_clear();
    pb.reset();
    pb.set_length(total_directives as u64);
    pb.set_position((total_skipped + total_failed_stage) as u64);
    pb.enable_steady_tick(std::time::Duration::from_millis(100));
    pb.set_message("Processing textures...");

    for _ in 0..total_skipped { reporter.directive_completed(); }
    for _ in 0..total_failed_stage { reporter.directive_completed(); }

    let failure_tracker = failure_tracker.clone();
    let reporter = reporter.clone();

    // Group by format
    let mut by_format: HashMap<String, Vec<StagedTexture>> = HashMap::new();
    for st in staged.drain(..) {
        let fmt_str = st.directive.image_state.format.to_uppercase();
        by_format.entry(fmt_str).or_default().push(st);
    }

    // Print format breakdown
    let mut format_summary: Vec<_> = by_format.iter().map(|(f, j)| (f.clone(), j.len())).collect();
    format_summary.sort_by(|a, b| b.1.cmp(&a.1));
    let summary_str = format_summary.iter()
        .map(|(f, n)| format!("{}: {}", f, n))
        .collect::<Vec<_>>()
        .join(", ");
    eprintln!("Processing textures by format [{}]", summary_str);

    // Initialize GPU for BC7
    let _ = crate::textures::init_gpu();

    // Process each format group
    for (fmt_str, jobs) in &by_format {
        let fmt = OutputFormat::from_str(fmt_str)
            .unwrap_or(if handlers::texture::is_fallback_mode() { OutputFormat::BC1 } else { OutputFormat::BC7 });

        if fmt == OutputFormat::BC7 {
            // BC7: parallel CPU prep → GPU batch encode
            pb.set_message(format!("BC7: {} textures (GPU+CPU)...", jobs.len()));

            let tex_jobs: Vec<TextureJob> = jobs.iter().filter_map(|st| {
                fs::read(&st.staged_path).ok().map(|data| TextureJob {
                    data,
                    width: st.directive.image_state.width,
                    height: st.directive.image_state.height,
                    format: OutputFormat::BC7,
                    id: Some(format!("{}", st.id)),
                })
            }).collect();

            let results = process_texture_batch(tex_jobs);

            for (st, (_job_id, result)) in jobs.iter().zip(results.into_iter()) {
                match result {
                    Ok(processed) => {
                        let output_path = ctx.resolve_output_path(&st.directive.to);
                        if let Err(e) = paths::ensure_parent_dirs(&output_path)
                            .and_then(|_| fs::write(&output_path, &processed.data))
                        {
                            failed.fetch_add(1, Ordering::Relaxed);
                            failure_tracker.record_failure("texture", &e.to_string());
                        } else {
                            completed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(e) => {
                        failed.fetch_add(1, Ordering::Relaxed);
                        failure_tracker.record_failure("texture", &format!("{:#}", e));
                        if failed.load(Ordering::Relaxed) <= 10 {
                            pb.println(format!("FAIL [{}] BC7 texture: {:#}", st.id, e));
                        }
                    }
                }
                pb.inc(1);
                reporter.directive_completed();
            }
        } else {
            // Non-BC7: full CPU parallelism (all cores, like Radium)
            pb.set_message(format!("{}: {} textures (CPU parallel)...", fmt.name(), jobs.len()));

            jobs.par_iter().for_each(|st| {
                let result: Result<()> = (|| {
                    let data = fs::read(&st.staged_path)
                        .with_context(|| format!("Failed to read staged texture {}", st.id))?;
                    let (processed, _) = process_texture_with_fallback(
                        &data, st.directive.image_state.width, st.directive.image_state.height, fmt,
                    )?;
                    let output_path = ctx.resolve_output_path(&st.directive.to);
                    paths::ensure_parent_dirs(&output_path)?;
                    fs::write(&output_path, &processed.data)?;
                    Ok(())
                })();
                match result {
                    Ok(()) => { completed.fetch_add(1, Ordering::Relaxed); }
                    Err(e) => {
                        failed.fetch_add(1, Ordering::Relaxed);
                        failure_tracker.record_failure("texture", &e.to_string());
                    }
                }
                pb.inc(1);
                reporter.directive_completed();
            });
        }
    }
    // staging_dir dropped here — all staged files cleaned up

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

    // Sort by file count (smallest first) for faster feedback.
    directives.sort_by_key(|(_, d)| d.file_states.len());

    let workers = ctx.config.max_parallel_bsa_archives.max(1);
    eprintln!(
        "Processing {} BSA/BA2 archives one-at-a-time (configured workers: {}, inner compression stays fully threaded)",
        directives.len(),
        workers
    );

    let process_one = |id: i64, directive: CreateBSADirective| {
        if handlers::output_bsa_valid(ctx, &directive) {
            skipped.fetch_add(1, Ordering::Relaxed);
            pb.inc(1);
            reporter.directive_completed();
            return;
        }

        let bsa_name = std::path::Path::new(&directive.to)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| directive.to.clone());

        let archive_type = match &directive.state {
            crate::modlist::BSAState::BSA(_) => "BSA",
            crate::modlist::BSAState::BA2(_) => "BA2",
        };

        match handlers::handle_create_bsa(ctx, &directive) {
            Ok(()) => {
                completed.fetch_add(1, Ordering::Relaxed);
                pb.println(format!(
                    "Created {}: {} ({} files)",
                    archive_type,
                    bsa_name,
                    directive.file_states.len()
                ));
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
    };

    // Always process one archive at a time. This avoids nested rayon pool throttling
    // while still letting BSA/BA2 builders use full parallelism internally.
    for (id, directive) in directives {
        process_one(id, directive);
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
