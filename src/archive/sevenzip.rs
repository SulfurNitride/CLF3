//! Archive extraction with native Rust crates and 7z binary fallback.
//!
//! This module provides a unified interface for extracting archive formats
//! (ZIP, 7z, RAR) using native Rust crates where possible, with fallback
//! to the 7zz binary for edge cases (e.g., RAR5 reference records).
//!
//! # Native crates used
//!
//! - **ZIP**: `zip` crate (already used for .wabbajack files)
//! - **7z**: `sevenz-rust2` - pure Rust, multi-threaded LZMA2 decompression
//! - **RAR**: `unrar` crate with 7z binary fallback for reference records
//!
//! # Archive Ordering
//!
//! For optimal extraction performance, archives should be processed in order:
//! 1. ZIP files (fastest - random access)
//! 2. RAR files (medium - can skip entries)
//! 3. 7z non-solid (medium - random access within blocks)
//! 4. 7z solid (slowest - requires sequential decompression)
//!
//! # Usage
//!
//! ```ignore
//! use clf3::archive::sevenzip::{list_archive, extract_file_case_insensitive, extract_all};
//!
//! let entries = list_archive("archive.7z")?;
//! let data = extract_file_case_insensitive("archive.7z", "path/in/archive.txt")?;
//! extract_all("archive.7z", "/output/dir")?;
//! ```

use anyhow::{bail, Context, Result};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Read};
use std::path::{Path, PathBuf};
use std::process::Command;

use std::sync::Mutex;

/// Windows file attribute flag for reparse points (symlinks, junctions).
const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;

// ============================================================================
// Archive listing cache — pre-lists archives into SQLite so extraction
// workers can resolve paths without spawning 7zz/reading archive headers.
// ============================================================================

/// SQLite-backed cache for archive file listings.
/// Stores normalized_path → actual_path mappings per archive.
pub struct ArchiveListingCache {
    conn: Mutex<rusqlite::Connection>,
}

impl ArchiveListingCache {
    /// Create a new in-memory listing cache.
    pub fn new() -> Result<Self> {
        let conn = rusqlite::Connection::open_in_memory()
            .context("Failed to create archive listing cache")?;
        conn.execute_batch(
            "CREATE TABLE archive_paths (
                archive TEXT NOT NULL,
                normalized TEXT NOT NULL,
                actual TEXT NOT NULL
            );
            CREATE INDEX idx_archive_paths ON archive_paths(archive, normalized);",
        )
        .context("Failed to create cache tables")?;
        // Use WAL mode and relaxed sync for speed — this is a transient cache
        conn.pragma_update(None, "journal_mode", "WAL").ok();
        conn.pragma_update(None, "synchronous", "OFF").ok();
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Pre-list an archive and store its path mappings in the cache.
    pub fn populate(&self, archive_path: &Path) -> Result<()> {
        let entries = list_archive(archive_path)?;
        let lookup = build_path_lookup(&entries);
        let archive_key = archive_path.to_string_lossy().to_string();

        let conn = self.conn.lock().expect("listing cache lock");
        let mut stmt = conn
            .prepare_cached(
                "INSERT INTO archive_paths (archive, normalized, actual) VALUES (?1, ?2, ?3)",
            )
            .context("Failed to prepare cache insert")?;

        for (normalized, actual) in &lookup {
            stmt.execute(rusqlite::params![archive_key, normalized, actual])?;
        }
        Ok(())
    }

    /// Resolve multiple files at once, returning the actual paths.
    pub fn resolve_files(
        &self,
        archive_path: &Path,
        files: &[String],
    ) -> Result<Vec<String>> {
        let archive_key = archive_path.to_string_lossy().to_string();
        let conn = self.conn.lock().expect("listing cache lock");
        let mut stmt = conn
            .prepare_cached(
                "SELECT actual FROM archive_paths WHERE archive = ?1 AND normalized = ?2",
            )
            .context("Failed to prepare cache query")?;

        let mut resolved = Vec::new();
        let mut seen = std::collections::HashSet::new();

        for file in files {
            let normalized = normalize_path(file);
            let actual: String = stmt
                .query_row(rusqlite::params![archive_key, normalized], |row| row.get(0))
                .with_context(|| {
                    format!(
                        "File '{}' not found in archive listing cache for '{}'",
                        file, archive_key
                    )
                })?;

            if seen.insert(actual.clone()) {
                resolved.push(actual);
            }
        }
        Ok(resolved)
    }

    /// Check if an archive has been listed.
    pub fn has_archive(&self, archive_path: &Path) -> bool {
        let archive_key = archive_path.to_string_lossy().to_string();
        let conn = self.conn.lock().expect("listing cache lock");
        conn.query_row(
            "SELECT 1 FROM archive_paths WHERE archive = ?1 LIMIT 1",
            rusqlite::params![archive_key],
            |_| Ok(()),
        )
        .is_ok()
    }
}

/// Archive type detected by magic bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveType {
    /// ZIP archive (PK signature)
    Zip,
    /// 7z archive (7z signature)
    SevenZ,
    /// RAR archive (Rar! signature)
    Rar,
    /// Bethesda BSA archive
    Bsa,
    /// Bethesda BA2 archive
    Ba2,
    /// Unknown/unsupported format
    Unknown,
}

/// Detect archive type by reading magic bytes.
///
/// Uses magic byte detection rather than file extension to handle
/// mislabeled archives (e.g., a `.zip` that's actually a RAR file).
pub fn detect_archive_type(path: &Path) -> Result<ArchiveType> {
    let mut file =
        File::open(path).with_context(|| format!("Failed to open file: {}", path.display()))?;

    let mut magic = [0u8; 8];
    let bytes_read = file.read(&mut magic).unwrap_or(0);

    if bytes_read < 4 {
        return Ok(ArchiveType::Unknown);
    }

    // ZIP: PK\x03\x04 or PK\x05\x06 (empty) or PK\x07\x08 (spanned)
    if magic[0..2] == [0x50, 0x4B] {
        return Ok(ArchiveType::Zip);
    }

    // RAR: Rar!\x1A\x07\x00 (RAR4) or Rar!\x1A\x07\x01\x00 (RAR5)
    if magic[0..4] == [0x52, 0x61, 0x72, 0x21] {
        return Ok(ArchiveType::Rar);
    }

    // 7z: 7z\xBC\xAF\x27\x1C
    if bytes_read >= 6 && magic[0..6] == [0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C] {
        return Ok(ArchiveType::SevenZ);
    }

    // BSA: BSA\x00
    if magic[0..4] == [0x42, 0x53, 0x41, 0x00] {
        return Ok(ArchiveType::Bsa);
    }

    // BA2: BTDX
    if magic[0..4] == [0x42, 0x54, 0x44, 0x58] {
        return Ok(ArchiveType::Ba2);
    }

    Ok(ArchiveType::Unknown)
}

/// Information about a file in an archive.
#[derive(Debug, Clone)]
pub struct ArchiveEntry {
    /// Path within the archive (forward slashes, case-preserved)
    pub path: String,
    /// Uncompressed size in bytes
    pub size: u64,
    /// Whether this is a directory (used by lib crate consumers)
    #[allow(dead_code)]
    pub is_dir: bool,
}

// ============================================================================
// Public API - dispatches to native crates by archive type
// ============================================================================

/// List all files in an archive.
///
/// Dispatches to native crate based on archive type, with 7z binary fallback.
pub fn list_archive(archive_path: &Path) -> Result<Vec<ArchiveEntry>> {
    let archive_type = detect_archive_type(archive_path).unwrap_or(ArchiveType::Unknown);

    match archive_type {
        ArchiveType::Zip => list_zip(archive_path)
            .or_else(|_| list_archive_7z_binary(archive_path)),
        ArchiveType::SevenZ => list_7z_native(archive_path)
            .or_else(|_| list_archive_7z_binary(archive_path)),
        ArchiveType::Rar => list_rar(archive_path)
            .or_else(|_| list_archive_7z_binary(archive_path)),
        _ => list_archive_7z_binary(archive_path),
    }
}

/// Extract a single file from an archive to memory.
pub fn extract_file(archive_path: &Path, file_path: &str) -> Result<Vec<u8>> {
    let archive_type = detect_archive_type(archive_path).unwrap_or(ArchiveType::Unknown);

    match archive_type {
        ArchiveType::Zip => extract_zip_file(archive_path, file_path)
            .or_else(|_| extract_file_7z_binary(archive_path, file_path)),
        // Match Wabbajack behavior: use external 7z process for 7z extraction.
        ArchiveType::SevenZ => extract_file_7z_binary(archive_path, file_path),
        ArchiveType::Rar => extract_rar_file(archive_path, file_path)
            .or_else(|_| extract_file_7z_binary(archive_path, file_path)),
        _ => extract_file_7z_binary(archive_path, file_path),
    }
}

/// Extract a single file from an archive using case-insensitive matching.
pub fn extract_file_case_insensitive(archive_path: &Path, file_path: &str) -> Result<Vec<u8>> {
    let target_normalized = normalize_path(file_path);

    // List archive and find matching entry
    let entries = list_archive(archive_path)?;

    for entry in &entries {
        if normalize_path(&entry.path) == target_normalized {
            return extract_file(archive_path, &entry.path);
        }
    }

    bail!(
        "File '{}' not found in archive '{}'",
        file_path,
        archive_path.display()
    );
}

/// Extract multiple files from an archive to a directory.
pub fn extract_files(archive_path: &Path, files: &[&str], output_dir: &Path) -> Result<()> {
    if files.is_empty() {
        return Ok(());
    }

    let archive_type = detect_archive_type(archive_path).unwrap_or(ArchiveType::Unknown);

    match archive_type {
        ArchiveType::Zip => extract_zip_files(archive_path, files, output_dir)
            .or_else(|_| extract_files_7z_binary(archive_path, files, output_dir)),
        // Match Wabbajack behavior: use external 7z process for 7z extraction.
        ArchiveType::SevenZ => extract_files_7z_binary(archive_path, files, output_dir),

        ArchiveType::Rar => extract_rar_files(archive_path, files, output_dir)
            .or_else(|_| extract_files_7z_binary(archive_path, files, output_dir)),
        _ => extract_files_7z_binary(archive_path, files, output_dir),
    }
}

/// Extract multiple files using case-insensitive matching.
///
/// Resolves archive paths once via `list_archive`, then performs a single
/// multi-file extraction call.
pub fn extract_files_case_insensitive(
    archive_path: &Path,
    files: &[String],
    output_dir: &Path,
) -> Result<usize> {
    if files.is_empty() {
        return Ok(0);
    }

    let entries = list_archive(archive_path)?;
    let lookup = build_path_lookup(&entries);

    let mut resolved = Vec::new();
    let mut seen = std::collections::HashSet::new();

    for file in files {
        let normalized = normalize_path(file);
        let actual = lookup.get(&normalized).with_context(|| {
            format!(
                "File '{}' not found in archive '{}'",
                file,
                archive_path.display()
            )
        })?;

        if seen.insert(actual.clone()) {
            resolved.push(actual.clone());
        }
    }

    let resolved_refs: Vec<&str> = resolved.iter().map(|s| s.as_str()).collect();
    extract_files(archive_path, &resolved_refs, output_dir)?;
    Ok(resolved_refs.len())
}

/// Extract multiple files using case-insensitive matching with a pre-populated listing cache.
///
/// Skips the `list_archive` call entirely — path resolution comes from the cache.
/// Falls back to `extract_files_case_insensitive` if the archive isn't cached.
pub fn extract_files_cached(
    archive_path: &Path,
    files: &[String],
    output_dir: &Path,
    cache: &ArchiveListingCache,
) -> Result<usize> {
    if files.is_empty() {
        return Ok(0);
    }

    if !cache.has_archive(archive_path) {
        return extract_files_case_insensitive(archive_path, files, output_dir);
    }

    let resolved = cache.resolve_files(archive_path, files)?;
    let resolved_refs: Vec<&str> = resolved.iter().map(|s| s.as_str()).collect();
    extract_files(archive_path, &resolved_refs, output_dir)?;
    Ok(resolved_refs.len())
}

/// Extract all files from an archive to a directory.
pub fn extract_all(archive_path: &Path, output_dir: &Path) -> Result<usize> {
    extract_all_with_threads(archive_path, output_dir, None)
}

/// Extract all files from an archive to a directory with controlled threading.
///
/// # Arguments
/// * `threads` - Number of threads:
///   - `None` or `Some(0)` = auto (all available cores)
///   - `Some(1)` = single-threaded
///   - `Some(n)` = use n threads
pub fn extract_all_with_threads(
    archive_path: &Path,
    output_dir: &Path,
    threads: Option<usize>,
) -> Result<usize> {
    let archive_type = detect_archive_type(archive_path).unwrap_or(ArchiveType::Unknown);

    match archive_type {
        ArchiveType::Zip => extract_zip_all(archive_path, output_dir)
            .or_else(|e| {
                tracing::warn!("Native ZIP extraction failed, falling back to 7z binary: {}", e);
                extract_all_7z_binary(archive_path, output_dir, threads)
            }),
        // Match Wabbajack behavior: use external 7z process for 7z extraction.
        ArchiveType::SevenZ => extract_all_7z_binary(archive_path, output_dir, threads),

        ArchiveType::Rar => extract_rar_all(archive_path, output_dir)
            .or_else(|e| {
                tracing::warn!("Native RAR extraction failed, falling back to 7z binary: {}", e);
                extract_all_7z_binary(archive_path, output_dir, threads)
            }),
        _ => extract_all_7z_binary(archive_path, output_dir, threads),
    }
}

/// Get the path to the 7z binary (used as fallback for RAR).
pub fn get_7z_path() -> Result<PathBuf> {
    // Try relative to executable first
    if let Ok(exe_path) = std::env::current_exe() {
        if let Some(exe_dir) = exe_path.parent() {
            for name in &["bin/7zz", "bin/7z.exe", "7zz"] {
                let bin_path = exe_dir.join(name);
                if bin_path.exists() {
                    return Ok(bin_path);
                }
            }
        }
    }

    // Try relative to current directory
    let cwd_path = PathBuf::from("bin/7zz");
    if cwd_path.exists() {
        return Ok(cwd_path);
    }

    // Try system PATH
    for name in &["7zz", "7z"] {
        if let Ok(output) = Command::new("which").arg(name).output() {
            if output.status.success() {
                let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
                if !path.is_empty() {
                    return Ok(PathBuf::from(path));
                }
            }
        }
    }

    bail!("7z binary not found. Please install p7zip or place 7zz in the bin/ directory.")
}

/// Get the path to an `innoextract` binary for Inno Setup installer fallback.
pub fn get_innoextract_path() -> Result<PathBuf> {
    // Try relative to executable first
    if let Ok(exe_path) = std::env::current_exe() {
        if let Some(exe_dir) = exe_path.parent() {
            for name in &["bin/innoextract", "innoextract"] {
                let bin_path = exe_dir.join(name);
                if bin_path.exists() {
                    return Ok(bin_path);
                }
            }
        }
    }

    // Try relative to current directory
    let cwd_path = PathBuf::from("bin/innoextract");
    if cwd_path.exists() {
        return Ok(cwd_path);
    }

    // Try system PATH
    if let Ok(output) = Command::new("which").arg("innoextract").output() {
        if output.status.success() {
            let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !path.is_empty() {
                return Ok(PathBuf::from(path));
            }
        }
    }

    bail!("innoextract binary not found. Please install innoextract or place it in bin/.")
}

/// Normalize a path for case-insensitive comparison.
fn normalize_path(path: &str) -> String {
    path.to_lowercase()
        .replace('\\', "/")
        .trim_matches('/')
        .to_string()
}

/// Build a case-insensitive lookup map from archive entries.
pub fn build_path_lookup(entries: &[ArchiveEntry]) -> HashMap<String, String> {
    let mut lookup = HashMap::new();
    for entry in entries {
        let normalized = normalize_path(&entry.path);
        lookup.insert(normalized, entry.path.clone());
    }
    lookup
}

// ============================================================================
// Native ZIP implementation (using `zip` crate)
// ============================================================================

/// List files in a ZIP archive.
fn list_zip(archive_path: &Path) -> Result<Vec<ArchiveEntry>> {
    let file = File::open(archive_path)
        .with_context(|| format!("Failed to open ZIP: {}", archive_path.display()))?;
    let reader = BufReader::new(file);
    let mut archive = zip::ZipArchive::new(reader)
        .with_context(|| format!("Failed to read ZIP: {}", archive_path.display()))?;

    let mut entries = Vec::new();
    for i in 0..archive.len() {
        let entry = archive.by_index_raw(i)?;
        if !entry.is_dir() {
            entries.push(ArchiveEntry {
                path: entry.name().to_string(),
                size: entry.size(),
                is_dir: false,
            });
        }
    }
    Ok(entries)
}

/// Extract a single file from a ZIP archive to memory (case-insensitive).
fn extract_zip_file(archive_path: &Path, file_path: &str) -> Result<Vec<u8>> {
    let file = File::open(archive_path)?;
    let reader = BufReader::new(file);
    let mut archive = zip::ZipArchive::new(reader)?;
    let target = normalize_path(file_path);

    for i in 0..archive.len() {
        let normalized = {
            let entry = archive.by_index_raw(i)?;
            normalize_path(entry.name())
        };

        if normalized == target {
            let mut entry = archive.by_index(i)?;
            let mut data = Vec::with_capacity(entry.size() as usize);
            entry.read_to_end(&mut data)?;
            return Ok(data);
        }
    }

    bail!(
        "File '{}' not found in ZIP '{}'",
        file_path,
        archive_path.display()
    );
}

/// Extract specific files from a ZIP archive to a directory.
fn extract_zip_files(archive_path: &Path, files: &[&str], output_dir: &Path) -> Result<()> {
    let file = File::open(archive_path)?;
    let reader = BufReader::new(file);
    let mut archive = zip::ZipArchive::new(reader)?;

    let target_set: std::collections::HashSet<String> =
        files.iter().map(|f| normalize_path(f)).collect();

    fs::create_dir_all(output_dir)?;

    for i in 0..archive.len() {
        let normalized = {
            let entry = archive.by_index_raw(i)?;
            normalize_path(entry.name())
        };

        if target_set.contains(&normalized) {
            let mut entry = archive.by_index(i)?;
            let entry_path = match entry.enclosed_name() {
                Some(p) => p.to_path_buf(),
                None => continue,
            };

            if entry.is_dir() {
                continue;
            }

            let output_path = output_dir.join(&entry_path);
            if let Some(parent) = output_path.parent() {
                fs::create_dir_all(parent)?;
            }
            let mut outfile = BufWriter::with_capacity(65536, File::create(&output_path)?);
            std::io::copy(&mut entry, &mut outfile)?;
        }
    }

    Ok(())
}

/// Extract all files from a ZIP archive to a directory.
fn extract_zip_all(archive_path: &Path, output_dir: &Path) -> Result<usize> {
    let file = File::open(archive_path)?;
    let reader = BufReader::new(file);
    let mut archive = zip::ZipArchive::new(reader)?;

    fs::create_dir_all(output_dir)?;
    let mut count = 0usize;

    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;
        let entry_path = match entry.enclosed_name() {
            Some(p) => p.to_path_buf(),
            None => continue,
        };

        let output_path = output_dir.join(&entry_path);

        if entry.is_dir() {
            fs::create_dir_all(&output_path)?;
        } else {
            if let Some(parent) = output_path.parent() {
                fs::create_dir_all(parent)?;
            }
            let mut outfile = BufWriter::with_capacity(65536, File::create(&output_path)?);
            std::io::copy(&mut entry, &mut outfile)?;
            count += 1;
        }
    }

    Ok(count)
}

// ============================================================================
// Native 7z implementation (using `sevenz-rust2` crate)
// ============================================================================

/// List files in a 7z archive using native Rust crate.
fn list_7z_native(archive_path: &Path) -> Result<Vec<ArchiveEntry>> {
    let archive = sevenz_rust2::Archive::open(archive_path)
        .with_context(|| format!("Failed to open 7z: {}", archive_path.display()))?;

    let mut entries = Vec::new();
    for file_entry in &archive.files {
        if file_entry.is_directory {
            continue;
        }
        // Skip reparse point entries
        if file_entry.has_windows_attributes
            && (file_entry.windows_attributes & FILE_ATTRIBUTE_REPARSE_POINT) != 0
        {
            continue;
        }
        // Include files with no stream (empty/zero-byte files)
        entries.push(ArchiveEntry {
            path: file_entry.name.clone(),
            size: file_entry.size,
            is_dir: false,
        });
    }
    Ok(entries)
}

// ============================================================================
// Native RAR implementation (using `unrar` crate)
// ============================================================================

/// List files in a RAR archive.
fn list_rar(archive_path: &Path) -> Result<Vec<ArchiveEntry>> {
    let archive = unrar::Archive::new(archive_path)
        .open_for_listing()
        .map_err(|e| anyhow::anyhow!("Failed to open RAR for listing: {:?}", e))?;

    let mut entries = Vec::new();
    for entry in archive {
        match entry {
            Ok(header) => {
                if header.is_file() {
                    entries.push(ArchiveEntry {
                        path: header.filename.to_string_lossy().to_string(),
                        size: header.unpacked_size,
                        is_dir: false,
                    });
                }
            }
            Err(e) => {
                tracing::debug!("RAR listing entry error: {:?}", e);
            }
        }
    }
    Ok(entries)
}

/// Extract a single file from a RAR archive to memory.
fn extract_rar_file(archive_path: &Path, file_path: &str) -> Result<Vec<u8>> {
    let target = normalize_path(file_path);

    let mut archive = unrar::Archive::new(archive_path)
        .open_for_processing()
        .map_err(|e| anyhow::anyhow!("Failed to open RAR: {:?}", e))?;

    while let Some(header) = archive
        .read_header()
        .map_err(|e| anyhow::anyhow!("RAR read_header error: {:?}", e))?
    {
        let entry_path = normalize_path(&header.entry().filename.to_string_lossy());

        if entry_path == target && header.entry().is_file() {
            let (data, _next) = header
                .read()
                .map_err(|e| anyhow::anyhow!("RAR read error: {:?}", e))?;
            return Ok(data);
        } else {
            archive = header
                .skip()
                .map_err(|e| anyhow::anyhow!("RAR skip error: {:?}", e))?;
        }
    }

    bail!(
        "File '{}' not found in RAR '{}'",
        file_path,
        archive_path.display()
    );
}

/// Extract specific files from a RAR archive to a directory.
fn extract_rar_files(archive_path: &Path, files: &[&str], output_dir: &Path) -> Result<()> {
    let target_set: std::collections::HashSet<String> =
        files.iter().map(|f| normalize_path(f)).collect();

    fs::create_dir_all(output_dir)?;

    let mut archive = unrar::Archive::new(archive_path)
        .open_for_processing()
        .map_err(|e| anyhow::anyhow!("Failed to open RAR: {:?}", e))?;

    while let Some(header) = archive
        .read_header()
        .map_err(|e| anyhow::anyhow!("RAR read_header error: {:?}", e))?
    {
        let entry_path = normalize_path(&header.entry().filename.to_string_lossy());

        if header.entry().is_file() && target_set.contains(&entry_path) {
            archive = header
                .extract_with_base(output_dir)
                .map_err(|e| {
                    // Check for RAR5 reference record errors
                    if e.code == unrar::error::Code::EReference {
                        anyhow::anyhow!("RAR5 reference record - falling back to 7z binary")
                    } else {
                        anyhow::anyhow!("RAR extract error: {:?}", e)
                    }
                })?;
        } else {
            archive = header
                .skip()
                .map_err(|e| anyhow::anyhow!("RAR skip error: {:?}", e))?;
        }
    }

    Ok(())
}

/// Extract all files from a RAR archive to a directory.
fn extract_rar_all(archive_path: &Path, output_dir: &Path) -> Result<usize> {
    fs::create_dir_all(output_dir)?;

    let mut archive = unrar::Archive::new(archive_path)
        .open_for_processing()
        .map_err(|e| anyhow::anyhow!("Failed to open RAR: {:?}", e))?;

    let mut count = 0usize;

    while let Some(header) = archive
        .read_header()
        .map_err(|e| anyhow::anyhow!("RAR read_header error: {:?}", e))?
    {
        if header.entry().is_file() {
            archive = header
                .extract_with_base(output_dir)
                .map_err(|e| {
                    if e.code == unrar::error::Code::EReference {
                        anyhow::anyhow!("RAR5 reference record - falling back to 7z binary")
                    } else {
                        anyhow::anyhow!("RAR extract error: {:?}", e)
                    }
                })?;
            count += 1;
        } else {
            archive = header
                .skip()
                .map_err(|e| anyhow::anyhow!("RAR skip error: {:?}", e))?;
        }
    }

    Ok(count)
}

// ============================================================================
// 7z binary fallback (kept for RAR edge cases and unknown formats)
// ============================================================================

/// List files using 7z binary.
fn list_archive_7z_binary(archive_path: &Path) -> Result<Vec<ArchiveEntry>> {
    let sz_path = get_7z_path()?;

    let output = Command::new(&sz_path)
        .arg("l")
        .arg("-slt")
        .arg("-ba")
        .arg("-scsUTF-8")
        .arg(archive_path)
        .output()
        .with_context(|| format!("Failed to run 7z list on {}", archive_path.display()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        if should_try_innoextract_fallback(archive_path, &stderr, &stdout) {
            tracing::warn!(
                "7z could not list {}, trying innoextract fallback",
                archive_path.display()
            );
            return list_archive_innoextract(archive_path);
        }
        bail!("7z list failed: {}", stderr);
    }

    parse_7z_list(&output.stdout)
}

/// Extract a single file using 7z binary.
fn extract_file_7z_binary(archive_path: &Path, file_path: &str) -> Result<Vec<u8>> {
    let sz_path = get_7z_path()?;
    let normalized_path = file_path.replace('\\', "/");

    let output = Command::new(&sz_path)
        .arg("e")
        .arg("-so")
        .arg("-y")
        .arg("-spd")
        .arg("-scsUTF-8")
        .arg("-mmt=2")
        .arg(archive_path)
        .arg(&normalized_path)
        .output()
        .with_context(|| {
            format!(
                "Failed to extract '{}' from {}",
                file_path,
                archive_path.display()
            )
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        if should_try_innoextract_fallback(archive_path, &stderr, &stdout) {
            tracing::warn!(
                "7z could not extract '{}' from {}, trying innoextract fallback",
                file_path,
                archive_path.display()
            );
            return extract_file_innoextract(archive_path, file_path);
        }
        bail!(
            "7z extract failed for '{}' in {}: {}",
            file_path,
            archive_path.display(),
            stderr
        );
    }

    if output.stdout.is_empty() {
        bail!(
            "7z returned no data for '{}' in {} - file not found in archive",
            file_path,
            archive_path.display()
        );
    }

    Ok(output.stdout)
}

/// Extract specific files using 7z binary.
fn extract_files_7z_binary(archive_path: &Path, files: &[&str], output_dir: &Path) -> Result<()> {
    if files.is_empty() {
        return Ok(());
    }

    let sz_path = get_7z_path()?;
    fs::create_dir_all(output_dir)?;

    let mut cmd = Command::new(&sz_path);
    cmd.arg("x")
        .arg("-y")
        .arg("-aoa")
        .arg("-scsUTF-8")
        .arg("-mmt=2")
        .arg(format!("-o{}", output_dir.display()))
        .arg(archive_path)
        .arg("--");

    for file in files {
        cmd.arg(file);
    }

    let output = cmd
        .output()
        .with_context(|| format!("Failed to extract files from {}", archive_path.display()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        let reparse_paths = parse_incorrect_reparse_paths(&stderr);

        if !reparse_paths.is_empty() {
            let mut retry = Command::new(&sz_path);
            retry.arg("x").arg("-y").arg("-aoa").arg("-scsUTF-8")
                .arg("-mmt=2")
                .arg(format!("-o{}", output_dir.display()));

            for path in &reparse_paths {
                retry.arg(format!("-x!{}", path));
            }
            retry.arg(archive_path).arg("--");
            for file in files {
                retry.arg(file);
            }

            let retry_output = retry.output()?;
            if retry_output.status.success() {
                return Ok(());
            }

            let retry_stderr = String::from_utf8_lossy(&retry_output.stderr);
            bail!(
                "7z extract failed for {}: {}; retry also failed: {}",
                archive_path.display(),
                stderr,
                retry_stderr
            );
        }

        if should_try_innoextract_fallback(archive_path, &stderr, &stdout) {
            tracing::warn!(
                "7z could not extract selected files from {}, trying innoextract fallback",
                archive_path.display()
            );
            return extract_files_innoextract(archive_path, files, output_dir);
        }

        bail!(
            "7z extract failed for {}: {}",
            archive_path.display(),
            stderr
        );
    }

    Ok(())
}

/// Extract all files using 7z binary.
fn extract_all_7z_binary(
    archive_path: &Path,
    output_dir: &Path,
    threads: Option<usize>,
) -> Result<usize> {
    let sz_path = get_7z_path()?;
    fs::create_dir_all(output_dir)?;

    let mut cmd = Command::new(&sz_path);
    cmd.arg("x").arg("-y").arg("-aoa").arg("-scsUTF-8");

    match threads {
        Some(n) if n >= 1 => {
            cmd.arg(format!("-mmt={}", n.max(2)));
        }
        _ => {
            cmd.arg("-mmt=on");
        }
    }

    cmd.arg(format!("-o{}", output_dir.display()))
        .arg(archive_path);

    let output = cmd
        .output()
        .with_context(|| format!("Failed to extract {}", archive_path.display()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        let reparse_paths = parse_incorrect_reparse_paths(&stderr);

        if !reparse_paths.is_empty() {
            let mut retry = Command::new(&sz_path);
            retry.arg("x").arg("-y").arg("-aoa").arg("-scsUTF-8");

            match threads {
                Some(n) if n >= 1 => {
                    retry.arg(format!("-mmt={}", n.max(2)));
                }
                _ => {
                    retry.arg("-mmt=on");
                }
            }

            retry.arg(format!("-o{}", output_dir.display()));
            for path in &reparse_paths {
                retry.arg(format!("-x!{}", path));
            }
            retry.arg(archive_path);

            let retry_output = retry.output()?;
            if retry_output.status.success() {
                let count = walkdir::WalkDir::new(output_dir)
                    .into_iter()
                    .filter_map(|e| e.ok())
                    .filter(|e| e.file_type().is_file())
                    .count();
                return Ok(count);
            }

            let retry_stderr = String::from_utf8_lossy(&retry_output.stderr);
            bail!(
                "7z extract all failed for {}: {}; retry also failed: {}",
                archive_path.display(),
                stderr,
                retry_stderr
            );
        }

        if should_try_innoextract_fallback(archive_path, &stderr, &stdout) {
            tracing::warn!(
                "7z could not extract {}, trying innoextract fallback",
                archive_path.display()
            );
            return extract_all_innoextract(archive_path, output_dir);
        }

        bail!(
            "7z extract all failed for {}: {}",
            archive_path.display(),
            stderr
        );
    }

    let count = walkdir::WalkDir::new(output_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .count();

    Ok(count)
}

// ============================================================================
// Internal helpers
// ============================================================================

/// Parse 7z technical listing output into ArchiveEntry structs.
fn parse_7z_list(output: &[u8]) -> Result<Vec<ArchiveEntry>> {
    let mut entries = Vec::new();
    let mut current: HashMap<String, String> = HashMap::new();

    for line in BufReader::new(output).lines() {
        let line = line?;
        let line = line.trim();

        if line.is_empty() {
            if let Some(path) = current.get("Path") {
                let is_dir = current.get("Folder").map(|v| v == "+").unwrap_or(false);
                let size = current
                    .get("Size")
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);

                if !is_dir && !path.is_empty() {
                    entries.push(ArchiveEntry {
                        path: path.clone(),
                        size,
                        is_dir: false,
                    });
                }
            }
            current.clear();
        } else if let Some((key, value)) = line.split_once(" = ") {
            current.insert(key.to_string(), value.to_string());
        }
    }

    // Handle last entry if no trailing newline
    if let Some(path) = current.get("Path") {
        let is_dir = current.get("Folder").map(|v| v == "+").unwrap_or(false);
        let size = current
            .get("Size")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        if !is_dir && !path.is_empty() {
            entries.push(ArchiveEntry {
                path: path.clone(),
                size,
                is_dir: false,
            });
        }
    }

    Ok(entries)
}

/// Parse "Incorrect reparse stream" paths from 7z stderr output.
fn parse_incorrect_reparse_paths(stderr: &str) -> Vec<String> {
    let mut paths = Vec::new();
    let mut seen = std::collections::HashSet::new();

    for line in stderr.lines() {
        if !line.contains("Incorrect reparse stream") {
            continue;
        }

        if let Some(path) = line.rsplit(" : ").next() {
            let trimmed = path.trim();
            if !trimmed.is_empty() && seen.insert(trimmed.to_string()) {
                paths.push(trimmed.to_string());
            }
        }
    }

    paths
}

fn should_try_innoextract_fallback(archive_path: &Path, stderr: &str, stdout: &str) -> bool {
    let is_exe = archive_path
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.eq_ignore_ascii_case("exe"))
        .unwrap_or(false);
    if !is_exe {
        return false;
    }

    let haystack = format!("{}\n{}", stderr, stdout).to_ascii_lowercase();
    haystack.contains("cannot open the file as archive")
}

fn parse_inno_size(size_text: &str) -> Option<u64> {
    // Examples: "558 B", "24.5 KiB", "1.2 MiB"
    let mut parts = size_text.split_whitespace();
    let value = parts.next()?.parse::<f64>().ok()?;
    let unit = parts.next().unwrap_or("B");
    let mult = match unit {
        "B" => 1.0,
        "KiB" => 1024.0,
        "MiB" => 1024.0 * 1024.0,
        "GiB" => 1024.0 * 1024.0 * 1024.0,
        _ => 1.0,
    };
    Some((value * mult).round() as u64)
}

fn parse_inno_list(output: &str) -> Vec<ArchiveEntry> {
    let mut entries = Vec::new();
    for line in output.lines() {
        let trimmed = line.trim();
        if !trimmed.starts_with("- \"") {
            continue;
        }

        let after_prefix = &trimmed[3..];
        let Some(path_end) = after_prefix.find('"') else {
            continue;
        };
        let path = &after_prefix[..path_end];
        if path.ends_with('/') {
            continue;
        }

        // Best-effort parse of the "(SIZE UNIT)" segment.
        let size = if let Some(open_idx) = trimmed.rfind('(') {
            if let Some(close_rel) = trimmed[open_idx + 1..].find(')') {
                let close_idx = open_idx + 1 + close_rel;
                parse_inno_size(&trimmed[open_idx + 1..close_idx]).unwrap_or(0)
            } else {
                0
            }
        } else {
            0
        };

        entries.push(ArchiveEntry {
            path: path.to_string(),
            size,
            is_dir: false,
        });
    }
    entries
}

fn list_archive_innoextract(archive_path: &Path) -> Result<Vec<ArchiveEntry>> {
    let inno_path = get_innoextract_path()?;
    let output = Command::new(&inno_path)
        .arg("-l")
        .arg("--collisions")
        .arg("rename-all")
        .arg(archive_path)
        .output()
        .with_context(|| format!("Failed to run innoextract list on {}", archive_path.display()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("innoextract list failed: {}", stderr);
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(parse_inno_list(&stdout))
}

fn extract_all_innoextract(archive_path: &Path, output_dir: &Path) -> Result<usize> {
    let inno_path = get_innoextract_path()?;
    fs::create_dir_all(output_dir)?;

    let output = Command::new(&inno_path)
        .arg("-e")
        .arg("-s")
        .arg("--collisions")
        .arg("rename-all")
        .arg("-d")
        .arg(output_dir)
        .arg(archive_path)
        .output()
        .with_context(|| format!("Failed to run innoextract on {}", archive_path.display()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("innoextract extract failed for {}: {}", archive_path.display(), stderr);
    }

    let count = walkdir::WalkDir::new(output_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .count();
    Ok(count)
}

fn extract_files_innoextract(archive_path: &Path, files: &[&str], output_dir: &Path) -> Result<()> {
    let inno_path = get_innoextract_path()?;
    fs::create_dir_all(output_dir)?;

    let mut cmd = Command::new(&inno_path);
    cmd.arg("-e")
        .arg("-s")
        .arg("--collisions")
        .arg("rename-all")
        .arg("-d")
        .arg(output_dir);
    for file in files {
        // innoextract path filters are slash-separated.
        // Strip any InnoSetup component suffix (#plus, #basic, etc.) since innoextract's
        // -I filter matches on base paths; --collisions rename-all handles the renaming.
        let normalized = file.replace('\\', "/");
        let base = if let Some(pos) = normalized.rfind('#') {
            &normalized[..pos]
        } else {
            &normalized
        };
        cmd.arg("-I").arg(base);
    }
    cmd.arg(archive_path);

    let output = cmd
        .output()
        .with_context(|| format!("Failed to run innoextract on {}", archive_path.display()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("innoextract selective extract failed: {}", stderr);
    }

    Ok(())
}

fn extract_file_innoextract(archive_path: &Path, file_path: &str) -> Result<Vec<u8>> {
    let tmp = tempfile::tempdir().context("Failed to create temp dir for innoextract")?;
    extract_files_innoextract(archive_path, &[file_path], tmp.path())?;

    let target = normalize_path(file_path);
    for entry in walkdir::WalkDir::new(tmp.path())
        .into_iter()
        .filter_map(|e| e.ok())
    {
        if !entry.file_type().is_file() {
            continue;
        }
        let rel = match entry.path().strip_prefix(tmp.path()) {
            Ok(r) => r,
            Err(_) => continue,
        };
        if normalize_path(&rel.to_string_lossy()) == target {
            return fs::read(entry.path()).with_context(|| {
                format!(
                    "Failed to read extracted inno file: {}",
                    entry.path().display()
                )
            });
        }
    }

    bail!(
        "innoextract did not produce '{}' from {}",
        file_path,
        archive_path.display()
    )
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_normalize_path() {
        assert_eq!(
            normalize_path("Data\\Textures\\test.dds"),
            "data/textures/test.dds"
        );
        assert_eq!(normalize_path("/foo/bar/"), "foo/bar");
        assert_eq!(normalize_path("FOO\\BAR\\BAZ.TXT"), "foo/bar/baz.txt");
        assert_eq!(
            normalize_path("Nemesis_Engine/mod/colis/0_master/#colis$15.txt"),
            "nemesis_engine/mod/colis/0_master/#colis$15.txt"
        );
    }

    #[test]
    fn test_build_path_lookup_keeps_hash_filenames_distinct() {
        let entries = vec![
            ArchiveEntry {
                path: "Nemesis_Engine/mod/colis/0_master/#colis$15.txt".to_string(),
                size: 1,
                is_dir: false,
            },
            ArchiveEntry {
                path: "Nemesis_Engine/mod/colis/0_master/#colis$16.txt".to_string(),
                size: 1,
                is_dir: false,
            },
        ];

        let lookup = build_path_lookup(&entries);
        assert_eq!(
            lookup
                .get("nemesis_engine/mod/colis/0_master/#colis$15.txt")
                .map(String::as_str),
            Some("Nemesis_Engine/mod/colis/0_master/#colis$15.txt")
        );
        assert_eq!(
            lookup
                .get("nemesis_engine/mod/colis/0_master/#colis$16.txt")
                .map(String::as_str),
            Some("Nemesis_Engine/mod/colis/0_master/#colis$16.txt")
        );
    }

    #[test]
    fn test_parse_7z_list() {
        let sample = b"\
Path = test.txt
Folder = -
Size = 1234
Attributes = ....A

Path = subdir/file.bin
Folder = -
Size = 5678
Attributes = ....A

Path = subdir
Folder = +
Size = 0
Attributes = D....

";
        let entries = parse_7z_list(sample).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].path, "test.txt");
        assert_eq!(entries[0].size, 1234);
        assert!(!entries[0].is_dir);
        assert_eq!(entries[1].path, "subdir/file.bin");
        assert_eq!(entries[1].size, 5678);
    }

    #[test]
    fn test_parse_incorrect_reparse_paths() {
        let stderr = "\
ERROR: Incorrect reparse stream : errno=2 : No such file or directory : Race-Based Textures (RBT)/HowToMakeItWork.txt
ERROR: Some other line
ERROR: Incorrect reparse stream : errno=17 : File exists : Foo/Bar.txt
ERROR: Incorrect reparse stream : errno=17 : File exists : Foo/Bar.txt";

        let paths = parse_incorrect_reparse_paths(stderr);
        assert_eq!(paths.len(), 2);
        assert_eq!(paths[0], "Race-Based Textures (RBT)/HowToMakeItWork.txt");
        assert_eq!(paths[1], "Foo/Bar.txt");
    }

    #[test]
    fn test_get_7z_path_exists() {
        let result = get_7z_path();
        if result.is_ok() {
            let path = result.unwrap();
            assert!(path.exists() || path.to_string_lossy().contains("7z"));
        }
    }

    #[test]
    fn test_parse_inno_size() {
        assert_eq!(parse_inno_size("558 B"), Some(558));
        assert_eq!(parse_inno_size("24.5 KiB"), Some(25088));
        assert_eq!(parse_inno_size("1 MiB"), Some(1048576));
    }

    #[test]
    fn test_parse_inno_list() {
        let sample = r#"
Listing "Example"
 - "app/Unofficial_Patch/maps/ch_hub_1.bsp" (12.5 KiB) - overwritten
 - "app/Docs/readme.txt" (558 B) - overwritten
"#;
        let entries = parse_inno_list(sample);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].path, "app/Unofficial_Patch/maps/ch_hub_1.bsp");
        assert_eq!(entries[0].size, 12800);
        assert_eq!(entries[1].path, "app/Docs/readme.txt");
        assert_eq!(entries[1].size, 558);
    }

    // --- Native ZIP tests (no 7z binary needed) ---

    #[test]
    fn test_list_zip_native() -> Result<()> {
        let dir = tempdir()?;
        let zip_path = dir.path().join("test.zip");

        {
            let file = File::create(&zip_path)?;
            let mut zip = zip::ZipWriter::new(file);
            let options = zip::write::SimpleFileOptions::default();

            zip.start_file("file1.txt", options)?;
            zip.write_all(b"hello")?;

            zip.start_file("subdir/file2.txt", options)?;
            zip.write_all(b"world!!")?;

            zip.finish()?;
        }

        let entries = list_zip(&zip_path)?;
        assert_eq!(entries.len(), 2);

        let lookup = build_path_lookup(&entries);
        assert!(lookup.contains_key("file1.txt"));
        assert!(lookup.contains_key("subdir/file2.txt"));

        Ok(())
    }

    #[test]
    fn test_extract_zip_file_native() -> Result<()> {
        let dir = tempdir()?;
        let zip_path = dir.path().join("test.zip");

        {
            let file = File::create(&zip_path)?;
            let mut zip = zip::ZipWriter::new(file);
            let options = zip::write::SimpleFileOptions::default();

            zip.start_file("Data/test.txt", options)?;
            zip.write_all(b"test content")?;
            zip.finish()?;
        }

        // Case-insensitive extraction
        let data = extract_zip_file(&zip_path, "data/TEST.TXT")?;
        assert_eq!(data, b"test content");

        Ok(())
    }

    #[test]
    fn test_extract_zip_all_native() -> Result<()> {
        let dir = tempdir()?;
        let zip_path = dir.path().join("test.zip");
        let output_dir = dir.path().join("output");

        {
            let file = File::create(&zip_path)?;
            let mut zip = zip::ZipWriter::new(file);
            let options = zip::write::SimpleFileOptions::default();

            zip.start_file("file1.txt", options)?;
            zip.write_all(b"one")?;

            zip.start_file("file2.txt", options)?;
            zip.write_all(b"two")?;

            zip.finish()?;
        }

        let count = extract_zip_all(&zip_path, &output_dir)?;
        assert_eq!(count, 2);
        assert!(output_dir.join("file1.txt").exists());
        assert!(output_dir.join("file2.txt").exists());

        assert_eq!(fs::read_to_string(output_dir.join("file1.txt"))?, "one");
        assert_eq!(fs::read_to_string(output_dir.join("file2.txt"))?, "two");

        Ok(())
    }

    #[test]
    fn test_extract_zip_files_selective() -> Result<()> {
        let dir = tempdir()?;
        let zip_path = dir.path().join("test.zip");
        let output_dir = dir.path().join("output");

        {
            let file = File::create(&zip_path)?;
            let mut zip = zip::ZipWriter::new(file);
            let options = zip::write::SimpleFileOptions::default();

            zip.start_file("file1.txt", options)?;
            zip.write_all(b"keep")?;

            zip.start_file("file2.txt", options)?;
            zip.write_all(b"skip")?;

            zip.start_file("subdir/file3.txt", options)?;
            zip.write_all(b"also keep")?;

            zip.finish()?;
        }

        extract_zip_files(&zip_path, &["file1.txt", "subdir/file3.txt"], &output_dir)?;

        assert!(output_dir.join("file1.txt").exists());
        assert!(!output_dir.join("file2.txt").exists()); // not extracted
        assert!(output_dir.join("subdir/file3.txt").exists());

        Ok(())
    }

    #[test]
    fn test_dispatch_zip_no_7z_needed() -> Result<()> {
        let dir = tempdir()?;
        let zip_path = dir.path().join("test.zip");
        let output_dir = dir.path().join("output");

        {
            let file = File::create(&zip_path)?;
            let mut zip = zip::ZipWriter::new(file);
            let options = zip::write::SimpleFileOptions::default();

            zip.start_file("hello.txt", options)?;
            zip.write_all(b"dispatch test")?;
            zip.finish()?;
        }

        // These use the public dispatch API
        let entries = list_archive(&zip_path)?;
        assert_eq!(entries.len(), 1);

        let data = extract_file(&zip_path, "hello.txt")?;
        assert_eq!(data, b"dispatch test");

        let count = extract_all(&zip_path, &output_dir)?;
        assert_eq!(count, 1);
        assert_eq!(fs::read_to_string(output_dir.join("hello.txt"))?, "dispatch test");

        Ok(())
    }

    #[test]
    fn test_zip_empty_files_extracted() -> Result<()> {
        let dir = tempdir()?;
        let zip_path = dir.path().join("test.zip");

        {
            let file = File::create(&zip_path)?;
            let mut zip = zip::ZipWriter::new(file);
            let options = zip::write::SimpleFileOptions::default();

            // Normal file
            zip.start_file("data/readme.txt", options)?;
            zip.write_all(b"hello")?;

            // Empty file (0 bytes) - like DynamicAnimationReplacer .txt files
            zip.start_file("data/plugins/empty_marker.txt", options)?;
            // Don't write anything - 0 byte file

            zip.finish()?;
        }

        // Listing should include both files
        let entries = list_archive(&zip_path)?;
        assert_eq!(entries.len(), 2);
        let names: Vec<&str> = entries.iter().map(|e| e.path.as_str()).collect();
        assert!(names.contains(&"data/readme.txt"));
        assert!(names.contains(&"data/plugins/empty_marker.txt"));

        // Extract single empty file
        let data = extract_file(&zip_path, "data/plugins/empty_marker.txt")?;
        assert!(data.is_empty());

        // Extract all - empty file should exist on disk
        let output_dir = dir.path().join("out");
        let count = extract_all(&zip_path, &output_dir)?;
        assert_eq!(count, 2);
        let empty_path = output_dir.join("data/plugins/empty_marker.txt");
        assert!(empty_path.exists(), "Empty file should be created on disk");
        assert_eq!(fs::metadata(&empty_path)?.len(), 0);

        Ok(())
    }

    // --- Tests that need 7z binary (skipped if not available) ---

    #[test]
    fn test_list_zip_archive_via_7z() -> Result<()> {
        let dir = tempdir()?;
        let zip_path = dir.path().join("test.zip");

        {
            let file = File::create(&zip_path)?;
            let mut zip = zip::ZipWriter::new(file);
            let options = zip::write::SimpleFileOptions::default();

            zip.start_file("file1.txt", options)?;
            zip.write_all(b"hello")?;

            zip.start_file("subdir/file2.txt", options)?;
            zip.write_all(b"world!!")?;

            zip.finish()?;
        }

        if get_7z_path().is_err() {
            return Ok(());
        }

        let entries = list_archive_7z_binary(&zip_path)?;
        assert_eq!(entries.len(), 2);

        let lookup = build_path_lookup(&entries);
        assert!(lookup.contains_key("file1.txt"));
        assert!(lookup.contains_key("subdir/file2.txt"));

        Ok(())
    }

    #[test]
    fn test_extract_from_zip_via_7z() -> Result<()> {
        let dir = tempdir()?;
        let zip_path = dir.path().join("test.zip");

        {
            let file = File::create(&zip_path)?;
            let mut zip = zip::ZipWriter::new(file);
            let options = zip::write::SimpleFileOptions::default();

            zip.start_file("Data/test.txt", options)?;
            zip.write_all(b"test content")?;
            zip.finish()?;
        }

        if get_7z_path().is_err() {
            return Ok(());
        }

        let data = extract_file_case_insensitive(&zip_path, "data/TEST.TXT")?;
        assert_eq!(data, b"test content");

        Ok(())
    }

    #[test]
    fn test_extract_all_via_dispatch() -> Result<()> {
        let dir = tempdir()?;
        let zip_path = dir.path().join("test.zip");
        let output_dir = dir.path().join("output");

        {
            let file = File::create(&zip_path)?;
            let mut zip = zip::ZipWriter::new(file);
            let options = zip::write::SimpleFileOptions::default();

            zip.start_file("file1.txt", options)?;
            zip.write_all(b"one")?;

            zip.start_file("file2.txt", options)?;
            zip.write_all(b"two")?;

            zip.finish()?;
        }

        let count = extract_all(&zip_path, &output_dir)?;
        assert_eq!(count, 2);
        assert!(output_dir.join("file1.txt").exists());
        assert!(output_dir.join("file2.txt").exists());

        Ok(())
    }

    #[test]
    fn test_detect_archive_type_magic() -> Result<()> {
        let dir = tempdir()?;

        // ZIP
        let zip_path = dir.path().join("test.zip");
        {
            let file = File::create(&zip_path)?;
            let mut zip = zip::ZipWriter::new(file);
            zip.start_file("f.txt", zip::write::SimpleFileOptions::default())?;
            zip.write_all(b"x")?;
            zip.finish()?;
        }
        assert_eq!(detect_archive_type(&zip_path)?, ArchiveType::Zip);

        // 7z magic bytes
        let sz_path = dir.path().join("test.7z");
        fs::write(&sz_path, [0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C, 0x00, 0x00])?;
        assert_eq!(detect_archive_type(&sz_path)?, ArchiveType::SevenZ);

        // RAR magic bytes
        let rar_path = dir.path().join("test.rar");
        fs::write(&rar_path, [0x52, 0x61, 0x72, 0x21, 0x1A, 0x07, 0x00, 0x00])?;
        assert_eq!(detect_archive_type(&rar_path)?, ArchiveType::Rar);

        Ok(())
    }
}
