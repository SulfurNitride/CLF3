//! 7z binary integration for archive extraction.
//!
//! This module provides a unified interface for extracting all archive formats
//! (ZIP, RAR, 7z) using the 7zz binary. This replaces the previous approach of
//! using separate Rust crates (zip, unrar, sevenz-rust).
//!
//! # Advantages of using 7z binary
//!
//! - Consistent behavior across all archive formats
//! - Better handling of edge cases (RAR5 reference records, mislabeled archives)
//! - Simpler codebase with fewer dependencies
//! - Native support for solid vs non-solid 7z detection
//!
//! # Archive Ordering
//!
//! For optimal extraction performance, archives should be processed in this order:
//! 1. ZIP files (fastest - random access)
//! 2. RAR files (medium - can skip entries)
//! 3. 7z non-solid (medium - random access within blocks)
//! 4. 7z solid (slowest - requires sequential decompression)
//!
//! # Solid Archive Detection
//!
//! 7z archives can be "solid" where multiple files are compressed together as a
//! single stream. This provides better compression but requires sequential
//! decompression. To extract file N, all files 1..N-1 must also be decompressed.
//!
//! Detection method: `7zz l -slt archive.7z` output includes "Solid = +" for solid
//! archives and "Solid = -" for non-solid.
//!
//! # Usage
//!
//! ```ignore
//! use clf3::archive::sevenzip::{list_archive, extract_file, extract_all, is_solid_archive};
//!
//! // List archive contents
//! let entries = list_archive("archive.7z")?;
//!
//! // Extract single file to memory (case-insensitive)
//! let data = extract_file_case_insensitive("archive.7z", "path/in/archive.txt")?;
//!
//! // Extract entire archive
//! extract_all("archive.7z", "/output/dir")?;
//!
//! // Check if archive is solid (7z only)
//! if is_solid_archive("archive.7z")? {
//!     println!("This is a solid archive - extraction will be slower");
//! }
//! ```
//!
//! # 7z Commands Reference
//!
//! - List files: `7zz l -slt -ba archive.zip`
//!   - `-slt`: Technical listing (key=value format)
//!   - `-ba`: Bare output (no headers)
//!
//! - Extract single file to stdout: `7zz e -so archive.zip "path/in/archive"`
//!   - `-so`: Write to stdout
//!
//! - Extract all to directory: `7zz x -o/output/dir archive.zip`
//!   - `-o{dir}`: Output directory
//!   - `-y`: Yes to all prompts
//!   - `-aoa`: Overwrite all existing files
//!
//! The 7z binary is bundled at `bin/7zz` (Linux) or `bin/7z.exe` (Windows).

use anyhow::{bail, Context, Result};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::Command;

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

impl ArchiveType {
    /// Get extraction priority (lower = faster, should be processed first).
    ///
    /// Order: ZIP (1) -> RAR (2) -> 7z (3) -> BSA/BA2 (4) -> Unknown (5)
    #[allow(dead_code)]
    pub fn priority(&self) -> u8 {
        match self {
            ArchiveType::Zip => 1,      // Fastest - random access
            ArchiveType::Rar => 2,      // Medium - can skip entries
            ArchiveType::SevenZ => 3,   // Variable - depends on solid flag
            ArchiveType::Bsa => 4,      // Special handling via bsa module
            ArchiveType::Ba2 => 4,      // Special handling via bsa module
            ArchiveType::Unknown => 5,  // Try all formats
        }
    }
}

/// Detect archive type by reading magic bytes.
///
/// This uses magic byte detection rather than file extension to handle
/// mislabeled archives (e.g., a `.zip` that's actually a RAR file).
pub fn detect_archive_type(path: &Path) -> Result<ArchiveType> {
    let mut file = File::open(path)
        .with_context(|| format!("Failed to open file: {}", path.display()))?;

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
#[allow(dead_code)]
pub struct ArchiveEntry {
    /// Path within the archive (forward slashes, case-preserved)
    pub path: String,
    /// Uncompressed size in bytes
    pub size: u64,
    /// Whether this is a directory
    pub is_dir: bool,
}

/// Get the path to the 7z binary.
///
/// Looks for the binary in the following locations:
/// 1. `bin/7zz` relative to the executable (Linux)
/// 2. `bin/7z.exe` relative to the executable (Windows)
/// 3. System PATH (`7z` or `7zz`)
pub fn get_7z_path() -> Result<PathBuf> {
    // Try relative to executable first
    if let Ok(exe_path) = std::env::current_exe() {
        if let Some(exe_dir) = exe_path.parent() {
            // Try bin/7zz (Linux)
            let bin_path = exe_dir.join("bin/7zz");
            if bin_path.exists() {
                return Ok(bin_path);
            }

            // Try bin/7z.exe (Windows)
            let bin_path = exe_dir.join("bin/7z.exe");
            if bin_path.exists() {
                return Ok(bin_path);
            }

            // Try just 7zz in same directory
            let bin_path = exe_dir.join("7zz");
            if bin_path.exists() {
                return Ok(bin_path);
            }
        }
    }

    // Try relative to current directory
    let cwd_path = PathBuf::from("bin/7zz");
    if cwd_path.exists() {
        return Ok(cwd_path);
    }

    // Try system PATH
    if let Ok(output) = Command::new("which").arg("7zz").output() {
        if output.status.success() {
            let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !path.is_empty() {
                return Ok(PathBuf::from(path));
            }
        }
    }

    if let Ok(output) = Command::new("which").arg("7z").output() {
        if output.status.success() {
            let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !path.is_empty() {
                return Ok(PathBuf::from(path));
            }
        }
    }

    bail!("7z binary not found. Please install p7zip or place 7zz in the bin/ directory.")
}

/// List all files in an archive.
///
/// Returns a vector of entries with their paths and sizes.
pub fn list_archive(archive_path: &Path) -> Result<Vec<ArchiveEntry>> {
    let sz_path = get_7z_path()?;

    let output = Command::new(&sz_path)
        .arg("l")           // List
        .arg("-slt")        // Technical listing format (key=value)
        .arg("-ba")         // Bare output (no headers)
        .arg("-scsUTF-8")   // Force UTF-8 charset for filenames
        .arg(archive_path)
        .output()
        .with_context(|| format!("Failed to run 7z list on {}", archive_path.display()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("7z list failed: {}", stderr);
    }

    parse_7z_list(&output.stdout)
}

/// Parse 7z technical listing output into ArchiveEntry structs.
fn parse_7z_list(output: &[u8]) -> Result<Vec<ArchiveEntry>> {
    let mut entries = Vec::new();
    let mut current: HashMap<String, String> = HashMap::new();

    for line in BufReader::new(output).lines() {
        let line = line?;
        let line = line.trim();

        if line.is_empty() {
            // End of entry - process it
            if let Some(path) = current.get("Path") {
                let is_dir = current.get("Folder").map(|v| v == "+").unwrap_or(false);
                let size = current
                    .get("Size")
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);

                // Skip directory entries for our purposes
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

/// Check if an archive is a solid 7z archive.
///
/// Solid archives store files as a single compressed stream, requiring
/// sequential decompression - we can't extract individual files efficiently.
///
/// For solid archives, we should extract all needed files at once to a temp
/// directory rather than extracting each file individually.
pub fn is_solid_archive(archive_path: &Path) -> Result<bool> {
    let sz_path = get_7z_path()?;

    let output = Command::new(&sz_path)
        .arg("l")           // List
        .arg("-slt")        // Technical listing format
        .arg(archive_path)
        .output()
        .with_context(|| format!("Failed to check if {} is solid", archive_path.display()))?;

    if !output.status.success() {
        // If we can't check, assume not solid (safer default)
        return Ok(false);
    }

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Look for "Solid = +" in the archive properties section
    for line in stdout.lines() {
        let line = line.trim();
        if line.starts_with("Solid = +") {
            return Ok(true);
        }
    }

    Ok(false)
}

/// Extract a single file from an archive to memory.
///
/// This is efficient for non-solid archives (ZIP, RAR, non-solid 7z).
/// For solid 7z archives, use `extract_files` or `extract_all` instead.
pub fn extract_file(archive_path: &Path, file_path: &str) -> Result<Vec<u8>> {
    let sz_path = get_7z_path()?;

    // Normalize path separators - 7z on Linux handles both, but be consistent
    let normalized_path = file_path.replace('\\', "/");

    // Use 7z's extract to stdout feature
    // -spd disables wildcard matching (treat path as literal)
    let output = Command::new(&sz_path)
        .arg("e")           // Extract
        .arg("-so")         // Output to stdout
        .arg("-y")          // Yes to all prompts
        .arg("-spd")        // Disable wildcard matching
        .arg("-scsUTF-8")   // Force UTF-8 charset for filenames
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
        bail!(
            "7z extract failed for '{}' in {}: {}",
            file_path,
            archive_path.display(),
            stderr
        );
    }

    // Verify we got data - empty output means file wasn't found
    if output.stdout.is_empty() {
        bail!(
            "7z returned no data for '{}' in {} - file not found in archive",
            file_path,
            archive_path.display()
        );
    }

    Ok(output.stdout)
}

/// Extract a single file from an archive using case-insensitive matching.
///
/// 7z's native matching is case-sensitive on Linux, so we need to:
/// 1. List the archive to find the actual path
/// 2. Extract using the exact path
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

/// Extract a single file from archive directly to a file using streaming (7z e -so).
/// This avoids loading the entire archive into memory.
/// Works with 7z, RAR, and ZIP formats.
#[allow(dead_code)] // Part of sevenzip API, not yet wired up
pub fn extract_file_streaming(
    archive_path: &Path,
    file_in_archive: &str,
    output_path: &Path,
) -> Result<u64> {
    use std::io::{BufWriter, Write};
    use std::process::Stdio;

    let bin = get_7z_path()?;

    // Create parent directories
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Normalize path separators
    let normalized_path = file_in_archive.replace('\\', "/");

    // 7z e -so archive.7z "path/in/archive" > output
    let mut child = Command::new(&bin)
        .arg("e")
        .arg("-so") // Write to stdout
        .arg("-bd") // Disable progress indicator
        .arg("-y") // Yes to all
        .arg("-spd") // Disable wildcard matching
        .arg(archive_path)
        .arg(&normalized_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .context("Failed to spawn 7z")?;

    let stdout = child.stdout.take().context("Failed to get stdout")?;
    let mut reader = std::io::BufReader::new(stdout);

    let output_file = std::fs::File::create(output_path)?;
    let mut writer = BufWriter::new(output_file);

    let bytes_written = std::io::copy(&mut reader, &mut writer)?;
    writer.flush()?;

    let status = child.wait()?;
    if !status.success() {
        anyhow::bail!("7z extraction failed for {}", file_in_archive);
    }

    // Verify we got data - empty output means file wasn't found
    if bytes_written == 0 {
        anyhow::bail!(
            "7z returned no data for '{}' in {} - file not found in archive",
            file_in_archive,
            archive_path.display()
        );
    }

    Ok(bytes_written)
}

/// Extract a single file from archive to memory using streaming (7z e -so).
/// Use this for small files or when you need the data in memory.
#[allow(dead_code)] // Part of sevenzip API, not yet wired up
pub fn extract_file_to_memory(archive_path: &Path, file_in_archive: &str) -> Result<Vec<u8>> {
    let bin = get_7z_path()?;

    // Normalize path separators
    let normalized_path = file_in_archive.replace('\\', "/");

    let output = Command::new(&bin)
        .arg("e")
        .arg("-so")
        .arg("-bd")
        .arg("-y")
        .arg("-spd") // Disable wildcard matching
        .arg(archive_path)
        .arg(&normalized_path)
        .output()
        .context("Failed to run 7z")?;

    if !output.status.success() {
        anyhow::bail!("7z extraction failed for {}", file_in_archive);
    }

    // Verify we got data - empty output means file wasn't found
    if output.stdout.is_empty() {
        anyhow::bail!(
            "7z returned no data for '{}' in {} - file not found in archive",
            file_in_archive,
            archive_path.display()
        );
    }

    Ok(output.stdout)
}

/// Extract multiple files from an archive to a directory.
///
/// This is more efficient than calling `extract_file` multiple times,
/// especially for solid archives.
#[allow(dead_code)]
pub fn extract_files(archive_path: &Path, files: &[&str], output_dir: &Path) -> Result<()> {
    if files.is_empty() {
        return Ok(());
    }

    let sz_path = get_7z_path()?;

    // Create output directory
    fs::create_dir_all(output_dir)?;

    // Build command with file list
    let mut cmd = Command::new(&sz_path);
    cmd.arg("x")            // Extract with full paths
        .arg("-y")          // Yes to all prompts
        .arg("-aoa")        // Overwrite all existing files
        .arg("-scsUTF-8")   // Force UTF-8 charset for filenames
        .arg(format!("-o{}", output_dir.display()))
        .arg(archive_path);

    // Add each file to extract
    for file in files {
        cmd.arg(file);
    }

    let output = cmd
        .output()
        .with_context(|| format!("Failed to extract files from {}", archive_path.display()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "7z extract failed for {}: {}",
            archive_path.display(),
            stderr
        );
    }

    Ok(())
}

/// Extract all files from an archive to a directory.
///
/// This is the most efficient method for solid archives.
/// Uses automatic thread count (all available cores).
pub fn extract_all(archive_path: &Path, output_dir: &Path) -> Result<usize> {
    extract_all_with_threads(archive_path, output_dir, None)
}

/// Extract all files from an archive to a directory with controlled threading.
///
/// # Arguments
/// * `archive_path` - Path to the archive file
/// * `output_dir` - Directory to extract files to
/// * `threads` - Number of threads for 7z to use:
///   - `None` or `Some(0)` = auto (use all available cores)
///   - `Some(1)` = single-threaded (good for parallel archive extraction)
///   - `Some(n)` = use exactly n threads
///
/// For parallel small archive extraction, use `threads = Some(1)` to avoid
/// thread over-subscription (e.g., 8 parallel archives × 16 threads = 128 threads).
/// For sequential large archive extraction, use `threads = None` for max throughput.
pub fn extract_all_with_threads(
    archive_path: &Path,
    output_dir: &Path,
    threads: Option<usize>,
) -> Result<usize> {
    let sz_path = get_7z_path()?;

    // Create output directory
    fs::create_dir_all(output_dir)?;

    let mut cmd = Command::new(&sz_path);
    cmd.arg("x")             // Extract with full paths
        .arg("-y")           // Yes to all prompts
        .arg("-aoa")         // Overwrite all existing files
        .arg("-scsUTF-8");   // Force UTF-8 charset for filenames

    // Add thread control
    match threads {
        Some(1) => {
            cmd.arg("-mmt=1"); // Single-threaded
        }
        Some(n) if n > 1 => {
            cmd.arg(format!("-mmt={}", n)); // Specific thread count
        }
        _ => {
            cmd.arg("-mmt=on"); // Auto (all cores)
        }
    }

    cmd.arg(format!("-o{}", output_dir.display()))
        .arg(archive_path);

    let output = cmd
        .output()
        .with_context(|| format!("Failed to extract {}", archive_path.display()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "7z extract all failed for {}: {}",
            archive_path.display(),
            stderr
        );
    }

    // Count extracted files
    let count = walkdir::WalkDir::new(output_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .count();

    Ok(count)
}

/// Extract files matching a predicate to a directory.
///
/// The predicate receives each ArchiveEntry and returns true if the file
/// should be extracted.
#[allow(dead_code)]
pub fn extract_matching<F>(
    archive_path: &Path,
    output_dir: &Path,
    mut predicate: F,
) -> Result<Vec<PathBuf>>
where
    F: FnMut(&ArchiveEntry) -> bool,
{
    // List archive and filter entries
    let entries = list_archive(archive_path)?;
    let files_to_extract: Vec<&str> = entries
        .iter()
        .filter(|e| predicate(e))
        .map(|e| e.path.as_str())
        .collect();

    if files_to_extract.is_empty() {
        return Ok(Vec::new());
    }

    // Extract matching files
    extract_files(archive_path, &files_to_extract, output_dir)?;

    // Return paths to extracted files
    let extracted_paths: Vec<PathBuf> = files_to_extract
        .iter()
        .map(|p| output_dir.join(p))
        .filter(|p| p.exists())
        .collect();

    Ok(extracted_paths)
}

/// Normalize a path for case-insensitive comparison.
///
/// - Converts to lowercase
/// - Normalizes path separators to forward slashes
/// - Removes leading/trailing slashes
fn normalize_path(path: &str) -> String {
    path.to_lowercase()
        .replace('\\', "/")
        .trim_matches('/')
        .to_string()
}

/// Build a case-insensitive lookup map from archive entries.
///
/// Returns a HashMap from normalized path to actual path in archive.
#[allow(dead_code)] // Used in tests; part of sevenzip API
pub fn build_path_lookup(entries: &[ArchiveEntry]) -> HashMap<String, String> {
    let mut lookup = HashMap::new();
    for entry in entries {
        let normalized = normalize_path(&entry.path);
        lookup.insert(normalized, entry.path.clone());
    }
    lookup
}

/// Find a file in the archive with case-insensitive matching.
///
/// Returns the actual path in the archive if found.
#[allow(dead_code)]
pub fn find_file_in_archive(
    archive_path: &Path,
    file_path: &str,
) -> Result<Option<String>> {
    let target_normalized = normalize_path(file_path);
    let entries = list_archive(archive_path)?;

    for entry in entries {
        if normalize_path(&entry.path) == target_normalized {
            return Ok(Some(entry.path));
        }
    }

    Ok(None)
}

/// Get the extraction priority for an archive.
///
/// Used to sort archives for optimal extraction order:
/// 1. ZIP (priority 1) - random access, fastest
/// 2. RAR (priority 2) - can skip entries
/// 3. 7z non-solid (priority 3) - random access within blocks
/// 4. 7z solid (priority 4) - sequential only
///
/// Returns (type_priority, is_solid).
/// For 7z archives, adds +1 to priority if solid.
#[allow(dead_code)]
pub fn get_extraction_priority(path: &Path) -> Result<(u8, bool)> {
    let archive_type = detect_archive_type(path)?;
    let mut priority = archive_type.priority();
    let mut solid = false;

    // Check solid flag for 7z archives
    if archive_type == ArchiveType::SevenZ {
        solid = is_solid_archive(path)?;
        if solid {
            priority += 1; // Solid archives are slower, lower priority
        }
    }

    Ok((priority, solid))
}

/// Sort archive paths by extraction priority.
///
/// Archives are sorted so faster formats are processed first:
/// 1. ZIP files
/// 2. RAR files
/// 3. 7z non-solid
/// 4. 7z solid
///
/// Returns an error if any archive cannot be analyzed.
#[allow(dead_code)]
pub fn sort_archives_by_priority(paths: &mut [PathBuf]) -> Result<()> {
    // Pre-compute priorities
    let mut priorities: Vec<(usize, u8, bool)> = Vec::with_capacity(paths.len());
    for (idx, path) in paths.iter().enumerate() {
        let (priority, is_solid) = get_extraction_priority(path)?;
        priorities.push((idx, priority, is_solid));
    }

    // Sort by priority (ascending), then by solid flag
    priorities.sort_by_key(|(_, priority, is_solid)| (*priority, *is_solid));

    // Reorder paths in-place using the sorted indices
    let original: Vec<PathBuf> = paths.to_vec();
    for (new_idx, (old_idx, _, _)) in priorities.into_iter().enumerate() {
        paths[new_idx] = original[old_idx].clone();
    }

    Ok(())
}

/// Fix permissions on extracted files (Unix only).
///
/// Windows archives sometimes extract with restrictive permissions on Linux.
/// This ensures all files are readable and directories are accessible.
#[cfg(unix)]
#[allow(dead_code)]
pub fn fix_permissions_recursive(dir: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    for entry in walkdir::WalkDir::new(dir)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        let path = entry.path();
        if let Ok(metadata) = fs::metadata(path) {
            let mut perms = metadata.permissions();
            let mode = perms.mode();

            if metadata.is_dir() {
                // Directories: rwx for owner, rx for others (0755)
                if mode & 0o700 != 0o700 {
                    perms.set_mode(mode | 0o755);
                    fs::set_permissions(path, perms).ok();
                }
            } else {
                // Files: rw for owner, r for others (0644)
                if mode & 0o600 != 0o600 {
                    perms.set_mode(mode | 0o644);
                    fs::set_permissions(path, perms).ok();
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_normalize_path() {
        assert_eq!(normalize_path("Data\\Textures\\test.dds"), "data/textures/test.dds");
        assert_eq!(normalize_path("/foo/bar/"), "foo/bar");
        assert_eq!(normalize_path("FOO\\BAR\\BAZ.TXT"), "foo/bar/baz.txt");
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
    fn test_get_7z_path_exists() {
        // This test will pass if 7z is installed
        let result = get_7z_path();
        if result.is_ok() {
            let path = result.unwrap();
            assert!(path.exists() || path.to_string_lossy().contains("7z"));
        }
        // If not installed, that's okay - the test just verifies the function doesn't panic
    }

    #[test]
    fn test_list_zip_archive() -> Result<()> {
        let dir = tempdir()?;
        let zip_path = dir.path().join("test.zip");

        // Create a test ZIP using the zip crate
        {
            let file = std::fs::File::create(&zip_path)?;
            let mut zip = zip::ZipWriter::new(file);
            let options = zip::write::SimpleFileOptions::default();

            zip.start_file("file1.txt", options)?;
            zip.write_all(b"hello")?;

            zip.start_file("subdir/file2.txt", options)?;
            zip.write_all(b"world!!")?;

            zip.finish()?;
        }

        // Skip if 7z not available
        if get_7z_path().is_err() {
            return Ok(());
        }

        let entries = list_archive(&zip_path)?;
        assert_eq!(entries.len(), 2);

        let lookup = build_path_lookup(&entries);
        assert!(lookup.contains_key("file1.txt"));
        assert!(lookup.contains_key("subdir/file2.txt"));

        Ok(())
    }

    #[test]
    fn test_extract_from_zip() -> Result<()> {
        let dir = tempdir()?;
        let zip_path = dir.path().join("test.zip");

        // Create a test ZIP
        {
            let file = std::fs::File::create(&zip_path)?;
            let mut zip = zip::ZipWriter::new(file);
            let options = zip::write::SimpleFileOptions::default();

            zip.start_file("Data/test.txt", options)?;
            zip.write_all(b"test content")?;
            zip.finish()?;
        }

        // Skip if 7z not available
        if get_7z_path().is_err() {
            return Ok(());
        }

        // Test case-insensitive extraction
        let data = extract_file_case_insensitive(&zip_path, "data/TEST.TXT")?;
        assert_eq!(data, b"test content");

        Ok(())
    }

    #[test]
    fn test_extract_all() -> Result<()> {
        let dir = tempdir()?;
        let zip_path = dir.path().join("test.zip");
        let output_dir = dir.path().join("output");

        // Create a test ZIP
        {
            let file = std::fs::File::create(&zip_path)?;
            let mut zip = zip::ZipWriter::new(file);
            let options = zip::write::SimpleFileOptions::default();

            zip.start_file("file1.txt", options)?;
            zip.write_all(b"one")?;

            zip.start_file("file2.txt", options)?;
            zip.write_all(b"two")?;

            zip.finish()?;
        }

        // Skip if 7z not available
        if get_7z_path().is_err() {
            return Ok(());
        }

        let count = extract_all(&zip_path, &output_dir)?;
        assert_eq!(count, 2);
        assert!(output_dir.join("file1.txt").exists());
        assert!(output_dir.join("file2.txt").exists());

        Ok(())
    }
}
