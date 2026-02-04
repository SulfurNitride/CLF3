//! TES3 (Morrowind) BSA reading with parallel extraction

use anyhow::{bail, Context, Result};
use ba2::tes3::Archive;
use ba2::{ByteSlice, Reader};
use rayon::prelude::*;
use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::{debug, info, warn};

use super::BsaFileEntry;

/// List all files in a TES3 (Morrowind) BSA archive
pub fn list_files(bsa_path: &Path) -> Result<Vec<BsaFileEntry>> {
    let archive: Archive = Archive::read(bsa_path)
        .with_context(|| format!("Failed to open TES3 BSA: {}", bsa_path.display()))?;

    let mut files = Vec::new();

    for (key, file) in archive.iter() {
        let path = String::from_utf8_lossy(key.name().as_bytes()).to_string();
        let size = file.len() as u64;

        files.push(BsaFileEntry { path, size });
    }

    debug!(
        "Listed {} files in TES3 BSA {}",
        files.len(),
        bsa_path.display()
    );
    Ok(files)
}

/// Extract a single file from a TES3 (Morrowind) BSA archive
pub fn extract_file(bsa_path: &Path, file_path: &str) -> Result<Vec<u8>> {
    let archive: Archive = Archive::read(bsa_path)
        .with_context(|| format!("Failed to open TES3 BSA: {}", bsa_path.display()))?;

    // Normalize path separators
    let normalized = file_path.replace('/', "\\");

    // Search case-insensitively
    for (key, file) in archive.iter() {
        let current_path = String::from_utf8_lossy(key.name().as_bytes());

        if current_path.eq_ignore_ascii_case(&normalized) {
            // TES3 BSAs are uncompressed, so just return the raw bytes
            return Ok(file.as_bytes().to_vec());
        }
    }

    bail!(
        "File not found in TES3 BSA: {} (looking for '{}')",
        bsa_path.display(),
        file_path
    )
}

/// Extract multiple files from a single TES3 BSA in parallel
///
/// Opens the BSA once and extracts matching files using rayon.
/// Returns a vec of (file_path, data) for successfully extracted files.
pub fn extract_batch_parallel(
    bsa_path: &Path,
    file_paths: &[&str],
    max_memory_bytes: Option<usize>,
) -> Result<Vec<(String, Vec<u8>)>> {
    let archive: Archive = Archive::read(bsa_path)
        .with_context(|| format!("Failed to open TES3 BSA: {}", bsa_path.display()))?;

    // Build set of normalized paths we need (lowercase for case-insensitive matching)
    let needed: HashSet<String> = file_paths
        .iter()
        .map(|p| p.replace('/', "\\").to_lowercase())
        .collect();

    // Build lookup for original casing
    let path_lookup: std::collections::HashMap<String, &str> = file_paths
        .iter()
        .map(|p| (p.replace('/', "\\").to_lowercase(), *p))
        .collect();

    // Memory tracking
    let bytes_extracted = AtomicUsize::new(0);
    let max_bytes = max_memory_bytes.unwrap_or(usize::MAX);

    // Collect matching files with their data
    let mut matches: Vec<(String, Vec<u8>)> = Vec::new();

    for (key, file) in archive.iter() {
        let path_lower = String::from_utf8_lossy(key.name().as_bytes()).to_lowercase();

        if needed.contains(&path_lower) {
            // Get original path casing
            let original = path_lookup
                .get(&path_lower)
                .map(|s| s.to_string())
                .unwrap_or_else(|| path_lower.clone());

            // TES3 BSAs are uncompressed
            matches.push((original, file.as_bytes().to_vec()));
        }
    }

    debug!(
        "Found {}/{} files in TES3 BSA {}",
        matches.len(),
        file_paths.len(),
        bsa_path.display()
    );

    // Filter by memory limit (TES3 files are already decompressed)
    let results: Vec<(String, Vec<u8>)> = matches
        .into_iter()
        .filter(|(_, data)| {
            let current = bytes_extracted.load(Ordering::Relaxed);
            if current >= max_bytes {
                return false;
            }
            bytes_extracted.fetch_add(data.len(), Ordering::Relaxed);
            true
        })
        .collect();

    info!(
        "Extracted {} files ({:.1} MB) from TES3 BSA {}",
        results.len(),
        bytes_extracted.load(Ordering::Relaxed) as f64 / 1024.0 / 1024.0,
        bsa_path.display()
    );

    Ok(results)
}
