//! BSA reading with parallel extraction

use anyhow::{bail, Context, Result};
use ba2::tes4::{Archive, FileCompressionOptions};
use ba2::{ByteSlice, Reader};
use rayon::prelude::*;
use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use sysinfo::System;
use tracing::{debug, info, warn};

/// BSA reader with access tracking
pub struct BsaReader {
    /// Track access counts per BSA for diagnostics
    access_counts: std::collections::HashMap<String, usize>,
}

impl BsaReader {
    pub fn new() -> Self {
        Self {
            access_counts: std::collections::HashMap::new(),
        }
    }

    /// Extract a single file from a BSA
    pub fn extract(&mut self, bsa_path: &Path, file_path: &str) -> Result<Vec<u8>> {
        let key = bsa_path.to_string_lossy().to_string();
        *self.access_counts.entry(key).or_insert(0) += 1;

        extract_file(bsa_path, file_path)
    }

    /// Check if a file exists in a BSA
    pub fn file_exists(&mut self, bsa_path: &Path, file_path: &str) -> Result<bool> {
        match self.extract(bsa_path, file_path) {
            Ok(_) => Ok(true),
            Err(e) if e.to_string().contains("not found") => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Get access statistics
    pub fn access_stats(&self) -> &std::collections::HashMap<String, usize> {
        &self.access_counts
    }

    /// Clear tracking data
    pub fn clear_stats(&mut self) {
        self.access_counts.clear();
    }
}

impl Default for BsaReader {
    fn default() -> Self {
        Self::new()
    }
}

/// Entry for a file in a BSA archive
pub struct BsaFileEntry {
    pub path: String,
    pub size: u64,
}

/// List all files in a BSA archive
pub fn list_files(bsa_path: &Path) -> Result<Vec<BsaFileEntry>> {
    let (archive, _): (Archive, _) = Archive::read(bsa_path)
        .with_context(|| format!("Failed to open BSA: {}", bsa_path.display()))?;

    let mut files = Vec::new();

    for (dir_key, folder) in archive.iter() {
        let dir_name = String::from_utf8_lossy(dir_key.name().as_bytes());

        for (file_key, file) in folder.iter() {
            let file_name = String::from_utf8_lossy(file_key.name().as_bytes());

            // Build full path with backslash (BSA convention)
            let full_path = if dir_name.is_empty() || dir_name == "." {
                file_name.to_string()
            } else {
                format!("{}\\{}", dir_name, file_name)
            };

            // Get decompressed size if compressed, otherwise raw size
            let size = file.decompressed_len().unwrap_or(file.len()) as u64;

            files.push(BsaFileEntry {
                path: full_path,
                size,
            });
        }
    }

    debug!("Listed {} files in BSA {}", files.len(), bsa_path.display());
    Ok(files)
}

/// Extract a single file from a BSA archive
pub fn extract_file(bsa_path: &Path, file_path: &str) -> Result<Vec<u8>> {
    let (archive, options): (Archive, _) = Archive::read(bsa_path)
        .with_context(|| format!("Failed to open BSA: {}", bsa_path.display()))?;

    // Convert archive options to compression options (includes version info)
    let compression_options: FileCompressionOptions = (&options).into();

    // Normalize to backslashes and split
    let normalized = file_path.replace('/', "\\");
    let (dir_name, file_name) = if let Some(idx) = normalized.rfind('\\') {
        (&normalized[..idx], &normalized[idx + 1..])
    } else {
        ("", normalized.as_str())
    };

    // Search case-insensitively
    for (dir_key, folder) in archive.iter() {
        let current_dir = String::from_utf8_lossy(dir_key.name().as_bytes());

        if current_dir.eq_ignore_ascii_case(dir_name) {
            for (file_key, file) in folder.iter() {
                let current_file = String::from_utf8_lossy(file_key.name().as_bytes());

                if current_file.eq_ignore_ascii_case(file_name) {
                    // Extract with decompression if needed (uses version from archive options)
                    let data = if file.is_decompressed() {
                        file.as_bytes().to_vec()
                    } else {
                        file.decompress(&compression_options)?.as_bytes().to_vec()
                    };
                    return Ok(data);
                }
            }
        }
    }

    bail!(
        "File not found in BSA: {} (dir='{}', file='{}')",
        file_path,
        dir_name,
        file_name
    )
}

/// Extract multiple files from a single BSA in parallel
///
/// Opens the BSA once and extracts matching files using rayon.
/// Returns a map of file_path -> data for successfully extracted files.
pub fn extract_batch_parallel(
    bsa_path: &Path,
    file_paths: &[&str],
    max_memory_bytes: Option<usize>,
) -> Result<Vec<(String, Vec<u8>)>> {
    let (archive, _): (Archive, _) = Archive::read(bsa_path)
        .with_context(|| format!("Failed to open BSA: {}", bsa_path.display()))?;

    // Build set of normalized paths we need
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

    // Collect matching files with their archive location
    let mut matches: Vec<(String, bool, Vec<u8>)> = Vec::new();

    for (dir_key, folder) in archive.iter() {
        let dir_name = String::from_utf8_lossy(dir_key.name().as_bytes()).to_lowercase();

        for (file_key, file) in folder.iter() {
            let file_name = String::from_utf8_lossy(file_key.name().as_bytes()).to_lowercase();
            let full_path = if dir_name.is_empty() || dir_name == "." {
                file_name.clone()
            } else {
                format!("{}\\{}", dir_name, file_name)
            };

            if needed.contains(&full_path) {
                // Get original path casing
                let original = path_lookup
                    .get(&full_path)
                    .map(|s| s.to_string())
                    .unwrap_or(full_path);

                matches.push((original, file.is_compressed(), file.as_bytes().to_vec()));
            }
        }
    }

    debug!(
        "Found {}/{} files in BSA {}",
        matches.len(),
        file_paths.len(),
        bsa_path.display()
    );

    // Process in parallel with rayon
    let results: Vec<(String, Vec<u8>)> = matches
        .into_par_iter()
        .filter_map(|(path, is_compressed, raw_data)| {
            // Check memory limit
            let current = bytes_extracted.load(Ordering::Relaxed);
            if current >= max_bytes {
                return None;
            }

            // Decompress if needed
            let data = if is_compressed {
                // We need to re-read this from the BSA due to ba2's API
                // For now, just decompress inline (not ideal but works)
                match decompress_zlib(&raw_data) {
                    Ok(decompressed) => decompressed,
                    Err(e) => {
                        warn!("Failed to decompress {}: {}", path, e);
                        return None;
                    }
                }
            } else {
                raw_data
            };

            bytes_extracted.fetch_add(data.len(), Ordering::Relaxed);
            Some((path, data))
        })
        .collect();

    info!(
        "Extracted {} files ({:.1} MB) from {}",
        results.len(),
        bytes_extracted.load(Ordering::Relaxed) as f64 / 1024.0 / 1024.0,
        bsa_path.display()
    );

    Ok(results)
}

/// Simple zlib decompression
fn decompress_zlib(data: &[u8]) -> Result<Vec<u8>> {
    use flate2::read::ZlibDecoder;
    use std::io::Read;

    let mut decoder = ZlibDecoder::new(data);
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed)?;
    Ok(decompressed)
}

/// Check current memory pressure
/// Returns true if available RAM is below 20% of total
pub fn memory_pressure() -> bool {
    let mut sys = System::new();
    sys.refresh_memory();
    let available = sys.available_memory();
    let total = sys.total_memory();
    let threshold = (total as f64 * 0.2) as u64;
    available < threshold
}

/// Get available memory in bytes
pub fn available_memory() -> u64 {
    let mut sys = System::new();
    sys.refresh_memory();
    sys.available_memory()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_functions() {
        let available = available_memory();
        assert!(available > 0);

        // Just test it runs without panic
        let _ = memory_pressure();
    }
}
