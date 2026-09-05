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

        // Some valid Oblivion BSAs have a filename table whose order differs
        // from the file-record order. bsa-rs associates those names by
        // position, so recover the correct association using the hash stored
        // in each file record instead.
        let files_by_hash: std::collections::HashMap<u64, _> = folder
            .iter()
            .map(|(file_key, file)| (file_key.hash().numeric(), file))
            .collect();

        for (file_key, _) in folder.iter() {
            let file_name = String::from_utf8_lossy(file_key.name().as_bytes());
            let wanted_key = ba2::tes4::DirectoryKey::from(file_key.name().as_bytes());
            let Some(file) = files_by_hash.get(&wanted_key.hash().numeric()) else {
                warn!(
                    "BSA filename hash did not match a file record: {}\\{}",
                    dir_name, file_name
                );
                continue;
            };

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

    let wanted_dir_key = ba2::tes4::ArchiveKey::from(dir_name.as_bytes());
    let wanted_file_key = ba2::tes4::DirectoryKey::from(file_name.as_bytes());

    // Match the hashes stored in the file records. This remains correct for
    // Oblivion BSAs whose filename table is not in file-record order.
    for (dir_key, folder) in archive.iter() {
        if dir_key.hash() == wanted_dir_key.hash() {
            for (file_key, file) in folder.iter() {
                if file_key.hash() == wanted_file_key.hash() {
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
    let (archive, options): (Archive, _) = Archive::read(bsa_path)
        .with_context(|| format!("Failed to open BSA: {}", bsa_path.display()))?;

    let compression_options: FileCompressionOptions = (&options).into();

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

    // Collect matching files — decompress using the ba2 crate's API
    // which correctly handles the BSA compressed format (4-byte size prefix + zlib)
    let mut matches: Vec<(String, Vec<u8>)> = Vec::new();

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
                let original = path_lookup
                    .get(&full_path)
                    .map(|s| s.to_string())
                    .unwrap_or(full_path);

                // Use the crate's decompress which handles BSA format correctly
                let data = if file.is_compressed() {
                    match file.decompress(&compression_options) {
                        Ok(decompressed) => decompressed.as_bytes().to_vec(),
                        Err(e) => {
                            warn!("Failed to decompress {}: {}", original, e);
                            continue;
                        }
                    }
                } else {
                    file.as_bytes().to_vec()
                };

                matches.push((original, data));
            }
        }
    }

    debug!(
        "Found {}/{} files in BSA {}",
        matches.len(),
        file_paths.len(),
        bsa_path.display()
    );

    // Filter by memory limit
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
        "Extracted {} files ({:.1} MB) from {}",
        results.len(),
        bytes_extracted.load(Ordering::Relaxed) as f64 / 1024.0 / 1024.0,
        bsa_path.display()
    );

    Ok(results)
}

/// Parallel batch extraction: collect matching file references, then decompress
/// and deliver via callback in parallel using rayon. Matches the fast BSA tool's approach.
pub fn extract_batch_streaming<F>(
    bsa_path: &Path,
    wanted: &HashSet<String>,
    callback: F,
) -> Result<usize>
where
    F: Fn(&str, Vec<u8>) -> Result<()> + Send + Sync,
{
    use ba2::tes4::File as BsaFile;

    let (archive, options): (Archive, _) = Archive::read(bsa_path)
        .with_context(|| format!("Failed to open BSA: {}", bsa_path.display()))?;

    let compression_options: FileCompressionOptions = (&options).into();

    // Build a lookup keyed by the directory/file hashes stored in BSA records.
    // Do not trust filename-table position: a number of valid Oblivion BSAs
    // use a different filename order than their file records.
    let wanted_by_hash: std::collections::HashMap<(u64, u64), &str> = wanted
        .iter()
        .filter_map(|path| {
            let normalized = path.replace('/', "\\");
            let (dir, file) = normalized
                .rfind('\\')
                .map(|idx| (&normalized[..idx], &normalized[idx + 1..]))
                .unwrap_or(("", normalized.as_str()));
            if file.is_empty() {
                return None;
            }
            let dir_hash = ba2::tes4::ArchiveKey::from(dir.as_bytes()).hash().numeric();
            let file_hash = ba2::tes4::DirectoryKey::from(file.as_bytes())
                .hash()
                .numeric();
            Some(((dir_hash, file_hash), path.as_str()))
        })
        .collect();

    // Phase 1: Collect matching file references (fast, no decompression)
    let mut entries: Vec<(String, &BsaFile)> = Vec::new();
    for (dir_key, folder) in archive.iter() {
        for (file_key, file) in folder.iter() {
            let key = (dir_key.hash().numeric(), file_key.hash().numeric());
            if let Some(path) = wanted_by_hash.get(&key) {
                entries.push(((*path).to_string(), file));
            }
        }
    }

    // Phase 2: Decompress + deliver in parallel
    let extracted = AtomicUsize::new(0);
    entries
        .par_iter()
        .try_for_each(|(path, file)| -> Result<()> {
            let data = if file.is_compressed() {
                match file.decompress(&compression_options) {
                    Ok(decompressed) => decompressed.as_bytes().to_vec(),
                    Err(e) => {
                        warn!("Failed to decompress {}: {}", path, e);
                        return Ok(());
                    }
                }
            } else {
                file.as_bytes().to_vec()
            };

            callback(path, data)?;
            extracted.fetch_add(1, Ordering::Relaxed);
            Ok(())
        })?;

    let count = extracted.load(Ordering::Relaxed);
    debug!(
        "Parallel extracted {}/{} files from BSA {}",
        count,
        wanted.len(),
        bsa_path.display()
    );

    Ok(count)
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
    use std::fs;
    use std::sync::Mutex;
    use tempfile::tempdir;

    #[test]
    fn test_memory_functions() {
        let available = available_memory();
        assert!(available > 0);

        // Just test it runs without panic
        let _ = memory_pressure();
    }

    #[test]
    fn test_out_of_order_oblivion_filename_table() -> Result<()> {
        let temp = tempdir()?;
        let alpha = temp.path().join("alpha.bin");
        let beta = temp.path().join("beta.bin");
        let archive_path = temp.path().join("Oblivion - Test.bsa");
        fs::write(&alpha, b"alpha payload")?;
        fs::write(&beta, b"beta payload")?;

        let mut builder = crate::bsa::BsaBuilder::from_name("Oblivion - Test.bsa");
        // Equal-length names make it safe to swap the raw filename-table entries.
        builder.add_file("meshes/alpha.dds", alpha);
        builder.add_file("meshes/beta_.dds", beta);
        builder.build(&archive_path)?;

        let mut raw = fs::read(&archive_path)?;
        let u32_at = |offset: usize| {
            u32::from_le_bytes(raw[offset..offset + 4].try_into().expect("u32 field")) as usize
        };
        let folder_count = u32_at(16);
        let file_count = u32_at(20);
        let folder_names_len = u32_at(24);
        let file_names_offset =
            0x24 + folder_count * 0x10 + folder_names_len + folder_count + file_count * 0x10;
        let name_len = "alpha.dds".len() + 1;
        for index in 0..name_len {
            raw.swap(
                file_names_offset + index,
                file_names_offset + name_len + index,
            );
        }
        fs::write(&archive_path, raw)?;

        let wanted = HashSet::from([
            "meshes/alpha.dds".to_string(),
            "meshes/beta_.dds".to_string(),
        ]);
        let extracted = Mutex::new(std::collections::HashMap::new());
        extract_batch_streaming(&archive_path, &wanted, |path, data| {
            extracted
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(path.to_string(), data);
            Ok(())
        })?;

        let extracted = extracted.into_inner().unwrap_or_else(|e| e.into_inner());
        assert_eq!(extracted["meshes/alpha.dds"], b"alpha payload");
        assert_eq!(extracted["meshes/beta_.dds"], b"beta payload");
        Ok(())
    }
}
