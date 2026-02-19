//! BA2 (Fallout 4/Starfield) archive reading
//!
//! Provides read support for FO4 format BA2 files (Fallout 4, Fallout 76, Starfield).

use anyhow::{bail, Context, Result};
use ba2::fo4::{Archive, FileWriteOptions};
use ba2::prelude::*;
use ba2::ByteSlice;
use rayon::prelude::*;
use std::collections::HashSet;
use std::io::Cursor;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::{debug, warn};

/// Normalize a file path to both forward-slash and backslash lowercase forms for BA2 lookup.
fn normalize_ba2_path(path: &str) -> (String, String) {
    let forward = path.replace('\\', "/").to_lowercase();
    let back = path.replace('/', "\\").to_lowercase();
    (forward, back)
}

/// Entry for a file in a BA2 archive
#[derive(Debug, Clone)]
pub struct Ba2FileEntry {
    pub path: String,
    pub size: u64,
    /// True if this is a DX10 texture file
    pub is_texture: bool,
}

/// List all files in a BA2 archive
pub fn list_files(ba2_path: &Path) -> Result<Vec<Ba2FileEntry>> {
    let (archive, _options): (Archive, _) = Archive::read(ba2_path)
        .with_context(|| format!("Failed to open BA2: {}", ba2_path.display()))?;

    let mut files = Vec::new();

    for (key, file) in archive.iter() {
        let path = String::from_utf8_lossy(key.name().as_bytes()).to_string();

        // Calculate decompressed size from all chunks
        let size: u64 = file
            .iter()
            .map(|chunk| chunk.decompressed_len().unwrap_or(chunk.len()) as u64)
            .sum();

        // Check if it's a DX10 texture based on header
        let is_texture = matches!(file.header, ba2::fo4::FileHeader::DX10(_));

        files.push(Ba2FileEntry {
            path,
            size,
            is_texture,
        });
    }

    debug!("Listed {} files in BA2 {}", files.len(), ba2_path.display());
    Ok(files)
}

/// Extract a single file from a BA2 archive
pub fn extract_file(ba2_path: &Path, file_path: &str) -> Result<Vec<u8>> {
    let (archive, options): (Archive, _) = Archive::read(ba2_path)
        .with_context(|| format!("Failed to open BA2: {}", ba2_path.display()))?;

    let write_options: FileWriteOptions = options.into();

    // Normalize path for comparison (BA2 uses forward slashes typically)
    let (normalized, normalized_backslash) = normalize_ba2_path(file_path);

    for (key, file) in archive.iter() {
        let current_path = String::from_utf8_lossy(key.name().as_bytes()).to_lowercase();

        // Try both slash conventions
        if current_path == normalized
            || current_path == normalized_backslash
            || current_path.replace('\\', "/") == normalized
            || current_path.replace('/', "\\") == normalized_backslash
        {
            // Write to memory buffer
            let mut buffer = Cursor::new(Vec::new());
            file.write(&mut buffer, &write_options)
                .with_context(|| format!("Failed to extract file: {}", file_path))?;

            return Ok(buffer.into_inner());
        }
    }

    bail!(
        "File not found in BA2: {} (searched for '{}')",
        file_path,
        normalized
    )
}

/// Extract multiple files from a BA2 archive in parallel.
/// Opens the archive once, collects matching entries, then decompresses
/// and delivers them in parallel using rayon via a callback.
/// `wanted` should contain lowercase forward-slash-separated paths.
pub fn extract_batch_parallel<F>(
    ba2_path: &Path,
    wanted: &HashSet<String>,
    callback: F,
) -> Result<usize>
where
    F: Fn(&str, Vec<u8>) -> Result<()> + Send + Sync,
{
    let (archive, options): (Archive, _) = Archive::read(ba2_path)
        .with_context(|| format!("Failed to open BA2: {}", ba2_path.display()))?;

    let write_options: FileWriteOptions = options.into();

    // Collect matching entries with references
    let mut entries: Vec<(String, &ba2::fo4::File)> = Vec::new();
    for (key, file) in archive.iter() {
        let path = String::from_utf8_lossy(key.name().as_bytes()).to_string();
        let lookup = path.replace('\\', "/").to_lowercase();
        if wanted.contains(&lookup) {
            entries.push((path, file));
        }
    }

    debug!(
        "Found {}/{} files in BA2 {}",
        entries.len(),
        wanted.len(),
        ba2_path.display()
    );

    // Decompress + deliver in parallel
    let extracted = AtomicUsize::new(0);
    entries
        .par_iter()
        .try_for_each(|(path, file)| -> Result<()> {
            let mut buffer = Cursor::new(Vec::new());
            file.write(&mut buffer, &write_options)
                .with_context(|| format!("Failed to extract file: {}", path))?;

            callback(path, buffer.into_inner())?;
            extracted.fetch_add(1, Ordering::Relaxed);
            Ok(())
        })?;

    let count = extracted.load(Ordering::Relaxed);
    debug!(
        "Batch extracted {} of {} wanted files from BA2 {}",
        count,
        wanted.len(),
        ba2_path.display()
    );
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_ba2() {
        // Test with a real BA2 file if available
        let test_path = Path::new("/home/luke/.local/share/Steam/steamapps/common/Fallout 4/Data/ccBGSFO4003-PipBoy(Camo01) - Main.ba2");
        if test_path.exists() {
            let files = list_files(test_path).expect("Failed to list BA2");
            println!("Found {} files in BA2", files.len());
            for f in files.iter().take(10) {
                println!("  {} ({} bytes, texture={})", f.path, f.size, f.is_texture);
            }
            assert!(!files.is_empty());
        }
    }

    #[test]
    fn test_extract_ba2() {
        let test_path = Path::new("/home/luke/.local/share/Steam/steamapps/common/Fallout 4/Data/ccBGSFO4003-PipBoy(Camo01) - Main.ba2");
        if test_path.exists() {
            let files = list_files(test_path).expect("Failed to list BA2");
            if let Some(first) = files.first() {
                let data = extract_file(test_path, &first.path).expect("Failed to extract");
                println!("Extracted {} bytes from {}", data.len(), first.path);
                assert!(!data.is_empty());
            }
        }
    }

    /// Test extracting BA2 from 7z archive and then extracting files from the BA2
    /// This tests the full nested extraction pipeline
    #[test]
    fn test_nested_ba2_from_7z() {
        use crate::archive::sevenzip;
        use std::fs;

        // Test archive with BA2 inside
        let archive_path = std::path::Path::new(
            "/mnt/1TB NVME/Mod Downloads/Fallout 4/APC Transport V2.2.7-16211-V2-2-7-1683739675.7z",
        );
        if !archive_path.exists() {
            println!("Test archive not found, skipping");
            return;
        }

        // List archive contents to find BA2 files
        println!("=== Step 1: List 7z archive contents ===");
        let entries = match sevenzip::list_archive(archive_path) {
            Ok(e) => e,
            Err(e) => {
                println!("Failed to list archive (7z not available?): {}", e);
                return;
            }
        };

        let ba2_entries: Vec<_> = entries
            .iter()
            .filter(|e| e.path.to_lowercase().ends_with(".ba2"))
            .collect();

        println!("Found {} BA2 files in archive:", ba2_entries.len());
        for e in &ba2_entries {
            println!("  {} ({} bytes)", e.path, e.size);
        }

        if ba2_entries.is_empty() {
            println!("No BA2 files found in archive");
            return;
        }

        // Extract the first BA2 to temp
        println!("\n=== Step 2: Extract BA2 from 7z ===");
        let ba2_path_in_archive = &ba2_entries[0].path;
        println!("Extracting: {}", ba2_path_in_archive);

        let ba2_data =
            match sevenzip::extract_file_case_insensitive(archive_path, ba2_path_in_archive) {
                Ok(d) => d,
                Err(e) => {
                    println!("Failed to extract BA2: {}", e);
                    return;
                }
            };
        println!("Extracted {} bytes", ba2_data.len());

        // Write to temp file with correct extension
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let temp_ba2_path = temp_dir.path().join("test.ba2");
        fs::write(&temp_ba2_path, &ba2_data).expect("Failed to write temp BA2");
        println!("Wrote temp BA2 to: {}", temp_ba2_path.display());

        // Verify magic bytes
        let mut magic = [0u8; 4];
        let mut f = std::fs::File::open(&temp_ba2_path).unwrap();
        std::io::Read::read_exact(&mut f, &mut magic).unwrap();
        println!(
            "Magic bytes: {:02X} {:02X} {:02X} {:02X}",
            magic[0], magic[1], magic[2], magic[3]
        );
        assert_eq!(
            &magic, b"BTDX",
            "Not a valid BA2 file (expected BTDX magic)"
        );

        // List files in the BA2
        println!("\n=== Step 3: List files in extracted BA2 ===");
        let ba2_files = list_files(&temp_ba2_path).expect("Failed to list BA2 files");
        println!("BA2 contains {} files:", ba2_files.len());
        for f in ba2_files.iter().take(10) {
            println!("  {} ({} bytes, texture={})", f.path, f.size, f.is_texture);
        }

        // Extract a file from the BA2
        if let Some(first_file) = ba2_files.first() {
            println!("\n=== Step 4: Extract file from BA2 ===");
            println!("Extracting: {}", first_file.path);
            let file_data = extract_file(&temp_ba2_path, &first_file.path)
                .expect("Failed to extract file from BA2");
            println!("Extracted {} bytes", file_data.len());
            assert!(!file_data.is_empty());
        }

        println!("\n=== Test passed! BA2 extraction pipeline works ===");
    }
}
