//! FromArchive directive handler
//!
//! Extracts files from downloaded archives (ZIP, 7z, RAR) to the output directory.
//! Handles nested paths including files inside BSAs within archives.

use crate::bsa;
use crate::installer::processor::ProcessContext;
use crate::modlist::FromArchiveDirective;
use crate::paths;

use anyhow::{bail, Context, Result};
use std::fs::{self, File};
use std::io::{BufReader, Read, Write};
use std::path::Path;

/// Handle a FromArchive directive
pub fn handle_from_archive(ctx: &ProcessContext, directive: &FromArchiveDirective) -> Result<()> {
    // Parse archive_hash_path: [archive_hash, path_in_archive, optional_nested_path...]
    // Special case: if only 1 element, the "archive" is a single file (e.g., GameFileSource)
    if directive.archive_hash_path.is_empty() {
        bail!("Invalid archive_hash_path: empty");
    }

    let archive_hash = &directive.archive_hash_path[0];

    // Find the downloaded archive/file
    let archive_path = ctx
        .get_archive_path(archive_hash)
        .with_context(|| format!("Archive not found for hash: {}", archive_hash))?;

    // Extract the file data
    let data = if directive.archive_hash_path.len() == 1 {
        // Single file - just read it directly (e.g., GameFileSource files)
        fs::read(archive_path)
            .with_context(|| format!("Failed to read file: {}", archive_path.display()))?
    } else if directive.archive_hash_path.len() == 2 {
        // Simple extraction from archive - try cache first
        let path_in_archive = &directive.archive_hash_path[1];
        if let Some(cached) = ctx.get_cached_file(archive_hash, path_in_archive) {
            cached
        } else {
            extract_from_archive_with_temp(archive_path, path_in_archive, &ctx.config.downloads_dir)?
        }
    } else {
        // Nested extraction: file is inside a BSA within the archive - try cache first
        let bsa_path_in_archive = &directive.archive_hash_path[1];
        let file_path_in_bsa = &directive.archive_hash_path[2];
        if let Some(cached) = ctx.get_cached_nested_bsa_file(archive_hash, bsa_path_in_archive, file_path_in_bsa) {
            cached
        } else if let Some(bsa_disk_path) = ctx.get_cached_bsa_path(archive_hash, bsa_path_in_archive) {
            // BSA is in working folder - extract directly from it
            bsa::extract_file(&bsa_disk_path, file_path_in_bsa)
                .with_context(|| format!("Failed to extract '{}' from BSA '{}'", file_path_in_bsa, bsa_path_in_archive))?
        } else {
            extract_nested_bsa(archive_path, bsa_path_in_archive, file_path_in_bsa, &ctx.config.downloads_dir)?
        }
    };

    // Verify size
    if data.len() as u64 != directive.size {
        bail!(
            "Size mismatch: expected {} bytes, got {}",
            directive.size,
            data.len()
        );
    }

    // Write to output
    let output_path = ctx.resolve_output_path(&directive.to);
    paths::ensure_parent_dirs(&output_path)?;

    let mut file = File::create(&output_path)
        .with_context(|| format!("Failed to create output file: {}", output_path.display()))?;
    file.write_all(&data)
        .with_context(|| format!("Failed to write output file: {}", output_path.display()))?;

    Ok(())
}

/// Extract a file from an archive (ZIP, 7z, RAR)
/// temp_base_dir is used for 7z/RAR extraction temp files (NOT /tmp)
pub fn extract_from_archive_with_temp(archive_path: &Path, file_path: &str, temp_base_dir: &Path) -> Result<Vec<u8>> {
    let extension = archive_path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();

    match extension.as_str() {
        "zip" => extract_from_zip(archive_path, file_path),
        "7z" => extract_from_7z(archive_path, file_path, temp_base_dir),
        "rar" => extract_from_rar(archive_path, file_path, temp_base_dir),
        "bsa" | "ba2" => {
            // Direct BSA extraction
            bsa::extract_file(archive_path, file_path)
        }
        _ => {
            // Try ZIP first (many archives use ZIP format with different extensions)
            extract_from_zip(archive_path, file_path)
                .or_else(|_| extract_from_7z(archive_path, file_path, temp_base_dir))
                .or_else(|_| extract_from_rar(archive_path, file_path, temp_base_dir))
                .with_context(|| {
                    format!(
                        "Failed to extract '{}' from '{}' (unknown format: {})",
                        file_path,
                        archive_path.display(),
                        extension
                    )
                })
        }
    }
}

/// Extract from ZIP archive with case-insensitive lookup
fn extract_from_zip(archive_path: &Path, file_path: &str) -> Result<Vec<u8>> {
    let file = File::open(archive_path)
        .with_context(|| format!("Failed to open ZIP: {}", archive_path.display()))?;
    let reader = BufReader::new(file);
    let mut archive = zip::ZipArchive::new(reader)
        .with_context(|| format!("Failed to read ZIP: {}", archive_path.display()))?;

    // Build case-insensitive lookup
    let target_normalized = paths::normalize_for_lookup(file_path);

    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;
        let entry_name = entry.name().to_string();
        let entry_normalized = paths::normalize_for_lookup(&entry_name);

        if entry_normalized == target_normalized {
            let mut data = Vec::with_capacity(entry.size() as usize);
            entry.read_to_end(&mut data)?;
            return Ok(data);
        }
    }

    bail!(
        "File '{}' not found in ZIP archive '{}'",
        file_path,
        archive_path.display()
    )
}

/// Extract from 7z archive with case-insensitive lookup
fn extract_from_7z(archive_path: &Path, file_path: &str, temp_base_dir: &Path) -> Result<Vec<u8>> {
    let target_normalized = paths::normalize_for_lookup(file_path);

    // Create a temp directory for extraction (in downloads dir, NOT /tmp)
    let temp_dir = tempfile::tempdir_in(temp_base_dir).context("Failed to create temp directory")?;

    // Decompress the entire archive to temp
    sevenz_rust::decompress_file(archive_path, temp_dir.path())
        .with_context(|| format!("Failed to decompress 7z archive: {}", archive_path.display()))?;

    // Find the file case-insensitively in the extracted directory
    find_file_in_dir(temp_dir.path(), &target_normalized)
        .and_then(|found_path| {
            fs::read(&found_path)
                .with_context(|| format!("Failed to read extracted file: {}", found_path.display()))
        })
        .with_context(|| {
            format!(
                "File '{}' not found in 7z archive '{}'",
                file_path,
                archive_path.display()
            )
        })
}

/// Find a file in a directory tree case-insensitively
fn find_file_in_dir(dir: &Path, target_normalized: &str) -> Result<std::path::PathBuf> {
    for entry in walkdir::WalkDir::new(dir).into_iter().filter_map(|e| e.ok()) {
        if entry.file_type().is_file() {
            // Get relative path from the base dir
            let rel_path = entry
                .path()
                .strip_prefix(dir)
                .unwrap_or(entry.path())
                .to_string_lossy();
            let entry_normalized = paths::normalize_for_lookup(&rel_path);

            if entry_normalized == *target_normalized {
                return Ok(entry.path().to_path_buf());
            }
        }
    }
    bail!("File not found: {}", target_normalized)
}

/// Extract from RAR archive with case-insensitive lookup
fn extract_from_rar(archive_path: &Path, file_path: &str, temp_base_dir: &Path) -> Result<Vec<u8>> {
    let target_normalized = paths::normalize_for_lookup(file_path);

    // Open for processing
    let archive = unrar::Archive::new(archive_path)
        .open_for_processing()
        .with_context(|| format!("Failed to open RAR: {}", archive_path.display()))?;

    // Create temp dir for extraction (in downloads dir, NOT /tmp)
    let temp_dir = tempfile::tempdir_in(temp_base_dir).context("Failed to create temp directory")?;

    // Iterate through entries using read_header pattern
    let mut cursor = Some(archive);

    while let Some(archive) = cursor.take() {
        match archive.read_header() {
            Ok(Some(entry)) => {
                let header = entry.entry();
                let entry_name = header.filename.to_string_lossy().to_string();
                let entry_normalized = paths::normalize_for_lookup(&entry_name);

                if entry_normalized == target_normalized && header.is_file() {
                    // Extract this file to temp
                    let temp_path = temp_dir.path().join("extracted");
                    entry
                        .extract_to(&temp_path)
                        .with_context(|| format!("Failed to extract RAR entry: {}", entry_name))?;

                    // Read the extracted file
                    return fs::read(&temp_path).with_context(|| {
                        format!("Failed to read extracted file: {}", temp_path.display())
                    });
                } else {
                    // Skip this entry
                    cursor = Some(entry.skip().context("Failed to skip RAR entry")?);
                }
            }
            Ok(None) => break, // End of archive
            Err(e) => return Err(e).context("Failed to read RAR header"),
        }
    }

    bail!(
        "File '{}' not found in RAR archive '{}'",
        file_path,
        archive_path.display()
    )
}

/// Extract a file that's inside a BSA within an archive
fn extract_nested_bsa(
    archive_path: &Path,
    bsa_path_in_archive: &str,
    file_path_in_bsa: &str,
    temp_dir: &Path,
) -> Result<Vec<u8>> {
    // First extract the BSA from the outer archive
    let bsa_data = extract_from_archive_with_temp(archive_path, bsa_path_in_archive, temp_dir)?;

    // Write BSA to a temp file in downloads folder (ba2 crate needs a file path)
    // Use tempfile crate for unique, auto-cleaned temp files
    let temp_file = tempfile::Builder::new()
        .prefix(".clf3_bsa_")
        .suffix(".bsa")
        .tempfile_in(temp_dir)
        .context("Failed to create temp file for BSA")?;

    fs::write(temp_file.path(), &bsa_data)
        .with_context(|| format!("Failed to write temp BSA: {}", temp_file.path().display()))?;

    // Extract from the BSA (temp_file auto-deleted when dropped)
    bsa::extract_file(temp_file.path(), file_path_in_bsa)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_extract_from_zip() -> Result<()> {
        let dir = tempdir()?;
        let zip_path = dir.path().join("test.zip");

        // Create a test ZIP
        {
            let file = File::create(&zip_path)?;
            let mut zip = zip::ZipWriter::new(file);

            let options = zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Stored);

            zip.start_file("Data/Textures/test.dds", options)?;
            zip.write_all(b"test texture data")?;

            zip.start_file("meshes/actor/character.nif", options)?;
            zip.write_all(b"test mesh data")?;

            zip.finish()?;
        }

        // Test case-insensitive extraction
        let data = extract_from_zip(&zip_path, "data\\textures\\TEST.DDS")?;
        assert_eq!(data, b"test texture data");

        let data = extract_from_zip(&zip_path, "MESHES/ACTOR/CHARACTER.NIF")?;
        assert_eq!(data, b"test mesh data");

        // Test not found
        let result = extract_from_zip(&zip_path, "notfound.txt");
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_extract_from_archive_zip() -> Result<()> {
        let dir = tempdir()?;
        let zip_path = dir.path().join("test.zip");

        // Create a test ZIP
        {
            let file = File::create(&zip_path)?;
            let mut zip = zip::ZipWriter::new(file);
            let options = zip::write::SimpleFileOptions::default();
            zip.start_file("test.txt", options)?;
            zip.write_all(b"hello world")?;
            zip.finish()?;
        }

        let data = extract_from_archive_with_temp(&zip_path, "test.txt", dir.path())?;
        assert_eq!(data, b"hello world");

        Ok(())
    }
}
