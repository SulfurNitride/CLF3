//! FromArchive directive handler
//!
//! Extracts files from downloaded archives (ZIP, 7z, RAR) to the output directory.
//! Handles nested paths including files inside BSAs within archives.
//!
//! Archive format detection uses magic bytes, not file extensions, to handle
//! mislabeled archives (e.g., a `.zip` that's actually RAR).
//!
//! Uses the 7z binary for all archive extraction (ZIP, 7z, RAR).

use crate::archive::sevenzip;
use crate::bsa;
use crate::installer::processor::ProcessContext;
use crate::modlist::FromArchiveDirective;
use crate::paths;

use anyhow::{bail, Context, Result};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;

/// Archive type detected by magic bytes
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ArchiveType {
    Zip,
    SevenZ,
    Rar,
    Bsa,
    Ba2,
    Unknown,
}

/// Detect archive type by reading magic bytes
pub fn detect_archive_type(path: &Path) -> Result<ArchiveType> {
    let mut file = File::open(path)
        .with_context(|| format!("Failed to open file for magic detection: {}", path.display()))?;

    let mut magic = [0u8; 8];
    let bytes_read = file.read(&mut magic).unwrap_or(0);

    if bytes_read < 4 {
        return Ok(ArchiveType::Unknown);
    }

    // Check magic bytes
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

    // BSA: BSA\x00 followed by version
    if magic[0..4] == [0x42, 0x53, 0x41, 0x00] {
        return Ok(ArchiveType::Bsa);
    }

    // BA2: BTDX
    if magic[0..4] == [0x42, 0x54, 0x44, 0x58] {
        return Ok(ArchiveType::Ba2);
    }

    Ok(ArchiveType::Unknown)
}

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
            bsa::extract_archive_file(&bsa_disk_path, file_path_in_bsa)
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

/// Extract a file from an archive (ZIP, 7z, RAR) using the 7z binary.
/// temp_base_dir is used for extraction temp files (NOT /tmp)
///
/// Archive format is detected by magic bytes, NOT file extension, to handle
/// mislabeled archives (e.g., `.zip` files that are actually RAR).
pub fn extract_from_archive_with_temp(archive_path: &Path, file_path: &str, temp_base_dir: &Path) -> Result<Vec<u8>> {
    // Detect archive type by magic bytes, not extension
    let archive_type = detect_archive_type(archive_path)?;

    match archive_type {
        ArchiveType::Bsa | ArchiveType::Ba2 => {
            // Direct BSA/BA2 extraction (uses ba2 crate)
            bsa::extract_archive_file(archive_path, file_path)
        }
        ArchiveType::Zip | ArchiveType::SevenZ | ArchiveType::Rar => {
            // Use 7z binary for all standard archives
            extract_with_7z(archive_path, file_path, temp_base_dir)
        }
        ArchiveType::Unknown => {
            // Fall back to extension hint for BSA/BA2, otherwise try 7z
            let extension = archive_path
                .extension()
                .and_then(|e| e.to_str())
                .unwrap_or("")
                .to_lowercase();

            match extension.as_str() {
                "bsa" | "ba2" => bsa::extract_archive_file(archive_path, file_path),
                _ => {
                    // Try 7z - it handles most formats
                    extract_with_7z(archive_path, file_path, temp_base_dir)
                }
            }
        }
    }
}

/// Extract a file from an archive using the 7z binary.
/// Handles case-insensitive path matching.
fn extract_with_7z(archive_path: &Path, file_path: &str, temp_base_dir: &Path) -> Result<Vec<u8>> {
    // First try direct extraction (case-insensitive via 7z)
    match sevenzip::extract_file_case_insensitive(archive_path, file_path) {
        Ok(data) => return Ok(data),
        Err(_) => {
            // Fall back to full extraction with case-insensitive search
            tracing::debug!(
                "Direct extraction failed for '{}', trying full extraction",
                file_path
            );
        }
    }

    // Full extraction fallback - extract to temp and find the file
    let temp_dir = tempfile::tempdir_in(temp_base_dir)
        .context("Failed to create temp directory")?;

    sevenzip::extract_all(archive_path, temp_dir.path())
        .with_context(|| format!("Failed to extract archive: {}", archive_path.display()))?;

    // Find the file case-insensitively
    let target_normalized = paths::normalize_for_lookup(file_path);

    for entry in walkdir::WalkDir::new(temp_dir.path())
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
    {
        let rel_path = entry.path()
            .strip_prefix(temp_dir.path())
            .unwrap_or(entry.path());
        let entry_normalized = paths::normalize_for_lookup(&rel_path.to_string_lossy());

        if entry_normalized == target_normalized {
            return fs::read(entry.path())
                .with_context(|| format!("Failed to read extracted file: {}", entry.path().display()));
        }
    }

    bail!(
        "File '{}' not found in archive '{}'",
        file_path,
        archive_path.display()
    )
}

/// Extract an entire archive to a directory using the 7z binary.
///
/// Detects archive type by magic bytes and extracts all contents.
/// Returns the number of files extracted.
pub fn extract_archive_to_dir(archive_path: &Path, output_dir: &Path) -> Result<usize> {
    let archive_type = detect_archive_type(archive_path)?;

    match archive_type {
        ArchiveType::Bsa | ArchiveType::Ba2 => {
            bail!("BSA/BA2 archives should be handled by the bsa module")
        }
        _ => {
            // Use 7z for all other archive types
            sevenzip::extract_all(archive_path, output_dir)
        }
    }
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
    bsa::extract_archive_file(temp_file.path(), file_path_in_bsa)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_detect_archive_type_zip() -> Result<()> {
        let dir = tempdir()?;
        let zip_path = dir.path().join("test.zip");

        // Create a minimal ZIP file manually (PK header)
        // ZIP files start with PK\x03\x04
        let zip_data = [
            0x50, 0x4B, 0x03, 0x04, // Local file header signature
            0x0A, 0x00, // Version needed
            0x00, 0x00, // General purpose bit flag
            0x00, 0x00, // Compression method (stored)
            0x00, 0x00, // File last modification time
            0x00, 0x00, // File last modification date
            0x00, 0x00, 0x00, 0x00, // CRC-32
            0x00, 0x00, 0x00, 0x00, // Compressed size
            0x00, 0x00, 0x00, 0x00, // Uncompressed size
            0x00, 0x00, // File name length
            0x00, 0x00, // Extra field length
        ];
        fs::write(&zip_path, &zip_data)?;

        assert_eq!(detect_archive_type(&zip_path)?, ArchiveType::Zip);
        Ok(())
    }

    #[test]
    fn test_detect_archive_type_7z() -> Result<()> {
        let dir = tempdir()?;
        let sz_path = dir.path().join("test.7z");

        // 7z files start with 7z\xBC\xAF\x27\x1C
        let sz_data = [0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C, 0x00, 0x00];
        fs::write(&sz_path, &sz_data)?;

        assert_eq!(detect_archive_type(&sz_path)?, ArchiveType::SevenZ);
        Ok(())
    }

    #[test]
    fn test_detect_archive_type_rar() -> Result<()> {
        let dir = tempdir()?;
        let rar_path = dir.path().join("test.rar");

        // RAR files start with Rar!
        let rar_data = [0x52, 0x61, 0x72, 0x21, 0x1A, 0x07, 0x00, 0x00];
        fs::write(&rar_path, &rar_data)?;

        assert_eq!(detect_archive_type(&rar_path)?, ArchiveType::Rar);
        Ok(())
    }

    #[test]
    fn test_detect_archive_type_bsa() -> Result<()> {
        let dir = tempdir()?;
        let bsa_path = dir.path().join("test.bsa");

        // BSA files start with BSA\x00
        let bsa_data = [0x42, 0x53, 0x41, 0x00, 0x68, 0x00, 0x00, 0x00];
        fs::write(&bsa_path, &bsa_data)?;

        assert_eq!(detect_archive_type(&bsa_path)?, ArchiveType::Bsa);
        Ok(())
    }

    #[test]
    fn test_detect_archive_type_ba2() -> Result<()> {
        let dir = tempdir()?;
        let ba2_path = dir.path().join("test.ba2");

        // BA2 files start with BTDX
        let ba2_data = [0x42, 0x54, 0x44, 0x58, 0x01, 0x00, 0x00, 0x00];
        fs::write(&ba2_path, &ba2_data)?;

        assert_eq!(detect_archive_type(&ba2_path)?, ArchiveType::Ba2);
        Ok(())
    }

    #[test]
    fn test_detect_archive_type_mislabeled() -> Result<()> {
        let dir = tempdir()?;

        // Create a file with ZIP magic but .rar extension
        let fake_rar_path = dir.path().join("mislabeled.rar");
        let zip_data = [0x50, 0x4B, 0x03, 0x04, 0x00, 0x00, 0x00, 0x00];
        fs::write(&fake_rar_path, &zip_data)?;

        // Magic byte detection should identify it as ZIP despite .rar extension
        assert_eq!(detect_archive_type(&fake_rar_path)?, ArchiveType::Zip);

        Ok(())
    }
}
