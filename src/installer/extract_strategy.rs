//! Shared extraction strategy decisions for archive processing.
//!
//! Keeps selective-vs-full extraction logic consistent across install and patch phases.

use crate::archive::sevenzip;
use crate::installer::handlers::from_archive::{detect_archive_type, ArchiveType};

use std::path::Path;
use std::sync::OnceLock;

/// Max number of files to request selectively before preferring full extraction.
/// Set very high by default — Wabbajack always passes file lists to 7z regardless
/// of count, and selective extraction avoids writing unneeded files to temp.
pub fn selective_extract_threshold() -> usize {
    static THRESHOLD: OnceLock<usize> = OnceLock::new();
    *THRESHOLD.get_or_init(|| {
        std::env::var("CLF3_SELECTIVE_EXTRACT_THRESHOLD")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&v| v > 0)
            .unwrap_or(10_000)
    })
}

/// Decide whether selective extraction should be used for an archive.
///
/// Rules:
/// - Never selective when no files are needed.
/// - Never selective for BSA/BA2 (handled by direct readers).
/// - Always selective for solid 7z: decompression work is identical (must decompress
///   everything in a solid archive), but selective skips writing unneeded files to disk.
///   An 11 GB solid archive needing 50 files writes 10 MB instead of 11 GB to temp.
/// - For non-solid archives, selective up to a high threshold (10k files).
pub fn should_use_selective_extraction(archive_path: &Path, needed_files: usize) -> bool {
    if needed_files == 0 {
        return false;
    }

    match detect_archive_type(archive_path).unwrap_or(ArchiveType::Unknown) {
        // Solid 7z: ALWAYS selective — decompression cost is the same, but avoids
        // writing the entire archive to temp. Unwanted entries drain to io::sink().
        ArchiveType::SevenZ => match sevenzip::is_solid_archive(archive_path) {
            Ok(true) => true,
            Ok(false) => needed_files <= selective_extract_threshold(),
            Err(_) => needed_files <= selective_extract_threshold(),
        },
        ArchiveType::Zip | ArchiveType::Rar | ArchiveType::Unknown => {
            needed_files <= selective_extract_threshold()
        }
        ArchiveType::Tes3Bsa | ArchiveType::Bsa | ArchiveType::Ba2 => false,
    }
}
