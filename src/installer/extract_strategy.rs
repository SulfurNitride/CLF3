//! Shared extraction strategy decisions for archive processing.
//!
//! Keeps selective-vs-full extraction logic consistent across install and patch phases.

use crate::archive::sevenzip;
use crate::installer::handlers::from_archive::{detect_archive_type, ArchiveType};

use std::path::Path;
use std::sync::OnceLock;

/// Max number of files to request selectively before preferring full extraction.
pub fn selective_extract_threshold() -> usize {
    static THRESHOLD: OnceLock<usize> = OnceLock::new();
    *THRESHOLD.get_or_init(|| {
        std::env::var("CLF3_SELECTIVE_EXTRACT_THRESHOLD")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&v| v > 0)
            .unwrap_or(25)
    })
}

/// Decide whether selective extraction should be used for an archive.
///
/// Rules:
/// - Never selective when no files are needed.
/// - Never selective when request set exceeds configured threshold.
/// - Never selective for BSA/BA2 (handled by direct readers).
/// - For 7z, only selective for non-solid archives.
pub fn should_use_selective_extraction(archive_path: &Path, needed_files: usize) -> bool {
    if needed_files == 0 || needed_files > selective_extract_threshold() {
        return false;
    }

    match detect_archive_type(archive_path).unwrap_or(ArchiveType::Unknown) {
        ArchiveType::SevenZ => matches!(sevenzip::is_solid_archive(archive_path), Ok(false)),
        ArchiveType::Zip | ArchiveType::Rar | ArchiveType::Unknown => true,
        ArchiveType::Tes3Bsa | ArchiveType::Bsa | ArchiveType::Ba2 => false,
    }
}
