//! BSA/BA2 (Bethesda Archive) handling
//!
//! Provides read/write support for:
//! - TES3 format BSA files (Morrowind)
//! - TES4 format BSA files (Oblivion, FO3, FNV, Skyrim)
//! - FO4 format BA2 files (Fallout 4, Fallout 76, Starfield)
//!
//! Uses SQLite-based caching and rayon parallelism.

// Module not yet integrated into main installation pipeline
#![allow(dead_code)]
#![allow(unused_imports)]

mod ba2_reader;
mod ba2_writer;
mod cache;
mod reader;
mod tes3_reader;
mod writer;

pub use cache::BsaCache;
pub use reader::{BsaReader, BsaFileEntry, extract_file, extract_batch_parallel, list_files};
pub use writer::{BsaBuilder, BsaWriterManager};

// TES3 (Morrowind) support
pub use tes3_reader::{
    list_files as list_tes3_files,
    extract_file as extract_tes3_file,
    extract_batch_parallel as extract_tes3_batch_parallel,
};

// BA2 support for Fallout 4/Starfield
pub use ba2_reader::{
    Ba2FileEntry,
    list_files as list_ba2_files,
    extract_file as extract_ba2_file,
    extract_batch_parallel as extract_ba2_batch_parallel,
};
pub use ba2_writer::{Ba2Builder, Ba2Format, Ba2CompressionFormat, Ba2Version};

use anyhow::{bail, Result};
use ba2::tes4::{ArchiveFlags, ArchiveTypes, Version};
use ba2::{FileFormat, guess_format};
use std::fs::File;
use std::io::{Read, BufReader};
use std::path::Path;
use tracing::debug;

/// Archive format type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveFormat {
    /// TES3 BSA (Morrowind)
    Tes3Bsa,
    /// TES4 BSA (Oblivion, FO3, FNV, Skyrim)
    Bsa,
    /// FO4 BA2 (Fallout 4, Fallout 76, Starfield)
    Ba2,
}

/// Detect archive format using ba2 crate's guess_format
pub fn detect_format(path: &Path) -> Option<ArchiveFormat> {
    // Use ba2's built-in format detection
    if let Ok(file) = File::open(path) {
        let mut reader = BufReader::new(file);
        if let Some(format) = guess_format(&mut reader) {
            let result = match format {
                FileFormat::TES3 => ArchiveFormat::Tes3Bsa,
                FileFormat::TES4 => ArchiveFormat::Bsa,
                FileFormat::FO4 => ArchiveFormat::Ba2,
            };
            debug!("Detected {:?} format for: {}", result, path.display());
            return Some(result);
        }
    }

    // Fall back to extension
    let ext = path.extension()?.to_str()?.to_lowercase();
    match ext.as_str() {
        "bsa" => {
            debug!("Detected BSA by extension (assuming TES4): {}", path.display());
            Some(ArchiveFormat::Bsa)
        }
        "ba2" => {
            debug!("Detected BA2 by extension: {}", path.display());
            Some(ArchiveFormat::Ba2)
        }
        _ => None,
    }
}

/// Universal archive file entry
#[derive(Debug, Clone)]
pub struct ArchiveFileEntry {
    pub path: String,
    pub size: u64,
    pub format: ArchiveFormat,
    /// True if this is a DX10 texture (BA2 only)
    pub is_texture: bool,
}

/// List files from any Bethesda archive (TES3 BSA, TES4 BSA, or BA2)
pub fn list_archive_files(archive_path: &Path) -> Result<Vec<ArchiveFileEntry>> {
    match detect_format(archive_path) {
        Some(ArchiveFormat::Tes3Bsa) => {
            let files = list_tes3_files(archive_path)?;
            Ok(files.into_iter().map(|f| ArchiveFileEntry {
                path: f.path,
                size: f.size,
                format: ArchiveFormat::Tes3Bsa,
                is_texture: false,
            }).collect())
        }
        Some(ArchiveFormat::Bsa) => {
            let files = list_files(archive_path)?;
            Ok(files.into_iter().map(|f| ArchiveFileEntry {
                path: f.path,
                size: f.size,
                format: ArchiveFormat::Bsa,
                is_texture: false,
            }).collect())
        }
        Some(ArchiveFormat::Ba2) => {
            let files = list_ba2_files(archive_path)?;
            Ok(files.into_iter().map(|f| ArchiveFileEntry {
                path: f.path,
                size: f.size,
                format: ArchiveFormat::Ba2,
                is_texture: f.is_texture,
            }).collect())
        }
        None => bail!("Unknown archive format: {}", archive_path.display()),
    }
}

/// Extract a file from any Bethesda archive (TES3 BSA, TES4 BSA, or BA2)
pub fn extract_archive_file(archive_path: &Path, file_path: &str) -> Result<Vec<u8>> {
    let format = detect_format(archive_path);
    debug!(
        "extract_archive_file: archive={}, file={}, format={:?}",
        archive_path.display(),
        file_path,
        format
    );
    match format {
        Some(ArchiveFormat::Tes3Bsa) => extract_tes3_file(archive_path, file_path),
        Some(ArchiveFormat::Bsa) => extract_file(archive_path, file_path),
        Some(ArchiveFormat::Ba2) => extract_ba2_file(archive_path, file_path),
        None => bail!("Unknown archive format: {}", archive_path.display()),
    }
}

/// BSA version detection from archive name
pub fn detect_version(name: &str) -> Version {
    let name_lower = name.to_lowercase();

    // Oblivion uses v103
    if name_lower.contains("oblivion")
        || name_lower.contains("shiveringisles")
        || name_lower.contains("dlcshiveringisles")
        || name_lower.contains("dlcbattlehorn")
        || name_lower.contains("dlcfrostcrag")
        || name_lower.contains("dlchorse")
        || name_lower.contains("dlcorrery")
        || name_lower.contains("dlcthievesden")
        || name_lower.contains("dlcvilelair")
        || name_lower.contains("knights")
    {
        Version::v103
    } else {
        // Default to FO3/FNV
        Version::v104
    }
}

/// Detect archive types from BSA name
pub fn detect_types(name: &str) -> ArchiveTypes {
    let name_lower = name.to_lowercase();

    if name_lower.contains("meshes") {
        ArchiveTypes::MESHES
    } else if name_lower.contains("textures") {
        ArchiveTypes::TEXTURES
    } else if name_lower.contains("menuvoices") {
        ArchiveTypes::MENUS | ArchiveTypes::VOICES
    } else if name_lower.contains("voices") {
        ArchiveTypes::VOICES
    } else if name_lower.contains("sound") {
        ArchiveTypes::SOUNDS
    } else {
        ArchiveTypes::MISC
    }
}

/// Default flags for FO3/FNV BSAs
pub fn default_flags_fo3() -> ArchiveFlags {
    ArchiveFlags::DIRECTORY_STRINGS
        | ArchiveFlags::FILE_STRINGS
        | ArchiveFlags::COMPRESSED
        | ArchiveFlags::RETAIN_DIRECTORY_NAMES
        | ArchiveFlags::RETAIN_FILE_NAMES
        | ArchiveFlags::RETAIN_FILE_NAME_OFFSETS
}

/// Default flags for Oblivion BSAs (no compression)
pub fn default_flags_oblivion() -> ArchiveFlags {
    ArchiveFlags::DIRECTORY_STRINGS | ArchiveFlags::FILE_STRINGS
}
