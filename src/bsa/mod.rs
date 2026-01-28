//! BSA (Bethesda Archive) handling
//!
//! Provides read/write support for TES4 format BSA files (Oblivion, FO3, FNV)
//! with SQLite-based caching and rayon parallelism.

// Module not yet integrated into main installation pipeline
#![allow(dead_code)]
#![allow(unused_imports)]

mod cache;
mod reader;
mod writer;

pub use cache::BsaCache;
pub use reader::{BsaReader, BsaFileEntry, extract_file, extract_batch_parallel, list_files};
pub use writer::{BsaBuilder, BsaWriterManager};

use ba2::tes4::{ArchiveFlags, ArchiveTypes, Version};

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
