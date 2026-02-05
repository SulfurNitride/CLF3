//! Archive handling utilities.
//!
//! This module provides unified archive extraction using the 7z binary for
//! all archive formats (ZIP, 7z, RAR). This replaces the previous approach
//! of using separate Rust crates (zip, unrar, sevenz-rust).
//!
//! # Archive Ordering
//!
//! For optimal extraction performance, archives should be processed in this order:
//! 1. ZIP files (fastest - random access)
//! 2. RAR files (medium - can skip entries)
//! 3. 7z non-solid (medium - random access within blocks)
//! 4. 7z solid (slowest - requires sequential decompression)
//!
//! # Solid Archive Detection
//!
//! 7z archives can be "solid" where multiple files are compressed together.
//! This provides better compression but requires sequential decompression.
//! Use `is_solid_archive()` to detect solid archives.
//!
//! For BSA/BA2 Bethesda archives, see the `bsa` module which uses the ba2 crate.

pub mod sevenzip;

// Re-export commonly used functions for convenience
#[allow(unused_imports)] // Used by lib crate consumers
pub use sevenzip::{
    extract_all, extract_all_with_threads, extract_file, extract_file_case_insensitive,
    get_7z_path, is_solid_archive, list_archive,
};
