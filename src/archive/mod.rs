//! Archive handling utilities.
//!
//! Provides unified archive extraction using native Rust crates for
//! ZIP (zip crate), 7z (sevenz-rust2), and RAR (unrar), with fallback
//! to the 7z binary for edge cases.
//!
//! For BSA/BA2 Bethesda archives, see the `bsa` module which uses the ba2 crate.

pub mod sevenzip;

// Re-export commonly used functions for convenience
#[allow(unused_imports)] // Used by lib crate consumers
pub use sevenzip::{
    extract_all, extract_all_with_threads, extract_file, extract_file_case_insensitive,
    get_7z_path, is_solid_archive, list_archive,
};
