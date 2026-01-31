//! File verification and cleanup for Wabbajack installations.
//!
//! This module provides post-installation verification to ensure all files
//! match expected sizes/hashes. Files that don't match are cleaned up and
//! their source archives are marked for reprocessing.
//!
//! # Verification Flow
//!
//! 1. After FromArchive extraction: verify all outputs match expected sizes
//! 2. After patching: verify patched files match expected sizes
//! 3. Failed files are deleted
//! 4. Failed archives are returned for reprocessing

use crate::modlist::ModlistDb;
use crate::paths;
use anyhow::Result;
use indicatif::ProgressBar;
use rayon::prelude::*;
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

/// Result of verification for a single file
#[derive(Debug, Clone)]
pub struct VerificationResult {
    /// Directive ID
    pub id: i64,
    /// Output path (relative to output dir)
    pub to_path: String,
    /// Expected size in bytes
    pub expected_size: u64,
    /// Actual size found (None if file doesn't exist)
    pub actual_size: Option<u64>,
    /// Archive hash this file came from
    pub archive_hash: Option<String>,
    /// Whether verification passed
    pub passed: bool,
}

/// Summary of verification results
#[derive(Debug, Default)]
pub struct VerificationSummary {
    /// Total files verified
    pub total: usize,
    /// Files that passed verification
    pub passed: usize,
    /// Files that failed (wrong size or missing)
    pub failed: usize,
    /// Files deleted due to failure
    pub cleaned: usize,
    /// Archive hashes that need reprocessing
    pub failed_archives: HashSet<String>,
}

/// Verify all completed FromArchive directives match expected sizes.
///
/// Returns a summary with failed archives that need reprocessing.
pub fn verify_from_archive_outputs(
    db: &ModlistDb,
    output_dir: &Path,
    pb: &ProgressBar,
    clean_failures: bool,
) -> Result<VerificationSummary> {
    pb.set_message("Loading completed FromArchive directives for verification...");

    // Get all completed FromArchive directives
    let completed = db.get_completed_directives_of_type("FromArchive")?;

    if completed.is_empty() {
        return Ok(VerificationSummary::default());
    }

    eprintln!("Verifying {} FromArchive outputs...", completed.len());

    // Reset progress bar
    pb.finish_and_clear();
    pb.reset();
    pb.set_length(completed.len() as u64);
    pb.set_position(0);
    pb.enable_steady_tick(std::time::Duration::from_millis(100));
    pb.set_message("Verifying extracted files...");

    let passed = AtomicUsize::new(0);
    let failed_count = AtomicUsize::new(0);
    let failed_archives: Mutex<HashSet<String>> = Mutex::new(HashSet::new());
    let files_to_clean: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

    // Verify in parallel
    completed.par_iter().for_each(|(_id, to_path, expected_size, archive_hash)| {
        let output_path = paths::join_windows_path(output_dir, to_path);

        let actual_size = fs::metadata(&output_path).ok().map(|m| m.len());

        let is_valid = actual_size == Some(*expected_size as u64);

        if is_valid {
            passed.fetch_add(1, Ordering::Relaxed);
        } else {
            failed_count.fetch_add(1, Ordering::Relaxed);

            // Track the archive for reprocessing
            if let Some(hash) = archive_hash {
                failed_archives.lock().unwrap().insert(hash.clone());
            }

            // Mark file for cleanup
            if output_path.exists() {
                files_to_clean.lock().unwrap().push(output_path);
            }
        }

        pb.inc(1);
        pb.set_message(format!(
            "Verified: {} ok, {} failed",
            passed.load(Ordering::Relaxed),
            failed_count.load(Ordering::Relaxed)
        ));
    });

    let failed_archives = failed_archives.into_inner().unwrap();
    let files_to_clean = files_to_clean.into_inner().unwrap();
    let cleaned_count;

    // Clean up failed files if requested
    if clean_failures && !files_to_clean.is_empty() {
        pb.set_message(format!("Cleaning {} invalid files...", files_to_clean.len()));
        cleaned_count = clean_files(&files_to_clean);
    } else {
        cleaned_count = 0;
    }

    let summary = VerificationSummary {
        total: completed.len(),
        passed: passed.load(Ordering::Relaxed),
        failed: failed_count.load(Ordering::Relaxed),
        cleaned: cleaned_count,
        failed_archives,
    };

    if summary.failed > 0 {
        eprintln!(
            "Verification: {} passed, {} failed ({} archives need reprocessing)",
            summary.passed,
            summary.failed,
            summary.failed_archives.len()
        );
    } else {
        eprintln!("Verification: all {} files passed", summary.passed);
    }

    Ok(summary)
}

/// Verify all completed PatchedFromArchive directives.
pub fn verify_patched_outputs(
    db: &ModlistDb,
    output_dir: &Path,
    pb: &ProgressBar,
    clean_failures: bool,
) -> Result<VerificationSummary> {
    pb.set_message("Loading completed PatchedFromArchive directives for verification...");

    let completed = db.get_completed_directives_of_type("PatchedFromArchive")?;

    if completed.is_empty() {
        return Ok(VerificationSummary::default());
    }

    eprintln!("Verifying {} patched outputs...", completed.len());

    pb.finish_and_clear();
    pb.reset();
    pb.set_length(completed.len() as u64);
    pb.set_position(0);
    pb.enable_steady_tick(std::time::Duration::from_millis(100));
    pb.set_message("Verifying patched files...");

    let passed = AtomicUsize::new(0);
    let failed_count = AtomicUsize::new(0);
    let failed_archives: Mutex<HashSet<String>> = Mutex::new(HashSet::new());
    let files_to_clean: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

    completed.par_iter().for_each(|(_id, to_path, expected_size, archive_hash)| {
        let output_path = paths::join_windows_path(output_dir, to_path);

        let actual_size = fs::metadata(&output_path).ok().map(|m| m.len());
        let is_valid = actual_size == Some(*expected_size as u64);

        if is_valid {
            passed.fetch_add(1, Ordering::Relaxed);
        } else {
            failed_count.fetch_add(1, Ordering::Relaxed);

            if let Some(hash) = archive_hash {
                failed_archives.lock().unwrap().insert(hash.clone());
            }

            if output_path.exists() {
                files_to_clean.lock().unwrap().push(output_path);
            }
        }

        pb.inc(1);
    });

    let failed_archives = failed_archives.into_inner().unwrap();
    let files_to_clean = files_to_clean.into_inner().unwrap();
    let cleaned_count;

    if clean_failures && !files_to_clean.is_empty() {
        pb.set_message(format!("Cleaning {} invalid patched files...", files_to_clean.len()));
        cleaned_count = clean_files(&files_to_clean);
    } else {
        cleaned_count = 0;
    }

    Ok(VerificationSummary {
        total: completed.len(),
        passed: passed.load(Ordering::Relaxed),
        failed: failed_count.load(Ordering::Relaxed),
        cleaned: cleaned_count,
        failed_archives,
    })
}

/// Reset failed directives back to pending status for reprocessing.
///
/// This allows the installer to re-run processing for specific archives.
pub fn reset_failed_archives(db: &ModlistDb, archive_hashes: &HashSet<String>) -> Result<usize> {
    if archive_hashes.is_empty() {
        return Ok(0);
    }

    let mut total_reset = 0;

    for hash in archive_hashes {
        let reset = db.reset_directives_for_archive(hash)?;
        total_reset += reset;
    }

    eprintln!("Reset {} directives for {} archives", total_reset, archive_hashes.len());

    Ok(total_reset)
}

/// Clean (delete) a list of files.
fn clean_files(files: &[PathBuf]) -> usize {
    let mut cleaned = 0;
    for path in files {
        if fs::remove_file(path).is_ok() {
            cleaned += 1;
        }
    }
    cleaned
}

/// Scan output directory for files not expected by Wabbajack.
///
/// Returns paths of unexpected files that should be removed for a clean install.
pub fn find_unexpected_files(
    db: &ModlistDb,
    output_dir: &Path,
    pb: &ProgressBar,
) -> Result<Vec<PathBuf>> {
    pb.set_message("Building expected files list from directives...");

    // Get all expected output paths (normalized for lookup)
    let expected_paths = db.get_all_expected_output_paths()?;
    let expected_set: HashSet<String> = expected_paths
        .into_iter()
        .map(|p| paths::normalize_for_lookup(&p))
        .collect();

    eprintln!("Expected {} files from Wabbajack", expected_set.len());

    pb.set_message("Scanning output directory for unexpected files...");

    // Walk the output directory
    let mut unexpected = Vec::new();
    let mut scanned = 0;

    for entry in walkdir::WalkDir::new(output_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
    {
        scanned += 1;
        if scanned % 10000 == 0 {
            pb.set_message(format!("Scanned {} files...", scanned));
        }

        // Get relative path from output_dir
        if let Ok(rel_path) = entry.path().strip_prefix(output_dir) {
            let normalized = paths::normalize_for_lookup(&rel_path.to_string_lossy());

            if !expected_set.contains(&normalized) {
                unexpected.push(entry.path().to_path_buf());
            }
        }
    }

    eprintln!(
        "Scanned {} files, found {} unexpected",
        scanned,
        unexpected.len()
    );

    Ok(unexpected)
}

/// Remove unexpected files for a clean install.
pub fn clean_unexpected_files(files: &[PathBuf], pb: &ProgressBar) -> usize {
    if files.is_empty() {
        return 0;
    }

    pb.set_message(format!("Removing {} unexpected files...", files.len()));
    pb.set_length(files.len() as u64);
    pb.set_position(0);

    let mut removed = 0;
    for path in files {
        if fs::remove_file(path).is_ok() {
            removed += 1;
        }
        pb.inc(1);
    }

    // Clean up empty directories
    pb.set_message("Removing empty directories...");
    // Note: We could walk directories bottom-up and remove empty ones here

    eprintln!("Removed {} unexpected files", removed);
    removed
}
