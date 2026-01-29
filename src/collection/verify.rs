//! Archive verification for collection installations.
//!
//! Provides verification of downloaded archives:
//! - File size validation
//! - MD5 hash verification
//! - Auto-fix capabilities (delete and re-download corrupted files)

use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::Path;

use anyhow::{Context, Result};
use tracing::{info, warn};

use super::db::{CollectionDb, ModDbEntry, ModStatus};

/// Result of verifying a single archive
#[derive(Debug, Clone)]
pub enum VerifyResult {
    /// Archive is valid
    Valid,
    /// Archive file is missing
    Missing,
    /// Archive size doesn't match expected
    SizeMismatch {
        expected: u64,
        actual: u64,
    },
    /// MD5 hash doesn't match
    HashMismatch {
        expected: String,
        actual: String,
    },
    /// File cannot be read
    ReadError(String),
}

impl VerifyResult {
    pub fn is_valid(&self) -> bool {
        matches!(self, VerifyResult::Valid)
    }

    pub fn error_message(&self) -> Option<String> {
        match self {
            VerifyResult::Valid => None,
            VerifyResult::Missing => Some("File is missing".to_string()),
            VerifyResult::SizeMismatch { expected, actual } => {
                let percent = if *expected > 0 {
                    (*actual as f64 / *expected as f64 * 100.0) as u64
                } else {
                    0
                };
                Some(format!(
                    "Size mismatch: expected {} bytes, got {} ({}%)",
                    expected, actual, percent
                ))
            }
            VerifyResult::HashMismatch { expected, actual } => {
                Some(format!(
                    "MD5 mismatch: expected {}, got {}",
                    expected, actual
                ))
            }
            VerifyResult::ReadError(msg) => Some(format!("Read error: {}", msg)),
        }
    }
}

/// Verification options
#[derive(Debug, Clone)]
pub struct VerifyOptions {
    /// Check file size
    pub check_size: bool,
    /// Check MD5 hash (slower)
    pub check_hash: bool,
}

impl Default for VerifyOptions {
    fn default() -> Self {
        Self {
            check_size: true,
            check_hash: true,
        }
    }
}

/// Verify a single archive file
pub fn verify_archive(
    path: &Path,
    expected_size: Option<u64>,
    expected_md5: Option<&str>,
    options: &VerifyOptions,
) -> VerifyResult {
    // Check if file exists
    if !path.exists() {
        return VerifyResult::Missing;
    }

    // Get file metadata
    let metadata = match fs::metadata(path) {
        Ok(m) => m,
        Err(e) => return VerifyResult::ReadError(e.to_string()),
    };

    // Check size
    if options.check_size {
        if let Some(expected) = expected_size {
            if expected > 0 && metadata.len() != expected {
                return VerifyResult::SizeMismatch {
                    expected,
                    actual: metadata.len(),
                };
            }
        }
    }

    // Check MD5 hash
    if options.check_hash {
        if let Some(expected) = expected_md5 {
            if !expected.is_empty() {
                match compute_md5(path) {
                    Ok(actual) => {
                        if !actual.eq_ignore_ascii_case(expected) {
                            return VerifyResult::HashMismatch {
                                expected: expected.to_string(),
                                actual,
                            };
                        }
                    }
                    Err(e) => return VerifyResult::ReadError(e.to_string()),
                }
            }
        }
    }

    VerifyResult::Valid
}

/// Compute MD5 hash of a file
pub fn compute_md5(path: &Path) -> Result<String> {
    use std::io::Write;

    let file = File::open(path).with_context(|| format!("Failed to open {}", path.display()))?;
    let mut reader = BufReader::with_capacity(1024 * 1024, file); // 1MB buffer
    let mut context = md5::Context::new();

    let mut buffer = [0u8; 65536]; // 64KB read buffer
    loop {
        let bytes_read = reader
            .read(&mut buffer)
            .with_context(|| format!("Failed to read {}", path.display()))?;
        if bytes_read == 0 {
            break;
        }
        context.write_all(&buffer[..bytes_read])?;
    }

    let digest = context.compute();
    Ok(format!("{:x}", digest))
}

/// Verify all downloaded mods in a collection (parallel with rayon)
pub fn verify_all_downloads(
    db: &CollectionDb,
    downloads_dir: &Path,
    options: &VerifyOptions,
) -> Result<Vec<(ModDbEntry, VerifyResult)>> {
    use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
    use rayon::prelude::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    let downloaded_mods = db.get_mods_by_status(ModStatus::Downloaded)?;

    if downloaded_mods.is_empty() {
        return Ok(Vec::new());
    }

    println!("Verifying {} downloaded archives...", downloaded_mods.len());

    // Setup progress display
    let mp = MultiProgress::new();
    let overall_pb = mp.add(ProgressBar::new(downloaded_mods.len() as u64));
    overall_pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] Verifying [{bar:40.cyan/blue}] {pos}/{len} | {msg}")
            .unwrap()
            .progress_chars("=>-"),
    );
    overall_pb.enable_steady_tick(Duration::from_millis(100));

    let valid_count = AtomicUsize::new(0);
    let invalid_count = AtomicUsize::new(0);

    // Prepare data for parallel processing
    let downloads_dir = downloads_dir.to_path_buf();
    let options = options.clone();

    // Verify in parallel
    let results: Vec<(ModDbEntry, VerifyResult)> = downloaded_mods
        .into_par_iter()
        .map(|mod_entry| {
            let archive_path = if let Some(ref local_path) = mod_entry.local_path {
                Path::new(local_path).to_path_buf()
            } else {
                downloads_dir.join(&mod_entry.logical_filename)
            };

            let result = verify_archive(
                &archive_path,
                if mod_entry.file_size > 0 {
                    Some(mod_entry.file_size as u64)
                } else {
                    None
                },
                if mod_entry.md5.is_empty() {
                    None
                } else {
                    Some(&mod_entry.md5)
                },
                &options,
            );

            if result.is_valid() {
                valid_count.fetch_add(1, Ordering::Relaxed);
            } else {
                invalid_count.fetch_add(1, Ordering::Relaxed);
            }

            overall_pb.inc(1);
            overall_pb.set_message(format!(
                "OK:{} Fail:{}",
                valid_count.load(Ordering::Relaxed),
                invalid_count.load(Ordering::Relaxed)
            ));

            (mod_entry, result)
        })
        .collect();

    overall_pb.finish_and_clear();

    let valid = valid_count.load(Ordering::Relaxed);
    let invalid = invalid_count.load(Ordering::Relaxed);
    println!("Verification complete: {} valid, {} invalid", valid, invalid);

    Ok(results)
}

/// Auto-fix corrupted archives by deleting and resetting their status
pub fn auto_fix_corrupted(
    db: &CollectionDb,
    downloads_dir: &Path,
    corrupted: &[(ModDbEntry, VerifyResult)],
) -> Result<usize> {
    let mut fixed_count = 0;

    for (mod_entry, result) in corrupted {
        if result.is_valid() {
            continue;
        }

        // Delete the corrupted file if it exists
        let archive_path = if let Some(ref local_path) = mod_entry.local_path {
            Path::new(local_path).to_path_buf()
        } else {
            downloads_dir.join(&mod_entry.logical_filename)
        };

        if archive_path.exists() {
            if let Err(e) = fs::remove_file(&archive_path) {
                warn!(
                    "Failed to delete corrupted file {}: {}",
                    archive_path.display(),
                    e
                );
                continue;
            }
            info!("Deleted corrupted archive: {}", archive_path.display());
        }

        // Reset mod status to pending for re-download
        db.update_mod_status(mod_entry.id, ModStatus::Pending)?;
        fixed_count += 1;
    }

    Ok(fixed_count)
}

/// Run a full verification pass with auto-fix and retry logic
pub struct VerificationLoop {
    pub max_attempts: usize,
    pub options: VerifyOptions,
}

impl Default for VerificationLoop {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            options: VerifyOptions::default(),
        }
    }
}

/// Result of running the verification loop
#[derive(Debug)]
pub struct VerificationLoopResult {
    /// Total number of mods verified
    pub total_verified: usize,
    /// Number of mods that passed verification
    pub valid: usize,
    /// Number of mods that failed after all attempts
    pub failed: usize,
    /// Number of re-download attempts made
    pub attempts: usize,
    /// Final list of failures (mod name, error message)
    pub failures: Vec<(String, String)>,
}

impl VerificationLoopResult {
    pub fn all_valid(&self) -> bool {
        self.failed == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    #[test]
    fn test_verify_missing_file() {
        let result = verify_archive(
            Path::new("/nonexistent/file.zip"),
            Some(1000),
            None,
            &VerifyOptions::default(),
        );
        assert!(matches!(result, VerifyResult::Missing));
    }

    #[test]
    fn test_verify_size_mismatch() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.zip");

        // Create a file with 100 bytes
        let mut file = File::create(&file_path).unwrap();
        file.write_all(&[0u8; 100]).unwrap();

        let result = verify_archive(
            &file_path,
            Some(200), // Expected 200 bytes
            None,
            &VerifyOptions::default(),
        );

        match result {
            VerifyResult::SizeMismatch { expected, actual } => {
                assert_eq!(expected, 200);
                assert_eq!(actual, 100);
            }
            _ => panic!("Expected SizeMismatch, got {:?}", result),
        }
    }

    #[test]
    fn test_verify_valid() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.zip");

        // Create a file with known content
        let content = b"test content";
        let mut file = File::create(&file_path).unwrap();
        file.write_all(content).unwrap();

        let result = verify_archive(
            &file_path,
            Some(content.len() as u64),
            None,
            &VerifyOptions {
                check_size: true,
                check_hash: false, // Skip hash for this test
            },
        );

        assert!(result.is_valid());
    }

    #[test]
    fn test_compute_md5() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");

        // Known MD5: "test" = 098f6bcd4621d373cade4e832627b4f6
        let mut file = File::create(&file_path).unwrap();
        file.write_all(b"test").unwrap();

        let md5 = compute_md5(&file_path).unwrap();
        assert_eq!(md5, "098f6bcd4621d373cade4e832627b4f6");
    }

    #[test]
    fn test_verify_result_error_messages() {
        assert!(VerifyResult::Valid.error_message().is_none());
        assert!(VerifyResult::Missing.error_message().is_some());
        assert!(VerifyResult::SizeMismatch {
            expected: 100,
            actual: 50
        }
        .error_message()
        .unwrap()
        .contains("50%"));
    }
}
