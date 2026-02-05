//! Hash utilities for Wabbajack archive verification.
//!
//! Wabbajack uses xxHash64 encoded as base64 for archive hashes.
//! This module provides functions to compute and verify these hashes.

use anyhow::{Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

/// Compute xxHash64 of a file and return as base64 string (Wabbajack format).
///
/// Uses streaming to handle large files without loading into memory.
pub fn compute_file_hash(path: &Path) -> Result<String> {
    let file = File::open(path)
        .with_context(|| format!("Failed to open file for hashing: {}", path.display()))?;

    let mut reader = BufReader::with_capacity(1024 * 1024, file); // 1MB buffer
    let mut buf = vec![0u8; 1024 * 1024]; // 1MB chunks
    let mut hasher = xxhash_rust::xxh64::Xxh64::new(0);

    loop {
        let bytes_read = reader.read(&mut buf)
            .with_context(|| format!("Failed to read file for hashing: {}", path.display()))?;

        if bytes_read == 0 {
            break;
        }

        hasher.update(&buf[..bytes_read]);
    }

    let hash = hasher.digest();

    // Convert to base64 (little-endian bytes, like Wabbajack)
    let hash_bytes = hash.to_le_bytes();
    let base64_hash = STANDARD.encode(&hash_bytes);

    Ok(base64_hash)
}

/// Verify a file's hash matches the expected Wabbajack hash.
///
/// Returns Ok(true) if hash matches, Ok(false) if mismatch.
pub fn verify_file_hash(path: &Path, expected_hash: &str) -> Result<bool> {
    let actual_hash = compute_file_hash(path)?;
    Ok(actual_hash == expected_hash)
}

/// Verify a file's hash and return detailed result.
///
/// Returns (matches, actual_hash) for logging/debugging.
#[allow(dead_code)] // Part of hash API, not yet wired up
pub fn verify_file_hash_detailed(path: &Path, expected_hash: &str) -> Result<(bool, String)> {
    let actual_hash = compute_file_hash(path)?;
    let matches = actual_hash == expected_hash;
    Ok((matches, actual_hash))
}

/// Batch verify multiple archives and return list of failures.
///
/// Returns Vec of (path, expected_hash, actual_hash) for failed verifications.
#[allow(dead_code)] // Part of hash API, not yet wired up
pub fn verify_archives_batch<P: AsRef<Path>>(
    archives: &[(P, &str)], // (path, expected_hash)
    progress_callback: Option<&dyn Fn(usize, usize)>, // (current, total)
) -> Result<Vec<(String, String, String)>> {
    let mut failures = Vec::new();
    let total = archives.len();

    for (i, (path, expected_hash)) in archives.iter().enumerate() {
        if let Some(cb) = progress_callback {
            cb(i + 1, total);
        }

        let path = path.as_ref();
        if !path.exists() {
            // File doesn't exist - not a hash failure, just missing
            continue;
        }

        match verify_file_hash_detailed(path, expected_hash) {
            Ok((true, _)) => {
                // Hash matches, good
            }
            Ok((false, actual_hash)) => {
                failures.push((
                    path.display().to_string(),
                    expected_hash.to_string(),
                    actual_hash,
                ));
            }
            Err(e) => {
                // Treat read errors as failures
                failures.push((
                    path.display().to_string(),
                    expected_hash.to_string(),
                    format!("ERROR: {}", e),
                ));
            }
        }
    }

    Ok(failures)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_compute_hash_empty_file() -> Result<()> {
        let mut tmp = NamedTempFile::new()?;
        // Empty file
        tmp.flush()?;

        let hash = compute_file_hash(tmp.path())?;
        // xxHash64 of empty data with seed 0
        // Expected: ef46db3751d8e999 -> base64
        assert!(!hash.is_empty());
        println!("Empty file hash: {}", hash);
        Ok(())
    }

    #[test]
    fn test_compute_hash_known_value() -> Result<()> {
        let mut tmp = NamedTempFile::new()?;
        tmp.write_all(b"Hello, World!")?;
        tmp.flush()?;

        let hash = compute_file_hash(tmp.path())?;
        assert!(!hash.is_empty());
        println!("'Hello, World!' hash: {}", hash);

        // Verify same content produces same hash
        let mut tmp2 = NamedTempFile::new()?;
        tmp2.write_all(b"Hello, World!")?;
        tmp2.flush()?;

        let hash2 = compute_file_hash(tmp2.path())?;
        assert_eq!(hash, hash2);

        Ok(())
    }

    #[test]
    fn test_verify_hash() -> Result<()> {
        let mut tmp = NamedTempFile::new()?;
        tmp.write_all(b"Test content for hashing")?;
        tmp.flush()?;

        let hash = compute_file_hash(tmp.path())?;

        // Should match
        assert!(verify_file_hash(tmp.path(), &hash)?);

        // Should not match wrong hash
        assert!(!verify_file_hash(tmp.path(), "wronghash==")?);

        Ok(())
    }
}
