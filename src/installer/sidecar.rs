//! Sidecar hash cache for BSA/DDS skip-on-update
//!
//! After successfully creating a BSA or transforming a DDS texture, a `.clf3hash`
//! sidecar file is written next to the output containing the directive's expected
//! hash and the output file's actual size. On subsequent runs, if the sidecar
//! matches the directive and the file size is correct, the work is skipped.

use std::fs;
use std::path::Path;

const SIDECAR_EXT: &str = "clf3hash";

/// Build the sidecar path for a given output file.
fn sidecar_path(output_path: &Path) -> std::path::PathBuf {
    let mut p = output_path.as_os_str().to_owned();
    p.push(".");
    p.push(SIDECAR_EXT);
    std::path::PathBuf::from(p)
}

/// Check if a sidecar-cached output is still valid for the given directive hash.
///
/// Returns `true` only if:
/// 1. The output file exists
/// 2. The sidecar file exists and is parseable
/// 3. The sidecar's `hash` matches `expected_hash`
/// 4. The output file's actual size matches the sidecar's `size`
pub fn sidecar_valid(output_path: &Path, expected_hash: &str) -> bool {
    let sp = sidecar_path(output_path);

    // Read sidecar
    let content = match fs::read_to_string(&sp) {
        Ok(c) => c,
        Err(_) => return false,
    };

    // Parse hash and size
    let mut stored_hash = None;
    let mut stored_size = None;
    for line in content.lines() {
        if let Some(h) = line.strip_prefix("hash=") {
            stored_hash = Some(h.trim());
        } else if let Some(s) = line.strip_prefix("size=") {
            stored_size = s.trim().parse::<u64>().ok();
        }
    }

    let (Some(stored_hash), Some(stored_size)) = (stored_hash, stored_size) else {
        return false;
    };

    // Compare directive hash
    if stored_hash != expected_hash {
        return false;
    }

    // Compare actual file size
    match fs::metadata(output_path) {
        Ok(meta) => meta.len() == stored_size,
        Err(_) => false,
    }
}

/// Write a sidecar after successful output creation.
///
/// Records the directive's expected hash and the output file's actual size.
pub fn write_sidecar(output_path: &Path, directive_hash: &str) -> std::io::Result<()> {
    let actual_size = fs::metadata(output_path)?.len();
    let sp = sidecar_path(output_path);
    let content = format!("hash={}\nsize={}\n", directive_hash, actual_size);
    tracing::info!("Writing sidecar: {} (hash={}, size={})", sp.display(), directive_hash, actual_size);
    fs::write(&sp, content)
}

/// Delete a stale sidecar (called before regenerating an output).
pub fn remove_sidecar(output_path: &Path) {
    let _ = fs::remove_file(sidecar_path(output_path));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_sidecar_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("test.ba2");

        // Create a fake output file
        {
            let mut f = fs::File::create(&output).unwrap();
            f.write_all(b"BTDX fake bsa content here").unwrap();
        }

        // No sidecar yet
        assert!(!sidecar_valid(&output, "abc123"));

        // Write sidecar
        write_sidecar(&output, "abc123").unwrap();
        assert!(sidecar_valid(&output, "abc123"));

        // Wrong hash → invalid
        assert!(!sidecar_valid(&output, "different_hash"));

        // Corrupt the file (change size)
        {
            let mut f = fs::File::create(&output).unwrap();
            f.write_all(b"short").unwrap();
        }
        assert!(!sidecar_valid(&output, "abc123"));

        // Remove sidecar
        remove_sidecar(&output);
        assert!(!sidecar_valid(&output, "abc123"));
    }

    #[test]
    fn test_sidecar_missing_output() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("nonexistent.dds");
        assert!(!sidecar_valid(&output, "abc123"));
    }
}
