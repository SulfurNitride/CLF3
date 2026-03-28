//! Sidecar hash cache for BSA/DDS skip-on-update
//!
//! After successfully creating a BSA or transforming a DDS texture, a `.clf3hash`
//! sidecar file is written next to the output containing the directive's expected
//! hash and the output file's actual size. On subsequent runs, if the sidecar
//! matches the directive and the file size is correct, the work is skipped.
//!
//! BSA archives also get a `.clf3manifest` companion that stores per-file hashes.
//! On update, this allows partial reuse: unchanged files are extracted from the
//! existing BSA instead of re-downloading their source archives.

use std::collections::HashMap;
use std::fs;
use std::io::BufRead;
use std::path::Path;

const SIDECAR_EXT: &str = "clf3hash";
const MANIFEST_EXT: &str = "clf3manifest";

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

// ---------------------------------------------------------------------------
// Archive download verification sidecar
// ---------------------------------------------------------------------------

/// Check if a downloaded archive's hash sidecar is still valid.
///
/// The sidecar records the verified xxHash64 + file size + mtime.
/// On re-runs, we just stat() the file — if size and mtime match,
/// the hash is trusted without re-reading the entire archive.
pub fn archive_hash_valid(archive_path: &Path, expected_hash: &str) -> bool {
    let sp = sidecar_path(archive_path);

    let content = match fs::read_to_string(&sp) {
        Ok(c) => c,
        Err(_) => return false,
    };

    let mut stored_hash: Option<&str> = None;
    let mut stored_size: Option<u64> = None;
    let mut stored_mtime: Option<u64> = None;
    for line in content.lines() {
        if let Some(h) = line.strip_prefix("hash=") {
            stored_hash = Some(h.trim());
        } else if let Some(s) = line.strip_prefix("size=") {
            stored_size = s.trim().parse().ok();
        } else if let Some(m) = line.strip_prefix("mtime=") {
            stored_mtime = m.trim().parse().ok();
        }
    }

    let (Some(stored_hash), Some(stored_size), Some(stored_mtime)) =
        (stored_hash, stored_size, stored_mtime)
    else {
        return false;
    };

    if stored_hash != expected_hash {
        return false;
    }

    // Check actual file size and mtime match what we recorded
    let meta = match fs::metadata(archive_path) {
        Ok(m) => m,
        Err(_) => return false,
    };

    if meta.len() != stored_size {
        return false;
    }

    // Compare mtime (seconds since epoch)
    if let Ok(mtime) = meta.modified() {
        if let Ok(dur) = mtime.duration_since(std::time::UNIX_EPOCH) {
            return dur.as_secs() == stored_mtime;
        }
    }

    false
}

/// Write an archive hash sidecar after successful verification.
pub fn write_archive_hash(archive_path: &Path, hash: &str) -> std::io::Result<()> {
    let meta = fs::metadata(archive_path)?;
    let mtime_secs = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0);

    let sp = sidecar_path(archive_path);
    let content = format!("hash={}\nsize={}\nmtime={}\n", hash, meta.len(), mtime_secs);
    fs::write(&sp, content)
}

// ---------------------------------------------------------------------------
// BSA per-file manifest
// ---------------------------------------------------------------------------

/// Build the manifest path for a given BSA output file.
fn manifest_path(output_path: &Path) -> std::path::PathBuf {
    let mut p = output_path.as_os_str().to_owned();
    p.push(".");
    p.push(MANIFEST_EXT);
    std::path::PathBuf::from(p)
}

/// Normalize a BSA-internal path for manifest storage.
///
/// Converts backslashes to forward slashes and lowercases everything so that
/// comparisons are consistent regardless of BSA vs BA2 conventions.
pub fn normalize_manifest_path(path: &str) -> String {
    path.replace('\\', "/").to_lowercase()
}

/// Write a per-file manifest after BSA creation.
///
/// Each entry is `(normalized_bsa_path, xxhash64_base64)`.
/// Format: one `path=hash` pair per line, sorted for determinism.
pub fn write_manifest(
    output_path: &Path,
    entries: &[(String, String)],
) -> std::io::Result<()> {
    let mp = manifest_path(output_path);

    let mut sorted: Vec<_> = entries.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));

    let mut content = String::with_capacity(sorted.len() * 60);
    for (path, hash) in &sorted {
        content.push_str(path);
        content.push('=');
        content.push_str(hash);
        content.push('\n');
    }

    tracing::info!(
        "Writing BSA manifest: {} ({} files)",
        mp.display(),
        sorted.len()
    );
    fs::write(&mp, content)
}

/// Read an existing per-file manifest.
///
/// Returns `None` if the manifest doesn't exist or can't be parsed.
/// Keys are normalized paths (lowercase, forward slashes).
pub fn read_manifest(output_path: &Path) -> Option<HashMap<String, String>> {
    let mp = manifest_path(output_path);
    let file = fs::File::open(&mp).ok()?;
    let reader = std::io::BufReader::new(file);

    let mut map = HashMap::new();
    for line in reader.lines() {
        let line = line.ok()?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some((path, hash)) = line.split_once('=') {
            map.insert(path.to_string(), hash.to_string());
        }
    }

    if map.is_empty() {
        None
    } else {
        Some(map)
    }
}

/// Delete a stale manifest.
pub fn remove_manifest(output_path: &Path) {
    let _ = fs::remove_file(manifest_path(output_path));
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

    #[test]
    fn test_manifest_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("test.bsa");

        // No manifest yet
        assert!(read_manifest(&output).is_none());

        // Write manifest
        let entries = vec![
            ("textures/armor/iron.dds".to_string(), "hash1".to_string()),
            ("meshes/armor/iron.nif".to_string(), "hash2".to_string()),
        ];
        write_manifest(&output, &entries).unwrap();

        // Read it back
        let manifest = read_manifest(&output).unwrap();
        assert_eq!(manifest.len(), 2);
        assert_eq!(manifest.get("textures/armor/iron.dds").unwrap(), "hash1");
        assert_eq!(manifest.get("meshes/armor/iron.nif").unwrap(), "hash2");

        // Remove manifest
        remove_manifest(&output);
        assert!(read_manifest(&output).is_none());
    }

    #[test]
    fn test_normalize_manifest_path() {
        assert_eq!(
            normalize_manifest_path("Textures\\Armor\\Iron.dds"),
            "textures/armor/iron.dds"
        );
        assert_eq!(
            normalize_manifest_path("textures/armor/iron.dds"),
            "textures/armor/iron.dds"
        );
    }
}
