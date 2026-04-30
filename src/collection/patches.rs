//! Vortex collection bsdiff patches.
//!
//! Vortex stores per-file binary diffs in collections under
//! `<collection_root>/patches/<mod_name>/<rel_path>.diff`. The collection JSON
//! `mods[].patches` map records `<rel_path>` → `<crc32_hex>` of the *original*
//! file before patching. After a mod's archive is extracted into its mod
//! folder, walk the map and for each entry: verify the source file's CRC32
//! matches, apply the bsdiff 4.x patch in place.
//!
//! Format note: `bsdiff-node` (used by Vortex) writes Colin Percival's bsdiff
//! 4.x format (BSDIFF40 magic + bzip2-compressed control/diff/extra blocks).
//! This is what `qbsdiff::Bspatch` consumes.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use qbsdiff::Bspatch;
use tracing::{debug, warn};

/// Result of applying patches for one mod.
#[derive(Debug, Default, Clone)]
pub struct PatchSummary {
    pub applied: usize,
    pub skipped_crc: usize,
    pub missing_diff: usize,
    pub failed: usize,
}

/// Apply all patches for `mod_name` against files in `mod_dir`.
///
/// For each `(rel_path, expected_crc)` in `patches`:
/// 1. Read `mod_dir/rel_path` (skip if missing — log + count as failed).
/// 2. Compute CRC32; skip if it doesn't match `expected_crc` (mod was already
///    patched on a previous run, or extracted file differs from what Vortex
///    saw).
/// 3. Read diff blob from `collection_root/patches/mod_name/rel_path.diff`.
/// 4. Apply via `qbsdiff::Bspatch` and replace the source file atomically.
pub fn apply_patches_for_mod(
    mod_name: &str,
    mod_dir: &Path,
    collection_root: &Path,
    patches: &HashMap<String, String>,
) -> Result<PatchSummary> {
    if patches.is_empty() {
        return Ok(PatchSummary::default());
    }

    let patches_root = collection_root.join("patches").join(mod_name);
    let mut summary = PatchSummary::default();

    for (rel_path, expected_crc_hex) in patches {
        match apply_one(mod_dir, &patches_root, rel_path, expected_crc_hex) {
            Ok(PatchOutcome::Applied) => summary.applied += 1,
            Ok(PatchOutcome::CrcMismatch { actual }) => {
                debug!(
                    "skip patch '{}/{}' — source CRC {} != expected {}",
                    mod_name, rel_path, actual, expected_crc_hex
                );
                summary.skipped_crc += 1;
            }
            Ok(PatchOutcome::DiffMissing) => {
                warn!(
                    "patch diff missing: {}",
                    patches_root.join(format!("{rel_path}.diff")).display()
                );
                summary.missing_diff += 1;
            }
            Err(e) => {
                warn!("patch failed for '{}/{}': {:#}", mod_name, rel_path, e);
                summary.failed += 1;
            }
        }
    }

    Ok(summary)
}

enum PatchOutcome {
    Applied,
    CrcMismatch { actual: String },
    DiffMissing,
}

fn apply_one(
    mod_dir: &Path,
    patches_root: &Path,
    rel_path: &str,
    expected_crc_hex: &str,
) -> Result<PatchOutcome> {
    let normalized = rel_path.replace('\\', "/");
    let src_path = join_rel(mod_dir, &normalized);
    let diff_path = patches_root.join(format!("{normalized}.diff"));

    if !diff_path.exists() {
        return Ok(PatchOutcome::DiffMissing);
    }

    let src_bytes = std::fs::read(&src_path)
        .with_context(|| format!("read source: {}", src_path.display()))?;

    let actual_crc = crc32_hex(&src_bytes);
    if !actual_crc.eq_ignore_ascii_case(expected_crc_hex) {
        return Ok(PatchOutcome::CrcMismatch { actual: actual_crc });
    }

    let diff_bytes = std::fs::read(&diff_path)
        .with_context(|| format!("read diff: {}", diff_path.display()))?;

    let patcher = Bspatch::new(&diff_bytes)
        .with_context(|| format!("parse diff header: {}", diff_path.display()))?;
    let mut target = Vec::with_capacity(patcher.hint_target_size() as usize);
    patcher
        .apply(&src_bytes, std::io::Cursor::new(&mut target))
        .with_context(|| format!("apply diff: {}", diff_path.display()))?;

    write_atomic(&src_path, &target)
        .with_context(|| format!("write patched: {}", src_path.display()))?;

    Ok(PatchOutcome::Applied)
}

fn join_rel(base: &Path, rel: &str) -> PathBuf {
    let mut out = base.to_path_buf();
    for component in rel.split('/').filter(|c| !c.is_empty()) {
        out.push(component);
    }
    out
}

fn crc32_hex(data: &[u8]) -> String {
    let mut h = crc32fast::Hasher::new();
    h.update(data);
    format!("{:08X}", h.finalize())
}

fn write_atomic(target: &Path, bytes: &[u8]) -> Result<()> {
    let parent = target
        .parent()
        .ok_or_else(|| anyhow::anyhow!("no parent dir for {}", target.display()))?;
    let mut tmp = tempfile::NamedTempFile::new_in(parent)?;
    std::io::Write::write_all(&mut tmp, bytes)?;
    tmp.persist(target)
        .map_err(|e| anyhow::anyhow!("persist patched file: {}", e.error))?;
    Ok(())
}

/// Aggregate two summaries (used when looping across mods).
pub fn merge(a: PatchSummary, b: PatchSummary) -> PatchSummary {
    PatchSummary {
        applied: a.applied + b.applied,
        skipped_crc: a.skipped_crc + b.skipped_crc,
        missing_diff: a.missing_diff + b.missing_diff,
        failed: a.failed + b.failed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32_hex_matches_npm_crc32_buf() {
        // npm crc-32 produces 0xCBF43926 for "123456789" (CRC32 IEEE).
        assert_eq!(crc32_hex(b"123456789"), "CBF43926");
    }

    #[test]
    fn empty_patches_no_op() {
        let tmp = tempfile::tempdir().unwrap();
        let summary = apply_patches_for_mod(
            "mod",
            tmp.path(),
            tmp.path(),
            &HashMap::new(),
        )
        .unwrap();
        assert_eq!(summary.applied, 0);
    }

    #[test]
    fn round_trip_bsdiff_4x() {
        use qbsdiff::Bsdiff;
        use std::io::Cursor;

        let source = b"the quick brown fox jumps over the lazy dog".repeat(64);
        let target = b"THE QUICK brown fox jumps OVER the LAZY dog!".repeat(64);

        let mut diff = Vec::new();
        Bsdiff::new(&source, &target)
            .compare(Cursor::new(&mut diff))
            .unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let mod_dir = tmp.path().join("mods").join("MyMod");
        let collection_root = tmp.path().join("col");
        let patches_dir = collection_root.join("patches").join("MyMod");
        std::fs::create_dir_all(mod_dir.join("data")).unwrap();
        std::fs::create_dir_all(patches_dir.join("data")).unwrap();

        let src_path = mod_dir.join("data/file.bin");
        std::fs::write(&src_path, &source).unwrap();
        std::fs::write(patches_dir.join("data/file.bin.diff"), &diff).unwrap();

        let mut patches = HashMap::new();
        patches.insert("data/file.bin".to_string(), crc32_hex(&source));

        let summary =
            apply_patches_for_mod("MyMod", &mod_dir, &collection_root, &patches).unwrap();
        assert_eq!(summary.applied, 1);
        assert_eq!(summary.failed, 0);
        assert_eq!(std::fs::read(&src_path).unwrap(), target);
    }

    #[test]
    fn skips_when_crc_mismatches() {
        let tmp = tempfile::tempdir().unwrap();
        let mod_dir = tmp.path().join("mod");
        let patches_dir = tmp.path().join("col").join("patches").join("M");
        std::fs::create_dir_all(&mod_dir).unwrap();
        std::fs::create_dir_all(&patches_dir).unwrap();
        std::fs::write(mod_dir.join("a.txt"), b"hello").unwrap();
        std::fs::write(patches_dir.join("a.txt.diff"), b"unused").unwrap();

        let mut patches = HashMap::new();
        patches.insert("a.txt".to_string(), "DEADBEEF".to_string());

        let summary =
            apply_patches_for_mod("M", &mod_dir, &tmp.path().join("col"), &patches).unwrap();
        assert_eq!(summary.applied, 0);
        assert_eq!(summary.skipped_crc, 1);
        assert_eq!(std::fs::read(mod_dir.join("a.txt")).unwrap(), b"hello");
    }
}

