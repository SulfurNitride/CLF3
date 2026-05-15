//! BSA partial reuse: extract unchanged files from existing BSAs on update.
//!
//! When a modlist update changes some files inside a BSA, this module identifies
//! which files are unchanged (via per-file manifest comparison) and pre-extracts
//! them from the existing BSA on disk into the staging directory. This avoids
//! re-downloading source archives for files that haven't changed.

use crate::bsa;
use crate::installer::processor::ProcessContext;
use crate::installer::sidecar;
use crate::modlist::{Directive, ModlistDb};
use crate::paths;

use anyhow::{Context, Result};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::installer::progress::ProgressReporter;

/// Statistics from BSA pre-extraction.
#[derive(Debug, Default)]
pub struct BsaReuseStats {
    /// Number of BSAs that had reusable files
    pub bsas_with_reuse: usize,
    /// Total files pre-extracted from existing BSAs
    pub files_reused: usize,
    /// Total files that need downloading (changed or new)
    pub files_changed: usize,
}

/// Reuse plan for a single BSA.
struct BsaReusePlan {
    /// BSA output path on disk (existing archive to extract from)
    bsa_path: std::path::PathBuf,
    /// Staging directory for this BSA's temp_id
    staging_dir: std::path::PathBuf,
    /// Files to extract from existing BSA: (bsa_internal_path, staging_dest, expected_size)
    reusable: Vec<(String, std::path::PathBuf, u64)>,
    /// Number of files that changed or are new
    changed_count: usize,
}

/// Pre-extract reusable files from existing BSAs into staging directories.
///
/// For each CreateBSA directive where:
/// - The BSA's overall sidecar is INVALID (hash changed — needs rebuild)
/// - A per-file manifest EXISTS from the previous build
/// - The BSA file still exists on disk
///
/// We diff the manifest against the new directive hashes, extract unchanged
/// files from the existing BSA, and add them to `ctx.existing_files` so the
/// downloader and pipeline skip those directives.
pub fn pre_extract_reusable_bsa_files(
    db: &ModlistDb,
    ctx: &mut ProcessContext,
    reporter: &Arc<dyn ProgressReporter>,
) -> Result<BsaReuseStats> {
    let mut stats = BsaReuseStats::default();

    // Load all CreateBSA directives
    let bsa_directives = db
        .get_all_pending_directives_of_type("CreateBSA")
        .unwrap_or_default();

    if bsa_directives.is_empty() {
        return Ok(stats);
    }

    let mut plans: Vec<BsaReusePlan> = Vec::new();

    for (_id, json) in &bsa_directives {
        let directive = match serde_json::from_str::<Directive>(json) {
            Ok(Directive::CreateBSA(d)) => d,
            _ => continue,
        };

        let output_path = ctx.resolve_output_path(&directive.to);

        // Skip if BSA is fully valid (sidecar matches) — nothing to do
        if sidecar::sidecar_valid(&output_path, &directive.hash) {
            continue;
        }

        // Skip if no manifest exists (first install or pre-feature BSA)
        let old_manifest = match sidecar::read_manifest(&output_path) {
            Some(m) => m,
            None => continue,
        };

        // Skip if the existing BSA file doesn't exist on disk
        if !output_path.exists() {
            continue;
        }

        // Query all directives targeting this BSA's staging path
        let temp_id_str = directive.temp_id.to_string();
        let staging_directives = db
            .get_directives_for_bsa_staging(&temp_id_str)
            .unwrap_or_default();

        if staging_directives.is_empty() {
            continue;
        }

        let staging_dir = ctx
            .config
            .output_dir
            .join("TEMP_BSA_FILES")
            .join(&temp_id_str);

        // Diff: compare each new directive's hash against the old manifest
        let mut reusable = Vec::new();
        let mut changed_count = 0usize;

        for (to_path, new_hash, size) in &staging_directives {
            // Extract BSA-internal path by stripping TEMP_BSA_FILES/{uuid}/ prefix
            let normalized_to = to_path.replace('\\', "/");
            let bsa_internal = match strip_staging_prefix(&normalized_to) {
                Some(p) => p,
                None => {
                    changed_count += 1;
                    continue;
                }
            };

            let normalized_internal = sidecar::normalize_manifest_path(&bsa_internal);

            // Check if old manifest has this file with the same hash
            if let Some(old_hash) = old_manifest.get(&normalized_internal) {
                if old_hash == new_hash {
                    // File unchanged — can extract from existing BSA
                    let staging_path =
                        staging_dir.join(bsa_internal.replace('/', std::path::MAIN_SEPARATOR_STR));
                    reusable.push((normalized_internal, staging_path, *size));
                    continue;
                }
            }

            // File changed, new, or removed from manifest
            changed_count += 1;
        }

        if !reusable.is_empty() {
            plans.push(BsaReusePlan {
                bsa_path: output_path,
                staging_dir,
                reusable,
                changed_count,
            });
        }
    }

    if plans.is_empty() {
        return Ok(stats);
    }

    // Execute pre-extraction for each plan
    for plan in &plans {
        match execute_pre_extraction(plan, ctx, reporter) {
            Ok(extracted) => {
                if extracted > 0 {
                    stats.bsas_with_reuse += 1;
                    stats.files_reused += extracted;
                    stats.files_changed += plan.changed_count;
                }
            }
            Err(e) => {
                // Fall back to full rebuild — just log and continue
                tracing::warn!(
                    "BSA pre-extraction failed for {}, falling back to full rebuild: {}",
                    plan.bsa_path.display(),
                    e
                );
                // Clean up any partially-extracted staging files
                let _ = std::fs::remove_dir_all(&plan.staging_dir);
            }
        }
    }

    Ok(stats)
}

/// Execute pre-extraction for a single BSA reuse plan.
///
/// Extracts reusable files from the existing BSA into the staging directory
/// and registers them in `ctx.existing_files`.
fn execute_pre_extraction(
    plan: &BsaReusePlan,
    ctx: &mut ProcessContext,
    reporter: &Arc<dyn ProgressReporter>,
) -> Result<usize> {
    let bsa_name = plan
        .bsa_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown");

    reporter.log(&format!(
        "BSA reuse: extracting {} unchanged files from {} ({} changed)",
        plan.reusable.len(),
        bsa_name,
        plan.changed_count,
    ));

    // Ensure staging directory exists
    std::fs::create_dir_all(&plan.staging_dir).with_context(|| {
        format!(
            "Failed to create staging dir: {}",
            plan.staging_dir.display()
        )
    })?;

    // Build the set of wanted paths for batch extraction.
    // extract_archive_batch expects lowercase forward-slash paths.
    let wanted: HashSet<String> = plan
        .reusable
        .iter()
        .map(|(path, _, _)| path.clone())
        .collect();

    // Build lookup: normalized_bsa_path -> (staging_path, expected_size)
    let lookup: HashMap<String, (&std::path::Path, u64)> = plan
        .reusable
        .iter()
        .map(|(path, staging, size)| (path.clone(), (staging.as_path(), *size)))
        .collect();

    let extracted = std::sync::atomic::AtomicUsize::new(0);
    let failed = std::sync::atomic::AtomicBool::new(false);

    bsa::extract_archive_batch(&plan.bsa_path, &wanted, |path, data| {
        let normalized = sidecar::normalize_manifest_path(path);
        if let Some(&(staging_path, expected_size)) = lookup.get(&normalized) {
            // Verify size matches
            if data.len() as u64 != expected_size {
                tracing::warn!(
                    "BSA reuse: size mismatch for {} (expected {}, got {}), aborting this BSA",
                    path,
                    expected_size,
                    data.len()
                );
                failed.store(true, std::sync::atomic::Ordering::Relaxed);
                return Ok(());
            }

            // Write to staging
            ctx.dir_cache.ensure_parent_dirs(staging_path)?;
            std::fs::write(staging_path, &data).with_context(|| {
                format!(
                    "Failed to write pre-extracted file: {}",
                    staging_path.display()
                )
            })?;

            extracted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(())
    })?;

    if failed.load(std::sync::atomic::Ordering::Relaxed) {
        // Size mismatch detected — BSA might be corrupted, abort reuse
        let _ = std::fs::remove_dir_all(&plan.staging_dir);
        anyhow::bail!(
            "Size mismatch during pre-extraction from {}",
            plan.bsa_path.display()
        );
    }

    let count = extracted.load(std::sync::atomic::Ordering::Relaxed);

    // Register pre-extracted files in existing_files so the pipeline skips them
    for (_, staging_path, size) in &plan.reusable {
        if staging_path.exists() {
            if let Ok(rel_path) = staging_path.strip_prefix(&ctx.config.output_dir) {
                let normalized = paths::normalize_for_lookup(&rel_path.to_string_lossy());
                ctx.existing_files.insert(normalized, *size);
            }
        }
    }

    Ok(count)
}

/// Strip the `TEMP_BSA_FILES/{uuid}/` prefix from a staging path.
///
/// Input:  `TEMP_BSA_FILES/550e8400-.../textures/armor/iron.dds`
/// Output: `textures/armor/iron.dds`
fn strip_staging_prefix(path: &str) -> Option<String> {
    let parts: Vec<&str> = path.splitn(3, '/').collect();
    if parts.len() >= 3 && parts[0] == "TEMP_BSA_FILES" {
        Some(parts[2].to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strip_staging_prefix() {
        assert_eq!(
            strip_staging_prefix("TEMP_BSA_FILES/abc-123/textures/armor/iron.dds"),
            Some("textures/armor/iron.dds".to_string())
        );
        assert_eq!(
            strip_staging_prefix("TEMP_BSA_FILES/abc-123/meshes/nif.nif"),
            Some("meshes/nif.nif".to_string())
        );
        assert_eq!(strip_staging_prefix("mods/some_mod/file.esp"), None);
        assert_eq!(strip_staging_prefix("TEMP_BSA_FILES/abc"), None);
    }
}
