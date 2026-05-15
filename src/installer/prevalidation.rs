//! Pre-validation pass: classify all directives as valid/needs-work upfront.
//!
//! Runs once before downloading or extracting to produce:
//! - A skip set of directive IDs that are already valid
//! - Per-type statistics for accurate progress bars
//! - Set of archive hashes actually needed
//! - List of extra files in the output directory

use crate::installer::pipeline::extract_bsa_temp_id;
use crate::installer::sidecar;
use crate::modlist::ModlistDb;
use crate::paths;

use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use crate::installer::progress::ProgressReporter;

/// Per-type statistics from pre-validation.
#[derive(Debug, Clone, Default)]
pub struct TypeStats {
    pub total: usize,
    pub already_valid: usize,
    pub needs_work: usize,
}

/// Result of the pre-validation pass.
pub struct PreValidationResult {
    /// Directive IDs that are already valid (skip in all phases)
    pub skip_set: HashSet<i64>,
    /// Per-type breakdown
    pub type_stats: HashMap<String, TypeStats>,
    /// Archive hashes that have at least one directive needing work
    pub needed_archive_hashes: HashSet<String>,
    /// Normalized paths of extra files in output dir (for cleanup)
    pub extra_files: Vec<String>,
}

impl PreValidationResult {
    /// Convert type_stats to the (total, needs_work) tuple map for ProcessContext.
    pub fn stats_as_tuples(&self) -> HashMap<String, (usize, usize)> {
        self.type_stats
            .iter()
            .map(|(k, v)| (k.clone(), (v.total, v.needs_work)))
            .collect()
    }

    /// Log a summary of the pre-validation results.
    pub fn log_summary(&self, reporter: &Arc<dyn ProgressReporter>) {
        let total_valid: usize = self.type_stats.values().map(|s| s.already_valid).sum();
        let total_work: usize = self.type_stats.values().map(|s| s.needs_work).sum();

        reporter.log(&format!(
            "Pre-validation: {} already valid, {} need work",
            total_valid, total_work,
        ));

        // Sort by type name for consistent output
        let mut types: Vec<_> = self.type_stats.iter().collect();
        types.sort_by_key(|(name, _)| (*name).clone());

        for (name, stats) in &types {
            if stats.total > 0 {
                reporter.log(&format!(
                    "  {:<25} {:>7} valid / {:>7} need work",
                    name, stats.already_valid, stats.needs_work,
                ));
            }
        }

        let total_archives: usize = self.type_stats.values().map(|s| s.total).sum::<usize>();
        if total_archives > 0 {
            reporter.log(&format!(
                "Archives needed: {} (of directives needing work)",
                self.needed_archive_hashes.len(),
            ));
        }

        if !self.extra_files.is_empty() {
            reporter.log(&format!("Extra files to clean: {}", self.extra_files.len()));
        }
    }
}

/// Run the pre-validation pass over all directives.
///
/// Classifies each directive as valid (skip) or needs-work based on:
/// - FromArchive/Patched/Inline/Remapped: file exists with correct size
/// - TransformedTexture: sidecar hash check
/// - CreateBSA: sidecar hash check
/// - BSA staging paths: valid if parent BSA is valid
pub fn run_prevalidation(
    db: &ModlistDb,
    existing_files: &HashMap<String, u64>,
    output_dir: &Path,
    reporter: &Arc<dyn ProgressReporter>,
) -> Result<PreValidationResult> {
    reporter.status("Pre-validating installed files...");

    let directives = db.get_all_directives_summary()?;

    if directives.is_empty() {
        return Ok(PreValidationResult {
            skip_set: HashSet::new(),
            type_stats: HashMap::new(),
            needed_archive_hashes: HashSet::new(),
            extra_files: Vec::new(),
        });
    }

    let mut skip_set = HashSet::new();
    let mut type_stats: HashMap<String, TypeStats> = HashMap::new();
    let mut needed_archive_hashes = HashSet::new();

    // Pass 1: Check CreateBSA directives first to build valid_bsa_temp_ids
    let mut valid_bsa_temp_ids: HashSet<String> = HashSet::new();

    for d in &directives {
        if d.directive_type == "CreateBSA" {
            let stats = type_stats.entry(d.directive_type.clone()).or_default();
            stats.total += 1;

            let output_path = paths::join_windows_path(output_dir, &d.to_path);
            if sidecar::sidecar_valid(&output_path, &d.hash) {
                stats.already_valid += 1;
                skip_set.insert(d.id);

                // Extract temp_id from the to_path to mark all staging files as valid too
                // CreateBSA to_path is the final BSA path, but we need the temp_id.
                // We can't get it from to_path — we need data_json for that.
                // Instead, we'll mark BSA staging files by checking valid_bsa_temp_ids
                // from the DB query in pass 2.
            } else {
                stats.needs_work += 1;
            }
        }
    }

    // We need temp_ids for valid BSAs. Query CreateBSA directives that are in skip_set.
    // The temp_id is in data_json which we're trying to avoid parsing.
    // Alternative: query staging directives grouped by BSA and check if any BSA ID in skip_set.
    // Simpler: just parse CreateBSA directives (there are few — typically <400).
    let bsa_directives = db
        .get_all_pending_directives_of_type("CreateBSA")
        .unwrap_or_default();

    for (_id, json) in &bsa_directives {
        if let Ok(crate::modlist::Directive::CreateBSA(d)) =
            serde_json::from_str::<crate::modlist::Directive>(json)
        {
            let output_path = paths::join_windows_path(output_dir, &d.to);
            if sidecar::sidecar_valid(&output_path, &d.hash) {
                valid_bsa_temp_ids.insert(d.temp_id.to_string());
            }
        }
    }

    // Pass 2: Check all other directives
    for d in &directives {
        if d.directive_type == "CreateBSA" {
            continue; // Already handled in pass 1
        }

        let stats = type_stats.entry(d.directive_type.clone()).or_default();
        stats.total += 1;

        let is_valid = check_directive_valid(d, existing_files, output_dir, &valid_bsa_temp_ids);

        if is_valid {
            stats.already_valid += 1;
            skip_set.insert(d.id);
        } else {
            stats.needs_work += 1;
            if let Some(ref archive_hash) = d.archive_hash {
                if !archive_hash.is_empty() {
                    needed_archive_hashes.insert(archive_hash.clone());
                }
            }
        }
    }

    // Compute extra files: files in output dir not in any directive
    let expected_paths: HashSet<String> = directives
        .iter()
        .filter(|d| !d.to_path.is_empty())
        .map(|d| paths::normalize_for_lookup(&d.to_path))
        .collect();

    let downloads_prefix = {
        let downloads_dir = &output_dir.join("downloads"); // common location
        downloads_dir
            .strip_prefix(output_dir)
            .ok()
            .map(|p| paths::normalize_for_lookup(&p.to_string_lossy()))
    };

    let extra_files: Vec<String> = existing_files
        .keys()
        .filter(|path| {
            // Skip sidecar/manifest files
            if path.ends_with(".clf3hash") || path.ends_with(".clf3manifest") {
                return false;
            }
            // Skip downloads directory
            if let Some(ref prefix) = downloads_prefix {
                if path.starts_with(prefix.as_str()) {
                    return false;
                }
            }
            // Skip TEMP_BSA_FILES staging
            let lower = path.to_lowercase();
            if lower.starts_with("temp_bsa_files") {
                return false;
            }
            !expected_paths.contains(path.as_str())
        })
        .cloned()
        .collect();

    Ok(PreValidationResult {
        skip_set,
        type_stats,
        needed_archive_hashes,
        extra_files,
    })
}

/// Check if a single directive's output is already valid.
fn check_directive_valid(
    d: &crate::modlist::DirectiveSummary,
    existing_files: &HashMap<String, u64>,
    output_dir: &Path,
    valid_bsa_temp_ids: &HashSet<String>,
) -> bool {
    // BSA staging paths: valid if parent BSA is fully built
    let normalized_to = d.to_path.replace('\\', "/");
    if normalized_to.starts_with("TEMP_BSA_FILES") || normalized_to.starts_with("TEMP_BSA_FILES") {
        if let Some(temp_id) = extract_bsa_temp_id(&d.to_path) {
            return valid_bsa_temp_ids.contains(&temp_id.to_string());
        }
    }

    match d.directive_type.as_str() {
        // Size-only check for simple directives
        "FromArchive" | "PatchedFromArchive" | "InlineFile" | "RemappedInlineFile" => {
            let normalized = paths::normalize_for_lookup(&d.to_path);
            existing_files
                .get(&normalized)
                .map(|&existing_size| existing_size == d.size)
                .unwrap_or(false)
        }
        // Sidecar hash check for DDS textures
        "TransformedTexture" => {
            let output_path = paths::join_windows_path(output_dir, &d.to_path);
            sidecar::sidecar_valid(&output_path, &d.hash)
        }
        // CreateBSA handled in pass 1
        "CreateBSA" => false,
        // Unknown directive types: always process
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_stats_default() {
        let stats = TypeStats::default();
        assert_eq!(stats.total, 0);
        assert_eq!(stats.already_valid, 0);
        assert_eq!(stats.needs_work, 0);
    }

    #[test]
    fn test_stats_as_tuples() {
        let mut result = PreValidationResult {
            skip_set: HashSet::new(),
            type_stats: HashMap::new(),
            needed_archive_hashes: HashSet::new(),
            extra_files: Vec::new(),
        };
        result.type_stats.insert(
            "FromArchive".to_string(),
            TypeStats {
                total: 100,
                already_valid: 80,
                needs_work: 20,
            },
        );
        let tuples = result.stats_as_tuples();
        assert_eq!(tuples.get("FromArchive"), Some(&(100, 20)));
    }
}
