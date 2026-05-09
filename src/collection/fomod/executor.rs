//! FOMOD executor - applies choices and installs files.
//!
//! This module executes FOMOD installers by:
//! 1. Collecting flags from selected options
//! 2. Installing required files
//! 3. Installing files from selected options
//! 4. Evaluating conditional installs based on flags

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
// WalkDir removed - using fs::read_dir instead

use super::parser::{Dependencies, DependencyOperator, FomodConfig, InstallFile, Plugin, PluginType};
use crate::collection::FomodChoices;

/// Statistics from FOMOD execution.
#[derive(Debug, Default)]
pub struct FomodStats {
    /// Number of files installed.
    pub files_installed: usize,
    /// Number of folders installed.
    pub folders_installed: usize,
    /// Flags that were set during execution.
    pub flags_set: HashMap<String, String>,
}

/// Execute a FOMOD installer with pre-recorded choices.
///
/// # Arguments
/// * `data_root` - Root directory of the extracted mod (parent of fomod folder)
/// * `dest_dir` - Destination directory for installed files
/// * `config` - Parsed FOMOD configuration
/// * `choices` - Pre-recorded choices from collection JSON
///
/// # Returns
/// Statistics about the installation.
pub fn execute_fomod(
    data_root: &Path,
    dest_dir: &Path,
    config: &FomodConfig,
    choices: &FomodChoices,
) -> Result<FomodStats> {
    use tracing::{debug, info};

    let mut stats = FomodStats::default();

    info!("FOMOD execution: {} required files, {} steps, {} conditional",
        config.required_files.len(),
        config.install_steps.len(),
        config.conditional_installs.len());

    // Collect all files that should install, in the order they're decided
    // (required → step plugins → conditional patterns). The final install
    // pass sorts by FOMOD priority (ASC) — higher priority files therefore
    // overwrite lower-priority files at the same destination.
    let mut planned: Vec<&InstallFile> = Vec::new();

    for file in &config.required_files {
        debug!("Plan required: {} -> {}", file.source, file.destination);
        planned.push(file);
    }

    // Phase: process install steps, set flags from selected plugins.
    for step in &config.install_steps {
        debug!("Processing step: {}", step.name);

        for group in &step.groups {
            debug!("  Processing group: {}", group.name);

            // Match choice entry first by (step name + group name). If
            // that misses, fall back to group name only — Vortex sometimes
            // exports a step with `name: ""` (single-step FOMODs, lost
            // metadata, etc.) and group names are unique within an
            // installer in practice. Without this fallback, FSMP and other
            // mods with an unnamed step silently install with no choices,
            // leaving critical files (hdtSMP64.dll!) on the cutting room
            // floor.
            let group_choices = choices
                .options
                .iter()
                .filter(|s| s.name.eq_ignore_ascii_case(&step.name))
                .find_map(|s| s.groups.iter().find(|g| g.name.eq_ignore_ascii_case(&group.name)))
                .or_else(|| {
                    choices.options.iter().find_map(|s| {
                        s.groups
                            .iter()
                            .find(|g| g.name.eq_ignore_ascii_case(&group.name))
                    })
                });

            if group_choices.is_none() {
                debug!("    No matching choices for step '{}' + group '{}'", step.name, group.name);
            }

            for (plugin_idx, plugin) in group.plugins.iter().enumerate() {
                let effective = effective_plugin_type(plugin, &stats.flags_set, dest_dir);
                // Match by ASCII-case-insensitive name first; if that misses
                // fall back to idx. Vortex's `installerChoices` exports both
                // name and idx, but the name can drift from the FOMOD's XML
                // (curly apostrophe `’` vs ASCII `'`, trailing spaces,
                // unicode dashes) and break a pure name match. Idx is the
                // plugin's position within the group — robust across those
                // cosmetic differences.
                let is_selected_in_choices = group_choices.is_some_and(|g| {
                    g.choices.iter().any(|c| {
                        c.name.eq_ignore_ascii_case(&plugin.name)
                            || (c.idx as usize == plugin_idx && !c.name.is_empty())
                    })
                });
                // An explicit choice in `choices_json` is authoritative.
                // Vortex captured it during the curator's interactive FOMOD
                // run with their actual game version, so the typeDescriptor
                // (which gates *interactive* selection — NotUsable etc.) is
                // already satisfied and shouldn't second-guess the recorded
                // choice. Without this, FSMP's "1.6.1170" plugin gets
                // typed NotUsable (its dependencyType conditions on game
                // version, which we treat as always-satisfied) and we never
                // set its `161170=On` flag → conditional variant install
                // never fires → hdtSMP64.dll is missing.
                //
                // For plugins NOT in choices, fall back to plugin-type
                // semantics: Required = always; Recommended = default-on
                // when no choices recorded for the group; Optional /
                // CouldBeUsable / NotUsable = never auto-install.
                let install = if is_selected_in_choices {
                    true
                } else {
                    match effective {
                        PluginType::Required => true,
                        PluginType::Recommended => group_choices.is_none(),
                        PluginType::Optional
                        | PluginType::CouldBeUsable
                        | PluginType::NotUsable => false,
                    }
                };

                debug!(
                    "    Plugin '{}': type={:?} selected_in_choices={} install={} files={}",
                    plugin.name, effective, is_selected_in_choices, install, plugin.files.len()
                );

                if install {
                    for flag in &plugin.condition_flags {
                        debug!("      Setting flag: {}={}", flag.name, flag.value);
                        stats.flags_set.insert(flag.name.clone(), flag.value.clone());
                    }
                    for file in &plugin.files {
                        planned.push(file);
                    }
                }
            }
        }
    }

    // Phase: conditional installs, using flags collected above.
    debug!("Processing {} conditional patterns with flags: {:?}",
        config.conditional_installs.len(), stats.flags_set);

    for pattern in &config.conditional_installs {
        let matches =
            evaluate_dependencies(&pattern.dependencies, &stats.flags_set, dest_dir);
        debug!("  Conditional pattern: matches={}, files={}", matches, pattern.files.len());

        if matches {
            for file in &pattern.files {
                planned.push(file);
            }
        }
    }

    // Final pass: sort by priority ASC (stable so equal-priority entries
    // keep author-defined order) and copy.
    planned.sort_by_key(|f| f.priority);
    for file in planned {
        debug!(
            "Installing (priority {}): {} -> {}",
            file.priority, file.source, file.destination
        );
        install_file(data_root, dest_dir, file, &mut stats)?;
    }

    info!("FOMOD complete: {} files, {} folders installed",
        stats.files_installed, stats.folders_installed);

    Ok(stats)
}

/// Resolve a plugin's effective install-time type, honoring
/// `<typeDescriptor><dependencyType>` patterns. The first pattern whose
/// dependencies match wins; falls back to `dep_type_default`, then to the
/// plain `<type/>` value.
fn effective_plugin_type(
    plugin: &Plugin,
    flags: &HashMap<String, String>,
    dest_dir: &Path,
) -> PluginType {
    for pattern in &plugin.dep_type_patterns {
        if evaluate_dependencies(&pattern.dependencies, flags, dest_dir) {
            return pattern.plugin_type;
        }
    }
    plugin.dep_type_default.unwrap_or(plugin.type_descriptor)
}

/// Evaluate dependencies against collected flags + the current install
/// destination tree (for `<fileDependency>` checks).
///
/// `dest_dir` is the mod's payload root (post-Data-strip). File deps are
/// evaluated case-insensitively against files already placed there during
/// this FOMOD run, since cross-mod state isn't available here.
fn evaluate_dependencies(
    deps: &Dependencies,
    flags: &HashMap<String, String>,
    dest_dir: &Path,
) -> bool {
    let mut results: Vec<bool> = Vec::new();

    // Flag deps.
    for flag_dep in &deps.flags {
        let matches = flags
            .get(&flag_dep.flag)
            .is_some_and(|v| v.eq_ignore_ascii_case(&flag_dep.value));
        results.push(matches);
    }

    // File-state deps. We can only see files in the current mod's dest, so
    // approximate: Active = present, Missing = absent, Inactive = absent.
    // FOMOD authors rarely use Inactive in collection-level conditionals.
    for file_dep in &deps.files {
        let exists = super::executor::find_path_case_insensitive(dest_dir, &file_dep.file)
            .is_some();
        let want_present = !file_dep.state.eq_ignore_ascii_case("Missing")
            && !file_dep.state.eq_ignore_ascii_case("Inactive");
        results.push(exists == want_present);
    }

    // Game/FOMM version deps — pipeline has no way to verify, treat as
    // satisfied so we don't drop conditional installs that gate on them.
    for _ in &deps.game_versions {
        results.push(true);
    }
    for _ in &deps.fomm_versions {
        results.push(true);
    }

    // Nested dep groups recurse with the same evaluation rules.
    for nested in &deps.nested {
        results.push(evaluate_dependencies(nested, flags, dest_dir));
    }

    // No predicates → vacuously satisfied.
    if results.is_empty() {
        return true;
    }

    match deps.operator {
        DependencyOperator::And => results.iter().all(|&r| r),
        DependencyOperator::Or => results.iter().any(|&r| r),
    }
}

/// Install a file or folder from source to destination.
/// Matches C++ FOMOD installer behavior exactly.
fn install_file(
    data_root: &Path,
    dest_dir: &Path,
    file: &InstallFile,
    stats: &mut FomodStats,
) -> Result<()> {
    // Normalize paths: FOMOD configs may have Windows backslashes
    let source_normalized = file.source.replace('\\', "/");
    let mut dest_normalized = file.destination.replace('\\', "/");

    // Empty `<file source=""/>` is meaningless — skip. Empty
    // `<folder source=""/>` means "install everything in the FOMOD data
    // root", a not-uncommon idiom for archives whose payload is laid out
    // at root with the FOMOD beside it (`fomod/`, plus loose payload).
    if file.source.is_empty() {
        if file.is_folder {
            let dest_path = if dest_normalized.is_empty() {
                dest_dir.to_path_buf()
            } else {
                let dest_lower = dest_normalized.to_lowercase();
                let stripped = if dest_lower == "data" || dest_lower == "data/" || dest_lower == "data\\" {
                    String::new()
                } else if dest_lower.starts_with("data/") || dest_lower.starts_with("data\\") {
                    dest_normalized[5..].to_string()
                } else {
                    dest_normalized.clone()
                };
                if stripped.is_empty() {
                    dest_dir.to_path_buf()
                } else {
                    dest_dir.join(&stripped)
                }
            };
            install_data_root_excluding_fomod(data_root, &dest_path, stats)?;
        }
        return Ok(());
    }

    // Handle root destination markers (Windows \ or /) - matches C++ behavior
    if dest_normalized == "/" || dest_normalized == "\\" {
        if file.is_folder {
            // Folder to root = install contents directly to dest root
            dest_normalized = String::new();
        } else {
            // File to root = use just the filename
            dest_normalized = Path::new(&source_normalized)
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();
        }
    }

    // Strip "Data/" or "Data" prefix - MO2 mods are implicitly in Data folder context
    // So destination="Data/scripts/" should become "scripts/"
    let dest_lower = dest_normalized.to_lowercase();
    if dest_lower == "data" || dest_lower == "data/" || dest_lower == "data\\" {
        // Just "Data" or "Data/" means root
        dest_normalized = String::new();
    } else if dest_lower.starts_with("data/") || dest_lower.starts_with("data\\") {
        dest_normalized = dest_normalized[5..].to_string();
    }

    // Find source path case-insensitively. For *folder* sources we treat
    // a missing path as a no-op rather than a hard error: archives often
    // include empty folders the FOMOD references but the chosen variant
    // doesn't populate (e.g. SKSE/ left empty for the user's plugin set).
    // A missing *file* source is still an error — it usually means the
    // FOMOD config drifted from the archive contents.
    let source_path = match find_path_case_insensitive(data_root, &source_normalized) {
        Some(p) => p,
        None if file.is_folder => {
            tracing::debug!(
                "FOMOD folder source missing — treating as no-op: {}",
                file.source
            );
            return Ok(());
        }
        None => {
            return Err(anyhow::anyhow!("Source not found: {}", file.source));
        }
    };

    // Determine destination path - matches C++ behavior
    let dest_path = if dest_normalized.is_empty() {
        if file.is_folder {
            // Folder with no destination = install contents to root
            dest_dir.to_path_buf()
        } else {
            // File with no destination = use just the filename (C++ line 186-189)
            let filename = Path::new(&source_normalized)
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_else(|| source_normalized.clone());
            dest_dir.join(&filename)
        }
    } else if !file.is_folder && (dest_normalized.ends_with('/') || dest_normalized.ends_with('\\')) {
        // Destination ends with / or \ means "copy file into this directory with original filename"
        // C++ handles this implicitly through std::filesystem::path behavior
        let filename = Path::new(&source_normalized)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| source_normalized.clone());
        dest_dir.join(&dest_normalized).join(&filename)
    } else {
        dest_dir.join(&dest_normalized)
    };

    if file.is_folder {
        install_folder_merge(&source_path, &dest_path, stats)?;
    } else {
        install_single_file(&source_path, &dest_path, stats)?;
    }

    Ok(())
}

/// Find existing folder with case-insensitive match in destination.
/// Matches C++ findExistingFolder behavior.
fn find_existing_folder(dest_dir: &Path, folder_name: &str) -> Option<std::path::PathBuf> {
    if !dest_dir.exists() || !dest_dir.is_dir() {
        return None;
    }

    let name_lower = folder_name.to_lowercase();

    if let Ok(entries) = fs::read_dir(dest_dir) {
        for entry in entries.flatten() {
            if entry.path().is_dir() {
                let entry_name = entry.file_name().to_string_lossy().to_string();
                if entry_name.to_lowercase() == name_lower {
                    return Some(entry.path());
                }
            }
        }
    }
    None
}

/// Install a folder with case-insensitive merging.
/// Matches C++ copyDirMerge behavior exactly.
fn install_folder_merge(source: &Path, dest: &Path, stats: &mut FomodStats) -> Result<()> {
    if !dest.exists() {
        fs::create_dir_all(dest)
            .with_context(|| format!("Failed to create directory: {}", dest.display()))?;
    }

    if let Ok(entries) = fs::read_dir(source) {
        for entry in entries.flatten() {
            let item_name = entry.file_name().to_string_lossy().to_string();

            if entry.path().is_dir() {
                // Check for case-insensitive match in destination
                let target_dir = if let Some(existing) = find_existing_folder(dest, &item_name) {
                    // Merge into existing folder (preserves original case)
                    existing
                } else {
                    // Create new folder
                    let new_dir = dest.join(&item_name);
                    fs::create_dir_all(&new_dir)
                        .with_context(|| format!("Failed to create directory: {}", new_dir.display()))?;
                    new_dir
                };
                // Recursively merge
                install_folder_merge(&entry.path(), &target_dir, stats)?;
            } else {
                // Copy file, overwriting if exists
                let target = dest.join(&item_name);
                if target.exists() {
                    fs::remove_file(&target)
                        .with_context(|| format!("Failed to remove existing file: {}", target.display()))?;
                }
                fs::copy(entry.path(), &target)
                    .with_context(|| format!("Failed to copy: {} -> {}", entry.path().display(), target.display()))?;
                stats.files_installed += 1;
            }
        }
    }
    stats.folders_installed += 1;
    Ok(())
}

/// Install everything in `data_root` into `dest`, except the top-level
/// `fomod/` directory (which holds ModuleConfig.xml + assets and shouldn't
/// ship into the deployed mod folder). Used for `<folder source=""/>` —
/// FOMOD's idiom for "install the whole archive payload".
fn install_data_root_excluding_fomod(
    data_root: &Path,
    dest: &Path,
    stats: &mut FomodStats,
) -> Result<()> {
    if !dest.exists() {
        fs::create_dir_all(dest)
            .with_context(|| format!("Failed to create directory: {}", dest.display()))?;
    }
    if let Ok(entries) = fs::read_dir(data_root) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.eq_ignore_ascii_case("fomod") && entry.path().is_dir() {
                continue;
            }
            if entry.path().is_dir() {
                let target_dir = if let Some(existing) = find_existing_folder(dest, &name_str) {
                    existing
                } else {
                    let new_dir = dest.join(&*name_str);
                    fs::create_dir_all(&new_dir).with_context(|| {
                        format!("Failed to create directory: {}", new_dir.display())
                    })?;
                    new_dir
                };
                install_folder_merge(&entry.path(), &target_dir, stats)?;
            } else {
                let target = dest.join(&*name_str);
                if target.exists() {
                    fs::remove_file(&target).with_context(|| {
                        format!("Failed to remove existing file: {}", target.display())
                    })?;
                }
                fs::copy(entry.path(), &target).with_context(|| {
                    format!(
                        "Failed to copy: {} -> {}",
                        entry.path().display(),
                        target.display()
                    )
                })?;
                stats.files_installed += 1;
            }
        }
    }
    stats.folders_installed += 1;
    Ok(())
}

/// Install a single file.
fn install_single_file(source: &Path, dest: &Path, stats: &mut FomodStats) -> Result<()> {
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create parent directory: {}", parent.display()))?;
    }
    // Remove existing file if it exists (might have restrictive permissions from previous run)
    if dest.exists() {
        fs::remove_file(dest)
            .with_context(|| format!("Failed to remove existing file: {}", dest.display()))?;
    }
    fs::copy(source, dest)
        .with_context(|| format!("Failed to copy: {} -> {}", source.display(), dest.display()))?;
    stats.files_installed += 1;
    Ok(())
}

/// Find a path case-insensitively.
///
/// FOMOD source paths may not match actual filesystem case on Linux.
pub fn find_path_case_insensitive(root: &Path, relative: &str) -> Option<std::path::PathBuf> {
    // Normalize the path separators (FOMOD uses backslashes)
    let normalized = relative.replace('\\', "/");
    let parts: Vec<&str> = normalized.split('/').filter(|p| !p.is_empty()).collect();

    let mut current = root.to_path_buf();

    for part in parts {
        let part_lower = part.to_lowercase();

        // Try exact match first
        let exact = current.join(part);
        if exact.exists() {
            current = exact;
            continue;
        }

        // Try case-insensitive match
        let mut found = false;
        if let Ok(entries) = fs::read_dir(&current) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                if name_str.to_lowercase() == part_lower {
                    current = entry.path();
                    found = true;
                    break;
                }
            }
        }

        if !found {
            return None;
        }
    }

    if current.exists() {
        Some(current)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::{FomodChoice, FomodGroup, FomodStep};
    use tempfile::tempdir;

    #[test]
    fn test_evaluate_dependencies_and() {
        let deps = Dependencies {
            operator: DependencyOperator::And,
            flags: vec![
                super::super::parser::FlagDependency {
                    flag: "A".to_string(),
                    value: "true".to_string(),
                },
                super::super::parser::FlagDependency {
                    flag: "B".to_string(),
                    value: "true".to_string(),
                },
            ],
            nested: vec![],
            ..Default::default()
        };

        let mut flags = HashMap::new();
        flags.insert("A".to_string(), "true".to_string());
        assert!(!evaluate_dependencies(&deps, &flags, std::path::Path::new(""))); // Missing B

        flags.insert("B".to_string(), "true".to_string());
        assert!(evaluate_dependencies(&deps, &flags, std::path::Path::new(""))); // Both set
    }

    #[test]
    fn test_evaluate_dependencies_or() {
        let deps = Dependencies {
            operator: DependencyOperator::Or,
            flags: vec![
                super::super::parser::FlagDependency {
                    flag: "A".to_string(),
                    value: "true".to_string(),
                },
                super::super::parser::FlagDependency {
                    flag: "B".to_string(),
                    value: "true".to_string(),
                },
            ],
            nested: vec![],
            ..Default::default()
        };

        let mut flags = HashMap::new();
        assert!(!evaluate_dependencies(&deps, &flags, std::path::Path::new(""))); // Neither set

        flags.insert("A".to_string(), "true".to_string());
        assert!(evaluate_dependencies(&deps, &flags, std::path::Path::new(""))); // One set
    }

    #[test]
    fn test_case_insensitive_path() {
        let temp = tempdir().unwrap();
        fs::create_dir_all(temp.path().join("Meshes/Armor")).unwrap();
        fs::write(temp.path().join("Meshes/Armor/test.nif"), "data").unwrap();

        // Try finding with different case
        let result = find_path_case_insensitive(temp.path(), "meshes/armor/test.nif");
        assert!(result.is_some());
        assert!(result.unwrap().exists());
    }

    #[test]
    fn test_execute_required_files() {
        let temp = tempdir().unwrap();
        let dest = tempdir().unwrap();

        // Create source file
        fs::write(temp.path().join("test.esp"), "plugin data").unwrap();

        let config = FomodConfig {
            module_name: "Test".to_string(),
            required_files: vec![InstallFile {
                source: "test.esp".to_string(),
                destination: String::new(),
                priority: 0,
                is_folder: false,
            }],
            install_steps: vec![],
            conditional_installs: vec![],
        };

        let choices = FomodChoices::default();
        let stats = execute_fomod(temp.path(), dest.path(), &config, &choices).unwrap();

        assert_eq!(stats.files_installed, 1);
        assert!(dest.path().join("test.esp").exists());
    }

    #[test]
    fn test_priority_order_overwrite() {
        // High-priority plugin file wins at the destination — even though
        // it's processed earlier in step order.
        let temp = tempdir().unwrap();
        let dest = tempdir().unwrap();
        fs::write(temp.path().join("hi.esp"), b"HI").unwrap();
        fs::write(temp.path().join("lo.esp"), b"LO").unwrap();

        let config = FomodConfig {
            module_name: "T".to_string(),
            required_files: vec![
                InstallFile {
                    source: "hi.esp".to_string(),
                    destination: "out.esp".to_string(),
                    priority: 100,
                    is_folder: false,
                },
                InstallFile {
                    source: "lo.esp".to_string(),
                    destination: "out.esp".to_string(),
                    priority: 0,
                    is_folder: false,
                },
            ],
            install_steps: vec![],
            conditional_installs: vec![],
        };

        execute_fomod(temp.path(), dest.path(), &config, &FomodChoices::default()).unwrap();
        assert_eq!(fs::read(dest.path().join("out.esp")).unwrap(), b"HI");
    }

    #[test]
    fn test_empty_source_folder_installs_data_root_excluding_fomod() {
        let temp = tempdir().unwrap();
        let dest = tempdir().unwrap();
        fs::create_dir_all(temp.path().join("fomod")).unwrap();
        fs::write(temp.path().join("fomod/ModuleConfig.xml"), b"<config/>").unwrap();
        fs::create_dir_all(temp.path().join("meshes")).unwrap();
        fs::write(temp.path().join("meshes/x.nif"), b"m").unwrap();
        fs::write(temp.path().join("plugin.esp"), b"p").unwrap();

        let config = FomodConfig {
            module_name: "T".to_string(),
            required_files: vec![InstallFile {
                source: String::new(),
                destination: String::new(),
                priority: 0,
                is_folder: true,
            }],
            install_steps: vec![],
            conditional_installs: vec![],
        };

        execute_fomod(temp.path(), dest.path(), &config, &FomodChoices::default()).unwrap();
        assert!(dest.path().join("plugin.esp").exists(), "loose esp not copied");
        assert!(dest.path().join("meshes/x.nif").exists(), "subdir not copied");
        assert!(!dest.path().join("fomod").exists(), "fomod dir leaked into mod");
    }

    #[test]
    fn test_required_plugin_installs_without_choice() {
        // SelectExactlyOne with one plugin marked Required + one Optional
        // not in choices → Required installs anyway, Optional skipped.
        let temp = tempdir().unwrap();
        let dest = tempdir().unwrap();
        fs::write(temp.path().join("req.esp"), b"R").unwrap();
        fs::write(temp.path().join("opt.esp"), b"O").unwrap();

        let mk_plugin = |name: &str, src: &str, ty| super::super::parser::Plugin {
            name: name.to_string(),
            description: String::new(),
            files: vec![InstallFile {
                source: src.to_string(),
                destination: String::new(),
                priority: 0,
                is_folder: false,
            }],
            condition_flags: vec![],
            type_descriptor: ty,
            dep_type_default: None,
            dep_type_patterns: vec![],
        };

        let config = FomodConfig {
            module_name: "T".to_string(),
            required_files: vec![],
            install_steps: vec![super::super::parser::InstallStep {
                name: "S".to_string(),
                groups: vec![super::super::parser::OptionGroup {
                    name: "G".to_string(),
                    group_type: super::super::parser::GroupType::SelectAny,
                    plugins: vec![
                        mk_plugin("Req", "req.esp", PluginType::Required),
                        mk_plugin("Opt", "opt.esp", PluginType::Optional),
                    ],
                }],
            }],
            conditional_installs: vec![],
        };

        // Choices empty → Optional should NOT install but Required must.
        let choices = FomodChoices::default();
        execute_fomod(temp.path(), dest.path(), &config, &choices).unwrap();

        assert!(dest.path().join("req.esp").exists(), "Required plugin not installed");
        assert!(!dest.path().join("opt.esp").exists(), "Optional plugin installed without choice");
    }

    #[test]
    fn test_recommended_plugin_installs_when_no_choices_recorded() {
        // Group not in choices JSON at all (legacy / partial install) →
        // Recommended plugin should install as a default.
        let temp = tempdir().unwrap();
        let dest = tempdir().unwrap();
        fs::write(temp.path().join("rec.esp"), b"R").unwrap();

        let config = FomodConfig {
            module_name: "T".to_string(),
            required_files: vec![],
            install_steps: vec![super::super::parser::InstallStep {
                name: "S".to_string(),
                groups: vec![super::super::parser::OptionGroup {
                    name: "G".to_string(),
                    group_type: super::super::parser::GroupType::SelectAny,
                    plugins: vec![super::super::parser::Plugin {
                        name: "Rec".to_string(),
                        description: String::new(),
                        files: vec![InstallFile {
                            source: "rec.esp".to_string(),
                            destination: String::new(),
                            priority: 0,
                            is_folder: false,
                        }],
                        condition_flags: vec![],
                        type_descriptor: PluginType::Recommended,
                        dep_type_default: None,
                        dep_type_patterns: vec![],
                    }],
                }],
            }],
            conditional_installs: vec![],
        };

        let choices = FomodChoices::default();
        execute_fomod(temp.path(), dest.path(), &config, &choices).unwrap();
        assert!(dest.path().join("rec.esp").exists());
    }

    #[test]
    fn test_execute_with_choices() {
        let temp = tempdir().unwrap();
        let dest = tempdir().unwrap();

        // Create source files
        fs::create_dir_all(temp.path().join("optionA")).unwrap();
        fs::write(temp.path().join("optionA/a.esp"), "A").unwrap();
        fs::create_dir_all(temp.path().join("optionB")).unwrap();
        fs::write(temp.path().join("optionB/b.esp"), "B").unwrap();

        let config = FomodConfig {
            module_name: "Test".to_string(),
            required_files: vec![],
            install_steps: vec![super::super::parser::InstallStep {
                name: "Choose".to_string(),
                groups: vec![super::super::parser::OptionGroup {
                    name: "Options".to_string(),
                    group_type: super::super::parser::GroupType::SelectExactlyOne,
                    plugins: vec![
                        super::super::parser::Plugin {
                            name: "Option A".to_string(),
                            description: String::new(),
                            files: vec![InstallFile {
                                source: "optionA/a.esp".to_string(),
                                destination: String::new(),
                                priority: 0,
                                is_folder: false,
                            }],
                            condition_flags: vec![],
                            type_descriptor: super::super::parser::PluginType::Optional,
                            dep_type_default: None,
                            dep_type_patterns: vec![],
                        },
                        super::super::parser::Plugin {
                            name: "Option B".to_string(),
                            description: String::new(),
                            files: vec![InstallFile {
                                source: "optionB/b.esp".to_string(),
                                destination: String::new(),
                                priority: 0,
                                is_folder: false,
                            }],
                            condition_flags: vec![],
                            type_descriptor: super::super::parser::PluginType::Optional,
                            dep_type_default: None,
                            dep_type_patterns: vec![],
                        },
                    ],
                }],
            }],
            conditional_installs: vec![],
        };

        // Select Option A
        let choices = FomodChoices {
            options: vec![FomodStep {
                name: "Choose".to_string(),
                groups: vec![FomodGroup {
                    name: "Options".to_string(),
                    choices: vec![FomodChoice {
                        name: "Option A".to_string(),
                        idx: 0,
                    }],
                }],
            }],
        };

        let stats = execute_fomod(temp.path(), dest.path(), &config, &choices).unwrap();

        assert_eq!(stats.files_installed, 1);
        // When destination is empty, file goes to root with just filename
        assert!(dest.path().join("a.esp").exists());
        assert!(!dest.path().join("b.esp").exists());
    }
}
