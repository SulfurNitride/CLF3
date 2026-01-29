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

use super::parser::{Dependencies, DependencyOperator, FomodConfig, InstallFile};
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

    // Phase 1: Install required files
    for file in &config.required_files {
        debug!("Installing required: {} -> {}", file.source, file.destination);
        install_file(data_root, dest_dir, file, &mut stats)?;
    }

    // Phase 2: Process install steps with choices, collecting flags
    for step in &config.install_steps {
        debug!("Processing step: {}", step.name);

        for group in &step.groups {
            debug!("  Processing group: {}", group.name);

            // Find matching choice entry by BOTH step name AND group name
            // Nexus collection JSON stores each step+group combination as separate entries
            // all with the same step name but different group names
            let group_choices = choices.options.iter()
                .filter(|s| s.name.eq_ignore_ascii_case(&step.name))
                .find_map(|s| {
                    s.groups.iter().find(|g| g.name.eq_ignore_ascii_case(&group.name))
                });

            if group_choices.is_none() {
                debug!("    No matching choices for step '{}' + group '{}'", step.name, group.name);
                debug!("    Available: {:?}",
                    choices.options.iter()
                        .map(|s| format!("{}:{}", s.name, s.groups.iter().map(|g| g.name.as_str()).collect::<Vec<_>>().join(",")))
                        .collect::<Vec<_>>());
            }

            for plugin in &group.plugins {
                // Check if this plugin is selected in choices
                let is_selected = group_choices.map_or(false, |g| {
                    g.choices.iter().any(|c| c.name.eq_ignore_ascii_case(&plugin.name))
                });

                debug!("    Plugin '{}': selected={}, files={}",
                    plugin.name, is_selected, plugin.files.len());

                if is_selected {
                    // Set condition flags
                    for flag in &plugin.condition_flags {
                        debug!("      Setting flag: {}={}", flag.name, flag.value);
                        stats.flags_set.insert(flag.name.clone(), flag.value.clone());
                    }

                    // Install files from this plugin
                    for file in &plugin.files {
                        debug!("      Installing: {} -> {}", file.source, file.destination);
                        install_file(data_root, dest_dir, file, &mut stats)?;
                    }
                }
            }
        }
    }

    // Phase 3: Process conditional installs based on collected flags
    debug!("Processing {} conditional patterns with flags: {:?}",
        config.conditional_installs.len(), stats.flags_set);

    for pattern in &config.conditional_installs {
        let matches = evaluate_dependencies(&pattern.dependencies, &stats.flags_set);
        debug!("  Conditional pattern: matches={}, files={}", matches, pattern.files.len());

        if matches {
            for file in &pattern.files {
                debug!("    Installing: {} -> {}", file.source, file.destination);
                install_file(data_root, dest_dir, file, &mut stats)?;
            }
        }
    }

    info!("FOMOD complete: {} files, {} folders installed",
        stats.files_installed, stats.folders_installed);

    Ok(stats)
}

/// Evaluate dependencies against collected flags.
fn evaluate_dependencies(deps: &Dependencies, flags: &HashMap<String, String>) -> bool {
    let mut results: Vec<bool> = Vec::new();

    // Check flag dependencies
    for flag_dep in &deps.flags {
        let matches = flags
            .get(&flag_dep.flag)
            .map_or(false, |v| v.eq_ignore_ascii_case(&flag_dep.value));
        results.push(matches);
    }

    // Check nested dependencies recursively
    for nested in &deps.nested {
        results.push(evaluate_dependencies(nested, flags));
    }

    // If no conditions, treat as satisfied
    if results.is_empty() {
        return true;
    }

    // Combine results based on operator
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

    if file.source.is_empty() {
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
    } else if dest_lower.starts_with("data/") {
        dest_normalized = dest_normalized[5..].to_string();
    } else if dest_lower.starts_with("data\\") {
        dest_normalized = dest_normalized[5..].to_string();
    }

    // Find source path case-insensitively
    let source_path = find_path_case_insensitive(data_root, &source_normalized)
        .with_context(|| format!("Source not found: {}", file.source))?;

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
        };

        let mut flags = HashMap::new();
        flags.insert("A".to_string(), "true".to_string());
        assert!(!evaluate_dependencies(&deps, &flags)); // Missing B

        flags.insert("B".to_string(), "true".to_string());
        assert!(evaluate_dependencies(&deps, &flags)); // Both set
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
        };

        let mut flags = HashMap::new();
        assert!(!evaluate_dependencies(&deps, &flags)); // Neither set

        flags.insert("A".to_string(), "true".to_string());
        assert!(evaluate_dependencies(&deps, &flags)); // One set
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
