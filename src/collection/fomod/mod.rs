//! FOMOD installer support for Nexus Collections.
#![allow(unused_imports)] // Re-exports for public API
//!
//! FOMOD (Fallout Mod) is an XML-based mod installer format originally created
//! for Fallout 3/NV but now widely used across Bethesda games. It provides:
//! - Interactive installation steps with user choices
//! - Conditional file installation based on flags
//! - Required files that are always installed
//!
//! This module handles:
//! - XML encoding detection (UTF-16 LE/BE, UTF-8 BOM)
//! - ModuleConfig.xml parsing
//! - Applying pre-recorded choices from collection JSON
//! - File/folder installation with case-insensitive path matching
//!
//! # Example
//!
//! ```ignore
//! use clf3::collection::fomod::{find_module_config, parse_fomod, execute_fomod};
//! use clf3::collection::FomodChoices;
//!
//! // Find ModuleConfig.xml in extracted archive
//! let (config_path, data_root) = find_module_config(&extracted_path).unwrap();
//!
//! // Parse the FOMOD configuration
//! let config = parse_fomod(&config_path).unwrap();
//!
//! // Execute with choices from collection JSON
//! let stats = execute_fomod(&data_root, &dest_path, &config, &choices).unwrap();
//! ```

mod encoding;
pub mod executor;
pub mod parser;

pub use encoding::read_xml_with_encoding;
pub use executor::{execute_fomod, find_path_case_insensitive, FomodStats};
pub use parser::{parse_fomod, FomodConfig, InstallFile};

use std::path::{Path, PathBuf};
use walkdir::WalkDir;

/// Find ModuleConfig.xml in an extracted archive.
///
/// Searches recursively for a `fomod/ModuleConfig.xml` file (case-insensitive).
/// Returns the path to the config file and the data root directory (parent of fomod folder).
///
/// Handles common archive structures:
/// - `fomod/ModuleConfig.xml` (direct)
/// - `ModName/fomod/ModuleConfig.xml` (wrapper folder)
/// - `data/fomod/ModuleConfig.xml` (data subfolder)
pub fn find_module_config(archive_root: &Path) -> Option<(PathBuf, PathBuf)> {
    for entry in WalkDir::new(archive_root)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        if !entry.file_type().is_file() {
            continue;
        }

        let path = entry.path();
        let file_name = path.file_name()?.to_string_lossy().to_lowercase();

        // Check if this is ModuleConfig.xml
        if file_name != "moduleconfig.xml" {
            continue;
        }

        // Check if parent folder is "fomod" (case-insensitive)
        let parent = path.parent()?;
        let parent_name = parent.file_name()?.to_string_lossy().to_lowercase();

        if parent_name == "fomod" {
            // Data root is the parent of the fomod folder
            let data_root = parent.parent()?.to_path_buf();
            return Some((path.to_path_buf(), data_root));
        }
    }

    None
}

/// Check if an extracted archive contains a FOMOD installer.
pub fn has_fomod(archive_root: &Path) -> bool {
    find_module_config(archive_root).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_find_module_config_direct() {
        let temp = tempdir().unwrap();
        let fomod_dir = temp.path().join("fomod");
        fs::create_dir_all(&fomod_dir).unwrap();
        fs::write(fomod_dir.join("ModuleConfig.xml"), "<config/>").unwrap();

        let result = find_module_config(temp.path());
        assert!(result.is_some());

        let (config_path, data_root) = result.unwrap();
        assert!(config_path.ends_with("ModuleConfig.xml"));
        assert_eq!(data_root, temp.path());
    }

    #[test]
    fn test_find_module_config_wrapper_folder() {
        let temp = tempdir().unwrap();
        let mod_dir = temp.path().join("MyMod").join("fomod");
        fs::create_dir_all(&mod_dir).unwrap();
        fs::write(mod_dir.join("ModuleConfig.xml"), "<config/>").unwrap();

        let result = find_module_config(temp.path());
        assert!(result.is_some());

        let (_, data_root) = result.unwrap();
        assert_eq!(data_root, temp.path().join("MyMod"));
    }

    #[test]
    fn test_find_module_config_case_insensitive() {
        let temp = tempdir().unwrap();
        let fomod_dir = temp.path().join("FOMOD");
        fs::create_dir_all(&fomod_dir).unwrap();
        fs::write(fomod_dir.join("moduleconfig.XML"), "<config/>").unwrap();

        let result = find_module_config(temp.path());
        assert!(result.is_some());
    }

    #[test]
    fn test_find_module_config_not_found() {
        let temp = tempdir().unwrap();
        fs::write(temp.path().join("readme.txt"), "hello").unwrap();

        let result = find_module_config(temp.path());
        assert!(result.is_none());
    }

    #[test]
    fn test_has_fomod() {
        let temp = tempdir().unwrap();
        assert!(!has_fomod(temp.path()));

        let fomod_dir = temp.path().join("fomod");
        fs::create_dir_all(&fomod_dir).unwrap();
        fs::write(fomod_dir.join("ModuleConfig.xml"), "<config/>").unwrap();

        assert!(has_fomod(temp.path()));
    }
}
