//! Case-insensitive path handling for Windows paths on Linux
//!
//! Wabbajack modlists use Windows-style paths with backslashes.
//! This module handles:
//! - Converting `\` to `/` for Linux filesystem operations
//! - Case-insensitive file lookups (Windows is case-insensitive, Linux is not)
//! - Preserving intended case for output paths

// Module not yet integrated into main installation pipeline
#![allow(dead_code)]

use std::path::{Path, PathBuf};

/// Convert Windows path separators to Linux
/// `Data\Textures\armor.dds` -> `Data/Textures/armor.dds`
pub fn to_linux_path(path: &str) -> String {
    path.replace('\\', "/")
}

/// Convert a Windows-style path to a native PathBuf
/// Handles both forward and backslashes
pub fn to_native_pathbuf(path: &str) -> PathBuf {
    PathBuf::from(to_linux_path(path))
}

/// Normalize a path for lookups and comparisons (lowercase, forward slashes, trimmed)
pub fn normalize_for_lookup(path: &str) -> String {
    path.to_lowercase()
        .replace('\\', "/")
        .trim_matches('/')
        .to_string()
}

/// Check if two paths are equal (case-insensitive)
pub fn paths_equal(a: &str, b: &str) -> bool {
    normalize_for_lookup(a) == normalize_for_lookup(b)
}

/// Find a file case-insensitively within a directory
///
/// Given a base directory and a relative path like `Data\Textures\armor.dds`,
/// finds the actual file even if the real path is `data\TEXTURES\Armor.DDS`
pub fn resolve_case_insensitive(base: &Path, relative: &str) -> Option<PathBuf> {
    // Split the relative path into components
    let components: Vec<&str> = relative
        .split(['\\', '/'])
        .filter(|s| !s.is_empty())
        .collect();

    if components.is_empty() {
        return Some(base.to_path_buf());
    }

    let mut current = base.to_path_buf();

    for component in components {
        let target_lower = component.to_lowercase();

        // Read directory and find matching entry
        let found = std::fs::read_dir(&current).ok()?.find_map(|entry| {
            let entry = entry.ok()?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();

            if name_str.to_lowercase() == target_lower {
                Some(entry.path())
            } else {
                None
            }
        });

        match found {
            Some(path) => current = path,
            None => return None, // Component not found
        }
    }

    Some(current)
}

/// Get the parent directory of a path (handles both / and \)
pub fn parent_path(path: &str) -> Option<&str> {
    path.rfind(['\\', '/']).map(|idx| &path[..idx])
}

/// Get the filename from a path (handles both / and \)
pub fn file_name(path: &str) -> &str {
    path.rfind(['\\', '/'])
        .map(|idx| &path[idx + 1..])
        .unwrap_or(path)
}

/// Get file extension (lowercase)
pub fn extension(path: &str) -> Option<&str> {
    let name = file_name(path);
    name.rfind('.').map(|idx| &name[idx + 1..])
}

/// Find a file in a list of archive entries case-insensitively
/// Returns the actual path as it exists in the archive
pub fn find_in_archive_entries<'a>(entries: &'a [String], target: &str) -> Option<&'a str> {
    let target_normalized = normalize_for_lookup(target);
    entries.iter()
        .find(|e| normalize_for_lookup(e) == target_normalized)
        .map(|s| s.as_str())
}

/// Create parent directories for a path if they don't exist
pub fn ensure_parent_dirs(path: &Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent)?;
        }
    }
    Ok(())
}

/// Join a base path with a Windows-style relative path
/// Converts separators and creates a proper PathBuf
pub fn join_windows_path(base: &Path, relative: &str) -> PathBuf {
    base.join(to_linux_path(relative))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_linux_path() {
        assert_eq!(to_linux_path("Data\\Textures\\armor.dds"), "Data/Textures/armor.dds");
        assert_eq!(to_linux_path("already/linux/path"), "already/linux/path");
        assert_eq!(to_linux_path("mixed\\path/style"), "mixed/path/style");
    }

    #[test]
    fn test_normalize() {
        assert_eq!(normalize_for_lookup("Data\\Textures\\Armor.dds"), "data/textures/armor.dds");
        assert_eq!(normalize_for_lookup("MESHES/Actor/Character"), "meshes/actor/character");
    }

    #[test]
    fn test_find_in_archive_entries() {
        let entries = vec![
            "Data/Textures/ARMOR.dds".to_string(),
            "meshes/actor/character.nif".to_string(),
        ];
        assert_eq!(
            find_in_archive_entries(&entries, "data\\textures\\armor.dds"),
            Some("Data/Textures/ARMOR.dds")
        );
        assert_eq!(
            find_in_archive_entries(&entries, "MESHES\\ACTOR\\CHARACTER.NIF"),
            Some("meshes/actor/character.nif")
        );
        assert_eq!(
            find_in_archive_entries(&entries, "notfound.txt"),
            None
        );
    }

    #[test]
    fn test_paths_equal() {
        assert!(paths_equal("Data\\Textures\\armor.dds", "data\\textures\\ARMOR.DDS"));
        assert!(!paths_equal("Data\\Textures\\armor.dds", "data\\textures\\sword.dds"));
    }

    #[test]
    fn test_file_name() {
        assert_eq!(file_name("Data\\Textures\\armor.dds"), "armor.dds");
        assert_eq!(file_name("armor.dds"), "armor.dds");
        assert_eq!(file_name("Data/Textures/armor.dds"), "armor.dds");
    }

    #[test]
    fn test_extension() {
        assert_eq!(extension("armor.dds"), Some("dds"));
        assert_eq!(extension("Data\\armor.dds"), Some("dds"));
        assert_eq!(extension("noext"), None);
    }

    #[test]
    fn test_parent_path() {
        assert_eq!(parent_path("Data\\Textures\\armor.dds"), Some("Data\\Textures"));
        assert_eq!(parent_path("armor.dds"), None);
    }
}
