//! Case-insensitive path handling for Windows paths on Linux
//!
//! Wabbajack modlists use Windows-style paths with backslashes.
//! This module handles:
//! - Converting `\` to `/` for Linux filesystem operations
//! - Case-insensitive file lookups (Windows is case-insensitive, Linux is not)
//! - Preserving intended case for output paths
//! - Unicode normalization (NFC) for consistent path matching
//! - CP437 to UTF-8 conversion for legacy Windows archives

// Module not yet integrated into main installation pipeline
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use unicode_normalization::UnicodeNormalization;

/// CP437 to Unicode mapping for bytes 0x80-0xFF
/// Used to convert legacy DOS/Windows filenames to UTF-8
const CP437_TO_UNICODE: [char; 128] = [
    'Ç', 'ü', 'é', 'â', 'ä', 'à', 'å', 'ç', 'ê', 'ë', 'è', 'ï', 'î', 'ì', 'Ä', 'Å', 'É', 'æ', 'Æ',
    'ô', 'ö', 'ò', 'û', 'ù', 'ÿ', 'Ö', 'Ü', '¢', '£', '¥', '₧', 'ƒ', 'á', 'í', 'ó', 'ú', 'ñ', 'Ñ',
    'ª', 'º', '¿', '⌐', '¬', '½', '¼', '¡', '«', '»', '░', '▒', '▓', '│', '┤', '╡', '╢', '╖', '╕',
    '╣', '║', '╗', '╝', '╜', '╛', '┐', '└', '┴', '┬', '├', '─', '┼', '╞', '╟', '╚', '╔', '╩', '╦',
    '╠', '═', '╬', '╧', '╨', '╤', '╥', '╙', '╘', '╒', '╓', '╫', '╪', '┘', '┌', '█', '▄', '▌', '▐',
    '▀', 'α', 'ß', 'Γ', 'π', 'Σ', 'σ', 'µ', 'τ', 'Φ', 'Θ', 'Ω', 'δ', '∞', 'φ', 'ε', '∩', '≡', '±',
    '≥', '≤', '⌠', '⌡', '÷', '≈', '°', '∙', '·', '√', 'ⁿ', '²', '■', ' ',
];

/// Convert a byte sequence that might contain CP437 characters to UTF-8
///
/// This handles the case where 7z extracts files with CP437-encoded filenames
/// on Linux, resulting in raw bytes 0x80-0xFF that need conversion to UTF-8.
pub fn cp437_to_utf8(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|&b| {
            if b < 0x80 {
                b as char
            } else {
                CP437_TO_UNICODE[(b - 0x80) as usize]
            }
        })
        .collect()
}

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

/// Normalize a path for lookups and comparisons (NFC normalized, lowercase, forward slashes, trimmed)
///
/// Uses Unicode NFC normalization to handle accented characters consistently.
/// e.g., "atúlg" stored as u+combining accent matches "atúlg" stored as single ú character.
pub fn normalize_for_lookup(path: &str) -> String {
    path.nfc()
        .collect::<String>()
        .to_lowercase()
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
        let target_lower = component.nfc().collect::<String>().to_lowercase();

        // Read directory and find matching entry
        let found = std::fs::read_dir(&current).ok()?.find_map(|entry| {
            let entry = entry.ok()?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            let name_normalized = name_str.nfc().collect::<String>().to_lowercase();

            if name_normalized == target_lower {
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
    entries
        .iter()
        .find(|e| normalize_for_lookup(e) == target_normalized)
        .map(|s| s.as_str())
}

/// Create parent directories for a path if they don't exist.
/// Safe to call concurrently from multiple threads.
///
/// If a non-directory entry (file, symlink, etc.) blocks directory creation,
/// it is removed and creation is retried.
pub fn ensure_parent_dirs(path: &Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                // Something non-directory exists where a directory is needed.
                // Walk path components to find the blocker (file, symlink, etc.)
                if let Some(blocker) = find_blocking_entry(parent) {
                    // Use symlink_metadata so we can identify broken symlinks too
                    let kind = match std::fs::symlink_metadata(&blocker) {
                        Ok(m) => {
                            if m.is_symlink() {
                                "symlink"
                            } else if m.is_file() {
                                "file"
                            } else {
                                "other"
                            }
                        }
                        Err(_) => "unknown",
                    };
                    tracing::warn!(
                        "Removing {} blocking directory creation: {}",
                        kind,
                        blocker.display()
                    );
                    // remove_file works on symlinks too.
                    // Ignore NotFound - another thread may have already removed it.
                    match std::fs::remove_file(&blocker) {
                        Ok(()) => {}
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                        Err(e) => return Err(e),
                    }
                    std::fs::create_dir_all(parent)?;
                } else if parent.is_dir() {
                    // Directory exists now (concurrent creation) - that's fine
                } else {
                    return Err(e);
                }
            } else {
                return Err(e);
            }
        }
    }
    Ok(())
}

/// Walk path components to find a non-directory entry blocking directory creation.
/// Uses symlink_metadata (lstat) to detect broken symlinks that stat/is_file miss.
fn find_blocking_entry(dir_path: &Path) -> Option<std::path::PathBuf> {
    let mut current = std::path::PathBuf::new();
    for component in dir_path.components() {
        current.push(component);
        // symlink_metadata doesn't follow symlinks, so it detects:
        // - regular files
        // - symlinks (broken or valid)
        // - any other non-directory entry
        match std::fs::symlink_metadata(&current) {
            Ok(meta) if !meta.is_dir() => return Some(current),
            Err(_) => return None, // path doesn't exist yet, nothing blocking
            _ => {}                // is a directory, keep going
        }
    }
    None
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
        assert_eq!(
            to_linux_path("Data\\Textures\\armor.dds"),
            "Data/Textures/armor.dds"
        );
        assert_eq!(to_linux_path("already/linux/path"), "already/linux/path");
        assert_eq!(to_linux_path("mixed\\path/style"), "mixed/path/style");
    }

    #[test]
    fn test_normalize() {
        assert_eq!(
            normalize_for_lookup("Data\\Textures\\Armor.dds"),
            "data/textures/armor.dds"
        );
        assert_eq!(
            normalize_for_lookup("MESHES/Actor/Character"),
            "meshes/actor/character"
        );
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
        assert_eq!(find_in_archive_entries(&entries, "notfound.txt"), None);
    }

    #[test]
    fn test_paths_equal() {
        assert!(paths_equal(
            "Data\\Textures\\armor.dds",
            "data\\textures\\ARMOR.DDS"
        ));
        assert!(!paths_equal(
            "Data\\Textures\\armor.dds",
            "data\\textures\\sword.dds"
        ));
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
        assert_eq!(
            parent_path("Data\\Textures\\armor.dds"),
            Some("Data\\Textures")
        );
        assert_eq!(parent_path("armor.dds"), None);
    }

    #[test]
    fn test_cp437_to_utf8() {
        // CP437 byte 0xA3 = ú (lowercase u with acute)
        let cp437_bytes = b"at\xa3lg gro-larg\xa3m";
        let utf8_result = cp437_to_utf8(cp437_bytes);
        assert_eq!(utf8_result, "atúlg gro-largúm");

        // Test that the normalized versions match
        let utf8_path = "atúlg gro-largúm/file.mp3";
        let cp437_path_bytes = b"at\xa3lg gro-larg\xa3m/file.mp3";
        let converted = cp437_to_utf8(cp437_path_bytes);

        assert_eq!(
            normalize_for_lookup(utf8_path),
            normalize_for_lookup(&converted)
        );
    }

    #[test]
    fn test_unicode_normalization() {
        // Precomposed ú (U+00FA) vs decomposed u + combining acute (U+0075 U+0301)
        let precomposed = "atúlg gro-largúm";
        let decomposed = "atu\u{0301}lg gro-largu\u{0301}m";

        // These should match after NFC normalization
        assert_eq!(
            normalize_for_lookup(precomposed),
            normalize_for_lookup(decomposed)
        );

        // Test in full path context
        let path1 = "00 - Core/Sound/Vo/AIV/orc/m/atúlg gro-largúm/file.mp3";
        let path2 = "00 - Core/Sound/Vo/AIV/orc/m/atu\u{0301}lg gro-largu\u{0301}m/file.mp3";
        assert_eq!(normalize_for_lookup(path1), normalize_for_lookup(path2));
    }
}
