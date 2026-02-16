//! Proton detection for Steam
//!
//! Finds Proton 10+ versions for Steam integration.
//! Supports Steam's built-in Protons and custom Protons in compatibilitytools.d.

use std::fs;
use std::path::PathBuf;

/// Information about an installed Proton version
#[derive(Debug, Clone)]
pub struct SteamProton {
    /// Display name (e.g., "GE-Proton10-20", "Proton Experimental")
    pub name: String,
    /// Internal name used in config.vdf
    pub config_name: String,
    /// Full path to the Proton installation
    pub path: PathBuf,
    /// Whether this is a Steam-provided Proton (vs custom)
    pub is_steam_proton: bool,
    /// Whether this is Proton Experimental
    pub is_experimental: bool,
}

/// Find the primary Steam installation path
pub fn find_steam_path() -> Option<PathBuf> {
    let home = std::env::var("HOME").ok()?;
    let home_path = PathBuf::from(&home);

    let candidates = [
        home_path.join(".local/share/Steam"),
        home_path.join(".steam/debian-installation"),
        home_path.join(".steam/steam"),
        home_path.join(".var/app/com.valvesoftware.Steam/data/Steam"),
        home_path.join(".var/app/com.valvesoftware.Steam/.local/share/Steam"),
        home_path.join("snap/steam/common/.local/share/Steam"),
    ];

    candidates.into_iter().find(|p| p.join("steamapps").exists())
}

/// Find all compatible Protons (Proton 10+ only)
pub fn find_steam_protons() -> Vec<SteamProton> {
    let mut protons = Vec::new();

    let Some(steam_path) = find_steam_path() else {
        return protons;
    };

    let is_flatpak = steam_path.to_string_lossy().contains(".var/app/com.valvesoftware.Steam");

    // 1. Steam's built-in Protons (steamapps/common/Proton*)
    protons.extend(find_builtin_protons(&steam_path));

    // 2. Custom Protons in user's compatibilitytools.d
    protons.extend(find_custom_protons(&steam_path));

    // 3. System-level Protons (skip for Flatpak)
    if !is_flatpak {
        protons.extend(find_system_protons());
    }

    // Filter to only Proton 10+ (required for Steam-native integration)
    protons.retain(is_proton_10_or_newer);

    // Filter to only Protons with valid wine binaries
    protons.retain(|p| has_wine_binary(p));

    // Sort: Experimental first, then by name descending (newest first)
    protons.sort_by(|a, b| {
        if a.is_experimental != b.is_experimental {
            return b.is_experimental.cmp(&a.is_experimental);
        }
        b.name.cmp(&a.name)
    });

    protons
}

/// Check if a Proton has a valid wine binary
fn has_wine_binary(proton: &SteamProton) -> bool {
    proton.path.join("files/bin/wine").exists()
        || proton.path.join("dist/bin/wine").exists()
}

/// Check if a Proton version is 10 or newer
fn is_proton_10_or_newer(proton: &SteamProton) -> bool {
    let name = &proton.name;

    // Experimental is always allowed
    if proton.is_experimental || name.contains("Experimental") {
        return true;
    }

    // CachyOS is always 10+ based
    if name.contains("CachyOS") || name.contains("cachyos") {
        return true;
    }

    // LegacyRuntime is not a Proton - skip it
    if name == "LegacyRuntime" || name.contains("Runtime") {
        return false;
    }

    // GE-Proton: extract version from "GE-Proton10-27" format
    if name.starts_with("GE-Proton") {
        if let Some(version_part) = name.strip_prefix("GE-Proton") {
            let major: Option<u32> = version_part
                .split('-')
                .next()
                .and_then(|s| s.parse().ok());
            return major.map(|v| v >= 10).unwrap_or(false);
        }
    }

    // Steam Proton: extract version from "Proton 10.0" or "Proton 9.0" format
    if name.starts_with("Proton ") {
        if let Some(version_part) = name.strip_prefix("Proton ") {
            let major: Option<u32> = version_part
                .split('.')
                .next()
                .and_then(|s| s.parse().ok());
            return major.map(|v| v >= 10).unwrap_or(false);
        }
    }

    // EM-Proton: "EM-10.0-33" format
    if name.starts_with("EM-") {
        if let Some(version_part) = name.strip_prefix("EM-") {
            let major: Option<u32> = version_part
                .split('.')
                .next()
                .and_then(|s| s.parse().ok());
            return major.map(|v| v >= 10).unwrap_or(false);
        }
    }

    // Proton-GE Latest symlink
    if name == "Proton-GE Latest" {
        return true;
    }

    // Unknown format - allow it (might be a custom build)
    true
}

/// Find Steam's built-in Proton versions
fn find_builtin_protons(steam_path: &std::path::Path) -> Vec<SteamProton> {
    let mut found = Vec::new();
    let common_dir = steam_path.join("steamapps/common");

    let Ok(entries) = fs::read_dir(&common_dir) else {
        return found;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }

        let name = entry.file_name().to_string_lossy().to_string();

        // Check for valid Proton directory (must have 'proton' script)
        if name.starts_with("Proton") && path.join("proton").exists() {
            let is_experimental = name.contains("Experimental");

            // Config name is lowercase with underscores
            let config_name = if is_experimental {
                "proton_experimental".to_string()
            } else {
                let version = name.replace("Proton ", "");
                let major = version.split('.').next().unwrap_or(&version);
                format!("proton_{}", major)
            };

            found.push(SteamProton {
                name: name.clone(),
                config_name,
                path,
                is_steam_proton: true,
                is_experimental,
            });
        }
    }

    found
}

/// Find custom Protons in compatibilitytools.d
fn find_custom_protons(steam_path: &std::path::Path) -> Vec<SteamProton> {
    let mut found = Vec::new();
    let compat_dir = steam_path.join("compatibilitytools.d");

    let Ok(entries) = fs::read_dir(&compat_dir) else {
        return found;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }

        let name = entry.file_name().to_string_lossy().to_string();

        // Check for valid Proton (has proton script or compatibilitytool.vdf)
        let has_proton = path.join("proton").exists();
        let has_vdf = path.join("compatibilitytool.vdf").exists();

        if has_proton || has_vdf {
            found.push(SteamProton {
                name: name.clone(),
                config_name: name.clone(),
                path,
                is_steam_proton: false,
                is_experimental: false,
            });
        }
    }

    found
}

/// Find system-level Protons in /usr/share/steam/compatibilitytools.d/
fn find_system_protons() -> Vec<SteamProton> {
    let mut found = Vec::new();
    let system_compat_dir = PathBuf::from("/usr/share/steam/compatibilitytools.d");

    let Ok(entries) = fs::read_dir(&system_compat_dir) else {
        return found;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }

        let name = entry.file_name().to_string_lossy().to_string();

        // Check for valid Proton
        let has_proton = path.join("proton").exists();
        let has_vdf = path.join("compatibilitytool.vdf").exists();

        if has_proton || has_vdf {
            found.push(SteamProton {
                name: name.clone(),
                config_name: name.clone(),
                path,
                is_steam_proton: false,
                is_experimental: false,
            });
        }
    }

    found
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_protons() {
        let protons = find_steam_protons();
        println!("Found {} compatible Protons:", protons.len());
        for p in &protons {
            println!("  {} ({})", p.name, if p.is_steam_proton { "Steam" } else { "Custom" });
        }
    }

    #[test]
    fn test_proton_version_check() {
        // Test cases for version checking
        let test_proton = |name: &str, expected: bool| {
            let p = SteamProton {
                name: name.to_string(),
                config_name: name.to_string(),
                path: PathBuf::new(),
                is_steam_proton: false,
                is_experimental: name.contains("Experimental"),
            };
            assert_eq!(is_proton_10_or_newer(&p), expected, "Failed for: {}", name);
        };

        test_proton("Proton Experimental", true);
        test_proton("Proton - Experimental", true);
        test_proton("GE-Proton10-29", true);
        test_proton("GE-Proton9-20", false);
        test_proton("Proton 10.0", true);
        test_proton("Proton 9.0", false);
        test_proton("proton-cachyos", true);
        test_proton("LegacyRuntime", false);
    }
}
