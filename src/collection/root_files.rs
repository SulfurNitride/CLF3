//! Per-game allowlists for files that belong at the **game install root**
//! rather than under `Data/`. Used at extract time to route matching files
//! out of the mod's payload directory and into a sibling `Root/` folder
//! that Fluorine's VFS Root Builder deploys to the game directory before
//! launch.
//!
//! The allowlist mirrors what Amethyst encodes per-game in
//! `Games/<game>/<game>.py`'s `custom_routing_rules`. Mapping is keyed by
//! Nexus domain (e.g. `skyrimspecialedition`) so the same routing logic
//! applies whether the mod arrived via Vortex collection metadata or as a
//! plain archive.
//!
//! # Why filename-based?
//!
//! Vortex's `deploy_type` only fires when a collection author sets it, and
//! many SKSE/ENB packages ship as ordinary mods with no metadata at all.
//! Filename matching catches those — e.g. `skse64_loader.exe` is
//! game-rooted regardless of how it's packaged.

use std::collections::HashMap;
use std::sync::OnceLock;

/// Per-game routing rules. `files` and `folders` match **case-insensitively**
/// against top-level entries in a mod's payload directory; matches are moved
/// to `<mod>/Root/<name>` so Fluorine's Root Builder deploys them to the
/// game install root.
pub struct RootRules {
    /// Exact filename matches (lowercase). Top-level files in the mod
    /// payload that match are routed to game root.
    pub files: &'static [&'static str],
    /// Exact folder name matches (lowercase). Top-level folders in the
    /// payload — moved as a unit, preserving structure.
    pub folders: &'static [&'static str],
    /// Filename suffix matches (e.g. `.fx`, `.ini`). Catches the long tail
    /// of ENB shader files without enumerating every variant.
    pub file_suffixes: &'static [&'static str],
    /// Filename prefix matches (e.g. `enb`, `skse64_`). Catches version-
    /// specific binaries (`skse64_1_6_1170.dll` etc.) without listing each.
    /// Combined with `file_suffixes` via AND when both non-empty: a file
    /// matches when its name has any allowed prefix AND any allowed suffix.
    pub file_prefix_suffix: &'static [(&'static str, &'static [&'static str])],
}

impl RootRules {
    /// True when `filename` (any case) belongs at game root under this game's
    /// rules.
    pub fn matches_file(&self, filename: &str) -> bool {
        let lower = filename.to_lowercase();
        if self.files.iter().any(|f| *f == lower) {
            return true;
        }
        if self.file_suffixes.iter().any(|s| lower.ends_with(s)) {
            // Pure suffix-only match — used for blanket extensions like
            // `.fx` where the filename body is irrelevant.
            return true;
        }
        for (prefix, suffixes) in self.file_prefix_suffix {
            if !lower.starts_with(prefix) {
                continue;
            }
            if suffixes.iter().any(|s| lower.ends_with(s)) {
                return true;
            }
        }
        false
    }

    /// True when a top-level folder named `folder_name` (any case) routes
    /// to game root as a unit.
    pub fn matches_folder(&self, folder_name: &str) -> bool {
        let lower = folder_name.to_lowercase();
        self.folders.iter().any(|f| *f == lower)
    }
}

/// Skyrim Special Edition / Anniversary Edition root files.
///
/// Mirrors `Amethyst-Mod-Manager/src/Games/Skyrim Special Edition/skyrim_se.py`'s
/// `custom_routing_rules`, expanded with prefix/suffix matchers so we don't
/// have to enumerate every SKSE point release.
const SKYRIM_SE: RootRules = RootRules {
    files: &[
        // DirectX hooks
        "d3dx9_42.dll",
        "d3d11.dll",
        "d3dcompiler_47.dll",
        "d3dcompiler_46e.dll",
        // ENB stub files (the .fx/.ini suffix matchers cover the long tail)
        "enbpalette.bmp",
        "enbraindrops.dds",
        "enbsunsprite.bmp",
        "enbsunsprite.fx",
        "enbunderwaternoise.bmp",
        // Engine Fixes preloader
        "engine_fixes.toml",
        // Creation Club content list (game-root)
        "skyrim.ccc",
        // .NET Script Framework
        "nvse_loader.exe", // (FNV — harmless to list here too)
    ],
    folders: &[
        "enbseries",
        // Reshade preset folders frequently land at game root
        "reshade-shaders",
        "reshade-presets",
    ],
    file_suffixes: &[],
    file_prefix_suffix: &[
        // SKSE versioned binaries: skse64_1_6_1170.dll, skse64_loader.exe, etc.
        ("skse64_", &[".dll", ".exe", ".log"]),
        // ENB shader / config files
        ("enb", &[".fx", ".ini", ".bmp", ".dds", ".tga"]),
    ],
};

/// Returns the per-game root routing rules for a Nexus game domain. `None`
/// when no rules are registered (so the caller falls back to the existing
/// `is_root_mod()` collection-metadata check).
pub fn rules_for(game_domain: &str) -> Option<&'static RootRules> {
    static TABLE: OnceLock<HashMap<&'static str, &'static RootRules>> = OnceLock::new();
    let table = TABLE.get_or_init(|| {
        let mut m: HashMap<&'static str, &'static RootRules> = HashMap::new();
        m.insert("skyrimspecialedition", &SKYRIM_SE);
        m
    });
    table.get(game_domain.to_lowercase().as_str()).copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skse_loader_matches() {
        let r = rules_for("skyrimspecialedition").unwrap();
        assert!(r.matches_file("skse64_loader.exe"));
        assert!(r.matches_file("SKSE64_LOADER.EXE"));
        assert!(r.matches_file("skse64_1_6_1170.dll"));
        assert!(r.matches_file("skse64_1_5_97.dll"));
    }

    #[test]
    fn enb_files_match() {
        let r = rules_for("skyrimspecialedition").unwrap();
        assert!(r.matches_file("enbeffect.fx"));
        assert!(r.matches_file("enbseries.ini"));
        assert!(r.matches_file("enblocal.ini"));
        assert!(r.matches_file("enbpalette.bmp"));
        assert!(r.matches_file("enbraindrops.dds"));
    }

    #[test]
    fn d3d_hooks_match() {
        let r = rules_for("skyrimspecialedition").unwrap();
        assert!(r.matches_file("d3d11.dll"));
        assert!(r.matches_file("d3dx9_42.dll"));
        assert!(r.matches_file("d3dcompiler_47.dll"));
    }

    #[test]
    fn folder_matches() {
        let r = rules_for("skyrimspecialedition").unwrap();
        assert!(r.matches_folder("enbseries"));
        assert!(r.matches_folder("ENBSeries"));
        assert!(r.matches_folder("reshade-shaders"));
        assert!(!r.matches_folder("meshes"));
        assert!(!r.matches_folder("scripts"));
    }

    #[test]
    fn unrelated_files_dont_match() {
        let r = rules_for("skyrimspecialedition").unwrap();
        assert!(!r.matches_file("plugin.esp"));
        assert!(!r.matches_file("textures.bsa"));
        assert!(!r.matches_file("readme.txt"));
        // SKSE plugin DLLs (Data-side, not root): caller checks they're
        // under `SKSE/Plugins/` first; matches_file alone wouldn't differ.
    }

    #[test]
    fn unsupported_game_returns_none() {
        assert!(rules_for("morrowind").is_none());
        assert!(rules_for("").is_none());
    }
}
