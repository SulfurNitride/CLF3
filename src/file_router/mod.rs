//! File router module for determining where mod files should be installed.
#![allow(unused_imports)] // Re-exports for public API
//!
//! This module analyzes file paths from mod archives and determines whether
//! they should be installed to:
//! - The game's Data folder (standard mod content)
//! - The game root directory (script extenders, ENB, etc.)
//! - BepInEx directories (for Unity-based games)

mod mod_types;
mod patterns;

pub use mod_types::ModType;
pub use patterns::*;

use crate::games::GameType;

/// Routes mod files to their correct installation location based on file paths.
pub struct FileRouter {
    game_type: GameType,
}

impl FileRouter {
    /// Creates a new FileRouter for the specified game.
    pub fn new(game_type: GameType) -> Self {
        Self { game_type }
    }

    /// Determines the mod type for a given file path.
    ///
    /// NOTE: This is now mainly used for legacy/fallback routing.
    /// The primary routing decision should use deploy_type from the collection JSON.
    ///
    /// # Arguments
    /// * `file_path` - The relative path of the file within the mod archive
    ///
    /// # Returns
    /// The `ModType` indicating where this file should be installed.
    pub fn route(&self, file_path: &str) -> ModType {
        // Get just the filename for root file checks
        let filename = file_path
            .rsplit(['/', '\\'])
            .next()
            .unwrap_or(file_path);

        // Check for root-level files (specific known injection DLLs/EXEs)
        if self.is_root_file(filename) {
            return ModType::Root;
        }

        // Check for root directory patterns (e.g., BepInEx/, enbseries/)
        if patterns::starts_with_root_dir(file_path) {
            let lower = file_path.to_lowercase();
            // Special case: BepInEx plugins
            if lower.starts_with("bepinex/plugins/") || lower.starts_with("bepinex\\plugins\\") {
                return ModType::BepInExPlugin;
            }
            if lower.starts_with("bepinex/") || lower.starts_with("bepinex\\") {
                return ModType::BepInExRoot;
            }
            return ModType::Root;
        }

        // Check for Data folder content
        if self.is_data_content(file_path) {
            return ModType::Default;
        }

        // Default: treat as data content (goes to mod folder)
        ModType::Default
    }

    /// Returns true if the filename indicates a root-level file.
    ///
    /// Root files are things like:
    /// - Script extender DLLs (dinput8.dll, dxgi.dll)
    /// - Script extender executables (skse64_loader.exe)
    /// - ENB configuration files (enbseries.ini, enblocal.ini)
    pub fn is_root_file(&self, filename: &str) -> bool {
        patterns::is_root_dll(filename)
            || patterns::is_root_exe(filename)
            || patterns::is_root_ini(filename)
    }

    /// Returns true if the path indicates Data folder content.
    ///
    /// Data content includes:
    /// - Plugin files (.esp, .esm, .esl)
    /// - Archive files (.bsa, .ba2)
    /// - Asset directories (textures/, meshes/, etc.)
    pub fn is_data_content(&self, path: &str) -> bool {
        patterns::has_data_extension(path) || patterns::starts_with_data_dir(path)
    }

    /// Analyzes an archive's file list and determines the overall mod type.
    ///
    /// This is useful for mods that contain mixed content (e.g., SKSE which
    /// has both root files and Data/SKSE plugins).
    ///
    /// # Returns
    /// A tuple of (has_root_files, has_data_files)
    pub fn analyze_archive(&self, file_paths: &[&str]) -> (bool, bool) {
        let mut has_root = false;
        let mut has_data = false;

        for path in file_paths {
            match self.route(path) {
                ModType::Root | ModType::BepInExRoot | ModType::BepInExPlugin => {
                    has_root = true;
                }
                ModType::Default => {
                    has_data = true;
                }
            }

            // Early exit if we've found both types
            if has_root && has_data {
                break;
            }
        }

        (has_root, has_data)
    }

    /// Returns the game type this router is configured for.
    pub fn game_type(&self) -> GameType {
        self.game_type
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn router() -> FileRouter {
        FileRouter::new(GameType::SkyrimSE)
    }

    #[test]
    fn test_route_root_dlls() {
        let r = router();
        assert_eq!(r.route("dinput8.dll"), ModType::Root);
        assert_eq!(r.route("dxgi.dll"), ModType::Root);
        assert_eq!(r.route("version.dll"), ModType::Root);
    }

    #[test]
    fn test_route_skse_files() {
        let r = router();
        assert_eq!(r.route("skse64_loader.exe"), ModType::Root);
        assert_eq!(r.route("skse64_1_6_640.dll"), ModType::Root);
        // SKSE plugins go to Data/SKSE/Plugins, which is data content
        assert_eq!(r.route("SKSE/Plugins/SomePlugin.dll"), ModType::Default);
    }

    #[test]
    fn test_route_enb_files() {
        let r = router();
        assert_eq!(r.route("enbseries.ini"), ModType::Root);
        assert_eq!(r.route("enblocal.ini"), ModType::Root);
        assert_eq!(r.route("enbseries/effect.fx"), ModType::Root);
    }

    #[test]
    fn test_route_data_content() {
        let r = router();
        assert_eq!(r.route("plugin.esp"), ModType::Default);
        assert_eq!(r.route("master.esm"), ModType::Default);
        assert_eq!(r.route("textures/diffuse.dds"), ModType::Default);
        assert_eq!(r.route("meshes/armor.nif"), ModType::Default);
        assert_eq!(r.route("Textures/Armor/Steel.dds"), ModType::Default);
    }

    #[test]
    fn test_route_bepinex() {
        let r = router();
        assert_eq!(r.route("BepInEx/core/BepInEx.dll"), ModType::BepInExRoot);
        assert_eq!(r.route("BepInEx/plugins/SomeMod.dll"), ModType::BepInExPlugin);
        assert_eq!(r.route("winhttp.dll"), ModType::Root);
    }

    #[test]
    fn test_analyze_mixed_archive() {
        let r = router();

        // SKSE-style archive with both root and data files
        let skse_files = vec![
            "skse64_loader.exe",
            "skse64_1_6_640.dll",
            "Data/SKSE/Plugins/Plugin.dll",
            "Data/SKSE/Plugins/Plugin.ini",
        ];
        let (has_root, has_data) = r.analyze_archive(&skse_files);
        assert!(has_root);
        assert!(has_data);

        // Pure data mod
        let data_files = vec![
            "textures/diffuse.dds",
            "meshes/armor.nif",
            "plugin.esp",
        ];
        let (has_root, has_data) = r.analyze_archive(&data_files);
        assert!(!has_root);
        assert!(has_data);

        // Pure root mod (ENB preset)
        let enb_files = vec![
            "enbseries.ini",
            "enblocal.ini",
            "enbseries/effect.fx",
        ];
        let (has_root, has_data) = r.analyze_archive(&enb_files);
        assert!(has_root);
        assert!(!has_data);
    }

    #[test]
    fn test_case_insensitive() {
        let r = router();
        assert_eq!(r.route("DINPUT8.DLL"), ModType::Root);
        assert_eq!(r.route("Textures/Normal.dds"), ModType::Default);
        assert_eq!(r.route("MESHES/ARMOR.NIF"), ModType::Default);
    }

    #[test]
    fn test_skse_plugins_stay_in_mod() {
        // SKSE plugins in Data/SKSE/Plugins should stay as mod content
        let r = router();
        assert_eq!(r.route("SKSE/Plugins/plugin.dll"), ModType::Default);
        assert_eq!(r.route("Data/SKSE/Plugins/SomePlugin.dll"), ModType::Default);
    }

    #[test]
    fn test_nested_skse_folder() {
        // SKSE loader/dll in nested folder should be detected as root
        // (though actual deployment decision comes from deploy_type)
        let r = router();
        assert_eq!(r.route("skse64_2_02_06/skse64_loader.exe"), ModType::Root);
        assert_eq!(r.route("skse64_2_02_06/skse64_1_6_1170.dll"), ModType::Root);
        // Data inside archive stays as mod content
        assert_eq!(r.route("skse64_2_02_06/Data/Scripts/SKSE.pex"), ModType::Default);
    }

    #[test]
    fn test_default_is_mod_folder() {
        // Unknown files default to mod folder (safe)
        // Actual root deployment is determined by deploy_type from collection
        let r = router();
        assert_eq!(r.route("SomeRandomMod.dll"), ModType::Default);
        assert_eq!(r.route("subfolder/unknown.dll"), ModType::Default);
        assert_eq!(r.route("tools/Pandora.exe"), ModType::Default);
        assert_eq!(r.route("Plugin.ESP"), ModType::Default);
    }
}
