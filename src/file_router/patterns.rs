//! File detection patterns for routing.

/// DLL files that should be installed to the game root directory.
/// These are typically script extenders, ENB injectors, or engine fixes.
pub const ROOT_DLLS: &[&str] = &[
    // Script extender injection points
    "dinput8.dll",
    "dxgi.dll",
    "d3d11.dll",
    "d3d9.dll",
    "d3d10.dll",
    // DirectX hooks (ENB, ReShade, Engine Fixes preloader)
    "d3dx9_42.dll",  // Engine Fixes SKSE64 Preloader
    "d3dx9_43.dll",
    // Video/audio hooks
    "binkw64.dll",
    "bink2w64.dll",
    "binkw32.dll",
    // Windows API hooks
    "version.dll",
    "winmm.dll",
    "winhttp.dll",
    // BepInEx
    "doorstop_config.ini",
];

/// Executable patterns that indicate root installation.
/// Uses prefix matching (e.g., "skse64_" matches "skse64_loader.exe").
pub const ROOT_EXE_PREFIXES: &[&str] = &[
    // SKSE (Skyrim Script Extender)
    "skse64_",
    "skse_",
    // F4SE (Fallout 4 Script Extender)
    "f4se_",
    // OBSE (Oblivion Script Extender)
    "obse_",
    // NVSE (New Vegas Script Extender)
    "nvse_",
    // SFSE (Starfield Script Extender)
    "sfse_",
];

/// INI files that belong in game root.
pub const ROOT_INI_FILES: &[&str] = &[
    "enbseries.ini",
    "enblocal.ini",
    "enbconvertor.ini",
    "d3dx.ini",
    "reshade.ini",
    "dxvk.conf",
];

/// Directory names that belong in game root.
pub const ROOT_DIRECTORIES: &[&str] = &[
    "bepinex",   // BepInEx framework folder
    "enbseries", // ENB shader files
    "reshade-shaders",
];

/// File extensions that indicate Data folder content (plugins, archives).
pub const DATA_EXTENSIONS: &[&str] = &[
    ".esp", // Elder Scrolls Plugin
    ".esm", // Elder Scrolls Master
    ".esl", // Elder Scrolls Light
    ".bsa", // Bethesda Softworks Archive (TES3-5, FO3/NV)
    ".ba2", // Bethesda Archive 2 (FO4, FO76, Starfield)
];

/// Directory names that indicate Data folder content.
/// These are case-insensitive in practice.
pub const DATA_DIRECTORIES: &[&str] = &[
    // Bethesda standard directories
    "textures",
    "meshes",
    "music",
    "sound",
    "shaders",
    "video",
    "interface",
    "fonts",
    "scripts",
    "facegen",
    "menus",
    "lodsettings",
    "strings",
    "trees",
    "seq",
    "grass",
    "terrain",
    "lod",
    "vis",
    "materials",
    "geometries",
    "planetdata",
    "particles",
    "distantlod",
    "facegendata",
    "dlclist",
    "calientetools", // BodySlide
    "nemesis_engine",
    "netscriptframework",
    "skse",       // SKSE plugins folder (goes in Data)
    "f4se",       // F4SE plugins folder (goes in Data)
    "sfse",       // SFSE plugins folder (goes in Data)
    "source",     // Papyrus source
    "pex",        // Compiled Papyrus
    "platform",   // Platform-specific assets
    "programs",   // Shader programs
    "share",      // Shared assets
    "actors",     // Actor assets
];

/// Returns true if the filename (lowercase) is a root DLL.
pub fn is_root_dll(filename: &str) -> bool {
    let lower = filename.to_lowercase();
    ROOT_DLLS.iter().any(|&dll| lower == dll)
}

/// Returns true if the filename (lowercase) matches a root executable pattern.
pub fn is_root_exe(filename: &str) -> bool {
    let lower = filename.to_lowercase();
    if !lower.ends_with(".exe") && !lower.ends_with(".dll") {
        return false;
    }
    ROOT_EXE_PREFIXES.iter().any(|&prefix| lower.starts_with(prefix))
}

/// Returns true if the filename (lowercase) is a root INI file.
pub fn is_root_ini(filename: &str) -> bool {
    let lower = filename.to_lowercase();
    ROOT_INI_FILES.iter().any(|&ini| lower == ini)
}

/// Returns true if the path's first directory component is a root directory.
pub fn starts_with_root_dir(path: &str) -> bool {
    let lower = path.to_lowercase();
    let first_component = lower.split(['/', '\\']).next().unwrap_or("");
    ROOT_DIRECTORIES.contains(&first_component)
}

/// Returns true if the path ends with a Data folder extension.
pub fn has_data_extension(path: &str) -> bool {
    let lower = path.to_lowercase();
    DATA_EXTENSIONS.iter().any(|&ext| lower.ends_with(ext))
}

/// Returns true if the path's first directory component is a Data directory.
pub fn starts_with_data_dir(path: &str) -> bool {
    let lower = path.to_lowercase();
    let first_component = lower.split(['/', '\\']).next().unwrap_or("");
    DATA_DIRECTORIES.contains(&first_component)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_root_dll() {
        assert!(is_root_dll("dinput8.dll"));
        assert!(is_root_dll("DINPUT8.DLL"));
        assert!(is_root_dll("dxgi.dll"));
        assert!(is_root_dll("version.dll"));
        assert!(!is_root_dll("random.dll"));
        assert!(!is_root_dll("textures/normal.dds"));
    }

    #[test]
    fn test_is_root_exe() {
        assert!(is_root_exe("skse64_loader.exe"));
        assert!(is_root_exe("SKSE64_LOADER.EXE"));
        assert!(is_root_exe("skse64_1_6_640.dll"));
        assert!(is_root_exe("f4se_loader.exe"));
        assert!(!is_root_exe("SkyrimSE.exe"));
        assert!(!is_root_exe("random.exe"));
    }

    #[test]
    fn test_is_root_ini() {
        assert!(is_root_ini("enbseries.ini"));
        assert!(is_root_ini("ENBSeries.ini"));
        assert!(is_root_ini("enblocal.ini"));
        assert!(!is_root_ini("skyrim.ini"));
        assert!(!is_root_ini("random.ini"));
    }

    #[test]
    fn test_starts_with_root_dir() {
        assert!(starts_with_root_dir("BepInEx/plugins/mod.dll"));
        assert!(starts_with_root_dir("enbseries/effect.fx"));
        assert!(!starts_with_root_dir("textures/diffuse.dds"));
        assert!(!starts_with_root_dir("meshes/armor.nif"));
    }

    #[test]
    fn test_has_data_extension() {
        assert!(has_data_extension("plugin.esp"));
        assert!(has_data_extension("master.esm"));
        assert!(has_data_extension("archive.bsa"));
        assert!(has_data_extension("archive.ba2"));
        assert!(has_data_extension("PLUGIN.ESP"));
        assert!(!has_data_extension("dinput8.dll"));
        assert!(!has_data_extension("config.ini"));
    }

    #[test]
    fn test_starts_with_data_dir() {
        assert!(starts_with_data_dir("textures/diffuse.dds"));
        assert!(starts_with_data_dir("Meshes/armor.nif"));
        assert!(starts_with_data_dir("SKSE/Plugins/mod.dll"));
        assert!(starts_with_data_dir("scripts/main.pex"));
        assert!(!starts_with_data_dir("BepInEx/plugins/mod.dll"));
        assert!(!starts_with_data_dir("dinput8.dll"));
    }
}
