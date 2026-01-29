//! Mod type definitions for file routing.

/// Determines where mod files should be installed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ModType {
    /// Standard data folder content (meshes, textures, plugins, etc.)
    /// Goes to: mods/<mod_name>/ (which maps to Data/)
    Default,

    /// Root-level files that go in the game directory
    /// Goes to: Stock Game/ (game root)
    /// Examples: dinput8.dll, skse64_loader.exe, ENB files
    Root,

    /// BepInEx root files (framework itself)
    /// Goes to: Stock Game/ (game root)
    /// Examples: BepInEx/, winhttp.dll, doorstop_config.ini
    BepInExRoot,

    /// BepInEx plugins
    /// Goes to: Stock Game/BepInEx/plugins/
    BepInExPlugin,
}

impl ModType {
    /// Returns the display name for this mod type.
    pub fn as_str(&self) -> &'static str {
        match self {
            ModType::Default => "Data",
            ModType::Root => "Root",
            ModType::BepInExRoot => "BepInEx Root",
            ModType::BepInExPlugin => "BepInEx Plugin",
        }
    }

    /// Returns true if this mod type installs to the game root.
    pub fn is_root(&self) -> bool {
        matches!(self, ModType::Root | ModType::BepInExRoot | ModType::BepInExPlugin)
    }
}

impl std::fmt::Display for ModType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mod_type_display() {
        assert_eq!(ModType::Default.to_string(), "Data");
        assert_eq!(ModType::Root.to_string(), "Root");
        assert_eq!(ModType::BepInExRoot.to_string(), "BepInEx Root");
        assert_eq!(ModType::BepInExPlugin.to_string(), "BepInEx Plugin");
    }

    #[test]
    fn test_is_root() {
        assert!(!ModType::Default.is_root());
        assert!(ModType::Root.is_root());
        assert!(ModType::BepInExRoot.is_root());
        assert!(ModType::BepInExPlugin.is_root());
    }
}
