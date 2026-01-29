//! Game type definitions.
//!
//! This module defines supported games and their properties.
//!
//! ## Future: GameFinder Integration
//!
//! For cross-platform game detection (Windows + Linux), we plan to integrate
//! [GameFinder](https://github.com/erri120/GameFinder) by erri120 (Wabbajack creator).
//! GameFinder supports Steam, GOG, Epic, Origin, EA Desktop, Xbox Game Pass,
//! and handles Wine/Proton path remapping on Linux.
//!
//! Integration approach TBD - likely via subprocess with a small .NET AOT-compiled CLI
//! that outputs JSON, which Rust parses.

use std::path::PathBuf;

/// Supported game types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GameType {
    /// The Elder Scrolls V: Skyrim Special Edition
    SkyrimSE,
    // Future games:
    // Fallout4,
    // SkyrimVR,
    // Fallout4VR,
    // Starfield,
}

impl GameType {
    /// Returns the display name for this game.
    pub fn name(&self) -> &'static str {
        match self {
            GameType::SkyrimSE => "Skyrim Special Edition",
        }
    }

    /// Returns the Steam App ID for this game.
    pub fn steam_app_id(&self) -> u32 {
        match self {
            GameType::SkyrimSE => 489830,
        }
    }

    /// Returns the Nexus Mods domain name for this game.
    pub fn nexus_domain(&self) -> &'static str {
        match self {
            GameType::SkyrimSE => "skyrimspecialedition",
        }
    }

    /// Returns the data folder name for this game.
    pub fn data_folder(&self) -> &'static str {
        match self {
            GameType::SkyrimSE => "Data",
        }
    }

    /// Returns the main executable name for this game.
    pub fn executable(&self) -> &'static str {
        match self {
            GameType::SkyrimSE => "SkyrimSE.exe",
        }
    }

    /// Parses a game type from a Nexus domain name.
    pub fn from_nexus_domain(domain: &str) -> Option<Self> {
        match domain.to_lowercase().as_str() {
            "skyrimspecialedition" => Some(GameType::SkyrimSE),
            _ => None,
        }
    }
}

impl std::fmt::Display for GameType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// Validates that a game installation directory looks correct.
pub fn validate_game_path(game_type: GameType, path: &PathBuf) -> anyhow::Result<()> {
    if !path.exists() {
        anyhow::bail!("Game path does not exist: {}", path.display());
    }

    let exe_path = path.join(game_type.executable());
    if !exe_path.exists() {
        anyhow::bail!(
            "{} installation appears incomplete: {} not found at {}",
            game_type.name(),
            game_type.executable(),
            path.display()
        );
    }

    let data_path = path.join(game_type.data_folder());
    if !data_path.exists() {
        anyhow::bail!(
            "{} installation appears incomplete: {} folder not found",
            game_type.name(),
            game_type.data_folder()
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_game_type_properties() {
        let skyrim = GameType::SkyrimSE;
        assert_eq!(skyrim.name(), "Skyrim Special Edition");
        assert_eq!(skyrim.steam_app_id(), 489830);
        assert_eq!(skyrim.nexus_domain(), "skyrimspecialedition");
        assert_eq!(skyrim.data_folder(), "Data");
        assert_eq!(skyrim.executable(), "SkyrimSE.exe");
    }

    #[test]
    fn test_game_type_display() {
        assert_eq!(GameType::SkyrimSE.to_string(), "Skyrim Special Edition");
    }

    #[test]
    fn test_from_nexus_domain() {
        assert_eq!(
            GameType::from_nexus_domain("skyrimspecialedition"),
            Some(GameType::SkyrimSE)
        );
        assert_eq!(
            GameType::from_nexus_domain("SKYRIMSPECIALEDITION"),
            Some(GameType::SkyrimSE)
        );
        assert_eq!(GameType::from_nexus_domain("fallout4"), None);
    }
}
