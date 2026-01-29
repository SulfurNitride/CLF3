//! ModOrganizer.ini generation for portable instances.

use crate::games::GameType;
use std::io::Write;
use std::path::Path;

/// Configuration for generating ModOrganizer.ini.
#[derive(Debug, Clone)]
pub struct IniConfig {
    /// Game type being configured.
    pub game_type: GameType,
    /// Path to the Stock Game folder (game files copy).
    pub stock_game_path: String,
    /// Profile name to use.
    pub profile_name: String,
    /// MO2 version string.
    pub version: String,
}

impl Default for IniConfig {
    fn default() -> Self {
        Self {
            game_type: GameType::SkyrimSE,
            stock_game_path: String::new(),
            profile_name: "Default".to_string(),
            version: "2.5.2".to_string(),
        }
    }
}

/// Converts a Linux path to Wine Z: drive format with escaped backslashes.
///
/// Example: `/home/user/games` -> `Z:\\home\\user\\games`
/// The double backslashes are required for Qt's INI parser.
pub fn to_wine_path(linux_path: &str) -> String {
    if linux_path.starts_with('/') {
        format!("Z:{}", linux_path.replace('/', "\\\\"))
    } else {
        linux_path.replace('/', "\\\\")
    }
}

/// Converts a path to Wine format with forward slashes (for some INI fields).
///
/// Example: `/home/user/games` -> `Z:/home/user/games`
pub fn to_wine_path_forward(linux_path: &str) -> String {
    if linux_path.starts_with('/') {
        format!("Z:{}", linux_path)
    } else {
        linux_path.to_string()
    }
}

/// Wraps a string in Qt's @ByteArray() format.
fn qt_byte_array(s: &str) -> String {
    format!("@ByteArray({})", s)
}

/// Game executable information for the customExecutables section.
#[derive(Debug, Clone)]
pub struct GameExecutable {
    pub title: String,
    pub binary: String,
    pub working_directory: String,
    pub arguments: String,
    pub steam_app_id: String,
}

/// Returns the default executables for a game type.
fn get_game_executables(game_type: GameType, stock_game_path: &str) -> Vec<GameExecutable> {
    let wine_path = to_wine_path_forward(stock_game_path);

    match game_type {
        GameType::SkyrimSE => vec![
            GameExecutable {
                title: "Skyrim Special Edition".to_string(),
                binary: format!("{}/SkyrimSE.exe", wine_path),
                working_directory: wine_path.clone(),
                arguments: String::new(),
                steam_app_id: String::new(),
            },
            GameExecutable {
                title: "Skyrim Special Edition Launcher".to_string(),
                binary: format!("{}/SkyrimSELauncher.exe", wine_path),
                working_directory: wine_path,
                arguments: String::new(),
                steam_app_id: String::new(),
            },
        ],
    }
}

/// Generates a ModOrganizer.ini file for a portable instance.
pub fn generate_ini(config: &IniConfig, output_path: &Path) -> std::io::Result<()> {
    let mut file = std::fs::File::create(output_path)?;

    // [General] section
    writeln!(file, "[General]")?;
    writeln!(file, "gameName={}", config.game_type.name())?;
    writeln!(
        file,
        "selected_profile={}",
        qt_byte_array(&config.profile_name)
    )?;
    writeln!(
        file,
        "gamePath={}",
        qt_byte_array(&to_wine_path(&config.stock_game_path))
    )?;
    writeln!(file, "game_edition=Steam")?;
    writeln!(file, "version={}", config.version)?;
    writeln!(file, "first_start=false")?;
    writeln!(file)?;

    // [customExecutables] section
    let executables = get_game_executables(config.game_type, &config.stock_game_path);
    writeln!(file, "[customExecutables]")?;
    writeln!(file, "size={}", executables.len())?;

    for (i, exe) in executables.iter().enumerate() {
        let idx = i + 1;
        writeln!(file, "{}\\arguments={}", idx, exe.arguments)?;
        writeln!(file, "{}\\binary={}", idx, exe.binary)?;
        writeln!(file, "{}\\hide=false", idx)?;
        writeln!(file, "{}\\ownicon=true", idx)?;
        writeln!(file, "{}\\steamAppID={}", idx, exe.steam_app_id)?;
        writeln!(file, "{}\\title={}", idx, exe.title)?;
        writeln!(file, "{}\\toolbar=false", idx)?;
        writeln!(file, "{}\\workingDirectory={}", idx, exe.working_directory)?;
    }
    writeln!(file)?;

    // [Settings] section
    writeln!(file, "[Settings]")?;
    writeln!(file, "profile_local_inis=true")?;
    writeln!(file, "profile_local_saves=false")?;
    writeln!(file, "profile_archive_invalidation=true")?;
    writeln!(file, "language=en")?;
    writeln!(file, "check_for_updates=false")?;
    writeln!(file, "use_prereleases=false")?;
    writeln!(file, "offline_mode=false")?;
    writeln!(file, "lock_gui=true")?;
    writeln!(file, "force_enable_core_files=true")?;
    writeln!(file, "log_level=1")?;
    writeln!(file, "crash_dumps_type=1")?;
    writeln!(file, "crash_dumps_max=5")?;
    writeln!(file)?;

    // [Plugins] section - minimal defaults
    writeln!(file, "[Plugins]")?;
    writeln!(file, "Fomod%20Installer\\prefer=true")?;
    writeln!(file)?;

    // [pluginBlacklist] section
    writeln!(file, "[pluginBlacklist]")?;
    writeln!(file, "size=0")?;

    Ok(())
}

/// Creates a default profile directory with required files.
pub fn create_profile(
    profiles_dir: &Path,
    profile_name: &str,
    game_type: GameType,
) -> std::io::Result<()> {
    let profile_dir = profiles_dir.join(profile_name);
    std::fs::create_dir_all(&profile_dir)?;

    // Create empty modlist.txt
    let modlist_path = profile_dir.join("modlist.txt");
    std::fs::write(&modlist_path, "# This file was automatically generated by CLF3.\n")?;

    // Create empty plugins.txt with game master files
    let plugins_path = profile_dir.join("plugins.txt");
    let plugins_content = match game_type {
        GameType::SkyrimSE => {
            "# This file was automatically generated by CLF3.\n\
             *Skyrim.esm\n\
             *Update.esm\n\
             *Dawnguard.esm\n\
             *HearthFires.esm\n\
             *Dragonborn.esm\n"
        }
    };
    std::fs::write(&plugins_path, plugins_content)?;

    // Create empty loadorder.txt
    let loadorder_path = profile_dir.join("loadorder.txt");
    let loadorder_content = match game_type {
        GameType::SkyrimSE => {
            "Skyrim.esm\n\
             Update.esm\n\
             Dawnguard.esm\n\
             HearthFires.esm\n\
             Dragonborn.esm\n"
        }
    };
    std::fs::write(&loadorder_path, loadorder_content)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_wine_path() {
        // Double backslashes are required for Qt's INI parser
        assert_eq!(
            to_wine_path("/home/user/games"),
            "Z:\\\\home\\\\user\\\\games"
        );
        assert_eq!(
            to_wine_path("/home/luke/.steam/steam/steamapps/common/Skyrim Special Edition"),
            "Z:\\\\home\\\\luke\\\\.steam\\\\steam\\\\steamapps\\\\common\\\\Skyrim Special Edition"
        );
    }

    #[test]
    fn test_to_wine_path_forward() {
        assert_eq!(
            to_wine_path_forward("/home/user/games"),
            "Z:/home/user/games"
        );
    }

    #[test]
    fn test_qt_byte_array() {
        assert_eq!(qt_byte_array("Default"), "@ByteArray(Default)");
        assert_eq!(
            qt_byte_array("Z:\\home\\user"),
            "@ByteArray(Z:\\home\\user)"
        );
    }

    #[test]
    fn test_get_game_executables() {
        let exes = get_game_executables(
            GameType::SkyrimSE,
            "/home/user/mo2/Stock Game",
        );
        assert_eq!(exes.len(), 2);
        assert_eq!(exes[0].title, "Skyrim Special Edition");
        assert!(exes[0].binary.contains("SkyrimSE.exe"));
        assert_eq!(exes[1].title, "Skyrim Special Edition Launcher");
    }
}
