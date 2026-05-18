//! Known games configuration
//!
//! Contains metadata for games that CLF3 supports, including:
//! - Steam App ID
//! - My Games folder name (Documents/My Games/*)
//! - AppData/Local folder name
//! - Registry path for game detection

/// Configuration for a known game
#[derive(Debug, Clone)]
pub struct KnownGame {
    /// Display name
    pub name: &'static str,
    /// Steam App ID
    pub steam_app_id: &'static str,
    /// GOG App ID (if available)
    pub gog_app_id: Option<&'static str>,
    /// Folder name in Documents/My Games (if applicable)
    pub my_games_folder: Option<&'static str>,
    /// Folder name in AppData/Local (if applicable)
    pub appdata_local_folder: Option<&'static str>,
    /// Folder name in AppData/Roaming (if applicable)
    pub appdata_roaming_folder: Option<&'static str>,
    /// Registry path under HKLM\Software\ (for game detection)
    pub registry_path: &'static str,
    /// Registry value name for install path
    pub registry_value: &'static str,
    /// Expected folder name in steamapps/common/
    pub steam_folder: &'static str,
    /// Wabbajack `GameType` string from modlist JSON (e.g. "FalloutNewVegas").
    /// None for store variants that don't have their own Wabbajack enum entry.
    pub wabbajack_type: Option<&'static str>,
}

/// All known games that CLF3 supports
pub const KNOWN_GAMES: &[KnownGame] = &[
    // Bethesda Games
    KnownGame {
        name: "Enderal",
        steam_app_id: "933480",
        gog_app_id: None,
        my_games_folder: Some("Enderal"),
        appdata_local_folder: None,
        appdata_roaming_folder: None,
        registry_path: r"Software\SureAI\Enderal",
        registry_value: "Install_Path",
        steam_folder: "Enderal",
        wabbajack_type: Some("Enderal"),
    },
    KnownGame {
        name: "Enderal Special Edition",
        steam_app_id: "976620",
        gog_app_id: None,
        my_games_folder: Some("Enderal Special Edition"),
        appdata_local_folder: None,
        appdata_roaming_folder: None,
        registry_path: r"Software\SureAI\Enderal SE",
        registry_value: "installed path",
        steam_folder: "Enderal Special Edition",
        wabbajack_type: Some("EnderalSpecialEdition"),
    },
    KnownGame {
        name: "Fallout 3",
        steam_app_id: "22300",
        gog_app_id: Some("1454315831"), // Fallout 3 GOTY
        my_games_folder: Some("Fallout3"),
        appdata_local_folder: Some("Fallout3"),
        appdata_roaming_folder: None,
        registry_path: r"Software\Bethesda Softworks\Fallout3",
        registry_value: "Installed Path",
        steam_folder: "Fallout 3",
        wabbajack_type: Some("Fallout3"),
    },
    KnownGame {
        name: "Fallout 3 GOTY",
        steam_app_id: "22370",
        gog_app_id: None,
        my_games_folder: Some("Fallout3"),
        appdata_local_folder: Some("Fallout3"),
        appdata_roaming_folder: None,
        registry_path: r"Software\Bethesda Softworks\Fallout3",
        registry_value: "Installed Path",
        steam_folder: "Fallout 3 goty",
        wabbajack_type: None, // store variant — Wabbajack treats as Fallout3
    },
    KnownGame {
        name: "Fallout 4",
        steam_app_id: "377160",
        gog_app_id: None,
        my_games_folder: Some("Fallout4"),
        appdata_local_folder: Some("Fallout4"),
        appdata_roaming_folder: None,
        registry_path: r"Software\Bethesda Softworks\Fallout4",
        registry_value: "Installed Path",
        steam_folder: "Fallout 4",
        wabbajack_type: Some("Fallout4"),
    },
    KnownGame {
        name: "Fallout 4 VR",
        steam_app_id: "611660",
        gog_app_id: None,
        my_games_folder: Some("Fallout4VR"),
        appdata_local_folder: None,
        appdata_roaming_folder: None,
        registry_path: r"Software\Bethesda Softworks\Fallout 4 VR",
        registry_value: "Installed Path",
        steam_folder: "Fallout 4 VR",
        wabbajack_type: Some("Fallout4VR"),
    },
    KnownGame {
        name: "Fallout New Vegas",
        steam_app_id: "22380",
        gog_app_id: Some("1454587428"), // Fallout NV Ultimate
        my_games_folder: Some("FalloutNV"),
        appdata_local_folder: Some("FalloutNV"),
        appdata_roaming_folder: None,
        registry_path: r"Software\Bethesda Softworks\FalloutNV",
        registry_value: "Installed Path",
        steam_folder: "Fallout New Vegas",
        wabbajack_type: Some("FalloutNewVegas"),
    },
    KnownGame {
        name: "Morrowind",
        steam_app_id: "22320",
        gog_app_id: Some("1440163901"), // Morrowind GOTY
        my_games_folder: Some("Morrowind"),
        appdata_local_folder: None,
        appdata_roaming_folder: None,
        registry_path: r"Software\Bethesda Softworks\Morrowind",
        registry_value: "Installed Path",
        steam_folder: "Morrowind",
        wabbajack_type: Some("Morrowind"),
    },
    KnownGame {
        name: "Oblivion",
        steam_app_id: "22330",
        gog_app_id: Some("1458058109"), // Oblivion GOTY Deluxe
        my_games_folder: Some("Oblivion"),
        appdata_local_folder: Some("Oblivion"),
        appdata_roaming_folder: None,
        registry_path: r"Software\Bethesda Softworks\Oblivion",
        registry_value: "Installed Path",
        steam_folder: "Oblivion",
        wabbajack_type: Some("Oblivion"),
    },
    KnownGame {
        name: "Skyrim",
        steam_app_id: "72850",
        gog_app_id: None, // Not on GOG
        my_games_folder: Some("Skyrim"),
        appdata_local_folder: Some("Skyrim"),
        appdata_roaming_folder: None,
        registry_path: r"Software\Bethesda Softworks\Skyrim",
        registry_value: "Installed Path",
        steam_folder: "Skyrim",
        wabbajack_type: Some("Skyrim"),
    },
    KnownGame {
        name: "Skyrim Special Edition",
        steam_app_id: "489830",
        gog_app_id: Some("1711230643"), // Skyrim SE Anniversary Edition
        my_games_folder: Some("Skyrim Special Edition"),
        appdata_local_folder: Some("Skyrim Special Edition"),
        appdata_roaming_folder: None,
        registry_path: r"Software\Bethesda Softworks\Skyrim Special Edition",
        registry_value: "Installed Path",
        steam_folder: "Skyrim Special Edition",
        wabbajack_type: Some("SkyrimSpecialEdition"),
    },
    KnownGame {
        name: "Skyrim VR",
        steam_app_id: "611670",
        gog_app_id: None,
        my_games_folder: Some("Skyrim VR"),
        appdata_local_folder: None,
        appdata_roaming_folder: None,
        registry_path: r"Software\Bethesda Softworks\Skyrim VR",
        registry_value: "Installed Path",
        steam_folder: "Skyrim VR",
        wabbajack_type: Some("SkyrimVR"),
    },
    KnownGame {
        name: "Starfield",
        steam_app_id: "1716740",
        gog_app_id: None,
        my_games_folder: Some("Starfield"),
        appdata_local_folder: None,
        appdata_roaming_folder: None,
        registry_path: r"Software\Bethesda Softworks\Starfield",
        registry_value: "Installed Path",
        steam_folder: "Starfield",
        wabbajack_type: Some("Starfield"),
    },
    // CD Projekt RED Games
    KnownGame {
        name: "The Witcher 3",
        steam_app_id: "292030",
        gog_app_id: Some("1495134320"), // Witcher 3 GOTY
        my_games_folder: Some("The Witcher 3"),
        appdata_local_folder: None,
        appdata_roaming_folder: None,
        registry_path: r"Software\CD Projekt Red\The Witcher 3",
        registry_value: "InstallFolder",
        steam_folder: "The Witcher 3 Wild Hunt",
        wabbajack_type: Some("Witcher3"),
    },
    KnownGame {
        name: "Cyberpunk 2077",
        steam_app_id: "1091500",
        gog_app_id: Some("1423049311"),
        my_games_folder: None,
        appdata_local_folder: Some("CD Projekt Red/Cyberpunk 2077"),
        appdata_roaming_folder: None,
        registry_path: r"Software\CD Projekt Red\Cyberpunk 2077",
        registry_value: "InstallFolder",
        steam_folder: "Cyberpunk 2077",
        wabbajack_type: Some("Cyberpunk2077"),
    },
    // Other popular moddable games
    KnownGame {
        name: "Baldur's Gate 3",
        steam_app_id: "1086940",
        gog_app_id: Some("1456460669"),
        my_games_folder: None,
        appdata_local_folder: Some("Larian Studios/Baldur's Gate 3"),
        appdata_roaming_folder: None,
        registry_path: r"Software\Larian Studios\Baldur's Gate 3",
        registry_value: "InstallDir",
        steam_folder: "Baldurs Gate 3",
        wabbajack_type: Some("BaldursGate3"),
    },
    // Square Enix Games
    KnownGame {
        name: "NieR: Automata",
        steam_app_id: "524220",
        gog_app_id: None,
        my_games_folder: None,
        appdata_local_folder: None,
        appdata_roaming_folder: None,
        registry_path: r"Software\Square Enix\NieR:Automata",
        registry_value: "Install_Dir",
        steam_folder: "NieRAutomata",
        wabbajack_type: Some("NieRAutomata"),
    },
];

/// Find a known game by Steam App ID
pub fn find_by_steam_id(app_id: &str) -> Option<&'static KnownGame> {
    KNOWN_GAMES.iter().find(|g| g.steam_app_id == app_id)
}

/// Find a known game by GOG App ID
pub fn find_by_gog_id(app_id: &str) -> Option<&'static KnownGame> {
    KNOWN_GAMES.iter().find(|g| g.gog_app_id == Some(app_id))
}

/// Find a known game by name (case-insensitive)
pub fn find_by_name(name: &str) -> Option<&'static KnownGame> {
    let name_lower = name.to_lowercase();
    KNOWN_GAMES
        .iter()
        .find(|g| g.name.to_lowercase() == name_lower)
}

/// Find a known game by its Wabbajack `GameType` string (e.g. "FalloutNewVegas",
/// "SkyrimSpecialEdition"). Accepts common aliases Wabbajack has used across versions.
pub fn find_by_wabbajack_type(wj_type: &str) -> Option<&'static KnownGame> {
    // Direct match against the canonical wabbajack_type field
    if let Some(g) = KNOWN_GAMES
        .iter()
        .find(|g| g.wabbajack_type == Some(wj_type))
    {
        return Some(g);
    }

    // Alias fallback — older modlists / short-form names
    let aliased = match wj_type {
        "SkyrimSE" => "SkyrimSpecialEdition",
        "FalloutNV" => "FalloutNewVegas",
        "EnderalSE" => "EnderalSpecialEdition",
        "TheWitcher3" => "Witcher3",
        _ => return None,
    };
    KNOWN_GAMES
        .iter()
        .find(|g| g.wabbajack_type == Some(aliased))
}

/// Convenience: return `(steam_app_id, gog_app_id)` for a Wabbajack game_type.
/// GOG id is `None` for games without a known GOG variant.
///
/// Returns only the *canonical* entry; for installs that come in store
/// variants (Fallout 3 vs. Fallout 3 GOTY both shipped on Steam under
/// different app IDs), use [`variants_for_wabbajack_type`] which returns
/// every install candidate.
pub fn ids_for_wabbajack_type(wj_type: &str) -> Option<(&'static str, Option<&'static str>)> {
    let g = find_by_wabbajack_type(wj_type)?;
    Some((g.steam_app_id, g.gog_app_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fallout3_resolves_to_both_steam_variants() {
        let v = variants_for_wabbajack_type("Fallout3");
        let ids: Vec<&str> = v.iter().map(|g| g.steam_app_id).collect();
        assert!(
            ids.contains(&"22300") && ids.contains(&"22370"),
            "expected Fallout3 lookup to include both 22300 and 22370; got {:?}",
            ids
        );
        // Canonical entry first.
        assert_eq!(v[0].steam_app_id, "22300");
    }

    #[test]
    fn variants_handles_aliases() {
        let v = variants_for_wabbajack_type("SkyrimSE");
        assert!(!v.is_empty(), "alias 'SkyrimSE' should resolve");
        assert_eq!(v[0].steam_app_id, "489830");
    }

    #[test]
    fn unknown_type_returns_empty() {
        assert!(variants_for_wabbajack_type("NotAGame").is_empty());
    }
}

/// Return every known `KnownGame` entry that maps to the same Wabbajack
/// `game_type`, ordered with the canonical entry first.
///
/// A store variant is recognised as `wabbajack_type: None` (i.e. Wabbajack
/// doesn't have its own enum for it) plus a shared `registry_path` with the
/// canonical entry — Bethesda variants use the same registry key regardless
/// of the Steam app ID, so this catches Fallout 3 GOTY (22370) when the
/// modlist asks for "Fallout3" (22300) and vice-versa.
pub fn variants_for_wabbajack_type(wj_type: &str) -> Vec<&'static KnownGame> {
    let Some(canonical) = find_by_wabbajack_type(wj_type) else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(2);
    out.push(canonical);
    for g in KNOWN_GAMES {
        if std::ptr::eq(g as *const _, canonical as *const _) {
            continue;
        }
        if g.wabbajack_type.is_none() && g.registry_path == canonical.registry_path {
            out.push(g);
        }
    }
    out
}
