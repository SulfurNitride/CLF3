//! Reviewed game-specific installation rules shared by planner, worker and GUI.
//! Catalog visibility is independent of this registry. Unknown games never
//! inherit another game's paths or plugin semantics.
use anyhow::{bail, Result};

#[derive(Debug)]
pub struct Masterlist {
    pub repository: &'static str,
    pub commit: &'static str,
    pub sha256: &'static str,
}

impl Masterlist {
    pub fn url(&self) -> String {
        format!(
            "https://raw.githubusercontent.com/loot/{}/{}/masterlist.yaml",
            self.repository, self.commit
        )
    }
}

#[derive(Debug)]
pub struct GameProfile {
    pub domain: &'static str,
    pub name: &'static str,
    pub executable: &'static str,
    pub steam_ids: &'static [&'static str],
    pub my_games: &'static str,
    pub ini_files: &'static [&'static str],
    pub default_ini: &'static str,
    pub extender: &'static str,
    pub extender_name: &'static str,
    pub extender_prefix: &'static str,
    pub extender_directory: &'static str,
    pub base_plugins: &'static [&'static str],
    pub creation_club_file: Option<&'static str>,
    pub loot: libloot::GameType,
    pub masterlist: Masterlist,
    pub asterisk_plugins: bool,
    pub timestamp_order: bool,
    pub light_plugins: bool,
    pub plugin_header_size: usize,
    pub experimental: bool,
}

impl GameProfile {
    pub fn manager_name(&self) -> &'static str {
        if self.domain == "newvegas" {
            "New Vegas"
        } else {
            self.name
        }
    }

    pub fn steam_id_for_path(&self, game: &std::path::Path) -> &'static str {
        // Prefer an explicit game-local ID (including regional NV editions).
        if let Ok(path) = super::paths::resolve_file(game, "steam_appid.txt") {
            if path.metadata().is_ok_and(|m| m.len() < 64) {
                if let Ok(id) = std::fs::read_to_string(path) {
                    if let Some(id) = self.steam_ids.iter().find(|known| **known == id.trim()) {
                        return id;
                    }
                }
            }
        }
        if self.domain == "fallout3"
            && game
                .file_name()
                .is_some_and(|n| n.to_string_lossy().eq_ignore_ascii_case("Fallout 3"))
        {
            "22300"
        } else {
            self.steam_ids[0]
        }
    }
    pub fn ini_target(&self, name: &str) -> Option<&'static str> {
        self.ini_files
            .iter()
            .copied()
            .find(|n| n.eq_ignore_ascii_case(name))
    }

    pub fn max_full_plugins(&self) -> usize {
        if self.light_plugins {
            254
        } else {
            255
        }
    }

    pub fn snapshot_files(&self) -> impl Iterator<Item = &'static str> + '_ {
        [
            "modlist.txt",
            "plugins.txt",
            "loadorder.txt",
            "settings.ini",
        ]
        .into_iter()
        .chain(self.ini_files.iter().copied())
    }

    /// Top-level extender binaries belong to the private game, while extender
    /// plugins retain their Data/<extender>/Plugins directory.
    pub fn root_file(&self, path: &str) -> bool {
        let lower = path.to_ascii_lowercase();
        if lower.contains('/') {
            return false;
        }
        (self.experimental && lower == self.executable.to_ascii_lowercase())
            || self
                .creation_club_file
                .is_some_and(|n| n.eq_ignore_ascii_case(path))
            || (lower.starts_with(self.extender_prefix)
                && [".exe", ".dll"]
                    .iter()
                    .any(|suffix| lower.ends_with(suffix)))
    }

    /// Native plugins.txt is Windows-1252. Modern games retain disabled rows;
    /// older games list only enabled plugins, without the asterisk prefix.
    pub fn plugin_list<'a>(
        &self,
        plugins: impl IntoIterator<Item = (&'a str, bool)>,
    ) -> Result<Vec<u8>> {
        let mut text = String::new();
        for (name, enabled) in plugins {
            if name.contains(['\n', '\r', '/', '\\']) || name.starts_with(['*', '#']) {
                bail!("Invalid plugin filename");
            }
            if self.asterisk_plugins || enabled {
                if self.asterisk_plugins && enabled {
                    text.push('*');
                }
                text.push_str(name);
                text.push('\n');
            }
        }
        let (bytes, _, errors) = encoding_rs::WINDOWS_1252.encode(&text);
        if errors {
            bail!("Plugin names cannot be represented in the game's Windows-1252 activation file");
        }
        Ok(bytes.into_owned())
    }
}

pub static PROFILES: &[GameProfile] = &[
    GameProfile {
        domain: "skyrimspecialedition",
        name: "Skyrim Special Edition",
        executable: "SkyrimSE.exe",
        steam_ids: &["489830"],
        my_games: "Skyrim Special Edition",
        ini_files: &["Skyrim.ini", "SkyrimPrefs.ini", "SkyrimCustom.ini"],
        default_ini: "Skyrim_Default.ini",
        extender: "skse64_loader.exe",
        extender_name: "SKSE",
        extender_prefix: "skse64_",
        extender_directory: "skse",
        base_plugins: &[
            "Skyrim.esm",
            "Update.esm",
            "Dawnguard.esm",
            "HearthFires.esm",
            "Dragonborn.esm",
        ],
        creation_club_file: Some("Skyrim.ccc"),
        loot: libloot::GameType::SkyrimSE,
        masterlist: Masterlist {
            repository: "skyrimse",
            commit: "e3c591ba9c041f23f407a0a0f87f72cc6325aa43",
            sha256: "95caf8492923b77386fc725150d38c635a581a16bf0e546c3a44eec85afbe484",
        },
        asterisk_plugins: true,
        timestamp_order: false,
        light_plugins: true,
        plugin_header_size: 24,
        experimental: false,
    },
    GameProfile {
        domain: "fallout4",
        name: "Fallout 4",
        executable: "Fallout4.exe",
        steam_ids: &["377160"],
        my_games: "Fallout4",
        ini_files: &["Fallout4.ini", "Fallout4Prefs.ini", "Fallout4Custom.ini"],
        default_ini: "Fallout4_Default.ini",
        extender: "f4se_loader.exe",
        extender_name: "F4SE",
        extender_prefix: "f4se_",
        extender_directory: "f4se",
        base_plugins: &[
            "Fallout4.esm",
            "DLCRobot.esm",
            "DLCworkshop01.esm",
            "DLCCoast.esm",
            "DLCworkshop02.esm",
            "DLCworkshop03.esm",
            "DLCNukaWorld.esm",
            "DLCUltraHighResolution.esm",
        ],
        creation_club_file: Some("Fallout4.ccc"),
        loot: libloot::GameType::Fallout4,
        masterlist: Masterlist {
            repository: "fallout4",
            commit: "22dcffee55f148f8b41981c8119cd6ed36869f56",
            sha256: "b018c8c92de4fe0c4cb19476ee41a7cc0b37d40e1922e3810b0cc33526d65c06",
        },
        asterisk_plugins: true,
        timestamp_order: false,
        light_plugins: true,
        plugin_header_size: 24,
        experimental: true,
    },
    GameProfile {
        domain: "newvegas",
        name: "Fallout New Vegas",
        executable: "FalloutNV.exe",
        steam_ids: &["22380", "22490"],
        my_games: "FalloutNV",
        ini_files: &["Fallout.ini", "FalloutPrefs.ini", "FalloutCustom.ini"],
        default_ini: "Fallout_default.ini",
        extender: "nvse_loader.exe",
        extender_name: "NVSE",
        extender_prefix: "nvse_",
        extender_directory: "nvse",
        base_plugins: &[
            "FalloutNV.esm",
            "DeadMoney.esm",
            "HonestHearts.esm",
            "OldWorldBlues.esm",
            "LonesomeRoad.esm",
            "GunRunnersArsenal.esm",
            "ClassicPack.esm",
            "MercenaryPack.esm",
            "TribalPack.esm",
            "CaravanPack.esm",
        ],
        creation_club_file: None,
        loot: libloot::GameType::FalloutNV,
        masterlist: Masterlist {
            repository: "falloutnv",
            commit: "79b2bb6db4ce560e8ecd80f66b375618d3e33405",
            sha256: "ec2d9f340f1ad308aef4fa8e4190408a9ddf0a2148131945fdd95ffd38064ad4",
        },
        asterisk_plugins: false,
        timestamp_order: true,
        light_plugins: false,
        plugin_header_size: 24,
        experimental: true,
    },
    GameProfile {
        domain: "fallout3",
        name: "Fallout 3",
        executable: "Fallout3.exe",
        steam_ids: &["22370", "22300"],
        my_games: "Fallout3",
        ini_files: &["Fallout.ini", "FalloutPrefs.ini", "FalloutCustom.ini"],
        default_ini: "Fallout_default.ini",
        extender: "fose_loader.exe",
        extender_name: "FOSE",
        extender_prefix: "fose_",
        extender_directory: "fose",
        base_plugins: &[
            "Fallout3.esm",
            "Anchorage.esm",
            "ThePitt.esm",
            "BrokenSteel.esm",
            "PointLookout.esm",
            "Zeta.esm",
        ],
        creation_club_file: None,
        loot: libloot::GameType::Fallout3,
        masterlist: Masterlist {
            repository: "fallout3",
            commit: "76e62d5479eedf24975895fe2019000c6df679c3",
            sha256: "4947d03d6e5dbe1e933d597eee467e7554fc5f744a207736c3964df12feb1f29",
        },
        asterisk_plugins: false,
        timestamp_order: true,
        light_plugins: false,
        plugin_header_size: 24,
        experimental: true,
    },
    GameProfile {
        domain: "oblivion",
        name: "Oblivion",
        executable: "Oblivion.exe",
        steam_ids: &["22330"],
        my_games: "Oblivion",
        ini_files: &["Oblivion.ini"],
        default_ini: "Oblivion_default.ini",
        extender: "obse_loader.exe",
        extender_name: "OBSE",
        extender_prefix: "obse_",
        extender_directory: "obse",
        base_plugins: &["Oblivion.esm", "DLCShiveringIsles.esp", "Knights.esp"],
        creation_club_file: None,
        loot: libloot::GameType::Oblivion,
        masterlist: Masterlist {
            repository: "oblivion",
            commit: "5e80ff2298d6dc433021bdc0e6be3ca044566d1c",
            sha256: "643e36cb730cabbe78849ebfa2a484ad60550b3a826bd3c76de400a3bb56c214",
        },
        asterisk_plugins: false,
        timestamp_order: true,
        light_plugins: false,
        plugin_header_size: 20,
        experimental: true,
    },
    GameProfile {
        domain: "skyrim",
        name: "Skyrim",
        executable: "TESV.exe",
        steam_ids: &["72850"],
        my_games: "Skyrim",
        ini_files: &["Skyrim.ini", "SkyrimPrefs.ini", "SkyrimCustom.ini"],
        default_ini: "Skyrim_default.ini",
        extender: "skse_loader.exe",
        extender_name: "SKSE",
        extender_prefix: "skse_",
        extender_directory: "skse",
        base_plugins: &[
            "Skyrim.esm",
            "Update.esm",
            "Dawnguard.esm",
            "HearthFires.esm",
            "Dragonborn.esm",
        ],
        creation_club_file: None,
        loot: libloot::GameType::Skyrim,
        masterlist: Masterlist {
            repository: "skyrim",
            commit: "2d65f851150a155998765446e8c7dc607eb4bd73",
            sha256: "6d1fed6da0f598e94d9b415e2e455987b1db43beab95d25f47d4c63adbbfa6e9",
        },
        asterisk_plugins: false,
        timestamp_order: false,
        light_plugins: false,
        plugin_header_size: 24,
        experimental: true,
    },
];

pub fn profile(domain: &str) -> Option<&'static GameProfile> {
    PROFILES.iter().find(|g| g.domain == domain)
}
pub fn require(domain: &str) -> Result<&'static GameProfile> {
    profile(domain).ok_or_else(|| anyhow::anyhow!("{}: {}", domain, unsupported_reason(domain)))
}

/// Extension boundaries for subsequent implementations. These do not enable
/// installation until deployment and activation have conformance coverage.
pub fn unsupported_reason(domain: &str) -> &'static str {
    match domain {
        "skyrimvr" | "fallout4vr" => {
            "VR runtime and light-plugin compatibility need a dedicated game profile"
        }
        "enderal" | "enderalspecialedition" => {
            "Enderal needs its own runtime, game detection and masterlist profile"
        }
        "morrowind" => {
            "Morrowind needs TES3 records, Data Files routing and Morrowind.ini activation"
        }
        "starfield" => {
            "Starfield needs medium/blueprint plugin handling and its content deployment rules"
        }
        "oblivionremastered" => {
            "Oblivion Remastered needs nested game paths and Unreal payload routing"
        }
        "cyberpunk2077" => {
            "Cyberpunk needs root-directory deployment, REDmod handling and archive conflict order"
        }
        "stardewvalley" => {
            "Stardew Valley needs SMAPI manifest/dependency handling and Mods-directory deployment"
        }
        "baldursgate3" => {
            "Baldur's Gate 3 needs PAK metadata, modsettings.lsx and profile deployment"
        }
        _ => "This game needs a reviewed deployment and activation adapter",
    }
}
