//! Type definitions for Nexus Collections JSON format.
//!
//! These types map to the JSON structure exported by Nexus Mods Collections.
//! The format includes collection metadata, mod information, installation rules,
//! and FOMOD choices for automated installation.

use serde::{Deserialize, Serialize};

/// Collection info/metadata wrapper.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct CollectionInfo {
    /// The name of the collection.
    #[serde(default)]
    pub name: String,

    /// The collection author's username.
    #[serde(default)]
    pub author: String,

    /// The Nexus Mods domain for this game (e.g., "skyrimspecialedition").
    #[serde(rename = "domainName", default)]
    pub domain_name: String,

    /// Collection description/summary.
    #[serde(default)]
    pub description: String,
}

/// A Nexus Mods Collection containing mods, rules, and plugin configuration.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Collection {
    /// Collection metadata (new format with "info" wrapper).
    #[serde(default)]
    pub info: Option<CollectionInfo>,

    /// The name of the collection (legacy format without "info" wrapper).
    #[serde(rename = "collectionName", default)]
    pub name: String,

    /// The collection author's username (legacy format).
    #[serde(default)]
    pub author: String,

    /// The Nexus Mods domain for this game (legacy format).
    #[serde(rename = "domainName", default)]
    pub domain_name: String,

    /// Collection description/summary (legacy format).
    #[serde(default)]
    pub description: String,

    /// Collection version.
    #[serde(default)]
    pub version: String,

    /// List of mods in the collection.
    #[serde(default)]
    pub mods: Vec<CollectionMod>,

    /// Mod ordering/conflict rules.
    #[serde(rename = "modRules", default)]
    pub mod_rules: Vec<ModRule>,

    /// Plugin load order configuration.
    #[serde(default)]
    pub plugins: Vec<PluginInfo>,
}

impl Collection {
    /// Parses a collection from JSON bytes.
    pub fn from_json(json: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(json)
    }

    /// Parses a collection from a JSON string.
    pub fn from_json_str(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Returns the collection name (handles both formats).
    pub fn get_name(&self) -> &str {
        if let Some(ref info) = self.info {
            if !info.name.is_empty() {
                return &info.name;
            }
        }
        &self.name
    }

    /// Returns the collection author (handles both formats).
    pub fn get_author(&self) -> &str {
        if let Some(ref info) = self.info {
            if !info.author.is_empty() {
                return &info.author;
            }
        }
        &self.author
    }

    /// Returns the game domain (handles both formats).
    pub fn get_domain_name(&self) -> &str {
        if let Some(ref info) = self.info {
            if !info.domain_name.is_empty() {
                return &info.domain_name;
            }
        }
        &self.domain_name
    }

    /// Returns the collection description (handles both formats).
    pub fn get_description(&self) -> &str {
        if let Some(ref info) = self.info {
            if !info.description.is_empty() {
                return &info.description;
            }
        }
        &self.description
    }

    /// Returns the number of mods in the collection.
    pub fn mod_count(&self) -> usize {
        self.mods.len()
    }

    /// Returns the number of plugins in the collection.
    pub fn plugin_count(&self) -> usize {
        self.plugins.len()
    }
}

/// A mod entry in a collection.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct CollectionMod {
    /// Display name of the mod.
    #[serde(default)]
    pub name: String,

    /// The filename as it should be named/recognized.
    #[serde(rename = "logicalFilename", default)]
    pub logical_filename: String,

    /// Folder name for this mod in the mods directory.
    #[serde(rename = "folderName", default)]
    pub folder_name: String,

    /// Version of the mod.
    #[serde(default)]
    pub version: String,

    /// Download source information.
    #[serde(default)]
    pub source: ModSource,

    /// FOMOD installation choices (if the mod has a FOMOD installer).
    #[serde(default)]
    pub choices: Option<FomodChoices>,

    /// Installation phase (for ordering installation steps).
    /// Lower phases are installed first.
    #[serde(default)]
    pub phase: i32,

    /// Hashes for file verification (array of path/md5 pairs).
    #[serde(default)]
    pub hashes: Vec<FileHash>,

    /// Instructions/notes for this mod.
    #[serde(default)]
    pub instructions: String,

    /// Whether this mod is optional.
    #[serde(default)]
    pub optional: bool,

    /// Mod details/metadata.
    #[serde(default)]
    pub details: Option<ModDetails>,

    /// Author of this specific mod entry.
    #[serde(default)]
    pub author: String,

    /// Domain name for this mod (e.g., "skyrimspecialedition").
    #[serde(rename = "domainName", default)]
    pub domain_name: String,
}

/// Download source information for a mod.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct ModSource {
    /// Type of source (e.g., "nexus", "direct", "browse").
    #[serde(rename = "type", default)]
    pub source_type: String,

    /// Nexus Mods mod ID.
    #[serde(rename = "modId", default)]
    pub mod_id: i64,

    /// Nexus Mods file ID.
    #[serde(rename = "fileId", default)]
    pub file_id: i64,

    /// Expected filename.
    #[serde(rename = "logicalFilename", default)]
    pub logical_filename: String,

    /// Expected file size in bytes.
    #[serde(rename = "fileSize", default)]
    pub file_size: i64,

    /// MD5 hash of the file.
    #[serde(default)]
    pub md5: String,

    /// Direct download URL (for non-Nexus sources).
    #[serde(default)]
    pub url: String,

    /// Instructions for manual download.
    #[serde(default)]
    pub instructions: String,
}

/// File hash entry for verification.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct FileHash {
    /// Path within the archive.
    #[serde(default)]
    pub path: String,

    /// MD5 hash of the file.
    #[serde(default)]
    pub md5: String,
}

/// Legacy mod hashes struct (some collections use this format).
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct ModHashes {
    /// MD5 hash.
    #[serde(default)]
    pub md5: String,

    /// SHA256 hash.
    #[serde(rename = "sha256", default)]
    pub sha256: String,

    /// xxHash64.
    #[serde(rename = "xxhash64", default)]
    pub xxhash64: String,
}

/// Mod details/metadata.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct ModDetails {
    /// Category of the mod.
    #[serde(default)]
    pub category: String,

    /// Type of the mod.
    #[serde(rename = "type", default)]
    pub mod_type: String,
}

/// A mod ordering rule (before/after relationships).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ModRule {
    /// Type of rule: "before" or "after".
    #[serde(rename = "type")]
    pub rule_type: String,

    /// The source mod (the one being positioned).
    pub source: RuleReference,

    /// The reference mod (the one being positioned relative to).
    pub reference: RuleReference,
}

impl ModRule {
    /// Returns true if this is a "before" rule.
    pub fn is_before(&self) -> bool {
        self.rule_type.eq_ignore_ascii_case("before")
    }

    /// Returns true if this is an "after" rule.
    pub fn is_after(&self) -> bool {
        self.rule_type.eq_ignore_ascii_case("after")
    }
}

/// Reference to a mod in a rule (identified by hash or filename).
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct RuleReference {
    /// MD5 hash of the mod archive.
    #[serde(rename = "fileMD5", default)]
    pub file_md5: String,

    /// Logical filename of the mod.
    #[serde(rename = "logicalFileName", default)]
    pub logical_file_name: String,

    /// Folder name of the mod.
    #[serde(rename = "folderName", default)]
    pub folder_name: String,
}

/// Plugin (ESP/ESM/ESL) load order information.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PluginInfo {
    /// Plugin filename (e.g., "MyMod.esp").
    pub name: String,

    /// Whether the plugin is enabled.
    #[serde(default)]
    pub enabled: bool,
}

/// FOMOD installer choices for automated installation.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct FomodChoices {
    /// The installation steps with selected options.
    #[serde(default)]
    pub options: Vec<FomodStep>,
}

impl FomodChoices {
    /// Returns true if there are any choices recorded.
    pub fn has_choices(&self) -> bool {
        !self.options.is_empty()
    }

    /// Returns the total number of selected options across all steps.
    pub fn total_selections(&self) -> usize {
        self.options
            .iter()
            .flat_map(|step| &step.groups)
            .flat_map(|group| &group.choices)
            .count()
    }
}

/// A step in a FOMOD installer.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FomodStep {
    /// Name of the installation step.
    pub name: String,

    /// Option groups within this step.
    #[serde(default)]
    pub groups: Vec<FomodGroup>,
}

/// A group of options within a FOMOD step.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FomodGroup {
    /// Name of the option group.
    pub name: String,

    /// Selected choices within this group.
    #[serde(default)]
    pub choices: Vec<FomodChoice>,
}

/// A selected option within a FOMOD group.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FomodChoice {
    /// Name of the selected option.
    pub name: String,

    /// Index of the selected option within the group.
    #[serde(default)]
    pub idx: i32,
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_COLLECTION_JSON: &str = r#"{
        "collectionName": "Test Collection",
        "author": "TestAuthor",
        "domainName": "skyrimspecialedition",
        "version": "1.0.0",
        "mods": [
            {
                "name": "Test Mod",
                "logicalFilename": "Test Mod-123-1-0.zip",
                "folderName": "Test Mod",
                "version": "1.0",
                "source": {
                    "type": "nexus",
                    "modId": 12345,
                    "fileId": 67890,
                    "logicalFilename": "Test Mod-123-1-0.zip",
                    "fileSize": 1048576,
                    "md5": "abc123def456"
                },
                "phase": 1
            },
            {
                "name": "FOMOD Mod",
                "logicalFilename": "FOMOD Mod-456-2-0.zip",
                "folderName": "FOMOD Mod",
                "source": {
                    "type": "nexus",
                    "modId": 45678,
                    "fileId": 11111
                },
                "choices": {
                    "options": [
                        {
                            "name": "Main Files",
                            "groups": [
                                {
                                    "name": "Core Options",
                                    "choices": [
                                        {"name": "Full Install", "idx": 0}
                                    ]
                                }
                            ]
                        }
                    ]
                }
            }
        ],
        "modRules": [
            {
                "type": "after",
                "source": {
                    "fileMD5": "abc123",
                    "logicalFileName": "FOMOD Mod-456-2-0.zip"
                },
                "reference": {
                    "fileMD5": "def456",
                    "logicalFileName": "Test Mod-123-1-0.zip"
                }
            }
        ],
        "plugins": [
            {"name": "TestMod.esp", "enabled": true},
            {"name": "FOMODMod.esp", "enabled": true}
        ]
    }"#;

    #[test]
    fn test_parse_collection() {
        let collection = Collection::from_json_str(SAMPLE_COLLECTION_JSON).unwrap();

        assert_eq!(collection.get_name(), "Test Collection");
        assert_eq!(collection.get_author(), "TestAuthor");
        assert_eq!(collection.get_domain_name(), "skyrimspecialedition");
        assert_eq!(collection.version, "1.0.0");
        assert_eq!(collection.mod_count(), 2);
        assert_eq!(collection.plugin_count(), 2);
    }

    #[test]
    fn test_parse_mod_source() {
        let collection = Collection::from_json_str(SAMPLE_COLLECTION_JSON).unwrap();
        let mod_entry = &collection.mods[0];

        assert_eq!(mod_entry.name, "Test Mod");
        assert_eq!(mod_entry.source.mod_id, 12345);
        assert_eq!(mod_entry.source.file_id, 67890);
        assert_eq!(mod_entry.source.file_size, 1048576);
        assert_eq!(mod_entry.phase, 1);
    }

    #[test]
    fn test_parse_fomod_choices() {
        let collection = Collection::from_json_str(SAMPLE_COLLECTION_JSON).unwrap();
        let fomod_mod = &collection.mods[1];

        assert!(fomod_mod.choices.is_some());
        let choices = fomod_mod.choices.as_ref().unwrap();
        assert!(choices.has_choices());
        assert_eq!(choices.total_selections(), 1);
        assert_eq!(choices.options[0].name, "Main Files");
        assert_eq!(choices.options[0].groups[0].name, "Core Options");
        assert_eq!(choices.options[0].groups[0].choices[0].name, "Full Install");
    }

    #[test]
    fn test_parse_mod_rules() {
        let collection = Collection::from_json_str(SAMPLE_COLLECTION_JSON).unwrap();
        assert_eq!(collection.mod_rules.len(), 1);

        let rule = &collection.mod_rules[0];
        assert!(rule.is_after());
        assert!(!rule.is_before());
        assert_eq!(rule.source.file_md5, "abc123");
        assert_eq!(rule.reference.file_md5, "def456");
    }

    #[test]
    fn test_parse_plugins() {
        let collection = Collection::from_json_str(SAMPLE_COLLECTION_JSON).unwrap();
        assert_eq!(collection.plugins.len(), 2);
        assert_eq!(collection.plugins[0].name, "TestMod.esp");
        assert!(collection.plugins[0].enabled);
    }

    #[test]
    fn test_empty_collection() {
        let json = r#"{"collectionName": "Empty"}"#;
        let collection = Collection::from_json_str(json).unwrap();

        assert_eq!(collection.get_name(), "Empty");
        assert_eq!(collection.mod_count(), 0);
        assert_eq!(collection.plugin_count(), 0);
        assert!(collection.mod_rules.is_empty());
    }

    #[test]
    fn test_parse_new_format_with_info() {
        // New format with "info" wrapper
        let json = r#"{
            "info": {
                "name": "New Format Collection",
                "author": "NewAuthor",
                "domainName": "skyrimspecialedition",
                "description": "A test collection"
            },
            "mods": []
        }"#;
        let collection = Collection::from_json_str(json).unwrap();

        assert_eq!(collection.get_name(), "New Format Collection");
        assert_eq!(collection.get_author(), "NewAuthor");
        assert_eq!(collection.get_domain_name(), "skyrimspecialedition");
        assert_eq!(collection.get_description(), "A test collection");
        assert_eq!(collection.mod_count(), 0);
    }
}
