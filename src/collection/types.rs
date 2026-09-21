//! Manifest vocabulary ported from CLF3's vortex-collections branch and extended
//! against Vortex's ICollection.ts. Unknown fields survive deserialization.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub type ExtraFields = BTreeMap<String, Value>;

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CollectionInfo {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub author: String,
    #[serde(default)]
    pub domain_name: String,
    #[serde(default)]
    pub game_versions: Vec<String>,
    #[serde(flatten)]
    pub extra: ExtraFields,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Collection {
    #[serde(default)]
    pub info: CollectionInfo,
    #[serde(default, rename = "collectionName")]
    pub name: String,
    #[serde(default)]
    pub domain_name: String,
    // Unlike the old branch, an absent mods array is not an empty collection.
    pub mods: Vec<CollectionMod>,
    #[serde(default)]
    pub mod_rules: Vec<ModRule>,
    #[serde(default)]
    pub plugins: Vec<PluginInfo>,
    #[serde(default)]
    pub plugin_rules: Value,
    #[serde(default)]
    pub collection_config: Value,
    #[serde(flatten)]
    pub extra: ExtraFields,
}

impl Collection {
    pub fn name(&self) -> &str {
        if self.info.name.is_empty() {
            &self.name
        } else {
            &self.info.name
        }
    }
    pub fn domain(&self) -> &str {
        if self.info.domain_name.is_empty() {
            &self.domain_name
        } else {
            &self.info.domain_name
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CollectionMod {
    pub name: String,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub domain_name: String,
    pub source: ModSource,
    #[serde(default)]
    pub optional: bool,
    #[serde(default)]
    pub phase: i32,
    // Installer-specific data must not be flattened to plugin names. In
    // particular blank/duplicate step names and selected-none groups matter.
    #[serde(default)]
    pub choices: Value,
    #[serde(default)]
    pub hashes: Vec<FileHash>,
    #[serde(default)]
    pub patches: BTreeMap<String, String>,
    /// Vortex excludes these paths from THIS provider during deployment.
    #[serde(default)]
    pub file_overrides: Vec<String>,
    #[serde(default)]
    pub details: Value,
    #[serde(flatten)]
    pub extra: ExtraFields,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModSource {
    #[serde(rename = "type")]
    pub source_type: String,
    #[serde(default)]
    pub mod_id: u64,
    #[serde(default)]
    pub file_id: u64,
    #[serde(default)]
    pub file_size: Option<u64>,
    #[serde(default)]
    pub md5: String,
    #[serde(default)]
    pub logical_filename: String,
    #[serde(default)]
    pub file_expression: String,
    #[serde(default)]
    pub tag: String,
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub update_policy: Option<String>,
    #[serde(flatten)]
    pub extra: ExtraFields,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FileHash {
    pub path: String,
    pub md5: String,
    #[serde(flatten)]
    pub extra: ExtraFields,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ModRule {
    #[serde(rename = "type")]
    pub rule_type: String,
    pub source: Value,
    pub reference: Value,
    #[serde(flatten)]
    pub extra: ExtraFields,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PluginInfo {
    pub name: String,
    pub enabled: bool,
    #[serde(flatten)]
    pub extra: ExtraFields,
}
