//! Nexus Collections support module.
//!
//! Provides functionality to parse and install Nexus Mods Collections.
//! Collections are JSON files that describe a curated set of mods with:
//! - Mod download information (Nexus mod/file IDs, hashes)
//! - FOMOD installer choices for automated installation
//! - Mod ordering rules (before/after relationships)
//! - Plugin load order configuration
//!
//! # Example
//!
//! ```no_run
//! use clf3::collection::Collection;
//!
//! let json = std::fs::read("collection.json").unwrap();
//! let collection = Collection::from_json(&json).unwrap();
//!
//! println!("Collection: {}", collection.name);
//! println!("Mods: {}", collection.mod_count());
//! ```

#![allow(unused_imports)] // Re-exports for public API

pub mod archive;
pub mod db;
pub mod fetch;
pub mod fomod;
pub mod gallery;
pub mod install;
pub mod modlist;
pub mod patches;
mod types;
pub mod verify;

pub use db::{
    ArchiveFileEntry, CollectionDb, InstalledFileEntry, ModDbEntry, ModRuleDbEntry, ModStats,
    ModStatus, PluginDbEntry,
};
pub use fetch::{fetch_collection, is_url, parse_collection_url, CollectionUrlInfo};
pub use install::{install_collection_streaming, InstallConfig, InstallStats};
pub use modlist::{ModInfo as ModListInfo, ModListGenerator, ModRule as ModListRule};
pub use types::{
    Collection, CollectionInfo, CollectionMod, FileHash, FomodChoice, FomodChoices, FomodGroup,
    FomodStep, ModDetails, ModHashes, ModRule, ModSource, PluginInfo, RuleReference,
};

use anyhow::{Context, Result};
use std::path::Path;

/// Loads a collection from a JSON file.
pub fn load_collection(path: &Path) -> Result<Collection> {
    let content = std::fs::read(path)
        .with_context(|| format!("Failed to read collection file: {}", path.display()))?;

    Collection::from_json(&content)
        .with_context(|| format!("Failed to parse collection JSON: {}", path.display()))
}

/// Loads a collection from a JSON string.
pub fn parse_collection(json: &str) -> Result<Collection> {
    Collection::from_json_str(json).context("Failed to parse collection JSON")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_collection_function() {
        let json = r#"{
            "collectionName": "Test",
            "domainName": "skyrimspecialedition",
            "mods": []
        }"#;

        let collection = parse_collection(json).unwrap();
        assert_eq!(collection.get_name(), "Test");
        assert_eq!(collection.get_domain_name(), "skyrimspecialedition");
    }
}
