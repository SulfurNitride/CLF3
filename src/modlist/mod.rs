//! Wabbajack modlist parsing and storage
//!
//! This module handles:
//! - Opening .wabbajack files (ZIP archives)
//! - Parsing the modlist JSON
//! - Storing directives in SQLite for efficient access

pub mod browser;
mod db;
pub mod install_manifest;
mod types;
pub mod update;

#[allow(unused_imports)] // Used by lib crate (GUI)
pub use browser::*;
pub use db::*;
#[allow(unused_imports)] // Several items are public surface; not all used in the binary.
pub use install_manifest::{
    compare_versions, InstallManifest, VersionCmp, CURRENT_SCHEMA_VERSION, MANIFEST_FILENAME,
};
pub use types::*;

use anyhow::{Context, Result};
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::Path;
use tracing::info;
use zip::ZipArchive;

/// Calculate a simple hash of file metadata (size + mtime) for change detection
/// This is fast and good enough for detecting if the wabbajack file changed
fn calculate_file_fingerprint(path: &Path) -> Result<String> {
    let metadata = fs::metadata(path)
        .with_context(|| format!("Cannot read metadata for {}", path.display()))?;

    let size = metadata.len();
    let mtime = metadata
        .modified()
        .map(|t| {
            t.duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        })
        .unwrap_or(0);

    Ok(format!("{}:{}", size, mtime))
}

/// Open a .wabbajack file and parse the modlist
pub fn parse_wabbajack_file(path: &Path) -> Result<Modlist> {
    info!("Opening wabbajack file: {}", path.display());

    let file = File::open(path).with_context(|| format!("Failed to open: {}", path.display()))?;

    let reader = BufReader::new(file);
    let mut archive = ZipArchive::new(reader).context("Failed to read as ZIP archive")?;

    info!("Archive contains {} files", archive.len());

    // Find and read the modlist file
    let mut modlist_file = archive
        .by_name("modlist")
        .context("No 'modlist' file found in archive")?;

    let mut json_data = String::new();
    modlist_file
        .read_to_string(&mut json_data)
        .context("Failed to read modlist JSON")?;

    info!("Read {} bytes of JSON", json_data.len());

    // Parse the JSON
    let modlist: Modlist =
        serde_json::from_str(&json_data).context("Failed to parse modlist JSON")?;

    info!(
        "Parsed modlist '{}' v{} - {} archives, {} directives",
        modlist.name,
        modlist.version,
        modlist.archives.len(),
        modlist.directives.len()
    );

    validate_modlist_paths(&modlist)?;

    Ok(modlist)
}

/// Bumped whenever load-time validation rules change, so state databases
/// imported under older rules are re-imported (and re-validated).
const PATH_VALIDATION_VERSION: &str = "1";

/// Reject modlists whose archive names or output paths would escape the
/// downloads / install directories (e.g. `..\..\.bashrc`, `/home/u/x`).
///
/// Every path a .wabbajack supplies is later joined onto a user directory and
/// written to (or, for archives, hashed and possibly deleted), so a single
/// bad entry is a hard error rather than a skipped directive.
pub fn validate_modlist_paths(modlist: &Modlist) -> Result<()> {
    const MAX_REPORTED: usize = 5;
    let mut problems = Vec::new();
    let mut total = 0usize;
    let mut record = |what: String| {
        total += 1;
        if problems.len() < MAX_REPORTED {
            problems.push(what);
        }
    };

    for archive in &modlist.archives {
        if let Some(reason) = crate::paths::unsafe_file_name_reason(&archive.name) {
            record(format!("archive name {:?}: {}", archive.name, reason));
        }
    }
    for directive in &modlist.directives {
        let to = directive.to_path();
        if let Some(reason) = crate::paths::unsafe_relative_path_reason(to) {
            record(format!("output path {:?}: {}", to, reason));
        }
        if let Directive::CreateBSA(bsa) = directive {
            for state in &bsa.file_states {
                if let Some(reason) = crate::paths::unsafe_relative_path_reason(state.path()) {
                    record(format!(
                        "BSA entry {:?} in {:?}: {}",
                        state.path(),
                        to,
                        reason
                    ));
                }
            }
        }
    }

    if total > 0 {
        anyhow::bail!(
            "Refusing to install '{}': {} unsafe path(s) would write outside the install or \
             downloads directory. This .wabbajack file is malformed or has been tampered with.\n  {}{}",
            modlist.name,
            total,
            problems.join("\n  "),
            if total > problems.len() {
                format!("\n  ... and {} more", total - problems.len())
            } else {
                String::new()
            }
        );
    }
    Ok(())
}

/// Open a .wabbajack file and import into a database
/// If the database exists but is for a different wabbajack file, it will be cleared and re-imported
pub fn import_wabbajack_to_db(wabbajack_path: &Path, db_path: &Path) -> Result<ModlistDb> {
    let current_fingerprint = calculate_file_fingerprint(wabbajack_path)?;
    let wabbajack_path_str = wabbajack_path.to_string_lossy().to_string();

    // Check if database exists and matches current wabbajack file
    let mut db = ModlistDb::open(db_path)?;

    let stored_fingerprint = db.get_metadata("wabbajack_fingerprint")?;
    let stored_path = db.get_metadata("wabbajack_path")?;

    let needs_reimport = match (stored_fingerprint, stored_path) {
        (Some(fp), Some(path)) => {
            if fp != current_fingerprint || path != wabbajack_path_str {
                info!("Wabbajack file changed, clearing database...");
                db.clear_all_data()?;
                true
            } else if db.get_metadata("path_validation")?.as_deref()
                != Some(PATH_VALIDATION_VERSION)
            {
                info!("Database predates path validation, re-importing...");
                db.clear_all_data()?;
                true
            } else {
                // Check if we already have archives imported
                let stats = db.get_directive_stats()?;
                stats.total == 0
            }
        }
        _ => {
            // No metadata stored - clear any orphaned data to prevent duplicates
            // This can happen if a previous import crashed before setting fingerprint
            let stats = db.get_directive_stats()?;
            if stats.total > 0 {
                info!(
                    "Clearing orphaned data from incomplete previous import ({} directives)",
                    stats.total
                );
                db.clear_all_data()?;
            }
            true
        }
    };

    if needs_reimport {
        info!("Importing modlist to database...");
        let modlist = parse_wabbajack_file(wabbajack_path)?;

        db.import_modlist(&modlist)?;

        // Store fingerprint and path for future checks
        db.set_metadata("wabbajack_fingerprint", &current_fingerprint)?;
        db.set_metadata("wabbajack_path", &wabbajack_path_str)?;
        db.set_metadata("path_validation", PATH_VALIDATION_VERSION)?;
    } else {
        info!("Using existing database (wabbajack file unchanged)");
    }

    let stats = db.get_directive_stats()?;
    info!(
        "Database ready: {} total directives ({} pending)",
        stats.total, stats.pending
    );

    Ok(db)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn modlist_with(archive_name: &str, to: &str) -> Modlist {
        serde_json::from_value(serde_json::json!({
            "Name": "Test", "Version": "1.0", "WabbajackVersion": "3.0",
            "GameType": "SkyrimSpecialEdition", "IsNSFW": false,
            "Archives": [{
                "Hash": "AAAAAAAAAAA=", "Meta": "", "Name": archive_name, "Size": 1,
                "State": {"$type": "HttpDownloader, Wabbajack.Lib", "Url": "https://example.invalid/a"}
            }],
            "Directives": [{
                "$type": "InlineFile", "To": to, "Hash": "AAAAAAAAAAA=", "Size": 1,
                "SourceDataID": "00000000-0000-0000-0000-000000000000"
            }]
        }))
        .expect("fixture parses")
    }

    #[test]
    fn validate_modlist_paths_accepts_normal_paths() {
        let modlist = modlist_with("SkyUI_5_2_SE-12604-5-2SE.7z", "mods\\SkyUI\\SkyUI_SE.esp");
        validate_modlist_paths(&modlist).expect("normal modlist is valid");
    }

    #[test]
    fn validate_modlist_paths_rejects_escapes() {
        let err = validate_modlist_paths(&modlist_with("ok.7z", "..\\..\\.bashrc")).unwrap_err();
        assert!(err.to_string().contains("'..' component"), "{err}");

        let err = validate_modlist_paths(&modlist_with("ok.7z", "\\home\\u\\.bashrc")).unwrap_err();
        assert!(err.to_string().contains("absolute path"), "{err}");

        let err = validate_modlist_paths(&modlist_with("/home/u/thesis.docx", "mods\\a.esp"))
            .unwrap_err();
        assert!(err.to_string().contains("path separator"), "{err}");
    }

    // Test with actual Tuxborn file if available
    #[test]
    #[ignore] // Run with: cargo test -- --ignored
    fn test_parse_tuxborn() {
        let path = Path::new(
            "/home/luke/Documents/Wabbajack Rust Update/Tuxborn-Wabbajack/Tuxborn.wabbajack",
        );
        if path.exists() {
            let modlist = parse_wabbajack_file(path).unwrap();
            assert_eq!(modlist.name, "Tuxborn");
            println!("Archives: {}", modlist.archives.len());
            println!("Directives: {}", modlist.directives.len());

            // Tuxborn should NOT require TTW
            let ttw = modlist.requires_ttw();
            assert!(!ttw.required, "Tuxborn should not require TTW");
        }
    }

    // Test TTW detection with Begin Again
    #[test]
    #[ignore] // Run with: cargo test -- --ignored
    fn test_ttw_detection_begin_again() {
        let path = Path::new("/home/luke/Downloads/beginagain.wabbajack");
        if path.exists() {
            let modlist = parse_wabbajack_file(path).unwrap();
            let ttw = modlist.requires_ttw();

            println!("Modlist: {}", modlist.name);
            println!("TTW Required: {}", ttw.required);
            println!("TTW Markers: {:?}", ttw.markers_found);

            assert!(ttw.required, "Begin Again should require TTW");
            assert!(!ttw.markers_found.is_empty());
        }
    }
}
