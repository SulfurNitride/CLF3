//! Wabbajack modlist parsing and storage
//!
//! This module handles:
//! - Opening .wabbajack files (ZIP archives)
//! - Parsing the modlist JSON
//! - Storing directives in SQLite for efficient access

mod types;
mod db;
pub mod browser;

pub use types::*;
pub use db::*;
pub use browser::*;

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
    let mtime = metadata.modified()
        .map(|t| t.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs())
        .unwrap_or(0);

    Ok(format!("{}:{}", size, mtime))
}

/// Open a .wabbajack file and parse the modlist
pub fn parse_wabbajack_file(path: &Path) -> Result<Modlist> {
    info!("Opening wabbajack file: {}", path.display());

    let file = File::open(path)
        .with_context(|| format!("Failed to open: {}", path.display()))?;

    let reader = BufReader::new(file);
    let mut archive = ZipArchive::new(reader)
        .context("Failed to read as ZIP archive")?;

    info!("Archive contains {} files", archive.len());

    // Find and read the modlist file
    let mut modlist_file = archive.by_name("modlist")
        .context("No 'modlist' file found in archive")?;

    let mut json_data = String::new();
    modlist_file.read_to_string(&mut json_data)
        .context("Failed to read modlist JSON")?;

    info!("Read {} bytes of JSON", json_data.len());

    // Parse the JSON
    let modlist: Modlist = serde_json::from_str(&json_data)
        .context("Failed to parse modlist JSON")?;

    info!(
        "Parsed modlist '{}' v{} - {} archives, {} directives",
        modlist.name,
        modlist.version,
        modlist.archives.len(),
        modlist.directives.len()
    );

    Ok(modlist)
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
                info!("Clearing orphaned data from incomplete previous import ({} directives)", stats.total);
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
    } else {
        info!("Using existing database (wabbajack file unchanged)");
    }

    let stats = db.get_directive_stats()?;
    info!(
        "Database ready: {} total directives ({} pending)",
        stats.total,
        stats.pending
    );

    Ok(db)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test with actual Tuxborn file if available
    #[test]
    #[ignore] // Run with: cargo test -- --ignored
    fn test_parse_tuxborn() {
        let path = Path::new("/home/luke/Documents/Wabbajack Rust Update/Tuxborn-Wabbajack/Tuxborn.wabbajack");
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
