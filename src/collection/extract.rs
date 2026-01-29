//! Archive extraction for collection installations.
//!
//! Handles extracting mod archives with:
//! - File routing (root vs data files)
//! - Archive indexing for case-insensitive lookups
//! - Parallel extraction using rayon
//! - Progress display with indicatif

use std::fs::{self, File};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rayon::prelude::*;
use tracing::{info, warn};

use crate::file_router::FileRouter;

use super::db::{ArchiveFileEntry, CollectionDb, ModDbEntry, ModStatus};
use super::fomod::{find_module_config, parse_fomod};

/// Extraction statistics
#[derive(Debug, Default)]
pub struct ExtractStats {
    pub extracted: usize,
    pub skipped: usize,
    pub failed: usize,
    pub indexed: usize,
}

/// FOMOD validation result
#[derive(Debug)]
pub struct FomodValidationResult {
    pub mod_id: i64,
    pub mod_name: String,
    pub valid: bool,
    pub module_name: Option<String>,
    pub error: Option<String>,
}

/// Preflight validation statistics
#[derive(Debug, Default)]
pub struct FomodPreflightStats {
    pub validated: usize,
    pub valid: usize,
    pub invalid: usize,
    pub skipped: usize,
}

/// Get path to 7z binary
pub fn get_7z_path() -> Result<PathBuf> {
    let exe_path = std::env::current_exe().context("Failed to get executable path")?;
    let exe_dir = exe_path.parent().unwrap_or(Path::new("."));

    let candidates = [
        exe_dir.join("7zzs"),
        exe_dir.join("bin/7zzs"),
        PathBuf::from("bin/7zzs"),
        PathBuf::from("7zzs"),
    ];

    for path in &candidates {
        if path.exists() {
            return Ok(path.clone());
        }
    }

    // Try system PATH
    if let Ok(output) = Command::new("which").arg("7z").output() {
        if output.status.success() {
            let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
            return Ok(PathBuf::from(path));
        }
    }

    bail!("7z binary not found. Please install p7zip or place 7zzs next to the executable.")
}

/// List all files in an archive using 7z
pub fn list_archive_files(archive_path: &Path) -> Result<Vec<ArchiveFileEntry>> {
    let sevenz_path = get_7z_path()?;

    let output = Command::new(&sevenz_path)
        .arg("l")
        .arg("-slt")
        .arg("-ba")
        .arg(archive_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .with_context(|| format!("Failed to list archive: {}", archive_path.display()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "7z list failed: {}",
            stderr.lines().next().unwrap_or("unknown error")
        );
    }

    let mut files = Vec::new();
    let mut current_path: Option<String> = None;
    let mut current_size: u64 = 0;
    let mut is_folder = false;

    for line in String::from_utf8_lossy(&output.stdout).lines() {
        if let Some(path) = line.strip_prefix("Path = ") {
            if let Some(prev_path) = current_path.take() {
                if !is_folder {
                    files.push(ArchiveFileEntry {
                        file_path: prev_path,
                        file_size: current_size,
                    });
                }
            }
            current_path = Some(path.to_string());
            current_size = 0;
            is_folder = false;
        } else if let Some(size) = line.strip_prefix("Size = ") {
            current_size = size.parse().unwrap_or(0);
        } else if let Some(folder) = line.strip_prefix("Folder = ") {
            is_folder = folder.trim() == "+";
        }
    }

    if let Some(path) = current_path {
        if !is_folder {
            files.push(ArchiveFileEntry {
                file_path: path,
                file_size: current_size,
            });
        }
    }

    Ok(files)
}

/// List files in a ZIP archive (faster than 7z for ZIPs)
pub fn list_zip_files(archive_path: &Path) -> Result<Vec<ArchiveFileEntry>> {
    let file = File::open(archive_path)?;
    let reader = BufReader::new(file);
    let mut archive = zip::ZipArchive::new(reader)?;

    let mut files = Vec::new();
    for i in 0..archive.len() {
        let file = archive.by_index_raw(i)?;
        if !file.is_dir() {
            files.push(ArchiveFileEntry {
                file_path: file.name().to_string(),
                file_size: file.size(),
            });
        }
    }

    Ok(files)
}

/// Detect archive type by magic bytes
fn detect_archive_type(path: &Path) -> String {
    if let Ok(mut file) = File::open(path) {
        let mut magic = [0u8; 8];
        if file.read(&mut magic).is_ok() {
            if &magic[0..4] == b"PK\x03\x04" || &magic[0..4] == b"PK\x05\x06" {
                return "zip".to_string();
            }
            if &magic[0..6] == b"7z\xBC\xAF\x27\x1C" {
                return "7z".to_string();
            }
            if &magic[0..4] == b"Rar!" {
                return "rar".to_string();
            }
        }
    }
    path.extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase()
}

/// Index result for collecting from parallel indexing
struct IndexResult {
    md5: String,
    files: Vec<ArchiveFileEntry>,
}

/// Index all downloaded mod archives (parallel, Wabbajack-style)
///
/// Indexes archives in parallel using rayon, then writes results to DB sequentially.
pub fn index_all_archives(
    db: &CollectionDb,
    _downloads_dir: &Path,
) -> Result<usize> {
    let mods = db.get_mods_by_status(ModStatus::Downloaded)?;

    if mods.is_empty() {
        return Ok(0);
    }

    // Filter to mods that need indexing
    let to_index: Vec<_> = mods
        .iter()
        .filter(|m| {
            if let Some(ref path) = m.local_path {
                let archive_path = Path::new(path);
                if !archive_path.exists() {
                    return false;
                }
                // Check if already indexed
                !db.is_archive_indexed(&m.md5).unwrap_or(true)
            } else {
                false
            }
        })
        .collect();

    if to_index.is_empty() {
        return Ok(0);
    }

    info!("Indexing {} archives in parallel...", to_index.len());

    // Setup multi-progress display (Wabbajack-style)
    let mp = MultiProgress::new();
    let overall_pb = mp.add(ProgressBar::new(to_index.len() as u64));
    overall_pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] Indexing [{bar:40.cyan/blue}] {pos}/{len} | {msg}")
            .unwrap()
            .progress_chars("=>-"),
    );
    overall_pb.enable_steady_tick(Duration::from_millis(100));

    let num_threads = rayon::current_num_threads();
    overall_pb.set_message(format!("OK:0 Fail:0 ({} threads)", num_threads));

    let indexed = AtomicUsize::new(0);
    let index_failed = AtomicUsize::new(0);

    let mp = Arc::new(mp);
    let overall_pb = Arc::new(overall_pb);

    // Index archives in parallel - collect results (no DB writes in parallel section)
    let index_results: Vec<Option<IndexResult>> = to_index
        .par_iter()
        .map(|mod_entry| {
            let archive_path = match &mod_entry.local_path {
                Some(p) => PathBuf::from(p),
                None => {
                    index_failed.fetch_add(1, Ordering::Relaxed);
                    overall_pb.inc(1);
                    return None;
                }
            };

            // Create progress spinner for this archive
            let idx_pb = mp.insert_before(&overall_pb, ProgressBar::new_spinner());
            idx_pb.set_style(
                ProgressStyle::default_spinner()
                    .template("  {spinner:.blue} {wide_msg}")
                    .unwrap(),
            );
            idx_pb.enable_steady_tick(Duration::from_millis(100));

            let display_name = if mod_entry.name.len() > 50 {
                format!("{}...", &mod_entry.name[..47])
            } else {
                mod_entry.name.clone()
            };
            idx_pb.set_message(format!("{} (indexing)", display_name));

            let archive_type = detect_archive_type(&archive_path);
            let list_result = match archive_type.as_str() {
                "zip" => list_zip_files(&archive_path),
                _ => list_archive_files(&archive_path),
            };

            idx_pb.finish_and_clear();

            match list_result {
                Ok(files) => {
                    let count = files.len();
                    indexed.fetch_add(1, Ordering::Relaxed);
                    overall_pb.inc(1);
                    overall_pb.println(format!("OK: {} ({} files)", mod_entry.name, count));
                    overall_pb.set_message(format!(
                        "OK:{} Fail:{} ({} threads)",
                        indexed.load(Ordering::Relaxed),
                        index_failed.load(Ordering::Relaxed),
                        num_threads
                    ));
                    Some(IndexResult {
                        md5: mod_entry.md5.clone(),
                        files,
                    })
                }
                Err(e) => {
                    index_failed.fetch_add(1, Ordering::Relaxed);
                    overall_pb.inc(1);
                    overall_pb.println(format!("FAIL: {} - {}", mod_entry.name, e));
                    overall_pb.set_message(format!(
                        "OK:{} Fail:{} ({} threads)",
                        indexed.load(Ordering::Relaxed),
                        index_failed.load(Ordering::Relaxed),
                        num_threads
                    ));
                    None
                }
            }
        })
        .collect();

    overall_pb.finish_and_clear();

    // Write index results to database (sequential)
    let mut indexed_count = 0;
    for result in index_results.into_iter().flatten() {
        if let Err(e) = db.index_archive_files(&result.md5, &result.files) {
            warn!("Failed to store index for {}: {}", result.md5, e);
        } else {
            indexed_count += 1;
        }
    }

    info!("Archive indexing complete ({} indexed)", indexed_count);

    Ok(indexed_count)
}

/// Extract a single file from a ZIP archive
fn extract_from_zip(archive_path: &Path, file_path: &str) -> Result<Vec<u8>> {
    let file = File::open(archive_path)?;
    let reader = BufReader::new(file);
    let mut archive = zip::ZipArchive::new(reader)?;

    // Try exact match first
    if let Ok(mut entry) = archive.by_name(file_path) {
        let mut data = Vec::with_capacity(entry.size() as usize);
        entry.read_to_end(&mut data)?;
        return Ok(data);
    }

    // Try case-insensitive match
    let normalized = file_path.to_lowercase().replace('\\', "/");
    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;
        let entry_normalized = entry.name().to_lowercase().replace('\\', "/");
        if entry_normalized == normalized {
            let mut data = Vec::with_capacity(entry.size() as usize);
            entry.read_to_end(&mut data)?;
            return Ok(data);
        }
    }

    bail!("File '{}' not found in archive", file_path)
}

/// Extract entire archive to a directory using 7z
fn extract_archive_to_dir(archive_path: &Path, output_dir: &Path) -> Result<()> {
    let sevenz_path = get_7z_path()?;

    fs::create_dir_all(output_dir)?;

    let output = Command::new(&sevenz_path)
        .arg("x")
        .arg("-y")
        .arg(format!("-o{}", output_dir.display()))
        .arg(archive_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .with_context(|| format!("Failed to extract {}", archive_path.display()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "7z extraction failed: {}",
            stderr.lines().next().unwrap_or("unknown error")
        );
    }

    // Fix permissions on extracted files (Windows archives may have restrictive permissions)
    fix_permissions_recursive(output_dir)?;

    Ok(())
}

/// Recursively fix permissions on extracted files.
///
/// Windows archives sometimes extract with no read permission on Linux.
/// This ensures all files are readable and all directories are accessible.
fn fix_permissions_recursive(dir: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    for entry in walkdir::WalkDir::new(dir).into_iter().filter_map(|e| e.ok()) {
        let path = entry.path();
        if let Ok(metadata) = fs::metadata(path) {
            let mut perms = metadata.permissions();
            let mode = perms.mode();

            if metadata.is_dir() {
                // Directories need rwx for owner, rx for others (0755)
                if mode & 0o700 != 0o700 {
                    perms.set_mode(mode | 0o755);
                    fs::set_permissions(path, perms).ok();
                }
            } else {
                // Files need rw for owner, r for others (0644)
                if mode & 0o600 != 0o600 {
                    perms.set_mode(mode | 0o644);
                    fs::set_permissions(path, perms).ok();
                }
            }
        }
    }

    Ok(())
}

/// Public wrapper for extracting an archive to a directory.
/// Used by FOMOD processing to extract archives for installer processing.
pub fn extract_archive_to_dir_pub(archive_path: &Path, output_dir: &Path) -> Result<()> {
    extract_archive_to_dir(archive_path, output_dir)
}

/// Preflight validation of all FOMOD configs.
///
/// This function validates FOMOD ModuleConfig.xml files before extraction:
/// - Extracts each FOMOD archive to temp
/// - Finds and parses ModuleConfig.xml
/// - Stores validation result in database
/// - Provides early failure detection with detailed errors
///
/// Run this after archive indexing but before extraction.
pub fn validate_fomod_configs(db: &CollectionDb, temp_base_dir: &Path) -> Result<FomodPreflightStats> {
    // Ensure temp base dir exists
    fs::create_dir_all(temp_base_dir)?;
    let fomod_mods = db.get_fomod_mods_needing_validation()?;

    if fomod_mods.is_empty() {
        info!("No FOMOD configs to validate");
        return Ok(FomodPreflightStats::default());
    }

    info!("Validating {} FOMOD configs...", fomod_mods.len());

    // Setup progress display
    let mp = MultiProgress::new();
    let overall_pb = mp.add(ProgressBar::new(fomod_mods.len() as u64));
    overall_pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] FOMOD Validation [{bar:40.cyan/blue}] {pos}/{len} | {msg}")
            .unwrap()
            .progress_chars("=>-"),
    );
    overall_pb.enable_steady_tick(Duration::from_millis(100));

    let num_threads = rayon::current_num_threads();
    overall_pb.set_message(format!("Valid:0 Invalid:0 ({} threads)", num_threads));

    let valid_count = AtomicUsize::new(0);
    let invalid_count = AtomicUsize::new(0);
    let skipped_count = AtomicUsize::new(0);

    let mp = Arc::new(mp);
    let overall_pb = Arc::new(overall_pb);
    let temp_base = temp_base_dir.to_path_buf();

    // Validate in parallel
    let results: Vec<FomodValidationResult> = fomod_mods
        .par_iter()
        .map(|mod_entry| {
            let archive_path = match &mod_entry.local_path {
                Some(p) => PathBuf::from(p),
                None => {
                    skipped_count.fetch_add(1, Ordering::Relaxed);
                    overall_pb.inc(1);
                    return FomodValidationResult {
                        mod_id: mod_entry.id,
                        mod_name: mod_entry.name.clone(),
                        valid: false,
                        module_name: None,
                        error: Some("No local archive path".to_string()),
                    };
                }
            };

            if !archive_path.exists() {
                skipped_count.fetch_add(1, Ordering::Relaxed);
                overall_pb.inc(1);
                return FomodValidationResult {
                    mod_id: mod_entry.id,
                    mod_name: mod_entry.name.clone(),
                    valid: false,
                    module_name: None,
                    error: Some(format!("Archive not found: {}", archive_path.display())),
                };
            }

            // Create progress spinner for this mod
            let val_pb = mp.insert_before(&overall_pb, ProgressBar::new_spinner());
            val_pb.set_style(
                ProgressStyle::default_spinner()
                    .template("  {spinner:.blue} {wide_msg}")
                    .unwrap(),
            );
            val_pb.enable_steady_tick(Duration::from_millis(100));

            let display_name = if mod_entry.name.len() > 50 {
                format!("{}...", &mod_entry.name[..47])
            } else {
                mod_entry.name.clone()
            };
            val_pb.set_message(format!("{} (validating FOMOD)", display_name));

            // Extract to temp and validate
            let result = validate_single_fomod(&archive_path, mod_entry, &temp_base);

            val_pb.finish_and_clear();

            if result.valid {
                valid_count.fetch_add(1, Ordering::Relaxed);
                overall_pb.println(format!("VALID: {} ({})", mod_entry.name,
                    result.module_name.as_deref().unwrap_or("unknown")));
            } else {
                invalid_count.fetch_add(1, Ordering::Relaxed);
                overall_pb.println(format!("INVALID: {} - {}", mod_entry.name,
                    result.error.as_deref().unwrap_or("unknown error")));
            }

            overall_pb.inc(1);
            overall_pb.set_message(format!(
                "Valid:{} Invalid:{} ({} threads)",
                valid_count.load(Ordering::Relaxed),
                invalid_count.load(Ordering::Relaxed),
                num_threads
            ));

            result
        })
        .collect();

    overall_pb.finish_and_clear();

    // Update database with validation results (sequential)
    for result in &results {
        if result.valid {
            if let Err(e) = db.mark_fomod_valid(result.mod_id, result.module_name.as_deref().unwrap_or("")) {
                warn!("Failed to mark FOMOD valid for {}: {}", result.mod_name, e);
            }
        } else if let Err(e) = db.mark_fomod_invalid(result.mod_id, result.error.as_deref().unwrap_or("Unknown error")) {
            warn!("Failed to mark FOMOD invalid for {}: {}", result.mod_name, e);
        }
    }

    let stats = FomodPreflightStats {
        validated: results.len(),
        valid: valid_count.load(Ordering::Relaxed),
        invalid: invalid_count.load(Ordering::Relaxed),
        skipped: skipped_count.load(Ordering::Relaxed),
    };

    info!(
        "FOMOD validation complete: {} valid, {} invalid, {} skipped",
        stats.valid, stats.invalid, stats.skipped
    );

    Ok(stats)
}

/// Validate a single FOMOD mod's ModuleConfig.xml
fn validate_single_fomod(archive_path: &Path, mod_entry: &ModDbEntry, temp_base_dir: &Path) -> FomodValidationResult {
    // Create temp directory in local folder (not /tmp)
    let temp_dir = match tempfile::tempdir_in(temp_base_dir) {
        Ok(dir) => dir,
        Err(e) => {
            return FomodValidationResult {
                mod_id: mod_entry.id,
                mod_name: mod_entry.name.clone(),
                valid: false,
                module_name: None,
                error: Some(format!("Failed to create temp directory: {}", e)),
            };
        }
    };

    // Extract archive
    if let Err(e) = extract_archive_to_dir(archive_path, temp_dir.path()) {
        return FomodValidationResult {
            mod_id: mod_entry.id,
            mod_name: mod_entry.name.clone(),
            valid: false,
            module_name: None,
            error: Some(format!("Failed to extract archive: {}", e)),
        };
    }

    // Find ModuleConfig.xml
    let (config_path, _data_root) = match find_module_config(temp_dir.path()) {
        Some(paths) => paths,
        None => {
            return FomodValidationResult {
                mod_id: mod_entry.id,
                mod_name: mod_entry.name.clone(),
                valid: false,
                module_name: None,
                error: Some("No fomod/ModuleConfig.xml found in archive".to_string()),
            };
        }
    };

    // Parse the FOMOD config
    match parse_fomod(&config_path) {
        Ok(config) => FomodValidationResult {
            mod_id: mod_entry.id,
            mod_name: mod_entry.name.clone(),
            valid: true,
            module_name: Some(if config.module_name.is_empty() {
                mod_entry.name.clone()
            } else {
                config.module_name
            }),
            error: None,
        },
        Err(e) => {
            // Capture full error chain for debugging
            let mut error_chain = format!("{}", e);
            let mut source = e.source();
            while let Some(s) = source {
                error_chain.push_str(&format!("\n  Caused by: {}", s));
                source = s.source();
            }

            FomodValidationResult {
                mod_id: mod_entry.id,
                mod_name: mod_entry.name.clone(),
                valid: false,
                module_name: None,
                error: Some(format!("FOMOD parse error: {}", error_chain)),
            }
        }
    }
}

/// Extract and route files from a mod archive
///
/// Routing is determined by the mod's deploy_type from the collection:
/// - "dinput" or "enb" → root-level DLLs/EXEs go to Stock Game, Data/ goes to mod folder
/// - "" (empty) → everything goes to mod folder (MO2 virtualizes these)
///
/// After extraction, wrapper folders are detected and unwrapped, and Data/ is flattened.
///
/// Note: This function is designed to be thread-safe. It does NOT access
/// the database directly - archive indexing should be done before calling this.
pub fn extract_and_route_mod(
    mod_entry: &ModDbEntry,
    archive_path: &Path,
    mod_dir: &Path,
    stock_game_dir: &Path,
    _file_router: &FileRouter,
    temp_base_dir: &Path,
) -> Result<ExtractStats> {
    let mut stats = ExtractStats::default();

    // Determine if this mod has root files based on deploy_type from collection
    let is_root_mod = mod_entry.is_root_mod();

    // Create mod directory
    fs::create_dir_all(mod_dir)?;

    let archive_type = detect_archive_type(archive_path);

    // Step 1: Extract to temp directory (in local folder, not /tmp)
    let temp_dir = tempfile::tempdir_in(temp_base_dir)?;

    if archive_type == "zip" {
        extract_zip_to_dir(archive_path, temp_dir.path())?;
    } else {
        extract_archive_to_dir(archive_path, temp_dir.path())?;
    }

    // Step 2: Detect and unwrap wrapper folders
    // Based on reference: only unwrap if exactly 1 folder and no real files
    let content_root = detect_wrapper_folder(temp_dir.path());

    // NOTE: No variant selection - reference implementation doesn't do this.
    // If archive has multiple variant folders, extract all of them.
    // FOMOD installer handles variant selection when choices are provided.

    // Step 3: Copy files from content root to mod folder, routing root files to Stock Game
    for entry in walkdir::WalkDir::new(&content_root)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        if entry.file_type().is_file() {
            let rel_path = entry
                .path()
                .strip_prefix(&content_root)
                .unwrap_or(entry.path());
            let rel_str = rel_path.to_string_lossy();

            // Skip fomod folder - contains only installer metadata, not mod content
            if rel_str.to_lowercase().starts_with("fomod/") || rel_str.to_lowercase().starts_with("fomod\\") {
                continue;
            }

            // Determine where this file should go
            let dest_path = if is_root_mod && is_root_level_file(&rel_str) {
                // Root mod file: determine correct path in game root
                get_root_dest_path(stock_game_dir, &rel_str)
            } else {
                // Everything else: preserve structure in mod folder
                mod_dir.join(rel_path)
            };

            // Create parent dirs and copy
            if let Some(parent) = dest_path.parent() {
                fs::create_dir_all(parent)?;
            }

            fs::copy(entry.path(), &dest_path)?;
            stats.extracted += 1;
        }
    }

    // Step 4: Flatten any remaining Data folder in the mod directory
    flatten_data_folder(mod_dir)?;

    Ok(stats)
}

/// Extract ZIP to a directory (simple extraction, no routing)
fn extract_zip_to_dir(archive_path: &Path, output_dir: &Path) -> Result<()> {
    let file = File::open(archive_path)?;
    let reader = BufReader::new(file);
    let mut archive = zip::ZipArchive::new(reader)?;

    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;
        // Normalize path: Windows ZIPs have backslashes, convert to forward slashes
        let entry_path = entry.name().replace('\\', "/");

        if entry.is_dir() {
            let dir_path = output_dir.join(&entry_path);
            fs::create_dir_all(dir_path)?;
        } else {
            let file_path = output_dir.join(&entry_path);
            if let Some(parent) = file_path.parent() {
                fs::create_dir_all(parent)?;
            }
            let mut data = Vec::with_capacity(entry.size() as usize);
            entry.read_to_end(&mut data)?;
            let mut output = File::create(&file_path)?;
            output.write_all(&data)?;
        }
    }

    Ok(())
}

/// Check if a file should go to game root (for dinput/enb mods)
/// Handles: DLLs, EXEs, ENB configs, and ENB preset folders
fn is_root_level_file(rel_path: &str) -> bool {
    let lower = rel_path.to_lowercase();

    // Get just the filename (last component)
    let filename = lower.rsplit(['/', '\\']).next().unwrap_or(&lower);

    // ENB-specific files that go to game root
    const ENB_ROOT_FILES: &[&str] = &[
        "enbseries.ini",
        "enblocal.ini",
        "enbbloom.fx",
        "enbeffect.fx",
        "enbeffectprepass.fx",
        "enbadaptation.fx",
        "enblens.fx",
        "enbdepthoffield.fx",
        "enbpalette.png",
        "d3dcompiler_46e.dll",
    ];

    // Check for ENB config files at root
    if ENB_ROOT_FILES.contains(&filename) {
        // Must not be inside a subfolder (except wrapper folders)
        // If the path has multiple components and contains data folders, reject
        if lower.contains("data/") || lower.contains("data\\") {
            return false;
        }
        return true;
    }

    // Check for enbseries/ folder contents - these go to game root
    if lower.starts_with("enbseries/") || lower.starts_with("enbseries\\") {
        return true;
    }
    // Also handle wrapper folder case: WrapperName/enbseries/...
    if lower.contains("/enbseries/") || lower.contains("\\enbseries\\") {
        return true;
    }

    // DLLs and EXEs
    if !lower.ends_with(".dll") && !lower.ends_with(".exe") {
        return false;
    }

    // Must NOT be inside these folders (they go to mod folder, not game root)
    const DATA_DIRS: &[&str] = &[
        "data/", "data\\",
        "skse/plugins/", "skse\\plugins\\",  // SKSE plugins go to Data/SKSE/Plugins
        "f4se/plugins/", "f4se\\plugins\\",  // F4SE plugins go to Data/F4SE/Plugins
        "src/", "src\\",
        "source/", "source\\",
        "scripts/", "scripts\\",
    ];

    for dir in DATA_DIRS {
        if lower.contains(dir) {
            return false;
        }
    }

    true
}

/// Get the destination path for a root-level file in the game directory.
/// - For enbseries/ folder contents: preserve the enbseries/... structure
/// - For regular root files (DLLs, configs): flatten to just the filename
fn get_root_dest_path(stock_game_dir: &Path, rel_path: &str) -> PathBuf {
    let lower = rel_path.to_lowercase();

    // Check for enbseries/ folder - preserve structure from enbseries/ onwards
    // Handle both direct (enbseries/...) and wrapper (WrapperName/enbseries/...)
    if let Some(pos) = lower.find("enbseries/") {
        let enb_relative = &rel_path[pos..];
        return stock_game_dir.join(enb_relative);
    }
    if let Some(pos) = lower.find("enbseries\\") {
        let enb_relative = &rel_path[pos..].replace('\\', "/");
        return stock_game_dir.join(enb_relative);
    }

    // Regular root file - flatten to just filename
    let filename = Path::new(rel_path)
        .file_name()
        .map(|f| f.to_string_lossy().to_string())
        .unwrap_or_else(|| rel_path.to_string());
    stock_game_dir.join(filename)
}

// =============================================================================
// WRAPPER FOLDER DETECTION - Based on Collections Manager reference implementation
// =============================================================================

/// Check if folder is the game's "Data" folder (case-insensitive)
fn is_game_data_folder(name: &str) -> bool {
    name.eq_ignore_ascii_case("data")
}
//
// The reference uses a SIMPLE rule:
// Only unwrap if there's EXACTLY ONE folder AND NO real files.
//
// Files that don't prevent unwrapping (ignorable):
// - readme.txt, license.txt, etc.
// - meta.ini
// - Images (png, jpg, etc.) that are previews
//
// This is much simpler than trying to detect "content folders" or "variants".

/// Files that should be ignored when deciding whether to unwrap
/// These are documentation/preview files, not mod content
fn is_ignorable_file(name: &str) -> bool {
    let lower = name.to_lowercase();

    // Ignorable by exact name
    const IGNORE_NAMES: &[&str] = &[
        "meta.ini", "readme.txt", "readme.md", "readme",
        "license.txt", "license.md", "license",
        "changelog.txt", "changelog.md", "credits.txt",
    ];

    if IGNORE_NAMES.iter().any(|&n| lower == n) {
        return true;
    }

    // Ignorable by extension (documentation/preview images)
    const IGNORE_EXTS: &[&str] = &[
        ".txt", ".md", ".pdf", ".doc", ".docx", ".rtf", ".url", ".html",
        ".png", ".jpg", ".jpeg", ".gif", ".bmp", // Preview images
    ];

    IGNORE_EXTS.iter().any(|ext| lower.ends_with(ext))
}

/// Check if a directory contains valid mod content at its top level.
/// Based on MO2's GamebryoModDataChecker::dataLooksValid() approach.
///
/// Returns true if ANY top-level entry is recognized mod content:
/// - A folder matching known game content folders (meshes, textures, skse, etc.)
/// - A file with a known mod extension (.esp, .esm, .bsa, etc.)
fn has_valid_mod_content(dir: &Path) -> bool {
    // Known game content folders (from MO2's possibleFolderNames)
    const VALID_FOLDERS: &[&str] = &[
        "fonts", "interface", "menus", "meshes", "music", "scripts",
        "shaders", "sound", "strings", "textures", "trees", "video",
        "facegen", "materials", "skse", "obse", "mwse", "nvse", "fose",
        "f4se", "sfse", "distantlod", "asi", "skyproc patchers", "tools",
        "mcm", "icons", "bookart", "distantland", "mits", "splash",
        "dllplugins", "calientetools", "netscriptframework", "shadersfx",
        "enbseries", "bodyslide", "seq", "grass", "lodsettings",
        "caliente tools", "docs", "source",
    ];

    // Known mod file extensions (from MO2's possibleFileExtensions)
    const VALID_EXTENSIONS: &[&str] = &[
        "esp", "esm", "esl", "bsa", "ba2", "modgroups", "ini",
    ];

    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return false,
    };

    for entry in entries.filter_map(|e| e.ok()) {
        let name = entry.file_name().to_string_lossy().to_lowercase();

        if entry.path().is_dir() {
            // Skip fomod - it's installer metadata
            if name == "fomod" {
                continue;
            }
            // Check if this folder is known game content
            if VALID_FOLDERS.contains(&name.as_str()) {
                return true;
            }
        } else {
            // Check file extension
            if let Some(ext) = Path::new(&name).extension() {
                let ext_str = ext.to_string_lossy().to_lowercase();
                if VALID_EXTENSIONS.contains(&ext_str.as_str()) {
                    return true;
                }
            }
        }
    }

    false
}

/// Detect and unwrap wrapper folders.
///
/// Based on MO2's InstallerQuick::getSimpleArchiveBase() approach:
/// 1. FIRST check if current level has valid mod content → STOP
/// 2. If no valid content AND exactly 1 folder → UNWRAP and repeat
/// 3. Otherwise → STOP
///
/// Also handles "Data" folder unwrapping specially.
fn detect_wrapper_folder(extracted_path: &Path) -> PathBuf {
    let mut current = extracted_path.to_path_buf();

    // Maximum 3 levels of unwrapping
    for _ in 0..3 {
        // MO2 approach: FIRST check if this level has valid mod content
        if has_valid_mod_content(&current) {
            info!("Found valid mod content at: {}", current.display());
            break; // STOP - this IS the mod data
        }

        // No valid content at this level - check if we can unwrap
        let mut dirs: Vec<PathBuf> = Vec::new();
        let mut has_real_files = false;

        let entries = match fs::read_dir(&current) {
            Ok(e) => e,
            Err(_) => return current,
        };

        for entry in entries.filter_map(|e| e.ok()) {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            let name_lower = name.to_lowercase();

            if path.is_dir() {
                // Skip fomod folder - it's installer metadata, not content
                if name_lower != "fomod" {
                    dirs.push(path);
                }
            } else {
                // Check if this is a real file or ignorable
                if !is_ignorable_file(&name) {
                    has_real_files = true;
                }
            }
        }

        // Only unwrap if exactly 1 dir AND no real files
        if dirs.len() == 1 && !has_real_files {
            let single_dir = &dirs[0];
            let dir_name = single_dir.file_name()
                .map(|n| n.to_string_lossy().to_lowercase())
                .unwrap_or_default();

            // Always unwrap "Data" folder
            if dir_name == "data" {
                info!("Unwrapping Data folder");
                current = single_dir.clone();
                continue;
            }

            // Unwrap wrapper folders (version wrappers like "ModName v1.0")
            info!("Unwrapping wrapper folder: {}", dir_name);
            current = single_dir.clone();
            continue;
        }

        // Multiple dirs or has real files but no valid content - stop
        break;
    }

    current
}

/// Flatten "Data" folder if it exists in the mod root
/// Moves contents of Data/ to root/ and removes Data/
fn flatten_data_folder(mod_root: &Path) -> Result<()> {
    // Find "Data" folder case-insensitively
    let mut data_path: Option<PathBuf> = None;

    if let Ok(entries) = fs::read_dir(mod_root) {
        for entry in entries.filter_map(|e| e.ok()) {
            if entry.path().is_dir() {
                if let Some(name) = entry.file_name().to_str() {
                    if is_game_data_folder(name) {
                        data_path = Some(entry.path());
                        break;
                    }
                }
            }
        }
    }

    let data_path = match data_path {
        Some(p) => p,
        None => return Ok(()), // No Data folder to flatten
    };

    info!("Flattening Data folder in {}", mod_root.display());

    // Move everything from Data/ to root/
    if let Ok(entries) = fs::read_dir(&data_path) {
        for entry in entries.filter_map(|e| e.ok()) {
            let src = entry.path();
            let dst = mod_root.join(src.file_name().unwrap());

            if dst.exists() {
                if src.is_dir() && dst.is_dir() {
                    // Merge directories recursively
                    merge_directories(&src, &dst)?;
                    fs::remove_dir_all(&src)?;
                } else {
                    // Overwrite file
                    fs::rename(&src, &dst)?;
                }
            } else {
                fs::rename(&src, &dst)?;
            }
        }
    }

    // Remove empty Data folder
    let _ = fs::remove_dir(&data_path);

    Ok(())
}

/// Find existing folder with case-insensitive match (like C code)
fn find_existing_folder(dest_dir: &Path, folder_name: &str) -> Option<PathBuf> {
    let name_lower = folder_name.to_lowercase();

    if let Ok(entries) = fs::read_dir(dest_dir) {
        for entry in entries.filter_map(|e| e.ok()) {
            let path = entry.path();
            if path.is_dir() {
                if let Some(entry_name) = path.file_name().and_then(|n| n.to_str()) {
                    if entry_name.to_lowercase() == name_lower {
                        return Some(path);
                    }
                }
            }
        }
    }
    None
}

/// Recursively merge source directory into destination with case-insensitive folder matching
fn merge_directories(src: &Path, dst: &Path) -> Result<()> {
    if let Ok(entries) = fs::read_dir(src) {
        for entry in entries.filter_map(|e| e.ok()) {
            let src_path = entry.path();
            let item_name = match src_path.file_name().and_then(|n| n.to_str()) {
                Some(name) => name,
                None => continue,
            };

            if src_path.is_dir() {
                // Check for case-insensitive match in destination
                let dst_path = if let Some(existing) = find_existing_folder(dst, item_name) {
                    // Merge into existing folder (preserves original case)
                    existing
                } else {
                    // Create new folder
                    let new_dir = dst.join(item_name);
                    fs::create_dir_all(&new_dir)?;
                    new_dir
                };
                merge_directories(&src_path, &dst_path)?;
            } else {
                // Copy file, overwriting if exists
                let dst_path = dst.join(item_name);
                fs::copy(&src_path, &dst_path)?;
            }
        }
    }
    Ok(())
}

/// Hash-based FOMOD fallback installation (matches C code approach)
///
/// For mods that have a FOMOD archive but no choices recorded in the collection,
/// we use the `hashes` array to determine which files should be installed.
/// Extract ZIP with proper routing for root vs data files
fn extract_zip_with_routing(
    archive_path: &Path,
    mod_dir: &Path,
    stock_game_dir: &Path,
    is_root_mod: bool,
) -> Result<ExtractStats> {
    let mut stats = ExtractStats::default();

    let file = File::open(archive_path)?;
    let reader = BufReader::new(file);
    let mut archive = zip::ZipArchive::new(reader)?;

    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;

        if entry.is_dir() {
            continue;
        }

        let entry_path = entry.name().to_string();
        let lower_path = entry_path.to_lowercase();

        // Determine where this file should go
        let dest_path = if is_root_mod && is_root_level_file(&lower_path) {
            // Root mod file: determine correct path in game root
            get_root_dest_path(stock_game_dir, &entry_path)
        } else {
            // Everything else: preserve structure in mod folder
            mod_dir.join(&entry_path)
        };

        // Create parent dirs
        if let Some(parent) = dest_path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Extract file
        let mut data = Vec::with_capacity(entry.size() as usize);
        entry.read_to_end(&mut data)?;

        let mut output = File::create(&dest_path)?;
        output.write_all(&data)?;

        stats.extracted += 1;
    }

    Ok(stats)
}

/// Extract all mods in parallel with progress display (Wabbajack-style)
///
/// This function uses the same pattern as Wabbajack:
/// 1. Pre-fetch mod data from database (sequential)
/// 2. Index archives in parallel using rayon (no DB access)
/// 3. Write index results to database (sequential)
/// 4. Extract archives in parallel using rayon (no DB access)
/// 5. Update database status after extraction (sequential)
pub fn extract_all_mods(
    db: &CollectionDb,
    _downloads_dir: &Path,
    mods_dir: &Path,
    stock_game_dir: &Path,
    file_router: &FileRouter,
    temp_base_dir: &Path,
) -> Result<ExtractStats> {
    // Ensure temp base dir exists
    fs::create_dir_all(temp_base_dir)?;

    // Get mods ready for extraction (Downloaded status = validated, ready to extract)
    let mods = db.get_mods_by_status(ModStatus::Downloaded)?;

    if mods.is_empty() {
        info!("No mods to extract");
        return Ok(ExtractStats::default());
    }

    info!("Extracting {} mods...", mods.len());

    // =========================================================================
    // Phase 1: Parallel archive indexing (Wabbajack-style)
    // =========================================================================

    // Identify archives that need indexing
    let to_index: Vec<_> = mods
        .iter()
        .filter(|m| {
            if let Some(ref path) = m.local_path {
                let archive_path = Path::new(path);
                archive_path.exists() && !db.is_archive_indexed(&m.md5).unwrap_or(true)
            } else {
                false
            }
        })
        .collect();

    let mut indexed_count = 0;

    if !to_index.is_empty() {
        info!("Indexing {} archives in parallel...", to_index.len());

        // Setup multi-progress display for indexing
        let mp = MultiProgress::new();
        let overall_pb = mp.add(ProgressBar::new(to_index.len() as u64));
        overall_pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] Indexing [{bar:40.cyan/blue}] {pos}/{len} | {msg}")
                .unwrap()
                .progress_chars("=>-"),
        );
        overall_pb.enable_steady_tick(Duration::from_millis(100));

        let num_threads = rayon::current_num_threads();
        overall_pb.set_message(format!("OK:0 Fail:0 ({} threads)", num_threads));

        let indexed = AtomicUsize::new(0);
        let index_failed = AtomicUsize::new(0);

        let mp = Arc::new(mp);
        let overall_pb = Arc::new(overall_pb);

        // Index archives in parallel - collect results (no DB writes here)
        let index_results: Vec<Option<IndexResult>> = to_index
            .par_iter()
            .map(|mod_entry| {
                let archive_path = match &mod_entry.local_path {
                    Some(p) => PathBuf::from(p),
                    None => {
                        index_failed.fetch_add(1, Ordering::Relaxed);
                        overall_pb.inc(1);
                        return None;
                    }
                };

                // Create progress spinner for this archive
                let idx_pb = mp.insert_before(&overall_pb, ProgressBar::new_spinner());
                idx_pb.set_style(
                    ProgressStyle::default_spinner()
                        .template("  {spinner:.blue} {wide_msg}")
                        .unwrap(),
                );
                idx_pb.enable_steady_tick(Duration::from_millis(100));

                let display_name = if mod_entry.name.len() > 50 {
                    format!("{}...", &mod_entry.name[..47])
                } else {
                    mod_entry.name.clone()
                };
                idx_pb.set_message(format!("{} (indexing)", display_name));

                let archive_type = detect_archive_type(&archive_path);
                let list_result = match archive_type.as_str() {
                    "zip" => list_zip_files(&archive_path),
                    _ => list_archive_files(&archive_path),
                };

                idx_pb.finish_and_clear();

                match list_result {
                    Ok(files) => {
                        indexed.fetch_add(1, Ordering::Relaxed);
                        overall_pb.inc(1);
                        overall_pb.set_message(format!(
                            "OK:{} Fail:{} ({} threads)",
                            indexed.load(Ordering::Relaxed),
                            index_failed.load(Ordering::Relaxed),
                            num_threads
                        ));
                        Some(IndexResult {
                            md5: mod_entry.md5.clone(),
                            files,
                        })
                    }
                    Err(e) => {
                        index_failed.fetch_add(1, Ordering::Relaxed);
                        overall_pb.inc(1);
                        overall_pb.println(format!("INDEX FAIL {}: {}", mod_entry.name, e));
                        overall_pb.set_message(format!(
                            "OK:{} Fail:{} ({} threads)",
                            indexed.load(Ordering::Relaxed),
                            index_failed.load(Ordering::Relaxed),
                            num_threads
                        ));
                        None
                    }
                }
            })
            .collect();

        overall_pb.finish_and_clear();

        // Write index results to database (sequential)
        for result in index_results.into_iter().flatten() {
            if let Err(e) = db.index_archive_files(&result.md5, &result.files) {
                warn!("Failed to store index for {}: {}", result.md5, e);
            } else {
                indexed_count += 1;
            }
        }

        info!("Archive indexing complete ({} indexed)", indexed_count);
    }

    // =========================================================================
    // Phase 2: Parallel extraction (Wabbajack-style)
    // =========================================================================

    // Setup multi-progress display like Wabbajack
    let mp = MultiProgress::new();
    let overall_pb = mp.add(ProgressBar::new(mods.len() as u64));
    overall_pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] Extracting [{bar:40.cyan/blue}] {pos}/{len} | {msg}")
            .unwrap()
            .progress_chars("=>-"),
    );
    overall_pb.enable_steady_tick(Duration::from_millis(100));

    let num_threads = rayon::current_num_threads();
    overall_pb.set_message(format!("OK:0 Skip:0 Fail:0 ({} threads)", num_threads));

    // Shared counters
    let extracted = AtomicUsize::new(0);
    let skipped = AtomicUsize::new(0);
    let failed = AtomicUsize::new(0);

    // Wrap for sharing across threads
    let mp = Arc::new(mp);
    let overall_pb = Arc::new(overall_pb);
    let temp_base = temp_base_dir.to_path_buf();

    // Collect results from parallel extraction (db_id, success)
    let results: Vec<(i64, bool)> = mods
        .par_iter()
        .map(|mod_entry| {
            let archive_path = match &mod_entry.local_path {
                Some(p) => PathBuf::from(p),
                None => {
                    failed.fetch_add(1, Ordering::Relaxed);
                    overall_pb.inc(1);
                    return (mod_entry.id, false);
                }
            };

            if !archive_path.exists() {
                failed.fetch_add(1, Ordering::Relaxed);
                overall_pb.inc(1);
                overall_pb.println(format!("FAIL {}: archive not found", mod_entry.name));
                return (mod_entry.id, false);
            }

            // Skip FOMOD mods WITH choices - they will be processed separately in Phase F
            if mod_entry.has_fomod() {
                skipped.fetch_add(1, Ordering::Relaxed);
                overall_pb.inc(1);
                overall_pb.set_message(format!(
                    "OK:{} Skip:{} Fail:{} ({} threads)",
                    extracted.load(Ordering::Relaxed),
                    skipped.load(Ordering::Relaxed),
                    failed.load(Ordering::Relaxed),
                    num_threads
                ));
                // Return success=true so status is set to Extracted (ready for FOMOD processing)
                return (mod_entry.id, true);
            }

            // Create progress bar for this mod
            let mod_pb = mp.insert_before(&overall_pb, ProgressBar::new_spinner());
            mod_pb.set_style(
                ProgressStyle::default_spinner()
                    .template("  {spinner:.blue} {wide_msg}")
                    .unwrap(),
            );
            mod_pb.enable_steady_tick(Duration::from_millis(100));

            let display_name = if mod_entry.name.len() > 50 {
                format!("{}...", &mod_entry.name[..47])
            } else {
                mod_entry.name.clone()
            };

            let mod_dir = mods_dir.join(&mod_entry.folder_name);

            // Always use standard extraction - hash-based extraction causes path issues
            // because collection JSON hashes often have incomplete paths
            mod_pb.set_message(format!("{} (extracting)", display_name));

            let success = match extract_and_route_mod(
                mod_entry,
                &archive_path,
                &mod_dir,
                stock_game_dir,
                file_router,
                &temp_base,
            ) {
                Ok(_stats) => {
                    extracted.fetch_add(1, Ordering::Relaxed);
                    mod_pb.finish_and_clear();
                    true
                }
                Err(e) => {
                    failed.fetch_add(1, Ordering::Relaxed);
                    mod_pb.finish_and_clear();
                    overall_pb.println(format!("FAIL {}: {}", mod_entry.name, e));
                    false
                }
            };

            overall_pb.inc(1);
            overall_pb.set_message(format!(
                "OK:{} Skip:{} Fail:{} ({} threads)",
                extracted.load(Ordering::Relaxed),
                skipped.load(Ordering::Relaxed),
                failed.load(Ordering::Relaxed),
                num_threads
            ));

            (mod_entry.id, success)
        })
        .collect();

    overall_pb.finish_and_clear();

    // Update database status after parallel extraction completes
    for (db_id, success) in &results {
        let status = if *success {
            ModStatus::Extracted
        } else {
            ModStatus::Failed
        };
        if let Err(e) = db.update_mod_status(*db_id, status) {
            warn!("Failed to update mod status for {}: {}", db_id, e);
        }
    }

    let stats = ExtractStats {
        extracted: extracted.load(Ordering::Relaxed),
        skipped: skipped.load(Ordering::Relaxed),
        failed: failed.load(Ordering::Relaxed),
        indexed: indexed_count,
    };

    info!(
        "Extraction complete: {} extracted, {} skipped, {} failed",
        stats.extracted, stats.skipped, stats.failed
    );

    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_archive_type() {
        // This test requires actual files, so just test the fallback
        assert_eq!(detect_archive_type(Path::new("test.zip")), "zip");
        assert_eq!(detect_archive_type(Path::new("test.7z")), "7z");
        assert_eq!(detect_archive_type(Path::new("test.rar")), "rar");
    }
}
