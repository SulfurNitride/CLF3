//! Tale of Two Wastelands (TTW) integration
//!
//! Handles detection, verification, and installation of TTW
//! for modlists that require it.
//!
//! ## Workflow
//!
//! 1. **Precheck** - When modlist is loaded, detect TTW requirement
//! 2. **Notify user** - Show that TTW is required, prompt for MPI file path
//! 3. **Auto-detect FO3** - Use game finder to locate Fallout 3
//! 4. **Install modlist** - Normal Wabbajack installation proceeds
//! 5. **Install TTW** - After modlist completes, run TTW installer
//! 6. **Update modlist.txt** - Add `+TTW Output` at top of load order

use anyhow::{bail, Context, Result};
use std::fs;
use std::io::{BufRead, BufReader};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use tracing::{info, warn};

use crate::game_finder::{detect_all_games, GameScanResult};

/// GitHub repo for TTW Linux Installer
const TTW_INSTALLER_REPO: &str = "SulfurNitride/TTW_Linux_Installer";
/// Binary name to look for in releases
const TTW_INSTALLER_BINARY: &str = "mpi_installer";

/// TTW mod folder name in the MO2 mods directory
pub const TTW_MOD_NAME: &str = "TTW Output";

/// Fallout 3 Steam App IDs
const FO3_STEAM_IDS: &[&str] = &["22300", "22370"]; // Base and GOTY

/// Fallout New Vegas Steam App ID
const FNV_STEAM_ID: &str = "22380";

/// TTW precheck result - shown to user before installation starts
#[derive(Debug, Clone)]
pub struct TtwPrecheck {
    /// Whether TTW is required for this modlist
    pub required: bool,
    /// TTW markers found in the modlist
    pub markers_found: Vec<String>,
    /// Auto-detected Fallout 3 installation (if found)
    pub fo3_detected: Option<PathBuf>,
    /// Auto-detected Fallout New Vegas installation (if found)
    pub fnv_detected: Option<PathBuf>,
    /// User-provided MPI file path (to be filled in)
    pub mpi_path: Option<PathBuf>,
    /// Message to display to user
    pub message: String,
}

impl TtwPrecheck {
    /// Check if ready to proceed (have all required paths)
    pub fn is_ready(&self) -> bool {
        if !self.required {
            return true;
        }
        self.mpi_path.is_some() && self.fo3_detected.is_some() && self.fnv_detected.is_some()
    }

    /// Get what's still missing
    pub fn missing(&self) -> Vec<&'static str> {
        if !self.required {
            return vec![];
        }

        let mut missing = vec![];
        if self.mpi_path.is_none() {
            missing.push("TTW MPI file");
        }
        if self.fo3_detected.is_none() {
            missing.push("Fallout 3 installation");
        }
        if self.fnv_detected.is_none() {
            missing.push("Fallout New Vegas installation");
        }
        missing
    }
}

/// Perform TTW precheck on a modlist
///
/// Call this when the modlist is first loaded to determine if TTW is needed
/// and what paths are available.
pub fn precheck_ttw(modlist: &crate::modlist::Modlist) -> TtwPrecheck {
    let ttw_result = modlist.requires_ttw();

    if !ttw_result.required {
        return TtwPrecheck {
            required: false,
            markers_found: vec![],
            fo3_detected: None,
            fnv_detected: None,
            mpi_path: None,
            message: String::new(),
        };
    }

    // Scan for installed games
    let games = detect_all_games();
    let fo3_detected = find_fallout3(&games);
    let fnv_detected = find_fallout_nv(&games);

    // Build user message
    let mut message = String::from("⚠️  This modlist requires Tale of Two Wastelands (TTW)\n\n");

    message.push_str(&format!(
        "Detected via: {}\n\n",
        ttw_result.markers_found.join(", ")
    ));

    if let Some(ref path) = fo3_detected {
        message.push_str(&format!("✓ Fallout 3 found: {}\n", path.display()));
    } else {
        message.push_str("✗ Fallout 3 not found - please install it first\n");
    }

    if let Some(ref path) = fnv_detected {
        message.push_str(&format!("✓ Fallout New Vegas found: {}\n", path.display()));
    } else {
        message.push_str("✗ Fallout New Vegas not found\n");
    }

    message.push_str("\n📁 Please provide the path to your TTW MPI file.\n");
    message.push_str("   TTW will be installed after the modlist completes.\n");

    TtwPrecheck {
        required: true,
        markers_found: ttw_result.markers_found,
        fo3_detected,
        fnv_detected,
        mpi_path: None,
        message,
    }
}

/// Find Fallout 3 installation from detected games
pub fn find_fallout3(games: &GameScanResult) -> Option<PathBuf> {
    // Try by app ID first (more reliable)
    for app_id in FO3_STEAM_IDS {
        if let Some(game) = games.find_by_app_id(app_id) {
            return Some(game.install_path.clone());
        }
    }

    // Fallback to name search
    if let Some(game) = games.find_by_name("Fallout 3") {
        return Some(game.install_path.clone());
    }
    if let Some(game) = games.find_by_name("Fallout 3 GOTY") {
        return Some(game.install_path.clone());
    }

    None
}

/// Find Fallout New Vegas installation from detected games
pub fn find_fallout_nv(games: &GameScanResult) -> Option<PathBuf> {
    if let Some(game) = games.find_by_app_id(FNV_STEAM_ID) {
        return Some(game.install_path.clone());
    }

    if let Some(game) = games.find_by_name("Fallout New Vegas") {
        return Some(game.install_path.clone());
    }

    None
}

/// Expected TTW output files that should exist after installation
const TTW_EXPECTED_FILES: &[&str] = &["TaleOfTwoWastelands.esm", "TaleOfTwoWastelands - Main.bsa"];

/// Verify that a TTW output directory contains valid TTW files
pub fn verify_ttw_output(path: &Path) -> bool {
    if !path.exists() {
        return false;
    }

    for expected in TTW_EXPECTED_FILES {
        let file_path = path.join(expected);
        if !file_path.exists() {
            info!("TTW output missing expected file: {}", expected);
            return false;
        }
    }

    info!("TTW output directory verified: {}", path.display());
    true
}

/// Get the TTW Output mod path within an install directory
pub fn ttw_mod_path(install_dir: &Path) -> PathBuf {
    install_dir.join("mods").join(TTW_MOD_NAME)
}

/// Check if TTW is already installed in the mods folder
pub fn is_ttw_installed(install_dir: &Path) -> bool {
    verify_ttw_output(&ttw_mod_path(install_dir))
}

/// Run the TTW MPI installer
///
/// Installs TTW to `{install_dir}/mods/TTW Output/`
///
/// # Arguments
/// * `mpi_path` - Path to the TTW .mpi file
/// * `fo3_path` - Path to Fallout 3 installation
/// * `fnv_path` - Path to Fallout New Vegas installation
/// * `install_dir` - Modlist installation directory
/// * `installer_path` - Path to TTW installer binary (optional, auto-detects if None)
pub fn install_ttw(
    mpi_path: &Path,
    fo3_path: &Path,
    fnv_path: &Path,
    install_dir: &Path,
    installer_path: Option<&Path>,
) -> Result<PathBuf> {
    let dest_path = ttw_mod_path(install_dir);

    // Check if already installed
    if verify_ttw_output(&dest_path) {
        info!("TTW already installed at {}", dest_path.display());
        return Ok(dest_path);
    }

    info!("Installing TTW...");
    info!("  MPI: {}", mpi_path.display());
    info!("  FO3: {}", fo3_path.display());
    info!("  FNV: {}", fnv_path.display());
    info!("  Dest: {}", dest_path.display());

    // Verify MPI file exists
    if !mpi_path.exists() {
        bail!("TTW MPI file not found: {}", mpi_path.display());
    }

    // Verify game paths exist
    if !fo3_path.exists() {
        bail!("Fallout 3 path not found: {}", fo3_path.display());
    }
    if !fnv_path.exists() {
        bail!("Fallout New Vegas path not found: {}", fnv_path.display());
    }

    // Create destination directory
    fs::create_dir_all(&dest_path).with_context(|| {
        format!(
            "Failed to create TTW output directory: {}",
            dest_path.display()
        )
    })?;

    // Find installer binary
    let installer = find_ttw_installer(installer_path)?;
    info!("  Installer: {}", installer.display());

    // Run the installer
    // Command: mpi_installer install --mpi <path> --fo3 <path> --fnv <path> --dest <path>
    info!("Running TTW installer:");
    info!(
        "  {} install --mpi {} --fo3 {} --fnv {} --dest {}",
        installer.display(),
        mpi_path.display(),
        fo3_path.display(),
        fnv_path.display(),
        dest_path.display()
    );

    let output = Command::new(&installer)
        .arg("install")
        .arg("--mpi")
        .arg(mpi_path)
        .arg("--fo3")
        .arg(fo3_path)
        .arg("--fnv")
        .arg(fnv_path)
        .arg("--dest")
        .arg(&dest_path)
        .output()
        .with_context(|| format!("Failed to execute TTW installer: {}", installer.display()))?;

    // Log stdout
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        if !line.trim().is_empty() {
            info!("[TTW] {}", line);
        }
    }

    // Log stderr
    let stderr = String::from_utf8_lossy(&output.stderr);
    for line in stderr.lines() {
        if !line.trim().is_empty() {
            warn!("[TTW] {}", line);
        }
    }

    if !output.status.success() {
        let error_msg = if !stderr.is_empty() {
            stderr.to_string()
        } else if !stdout.is_empty() {
            stdout.to_string()
        } else {
            format!("exit code {:?}", output.status.code())
        };
        bail!("TTW installer failed: {}", error_msg.trim());
    }

    // Verify installation
    if !verify_ttw_output(&dest_path) {
        bail!("TTW installation completed but output verification failed");
    }

    info!("TTW installation completed successfully");
    Ok(dest_path)
}

/// Run the TTW MPI installer with real-time progress streaming
///
/// Same as `install_ttw` but streams output line-by-line to a callback
/// so the GUI can display progress during the long-running installation.
///
/// # Arguments
/// * `mpi_path` - Path to the TTW .mpi file
/// * `fo3_path` - Path to Fallout 3 installation
/// * `fnv_path` - Path to Fallout New Vegas installation
/// * `install_dir` - Modlist installation directory
/// * `installer_path` - Path to TTW installer binary (optional, auto-detects if None)
/// * `progress_callback` - Called for each line of output (for GUI updates)
pub fn install_ttw_with_progress<F>(
    mpi_path: &Path,
    fo3_path: &Path,
    fnv_path: &Path,
    install_dir: &Path,
    installer_path: Option<&Path>,
    mut progress_callback: F,
) -> Result<PathBuf>
where
    F: FnMut(&str),
{
    let dest_path = ttw_mod_path(install_dir);

    // Check if already installed
    if verify_ttw_output(&dest_path) {
        info!("TTW already installed at {}", dest_path.display());
        progress_callback("TTW already installed, skipping...");
        return Ok(dest_path);
    }

    info!("Installing TTW...");
    info!("  MPI: {}", mpi_path.display());
    info!("  FO3: {}", fo3_path.display());
    info!("  FNV: {}", fnv_path.display());
    info!("  Dest: {}", dest_path.display());

    progress_callback("Starting TTW installation (this may take 5-20 minutes)...");

    // Verify MPI file exists
    if !mpi_path.exists() {
        bail!("TTW MPI file not found: {}", mpi_path.display());
    }

    // Verify game paths exist
    if !fo3_path.exists() {
        bail!("Fallout 3 path not found: {}", fo3_path.display());
    }
    if !fnv_path.exists() {
        bail!("Fallout New Vegas path not found: {}", fnv_path.display());
    }

    // Create destination directory
    fs::create_dir_all(&dest_path).with_context(|| {
        format!(
            "Failed to create TTW output directory: {}",
            dest_path.display()
        )
    })?;

    // Find installer binary
    let installer = find_ttw_installer(installer_path)?;
    info!("  Installer: {}", installer.display());

    progress_callback(&format!("Using TTW installer: {}", installer.display()));

    // Run the installer with piped stdout/stderr for real-time streaming
    info!("Running TTW installer:");
    info!(
        "  {} install --mpi {} --fo3 {} --fnv {} --dest {}",
        installer.display(),
        mpi_path.display(),
        fo3_path.display(),
        fnv_path.display(),
        dest_path.display()
    );

    let mut child = Command::new(&installer)
        .arg("install")
        .arg("--mpi")
        .arg(mpi_path)
        .arg("--fo3")
        .arg(fo3_path)
        .arg("--fnv")
        .arg(fnv_path)
        .arg("--dest")
        .arg(&dest_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("Failed to execute TTW installer: {}", installer.display()))?;

    // Stream stdout in real-time
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    // Read stdout line by line
    if let Some(stdout) = stdout {
        let reader = BufReader::new(stdout);
        for line in reader.lines() {
            if let Ok(line) = line {
                let trimmed = line.trim();
                if !trimmed.is_empty() {
                    info!("[TTW] {}", trimmed);
                    // Parse progress from TTW installer output
                    // Format: "[HH:MM:SS] Assets: X/Y - BSA N/M: ..."
                    if trimmed.contains("Assets:")
                        || trimmed.contains("Writing BSA")
                        || trimmed.contains("Installation complete")
                        || trimmed.contains("Processing")
                        || trimmed.contains("Extracting")
                        || trimmed.contains("checks passed")
                    {
                        // Extract just the message part (after timestamp)
                        let msg = if trimmed.starts_with('[') {
                            trimmed
                                .find(']')
                                .map(|i| &trimmed[i + 1..])
                                .unwrap_or(trimmed)
                                .trim()
                        } else {
                            trimmed
                        };
                        progress_callback(msg);
                    }
                }
            }
        }
    }

    // Read any remaining stderr
    if let Some(stderr) = stderr {
        let reader = BufReader::new(stderr);
        for line in reader.lines() {
            if let Ok(line) = line {
                let trimmed = line.trim();
                if !trimmed.is_empty() {
                    warn!("[TTW] {}", trimmed);
                    progress_callback(&format!("Warning: {}", trimmed));
                }
            }
        }
    }

    // Wait for process to complete
    let status = child.wait().context("Failed to wait for TTW installer")?;

    if !status.success() {
        bail!("TTW installer failed with exit code {:?}", status.code());
    }

    // Verify installation
    if !verify_ttw_output(&dest_path) {
        bail!("TTW installation completed but output verification failed");
    }

    info!("TTW installation completed successfully");
    progress_callback("TTW installation completed successfully!");
    Ok(dest_path)
}

/// Get the path where we cache the TTW installer
fn ttw_installer_cache_path() -> PathBuf {
    dirs::data_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("clf3")
        .join("bin")
        .join(TTW_INSTALLER_BINARY)
}

/// Find the TTW installer binary, downloading if necessary
fn find_ttw_installer(explicit_path: Option<&Path>) -> Result<PathBuf> {
    // Use explicit path if provided
    if let Some(path) = explicit_path {
        if path.exists() {
            return Ok(path.to_path_buf());
        }
        bail!(
            "TTW installer not found at specified path: {}",
            path.display()
        );
    }

    // Check common locations first
    let candidates = [
        // In PATH
        which::which(TTW_INSTALLER_BINARY).ok(),
        // Relative to clf3
        std::env::current_exe()
            .ok()
            .and_then(|p| p.parent().map(|d| d.join(TTW_INSTALLER_BINARY))),
        // Home directory
        dirs::home_dir().map(|h| h.join(".local/bin").join(TTW_INSTALLER_BINARY)),
        // Common install location
        Some(PathBuf::from("/usr/local/bin").join(TTW_INSTALLER_BINARY)),
        // Our cache location
        Some(ttw_installer_cache_path()),
    ];

    for candidate in candidates.into_iter().flatten() {
        if candidate.exists() {
            info!("Found TTW installer at: {}", candidate.display());
            return Ok(candidate);
        }
    }

    // Not found locally - download from GitHub
    info!("TTW installer not found locally, downloading from GitHub...");
    download_ttw_installer()
}

/// Download the latest TTW installer from GitHub releases
fn download_ttw_installer() -> Result<PathBuf> {
    let cache_path = ttw_installer_cache_path();

    // Create parent directory
    if let Some(parent) = cache_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create cache directory: {}", parent.display()))?;
    }

    // Get latest release info from GitHub API
    let api_url = format!(
        "https://api.github.com/repos/{}/releases/latest",
        TTW_INSTALLER_REPO
    );

    info!("Fetching release info from: {}", api_url);

    let client = reqwest::blocking::Client::builder()
        .user_agent("clf3")
        .build()
        .context("Failed to create HTTP client")?;

    let response = client
        .get(&api_url)
        .send()
        .context("Failed to fetch GitHub release info")?;

    if !response.status().is_success() {
        bail!("GitHub API returned error: {}", response.status());
    }

    let release: serde_json::Value = response
        .json()
        .context("Failed to parse GitHub release JSON")?;

    // Find the Linux asset (it's a zip file)
    let assets = release["assets"]
        .as_array()
        .context("No assets in release")?;

    let linux_asset = assets
        .iter()
        .find(|a| {
            let name = a["name"].as_str().unwrap_or("");
            name.contains("linux") && name.ends_with(".zip")
        })
        .context("No Linux zip found in release assets")?;

    let download_url = linux_asset["browser_download_url"]
        .as_str()
        .context("No download URL for asset")?;
    let asset_name = linux_asset["name"]
        .as_str()
        .unwrap_or("mpi-installer-linux.zip");

    info!("Downloading {} from: {}", asset_name, download_url);

    // Download to a temp file
    let temp_zip = cache_path.with_extension("zip");

    let mut response = client
        .get(download_url)
        .send()
        .context("Failed to download TTW installer")?;

    if !response.status().is_success() {
        bail!("Download failed: {}", response.status());
    }

    // Write ZIP to temp file
    {
        let mut file = fs::File::create(&temp_zip)
            .with_context(|| format!("Failed to create temp file: {}", temp_zip.display()))?;

        std::io::copy(&mut response, &mut file).context("Failed to write TTW installer zip")?;

        file.sync_all().context("Failed to sync file")?;
    }

    info!("Downloaded zip to: {}", temp_zip.display());

    // Extract the binary from the ZIP
    let zip_file = fs::File::open(&temp_zip).context("Failed to open downloaded zip")?;
    let mut archive = zip::ZipArchive::new(zip_file).context("Failed to read zip archive")?;

    // Extract both mpi_installer and tools/xdelta3
    let cache_dir = cache_path.parent().unwrap();
    let tools_dir = cache_dir.join("tools");
    fs::create_dir_all(&tools_dir)?;

    let mut found_binary = false;
    for i in 0..archive.len() {
        let mut file = archive.by_index(i)?;
        let name = file.name().to_string();

        // Extract mpi_installer binary (CLI version, not GUI)
        if name == "mpi_installer" || name.ends_with("/mpi_installer") {
            info!("Extracting: {} -> {}", name, cache_path.display());

            let mut outfile = fs::File::create(&cache_path)
                .with_context(|| format!("Failed to create: {}", cache_path.display()))?;
            std::io::copy(&mut file, &mut outfile)?;
            outfile.sync_all()?;
            found_binary = true;
        }

        // Extract xdelta3 to tools folder
        if name == "tools/xdelta3" || name.ends_with("/xdelta3") {
            let xdelta_path = tools_dir.join("xdelta3");
            info!("Extracting: {} -> {}", name, xdelta_path.display());

            let mut outfile = fs::File::create(&xdelta_path)
                .with_context(|| format!("Failed to create: {}", xdelta_path.display()))?;
            std::io::copy(&mut file, &mut outfile)?;
            outfile.sync_all()?;

            // Make xdelta3 executable
            #[cfg(unix)]
            {
                let mut perms = fs::metadata(&xdelta_path)?.permissions();
                perms.set_mode(0o755);
                fs::set_permissions(&xdelta_path, perms)?;
            }
        }
    }

    // Clean up temp zip
    let _ = fs::remove_file(&temp_zip);

    if !found_binary {
        bail!("Could not find mpi_installer binary in the downloaded zip");
    }

    // Make executable on Unix
    #[cfg(unix)]
    {
        let mut perms = fs::metadata(&cache_path)?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&cache_path, perms)?;
    }

    // Small delay to ensure filesystem has released the file
    std::thread::sleep(std::time::Duration::from_millis(100));

    info!("TTW installer extracted to: {}", cache_path.display());

    // Verify the download is valid (try to run --version or --help)
    let check = Command::new(&cache_path).arg("--version").output();

    match check {
        Ok(output) if output.status.success() => {
            let version = String::from_utf8_lossy(&output.stdout);
            info!("TTW installer version: {}", version.trim());
        }
        Ok(output) => {
            // --version might not be supported, try --help
            let help_check = Command::new(&cache_path).arg("--help").output();
            if help_check.is_err() {
                warn!(
                    "Could not verify TTW installer (exit code: {:?})",
                    output.status.code()
                );
            }
        }
        Err(e) => {
            // Remove invalid download
            let _ = fs::remove_file(&cache_path);
            bail!("Downloaded TTW installer is not executable: {}", e);
        }
    }

    Ok(cache_path)
}

/// Check if TTW installer needs updating (compares with latest GitHub release)
pub fn check_ttw_installer_update() -> Result<Option<String>> {
    let cache_path = ttw_installer_cache_path();

    if !cache_path.exists() {
        return Ok(Some("Not installed".to_string()));
    }

    // Get current version
    let current = Command::new(&cache_path)
        .arg("--version")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .unwrap_or_default();

    // Get latest version from GitHub
    let api_url = format!(
        "https://api.github.com/repos/{}/releases/latest",
        TTW_INSTALLER_REPO
    );

    let client = reqwest::blocking::Client::builder()
        .user_agent("clf3")
        .build()?;

    let response = client.get(&api_url).send()?;
    if !response.status().is_success() {
        return Ok(None);
    }

    let release: serde_json::Value = response.json()?;
    let latest_tag = release["tag_name"].as_str().unwrap_or("");

    if !current.contains(latest_tag) && !latest_tag.is_empty() {
        Ok(Some(format!("Update available: {}", latest_tag)))
    } else {
        Ok(None)
    }
}

/// Add TTW Output to the top of modlist.txt
///
/// MO2 modlist.txt format:
/// - Lines starting with `+` are enabled mods
/// - Lines starting with `-` are disabled mods
/// - Lines starting with `*` are separators
/// - Top = lowest priority, Bottom = highest priority
///
/// TTW Output should be at the top (after separators) so TTW patches can override it.
pub fn add_ttw_to_modlist(install_dir: &Path, profile_name: &str) -> Result<()> {
    let modlist_path = install_dir
        .join("profiles")
        .join(profile_name)
        .join("modlist.txt");

    info!("Adding TTW Output to modlist: {}", modlist_path.display());

    // Read existing modlist
    let content = if modlist_path.exists() {
        fs::read_to_string(&modlist_path)
            .with_context(|| format!("Failed to read modlist: {}", modlist_path.display()))?
    } else {
        String::new()
    };

    let ttw_entry = format!("+{}", TTW_MOD_NAME);

    // Check if already present
    if content.lines().any(|line| {
        let trimmed = line.trim();
        trimmed == ttw_entry || trimmed == format!("-{}", TTW_MOD_NAME)
    }) {
        info!("TTW Output already in modlist");
        // Make sure it's enabled
        let updated = content
            .lines()
            .map(|line| {
                if line.trim() == format!("-{}", TTW_MOD_NAME) {
                    ttw_entry.as_str()
                } else {
                    line
                }
            })
            .collect::<Vec<_>>()
            .join("\n");

        fs::write(&modlist_path, updated + "\n")
            .with_context(|| format!("Failed to write modlist: {}", modlist_path.display()))?;

        return Ok(());
    }

    // Add TTW Output at the bottom (highest priority in MO2)
    let mut new_content = content.clone();

    // Ensure there's a newline before adding
    if !new_content.ends_with('\n') && !new_content.is_empty() {
        new_content.push('\n');
    }

    // Append TTW Output at the end
    new_content.push_str(&ttw_entry);
    new_content.push('\n');

    // Ensure parent directory exists
    if let Some(parent) = modlist_path.parent() {
        fs::create_dir_all(parent)?;
    }

    fs::write(&modlist_path, new_content)
        .with_context(|| format!("Failed to write modlist: {}", modlist_path.display()))?;

    info!(
        "Added +{} to modlist (at bottom for highest priority)",
        TTW_MOD_NAME
    );
    Ok(())
}

/// Complete TTW setup: install and update modlist
///
/// Call this AFTER modlist installation completes.
pub fn finalize_ttw(
    precheck: &TtwPrecheck,
    install_dir: &Path,
    profile_name: &str,
    installer_path: Option<&Path>,
) -> Result<PathBuf> {
    if !precheck.required {
        bail!("TTW is not required for this modlist");
    }

    let mpi_path = precheck
        .mpi_path
        .as_ref()
        .context("MPI path not provided")?;
    let fo3_path = precheck
        .fo3_detected
        .as_ref()
        .context("Fallout 3 path not detected")?;
    let fnv_path = precheck
        .fnv_detected
        .as_ref()
        .context("Fallout New Vegas path not detected")?;

    // Install TTW
    let ttw_path = install_ttw(mpi_path, fo3_path, fnv_path, install_dir, installer_path)?;

    // Add to modlist
    add_ttw_to_modlist(install_dir, profile_name)?;

    Ok(ttw_path)
}

/// Simplified TTW finalization that takes paths directly
///
/// This is a convenience wrapper for GUI use that doesn't require building a TtwPrecheck.
pub fn finalize_ttw_from_paths(
    install_dir: &Path,
    mpi_path: &Path,
    fo3_path: &Path,
    fnv_path: &Path,
) -> Result<PathBuf> {
    info!("Installing TTW from paths:");
    info!("  MPI: {}", mpi_path.display());
    info!("  FO3: {}", fo3_path.display());
    info!("  FNV: {}", fnv_path.display());
    info!("  Install dir: {}", install_dir.display());

    // Validate paths exist
    if !mpi_path.exists() {
        bail!("MPI file not found: {}", mpi_path.display());
    }
    if !fo3_path.exists() {
        bail!("Fallout 3 path not found: {}", fo3_path.display());
    }
    if !fnv_path.exists() {
        bail!("Fallout New Vegas path not found: {}", fnv_path.display());
    }

    // Install TTW (no external installer, uses Wine/Proton internally if needed)
    let ttw_path = install_ttw(mpi_path, fo3_path, fnv_path, install_dir, None)?;

    // Add to modlist - find the first profile with a modlist.txt
    let profiles_dir = install_dir.join("profiles");
    if profiles_dir.exists() {
        if let Ok(entries) = fs::read_dir(&profiles_dir) {
            for entry in entries.flatten() {
                let profile_path = entry.path();
                if profile_path.is_dir() {
                    let modlist_file = profile_path.join("modlist.txt");
                    if modlist_file.exists() {
                        if let Some(profile_name) = profile_path.file_name() {
                            let profile_name = profile_name.to_string_lossy();
                            info!("Found profile: {}", profile_name);
                            if let Err(e) = add_ttw_to_modlist(install_dir, &profile_name) {
                                warn!("Failed to add TTW to profile {}: {}", profile_name, e);
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(ttw_path)
}

/// Simplified TTW finalization with progress callback for GUI
///
/// Same as `finalize_ttw_from_paths` but streams progress updates.
pub fn finalize_ttw_from_paths_with_progress<F>(
    install_dir: &Path,
    mpi_path: &Path,
    fo3_path: &Path,
    fnv_path: &Path,
    progress_callback: F,
) -> Result<PathBuf>
where
    F: FnMut(&str),
{
    info!("Installing TTW from paths:");
    info!("  MPI: {}", mpi_path.display());
    info!("  FO3: {}", fo3_path.display());
    info!("  FNV: {}", fnv_path.display());
    info!("  Install dir: {}", install_dir.display());

    // Validate paths exist
    if !mpi_path.exists() {
        bail!("MPI file not found: {}", mpi_path.display());
    }
    if !fo3_path.exists() {
        bail!("Fallout 3 path not found: {}", fo3_path.display());
    }
    if !fnv_path.exists() {
        bail!("Fallout New Vegas path not found: {}", fnv_path.display());
    }

    // Install TTW with progress streaming
    let ttw_path = install_ttw_with_progress(
        mpi_path,
        fo3_path,
        fnv_path,
        install_dir,
        None,
        progress_callback,
    )?;

    // Add to modlist - find the first profile with a modlist.txt
    let profiles_dir = install_dir.join("profiles");
    if profiles_dir.exists() {
        if let Ok(entries) = fs::read_dir(&profiles_dir) {
            for entry in entries.flatten() {
                let profile_path = entry.path();
                if profile_path.is_dir() {
                    let modlist_file = profile_path.join("modlist.txt");
                    if modlist_file.exists() {
                        if let Some(profile_name) = profile_path.file_name() {
                            let profile_name = profile_name.to_string_lossy();
                            info!("Found profile: {}", profile_name);
                            if let Err(e) = add_ttw_to_modlist(install_dir, &profile_name) {
                                warn!("Failed to add TTW to profile {}: {}", profile_name, e);
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(ttw_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    #[test]
    fn test_precheck_not_required() {
        // Create a minimal modlist without TTW markers
        let modlist = crate::modlist::Modlist {
            name: "Test".to_string(),
            author: "Test".to_string(),
            description: "Test".to_string(),
            version: "1.0".to_string(),
            wabbajack_version: "3.0".to_string(),
            game_type: "SkyrimSE".to_string(),
            website: String::new(),
            readme: String::new(),
            image: String::new(),
            is_nsfw: false,
            archives: vec![],
            directives: vec![],
        };

        let precheck = precheck_ttw(&modlist);
        assert!(!precheck.required);
        assert!(precheck.is_ready());
    }

    #[test]
    fn test_add_ttw_to_empty_modlist() {
        let temp = TempDir::new().unwrap();
        let profile_dir = temp.path().join("profiles").join("Default");
        fs::create_dir_all(&profile_dir).unwrap();

        add_ttw_to_modlist(temp.path(), "Default").unwrap();

        let content = fs::read_to_string(profile_dir.join("modlist.txt")).unwrap();
        assert!(content.contains("+TTW Output"));
    }

    #[test]
    fn test_add_ttw_to_existing_modlist() {
        let temp = TempDir::new().unwrap();
        let profile_dir = temp.path().join("profiles").join("Default");
        fs::create_dir_all(&profile_dir).unwrap();

        // Create existing modlist with some mods
        let modlist = profile_dir.join("modlist.txt");
        let mut f = fs::File::create(&modlist).unwrap();
        writeln!(f, "*Separator").unwrap();
        writeln!(f, "+Some Mod").unwrap();
        writeln!(f, "+Another Mod").unwrap();

        add_ttw_to_modlist(temp.path(), "Default").unwrap();

        let content = fs::read_to_string(&modlist).unwrap();
        let lines: Vec<&str> = content.lines().collect();

        // TTW Output should be at the bottom (highest priority)
        assert_eq!(lines[0], "*Separator");
        assert_eq!(lines[1], "+Some Mod");
        assert_eq!(lines[2], "+Another Mod");
        assert_eq!(lines[3], "+TTW Output");
    }

    #[test]
    fn test_ttw_mod_path() {
        let install_dir = Path::new("/home/user/modlist");
        let expected = PathBuf::from("/home/user/modlist/mods/TTW Output");
        assert_eq!(ttw_mod_path(install_dir), expected);
    }

    #[test]
    fn test_precheck_missing() {
        let modlist = crate::modlist::Modlist {
            name: "Test".to_string(),
            author: "Test".to_string(),
            description: "Test".to_string(),
            version: "1.0".to_string(),
            wabbajack_version: "3.0".to_string(),
            game_type: "FalloutNewVegas".to_string(),
            website: String::new(),
            readme: String::new(),
            image: String::new(),
            is_nsfw: false,
            archives: vec![],
            // Add a directive that references TTW
            directives: vec![crate::modlist::Directive::InlineFile(
                crate::modlist::InlineFileDirective {
                    to: "mods/TTW Patch/TaleOfTwoWastelands.esm".to_string(),
                    hash: "abc".to_string(),
                    size: 100,
                    source_data_id: uuid::Uuid::new_v4(),
                },
            )],
        };

        let precheck = precheck_ttw(&modlist);
        assert!(precheck.required);
        assert!(!precheck.is_ready()); // Missing MPI path at minimum

        let missing = precheck.missing();
        assert!(missing.contains(&"TTW MPI file"));
    }
}
