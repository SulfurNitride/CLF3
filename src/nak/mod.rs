//! NaK Integration Module
//!
//! Downloads and runs NaK for post-installation MO2 setup with Steam/Proton integration.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::io::Write;
use anyhow::{Context, Result, bail};

/// GitHub release info
#[derive(Debug, serde::Deserialize)]
struct GithubRelease {
    tag_name: String,
    assets: Vec<GithubAsset>,
}

#[derive(Debug, serde::Deserialize)]
struct GithubAsset {
    name: String,
    browser_download_url: String,
}

/// NaK binary manager
pub struct NakManager {
    cache_dir: PathBuf,
}

impl NakManager {
    /// Create a new NaK manager with the default cache directory
    pub fn new() -> Result<Self> {
        let cache_dir = dirs::cache_dir()
            .unwrap_or_else(|| PathBuf::from("/tmp"))
            .join("clf3")
            .join("nak");

        std::fs::create_dir_all(&cache_dir)?;

        Ok(Self { cache_dir })
    }

    /// Get the path to the NaK binary, downloading if necessary
    pub fn ensure_nak(&self) -> Result<PathBuf> {
        let nak_path = self.cache_dir.join("nak");

        // Check if we have a cached version
        if nak_path.exists() {
            // Check if it's recent (less than 24 hours old)
            if let Ok(metadata) = std::fs::metadata(&nak_path) {
                if let Ok(modified) = metadata.modified() {
                    let age = std::time::SystemTime::now()
                        .duration_since(modified)
                        .unwrap_or_default();

                    // If less than 24 hours old, use cached version
                    if age.as_secs() < 24 * 60 * 60 {
                        return Ok(nak_path);
                    }
                }
            }
        }

        // Download latest version
        self.download_latest()?;

        Ok(nak_path)
    }

    /// Force download the latest NaK release
    pub fn download_latest(&self) -> Result<PathBuf> {
        eprintln!("[NaK] Fetching latest release info...");

        // Fetch release info from GitHub API using blocking reqwest
        let client = reqwest::blocking::Client::new();
        let release: GithubRelease = client
            .get("https://api.github.com/repos/SulfurNitride/NaK/releases/latest")
            .header("User-Agent", "CLF3-Modlist-Installer")
            .send()
            .context("Failed to fetch NaK release info")?
            .json()
            .context("Failed to parse release info")?;

        eprintln!("[NaK] Latest version: {}", release.tag_name);

        // Find the Linux binary asset
        let asset = release.assets.iter()
            .find(|a| a.name == "nak" || a.name.contains("linux"))
            .context("No Linux binary found in NaK release")?;

        eprintln!("[NaK] Downloading: {}", asset.name);

        // Download the asset
        let response = client
            .get(&asset.browser_download_url)
            .header("User-Agent", "CLF3-Modlist-Installer")
            .send()
            .context("Failed to download NaK")?;

        let bytes = response.bytes().context("Failed to read NaK binary")?;
        let nak_path = self.cache_dir.join("nak");

        // Handle different archive formats
        if asset.name.ends_with(".zip") {
            // Extract ZIP
            let temp_path = self.cache_dir.join("nak.zip");
            let mut file = std::fs::File::create(&temp_path)?;
            file.write_all(&bytes)?;

            let zip_file = std::fs::File::open(&temp_path)?;
            let mut archive = zip::ZipArchive::new(zip_file)?;

            // Look for 'nak' binary in the archive
            for i in 0..archive.len() {
                let mut file = archive.by_index(i)?;
                let name = file.name().to_string();

                if name == "nak" || name.ends_with("/nak") {
                    let mut outfile = std::fs::File::create(&nak_path)?;
                    std::io::copy(&mut file, &mut outfile)?;
                    break;
                }
            }

            // Clean up
            std::fs::remove_file(&temp_path).ok();
        } else if asset.name.ends_with(".tar.gz") {
            // Extract tar.gz
            let temp_path = self.cache_dir.join("nak.tar.gz");
            let mut file = std::fs::File::create(&temp_path)?;
            file.write_all(&bytes)?;

            let tar_gz = std::fs::File::open(&temp_path)?;
            let tar = flate2::read::GzDecoder::new(tar_gz);
            let mut archive = tar::Archive::new(tar);
            archive.unpack(&self.cache_dir)?;

            // Clean up
            std::fs::remove_file(&temp_path).ok();
        } else {
            // Direct binary download
            let mut file = std::fs::File::create(&nak_path)?;
            file.write_all(&bytes)?;
        }

        // Make executable
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&nak_path)?.permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&nak_path, perms)?;
        }

        eprintln!("[NaK] Downloaded to: {}", nak_path.display());

        Ok(nak_path)
    }

    /// Get the current cached version, if any
    pub fn cached_version(&self) -> Option<String> {
        let nak_path = self.cache_dir.join("nak");
        if !nak_path.exists() {
            return None;
        }

        // Try to get version by running nak --version
        let output = Command::new(&nak_path)
            .arg("--version")
            .output()
            .ok()?;

        if output.status.success() {
            let version = String::from_utf8_lossy(&output.stdout);
            Some(version.trim().to_string())
        } else {
            Some("unknown".to_string())
        }
    }

    /// Run NaK to set up MO2 with Steam integration
    pub fn setup_mo2(
        &self,
        mo2_path: &Path,
        shortcut_name: &str,
        proton_name: &str,
    ) -> Result<SetupResult> {
        let nak_path = self.ensure_nak()?;

        eprintln!("[NaK] Setting up MO2 at: {}", mo2_path.display());
        eprintln!("[NaK] Shortcut name: {}", shortcut_name);
        eprintln!("[NaK] Proton: {}", proton_name);

        // Run NaK with working directory set to MO2 path (so log files go there)
        let output = Command::new(&nak_path)
            .arg("setup-mo2")
            .arg("--path")
            .arg(mo2_path)
            .arg("--name")
            .arg(shortcut_name)
            .arg("--proton")
            .arg(proton_name)
            .current_dir(mo2_path)  // Logs will be created in MO2 directory
            .output()
            .context("Failed to run NaK")?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        eprintln!("[NaK] stdout: {}", stdout);
        if !stderr.is_empty() {
            eprintln!("[NaK] stderr: {}", stderr);
        }

        if !output.status.success() {
            bail!("NaK setup failed: {}", stderr);
        }

        // Parse output to extract app ID and prefix path
        let mut app_id = String::new();
        let mut prefix_path = PathBuf::new();

        for line in stdout.lines() {
            if line.contains("Steam AppID:") {
                app_id = line.split(':').last().unwrap_or("").trim().to_string();
            } else if line.contains("Prefix path:") {
                prefix_path = PathBuf::from(line.split(':').last().unwrap_or("").trim());
            }
        }

        // Post-process the INI file to fix any paths with single backslashes
        let ini_path = mo2_path.join("ModOrganizer.ini");
        if ini_path.exists() {
            if let Err(e) = Self::fix_ini_paths(&ini_path) {
                eprintln!("[NaK] Warning: Could not fix INI paths: {}", e);
            } else {
                eprintln!("[NaK] Fixed INI path escaping");
            }
        }

        Ok(SetupResult {
            app_id,
            prefix_path,
            output: stdout.to_string(),
        })
    }

    /// Fix paths in MO2 INI file - convert single backslashes to double backslashes
    /// This is needed because NaK doesn't fix all paths (e.g., download_directory)
    fn fix_ini_paths(ini_path: &Path) -> Result<()> {
        use std::io::Write;

        let content = std::fs::read_to_string(ini_path)
            .context("Failed to read INI file")?;

        let mut fixed_lines = Vec::new();
        let mut changes_made = false;

        for line in content.lines() {
            // Skip empty lines and comments
            if line.trim().is_empty() || line.trim().starts_with(';') || line.trim().starts_with('#') {
                fixed_lines.push(line.to_string());
                continue;
            }

            // Check if this line contains a path with Z: or a Linux path
            if let Some(eq_pos) = line.find('=') {
                let key = &line[..eq_pos];
                let value = &line[eq_pos + 1..];

                // Check if value looks like a Windows/Wine path that needs fixing
                // Look for patterns like Z:\path or paths with single backslashes
                if (value.contains("Z:") || value.contains("z:")) && value.contains('\\') && !value.contains("\\\\") {
                    // This path has single backslashes - need to double them
                    // But be careful not to double already-doubled backslashes
                    let fixed_value = value
                        .replace("\\", "\\\\")
                        // Handle @ByteArray() format - it might already have escaped quotes
                        .replace("\\\\\"", "\\\"");  // Don't over-escape quotes

                    if fixed_value != value {
                        fixed_lines.push(format!("{}={}", key, fixed_value));
                        changes_made = true;
                        eprintln!("[NaK] Fixed path in {}: {} -> {}", key, value, fixed_value);
                        continue;
                    }
                }
            }

            fixed_lines.push(line.to_string());
        }

        if changes_made {
            let mut file = std::fs::File::create(ini_path)
                .context("Failed to write INI file")?;
            for line in fixed_lines {
                writeln!(file, "{}", line)?;
            }
        }

        Ok(())
    }

    /// Check if NaK can detect Steam
    pub fn check_steam(&self) -> Result<bool> {
        let nak_path = self.ensure_nak()?;

        let output = Command::new(&nak_path)
            .arg("check-steam")
            .output()
            .context("Failed to run NaK check-steam")?;

        Ok(output.status.success() &&
           String::from_utf8_lossy(&output.stdout).contains("Steam found"))
    }

    /// List available Protons via NaK
    pub fn list_protons(&self) -> Result<Vec<String>> {
        let nak_path = self.ensure_nak()?;

        let output = Command::new(&nak_path)
            .arg("list-protons")
            .output()
            .context("Failed to run NaK list-protons")?;

        if !output.status.success() {
            bail!("NaK list-protons failed");
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let protons: Vec<String> = stdout
            .lines()
            .filter(|line| line.trim().starts_with(char::is_numeric))
            .filter_map(|line| {
                // Parse "  0. GE-Proton10-29 (Custom)"
                line.split('.').nth(1).map(|s| {
                    s.split('(').next().unwrap_or(s).trim().to_string()
                })
            })
            .collect();

        Ok(protons)
    }

    /// Restart Steam to pick up new shortcuts
    pub fn restart_steam(&self) -> Result<()> {
        eprintln!("[NaK] Restarting Steam...");

        // Kill Steam gracefully first
        let _ = Command::new("pkill")
            .arg("-TERM")
            .arg("steam")
            .status();

        // Wait a moment for Steam to close
        std::thread::sleep(std::time::Duration::from_secs(2));

        // Force kill if still running
        let _ = Command::new("pkill")
            .arg("-9")
            .arg("steam")
            .status();

        std::thread::sleep(std::time::Duration::from_secs(1));

        // Restart Steam
        // Try common Steam paths
        let steam_paths = [
            "/usr/bin/steam",
            "/usr/games/steam",
            "steam",  // Let PATH find it
        ];

        for path in &steam_paths {
            if let Ok(child) = Command::new(path)
                .spawn()
            {
                eprintln!("[NaK] Steam restarted via {}", path);
                // Don't wait for Steam to finish, just detach
                std::mem::forget(child);
                return Ok(());
            }
        }

        // Also try flatpak Steam
        if let Ok(child) = Command::new("flatpak")
            .args(["run", "com.valvesoftware.Steam"])
            .spawn()
        {
            eprintln!("[NaK] Steam restarted via Flatpak");
            std::mem::forget(child);
            return Ok(());
        }

        bail!("Could not restart Steam - please restart it manually")
    }
}

/// Result from MO2 setup
#[derive(Debug)]
pub struct SetupResult {
    pub app_id: String,
    pub prefix_path: PathBuf,
    pub output: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nak_manager_creation() {
        let manager = NakManager::new();
        assert!(manager.is_ok());
    }

    #[test]
    fn test_download_nak() {
        let manager = NakManager::new().expect("Failed to create NaK manager");
        println!("Cache dir: {:?}", manager.cache_dir);

        match manager.download_latest() {
            Ok(path) => {
                println!("Downloaded NaK to: {}", path.display());
                assert!(path.exists(), "NaK binary should exist");

                // Try to get version
                if let Some(version) = manager.cached_version() {
                    println!("NaK version: {}", version);
                }
            }
            Err(e) => {
                println!("Download failed (might be network issue): {}", e);
            }
        }
    }

    #[test]
    fn test_fetch_nak_release_info() {
        // Test that we can fetch the release info from GitHub
        let client = reqwest::blocking::Client::new();
        let result = client
            .get("https://api.github.com/repos/SulfurNitride/NaK/releases/latest")
            .header("User-Agent", "CLF3-Modlist-Installer")
            .send();

        match result {
            Ok(response) => {
                println!("Response status: {}", response.status());
                if response.status().is_success() {
                    let release: Result<GithubRelease, _> = response.json();
                    match release {
                        Ok(rel) => {
                            println!("Latest NaK version: {}", rel.tag_name);
                            println!("Assets: {:?}", rel.assets.iter().map(|a| &a.name).collect::<Vec<_>>());
                            assert!(!rel.tag_name.is_empty());
                        }
                        Err(e) => {
                            println!("Failed to parse release (might be no releases yet): {}", e);
                        }
                    }
                } else {
                    println!("GitHub returned: {} (might be no releases yet)", response.status());
                }
            }
            Err(e) => {
                println!("Network error (might be offline): {}", e);
            }
        }
    }
}
