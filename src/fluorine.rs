//! Fluorine Manager integration.
//!
//! Fluorine is a Linux port of MO2 (https://github.com/SulfurNitride/Fluorine-Manager).
//! After a successful Wabbajack install we can register the install directory
//! as a "portable instance" so Fluorine picks it up automatically in its
//! sidebar. The registration is just a string-list entry in Fluorine's QSettings
//! INI file — no IPC, no API.
//!
//! This module also auto-downloads the latest Fluorine release when the user
//! wants to integrate but doesn't have it installed yet.

#![allow(dead_code)]

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

/// Owner/repo on GitHub.
const REPO: &str = "SulfurNitride/Fluorine-Manager";

/// Where Fluorine stores its global QSettings INI (where `PortableInstances=` lives).
/// Hard-coded because Fluorine sets `setApplicationName("ModOrganizer")` with no
/// organization override on Linux, so Qt picks `Mod Organizer Team` by default.
fn fluorine_settings_path() -> Result<PathBuf> {
    let config = dirs::config_dir().context("Could not determine $XDG_CONFIG_HOME")?;
    Ok(config.join("Mod Organizer Team").join("Mod Organizer.conf"))
}

/// Default location for auto-installed Fluorine releases.
fn default_install_root() -> Result<PathBuf> {
    let data = dirs::data_local_dir().context("Could not determine $XDG_DATA_HOME")?;
    Ok(data.join("fluorine-manager"))
}

/// A detected Fluorine install.
#[derive(Debug, Clone)]
pub struct FluorineInstall {
    /// Absolute path to the `fluorine-manager` executable.
    pub binary: PathBuf,
    /// Where we found it (for status display).
    pub source: &'static str,
}

/// Detect Fluorine on disk. Checks (in order):
/// 1. The explicit `Settings::fluorine_path` override (if it points at a dir or binary).
/// 2. `fluorine-manager` on `$PATH`.
/// 3. The default auto-install dir at `~/.local/share/fluorine-manager/`.
/// 4. `~/Applications/fluorine-manager/`.
pub fn detect(override_path: Option<&str>) -> Option<FluorineInstall> {
    if let Some(p) = override_path.filter(|s| !s.is_empty()) {
        if let Some(install) = check_path(Path::new(p), "settings override") {
            return Some(install);
        }
    }

    if let Ok(bin) = which::which("fluorine-manager") {
        return Some(FluorineInstall {
            binary: bin,
            source: "$PATH",
        });
    }

    if let Ok(root) = default_install_root() {
        if let Some(install) = check_path(&root, "auto-install dir") {
            return Some(install);
        }
    }

    if let Some(home) = dirs::home_dir() {
        let candidate = home.join("Applications").join("fluorine-manager");
        if let Some(install) = check_path(&candidate, "~/Applications") {
            return Some(install);
        }
    }

    None
}

/// Resolve a path that may be a directory containing `fluorine-manager` or the
/// binary itself.
fn check_path(path: &Path, source: &'static str) -> Option<FluorineInstall> {
    if !path.exists() {
        return None;
    }
    if path.is_file() {
        return Some(FluorineInstall {
            binary: path.to_path_buf(),
            source,
        });
    }
    // Directory: scan one level deep for the binary. Releases extract into a
    // top-level "fluorine-manager-X.Y.Z" folder, so check both the dir itself
    // and any subdirectory.
    for candidate in std::iter::once(path.to_path_buf()).chain(
        path.read_dir().ok().into_iter().flatten().filter_map(|e| {
            let e = e.ok()?;
            e.file_type().ok()?.is_dir().then(|| e.path())
        }),
    ) {
        let bin = candidate.join("fluorine-manager");
        if bin.is_file() {
            return Some(FluorineInstall {
                binary: bin,
                source,
            });
        }
    }
    None
}

/// Append `install_path` to Fluorine's `PortableInstances` list. Idempotent —
/// existing entries aren't duplicated. The install dir must already contain a
/// `ModOrganizer.ini` (Fluorine ignores entries without one).
pub fn register_portable_instance(install_path: &Path, make_current: bool) -> Result<()> {
    let install_path = install_path
        .canonicalize()
        .unwrap_or_else(|_| install_path.to_path_buf());
    let install_str = install_path.to_string_lossy().to_string();

    let settings_path = fluorine_settings_path()?;
    if let Some(parent) = settings_path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("Failed to create {:?}", parent))?;
    }

    let mut sections: Vec<IniSection> = if settings_path.exists() {
        parse_ini(&settings_path)?
    } else {
        Vec::new()
    };

    let general = ensure_section(&mut sections, "General");

    update_string_list_entry(general, "PortableInstances", &install_str);
    if make_current {
        set_or_replace(general, "CurrentInstance", &install_str);
    }

    write_ini(&settings_path, &sections)?;
    tracing::info!(
        "Registered '{}' as a Fluorine portable instance in {}",
        install_str,
        settings_path.display()
    );
    Ok(())
}

/// Download and extract the latest Fluorine release to the auto-install dir.
/// Returns the install root containing the binary.
pub async fn download_latest(dest_root: Option<PathBuf>) -> Result<PathBuf> {
    let dest = match dest_root {
        Some(p) => p,
        None => default_install_root()?,
    };

    let release = fetch_latest_release().await?;
    let asset = release
        .assets
        .iter()
        .find(|a| a.name.ends_with(".tar.gz"))
        .ok_or_else(|| anyhow!("Latest Fluorine release has no .tar.gz asset"))?;

    tracing::info!(
        "Downloading Fluorine {} ({}, {} MiB) → {}",
        release.tag_name,
        asset.name,
        asset.size / (1024 * 1024),
        dest.display()
    );

    let client = reqwest::Client::builder()
        .user_agent("clf3")
        .build()
        .context("Failed to build reqwest client")?;

    let bytes = client
        .get(&asset.browser_download_url)
        .send()
        .await
        .context("Fluorine download failed")?
        .error_for_status()
        .context("Fluorine download returned non-2xx")?
        .bytes()
        .await
        .context("Failed to read Fluorine download body")?;

    fs::create_dir_all(&dest).with_context(|| format!("Failed to create {:?}", dest))?;
    extract_tar_gz(&bytes, &dest).context("Failed to extract Fluorine release")?;

    // The release extracts into a single top-level dir like
    // "fluorine-manager-0.2.0/". Hoist its binary path so the caller knows
    // where to point at.
    let install = check_path(&dest, "fresh download").ok_or_else(|| {
        anyhow!(
            "Extracted Fluorine archive but couldn't find the binary in {}",
            dest.display()
        )
    })?;

    // Make sure it's executable (tar usually preserves the bit but `umask`
    // can chew it).
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&install.binary)?.permissions();
        perms.set_mode(perms.mode() | 0o755);
        fs::set_permissions(&install.binary, perms)?;
    }

    Ok(install
        .binary
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or(dest))
}

#[derive(Debug, Deserialize)]
struct GitHubRelease {
    tag_name: String,
    assets: Vec<GitHubAsset>,
}

#[derive(Debug, Deserialize)]
struct GitHubAsset {
    name: String,
    size: u64,
    browser_download_url: String,
}

async fn fetch_latest_release() -> Result<GitHubRelease> {
    let url = format!("https://api.github.com/repos/{}/releases/latest", REPO);
    let client = reqwest::Client::builder()
        .user_agent("clf3")
        .build()
        .context("Failed to build reqwest client")?;
    let release: GitHubRelease = client
        .get(&url)
        .header("Accept", "application/vnd.github+json")
        .send()
        .await
        .context("GitHub releases query failed")?
        .error_for_status()
        .context("GitHub releases query returned non-2xx")?
        .json()
        .await
        .context("Failed to decode GitHub releases JSON")?;
    Ok(release)
}

fn extract_tar_gz(bytes: &[u8], dest: &Path) -> Result<()> {
    let gz = flate2::read::GzDecoder::new(bytes);
    let mut archive = tar::Archive::new(gz);
    archive.unpack(dest)?;
    Ok(())
}

// --- Minimal QSettings-flavored INI handling ---------------------------------
//
// Fluorine's conf file is a Qt-flavored INI:
//
//   [General]
//   CurrentInstance=/home/u/Games/BG3
//   PortableInstances=/home/u/A, /home/u/B with space, /home/u/C
//
// QStringList entries are comma-space separated and quote-wrapped only when
// they contain commas. We don't roundtrip exotic Qt escapes — we only touch
// the two keys we care about and preserve every other line verbatim.

#[derive(Debug, Default)]
struct IniSection {
    /// Section name without brackets. The implicit leading section before any
    /// `[Section]` header uses an empty string and is rarely needed for Qt files.
    name: String,
    /// Raw lines inside this section. Each line is either `key=value` or a
    /// comment / blank line preserved as-is.
    lines: Vec<String>,
}

fn parse_ini(path: &Path) -> Result<Vec<IniSection>> {
    let file = fs::File::open(path).with_context(|| format!("Failed to open {:?}", path))?;
    let reader = BufReader::new(file);
    let mut sections: Vec<IniSection> = Vec::new();
    let mut current = IniSection::default();

    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim_start();
        if trimmed.starts_with('[') && trimmed.contains(']') {
            // Push the previous section (even if empty/unnamed — header order matters).
            sections.push(std::mem::take(&mut current));
            let name = trimmed
                .trim()
                .trim_start_matches('[')
                .trim_end_matches(']')
                .to_string();
            current.name = name;
        } else {
            current.lines.push(line);
        }
    }
    sections.push(current);
    Ok(sections)
}

fn write_ini(path: &Path, sections: &[IniSection]) -> Result<()> {
    let mut out = String::new();
    for (i, section) in sections.iter().enumerate() {
        if !section.name.is_empty() {
            out.push('[');
            out.push_str(&section.name);
            out.push_str("]\n");
        }
        for line in &section.lines {
            out.push_str(line);
            out.push('\n');
        }
        // Separate non-final sections with a blank line if they didn't already
        // end with one. Matches Qt's own writer style.
        if i + 1 < sections.len() && !section.lines.last().map(|l| l.is_empty()).unwrap_or(true) {
            out.push('\n');
        }
    }

    let mut file =
        fs::File::create(path).with_context(|| format!("Failed to create {:?}", path))?;
    file.write_all(out.as_bytes())
        .with_context(|| format!("Failed to write {:?}", path))?;
    Ok(())
}

fn ensure_section<'a>(sections: &'a mut Vec<IniSection>, name: &str) -> &'a mut IniSection {
    if let Some(pos) = sections.iter().position(|s| s.name == name) {
        return &mut sections[pos];
    }
    sections.push(IniSection {
        name: name.to_string(),
        lines: Vec::new(),
    });
    sections.last_mut().expect("just pushed")
}

/// Add `value` to a comma-space separated string-list key in this section.
/// Creates the key if missing. Skips if the value is already present.
fn update_string_list_entry(section: &mut IniSection, key: &str, value: &str) {
    for line in section.lines.iter_mut() {
        if let Some(rest) = key_match(line, key) {
            let mut entries: Vec<String> = if rest.trim().is_empty() {
                Vec::new()
            } else {
                rest.split(", ").map(|s| s.trim().to_string()).collect()
            };
            if !entries.iter().any(|e| e == value) {
                entries.push(value.to_string());
                *line = format!("{}={}", key, entries.join(", "));
            }
            return;
        }
    }
    section.lines.push(format!("{}={}", key, value));
}

/// Set or replace a single-value key in this section.
fn set_or_replace(section: &mut IniSection, key: &str, value: &str) {
    for line in section.lines.iter_mut() {
        if key_match(line, key).is_some() {
            *line = format!("{}={}", key, value);
            return;
        }
    }
    section.lines.push(format!("{}={}", key, value));
}

/// If `line` is `key=value`, return Some(value). Otherwise None.
fn key_match<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    let (lhs, rhs) = line.split_once('=')?;
    if lhs.trim() == key {
        Some(rhs)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn appends_new_portable_instance() {
        let dir = tempdir().unwrap();
        let ini_path = dir.path().join("Mod Organizer.conf");
        fs::write(
            &ini_path,
            "[General]\nCurrentInstance=/old\nPortableInstances=/a, /b\n",
        )
        .unwrap();

        let mut sections = parse_ini(&ini_path).unwrap();
        let g = ensure_section(&mut sections, "General");
        update_string_list_entry(g, "PortableInstances", "/c");
        write_ini(&ini_path, &sections).unwrap();

        let out = fs::read_to_string(&ini_path).unwrap();
        assert!(out.contains("PortableInstances=/a, /b, /c"));
        assert!(out.contains("CurrentInstance=/old"));
    }

    #[test]
    fn idempotent_register() {
        let dir = tempdir().unwrap();
        let ini_path = dir.path().join("Mod Organizer.conf");
        fs::write(&ini_path, "[General]\nPortableInstances=/x\n").unwrap();

        for _ in 0..3 {
            let mut sections = parse_ini(&ini_path).unwrap();
            let g = ensure_section(&mut sections, "General");
            update_string_list_entry(g, "PortableInstances", "/x");
            write_ini(&ini_path, &sections).unwrap();
        }

        let out = fs::read_to_string(&ini_path).unwrap();
        assert_eq!(out.matches("/x").count(), 1);
    }

    #[test]
    fn creates_general_when_missing() {
        let dir = tempdir().unwrap();
        let ini_path = dir.path().join("Mod Organizer.conf");
        // Empty file.
        fs::write(&ini_path, "").unwrap();

        let mut sections = parse_ini(&ini_path).unwrap();
        let g = ensure_section(&mut sections, "General");
        update_string_list_entry(g, "PortableInstances", "/only");
        write_ini(&ini_path, &sections).unwrap();

        let out = fs::read_to_string(&ini_path).unwrap();
        assert!(out.contains("[General]"));
        assert!(out.contains("PortableInstances=/only"));
    }
}
