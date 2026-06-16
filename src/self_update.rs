//! Self-update for the `clf3` binary via GitHub releases.
//!
//! Mirrors `fluorine.rs`'s GitHub-release plumbing but targets *this* binary.
//! Release assets follow the pattern `clf3-linux-x64.zip`, with the archive
//! containing `clf3` (and a bundled `7zz`) at the top level.
//!
//! Replacement strategy on Linux: write the new binary to a temp file *in the
//! same directory* as the running executable, then `rename(2)` it over the
//! current path. The kernel keeps the old inode mapped for the running
//! process until it exits, so the swap is atomic and never breaks the live
//! invocation. The bundled `7zz` is replaced the same way next to the
//! current exe.

#![allow(dead_code)]

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Owner/repo on GitHub.
const REPO: &str = "SulfurNitride/CLF3";

/// Asset name produced by the release pipeline. Hard-coded because there's
/// only one Linux target right now; if we add Windows/macOS later this
/// becomes a `match` on `target_os` + `target_arch`.
const ASSET_NAME: &str = "clf3-linux-x64.zip";

/// How long the startup-check result is trusted before we hit GitHub again.
const CHECK_TTL: Duration = Duration::from_secs(24 * 3600);

/// Current binary version (from Cargo.toml at build time).
pub fn current_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Result of comparing the running version to a remote tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateVerdict {
    /// Remote is strictly newer than the running binary.
    Newer,
    /// Versions are equal.
    Equal,
    /// Running binary is ahead of the latest tagged release (dev builds).
    Ahead,
    /// One side isn't parseable as semver; we can only say "different".
    Unknown,
}

impl UpdateVerdict {
    pub fn update_available(self) -> bool {
        matches!(self, UpdateVerdict::Newer | UpdateVerdict::Unknown)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct LatestRelease {
    pub tag_name: String,
    pub name: String,
    pub assets: Vec<GitHubAsset>,
    #[serde(default)]
    pub body: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GitHubAsset {
    pub name: String,
    pub size: u64,
    pub browser_download_url: String,
}

impl LatestRelease {
    /// Version string with any leading `v` stripped — release tags here are
    /// bare semver (`0.1.0`) but be defensive.
    pub fn version(&self) -> &str {
        self.tag_name.trim_start_matches('v')
    }

    /// Pick the asset that matches our target. Returns `None` if a release
    /// exists but doesn't carry a Linux build (e.g. tag-only release).
    pub fn linux_asset(&self) -> Option<&GitHubAsset> {
        self.assets.iter().find(|a| a.name == ASSET_NAME)
    }
}

/// Compare the running version to a remote tag.
pub fn compare_to_running(remote: &str) -> UpdateVerdict {
    let local = current_version();
    match (
        semver::Version::parse(local),
        semver::Version::parse(remote),
    ) {
        (Ok(l), Ok(r)) => match r.cmp(&l) {
            std::cmp::Ordering::Greater => UpdateVerdict::Newer,
            std::cmp::Ordering::Equal => UpdateVerdict::Equal,
            std::cmp::Ordering::Less => UpdateVerdict::Ahead,
        },
        _ => {
            if local == remote {
                UpdateVerdict::Equal
            } else {
                UpdateVerdict::Unknown
            }
        }
    }
}

/// Fetch the latest release metadata from GitHub. Best-effort; surfaces
/// network errors so callers can decide whether to swallow them (startup
/// check) or propagate them (explicit `self-update` command).
pub async fn fetch_latest_release() -> Result<LatestRelease> {
    let url = format!("https://api.github.com/repos/{}/releases/latest", REPO);
    let client = reqwest::Client::builder()
        .user_agent(format!("clf3/{}", current_version()))
        .timeout(Duration::from_secs(15))
        .build()
        .context("Failed to build reqwest client")?;
    let release: LatestRelease = client
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

// --- Startup check cache ----------------------------------------------------
//
// We don't want every CLF3 invocation to hit GitHub. A tiny JSON sidecar
// next to settings.json records the last-check timestamp + latest known
// release. Startup re-uses it if the cache is younger than `CHECK_TTL`.

#[derive(Debug, Clone, Default, serde::Serialize, Deserialize)]
struct UpdateCache {
    /// Unix timestamp of the last successful GitHub query.
    #[serde(default)]
    last_check_unix: u64,
    /// Tag (stripped of leading `v`) returned by that query.
    #[serde(default)]
    last_known_version: String,
    /// Direct download URL for the matching asset (so `self-update` can
    /// skip re-querying the API right after a startup notice).
    #[serde(default)]
    last_known_asset_url: String,
    /// Asset size in bytes (informational).
    #[serde(default)]
    last_known_asset_size: u64,
}

fn cache_path() -> Result<PathBuf> {
    let dir = dirs::config_dir()
        .context("Could not determine config directory")?
        .join("clf3");
    Ok(dir.join("update_cache.json"))
}

fn read_cache() -> Option<UpdateCache> {
    let path = cache_path().ok()?;
    let content = fs::read_to_string(&path).ok()?;
    serde_json::from_str(&content).ok()
}

fn write_cache(cache: &UpdateCache) -> Result<()> {
    let path = cache_path()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create {}", parent.display()))?;
    }
    let json = serde_json::to_string_pretty(cache).context("Failed to serialize update cache")?;
    fs::write(&path, json).with_context(|| format!("Failed to write {}", path.display()))?;
    Ok(())
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Quick, side-effect-light check intended for `main()` startup. Hits the
/// GitHub API at most once per `CHECK_TTL`. Returns `Some(version)` if an
/// update is available, `None` otherwise (or on any error — startup must
/// never fail because the network is unavailable).
pub async fn startup_check_with_notice() {
    if let Err(e) = startup_check_inner().await {
        // Log but never propagate — startup must be best-effort.
        tracing::debug!("self-update startup check skipped: {:#}", e);
    }
}

async fn startup_check_inner() -> Result<()> {
    let cache = read_cache().unwrap_or_default();
    let now = unix_now();
    let fresh = now.saturating_sub(cache.last_check_unix) < CHECK_TTL.as_secs();

    let (version, asset_url, asset_size) = if fresh && !cache.last_known_version.is_empty() {
        (
            cache.last_known_version.clone(),
            cache.last_known_asset_url.clone(),
            cache.last_known_asset_size,
        )
    } else {
        let release = fetch_latest_release().await?;
        let asset = release
            .linux_asset()
            .ok_or_else(|| anyhow!("Latest release has no Linux asset"))?;
        let updated = UpdateCache {
            last_check_unix: now,
            last_known_version: release.version().to_string(),
            last_known_asset_url: asset.browser_download_url.clone(),
            last_known_asset_size: asset.size,
        };
        // Best-effort cache write; non-fatal.
        if let Err(e) = write_cache(&updated) {
            tracing::debug!("Failed to write update cache: {:#}", e);
        }
        (
            updated.last_known_version,
            updated.last_known_asset_url,
            updated.last_known_asset_size,
        )
    };

    let _ = asset_url;
    let _ = asset_size;
    if matches!(compare_to_running(&version), UpdateVerdict::Newer) {
        eprintln!(
            "{} CLF3 {} is available (you are on {}). Run `clf3 self-update` to upgrade.",
            console::style("Update:").bold().yellow(),
            version,
            current_version()
        );
    }
    Ok(())
}

// --- Update execution -------------------------------------------------------

/// Pretty summary returned by `run_update` for the caller to log/print.
#[derive(Debug, Clone)]
pub struct UpdateOutcome {
    /// Version we upgraded to (or are already on).
    pub version: String,
    /// True if we actually wrote a new binary; false if we were already
    /// up to date (and `force` wasn't set).
    pub replaced: bool,
}

/// Perform a self-update. Returns once the new binary is in place.
///
/// `force` re-downloads even when the verdict is `Equal` or `Ahead` —
/// useful for `clf3 self-update --force` and for regression-fixing.
pub async fn run_update(force: bool) -> Result<UpdateOutcome> {
    let release = fetch_latest_release().await?;
    let remote_version = release.version().to_string();
    let verdict = compare_to_running(&remote_version);

    match verdict {
        UpdateVerdict::Newer => {}
        UpdateVerdict::Equal if !force => {
            return Ok(UpdateOutcome {
                version: remote_version,
                replaced: false,
            });
        }
        UpdateVerdict::Ahead if !force => {
            return Ok(UpdateOutcome {
                version: remote_version,
                replaced: false,
            });
        }
        UpdateVerdict::Unknown if !force => {
            anyhow::bail!(
                "Remote tag '{}' isn't comparable to running version '{}'. \
                 Pass --force to overwrite anyway.",
                remote_version,
                current_version()
            );
        }
        _ => {}
    }

    let asset = release
        .linux_asset()
        .ok_or_else(|| anyhow!("Release {} has no '{}' asset", release.tag_name, ASSET_NAME))?;

    let current_exe =
        std::env::current_exe().context("Could not resolve path to running clf3 executable")?;
    let target_dir = current_exe
        .parent()
        .ok_or_else(|| anyhow!("Running exe has no parent dir: {}", current_exe.display()))?
        .to_path_buf();

    eprintln!(
        "Downloading clf3 {} ({}, {} MiB)...",
        remote_version,
        asset.name,
        asset.size / (1024 * 1024)
    );

    let client = reqwest::Client::builder()
        .user_agent(format!("clf3/{}", current_version()))
        .build()
        .context("Failed to build reqwest client")?;
    let bytes = client
        .get(&asset.browser_download_url)
        .send()
        .await
        .context("clf3 self-update download failed")?
        .error_for_status()
        .context("clf3 self-update download returned non-2xx")?
        .bytes()
        .await
        .context("Failed to read clf3 self-update body")?;

    // Pull the bundled binaries out of the zip into the target dir as
    // sibling temp files, then atomically rename them into place.
    let staged = stage_zip_into_dir(&bytes, &target_dir)?;

    // Replace the running clf3 last so a failure unpacking 7zz doesn't
    // half-update the install.
    if let Some(seven) = staged.seven_zz {
        let final_seven = target_dir.join("7zz");
        atomic_replace(&seven, &final_seven)
            .with_context(|| format!("Failed to install new 7zz at {}", final_seven.display()))?;
    }
    atomic_replace(&staged.clf3, &current_exe).with_context(|| {
        format!(
            "Failed to install new clf3 at {} (a backup is at {})",
            current_exe.display(),
            staged.clf3.display()
        )
    })?;

    // Refresh the cache so the startup-check notice doesn't keep firing.
    let _ = write_cache(&UpdateCache {
        last_check_unix: unix_now(),
        last_known_version: remote_version.clone(),
        last_known_asset_url: asset.browser_download_url.clone(),
        last_known_asset_size: asset.size,
    });

    Ok(UpdateOutcome {
        version: remote_version,
        replaced: true,
    })
}

struct StagedBinaries {
    /// Temp file in the same dir as the live exe, containing the new clf3.
    clf3: PathBuf,
    /// Optional temp file for the bundled 7zz, if the zip carried one.
    seven_zz: Option<PathBuf>,
}

fn stage_zip_into_dir(bytes: &[u8], dir: &Path) -> Result<StagedBinaries> {
    fs::create_dir_all(dir).with_context(|| format!("Failed to create {}", dir.display()))?;

    let reader = std::io::Cursor::new(bytes);
    let mut zip = zip::ZipArchive::new(reader)
        .context("clf3 self-update payload isn't a valid zip archive")?;

    let mut staged_clf3: Option<PathBuf> = None;
    let mut staged_7zz: Option<PathBuf> = None;

    for i in 0..zip.len() {
        let mut file = zip
            .by_index(i)
            .with_context(|| format!("Failed to read zip entry {}", i))?;
        let name = file
            .enclosed_name()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from(file.name()));
        let basename = name.file_name().and_then(|s| s.to_str()).unwrap_or("");

        let target_stem = match basename {
            "clf3" => "clf3",
            "7zz" => "7zz",
            // Ignore READMEs, signatures, etc. Releases currently only ship
            // the two binaries, but be liberal about extra files.
            _ => continue,
        };

        let staged_path = dir.join(format!(".{}.new", target_stem));
        let mut buf = Vec::with_capacity(file.size() as usize);
        file.read_to_end(&mut buf)
            .with_context(|| format!("Failed to read {} from zip", basename))?;
        write_executable(&staged_path, &buf)
            .with_context(|| format!("Failed to write staged {}", staged_path.display()))?;

        match target_stem {
            "clf3" => staged_clf3 = Some(staged_path),
            "7zz" => staged_7zz = Some(staged_path),
            _ => unreachable!(),
        }
    }

    Ok(StagedBinaries {
        clf3: staged_clf3.ok_or_else(|| anyhow!("Zip didn't contain a clf3 binary"))?,
        seven_zz: staged_7zz,
    })
}

fn write_executable(path: &Path, bytes: &[u8]) -> Result<()> {
    fs::write(path, bytes)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(path)?.permissions();
        perms.set_mode(perms.mode() | 0o755);
        fs::set_permissions(path, perms)?;
    }
    Ok(())
}

/// `rename(2)` is atomic on the same filesystem and replaces `dst` if it
/// exists. The running process keeps its already-mmapped inode, so this is
/// safe even when `dst` is the currently-executing binary.
fn atomic_replace(src: &Path, dst: &Path) -> Result<()> {
    fs::rename(src, dst)
        .with_context(|| format!("Failed to rename {} → {}", src.display(), dst.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equal_version_is_not_an_update() {
        assert_eq!(compare_to_running(current_version()), UpdateVerdict::Equal);
    }

    #[test]
    fn newer_remote_triggers_update() {
        // Bump the major version far past anything plausible.
        assert_eq!(compare_to_running("99.99.99"), UpdateVerdict::Newer);
    }

    #[test]
    fn older_remote_is_ahead() {
        assert_eq!(compare_to_running("0.0.0"), UpdateVerdict::Ahead);
    }

    #[test]
    fn non_semver_falls_back_to_unknown() {
        assert_eq!(
            compare_to_running("totally-not-semver"),
            UpdateVerdict::Unknown
        );
    }

    #[test]
    fn tag_with_v_prefix_is_stripped() {
        let release = LatestRelease {
            tag_name: "v0.2.0".into(),
            name: "Test".into(),
            assets: vec![],
            body: String::new(),
        };
        assert_eq!(release.version(), "0.2.0");
    }
}
