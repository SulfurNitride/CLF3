//! Post-install manifest written next to a finished modlist install.
//!
//! After a successful `clf3 install`, we drop a small `.clf3-install.json`
//! file inside the install dir. It records which Wabbajack modlist + version
//! we installed and where its downloads live, so later runs of `clf3 modlist
//! check` / `clf3 modlist update` can offer to refresh the install.
//!
//! ## Schema
//!
//! Schema is versioned via the `schema_version` field. Bump it whenever the
//! on-disk shape changes in a non-backwards-compatible way. The current
//! version is `1`.
//!
//! ```json
//! {
//!   "schema_version": 1,
//!   "machine_name": "tuxborn",
//!   "name": "Tuxborn",
//!   "installed_version": "1.2.3",
//!   "wabbajack_url": "https://.../Tuxborn.wabbajack_xxx",
//!   "installed_at": "2026-05-15T12:34:56Z",
//!   "downloads_dir": "/home/u/wj/downloads",
//!   "output_dir": "/home/u/wj/tuxborn"
//! }
//! ```
//!
//! This schema is consumed by external tooling (Python launcher) — keep it
//! stable. New optional fields are fine; renames or removals require bumping
//! `schema_version`.

#![allow(dead_code)] // Re-exported through lib for external consumers

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Current on-disk schema version for `.clf3-install.json`.
pub const CURRENT_SCHEMA_VERSION: u32 = 1;

/// Filename of the manifest inside an install directory.
pub const MANIFEST_FILENAME: &str = ".clf3-install.json";

/// Manifest written into an install directory after a successful install.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallManifest {
    /// Schema version. Currently `1` — see [`CURRENT_SCHEMA_VERSION`].
    pub schema_version: u32,

    /// Gallery `machine_name` for this modlist. May be empty if CLF3 was
    /// invoked on a local file with no gallery match.
    #[serde(default)]
    pub machine_name: String,

    /// Human-readable modlist name as taken from the .wabbajack header.
    #[serde(default)]
    pub name: String,

    /// Version string the modlist self-reports.
    pub installed_version: String,

    /// Original URL the .wabbajack was downloaded from. `None` for local-file
    /// installs.
    #[serde(default)]
    pub wabbajack_url: Option<String>,

    /// RFC3339 timestamp of when the install finished.
    pub installed_at: String,

    /// Directory CLF3 was told to use for downloaded archives — needed when
    /// re-running an update so we reuse the same cache.
    pub downloads_dir: PathBuf,

    /// The install directory itself. Stored so that an external tool finding
    /// this manifest by path still knows what install dir owns it.
    pub output_dir: PathBuf,
}

impl InstallManifest {
    /// Build a new manifest at the current schema version, stamping
    /// `installed_at` to "now" in UTC RFC3339.
    pub fn new(
        machine_name: impl Into<String>,
        name: impl Into<String>,
        installed_version: impl Into<String>,
        wabbajack_url: Option<String>,
        downloads_dir: PathBuf,
        output_dir: PathBuf,
    ) -> Self {
        Self {
            schema_version: CURRENT_SCHEMA_VERSION,
            machine_name: machine_name.into(),
            name: name.into(),
            installed_version: installed_version.into(),
            wabbajack_url,
            installed_at: chrono::Utc::now().to_rfc3339(),
            downloads_dir,
            output_dir,
        }
    }

    /// Resolve the manifest path for an install directory.
    pub fn path_in(install_dir: &Path) -> PathBuf {
        install_dir.join(MANIFEST_FILENAME)
    }

    /// Read a manifest from an install directory. Returns `Ok(None)` if the
    /// file is absent — caller should treat that as a legacy install.
    pub fn load_from(install_dir: &Path) -> Result<Option<Self>> {
        let path = Self::path_in(install_dir);
        if !path.exists() {
            return Ok(None);
        }
        let content = std::fs::read_to_string(&path)
            .with_context(|| format!("Failed to read {}", path.display()))?;
        let manifest: Self = serde_json::from_str(&content)
            .with_context(|| format!("Failed to parse {}", path.display()))?;
        Ok(Some(manifest))
    }

    /// Write this manifest to its install directory, replacing any prior copy.
    pub fn save_to(&self, install_dir: &Path) -> Result<()> {
        let path = Self::path_in(install_dir);
        let content =
            serde_json::to_string_pretty(self).context("Failed to serialize install manifest")?;
        std::fs::write(&path, content)
            .with_context(|| format!("Failed to write {}", path.display()))?;
        Ok(())
    }
}

/// Result of comparing an installed version against a gallery version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionCmp {
    /// Gallery is strictly newer than installed (semver-ordered).
    Newer,
    /// Installed is strictly newer than gallery (semver-ordered).
    Older,
    /// Strings are equal.
    Equal,
    /// Strings differ but at least one is non-semver — ordering unknown.
    Different,
    /// Both versions are missing / empty.
    Unknown,
}

impl VersionCmp {
    /// Whether this comparison indicates an update is available.
    /// `Newer` and `Different` both warrant flagging — for non-semver
    /// versions we can't prove ordering, so any change should surface.
    pub fn update_available(&self) -> bool {
        matches!(self, VersionCmp::Newer | VersionCmp::Different)
    }
}

/// Compare two version strings.
///
/// Strategy:
/// - Empty / both-empty → `Unknown`.
/// - Trim a leading `v` and try `semver::Version::parse` on each side. If both
///   parse, compare numerically.
/// - Otherwise, fall back to `==` and report `Equal` or `Different` —
///   ordering cannot be inferred.
pub fn compare_versions(installed: &str, latest: &str) -> VersionCmp {
    let installed = installed.trim();
    let latest = latest.trim();

    if installed.is_empty() && latest.is_empty() {
        return VersionCmp::Unknown;
    }

    if installed == latest {
        return VersionCmp::Equal;
    }

    fn strip_v(s: &str) -> &str {
        s.strip_prefix('v')
            .or_else(|| s.strip_prefix('V'))
            .unwrap_or(s)
    }

    match (
        semver::Version::parse(strip_v(installed)),
        semver::Version::parse(strip_v(latest)),
    ) {
        (Ok(inst), Ok(lat)) => match inst.cmp(&lat) {
            std::cmp::Ordering::Less => VersionCmp::Newer,
            std::cmp::Ordering::Greater => VersionCmp::Older,
            std::cmp::Ordering::Equal => VersionCmp::Equal,
        },
        _ => VersionCmp::Different,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn manifest_round_trip() {
        let dir = tempdir().unwrap();
        let original = InstallManifest::new(
            "tuxborn",
            "Tuxborn",
            "1.2.3",
            Some("https://example.com/Tuxborn.wabbajack".into()),
            PathBuf::from("/home/u/wj/downloads"),
            dir.path().to_path_buf(),
        );

        original.save_to(dir.path()).unwrap();

        let loaded = InstallManifest::load_from(dir.path()).unwrap().unwrap();
        assert_eq!(loaded.schema_version, CURRENT_SCHEMA_VERSION);
        assert_eq!(loaded.machine_name, "tuxborn");
        assert_eq!(loaded.name, "Tuxborn");
        assert_eq!(loaded.installed_version, "1.2.3");
        assert_eq!(
            loaded.wabbajack_url.as_deref(),
            Some("https://example.com/Tuxborn.wabbajack")
        );
        assert_eq!(loaded.downloads_dir, PathBuf::from("/home/u/wj/downloads"));
        assert_eq!(loaded.output_dir, dir.path().to_path_buf());
        assert!(!loaded.installed_at.is_empty());
    }

    #[test]
    fn manifest_missing_returns_none() {
        let dir = tempdir().unwrap();
        let result = InstallManifest::load_from(dir.path()).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn manifest_tolerates_extra_fields_being_absent() {
        // Older manifests might lack wabbajack_url. Make sure the field
        // round-trips as None rather than failing to parse.
        let dir = tempdir().unwrap();
        let json = r#"{
            "schema_version": 1,
            "machine_name": "x",
            "name": "X",
            "installed_version": "0.1",
            "installed_at": "2026-01-01T00:00:00Z",
            "downloads_dir": "/tmp/d",
            "output_dir": "/tmp/o"
        }"#;
        std::fs::write(dir.path().join(MANIFEST_FILENAME), json).unwrap();
        let loaded = InstallManifest::load_from(dir.path()).unwrap().unwrap();
        assert!(loaded.wabbajack_url.is_none());
    }

    #[test]
    fn version_cmp_semver_ordering() {
        assert_eq!(compare_versions("1.2.3", "1.2.4"), VersionCmp::Newer);
        assert_eq!(compare_versions("1.2.4", "1.2.3"), VersionCmp::Older);
        assert_eq!(compare_versions("1.2.3", "1.2.3"), VersionCmp::Equal);
        assert_eq!(compare_versions("v1.2.3", "1.2.4"), VersionCmp::Newer);
        assert_eq!(compare_versions("V1.2.3", "v1.2.4"), VersionCmp::Newer);
        assert_eq!(compare_versions("2.0.0", "1.99.99"), VersionCmp::Older);
    }

    #[test]
    fn version_cmp_non_semver_fallback() {
        // Non-semver strings — we can detect difference but not ordering.
        assert_eq!(
            compare_versions("2024-01-rev3", "2024-02-rev1"),
            VersionCmp::Different
        );
        assert_eq!(
            compare_versions("release-alpha", "release-alpha"),
            VersionCmp::Equal
        );
    }

    #[test]
    fn version_cmp_mixed_semver_and_garbage() {
        // One side parseable, the other not — still must not panic, returns Different.
        assert_eq!(compare_versions("1.2.3", "garbage"), VersionCmp::Different);
        assert_eq!(compare_versions("garbage", "1.2.3"), VersionCmp::Different);
    }

    #[test]
    fn version_cmp_empty() {
        assert_eq!(compare_versions("", ""), VersionCmp::Unknown);
        // Empty vs. set → Different (can't order, but they're clearly not the same).
        assert_eq!(compare_versions("", "1.0.0"), VersionCmp::Different);
        assert_eq!(compare_versions("1.0.0", ""), VersionCmp::Different);
    }

    #[test]
    fn update_available_flagging() {
        assert!(VersionCmp::Newer.update_available());
        assert!(VersionCmp::Different.update_available());
        assert!(!VersionCmp::Equal.update_available());
        assert!(!VersionCmp::Older.update_available());
        assert!(!VersionCmp::Unknown.update_available());
    }
}
