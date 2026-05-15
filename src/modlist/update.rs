//! Discovery + version-check logic for `clf3 modlist check` / `clf3 modlist
//! update`.
//!
//! Two sources of truth for "what's installed":
//! 1. `Settings::installed_modlists` — written by the installer on success,
//!    keyed by gallery `machine_name`. The source of truth when an install dir
//!    isn't currently accessible.
//! 2. `Settings::browser_list_paths` — the GUI saves a per-machine_name path
//!    pair (downloads + install) when the user picks paths. We use the
//!    `install_dir` here to locate any `.clf3-install.json` written by the
//!    installer.
//!
//! On-disk `.clf3-install.json` files (when present) are authoritative for
//! version: they get updated even when the settings file fails to write.

#![allow(dead_code)] // public surface used by binary crate

use anyhow::{anyhow, Result};
use serde::Serialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::modlist::browser::ModlistMetadata;
use crate::modlist::install_manifest::{compare_versions, InstallManifest, VersionCmp};
use crate::settings::{InstalledModlistRecord, Settings};

/// Aggregated view of a known install. Combines whatever signals we have —
/// the on-disk manifest is preferred, settings record is the fallback.
#[derive(Debug, Clone)]
pub struct InstallRecord {
    /// Gallery `machine_name` for this install. May be empty for legacy
    /// installs predating the manifest.
    pub machine_name: String,
    /// Modlist title at install time (or `<unknown>` for very old installs).
    pub name: String,
    /// Version string the modlist self-reported when installed.
    pub installed_version: String,
    /// Install directory, if known. May not currently exist.
    pub install_dir: Option<PathBuf>,
    /// Downloads dir used at install time, if known.
    pub downloads_dir: Option<PathBuf>,
    /// URL the .wabbajack file was fetched from (None for local-file installs).
    pub wabbajack_url: Option<String>,
    /// Was an on-disk manifest the source for this record? (vs. settings-only)
    pub from_manifest: bool,
}

impl InstallRecord {
    fn from_manifest(manifest: InstallManifest) -> Self {
        Self {
            machine_name: manifest.machine_name,
            name: manifest.name,
            installed_version: manifest.installed_version,
            install_dir: Some(manifest.output_dir),
            downloads_dir: Some(manifest.downloads_dir),
            wabbajack_url: manifest.wabbajack_url,
            from_manifest: true,
        }
    }

    fn from_settings(machine_name: String, record: &InstalledModlistRecord) -> Self {
        Self {
            machine_name,
            name: record.name.clone(),
            installed_version: record.installed_version.clone(),
            install_dir: empty_or_path(&record.install_dir),
            downloads_dir: empty_or_path(&record.downloads_dir),
            wabbajack_url: record.wabbajack_url.clone(),
            from_manifest: false,
        }
    }
}

fn empty_or_path(s: &str) -> Option<PathBuf> {
    if s.is_empty() {
        None
    } else {
        Some(PathBuf::from(s))
    }
}

/// Discover every install CLF3 knows about.
///
/// For each `machine_name` we've seen (in either settings field), prefer the
/// on-disk `.clf3-install.json` over the settings mirror. Installs whose dir
/// is missing fall back to the settings record so they still appear in
/// `clf3 modlist check`.
pub fn discover_installs(settings: &Settings) -> Vec<InstallRecord> {
    let mut by_machine: HashMap<String, InstallRecord> = HashMap::new();

    // First pass: the settings mirror gives us a baseline for every record.
    for (machine_name, record) in &settings.installed_modlists {
        by_machine.insert(
            machine_name.clone(),
            InstallRecord::from_settings(machine_name.clone(), record),
        );
    }

    // Second pass: the browser path map gives us additional install_dir
    // candidates. Try to read `.clf3-install.json` from each one; that's the
    // freshest record. Skip dirs whose machine_name is already covered by an
    // on-disk manifest.
    for (machine_name, paths) in &settings.browser_list_paths {
        if paths.install_dir.is_empty() {
            continue;
        }
        let dir = Path::new(&paths.install_dir);
        match InstallManifest::load_from(dir) {
            Ok(Some(manifest)) => {
                let mut rec = InstallRecord::from_manifest(manifest);
                // The manifest's machine_name may be empty for old/non-gallery
                // installs — fall back to the settings key we found it under.
                if rec.machine_name.is_empty() {
                    rec.machine_name = machine_name.clone();
                }
                by_machine.insert(rec.machine_name.clone(), rec);
            }
            Ok(None) => {
                // Legacy install: no manifest. Use whatever we already have
                // from `installed_modlists` if any, otherwise synthesize a
                // record with unknown version so the user at least sees it.
                by_machine
                    .entry(machine_name.clone())
                    .or_insert_with(|| InstallRecord {
                        machine_name: machine_name.clone(),
                        name: machine_name.clone(),
                        installed_version: String::new(),
                        install_dir: Some(dir.to_path_buf()),
                        downloads_dir: empty_or_path(&paths.downloads_dir),
                        wabbajack_url: None,
                        from_manifest: false,
                    });
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to read manifest in {}: {:#}",
                    dir.display(),
                    e
                );
            }
        }
    }

    let mut out: Vec<InstallRecord> = by_machine.into_values().collect();
    out.sort_by(|a, b| a.machine_name.cmp(&b.machine_name));
    out
}

/// Resolve a user-supplied modlist name or partial-name query against the
/// gallery + the user's known installs.
///
/// Lookup order:
/// 1. Exact `machine_name` match against gallery.
/// 2. Exact `machine_name` match against a known install (e.g. modlist
///    removed from gallery but still installed).
/// 3. Partial-name match via `ModlistMetadata::matches_query` over the
///    gallery. If exactly one match, use it. Ambiguous → error with
///    candidates.
pub fn resolve_query(
    query: &str,
    gallery: &[ModlistMetadata],
    installs: &[InstallRecord],
) -> Result<String> {
    let q = query.trim();
    if q.is_empty() {
        anyhow::bail!("Modlist name required");
    }

    if let Some(m) = gallery
        .iter()
        .find(|m| m.machine_name.eq_ignore_ascii_case(q))
    {
        return Ok(m.machine_name.clone());
    }

    if let Some(rec) = installs
        .iter()
        .find(|r| r.machine_name.eq_ignore_ascii_case(q))
    {
        return Ok(rec.machine_name.clone());
    }

    let matches: Vec<&ModlistMetadata> = gallery.iter().filter(|m| m.matches_query(q)).collect();
    match matches.len() {
        0 => Err(anyhow!(
            "No modlist matches '{}'. Try `clf3 browser` to see available modlists.",
            q
        )),
        1 => Ok(matches[0].machine_name.clone()),
        _ => {
            let mut sample: Vec<String> = matches
                .iter()
                .take(10)
                .map(|m| format!("  - {}  ({})", m.machine_name, m.title))
                .collect();
            if matches.len() > 10 {
                sample.push(format!("  ... and {} more", matches.len() - 10));
            }
            Err(anyhow!(
                "Ambiguous query '{}' — {} candidates:\n{}",
                q,
                matches.len(),
                sample.join("\n")
            ))
        }
    }
}

/// Per-install update status, suitable for both table output and JSON.
#[derive(Debug, Clone, Serialize)]
pub struct UpdateReport {
    pub machine_name: String,
    pub name: String,
    pub installed_version: String,
    /// Latest version from the gallery, if known.
    pub gallery_version: Option<String>,
    /// Update-available verdict as a stable string for JSON consumers.
    /// One of `"newer"`, `"older"`, `"equal"`, `"different"`, `"unknown"`,
    /// `"missing-in-gallery"`.
    pub status: String,
    /// `true` for `newer` or `different`. Pre-computed so a JSON consumer
    /// doesn't have to re-derive the rule.
    pub update_available: bool,
    /// True if the install dir's `.clf3-install.json` was readable.
    pub from_manifest: bool,
}

/// Compare each install against the gallery and return one report per record.
/// Installs whose `machine_name` isn't in the gallery are flagged with
/// `status: "missing-in-gallery"`.
pub fn build_update_reports(
    installs: &[InstallRecord],
    gallery: &[ModlistMetadata],
) -> Vec<UpdateReport> {
    let by_name: HashMap<&str, &ModlistMetadata> = gallery
        .iter()
        .map(|m| (m.machine_name.as_str(), m))
        .collect();

    installs
        .iter()
        .map(|inst| match by_name.get(inst.machine_name.as_str()) {
            Some(meta) => {
                let cmp = compare_versions(&inst.installed_version, &meta.version);
                UpdateReport {
                    machine_name: inst.machine_name.clone(),
                    name: if inst.name.is_empty() {
                        meta.title.clone()
                    } else {
                        inst.name.clone()
                    },
                    installed_version: inst.installed_version.clone(),
                    gallery_version: Some(meta.version.clone()),
                    status: status_string(&cmp).to_string(),
                    update_available: cmp.update_available(),
                    from_manifest: inst.from_manifest,
                }
            }
            None => UpdateReport {
                machine_name: inst.machine_name.clone(),
                name: inst.name.clone(),
                installed_version: inst.installed_version.clone(),
                gallery_version: None,
                status: "missing-in-gallery".into(),
                update_available: false,
                from_manifest: inst.from_manifest,
            },
        })
        .collect()
}

fn status_string(cmp: &VersionCmp) -> &'static str {
    match cmp {
        VersionCmp::Newer => "newer",
        VersionCmp::Older => "older",
        VersionCmp::Equal => "equal",
        VersionCmp::Different => "different",
        VersionCmp::Unknown => "unknown",
    }
}

/// Format reports as a human-readable table.
pub fn format_table(reports: &[UpdateReport]) -> String {
    if reports.is_empty() {
        return "No tracked installs found. Install a modlist via `clf3 install` first.\n".into();
    }

    let mut col_name = "Modlist".len();
    let mut col_inst = "Installed".len();
    let mut col_galy = "Latest".len();
    for r in reports {
        col_name = col_name.max(r.machine_name.len().max(r.name.len()));
        col_inst = col_inst.max(r.installed_version.len().max(8));
        col_galy = col_galy
            .max(r.gallery_version.as_deref().map(str::len).unwrap_or(0).max(6));
    }

    let mut out = String::new();
    out.push_str(&format!(
        "{:<wname$}  {:<winst$}  {:<wgaly$}  Status\n",
        "Modlist",
        "Installed",
        "Latest",
        wname = col_name,
        winst = col_inst,
        wgaly = col_galy,
    ));
    out.push_str(&format!(
        "{:-<wname$}  {:-<winst$}  {:-<wgaly$}  {:-<8}\n",
        "",
        "",
        "",
        "",
        wname = col_name,
        winst = col_inst,
        wgaly = col_galy,
    ));
    for r in reports {
        let installed = if r.installed_version.is_empty() {
            "<unknown>"
        } else {
            r.installed_version.as_str()
        };
        let gallery = r.gallery_version.as_deref().unwrap_or("-");
        let display_name = if !r.name.is_empty() {
            r.name.as_str()
        } else {
            r.machine_name.as_str()
        };
        let marker = if r.update_available { " *" } else { "" };
        out.push_str(&format!(
            "{:<wname$}  {:<winst$}  {:<wgaly$}  {}{}\n",
            display_name,
            installed,
            gallery,
            r.status,
            marker,
            wname = col_name,
            winst = col_inst,
            wgaly = col_galy,
        ));
    }

    let updatable = reports.iter().filter(|r| r.update_available).count();
    if updatable > 0 {
        out.push_str(&format!(
            "\n{} update(s) available. Run `clf3 modlist update <machine_name>` to apply.\n",
            updatable
        ));
    } else {
        out.push_str("\nAll tracked modlists are up to date.\n");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modlist::browser::{ModlistLinks, ModlistMetadata};

    fn make_meta(machine_name: &str, title: &str, version: &str) -> ModlistMetadata {
        ModlistMetadata {
            title: title.into(),
            game: "Skyrim".into(),
            version: version.into(),
            links: Some(ModlistLinks {
                download: format!("https://example/{}.wabbajack", machine_name),
                machine_url: machine_name.into(),
                ..Default::default()
            }),
            machine_name: machine_name.into(),
            ..Default::default()
        }
    }

    fn make_install(machine_name: &str, name: &str, version: &str) -> InstallRecord {
        InstallRecord {
            machine_name: machine_name.into(),
            name: name.into(),
            installed_version: version.into(),
            install_dir: Some(PathBuf::from("/tmp/install")),
            downloads_dir: Some(PathBuf::from("/tmp/downloads")),
            wabbajack_url: None,
            from_manifest: true,
        }
    }

    #[test]
    fn check_reports_newer_when_gallery_advances() {
        let gallery = vec![
            make_meta("tuxborn", "Tuxborn", "1.2.4"),
            make_meta("lorerim", "LoreRim", "0.5.0"),
        ];
        let installs = vec![
            make_install("tuxborn", "Tuxborn", "1.2.3"),
            make_install("lorerim", "LoreRim", "0.5.0"),
        ];

        let reports = build_update_reports(&installs, &gallery);
        assert_eq!(reports.len(), 2);

        let tux = reports.iter().find(|r| r.machine_name == "tuxborn").unwrap();
        assert_eq!(tux.status, "newer");
        assert!(tux.update_available);
        assert_eq!(tux.gallery_version.as_deref(), Some("1.2.4"));

        let lore = reports.iter().find(|r| r.machine_name == "lorerim").unwrap();
        assert_eq!(lore.status, "equal");
        assert!(!lore.update_available);
    }

    #[test]
    fn check_flags_unknown_orderable_versions_as_different() {
        // Non-semver versions: the installed string differs from the gallery
        // string, but we can't say which is newer.
        let gallery = vec![make_meta("custom", "Custom", "2024-10-rev2")];
        let installs = vec![make_install("custom", "Custom", "2024-09-rev1")];
        let reports = build_update_reports(&installs, &gallery);
        assert_eq!(reports[0].status, "different");
        assert!(reports[0].update_available);
    }

    #[test]
    fn check_marks_missing_gallery_entry() {
        let gallery = vec![make_meta("tuxborn", "Tuxborn", "1.0.0")];
        let installs = vec![make_install("retired", "Retired", "0.9.0")];
        let reports = build_update_reports(&installs, &gallery);
        let r = &reports[0];
        assert_eq!(r.machine_name, "retired");
        assert_eq!(r.status, "missing-in-gallery");
        assert!(!r.update_available);
        assert!(r.gallery_version.is_none());
    }

    #[test]
    fn resolve_exact_machine_name() {
        let gallery = vec![
            make_meta("tuxborn", "Tuxborn", "1.0.0"),
            make_meta("fallenworld", "Fallen World", "2.0.0"),
        ];
        let installs: Vec<InstallRecord> = Vec::new();

        assert_eq!(
            resolve_query("tuxborn", &gallery, &installs).unwrap(),
            "tuxborn"
        );
        // Case-insensitive on the machine_name path.
        assert_eq!(
            resolve_query("TUXBORN", &gallery, &installs).unwrap(),
            "tuxborn"
        );
    }

    #[test]
    fn resolve_partial_name_unique() {
        let gallery = vec![
            make_meta("tuxborn", "Tuxborn", "1.0.0"),
            make_meta("fallenworld", "Fallen World", "2.0.0"),
        ];
        let installs: Vec<InstallRecord> = Vec::new();
        assert_eq!(
            resolve_query("fallen", &gallery, &installs).unwrap(),
            "fallenworld"
        );
    }

    #[test]
    fn resolve_partial_name_ambiguous_errors() {
        let gallery = vec![
            make_meta("skyrim-list-a", "Skyrim Pack A", "1.0.0"),
            make_meta("skyrim-list-b", "Skyrim Pack B", "1.0.0"),
        ];
        let installs: Vec<InstallRecord> = Vec::new();
        let err = resolve_query("skyrim", &gallery, &installs).unwrap_err();
        let msg = format!("{:#}", err);
        assert!(msg.contains("Ambiguous"));
        assert!(msg.contains("skyrim-list-a"));
        assert!(msg.contains("skyrim-list-b"));
    }

    #[test]
    fn resolve_falls_back_to_installs_when_gallery_misses() {
        let gallery: Vec<ModlistMetadata> = vec![];
        let installs = vec![make_install("retired", "Retired", "0.9.0")];
        assert_eq!(
            resolve_query("retired", &gallery, &installs).unwrap(),
            "retired"
        );
    }
}
