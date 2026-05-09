//! Post-install layout fixer for mods Vortex's runtime extensions would
//! normally rewrite. The collection JSON carries no install instructions
//! for these (no FOMOD choices, no `fileOverrides`) — Vortex relies on
//! per-game extensions like FormListManipulator, Description Framework,
//! Base Object Swapper to remap paths at install time. Without those
//! extensions we end up with a mod folder MO2's VFS can't usefully deploy
//! (the "no Data folder" red X).
//!
//! Two passes:
//!
//! 1. **Auto-routers** — pattern-matched, run unconditionally. FLM-style
//!    single-subdir wrappers get rewrapped under the right SKSE/Plugins/
//!    subtree.
//! 2. **Interactive picker** — multi-variant subdir mods (Wind Ruler-style
//!    `No Fur/` vs `Original/`). Prompts on stdin; only fires when the
//!    user opts in via `--interactive-fix`.
//!
//! Each fixed mod records the rewrite in its `.clf3-installed.json.mohidden`
//! marker so re-runs don't double-wrap.

use std::collections::HashSet;
use std::fs;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tracing::{info, warn};

/// Bethesda Data-folder signal at the top of a mod folder.
const STOP_FOLDERS: &[&str] = &[
    "meshes", "textures", "scripts", "interface", "sound", "sounds", "music",
    "fonts", "shaders", "video", "voices", "seq", "translations", "lodsettings",
    "source", "skse", "skse64", "sksevr", "f4se", "calientetools", "edit scripts",
    "skyproc patchers", "tools", "dyndolod", "dynamicanimationreplacer",
    "scriptsource", "pluginsource", "menus", "strings", "actors", "behaviors",
    // Game-root staging dir (route_root_files moves matching files here).
    "root",
    // Nemesis behavior patches deploy to Data/Nemesis_Engine/ — leaving the
    // wrapper IS the right behavior.
    "nemesis_engine",
];

const STOP_EXTENSIONS: &[&str] = &[
    ".esp", ".esm", ".esl", ".bsa", ".ba2",
];

/// Marker filenames; we read/write the post-install resolution flag through
/// the same JSON sidecar `extract_mod` writes.
const MARKER_FILE: &str = ".clf3-installed.json.mohidden";

#[derive(Debug)]
enum LayoutPlan {
    /// Mod already has a Data layout — nothing to do.
    Ok,
    /// Single subdir wrapper containing FLM-style content (.json or .ini
    /// configs). Move the wrapper under
    /// `SKSE/Plugins/FormListManipulator/Configs/<wrapper>/`.
    FlmStyle { wrapper: String },
    /// Multiple top-level subdirs that look like FOMOD variants
    /// (Meshes-bearing or plugin-bearing). Needs user picks.
    MultiVariant { variants: Vec<String> },
    /// Top level is loose loose .ini / .json files only (no subdir + no
    /// Data signal). User has to know what to do — we don't touch.
    Inscrutable,
}

/// Inspect a mod folder and propose a fix for its layout.
fn classify(mod_dir: &Path) -> LayoutPlan {
    let mut subdirs: Vec<String> = Vec::new();
    let mut had_loose_file = false;
    let entries = match fs::read_dir(mod_dir) {
        Ok(e) => e,
        Err(_) => return LayoutPlan::Ok,
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str == MARKER_FILE || name_str == ".clf3-installed.json" {
            continue;
        }
        let ft = match entry.file_type() {
            Ok(ft) => ft,
            Err(_) => continue,
        };
        if ft.is_dir() {
            let lc = name_str.to_lowercase();
            if STOP_FOLDERS.contains(&lc.as_str()) {
                return LayoutPlan::Ok;
            }
            subdirs.push(name_str.into_owned());
        } else if ft.is_file() {
            let lc = name_str.to_lowercase();
            if let Some(dot) = lc.rfind('.') {
                let ext = &lc[dot..];
                if STOP_EXTENSIONS.contains(&ext) {
                    return LayoutPlan::Ok;
                }
            }
            had_loose_file = true;
        }
    }

    match subdirs.as_slice() {
        [] => LayoutPlan::Ok, // Empty (or only marker) — leave alone.
        [single] if !had_loose_file => {
            let wrapper = single.clone();
            if subdir_has_flm_content(&mod_dir.join(&wrapper)) {
                LayoutPlan::FlmStyle { wrapper }
            } else {
                LayoutPlan::Inscrutable
            }
        }
        many if many.len() >= 2 && variants_look_like_fomod_options(mod_dir, many) => {
            LayoutPlan::MultiVariant {
                variants: subdirs,
            }
        }
        _ => LayoutPlan::Inscrutable,
    }
}

/// True when the wrapper directly or recursively contains FLM-style config
/// files (`*.json`, `*_FLM.ini`).
fn subdir_has_flm_content(dir: &Path) -> bool {
    use walkdir::WalkDir;
    for entry in WalkDir::new(dir).max_depth(3).into_iter().filter_map(|e| e.ok()) {
        if !entry.file_type().is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_lowercase();
        if name.ends_with(".json") || name.ends_with("_flm.ini") {
            return true;
        }
    }
    false
}

/// True when the candidate subdirs each look like a FOMOD variant — at
/// least one of them contains a STOP signal one level down (Meshes/, an
/// ESP, etc).
fn variants_look_like_fomod_options(mod_dir: &Path, subdirs: &[String]) -> bool {
    for sub in subdirs {
        let path = mod_dir.join(sub);
        let Ok(read) = fs::read_dir(&path) else { continue };
        for entry in read.flatten() {
            let name = entry.file_name().to_string_lossy().to_lowercase();
            let ft = match entry.file_type() {
                Ok(ft) => ft,
                Err(_) => continue,
            };
            if ft.is_dir() && STOP_FOLDERS.contains(&name.as_str()) {
                return true;
            }
            if ft.is_file() {
                if let Some(dot) = name.rfind('.') {
                    if STOP_EXTENSIONS.contains(&&name[dot..]) {
                        return true;
                    }
                }
            }
        }
    }
    false
}

/// Result of a layout-fix pass over a single mod.
#[derive(Debug, Clone, Copy)]
pub enum FixOutcome {
    AlreadyOk,
    AutoFixed,
    NeedsUserChoice,
    UserSkipped,
    UserPicked,
    LeftAlone,
}

/// Layout-fix every mod in `mods_dir`. Auto-routers run unconditionally;
/// multi-variant mods are interactive iff `interactive`.
pub fn fix_all(mods_dir: &Path, interactive: bool) -> Result<Vec<(String, FixOutcome)>> {
    let mut results = Vec::new();
    let mut entries: Vec<PathBuf> = fs::read_dir(mods_dir)?
        .flatten()
        .filter_map(|e| {
            let path = e.path();
            if !path.is_dir() {
                return None;
            }
            // Skip `tempfile::tempdir_in(mods_dir)` leftovers (`.tmpXXXXXX`).
            // Those are from FOMOD-precursor extraction that didn't reach
            // its Drop cleanup (process killed, panic mid-extract). Best-
            // effort: also remove them so next install starts clean.
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with(".tmp") {
                    let _ = fs::remove_dir_all(&path);
                    return None;
                }
                // Skip other dotfiles (markers, hidden state).
                if name.starts_with('.') {
                    return None;
                }
            }
            Some(path)
        })
        .collect();
    entries.sort();

    for mod_dir in entries {
        let folder_name = mod_dir
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let outcome = match classify(&mod_dir) {
            LayoutPlan::Ok => FixOutcome::AlreadyOk,
            LayoutPlan::FlmStyle { wrapper } => {
                match apply_flm_routing(&mod_dir, &wrapper) {
                    Ok(()) => FixOutcome::AutoFixed,
                    Err(e) => {
                        warn!("FLM-route failed for {}: {:#}", folder_name, e);
                        FixOutcome::LeftAlone
                    }
                }
            }
            LayoutPlan::MultiVariant { variants } => {
                if interactive {
                    match prompt_pick_variant(&mod_dir, &folder_name, &variants) {
                        Some(idx) => match apply_variant_pick(&mod_dir, &variants[idx]) {
                            Ok(()) => FixOutcome::UserPicked,
                            Err(e) => {
                                warn!("variant pick failed for {}: {:#}", folder_name, e);
                                FixOutcome::LeftAlone
                            }
                        },
                        None => FixOutcome::UserSkipped,
                    }
                } else {
                    FixOutcome::NeedsUserChoice
                }
            }
            LayoutPlan::Inscrutable => FixOutcome::LeftAlone,
        };
        results.push((folder_name, outcome));
    }
    Ok(results)
}

/// Move `<mod>/<wrapper>/...` into
/// `<mod>/SKSE/Plugins/FormListManipulator/Configs/<wrapper>/...` so
/// FormListManipulator picks the configs up at runtime.
fn apply_flm_routing(mod_dir: &Path, wrapper: &str) -> Result<()> {
    let src = mod_dir.join(wrapper);
    if !src.is_dir() {
        return Ok(());
    }
    let dst = mod_dir
        .join("SKSE")
        .join("Plugins")
        .join("FormListManipulator")
        .join("Configs")
        .join(wrapper);
    if dst.exists() {
        // Already routed (re-run on same install dir). Nothing to do.
        return Ok(());
    }
    fs::create_dir_all(dst.parent().unwrap())
        .with_context(|| format!("create FLM parent dirs at {}", dst.display()))?;
    fs::rename(&src, &dst)
        .with_context(|| format!("move {} -> {}", src.display(), dst.display()))?;
    info!(
        "post-install: rewrapped {} as FLM config under SKSE/Plugins/FormListManipulator/Configs/",
        wrapper
    );
    Ok(())
}

/// Interactive prompt: show the variant subdirs + a brief inventory of
/// what each contains, take a number (or 's' to skip) on stdin. Returns
/// the chosen index, or None when the user skips.
fn prompt_pick_variant(mod_dir: &Path, folder: &str, variants: &[String]) -> Option<usize> {
    println!("\nMod needs a variant pick: {}", folder);
    for (i, v) in variants.iter().enumerate() {
        println!(
            "  [{}] {} {}",
            i + 1,
            v,
            summarize_variant_path(&mod_dir.join(v))
        );
    }
    print!("Pick 1-{} or 's' to skip: ", variants.len());
    io::stdout().flush().ok();
    let stdin = io::stdin();
    let line = stdin.lock().lines().next()?.ok()?;
    let trimmed = line.trim();
    if trimmed.eq_ignore_ascii_case("s") || trimmed.is_empty() {
        return None;
    }
    let idx = trimmed.parse::<usize>().ok()?;
    if idx >= 1 && idx <= variants.len() {
        Some(idx - 1)
    } else {
        None
    }
}

/// Quick "(N MB, has plugin, has Meshes)" hint used by the variant prompt
/// so the user has *some* signal to choose by.
fn summarize_variant(_folder: &str, _variant: &str) -> String {
    String::new() // unused — see summarize_variant_path
}

fn summarize_variant_path(path: &Path) -> String {
    use walkdir::WalkDir;
    if !path.is_dir() {
        return String::new();
    }
    let mut total_bytes: u64 = 0;
    let mut has_plugin = false;
    let mut has_meshes = false;
    for entry in WalkDir::new(&path).into_iter().filter_map(|e| e.ok()) {
        if entry.file_type().is_file() {
            total_bytes += entry.metadata().map(|m| m.len()).unwrap_or(0);
            let lc = entry.file_name().to_string_lossy().to_lowercase();
            if lc.ends_with(".esp") || lc.ends_with(".esm") || lc.ends_with(".esl") {
                has_plugin = true;
            }
        } else if entry.file_type().is_dir()
            && entry.file_name().to_string_lossy().eq_ignore_ascii_case("meshes")
        {
            has_meshes = true;
        }
    }
    let mb = total_bytes as f64 / (1024.0 * 1024.0);
    let mut tags: Vec<&str> = Vec::new();
    if has_plugin {
        tags.push("plugin");
    }
    if has_meshes {
        tags.push("meshes");
    }
    let tag_str = if tags.is_empty() {
        String::new()
    } else {
        format!(" — {}", tags.join(", "))
    };
    format!("({:.1} MB{})", mb, tag_str)
}

/// Drop everything inside `<mod>/` except the picked variant + marker; lift
/// the picked variant's contents up to mod root.
fn apply_variant_pick(mod_dir: &Path, picked: &str) -> Result<()> {
    let to_remove: Vec<PathBuf> = fs::read_dir(mod_dir)?
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            if name == MARKER_FILE || name == ".clf3-installed.json" || name == picked {
                None
            } else {
                Some(e.path())
            }
        })
        .collect();
    for path in to_remove {
        if path.is_dir() {
            fs::remove_dir_all(&path)
                .with_context(|| format!("rm -r {}", path.display()))?;
        } else {
            let _ = fs::remove_file(&path);
        }
    }

    // Lift picked variant's children up.
    let picked_path = mod_dir.join(picked);
    let kept: HashSet<&str> = ["meta.ini"].iter().copied().collect();
    for entry in fs::read_dir(&picked_path)? {
        let entry = entry?;
        let from = entry.path();
        let name = entry.file_name();
        let to = mod_dir.join(&name);
        if to.exists() && !kept.contains(name.to_string_lossy().as_ref()) {
            // Defensive: shouldn't happen because we just cleared siblings.
            continue;
        }
        fs::rename(&from, &to)
            .with_context(|| format!("flatten {} -> {}", from.display(), to.display()))?;
    }
    let _ = fs::remove_dir(&picked_path);
    info!(
        "post-install: kept variant '{}' at {}",
        picked,
        mod_dir.display()
    );
    Ok(())
}
