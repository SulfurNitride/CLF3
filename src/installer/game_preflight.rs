//! Game file preflight verification.
//!
//! Hashes every `GameFileSource` archive in a modlist against the user's game
//! directory *before* any downloads start. Catches game updates, wrong store
//! variants, and missing DLC up front — avoiding the "download 300 GB, then
//! discover the game is the wrong version" failure mode.
//!
//! Two entry points:
//! - [`check_game_files_from_modlist`] — takes a parsed `Modlist` (used by the
//!   auto-detect path in `main.rs`, which runs before the installer DB exists).
//! - [`check_game_files_from_db`] — reads archives from the installer DB (used
//!   inside the install pipeline, after import_wabbajack_to_db).

use crate::hash::compute_file_hash;
use crate::modlist::{Archive, DownloadState, GameFileSourceState, Modlist, ModlistDb};
use crate::paths::resolve_case_insensitive;
use anyhow::Result;
use rayon::prelude::*;
use std::path::{Path, PathBuf};

/// Per-file verification result.
#[derive(Debug, Clone)]
pub struct GameFileCheck {
    /// Relative path from `GameFileSourceState.game_file` (Windows-style, as it
    /// appears in the modlist).
    pub file: String,
    /// Expected hash from the modlist.
    pub expected_hash: String,
    /// What we saw on disk.
    pub status: CheckStatus,
}

#[derive(Debug, Clone)]
pub enum CheckStatus {
    /// File present and hash matches.
    Ok,
    /// File not found under `game_dir` or `game_dir/Data`.
    Missing,
    /// File present but hash differs. Holds the actual hash for diagnostics.
    Mismatch(String),
    /// Read error while hashing (I/O, permission, etc).
    ReadError(String),
}

impl CheckStatus {
    pub fn is_ok(&self) -> bool {
        matches!(self, CheckStatus::Ok)
    }
}

/// Result of a full preflight run over one candidate game directory.
#[derive(Debug, Clone)]
pub struct PreflightReport {
    /// The game directory that was checked.
    pub game_dir: PathBuf,
    /// Total number of game files the modlist required.
    pub total: usize,
    /// One entry per required file. Empty `checks` means the modlist has no
    /// GameFileSource archives (trivially OK).
    pub checks: Vec<GameFileCheck>,
}

impl PreflightReport {
    pub fn all_ok(&self) -> bool {
        self.checks.iter().all(|c| c.status.is_ok())
    }

    pub fn missing(&self) -> Vec<&GameFileCheck> {
        self.checks
            .iter()
            .filter(|c| matches!(c.status, CheckStatus::Missing))
            .collect()
    }

    pub fn mismatched(&self) -> Vec<&GameFileCheck> {
        self.checks
            .iter()
            .filter(|c| {
                matches!(
                    c.status,
                    CheckStatus::Mismatch(_) | CheckStatus::ReadError(_)
                )
            })
            .collect()
    }

    /// Produce a multi-line human-readable report suitable for logging.
    pub fn format_summary(&self) -> String {
        let missing = self.missing();
        let mismatched = self.mismatched();
        if self.all_ok() {
            return format!(
                "All {} game files verified in {}",
                self.total,
                self.game_dir.display()
            );
        }

        let mut s = format!(
            "Game file preflight FAILED for {}: {} missing, {} hash mismatch (out of {})\n",
            self.game_dir.display(),
            missing.len(),
            mismatched.len(),
            self.total
        );
        for c in missing {
            s.push_str(&format!("  MISSING:  {}\n", c.file));
        }
        for c in mismatched {
            match &c.status {
                CheckStatus::Mismatch(actual) => s.push_str(&format!(
                    "  MISMATCH: {} (expected {}, got {})\n",
                    c.file, c.expected_hash, actual
                )),
                CheckStatus::ReadError(err) => {
                    s.push_str(&format!("  READ ERR: {} — {}\n", c.file, err))
                }
                _ => {}
            }
        }
        s
    }
}

/// Files that ship in known-different bytes across editions or stores but
/// behave identically at runtime — accept a size/hash mismatch rather than
/// failing. Match is case-insensitive against the basename of the modlist's
/// `game_file` path / archive name. Both preflight and the GameFile copy
/// step consult this list, so we accept the same set everywhere.
///
/// Add new entries here when modlist authors hit known alt-variant pain.
const ALT_VARIANT_FILE_BASENAMES: &[&str] = &[
    // Skyrim SE Curios Creation Club — Steam vs Bethesda.net builds ship
    // different bytes but the same runtime content. Wabbajack has accepted
    // this since forever.
    "ccbgssse037-curios.esl",
    "ccbgssse037-curios.bsa",
    // Fallout 4 Creation Club — same alt-variant pattern (Steam-Pipe-pushed
    // CC files re-stamped at distribution time so size + hash drift while
    // the gameplay payload is identical). Confirmed against modlists that
    // bake these in (e.g. Fallen World).
    "ccotmfo4001-remnants.esl",
    "ccbgsfo4046-tescan.esl",
];

/// Returns true if the given modlist `game_file` path (or archive name) is on
/// the known alt-variant list. Hash *and* size mismatches on these files are
/// warned-and-accepted instead of failed.
pub fn has_known_alt_variant(game_file: &str) -> bool {
    let basename = game_file
        .rsplit(['\\', '/'])
        .next()
        .unwrap_or(game_file)
        .to_lowercase();
    ALT_VARIANT_FILE_BASENAMES
        .iter()
        .any(|alt| basename == *alt)
}

/// Iterate a modlist's archives and collect just the GameFileSource entries we
/// need to verify.
fn collect_game_files_from_archives(archives: &[Archive]) -> Vec<GameFileSourceState> {
    archives
        .iter()
        .filter_map(|a| match &a.state {
            DownloadState::GameFileSource(gf) => Some(gf.clone()),
            _ => None,
        })
        .collect()
}

/// Same but from database rows (state is a JSON string there).
fn collect_game_files_from_db(db: &ModlistDb) -> Result<Vec<GameFileSourceState>> {
    let archives = db.get_all_archives()?;
    let mut out = Vec::new();
    for a in archives {
        if !a.state_json.contains("GameFileSourceDownloader") {
            continue;
        }
        if let Ok(DownloadState::GameFileSource(gf)) =
            serde_json::from_str::<DownloadState>(&a.state_json)
        {
            out.push(gf);
        }
    }
    Ok(out)
}

/// Core verification: given a list of required game files and a target game
/// directory, hash everything in parallel and return a report.
fn verify(game_files: &[GameFileSourceState], game_dir: &Path) -> PreflightReport {
    let total = game_files.len();

    // Parallel hashing — each GameFileSource is usually tens of MB; a handful
    // of rayon workers drives wall-clock close to disk-bound.
    let checks: Vec<GameFileCheck> = game_files
        .par_iter()
        .map(|gfs| {
            let file = gfs.game_file.clone();
            let expected_hash = gfs.hash.clone();

            // Try the path as written, then under Data/ which is where most
            // Bethesda GameFileSource entries actually live (they encode `.esm`
            // without a leading `Data\\` sometimes, sometimes with).
            let resolved = resolve_case_insensitive(game_dir, &file)
                .or_else(|| resolve_case_insensitive(game_dir, &format!("Data/{}", file)));

            let status = match resolved {
                None => CheckStatus::Missing,
                Some(path) => match compute_file_hash(&path) {
                    Ok(h) if h == expected_hash => CheckStatus::Ok,
                    Ok(h) if has_known_alt_variant(&file) => {
                        // Same content, different store/edition variant
                        // (e.g. Curios Steam vs Bethesda) — accept hash
                        // mismatch but keep a debug log via Ok status.
                        tracing::warn!(
                            "Game file '{}' hash differs from expected ({} vs {}) but \
                             is on known-alternate-variant list — accepting",
                            file,
                            h,
                            expected_hash
                        );
                        CheckStatus::Ok
                    }
                    Ok(h) => CheckStatus::Mismatch(h),
                    Err(e) => CheckStatus::ReadError(e.to_string()),
                },
            };

            GameFileCheck {
                file,
                expected_hash,
                status,
            }
        })
        .collect();

    PreflightReport {
        game_dir: game_dir.to_path_buf(),
        total,
        checks,
    }
}

/// Preflight-check a parsed modlist against a candidate game directory.
///
/// Intended for the auto-detect flow in `main.rs`, where we want to probe
/// several candidate installs (Steam, Heroic/GOG) before committing to one.
pub fn check_game_files_from_modlist(modlist: &Modlist, game_dir: &Path) -> PreflightReport {
    let game_files = collect_game_files_from_archives(&modlist.archives);
    verify(&game_files, game_dir)
}

/// Preflight-check against the installer DB. Runs inside the install pipeline
/// after `import_wabbajack_to_db` has populated the archives table.
pub fn check_game_files_from_db(db: &ModlistDb, game_dir: &Path) -> Result<PreflightReport> {
    let game_files = collect_game_files_from_db(db)?;
    Ok(verify(&game_files, game_dir))
}
