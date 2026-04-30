//! Collection install orchestrator with download+extract overlap.
//!
//! Mirrors the Wabbajack streaming pipeline (`crate::installer::pipeline`):
//! one tokio task pool fetches archives, a rayon pool extracts each archive
//! as soon as it lands. After all mods finish, modlist.txt and plugins.txt
//! are written from the populated `CollectionDb`.

use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{bail, Context, Result};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

use super::archive::{fetch_one, FetchOutcome};
use super::db::{CollectionDb, ModDbEntry, ModStatus};
use super::fomod::{execute_fomod, find_module_config, parse_fomod};
use super::modlist::{ModInfo, ModListGenerator, ModRule};
use super::patches::{apply_patches_for_mod, PatchSummary};
use crate::archive::sevenzip;
use crate::downloaders::{HttpClient, NexusDownloader};
use crate::games::GameType;
use crate::loot::{self, PluginSorter};

// ============================================================================
// Public API
// ============================================================================

/// Configuration for a streaming collection install.
#[derive(Debug, Clone)]
pub struct InstallConfig {
    /// Path to a parsed `collection.json` (use `super::fetch` to acquire one).
    pub collection_path: PathBuf,
    /// Path used for the install database (defaults to `output_dir/collection.db`).
    pub db_path: Option<PathBuf>,
    /// Where extracted mod folders go: `<mods_dir>/<mod_folder>/...`.
    pub mods_dir: PathBuf,
    /// Where archive files are stored.
    pub downloads_dir: PathBuf,
    /// Output dir (instance root) — modlist.txt + plugins.txt land in
    /// `output_dir/profiles/Default/`.
    pub output_dir: PathBuf,
    /// Nexus Premium API key (free-tier handoff is a follow-up).
    pub nexus_api_key: String,
    /// Max concurrent downloads. Default 4 — Nexus rate-limit friendly.
    pub max_concurrent_downloads: usize,
    /// Optional game install path. Required for LOOT plugin sorting; without
    /// it, plugins.txt falls back to collection-declared order.
    pub game_path: Option<PathBuf>,
    /// Root directory containing collection.json *and* the `patches/` subtree
    /// (the bsdiff blobs Vortex bundles into the collection ZIP). Defaults to
    /// `collection_path.parent()` when `None`.
    pub collection_root: Option<PathBuf>,
}

impl Default for InstallConfig {
    fn default() -> Self {
        Self {
            collection_path: PathBuf::new(),
            db_path: None,
            mods_dir: PathBuf::new(),
            downloads_dir: PathBuf::new(),
            output_dir: PathBuf::new(),
            nexus_api_key: String::new(),
            max_concurrent_downloads: 4,
            game_path: None,
            collection_root: None,
        }
    }
}

/// Aggregate stats returned from a streaming install.
#[derive(Debug, Default, Clone)]
pub struct InstallStats {
    pub mods_total: usize,
    pub mods_cached: usize,
    pub mods_downloaded: usize,
    pub mods_extracted: usize,
    pub mods_failed: usize,
    pub mods_manual: usize,
    /// Mods whose extract was a no-op because the install marker matched.
    pub mods_skipped: usize,
    /// Bsdiff patch counters across all mods.
    pub patches_applied: usize,
    pub patches_skipped_crc: usize,
    pub patches_missing: usize,
    pub patches_failed: usize,
    pub elapsed_secs: f64,
}

/// Run the streaming install end to end.
pub async fn install_collection_streaming(config: InstallConfig) -> Result<InstallStats> {
    let started = Instant::now();
    validate_config(&config)?;

    // Load + parse collection.json, populate DB.
    let collection = super::load_collection(&config.collection_path)?;
    let db_path = config
        .db_path
        .clone()
        .unwrap_or_else(|| config.output_dir.join("collection.db"));
    std::fs::create_dir_all(&config.output_dir)?;
    let mut db = CollectionDb::open(&db_path)?;
    db.import_collection(&collection)?;

    let game_domain = collection.get_domain_name().to_string();
    info!(
        "Installing collection '{}' ({} mods, game={})",
        collection.get_name(),
        collection.mods.len(),
        game_domain
    );

    // Validate Nexus credentials up-front so we fail fast.
    let nexus = Arc::new(NexusDownloader::new(&config.nexus_api_key)?);
    let user = nexus.validate().await?;
    info!(
        "Nexus user '{}' (Premium: {})",
        user.name, user.is_premium
    );

    let http = Arc::new(HttpClient::new()?);

    std::fs::create_dir_all(&config.mods_dir)?;
    std::fs::create_dir_all(&config.downloads_dir)?;

    let mods = db.get_all_mods()?;
    let total = mods.len();
    if total == 0 {
        warn!("collection has zero mods — nothing to install");
        return Ok(InstallStats::default());
    }

    // Build a multi-progress: one bar for downloads, one for extracts. Both
    // size `total`; they advance independently as events arrive.
    let mp = MultiProgress::new();
    let dl_bar = mp.add(ProgressBar::new(total as u64));
    dl_bar.set_style(
        ProgressStyle::with_template(
            "  download {bar:30.cyan/blue} {pos}/{len} ({percent}%) {msg}",
        )
        .unwrap()
        .progress_chars("=>-"),
    );
    dl_bar.enable_steady_tick(std::time::Duration::from_millis(200));
    let ex_bar = mp.add(ProgressBar::new(total as u64));
    ex_bar.set_style(
        ProgressStyle::with_template(
            "  extract  {bar:30.green/blue} {pos}/{len} ({percent}%) {msg}",
        )
        .unwrap()
        .progress_chars("=>-"),
    );
    ex_bar.enable_steady_tick(std::time::Duration::from_millis(200));

    // Channel: download tasks → extract pool.
    let (extract_tx, extract_rx) = mpsc::sync_channel::<DownloadEvent>(32);

    // Spawn download driver on its own task.
    let download_handle = {
        let http = http.clone();
        let nexus = nexus.clone();
        let downloads_dir = config.downloads_dir.clone();
        let game_domain = game_domain.clone();
        let max_concurrent = config.max_concurrent_downloads.max(1);
        let mods = mods.clone();
        tokio::spawn(async move {
            run_download_pool(
                mods,
                http,
                nexus,
                downloads_dir,
                game_domain,
                max_concurrent,
                extract_tx,
            )
            .await
        })
    };

    // Spawn dedicated DB-writer thread (rusqlite Connection is Send but !Sync).
    // Extract workers cannot safely borrow `&db` across rayon::scope, so DB
    // updates flow through this channel instead.
    let (db_tx, db_rx) = mpsc::channel::<DbEvent>();
    let db_path_for_writer = db_path.clone();
    let db_writer = std::thread::spawn(move || -> Result<()> {
        let writer_db = CollectionDb::open(&db_path_for_writer)?;
        for event in db_rx {
            apply_db_event(&writer_db, event);
        }
        Ok(())
    });

    // Drain events; dispatch each Ready archive to rayon for extract.
    let stats = run_extract_loop(
        extract_rx,
        db_tx.clone(),
        &config,
        total,
        dl_bar.clone(),
        ex_bar.clone(),
        game_domain.clone(),
    )?;
    drop(db_tx);
    dl_bar.finish_with_message("done");
    ex_bar.finish_with_message("done");

    // Reap the download driver — surface any task panic or error.
    download_handle
        .await
        .map_err(|e| anyhow::anyhow!("download task panicked: {e}"))??;

    db_writer
        .join()
        .map_err(|_| anyhow::anyhow!("DB writer thread panicked"))??;

    // Sort plugins via LOOT if we have a game path + supported game.
    let sorted_plugins = sort_plugins_or_fallback(&db, &config, &game_domain).await;

    // Post-extract: assemble modlist.txt + plugins.txt.
    write_modlist_outputs(&db, &config, &sorted_plugins)?;

    // Write the MO2 instance scaffolding so the output dir is a valid
    // portable instance MO2 can open directly.
    write_mo2_instance_files(&config, &game_domain)?;

    let mut stats = stats;
    stats.mods_total = total;
    stats.elapsed_secs = started.elapsed().as_secs_f64();
    info!(
        "Collection install done in {:.1}s — {} cached, {} downloaded, {} extracted, {} skipped, {} failed, {} manual; patches: {} applied / {} skipped (CRC) / {} missing / {} failed",
        stats.elapsed_secs,
        stats.mods_cached,
        stats.mods_downloaded,
        stats.mods_extracted,
        stats.mods_skipped,
        stats.mods_failed,
        stats.mods_manual,
        stats.patches_applied,
        stats.patches_skipped_crc,
        stats.patches_missing,
        stats.patches_failed,
    );
    Ok(stats)
}

// ============================================================================
// Download pool
// ============================================================================

/// One download outcome, sent across the channel to the extract loop.
enum DownloadEvent {
    Ready {
        mod_entry: ModDbEntry,
        archive_path: PathBuf,
        was_cached: bool,
    },
    Manual {
        mod_entry: ModDbEntry,
        url: String,
        notes: String,
    },
    Failed {
        mod_entry: ModDbEntry,
        error: String,
    },
}

async fn run_download_pool(
    mods: Vec<ModDbEntry>,
    http: Arc<HttpClient>,
    nexus: Arc<NexusDownloader>,
    downloads_dir: PathBuf,
    game_domain: String,
    max_concurrent: usize,
    tx: mpsc::SyncSender<DownloadEvent>,
) -> Result<()> {
    let sem = Arc::new(Semaphore::new(max_concurrent));
    let mut handles = Vec::with_capacity(mods.len());

    for mod_entry in mods {
        let permit = sem.clone().acquire_owned().await?;
        let http = http.clone();
        let nexus = nexus.clone();
        let downloads_dir = downloads_dir.clone();
        let game_domain = game_domain.clone();
        let tx = tx.clone();

        handles.push(tokio::spawn(async move {
            let _permit = permit; // released on drop
            let archive_path = downloads_dir.join(&mod_entry.logical_filename);
            let event =
                match fetch_one(&mod_entry, &archive_path, &http, &nexus, &game_domain).await {
                    Ok(FetchOutcome::Cached(path)) => DownloadEvent::Ready {
                        mod_entry,
                        archive_path: path,
                        was_cached: true,
                    },
                    Ok(FetchOutcome::Downloaded(path)) => DownloadEvent::Ready {
                        mod_entry,
                        archive_path: path,
                        was_cached: false,
                    },
                    Ok(FetchOutcome::Manual { url, notes }) => DownloadEvent::Manual {
                        mod_entry,
                        url,
                        notes,
                    },
                    Err(e) => DownloadEvent::Failed {
                        mod_entry,
                        error: format!("{e:#}"),
                    },
                };
            // If the receiver is gone the install was aborted; just drop.
            let _ = tx.send(event);
        }));
    }

    drop(tx);

    for h in handles {
        if let Err(e) = h.await {
            warn!("download task join failure: {e}");
        }
    }
    Ok(())
}

// ============================================================================
// Extract loop
// ============================================================================

fn run_extract_loop(
    rx: mpsc::Receiver<DownloadEvent>,
    db_tx: mpsc::Sender<DbEvent>,
    config: &InstallConfig,
    _total: usize,
    dl_bar: ProgressBar,
    ex_bar: ProgressBar,
    game_domain_for_meta: String,
) -> Result<InstallStats> {
    use std::sync::atomic::Ordering;

    let stats = Arc::new(InstallStatsAtomic::default());

    // Resolve the collection root used to find bsdiff patch blobs. Defaults
    // to the parent of `collection.json`.
    let collection_root: Arc<PathBuf> = Arc::new(
        config
            .collection_root
            .clone()
            .or_else(|| config.collection_path.parent().map(Path::to_path_buf))
            .unwrap_or_else(|| PathBuf::from(".")),
    );

    // Each Ready event spawns one rayon task and bumps `spawned`. After the
    // download channel closes we drain `done_rx` for `spawned` completions.
    // We can't pass `&Receiver<DownloadEvent>` into rayon::scope because mpsc
    // receivers are !Sync, so we drive the recv loop on this thread directly
    // and use rayon::spawn (global pool) for fan-out.
    let (done_tx, done_rx) = mpsc::channel::<()>();
    let mut spawned = 0usize;

    loop {
        match rx.recv() {
            Ok(DownloadEvent::Ready {
                mod_entry,
                archive_path,
                was_cached,
            }) => {
                if was_cached {
                    stats.cached.fetch_add(1, Ordering::Relaxed);
                } else {
                    stats.downloaded.fetch_add(1, Ordering::Relaxed);
                }
                dl_bar.inc(1);
                dl_bar.set_message(if was_cached {
                    format!("cached {}", short_name(&mod_entry.name))
                } else {
                    format!("got {}", short_name(&mod_entry.name))
                });
                if let Err(e) = write_meta_sidecar(&mod_entry, &archive_path, &game_domain_for_meta) {
                    warn!("meta sidecar failed for {}: {:#}", mod_entry.name, e);
                }
                let _ = db_tx.send(DbEvent::MarkDownloaded {
                    mod_id: mod_entry.id,
                    local_path: archive_path.display().to_string(),
                });

                let mods_dir = config.mods_dir.clone();
                let collection_root_arc = collection_root.clone();
                let db_tx_inner = db_tx.clone();
                let stats_inner = stats.clone();
                let done_tx_inner = done_tx.clone();
                let ex_bar_inner = ex_bar.clone();
                spawned += 1;
                rayon::spawn(move || {
                    let mod_name = mod_entry.name.clone();
                    ex_bar_inner.set_message(format!("→ {}", short_name(&mod_name)));
                    match extract_mod(&mod_entry, &archive_path, &mods_dir, &collection_root_arc) {
                        Ok(outcome) => {
                            ex_bar_inner.inc(1);
                            match outcome {
                                ExtractOutcome::Done(patch_summary) => {
                                    stats_inner.extracted.fetch_add(1, Ordering::Relaxed);
                                    ex_bar_inner
                                        .set_message(format!("✓ {}", short_name(&mod_name)));
                                    stats_inner
                                        .patches_applied
                                        .fetch_add(patch_summary.applied, Ordering::Relaxed);
                                    stats_inner
                                        .patches_skipped_crc
                                        .fetch_add(patch_summary.skipped_crc, Ordering::Relaxed);
                                    stats_inner
                                        .patches_missing
                                        .fetch_add(patch_summary.missing_diff, Ordering::Relaxed);
                                    stats_inner
                                        .patches_failed
                                        .fetch_add(patch_summary.failed, Ordering::Relaxed);
                                }
                                ExtractOutcome::Skipped => {
                                    stats_inner.skipped.fetch_add(1, Ordering::Relaxed);
                                    ex_bar_inner
                                        .set_message(format!("· {}", short_name(&mod_name)));
                                }
                            }
                            let _ = db_tx_inner.send(DbEvent::MarkInstalled {
                                mod_id: mod_entry.id,
                            });
                        }
                        Err(e) => {
                            stats_inner.failed.fetch_add(1, Ordering::Relaxed);
                            ex_bar_inner.inc(1);
                            ex_bar_inner.set_message(format!("✗ {}", short_name(&mod_name)));
                            let msg = format!("{e:#}");
                            error!("Extract failed for {}: {}", mod_entry.name, msg);
                            let _ = db_tx_inner.send(DbEvent::MarkFailed {
                                mod_id: mod_entry.id,
                                error: msg,
                            });
                        }
                    }
                    let _ = done_tx_inner.send(());
                });
            }
            Ok(DownloadEvent::Manual {
                mod_entry,
                url,
                notes,
            }) => {
                stats.manual.fetch_add(1, Ordering::Relaxed);
                dl_bar.inc(1);
                ex_bar.inc(1);
                dl_bar.set_message(format!("manual {}", short_name(&mod_entry.name)));
                info!(
                    "Manual download required: {} → {} ({})",
                    mod_entry.name, url, notes
                );
                let _ = db_tx.send(DbEvent::MarkFailed {
                    mod_id: mod_entry.id,
                    error: format!("manual download required: {url}"),
                });
            }
            Ok(DownloadEvent::Failed { mod_entry, error }) => {
                stats.failed.fetch_add(1, Ordering::Relaxed);
                dl_bar.inc(1);
                ex_bar.inc(1);
                dl_bar.set_message(format!("FAIL {}", short_name(&mod_entry.name)));
                warn!("Download failed: {} — {}", mod_entry.name, error);
                let _ = db_tx.send(DbEvent::MarkFailed {
                    mod_id: mod_entry.id,
                    error,
                });
            }
            Err(_) => break, // download pool dropped its sender
        }
    }

    // Wait for every spawned extract to complete.
    drop(done_tx);
    for _ in 0..spawned {
        let _ = done_rx.recv();
    }

    Ok(stats.snapshot())
}

#[derive(Default)]
struct InstallStatsAtomic {
    cached: std::sync::atomic::AtomicUsize,
    downloaded: std::sync::atomic::AtomicUsize,
    extracted: std::sync::atomic::AtomicUsize,
    failed: std::sync::atomic::AtomicUsize,
    manual: std::sync::atomic::AtomicUsize,
    skipped: std::sync::atomic::AtomicUsize,
    patches_applied: std::sync::atomic::AtomicUsize,
    patches_skipped_crc: std::sync::atomic::AtomicUsize,
    patches_missing: std::sync::atomic::AtomicUsize,
    patches_failed: std::sync::atomic::AtomicUsize,
}

impl InstallStatsAtomic {
    fn snapshot(&self) -> InstallStats {
        use std::sync::atomic::Ordering::Relaxed;
        InstallStats {
            mods_total: 0,
            mods_cached: self.cached.load(Relaxed),
            mods_downloaded: self.downloaded.load(Relaxed),
            mods_extracted: self.extracted.load(Relaxed),
            mods_failed: self.failed.load(Relaxed),
            mods_manual: self.manual.load(Relaxed),
            mods_skipped: self.skipped.load(Relaxed),
            patches_applied: self.patches_applied.load(Relaxed),
            patches_skipped_crc: self.patches_skipped_crc.load(Relaxed),
            patches_missing: self.patches_missing.load(Relaxed),
            patches_failed: self.patches_failed.load(Relaxed),
            elapsed_secs: 0.0,
        }
    }
}

enum DbEvent {
    MarkDownloaded { mod_id: i64, local_path: String },
    MarkInstalled { mod_id: i64 },
    MarkFailed { mod_id: i64, error: String },
}

fn apply_db_event(db: &CollectionDb, event: DbEvent) {
    let result = match event {
        DbEvent::MarkDownloaded { mod_id, local_path } => db.mark_mod_downloaded(mod_id, &local_path),
        DbEvent::MarkInstalled { mod_id } => db.update_mod_status(mod_id, ModStatus::Installed),
        DbEvent::MarkFailed { mod_id, error } => db.mark_mod_failed(mod_id, &error),
    };
    if let Err(e) = result {
        warn!("DB write failed: {e}");
    }
}

/// Bethesda-mod "stop folders": presence of any of these (case-insensitive,
/// directly inside a directory) marks that directory as the data root.
/// Mirrors the heuristic Vortex/MO2 use to detect when an archive's root is
/// already at Data level vs nested inside a wrapper subdir.
///
/// `data` is intentionally NOT here: MO2 deploys mod-folder contents AS-IF
/// rooted at Data, so an archive with a `Data/` wrapper needs that wrapper
/// stripped, not preserved.
const STOP_FOLDERS: &[&str] = &[
    "meshes", "textures", "scripts", "interface", "sound", "sounds", "music",
    "fonts", "shaders", "video", "voices", "seq", "translations", "lodsettings",
    "source", "skse", "skse64", "sksevr", "f4se", "calientetools", "edit scripts",
    "skyproc patchers", "tools", "dyndolod", "dynamicanimationreplacer",
    "scriptsource", "pluginsource", "menus", "strings", "actors", "behaviors",
];

/// File extensions that indicate a directory is at the Data-folder level.
/// `.dll` and `.asi` are intentionally absent — those land under framework
/// subdirs (`SKSE/Plugins/`, `NetScriptFramework/Plugins/`), not Data root.
const STOP_EXTENSIONS: &[&str] = &[
    ".esp", ".esm", ".esl", ".bsa", ".ba2",
];

/// Returns true if `dir` looks like a Bethesda Data root.
fn looks_like_data_root(dir: &Path) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name_str = name.to_string_lossy().to_lowercase();
        if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            if STOP_FOLDERS.contains(&name_str.as_str()) {
                return true;
            }
        } else if let Some(dot) = name_str.rfind('.') {
            let ext = &name_str[dot..];
            if STOP_EXTENSIONS.contains(&ext) {
                return true;
            }
        }
    }
    false
}

/// Install a mod via its recorded `hashes[]` (Vortex `installMode == 'clone'`).
///
/// The recorded paths are *deployed* paths — what the file should look like
/// after a FOMOD has been resolved. The archive itself often nests files
/// under FOMOD option folders (e.g. `00 Common/meshes/...`,
/// `01 Characters/Aela/meshes/...`), so a path-based lookup misses them.
///
/// Strategy: extract the whole archive, walk every file computing MD5,
/// build a `{md5 → src_path}` index, then for each `(rel_path, md5)` in
/// the recorded list place the matching file at `mod_dest/rel_path`.
fn install_via_hashes(
    mod_entry: &ModDbEntry,
    archive_path: &Path,
    mods_dir: &Path,
    mod_dest: &Path,
) -> Result<()> {
    use std::collections::HashMap;
    use walkdir::WalkDir;

    let hashes_json = mod_entry
        .hashes_json
        .as_deref()
        .filter(|j| !j.is_empty() && *j != "[]")
        .ok_or_else(|| anyhow::anyhow!("hash-install called with empty hashes_json"))?;
    let recorded: Vec<crate::collection::FileHash> = serde_json::from_str(hashes_json)
        .context("parse hashes_json")?;
    if recorded.is_empty() {
        bail!("hash list parsed but empty");
    }

    let temp = tempfile::tempdir_in(mods_dir).context("tempdir for hash-install extract")?;
    sevenzip::extract_all(archive_path, temp.path())
        .with_context(|| format!("hash-install extract: {}", archive_path.display()))?;

    // Build an md5 → source-path index of every extracted file. The collect
    // is single-threaded since file count is bounded by the archive and we
    // don't want to thrash the disk.
    let mut by_md5: HashMap<String, std::path::PathBuf> = HashMap::new();
    let mut hashed_files = 0usize;
    for entry in WalkDir::new(temp.path())
        .into_iter()
        .filter_map(|e| e.ok())
    {
        if !entry.file_type().is_file() {
            continue;
        }
        let bytes = match std::fs::read(entry.path()) {
            Ok(b) => b,
            Err(e) => {
                warn!("hash-install: read {} failed: {e}", entry.path().display());
                continue;
            }
        };
        let digest = format!("{:x}", md5::compute(&bytes));
        // Keep the first occurrence — duplicate-MD5 collisions are rare and
        // either source serves the same content.
        by_md5.entry(digest).or_insert_with(|| entry.path().to_path_buf());
        hashed_files += 1;
    }

    let mut copied = 0usize;
    let mut missing = 0usize;
    for hash in &recorded {
        let rel = hash.path.replace('\\', "/");
        let key = hash.md5.to_lowercase();
        let Some(src) = by_md5.get(&key) else {
            warn!("hash-install: md5 {} ({}) not found in archive", hash.md5, rel);
            missing += 1;
            continue;
        };
        let dest = join_rel(mod_dest, &rel);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // Always copy — duplicate-MD5 is common (e.g. shared eye/head NIFs
        // referenced from multiple character paths). A `rename` would move
        // the temp source out from under the next dest sharing that md5.
        std::fs::copy(src, &dest)
            .with_context(|| format!("copy {} -> {}", src.display(), dest.display()))?;
        copied += 1;
    }

    info!(
        "hash-install for {}: {}/{} files placed via md5 ({} hashed, {} missing)",
        mod_entry.name,
        copied,
        recorded.len(),
        hashed_files,
        missing
    );
    Ok(())
}

/// Resolve a forward-slash relative path against `base` without losing
/// platform path semantics on the destination side.
fn join_rel(base: &Path, rel: &str) -> std::path::PathBuf {
    let mut out = base.to_path_buf();
    for component in rel.split('/').filter(|c| !c.is_empty()) {
        out.push(component);
    }
    out
}

/// Returns true if `dir` is a sensible peel target — i.e. somewhere down
/// a single-subdir chain (up to 5 levels deep) we see a Data-root signal
/// or a literal `data/` directory. Distinguishes:
///
/// - `WrapperA/WrapperB/WrapperC/data/meshes/...` → peel (chain ends in
///   data + Data-root signal).
/// - `NetScriptFramework/Plugins/foo.dll` → keep (chain ends in
///   `[foo.dll, ...]` files which aren't a Data-root signal because
///   `.dll` is intentionally absent from `STOP_EXTENSIONS`).
fn wrapper_looks_legit_to_peel(wrapper: &Path) -> bool {
    let mut current = wrapper.to_path_buf();
    for _ in 0..5 {
        if looks_like_data_root(&current) {
            return true;
        }
        let Ok(read) = std::fs::read_dir(&current) else {
            return false;
        };
        let mut subdirs: Vec<std::path::PathBuf> = Vec::new();
        let mut files = 0usize;
        let mut data_short_circuit = false;
        for entry in read.flatten() {
            let Ok(ft) = entry.file_type() else { continue };
            if ft.is_dir() {
                if entry.file_name().to_string_lossy().eq_ignore_ascii_case("data") {
                    data_short_circuit = true;
                }
                subdirs.push(entry.path());
            } else if entry.file_name() != MARKER_FILE {
                files += 1;
            }
        }
        if data_short_circuit {
            return true;
        }
        if files == 0 && subdirs.len() == 1 {
            current = subdirs.into_iter().next().unwrap();
        } else {
            return false;
        }
    }
    false
}

/// Strip wrapper directories until `mod_dest` looks like a Data root. Two
/// shapes are peeled:
///
/// 1. **`data/` (or `Data/`) at root** — Bethesda archives often ship
///    with their files under a literal `Data/` directory. MO2 already
///    deploys mod contents as if rooted at Data, so the wrapper has to
///    come off.
/// 2. **Single-subdir wrapper whose contents look like Data** — many
///    archives wrap their payload in a folder (named after the mod, the
///    archive, or something arbitrary). We peel iff the subdir's contents
///    look like a Data root or a `data/` exists one level deeper. That
///    rejects framework dirs like `NetScriptFramework/Plugins/foo.dll`,
///    where the wrapper itself is the Data-root entry.
///
/// Runs iteratively (capped at 4 passes) to handle nested cases like
/// `WrapperName/Data/meshes/...`.
fn flatten_wrapper_dir(mod_dest: &Path) -> Result<()> {
    for _ in 0..4 {
        if looks_like_data_root(mod_dest) {
            return Ok(());
        }

        // Collect the immediate children, ignoring our hidden marker.
        let mut subdirs: Vec<std::path::PathBuf> = Vec::new();
        let mut files_at_root = 0usize;
        let mut data_dir: Option<std::path::PathBuf> = None;
        for entry in std::fs::read_dir(mod_dest)?.flatten() {
            let ft = entry.file_type()?;
            if ft.is_dir() {
                let name = entry.file_name().to_string_lossy().to_lowercase();
                if name == "data" {
                    data_dir = Some(entry.path());
                }
                subdirs.push(entry.path());
            } else {
                if entry.file_name() == MARKER_FILE {
                    continue;
                }
                files_at_root += 1;
            }
        }

        let wrapper = if let Some(d) = data_dir {
            // Literal `data/` strip — siblings stay put.
            d
        } else if files_at_root == 0 && subdirs.len() == 1
            && wrapper_looks_legit_to_peel(&subdirs[0])
        {
            subdirs.into_iter().next().unwrap()
        } else {
            return Ok(());
        };

        // Lift wrapper children up. Skip on collision rather than clobber a
        // sibling that already lives at the mod root.
        for entry in std::fs::read_dir(&wrapper)?.flatten() {
            let from = entry.path();
            let to = mod_dest.join(entry.file_name());
            if to.exists() {
                warn!(
                    "flatten: collision, leaving {} alone",
                    to.display()
                );
                continue;
            }
            std::fs::rename(&from, &to)
                .with_context(|| format!("flatten: {} -> {}", from.display(), to.display()))?;
        }
        // Remove the wrapper if it's empty (collisions may have left files).
        let _ = std::fs::remove_dir(&wrapper);
    }
    Ok(())
}

/// Cache marker written into a mod folder after successful extract + patch.
/// Lets a rerun skip work when the inputs (archive MD5, FOMOD choices,
/// bsdiff patches map) all match the previous successful run.
#[derive(serde::Serialize, serde::Deserialize)]
struct InstalledMarker {
    archive_md5: String,
    choices_json: Option<String>,
    patches_json: Option<String>,
    hashes_json: Option<String>,
    schema: u32,
}

// `.mohidden` suffix stops MO2 from treating the marker as a conflict source
// when comparing against other mods that contain a same-named file.
const MARKER_FILE: &str = ".clf3-installed.json.mohidden";
/// Pre-`.mohidden` filename. Migrated in place on first encounter so old
/// installs don't get re-extracted just because of the rename.
const LEGACY_MARKER_FILE: &str = ".clf3-installed.json";
// Bumped when the `InstalledMarker` shape changes — old markers fail the
// `marker_matches` schema check and the mod re-installs cleanly.
const MARKER_SCHEMA: u32 = 2;

fn marker_for(entry: &ModDbEntry) -> InstalledMarker {
    InstalledMarker {
        archive_md5: entry.md5.clone(),
        choices_json: entry.choices_json.clone(),
        patches_json: entry.patches_json.clone(),
        hashes_json: entry.hashes_json.clone(),
        schema: MARKER_SCHEMA,
    }
}

fn marker_matches(path: &Path, expected: &InstalledMarker) -> bool {
    let Ok(bytes) = std::fs::read(path) else {
        return false;
    };
    let Ok(found): Result<InstalledMarker, _> = serde_json::from_slice(&bytes) else {
        return false;
    };
    found.schema == expected.schema
        && found.archive_md5 == expected.archive_md5
        && found.choices_json == expected.choices_json
        && found.patches_json == expected.patches_json
        && found.hashes_json == expected.hashes_json
}

/// If a pre-`.mohidden` marker is sitting in `mod_dest`, rename it in place
/// so subsequent runs (and MO2's conflict scan) see the hidden form. Logs
/// and ignores rename failures — worst case is a redundant re-extract.
fn migrate_legacy_marker(mod_dest: &Path) {
    let legacy = mod_dest.join(LEGACY_MARKER_FILE);
    if !legacy.exists() {
        return;
    }
    let new_path = mod_dest.join(MARKER_FILE);
    if let Err(e) = std::fs::rename(&legacy, &new_path) {
        warn!(
            "could not rename legacy marker {} -> {}: {e}",
            legacy.display(),
            new_path.display()
        );
    }
}

/// Extract one mod's archive into `mods_dir/<folder_name>/` and apply any
/// Vortex bsdiff patches recorded for the mod. FOMOD-bearing mods replay
/// saved choices via `super::fomod` first.
///
/// Skipped on rerun when the existing `.clf3-installed.json` marker matches
/// the current (archive_md5, choices, patches) tuple — i.e. nothing to do.
enum ExtractOutcome {
    /// Extraction (and patches, if any) ran. Inner summary may be empty if
    /// the mod has no patches.
    Done(PatchSummary),
    /// Marker matched — extract + patches skipped this run.
    Skipped,
}

fn extract_mod(
    mod_entry: &ModDbEntry,
    archive_path: &Path,
    mods_dir: &Path,
    collection_root: &Path,
) -> Result<ExtractOutcome> {
    let mod_dest = mods_dir.join(&mod_entry.folder_name);
    if mod_dest.is_dir() {
        migrate_legacy_marker(&mod_dest);
    }
    let marker_path = mod_dest.join(MARKER_FILE);
    let expected = marker_for(mod_entry);

    if mod_dest.is_dir()
        && !mod_entry.md5.is_empty()
        && marker_matches(&marker_path, &expected)
    {
        // Already extracted with matching inputs — nothing to redo.
        return Ok(ExtractOutcome::Skipped);
    }

    std::fs::create_dir_all(&mod_dest)?;

    // Root-deployed mods (deploy_type=dinput/enb — DLL hooks like d3dx9
    // loaders, ENB, Engine Fixes preloader) install to game root, not
    // Data/. MO2's Root Builder picks them up from `<mod>/Root/`.
    let payload_dest: std::path::PathBuf = if mod_entry.is_root_mod() {
        mod_dest.join("Root")
    } else {
        mod_dest.clone()
    };
    std::fs::create_dir_all(&payload_dest)?;

    if let Some(choices) = mod_entry.get_choices() {
        // Vortex `installMode == 'choices'` — author baked in FOMOD picks.
        // Extract to a temp dir, find ModuleConfig.xml, replay choices.
        let temp = tempfile::tempdir_in(mods_dir).context("tempdir for FOMOD extract")?;
        sevenzip::extract_all(archive_path, temp.path())
            .with_context(|| format!("FOMOD-precursor extract: {}", archive_path.display()))?;

        let (config_path, data_root) = find_module_config(temp.path()).with_context(|| {
            format!("no FOMOD config found in {}", archive_path.display())
        })?;
        let config = parse_fomod(&config_path)
            .with_context(|| format!("FOMOD parse: {}", config_path.display()))?;
        execute_fomod(&data_root, &payload_dest, &config, &choices)
            .with_context(|| format!("FOMOD execute for {}", mod_entry.name))?;
    } else if mod_entry.has_hashes() {
        // Vortex `installMode == 'clone'` (replicate) — author recorded the
        // exact post-install file list as `hashes[]`. Extract whole archive
        // and cherry-pick only those paths into the payload folder.
        install_via_hashes(mod_entry, archive_path, mods_dir, &payload_dest)
            .with_context(|| format!("hash-install for {}", mod_entry.name))?;
    } else {
        // `installMode == 'fresh'` (or no FOMOD at all) — just extract the
        // archive and lift any decorative wrapper folder.
        sevenzip::extract_all(archive_path, &payload_dest)
            .with_context(|| format!("extract: {}", archive_path.display()))?;
        if let Err(e) = flatten_wrapper_dir(&payload_dest) {
            warn!("wrapper-flatten skipped for {}: {:#}", mod_entry.name, e);
        }
    }

    // Vortex bsdiff patches are applied in place after extraction. Use the
    // mod's display name (matches the `patches/<modName>/` subtree key Vortex
    // writes via transformCollection.ts).
    let patch_summary = if let Some(patches) = mod_entry.get_patches() {
        apply_patches_for_mod(&mod_entry.name, &payload_dest, collection_root, &patches)
            .with_context(|| format!("apply patches for {}", mod_entry.name))?
    } else {
        PatchSummary::default()
    };

    // Persist the marker at the mod-folder root (alongside the optional
    // `Root/` subdir for root-deployed mods, so MO2 sees it as hidden).
    if !mod_entry.md5.is_empty() {
        let bytes = serde_json::to_vec_pretty(&expected).unwrap_or_default();
        if let Err(e) = std::fs::write(&marker_path, bytes) {
            warn!("failed to write install marker for {}: {e}", mod_entry.name);
        }
    }

    Ok(ExtractOutcome::Done(patch_summary))
}

// ============================================================================
// Post-extract: write modlist.txt / plugins.txt
// ============================================================================

fn write_modlist_outputs(
    db: &CollectionDb,
    config: &InstallConfig,
    sorted_plugins: &[String],
) -> Result<()> {
    use std::collections::HashSet;

    let profile_dir = config.output_dir.join("profiles").join("Default");
    std::fs::create_dir_all(&profile_dir)?;

    // Pull mod metadata + rules from DB.
    let all_mods = db.get_all_mods()?;
    let optional_folders: HashSet<String> = all_mods
        .iter()
        .filter(|m| m.optional)
        .map(|m| m.folder_name.clone())
        .collect();

    let mods = all_mods
        .into_iter()
        .map(|m| ModInfo {
            name: m.name,
            logical_filename: m.logical_filename,
            folder_name: m.folder_name,
            md5: m.md5,
            phase: m.phase,
        })
        .collect::<Vec<_>>();

    let rules = db
        .get_mod_rules()?
        .into_iter()
        .map(|r| ModRule {
            rule_type: r.rule_type,
            source_logical_name: r.source_filename,
            source_md5: r.source_md5,
            reference_logical_name: r.reference_filename,
            reference_md5: r.reference_md5,
        })
        .collect::<Vec<_>>();

    let mod_order = ModListGenerator::generate_mod_order_combined(
        &mods,
        &rules,
        sorted_plugins,
        &config.mods_dir,
    );

    let modlist_txt = profile_dir.join("modlist.txt");
    write_modlist_txt(&modlist_txt, &mod_order, &optional_folders)?;
    info!(
        "wrote modlist.txt ({} mods, {} optional/disabled)",
        mod_order.len(),
        optional_folders.len()
    );

    let plugins_txt = profile_dir.join("plugins.txt");
    write_plugins_txt(&plugins_txt, sorted_plugins)?;
    info!("wrote plugins.txt ({} plugins)", sorted_plugins.len());

    Ok(())
}

/// Run LOOT sort if we have game_path + a supported game; otherwise fall back
/// to the collection-declared plugin order. Never fatal — a sort failure logs
/// and uses the fallback.
async fn sort_plugins_or_fallback(
    db: &CollectionDb,
    config: &InstallConfig,
    game_domain: &str,
) -> Vec<String> {
    let collection_order = || -> Vec<String> {
        db.get_plugins()
            .map(|rows| rows.into_iter().map(|p| p.name).collect())
            .unwrap_or_default()
    };

    let Some(game_path) = config.game_path.as_ref() else {
        warn!("game_path not set — using collection-declared plugin order");
        return collection_order();
    };
    let Some(game_type) = GameType::from_nexus_domain(game_domain) else {
        warn!(
            "unsupported game domain '{}' for LOOT — using collection-declared plugin order",
            game_domain
        );
        return collection_order();
    };

    match run_loot_sort(game_type, game_path, &config.mods_dir).await {
        Ok(sorted) if !sorted.is_empty() => sorted,
        Ok(_) => {
            warn!("LOOT returned empty plugin list — using collection-declared order");
            collection_order()
        }
        Err(e) => {
            warn!("LOOT sort failed: {e:#} — using collection-declared order");
            collection_order()
        }
    }
}

async fn run_loot_sort(
    game_type: GameType,
    game_path: &Path,
    mods_dir: &Path,
) -> Result<Vec<String>> {
    let mut sorter = PluginSorter::new(game_type, game_path, mods_dir)
        .context("PluginSorter::new failed")?;
    sorter
        .ensure_masterlist(false)
        .await
        .context("LOOT masterlist load/download failed")?;
    let plugins = loot::discover_plugins(mods_dir).context("plugin discovery failed")?;
    if plugins.is_empty() {
        return Ok(Vec::new());
    }
    sorter.sort_all(&plugins)
}

fn write_modlist_txt(
    path: &Path,
    mod_folders_in_order: &[String],
    optional_folders: &std::collections::HashSet<String>,
) -> Result<()> {
    use std::io::Write;
    let mut f = std::fs::File::create(path)
        .with_context(|| format!("create modlist.txt: {}", path.display()))?;
    // MO2 reads top-of-file as highest priority. ModListGenerator returns
    // priority-descending (first = highest). `+` = enabled, `-` = disabled.
    // Optional/recommends mods install but stay disabled — user opts in.
    for folder in mod_folders_in_order {
        let prefix = if optional_folders.contains(folder) {
            '-'
        } else {
            '+'
        };
        writeln!(f, "{prefix}{folder}")?;
    }
    Ok(())
}

fn write_plugins_txt(path: &Path, plugins_in_order: &[String]) -> Result<()> {
    use std::io::Write;
    let mut f = std::fs::File::create(path)
        .with_context(|| format!("create plugins.txt: {}", path.display()))?;
    for plugin in plugins_in_order {
        writeln!(f, "*{plugin}")?;
    }
    Ok(())
}

// ============================================================================
// Validation
// ============================================================================

/// Write a `<archive>.meta` sidecar next to a freshly downloaded archive so
/// MO2 can map it back to the Nexus mod. Skipped for non-Nexus sources or
/// cached entries we already wrote on a prior run.
fn write_meta_sidecar(
    mod_entry: &ModDbEntry,
    archive_path: &Path,
    game_domain: &str,
) -> Result<()> {
    if mod_entry.source_type.eq_ignore_ascii_case("nexus") == false
        || mod_entry.mod_id <= 0
        || mod_entry.file_id <= 0
    {
        return Ok(());
    }
    let meta_path = archive_path.with_extension(
        archive_path
            .extension()
            .map(|e| format!("{}.meta", e.to_string_lossy()))
            .unwrap_or_else(|| "meta".to_string()),
    );
    if meta_path.exists() {
        return Ok(());
    }
    let body = format!(
        "[General]\n\
         gameName={game}\n\
         modID={mod_id}\n\
         fileID={file_id}\n\
         url=\n\
         name={name}\n\
         modName={name}\n\
         fileName={file_name}\n\
         hash={md5}\n\
         installed=true\n",
        game = game_domain,
        mod_id = mod_entry.mod_id,
        file_id = mod_entry.file_id,
        name = mod_entry.name.replace(['\r', '\n'], " "),
        file_name = mod_entry.logical_filename,
        md5 = mod_entry.md5,
    );
    std::fs::write(&meta_path, body)
        .with_context(|| format!("write meta: {}", meta_path.display()))?;
    Ok(())
}

/// Convert a Linux path to MO2's Wine-INI form: `Z:\\path\\with\\dbl\\backslashes`.
///
/// MO2 (a Qt/Windows app under Wine/Proton) expects paths in Z-drive form;
/// Qt's INI writer escapes single backslashes, so the *file* representation
/// uses doubled backslashes. Mirrors the inline handler in
/// `installer/handlers/inline.rs`.
fn to_wine_ini_path(p: &Path) -> String {
    let s = p
        .to_string_lossy()
        .trim_end_matches('/')
        .trim_end_matches('\\')
        .replace('\\', "/");
    format!("Z:{}", s.replace('/', "\\\\"))
}

/// Write `ModOrganizer.ini` + `portable.txt` so the output dir is a valid
/// portable MO2 instance. Idempotent — overwrites both files each run.
fn write_mo2_instance_files(config: &InstallConfig, game_domain: &str) -> Result<()> {
    let install = &config.output_dir;
    std::fs::create_dir_all(install.join("overwrite"))?;

    // Portable marker (file content irrelevant; presence is what matters).
    std::fs::write(install.join("portable.txt"), "Created by clf3\n")
        .with_context(|| format!("write portable.txt in {}", install.display()))?;

    // Map Nexus domain → MO2's human-readable gameName. Fall back to the
    // domain when we don't know the mapping (user can edit the INI).
    let game_name = GameType::from_nexus_domain(game_domain)
        .map(|g| g.name().to_string())
        .unwrap_or_else(|| game_domain.to_string());

    let base_dir = to_wine_ini_path(install);
    // MO2 expands `%BASE_DIR%` to base_directory at runtime. Use it whenever
    // downloads sits inside the instance; otherwise fall back to a Wine path.
    let downloads = match config.downloads_dir.strip_prefix(install) {
        Ok(rel) => format!(
            "%BASE_DIR%/{}",
            rel.to_string_lossy().replace('\\', "/")
        ),
        Err(_) => to_wine_ini_path(&config.downloads_dir),
    };
    let game_path = config
        .game_path
        .as_ref()
        .map(|p| to_wine_ini_path(p))
        .unwrap_or_default();

    // Layout matches a portable MO2 instance. base_directory + gamePath are
    // absolute Wine paths; the per-instance subdirs use %BASE_DIR% so MO2
    // resolves them relative to the (possibly relocated) base.
    let ini = format!(
        "[General]\n\
         gameName={game_name}\n\
         gameEdition=\n\
         gamePath={game_path}\n\
         selected_profile=@ByteArray(Default)\n\
         version=2.5.2\n\
         first_start=false\n\
         \n\
         [Settings]\n\
         base_directory={base_dir}\n\
         download_directory={downloads}\n\
         mod_directory=%BASE_DIR%/mods\n\
         cache_directory=%BASE_DIR%/webcache\n\
         profiles_directory=%BASE_DIR%/profiles\n\
         overwrite_directory=%BASE_DIR%/overwrite\n\
         style=\n"
    );
    std::fs::write(install.join("ModOrganizer.ini"), ini)
        .with_context(|| format!("write ModOrganizer.ini in {}", install.display()))?;
    info!("wrote ModOrganizer.ini + portable.txt at {}", install.display());
    Ok(())
}

/// Truncate a mod name for inline progress messages.
fn short_name(s: &str) -> String {
    const MAX: usize = 40;
    if s.chars().count() <= MAX {
        s.to_string()
    } else {
        let mut out: String = s.chars().take(MAX - 1).collect();
        out.push('…');
        out
    }
}

fn validate_config(config: &InstallConfig) -> Result<()> {
    if config.collection_path.as_os_str().is_empty() {
        bail!("InstallConfig.collection_path is empty");
    }
    if !config.collection_path.exists() {
        bail!("collection JSON not found: {}", config.collection_path.display());
    }
    if config.output_dir.as_os_str().is_empty() {
        bail!("InstallConfig.output_dir is empty");
    }
    if config.mods_dir.as_os_str().is_empty() {
        bail!("InstallConfig.mods_dir is empty");
    }
    if config.downloads_dir.as_os_str().is_empty() {
        bail!("InstallConfig.downloads_dir is empty");
    }
    if config.nexus_api_key.is_empty() {
        bail!("InstallConfig.nexus_api_key is empty");
    }
    Ok(())
}

// ============================================================================
// Tests — exercise extract_mod against a real ZIP archive carrying a real
// FOMOD layout, replaying choices. Validates the streaming-pipeline path end
// to end (without touching Nexus or the DB).
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn build_zip(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut buf = Vec::new();
        {
            let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
            let opts: zip::write::FileOptions<'_, ()> = zip::write::FileOptions::default()
                .compression_method(zip::CompressionMethod::Stored);
            for (name, data) in entries {
                zip.start_file(*name, opts).unwrap();
                zip.write_all(data).unwrap();
            }
            zip.finish().unwrap();
        }
        buf
    }

    fn make_mod_entry(folder: &str, choices_json: Option<&str>) -> ModDbEntry {
        ModDbEntry {
            id: 1,
            name: "TestMod".into(),
            folder_name: folder.into(),
            logical_filename: "TestMod-1.zip".into(),
            md5: "deadbeef".into(),
            file_size: 0,
            mod_id: 1,
            file_id: 1,
            source_type: "nexus".into(),
            source_url: String::new(),
            deploy_type: String::new(),
            phase: 0,
            status: "pending".into(),
            local_path: None,
            choices_json: choices_json.map(String::from),
            error_message: None,
            fomod_validated: false,
            fomod_valid: false,
            fomod_error: None,
            fomod_module_name: None,
            hashes_json: None,
            patches_json: None,
            optional: false,
        }
    }

    #[test]
    fn flatten_wrapper_dir_lifts_arbitrarily_named_subdir() {
        // Wrapper name doesn't match the mod, but contents look like Data.
        // (E.g. "HD Reworked Blended Roads" archive ships as `4k roads/{meshes,textures}/...`.)
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("Mod");
        let wrap = dest.join("4k roads");
        std::fs::create_dir_all(wrap.join("meshes/x")).unwrap();
        std::fs::create_dir_all(wrap.join("textures")).unwrap();

        flatten_wrapper_dir(&dest).unwrap();
        assert!(dest.join("meshes/x").exists());
        assert!(dest.join("textures").exists());
        assert!(!wrap.exists());
    }

    #[test]
    fn flatten_wrapper_dir_lifts_wrapper_with_only_esp() {
        // "Glorious Doors of Skyrim (GDOS) - Update 1.04/foo.esp" — wrapper
        // contains a single ESP at root, which is a Data-root signal.
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("Mod");
        let wrap = dest.join("Glorious Doors of Skyrim (GDOS) - Update 1.04");
        std::fs::create_dir_all(&wrap).unwrap();
        std::fs::write(wrap.join("GDOS.esp"), b"x").unwrap();

        flatten_wrapper_dir(&dest).unwrap();
        assert!(dest.join("GDOS.esp").exists());
        assert!(!wrap.exists());
    }

    #[test]
    fn flatten_wrapper_dir_lifts_wrapper_then_strips_data() {
        // "Bijin AIO for SE USSEP ver/data/Bijin AIO.esp" — wrapper
        // contains only `data/`, so peel via the one-level data-peek, then
        // strip data on the next pass.
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("Mod");
        std::fs::create_dir_all(dest.join("Bijin AIO for SE USSEP ver/data")).unwrap();
        std::fs::write(
            dest.join("Bijin AIO for SE USSEP ver/data/Bijin AIO.esp"),
            b"x",
        )
        .unwrap();

        flatten_wrapper_dir(&dest).unwrap();
        assert!(dest.join("Bijin AIO.esp").exists());
        assert!(!dest.join("Bijin AIO for SE USSEP ver").exists());
        assert!(!dest.join("data").exists());
    }

    #[test]
    fn flatten_wrapper_dir_preserves_framework_wrapper() {
        // NetScriptFramework/Plugins/foo.dll — `Plugins` isn't in
        // STOP_FOLDERS and `.dll` isn't a data-root signal, so the wrapper
        // has to stay (it IS the data-root entry).
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("Mod");
        let nsf = dest.join("NetScriptFramework").join("Plugins");
        std::fs::create_dir_all(&nsf).unwrap();
        std::fs::write(nsf.join("GrassControl.dll"), b"x").unwrap();

        flatten_wrapper_dir(&dest).unwrap();
        assert!(dest.join("NetScriptFramework/Plugins/GrassControl.dll").exists());
    }

    #[test]
    fn flatten_wrapper_dir_preserves_skse_wrapper() {
        // SKSE/Plugins/foo.dll — same shape as NetScriptFramework.
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("Mod");
        let skse = dest.join("SKSE").join("Plugins");
        std::fs::create_dir_all(&skse).unwrap();
        std::fs::write(skse.join("plugin.dll"), b"x").unwrap();

        flatten_wrapper_dir(&dest).unwrap();
        assert!(dest.join("SKSE/Plugins/plugin.dll").exists());
    }

    #[test]
    fn flatten_wrapper_dir_leaves_data_root_untouched() {
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("Mod");
        std::fs::create_dir_all(dest.join("meshes")).unwrap();
        std::fs::write(dest.join("plugin.esp"), b"x").unwrap();

        flatten_wrapper_dir(&dest).unwrap();
        assert!(dest.join("plugin.esp").exists());
        assert!(dest.join("meshes").exists());
    }

    #[test]
    fn flatten_wrapper_dir_strips_data_folder() {
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("Mod");
        std::fs::create_dir_all(dest.join("data/meshes/x")).unwrap();
        std::fs::write(dest.join("data/plugin.esp"), b"x").unwrap();

        flatten_wrapper_dir(&dest).unwrap();
        assert!(dest.join("meshes/x").exists());
        assert!(dest.join("plugin.esp").exists());
        assert!(!dest.join("data").exists());
    }

    #[test]
    fn flatten_wrapper_dir_handles_three_layer_wrapper_then_data() {
        // Lifelike Idle Animations style:
        //   <mod>/Outer/Middle/data/meshes/...
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("Mod");
        std::fs::create_dir_all(dest.join("Outer/Middle/data/meshes/x")).unwrap();

        flatten_wrapper_dir(&dest).unwrap();
        assert!(dest.join("meshes/x").exists());
        assert!(!dest.join("Outer").exists());
        assert!(!dest.join("data").exists());
    }

    #[test]
    fn flatten_wrapper_dir_handles_nested_wrapper_then_data() {
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("Mod");
        std::fs::create_dir_all(dest.join("WrapperX/Data/meshes")).unwrap();
        std::fs::write(dest.join("WrapperX/Data/plug.esp"), b"x").unwrap();

        flatten_wrapper_dir(&dest).unwrap();
        assert!(dest.join("meshes").exists());
        assert!(dest.join("plug.esp").exists());
        assert!(!dest.join("WrapperX").exists());
        assert!(!dest.join("data").exists());
    }

    #[test]
    fn flatten_wrapper_dir_skips_when_extra_files_at_root() {
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("Mod");
        let wrap = dest.join("Inner");
        std::fs::create_dir_all(wrap.join("meshes")).unwrap();
        std::fs::write(dest.join("readme.txt"), b"r").unwrap();

        flatten_wrapper_dir(&dest).unwrap();
        // Ambiguous (extra file at root) — leave it alone.
        assert!(dest.join("readme.txt").exists());
        assert!(wrap.join("meshes").exists());
    }

    #[test]
    fn extract_plain_mod_zip() {
        let archive_bytes = build_zip(&[
            ("plugin.esp", b"plugin payload"),
            ("meshes/x.nif", b"mesh payload"),
        ]);

        let tmp = tempfile::tempdir().unwrap();
        let archive_path = tmp.path().join("plain.zip");
        std::fs::write(&archive_path, &archive_bytes).unwrap();

        let mods_dir = tmp.path().join("mods");
        std::fs::create_dir_all(&mods_dir).unwrap();
        let collection_root = tmp.path().to_path_buf();

        let entry = make_mod_entry("PlainMod", None);
        let outcome =
            extract_mod(&entry, &archive_path, &mods_dir, &collection_root).unwrap();
        match outcome {
            ExtractOutcome::Done(s) => assert_eq!(s.applied, 0),
            ExtractOutcome::Skipped => panic!("first run shouldn't skip"),
        }
        let dest = mods_dir.join("PlainMod");
        assert!(dest.join("plugin.esp").exists());
        assert!(dest.join("meshes").join("x.nif").exists());
        assert!(dest.join(MARKER_FILE).exists(), "marker not written");

        // Second run with same inputs must skip (marker matches).
        let outcome2 =
            extract_mod(&entry, &archive_path, &mods_dir, &collection_root).unwrap();
        assert!(
            matches!(outcome2, ExtractOutcome::Skipped),
            "second run should skip via marker"
        );
    }

    #[test]
    fn extract_mod_strips_wrapper_dir() {
        // Archive's wrapper folder contains data-root contents. Wrapper
        // name is irrelevant — we peel based on the contents.
        let archive_bytes = build_zip(&[
            ("ArbitraryName/plugin.esp", b"plugin payload"),
            ("ArbitraryName/meshes/x.nif", b"mesh payload"),
        ]);

        let tmp = tempfile::tempdir().unwrap();
        let archive_path = tmp.path().join("wrap.zip");
        std::fs::write(&archive_path, &archive_bytes).unwrap();
        let mods_dir = tmp.path().join("mods");
        std::fs::create_dir_all(&mods_dir).unwrap();
        let collection_root = tmp.path().to_path_buf();

        let entry = make_mod_entry("WrappedMod", None);
        let _ = extract_mod(&entry, &archive_path, &mods_dir, &collection_root).unwrap();

        let dest = mods_dir.join("WrappedMod");
        assert!(
            dest.join("plugin.esp").exists(),
            "wrapper dir not flattened — plugin.esp still nested"
        );
        assert!(dest.join("meshes/x.nif").exists());
        assert!(!dest.join("ArbitraryName").exists());
    }

    #[test]
    fn extract_mod_preserves_framework_subdir() {
        // GrassControl ships under NetScriptFramework/Plugins/. The wrapper
        // name doesn't match the mod, so it must stay.
        let archive_bytes = build_zip(&[
            ("NetScriptFramework/Plugins/GrassControl.dll", b"dll"),
            ("NetScriptFramework/Plugins/GrassControl.config.txt", b"cfg"),
        ]);

        let tmp = tempfile::tempdir().unwrap();
        let archive_path = tmp.path().join("grass.zip");
        std::fs::write(&archive_path, &archive_bytes).unwrap();
        let mods_dir = tmp.path().join("mods");
        std::fs::create_dir_all(&mods_dir).unwrap();
        let collection_root = tmp.path().to_path_buf();

        let entry = make_mod_entry("Grass Control", None);
        let _ = extract_mod(&entry, &archive_path, &mods_dir, &collection_root).unwrap();

        let dest = mods_dir.join("Grass Control");
        assert!(dest.join("NetScriptFramework/Plugins/GrassControl.dll").exists());
        assert!(
            !dest.join("Plugins").exists(),
            "framework wrapper got flattened — Plugins shouldn't be at mod root"
        );
    }

    #[test]
    fn extract_fomod_mod_replays_choices() {
        // Build a FOMOD archive with required + chosen + unchosen + conditional
        // payloads. Layout mirrors a typical mod:
        //   fomod/ModuleConfig.xml
        //   common/script.psc        (required)
        //   optionA/textures.dds     (chosen)
        //   optionB/textures.dds     (NOT chosen)
        //   patch/Patch.esp          (conditional on flag from optionA)
        let module_config = br#"<?xml version="1.0" encoding="utf-8"?>
<config xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:noNamespaceSchemaLocation="http://qconsulting.ca/fo3/ModConfig5.0.xsd">
  <moduleName>Test FOMOD</moduleName>
  <requiredInstallFiles>
    <file source="common\script.psc" destination="scripts\script.psc" priority="0"/>
  </requiredInstallFiles>
  <installSteps>
    <installStep name="Choose">
      <optionalFileGroups>
        <group name="Textures" type="SelectExactlyOne">
          <plugins>
            <plugin name="Option A">
              <description>A</description>
              <files>
                <folder source="optionA" destination="textures" priority="0"/>
              </files>
              <conditionFlags>
                <flag name="VARIANT">A</flag>
              </conditionFlags>
              <typeDescriptor><type name="Optional"/></typeDescriptor>
            </plugin>
            <plugin name="Option B">
              <description>B</description>
              <files>
                <folder source="optionB" destination="textures" priority="0"/>
              </files>
              <typeDescriptor><type name="Optional"/></typeDescriptor>
            </plugin>
          </plugins>
        </group>
      </optionalFileGroups>
    </installStep>
  </installSteps>
  <conditionalFileInstalls>
    <patterns>
      <pattern>
        <dependencies operator="And">
          <flagDependency flag="VARIANT" value="A"/>
        </dependencies>
        <files>
          <file source="patch\Patch.esp" destination="Patch.esp" priority="0"/>
        </files>
      </pattern>
    </patterns>
  </conditionalFileInstalls>
</config>"#;

        let archive_bytes = build_zip(&[
            ("fomod/ModuleConfig.xml", module_config),
            ("common/script.psc", b"required script"),
            ("optionA/diffuse.dds", b"A diffuse"),
            ("optionB/diffuse.dds", b"B diffuse"),
            ("patch/Patch.esp", b"variant-A patch"),
        ]);

        let tmp = tempfile::tempdir().unwrap();
        let archive_path = tmp.path().join("fomod.zip");
        std::fs::write(&archive_path, &archive_bytes).unwrap();

        let mods_dir = tmp.path().join("mods");
        std::fs::create_dir_all(&mods_dir).unwrap();
        let collection_root = tmp.path().to_path_buf();

        let choices_json = r#"{"options":[{"name":"Choose","groups":[{"name":"Textures","choices":[{"name":"Option A","idx":0}]}]}]}"#;
        let entry = make_mod_entry("FomodMod", Some(choices_json));

        let outcome =
            extract_mod(&entry, &archive_path, &mods_dir, &collection_root).unwrap();
        match outcome {
            ExtractOutcome::Done(s) => assert_eq!(s.applied, 0),
            ExtractOutcome::Skipped => panic!("FOMOD first run shouldn't skip"),
        }

        let dest = mods_dir.join("FomodMod");
        // Required file went through Data/ stripping handled by executor.
        assert!(
            dest.join("scripts").join("script.psc").exists(),
            "required script missing"
        );
        // Chosen variant landed in textures/.
        assert!(
            dest.join("textures").join("diffuse.dds").exists(),
            "Option A textures missing"
        );
        // Unchosen variant did NOT install. (Both options write to textures/
        // diffuse.dds; we asserted optionA's content is present.)
        let bytes = std::fs::read(dest.join("textures").join("diffuse.dds")).unwrap();
        assert_eq!(bytes, b"A diffuse", "wrong variant installed");
        // Conditional install fired because VARIANT=A flag was set.
        assert!(
            dest.join("Patch.esp").exists(),
            "conditional patch missing"
        );
    }
}

