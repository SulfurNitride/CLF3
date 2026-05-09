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

use super::archive::{fetch_one, FetchOutcome, NxmContext};
use super::root_files;
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
    /// Browser command for the free-tier NXM flow. Defaults to `xdg-open`.
    pub browser: Option<String>,
    /// Run the post-install layout fixer interactively (multi-variant mods
    /// prompt for a variant pick on stdin). Auto-routers (FLM-style etc.)
    /// always run regardless. Defaults to `false`.
    pub interactive_fix: bool,
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
            browser: None,
            interactive_fix: false,
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

    // Free-tier: register the nxm:// handler + spin up a Unix-socket listener
    // so browser clicks can hand CDN URLs back to us. Premium path leaves
    // `nxm_ctx` as None and downloads run unattended via the API key.
    let nxm_ctx: Option<Arc<NxmContext>> = if !user.is_premium {
        info!("Free-tier Nexus account — install will require browser interaction.");
        let exe = std::env::current_exe()
            .ok()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_default();
        if !exe.is_empty() {
            if let Err(e) = crate::nxm_handler::register_handler(&exe) {
                warn!("nxm-handler register failed: {e:#} (browser clicks may not work)");
            }
        }
        let (rx, _sock_path) = crate::nxm_handler::start_listener().await?;
        let browser = config
            .browser
            .clone()
            .unwrap_or_else(|| "xdg-open".to_string());
        Some(Arc::new(NxmContext {
            rx: tokio::sync::Mutex::new(rx),
            browser,
        }))
    } else {
        None
    };
    // Free-tier inherently serializes (one tab at a time); concurrency > 1
    // would race the listener.
    let effective_concurrency = if nxm_ctx.is_some() {
        1
    } else {
        config.max_concurrent_downloads.max(1)
    };

    std::fs::create_dir_all(&config.mods_dir)?;
    std::fs::create_dir_all(&config.downloads_dir)?;

    let mods = db.get_all_mods()?;
    let total = mods.len();
    if total == 0 {
        warn!("collection has zero mods — nothing to install");
        return Ok(InstallStats::default());
    }

    // Build a multi-progress: one bar for downloads, one for extracts. Both
    // size `total`; they advance across all phases as events arrive.
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

    // DB writer thread runs for the whole install — single SQLite connection,
    // events from all phases flow through this channel.
    let (db_tx, db_rx) = mpsc::channel::<DbEvent>();
    let db_path_for_writer = db_path.clone();
    let db_writer = std::thread::spawn(move || -> Result<()> {
        let writer_db = CollectionDb::open(&db_path_for_writer)?;
        for event in db_rx {
            apply_db_event(&writer_db, event);
        }
        Ok(())
    });

    // Group mods by Vortex install phase. We must finish phase N (download
    // + extract + patches) before starting phase N+1, because later-phase
    // mods may depend on earlier-phase files being on disk (Vortex spec).
    let mut by_phase: std::collections::BTreeMap<i32, Vec<ModDbEntry>> =
        std::collections::BTreeMap::new();
    for m in &mods {
        by_phase.entry(m.phase).or_default().push(m.clone());
    }
    if by_phase.len() > 1 {
        info!(
            "Phased install: {} phases ({})",
            by_phase.len(),
            by_phase
                .iter()
                .map(|(p, v)| format!("{p}={}", v.len()))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    let stats_atomic = Arc::new(InstallStatsAtomic::default());

    for (phase, phase_mods) in &by_phase {
        if by_phase.len() > 1 {
            info!("Phase {}: {} mod(s)", phase, phase_mods.len());
        }

        let (extract_tx, extract_rx) = mpsc::sync_channel::<DownloadEvent>(32);

        let download_handle = {
            let http = http.clone();
            let nexus = nexus.clone();
            let downloads_dir = config.downloads_dir.clone();
            let game_domain = game_domain.clone();
            let max_concurrent = effective_concurrency;
            let mods = phase_mods.clone();
            let nxm_ctx_inner = nxm_ctx.clone();
            let collection_root_for_pool = config
                .collection_root
                .clone()
                .or_else(|| config.collection_path.parent().map(Path::to_path_buf))
                .unwrap_or_else(|| PathBuf::from("."));
            tokio::spawn(async move {
                run_download_pool(
                    mods,
                    http,
                    nexus,
                    downloads_dir,
                    game_domain,
                    max_concurrent,
                    extract_tx,
                    nxm_ctx_inner,
                    collection_root_for_pool,
                )
                .await
            })
        };

        run_extract_loop(
            extract_rx,
            db_tx.clone(),
            &config,
            phase_mods.len(),
            dl_bar.clone(),
            ex_bar.clone(),
            game_domain.clone(),
            stats_atomic.clone(),
        )?;

        download_handle
            .await
            .map_err(|e| anyhow::anyhow!("download task panicked: {e}"))??;
    }

    drop(db_tx);
    dl_bar.finish_with_message("done");
    ex_bar.finish_with_message("done");

    db_writer
        .join()
        .map_err(|_| anyhow::anyhow!("DB writer thread panicked"))??;

    let stats = stats_atomic.snapshot();

    // Post-install layout fix: rewrap FLM-style mods, prompt for any
    // multi-variant mods (when --interactive-fix). Runs BEFORE LOOT so the
    // newly-routed plugins get discovered.
    match super::post_install::fix_all(&config.mods_dir, config.interactive_fix) {
        Ok(results) => {
            let mut fixed = 0usize;
            let mut needs_user = 0usize;
            for (_name, outcome) in &results {
                match outcome {
                    super::post_install::FixOutcome::AutoFixed
                    | super::post_install::FixOutcome::UserPicked => fixed += 1,
                    super::post_install::FixOutcome::NeedsUserChoice => needs_user += 1,
                    _ => {}
                }
            }
            if fixed > 0 || needs_user > 0 {
                info!(
                    "post-install layout fix: {} auto-routed, {} need user pick",
                    fixed, needs_user
                );
            }
        }
        Err(e) => warn!("post-install layout fix failed: {:#}", e),
    }

    // Sort plugins via LOOT if we have a game path + supported game.
    let sorted_plugins = sort_plugins_or_fallback(&db, &config, &game_domain).await;

    // Post-extract: assemble modlist.txt + plugins.txt (+ loadorder.txt
    // for older games).
    write_modlist_outputs(&db, &config, &sorted_plugins, &game_domain)?;

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
    Bundled {
        mod_entry: ModDbEntry,
        bundle_dir: PathBuf,
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
    nxm_ctx: Option<Arc<NxmContext>>,
    collection_root: PathBuf,
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
        let nxm_ctx = nxm_ctx.clone();
        let collection_root_for_fetch = collection_root.clone();

        handles.push(tokio::spawn(async move {
            let _permit = permit; // released on drop
            // Pick a target filename. Many collection mods share the same
            // `logical_filename` ("Main File", "Main", etc.) so a plain
            // join would collide on disk — two parallel downloads racing
            // to overwrite the same path. Disambiguate Nexus mods with
            // `<base>-<modId>-<fileId><ext>`. Direct sources fall back to
            // the URL's last path segment.
            let base_name = if !mod_entry.logical_filename.is_empty() {
                mod_entry.logical_filename.clone()
            } else if !mod_entry.source_url.is_empty() {
                mod_entry
                    .source_url
                    .rsplit(['/', '\\'])
                    .find(|s| !s.is_empty())
                    .map(|s| s.split(['?', '#']).next().unwrap_or(s).to_string())
                    .unwrap_or_else(|| format!("mod-{}.bin", mod_entry.id))
            } else {
                format!("mod-{}.bin", mod_entry.id)
            };
            let target_name = if mod_entry.mod_id > 0 && mod_entry.file_id > 0 {
                let (stem, ext) = match base_name.rfind('.') {
                    Some(dot) => (&base_name[..dot], &base_name[dot..]),
                    None => (base_name.as_str(), ""),
                };
                let stamp = format!("-{}-{}", mod_entry.mod_id, mod_entry.file_id);
                if stem.contains(&stamp) {
                    base_name.clone()
                } else {
                    format!("{stem}{stamp}{ext}")
                }
            } else {
                base_name
            };
            let archive_path = downloads_dir.join(&target_name);
            let event =
                match fetch_one(
                    &mod_entry,
                    &archive_path,
                    &http,
                    &nexus,
                    &game_domain,
                    nxm_ctx.as_ref(),
                    &collection_root_for_fetch,
                )
                .await
                {
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
                    Ok(FetchOutcome::Bundled(dir)) => DownloadEvent::Bundled {
                        mod_entry,
                        bundle_dir: dir,
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
    _phase_total: usize,
    dl_bar: ProgressBar,
    ex_bar: ProgressBar,
    game_domain_for_meta: String,
    stats: Arc<InstallStatsAtomic>,
) -> Result<()> {
    use std::sync::atomic::Ordering;

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
                let game_domain_inner = game_domain_for_meta.clone();
                spawned += 1;
                rayon::spawn(move || {
                    let mod_name = mod_entry.name.clone();
                    ex_bar_inner.set_message(format!("→ {}", short_name(&mod_name)));
                    match extract_mod(&mod_entry, &archive_path, &mods_dir, &collection_root_arc, &game_domain_inner) {
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
            Ok(DownloadEvent::Bundled {
                mod_entry,
                bundle_dir,
            }) => {
                stats.cached.fetch_add(1, Ordering::Relaxed);
                dl_bar.inc(1);
                dl_bar.set_message(format!("bundled {}", short_name(&mod_entry.name)));
                let _ = db_tx.send(DbEvent::MarkDownloaded {
                    mod_id: mod_entry.id,
                    local_path: bundle_dir.display().to_string(),
                });

                let mods_dir = config.mods_dir.clone();
                let collection_root_arc = collection_root.clone();
                let db_tx_inner = db_tx.clone();
                let stats_inner = stats.clone();
                let done_tx_inner = done_tx.clone();
                let ex_bar_inner = ex_bar.clone();
                let game_domain_inner = game_domain_for_meta.clone();
                spawned += 1;
                rayon::spawn(move || {
                    let mod_name = mod_entry.name.clone();
                    ex_bar_inner.set_message(format!("→ {}", short_name(&mod_name)));
                    match extract_bundled_mod(
                        &mod_entry,
                        &bundle_dir,
                        &mods_dir,
                        &collection_root_arc,
                        &game_domain_inner,
                    ) {
                        Ok(patch_summary) => {
                            stats_inner.extracted.fetch_add(1, Ordering::Relaxed);
                            ex_bar_inner.inc(1);
                            ex_bar_inner.set_message(format!("✓ {}", short_name(&mod_name)));
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
                            let _ = db_tx_inner.send(DbEvent::MarkInstalled {
                                mod_id: mod_entry.id,
                            });
                        }
                        Err(e) => {
                            stats_inner.failed.fetch_add(1, Ordering::Relaxed);
                            ex_bar_inner.inc(1);
                            ex_bar_inner.set_message(format!("✗ {}", short_name(&mod_name)));
                            let msg = format!("{e:#}");
                            error!("Bundle copy failed for {}: {}", mod_entry.name, msg);
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
                let detail = if url.is_empty() {
                    notes
                } else {
                    format!("{url} — {notes}")
                };
                let _ = db_tx.send(DbEvent::MarkManual {
                    mod_id: mod_entry.id,
                    notes: detail,
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

    Ok(())
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
    MarkManual { mod_id: i64, notes: String },
}

fn apply_db_event(db: &CollectionDb, event: DbEvent) {
    let result = match event {
        DbEvent::MarkDownloaded { mod_id, local_path } => db.mark_mod_downloaded(mod_id, &local_path),
        DbEvent::MarkInstalled { mod_id } => db.update_mod_status(mod_id, ModStatus::Installed),
        DbEvent::MarkFailed { mod_id, error } => db.mark_mod_failed(mod_id, &error),
        DbEvent::MarkManual { mod_id, notes } => db.mark_mod_manual(mod_id, &notes),
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

    // Build an md5 → source-path index of every extracted file. Stream the
    // hash from disk so a 3 GB texture pack doesn't slurp 3 GB into RAM.
    let mut by_md5: HashMap<String, std::path::PathBuf> = HashMap::new();
    let mut hashed_files = 0usize;
    for entry in WalkDir::new(temp.path())
        .into_iter()
        .filter_map(|e| e.ok())
    {
        if !entry.file_type().is_file() {
            continue;
        }
        let digest = match super::verify::compute_md5(entry.path()) {
            Ok(d) => d,
            Err(e) => {
                warn!("hash-install: hash {} failed: {e:#}", entry.path().display());
                continue;
            }
        };
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

/// Install a Vortex `bundle`-source mod whose payload was unpacked from
/// the collection ZIP into `bundle_dir`. Mirrors `extract_mod`'s post-extract
/// steps (wrapper-flatten, patches) but the "extract" itself is a recursive
/// directory copy.
fn extract_bundled_mod(
    mod_entry: &ModDbEntry,
    bundle_dir: &Path,
    mods_dir: &Path,
    collection_root: &Path,
    game_domain: &str,
) -> Result<PatchSummary> {
    let mod_dest = mods_dir.join(&mod_entry.folder_name);
    std::fs::create_dir_all(&mod_dest)?;
    // Always copy bundle contents to mod root; per-file `route_root_files`
    // splits root binaries off after.
    let payload_dest: std::path::PathBuf = mod_dest.clone();
    std::fs::create_dir_all(&payload_dest)?;

    copy_dir_recursive(bundle_dir, &payload_dest)
        .with_context(|| format!("copy bundle: {}", bundle_dir.display()))?;

    if let Err(e) = flatten_wrapper_dir(&payload_dest) {
        warn!("wrapper-flatten skipped for {}: {:#}", mod_entry.name, e);
    }

    if let Err(e) = route_root_files(&payload_dest, &mod_dest, game_domain) {
        warn!("route-root failed for {}: {:#}", mod_entry.name, e);
    }

    if let Some(patches) = mod_entry.get_patches() {
        apply_patches_for_mod(&mod_entry.name, &payload_dest, collection_root, &patches)
            .with_context(|| format!("apply patches for {}", mod_entry.name))
    } else {
        Ok(PatchSummary::default())
    }
}

/// Move top-level files/folders matching the per-game root allowlist out
/// of `payload_dest` and into `mod_dest/Root/` so Fluorine's VFS Root
/// Builder deploys them to the game install root.
///
/// No-op when `is_root_mod()` already routed the entire mod to `Root/`
/// (those mods skip the Data layer wholesale). For everything else, this
/// catches SKSE/ENB/DLL hooks Vortex doesn't tag with `deploy_type`.
fn route_root_files(
    payload_dest: &Path,
    mod_dest: &Path,
    game_domain: &str,
) -> Result<usize> {
    let Some(rules) = root_files::rules_for(game_domain) else {
        return Ok(0);
    };
    if !payload_dest.is_dir() {
        return Ok(0);
    }

    // Collect first; can't iterate read_dir while mutating the tree.
    let mut to_move: Vec<(std::path::PathBuf, bool)> = Vec::new();
    for entry in std::fs::read_dir(payload_dest)?.flatten() {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        // Don't relocate our own marker file.
        if name_str == MARKER_FILE || name_str == LEGACY_MARKER_FILE {
            continue;
        }
        let ft = match entry.file_type() {
            Ok(ft) => ft,
            Err(_) => continue,
        };
        if ft.is_dir() {
            if rules.matches_folder(&name_str) {
                to_move.push((entry.path(), true));
            }
        } else if rules.matches_file(&name_str) {
            to_move.push((entry.path(), false));
        }
    }

    if to_move.is_empty() {
        return Ok(0);
    }

    let root_dir = mod_dest.join("Root");
    std::fs::create_dir_all(&root_dir)
        .with_context(|| format!("create Root/ at {}", root_dir.display()))?;

    let mut moved = 0usize;
    for (src, _is_dir) in to_move {
        let Some(name) = src.file_name() else { continue };
        let dst = root_dir.join(name);
        if dst.exists() {
            // Pre-existing Root/ entry from a prior install; rename would
            // fail. Leave the existing entry alone.
            warn!(
                "route-root: {} already at {} — skipping",
                src.display(),
                dst.display()
            );
            continue;
        }
        std::fs::rename(&src, &dst).with_context(|| {
            format!("route-root: {} -> {}", src.display(), dst.display())
        })?;
        moved += 1;
    }

    if moved > 0 {
        info!(
            "route-root: moved {} item(s) into {}/Root/ for {}",
            moved,
            mod_dest.display(),
            game_domain
        );
    }
    Ok(moved)
}

/// Recursively copy `src` into `dst` (both directories). Skips entries that
/// already exist at the destination so a partially-installed bundle resumes
/// cleanly.
fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let from = entry.path();
        let to = dst.join(entry.file_name());
        let ft = entry.file_type()?;
        if ft.is_dir() {
            copy_dir_recursive(&from, &to)?;
        } else if !to.exists() {
            std::fs::copy(&from, &to)
                .with_context(|| format!("copy {} -> {}", from.display(), to.display()))?;
        }
    }
    Ok(())
}

fn extract_mod(
    mod_entry: &ModDbEntry,
    archive_path: &Path,
    mods_dir: &Path,
    collection_root: &Path,
    game_domain: &str,
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

    // Always extract to the mod root. Per-file routing into `Root/` happens
    // *after* extraction via `route_root_files`, which only moves entries
    // matching the per-game allowlist (skse64_*.dll/exe, d3d*.dll, ENB
    // files, enbseries/, etc.).
    //
    // The earlier `is_root_mod()` whole-mod redirect was wrong: SKSE ships
    // both root binaries AND a Data-side `Scripts/` folder of Papyrus
    // bytecode (UI.pex etc. that SkyUI's MCM checks). Routing the whole
    // mod to `Root/` deployed `Scripts/UI.pex` to `<game>/Scripts/UI.pex`
    // instead of `<game>/Data/Scripts/UI.pex` → SkyUI Error 7.
    let payload_dest: std::path::PathBuf = mod_dest.clone();
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

    // Route SKSE/ENB/DLL-hook files into `<mod>/Root/` so Fluorine's VFS
    // Root Builder deploys them to the game install dir. Per-file
    // allowlist — leaves `Scripts/`, ESPs, BSAs, etc. at mod root for
    // Data-side deployment.
    if let Err(e) = route_root_files(&payload_dest, &mod_dest, game_domain) {
        warn!("route-root failed for {}: {:#}", mod_entry.name, e);
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
    game_domain: &str,
) -> Result<()> {
    use std::collections::HashSet;

    let profile_dir = config.output_dir.join("profiles").join("Default");
    std::fs::create_dir_all(&profile_dir)?;

    // Map plugin name (lowercase) → enabled flag from the collection JSON.
    // MO2 writes `*Foo.esp` for enabled and `Foo.esp` (no prefix) for
    // disabled in `plugins.txt`.
    let plugin_enabled: std::collections::HashMap<String, bool> = db
        .get_plugins()
        .map(|rows| {
            rows.into_iter()
                .map(|p| (p.name.to_lowercase(), p.enabled))
                .collect()
        })
        .unwrap_or_default();

    // Pull mod metadata + rules from DB.
    //
    // Vortex `recommends` rules mark a mod with `optional=true` in the
    // collection JSON. That flag is a *pre-install* hint ("user can opt
    // out") — Vortex itself enables the mod once it's installed. We used
    // to map optional → `-` prefix in modlist.txt, which left SKSE and
    // Engine Fixes disabled (they're commonly recommends-flagged because
    // the user might already have them) and broke the runtime: no SKSE
    // VFS → no DLL hooks → SkyUI MCM and FSMP physics fail. Match Vortex
    // and the legacy NexusBridge tool: always enable installed mods.
    let all_mods = db.get_all_mods()?;
    let optional_folders: HashSet<String> = HashSet::new();

    // Filter `sorted_plugins` against what's actually deployable. Two
    // exclusions, both producing "Plugin not found" in Fluorine if left in:
    //   1. Plugin name has no .esp/.esm/.esl on disk at all.
    //   2. Plugin's only on-disk copy lives in a mod we marked disabled
    //      (Vortex `recommends` → optional → `-` prefix in modlist.txt).
    //      Disabled mods aren't in MO2/Fluorine's VFS, so their plugins
    //      can't load — listing them in plugins.txt only spams warnings.
    let installed_with_owner = crate::loot::discover_plugins_with_owner(&config.mods_dir)
        .unwrap_or_default();
    // A plugin can ship in multiple mods; we keep it as long as at least
    // ONE of those mods is enabled in modlist.txt.
    let mut deployable_plugins: HashSet<String> = HashSet::new();
    for (name, owner) in &installed_with_owner {
        if !optional_folders.contains(owner) {
            deployable_plugins.insert(name.to_lowercase());
        }
    }
    let filtered_plugins: Vec<String> = sorted_plugins
        .iter()
        .filter(|p| deployable_plugins.contains(&p.to_lowercase()))
        .cloned()
        .collect();
    let dropped = sorted_plugins.len().saturating_sub(filtered_plugins.len());
    if dropped > 0 {
        info!(
            "plugins.txt: dropped {} plugin name(s) (missing or in disabled mods)",
            dropped
        );
    }

    let mods = all_mods
        .into_iter()
        .map(|m| {
            let has_file_overrides = m.has_file_overrides();
            ModInfo {
                name: m.name,
                logical_filename: m.logical_filename,
                folder_name: m.folder_name,
                md5: m.md5,
                phase: m.phase,
                has_file_overrides,
            }
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
        &filtered_plugins,
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
    write_plugins_txt(&plugins_txt, &filtered_plugins, &plugin_enabled)?;
    info!("wrote plugins.txt ({} plugins)", filtered_plugins.len());

    // Older Bethesda games (Skyrim LE, FO3, FNV) need a separate
    // `loadorder.txt` for MO2 to honor LOOT-resolved ordering. Newer titles
    // (SSE, FO4, Starfield) use `plugins.txt` alone with the `*` prefix.
    if needs_loadorder_txt(game_domain) {
        let loadorder_txt = profile_dir.join("loadorder.txt");
        write_loadorder_txt(&loadorder_txt, &filtered_plugins)?;
        info!("wrote loadorder.txt ({} plugins)", filtered_plugins.len());
    }

    Ok(())
}

/// Older Gamebryo titles (Skyrim LE, Fallout 3, New Vegas, Oblivion) keep
/// enable state in `plugins.txt` and sort order in a separate
/// `loadorder.txt`. Skyrim SE / FO4 / Starfield collapsed both into
/// `plugins.txt` (with `*` prefix marking enabled). Game support in
/// `GameType` is currently SSE-only, so we match against the Nexus domain
/// directly.
fn needs_loadorder_txt(game_domain: &str) -> bool {
    matches!(
        game_domain.to_lowercase().as_str(),
        "skyrim" | "oblivion" | "fallout3" | "newvegas" | "falloutnewvegas"
    )
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

    // Pre-LOOT input order matters: libloot's tie-breaking (when no
    // masterlist rule applies between two plugins) preserves the input
    // sequence. Feeding alphabetical disagrees with Fluorine's output by
    // ~95% positionally; feeding the curator's `plugins[]` order from the
    // collection JSON brings it to ~90%+.
    let initial_order = collection_order();
    match run_loot_sort(game_type, game_path, &config.mods_dir, &initial_order).await {
        Ok(sorted) if !sorted.is_empty() => sorted,
        Ok(_) => {
            warn!("LOOT returned empty plugin list — using collection-declared order");
            initial_order
        }
        Err(e) => {
            warn!("LOOT sort failed: {e:#} — using collection-declared order");
            initial_order
        }
    }
}

async fn run_loot_sort(
    game_type: GameType,
    game_path: &Path,
    mods_dir: &Path,
    initial_order: &[String],
) -> Result<Vec<String>> {
    use std::collections::HashSet;

    let mut sorter = PluginSorter::new(game_type, game_path, mods_dir)
        .context("PluginSorter::new failed")?;
    sorter
        .ensure_masterlist(false)
        .await
        .context("LOOT masterlist load/download failed")?;
    let installed = loot::discover_plugins(mods_dir).context("plugin discovery failed")?;
    if installed.is_empty() {
        return Ok(Vec::new());
    }

    // Order plugins for libloot input: take the curator's order from the
    // collection JSON first (case-insensitively intersected with what's on
    // disk), then append any disk-discovered plugin the JSON missed. This
    // is what libloot's tie-breaker uses when no masterlist rule applies
    // between two plugins.
    let installed_set: HashSet<String> = installed.iter().map(|n| n.to_lowercase()).collect();
    let mut already: HashSet<String> = HashSet::new();
    let mut ordered: Vec<String> = Vec::with_capacity(installed.len());
    for name in initial_order {
        let key = name.to_lowercase();
        if installed_set.contains(&key) && already.insert(key) {
            ordered.push(name.clone());
        }
    }
    for name in &installed {
        let key = name.to_lowercase();
        if already.insert(key) {
            ordered.push(name.clone());
        }
    }

    sorter.sort_all(&ordered)
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

fn write_plugins_txt(
    path: &Path,
    plugins_in_order: &[String],
    enabled_lookup: &std::collections::HashMap<String, bool>,
) -> Result<()> {
    use std::io::Write;
    let mut f = std::fs::File::create(path)
        .with_context(|| format!("create plugins.txt: {}", path.display()))?;
    for plugin in plugins_in_order {
        // Default to enabled when the collection JSON didn't ship a row
        // (LOOT may discover plugins from masters not listed there).
        let enabled = enabled_lookup
            .get(&plugin.to_lowercase())
            .copied()
            .unwrap_or(true);
        if enabled {
            writeln!(f, "*{plugin}")?;
        } else {
            writeln!(f, "{plugin}")?;
        }
    }
    Ok(())
}

/// `loadorder.txt` for older Gamebryo titles: one plugin name per line, in
/// load-order sequence. Enable state lives in `plugins.txt`.
fn write_loadorder_txt(path: &Path, plugins_in_order: &[String]) -> Result<()> {
    use std::io::Write;
    let mut f = std::fs::File::create(path)
        .with_context(|| format!("create loadorder.txt: {}", path.display()))?;
    for plugin in plugins_in_order {
        writeln!(f, "{plugin}")?;
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

/// Map a Nexus game domain to its xSE loader filename. Used to detect when
/// a collection shipped a script extender we can wire as an executable.
fn xse_loader_for(game_domain: &str) -> Option<&'static str> {
    match game_domain.to_lowercase().as_str() {
        "skyrimspecialedition" => Some("skse64_loader.exe"),
        "skyrim" => Some("skse_loader.exe"),
        "fallout4" => Some("f4se_loader.exe"),
        "newvegas" | "falloutnewvegas" => Some("nvse_loader.exe"),
        "fallout3" => Some("fose_loader.exe"),
        "oblivion" => Some("obse_loader.exe"),
        "starfield" => Some("sfse_loader.exe"),
        _ => None,
    }
}

/// xSE display title for the executable entry.
fn xse_title_for(game_domain: &str) -> &'static str {
    match game_domain.to_lowercase().as_str() {
        "skyrimspecialedition" | "skyrim" => "SKSE",
        "fallout4" => "F4SE",
        "newvegas" | "falloutnewvegas" => "NVSE",
        "fallout3" => "FOSE",
        "oblivion" => "OBSE",
        "starfield" => "SFSE",
        _ => "Script Extender",
    }
}

/// True when any installed mod ships `loader_name` under its `Root/`
/// subdir — i.e. clf3 routed an xSE loader into game-root staging during
/// extract. After Fluorine deploys Root/, the file lives at
/// `<game_path>/<loader_name>`.
fn xse_loader_present(mods_dir: &Path, loader_name: &str) -> bool {
    let want = loader_name.to_lowercase();
    let Ok(read) = std::fs::read_dir(mods_dir) else {
        return false;
    };
    for mod_entry in read.flatten() {
        let root = mod_entry.path().join("Root");
        if !root.is_dir() {
            continue;
        }
        let Ok(rread) = std::fs::read_dir(&root) else {
            continue;
        };
        for f in rread.flatten() {
            let n = f.file_name();
            if n.to_string_lossy().to_lowercase() == want {
                return true;
            }
        }
    }
    false
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

    // Auto-add an xSE loader entry to `[customExecutables]` when:
    //   1. game_path is known (so we can resolve the deployed binary path)
    //   2. the game has a known xSE loader name
    //   3. some mod actually shipped that loader under Root/ (clf3 detected
    //      via root_files routing during extract)
    // After Fluorine deploys Root/ → game install dir, the loader lives at
    // `<game_path>/<loader>.exe`, so that's the binary path we point at.
    let custom_executables = if let (Some(game_path_buf), Some(loader)) =
        (config.game_path.as_ref(), xse_loader_for(game_domain))
    {
        if xse_loader_present(&config.mods_dir, loader) {
            let loader_path = to_wine_ini_path(&game_path_buf.join(loader));
            let game_path_wine = to_wine_ini_path(game_path_buf);
            let title = xse_title_for(game_domain);
            format!(
                "\n[customExecutables]\nsize=1\n\
                 1\\title={title}\n\
                 1\\binary={loader_path}\n\
                 1\\arguments=\n\
                 1\\workingDirectory={game_path_wine}\n\
                 1\\steamAppID=\n\
                 1\\toolbar=true\n\
                 1\\ownicon=true\n\
                 1\\hide=false\n"
            )
        } else {
            String::new()
        }
    } else {
        String::new()
    };

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
         style=\n\
         {custom_executables}"
    );
    std::fs::write(install.join("ModOrganizer.ini"), ini)
        .with_context(|| format!("write ModOrganizer.ini in {}", install.display()))?;
    if !custom_executables.is_empty() {
        info!(
            "wrote ModOrganizer.ini at {} (with {} executable)",
            install.display(),
            xse_title_for(game_domain),
        );
    } else {
        info!("wrote ModOrganizer.ini + portable.txt at {}", install.display());
    }
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
            file_overrides_json: None,
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
    fn extract_routes_skse_loader_to_root() {
        // Mod ships SKSE binaries at archive root + an esp + meshes.
        // Loader/DLL must move to <mod>/Root/, esp + meshes stay put.
        let archive_bytes = build_zip(&[
            ("skse64_loader.exe", b"loader"),
            ("skse64_1_6_1170.dll", b"runtime"),
            ("plugin.esp", b"esp"),
            ("meshes/x.nif", b"mesh"),
        ]);

        let tmp = tempfile::tempdir().unwrap();
        let archive_path = tmp.path().join("skse.zip");
        std::fs::write(&archive_path, &archive_bytes).unwrap();
        let mods_dir = tmp.path().join("mods");
        std::fs::create_dir_all(&mods_dir).unwrap();
        let collection_root = tmp.path().to_path_buf();

        let entry = make_mod_entry("SKSE", None);
        extract_mod(
            &entry,
            &archive_path,
            &mods_dir,
            &collection_root,
            "skyrimspecialedition",
        )
        .unwrap();

        let dest = mods_dir.join("SKSE");
        assert!(dest.join("Root/skse64_loader.exe").exists(), "loader not routed");
        assert!(dest.join("Root/skse64_1_6_1170.dll").exists(), "runtime DLL not routed");
        assert!(dest.join("plugin.esp").exists(), "esp moved away from Data");
        assert!(dest.join("meshes/x.nif").exists(), "mesh moved away from Data");
        // Source files should be gone from payload root after the rename.
        assert!(!dest.join("skse64_loader.exe").exists());
    }

    #[test]
    fn extract_routes_enbseries_folder_to_root() {
        let archive_bytes = build_zip(&[
            ("enbseries/effect.fx", b"shader"),
            ("enbseries/sky.fx", b"shader"),
            ("enbseries.ini", b"cfg"),
            ("d3d11.dll", b"hook"),
            ("textures/x.dds", b"tex"),
        ]);

        let tmp = tempfile::tempdir().unwrap();
        let archive_path = tmp.path().join("enb.zip");
        std::fs::write(&archive_path, &archive_bytes).unwrap();
        let mods_dir = tmp.path().join("mods");
        std::fs::create_dir_all(&mods_dir).unwrap();
        let collection_root = tmp.path().to_path_buf();

        let entry = make_mod_entry("ENB", None);
        extract_mod(
            &entry,
            &archive_path,
            &mods_dir,
            &collection_root,
            "skyrimspecialedition",
        )
        .unwrap();

        let dest = mods_dir.join("ENB");
        assert!(dest.join("Root/enbseries/effect.fx").exists());
        assert!(dest.join("Root/enbseries/sky.fx").exists());
        assert!(dest.join("Root/enbseries.ini").exists());
        assert!(dest.join("Root/d3d11.dll").exists());
        assert!(dest.join("textures/x.dds").exists(), "non-root texture moved");
        assert!(!dest.join("enbseries").exists(), "enbseries left at payload root");
    }

    #[test]
    fn extract_skips_root_routing_for_unknown_game() {
        let archive_bytes = build_zip(&[("skse64_loader.exe", b"loader")]);
        let tmp = tempfile::tempdir().unwrap();
        let archive_path = tmp.path().join("u.zip");
        std::fs::write(&archive_path, &archive_bytes).unwrap();
        let mods_dir = tmp.path().join("mods");
        std::fs::create_dir_all(&mods_dir).unwrap();
        let collection_root = tmp.path().to_path_buf();

        let entry = make_mod_entry("Mod", None);
        extract_mod(&entry, &archive_path, &mods_dir, &collection_root, "morrowind").unwrap();

        let dest = mods_dir.join("Mod");
        // No rules registered for morrowind → file stays at payload root.
        assert!(dest.join("skse64_loader.exe").exists());
        assert!(!dest.join("Root").exists(), "Root/ created for unsupported game");
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
            extract_mod(&entry, &archive_path, &mods_dir, &collection_root, "skyrimspecialedition").unwrap();
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
            extract_mod(&entry, &archive_path, &mods_dir, &collection_root, "skyrimspecialedition").unwrap();
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
        let _ = extract_mod(&entry, &archive_path, &mods_dir, &collection_root, "skyrimspecialedition").unwrap();

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
        let _ = extract_mod(&entry, &archive_path, &mods_dir, &collection_root, "skyrimspecialedition").unwrap();

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
            extract_mod(&entry, &archive_path, &mods_dir, &collection_root, "skyrimspecialedition").unwrap();
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

