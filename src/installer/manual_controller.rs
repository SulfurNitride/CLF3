//! Local browser controller for non-premium/manual downloads.
//!
//! The browser is only used to authenticate and start downloads. CLF3 owns the
//! completion signal by watching a user-selected folder, hashing finished files,
//! and moving matching archives into the normal downloads cache.

use super::config::InstallConfig;
use super::downloader::{ArchiveEvent, DownloadStats};
use super::progress::{ProgressHandle, ProgressReporter};
use crate::downloaders::NexusDownloader;
use crate::hash::compute_file_hash;
use crate::modlist::{ArchiveInfo, DownloadState, ModlistDb};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::mpsc::SyncSender;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, warn};
use uuid::Uuid;

const SCAN_INTERVAL: Duration = Duration::from_secs(1);
const STABLE_FOR: Duration = Duration::from_secs(2);
const MISSING_DOWNLOAD_FOR: Duration = Duration::from_secs(3);
const TEMP_SUFFIXES: &[&str] = &[".crdownload", ".part", ".tmp", ".download", ".opdownload"];

#[derive(Debug, Clone)]
struct ManualArchive {
    archive: ArchiveInfo,
    url: String,
    prompt: Option<String>,
    token: String,
}

#[derive(Debug, Clone, Serialize)]
struct ControllerItem {
    token: String,
    name: String,
    hash: String,
    url: String,
    prompt: Option<String>,
    expected_size: u64,
    status: String,
    message: String,
}

#[derive(Debug, Clone, Serialize)]
struct ControllerView {
    watch_dir: String,
    cache_dir: String,
    max_active: usize,
    paused: bool,
    scan_existing: bool,
    message: String,
    completed: usize,
    active: usize,
    opened: usize,
    next_token: Option<String>,
    next_url: Option<String>,
    total: usize,
    items: Vec<ControllerItem>,
}

#[derive(Debug)]
struct ControllerState {
    watch_dir: PathBuf,
    cache_dir: PathBuf,
    max_active: usize,
    paused: bool,
    queue_running: bool,
    scan_existing: bool,
    message: String,
    open_probe: Option<OpenProbe>,
    items: Vec<ControllerItem>,
}

impl ControllerState {
    fn view(&self) -> ControllerView {
        let completed = self.completed_count();
        let active = self.active_download_count();
        let opened = self.opened_count();
        let next = self.next_open_item();
        ControllerView {
            watch_dir: self.watch_dir.display().to_string(),
            cache_dir: self.cache_dir.display().to_string(),
            max_active: self.max_active,
            paused: self.paused,
            scan_existing: self.scan_existing,
            message: self.message.clone(),
            completed,
            active,
            opened,
            next_token: next.map(|item| item.token.clone()),
            next_url: next.map(|item| item.url.clone()),
            total: self.items.len(),
            items: self.items.clone(),
        }
    }

    fn completed_count(&self) -> usize {
        self.items
            .iter()
            .filter(|item| item.status == "complete")
            .count()
    }

    fn active_download_count(&self) -> usize {
        self.items
            .iter()
            .filter(|item| matches!(item.status.as_str(), "downloading" | "hashing"))
            .count()
    }

    fn opened_count(&self) -> usize {
        self.items
            .iter()
            .filter(|item| item.status == "opened")
            .count()
    }

    fn next_open_item(&self) -> Option<&ControllerItem> {
        if self.paused
            || !self.queue_running
            || self.opened_count() > 0
            || self.active_download_count() >= self.max_active
        {
            return None;
        }
        self.items.iter().find(|item| item.status == "queued")
    }

    fn set_item_status(&mut self, token: &str, status: &str, message: impl Into<String>) {
        if let Some(item) = self.items.iter_mut().find(|item| item.token == token) {
            item.status = status.to_string();
            item.message = message.into();
        }
    }

    fn mark_opened_as_downloading(&mut self) {
        if let Some(item) = self.items.iter_mut().find(|item| item.status == "opened") {
            item.status = "downloading".to_string();
            item.message = "Download detected".to_string();
        }
        self.open_probe = None;
    }
}

#[derive(Debug)]
enum ControllerAction {
    StartQueue,
    Opened(String),
    Closed(String),
    Pause,
    Resume,
    ScanExisting,
    SetWatchDir(PathBuf),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileStamp {
    size: u64,
    modified_ns: u128,
}

#[derive(Debug)]
struct SeenFile {
    stamp: FileStamp,
    first_stable_at: Instant,
    checked: bool,
}

#[derive(Debug, Clone)]
struct OpenProbe {
    expected_name: String,
    opened_at: SystemTime,
    baseline: HashMap<PathBuf, FileStamp>,
}

pub fn default_watch_dir() -> PathBuf {
    dirs::download_dir()
        .or_else(|| dirs::home_dir().map(|home| home.join("Downloads")))
        .unwrap_or_else(|| PathBuf::from("."))
}

/// Run the manual browser controller until every provided archive is accepted
/// or the process is interrupted.
pub async fn download_manual_browser_archives(
    db: &ModlistDb,
    config: &InstallConfig,
    pending: Vec<ArchiveInfo>,
    tx: Option<&SyncSender<ArchiveEvent>>,
) -> Result<DownloadStats> {
    let reporter = &config.reporter;
    let archives = build_manual_archives(pending)?;
    if archives.is_empty() {
        return Ok(DownloadStats::default());
    }

    let watch_dir = config
        .manual_watch_dir
        .clone()
        .unwrap_or_else(default_watch_dir);
    fs::create_dir_all(&watch_dir)
        .with_context(|| format!("Failed to create watch dir: {}", watch_dir.display()))?;
    fs::create_dir_all(&config.downloads_dir).with_context(|| {
        format!(
            "Failed to create archive cache dir: {}",
            config.downloads_dir.display()
        )
    })?;

    let items = archives
        .iter()
        .map(|archive| ControllerItem {
            token: archive.token.clone(),
            name: archive.archive.name.clone(),
            hash: archive.archive.hash.clone(),
            url: archive.url.clone(),
            prompt: archive.prompt.clone(),
            expected_size: archive.archive.size.max(0) as u64,
            status: "queued".to_string(),
            message: "Waiting to open".to_string(),
        })
        .collect();

    let state = Arc::new(Mutex::new(ControllerState {
        watch_dir: watch_dir.clone(),
        cache_dir: config.downloads_dir.clone(),
        max_active: config.manual_max_active.max(1),
        paused: false,
        queue_running: false,
        scan_existing: false,
        message: "Click Start queue, then start each manual/slow download in the browser."
            .to_string(),
        open_probe: None,
        items,
    }));

    let (action_tx, mut action_rx) = mpsc::unbounded_channel();
    let (server_url, server_task) = start_controller_server(Arc::clone(&state), action_tx).await?;

    reporter.log("\n=== Manual Browser Mode ===");
    reporter.log(&format!("Controller: {}", server_url));
    reporter.log(&format!("Watching:   {}", watch_dir.display()));
    reporter.log(&format!("Cache:      {}", config.downloads_dir.display()));
    reporter.log(&format!("Items:      {}", archives.len()));
    reporter.log(
        "Start the queue in the controller, download files normally, and CLF3 will hash-match them.\n",
    );

    if let Err(e) = std::process::Command::new(&config.browser)
        .arg(&server_url)
        .spawn()
    {
        warn!(
            "Failed to open manual controller with '{}': {}",
            config.browser, e
        );
        reporter.log(&format!(
            "Could not open browser automatically. Open this URL manually: {}",
            server_url
        ));
    }

    let started_at = SystemTime::now();
    let mut seen = HashMap::<PathBuf, SeenFile>::new();
    let mut missing_downloads = HashMap::<String, Instant>::new();
    let mut completed = HashSet::<String>::new();
    let mut progress_handles = HashMap::<String, Arc<dyn ProgressHandle>>::new();
    let manual_status = reporter.begin_status("Manual");
    reporter.overall_set_total(archives.len() as u64);
    let mut stats = DownloadStats::default();

    loop {
        while let Ok(action) = action_rx.try_recv() {
            apply_action(&state, action).await?;
        }

        scan_once(
            db,
            config,
            &archives,
            tx,
            &state,
            &mut seen,
            &mut missing_downloads,
            &mut completed,
            &mut stats,
            started_at,
        )
        .await?;

        sync_progress(
            &state,
            reporter.as_ref(),
            &manual_status,
            &mut progress_handles,
        )
        .await;

        if completed.len() == archives.len() {
            break;
        }

        tokio::time::sleep(SCAN_INTERVAL).await;
    }

    {
        let mut state = state.lock().await;
        state.message = "All manual downloads matched. Continuing install.".to_string();
    }

    server_task.abort();
    manual_status.finish();
    for (_, handle) in progress_handles.drain() {
        handle.finish();
    }
    reporter.overall_finish();
    reporter.log("Manual browser downloads complete.");
    Ok(stats)
}

fn build_manual_archives(pending: Vec<ArchiveInfo>) -> Result<Vec<ManualArchive>> {
    let mut archives = Vec::new();
    for archive in pending {
        let state: DownloadState = serde_json::from_str(&archive.state_json)
            .with_context(|| format!("Failed to parse download state for {}", archive.name))?;
        let (url, prompt) = match state {
            DownloadState::Nexus(nexus_state) => {
                let domain = NexusDownloader::game_domain(&nexus_state.game_name);
                (
                    nexus_manual_file_url(domain, nexus_state.mod_id, nexus_state.file_id),
                    None,
                )
            }
            DownloadState::Manual(manual_state) => (manual_state.url, Some(manual_state.prompt)),
            _ => continue,
        };
        archives.push(ManualArchive {
            archive,
            url,
            prompt,
            token: Uuid::new_v4().to_string(),
        });
    }
    Ok(archives)
}

fn nexus_manual_file_url(game_domain: &str, mod_id: u64, file_id: u64) -> String {
    format!(
        "https://www.nexusmods.com/{}/mods/{}?tab=files&file_id={}",
        game_domain.to_lowercase(),
        mod_id,
        file_id
    )
}

async fn apply_action(state: &Arc<Mutex<ControllerState>>, action: ControllerAction) -> Result<()> {
    let mut state = state.lock().await;
    match action {
        ControllerAction::StartQueue => {
            state.queue_running = true;
            state.paused = false;
            state.message =
                "Queue started. Open the manual/slow download from the browser window.".to_string();
        }
        ControllerAction::Opened(token) => {
            state.open_probe = state
                .items
                .iter()
                .find(|item| item.token == token)
                .map(|item| OpenProbe {
                    expected_name: item.name.clone(),
                    opened_at: SystemTime::now(),
                    baseline: snapshot_expected_downloads(&state.watch_dir, &item.name),
                });
            state.set_item_status(&token, "opened", "Browser page opened");
        }
        ControllerAction::Closed(token) => {
            if let Some(item) = state
                .items
                .iter_mut()
                .find(|item| item.token == token && item.status == "opened")
            {
                item.status = "queued".to_string();
                item.message = "Browser window closed; queued again".to_string();
            }
        }
        ControllerAction::Pause => {
            state.paused = true;
            state.message = "Opening paused. Existing downloads are still watched.".to_string();
        }
        ControllerAction::Resume => {
            state.paused = false;
            state.queue_running = true;
            state.message = "Opening resumed.".to_string();
        }
        ControllerAction::ScanExisting => {
            state.scan_existing = true;
            state.message = "Scanning existing files in the watched folder.".to_string();
        }
        ControllerAction::SetWatchDir(path) => {
            fs::create_dir_all(&path)
                .with_context(|| format!("Failed to create watch dir: {}", path.display()))?;
            state.watch_dir = path;
            state.open_probe = None;
            state.scan_existing = true;
            state.message = "Watch folder changed; scanning existing files.".to_string();
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn scan_once(
    db: &ModlistDb,
    config: &InstallConfig,
    archives: &[ManualArchive],
    tx: Option<&SyncSender<ArchiveEvent>>,
    state: &Arc<Mutex<ControllerState>>,
    seen: &mut HashMap<PathBuf, SeenFile>,
    missing_downloads: &mut HashMap<String, Instant>,
    completed: &mut HashSet<String>,
    stats: &mut DownloadStats,
    started_at: SystemTime,
) -> Result<()> {
    let (watch_dir, scan_existing, open_probe, downloading_items) = {
        let state = state.lock().await;
        (
            state.watch_dir.clone(),
            state.scan_existing,
            state.open_probe.clone(),
            state
                .items
                .iter()
                .filter(|item| item.status == "downloading")
                .map(|item| (item.token.clone(), item.name.clone()))
                .collect::<Vec<_>>(),
        )
    };

    let mut expected_sizes = HashSet::new();
    let mut by_hash = HashMap::<String, &ManualArchive>::new();
    for archive in archives {
        if !completed.contains(&archive.archive.hash) {
            expected_sizes.insert(archive.archive.size.max(0) as u64);
            by_hash.insert(archive.archive.hash.clone(), archive);
        }
    }

    let entries = match fs::read_dir(&watch_dir) {
        Ok(entries) => entries,
        Err(e) => {
            state.lock().await.message = format!(
                "Could not read watched folder {}: {}",
                watch_dir.display(),
                e
            );
            return Ok(());
        }
    };

    let mut opened_download_detected = false;
    let mut present_downloads = HashSet::<String>::new();

    for entry in entries.filter_map(|entry| entry.ok()) {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        let meta = match fs::metadata(&path) {
            Ok(meta) => meta,
            Err(_) => continue,
        };
        let modified = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        let stamp = FileStamp {
            size: meta.len(),
            modified_ns: modified_ns(modified),
        };

        if open_probe_detected_download(&open_probe, &path, stamp, modified) {
            opened_download_detected = true;
        }
        for (token, name) in &downloading_items {
            if matches_expected_download_candidate(&path, name) {
                present_downloads.insert(token.clone());
                break;
            }
        }

        if is_temp_download_file(&path) {
            continue;
        }

        if !scan_existing {
            if modified < started_at {
                continue;
            }
        }

        if !expected_sizes.contains(&stamp.size) {
            continue;
        }

        let now = Instant::now();
        let seen_file = seen.entry(path.clone()).or_insert(SeenFile {
            stamp,
            first_stable_at: now,
            checked: false,
        });

        if seen_file.stamp != stamp {
            seen_file.stamp = stamp;
            seen_file.first_stable_at = now;
            seen_file.checked = false;
            continue;
        }
        if seen_file.checked || seen_file.first_stable_at.elapsed() < STABLE_FOR {
            continue;
        }

        let display_name = path
            .file_name()
            .map(|name| name.to_string_lossy().to_string())
            .unwrap_or_else(|| path.display().to_string());
        {
            let mut state = state.lock().await;
            state.message = format!("Hashing {}", display_name);
        }

        let hash_path = path.clone();
        let actual_hash = tokio::task::spawn_blocking(move || compute_file_hash(&hash_path))
            .await
            .context("Hash task failed")??;
        seen_file.checked = true;

        let Some(manual_archive) = by_hash.get(&actual_hash).copied() else {
            debug!(
                "Ignoring browser download {} with non-matching hash {}",
                path.display(),
                actual_hash
            );
            continue;
        };

        accept_downloaded_file(db, config, manual_archive, &path)?;
        completed.insert(manual_archive.archive.hash.clone());
        stats.downloaded += 1;

        {
            let mut state = state.lock().await;
            state.set_item_status(
                &manual_archive.token,
                "complete",
                format!("Matched {}", display_name),
            );
            state.message = format!(
                "Matched {} ({}/{})",
                manual_archive.archive.name,
                completed.len(),
                archives.len()
            );
        }

        if let Some(tx) = tx {
            let output_path = config.downloads_dir.join(&manual_archive.archive.name);
            let _ = tx.send(ArchiveEvent::Ready {
                hash: manual_archive.archive.hash.clone(),
                name: manual_archive.archive.name.clone(),
                path: output_path,
            });
        }
    }

    if opened_download_detected {
        state.lock().await.mark_opened_as_downloading();
    }
    requeue_missing_downloads(
        state,
        missing_downloads,
        &downloading_items,
        &present_downloads,
    )
    .await;

    Ok(())
}

async fn requeue_missing_downloads(
    state: &Arc<Mutex<ControllerState>>,
    missing_downloads: &mut HashMap<String, Instant>,
    downloading_items: &[(String, String)],
    present_downloads: &HashSet<String>,
) {
    let now = Instant::now();
    let active_tokens = downloading_items
        .iter()
        .map(|(token, _)| token.clone())
        .collect::<HashSet<_>>();
    missing_downloads.retain(|token, _| active_tokens.contains(token));

    let mut missing_ready = Vec::new();
    for (token, _) in downloading_items {
        if present_downloads.contains(token) {
            missing_downloads.remove(token);
            continue;
        }
        let first_missing = missing_downloads.entry(token.clone()).or_insert(now);
        if first_missing.elapsed() >= MISSING_DOWNLOAD_FOR {
            missing_ready.push(token.clone());
        }
    }

    if missing_ready.is_empty() {
        return;
    }

    let mut state = state.lock().await;
    for token in missing_ready {
        state.set_item_status(&token, "queued", "Download disappeared; queued again");
        missing_downloads.remove(&token);
    }
    state.message = "A browser download disappeared; queued it again.".to_string();
}

fn open_probe_detected_download(
    probe: &Option<OpenProbe>,
    path: &Path,
    stamp: FileStamp,
    modified: SystemTime,
) -> bool {
    let Some(probe) = probe else {
        return false;
    };
    if !matches_expected_download_candidate(path, &probe.expected_name) {
        return false;
    }
    if modified < probe.opened_at {
        return false;
    }
    probe.baseline.get(path).copied() != Some(stamp)
}

fn snapshot_expected_downloads(
    watch_dir: &Path,
    expected_name: &str,
) -> HashMap<PathBuf, FileStamp> {
    let Ok(entries) = fs::read_dir(watch_dir) else {
        return HashMap::new();
    };
    entries
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.is_file() && matches_expected_download_candidate(path, expected_name))
        .filter_map(|path| {
            let meta = fs::metadata(&path).ok()?;
            let modified = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
            Some((
                path,
                FileStamp {
                    size: meta.len(),
                    modified_ns: modified_ns(modified),
                },
            ))
        })
        .collect()
}

fn matches_expected_download_candidate(path: &Path, expected_name: &str) -> bool {
    let Some(actual_name) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    let expected_name = Path::new(expected_name)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(expected_name);

    let actual_base = strip_temp_suffix(actual_name);
    actual_base == expected_name || matches_duplicate_download_name(actual_base, expected_name)
}

fn strip_temp_suffix(name: &str) -> &str {
    for suffix in TEMP_SUFFIXES {
        if let Some(stripped) = name.strip_suffix(suffix) {
            return stripped;
        }
    }
    name
}

fn matches_duplicate_download_name(actual_name: &str, expected_name: &str) -> bool {
    let expected = Path::new(expected_name);
    let Some(stem) = expected.file_stem().and_then(|stem| stem.to_str()) else {
        return false;
    };
    let Some(ext) = expected.extension().and_then(|ext| ext.to_str()) else {
        return false;
    };
    let suffix = format!(".{}", ext);
    let Some(actual_stem) = actual_name.strip_suffix(&suffix) else {
        return false;
    };
    let Some(rest) = actual_stem.strip_prefix(stem) else {
        return false;
    };
    rest.starts_with(" (")
        && rest.ends_with(')')
        && rest[2..rest.len() - 1]
            .chars()
            .all(|ch| ch.is_ascii_digit())
}

async fn sync_progress(
    state: &Arc<Mutex<ControllerState>>,
    reporter: &dyn ProgressReporter,
    manual_status: &Arc<dyn ProgressHandle>,
    handles: &mut HashMap<String, Arc<dyn ProgressHandle>>,
) {
    let view = state.lock().await.view();
    manual_status.set_count(view.completed, view.total);
    manual_status.set_message(&format!(
        "manual {}/{} active {}/{}",
        view.completed, view.total, view.active, view.max_active
    ));
    reporter.overall_set_message(&format!(
        "Manual browser: {}/{} complete, active {}/{}, page {}",
        view.completed,
        view.total,
        view.active,
        view.max_active,
        if view.opened > 0 { "open" } else { "idle" }
    ));

    let active_tokens: HashSet<String> = view
        .items
        .iter()
        .filter(|item| matches!(item.status.as_str(), "opened" | "downloading" | "hashing"))
        .map(|item| item.token.clone())
        .collect();

    for item in view
        .items
        .iter()
        .filter(|item| active_tokens.contains(&item.token))
    {
        let handle = handles
            .entry(item.token.clone())
            .or_insert_with(|| reporter.begin_item(&item.name, Some(item.expected_size)));
        handle.set_message(&format!("manual {}: {}", item.status, item.name));
    }

    let stale: Vec<String> = handles
        .keys()
        .filter(|token| !active_tokens.contains(*token))
        .cloned()
        .collect();
    for token in stale {
        if let Some(handle) = handles.remove(&token) {
            handle.finish();
        }
    }
}

fn accept_downloaded_file(
    db: &ModlistDb,
    config: &InstallConfig,
    manual_archive: &ManualArchive,
    source_path: &Path,
) -> Result<()> {
    let output_path = config.downloads_dir.join(&manual_archive.archive.name);
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }

    if source_path != output_path {
        if output_path.exists() {
            fs::remove_file(&output_path)
                .with_context(|| format!("Failed to replace existing {}", output_path.display()))?;
        }
        move_file(source_path, &output_path).with_context(|| {
            format!(
                "Failed to move {} to {}",
                source_path.display(),
                output_path.display()
            )
        })?;
    }

    super::sidecar::write_archive_hash(&output_path, &manual_archive.archive.hash)?;
    db.mark_archive_downloaded(
        &manual_archive.archive.hash,
        output_path.to_string_lossy().as_ref(),
    )?;
    Ok(())
}

fn move_file(source: &Path, dest: &Path) -> io::Result<()> {
    match fs::rename(source, dest) {
        Ok(()) => Ok(()),
        Err(rename_err) => {
            fs::copy(source, dest)?;
            fs::remove_file(source).map_err(|remove_err| {
                io::Error::new(
                    remove_err.kind(),
                    format!(
                        "rename failed: {}; copied but failed to remove source: {}",
                        rename_err, remove_err
                    ),
                )
            })
        }
    }
}

fn is_temp_download_file(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    TEMP_SUFFIXES.iter().any(|suffix| name.ends_with(suffix))
}

fn modified_ns(time: SystemTime) -> u128 {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default()
}

async fn start_controller_server(
    state: Arc<Mutex<ControllerState>>,
    action_tx: mpsc::UnboundedSender<ControllerAction>,
) -> Result<(String, tokio::task::JoinHandle<()>)> {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .context("Failed to bind manual controller server")?;
    let addr = listener.local_addr()?;
    let url = format!("http://{}", addr);

    let task = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let state = Arc::clone(&state);
            let action_tx = action_tx.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, state, action_tx).await {
                    debug!("manual controller request failed: {}", e);
                }
            });
        }
    });

    Ok((url, task))
}

async fn handle_connection(
    mut stream: TcpStream,
    state: Arc<Mutex<ControllerState>>,
    action_tx: mpsc::UnboundedSender<ControllerAction>,
) -> Result<()> {
    let request = read_http_request(&mut stream).await?;
    let response = route_request(&request, state, action_tx).await;
    stream.write_all(&response).await?;
    stream.shutdown().await?;
    Ok(())
}

#[derive(Debug)]
struct HttpRequest {
    method: String,
    path: String,
    body: Vec<u8>,
}

async fn read_http_request(stream: &mut TcpStream) -> Result<HttpRequest> {
    let mut buf = Vec::new();
    let header_end;
    loop {
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            anyhow::bail!("connection closed before headers");
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(pos) = find_header_end(&buf) {
            header_end = pos;
            break;
        }
        if buf.len() > 64 * 1024 {
            anyhow::bail!("request headers too large");
        }
    }

    let header_text = String::from_utf8_lossy(&buf[..header_end]);
    let mut lines = header_text.lines();
    let request_line = lines.next().context("missing request line")?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let path = parts.next().unwrap_or("/").to_string();
    let mut content_length = 0usize;
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                content_length = value.trim().parse().unwrap_or(0);
            }
        }
    }

    let body_start = header_end + 4;
    while buf.len() < body_start + content_length {
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    let body = buf
        .get(body_start..body_start + content_length)
        .unwrap_or_default()
        .to_vec();

    Ok(HttpRequest { method, path, body })
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|window| window == b"\r\n\r\n")
}

async fn route_request(
    request: &HttpRequest,
    state: Arc<Mutex<ControllerState>>,
    action_tx: mpsc::UnboundedSender<ControllerAction>,
) -> Vec<u8> {
    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/") => response(200, "text/html; charset=utf-8", CONTROLLER_HTML),
        ("GET", "/state") => match serde_json::to_string(&state.lock().await.view()) {
            Ok(json) => response(200, "application/json", &json),
            Err(e) => response(500, "text/plain", &format!("state error: {}", e)),
        },
        ("POST", "/start") => {
            let _ = action_tx.send(ControllerAction::StartQueue);
            response(204, "text/plain", "")
        }
        ("POST", "/pause") => {
            let _ = action_tx.send(ControllerAction::Pause);
            response(204, "text/plain", "")
        }
        ("POST", "/resume") => {
            let _ = action_tx.send(ControllerAction::Resume);
            response(204, "text/plain", "")
        }
        ("POST", "/scan-existing") => {
            let _ = action_tx.send(ControllerAction::ScanExisting);
            response(204, "text/plain", "")
        }
        ("POST", "/watch-dir") => match serde_json::from_slice::<WatchDirRequest>(&request.body) {
            Ok(body) => {
                let _ = action_tx.send(ControllerAction::SetWatchDir(PathBuf::from(body.path)));
                response(204, "text/plain", "")
            }
            Err(e) => response(400, "text/plain", &format!("bad watch-dir body: {}", e)),
        },
        _ if request.method == "POST" && request.path.starts_with("/opened/") => {
            let token = request.path.trim_start_matches("/opened/").to_string();
            let _ = action_tx.send(ControllerAction::Opened(token));
            response(204, "text/plain", "")
        }
        _ if request.method == "POST" && request.path.starts_with("/closed/") => {
            let token = request.path.trim_start_matches("/closed/").to_string();
            let _ = action_tx.send(ControllerAction::Closed(token));
            response(204, "text/plain", "")
        }
        _ => response(404, "text/plain", "not found"),
    }
}

#[derive(Debug, Deserialize)]
struct WatchDirRequest {
    path: String,
}

fn response(status: u16, content_type: &str, body: &str) -> Vec<u8> {
    let reason = match status {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "OK",
    };
    let bytes = body.as_bytes();
    let header = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
        status,
        reason,
        content_type,
        bytes.len()
    );
    let mut response = header.into_bytes();
    response.extend_from_slice(bytes);
    response
}

const CONTROLLER_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>CLF3 Manual Downloads</title>
<style>
:root { color-scheme: dark; font-family: system-ui, sans-serif; background: #111318; color: #e6e8ee; }
body { margin: 0; }
header { position: sticky; top: 0; background: #191d25; border-bottom: 1px solid #303642; padding: 14px 18px; z-index: 1; }
h1 { font-size: 18px; margin: 0 0 10px; font-weight: 650; }
.bar { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
button { background: #2f7dd1; color: white; border: 0; border-radius: 6px; padding: 8px 11px; font-weight: 650; cursor: pointer; }
button.secondary { background: #303642; }
button.warning { background: #8b5a16; }
button:disabled { opacity: .5; cursor: default; }
input { background: #0d0f14; color: #e6e8ee; border: 1px solid #303642; border-radius: 6px; padding: 8px; min-width: min(520px, 90vw); }
main { padding: 16px 18px 28px; }
.meta { color: #b6bfce; font-size: 13px; display: grid; gap: 4px; margin-top: 10px; }
.message { margin-top: 10px; color: #d9e7ff; }
.grid { display: grid; gap: 8px; margin-top: 14px; }
.item { display: grid; grid-template-columns: minmax(0, 1fr) auto; gap: 10px; align-items: center; padding: 10px 12px; border: 1px solid #303642; border-radius: 8px; background: #191d25; }
.name { overflow-wrap: anywhere; font-weight: 620; }
.sub { color: #b6bfce; font-size: 12px; margin-top: 4px; overflow-wrap: anywhere; }
.pill { border-radius: 999px; padding: 4px 8px; font-size: 12px; font-weight: 700; background: #303642; color: #d7dce6; white-space: nowrap; }
.complete { background: #166534; }
.hashing { background: #6b4f0d; }
.opened { background: #1d4f8f; }
.downloading { background: #0f766e; }
.closed { background: #70302f; }
</style>
</head>
<body>
<header>
  <h1>CLF3 Manual Downloads</h1>
  <div class="bar">
    <button id="startQueue">Start queue</button>
    <button id="scan" class="secondary">Scan existing</button>
  </div>
  <div class="bar" style="margin-top:8px">
    <input id="watchDir" spellcheck="false">
    <button id="setWatch" class="secondary">Set watch folder</button>
  </div>
  <div class="meta">
    <div id="counts"></div>
    <div id="cache"></div>
    <div class="message" id="message"></div>
  </div>
</header>
<main><div class="grid" id="items"></div></main>
<script>
let downloadWindow = null;
let currentToken = null;
let current = null;

async function post(path, body) {
  await fetch(path, { method: 'POST', headers: body ? {'Content-Type': 'application/json'} : {}, body: body ? JSON.stringify(body) : undefined });
}

function statusClass(status) {
  return ['complete', 'hashing', 'opened', 'downloading', 'closed'].includes(status) ? status : '';
}

function render(state) {
  current = state;
  document.getElementById('watchDir').value = state.watch_dir;
  document.getElementById('startQueue').textContent = primaryButtonText(state);
  document.getElementById('counts').textContent = `${state.completed}/${state.total} complete - active downloads ${state.active}/${state.max_active}${state.opened ? ' - page open' : ''}${state.paused ? ' - paused' : ''}`;
  document.getElementById('cache').textContent = `Cache: ${state.cache_dir}`;
  document.getElementById('message').textContent = state.message;
  driveQueue(state);
  const items = document.getElementById('items');
  items.textContent = '';
  for (const item of state.items) {
    const row = document.createElement('div');
    row.className = 'item';
    const text = document.createElement('div');
    const name = document.createElement('div');
    name.className = 'name';
    name.textContent = item.name;
    const sub = document.createElement('div');
    sub.className = 'sub';
    sub.textContent = `${item.message} - ${item.expected_size} bytes`;
    text.append(name, sub);
    const pill = document.createElement('span');
    pill.className = 'pill ' + statusClass(item.status);
    pill.textContent = item.status;
    row.append(text, pill);
    items.append(row);
  }
}

function primaryButtonText(state) {
  if (state.paused) return 'Resume queue';
  if ((!downloadWindow || downloadWindow.closed) && state.next_token) return 'Open download window';
  return 'Start queue';
}

function driveQueue(state) {
  if (downloadWindow && downloadWindow.closed) {
    if (currentToken) post('/closed/' + currentToken);
    downloadWindow = null;
    currentToken = null;
  }
  if (state.completed === state.total && downloadWindow && !downloadWindow.closed) {
    downloadWindow.close();
    downloadWindow = null;
    currentToken = null;
    return;
  }
  if (!state.next_token || !state.next_url || state.paused) return;
  if (!downloadWindow || downloadWindow.closed) return;
  if (currentToken === state.next_token) return;
  currentToken = state.next_token;
  downloadWindow.location.href = state.next_url;
  post('/opened/' + state.next_token);
}

async function refresh() {
  const response = await fetch('/state', { cache: 'no-store' });
  render(await response.json());
}

async function startQueue() {
  await post('/start');
  await refresh();
  await openDownloadWindowFromState();
}

async function openDownloadWindowFromState() {
  if (!current || !current.next_token || !current.next_url) return;
  downloadWindow = window.open(current.next_url, 'clf3_manual_download');
  if (downloadWindow) {
    currentToken = current.next_token;
    await post('/opened/' + current.next_token);
    setTimeout(refresh, 250);
  }
}

document.getElementById('startQueue').onclick = () => startQueue();
document.getElementById('scan').onclick = () => post('/scan-existing').then(refresh);
document.getElementById('setWatch').onclick = () => post('/watch-dir', { path: document.getElementById('watchDir').value }).then(refresh);

refresh();
setInterval(refresh, 1000);
</script>
</body>
</html>
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temp_download_suffixes_are_ignored() {
        assert!(is_temp_download_file(Path::new("file.zip.crdownload")));
        assert!(is_temp_download_file(Path::new("file.7z.part")));
        assert!(!is_temp_download_file(Path::new("file.7z")));
    }

    #[test]
    fn expected_download_candidates_match_archive_and_browser_variants() {
        let expected = "Some Mod-123-1-0.zip";
        assert!(matches_expected_download_candidate(
            Path::new("Some Mod-123-1-0.zip"),
            expected
        ));
        assert!(matches_expected_download_candidate(
            Path::new("Some Mod-123-1-0.zip.crdownload"),
            expected
        ));
        assert!(matches_expected_download_candidate(
            Path::new("Some Mod-123-1-0 (1).zip"),
            expected
        ));
        assert!(matches_expected_download_candidate(
            Path::new("Some Mod-123-1-0 (2).zip.part"),
            expected
        ));
        assert!(!matches_expected_download_candidate(
            Path::new("Other Mod.zip.crdownload"),
            expected
        ));
    }

    #[test]
    fn default_watch_dir_has_a_fallback() {
        assert!(!default_watch_dir().as_os_str().is_empty());
    }
}
