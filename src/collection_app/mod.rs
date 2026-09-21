//! Standalone application services; account access lives here, outside the engine.
pub mod acquire;
pub mod catalog;
use crate::collection::{
    package::digest_file,
    progress::{safe_message, WorkerEvent},
    publish::PublicationReport,
    url::parse_collection_url,
    worker::{read_json, WorkerRequest},
    CollectionPackage, CollectionPlan, PlanOptions,
};
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc::SyncSender,
        Mutex,
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Inputs {
    pub source: String,
    pub game: PathBuf,
    pub cache: PathBuf,
    pub output: PathBuf,
    pub profile_ini: PathBuf,
    pub masterlist: PathBuf,
    pub selected_optional: BTreeSet<String>,
    pub all_optional: bool,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Prepared {
    #[serde(default)]
    pub author: String,
    pub inputs: Inputs,
    pub job: PathBuf,
    pub package: PathBuf,
    pub plan: CollectionPlan,
    #[serde(default)]
    pub manual_archives: BTreeMap<String, PathBuf>,
}

#[derive(Clone)]
pub struct MissingArtifact {
    pub id: String,
    pub name: String,
    pub reason: String,
    pub page: Option<String>,
}

pub enum Event {
    Prepared(Box<Prepared>),
    Worker(WorkerEvent),
    Downloads {
        verified: usize,
        total: usize,
        bytes: u64,
        total_bytes: u64,
        item: String,
    },
    NeedsFiles(Vec<MissingArtifact>),
    Finished(Box<PublicationReport>),
    Failed(String),
    Cancelled,
}

pub fn cache_root() -> PathBuf {
    dirs::cache_dir()
        .unwrap_or_else(std::env::temp_dir)
        .join("clf3/collections")
}
pub fn recent_path() -> PathBuf {
    cache_root().join("last-gui-job.json")
}

pub fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path.parent().context("Missing job directory")?;
    std::fs::create_dir_all(parent)?;
    let mut file = tempfile::NamedTempFile::new_in(parent)?;
    serde_json::to_writer(&mut file, value)?;
    file.flush()?;
    file.as_file().sync_all()?;
    file.persist(path).map_err(|e| e.error)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

impl Prepared {
    pub fn save(&self) -> Result<()> {
        atomic_json(&self.job.join("prepared.json"), self)?;
        atomic_json(&recent_path(), &self.job)
    }
    pub fn restore(job: &Path) -> Result<Self> {
        let mut prepared: Self = read_json(&job.join("prepared.json"), 32 * 1024 * 1024)?;
        if prepared.job != job || !job.is_absolute() {
            bail!("Saved job path does not match");
        }
        if prepared.author.is_empty() {
            if let Ok(package) = CollectionPackage::open(&prepared.package) {
                if package.digest == prepared.plan.package_sha256 {
                    prepared.author = package
                        .raw
                        .get("info")
                        .and_then(|i| i.get("author"))
                        .and_then(|v| v.as_str())
                        .map(safe_message)
                        .unwrap_or_default();
                }
            }
        }
        Ok(prepared)
    }
    pub fn options(&self) -> PlanOptions {
        PlanOptions {
            schema_id: self.plan.schema_id,
            locator: self.plan.locator.clone(),
            game_version: self.plan.game_version.clone(),
            selected_optional: self.inputs.selected_optional.clone(),
            all_optional: self.inputs.all_optional,
        }
    }
}

pub fn prepare(
    mut inputs: Inputs,
    previous: Option<&Prepared>,
    key: &str,
    token: &CancellationToken,
) -> Result<Prepared> {
    acquire::check(token)?;
    if !inputs.game.as_os_str().is_empty() {
        inputs.game = inputs
            .game
            .canonicalize()
            .context("Choose an existing source game directory")?;
    }
    if !inputs.cache.is_absolute() {
        bail!("Choose an absolute download cache directory");
    }
    let same_source = previous.filter(|p| p.inputs.source == inputs.source);
    let (package, locator, schema, job) = if let Some(previous) = same_source {
        // Replanning options keeps the resolved revision/package, even if Nexus
        // has published a newer revision since the first review.
        (
            previous.package.clone(),
            previous.plan.locator.clone(),
            previous.plan.schema_id,
            cache_root()
                .join("gui-jobs")
                .join(uuid::Uuid::new_v4().to_string()),
        )
    } else {
        let job = cache_root()
            .join("gui-jobs")
            .join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&job)?;
        if inputs.source.trim().starts_with("https:") {
            let locator = parse_collection_url(inputs.source.trim())?;
            crate::collection::games::require(&locator.domain)?;
            let (package, pinned, schema) =
                acquire::Nexus::new(key)?.package(&locator, &job, token)?;
            inputs.source = format!(
                "https://www.nexusmods.com/games/{}/collections/{}/revisions/{}",
                pinned.domain,
                pinned.slug,
                pinned.revision.unwrap()
            );
            (package, Some(pinned), Some(schema), job)
        } else {
            (
                PathBuf::from(inputs.source.trim())
                    .canonicalize()
                    .context("Open a full collection package or paste its Nexus URL")?,
                None,
                None,
                job,
            )
        }
    };
    acquire::check(token)?;
    let opened = CollectionPackage::open(&package)?;
    let game_profile = crate::collection::games::require(opened.collection.domain())?;
    let version = if inputs.game.as_os_str().is_empty() {
        None
    } else {
        let executable =
            crate::collection::paths::resolve_file(&inputs.game, game_profile.executable)
                .with_context(|| {
                    format!(
                        "Choose the {} source directory containing {}",
                        game_profile.name, game_profile.executable
                    )
                })?;
        crate::collection::publish::executable_version(&executable)
            .context("Read the source game runtime")?
    };
    if let Some(previous) = same_source {
        if opened.digest != previous.plan.package_sha256 {
            bail!("The reviewed package changed; load it as a new source");
        }
    }
    let plan = CollectionPlan::build(
        &opened,
        &PlanOptions {
            schema_id: schema,
            locator,
            game_version: version,
            selected_optional: inputs.selected_optional.clone(),
            all_optional: inputs.all_optional,
        },
    )?;
    if inputs.output.as_os_str().is_empty() {
        let name: String = plan
            .name
            .chars()
            .filter(|c| c.is_alphanumeric() || matches!(c, ' ' | '-' | '_'))
            .take(80)
            .collect();
        inputs.output = dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("/tmp"))
            .join("Games")
            .join(if name.is_empty() { "Collection" } else { &name });
    }
    let result = Prepared {
        author: opened
            .raw
            .get("info")
            .and_then(|i| i.get("author"))
            .and_then(|v| v.as_str())
            .map(safe_message)
            .unwrap_or_default(),
        inputs,
        job,
        package,
        plan,
        manual_archives: same_source
            .map(|p| p.manual_archives.clone())
            .unwrap_or_default(),
    };
    result.save()?;
    Ok(result)
}

pub fn credential_variable(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    [
        "nexus",
        "loverslab",
        "token",
        "secret",
        "password",
        "credential",
        "api_key",
        "apikey",
        "authorization",
        "cookie",
    ]
    .iter()
    .any(|part| name.contains(part))
}

fn canonical_target(path: &Path) -> Result<PathBuf> {
    if !path.is_absolute()
        || path
            .components()
            .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        bail!("Choose an absolute destination without parent traversal");
    }
    let mut ancestor = path;
    while !ancestor.exists() {
        ancestor = ancestor.parent().context("No destination parent")?;
    }
    Ok(ancestor.canonicalize()?.join(path.strip_prefix(ancestor)?))
}

pub fn preflight(prepared: &Prepared) -> Result<()> {
    if !prepared.plan.blockers.is_empty() {
        bail!("Resolve the compatibility problems before installing");
    }
    let input = &prepared.inputs;
    let game_profile = crate::collection::games::require(&prepared.plan.domain)?;
    if input.game.as_os_str().is_empty()
        || crate::collection::paths::resolve_file(&input.game, game_profile.executable).is_err()
    {
        bail!(
            "Choose the {} source directory containing {}",
            game_profile.name,
            game_profile.executable
        );
    }
    let game = input.game.canonicalize()?;
    let output = canonical_target(&input.output)?;
    if input.output.symlink_metadata().is_ok() {
        bail!(
            "Destination already exists. Choose a new folder; existing installations are preserved"
        );
    }
    let cache = canonical_target(&input.cache)?;
    let job = prepared.job.canonicalize()?;
    for protected in [&game, &cache, &job] {
        if output.starts_with(protected) || protected.starts_with(&output) {
            bail!("Installation destination must be separate from the game, cache and job folders");
        }
    }
    if cache.starts_with(&game) || game.starts_with(&cache) {
        bail!("The download cache must be separate from the source game");
    }
    if !input.masterlist.as_os_str().is_empty() && !input.masterlist.is_file() {
        bail!("Choose an existing LOOT masterlist or leave it blank for the pinned default");
    }
    if !input.profile_ini.as_os_str().is_empty() && !input.profile_ini.is_dir() {
        bail!("The profile INI source directory is missing");
    }
    for path in [
        &input.cache,
        input.output.parent().context("Missing output parent")?,
    ] {
        std::fs::create_dir_all(path)?;
        let probe = tempfile::NamedTempFile::new_in(path).context("Destination is not writable")?;
        drop(probe);
    }
    let mut game_bytes = 0u64;
    for entry in walkdir::WalkDir::new(&game).follow_links(false) {
        let entry = entry?;
        if entry.file_type().is_symlink()
            || !(entry.file_type().is_dir() || entry.file_type().is_file())
        {
            bail!("Source game contains a link or special file");
        }
        if entry.file_type().is_file() {
            game_bytes = game_bytes.saturating_add(entry.metadata()?.len());
        }
    }
    let archives: u64 = prepared
        .plan
        .artifacts
        .iter()
        .filter_map(|a| a.expected_size)
        .sum();
    // Expanded payload size is not known until extraction. This checks a
    // conservative known minimum; it never presents it as a final size estimate.
    if available_bytes(input.output.parent().unwrap())? < game_bytes.saturating_add(archives) {
        bail!("Insufficient destination space for the private game and retained archives (expanded mods require additional space)");
    }
    let missing_bytes: u64 = prepared
        .plan
        .artifacts
        .iter()
        .filter(|a| {
            a.source_type != "bundle"
                && !input
                    .cache
                    .join(format!("{}.archive", a.expected_md5))
                    .is_file()
                && !prepared.manual_archives.contains_key(&a.id)
        })
        .filter_map(|a| a.expected_size)
        .sum();
    if available_bytes(&input.cache)? < missing_bytes {
        bail!("Insufficient cache space for the requested downloads");
    }
    Ok(())
}

#[allow(clippy::unnecessary_cast)] // libc field widths differ across Unix targets.
fn available_bytes(path: &Path) -> Result<u64> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let path = std::ffi::CString::new(path.as_os_str().as_bytes())?;
        let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
        if unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let stat = unsafe { stat.assume_init() };
        Ok((stat.f_bavail as u64).saturating_mul(stat.f_frsize as u64))
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(u64::MAX)
    }
}

/// One job owns the destination through acquisition and publication. Existing
/// staging's own lock remains authoritative while the worker is running.
pub fn install(
    prepared: &Prepared,
    key: &str,
    executable: &Path,
    token: &CancellationToken,
    events: &SyncSender<Event>,
) -> Result<()> {
    acquire::check(token)?;
    let _job_lock = crate::collection::stage::lock_job(&prepared.job)?;
    let output = canonical_target(&prepared.inputs.output)?;
    let lock_dir =
        cache_root()
            .join("destination-locks")
            .join(crate::collection::package::digest_bytes(
                output.as_os_str().as_encoded_bytes(),
            ));
    std::fs::create_dir_all(&lock_dir)?;
    let _destination_lock = crate::collection::stage::lock_job(&lock_dir)?;
    if prepared.inputs.output.symlink_metadata().is_ok() {
        let report = crate::collection::worker::recover(
            &prepared.inputs.output,
            &job_identity(&prepared.job),
            &prepared.plan,
            token,
            &|e| {
                let _ = events.try_send(Event::Worker(e));
            },
        )?;
        atomic_json(&prepared.job.join("completed.json"), &report)?;
        let _ = events.send(Event::Finished(Box::new(report)));
        return Ok(());
    }
    preflight(prepared)?;
    let package = CollectionPackage::open(&prepared.package)?;
    let rebuilt = CollectionPlan::build(&package, &prepared.options())?;
    if serde_json::to_vec(&rebuilt)? != serde_json::to_vec(&prepared.plan)? {
        bail!("The reviewed plan changed; review the collection again");
    }
    prepared.save()?;
    let plan_path = prepared.job.join("plan.json");
    atomic_json(&plan_path, &prepared.plan)?;
    let artifact_path = prepared.job.join("artifacts.json");
    let artifacts: Vec<_> = prepared
        .plan
        .artifacts
        .iter()
        .filter(|a| {
            a.source_type != "bundle"
                && prepared
                    .plan
                    .members
                    .iter()
                    .any(|m| m.selected && m.artifact_id == a.id)
        })
        .collect();
    let total_bytes: u64 = artifacts.iter().filter_map(|a| a.expected_size).sum();
    let verified = AtomicUsize::new(0);
    let verified_bytes = AtomicU64::new(0);
    let mapping = Mutex::new(BTreeMap::<String, PathBuf>::new());
    let missing = Mutex::new(Vec::new());
    let nexus = acquire::Nexus::new(key)?;
    // Account validation is lazy: an entirely cached/offline install needs no key.
    let premium = std::sync::OnceLock::<bool>::new();
    let pool = rayon::ThreadPoolBuilder::new().num_threads(4).build()?;
    use rayon::prelude::*;
    pool.install(|| artifacts.par_iter().for_each(|artifact| {
        if token.is_cancelled() { return; }
        let name = prepared.plan.members.iter().find(|m| m.selected && m.artifact_id == artifact.id).map(|m| m.name.clone()).unwrap_or_else(|| artifact.id.clone());
        let target = prepared.inputs.cache.join(format!("{}.archive", artifact.expected_md5));
        let result = (|| -> Result<PathBuf> {
            let chosen = prepared.manual_archives.get(&artifact.id).unwrap_or(&target);
            if acquire::verify_archive(chosen, artifact, token).is_ok() { return chosen.canonicalize().context("Resolve verified archive"); }
            if prepared.manual_archives.contains_key(&artifact.id) { bail!("The selected archive does not match this exact file; choose the requested version"); }
            acquire::check(token)?;
            if artifact.source_type == "nexus" && !*premium.get_or_init(|| nexus.premium().unwrap_or(false)) {
                bail!("Download the exact file in your browser and select its archive, or connect a Premium account in Settings");
            }
            let direct = if artifact.source_type == "direct" {
                prepared.plan.members.iter().find(|m| m.selected && m.artifact_id == artifact.id).and_then(|m| package.collection.mods.get(m.source_index)).map(|m| m.source.url.as_str())
            } else { None };
            let notify = |bytes| { let _ = events.try_send(Event::Downloads { verified: verified.load(Ordering::Relaxed), total: artifacts.len(), bytes: verified_bytes.load(Ordering::Relaxed).saturating_add(bytes), total_bytes, item: name.clone() }); };
            nexus.artifact(artifact, direct, &target, token, &notify)?;
            target.canonicalize().context("Resolve downloaded archive")
        })();
        match result {
            Ok(path) => {
                verified_bytes.fetch_add(path.metadata().map(|m| m.len()).unwrap_or(0), Ordering::Relaxed);
                verified.fetch_add(1, Ordering::Relaxed);
                let mut map = mapping.lock().unwrap();
                map.insert(artifact.id.clone(), path);
                if let Err(e) = atomic_json(&artifact_path, &*map) { missing.lock().unwrap().push(MissingArtifact { id: artifact.id.clone(), name: name.clone(), reason: e.to_string(), page: None }); }
            }
            Err(e) => missing.lock().unwrap().push(MissingArtifact { id: artifact.id.clone(), name: name.clone(), reason: safe_message(&e.to_string()), page: (artifact.source_type == "nexus").then(|| format!("https://www.nexusmods.com/{}/mods/{}?tab=files&file_id={}", artifact.domain, artifact.mod_id, artifact.file_id)) }),
        }
        let _ = events.try_send(Event::Downloads { verified: verified.load(Ordering::Relaxed), total: artifacts.len(), bytes: verified_bytes.load(Ordering::Relaxed), total_bytes, item: name });
    }));
    acquire::check(token)?;
    let missing = missing.into_inner().unwrap();
    if !missing.is_empty() {
        let _ = events.send(Event::NeedsFiles(missing));
        return Ok(());
    }
    atomic_json(&artifact_path, &mapping.into_inner().unwrap())?;
    let _ = events.try_send(Event::Worker(WorkerEvent::Progress {
        phase: "Preparing plugin rules".into(),
        completed: 0,
        total: 0,
        item: "Pinning the LOOT masterlist".into(),
    }));
    let masterlist = prepared.job.join("masterlist.yaml");
    let pin_path = prepared.job.join("masterlist-pin.json");
    let masterlist_hash = if pin_path.exists() {
        let expected: String = read_json(&pin_path, 1024)?;
        if digest_file(&masterlist)? != expected {
            bail!("The job's pinned LOOT masterlist changed");
        }
        expected
    } else {
        if prepared.inputs.masterlist.as_os_str().is_empty() {
            acquire::masterlist_for(&prepared.plan.domain, &masterlist, token)?;
        } else {
            std::fs::copy(&prepared.inputs.masterlist, &masterlist)?;
        }
        let hash = digest_file(&masterlist)?;
        atomic_json(&pin_path, &hash)?;
        hash
    };
    let request = WorkerRequest {
        protocol_version: 1,
        job_identity: Some(job_identity(&prepared.job)),
        package: prepared.package.clone(),
        plan: plan_path,
        artifacts: artifact_path,
        stage: prepared.job.join("stage"),
        game: prepared.inputs.game.clone(),
        output: prepared.inputs.output.clone(),
        profile_ini: (!prepared.inputs.profile_ini.as_os_str().is_empty())
            .then(|| prepared.inputs.profile_ini.clone()),
        masterlist: Some(masterlist),
        masterlist_sha256: Some(masterlist_hash),
    };
    let request_path = prepared.job.join("worker.json");
    atomic_json(&request_path, &request)?;
    run_worker(executable, &request_path, token, events)
}

fn run_worker(
    executable: &Path,
    request_path: &Path,
    token: &CancellationToken,
    events: &SyncSender<Event>,
) -> Result<()> {
    acquire::check(token)?;
    let mut command = Command::new(executable);
    command
        .args(["collection", "gui-worker"])
        .arg(request_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    for (name, _) in std::env::vars_os() {
        if credential_variable(&name.to_string_lossy()) {
            command.env_remove(name);
        }
    }
    let mut child = command.spawn().context("Start the Collections worker")?;
    let stdout = child.stdout.take().context("Worker output missing")?;
    let (tx, rx) = std::sync::mpsc::sync_channel(64);
    let reader = std::thread::spawn(move || {
        use std::io::Read;
        let mut reader = BufReader::new(stdout);
        loop {
            let mut line = String::new();
            match reader.by_ref().take(64 * 1024 + 1).read_line(&mut line) {
                Ok(0) => break,
                Ok(n) if n <= 64 * 1024 => {
                    let event = serde_json::from_str::<WorkerEvent>(&line).map_err(|_| ());
                    if tx.send(event).is_err() {
                        break;
                    }
                }
                _ => {
                    let _ = tx.send(Err(()));
                    break;
                }
            }
        }
    });
    let mut terminal = None;
    let mut cancelled = false;
    loop {
        if token.is_cancelled() && !cancelled {
            if let Some(mut input) = child.stdin.take() {
                let _ = input.write_all(b"cancel\n");
            }
            cancelled = true;
        }
        match rx.recv_timeout(Duration::from_millis(100)) {
            Ok(Ok(event)) => match event {
                WorkerEvent::Progress { .. } => {
                    let _ = events.try_send(Event::Worker(event));
                }
                other => {
                    terminal = Some(other);
                }
            },
            Ok(Err(())) => {
                child.stdin.take();
                let _ = child.kill();
                terminal = Some(WorkerEvent::Failed {
                    message: "Invalid worker response".into(),
                });
                break;
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
        }
    }
    child.stdin.take();
    let status = child.wait()?;
    let _ = reader.join();
    match terminal {
        Some(WorkerEvent::Completed { report_path }) if status.success() => {
            let request: WorkerRequest = read_json(request_path, 64 * 1024)?;
            if report_path != request.output.join(".collection/report.json") { bail!("Worker returned a different output destination"); }
            let report: PublicationReport = read_json(&report_path, 1024 * 1024)?;
            atomic_json(&request_path.with_file_name("completed.json"), &report)?;
            let _ = events.send(Event::Finished(Box::new(report))); Ok(())
        }
        Some(WorkerEvent::Cancelled) => { let _ = events.send(Event::Cancelled); Ok(()) }
        Some(WorkerEvent::Failed { message }) => bail!("{}", safe_message(&message)),
        _ if token.is_cancelled() => { let _ = events.send(Event::Cancelled); Ok(()) }
        _ => bail!("The worker stopped before completion. Verified downloads and staged files can be resumed"),
    }
}

fn job_identity(job: &Path) -> String {
    crate::collection::package::digest_bytes(job.as_os_str().as_encoded_bytes())
}
