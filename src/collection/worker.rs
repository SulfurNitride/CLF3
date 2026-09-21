//! Standalone worker. Reads no account settings and performs no HTTP requests.
use super::{
    package::digest_file,
    plan::PlanOptions,
    progress::{safe_message, WorkerEvent},
    publish::PublicationOptions,
    CollectionPackage, CollectionPlan,
};
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
};
use tokio_util::sync::CancellationToken;

pub const WORKER_CAPABILITY: &str = "collection_local_worker_v1";

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerRequest {
    pub protocol_version: u32,
    #[serde(default)]
    pub job_identity: Option<String>,
    pub package: PathBuf,
    pub plan: PathBuf,
    pub artifacts: PathBuf,
    pub stage: PathBuf,
    pub game: PathBuf,
    pub output: PathBuf,
    pub profile_ini: Option<PathBuf>,
    pub masterlist: Option<PathBuf>,
    pub masterlist_sha256: Option<String>,
}

pub fn read_json<T: serde::de::DeserializeOwned>(path: &Path, limit: u64) -> Result<T> {
    let file = File::open(path)?;
    if !path.symlink_metadata()?.is_file() || file.metadata()?.len() > limit {
        bail!("Invalid or oversized job file");
    }
    Ok(serde_json::from_reader(BufReader::new(file))?)
}

pub fn execute(
    request: &WorkerRequest,
    token: &CancellationToken,
    progress: &super::progress::Progress<'_>,
) -> Result<PathBuf> {
    if request.protocol_version != 1 {
        bail!("Unsupported worker protocol");
    }
    for path in [
        &request.package,
        &request.plan,
        &request.artifacts,
        &request.stage,
        &request.game,
        &request.output,
    ] {
        if !path.is_absolute() {
            bail!("Worker paths must be absolute");
        }
    }
    let expected: CollectionPlan = read_json(&request.plan, 32 * 1024 * 1024)?;
    let package = CollectionPackage::open(&request.package)?;
    let actual = CollectionPlan::build(
        &package,
        &PlanOptions {
            schema_id: expected.schema_id,
            locator: expected.locator.clone(),
            game_version: expected.game_version.clone(),
            all_optional: false,
            selected_optional: expected
                .members
                .iter()
                .filter(|m| m.optional && m.selected)
                .map(|m| m.id.clone())
                .collect::<BTreeSet<_>>(),
        },
    )?;
    if serde_json::to_vec(&actual)? != serde_json::to_vec(&expected)? {
        bail!("The package or plan changed; review the collection again");
    }
    if let Some(path) = &request.masterlist {
        if request.masterlist_sha256.as_deref() != Some(digest_file(path)?.as_str()) {
            bail!("The pinned LOOT masterlist changed");
        }
    }
    let artifacts: BTreeMap<String, PathBuf> = read_json(&request.artifacts, 16 * 1024 * 1024)?;
    let staged = super::stage::stage_with_progress(
        &package,
        &actual,
        &artifacts,
        &request.stage,
        token,
        progress,
    )?;
    drop(staged);
    if token.is_cancelled() {
        bail!("Installation cancelled");
    }
    let report = super::publish::publish_with_progress(
        &request.stage,
        &request.game,
        &request.output,
        &PublicationOptions {
            profile_ini: request.profile_ini.as_deref(),
            masterlist: request.masterlist.as_deref(),
            artifacts: Some(&request.artifacts),
            job_identity: request.job_identity.as_deref(),
        },
        token,
        progress,
    )?;
    Ok(report.output.join(".collection/report.json"))
}

pub fn run(path: &Path) -> Result<()> {
    let request: WorkerRequest = read_json(path, 64 * 1024).context("Read worker request")?;
    let token = CancellationToken::new();
    let input_token = token.clone();
    // EOF means the host disappeared. A bounded reader prevents untrusted input
    // from allocating arbitrarily; no incoming content is printed.
    std::thread::spawn(move || {
        use std::io::Read;
        let mut reader = BufReader::new(std::io::stdin()).take(1024);
        let mut command = String::new();
        let _ = reader.read_line(&mut command);
        input_token.cancel();
    });
    let progress = |event| {
        if super::cli::emit(&event).is_err() {
            token.cancel();
        }
    };
    match execute(&request, &token, &progress) {
        Ok(report_path) => super::cli::emit(&WorkerEvent::Completed { report_path }),
        Err(_) if token.is_cancelled() => super::cli::emit(&WorkerEvent::Cancelled),
        Err(error) => super::cli::emit(&WorkerEvent::Failed {
            message: safe_message(&format!("{error:#}")),
        }),
    }
}

/// A result committed before a host disconnect belongs to this exact job.
/// Recheck it read-only; an unrelated existing destination is never adopted.
pub fn recover(
    output: &Path,
    identity: &str,
    expected: &CollectionPlan,
    token: &CancellationToken,
    progress: &super::progress::Progress<'_>,
) -> Result<super::publish::PublicationReport> {
    if output.symlink_metadata()?.file_type().is_symlink() {
        bail!("Destination is a link; choose a new folder");
    }
    let marker: serde_json::Value = read_json(&output.join(".collection/gui-job.json"), 4096)
        .context(
            "Destination already exists and does not belong to this GUI job; choose a new folder",
        )?;
    let signature = super::package::digest_bytes(&serde_json::to_vec(expected)?);
    if marker["job_identity"].as_str() != Some(identity)
        || marker["plan_sha256"].as_str() != Some(&signature)
    {
        bail!("Destination belongs to a different job or plan; choose a new folder");
    }
    let journal: super::stage::StagingJournal = read_json(
        &output.join(".collection/installation.json"),
        super::stage::MAX_JOURNAL_BYTES,
    )?;
    if journal.status != "published"
        || serde_json::to_vec(&journal.plan)? != serde_json::to_vec(expected)?
    {
        bail!("Published plan differs from the reviewed plan");
    }
    let report: super::publish::PublicationReport =
        read_json(&output.join(".collection/report.json"), 1024 * 1024)?;
    if report.package_sha256 != expected.package_sha256
        || report.output != output
        || report.installed_members != expected.installation_order.len()
    {
        bail!("Published report differs from the reviewed job");
    }
    let verified = super::publish::verify_published_payloads(output, &journal, token, progress)?;
    if verified != report.verified_mod_files {
        bail!("Published file count changed");
    }
    for name in super::games::require(&journal.plan.domain)?.snapshot_files() {
        let snapshot = output.join(".collection/profile-snapshot").join(name);
        if snapshot.is_file()
            && std::fs::read(snapshot)?
                != std::fs::read(output.join("profiles/Default").join(name))?
        {
            bail!("Published profile changed since installation");
        }
    }
    if super::games::require(&journal.plan.domain)?.timestamp_order {
        let timestamps: BTreeMap<String, u64> = read_json(
            &output.join(".collection/plugin-timestamps.json"),
            4 * 1024 * 1024,
        )?;
        let order = std::fs::read_to_string(output.join("profiles/Default/loadorder.txt"))?;
        let expected: BTreeMap<_, _> = order
            .lines()
            .filter(|name| !name.is_empty() && !name.starts_with('#'))
            .enumerate()
            .map(|(index, name)| (name.to_lowercase(), 1_577_836_800 + index as u64 * 60))
            .collect();
        let recorded: BTreeMap<_, _> = timestamps
            .iter()
            .map(|(path, seconds)| {
                (
                    path.rsplit('/').next().unwrap_or(path).to_lowercase(),
                    *seconds,
                )
            })
            .collect();
        if timestamps.len() != expected.len() || recorded != expected {
            bail!("Published plugin timestamps do not cover the recorded load order");
        }
        for (path, seconds) in timestamps {
            let file = super::paths::resolve_file(output, &path)?;
            if file
                .metadata()?
                .modified()?
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs()
                != seconds
            {
                bail!("Published plugin timestamps changed since installation");
            }
        }
    }
    let mut roots = BTreeMap::new();
    for id in &journal.asset_order {
        for file in &journal
            .members
            .get(id)
            .context("Missing published member")?
            .files
        {
            if file.deployment_root == "game" && !file.excluded {
                roots.insert(
                    file.staged_path
                        .strip_prefix("Root/")
                        .context("Invalid root payload")?
                        .to_lowercase(),
                    &file.sha256,
                );
            }
        }
    }
    for (path, hash) in roots {
        if token.is_cancelled() {
            bail!("Verification cancelled");
        }
        if &digest_file(&super::paths::resolve_file(
            &output.join("Stock Game"),
            &path,
        )?)? != hash
        {
            bail!("Published root files changed");
        }
    }
    let archives: BTreeMap<String, PathBuf> =
        read_json(&output.join(".collection/artifacts.json"), 16 * 1024 * 1024)?;
    for artifact in expected
        .artifacts
        .iter()
        .filter(|a| a.source_type != "bundle")
    {
        let path = archives
            .get(&artifact.id)
            .context("A retained archive is missing")?;
        let relative = path
            .strip_prefix(output)
            .context("Retained archive is outside the published installation")?
            .to_str()
            .context("Invalid archive path")?;
        let path = super::paths::resolve_file(output, relative)?;
        if artifact
            .expected_size
            .is_some_and(|s| path.metadata().map(|m| m.len() != s).unwrap_or(true))
            || super::stage::md5_file(&path, token)? != artifact.expected_md5
        {
            bail!("Retained archive differs from the pinned file");
        }
    }
    Ok(report)
}
