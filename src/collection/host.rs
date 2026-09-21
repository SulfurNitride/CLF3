//! Collections operations negotiated separately from Wabbajack protocol v1.
//! The host supplies an already acquired package; credentials and signed URLs
//! are not accepted by this protocol or stored in the resulting plan.

use super::{
    cli::emit,
    package::CollectionPackage,
    plan::{CollectionPlan, PlanOptions},
    url::{parse_collection_url, CollectionLocator},
};
use anyhow::{bail, Context, Result};
use serde::Deserialize;
use serde_json::json;
use std::io::BufRead;
use std::{path::PathBuf, time::Duration};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

pub const COLLECTION_PROTOCOL_VERSION: u32 = 1;
pub const INSTALL_CAPABILITY: &str = "collection_hosted_install_v1";
pub const PLAN_CAPABILITY: &str = "collection_plan_v1";

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum CollectionHostCommand {
    HelloAck {
        protocol_version: u32,
        capabilities: Vec<String>,
    },
    CollectionPackageResult {
        job_id: String,
        request_id: String,
        locator: CollectionLocator,
        schema_id: u32,
        package_path: PathBuf,
    },
    CollectionRequestFailed {
        job_id: String,
        request_id: String,
    },
    CollectionInstall {
        job_id: String,
        request: super::worker::WorkerRequest,
    },
    Cancel {
        job_id: String,
    },
}

impl CollectionHostCommand {
    pub fn validate_package(
        &self,
        expected_job: &str,
        expected_request: &str,
        expected_locator: &CollectionLocator,
    ) -> Result<(&PathBuf, u32, &CollectionLocator)> {
        let Self::CollectionPackageResult {
            job_id,
            request_id,
            locator,
            schema_id,
            package_path,
        } = self
        else {
            bail!("Expected collection package result");
        };
        if job_id != expected_job || request_id != expected_request {
            bail!("Collection response does not match the pending job/request");
        }
        if locator.domain != expected_locator.domain
            || locator.slug != expected_locator.slug
            || locator.revision.is_none()
            || locator.revision == Some(0)
            || expected_locator
                .revision
                .is_some_and(|r| locator.revision != Some(r))
        {
            bail!("Host returned a different or unpinned collection revision");
        }
        if *schema_id != 1 {
            bail!("Unsupported collection schema from host");
        }
        if !package_path.is_absolute() {
            bail!("Host package path must be absolute");
        }
        Ok((package_path, *schema_id, locator))
    }
}

pub async fn run(source_url: &str, all_optional: bool, game_version: Option<String>) -> Result<()> {
    run_with_options(
        source_url,
        all_optional,
        game_version,
        None,
        Default::default(),
    )
    .await
}

pub async fn run_with_options(
    source_url: &str,
    all_optional: bool,
    game_version: Option<String>,
    game_path: Option<PathBuf>,
    selected_optional: std::collections::BTreeSet<String>,
) -> Result<()> {
    let locator = parse_collection_url(source_url)?;
    let job_id = Uuid::new_v4().to_string();
    let request_id = Uuid::new_v4().to_string();
    emit(
        &json!({"type":"hello", "protocol_version": COLLECTION_PROTOCOL_VERSION,
        "engine_version":env!("CARGO_PKG_VERSION"), "job_id":job_id,
        "capabilities":[PLAN_CAPABILITY, INSTALL_CAPABILITY], "game_support":game_support(), "required_capabilities":[PLAN_CAPABILITY]}),
    )?;
    let mut input = spawn_reader()?;
    let result = async {
        let hello = receive(&mut input, Duration::from_secs(30)).await?;
        match hello {
            CollectionHostCommand::HelloAck {
                protocol_version,
                capabilities,
            } if protocol_version == COLLECTION_PROTOCOL_VERSION
                && capabilities.iter().any(|c| c == PLAN_CAPABILITY) => {}
            CollectionHostCommand::Cancel { job_id: id } if id == job_id => return Ok(None),
            _ => bail!("Host does not support collection_plan_v1; use a compatible Fluorine host"),
        }
        emit(
            &json!({"type":"collection_revision_required", "job_id":job_id, "request_id":request_id,
            "locator":locator, "required_schema_id":1, "result_kind":"local_package"}),
        )?;
        let response = receive(&mut input, Duration::from_secs(300)).await?;
        match &response {
            CollectionHostCommand::Cancel { job_id: id } if id == &job_id => return Ok(None),
            CollectionHostCommand::CollectionRequestFailed {
                job_id: id,
                request_id: request,
            } if id == &job_id && request == &request_id => {
                bail!("Host could not acquire the collection package")
            }
            _ => {}
        }
        let (path, schema_id, resolved) =
            response.validate_package(&job_id, &request_id, &locator)?;
        let mut options = PlanOptions {
            schema_id: Some(schema_id),
            locator: Some(resolved.clone()),
            all_optional,
            game_version,
            selected_optional,
        };
        // Blocking archive work stays off the protocol reader. A cancellation or
        // disconnected host wins over a prepared plan and prevents publication.
        let path = path.clone();
        let (sender, work) = oneshot::channel();
        std::thread::Builder::new()
            .name("collection-planner".into())
            .spawn(move || {
                let result = (|| {
                    let package = CollectionPackage::open(&path)?;
                    if let Some(game_path) = game_path {
                        let game = super::games::require(package.collection.domain())?;
                        let exe = super::paths::resolve_file(&game_path, game.executable)?;
                        options.game_version = super::publish::executable_version(&exe)?;
                    }
                    let plan = CollectionPlan::build(&package, &options)?;
                    let sources: std::collections::BTreeMap<_, _> = plan
                        .members
                        .iter()
                        .filter(|m| m.selected)
                        .filter_map(|m| {
                            let source = &package.collection.mods[m.source_index].source;
                            (source.source_type == "direct")
                                .then(|| (m.artifact_id.clone(), source.url.clone()))
                        })
                        .collect();
                    Ok::<_, anyhow::Error>((plan, sources))
                })();
                let _ = sender.send(result);
            })
            .context("Start collection planning worker")?;
        tokio::select! {
            biased;
            command = receive(&mut input, Duration::from_secs(300)) => {
                match command? {
                    CollectionHostCommand::Cancel { job_id: id } if id == job_id => Ok(None),
                    _ => bail!("Unexpected command during collection planning"),
                }
            }
            plan = work => Ok(Some(plan.context("Collection planning worker failed")??)),
        }
    }
    .await;
    match result {
        Ok(Some((plan, sources))) => {
            emit(
                &json!({"type":"collection_plan_ready", "job_id":job_id, "request_id":request_id,
                "status":if plan.blockers.is_empty() {"planned"} else {"blocked"},
                "installation_available":false, "acquisition_sources":sources, "plan":plan}),
            )?;
            Ok(())
        }
        Ok(None) => emit(&json!({"type":"collection_cancelled", "job_id":job_id})),
        Err(error) => {
            // No raw host payload, URLs, package data or chained I/O errors.
            emit(
                &json!({"type":"collection_failed", "job_id":job_id, "message":error.to_string()}),
            )?;
            bail!("Collection planning failed; see structured failure event")
        }
    }
}

// Tokio stdin uses an uncancellable blocking-pool read, which keeps runtime
// shutdown waiting for EOF even after plan_ready. A dedicated reader thread
// must not keep the child alive while Fluorine waits for its exit status.
fn spawn_reader() -> Result<mpsc::Receiver<Result<CollectionHostCommand>>> {
    let (sender, receiver) = mpsc::channel(8);
    std::thread::Builder::new()
        .name("collection-host-stdin".into())
        .spawn(move || {
            let mut input = std::io::stdin().lock();
            loop {
                let command = read_command(&mut input);
                let failed = command.is_err();
                if sender.blocking_send(command).is_err() || failed {
                    break;
                }
            }
        })
        .context("Start collection protocol reader")?;
    Ok(receiver)
}

fn read_command(input: &mut impl BufRead) -> Result<CollectionHostCommand> {
    let mut line = Vec::new();
    loop {
        let available = input.fill_buf().context("Read host command")?;
        if available.is_empty() {
            bail!("Collection host disconnected");
        }
        let consumed = available
            .iter()
            .position(|&b| b == b'\n')
            .map(|n| n + 1)
            .unwrap_or(available.len());
        if line.len() + consumed > 64 * 1024 {
            bail!("Host command exceeds size limit");
        }
        line.extend_from_slice(&available[..consumed]);
        input.consume(consumed);
        if line.ends_with(b"\n") {
            break;
        }
    }
    serde_json::from_slice(&line)
        .map_err(|_| anyhow::anyhow!("Malformed or unsupported collection host command"))
}

async fn receive(
    input: &mut mpsc::Receiver<Result<CollectionHostCommand>>,
    timeout: Duration,
) -> Result<CollectionHostCommand> {
    tokio::time::timeout(timeout, input.recv())
        .await
        .context("Timed out waiting for collection host")?
        .context("Collection host disconnected")?
}

/// Export the engine registry so frontends never guess another game's adapter.
pub fn game_support() -> serde_json::Value {
    json!(super::games::PROFILES
        .iter()
        .map(|g| json!({
            "domain":g.domain, "name":g.name, "manager_name":g.manager_name(),
            "executable":g.executable, "steam_ids":g.steam_ids,
            "experimental":g.experimental, "masterlist_url":g.masterlist.url(),
            "masterlist_sha256":g.masterlist.sha256
        }))
        .collect::<Vec<_>>())
}

pub fn install_local(
    request: &super::worker::WorkerRequest,
    token: &tokio_util::sync::CancellationToken,
    progress: &super::progress::Progress<'_>,
) -> Result<PathBuf> {
    use super::worker::{read_json, recover};
    if request.protocol_version != 1
        || [
            &request.package,
            &request.plan,
            &request.artifacts,
            &request.stage,
            &request.game,
            &request.output,
        ]
        .iter()
        .any(|p| !p.is_absolute())
    {
        bail!("Invalid hosted worker protocol or local paths");
    }
    let identity = request
        .job_identity
        .as_deref()
        .filter(|s| !s.is_empty() && s.len() <= 128)
        .context("A hosted installation requires a stable job identity")?;
    let plan: CollectionPlan = read_json(&request.plan, 32 * 1024 * 1024)?;
    if plan
        .locator
        .as_ref()
        .is_some_and(|l| l.revision.is_none() || l.revision == Some(0))
    {
        bail!("Review an explicitly pinned collection revision");
    }
    let job = request.plan.parent().context("Missing job directory")?;
    if request.stage.parent() != Some(job) || request.artifacts.parent() != Some(job) {
        bail!("Staging and artifact mapping must belong to the hosted job directory");
    }
    let _lock = super::stage::lock_job(job)?;
    if request.output.symlink_metadata().is_ok() {
        recover(&request.output, identity, &plan, token, progress)?;
        return Ok(request.output.join(".collection/report.json"));
    }
    let prepared = crate::collection_app::Prepared {
        author: String::new(),
        job: job.into(),
        package: request.package.clone(),
        inputs: crate::collection_app::Inputs {
            game: request.game.clone(),
            output: request.output.clone(),
            cache: job.join("acquired"),
            masterlist: request.masterlist.clone().unwrap_or_default(),
            profile_ini: request.profile_ini.clone().unwrap_or_default(),
            ..Default::default()
        },
        plan: plan.clone(),
        manual_archives: Default::default(),
    };
    crate::collection_app::preflight(&prepared)?;
    let path = super::worker::execute(request, token, progress)?;
    // Completion is a verified publication, including a rename that raced cancellation.
    recover(
        &request.output,
        identity,
        &plan,
        &Default::default(),
        progress,
    )?;
    Ok(path)
}

pub async fn run_install() -> Result<()> {
    let job_id = Uuid::new_v4().to_string();
    emit(&json!({"type":"hello", "protocol_version":1,
        "engine_version":env!("CARGO_PKG_VERSION"), "job_id":job_id,
        "capabilities":[INSTALL_CAPABILITY], "required_capabilities":[INSTALL_CAPABILITY]}))?;
    let mut input = spawn_reader()?;
    let token = tokio_util::sync::CancellationToken::new();
    let result = async {
        match receive(&mut input, Duration::from_secs(30)).await? {
            CollectionHostCommand::HelloAck { protocol_version:1, capabilities }
                if capabilities.iter().any(|c| c == INSTALL_CAPABILITY) => (),
            CollectionHostCommand::Cancel { job_id:id } if id == job_id => return Ok(None),
            _ => bail!("Host does not support collection_hosted_install_v1"),
        }
        let request = match receive(&mut input, Duration::from_secs(30)).await? {
            CollectionHostCommand::CollectionInstall {job_id:id, request} if id == job_id => request,
            CollectionHostCommand::Cancel {job_id:id} if id == job_id => return Ok(None),
            _ => bail!("Invalid hosted installation request"),
        };
        let worker_token = token.clone();
        let worker_job = job_id.clone();
        let (tx, mut work) = oneshot::channel();
        std::thread::spawn(move || {
            let progress = |event| {
                if emit(&json!({"type":"collection_progress", "job_id":worker_job, "progress":event})).is_err() {
                    worker_token.cancel();
                }
            };
            let result = install_local(&request, &worker_token, &progress);
            let _ = tx.send(result);
        });
        tokio::select! {
            biased;
            command = receive(&mut input, Duration::from_secs(7 * 24 * 3600)) => {
                token.cancel();
                let valid_cancel = matches!(command, Ok(CollectionHostCommand::Cancel {job_id:id}) if id == job_id);
                // Join before reporting cancellation; publication/staging must be quiescent.
                let result = (&mut work).await.context("Installation worker stopped")?;
                if !valid_cancel { bail!("Collection host disconnected or sent an invalid command"); }
                match result { Ok(path) => Ok(Some(path)), Err(_) => Ok(None) }
            }
            result = &mut work => Ok(Some(result.context("Installation worker stopped")??)),
        }
    }.await;
    match result {
        Ok(Some(path)) => emit(
            &json!({"type":"collection_install_completed", "job_id":job_id, "report_path":path}),
        ),
        Ok(None) => emit(&json!({"type":"collection_cancelled", "job_id":job_id})),
        Err(_) => {
            // Package and OS errors can contain untrusted strings. Never echo them.
            emit(&json!({"type":"collection_failed", "job_id":job_id,
                "message":"Collection installation failed verification. Review the game, plan and local files; the job can be resumed."}))?;
            bail!("Hosted collection installation failed")
        }
    }
}
