//! Planning-only extension negotiated separately from Wabbajack protocol v1.
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
pub const PLAN_CAPABILITY: &str = "collection_plan_v1";

#[derive(Debug, Deserialize)]
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
    let locator = parse_collection_url(source_url)?;
    let job_id = Uuid::new_v4().to_string();
    let request_id = Uuid::new_v4().to_string();
    emit(
        &json!({"type":"hello", "protocol_version": COLLECTION_PROTOCOL_VERSION,
        "engine_version":env!("CARGO_PKG_VERSION"), "job_id":job_id,
        "capabilities":[PLAN_CAPABILITY], "required_capabilities":[PLAN_CAPABILITY]}),
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
        let options = PlanOptions {
            schema_id: Some(schema_id),
            locator: Some(resolved.clone()),
            all_optional,
            game_version,
            ..Default::default()
        };
        // Blocking archive work stays off the protocol reader. A cancellation or
        // disconnected host wins over a prepared plan and prevents publication.
        let path = path.clone();
        let (sender, work) = oneshot::channel();
        std::thread::Builder::new()
            .name("collection-planner".into())
            .spawn(move || {
                let result = CollectionPackage::open(&path)
                    .and_then(|package| CollectionPlan::build(&package, &options));
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
        Ok(Some(plan)) => {
            emit(
                &json!({"type":"collection_plan_ready", "job_id":job_id, "request_id":request_id,
                "status":if plan.blockers.is_empty() {"planned"} else {"blocked"},
                "installation_available":false, "plan":plan}),
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
