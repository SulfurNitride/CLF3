use super::{
    package::CollectionPackage,
    plan::{CollectionPlan, PlanOptions},
    url::parse_collection_url,
};
use anyhow::{bail, Result};
use clap::{Args, Subcommand};
use serde_json::json;
use std::{collections::BTreeSet, io::Write, path::PathBuf};

#[derive(Debug, Args)]
pub struct CollectionArgs {
    #[command(subcommand)]
    pub command: CollectionCommand,
}

#[derive(Debug, Subcommand)]
pub enum CollectionCommand {
    /// Credential-free local worker for the standalone Collections GUI.
    #[command(hide = true)]
    GuiWorker { request: PathBuf },
    /// Inspect a local collection .7z/.zip, extracted package or manifest as JSON.
    Inspect(PackageArgs),
    /// Produce a JSON plan; exit unsuccessfully when compatibility blockers exist.
    Plan(PackageArgs),
    /// Reproduce supported members from local artifacts into an isolated job.
    Stage {
        #[command(flatten)]
        input: PackageArgs,
        /// JSON object mapping planned artifact IDs to local archive paths.
        #[arg(long)]
        artifacts: Option<PathBuf>,
        /// New absolute directory, or an existing collection staging job.
        #[arg(long)]
        output: PathBuf,
    },
    /// Publish a completed job as a new portable instance with a private game copy.
    Publish {
        #[arg(long)]
        job: PathBuf,
        #[arg(long)]
        game_path: PathBuf,
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        profile_ini: Option<PathBuf>,
        #[arg(long)]
        masterlist: Option<PathBuf>,
        /// Preserve these verified archives in the published downloads directory.
        #[arg(long)]
        artifacts: Option<PathBuf>,
    },
    /// Describe the Collections protocol and currently implemented operations.
    Capabilities,
    /// Install a reviewed local job using negotiated, credential-free stdio.
    HostedInstall,
    /// Ask Fluorine for a pinned local package over versioned JSON stdio.
    HostedPlan {
        source_url: String,
        #[arg(long)]
        all_optional: bool,
        #[arg(long)]
        game_version: Option<String>,
        #[arg(long)]
        game_path: Option<PathBuf>,
        #[arg(long = "include-optional")]
        selected_optional: Vec<String>,
    },
}

#[derive(Debug, Args)]
pub struct PackageArgs {
    pub package: PathBuf,
    /// Nexus collection URL, preferably containing /revisions/N.
    #[arg(long)]
    pub source_url: Option<String>,
    /// Schema ID returned by Nexus for the pinned revision.
    #[arg(long)]
    pub schema_id: Option<u32>,
    #[arg(long)]
    pub game_version: Option<String>,
    /// Include an optional member by its reported member ID or reference tag.
    #[arg(long = "include-optional")]
    pub selected_optional: Vec<String>,
    #[arg(long)]
    pub all_optional: bool,
}

pub async fn run(args: &CollectionArgs) -> Result<()> {
    match &args.command {
        CollectionCommand::HostedInstall => super::host::run_install().await?,
        CollectionCommand::GuiWorker { request } => super::worker::run(request)?,
        CollectionCommand::Publish {
            job,
            game_path,
            output,
            profile_ini,
            masterlist,
            artifacts,
        } => {
            let (job, game, output, ini, masterlist, artifacts) = (
                job.clone(),
                game_path.clone(),
                output.clone(),
                profile_ini.clone(),
                masterlist.clone(),
                artifacts.clone(),
            );
            let token = tokio_util::sync::CancellationToken::new();
            let signal_token = token.clone();
            let signal = tokio::spawn(async move {
                if tokio::signal::ctrl_c().await.is_ok() {
                    signal_token.cancel();
                }
            });
            let result = tokio::task::spawn_blocking(move || {
                super::publish::publish(
                    &job,
                    &game,
                    &output,
                    &super::publish::PublicationOptions {
                        profile_ini: ini.as_deref(),
                        masterlist: masterlist.as_deref(),
                        artifacts: artifacts.as_deref(),
                        ..Default::default()
                    },
                    &token,
                )
            })
            .await?;
            signal.abort();
            emit(&result?)?;
        }
        CollectionCommand::Capabilities => emit(&json!({
            "type": "collection_capabilities", "protocol_version": 1,
            "engine_version": env!("CARGO_PKG_VERSION"), "plan_schema_version": 1,
            "capabilities": ["collection_hosted_install_v1", "collection_plan_v1", "local_packages", "artifact_member_identity", "asset_rule_validation", "provider_exclusion_planning", "local_staging", "recorded_files", "bundle", "bsdiff40", "verified_resume", "fomod_replay", "rar", "portable_publication", "loot", "skse_launcher", "game_extender_launcher", "profile_ini_tweaks", "collection_separators", "game_profiles", "legacy_plugin_activation", "timestamp_plugin_order"],
            "collection_schema_ids": [1], "games": super::games::PROFILES.iter().map(|g| g.domain).collect::<Vec<_>>(),
            "game_support": super::host::game_support(),
            "installation_available": true, "hosted_installation_available": true,
            "standalone_worker": super::worker::WORKER_CAPABILITY, "credentials": "host_only"
        }))?,
        CollectionCommand::Inspect(input) | CollectionCommand::Plan(input) => {
            let options = PlanOptions {
                schema_id: input.schema_id,
                locator: input
                    .source_url
                    .as_deref()
                    .map(parse_collection_url)
                    .transpose()?,
                all_optional: input.all_optional,
                selected_optional: input
                    .selected_optional
                    .iter()
                    .cloned()
                    .collect::<BTreeSet<_>>(),
                game_version: input.game_version.clone(),
            };
            let package = CollectionPackage::open(&input.package)?;
            let plan = CollectionPlan::build(&package, &options)?;
            emit(&plan)?;
            if matches!(args.command, CollectionCommand::Plan(_)) && !plan.blockers.is_empty() {
                bail!("Collection plan has {} compatibility blocker(s); no installed outputs were written", plan.blockers.len());
            }
        }
        CollectionCommand::HostedPlan {
            source_url,
            all_optional,
            game_version,
            game_path,
            selected_optional,
        } => {
            super::host::run_with_options(
                source_url,
                *all_optional,
                game_version.clone(),
                game_path.clone(),
                selected_optional.iter().cloned().collect(),
            )
            .await?;
        }
        CollectionCommand::Stage {
            input,
            artifacts,
            output,
        } => {
            let options = PlanOptions {
                schema_id: input.schema_id,
                locator: input
                    .source_url
                    .as_deref()
                    .map(parse_collection_url)
                    .transpose()?,
                all_optional: input.all_optional,
                selected_optional: input.selected_optional.iter().cloned().collect(),
                game_version: input.game_version.clone(),
            };
            let package = CollectionPackage::open(&input.package)?;
            let plan = CollectionPlan::build(&package, &options)?;
            let artifacts = if let Some(path) = artifacts {
                if std::fs::metadata(path)?.len() > 16 * 1024 * 1024 {
                    bail!("Artifact mapping exceeds size limit");
                }
                serde_json::from_reader(std::fs::File::open(path)?)?
            } else {
                std::collections::BTreeMap::new()
            };
            let token = tokio_util::sync::CancellationToken::new();
            let signal_token = token.clone();
            let signal = tokio::spawn(async move {
                if tokio::signal::ctrl_c().await.is_ok() {
                    signal_token.cancel();
                }
            });
            let output = output.clone();
            let result = tokio::task::spawn_blocking(move || {
                super::stage::stage(&package, &plan, &artifacts, &output, &token)
            })
            .await?;
            signal.abort();
            emit(
                &json!({"type":"collection_staged", "publication_required":true, "journal":result?}),
            )?;
        }
    }
    Ok(())
}

pub(crate) fn emit<T: serde::Serialize>(value: &T) -> Result<()> {
    let mut stdout = std::io::stdout().lock();
    serde_json::to_writer(&mut stdout, value)?;
    writeln!(stdout)?;
    stdout.flush()?;
    Ok(())
}
