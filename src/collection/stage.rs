//! Local-artifact staging. Never writes a Fluorine/MO2 profile or game directory.
//! A journal references only fully prepared member generations. Interrupted or
//! modified members are rebuilt in new directories; existing content is retained.

use super::{
    package::{digest_bytes, digest_file, CollectionPackage, MAX_PAYLOAD_BYTES},
    paths::{relative_path, resolve_file},
    plan::{CollectionPlan, MemberPlan},
    types::CollectionMod,
};
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};
use tokio_util::sync::CancellationToken;

const JOURNAL: &str = "collection-job.json";
pub(crate) const MAX_JOURNAL_BYTES: u64 = 512 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputFile {
    pub logical_path: String,
    pub staged_path: String,
    pub sha256: String,
    pub size: u64,
    pub deployment_root: String,
    pub excluded: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StagedMember {
    pub member_id: String,
    pub install_signature: String,
    pub directory: String,
    pub files: Vec<OutputFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StagingJournal {
    pub journal_schema: u32,
    pub job_id: String,
    pub plan_sha256: String,
    pub package_sha256: String,
    pub status: String,
    pub members: BTreeMap<String, StagedMember>,
    pub asset_order: Vec<String>,
    /// Retain plugin state, rules and required host capabilities for publication.
    pub plan: CollectionPlan,
}

/// `artifacts` maps artifact IDs from the plan to locally supplied archive paths.
/// This has no credential or download service dependency.
pub fn stage(
    package: &CollectionPackage,
    plan: &CollectionPlan,
    artifacts: &BTreeMap<String, PathBuf>,
    output: &Path,
    cancellation: &CancellationToken,
) -> Result<StagingJournal> {
    stage_with_progress(package, plan, artifacts, output, cancellation, &|_| {})
}

pub fn stage_with_progress(
    package: &CollectionPackage,
    plan: &CollectionPlan,
    artifacts: &BTreeMap<String, PathBuf>,
    output: &Path,
    cancellation: &CancellationToken,
    progress: &super::progress::Progress<'_>,
) -> Result<StagingJournal> {
    if !plan.blockers.is_empty() {
        bail!("Cannot stage a plan with compatibility blockers");
    }
    if package.digest != plan.package_sha256 {
        bail!("Package differs from the planned package");
    }
    if !output.is_absolute() {
        bail!("Staging directory must be absolute");
    }
    package.verify_unchanged()?;
    let signature = digest_bytes(&serde_json::to_vec(plan)?);
    // Refuse existing directories unless they carry our journal. In particular,
    // an existing portable instance or game directory cannot be a staging root.
    if output.exists() {
        if output.symlink_metadata()?.file_type().is_symlink() || !output.join(JOURNAL).is_file() {
            bail!("Staging requires a new directory or an existing collection job");
        }
    } else {
        std::fs::create_dir(output).context("Create isolated collection staging directory")?;
    }
    let root = output.canonicalize()?;
    let _lock = lock_job(&root)?;
    let mut journal = if root.join(JOURNAL).exists() {
        let path = resolve_file(&root, JOURNAL)?;
        if path.metadata()?.len() > MAX_JOURNAL_BYTES {
            bail!("Oversized staging journal");
        }
        let journal: StagingJournal =
            serde_json::from_reader(std::io::BufReader::new(File::open(path)?))
                .context("Read staging journal")?;
        if journal.journal_schema != 1
            || journal.plan_sha256 != signature
            || journal.package_sha256 != package.digest
        {
            bail!("Staging job belongs to a different plan; use a new job directory for revision updates");
        }
        journal
    } else {
        StagingJournal {
            journal_schema: 1,
            job_id: uuid::Uuid::new_v4().to_string(),
            plan_sha256: signature,
            package_sha256: package.digest.clone(),
            status: "staging".into(),
            members: BTreeMap::new(),
            asset_order: plan.asset_order.clone(),
            plan: plan.clone(),
        }
    };
    journal.status = "staging".into();
    save_journal(&root, &journal)?;
    let mut context = super::fomod::InstallerContext {
        game_version: plan.game_version.clone().unwrap_or_default(),
        ..Default::default()
    };
    for plugin in &plan.plugins {
        context.installed.insert(plugin.name.to_lowercase());
        if plugin.enabled {
            context.active.insert(plugin.name.to_lowercase());
        }
    }
    let game = super::games::require(&plan.domain)?;
    // Preserve the established SE replay context. Other adapters only assume
    // the game's main master; DLC availability comes from authored plugins.
    let base = if game.domain == "skyrimspecialedition" {
        game.base_plugins
    } else {
        &game.base_plugins[..1]
    };
    for name in base {
        context.installed.insert(name.to_ascii_lowercase());
        context.active.insert(name.to_ascii_lowercase());
    }
    for member in journal.members.values() {
        for file in &member.files {
            if file.deployment_root == "data" && !file.excluded {
                context.installed.insert(file.staged_path.to_lowercase());
                if !plan
                    .plugins
                    .iter()
                    .any(|p| !p.enabled && p.name.eq_ignore_ascii_case(&file.staged_path))
                {
                    context.active.insert(file.staged_path.to_lowercase());
                }
            }
        }
    }
    let work = (|| -> Result<()> {
        for (index, id) in plan.installation_order.iter().enumerate() {
            check_cancel(cancellation)?;
            let member_plan = plan
                .members
                .iter()
                .find(|m| &m.id == id && m.selected)
                .context("Invalid member in plan")?;
            super::progress::report(
                progress,
                "Installing",
                index,
                plan.installation_order.len(),
                &member_plan.name,
            );
            if journal.members.get(id).is_some_and(|member| {
                member.install_signature == member_plan.install_signature
                    && verify_member(&root, member).is_ok()
            }) {
                super::progress::report(
                    progress,
                    "Installing",
                    index + 1,
                    plan.installation_order.len(),
                    &member_plan.name,
                );
                continue;
            }
            let member = package
                .collection
                .mods
                .get(member_plan.source_index)
                .context("Missing source member")?;
            let temporary = tempfile::Builder::new()
                .prefix("member-")
                .tempdir_in(&root)?;
            eprintln!(
                "Staging {}/{}: {}",
                journal.members.len() + 1,
                plan.installation_order.len(),
                member.name
            );
            let scratch = tempfile::Builder::new()
                .prefix("extract-")
                .tempdir_in(&root)?;
            let mut logical = reproduce_member(
                package,
                plan,
                member_plan,
                member,
                artifacts,
                cancellation,
                scratch.path(),
                &context,
            )
            .with_context(|| format!("Reproduce member: {}", member.name))?;
            apply_patches(package, member, &mut logical, scratch.path(), cancellation)?;
            let mut outputs = Vec::new();
            let mut destinations = BTreeSet::new();
            let mut output_paths = super::paths::OutputPaths::new(temporary.path());
            for (path, source) in logical {
                check_cancel(cancellation)?;
                let (mut deployment_root, deployed) = route(&path, game);
                if member
                    .details
                    .get("type")
                    .and_then(serde_json::Value::as_str)
                    == Some("dinput")
                    && !path.to_lowercase().starts_with("data/")
                {
                    deployment_root = "game";
                }
                let staged = if deployment_root == "game" {
                    format!("Root/{deployed}")
                } else {
                    deployed.clone()
                };
                let staged = relative_path(&staged)?;
                if !destinations.insert(staged.to_lowercase()) {
                    bail!("Root/Data routing produces colliding outputs");
                }
                let target = output_paths.path(&staged)?;
                let staged = target
                    .strip_prefix(temporary.path())?
                    .to_str()
                    .context("Invalid staged output path")?
                    .to_owned();
                std::fs::create_dir_all(target.parent().context("Missing output parent")?)?;
                reflink_copy::reflink_or_copy(&source, &target)?;
                File::open(&target)?.sync_all()?;
                if deployment_root == "data" {
                    context.installed.insert(deployed.to_lowercase());
                    if !plan
                        .plugins
                        .iter()
                        .any(|p| !p.enabled && p.name.eq_ignore_ascii_case(&deployed))
                    {
                        context.active.insert(deployed.to_lowercase());
                    }
                }
                outputs.push(OutputFile {
                    logical_path: path.clone(),
                    staged_path: staged,
                    size: target.metadata()?.len(),
                    sha256: digest_file(&target)?,
                    deployment_root: deployment_root.into(),
                    excluded: member_plan
                        .excluded_paths
                        .iter()
                        .any(|excluded| excluded.eq_ignore_ascii_case(&path)),
                });
            }
            check_cancel(cancellation)?;
            let directory = temporary
                .path()
                .file_name()
                .unwrap()
                .to_str()
                .context("Invalid staging path")?
                .to_string();
            // Keep only after all required operations and syncs succeeded.
            let _ = temporary.keep();
            journal.members.insert(
                id.clone(),
                StagedMember {
                    member_id: id.clone(),
                    install_signature: member_plan.install_signature.clone(),
                    directory,
                    files: outputs,
                },
            );
            save_journal(&root, &journal)?;
        }
        check_cancel(cancellation)?;
        Ok(())
    })();
    if let Err(error) = work {
        journal.status = if cancellation.is_cancelled() {
            "cancelled"
        } else {
            "incomplete"
        }
        .into();
        save_journal(&root, &journal)?;
        return Err(error);
    }
    if let Err(error) = package.verify_unchanged() {
        journal.status = "incomplete".into();
        save_journal(&root, &journal)?;
        return Err(error);
    }
    journal.status = "staged".into();
    save_journal(&root, &journal)?;
    Ok(journal)
}

#[allow(clippy::too_many_arguments)] // Explicit job, payload and game contexts.
fn reproduce_member(
    package: &CollectionPackage,
    plan: &CollectionPlan,
    member_plan: &MemberPlan,
    member: &CollectionMod,
    artifacts: &BTreeMap<String, PathBuf>,
    cancellation: &CancellationToken,
    scratch: &Path,
    context: &super::fomod::InstallerContext,
) -> Result<super::fomod::Files> {
    let (payload, prefix);
    let owned;
    if member.source.source_type == "bundle" {
        payload = package;
        prefix = format!(
            "bundled/{}/",
            relative_path(&member.source.file_expression)?
        )
        .to_lowercase();
    } else {
        let artifact = plan
            .artifacts
            .iter()
            .find(|a| a.id == member_plan.artifact_id)
            .context("Missing planned artifact")?;
        let path = artifacts
            .get(&artifact.id)
            .context("Required artifact has not been supplied locally")?;
        if !path.is_file() || path.symlink_metadata()?.file_type().is_symlink() {
            bail!("Artifact must be a regular archive file");
        }
        if artifact
            .expected_size
            .is_some_and(|n| path.metadata().map(|m| m.len() != n).unwrap_or(true))
        {
            bail!("Artifact size mismatch");
        }
        if artifact.expected_md5.is_empty() {
            bail!("Exact local staging requires the manifest artifact MD5");
        }
        if md5_file(path, cancellation)? != artifact.expected_md5 {
            bail!("Artifact MD5 mismatch");
        }
        owned = CollectionPackage::open_payload(path)?;
        if owned.manifest_only {
            bail!("Unsupported artifact archive type");
        }
        payload = &owned;
        prefix = String::new();
    }
    let extracted = payload.extract_to(scratch, cancellation)?;
    let files: super::fomod::Files = extracted
        .into_iter()
        .filter(|(p, _)| p.to_lowercase().starts_with(&prefix))
        .map(|(p, v)| (p[prefix.len()..].to_owned(), v))
        .collect();
    if !member.hashes.is_empty() {
        let needed: BTreeSet<_> = member.hashes.iter().map(|h| h.md5.to_lowercase()).collect();
        let mut by_hash = BTreeMap::new();
        for source in files.values() {
            let hash = md5_file(source, cancellation)?;
            if needed.contains(&hash) {
                by_hash.entry(hash).or_insert(source);
            }
        }
        let mut output = BTreeMap::new();
        for file in &member.hashes {
            let path = by_hash
                .get(&file.md5.to_lowercase())
                .context("Required recorded output was not found in the source archive")?;
            output.insert(relative_path(&file.path)?, (*path).clone());
        }
        return Ok(output);
    }
    let has_fomod = files
        .keys()
        .any(|p| p.to_lowercase().ends_with("fomod/moduleconfig.xml"));
    if has_fomod {
        return super::fomod::replay(&files, &member.choices, context);
    }
    if !member.choices.is_null() {
        bail!("Recorded installer is missing from archive");
    }
    if files.keys().any(|p| {
        p.to_lowercase().ends_with("fomod/script.cs") || p.to_lowercase().ends_with("wizard.txt")
    }) {
        bail!("Installer interaction required: scripted installer");
    }
    raw_layout(files, super::games::require(&plan.domain)?)
}

fn raw_layout(
    files: super::fomod::Files,
    game: &super::games::GameProfile,
) -> Result<super::fomod::Files> {
    // Recognize a Data root before removing any wrappers. A single directory
    // alone is not evidence of a wrapper: framework DLLs/configs need their
    // containing folders, including folders we do not yet recognize. This
    // retains the positive root detection from the old Collections branch.
    const DATA_DIRS: &[&str] = &[
        "data",
        "root",
        "textures",
        "meshes",
        "scripts",
        "interface",
        "sound",
        "sounds",
        "music",
        "skse",
        "seq",
        "strings",
        "source",
        "calientetools",
        "nemesis_engine",
        "tools",
        "grass",
        "lodsettings",
        "video",
        "fonts",
        "dyndolod",
        "complexgrass",
        "enbseries",
        "reshade-shaders",
        "netscriptframework",
        "mcm",
        "dllplugins",
        "asi",
        "menus",
        "shaders",
        "trees",
        "facegen",
        "materials",
        "distantlod",
        "distantland",
        "mits",
        "shadersfx",
        "platform",
        "lightplacer",
        "mainmenuwallpapers",
        "mainmenuvideo",
        "pbrmaterialobjects",
        "pbrnifpatcher",
        "pbrtexturesets",
    ];
    // DLLs and INIs are deliberately not root signals: both commonly live
    // inside framework directories (e.g. NetScriptFramework/Plugins or MCM).
    const DATA_EXTENSIONS: &[&str] = &["esp", "esm", "esl", "bsa", "ba2"];
    let mut offset = 0;
    for _ in 0..=8 {
        let has_data_root = files.keys().any(|p| {
            let relative = &p[offset..];
            match relative.split_once('/') {
                Some((dir, _)) => {
                    DATA_DIRS.contains(&dir.to_ascii_lowercase().as_str())
                        || dir.eq_ignore_ascii_case(game.extender_directory)
                }
                None => {
                    (game.experimental && game.root_file(relative))
                        || relative.rsplit_once('.').is_some_and(|(_, ext)| {
                            DATA_EXTENSIONS.contains(&ext.to_ascii_lowercase().as_str())
                        })
                }
            }
        });
        if has_data_root {
            return Ok(files
                .into_iter()
                .map(|(p, v)| (p[offset..].into(), v))
                .collect());
        }
        let Some((prefix, _)) = files
            .keys()
            .next()
            .and_then(|p| p[offset..].split_once('/'))
        else {
            break;
        };
        if !files.keys().all(|p| {
            p[offset..]
                .split_once('/')
                .is_some_and(|(dir, _)| dir == prefix)
        }) {
            break;
        }
        offset += prefix.len() + 1;
    }
    // Vortex's basic FOMOD installer walks each directory's immediate entries
    // before descending into its children. It removes the first detected
    // prefix only from that subtree and retains sibling variants verbatim.
    // This matters for archives such as "No Fur/Meshes" + "Original/Meshes".
    // Generic INI/DLL files remain insufficient evidence of a Data root.
    fn find_prefix(paths: &[&str], base: &str, game: &super::games::GameProfile) -> Option<String> {
        let mut children = BTreeMap::new();
        for path in paths {
            let relative = path.strip_prefix(base)?;
            let (name, directory) = relative
                .split_once('/')
                .map_or((relative, false), |(name, _)| (name, true));
            children.insert(name.to_lowercase(), (name, directory));
        }
        for (lower, (_, directory)) in &children {
            if (*directory
                && (DATA_DIRS.contains(&lower.as_str()) || lower == game.extender_directory))
                || (!directory && game.experimental && game.root_file(lower))
                || (!directory
                    && lower
                        .rsplit_once('.')
                        .is_some_and(|(_, ext)| DATA_EXTENSIONS.contains(&ext)))
            {
                return Some(base.to_owned());
            }
        }
        for (_, (name, directory)) in children {
            if directory && !name.eq_ignore_ascii_case("__MACOSX") {
                let next = format!("{base}{name}/");
                let below: Vec<_> = paths
                    .iter()
                    .copied()
                    .filter(|p| p.starts_with(&next))
                    .collect();
                if let Some(found) = find_prefix(&below, &next, game) {
                    return Some(found);
                }
            }
        }
        None
    }
    let paths: Vec<_> = files.keys().map(String::as_str).collect();
    let Some(prefix) = find_prefix(&paths, "", game) else {
        return Ok(files);
    };
    let mut output = BTreeMap::new();
    for (path, source) in files {
        let destination = path.strip_prefix(&prefix).unwrap_or(&path).to_owned();
        if output.insert(destination, source).is_some() {
            bail!("Raw installer prefix produces colliding outputs");
        }
    }
    Ok(output)
}

pub(crate) fn md5_file(path: &Path, token: &CancellationToken) -> Result<String> {
    let mut input = File::open(path)?;
    let mut hash = md5::Context::new();
    let mut buffer = [0u8; 128 * 1024];
    loop {
        check_cancel(token)?;
        let n = input.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.consume(&buffer[..n]);
    }
    Ok(format!("{:x}", hash.compute()))
}

fn apply_patches(
    package: &CollectionPackage,
    member: &CollectionMod,
    files: &mut super::fomod::Files,
    scratch: &Path,
    cancellation: &CancellationToken,
) -> Result<()> {
    for (path, crc) in &member.patches {
        check_cancel(cancellation)?;
        let logical = relative_path(path)?;
        let key = files
            .keys()
            .find(|key| key.eq_ignore_ascii_case(&logical))
            .cloned()
            .context("Patch source is missing from reproduced member")?;
        let source_path = &files[&key];
        if source_path.metadata()?.len() > MAX_PAYLOAD_BYTES {
            bail!("Patch source exceeds 64 MiB limit");
        }
        let source = std::fs::read(source_path)?;
        if format!("{:08X}", crc32fast::hash(&source)).to_lowercase() != crc.to_lowercase() {
            bail!("Required patch source CRC32 mismatch");
        }
        let payload = format!("patches/{}/{}.diff", relative_path(&member.name)?, logical);
        let bytes = package.read(&payload, MAX_PAYLOAD_BYTES)?;
        let patcher = qbsdiff::Bspatch::new(&bytes).context("Invalid BSDIFF40 patch")?;
        if patcher.hint_target_size() > MAX_PAYLOAD_BYTES {
            bail!("BSDIFF target exceeds 64 MiB limit");
        }
        let mut writer = LimitedBuffer {
            bytes: Vec::new(),
            limit: MAX_PAYLOAD_BYTES as usize,
        };
        patcher
            .apply(&source, &mut writer)
            .context("Required BSDIFF patch failed")?;
        let target = scratch.join(format!("patched-{}", uuid::Uuid::new_v4()));
        std::fs::write(&target, writer.bytes)?;
        files.insert(key, target);
    }
    Ok(())
}

struct LimitedBuffer {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for LimitedBuffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other("Patch output limit exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn route(logical: &str, game: &super::games::GameProfile) -> (&'static str, String) {
    let lower = logical.to_lowercase();
    if lower.starts_with("data/") {
        return ("data", logical[5..].into());
    }
    if lower.starts_with("root/") {
        return ("game", logical[5..].into());
    }
    let root_file = !lower.contains('/')
        && (matches!(
            lower.as_str(),
            "d3dx9_42.dll" | "d3d11.dll" | "d3dcompiler_47.dll" | "engine_fixes.toml"
        ) || game.root_file(logical))
        || (game.experimental
            && !lower.contains('/')
            && matches!(
                lower.as_str(),
                "d3d9.dll" | "dxgi.dll" | "enblocal.ini" | "enbseries.ini"
            ))
        || lower.starts_with("enbseries/")
        || lower.starts_with("reshade-shaders/");
    if root_file {
        ("game", logical.into())
    } else {
        ("data", logical.into())
    }
}

pub(crate) fn verify_member(root: &Path, member: &StagedMember) -> Result<()> {
    let files = member_files(root, member)?;
    for file in &member.files {
        let path = &files[&super::paths::path_key(&file.staged_path)?];
        if path.metadata()?.len() != file.size || digest_file(path)? != file.sha256 {
            bail!("Staged output changed");
        }
    }
    Ok(())
}

/// Index one member once, retaining path/case/link checks without repeatedly
/// scanning every sibling mod directory for every payload file in a large list.
pub(crate) fn member_files(
    root: &Path,
    member: &StagedMember,
) -> Result<BTreeMap<String, PathBuf>> {
    let directory = super::paths::resolve_entry(root, &member.directory)?;
    if !directory.is_dir() {
        bail!("Staged member is not a directory");
    }
    let mut names = BTreeSet::new();
    let mut files = BTreeMap::new();
    for entry in walkdir::WalkDir::new(&directory)
        .min_depth(1)
        .follow_links(false)
    {
        let entry = entry?;
        if !(entry.file_type().is_file() || entry.file_type().is_dir()) {
            bail!("Staged member contains a link or special file");
        }
        let relative = entry
            .path()
            .strip_prefix(&directory)?
            .to_str()
            .context("Invalid staged filename")?;
        let key = super::paths::path_key(relative)?;
        if !names.insert(key.clone()) {
            bail!("Staged member contains a case collision");
        }
        if entry.file_type().is_file() {
            files.insert(key, entry.path().to_owned());
        }
    }
    if files.len() != member.files.len() {
        bail!("Staged member has missing or additional files");
    }
    let mut expected = BTreeSet::new();
    for file in &member.files {
        let key = super::paths::path_key(&file.staged_path)?;
        if !expected.insert(key.clone()) || !files.contains_key(&key) {
            bail!("Staged member has missing or additional files");
        }
    }
    Ok(files)
}

fn save_journal(root: &Path, journal: &StagingJournal) -> Result<()> {
    let mut tmp = tempfile::NamedTempFile::new_in(root)?;
    {
        let mut buffer = std::io::BufWriter::new(&mut tmp);
        serde_json::to_writer_pretty(&mut buffer, journal)?;
        buffer.flush()?;
    }
    tmp.as_file_mut().sync_all()?;
    tmp.persist(root.join(JOURNAL)).map_err(|e| e.error)?;
    File::open(root)?.sync_all()?;
    Ok(())
}
fn check_cancel(token: &CancellationToken) -> Result<()> {
    if token.is_cancelled() {
        bail!("Collection staging cancelled");
    }
    Ok(())
}

pub(crate) fn lock_job(root: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.create(true).truncate(false).read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options.open(root.join("collection-job.lock"))?;
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            bail!("Another process owns this staging job");
        }
    }
    Ok(file)
}
