//! Publish a verified job as a new isolated portable instance. Game-root files
//! are copied into a private Stock Game; the source Steam installation is read
//! only. Publication is an atomic directory rename after all checks succeed.
use super::{
    package::digest_file,
    paths::{relative_path, resolve_file},
    stage::{verify_member, StagingJournal},
};
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::Read,
    path::{Path, PathBuf},
};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicationReport {
    pub collection: String,
    pub revision: Option<u32>,
    pub package_sha256: String,
    pub output: PathBuf,
    pub installed_members: usize,
    pub enabled_plugins: usize,
    pub full_plugins: usize,
    pub light_plugins: usize,
    pub root_files: usize,
    pub downloaded_archives: usize,
    pub preserved_metadata_files: usize,
    pub profile_ini_edits: usize,
    pub source_game_version: Option<String>,
    pub runtime_patch_inputs_verified: usize,
    pub omitted_plugin_records: Vec<String>,
    pub added_plugin_records: Vec<String>,
    pub plugin_order_changes: usize,
    pub runtime_launch_tested: bool,
    pub plugin_sorter: String,
    pub loot_masterlist_sha256: Option<String>,
    #[serde(default)]
    pub verified_mod_files: usize,
}

#[derive(Default)]
pub struct PublicationOptions<'a> {
    pub profile_ini: Option<&'a Path>,
    pub masterlist: Option<&'a Path>,
    pub artifacts: Option<&'a Path>,
    pub job_identity: Option<&'a str>,
}

#[derive(Debug)]
struct Plugin {
    name: String,
    masters: Vec<String>,
    master: bool,
    light: bool,
    enabled: bool,
    rank: usize,
}

fn plugin_info(
    path: &Path,
    name: String,
    rank: usize,
    enabled: bool,
    game: &super::games::GameProfile,
) -> Result<Plugin> {
    let mut reader = File::open(path)?;
    let mut header = vec![0u8; game.plugin_header_size];
    reader
        .read_exact(&mut header)
        .context("Read TES4 plugin header")?;
    if &header[..4] != b"TES4" {
        bail!("Invalid plugin header: {name}");
    }
    let size = u32::from_le_bytes(header[4..8].try_into()?) as usize;
    let flags = u32::from_le_bytes(header[8..12].try_into()?);
    if size > 16 * 1024 * 1024 {
        bail!("Oversized plugin metadata: {name}");
    }
    let mut data = vec![0u8; size];
    reader.read_exact(&mut data)?;
    let mut offset = 0;
    let mut masters = Vec::new();
    let mut extended = None;
    while offset + 6 <= data.len() {
        let tag = &data[offset..offset + 4];
        let size = u16::from_le_bytes(data[offset + 4..offset + 6].try_into()?) as usize;
        offset += 6;
        let size = extended.take().unwrap_or(size);
        if offset + size > data.len() {
            bail!("Truncated plugin metadata: {name}");
        }
        if tag == b"XXXX" {
            if size != 4 {
                bail!("Invalid extended plugin subrecord");
            }
            extended = Some(u32::from_le_bytes(data[offset..offset + 4].try_into()?) as usize);
        } else if tag == b"MAST" {
            let bytes = data[offset..offset + size]
                .strip_suffix(&[0])
                .unwrap_or(&data[offset..offset + size]);
            let (text, _, errors) = encoding_rs::WINDOWS_1252.decode(bytes);
            if errors {
                bail!("Invalid plugin master name");
            }
            masters.push(relative_path(&text)?.to_lowercase());
        }
        offset += size;
    }
    let light = flags & 0x200 != 0 || name.to_lowercase().ends_with(".esl");
    if light && !game.light_plugins {
        bail!("{} does not support light plugins: {name}", game.name);
    }
    let master = flags & 1 != 0
        || name.to_lowercase().ends_with(".esm")
        || name.to_lowercase().ends_with(".esl");
    Ok(Plugin {
        name,
        masters,
        master,
        light,
        enabled,
        rank,
    })
}

fn is_plugin(path: &str) -> bool {
    let p = path.to_lowercase();
    !p.contains('/') && [".esm", ".esp", ".esl"].iter().any(|e| p.ends_with(e))
}

fn safe_name(name: &str, id: &str) -> String {
    let cleaned: String = name
        .chars()
        .map(|c| {
            if c.is_control() || "/\\:*?\"<>|".contains(c) {
                '_'
            } else {
                c
            }
        })
        .take(150)
        .collect();
    format!(
        "{} [{}]",
        cleaned.trim_matches(['.', ' ']),
        &id[id.len() - 8..]
    )
}

fn check(token: &CancellationToken) -> Result<()> {
    if token.is_cancelled() {
        bail!("Collection publication cancelled");
    }
    Ok(())
}

fn copy_file(source: &Path, destination: &Path) -> Result<()> {
    std::fs::create_dir_all(destination.parent().context("Missing output parent")?)?;
    let temporary = destination.with_file_name(format!(".copy-{}", uuid::Uuid::new_v4()));
    reflink_copy::reflink_or_copy(source, &temporary)?;
    std::fs::rename(temporary, destination)?;
    Ok(())
}

// Reuse existing component spelling for game-root replacements. A Linux game
// copy must never contain both data and Data, or two case variants of a DLL.
fn destination(root: &Path, path: &str) -> Result<PathBuf> {
    let mut output = root.to_path_buf();
    for component in relative_path(path)?.split('/') {
        let mut matches = Vec::new();
        if output.is_dir() {
            for entry in std::fs::read_dir(&output)? {
                let entry = entry?;
                if entry
                    .file_name()
                    .to_string_lossy()
                    .eq_ignore_ascii_case(component)
                {
                    matches.push(entry.path());
                }
            }
        }
        if matches.len() > 1 {
            bail!("Case-ambiguous publication path");
        }
        output = matches.pop().unwrap_or_else(|| output.join(component));
        if output
            .symlink_metadata()
            .is_ok_and(|m| m.file_type().is_symlink())
        {
            bail!("Publication path is a symlink");
        }
    }
    Ok(output)
}

pub fn publish(
    job: &Path,
    game: &Path,
    output: &Path,
    options: &PublicationOptions<'_>,
    token: &CancellationToken,
) -> Result<PublicationReport> {
    publish_with_progress(job, game, output, options, token, &|_| {})
}

pub fn publish_with_progress(
    job: &Path,
    game: &Path,
    output: &Path,
    options: &PublicationOptions<'_>,
    token: &CancellationToken,
    progress: &super::progress::Progress<'_>,
) -> Result<PublicationReport> {
    let PublicationOptions {
        profile_ini,
        masterlist,
        artifacts,
        job_identity,
    } = *options;
    if !output.is_absolute() || output.exists() {
        bail!("Publication requires a new absolute output directory");
    }
    let job = job.canonicalize()?;
    let _lock = super::stage::lock_job(&job)?;
    let game = game.canonicalize()?;
    if output.starts_with(&game) || output.starts_with(&job) {
        bail!("Publication must be outside game and staging directories");
    }
    let journal_path = resolve_file(&job, "collection-job.json")?;
    if journal_path.metadata()?.len() > super::stage::MAX_JOURNAL_BYTES {
        bail!("Oversized staging journal");
    }
    let mut journal: StagingJournal =
        serde_json::from_reader(std::io::BufReader::new(File::open(journal_path)?))?;
    if journal.status != "staged"
        || !journal.plan.blockers.is_empty()
        || journal.members.len() != journal.plan.installation_order.len()
    {
        bail!("Collection job is incomplete");
    }
    let game_profile = super::games::require(&journal.plan.domain)?;
    let game_exe = resolve_file(&game, game_profile.executable).with_context(|| {
        format!(
            "Game directory does not contain {}",
            game_profile.executable
        )
    })?;
    let source_game_version = executable_version(&game_exe)?;
    if journal
        .plan
        .game_version
        .as_ref()
        .is_some_and(|expected| source_game_version.as_ref() != Some(expected))
    {
        bail!("Actual source-game version differs from the planned runtime");
    }
    eprintln!(
        "Verifying {} staged members before publication",
        journal.members.len()
    );
    for (index, member) in journal.members.values().enumerate() {
        super::progress::report(
            progress,
            "Verifying staged files",
            index,
            journal.members.len(),
            &member.member_id,
        );
        check(token)?;
        verify_member(&job, member)?;
    }
    let parent = output.parent().context("Missing output parent")?;
    std::fs::create_dir_all(parent)?;
    let temporary = tempfile::Builder::new()
        .prefix(".collection-publish-")
        .tempdir_in(parent)?;
    let root = temporary.path();
    let stock = root.join("Stock Game");
    std::fs::create_dir(&stock)?;
    eprintln!("Creating an independent game copy");
    super::progress::report(
        progress,
        "Copying game",
        0,
        0,
        "Creating a private game copy",
    );
    for entry in walkdir::WalkDir::new(&game)
        .min_depth(1)
        .follow_links(false)
    {
        check(token)?;
        let entry = entry?;
        if entry.file_type().is_symlink()
            || !(entry.file_type().is_dir() || entry.file_type().is_file())
        {
            bail!("Source game contains a link or special file");
        }
        if entry.file_type().is_file() {
            let relative = relative_path(
                entry
                    .path()
                    .strip_prefix(&game)?
                    .to_str()
                    .context("Non-UTF8 game path")?,
            )?;
            copy_file(entry.path(), &stock.join(relative))?;
        }
    }
    let mut plugin_paths = BTreeMap::<String, (String, PathBuf)>::new();
    for entry in std::fs::read_dir(stock.join("Data"))? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if entry.file_type()?.is_file() && is_plugin(&name) {
            plugin_paths.insert(name.to_lowercase(), (name, entry.path()));
        }
    }
    let base_plugins: BTreeSet<_> = plugin_paths.keys().cloned().collect();
    let mut folders = BTreeMap::new();
    let mut root_files = 0;
    let mut preserved_metadata_files = 0;
    let members_by_id: BTreeMap<_, _> = journal
        .plan
        .members
        .iter()
        .map(|m| (m.id.clone(), m.clone()))
        .collect();
    eprintln!("Publishing mod folders and isolated root files");
    for (index, id) in journal.asset_order.iter().enumerate() {
        check(token)?;
        let plan_member = members_by_id.get(id).context("Missing planned member")?;
        super::progress::report(
            progress,
            "Publishing mods",
            index,
            journal.asset_order.len(),
            &plan_member.name,
        );
        let member = journal
            .members
            .get_mut(id)
            .context("Missing staged member")?;
        let folder = safe_name(&plan_member.name, id);
        let mod_path = root.join("mods").join(&folder);
        std::fs::create_dir_all(&mod_path)?;
        let source_files = super::stage::member_files(&job, member)?;
        let mut output_paths = super::paths::OutputPaths::new(&mod_path);
        for file in &mut member.files {
            check(token)?;
            let source = &source_files[&super::paths::path_key(&file.staged_path)?];
            if file.deployment_root == "data"
                && file.staged_path.eq_ignore_ascii_case("meta.ini")
                && !file.excluded
            {
                file.staged_path = format!(".clf3-source-meta-{}.ini.mohidden", file.sha256);
                file.deployment_root = "metadata".into();
                preserved_metadata_files += 1;
            }
            let target = output_paths.path(&file.staged_path)?;
            if target.exists() {
                bail!("Publication payload path collision");
            }
            if file.excluded {
                let hidden = target.with_file_name(format!(
                    "{}.mohidden",
                    target
                        .file_name()
                        .context("Missing output filename")?
                        .to_string_lossy()
                ));
                if hidden.exists() {
                    bail!("Hidden publication path collision");
                }
                copy_file(source, &hidden)?;
                continue;
            }
            copy_file(source, &target)?;
            if file.deployment_root == "game" {
                let relative = file
                    .staged_path
                    .strip_prefix("Root/")
                    .context("Invalid root staging path")?;
                let deployed = destination(&stock, relative)?;
                copy_file(source, &deployed)?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let mut magic = [0u8; 4];
                    if File::open(&deployed)?.read(&mut magic)? == 4 && &magic == b"\x7fELF" {
                        std::fs::set_permissions(
                            &deployed,
                            std::fs::Permissions::from_mode(0o755),
                        )?;
                    }
                }
                if digest_file(&deployed)? != file.sha256 {
                    bail!("Root publication verification failed");
                }
                root_files += 1;
            } else if is_plugin(&file.staged_path) {
                plugin_paths.insert(
                    file.staged_path.to_lowercase(),
                    (file.staged_path.clone(), target),
                );
            }
        }
        let artifact = journal
            .plan
            .artifacts
            .iter()
            .find(|a| a.id == plan_member.artifact_id)
            .context("Missing artifact identity")?;
        let version = serde_json::to_string(&plan_member.version)?;
        std::fs::write(mod_path.join("meta.ini"), format!("[General]\ngameName={}\nmodid={}\nversion={version}\ninstallationFile={}\n[installedFiles]\n1\\modid={}\n1\\fileid={}\nsize=1\n", game_profile.manager_name(), artifact.mod_id, artifact.expected_md5, artifact.mod_id, artifact.file_id))?;
        folders.insert(id.clone(), folder);
        member.directory = format!("mods/{}", folders[id]);
    }
    let runtime_patch_inputs_verified = if game_profile.domain == "skyrimspecialedition" {
        verify_runtime_swap_inputs(&stock, token)?
    } else {
        0
    };
    let mut declared = BTreeMap::new();
    let mut omitted = BTreeSet::new();
    for (i, p) in journal.plan.plugins.iter().enumerate() {
        let key = p.name.to_lowercase();
        if !plugin_paths.contains_key(&key) {
            if p.enabled {
                bail!(
                    "Authored enabled plugin is missing from the reproduced collection: {}",
                    p.name
                );
            }
            omitted.insert(p.name.clone());
            continue;
        }
        if let Some((_, enabled)) = declared.get(&key) {
            if *enabled != p.enabled {
                bail!("Conflicting recorded plugin enablement: {}", p.name);
            }
        } else {
            declared.insert(key, (i + 1000, p.enabled));
        }
    }
    let mut base_order: Vec<_> = game_profile
        .base_plugins
        .iter()
        .map(|p| p.to_ascii_lowercase())
        .collect();
    if let Some(ccc) = game_profile
        .creation_club_file
        .and_then(|name| resolve_file(&stock, name).ok())
        .and_then(|path| std::fs::read_to_string(path).ok())
    {
        base_order.extend(
            ccc.lines()
                .map(|l| l.trim().to_lowercase())
                .filter(|l| !l.is_empty()),
        );
    }
    let mut plugins = BTreeMap::new();
    let mut added = Vec::new();
    let resolved_paths: Vec<PathBuf> = plugin_paths.values().map(|(_, p)| p.clone()).collect();
    let published_plugin_paths = plugin_paths.clone();
    for (key, (name, path)) in plugin_paths {
        let (rank, enabled) = if base_plugins.contains(&key) {
            (
                base_order.iter().position(|p| p == &key).unwrap_or(900),
                declared.get(&key).is_none_or(|(_, enabled)| *enabled),
            )
        } else if let Some(v) = declared.get(&key) {
            *v
        } else {
            added.push(name.clone());
            // Vortex exports enabled mod plugins, then disables installed
            // plugins absent from that list during collection postprocessing.
            (journal.plan.plugins.len() + 1000 + added.len(), false)
        };
        plugins.insert(key, plugin_info(&path, name, rank, enabled, game_profile)?);
    }
    let (mut order, mut changes) = sort_plugins(
        &plugins,
        &journal.plan.plugin_rules,
        journal.plan.exclude_plugin_rules || masterlist.is_some(),
    )?;
    if let Some(masterlist) = masterlist {
        eprintln!("Sorting plugins with LOOT and the collection's custom rules");
        let original = order.clone();
        order = loot_sort(
            root,
            &stock,
            masterlist,
            &journal,
            &folders,
            &plugins,
            &resolved_paths,
            &order,
        )?;
        changes = order.iter().zip(&original).filter(|(a, b)| a != b).count();
    }
    let full = plugins.values().filter(|p| p.enabled && !p.light).count();
    let light = plugins.values().filter(|p| p.enabled && p.light).count();
    if full > game_profile.max_full_plugins() || light > 4096 {
        bail!("Plugin limits exceeded: {full} full, {light} light");
    }
    let profile = root.join("profiles/Default");
    std::fs::create_dir_all(profile.join("saves"))?;
    for directory in ["downloads", "overwrite", ".collection"] {
        std::fs::create_dir_all(root.join(directory))?;
    }
    // Oblivion / Fallout 3 / New Vegas actually order by file modification
    // times. Stamp only the new private copies, never the source or stage.
    if game_profile.timestamp_order {
        let mut timestamps = BTreeMap::new();
        for (index, key) in order.iter().enumerate() {
            let path = &published_plugin_paths[key].1;
            let seconds = 1_577_836_800 + index as u64 * 60;
            File::open(path)?
                .set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(seconds))?;
            timestamps.insert(
                relative_path(
                    path.strip_prefix(root)?
                        .to_str()
                        .context("Invalid plugin path")?,
                )?,
                seconds,
            );
        }
        std::fs::write(
            root.join(".collection/plugin-timestamps.json"),
            serde_json::to_vec_pretty(&timestamps)?,
        )?;
    }
    let mut downloaded_archives = 0;
    if let Some(mapping) = artifacts {
        if mapping.metadata()?.len() > 16 * 1024 * 1024 {
            bail!("Oversized artifact mapping");
        }
        let mapping: BTreeMap<String, PathBuf> = serde_json::from_reader(File::open(mapping)?)?;
        let mut published_mapping = BTreeMap::new();
        eprintln!("Retaining verified download archives in the new instance");
        for (index, artifact) in journal.plan.artifacts.iter().enumerate() {
            super::progress::report(
                progress,
                "Retaining archives",
                index,
                journal.plan.artifacts.len(),
                &format!("File {}", artifact.file_id),
            );
            check(token)?;
            if artifact.source_type == "bundle" {
                continue;
            }
            let source = mapping
                .get(&artifact.id)
                .context("Required archive missing from publication map")?;
            if !source.symlink_metadata()?.is_file() {
                bail!("Archive must be a regular file");
            }
            if artifact
                .expected_size
                .is_some_and(|s| source.metadata().map(|m| m.len() != s).unwrap_or(true))
                || super::stage::md5_file(source, token)? != artifact.expected_md5
            {
                bail!("Downloaded archive verification failed during publication");
            }
            let mut magic = [0u8; 6];
            File::open(source)?.read_exact(&mut magic)?;
            let extension = if &magic[..2] == b"PK" {
                "zip"
            } else if &magic[..4] == b"Rar!" {
                "rar"
            } else if magic == [0x37, 0x7a, 0xbc, 0xaf, 0x27, 0x1c] {
                "7z"
            } else {
                bail!("Unsupported download archive format")
            };
            let member = journal
                .plan
                .members
                .iter()
                .find(|m| m.artifact_id == artifact.id)
                .context("Missing artifact member")?;
            let relative = format!(
                "downloads/{}.{}",
                safe_name(&member.name, &artifact.id),
                extension
            );
            copy_file(source, &root.join(&relative))?;
            if super::stage::md5_file(&root.join(&relative), token)? != artifact.expected_md5 {
                bail!("Retained archive verification failed");
            }
            let archive_name = std::path::Path::new(&relative)
                .file_name()
                .and_then(|n| n.to_str())
                .context("Invalid retained archive filename")?;
            let installation_file =
                format!("installationFile={}", serde_json::to_string(archive_name)?);
            for installed in journal
                .plan
                .members
                .iter()
                .filter(|m| m.selected && m.artifact_id == artifact.id)
            {
                let meta_path = root
                    .join("mods")
                    .join(&folders[&installed.id])
                    .join("meta.ini");
                let meta = std::fs::read_to_string(&meta_path)?;
                let updated = meta
                    .lines()
                    .map(|line| {
                        if line.starts_with("installationFile=") {
                            installation_file.as_str()
                        } else {
                            line
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
                    + "\n";
                std::fs::write(meta_path, updated)?;
            }
            published_mapping.insert(&artifact.id, output.join(relative));
            downloaded_archives += 1;
        }
        std::fs::write(
            root.join(".collection/artifacts.json"),
            serde_json::to_vec_pretty(&published_mapping)?,
        )?;
    }
    let modlist = journal
        .asset_order
        .iter()
        .rev()
        .map(|id| format!("+{}\n", folders[id]))
        .collect::<String>();
    for name in ["Collection Mods_separator", "Mod Additions_separator"] {
        std::fs::create_dir(root.join("mods").join(name))?;
    }
    std::fs::write(
        profile.join("modlist.txt"),
        format!("# Generated by CLF3 Collections\n-Mod Additions_separator\n{modlist}-Collection Mods_separator\n"),
    )?;
    let mut pluginlist = b"# Generated by CLF3 Collections\n".to_vec();
    pluginlist.extend(
        game_profile.plugin_list(
            order
                .iter()
                .map(|k| (plugins[k].name.as_str(), plugins[k].enabled)),
        )?,
    );
    std::fs::write(profile.join("plugins.txt"), pluginlist)?;
    std::fs::write(
        profile.join("loadorder.txt"),
        order
            .iter()
            .map(|k| format!("{}\n", plugins[k].name))
            .collect::<String>(),
    )?;
    std::fs::write(
        profile.join("settings.ini"),
        "[General]\nLocalSaves=true\nLocalSettings=true\n",
    )?;
    if let Some(source) = profile_ini {
        for name in game_profile.ini_files {
            let candidate =
                super::paths::resolve_entry(source, name).unwrap_or_else(|_| source.join(name));
            if candidate
                .symlink_metadata()
                .is_ok_and(|m| m.file_type().is_symlink())
            {
                eprintln!("Skipping linked {name}; keeping the new profile independent");
            } else if candidate.is_file() {
                copy_file(&resolve_file(source, name)?, &profile.join(name))?;
            }
        }
    }
    for name in game_profile
        .ini_files
        .iter()
        .filter(|n| n.ends_with("Custom.ini"))
    {
        if !profile.join(name).exists() {
            std::fs::write(profile.join(name), "; Profile-specific overrides\n")?;
        }
    }
    if !profile.join(game_profile.ini_files[0]).exists() {
        copy_file(
            &resolve_file(&stock, game_profile.default_ini)
                .context("Source game default INI is missing; provide this game's profile INIs")?,
            &profile.join(game_profile.ini_files[0]),
        )?;
    }
    let profile_ini_edits = super::ini::apply(
        &profile,
        &root.join(".collection/ini-tweaks"),
        &journal.plan.ini_tweaks,
        &journal.plan.domain,
    )?;
    let out = output.to_str().context("Non-UTF8 instance path")?;
    if journal.plan.launcher_name.is_some() {
        resolve_file(&stock, game_profile.extender)
            .context("Collection's script-extender launcher is missing")?;
    }
    // QSettings uses backslash escaping even for Unix path-valued strings.
    if out.contains(['\n', '\r', '\\', '"']) {
        bail!("Unsupported instance INI path");
    }
    let has_extender = resolve_file(&stock, game_profile.extender).is_ok();
    let launcher_binary = if has_extender {
        game_profile.extender
    } else {
        game_profile.executable
    };
    let launcher = journal
        .plan
        .launcher_name
        .as_deref()
        .unwrap_or(if has_extender {
            game_profile.extender_name
        } else {
            game_profile.name
        });
    let steam_id = game_profile.steam_id_for_path(&game);
    let game_name = game_profile.manager_name();
    let ini = format!("[General]\ngameName={game_name}\ngamePath={out}/Stock Game\ngameEdition=Steam\nselected_profile=Default\n[Settings]\nbase_directory={out}\nmod_directory={out}/mods\nprofiles_directory={out}/profiles\ndownload_directory={out}/downloads\noverwrite_directory={out}/overwrite\n[customExecutables]\nsize=1\n1\\title={launcher}\n1\\binary={out}/Stock Game/{launcher_binary}\n1\\workingDirectory={out}/Stock Game\n1\\arguments=\n1\\steamAppID={steam_id}\n1\\ownicon=true\n1\\toolbar=true\n");
    std::fs::write(root.join("ModOrganizer.ini"), ini)?;
    std::fs::write(root.join("portable.txt"), "")?;
    let snapshot = root.join(".collection/profile-snapshot");
    std::fs::create_dir(&snapshot)?;
    for name in game_profile.snapshot_files() {
        if profile.join(name).is_file() {
            copy_file(&profile.join(name), &snapshot.join(name))?;
        }
    }
    // Verify the private destination before its atomic publication. This also
    // catches damaged copies rather than relying only on staging verification.
    let verified_mod_files = verify_published_payloads(root, &journal, token, progress)?;
    journal.status = "published".into();
    let report = PublicationReport {
        collection: journal.plan.name.clone(),
        revision: journal.plan.locator.as_ref().and_then(|l| l.revision),
        package_sha256: journal.package_sha256.clone(),
        output: output.into(),
        installed_members: journal.members.len(),
        enabled_plugins: full + light,
        full_plugins: full,
        light_plugins: light,
        root_files,
        downloaded_archives,
        preserved_metadata_files,
        profile_ini_edits,
        source_game_version,
        runtime_patch_inputs_verified,
        omitted_plugin_records: omitted.into_iter().collect(),
        added_plugin_records: added,
        plugin_order_changes: changes,
        runtime_launch_tested: false,
        plugin_sorter: if masterlist.is_some() {
            "libloot 0.29.6 + collection rules".into()
        } else {
            "authored order + masters + collection rules".into()
        },
        loot_masterlist_sha256: masterlist.map(digest_file).transpose()?,
        verified_mod_files,
    };
    std::fs::write(
        root.join(".collection/installation.json"),
        serde_json::to_vec_pretty(&journal)?,
    )?;
    std::fs::write(
        root.join(".collection/report.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    if let Some(identity) = job_identity {
        std::fs::write(
            root.join(".collection/gui-job.json"),
            serde_json::to_vec(
                &serde_json::json!({"job_identity":identity,"plan_sha256":journal.plan_sha256}),
            )?,
        )?;
    }
    let swap_note = if runtime_patch_inputs_verified > 0 {
        "The included runtime swapper runs before SKSE in this private game copy. Follow its Proton instructions; the version DLL must be loaded. "
    } else {
        ""
    };
    std::fs::write(root.join(".collection/launch-notes.txt"), format!("This portable installation includes the {launcher} launcher configuration. Its game path is the isolated Stock Game directory. {swap_note}The original Steam installation was not modified. Launching the game has not been tested. In the ascending Priority view, Collection Mods contains the imported list and Mod Additions is reserved for later additions.\n"))?;
    check(token)?;
    if output.exists() {
        bail!("Output appeared during publication");
    }
    // A destination created between validation and commit must never be replaced.
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::ffi::OsStrExt;
        let source = std::ffi::CString::new(root.as_os_str().as_bytes())?;
        let target = std::ffi::CString::new(output.as_os_str().as_bytes())?;
        if unsafe {
            libc::renameat2(
                libc::AT_FDCWD,
                source.as_ptr(),
                libc::AT_FDCWD,
                target.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        } != 0
        {
            return Err(std::io::Error::last_os_error()).context(
                "Publish completed portable instance without replacing an existing destination",
            );
        }
    }
    #[cfg(not(target_os = "linux"))]
    std::fs::rename(root, output).context("Publish completed portable instance")?;
    File::open(parent)?.sync_all()?;
    Ok(report)
}

pub fn executable_version(path: &Path) -> Result<Option<String>> {
    if path.metadata()?.len() > 128 * 1024 * 1024 {
        bail!("Oversized game executable");
    }
    let bytes = std::fs::read(path)?;
    let marker = [0xbd, 0x04, 0xef, 0xfe, 0, 0, 1, 0];
    let Some(offset) = bytes.windows(marker.len()).position(|w| w == marker) else {
        return Ok(None);
    };
    let data = bytes
        .get(offset + 8..offset + 16)
        .context("Truncated executable version resource")?;
    let ms = u32::from_le_bytes(data[..4].try_into()?);
    let ls = u32::from_le_bytes(data[4..].try_into()?);
    Ok(Some(format!(
        "{}.{}.{}.{}",
        ms >> 16,
        ms & 0xffff,
        ls >> 16,
        ls & 0xffff
    )))
}

fn verify_runtime_swap_inputs(stock: &Path, token: &CancellationToken) -> Result<usize> {
    let Ok(manifest) = resolve_file(stock, "RuntimeSwap/manifest.json") else {
        return Ok(0);
    };
    if manifest.metadata()?.len() > 16 * 1024 * 1024 {
        bail!("Oversized runtime swap manifest");
    }
    let manifest: serde_json::Value = serde_json::from_reader(File::open(manifest)?)?;
    if manifest["format"] != 3 || manifest["gameId"] != "skyrimse" {
        bail!("Unsupported runtime swap manifest");
    }
    let files = manifest["files"]
        .as_array()
        .context("Invalid runtime swap file list")?;
    for entry in files {
        check(token)?;
        let relative = relative_path(
            entry["path"]
                .as_str()
                .context("Missing runtime patch path")?,
        )?;
        if entry["sourcePresent"] != true {
            bail!("Unsupported absent-source runtime patch");
        }
        let source = resolve_file(stock, &relative)?;
        if Some(source.metadata()?.len()) != entry["sourceSize"].as_u64()
            || Some(digest_file(&source)?.as_str()) != entry["sourceSha256"].as_str()
        {
            bail!("Runtime swap source does not match its required hash: {relative}");
        }
    }
    Ok(files.len())
}

fn sort_plugins(
    plugins: &BTreeMap<String, Plugin>,
    rules: &serde_json::Value,
    excluded: bool,
) -> Result<(Vec<String>, usize)> {
    let mut outgoing: BTreeMap<String, BTreeSet<String>> = plugins
        .keys()
        .map(|k| (k.clone(), BTreeSet::new()))
        .collect();
    let mut incoming: BTreeMap<String, usize> = plugins.keys().map(|k| (k.clone(), 0)).collect();
    let mut edge = |a: &str, b: &str| {
        if a != b && outgoing.get_mut(a).is_some_and(|set| set.insert(b.into())) {
            *incoming.get_mut(b).unwrap() += 1;
        }
    };
    for (name, p) in plugins {
        for master in &p.masters {
            let Some(required) = plugins.get(master) else {
                if p.enabled {
                    bail!("{} requires missing master {}", p.name, master);
                }
                continue;
            };
            if p.enabled && !required.enabled {
                bail!("{} requires disabled master {}", p.name, master);
            }
            edge(master, name);
        }
    }
    for (a, _) in plugins.iter().filter(|(_, p)| p.master) {
        for (b, _) in plugins.iter().filter(|(_, p)| !p.master) {
            edge(a, b);
        }
    }
    if !excluded && !rules.is_null() {
        if rules
            .get("groups")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|g| !g.is_empty())
        {
            bail!("Custom LOOT groups require LOOT integration");
        }
        if let Some(entries) = rules.get("plugins").and_then(serde_json::Value::as_array) {
            for rule in entries {
                let name = rule
                    .get("name")
                    .and_then(serde_json::Value::as_str)
                    .context("Invalid plugin rule")?
                    .to_lowercase();
                if !plugins.contains_key(&name) {
                    continue;
                }
                for field in rule.as_object().context("Invalid plugin rule")?.keys() {
                    if !matches!(field.as_str(), "name" | "after") {
                        bail!("Unsupported plugin rule field: {field}");
                    }
                }
                for after in rule
                    .get("after")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    let after = after
                        .as_str()
                        .context("Conditional plugin rules require LOOT")?
                        .to_lowercase();
                    if plugins.contains_key(&after) {
                        edge(&after, &name);
                    }
                }
            }
        }
    }
    let mut ready: BTreeSet<_> = plugins
        .iter()
        .filter(|(k, _)| incoming[*k] == 0)
        .map(|(k, p)| (p.rank, k.clone()))
        .collect();
    let mut sorted = Vec::new();
    while let Some((_, name)) = ready.pop_first() {
        sorted.push(name.clone());
        for target in &outgoing[&name] {
            let count = incoming.get_mut(target).unwrap();
            *count -= 1;
            if *count == 0 {
                ready.insert((plugins[target].rank, target.clone()));
            }
        }
    }
    if sorted.len() != plugins.len() {
        bail!("Plugin master/order rules contain a cycle");
    }
    let mut declared: Vec<_> = plugins.iter().map(|(k, p)| (p.rank, k.clone())).collect();
    declared.sort();
    let changes = sorted
        .iter()
        .zip(declared.iter())
        .filter(|(a, (_, b))| *a != b)
        .count();
    Ok((sorted, changes))
}

#[allow(clippy::too_many_arguments)] // Explicit job, payload and game contexts.
fn loot_sort(
    root: &Path,
    stock: &Path,
    masterlist: &Path,
    journal: &StagingJournal,
    folders: &BTreeMap<String, String>,
    plugins: &BTreeMap<String, Plugin>,
    paths: &[PathBuf],
    order: &[String],
) -> Result<Vec<String>> {
    let local = root.join(".collection/loot");
    std::fs::create_dir_all(&local)?;
    copy_file(masterlist, &local.join("masterlist.yaml"))?;
    let userlist = if journal.plan.exclude_plugin_rules || journal.plan.plugin_rules.is_null() {
        serde_json::json!({"plugins":[]})
    } else {
        journal.plan.plugin_rules.clone()
    };
    std::fs::write(
        local.join("userlist.yaml"),
        serde_json::to_vec_pretty(&userlist)?,
    )?;
    let game_profile = super::games::require(&journal.plan.domain)?;
    std::fs::write(
        local.join("plugins.txt"),
        game_profile.plugin_list(
            order
                .iter()
                .map(|k| (plugins[k].name.as_str(), plugins[k].enabled)),
        )?,
    )?;
    std::fs::write(
        local.join("loadorder.txt"),
        order
            .iter()
            .map(|k| format!("{}\n", plugins[k].name))
            .collect::<String>(),
    )?;
    let mut game = libloot::Game::with_local_path(game_profile.loot, stock, &local)
        .context("Create isolated LOOT game context")?;
    let data_paths = journal
        .asset_order
        .iter()
        .rev()
        .map(|id| root.join("mods").join(&folders[id]))
        .collect();
    game.set_additional_data_paths(data_paths)?;
    game.load_current_load_order_state()
        .context("Load isolated plugin activation state")?;
    {
        let database = game.database();
        let mut db = database
            .write()
            .map_err(|_| anyhow::anyhow!("LOOT database lock poisoned"))?;
        db.load_masterlist(&local.join("masterlist.yaml"))
            .context("Load LOOT masterlist")?;
        db.load_userlist(&local.join("userlist.yaml"))
            .context("Load collection plugin rules")?;
    }
    let paths: Vec<_> = paths.iter().map(PathBuf::as_path).collect();
    game.load_plugins(&paths)
        .context("Load plugins for LOOT sorting")?;
    let names: Vec<_> = order.iter().map(|k| plugins[k].name.as_str()).collect();
    let sorted: Vec<_> = game
        .sort_plugins(&names)
        .context("LOOT sorting failed")?
        .into_iter()
        .map(|n| n.to_lowercase())
        .collect();
    if sorted.iter().collect::<BTreeSet<_>>() != plugins.keys().collect::<BTreeSet<_>>()
        || sorted.len() != plugins.len()
    {
        bail!("LOOT result did not preserve the plugin set");
    }
    Ok(sorted)
}

/// Verify publication payloads again when a host recovers after a disconnect.
pub fn verify_published_payloads(
    root: &Path,
    journal: &StagingJournal,
    token: &CancellationToken,
    progress: &super::progress::Progress<'_>,
) -> Result<usize> {
    for path in [root.to_path_buf(), root.join("mods")] {
        let metadata = path.symlink_metadata()?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            bail!("Invalid published mod root");
        }
    }
    let total_files = journal.members.values().map(|m| m.files.len()).sum();
    let mut verified_mod_files = 0;
    for member in journal.members.values() {
        let relative = relative_path(&member.directory)?;
        if relative.split('/').count() != 2 || !relative.starts_with("mods/") {
            bail!("Invalid published member directory");
        }
        let directory = root.join(&relative);
        let mut expected = BTreeSet::from(["meta.ini".to_owned()]);
        for file in &member.files {
            let path = format!(
                "{}{}",
                file.staged_path,
                if file.excluded { ".mohidden" } else { "" }
            );
            expected.insert(super::paths::path_key(&path)?);
        }
        for entry in walkdir::WalkDir::new(&directory).follow_links(false) {
            check(token)?;
            let entry = entry?;
            if entry.file_type().is_symlink()
                || !(entry.file_type().is_dir() || entry.file_type().is_file())
            {
                bail!("Published member contains a link or special file");
            }
            if entry.file_type().is_file() {
                let path = entry
                    .path()
                    .strip_prefix(&directory)?
                    .to_str()
                    .context("Invalid published file path")?;
                if !expected.remove(&super::paths::path_key(path)?) {
                    bail!("Published member contains an unexpected file");
                }
            }
        }
        if !expected.is_empty() {
            bail!("Published member is missing a file");
        }
        super::progress::report(
            progress,
            "Verifying installed files",
            verified_mod_files,
            total_files,
            &member.member_id,
        );
        for file in &member.files {
            check(token)?;
            let relative = if file.excluded {
                format!("{}.mohidden", file.staged_path)
            } else {
                file.staged_path.clone()
            };
            let path = root
                .join(relative_path(&member.directory)?)
                .join(relative_path(&relative)?);
            if !path.symlink_metadata()?.is_file()
                || path.metadata()?.len() != file.size
                || digest_file(&path)? != file.sha256
            {
                bail!(
                    "Installed payload verification failed for member {}",
                    member.member_id
                );
            }
            verified_mod_files += 1;
        }
    }
    super::progress::report(
        progress,
        "Verifying installed files",
        verified_mod_files,
        total_files,
        "All installed payloads verified",
    );
    Ok(verified_mod_files)
}
