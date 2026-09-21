//! FOMOD replay with hierarchical recorded choices and final-profile context.
//!
//! Reviewed against the old CLF3 parser/executor and Amethyst's dependency and
//! three-phase install behavior. This executor is independently implemented;
//! the BOM decoder is ported from CLF3's vortex-collections branch.
use super::{paths::relative_path, xml_encoding::read_xml_with_encoding};
use anyhow::{bail, Context, Result};
use roxmltree::Node;
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

pub type Files = BTreeMap<String, PathBuf>;

#[derive(Default)]
pub struct InstallerContext {
    pub installed: BTreeSet<String>,
    pub active: BTreeSet<String>,
    pub game_version: String,
}

fn named(node: Node<'_, '_>, name: &str) -> bool {
    node.is_element() && node.tag_name().name().eq_ignore_ascii_case(name)
}
fn child<'a, 'i>(node: Node<'a, 'i>, name: &str) -> Option<Node<'a, 'i>> {
    node.children().find(|n| named(*n, name))
}
fn children<'a, 'i>(node: Node<'a, 'i>, name: &str) -> Vec<Node<'a, 'i>> {
    node.children().filter(|n| named(*n, name)).collect()
}
fn ordered<'a, 'i>(node: Node<'a, 'i>, name: &str) -> Vec<Node<'a, 'i>> {
    let mut list = children(node, name);
    let order = node.attribute("order").unwrap_or("Explicit");
    if order.eq_ignore_ascii_case("Ascending") || order.eq_ignore_ascii_case("Descending") {
        list.sort_by_key(|n| n.attribute("name").unwrap_or("").to_lowercase());
        if order.eq_ignore_ascii_case("Descending") {
            list.reverse();
        }
    }
    list
}

fn version_parts(v: &str) -> Vec<u64> {
    v.split('.')
        .map(|s| s.parse().unwrap_or(0))
        .chain(std::iter::repeat(0))
        .take(4)
        .collect()
}
fn dependency(
    node: Node<'_, '_>,
    flags: &BTreeMap<String, String>,
    context: &InstallerContext,
) -> Result<bool> {
    let tag = node.tag_name().name().to_ascii_lowercase();
    let attr = |key| node.attribute(key).unwrap_or("");
    Ok(match tag.as_str() {
        "dependencies" | "visible" | "moduledependencies" => {
            let values: Vec<bool> = node
                .children()
                .filter(Node::is_element)
                .map(|n| dependency(n, flags, context))
                .collect::<Result<_>>()?;
            if attr("operator").eq_ignore_ascii_case("Or") {
                values.into_iter().any(|v| v)
            } else {
                values.into_iter().all(|v| v)
            }
        }
        "flagdependency" => {
            flags.get(attr("flag")).map(String::as_str).unwrap_or("") == attr("value")
        }
        "filedependency" => {
            let path = relative_path(attr("file"))?.to_lowercase();
            let present = context.installed.contains(&path) || context.active.contains(&path);
            match attr("state").to_ascii_lowercase().as_str() {
                "active" => context.active.contains(&path),
                "inactive" => present && !context.active.contains(&path),
                "missing" => !present,
                _ => bail!("Unknown FOMOD file dependency state"),
            }
        }
        "gamedependency" => {
            if context.game_version.is_empty() {
                bail!("FOMOD requires a game version");
            }
            version_parts(&context.game_version) >= version_parts(attr("version"))
        }
        // These specify installer capabilities, not a game executable to run.
        "fommdependency" => true,
        _ => bail!("Unsupported FOMOD dependency: {tag}"),
    })
}

fn plugin_type<'a>(
    plugin: Node<'a, '_>,
    flags: &BTreeMap<String, String>,
    context: &InstallerContext,
) -> Result<&'a str> {
    let Some(desc) = child(plugin, "typeDescriptor") else {
        return Ok("Optional");
    };
    if let Some(simple) = child(desc, "type") {
        return Ok(simple.attribute("name").unwrap_or("Optional"));
    }
    if let Some(dep) = child(desc, "dependencyType") {
        if let Some(patterns) = child(dep, "patterns") {
            for pattern in children(patterns, "pattern") {
                if child(pattern, "dependencies")
                    .map(|n| dependency(n, flags, context))
                    .transpose()?
                    .unwrap_or(true)
                {
                    return Ok(child(pattern, "type")
                        .and_then(|n| n.attribute("name"))
                        .unwrap_or("Optional"));
                }
            }
        }
        return Ok(child(dep, "defaultType")
            .and_then(|n| n.attribute("name"))
            .unwrap_or("Optional"));
    }
    Ok("Optional")
}

fn normalized(input: &str, allow_empty: bool) -> Result<String> {
    let p = input.replace('\\', "/");
    let p = p.trim_end_matches('/').trim_start_matches("./");
    if allow_empty && (p.is_empty() || p == ".") {
        return Ok(String::new());
    }
    relative_path(p).with_context(|| format!("Invalid FOMOD relative path: {input:?}"))
}

fn install(
    files: &Files,
    root: &str,
    instruction: Node<'_, '_>,
    outputs: &mut Files,
    archive_root: &std::path::Path,
) -> Result<()> {
    let folder = named(instruction, "folder");
    let source = normalized(instruction.attribute("source").unwrap_or(""), folder)?;
    let destination = normalized(instruction.attribute("destination").unwrap_or(""), true)?;
    let source_key = format!("{root}{source}").to_lowercase();
    let prefix = if source_key.is_empty() {
        String::new()
    } else {
        format!("{source_key}/")
    };
    let mut count = 0;
    for (name, path) in files {
        let lower = name.to_lowercase();
        let tail = if folder && lower.starts_with(&prefix) {
            &name[prefix.len()..]
        } else if !folder && lower == source_key {
            ""
        } else {
            continue;
        };
        if folder
            && (tail.to_lowercase().starts_with("fomod/") || tail.eq_ignore_ascii_case("fomod"))
        {
            continue;
        }
        let target = if folder {
            if destination.is_empty() {
                tail.into()
            } else {
                format!("{destination}/{tail}")
            }
        } else if destination.is_empty()
            || instruction
                .attribute("destination")
                .is_some_and(|d| d.ends_with(['/', '\\']))
        {
            let leaf = source
                .rsplit('/')
                .next()
                .context("Missing FOMOD source filename")?;
            if destination.is_empty() {
                leaf.into()
            } else {
                format!("{destination}/{leaf}")
            }
        } else {
            destination.clone()
        };
        let target = relative_path(&target)?;
        // Destination matching is Windows case-insensitive; later instructions
        // replace the earlier provider without creating duplicate Linux files.
        if let Some(old) = outputs
            .keys()
            .find(|k| k.eq_ignore_ascii_case(&target))
            .cloned()
        {
            outputs.remove(&old);
        }
        outputs.insert(target, path.clone());
        count += 1;
    }
    if count == 0 {
        // Empty directory markers are real installer instructions (for example
        // KS Hairdos' "== Installer ==" option). Missing files still fail.
        if folder
            && (source_key.is_empty()
                || super::paths::resolve_entry(archive_root, &source_key).is_ok_and(|p| p.is_dir()))
        {
            return Ok(());
        }
        bail!("FOMOD source is missing or empty: {source}");
    }
    Ok(())
}

/// Replay authored choices, or accept a fresh installer only when every choice
/// can be resolved uniquely from its required/recommended defaults.
pub fn replay(files: &Files, recorded: &Value, context: &InstallerContext) -> Result<Files> {
    let mut configs: Vec<_> = files
        .iter()
        .filter(|(k, _)| k.to_lowercase().ends_with("fomod/moduleconfig.xml"))
        .collect();
    configs.sort_by_key(|(name, _)| name.split('/').count());
    let &(config_name, config_path) = configs.first().context("Missing FOMOD ModuleConfig.xml")?;
    let prefix = &config_name[..config_name.len() - "fomod/moduleconfig.xml".len()];
    // A main installer may bundle an older/nested installer as payload (Better
    // Skyrim Parties does this). Use the outer script when all other scripts
    // lie beneath its package root; independent sibling installers are ambiguous.
    if configs.iter().skip(1).any(|(name, _)| {
        name.split('/').count() <= config_name.split('/').count() || !name.starts_with(prefix)
    }) {
        bail!("Ambiguous independent FOMOD ModuleConfig.xml files");
    }
    if config_path.metadata()?.len() > 16 * 1024 * 1024 {
        bail!("FOMOD XML exceeds size limit");
    }
    let xml = read_xml_with_encoding(config_path)?;
    let document = roxmltree::Document::parse(&xml).context("Invalid FOMOD XML")?;
    let config = document.root_element();
    if !named(config, "config") {
        bail!("Invalid FOMOD root element");
    }
    let data_root = &config_name[..config_name.len() - "fomod/moduleconfig.xml".len()];
    let mut archive_root = config_path.as_path();
    for _ in 0..config_name.split('/').count() {
        archive_root = archive_root
            .parent()
            .context("Invalid extracted FOMOD location")?;
    }
    let authored = !recorded.is_null();
    if authored
        && recorded
            .get("type")
            .and_then(Value::as_str)
            .is_some_and(|s| s != "fomod")
    {
        bail!("Unsupported recorded installer type");
    }
    let records = recorded.get("options").and_then(Value::as_array);
    if authored && records.is_none() {
        bail!("FOMOD recorded options are missing");
    }
    let mut flags = BTreeMap::new();
    if !authored {
        if let Some(deps) = child(config, "moduleDependencies") {
            if !dependency(deps, &flags, context)? {
                bail!("FOMOD module dependencies are unsatisfied");
            }
        }
    }
    let mut required = Vec::new();
    let mut selected = Vec::new();
    let mut conditional = Vec::new();
    if let Some(n) = child(config, "requiredInstallFiles") {
        required.extend(
            n.children()
                .filter(|n| named(*n, "file") || named(*n, "folder")),
        );
    }
    let steps = child(config, "installSteps")
        .map(|n| ordered(n, "installStep"))
        .unwrap_or_default();
    let mut used_records = BTreeSet::new();
    for (step_index, step) in steps.iter().enumerate() {
        let name = step.attribute("name").unwrap_or("");
        let candidates: Vec<_> = records
            .into_iter()
            .flatten()
            .enumerate()
            .filter(|(i, r)| {
                !used_records.contains(i)
                    && r.get("name")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .eq_ignore_ascii_case(name)
            })
            .collect();
        let record = if candidates.len() == 1 {
            Some(candidates[0])
        } else {
            records
                .and_then(|r| r.get(step_index))
                .filter(|r| {
                    r.get("name")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .is_empty()
                        || candidates.iter().any(|(i, _)| *i == step_index)
                })
                .map(|r| (step_index, r))
        };
        if let Some((i, _)) = record {
            used_records.insert(i);
        }
        let groups_recorded = record
            .and_then(|(_, r)| r.get("groups"))
            .and_then(Value::as_array);
        let has_selections = groups_recorded.into_iter().flatten().any(|g| {
            g.get("choices")
                .and_then(Value::as_array)
                .is_some_and(|a| !a.is_empty())
        });
        if let Some(visible) = child(*step, "visible") {
            if !(authored && has_selections || dependency(visible, &flags, context)?) {
                continue;
            }
        }
        let groups = child(*step, "optionalFileGroups")
            .map(|n| ordered(n, "group"))
            .unwrap_or_default();
        let mut used_groups = BTreeSet::new();
        for (group_index, group) in groups.iter().enumerate() {
            let name = group.attribute("name").unwrap_or("");
            let matches: Vec<_> = groups_recorded
                .into_iter()
                .flatten()
                .enumerate()
                .filter(|(i, r)| {
                    !used_groups.contains(i)
                        && r.get("name")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .eq_ignore_ascii_case(name)
                })
                .collect();
            let group_record = if matches.len() == 1 {
                Some(matches[0])
            } else {
                matches.iter().find(|(i, _)| *i == group_index).copied()
            };
            if let Some((i, _)) = group_record {
                used_groups.insert(i);
            }
            let plugins = child(*group, "plugins")
                .map(|n| ordered(n, "plugin"))
                .unwrap_or_default();
            let mut chosen = BTreeSet::new();
            if let Some((_, record)) = group_record {
                for choice in record
                    .get("choices")
                    .and_then(Value::as_array)
                    .context("Malformed recorded FOMOD group")?
                {
                    let name = choice
                        .get("name")
                        .and_then(Value::as_str)
                        .context("Missing recorded choice name")?;
                    let matches: Vec<_> = plugins
                        .iter()
                        .enumerate()
                        .filter(|(_, p)| {
                            p.attribute("name").unwrap_or("").eq_ignore_ascii_case(name)
                        })
                        .map(|(i, _)| i)
                        .collect();
                    let i = if matches.len() == 1 {
                        matches[0]
                    } else {
                        let i = choice
                            .get("idx")
                            .and_then(Value::as_u64)
                            .context("Unresolved recorded FOMOD choice")?
                            as usize;
                        if i >= plugins.len() || (!matches.is_empty() && !matches.contains(&i)) {
                            bail!("Recorded FOMOD choice index is invalid");
                        }
                        i
                    };
                    chosen.insert(i);
                }
            }
            let group_type = group.attribute("type").unwrap_or("SelectAny");
            if group_type == "SelectAll" {
                chosen.extend(0..plugins.len());
            }
            if !authored {
                for (i, p) in plugins.iter().enumerate() {
                    if matches!(
                        plugin_type(*p, &flags, context)?,
                        "Required" | "Recommended"
                    ) {
                        chosen.insert(i);
                    }
                }
                if chosen.is_empty()
                    && matches!(group_type, "SelectExactlyOne" | "SelectAtLeastOne")
                {
                    let usable: Vec<_> = plugins
                        .iter()
                        .enumerate()
                        .filter_map(|(i, p)| match plugin_type(*p, &flags, context) {
                            Ok("NotUsable") => None,
                            _ => Some(i),
                        })
                        .collect();
                    if usable.len() == 1 {
                        chosen.insert(usable[0]);
                    } else {
                        bail!("Installer interaction required: ambiguous FOMOD group {name}");
                    }
                }
                if matches!(group_type, "SelectExactlyOne" | "SelectAtMostOne") && chosen.len() > 1
                {
                    bail!("Installer interaction required: ambiguous FOMOD defaults");
                }
            }
            for (i, plugin) in plugins.iter().enumerate() {
                let is_selected = chosen.contains(&i);
                if is_selected {
                    if let Some(cf) = child(*plugin, "conditionFlags") {
                        for flag in children(cf, "flag") {
                            flags.insert(
                                flag.attribute("name").unwrap_or("").to_owned(),
                                flag.text().unwrap_or("").trim().to_owned(),
                            );
                        }
                    }
                }
                if let Some(filelist) = child(*plugin, "files") {
                    for file in filelist
                        .children()
                        .filter(|n| named(*n, "file") || named(*n, "folder"))
                    {
                        if is_selected
                            || file.attribute("alwaysInstall") == Some("true")
                            || (file.attribute("installIfUsable") == Some("true")
                                && plugin_type(*plugin, &flags, context)? != "NotUsable")
                        {
                            selected.push(file);
                        }
                    }
                }
            }
        }
        if groups_recorded.is_some_and(|g| used_groups.len() != g.len()) {
            bail!("A recorded FOMOD group was not matched");
        }
    }
    if records.is_some_and(|r| r.len() != used_records.len()) {
        bail!("A recorded FOMOD step was not matched");
    }
    if let Some(patterns) =
        child(config, "conditionalFileInstalls").and_then(|n| child(n, "patterns"))
    {
        for pattern in children(patterns, "pattern") {
            if child(pattern, "dependencies")
                .map(|n| dependency(n, &flags, context))
                .transpose()?
                .unwrap_or(true)
            {
                if let Some(files) = child(pattern, "files") {
                    conditional.extend(
                        files
                            .children()
                            .filter(|n| named(*n, "file") || named(*n, "folder")),
                    );
                }
            }
        }
    }
    let mut outputs = Files::new();
    for bucket in [&mut required, &mut selected, &mut conditional] {
        bucket.sort_by_key(|n| {
            n.attribute("priority")
                .and_then(|p| p.parse::<i64>().ok())
                .unwrap_or(0)
        });
        for instruction in bucket.iter() {
            install(files, data_root, *instruction, &mut outputs, archive_root)?;
        }
    }
    if outputs.is_empty() {
        bail!("FOMOD produced no files");
    }
    Ok(outputs)
}
