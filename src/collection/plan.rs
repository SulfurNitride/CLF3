use super::{
    package::{digest_bytes, CollectionPackage, MAX_PAYLOAD_BYTES},
    paths::relative_path,
    types::{CollectionMod, ModSource, PluginInfo},
    url::CollectionLocator,
};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

pub const PLAN_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Default)]
pub struct PlanOptions {
    /// Schema ID is supplied by the revision API, not inferred from a mod version.
    pub schema_id: Option<u32>,
    pub locator: Option<CollectionLocator>,
    pub selected_optional: BTreeSet<String>,
    pub all_optional: bool,
    pub game_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Diagnostic {
    pub code: String,
    pub member_id: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactPlan {
    pub id: String,
    pub source_type: String,
    pub domain: String,
    pub mod_id: u64,
    pub file_id: u64,
    pub expected_md5: String,
    pub expected_size: Option<u64>,
    pub update_policy: String,
    // Deliberately no source URLs: signed query strings stay out of journals,
    // logs and UI reports. Acquisition retrieves them from its private input.
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberPlan {
    pub id: String,
    pub source_index: usize,
    pub name: String,
    #[serde(default)]
    pub version: String,
    pub tag: String,
    pub artifact_id: String,
    pub install_signature: String,
    pub optional: bool,
    pub selected: bool,
    pub phase: i32,
    pub mode: String,
    pub excluded_paths: Vec<String>,
    pub patch_digests: BTreeMap<String, String>,
    pub recorded_output_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionPlan {
    pub plan_schema_version: u32,
    pub engine_version: String,
    pub package_sha256: String,
    pub schema_id: Option<u32>,
    pub locator: Option<CollectionLocator>,
    pub name: String,
    pub domain: String,
    #[serde(default)]
    pub launcher_name: Option<String>,
    #[serde(default)]
    pub ini_tweaks: Vec<super::ini::IniTweak>,
    pub game_versions: Vec<String>,
    #[serde(default)]
    pub game_version: Option<String>,
    pub members: Vec<MemberPlan>,
    pub artifacts: Vec<ArtifactPlan>,
    /// IDs sorted by phase then authored member position.
    pub installation_order: Vec<String>,
    /// IDs ordered lowest to highest asset priority; independent of plugins.
    pub asset_order: Vec<String>,
    pub plugins: Vec<PluginInfo>,
    pub plugin_rules: Value,
    pub exclude_plugin_rules: bool,
    pub required_capabilities: BTreeSet<String>,
    pub blockers: Vec<Diagnostic>,
    pub warnings: Vec<Diagnostic>,
}

impl CollectionPlan {
    pub fn build(package: &CollectionPackage, options: &PlanOptions) -> Result<Self> {
        let collection = &package.collection;
        let mut plan = Self {
            plan_schema_version: PLAN_SCHEMA_VERSION,
            engine_version: env!("CARGO_PKG_VERSION").into(),
            package_sha256: package.digest.clone(),
            schema_id: options.schema_id,
            locator: options.locator.clone(),
            name: collection.name().into(),
            domain: collection.domain().into(),
            launcher_name: None,
            ini_tweaks: Vec::new(),
            game_versions: collection.info.game_versions.clone(),
            game_version: options.game_version.clone(),
            members: Vec::new(),
            artifacts: Vec::new(),
            installation_order: Vec::new(),
            asset_order: Vec::new(),
            plugins: collection.plugins.clone(),
            plugin_rules: collection.plugin_rules.clone(),
            exclude_plugin_rules: collection
                .collection_config
                .get("excludePluginRules")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            required_capabilities: BTreeSet::new(),
            blockers: Vec::new(),
            warnings: Vec::new(),
        };
        if options.schema_id.is_some_and(|id| id != 1) {
            plan.block(
                "unsupported_schema",
                None,
                "Only Nexus Collection schema 1 is understood",
            );
        } else if options.schema_id.is_none() {
            plan.warnings.push(Diagnostic { code: "unverified_schema".into(), member_id: None, message: "Offline manifest: schema ID has not been verified against Nexus revision metadata".into() });
        }
        if package.manifest_only {
            plan.block("manifest_only", None, "Supply the full collection package or extracted package directory to retain bundles, patches and tweaks");
        }
        let game = super::games::profile(&plan.domain);
        if game.is_none() {
            plan.block(
                "unsupported_game",
                None,
                super::games::unsupported_reason(&plan.domain),
            );
        } else if game.is_some_and(|g| g.experimental) {
            plan.warnings.push(Diagnostic { code:"experimental_game".into(), member_id:None, message:"This game adapter has fixture coverage; full collection and game-launch validation remain collection-specific".into() });
        }
        if let Some(locator) = &options.locator {
            if locator.domain != plan.domain {
                plan.block(
                    "domain_mismatch",
                    None,
                    "Package domain differs from the requested collection",
                );
            }
            if locator.revision.is_none() {
                plan.block(
                    "revision_not_pinned",
                    None,
                    "Resolve the collection to an explicit revision before installation",
                );
            }
        }
        if !plan.game_versions.is_empty() {
            match &options.game_version {
                Some(version) if plan.game_versions.contains(version) => {}
                Some(_) => plan.block(
                    "game_version_mismatch",
                    None,
                    "Game runtime does not match the authored versions",
                ),
                None => plan.block(
                    "game_version_required",
                    None,
                    "Host must supply the game runtime for compatibility checking",
                ),
            }
        }
        // Preserve extensions but never silently promise their effects.
        for (key, value) in &collection.extra {
            if key == "tools" {
                if let Some(tools) = value.as_array() {
                    if tools.is_empty() {
                        continue;
                    }
                    if tools.len() == 1 {
                        let tool = &tools[0];
                        let name = tool.get("name").and_then(Value::as_str).unwrap_or("");
                        let cwd = tool.get("cwd").and_then(Value::as_str).unwrap_or("");
                        if game.is_some_and(|g| {
                            tool.get("exe")
                                .and_then(Value::as_str)
                                .is_some_and(|e| e.eq_ignore_ascii_case(g.extender))
                        }) && tool
                            .get("args")
                            .and_then(Value::as_array)
                            .is_some_and(Vec::is_empty)
                            && tool.get("detach").and_then(Value::as_bool) == Some(true)
                            && !name.is_empty()
                            && name.len() <= 120
                            && !name.chars().any(|c| c.is_control() || "\\\"=;".contains(c))
                            && game.is_some_and(|g| {
                                cwd.replace('\\', "/")
                                    .to_ascii_lowercase()
                                    .replace(' ', "")
                                    .trim_end_matches('/')
                                    .ends_with(&format!(
                                        "/{}",
                                        g.name.to_ascii_lowercase().replace(' ', "")
                                    ))
                            })
                            && tool.as_object().is_some_and(|o| {
                                o.keys().all(|k| {
                                    matches!(k.as_str(), "name" | "exe" | "args" | "cwd" | "detach")
                                })
                            })
                        {
                            plan.launcher_name = Some(name.into());
                            plan.required_capabilities.insert(
                                if plan.domain == "skyrimspecialedition" {
                                    "skse_launcher"
                                } else {
                                    "game_extender_launcher"
                                }
                                .into(),
                            );
                            continue;
                        }
                    }
                }
            }
            if !matches!(key.as_str(), "author" | "description" | "version") {
                plan.block(
                    "unsupported_collection_field",
                    None,
                    &format!("Collection field {key} requires an implementation"),
                );
            }
        }
        let mut artifacts = BTreeMap::new();
        let mut seen_tags = BTreeSet::new();
        let mut matched_selections = BTreeSet::new();
        for (index, member) in collection.mods.iter().enumerate() {
            let source = &member.source;
            let domain = if member.domain_name.is_empty() {
                collection.domain()
            } else {
                &member.domain_name
            };
            let namespace = options
                .locator
                .as_ref()
                .map(|l| format!("{}/{}", l.domain, l.slug))
                .unwrap_or_else(|| package.digest.clone());
            let identity = if source.tag.is_empty() {
                json!([namespace, index, member])
            } else {
                json!([namespace, source.tag])
            };
            let id = format!("member-{}", digest_bytes(&serde_json::to_vec(&identity)?));
            if !source.tag.is_empty() && !seen_tags.insert(source.tag.clone()) {
                plan.block(
                    "duplicate_member_tag",
                    Some(&id),
                    "Member reference tag is not unique",
                );
            }
            let explicitly_selected = options.selected_optional.contains(&id)
                || (!source.tag.is_empty() && options.selected_optional.contains(&source.tag));
            for key in [&id, &source.tag] {
                if options.selected_optional.contains(key) {
                    matched_selections.insert(key.clone());
                }
            }
            let selected = !member.optional || options.all_optional || explicitly_selected;
            let policy = source.update_policy.as_deref().unwrap_or("exact");
            let artifact_identity = json!([
                source.source_type,
                domain,
                source.mod_id,
                source.file_id,
                source.md5.to_lowercase(),
                source.file_size,
                source.url,
                source.file_expression,
                policy
            ]);
            let artifact_id = format!(
                "artifact-{}",
                digest_bytes(&serde_json::to_vec(&artifact_identity)?)
            );
            let artifact = ArtifactPlan {
                id: artifact_id.clone(),
                source_type: source.source_type.clone(),
                domain: domain.into(),
                mod_id: source.mod_id,
                file_id: source.file_id,
                expected_md5: source.md5.to_lowercase(),
                expected_size: source.file_size,
                update_policy: policy.into(),
            };
            let mode = if !member.hashes.is_empty() {
                "recorded_files"
            } else if !member.choices.is_null() {
                "installer_choices"
            } else if source.source_type == "bundle" {
                "bundle"
            } else {
                "detect_installer"
            };
            let mut excluded_paths = Vec::new();
            let mut patch_digests = BTreeMap::new();
            if selected {
                artifacts.entry(artifact_id.clone()).or_insert(artifact);
                plan.required_capabilities.insert(mode.into());
                plan.validate_source(source, &id);
                if policy == "prefer" {
                    plan.warnings.push(Diagnostic {
                        code: "prefer_pinned_artifact".into(), member_id: Some(id.clone()),
                        message: "Using the preferred pinned file and digest; unavailable pinned files fail instead of silently selecting a replacement".into(),
                    });
                } else if policy != "exact" {
                    plan.block("unsupported_update_policy", Some(&id), "Resolve prefer/latest policies in the host; this planner supports exact artifacts");
                }
                if mode == "installer_choices"
                    && member
                        .choices
                        .get("type")
                        .and_then(Value::as_str)
                        .is_some_and(|s| s != "fomod")
                {
                    plan.block(
                        "unsupported_installer",
                        Some(&id),
                        "Only recorded FOMOD installers are supported",
                    );
                }
                if !member.choices.is_null() && !member.hashes.is_empty() {
                    plan.block("ambiguous_install_mode", Some(&id), "Member supplies both choices and recorded files; precedence must be resolved explicitly");
                }
                if source.source_type == "bundle" {
                    match relative_path(&source.file_expression) {
                        Ok(folder) => {
                            let prefix = format!("bundled/{}/", folder.to_lowercase());
                            if !package.entries.keys().any(|key| key.starts_with(&prefix)) {
                                plan.block(
                                    "missing_bundle",
                                    Some(&id),
                                    "Bundled payload is missing from the collection package",
                                );
                            }
                        }
                        Err(_) => plan.block(
                            "unsafe_bundle_path",
                            Some(&id),
                            "Bundle fileExpression must be a safe relative directory",
                        ),
                    }
                }
                let mut outputs = BTreeSet::new();
                for file in &member.hashes {
                    match relative_path(&file.path) {
                        Ok(path) if outputs.insert(path.to_lowercase()) => {}
                        Ok(_) => plan.block(
                            "duplicate_recorded_output",
                            Some(&id),
                            "Recorded output paths collide after normalization",
                        ),
                        Err(_) => plan.block(
                            "unsafe_recorded_path",
                            Some(&id),
                            "Recorded output has an unsafe path",
                        ),
                    }
                    if !valid_hex(&file.md5, 32) {
                        plan.block(
                            "invalid_recorded_md5",
                            Some(&id),
                            "Recorded output MD5 must contain 32 hexadecimal digits",
                        );
                    }
                }
                for path in &member.file_overrides {
                    match relative_path(path) {
                        Ok(path) => excluded_paths.push(path),
                        Err(_) => plan.block("unmapped_exclusion", Some(&id), "Excluded path is absolute or ambiguous; the host must map the author's deployment root"),
                    }
                }
                if !excluded_paths.is_empty() {
                    plan.required_capabilities
                        .insert("provider_exclusions".into());
                }
                for (path, crc) in &member.patches {
                    if !valid_hex(crc, 8) {
                        plan.block(
                            "invalid_patch_crc",
                            Some(&id),
                            "Patch source CRC32 must contain 8 hexadecimal digits",
                        );
                    }
                    let payload = relative_path(path).and_then(|path| {
                        relative_path(&member.name)
                            .and_then(|name| relative_path(&format!("patches/{name}/{path}.diff")))
                    });
                    match payload.and_then(|path| package.read(&path, MAX_PAYLOAD_BYTES)) {
                        Ok(bytes) if bytes.starts_with(b"BSDIFF40") => {
                            patch_digests.insert(path.clone(), digest_bytes(&bytes));
                        }
                        _ => plan.block(
                            "invalid_patch_payload",
                            Some(&id),
                            "Required BSDIFF40 patch is missing, unsafe, oversized or invalid",
                        ),
                    }
                }
                if !member.patches.is_empty() {
                    plan.required_capabilities.insert("bsdiff40".into());
                }
                if let Some(kind) = member.details.get("type").and_then(Value::as_str) {
                    if !kind.is_empty() && !matches!(kind, "default" | "dinput") {
                        plan.block(
                            "special_installer_type",
                            Some(&id),
                            "Member requires game-specific deployment/installer handling",
                        );
                    }
                }
            }
            let signature = digest_bytes(&serde_json::to_vec(&json!({
                "member": member, "artifact": artifact_id, "patches": patch_digests,
                "schema": options.schema_id, "engine": env!("CARGO_PKG_VERSION"),
                "plan_schema": PLAN_SCHEMA_VERSION, "routing": if plan.domain == "skyrimspecialedition" { "logical-v4-basic-prefix".to_owned() } else { format!("logical-v5-game-profile:{}", plan.domain) }, "game_version": options.game_version
            }))?);
            plan.members.push(MemberPlan {
                id,
                source_index: index,
                name: member.name.clone(),
                version: member.version.clone(),
                tag: source.tag.clone(),
                artifact_id,
                install_signature: signature,
                optional: member.optional,
                selected,
                phase: member.phase,
                mode: mode.into(),
                excluded_paths,
                patch_digests,
                recorded_output_count: member.hashes.len(),
            });
        }
        for _ in options.selected_optional.difference(&matched_selections) {
            plan.block(
                "unknown_optional_selection",
                None,
                "An optional selection does not match a member ID or reference tag",
            );
        }
        plan.artifacts = artifacts.into_values().collect();
        let mut installation: Vec<_> = plan.members.iter().filter(|m| m.selected).collect();
        installation.sort_by_key(|m| (m.phase, m.source_index));
        plan.installation_order = installation.iter().map(|m| m.id.clone()).collect();
        plan.resolve_rules(package);
        if !plan.exclude_plugin_rules && !collection.plugin_rules.is_null() {
            plan.required_capabilities.insert("plugin_rules".into());
        }
        if package
            .entries
            .keys()
            .any(|path| path.starts_with("ini tweaks/") || path.starts_with("ini_tweaks/"))
        {
            plan.required_capabilities
                .insert("profile_ini_tweaks".into());
            match super::ini::load(package) {
                Ok(tweaks) => plan.ini_tweaks = tweaks,
                Err(error) => plan.block(
                    "unsupported_ini_tweak",
                    None,
                    &format!("Cannot reproduce profile INI tweaks: {error}"),
                ),
            }
        }
        Ok(plan)
    }

    fn block(&mut self, code: &str, member: Option<&str>, message: &str) {
        self.blockers.push(Diagnostic {
            code: code.into(),
            member_id: member.map(str::to_owned),
            message: message.into(),
        });
    }

    fn validate_source(&mut self, source: &ModSource, id: &str) {
        match source.source_type.as_str() {
            "nexus" if source.mod_id > 0 && source.file_id > 0 => {}
            "nexus" => self.block(
                "invalid_nexus_identity",
                Some(id),
                "Nexus sources require positive modId and fileId",
            ),
            "bundle" => {}
            "direct" | "browse" => {
                let valid = reqwest::Url::parse(&source.url).ok().is_some_and(|u| {
                    matches!(u.scheme(), "http" | "https")
                        && u.host_str().is_some()
                        && u.username().is_empty()
                        && u.password().is_none()
                });
                if !valid {
                    self.block(
                        "invalid_source_url",
                        Some(id),
                        "Off-site source requires an HTTP(S) URL without embedded credentials",
                    );
                }
            }
            "manual" => {}
            _ => self.block(
                "unsupported_source",
                Some(id),
                "Unrecognized collection source type",
            ),
        }
        if !source.md5.is_empty() && !valid_hex(&source.md5, 32) {
            self.block(
                "invalid_artifact_md5",
                Some(id),
                "Artifact MD5 must contain 32 hexadecimal digits",
            );
        }
    }

    fn resolve_rules(&mut self, package: &CollectionPackage) {
        let n = self.members.len();
        let mut outgoing = vec![BTreeSet::new(); n];
        let mut incoming = vec![0usize; n];
        for rule in &package.collection.mod_rules {
            let source = match_reference(&rule.source, &package.collection.mods);
            let target = match_reference(&rule.reference, &package.collection.mods);
            let (source, target) = match (source, target) {
                (Ok(Some(a)), Ok(Some(b))) => (a, b),
                (Ok(None), _) | (_, Ok(None))
                    if matches!(rule.rule_type.as_str(), "before" | "after" | "conflicts") =>
                {
                    self.warnings.push(Diagnostic {code:"inapplicable_mod_rule".into(), member_id:None, message:"Ordering/conflict rule references an artifact or version outside this revision".into()});
                    continue;
                }
                _ => {
                    self.block(
                        "unresolved_mod_rule",
                        None,
                        "A mod rule has an unsupported, missing or ambiguous reference",
                    );
                    continue;
                }
            };
            if !self.members[source].selected {
                continue;
            }
            match rule.rule_type.as_str() {
                "before" | "after" if self.members[target].selected => {
                    let (a, b) = if rule.rule_type == "before" {
                        (source, target)
                    } else {
                        (target, source)
                    };
                    if outgoing[a].insert(b) {
                        incoming[b] += 1;
                    }
                }
                "before" | "after" => {}
                "requires" if !self.members[target].selected => self.block(
                    "required_member_omitted",
                    None,
                    "A selected member requires an omitted optional member",
                ),
                "requires" => {}
                "conflicts" if self.members[target].selected => self.block(
                    "conflicting_members",
                    None,
                    "Selected collection members declare a conflict",
                ),
                "conflicts" => {}
                _ => self.block(
                    "unsupported_rule_type",
                    None,
                    "This rule type requires explicit compatibility handling",
                ),
            }
        }
        let mut ready: BTreeSet<_> = (0..n)
            .filter(|&i| self.members[i].selected && incoming[i] == 0)
            .collect();
        while let Some(i) = ready.pop_first() {
            self.asset_order.push(self.members[i].id.clone());
            for &target in &outgoing[i] {
                incoming[target] -= 1;
                if incoming[target] == 0 {
                    ready.insert(target);
                }
            }
        }
        if self.asset_order.len() != self.members.iter().filter(|m| m.selected).count() {
            self.asset_order.clear();
            self.block(
                "cyclic_mod_rules",
                None,
                "Asset ordering rules form a cycle; no substitute order was generated",
            );
        }
    }
}

fn valid_hex(value: &str, len: usize) -> bool {
    value.len() == len && value.bytes().all(|b| b.is_ascii_hexdigit())
}

/// An exact archive digest takes precedence over stale filename hints. Missing
/// historical references are distinguishable from ambiguous references.
fn match_reference(reference: &Value, members: &[CollectionMod]) -> Result<Option<usize>, ()> {
    let fields = reference.as_object().ok_or(())?;
    let mut has_identity = false;
    for key in fields.keys() {
        match key.as_str() {
            "fileMD5" | "logicalFileName" | "fileExpression" | "tag" | "referenceTag" => {
                has_identity = true
            }
            "versionMatch" | "description" | "idHint" => {}
            _ => return Err(()),
        }
    }
    if !has_identity {
        return Err(());
    }
    let matches: Vec<_> = members
        .iter()
        .enumerate()
        .filter(|(_, member)| {
            if let Some(md5) = fields
                .get("fileMD5")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
            {
                return member.source.md5.eq_ignore_ascii_case(md5)
                    && fields
                        .get("tag")
                        .or_else(|| fields.get("referenceTag"))
                        .and_then(Value::as_str)
                        .is_none_or(|t| member.source.tag == t);
            }
            fields.iter().all(|(key, value)| {
                let Some(value) = value.as_str() else {
                    return false;
                };
                match key.as_str() {
                    "fileMD5" => !value.is_empty() && member.source.md5.eq_ignore_ascii_case(value),
                    "logicalFileName" => {
                        !value.is_empty()
                            && member.source.logical_filename.eq_ignore_ascii_case(value)
                    }
                    "fileExpression" => {
                        !value.is_empty()
                            && (member.source.file_expression.eq_ignore_ascii_case(value)
                                || fields
                                    .get("fileMD5")
                                    .and_then(Value::as_str)
                                    .is_some_and(|s| !s.is_empty()))
                    }
                    "tag" | "referenceTag" => !value.is_empty() && member.source.tag == value,
                    "versionMatch" => version_matches(value, &member.version),
                    "description" | "idHint" => true,
                    _ => false,
                }
            })
        })
        .map(|(i, _)| i)
        .collect();
    if matches.len() == 1 {
        Ok(Some(matches[0]))
    } else if matches.is_empty() {
        Ok(None)
    } else {
        Err(())
    }
}

fn version_matches(requirement: &str, version: &str) -> bool {
    if requirement == "*" || requirement == version {
        return true;
    }
    let version = semver::Version::parse(version);
    let Ok(version) = version else {
        return false;
    };
    requirement.split("||").any(|part| {
        let part = part.trim();
        if !part.starts_with(['<', '>', '=', '~', '^', '*']) {
            return false;
        }
        semver::VersionReq::parse(part).is_ok_and(|req| req.matches(&version))
    })
}
