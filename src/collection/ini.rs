//! Collection-authored INI edits, applied only to the new profile's copies.
use super::package::{digest_bytes, CollectionPackage, MAX_MANIFEST_BYTES};
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, path::Path};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IniEdit {
    pub section: String,
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IniTweak {
    pub source: String,
    pub source_sha256: String,
    pub target: String,
    pub edits: Vec<IniEdit>,
}

fn target_name(domain: &str, name: &str) -> Option<&'static str> {
    super::games::profile(domain)?.ini_target(name)
}

fn section(line: &str) -> Option<&str> {
    line.trim()
        .strip_prefix('[')?
        .strip_suffix(']')
        .map(str::trim)
}

fn valid(edit: &IniEdit) -> bool {
    !edit.section.is_empty()
        && !edit.key.is_empty()
        && !edit.section.contains(['[', ']'])
        && !edit.key.contains(['[', ']', '='])
        && !edit.key.starts_with([';', '#'])
        && [&edit.section, &edit.key, &edit.value]
            .iter()
            .all(|s| !s.chars().any(char::is_control))
}

pub fn load(package: &CollectionPackage) -> Result<Vec<IniTweak>> {
    let mut tweaks = Vec::new();
    for entry in package.entries.values().filter(|e| {
        let p = e.path.to_ascii_lowercase();
        p.starts_with("ini tweaks/") || p.starts_with("ini_tweaks/")
    }) {
        let name = entry.path.rsplit('/').next().unwrap_or("");
        let (_, suffix) = name
            .rsplit_once('[')
            .context("INI tweak lacks [Target] filename")?;
        let stem = suffix
            .get(..suffix.len().saturating_sub(5))
            .filter(|_| suffix.to_ascii_lowercase().ends_with("].ini"))
            .context("INI tweak lacks [Target].ini filename")?;
        let target = target_name(package.collection.domain(), &format!("{stem}.ini"))
            .context("Unsupported profile INI target for this game")?;
        let bytes = package.read(&entry.path, MAX_MANIFEST_BYTES)?;
        let text = std::str::from_utf8(&bytes).context("INI tweak is not UTF-8")?;
        let mut current = None;
        let mut edits = Vec::new();
        for line in text.trim_start_matches('\u{feff}').lines().map(str::trim) {
            if line.is_empty() || line.starts_with([';', '#']) {
                continue;
            }
            if let Some(s) = section(line) {
                current = Some(s);
                continue;
            }
            let (key, value) = line
                .split_once('=')
                .context("Unsupported INI tweak syntax")?;
            let edit = IniEdit {
                section: current.context("INI tweak key outside a section")?.into(),
                key: key.trim().into(),
                value: value.trim().into(),
            };
            if !valid(&edit) {
                bail!("Invalid INI tweak section/key/value");
            }
            edits.push(edit);
        }
        tweaks.push(IniTweak {
            source: entry.path.clone(),
            source_sha256: digest_bytes(&bytes),
            target: target.into(),
            edits,
        });
    }
    Ok(tweaks)
}

fn merge(text: &str, edit: &IniEdit) -> Result<String> {
    if !valid(edit) {
        bail!("Invalid profile INI edit");
    }
    let mut lines: Vec<String> = text
        .trim_start_matches('\u{feff}')
        .lines()
        .map(str::to_owned)
        .collect();
    let mut current = "";
    let mut found_section = false;
    let mut insertion = None;
    let mut matches = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        if let Some(s) = section(line) {
            if current.eq_ignore_ascii_case(&edit.section) && insertion.is_none() {
                insertion = Some(index);
            }
            current = s;
            found_section |= s.eq_ignore_ascii_case(&edit.section);
        } else if current.eq_ignore_ascii_case(&edit.section) {
            if let Some((key, _)) = line.split_once('=') {
                if key.trim().eq_ignore_ascii_case(&edit.key) {
                    matches.push(index);
                }
            }
        }
    }
    let line = format!("{}={}", edit.key, edit.value);
    if let Some(&first) = matches.first() {
        lines[first] = line;
        for index in matches.into_iter().skip(1).rev() {
            lines.remove(index);
        }
    } else if found_section {
        lines.insert(insertion.unwrap_or(lines.len()), line);
    } else {
        lines.push(format!("[{}]", edit.section));
        lines.push(line);
    }
    Ok(format!("{}\n", lines.join("\n")))
}

pub fn apply(profile: &Path, records: &Path, tweaks: &[IniTweak], domain: &str) -> Result<usize> {
    if tweaks.is_empty() {
        return Ok(0);
    }
    std::fs::create_dir_all(records)?;
    let mut backed_up = BTreeSet::new();
    let mut changes = Vec::new();
    let mut count = 0;
    for tweak in tweaks {
        let target = target_name(domain, &tweak.target)
            .context("Unsupported profile INI target for this game")?;
        let path = profile.join(target);
        if path
            .symlink_metadata()
            .is_ok_and(|m| m.file_type().is_symlink())
        {
            bail!("Profile INI cannot be a symlink");
        }
        let existed = path.exists();
        let original = if existed {
            std::fs::read(&path)?
        } else {
            Vec::new()
        };
        if original.len() as u64 > MAX_MANIFEST_BYTES {
            bail!("Oversized profile INI");
        }
        if backed_up.insert(target) {
            std::fs::write(records.join(format!("{target}.before")), &original)?;
        }
        let mut text = String::from_utf8(original.clone()).context("Profile INI is not UTF-8")?;
        for edit in &tweak.edits {
            text = merge(&text, edit)?;
            count += 1;
        }
        std::fs::write(&path, &text)?;
        changes.push(serde_json::json!({"source":tweak.source,"source_sha256":tweak.source_sha256,"target":target,"existed":existed,"before_sha256":digest_bytes(&original),"after_sha256":digest_bytes(text.as_bytes()),"edits":tweak.edits.len()}));
    }
    std::fs::write(
        records.join("changes.json"),
        serde_json::to_vec_pretty(&changes)?,
    )?;
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_matches_case_preserves_other_values_and_supports_empty_values() {
        let edit = IniEdit {
            section: "General".into(),
            key: "sIntroSequence".into(),
            value: "".into(),
        };
        let text = "; keep\n[GENERAL]\nsintrosequence=old\nsIntroSequence=duplicate\nOther=7\n[Display]\nx=1\n";
        let result = merge(text, &edit).unwrap();
        assert_eq!(
            result,
            "; keep\n[GENERAL]\nsIntroSequence=\nOther=7\n[Display]\nx=1\n"
        );
        assert_eq!(merge(&result, &edit).unwrap(), result);
        let add = IniEdit {
            section: "GENERAL".into(),
            key: "bAlwaysActive".into(),
            value: "1".into(),
        };
        assert!(merge(&result, &add)
            .unwrap()
            .contains("bAlwaysActive=1\n[Display]"));
    }

    #[test]
    fn rejects_non_profile_targets_and_injected_edits() {
        assert!(target_name("skyrimspecialedition", "../Skyrim.ini").is_none());
        assert!(target_name("fallout4", "Skyrim.ini").is_none());
        assert_eq!(
            target_name("fallout4", "Fallout4Custom.ini"),
            Some("Fallout4Custom.ini")
        );
        let edit = IniEdit {
            section: "General".into(),
            key: "x".into(),
            value: "1\n[Other]".into(),
        };
        assert!(merge("", &edit).is_err());
    }
}
