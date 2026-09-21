//! Strict logical paths for collection-owned files. Never repair traversal or
//! guess how an author's absolute deployment path maps to this computer.

use anyhow::{bail, Context, Result};
use std::path::{Path, PathBuf};
use unicode_normalization::UnicodeNormalization;

pub fn relative_path(input: &str) -> Result<String> {
    let normalized = input.replace('\\', "/").nfc().collect::<String>();
    if normalized.is_empty() || normalized.starts_with('/') {
        bail!("Expected a nonempty relative path");
    }
    for component in normalized.split('/') {
        if component.is_empty()
            || component == "."
            || component == ".."
            || component.ends_with(['.', ' '])
            || component
                .chars()
                .any(|c| c.is_control() || ":*?\"<>|".contains(c))
        {
            bail!("Unsafe or ambiguous path component");
        }
        let stem = component
            .split('.')
            .next()
            .unwrap_or("")
            .to_ascii_uppercase();
        if matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
            || (stem.len() == 4
                && (stem.starts_with("COM") || stem.starts_with("LPT"))
                && matches!(stem.as_bytes()[3], b'1'..=b'9'))
        {
            bail!("Reserved Windows filename");
        }
    }
    Ok(normalized)
}

pub fn path_key(input: &str) -> Result<String> {
    Ok(relative_path(input)?.to_lowercase())
}

/// Destination spelling for a private, initially empty tree owned by this
/// operation. Indexing avoids rescanning a large flat directory for every file.
pub(crate) struct OutputPaths {
    root: PathBuf,
    paths: std::collections::BTreeMap<String, PathBuf>,
}

impl OutputPaths {
    pub(crate) fn new(root: &Path) -> Self {
        Self {
            root: root.to_owned(),
            paths: Default::default(),
        }
    }

    pub(crate) fn path(&mut self, input: &str) -> Result<PathBuf> {
        let mut current = self.root.clone();
        let mut key = String::new();
        for component in relative_path(input)?.split('/') {
            key.push('/');
            key.push_str(&component.to_lowercase());
            current = self
                .paths
                .entry(key.clone())
                .or_insert_with(|| current.join(component))
                .clone();
            if current
                .symlink_metadata()
                .is_ok_and(|m| m.file_type().is_symlink())
            {
                bail!("Output path contains a symlink");
            }
        }
        Ok(current)
    }
}

/// Resolve a previously validated relative path without following links or
/// selecting one of two case-colliding entries arbitrarily.
pub fn resolve_file(root: &Path, input: &str) -> Result<PathBuf> {
    let current = resolve_entry(root, input)?;
    if !current.is_file() {
        bail!("Expected a regular package file");
    }
    Ok(current)
}

pub(crate) fn resolve_entry(root: &Path, input: &str) -> Result<PathBuf> {
    let path = relative_path(input)?;
    let mut current = root.canonicalize().context("Open package root")?;
    for component in path.split('/') {
        let key = component.to_lowercase();
        let mut matches = Vec::new();
        for entry in std::fs::read_dir(&current).context("Read package directory")? {
            let entry = entry?;
            if entry
                .file_name()
                .to_string_lossy()
                .nfc()
                .collect::<String>()
                .to_lowercase()
                == key
            {
                matches.push(entry.path());
            }
        }
        if matches.len() != 1 {
            bail!("Missing or case-ambiguous package path");
        }
        current = matches.pop().unwrap();
        if current.symlink_metadata()?.file_type().is_symlink() {
            bail!("Symlinks are not supported in collection packages");
        }
    }
    Ok(current)
}
