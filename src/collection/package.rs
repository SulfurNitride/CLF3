use super::{
    paths::{path_key, relative_path, resolve_file},
    types::Collection,
};
use anyhow::{bail, Context, Result};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
};

pub const MAX_MANIFEST_BYTES: u64 = 16 * 1024 * 1024;
pub const MAX_PAYLOAD_BYTES: u64 = 64 * 1024 * 1024;
const MAX_ENTRIES: usize = 100_000;

#[derive(Debug, Clone, Serialize)]
pub struct PackageEntry {
    pub path: String,
    pub size: u64,
    #[serde(skip)]
    original: String,
}

#[derive(Debug)]
enum Container {
    Directory,
    Zip,
    SevenZip,
    Rar,
    Manifest,
}

/// The raw source stays in memory and is not included in host/log output: a
/// curator's off-site URL may itself contain a temporary authorization token.
#[derive(Debug)]
pub struct CollectionPackage {
    source: PathBuf,
    container: Container,
    pub digest: String,
    pub entries: BTreeMap<String, PackageEntry>,
    directories: Vec<String>,
    pub raw: Value,
    pub collection: Collection,
    pub manifest_only: bool,
}

pub fn digest_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub fn digest_file(path: &Path) -> Result<String> {
    let mut reader = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 128 * 1024];
    loop {
        let n = reader.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn read_bounded(reader: &mut dyn Read, limit: u64) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader.take(limit + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        bail!("Package entry exceeds the configured size limit");
    }
    Ok(bytes)
}

impl CollectionPackage {
    pub fn open(source: &Path) -> Result<Self> {
        let mut package = Self::open_payload(source)?;
        let bytes = package
            .read("collection.json", MAX_MANIFEST_BYTES)
            .context("Read collection.json at package root")?;
        let raw: Value =
            serde_json::from_slice(&bytes).context("Invalid collection manifest JSON")?;
        let collection: Collection =
            serde_json::from_value(raw.clone()).context("Invalid collection manifest structure")?;
        package.raw = raw;
        package.collection = collection;
        Ok(package)
    }

    /// Open an artifact for validated, bounded reads without requiring a
    /// collection manifest. Unsupported container types remain manifest-only.
    pub fn open_payload(source: &Path) -> Result<Self> {
        if source.symlink_metadata()?.file_type().is_symlink() {
            bail!("Package source must not be a symlink");
        }
        let source = source.canonicalize().context("Open collection package")?;
        let container = if source.is_dir() {
            Container::Directory
        } else {
            let mut magic = [0u8; 6];
            let count = File::open(&source)?.read(&mut magic)?;
            if count >= 4 && &magic[..2] == b"PK" {
                Container::Zip
            } else if count == 6 && magic == [0x37, 0x7a, 0xbc, 0xaf, 0x27, 0x1c] {
                Container::SevenZip
            } else if count >= 4 && &magic[..4] == b"Rar!" {
                Container::Rar
            } else {
                Container::Manifest
            }
        };
        let mut entries = BTreeMap::new();
        let mut components: BTreeMap<String, (String, bool)> = BTreeMap::new();
        let mut add = |original: String, size: u64, directory: bool| -> Result<()> {
            let name = if directory {
                original.trim_end_matches(['/', '\\'])
            } else {
                &original
            };
            let name = relative_path(name).context("Unsafe collection package path")?;
            let parts: Vec<_> = name.split('/').collect();
            for i in 1..=parts.len() {
                let prefix = parts[..i].join("/");
                let is_dir = i < parts.len() || directory;
                if let Some((spelling, previous_dir)) = components.get(&prefix.to_lowercase()) {
                    if spelling != &prefix || *previous_dir != is_dir {
                        bail!("Case collision or file/directory collision in package");
                    }
                }
                components.insert(prefix.to_lowercase(), (prefix, is_dir));
            }
            if !directory {
                let key = name.to_lowercase();
                if entries
                    .insert(
                        key,
                        PackageEntry {
                            path: name,
                            size,
                            original,
                        },
                    )
                    .is_some()
                {
                    bail!("Duplicate package entry");
                }
            }
            if components.len() > MAX_ENTRIES {
                bail!("Too many collection package entries");
            }
            Ok(())
        };
        match container {
            Container::Directory => {
                for entry in walkdir::WalkDir::new(&source)
                    .min_depth(1)
                    .follow_links(false)
                {
                    let entry = entry?;
                    if entry.file_type().is_symlink()
                        || !(entry.file_type().is_file() || entry.file_type().is_dir())
                    {
                        bail!("Package contains a link or special file");
                    }
                    let name = entry
                        .path()
                        .strip_prefix(&source)?
                        .to_str()
                        .context("Non-UTF8 package filename")?
                        .to_owned();
                    add(name, entry.metadata()?.len(), entry.file_type().is_dir())?;
                }
            }
            Container::Zip => {
                let mut archive = zip::ZipArchive::new(File::open(&source)?)?;
                if archive.len() > MAX_ENTRIES {
                    bail!("Too many package entries");
                }
                for i in 0..archive.len() {
                    let entry = archive.by_index(i)?;
                    if entry.unix_mode().is_some_and(|mode| {
                        matches!(
                            mode & 0o170000,
                            0o120000 | 0o060000 | 0o020000 | 0o010000 | 0o140000
                        )
                    }) {
                        bail!("Package contains a link or special file");
                    }
                    add(entry.name().into(), entry.size(), entry.is_dir())?;
                }
            }
            Container::SevenZip => {
                let archive =
                    sevenz_rust2::ArchiveReader::open(&source, sevenz_rust2::Password::empty())?;
                if archive.archive().files.len() > MAX_ENTRIES {
                    bail!("Too many package entries");
                }
                for entry in &archive.archive().files {
                    // Some 7z writers include a streamless root-directory
                    // marker (including our native writer). It names no output
                    // and may carry an anti-item bit; never treat it as a file.
                    if entry.is_directory
                        && !entry.has_stream
                        && entry.size == 0
                        && matches!(entry.name.as_str(), "" | ".")
                    {
                        continue;
                    }
                    let mode = (entry.windows_attributes >> 16) & 0o170000;
                    if entry.is_anti_item
                        || entry.windows_attributes & 0x400 != 0
                        || matches!(mode, 0o120000 | 0o060000 | 0o020000 | 0o010000 | 0o140000)
                    {
                        bail!("Package contains a link, special file or deletion entry");
                    }
                    add(entry.name.clone(), entry.size, entry.is_directory)?;
                }
            }
            Container::Rar => {
                // libunrar's public header omits RAR5 redirection metadata.
                // Inspect that metadata before allowing its extraction API.
                let listing = std::process::Command::new("7z")
                    .args(["l", "-slt", "-sccUTF-8"])
                    .arg(&source)
                    .output()?;
                if !listing.status.success() {
                    bail!("Cannot inspect RAR archive");
                }
                let listing = String::from_utf8(listing.stdout)?;
                if listing.lines().any(|l| {
                    ["Symbolic Link =", "Hard Link =", "Copy Link ="]
                        .iter()
                        .any(|prefix| l.strip_prefix(prefix).is_some_and(|v| !v.trim().is_empty()))
                }) {
                    bail!("RAR contains a link");
                }
                let unix_paths: std::collections::BTreeSet<_> = listing
                    .split("\n\n")
                    .filter(|block| block.lines().any(|l| l.trim() == "Host OS = Unix"))
                    .filter_map(|block| block.lines().find_map(|l| l.strip_prefix("Path = ")))
                    .map(str::to_owned)
                    .collect();
                for header in unrar::Archive::new(&source).open_for_listing()? {
                    let header = header?;
                    let name = header.filename.to_str().context("Non-UTF8 RAR filename")?;
                    let unix = unix_paths.contains(name);
                    // Win32's NOT_CONTENT_INDEXED (0x2000) overlaps the Unix
                    // character-device mode bits. Interpret modes by host OS.
                    let special = if unix {
                        matches!(
                            header.file_attr & 0o170000,
                            0o120000 | 0o060000 | 0o020000 | 0o010000 | 0o140000
                        )
                    } else {
                        header.file_attr & 0x400 != 0
                    };
                    if header.is_encrypted() || header.is_split() || special {
                        bail!("Unsupported RAR entry");
                    }
                    add(name.into(), header.unpacked_size, header.is_directory())?;
                }
            }
            Container::Manifest => add("collection.json".into(), source.metadata()?.len(), false)?,
        }
        let manifest_only = matches!(container, Container::Manifest);
        let directories = components
            .into_values()
            .filter_map(|(path, dir)| dir.then_some(path))
            .collect();
        let digest = if matches!(container, Container::Directory) {
            let mut contents = Vec::new();
            for entry in entries.values() {
                contents.push((
                    &entry.path,
                    digest_file(&resolve_file(&source, &entry.path)?)?,
                ));
            }
            digest_bytes(&serde_json::to_vec(&contents)?)
        } else {
            digest_file(&source)?
        };
        // Construct a temporary shell to share the bounded container reader.
        let package = Self {
            source,
            container,
            digest,
            entries,
            directories,
            raw: Value::Null,
            collection: serde_json::from_value(serde_json::json!({"mods": []}))?,
            manifest_only,
        };
        Ok(package)
    }

    pub fn verify_unchanged(&self) -> Result<()> {
        let current = Self::open_payload(&self.source)?;
        if current.digest != self.digest {
            bail!("Collection package changed after planning");
        }
        Ok(())
    }

    pub fn read(&self, logical: &str, limit: u64) -> Result<Vec<u8>> {
        let entry = self
            .entries
            .get(&path_key(logical)?)
            .context("Required package entry is missing")?;
        if entry.size > limit {
            bail!("Package entry exceeds the configured size limit");
        }
        let bytes = match self.container {
            Container::Directory => read_bounded(
                &mut File::open(resolve_file(&self.source, &entry.path)?)?,
                limit,
            )?,
            Container::Manifest => read_bounded(&mut File::open(&self.source)?, limit)?,
            Container::Zip => {
                let mut archive = zip::ZipArchive::new(File::open(&self.source)?)?;
                let mut file = archive.by_name(&entry.original)?;
                read_bounded(&mut file, limit)?
            }
            Container::SevenZip => {
                let mut archive = sevenz_rust2::ArchiveReader::open(
                    &self.source,
                    sevenz_rust2::Password::empty(),
                )?;
                archive.set_thread_count(1);
                let mut found = None;
                archive.for_each_entries(|file, reader| {
                    if found.is_some() {
                        return Ok(false);
                    }
                    if file.name == entry.original {
                        found = Some(read_bounded(reader, limit));
                        return Ok(false);
                    }
                    // Solid blocks share a decoder. Earlier entries must be
                    // consumed even when only a later member is requested.
                    std::io::copy(reader, &mut std::io::sink())?;
                    Ok(true)
                })?;
                found.context("Required package entry disappeared")??
            }
            Container::Rar => {
                let mut archive = unrar::Archive::new(&self.source).open_for_processing()?;
                let mut found = None;
                while let Some(header) = archive.read_header()? {
                    if header.entry().filename.to_str() == Some(&entry.original) {
                        found = Some(header.read()?.0);
                        break;
                    }
                    archive = header.skip()?;
                }
                found.context("RAR entry disappeared")?
            }
        };
        if bytes.len() as u64 != entry.size {
            bail!("Package entry size changed during inspection");
        }
        Ok(bytes)
    }

    /// Extract each validated entry once, with bounded streaming instead of
    /// repeatedly decompressing solid archives into whole-member RAM buffers.
    pub fn extract_to(
        &self,
        root: &Path,
        cancellation: &tokio_util::sync::CancellationToken,
    ) -> Result<BTreeMap<String, PathBuf>> {
        if self
            .entries
            .values()
            .try_fold(0u64, |n, e| n.checked_add(e.size))
            .is_none_or(|n| n > 128 * 1024 * 1024 * 1024)
        {
            bail!("Artifact exceeds 128 GiB expanded budget");
        }
        if root.read_dir()?.next().is_some() {
            bail!("Extraction directory must be empty");
        }
        for directory in &self.directories {
            std::fs::create_dir_all(root.join(directory))?;
        }
        // Use the optimized system decoder when available. Every archive path
        // and entry type was validated above, and the destination is private
        // and empty. Check the resulting tree before exposing any output.
        if matches!(self.container, Container::SevenZip) && which::which("7z").is_ok() {
            let mut child = std::process::Command::new("7z")
                .args(["x", "-y", "-mmt=2"])
                .arg(format!("-o{}", root.display()))
                .arg("--")
                .arg(&self.source)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()?;
            loop {
                if cancellation.is_cancelled() {
                    let _ = child.kill();
                    let _ = child.wait();
                    bail!("Collection staging cancelled");
                }
                if let Some(status) = child.try_wait()? {
                    if !status.success() {
                        bail!("7z extraction failed");
                    }
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            let mut names = std::collections::BTreeSet::new();
            let mut extracted = BTreeMap::new();
            for entry in walkdir::WalkDir::new(root).min_depth(1).follow_links(false) {
                if cancellation.is_cancelled() {
                    bail!("Collection staging cancelled");
                }
                let entry = entry?;
                if entry.file_type().is_symlink()
                    || !(entry.file_type().is_dir() || entry.file_type().is_file())
                {
                    bail!("Extracted archive contains a link or special file");
                }
                let key = path_key(
                    entry
                        .path()
                        .strip_prefix(root)?
                        .to_str()
                        .context("Invalid extracted path")?,
                )?;
                if !names.insert(key.clone()) {
                    bail!("Extracted archive contains a case collision");
                }
                if entry.file_type().is_file() {
                    extracted.insert(key, entry.path().to_owned());
                }
            }
            if extracted.len() != self.entries.len() {
                bail!("Extraction differs from validated archive inventory");
            }
            return self
                .entries
                .values()
                .map(|entry| {
                    let path = extracted
                        .get(&path_key(&entry.path)?)
                        .context("Extracted archive path mismatch")?
                        .clone();
                    if path.metadata()?.len() != entry.size {
                        bail!("Extracted archive entry size mismatch");
                    }
                    Ok((entry.path.clone(), path))
                })
                .collect();
        }
        let mut outputs = BTreeMap::new();
        let mut copy = |entry: &PackageEntry, reader: &mut dyn Read| -> Result<()> {
            if cancellation.is_cancelled() {
                bail!("Collection staging cancelled");
            }
            let target = root.join(&entry.path);
            std::fs::create_dir_all(target.parent().context("Missing extraction parent")?)?;
            let mut out = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&target)?;
            let mut left = entry.size;
            let mut buffer = [0u8; 128 * 1024];
            loop {
                if cancellation.is_cancelled() {
                    bail!("Collection staging cancelled");
                }
                let n = reader.read(&mut buffer)?;
                if n == 0 {
                    break;
                }
                if n as u64 > left {
                    bail!("Expanded archive entry exceeded its declared size");
                }
                std::io::Write::write_all(&mut out, &buffer[..n])?;
                left -= n as u64;
            }
            if left != 0 {
                bail!("Truncated archive entry");
            }
            outputs.insert(entry.path.clone(), target);
            Ok(())
        };
        match self.container {
            Container::Zip => {
                let mut archive = zip::ZipArchive::new(File::open(&self.source)?)?;
                for entry in self.entries.values() {
                    copy(entry, &mut archive.by_name(&entry.original)?)?;
                }
            }
            Container::SevenZip => {
                let mut archive = sevenz_rust2::ArchiveReader::open(
                    &self.source,
                    sevenz_rust2::Password::empty(),
                )?;
                archive.set_thread_count(2);
                let mut failure = None;
                archive.for_each_entries(|entry, reader| {
                    if entry.is_directory {
                        return Ok(true);
                    }
                    let result = (|| -> Result<()> {
                        let expected = self
                            .entries
                            .get(&path_key(&entry.name)?)
                            .context("Unplanned archive entry")?;
                        copy(expected, reader)
                    })();
                    if let Err(e) = result {
                        failure = Some(e);
                        return Ok(false);
                    }
                    Ok(true)
                })?;
                if let Some(error) = failure {
                    return Err(error);
                }
            }
            Container::Directory => {
                for entry in self.entries.values() {
                    copy(
                        entry,
                        &mut File::open(resolve_file(&self.source, &entry.path)?)?,
                    )?;
                }
            }
            Container::Rar => {
                let mut archive = unrar::Archive::new(&self.source).open_for_processing()?;
                while let Some(header) = archive.read_header()? {
                    if cancellation.is_cancelled() {
                        bail!("Collection staging cancelled");
                    }
                    if header.entry().is_directory() {
                        archive = header.skip()?;
                        continue;
                    }
                    let key = path_key(
                        header
                            .entry()
                            .filename
                            .to_str()
                            .context("Invalid RAR path")?,
                    )?;
                    let entry = self.entries.get(&key).context("Unplanned RAR entry")?;
                    let target = root.join(&entry.path);
                    std::fs::create_dir_all(target.parent().context("Missing extraction parent")?)?;
                    if target.exists() {
                        bail!("RAR output collision");
                    }
                    archive = header.extract_to(&target)?;
                    if !target.symlink_metadata()?.is_file()
                        || target.metadata()?.len() != entry.size
                    {
                        bail!("RAR output differs from validated header");
                    }
                    outputs.insert(entry.path.clone(), target);
                }
            }
            Container::Manifest => bail!("Unsupported artifact container"),
        }
        if outputs.len() != self.entries.len() {
            bail!("Archive extraction missed required entries");
        }
        Ok(outputs)
    }
}
