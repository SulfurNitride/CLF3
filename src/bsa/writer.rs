//! BSA archive creation

use anyhow::{bail, Context, Result};
use ba2::tes4::{
    Archive, ArchiveFlags, ArchiveKey, ArchiveOptions, ArchiveTypes, Directory, DirectoryKey,
    File as BsaFile, FileCompressionOptions, FileReadOptions, Version,
};
use ba2::CompressableFrom;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs;
use std::io::BufWriter;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tracing::info;

use super::{default_flags_fo3, default_flags_oblivion, detect_types, detect_version};

/// Helper struct to hold file data with lifetime for BSA creation
struct FileEntry {
    dir_path: String,
    file_name: String,
    data: Vec<u8>,
}

impl FileEntry {
    /// Create a BSA file, optionally compressing it
    fn as_bsa_file(&self, version: Version, should_compress: bool) -> Result<BsaFile<'static>> {
        // Create an uncompressed file from our raw data
        let uncompressed = BsaFile::from_decompressed(self.data.clone().into_boxed_slice());

        if should_compress {
            // Compress the file using ba2's compress method
            let compression_options = FileCompressionOptions::builder().version(version).build();

            uncompressed
                .compress(&compression_options)
                .with_context(|| {
                    format!("Failed to compress: {}/{}", self.dir_path, self.file_name)
                })
        } else {
            Ok(uncompressed)
        }
    }
}

/// Builder for creating BSA archives
pub struct BsaBuilder {
    /// Files organized by directory -> filename -> data
    files: HashMap<String, HashMap<String, Vec<u8>>>,
    flags: ArchiveFlags,
    types: ArchiveTypes,
    version: Version,
}

impl BsaBuilder {
    pub fn new() -> Self {
        Self {
            files: HashMap::new(),
            flags: default_flags_fo3(),
            types: ArchiveTypes::empty(),
            version: Version::v104,
        }
    }

    /// Create builder with settings detected from BSA name
    pub fn from_name(name: &str) -> Self {
        let version = detect_version(name);
        let types = detect_types(name);
        let flags = if version == Version::v103 {
            default_flags_oblivion()
        } else {
            default_flags_fo3()
        };

        Self {
            files: HashMap::new(),
            flags,
            types,
            version,
        }
    }

    /// Set archive flags
    pub fn with_flags(mut self, flags: ArchiveFlags) -> Self {
        self.flags = flags;
        self
    }

    /// Set archive types
    pub fn with_types(mut self, types: ArchiveTypes) -> Self {
        self.types = types;
        self
    }

    /// Set BSA version
    pub fn with_version(mut self, version: Version) -> Self {
        self.version = version;
        self
    }

    /// Enable or disable compression
    pub fn with_compression(mut self, compress: bool) -> Self {
        if compress {
            self.flags |= ArchiveFlags::COMPRESSED;
        } else {
            self.flags &= !ArchiveFlags::COMPRESSED;
        }
        self
    }

    /// Add a file to the archive
    pub fn add_file(&mut self, path: &str, data: Vec<u8>) {
        // Normalize: forward slashes, strip leading slash
        let normalized = path.replace('\\', "/");
        let normalized = normalized.trim_start_matches('/');

        let (dir_path, file_name) = if let Some(idx) = normalized.rfind('/') {
            (
                normalized[..idx].to_string(),
                normalized[idx + 1..].to_string(),
            )
        } else {
            (".".to_string(), normalized.to_string())
        };

        self.files
            .entry(dir_path)
            .or_default()
            .insert(file_name, data);
    }

    /// Get number of files
    pub fn file_count(&self) -> usize {
        self.files.values().map(|d| d.len()).sum()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.file_count() == 0
    }

    /// Build and write the BSA to disk
    pub fn build(self, output_path: &Path) -> Result<()> {
        if self.is_empty() {
            bail!("Cannot create empty BSA archive");
        }

        let file_count = self.file_count();
        let total_size: u64 = self
            .files
            .values()
            .flat_map(|files| files.values())
            .map(|data| data.len() as u64)
            .sum();

        info!(
            "Building BSA: {} ({} files, {} MB, version {:?}, flags {:?})",
            output_path.display(),
            file_count,
            total_size / 1_000_000,
            self.version,
            self.flags
        );

        // Check if we should compress files
        let should_compress = self.flags.contains(ArchiveFlags::COMPRESSED);

        // Flatten to FileEntry structs that own their data
        let entries: Vec<FileEntry> = self
            .files
            .into_iter()
            .flat_map(|(dir_path, files)| {
                files.into_iter().map(move |(file_name, data)| FileEntry {
                    dir_path: dir_path.clone(),
                    file_name,
                    data,
                })
            })
            .collect();

        // Process files in parallel - create and compress BsaFile entries
        let version = self.version;
        let processed: Result<Vec<(String, String, BsaFile)>> = entries
            .par_iter()
            .map(|entry| {
                let file = entry.as_bsa_file(version, should_compress)?;
                Ok((entry.dir_path.clone(), entry.file_name.clone(), file))
            })
            .collect();

        let processed = processed?;

        // Build archive
        let mut archive = Archive::new();
        for (dir_path, file_name, file) in processed {
            let archive_key = ArchiveKey::from(dir_path.as_bytes());
            let directory_key = DirectoryKey::from(file_name.as_bytes());

            match archive.get_mut(&archive_key) {
                Some(directory) => {
                    directory.insert(directory_key, file);
                }
                None => {
                    let mut directory = Directory::default();
                    directory.insert(directory_key, file);
                    archive.insert(archive_key, directory);
                }
            }
        }

        let options = ArchiveOptions::builder()
            .version(self.version)
            .flags(self.flags)
            .types(self.types)
            .build();

        // Create parent directory
        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Write archive
        let file = fs::File::create(output_path)
            .with_context(|| format!("Failed to create BSA: {}", output_path.display()))?;
        let mut writer = BufWriter::new(file);

        archive
            .write(&mut writer, &options)
            .with_context(|| format!("Failed to write BSA: {}", output_path.display()))?;

        info!("Created BSA: {}", output_path.display());
        Ok(())
    }
}

impl Default for BsaBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Manages multiple BSA archives being built during installation
pub struct BsaWriterManager {
    /// Builders keyed by location index
    builders: HashMap<i32, (String, Arc<Mutex<BsaBuilder>>)>,
}

impl BsaWriterManager {
    pub fn new() -> Self {
        Self {
            builders: HashMap::new(),
        }
    }

    /// Register a BSA target
    pub fn register(
        &mut self,
        location_index: i32,
        bsa_name: &str,
        version: Option<Version>,
        flags: Option<ArchiveFlags>,
        types: Option<ArchiveTypes>,
    ) {
        let mut builder = BsaBuilder::from_name(bsa_name);

        if let Some(v) = version {
            builder = builder.with_version(v);
        }
        if let Some(f) = flags {
            builder = builder.with_flags(f);
        }
        if let Some(t) = types {
            builder = builder.with_types(t);
        }

        info!(
            "Registered BSA target: [{}] = {} ({:?})",
            location_index, bsa_name, builder.version
        );

        self.builders.insert(
            location_index,
            (bsa_name.to_string(), Arc::new(Mutex::new(builder))),
        );
    }

    /// Check if a location is a registered BSA
    pub fn is_bsa(&self, location_index: i32) -> bool {
        self.builders.contains_key(&location_index)
    }

    /// Add a file to a BSA (thread-safe)
    pub fn add_file(&self, location_index: i32, path: &str, data: Vec<u8>) -> Result<()> {
        let (_, builder) = self
            .builders
            .get(&location_index)
            .ok_or_else(|| anyhow::anyhow!("Location {} is not a BSA target", location_index))?;

        builder
            .lock()
            .expect("BSA builder lock poisoned")
            .add_file(path, data);
        Ok(())
    }

    /// Write all BSAs in parallel
    pub fn write_all_parallel(&self, dest_dir: &Path) -> Result<(usize, usize)> {
        let non_empty: Vec<_> = self
            .builders
            .iter()
            .filter(|(_, (_, b))| !b.lock().expect("BSA builder lock poisoned").is_empty())
            .collect();

        if non_empty.is_empty() {
            info!("No BSA files to create");
            return Ok((0, 0));
        }

        info!("Writing {} BSA archives in parallel", non_empty.len());

        let results: Vec<Result<()>> = non_empty
            .par_iter()
            .map(|(_, (name, builder))| {
                let output_path = dest_dir.join(name);
                let builder_data = {
                    let guard = builder.lock().expect("BSA builder lock poisoned");
                    // Clone the data we need for building
                    BsaBuilder {
                        files: guard.files.clone(),
                        flags: guard.flags,
                        types: guard.types,
                        version: guard.version,
                    }
                };
                builder_data.build(&output_path)
            })
            .collect();

        let success = results.iter().filter(|r| r.is_ok()).count();
        let failed = results.iter().filter(|r| r.is_err()).count();

        // Log any errors
        for result in &results {
            if let Err(e) = result {
                tracing::error!("BSA creation failed: {}", e);
            }
        }

        info!(
            "BSA creation complete: {}/{} succeeded",
            success,
            non_empty.len()
        );

        Ok((success, failed))
    }

    /// Write all BSAs with progress callback
    pub fn write_all_with_progress<F>(&self, dest_dir: &Path, progress: F) -> Result<(usize, usize)>
    where
        F: Fn(usize, usize, &str) + Send + Sync,
    {
        let non_empty: Vec<_> = self
            .builders
            .iter()
            .filter(|(_, (_, b))| !b.lock().expect("BSA builder lock poisoned").is_empty())
            .collect();

        if non_empty.is_empty() {
            return Ok((0, 0));
        }

        let total = non_empty.len();
        let completed = std::sync::atomic::AtomicUsize::new(0);

        let results: Vec<Result<()>> = non_empty
            .par_iter()
            .map(|(_, (name, builder))| {
                let output_path = dest_dir.join(name);
                let builder_data = {
                    let guard = builder.lock().expect("BSA builder lock poisoned");
                    BsaBuilder {
                        files: guard.files.clone(),
                        flags: guard.flags,
                        types: guard.types,
                        version: guard.version,
                    }
                };

                let result = builder_data.build(&output_path);

                let current = completed.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                progress(current, total, name);

                result
            })
            .collect();

        let success = results.iter().filter(|r| r.is_ok()).count();
        let failed = results.len() - success;

        Ok((success, failed))
    }
}

impl Default for BsaWriterManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_builder_from_name() {
        let builder = BsaBuilder::from_name("Fallout - Textures.bsa");
        assert_eq!(builder.version, Version::v104);
        assert_eq!(builder.types, ArchiveTypes::TEXTURES);

        let builder = BsaBuilder::from_name("Oblivion - Meshes.bsa");
        assert_eq!(builder.version, Version::v103);
        assert_eq!(builder.types, ArchiveTypes::MESHES);
    }

    #[test]
    fn test_add_files() {
        let mut builder = BsaBuilder::new();
        builder.add_file("textures/test.dds", vec![1, 2, 3]);
        builder.add_file("textures/sub/test2.dds", vec![4, 5, 6]);

        assert_eq!(builder.file_count(), 2);
        assert!(!builder.is_empty());
    }
}
