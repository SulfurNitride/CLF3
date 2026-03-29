//! BA2 (Fallout 4/Starfield) archive creation
//!
//! Provides write support for FO4 format BA2 files (Fallout 4, Fallout 76, Starfield).

use anyhow::{bail, Context, Result};
use ba2::fo4::{
    Archive, ArchiveKey, ArchiveOptionsBuilder, Chunk, ChunkCompressionOptions,
    CompressionFormat as Ba2CrateCompression, CompressionLevel, File as Ba2File,
    FileReadOptionsBuilder, Format, Version,
};
use ba2::prelude::*;
use ba2::{CompressionResult, Copied};
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs;
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use tracing::info;

/// Compression format for BA2 archives
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Ba2CompressionFormat {
    /// No compression
    #[default]
    None,
    /// zlib compression (Fallout 4, Fallout 76)
    Zlib,
    /// LZ4 compression (Starfield)
    Lz4,
}

/// Archive format variant
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Ba2Format {
    /// General archive (GNRL) - for meshes, scripts, etc.
    #[default]
    General,
    /// DirectX 10 textures (DX10) - for DDS textures
    DX10,
}

/// BA2 archive version
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Ba2Version {
    /// Version 1 - Old-gen Fallout 4, Fallout 76
    V1,
    /// Version 7 - Next-gen Fallout 4
    #[default]
    V7,
    /// Version 8 - Next-gen Fallout 4
    V8,
    /// Version 2 - Starfield
    V2,
    /// Version 3 - Starfield
    V3,
}

impl Ba2Version {
    /// Create from numeric version in modlist
    pub fn from_u32(v: u32) -> Self {
        match v {
            1 => Ba2Version::V1,
            2 => Ba2Version::V2,
            3 => Ba2Version::V3,
            7 => Ba2Version::V7,
            8 => Ba2Version::V8,
            _ => {
                tracing::warn!("Unknown BA2 version {}, defaulting to v7", v);
                Ba2Version::V7
            }
        }
    }

    /// Convert to ba2 crate Version
    fn to_crate_version(self) -> Version {
        match self {
            Ba2Version::V1 => Version::v1,
            Ba2Version::V2 => Version::v2,
            Ba2Version::V3 => Version::v3,
            Ba2Version::V7 => Version::v7,
            Ba2Version::V8 => Version::v8,
        }
    }
}

/// File entry that reads from disk on demand instead of holding data in memory
struct FileEntry {
    archive_path: String,
    disk_path: PathBuf,
}

/// Builder for creating BA2 archives
///
/// Stores file paths on disk instead of raw data. Files are read one at a time
/// during build(), keeping peak memory at ~1 file per rayon thread instead of
/// the entire archive's worth of data.
pub struct Ba2Builder {
    /// Files: archive_path -> disk path (no data loaded)
    files: HashMap<String, PathBuf>,
    /// Archive format (General or DX10)
    format: Ba2Format,
    /// Compression format
    compression: Ba2CompressionFormat,
    /// Whether to include string table
    strings: bool,
    /// BA2 version (v1 for OG FO4, v7/v8 for NG FO4, v2/v3 for Starfield)
    version: Ba2Version,
}

impl Ba2Builder {
    pub fn new() -> Self {
        Self {
            files: HashMap::new(),
            format: Ba2Format::General,
            compression: Ba2CompressionFormat::None,
            strings: true,
            version: Ba2Version::V7,
        }
    }

    /// Create builder with settings detected from BA2 name
    pub fn from_name(name: &str) -> Self {
        let name_lower = name.to_lowercase();

        // Texture archives need DX10 format for proper texture headers
        // General archives (meshes, scripts, etc.) use GNRL format
        // Check the archive suffix, not the full path - "Main.ba2" contains meshes,
        // "Textures.ba2" contains DDS textures, even if mod folder has "texture" in name
        let is_texture_archive = {
            // Get just the filename part
            let filename = name_lower.rsplit(['/', '\\']).next().unwrap_or(&name_lower);
            // Check if it ends with texture patterns: "textures.ba2", "textures1.ba2", etc.
            filename.contains(" - textures") ||
            filename.starts_with("textures") ||
            // Also catch patterns like "modname - textures.ba2" without space
            (filename.contains("textures") && !filename.contains(" - main") && !filename.contains("_main"))
        };

        let format = if is_texture_archive {
            Ba2Format::DX10
        } else {
            Ba2Format::General
        };

        // Default to zlib compression for FO4
        let compression = Ba2CompressionFormat::Zlib;

        Self {
            files: HashMap::new(),
            format,
            compression,
            strings: true,
            version: Ba2Version::V7,
        }
    }

    /// Set BA2 version (v1 for OG FO4, v7/v8 for NG FO4, v2/v3 for Starfield)
    pub fn with_version(mut self, version: Ba2Version) -> Self {
        self.version = version;
        self
    }

    /// Set archive format
    pub fn with_format(mut self, format: Ba2Format) -> Self {
        self.format = format;
        self
    }

    /// Set compression format
    pub fn with_compression(mut self, compression: Ba2CompressionFormat) -> Self {
        self.compression = compression;
        self
    }

    /// Enable or disable string table
    pub fn with_strings(mut self, strings: bool) -> Self {
        self.strings = strings;
        self
    }

    /// Register a staged file for inclusion. The file is NOT read — only the path is stored.
    pub fn add_file(&mut self, path: &str, disk_path: PathBuf) {
        let normalized = path.replace('\\', "/");
        let normalized = normalized.trim_start_matches('/').to_string();
        self.files.insert(normalized, disk_path);
    }

    /// Add a file with data already in memory (for callers that already have it)
    pub fn add_file_data(&mut self, path: &str, data: Vec<u8>, staging_dir: &Path) -> Result<()> {
        let normalized = path.replace('\\', "/");
        let normalized = normalized.trim_start_matches('/').to_string();
        // Write to staging dir so we can read it back on demand
        let disk_path = staging_dir.join(&normalized);
        if let Some(parent) = disk_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&disk_path, &data)?;
        self.files.insert(normalized, disk_path);
        Ok(())
    }

    /// Get number of files
    pub fn file_count(&self) -> usize {
        self.files.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    /// Build and write the BA2 to disk.
    ///
    /// Reads files from disk on demand during parallel compression.
    /// Peak memory ≈ num_rayon_threads * largest_file_size (not total archive size).
    pub fn build(self, output_path: &Path) -> Result<()> {
        if self.is_empty() {
            bail!("Cannot create empty BA2 archive");
        }

        let file_count = self.file_count();

        info!(
            "Building BA2: {} ({} files, format {:?}, compression {:?})",
            output_path.display(),
            file_count,
            self.format,
            self.compression
        );

        // For DX10 (texture) archives, we need special handling
        if self.format == Ba2Format::DX10 {
            return self.build_dx10(output_path);
        }

        // Flatten to FileEntry structs — no data loaded yet, just paths
        let entries: Vec<FileEntry> = self
            .files
            .into_iter()
            .map(|(archive_path, disk_path)| FileEntry {
                archive_path,
                disk_path,
            })
            .collect();

        let compress = self.compression != Ba2CompressionFormat::None;

        // Read + compress files in parallel. Each thread reads one file at a time.
        let archive_entries: Result<Vec<(ArchiveKey<'static>, Ba2File<'static>)>> = entries
            .into_par_iter()
            .map(|entry| {
                let data = fs::read(&entry.disk_path).with_context(|| {
                    format!("Failed to read staged file: {}", entry.disk_path.display())
                })?;

                let chunk = Chunk::from_decompressed(data.into_boxed_slice());

                let chunk = if compress {
                    let options = ChunkCompressionOptions::default();
                    match chunk.compress(&options) {
                        Ok(compressed) => compressed,
                        Err(_) => chunk,
                    }
                } else {
                    chunk
                };

                let file: Ba2File = [chunk].into_iter().collect();
                let key: ArchiveKey = entry.archive_path.as_bytes().into();

                Ok((key, file))
            })
            .collect();

        let archive_entries = archive_entries?;

        // Build archive from entries
        let archive: Archive = archive_entries.into_iter().collect();

        // Configure options with version from modlist
        let options = ArchiveOptionsBuilder::default()
            .version(self.version.to_crate_version())
            .strings(self.strings)
            .build();

        // Create parent directory
        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Write archive
        let file = fs::File::create(output_path)
            .with_context(|| format!("Failed to create BA2: {}", output_path.display()))?;
        let mut writer = BufWriter::with_capacity(65536, file);

        archive
            .write(&mut writer, &options)
            .with_context(|| format!("Failed to write BA2: {}", output_path.display()))?;

        info!("Created BA2: {}", output_path.display());
        Ok(())
    }

    /// Build a DX10 (texture) archive
    ///
    /// DX10 archives require special handling for DDS textures.
    /// The ba2 crate's File::read() with Format::DX10 properly parses DDS files:
    /// - Extracts texture metadata (width, height, format, mip levels)
    /// - Strips DDS header
    /// - Creates proper mip-level chunks for streaming
    fn build_dx10(self, output_path: &Path) -> Result<()> {
        let file_count = self.file_count();
        let compress = self.compression != Ba2CompressionFormat::None;

        // Flatten to FileEntry structs — no data loaded
        let entries: Vec<FileEntry> = self
            .files
            .into_iter()
            .map(|(archive_path, disk_path)| FileEntry {
                archive_path,
                disk_path,
            })
            .collect();

        // Build read options for DX10 format
        let read_options = FileReadOptionsBuilder::new()
            .format(Format::DX10)
            .compression_format(Ba2CrateCompression::Zip)
            .compression_level(CompressionLevel::FO4)
            .compression_result(if compress {
                CompressionResult::Compressed
            } else {
                CompressionResult::Decompressed
            })
            .build();

        // Read + compress files in parallel — one file per rayon thread
        let archive_entries: Result<Vec<(ArchiveKey<'static>, Ba2File<'static>)>> = entries
            .into_par_iter()
            .map(|entry| {
                let data = fs::read(&entry.disk_path).with_context(|| {
                    format!("Failed to read staged file: {}", entry.disk_path.display())
                })?;

                let file = Ba2File::read(Copied(&data), &read_options)
                    .with_context(|| format!("Failed to parse DDS texture: {}", entry.archive_path))?;

                let key: ArchiveKey = entry.archive_path.as_bytes().into();
                Ok((key, file))
            })
            .collect();

        let archive_entries = archive_entries?;
        let archive: Archive = archive_entries.into_iter().collect();

        // DX10 format requires format flag set in archive options
        let options = ArchiveOptionsBuilder::default()
            .version(self.version.to_crate_version())
            .format(Format::DX10)
            .compression_format(Ba2CrateCompression::Zip)
            .strings(self.strings)
            .build();

        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = fs::File::create(output_path)
            .with_context(|| format!("Failed to create BA2: {}", output_path.display()))?;
        let mut writer = BufWriter::with_capacity(65536, file);

        archive
            .write(&mut writer, &options)
            .with_context(|| format!("Failed to write BA2: {}", output_path.display()))?;

        info!(
            "Created DX10 BA2: {} ({} files)",
            output_path.display(),
            file_count
        );
        Ok(())
    }
}

impl Default for Ba2Builder {
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
        let builder = Ba2Builder::from_name("Fallout4 - Textures.ba2");
        assert_eq!(builder.format, Ba2Format::DX10);

        let builder = Ba2Builder::from_name("Fallout4 - Main.ba2");
        assert_eq!(builder.format, Ba2Format::General);
    }

    #[test]
    fn test_create_simple_ba2() -> Result<()> {
        let dir = tempdir()?;
        let output = dir.path().join("test.ba2");

        let mut builder = Ba2Builder::new().with_compression(Ba2CompressionFormat::None);

        // Write staged files to disk
        let f1 = dir.path().join("hello.txt");
        let f2 = dir.path().join("world.txt");
        fs::write(&f1, b"Hello world!")?;
        fs::write(&f2, b"World!")?;

        builder.add_file("test/hello.txt", f1);
        builder.add_file("test/sub/world.txt", f2);

        builder.build(&output)?;

        // Verify the file was created
        assert!(output.exists());

        // Try to read it back using the path
        let (archive, _) =
            Archive::read(output.as_path()).with_context(|| "Failed to read created BA2")?;

        assert_eq!(archive.len(), 2);

        Ok(())
    }
}
