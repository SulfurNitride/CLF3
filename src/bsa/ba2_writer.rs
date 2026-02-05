//! BA2 (Fallout 4/Starfield) archive creation
//!
//! Provides write support for FO4 format BA2 files (Fallout 4, Fallout 76, Starfield).

use anyhow::{bail, Context, Result};
use ba2::fo4::{
    Archive, ArchiveKey, ArchiveOptionsBuilder,
    Chunk, ChunkCompressionOptions, File as Ba2File,
    FileReadOptionsBuilder, Format, CompressionFormat as Ba2CrateCompression,
    CompressionLevel, Version,
};
use ba2::prelude::*;
use ba2::{Copied, CompressionResult};
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs;
use std::io::BufWriter;
use std::path::Path;
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

/// Builder for creating BA2 archives
pub struct Ba2Builder {
    /// Files organized by path -> data
    files: HashMap<String, Vec<u8>>,
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

    /// Add a file to the archive
    pub fn add_file(&mut self, path: &str, data: Vec<u8>) {
        // Normalize: forward slashes, strip leading slash
        let normalized = path.replace('\\', "/");
        let normalized = normalized.trim_start_matches('/').to_string();
        self.files.insert(normalized, data);
    }

    /// Get number of files
    pub fn file_count(&self) -> usize {
        self.files.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    /// Build and write the BA2 to disk
    pub fn build(self, output_path: &Path) -> Result<()> {
        if self.is_empty() {
            bail!("Cannot create empty BA2 archive");
        }

        let file_count = self.file_count();
        let total_size: u64 = self.files.values().map(|data| data.len() as u64).sum();

        info!(
            "Building BA2: {} ({} files, {} MB, format {:?}, compression {:?})",
            output_path.display(),
            file_count,
            total_size / 1_000_000,
            self.format,
            self.compression
        );

        // For DX10 (texture) archives, we need special handling
        // For now, only support General archives
        if self.format == Ba2Format::DX10 {
            return self.build_dx10(output_path);
        }

        // Build archive entries in parallel
        let entries: Vec<(String, Vec<u8>)> = self.files.into_iter().collect();

        let archive_entries: Result<Vec<(ArchiveKey<'static>, Ba2File<'static>)>> = entries
            .par_iter()
            .map(|(path, data)| {
                // Create chunk from data
                let chunk = Chunk::from_decompressed(data.clone().into_boxed_slice());

                // Optionally compress the chunk
                let chunk = if self.compression != Ba2CompressionFormat::None {
                    let options = ChunkCompressionOptions::default();
                    match chunk.compress(&options) {
                        Ok(compressed) => compressed,
                        Err(_) => chunk, // Fall back to uncompressed if compression fails
                    }
                } else {
                    chunk
                };

                // Create file from chunk
                let file: Ba2File = [chunk].into_iter().collect();

                // Create key from path
                let key: ArchiveKey = path.as_bytes().into();

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
        let mut writer = BufWriter::new(file);

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
        let entries: Vec<(String, Vec<u8>)> = self.files.into_iter().collect();

        // Build read options for DX10 format
        // This tells ba2 to parse DDS files and create proper texture chunks
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

        let archive_entries: Result<Vec<(ArchiveKey<'static>, Ba2File<'static>)>> = entries
            .par_iter()
            .map(|(path, data)| {
                // Use ba2's DX10 reader to properly parse the DDS file
                // This extracts metadata, creates DX10 header, and chunks mip levels
                // Use Copied to make a deep copy so the File doesn't borrow from entries
                let file = Ba2File::read(Copied(data), &read_options)
                    .with_context(|| format!("Failed to parse DDS texture: {}", path))?;

                let key: ArchiveKey = path.as_bytes().into();
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
        let mut writer = BufWriter::new(file);

        archive
            .write(&mut writer, &options)
            .with_context(|| format!("Failed to write BA2: {}", output_path.display()))?;

        info!("Created DX10 BA2: {} ({} files)", output_path.display(), file_count);
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

        let mut builder = Ba2Builder::new()
            .with_compression(Ba2CompressionFormat::None);
        builder.add_file("test/hello.txt", b"Hello world!".to_vec());
        builder.add_file("test/sub/world.txt", b"World!".to_vec());

        builder.build(&output)?;

        // Verify the file was created
        assert!(output.exists());

        // Try to read it back using the path
        let (archive, _) = Archive::read(output.as_path())
            .with_context(|| "Failed to read created BA2")?;

        assert_eq!(archive.len(), 2);

        Ok(())
    }
}
