//! BA2 (Fallout 4/Starfield) archive creation
//!
//! Provides write support for FO4 format BA2 files (Fallout 4, Fallout 76, Starfield).

use anyhow::{bail, Context, Result};
use ba2::fo4::{
    Archive, ArchiveKey, ArchiveOptionsBuilder, Chunk, ChunkCompressionOptions,
    CompressionFormat as Ba2CrateCompression, CompressionLevel, File as Ba2File, FileHeader,
    FileReadOptionsBuilder, Format, Version,
};
use ba2::prelude::*;
use ba2::{CompressableFrom, CompressionResult, Copied};
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs;
use std::io::BufWriter;
use std::ops::Range;
use std::path::{Path, PathBuf};
use tracing::info;

use super::disk_spool::{DiskSpool, DiskSpoolWriter};

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

impl Ba2CompressionFormat {
    fn to_crate_format(self) -> Ba2CrateCompression {
        match self {
            // The compression format is irrelevant for uncompressed chunks,
            // but ZIP is the compatible archive-level value for FO4 BA2s.
            Self::None | Self::Zlib => Ba2CrateCompression::Zip,
            Self::Lz4 => Ba2CrateCompression::LZ4,
        }
    }
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

/// Small, in-memory description of a chunk whose bytes live in `DiskSpool`.
struct SpooledChunk {
    range: Range<usize>,
    decompressed_len: Option<usize>,
    mips: Option<std::ops::RangeInclusive<u16>>,
}

/// The metadata retained for a compressed BA2 file. Unlike `Ba2File<'static>`,
/// this does not own the compressed payload.
struct SpooledBa2File {
    key: ArchiveKey<'static>,
    header: FileHeader,
    chunks: Vec<SpooledChunk>,
}

impl SpooledBa2File {
    fn from_file(
        archive_path: String,
        file: Ba2File<'static>,
        spool: &DiskSpoolWriter,
    ) -> Result<Self> {
        let ranges = spool.append(file.iter().map(Chunk::as_bytes))?;
        let chunks = file
            .iter()
            .zip(ranges)
            .map(|(chunk, range)| SpooledChunk {
                range,
                decompressed_len: chunk.decompressed_len(),
                mips: chunk.mips.clone(),
            })
            .collect();

        Ok(Self {
            key: ArchiveKey::from(archive_path.as_bytes()),
            header: file.header.clone(),
            chunks,
        })
    }

    fn materialize<'spool>(self, spool: &'spool [u8]) -> (ArchiveKey<'static>, Ba2File<'spool>) {
        let chunks = self.chunks.into_iter().map(|metadata| {
            let bytes = &spool[metadata.range];
            let mut chunk = match metadata.decompressed_len {
                Some(len) => Chunk::from_compressed(bytes, len),
                None => Chunk::from_decompressed(bytes),
            };
            chunk.mips = metadata.mips;
            chunk
        });
        let mut file: Ba2File<'spool> = chunks.collect();
        file.header = self.header;
        (self.key, file)
    }
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
    /// Files are compressed in parallel and immediately moved to a disk-backed
    /// spool. Peak anonymous memory is therefore bounded by active workers,
    /// rather than growing to the complete compressed archive size.
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

        let spool = DiskSpool::new_near(output_path)?;
        let spool_writer = spool.writer();
        let compression_options = ChunkCompressionOptions::builder()
            .compression_format(self.compression.to_crate_format())
            .compression_level(CompressionLevel::FO4)
            .build();

        // Compress at full rayon parallelism, but spill each finished file
        // immediately. Only one file per active worker remains anonymous RAM.
        let archive_entries: Result<Vec<SpooledBa2File>> = entries
            .into_par_iter()
            .map(|entry| {
                let data = fs::read(&entry.disk_path).with_context(|| {
                    format!("Failed to read staged file: {}", entry.disk_path.display())
                })?;

                let chunk = Chunk::from_decompressed(data.into_boxed_slice());

                let chunk = if compress {
                    match chunk.compress(&compression_options) {
                        Ok(compressed) => compressed,
                        Err(_) => chunk,
                    }
                } else {
                    chunk
                };

                let file: Ba2File = [chunk].into_iter().collect();
                SpooledBa2File::from_file(entry.archive_path, file, &spool_writer)
            })
            .collect();

        let archive_entries = archive_entries?;
        let mapping = spool.map()?;
        let archive: Archive = archive_entries
            .into_iter()
            .map(|entry| entry.materialize(&mapping))
            .collect();

        // Configure options with version from modlist
        let options = ArchiveOptionsBuilder::default()
            .version(self.version.to_crate_version())
            .strings(self.strings)
            .compression_format(self.compression.to_crate_format())
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
            .compression_format(self.compression.to_crate_format())
            .compression_level(CompressionLevel::FO4)
            .compression_result(if compress {
                CompressionResult::Compressed
            } else {
                CompressionResult::Decompressed
            })
            .build();

        let spool = DiskSpool::new_near(output_path)?;
        let spool_writer = spool.writer();

        // Read + compress files in parallel, spilling each completed texture
        // into the shared disk-backed spool before accepting more work.
        let archive_entries: Result<Vec<SpooledBa2File>> = entries
            .into_par_iter()
            .map(|entry| {
                let data = fs::read(&entry.disk_path).with_context(|| {
                    format!("Failed to read staged file: {}", entry.disk_path.display())
                })?;

                let file = Ba2File::read(Copied(&data), &read_options).with_context(|| {
                    format!("Failed to parse DDS texture: {}", entry.archive_path)
                })?;

                SpooledBa2File::from_file(entry.archive_path, file, &spool_writer)
            })
            .collect();

        let archive_entries = archive_entries?;
        let mapping = spool.map()?;
        let archive: Archive = archive_entries
            .into_iter()
            .map(|entry| entry.materialize(&mapping))
            .collect();

        // DX10 format requires format flag set in archive options
        let options = ArchiveOptionsBuilder::default()
            .version(self.version.to_crate_version())
            .format(Format::DX10)
            .compression_format(self.compression.to_crate_format())
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
    use ba2::fo4::FileWriteOptions;
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

        let mut builder = Ba2Builder::new().with_compression(Ba2CompressionFormat::Zlib);

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
        let (archive, options) =
            Archive::read(output.as_path()).with_context(|| "Failed to read created BA2")?;

        assert_eq!(archive.len(), 2);

        let key = ArchiveKey::from(b"test/hello.txt".as_slice());
        let archived = archive.get(&key).context("hello.txt missing from BA2")?;
        let write_options: FileWriteOptions = options.into();
        let mut restored = Vec::new();
        archived.write(&mut restored, &write_options)?;
        assert_eq!(restored, b"Hello world!");

        // The disk spool changes storage ownership only. Verify that it still
        // produces exactly the bytes emitted by the crate's original fully
        // in-memory construction path (important for Wabbajack output hashes).
        let compression = ChunkCompressionOptions::builder()
            .compression_format(Ba2CrateCompression::Zip)
            .compression_level(CompressionLevel::FO4)
            .build();
        let reference_entries = [
            ("test/hello.txt", b"Hello world!".as_slice()),
            ("test/sub/world.txt", b"World!".as_slice()),
        ]
        .into_iter()
        .map(|(path, data)| {
            let chunk = Chunk::from_decompressed(data.to_vec().into_boxed_slice())
                .compress(&compression)?;
            let file: Ba2File = [chunk].into_iter().collect();
            Ok((ArchiveKey::from(path.as_bytes()), file))
        })
        .collect::<Result<Vec<_>>>()?;
        let reference_archive: Archive = reference_entries.into_iter().collect();
        let reference_options = ArchiveOptionsBuilder::default()
            .version(Version::v7)
            .strings(true)
            .compression_format(Ba2CrateCompression::Zip)
            .build();
        let mut reference_bytes = Vec::new();
        reference_archive.write(&mut reference_bytes, &reference_options)?;
        assert_eq!(fs::read(&output)?, reference_bytes);

        Ok(())
    }

    #[test]
    fn test_create_dx10_ba2_matches_in_memory_writer() -> Result<()> {
        use image_dds::ddsfile::{D3DFormat, Dds, NewD3dParams};

        let dir = tempdir()?;
        let source = dir.path().join("texture.dds");
        let output = dir.path().join("Example - Textures.ba2");
        let mut dds = Dds::new_d3d(NewD3dParams {
            height: 64,
            width: 64,
            depth: None,
            format: D3DFormat::DXT1,
            mipmap_levels: Some(1),
            caps2: None,
        })?;
        for (index, byte) in dds.data.iter_mut().enumerate() {
            *byte = index as u8;
        }
        let mut source_file = fs::File::create(&source)?;
        dds.write(&mut source_file)?;

        let mut builder = Ba2Builder::from_name("Example - Textures.ba2")
            .with_version(Ba2Version::V8)
            .with_compression(Ba2CompressionFormat::Zlib);
        builder.add_file("textures/example/texture.dds", source.clone());
        builder.build(&output)?;

        let source_bytes = fs::read(&source)?;
        let read_options = FileReadOptionsBuilder::new()
            .format(Format::DX10)
            .compression_format(Ba2CrateCompression::Zip)
            .compression_level(CompressionLevel::FO4)
            .compression_result(CompressionResult::Compressed)
            .build();
        let file = Ba2File::read(Copied(&source_bytes), &read_options)?;
        let key = ArchiveKey::from(b"textures/example/texture.dds".as_slice());
        let reference: Archive = [(key, file)].into_iter().collect();
        let options = ArchiveOptionsBuilder::default()
            .version(Version::v8)
            .format(Format::DX10)
            .compression_format(Ba2CrateCompression::Zip)
            .strings(true)
            .build();
        let mut reference_bytes = Vec::new();
        reference.write(&mut reference_bytes, &options)?;

        assert_eq!(fs::read(&output)?, reference_bytes);
        let (archive, read_back_options) = Archive::read(output.as_path())?;
        assert_eq!(archive.len(), 1);
        assert_eq!(read_back_options.format(), Format::DX10);
        assert_eq!(read_back_options.version(), Version::v8);

        Ok(())
    }
}
