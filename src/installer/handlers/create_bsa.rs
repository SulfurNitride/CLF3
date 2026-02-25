//! CreateBSA directive handler
//!
//! Builds BSA/BA2 archives from files staged in TEMP_BSA_FILES folders.

use crate::bsa::{Ba2Builder, Ba2CompressionFormat, Ba2Version, BsaBuilder};
use crate::installer::processor::ProcessContext;
use crate::modlist::{BSAState, CreateBSADirective};
use crate::paths;

use anyhow::{Context, Result};
use ba2::tes4::{ArchiveFlags, ArchiveTypes, Version};
use std::fs;
use std::path::PathBuf;
use walkdir::WalkDir;

/// Archive type to create
enum ArchiveKind {
    /// TES4 BSA (Oblivion, FO3, FNV, Skyrim)
    Bsa {
        version: Version,
        flags: ArchiveFlags,
        types: ArchiveTypes,
    },
    /// FO4 BA2 (Fallout 4, Fallout 76, Starfield)
    Ba2 { version: Ba2Version },
}

/// Collect staged file paths from a staging directory.
/// Returns (archive_path, disk_path) pairs. Does NOT read file contents.
fn collect_staged_files(
    staging_dir: &std::path::Path,
    archive_kind: &ArchiveKind,
) -> Result<Vec<(String, PathBuf)>> {
    let mut files = Vec::new();

    for entry in WalkDir::new(staging_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
    {
        let file_path = entry.into_path();

        let rel_path = file_path
            .strip_prefix(staging_dir)
            .with_context(|| format!("Failed to get relative path for {}", file_path.display()))?;

        let archive_path = match archive_kind {
            ArchiveKind::Bsa { .. } => rel_path.to_string_lossy().replace('/', "\\"),
            ArchiveKind::Ba2 { .. } => rel_path.to_string_lossy().replace('\\', "/"),
        };

        files.push((archive_path, file_path));
    }

    Ok(files)
}

/// Handle a CreateBSA directive
///
/// Reads files from the staging directory and builds a BSA/BA2 archive.
pub fn handle_create_bsa(ctx: &ProcessContext, directive: &CreateBSADirective) -> Result<()> {
    // Staging directory: {output_dir}/TEMP_BSA_FILES/{temp_id}/
    let staging_dir = ctx
        .config
        .output_dir
        .join("TEMP_BSA_FILES")
        .join(directive.temp_id.to_string());

    if !staging_dir.exists() {
        anyhow::bail!("Staging directory not found: {}", staging_dir.display());
    }

    // Get archive settings from state
    let archive_kind = match &directive.state {
        BSAState::BSA(state) => {
            tracing::info!(
                "Creating TES4 BSA: {} (version {}, magic {})",
                directive.to,
                state.version,
                state.magic
            );
            let version = match state.version {
                103 => Version::v103,
                104 => Version::v104,
                105 => Version::v105,
                _ => Version::v105,
            };
            let flags = ArchiveFlags::from_bits_truncate(state.archive_flags);
            let types = ArchiveTypes::from_bits_truncate(state.file_flags as u16);
            ArchiveKind::Bsa {
                version,
                flags,
                types,
            }
        }
        BSAState::BA2(state) => {
            let version = Ba2Version::from_u32(state.version);
            tracing::info!(
                "Creating FO4 BA2: {} (type {:?}, version {:?})",
                directive.to,
                state.archive_type,
                version
            );
            ArchiveKind::Ba2 { version }
        }
    };

    // Collect staged file paths (no data read yet)
    let files = collect_staged_files(&staging_dir, &archive_kind)?;

    if files.is_empty() {
        anyhow::bail!(
            "No files found in staging directory: {}",
            staging_dir.display()
        );
    }

    // Write the archive
    let output_path = ctx.resolve_output_path(&directive.to);
    paths::ensure_parent_dirs(&output_path)?;

    match archive_kind {
        ArchiveKind::Bsa {
            version,
            flags,
            types,
        } => {
            // Build BSA — files are read from disk on demand during build()
            let build_result = {
                let mut builder = BsaBuilder::new()
                    .with_version(version)
                    .with_flags(flags)
                    .with_types(types);

                for (path, disk_path) in &files {
                    builder.add_file(path, disk_path.clone());
                }

                builder.build(&output_path)
            };

            // If it fails with overflow, retry without compression
            if let Err(e) = build_result {
                let err_str = format!("{:?}", e);
                if err_str.contains("overflow") {
                    tracing::warn!("BSA build failed with overflow, retrying without compression");

                    let flags_no_compress = flags & !ArchiveFlags::COMPRESSED;

                    let mut builder = BsaBuilder::new()
                        .with_version(version)
                        .with_flags(flags_no_compress)
                        .with_types(types);

                    for (path, disk_path) in &files {
                        builder.add_file(path, disk_path.clone());
                    }

                    builder.build(&output_path).with_context(|| {
                        format!(
                            "Failed to build BSA (uncompressed): {}",
                            output_path.display()
                        )
                    })?;
                } else {
                    return Err(e).with_context(|| {
                        format!("Failed to build BSA: {}", output_path.display())
                    });
                }
            }
        }
        ArchiveKind::Ba2 { version } => {
            // BA2 builder still takes Vec<u8> — read files for it
            let mut builder = Ba2Builder::from_name(&directive.to)
                .with_version(version)
                .with_compression(Ba2CompressionFormat::Zlib);

            for (path, disk_path) in files {
                let data = fs::read(&disk_path).with_context(|| {
                    format!("Failed to read staged file: {}", disk_path.display())
                })?;
                builder.add_file(&path, data);
            }

            builder
                .build(&output_path)
                .with_context(|| format!("Failed to build BA2: {}", output_path.display()))?;
        }
    }

    // Clean up staging directory
    if let Err(e) = fs::remove_dir_all(&staging_dir) {
        tracing::warn!(
            "Failed to clean up staging directory {}: {}",
            staging_dir.display(),
            e
        );
    }

    Ok(())
}

/// Check if a CreateBSA output already exists and is valid
pub fn output_bsa_valid(ctx: &ProcessContext, directive: &CreateBSADirective) -> bool {
    let output_path = ctx.resolve_output_path(&directive.to);

    if !output_path.exists() {
        return false;
    }

    // Check magic bytes - BSA or BA2
    if let Ok(file) = fs::File::open(&output_path) {
        use std::io::BufReader;
        let mut reader = BufReader::new(file);
        let mut magic = [0u8; 4];
        if std::io::Read::read_exact(&mut reader, &mut magic).is_ok() {
            // BSA magic is "BSA\0", BA2 magic is "BTDX"
            return &magic == b"BSA\0" || &magic == b"BTDX";
        }
    }

    false
}
