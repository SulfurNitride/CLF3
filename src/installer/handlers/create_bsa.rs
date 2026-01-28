//! CreateBSA directive handler
//!
//! Builds BSA archives from files staged in TEMP_BSA_FILES folders.

use crate::bsa::BsaBuilder;
use crate::installer::processor::ProcessContext;
use crate::modlist::{BSAState, CreateBSADirective};
use crate::paths;

use anyhow::{Context, Result};
use ba2::tes4::{ArchiveFlags, ArchiveTypes, Version};
use std::fs;
use walkdir::WalkDir;

/// Handle a CreateBSA directive
///
/// Reads files from the staging directory and builds a BSA archive.
pub fn handle_create_bsa(ctx: &ProcessContext, directive: &CreateBSADirective) -> Result<()> {
    // Staging directory: {output_dir}/TEMP_BSA_FILES/{temp_id}/
    let staging_dir = ctx
        .config
        .output_dir
        .join("TEMP_BSA_FILES")
        .join(directive.temp_id.to_string());

    if !staging_dir.exists() {
        anyhow::bail!(
            "Staging directory not found: {}",
            staging_dir.display()
        );
    }

    // Get BSA settings from state
    let (version, flags, types) = match &directive.state {
        BSAState::BSA(state) => {
            let version = match state.version {
                103 => Version::v103,
                104 => Version::v104,
                105 => Version::v105,
                _ => Version::v105,
            };
            let flags = ArchiveFlags::from_bits_truncate(state.archive_flags);
            let types = ArchiveTypes::from_bits_truncate(state.file_flags as u16);
            (version, flags, types)
        }
        BSAState::BA2(_state) => {
            // BA2 not supported yet - would need different builder
            anyhow::bail!("BA2 archive creation not yet supported");
        }
    };

    // Collect all files from staging directory first
    let mut files: Vec<(String, Vec<u8>)> = Vec::new();

    for entry in WalkDir::new(&staging_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
    {
        let file_path = entry.path();

        // Get relative path from staging dir
        let rel_path = file_path
            .strip_prefix(&staging_dir)
            .with_context(|| format!("Failed to get relative path for {}", file_path.display()))?;

        // Read file data
        let data = fs::read(file_path)
            .with_context(|| format!("Failed to read staged file: {}", file_path.display()))?;

        // Convert path to BSA format (backslashes)
        let bsa_path = rel_path.to_string_lossy().replace('/', "\\");

        files.push((bsa_path, data));
    }

    if files.is_empty() {
        anyhow::bail!(
            "No files found in staging directory: {}",
            staging_dir.display()
        );
    }

    // Write the BSA
    let output_path = ctx.resolve_output_path(&directive.to);
    paths::ensure_parent_dirs(&output_path)?;

    // Try building with original flags first
    let build_result = {
        let mut builder = BsaBuilder::new()
            .with_version(version)
            .with_flags(flags)
            .with_types(types);

        for (path, data) in &files {
            builder.add_file(path, data.clone());
        }

        builder.build(&output_path)
    };

    // If it fails with overflow, try without compression
    if let Err(e) = build_result {
        let err_str = format!("{:?}", e);
        if err_str.contains("overflow") {
            tracing::warn!("BSA build failed with overflow, retrying without compression");

            // Remove COMPRESSED flag
            let flags_no_compress = flags & !ArchiveFlags::COMPRESSED;

            let mut builder = BsaBuilder::new()
                .with_version(version)
                .with_flags(flags_no_compress)
                .with_types(types);

            for (path, data) in files {
                builder.add_file(&path, data);
            }

            builder
                .build(&output_path)
                .with_context(|| format!("Failed to build BSA (uncompressed): {}", output_path.display()))?;
        } else {
            return Err(e).with_context(|| format!("Failed to build BSA: {}", output_path.display()));
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

    // Check BSA magic bytes
    if let Ok(file) = fs::File::open(&output_path) {
        use std::io::BufReader;
        let mut reader = BufReader::new(file);
        let mut magic = [0u8; 4];
        if std::io::Read::read_exact(&mut reader, &mut magic).is_ok() {
            // BSA magic is "BSA\0"
            return &magic == b"BSA\0";
        }
    }

    false
}
