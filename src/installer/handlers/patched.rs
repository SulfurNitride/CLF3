//! PatchedFromArchive directive handler
//!
//! Extracts a file from an archive, applies an octodiff delta patch,
//! and writes the patched result to the output directory.

use crate::installer::processor::ProcessContext;
use crate::modlist::PatchedFromArchiveDirective;
use crate::octodiff::DeltaReader;
use crate::paths;

use crate::installer::handlers::from_archive::extract_from_archive_with_temp;

use anyhow::{Context, Result};
use std::fs::File;
use std::io::{Cursor, Read, Write};

/// Handle a PatchedFromArchive directive
pub fn handle_patched_from_archive(
    ctx: &ProcessContext,
    directive: &PatchedFromArchiveDirective,
) -> Result<()> {
    // 1. Extract source file from archive
    let archive_hash = directive
        .archive_hash_path
        .first()
        .context("Missing archive hash in PatchedFromArchive")?;

    let archive_path = ctx
        .get_archive_path(archive_hash)
        .with_context(|| format!("Archive not found for hash: {}", archive_hash))?;

    let source_data = if directive.archive_hash_path.len() == 1 {
        // Whole file is the source
        std::fs::read(archive_path)
            .with_context(|| format!("Failed to read source file: {}", archive_path.display()))?
    } else if directive.archive_hash_path.len() == 2 {
        // Simple extraction from archive
        let path_in_archive = &directive.archive_hash_path[1];
        // Try cache first (for pre-extracted 7z/RAR files)
        if let Some(cached) = ctx.get_cached_file(archive_hash, path_in_archive) {
            cached
        } else {
            // Fall back to direct extraction
            extract_from_archive_with_temp(archive_path, path_in_archive, &ctx.config.downloads_dir)?
        }
    } else {
        // Nested BSA: archive -> BSA -> file
        let bsa_path = &directive.archive_hash_path[1];
        let file_in_bsa = &directive.archive_hash_path[2];

        // Try cache first (individual files cached in SQLite)
        if let Some(cached) = ctx.get_cached_nested_bsa_file(archive_hash, bsa_path, file_in_bsa) {
            cached
        } else if let Some(bsa_disk_path) = ctx.get_cached_bsa_path(archive_hash, bsa_path) {
            // BSA is in working folder - extract directly from it
            crate::bsa::extract_file(&bsa_disk_path, file_in_bsa)
                .with_context(|| format!("Failed to extract {} from BSA {}", file_in_bsa, bsa_path))?
        } else {
            // Fall back to direct extraction: extract BSA from archive, then file from BSA
            let bsa_data = extract_from_archive_with_temp(archive_path, bsa_path, &ctx.config.downloads_dir)?;

            // Write BSA to temp file and extract from it
            let temp_bsa = tempfile::Builder::new()
                .prefix(".clf3_bsa_")
                .suffix(".bsa")
                .tempfile_in(&ctx.config.downloads_dir)
                .context("Failed to create temp BSA file")?;
            std::fs::write(temp_bsa.path(), &bsa_data)?;

            crate::bsa::extract_file(temp_bsa.path(), file_in_bsa)
                .with_context(|| format!("Failed to extract {} from BSA {}", file_in_bsa, bsa_path))?
        }
    };

    // 2. Load delta from wabbajack archive
    let patch_name = directive.patch_id.to_string();
    let delta_data = ctx
        .read_wabbajack_file(&patch_name)
        .with_context(|| format!("Failed to read patch {}", patch_name))?;

    // 3. Apply delta patch
    let basis = Cursor::new(source_data);
    let delta = Cursor::new(delta_data);

    let mut reader = DeltaReader::new(basis, delta)
        .with_context(|| format!("Failed to create delta reader for patch {}", patch_name))?;

    let mut patched_data = Vec::with_capacity(directive.size as usize);
    reader
        .read_to_end(&mut patched_data)
        .with_context(|| format!("Failed to apply patch {}", patch_name))?;

    // 4. Verify size
    if patched_data.len() as u64 != directive.size {
        anyhow::bail!(
            "Size mismatch after patching: expected {} bytes, got {}",
            directive.size,
            patched_data.len()
        );
    }

    // 5. Write to output
    let output_path = ctx.resolve_output_path(&directive.to);
    paths::ensure_parent_dirs(&output_path)?;

    let mut file = File::create(&output_path)
        .with_context(|| format!("Failed to create output file: {}", output_path.display()))?;
    file.write_all(&patched_data)
        .with_context(|| format!("Failed to write output file: {}", output_path.display()))?;

    Ok(())
}
