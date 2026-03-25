//! PatchedFromArchive directive handler
//!
//! Extracts a file from an archive, applies an octodiff delta patch,
//! and streams the patched result directly to the output file.
//!
//! Memory optimization: uses mmap for on-disk source files (len==1 case)
//! and streams the DeltaReader output directly to a BufWriter, avoiding
//! a full in-memory copy of the patched output.

use crate::installer::processor::ProcessContext;
use crate::modlist::PatchedFromArchiveDirective;
use crate::octodiff::DeltaReader;
use crate::paths;

use crate::installer::handlers::from_archive::extract_from_archive_with_temp;

use anyhow::{Context, Result};
use std::fs::File;
use std::io::{BufWriter, Cursor, Read, Seek, Write};

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

    // 2. Load delta from wabbajack archive
    let patch_name = directive.patch_id.to_string();
    let delta_data = ctx
        .read_wabbajack_file(&patch_name)
        .with_context(|| format!("Failed to read patch {}", patch_name))?;
    let delta = Cursor::new(delta_data);

    // 3. Prepare output file
    let output_path = ctx.resolve_output_path(&directive.to);
    paths::ensure_parent_dirs(&output_path)?;
    let out_file = File::create(&output_path)
        .with_context(|| format!("Failed to create output file: {}", output_path.display()))?;
    let mut writer = BufWriter::new(out_file);

    // 4. Apply delta patch — stream directly to disk
    // For len==1 (whole file on disk), mmap the source to avoid loading into memory.
    // For extracted sources, we already have Vec<u8> from extraction.
    let bytes_written = if directive.archive_hash_path.len() == 1 {
        // Mmap the source file — zero-copy, OS manages paging
        let file = File::open(&archive_path)
            .with_context(|| format!("Failed to open source file: {}", archive_path.display()))?;
        // Safety: file is read-only and we hold it open for the duration of patching
        let mmap = unsafe { memmap2::Mmap::map(&file) }
            .with_context(|| format!("Failed to mmap source file: {}", archive_path.display()))?;
        let basis = Cursor::new(mmap);
        apply_and_stream(basis, delta, &mut writer, &patch_name)?
    } else if directive.archive_hash_path.len() == 2 {
        let path_in_archive = &directive.archive_hash_path[1];
        let source_data = if let Some(cached) = ctx.get_cached_file(archive_hash, path_in_archive)
        {
            cached
        } else {
            extract_from_archive_with_temp(
                &archive_path,
                path_in_archive,
                &ctx.config.downloads_dir,
            )?
        };
        let basis = Cursor::new(source_data);
        apply_and_stream(basis, delta, &mut writer, &patch_name)?
    } else {
        let bsa_path = &directive.archive_hash_path[1];
        let file_in_bsa = &directive.archive_hash_path[2];
        let source_data = if let Some(cached) =
            ctx.get_cached_nested_bsa_file(archive_hash, bsa_path, file_in_bsa)
        {
            cached
        } else if let Some(bsa_disk_path) = ctx.get_cached_bsa_path(archive_hash, bsa_path) {
            crate::bsa::extract_archive_file(&bsa_disk_path, file_in_bsa).with_context(|| {
                format!("Failed to extract {} from BSA {}", file_in_bsa, bsa_path)
            })?
        } else {
            let bsa_data =
                extract_from_archive_with_temp(&archive_path, bsa_path, &ctx.config.downloads_dir)?;
            let temp_bsa = tempfile::Builder::new()
                .prefix(".clf3_bsa_")
                .suffix(".bsa")
                .tempfile_in(&ctx.config.downloads_dir)
                .context("Failed to create temp BSA file")?;
            std::fs::write(temp_bsa.path(), &bsa_data)?;
            crate::bsa::extract_archive_file(temp_bsa.path(), file_in_bsa).with_context(|| {
                format!("Failed to extract {} from BSA {}", file_in_bsa, bsa_path)
            })?
        };
        let basis = Cursor::new(source_data);
        apply_and_stream(basis, delta, &mut writer, &patch_name)?
    };

    writer.flush()?;

    // 5. Verify size
    if bytes_written != directive.size {
        // Clean up the bad output
        let _ = std::fs::remove_file(&output_path);
        anyhow::bail!(
            "Size mismatch after patching: expected {} bytes, got {}",
            directive.size,
            bytes_written
        );
    }

    Ok(())
}

/// Apply a delta patch and stream the output to a writer. Returns bytes written.
fn apply_and_stream<B: Read + Seek, W: Write>(
    basis: B,
    delta: Cursor<Vec<u8>>,
    writer: &mut W,
    patch_name: &str,
) -> Result<u64> {
    let mut reader = DeltaReader::new(basis, delta)
        .with_context(|| format!("Failed to create delta reader for patch {}", patch_name))?;
    let written = std::io::copy(&mut reader, writer)
        .with_context(|| format!("Failed to apply patch {}", patch_name))?;
    Ok(written)
}
