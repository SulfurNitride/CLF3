//! Pipelined install coordinator.
//!
//! Overlaps archive downloads with extraction by processing each archive
//! as soon as it finishes downloading, rather than waiting for all downloads
//! to complete before starting extraction.
//!
//! # Architecture
//!
//! ```text
//! Thread A (tokio): download archives, emit ArchiveEvent::Ready per completion
//!                              ↓ (mpsc channel)
//! Thread B (rayon):  receive events → index → resolve paths → extract → finalize
//! ```

use crate::installer::handlers::from_archive::{
    detect_archive_type, ArchiveType as NestedArchiveType,
};
use crate::installer::handlers::create_bsa::{handle_create_bsa, output_bsa_valid};
use crate::modlist::{
    CreateBSADirective, Directive, FromArchiveDirective, ModlistDb, PatchedFromArchiveDirective,
    TransformedTextureDirective,
};
use crate::paths;

use super::downloader::ArchiveEvent;
use super::processor::{
    build_patch_basis_key, build_patch_basis_key_from_archive_hash_path, index_single_archive,
    ProcessContext,
};
use super::streaming::{
    cleanup_temp_dirs, collect_textures_from_bsa, collect_textures_from_nested_bsas,
    collect_textures_from_temp_dir, finalize_archive, process_bsa_archive,
    process_bsa_patched_directives, process_dds_jobs_inline, process_single_archive_fused,
    process_whole_file_directives, ArchiveDirective, NestedTextureLookupInner,
    ProgressCallback, StreamingConfig, StreamingStats, TextureLookupInner,
};

use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Pre-parsed directives grouped by source archive hash.
///
/// Built at startup from the modlist DB before any downloads start.
/// Path resolution (lookup_archive_file) is deferred until each archive is indexed.
pub(crate) struct GroupedDirectives {
    /// archive_hash -> list of (id, parsed FromArchive directive, raw_path_in_archive, file_in_bsa)
    pub from_archive: HashMap<String, Vec<(i64, FromArchiveDirective)>>,
    /// archive_hash -> list of (id, parsed PatchedFromArchive directive)
    pub patched: HashMap<String, Vec<(i64, PatchedFromArchiveDirective)>>,
    /// archive_hash -> list of (id, parsed TransformedTexture directive)
    pub textures: HashMap<String, Vec<(i64, TransformedTextureDirective)>>,
    /// Whole-file directives (archive_hash_path.len() == 1) — no extraction needed
    pub whole_file: Vec<(i64, FromArchiveDirective)>,
    /// Number of directives pre-skipped (output already exists)
    pub pre_skipped: usize,
    /// archive_hash -> priority score (higher = more important to download first)
    pub priority: HashMap<String, u32>,
    /// Total number of unique archive hashes that have directives
    pub total_archives: usize,
}

/// Load and group all directives by source archive hash.
///
/// Does NOT resolve archive file paths (that requires indexing).
/// Path resolution happens per-archive after indexing in `resolve_directives_for_archive`.
pub(crate) fn load_and_group_directives(
    db: &ModlistDb,
    ctx: &ProcessContext,
) -> Result<GroupedDirectives> {
    let mut from_archive: HashMap<String, Vec<(i64, FromArchiveDirective)>> = HashMap::new();
    let mut patched: HashMap<String, Vec<(i64, PatchedFromArchiveDirective)>> = HashMap::new();
    let mut textures: HashMap<String, Vec<(i64, TransformedTextureDirective)>> = HashMap::new();
    let mut whole_file: Vec<(i64, FromArchiveDirective)> = Vec::new();
    let mut pre_skipped = 0usize;

    // Load FromArchive directives
    let from_archive_raw = db.get_all_pending_directives_of_type("FromArchive")?;
    for (id, json) in from_archive_raw {
        if let Ok(Directive::FromArchive(d)) = serde_json::from_str::<Directive>(&json) {
            // Pre-filter: skip if output already exists with correct size
            let normalized_to = paths::normalize_for_lookup(&d.to);
            if let Some(&existing_size) = ctx.existing_files.get(&normalized_to) {
                if existing_size == d.size {
                    pre_skipped += 1;
                    continue;
                }
            }

            if d.archive_hash_path.len() == 1 {
                whole_file.push((id, d));
            } else if let Some(hash) = d.archive_hash_path.first() {
                from_archive
                    .entry(hash.clone())
                    .or_default()
                    .push((id, d));
            }
        }
    }

    // Load PatchedFromArchive directives
    let patched_raw = db.get_all_pending_directives_of_type("PatchedFromArchive")?;
    for (id, json) in patched_raw {
        if let Ok(Directive::PatchedFromArchive(d)) = serde_json::from_str::<Directive>(&json) {
            let normalized_to = paths::normalize_for_lookup(&d.to);
            if let Some(&existing_size) = ctx.existing_files.get(&normalized_to) {
                if existing_size == d.size {
                    pre_skipped += 1;
                    continue;
                }
            }

            if let Some(hash) = d.archive_hash_path.first() {
                patched
                    .entry(hash.clone())
                    .or_default()
                    .push((id, d));
            }
        }
    }

    // Load TransformedTexture directives
    let texture_raw = db.get_all_pending_directives_of_type("TransformedTexture")?;
    for (id, json) in texture_raw {
        if let Ok(Directive::TransformedTexture(d)) = serde_json::from_str::<Directive>(&json) {
            let output_path = paths::join_windows_path(&ctx.config.output_dir, &d.to);
            if output_path.exists() {
                continue;
            }
            if let Some(hash) = d.archive_hash_path.first() {
                textures
                    .entry(hash.clone())
                    .or_default()
                    .push((id, d));
            }
        }
    }

    // Compute priority scores per archive hash.
    // Higher priority = more important to process first.
    let mut priority: HashMap<String, u32> = HashMap::new();

    // Check which archives feed BSA staging dirs
    let mut bsa_feeding_archives: HashSet<String> = HashSet::new();
    for (hash, directives) in &from_archive {
        for (_id, d) in directives {
            if extract_bsa_temp_id(&d.to).is_some() {
                bsa_feeding_archives.insert(hash.clone());
            }
        }
    }
    for (hash, directives) in &patched {
        for (_id, d) in directives {
            if extract_bsa_temp_id(&d.to).is_some() {
                bsa_feeding_archives.insert(hash.clone());
            }
        }
    }

    // Score each archive
    let all_hashes: HashSet<&String> = from_archive
        .keys()
        .chain(patched.keys())
        .chain(textures.keys())
        .collect();

    for hash in &all_hashes {
        let mut score: u32 = 0;

        // BSA-feeding archives are highest priority
        if bsa_feeding_archives.contains(*hash) {
            score += 100;
        }

        // Patched directives need this archive as basis
        if let Some(p) = patched.get(*hash) {
            score += 50 + p.len() as u32;
        }

        // Textures
        if let Some(t) = textures.get(*hash) {
            score += 25 + t.len() as u32;
        }

        // Simple extracts
        if let Some(f) = from_archive.get(*hash) {
            score += 1 + f.len() as u32;
        }

        priority.insert((*hash).clone(), score);
    }

    let total_archives = all_hashes.len();

    Ok(GroupedDirectives {
        from_archive,
        patched,
        textures,
        whole_file,
        pre_skipped,
        priority,
        total_archives,
    })
}

/// Extract BSA temp_id from a directive `to` path like `TEMP_BSA_FILES\{uuid}\path\file`.
fn extract_bsa_temp_id(to_path: &str) -> Option<Uuid> {
    let normalized = to_path.replace('\\', "/");
    let parts: Vec<&str> = normalized.split('/').collect();
    if parts.len() >= 2 && parts[0] == "TEMP_BSA_FILES" {
        Uuid::parse_str(parts[1]).ok()
    } else {
        None
    }
}

/// Tracks BSA readiness during pipelined processing.
///
/// For each CreateBSA directive, tracks which source archives contribute files
/// (including texture sources, since DDS textures are now processed inline).
/// When all contributing archives have been processed, the BSA can be built
/// immediately instead of waiting for the separate `bsa_phase()`.
pub(crate) struct BsaReadinessTracker {
    /// CreateBSA directives indexed by temp_id
    bsa_directives: HashMap<Uuid, (i64, CreateBSADirective)>,
    /// temp_id → set of archive hashes that still need to be processed
    pending_sources: HashMap<Uuid, HashSet<String>>,
    /// Number of BSAs built during the pipeline
    pub built_count: usize,
}

impl BsaReadinessTracker {
    /// Build tracker from CreateBSA directives and grouped extraction directives.
    pub fn new(
        db: &ModlistDb,
        ctx: &ProcessContext,
        grouped: &GroupedDirectives,
    ) -> Result<Self> {
        // Parse CreateBSA directives
        let all_raw = db.get_all_pending_directives_of_type("CreateBSA")?;
        let mut bsa_directives = HashMap::new();

        for (id, json) in all_raw {
            if let Ok(Directive::CreateBSA(d)) = serde_json::from_str::<Directive>(&json) {
                if !output_bsa_valid(ctx, &d) {
                    bsa_directives.insert(d.temp_id, (id, d));
                }
            }
        }

        if bsa_directives.is_empty() {
            return Ok(Self {
                bsa_directives,
                pending_sources: HashMap::new(),
                built_count: 0,
            });
        }

        // Initialize pending_sources for all BSAs
        // (textures are now processed inline per-archive, so texture-dependent BSAs can build early too)
        let mut pending_sources: HashMap<Uuid, HashSet<String>> = HashMap::new();
        for temp_id in bsa_directives.keys() {
            pending_sources.insert(*temp_id, HashSet::new());
        }

        // Scan FromArchive directives for BSA staging targets
        for (hash, directives) in &grouped.from_archive {
            for (_id, d) in directives {
                if let Some(temp_id) = extract_bsa_temp_id(&d.to) {
                    if let Some(sources) = pending_sources.get_mut(&temp_id) {
                        sources.insert(hash.clone());
                    }
                }
            }
        }

        // Scan PatchedFromArchive directives
        for (hash, directives) in &grouped.patched {
            for (_id, d) in directives {
                if let Some(temp_id) = extract_bsa_temp_id(&d.to) {
                    if let Some(sources) = pending_sources.get_mut(&temp_id) {
                        sources.insert(hash.clone());
                    }
                }
            }
        }

        // Scan TransformedTexture directives (textures now processed inline)
        for (hash, directives) in &grouped.textures {
            for (_id, d) in directives {
                if let Some(temp_id) = extract_bsa_temp_id(&d.to) {
                    if let Some(sources) = pending_sources.get_mut(&temp_id) {
                        sources.insert(hash.clone());
                    }
                }
            }
        }

        // Log tracker state
        for (temp_id, sources) in &pending_sources {
            if let Some((_, d)) = bsa_directives.get(temp_id) {
                let bsa_name = Path::new(&d.to)
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| d.to.clone());
                info!(
                    "BSA tracker: {} needs {} source archives ({} files)",
                    bsa_name,
                    sources.len(),
                    d.file_states.len()
                );
            }
        }

        Ok(Self {
            bsa_directives,
            pending_sources,
            built_count: 0,
        })
    }

    /// Mark an archive as completed. Returns temp_ids of BSAs now ready to build.
    pub fn archive_completed(&mut self, archive_hash: &str) -> Vec<Uuid> {
        let mut ready = Vec::new();

        for (temp_id, sources) in &mut self.pending_sources {
            sources.remove(archive_hash);
            if sources.is_empty() {
                ready.push(*temp_id);
            }
        }

        for temp_id in &ready {
            self.pending_sources.remove(temp_id);
        }

        ready
    }

    /// Take the CreateBSA directive for a ready temp_id.
    pub fn take_directive(&mut self, temp_id: &Uuid) -> Option<(i64, CreateBSADirective)> {
        self.bsa_directives.remove(temp_id)
    }

    /// Whether any BSAs are being tracked for early building.
    pub fn has_tracked_bsas(&self) -> bool {
        !self.pending_sources.is_empty()
    }
}

/// Resolve file paths for a single archive's directives after indexing.
///
/// Returns `ArchiveDirective` variants with resolved paths from the archive index.
fn resolve_directives_for_archive(
    db: &ModlistDb,
    archive_hash: &str,
    from_directives: &[(i64, FromArchiveDirective)],
    patched_directives: &[(i64, PatchedFromArchiveDirective)],
) -> Vec<ArchiveDirective> {
    let mut result = Vec::new();

    for (id, d) in from_directives {
        let resolved_path = if d.archive_hash_path.len() >= 2 {
            db.lookup_archive_file(archive_hash, &d.archive_hash_path[1])
                .ok()
                .flatten()
        } else {
            None
        };

        let file_in_bsa = if d.archive_hash_path.len() >= 3 {
            Some(d.archive_hash_path[2].clone())
        } else {
            None
        };

        result.push(ArchiveDirective::FromArchive {
            id: *id,
            directive: d.clone(),
            resolved_path,
            file_in_bsa,
        });
    }

    for (id, d) in patched_directives {
        let resolved_path = if d.archive_hash_path.len() >= 2 {
            db.lookup_archive_file(archive_hash, &d.archive_hash_path[1])
                .ok()
                .flatten()
        } else {
            None
        };

        result.push(ArchiveDirective::Patched {
            id: *id,
            directive: d.clone(),
            resolved_path,
        });
    }

    result
}

/// Build texture lookup maps for a single archive.
fn build_texture_lookups(
    _archive_hash: &str,
    texture_directives: &[(i64, TransformedTextureDirective)],
) -> (TextureLookupInner, NestedTextureLookupInner) {
    let mut depth2: TextureLookupInner = HashMap::new();
    let mut depth3: NestedTextureLookupInner = HashMap::new();

    for (id, d) in texture_directives {
        if d.archive_hash_path.len() == 2 {
            let source = d.archive_hash_path[1].replace('\\', "/").to_lowercase();
            depth2.entry(source).or_default().push((*id, d.clone()));
        } else if d.archive_hash_path.len() >= 3 {
            let bsa_name = d.archive_hash_path[1].replace('\\', "/").to_lowercase();
            let file_in_bsa = d.archive_hash_path[2].replace('\\', "/").to_lowercase();
            depth3
                .entry(bsa_name)
                .or_default()
                .entry(file_in_bsa)
                .or_default()
                .push((*id, d.clone()));
        }
    }

    (depth2, depth3)
}

/// Resolve patch basis keys for a single archive's patched directives.
fn resolve_patch_basis_for_archive(
    db: &ModlistDb,
    ctx: &ProcessContext,
    archive_hash: &str,
    patched_directives: &[(i64, PatchedFromArchiveDirective)],
) {
    for (_id, d) in patched_directives {
        let resolved_path = if d.archive_hash_path.len() >= 2 {
            db.lookup_archive_file(archive_hash, &d.archive_hash_path[1])
                .ok()
                .flatten()
                .unwrap_or_else(|| d.archive_hash_path[1].clone())
        } else {
            String::new()
        };

        let key = if d.archive_hash_path.len() >= 2 {
            build_patch_basis_key(
                archive_hash,
                Some(&resolved_path),
                d.archive_hash_path.get(2).map(|s| s.as_str()),
            )
        } else {
            build_patch_basis_key(archive_hash, None, None)
        };

        // Add to needed keys set
        let mut needed = ctx
            .needed_patch_basis_keys
            .write()
            .expect("needed_patch_basis_keys lock");
        needed.insert(key);

        if let Some(raw_key) =
            build_patch_basis_key_from_archive_hash_path(&d.archive_hash_path)
        {
            needed.insert(raw_key);
        }
    }
}

/// Prepared archive data ready for extraction (no DB access needed).
struct PreparedArchive {
    archive_hash: String,
    archive_name: String,
    archive_path: PathBuf,
    resolved: Vec<ArchiveDirective>,
    tex_d2: TextureLookupInner,
    tex_d3: NestedTextureLookupInner,
    extra_paths: Vec<String>,
}

/// Prepare a single archive for extraction: index it, resolve paths.
/// This step needs DB access and must run on the receiver thread.
fn prepare_archive(
    db: &ModlistDb,
    ctx: &ProcessContext,
    archive_hash: &str,
    archive_name: &str,
    archive_path: &Path,
    grouped: &GroupedDirectives,
) -> Option<PreparedArchive> {
    // 1. Index this archive
    if let Err(e) = index_single_archive(db, archive_hash, archive_path, archive_name) {
        warn!("Failed to index {}: {}", archive_name, e);
    }

    // 2. Resolve patch basis keys for this archive's patched directives
    if let Some(patched_directives) = grouped.patched.get(archive_hash) {
        resolve_patch_basis_for_archive(db, ctx, archive_hash, patched_directives);
    }

    // 3. Gather directives for this archive
    let from_directives = grouped.from_archive.get(archive_hash);
    let patched_directives = grouped.patched.get(archive_hash);
    let texture_directives = grouped.textures.get(archive_hash);

    let empty_from: Vec<(i64, FromArchiveDirective)> = Vec::new();
    let empty_patched: Vec<(i64, PatchedFromArchiveDirective)> = Vec::new();
    let from_slice = from_directives.map(|v| v.as_slice()).unwrap_or(&empty_from);
    let patched_slice = patched_directives.map(|v| v.as_slice()).unwrap_or(&empty_patched);

    // No directives for this archive? Skip.
    if from_slice.is_empty() && patched_slice.is_empty() && texture_directives.is_none() {
        debug!("No directives for archive {}, skipping", archive_name);
        return None;
    }

    // 4. Resolve file paths now that archive is indexed
    let resolved = resolve_directives_for_archive(db, archive_hash, from_slice, patched_slice);

    // 5. Build texture lookups for this archive
    let (tex_d2, tex_d3) = if let Some(tex) = texture_directives {
        build_texture_lookups(archive_hash, tex)
    } else {
        (HashMap::new(), HashMap::new())
    };

    let mut extra_paths: Vec<String> = Vec::new();
    for source_path in tex_d2.keys() {
        extra_paths.push(source_path.clone());
    }
    for bsa_name in tex_d3.keys() {
        extra_paths.push(bsa_name.clone());
    }

    Some(PreparedArchive {
        archive_hash: archive_hash.to_string(),
        archive_name: archive_name.to_string(),
        archive_path: archive_path.to_path_buf(),
        resolved,
        tex_d2,
        tex_d3,
        extra_paths,
    })
}

/// Extract and finalize a prepared archive. No DB access needed — safe for rayon.
/// DDS textures are processed inline (no channel/spill).
#[allow(clippy::too_many_arguments)]
fn extract_prepared_archive(
    prepared: PreparedArchive,
    ctx: &ProcessContext,
    // Stats
    extracted: &AtomicUsize,
    written: &AtomicUsize,
    skipped: &AtomicUsize,
    failed: &AtomicUsize,
    logged_failures: &Arc<AtomicUsize>,
    progress_callback: &Option<ProgressCallback>,
    // CLI progress
    archives_completed: &AtomicUsize,
    total_archives: usize,
) {
    const MAX_LOGGED_FAILURES: usize = 100;

    let archive_name_for_progress = prepared.archive_name.clone();
    let directive_count_for_progress =
        prepared.resolved.len() + prepared.tex_d2.len() + prepared.tex_d3.len();

    let archive_type =
        detect_archive_type(&prepared.archive_path).unwrap_or(NestedArchiveType::Unknown);

    if matches!(
        archive_type,
        NestedArchiveType::Tes3Bsa | NestedArchiveType::Bsa | NestedArchiveType::Ba2
    ) {
        // BSA/BA2 direct-read path
        let bsa_from: Vec<(i64, FromArchiveDirective, Option<String>, Option<String>)> =
            prepared
                .resolved
                .iter()
                .filter_map(|d| match d {
                    ArchiveDirective::FromArchive {
                        id,
                        directive,
                        resolved_path,
                        file_in_bsa,
                    } => Some((
                        *id,
                        directive.clone(),
                        resolved_path.clone(),
                        file_in_bsa.clone(),
                    )),
                    _ => None,
                })
                .collect();

        let bsa_patched: Vec<(i64, &PatchedFromArchiveDirective)> = prepared
            .resolved
            .iter()
            .filter_map(|d| match d {
                ArchiveDirective::Patched {
                    id, directive, ..
                } => Some((*id, directive)),
                _ => None,
            })
            .collect();

        let arc_extracted = Arc::new(AtomicUsize::new(0));
        let arc_written = Arc::new(AtomicUsize::new(0));
        let arc_skipped = Arc::new(AtomicUsize::new(0));
        let arc_failed = Arc::new(AtomicUsize::new(0));
        let arc_logged = Arc::new(AtomicUsize::new(0));

        if !bsa_from.is_empty() {
            process_bsa_archive(
                &prepared.archive_path,
                &bsa_from,
                ctx,
                &arc_extracted,
                &arc_written,
                &arc_skipped,
                &arc_failed,
                &arc_logged,
                progress_callback.clone(),
            );
        }

        if !bsa_patched.is_empty() {
            process_bsa_patched_directives(
                &prepared.archive_path,
                &prepared.archive_hash,
                &bsa_patched,
                ctx,
                &arc_extracted,
                &arc_written,
                &arc_skipped,
                &arc_failed,
                &arc_logged,
                progress_callback.clone(),
            );
        }

        // Collect and process textures from BSA inline
        if !prepared.tex_d2.is_empty() {
            let dds_jobs = collect_textures_from_bsa(&prepared.archive_path, &prepared.tex_d2);
            if !dds_jobs.is_empty() {
                let (ok, fail) = process_dds_jobs_inline(dds_jobs, ctx);
                written.fetch_add(ok, Ordering::Relaxed);
                failed.fetch_add(fail, Ordering::Relaxed);
            }
        }

        extracted.fetch_add(arc_extracted.load(Ordering::Relaxed), Ordering::Relaxed);
        written.fetch_add(arc_written.load(Ordering::Relaxed), Ordering::Relaxed);
        skipped.fetch_add(arc_skipped.load(Ordering::Relaxed), Ordering::Relaxed);
        failed.fetch_add(arc_failed.load(Ordering::Relaxed), Ordering::Relaxed);
    } else {
        // Extract via 7z/ZIP/RAR, then finalize
        let result = process_single_archive_fused(
            &prepared.archive_path,
            &prepared.archive_hash,
            &prepared.resolved,
            ctx,
            Some(1),
            &prepared.extra_paths,
            None, // pipeline path doesn't use listing cache yet
        );

        match result {
            Ok(archive_result) => {
                extracted.fetch_add(
                    archive_result.extracted_count + archive_result.patched_count,
                    Ordering::Relaxed,
                );
                skipped.fetch_add(archive_result.skipped_count, Ordering::Relaxed);
                failed.fetch_add(archive_result.failed_count, Ordering::Relaxed);

                // Collect and process textures inline before finalization
                // (temp dir still exists, so we can read source files)
                let mut dds_jobs = Vec::new();
                if !prepared.tex_d2.is_empty() {
                    dds_jobs.extend(collect_textures_from_temp_dir(
                        archive_result.temp_dir.path(),
                        &prepared.tex_d2,
                    ));
                }
                if !prepared.tex_d3.is_empty() {
                    dds_jobs.extend(collect_textures_from_nested_bsas(
                        archive_result.temp_dir.path(),
                        &prepared.tex_d3,
                    ));
                }
                if !dds_jobs.is_empty() {
                    let (ok, fail) = process_dds_jobs_inline(dds_jobs, ctx);
                    written.fetch_add(ok, Ordering::Relaxed);
                    failed.fetch_add(fail, Ordering::Relaxed);
                }

                let fin_stats =
                    finalize_archive(archive_result, &ctx.config.output_dir, logged_failures, progress_callback);
                written.fetch_add(fin_stats.written, Ordering::Relaxed);
                skipped.fetch_add(fin_stats.skipped, Ordering::Relaxed);
                failed.fetch_add(fin_stats.failed, Ordering::Relaxed);
            }
            Err(e) => {
                let count = logged_failures.fetch_add(1, Ordering::Relaxed);
                if count < MAX_LOGGED_FAILURES {
                    error!("FAIL: Archive {}: {:#}", prepared.archive_name, e);
                }
                failed.fetch_add(prepared.resolved.len(), Ordering::Relaxed);
            }
        }
    }

    // CLI progress: print per-archive completion status
    let done = archives_completed.fetch_add(1, Ordering::Relaxed) + 1;
    let w = written.load(Ordering::Relaxed);
    let f = failed.load(Ordering::Relaxed);
    eprintln!(
        "[{}/{}] Extracted: {} ({} directives, {} written, {} failed)",
        done, total_archives, archive_name_for_progress, directive_count_for_progress, w, f
    );
}

/// Run the pipelined processing coordinator.
///
/// Receives `ArchiveEvent`s from the download thread and processes each archive
/// incrementally as it becomes available. Archives are extracted in parallel.
/// Returns streaming stats.
pub(crate) fn run_processing_loop(
    db: &ModlistDb,
    ctx: &ProcessContext,
    rx: &std::sync::mpsc::Receiver<ArchiveEvent>,
    grouped: &GroupedDirectives,
    config: StreamingConfig,
    progress_callback: Option<ProgressCallback>,
) -> Result<StreamingStats> {
    // Clean up leftover temp dirs from previous interrupted runs
    cleanup_temp_dirs(&ctx.config.downloads_dir);

    // Stats
    let extracted = AtomicUsize::new(0);
    let written = AtomicUsize::new(0);
    let skipped = AtomicUsize::new(0);
    let failed = AtomicUsize::new(0);
    let logged_failures = Arc::new(AtomicUsize::new(0));

    // Initialize GPU once for inline DDS processing (BC7 textures)
    {
        let texture_count: usize = grouped.textures.values().map(|v| v.len()).sum();
        if texture_count > 0 {
            use crate::textures::init_gpu;
            let _ = init_gpu();
            eprintln!("DDS: {} texture directives will be processed inline per-archive", texture_count);
        }
    }

    // Global counter for GUI progress
    let global_written = Arc::new(AtomicUsize::new(0));
    let adjusted_callback: Option<ProgressCallback> = progress_callback.map(|cb| {
        let offset = grouped.pre_skipped;
        let counter = global_written.clone();
        Arc::new(move |_per_archive_count: usize| {
            let total = counter.fetch_add(1, Ordering::Relaxed) + 1;
            cb(offset + total);
        }) as ProgressCallback
    });

    // Process whole-file directives first (simple copy, no archive extraction needed)
    if !grouped.whole_file.is_empty() {
        eprintln!(
            "Copying {} whole-file directives...",
            grouped.whole_file.len()
        );
        let arc_extracted = Arc::new(AtomicUsize::new(0));
        let arc_skipped = Arc::new(AtomicUsize::new(0));
        let arc_failed = Arc::new(AtomicUsize::new(0));
        process_whole_file_directives(
            &grouped.whole_file,
            ctx,
            &arc_extracted,
            &arc_skipped,
            &arc_failed,
        );
        extracted.fetch_add(arc_extracted.load(Ordering::Relaxed), Ordering::Relaxed);
        skipped.fetch_add(arc_skipped.load(Ordering::Relaxed), Ordering::Relaxed);
        failed.fetch_add(arc_failed.load(Ordering::Relaxed), Ordering::Relaxed);
    }

    // Build extraction thread pool (used for within-archive parallel decompression)
    let extract_workers = config.max_extract_workers.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|n| n.get().max(2))
            .unwrap_or(4)
    });
    // Concurrency limiter for parallel archive extraction.
    // Limits how many archives are being extracted simultaneously.
    let max_concurrent = extract_workers.max(2);
    let active_count = std::sync::Mutex::new(0usize);
    let active_cvar = std::sync::Condvar::new();

    // CLI progress counter
    let archives_completed = AtomicUsize::new(0);
    let total_archives = grouped.total_archives;

    // Completion channel: extraction threads signal when done (hash of completed archive)
    let (done_tx, done_rx) = std::sync::mpsc::channel::<String>();

    std::thread::scope(|thread_scope| {
        let mut archives_processed = 0usize;
        let mut archives_failed = 0usize;

        // Rebind shared state as references so `move` closures copy the references
        let extracted = &extracted;
        let written = &written;
        let skipped = &skipped;
        let failed = &failed;
        let logged_failures = &logged_failures;
        let adjusted_callback = &adjusted_callback;
        let active_count = &active_count;
        let active_cvar = &active_cvar;
        let archives_completed = &archives_completed;

        // BSA readiness tracker — builds BSAs as soon as all their source archives are processed
        let mut bsa_tracker = BsaReadinessTracker::new(db, ctx, grouped)
            .unwrap_or_else(|e| {
                warn!("Failed to initialize BSA tracker: {}", e);
                BsaReadinessTracker {
                    bsa_directives: HashMap::new(),
                    pending_sources: HashMap::new(),
                    built_count: 0,
                }
            });

        if bsa_tracker.has_tracked_bsas() {
            eprintln!(
                "BSA tracker: {} BSAs eligible for early building",
                bsa_tracker.pending_sources.len()
            );
        }

        // Main processing loop: receive archive events, prepare (DB work) on this thread,
        // then spawn extraction threads in parallel.
        while let Ok(event) = rx.recv() {
            // Drain completion channel (non-blocking) to update BSA readiness
            while let Ok(completed_hash) = done_rx.try_recv() {
                let ready_bsas = bsa_tracker.archive_completed(&completed_hash);
                for temp_id in ready_bsas {
                    if let Some((_id, directive)) = bsa_tracker.take_directive(&temp_id) {
                        let bsa_name = Path::new(&directive.to)
                            .file_name()
                            .map(|n| n.to_string_lossy().to_string())
                            .unwrap_or_else(|| directive.to.clone());

                        eprintln!(
                            "BSA ready — building: {} ({} files)",
                            bsa_name,
                            directive.file_states.len()
                        );

                        // Build BSA on a separate thread
                        thread_scope.spawn(move || {
                            match handle_create_bsa(ctx, &directive) {
                                Ok(()) => {
                                    eprintln!("Created BSA: {}", bsa_name);
                                }
                                Err(e) => {
                                    error!("Failed to build BSA {}: {:#}", bsa_name, e);
                                    failed.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        });
                        bsa_tracker.built_count += 1;
                    }
                }
            }

            match event {
                ArchiveEvent::Ready { hash, name, path } => {
                    // Register the archive path for extraction
                    ctx.register_archive_path(hash.clone(), path.clone());

                    // Phase 1: Prepare (DB work — needs ModlistDb, runs on main thread)
                    let prepared = prepare_archive(
                        db, ctx, &hash, &name, &path, grouped,
                    );

                    if let Some(prepared) = prepared {
                        // Wait for a concurrency slot
                        {
                            let mut count = active_count.lock().expect("active_count lock");
                            while *count >= max_concurrent {
                                count = active_cvar.wait(count).expect("active_count cvar");
                            }
                            *count += 1;
                        }

                        // Phase 2: Extract + inline DDS (no DB needed — runs on separate thread)
                        let done_tx = done_tx.clone();
                        let hash_done = hash.clone();

                        thread_scope.spawn(move || {
                            extract_prepared_archive(
                                prepared,
                                ctx,
                                extracted,
                                written,
                                skipped,
                                failed,
                                logged_failures,
                                adjusted_callback,
                                archives_completed,
                                total_archives,
                            );

                            // Signal completion for BSA readiness tracking
                            let _ = done_tx.send(hash_done);

                            // Release concurrency slot
                            let mut count = active_count.lock().expect("active_count lock");
                            *count -= 1;
                            active_cvar.notify_one();
                        });
                    }

                    archives_processed += 1;
                }
                ArchiveEvent::Failed { hash, name, error } => {
                    warn!("Archive download failed: {} ({}): {}", name, hash, error);
                    archives_failed += 1;
                    let from_count = grouped
                        .from_archive
                        .get(&hash)
                        .map(|v| v.len())
                        .unwrap_or(0);
                    let patched_count =
                        grouped.patched.get(&hash).map(|v| v.len()).unwrap_or(0);
                    failed.fetch_add(from_count + patched_count, Ordering::Relaxed);
                }
                ArchiveEvent::Manual { hash, name } => {
                    info!("Archive requires manual download: {} ({})", name, hash);
                }
            }
        }

        // Download channel closed — wait for all in-flight extractions to finish
        {
            let mut count = active_count.lock().expect("active_count lock");
            while *count > 0 {
                count = active_cvar.wait(count).expect("active_count cvar");
            }
        }

        // Drain remaining BSA completions
        drop(done_tx);
        while let Ok(completed_hash) = done_rx.recv() {
            let ready_bsas = bsa_tracker.archive_completed(&completed_hash);
            for temp_id in ready_bsas {
                if let Some((_id, directive)) = bsa_tracker.take_directive(&temp_id) {
                    let bsa_name = Path::new(&directive.to)
                        .file_name()
                        .map(|n| n.to_string_lossy().to_string())
                        .unwrap_or_else(|| directive.to.clone());

                    eprintln!(
                        "BSA ready — building: {} ({} files)",
                        bsa_name,
                        directive.file_states.len()
                    );

                    match handle_create_bsa(ctx, &directive) {
                        Ok(()) => {
                            eprintln!("Created BSA: {}", bsa_name);
                        }
                        Err(e) => {
                            error!("Failed to build BSA {}: {:#}", bsa_name, e);
                            failed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    bsa_tracker.built_count += 1;
                }
            }
        }

        if bsa_tracker.built_count > 0 {
            eprintln!(
                "Pipeline complete: {} archives processed, {} failed, {} BSAs built early",
                archives_processed, archives_failed, bsa_tracker.built_count
            );
        } else {
            eprintln!(
                "Pipeline complete: {} archives processed, {} failed",
                archives_processed, archives_failed
            );
        }
    });

    Ok(StreamingStats {
        extracted: extracted.load(Ordering::Relaxed),
        written: written.load(Ordering::Relaxed),
        skipped: skipped.load(Ordering::Relaxed) + grouped.pre_skipped,
        failed: failed.load(Ordering::Relaxed),
        failed_archive_hashes: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_bsa_temp_id_backslash() {
        let id = Uuid::parse_str("a1b2c3d4-e5f6-7890-abcd-ef1234567890").unwrap();
        let path = format!("TEMP_BSA_FILES\\{}\\meshes\\armor.nif", id);
        assert_eq!(extract_bsa_temp_id(&path), Some(id));
    }

    #[test]
    fn test_extract_bsa_temp_id_forward_slash() {
        let id = Uuid::parse_str("a1b2c3d4-e5f6-7890-abcd-ef1234567890").unwrap();
        let path = format!("TEMP_BSA_FILES/{}/textures/skin.dds", id);
        assert_eq!(extract_bsa_temp_id(&path), Some(id));
    }

    #[test]
    fn test_extract_bsa_temp_id_not_bsa_path() {
        assert_eq!(extract_bsa_temp_id("mods\\some_mod\\file.esp"), None);
        assert_eq!(extract_bsa_temp_id("TEMP_BSA_FILES"), None);
    }

    #[test]
    fn test_extract_bsa_temp_id_invalid_uuid() {
        assert_eq!(
            extract_bsa_temp_id("TEMP_BSA_FILES\\not-a-uuid\\file.nif"),
            None
        );
    }
}
