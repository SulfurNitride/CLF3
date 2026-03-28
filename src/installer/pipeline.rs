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
use super::downloader::ArchiveEvent;
use super::processor::{
    build_patch_basis_key, build_patch_basis_key_from_archive_hash_path, index_single_archive,
    ProcessContext,
};
use super::streaming::{
    cleanup_temp_dirs, finalize_archive, process_bsa_archive,
    process_bsa_patched_directives, process_single_archive_fused,
    process_textures_from_bsa_streaming, process_textures_from_nested_bsas_streaming,
    process_textures_from_temp_streaming, process_whole_file_directives, ArchiveDirective,
    NestedTextureLookupInner, StreamingConfig, StreamingStats,
    TextureLookupInner,
};

use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Total system RAM in KB, cached at first access.
fn total_ram_kb() -> u64 {
    static TOTAL: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *TOTAL.get_or_init(|| {
        let sys = sysinfo::System::new_with_specifics(
            sysinfo::RefreshKind::nothing()
                .with_memory(sysinfo::MemoryRefreshKind::everything()),
        );
        sys.total_memory() / 1024
    })
}

/// Check if RSS exceeds `pct`% of total RAM. If so, call malloc_trim and
/// sleep briefly to let other threads finish and free memory.
/// Returns true if throttling was applied.
fn memory_pressure_gate(pct: u64) -> bool {
    if let Some(rss) = crate::installer::current_rss_kb() {
        let limit = total_ram_kb() * pct / 100;
        if rss > limit {
            #[cfg(target_os = "linux")]
            unsafe { libc::malloc_trim(0); }
            std::thread::sleep(std::time::Duration::from_millis(500));
            return true;
        }
    }
    false
}

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
    /// Extraction tier counts: (direct, conflict, patch)
    pub tier_counts: (usize, usize, usize),
}

/// Per-format timing bucket (thread-safe via atomics).
struct FormatBucket {
    bytes: AtomicU64,
    /// Cumulative thread-time spent extracting (sum across all workers).
    extract_ms: AtomicU64,
    dds_ms: AtomicU64,
    finalize_ms: AtomicU64,
    count: AtomicUsize,
}

impl FormatBucket {
    fn new() -> Self {
        Self {
            bytes: AtomicU64::new(0),
            extract_ms: AtomicU64::new(0),
            dds_ms: AtomicU64::new(0),
            finalize_ms: AtomicU64::new(0),
            count: AtomicUsize::new(0),
        }
    }
}

/// Per-format extraction metrics (thread-safe via atomics).
pub(crate) struct ExtractionMetrics {
    zip: FormatBucket,
    sevenz: FormatBucket,
    rar: FormatBucket,
    bsa: FormatBucket,
    // Tier metrics
    direct_count: AtomicUsize,
    direct_bytes: AtomicU64,
    conflict_count: AtomicUsize,
    conflict_bytes: AtomicU64,
    patch_count: AtomicUsize,
    patch_bytes: AtomicU64,
    /// Wall-clock start of the extraction phase.
    wall_start: std::sync::Mutex<Option<std::time::Instant>>,
    /// Wall-clock end of the extraction phase.
    wall_end: std::sync::Mutex<Option<std::time::Instant>>,
    /// Number of concurrent extraction workers.
    worker_count: AtomicUsize,
}

impl ExtractionMetrics {
    pub fn new() -> Self {
        Self {
            zip: FormatBucket::new(),
            sevenz: FormatBucket::new(),
            rar: FormatBucket::new(),
            bsa: FormatBucket::new(),
            direct_count: AtomicUsize::new(0),
            direct_bytes: AtomicU64::new(0),
            conflict_count: AtomicUsize::new(0),
            conflict_bytes: AtomicU64::new(0),
            patch_count: AtomicUsize::new(0),
            patch_bytes: AtomicU64::new(0),
            wall_start: std::sync::Mutex::new(None),
            wall_end: std::sync::Mutex::new(None),
            worker_count: AtomicUsize::new(0),
        }
    }

    /// Mark the start of extraction (call once before spawning workers).
    pub fn start_wall_clock(&self, workers: usize) {
        *self.wall_start.lock().expect("wall_start lock") = Some(std::time::Instant::now());
        self.worker_count.store(workers, Ordering::Relaxed);
    }

    /// Mark the end of extraction (call once after all workers finish).
    pub fn stop_wall_clock(&self) {
        *self.wall_end.lock().expect("wall_end lock") = Some(std::time::Instant::now());
    }

    /// Record which extraction tier was used for an archive.
    /// `tier`: 0 = direct, 1 = conflict (staged), 2 = patch (staged)
    pub fn record_tier(&self, tier: u8, bytes: u64) {
        match tier {
            0 => {
                self.direct_count.fetch_add(1, Ordering::Relaxed);
                self.direct_bytes.fetch_add(bytes, Ordering::Relaxed);
            }
            1 => {
                self.conflict_count.fetch_add(1, Ordering::Relaxed);
                self.conflict_bytes.fetch_add(bytes, Ordering::Relaxed);
            }
            _ => {
                self.patch_count.fetch_add(1, Ordering::Relaxed);
                self.patch_bytes.fetch_add(bytes, Ordering::Relaxed);
            }
        }
    }

    fn bucket(&self, archive_type: &NestedArchiveType) -> &FormatBucket {
        match archive_type {
            NestedArchiveType::Zip => &self.zip,
            NestedArchiveType::SevenZ | NestedArchiveType::Unknown => &self.sevenz,
            NestedArchiveType::Rar => &self.rar,
            NestedArchiveType::Bsa | NestedArchiveType::Ba2 | NestedArchiveType::Tes3Bsa => &self.bsa,
        }
    }

    pub fn record(
        &self,
        archive_type: &NestedArchiveType,
        bytes: u64,
        extract_ms: u64,
        dds_ms: u64,
        finalize_ms: u64,
    ) {
        let b = self.bucket(archive_type);
        b.bytes.fetch_add(bytes, Ordering::Relaxed);
        b.extract_ms.fetch_add(extract_ms, Ordering::Relaxed);
        b.dds_ms.fetch_add(dds_ms, Ordering::Relaxed);
        b.finalize_ms.fetch_add(finalize_ms, Ordering::Relaxed);
        b.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Log per-format throughput summary.
    pub fn log_summary(&self, reporter: &Arc<dyn super::progress::ProgressReporter>) {
        let formats: &[(&str, &FormatBucket)] = &[
            ("ZIP", &self.zip),
            ("7z", &self.sevenz),
            ("RAR", &self.rar),
            ("BSA/BA2", &self.bsa),
        ];

        // Wall-clock duration and worker count
        let wall_secs = match (
            *self.wall_start.lock().expect("lock"),
            *self.wall_end.lock().expect("lock"),
        ) {
            (Some(start), Some(end)) => Some(end.duration_since(start).as_secs_f64()),
            _ => None,
        };
        let workers = self.worker_count.load(Ordering::Relaxed).max(1);

        // Total bytes across all formats for overall rate
        let total_bytes: u64 = formats.iter().map(|(_, b)| b.bytes.load(Ordering::Relaxed)).sum();
        let total_mb = total_bytes as f64 / (1024.0 * 1024.0);

        let mut has_data = false;
        for (name, bucket) in formats {
            let c = bucket.count.load(Ordering::Relaxed);
            if c == 0 {
                continue;
            }
            if !has_data {
                // Header with wall-clock info
                if let Some(ws) = wall_secs {
                    reporter.log(&format!(
                        "Extraction: {:.1} GB in {:.0}s ({:.0} MB/s effective, {} workers)",
                        total_mb / 1024.0, ws,
                        total_mb / ws,
                        workers,
                    ));
                } else {
                    reporter.log("Extraction throughput:");
                }
                has_data = true;
            }

            let bytes = bucket.bytes.load(Ordering::Relaxed);
            let ext_ms = bucket.extract_ms.load(Ordering::Relaxed);
            let dds_ms = bucket.dds_ms.load(Ordering::Relaxed);
            let fin_ms = bucket.finalize_ms.load(Ordering::Relaxed);
            let mb = bytes as f64 / (1024.0 * 1024.0);

            let ext_s = ext_ms as f64 / 1000.0;
            let dds_s = dds_ms as f64 / 1000.0;
            let fin_s = fin_ms as f64 / 1000.0;

            // Show per-thread rate and effective rate (cumulative / workers)
            let per_thread_rate = if ext_s > 0.0 { mb / ext_s } else { 0.0 };
            let effective_s = ext_s / workers as f64;
            let effective_rate = if effective_s > 0.0 { mb / effective_s } else { 0.0 };

            let mut parts = vec![format!(
                "  {:<8} {} archives, {:.1} MB — {:.0}s cpu-time ({:.0} MB/s/thread, ~{:.0} MB/s effective)",
                format!("{}:", name), c, mb, ext_s, per_thread_rate, effective_rate,
            )];
            if dds_ms > 0 {
                parts.push(format!(", dds: {:.1}s", dds_s));
            }
            if fin_ms > 0 {
                let fin_rate = if fin_s > 0.0 { format!("{:.0} MB/s", mb / fin_s) } else { "-".into() };
                parts.push(format!(", finalize: {:.1}s ({})", fin_s, fin_rate));
            }
            reporter.log(&parts.join(""));
        }

        // Bottleneck analysis
        if let Some(ws) = wall_secs {
            let total_cpu_s: f64 = formats.iter()
                .map(|(_, b)| b.extract_ms.load(Ordering::Relaxed) as f64 / 1000.0)
                .sum();
            let total_fin_s: f64 = formats.iter()
                .map(|(_, b)| b.finalize_ms.load(Ordering::Relaxed) as f64 / 1000.0)
                .sum();
            let total_dds_s: f64 = formats.iter()
                .map(|(_, b)| b.dds_ms.load(Ordering::Relaxed) as f64 / 1000.0)
                .sum();
            let available_s = ws * workers as f64;
            let utilization = if available_s > 0.0 { total_cpu_s / available_s * 100.0 } else { 0.0 };
            reporter.log(&format!(
                "  Bottleneck: decompression ({:.0}% CPU utilization, {:.0}s decompress + {:.0}s finalize + {:.0}s dds of {:.0}s available)",
                utilization, total_cpu_s, total_fin_s, total_dds_s, available_s,
            ));
        }

        // Tier breakdown
        let dc = self.direct_count.load(Ordering::Relaxed);
        let cc = self.conflict_count.load(Ordering::Relaxed);
        let pc = self.patch_count.load(Ordering::Relaxed);
        let total = dc + cc + pc;
        if total > 0 {
            let db = self.direct_bytes.load(Ordering::Relaxed) as f64 / (1024.0 * 1024.0);
            let cb = self.conflict_bytes.load(Ordering::Relaxed) as f64 / (1024.0 * 1024.0);
            let pb = self.patch_bytes.load(Ordering::Relaxed) as f64 / (1024.0 * 1024.0);
            reporter.log(&format!(
                "Extraction tiers: {} direct ({:.0} MB), {} conflict ({:.0} MB), {} patch ({:.0} MB)",
                dc, db, cc, cb, pc, pb,
            ));
        }
    }
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
            // Pre-filter: skip if pre-validation marked as valid
            if ctx.skip_set.contains(&id) {
                pre_skipped += 1;
                continue;
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
            if ctx.skip_set.contains(&id) {
                pre_skipped += 1;
                continue;
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
            if ctx.skip_set.contains(&id) {
                pre_skipped += 1;
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

    // Compute tier counts:
    // - Direct: no patches, no nested BSA reads — callback extraction to output
    // - Conflict: no patches, but has nested BSA reads — needs temp dir
    // - Patch: has PatchedFromArchive — needs temp dir for basis files
    let mut tier_direct = 0usize;
    let mut tier_conflict = 0usize;
    let mut tier_patch = 0usize;
    for hash in &all_hashes {
        let has_patched = patched.contains_key(*hash);
        let has_nested = from_archive.get(*hash).map_or(false, |ds| {
            ds.iter().any(|(_, d)| d.archive_hash_path.len() >= 3)
        });
        if has_patched {
            tier_patch += 1;
        } else if has_nested {
            tier_conflict += 1;
        } else {
            tier_direct += 1;
        }
    }

    Ok(GroupedDirectives {
        from_archive,
        patched,
        textures,
        whole_file,
        pre_skipped,
        priority,
        total_archives,
        tier_counts: (tier_direct, tier_conflict, tier_patch),
    })
}

/// Extract BSA temp_id from a directive `to` path like `TEMP_BSA_FILES\{uuid}\path\file`.
pub(crate) fn extract_bsa_temp_id(to_path: &str) -> Option<Uuid> {
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
    reporter: &Arc<dyn super::progress::ProgressReporter>,
    // Status counters
    extract_status: &Mutex<Option<Arc<dyn super::progress::ProgressHandle>>>,
    extract_counter: &AtomicUsize,
    total_archives: usize,
    dds_status: &Mutex<Option<Arc<dyn super::progress::ProgressHandle>>>,
    dds_counter: &AtomicUsize,
    total_textures: usize,
    metrics: &ExtractionMetrics,
    // When Some, spill DDS data to channel instead of processing inline.
    // Used by phased path to defer all DDS to a dedicated phase.
    dds_spill_tx: Option<&std::sync::mpsc::SyncSender<super::streaming::DdsJob>>,
) {
    const MAX_LOGGED_FAILURES: usize = 100;
    let extract_start = std::time::Instant::now();

    let archive_name_for_progress = prepared.archive_name.clone();
    let rss_before = crate::installer::current_rss_kb().unwrap_or(0);

    // Log to file to avoid stdout/stderr buffering issues
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true).append(true)
        .open("/tmp/clf3_rss.log")
    {
        use std::io::Write;
        let _ = writeln!(f, "[RSS-START] {} : {}MB ({}directives)",
            archive_name_for_progress, rss_before / 1024, prepared.resolved.len());
    }

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
                reporter,
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
                reporter,
            );
        }

        // Process textures from BSA
        if !prepared.tex_d2.is_empty() {
            if let Some(tx) = dds_spill_tx {
                // Phased path: spill raw texture data for deferred processing
                super::streaming::extract_textures_from_bsa(
                    &prepared.archive_path, &prepared.tex_d2, tx,
                );
            } else {
                // Overlapped path: process inline
                let (ok, fail) = process_textures_from_bsa_streaming(
                    &prepared.archive_path, &prepared.tex_d2, ctx,
                );
                written.fetch_add(ok, Ordering::Relaxed);
                failed.fetch_add(fail, Ordering::Relaxed);
                let done = dds_counter.fetch_add(ok + fail, Ordering::Relaxed) + ok + fail;
                if let Some(ref s) = *dds_status.lock().expect("dds lock") {
                    s.set_count(done, total_textures);
                }
            }
        }

        extracted.fetch_add(arc_extracted.load(Ordering::Relaxed), Ordering::Relaxed);
        written.fetch_add(arc_written.load(Ordering::Relaxed), Ordering::Relaxed);
        skipped.fetch_add(arc_skipped.load(Ordering::Relaxed), Ordering::Relaxed);
        failed.fetch_add(arc_failed.load(Ordering::Relaxed), Ordering::Relaxed);

        // BSA reads don't have separate finalization — record all time as extraction
        let archive_bytes = std::fs::metadata(&prepared.archive_path)
            .map(|m| m.len())
            .unwrap_or(0);
        metrics.record(&archive_type, archive_bytes, extract_start.elapsed().as_millis() as u64, 0, 0);
    } else {
        // Extract via 7z/ZIP/RAR, then finalize
        let phase_extract_start = std::time::Instant::now();
        let result = process_single_archive_fused(
            &prepared.archive_path,
            &prepared.archive_hash,
            &prepared.resolved,
            ctx,
            Some(1),
            &prepared.extra_paths,
            None, // pipeline path doesn't use listing cache yet
        );
        let extract_elapsed = phase_extract_start.elapsed();

        match result {
            Ok(archive_result) => {
                extracted.fetch_add(
                    archive_result.extracted_count + archive_result.patched_count,
                    Ordering::Relaxed,
                );
                skipped.fetch_add(archive_result.skipped_count, Ordering::Relaxed);
                if archive_result.failed_count > 0 {
                    error!(
                        "Archive {} extraction had {} failures",
                        prepared.archive_name, archive_result.failed_count
                    );
                }
                failed.fetch_add(archive_result.failed_count, Ordering::Relaxed);

                // DDS textures: spill raw data for deferred processing, or process inline
                let phase_dds_start = std::time::Instant::now();
                if let Some(tx) = dds_spill_tx {
                    // Phased path: spill raw texture data to channel for Phase 2
                    if !prepared.tex_d2.is_empty() {
                        super::streaming::extract_textures_from_temp_dir(
                            archive_result.temp_dir.path(), &prepared.tex_d2, tx,
                        );
                    }
                    if !prepared.tex_d3.is_empty() {
                        super::streaming::extract_textures_from_nested_bsas(
                            archive_result.temp_dir.path(), &prepared.tex_d3, tx,
                        );
                    }
                } else {
                    // Overlapped path: process inline while temp dir exists
                    if !prepared.tex_d2.is_empty() {
                        let (ok, fail) = process_textures_from_temp_streaming(
                            archive_result.temp_dir.path(), &prepared.tex_d2, ctx,
                        );
                        written.fetch_add(ok, Ordering::Relaxed);
                        failed.fetch_add(fail, Ordering::Relaxed);
                        let done = dds_counter.fetch_add(ok + fail, Ordering::Relaxed) + ok + fail;
                        if let Some(ref s) = *dds_status.lock().expect("dds lock") {
                            s.set_count(done, total_textures);
                        }
                    }
                    if !prepared.tex_d3.is_empty() {
                        let (ok, fail) = process_textures_from_nested_bsas_streaming(
                            archive_result.temp_dir.path(), &prepared.tex_d3, ctx,
                        );
                        written.fetch_add(ok, Ordering::Relaxed);
                        failed.fetch_add(fail, Ordering::Relaxed);
                        let done = dds_counter.fetch_add(ok + fail, Ordering::Relaxed) + ok + fail;
                        if let Some(ref s) = *dds_status.lock().expect("dds lock") {
                            s.set_count(done, total_textures);
                        }
                    }
                }
                let dds_elapsed = phase_dds_start.elapsed();

                let phase_fin_start = std::time::Instant::now();
                let fin_stats =
                    finalize_archive(archive_result, &ctx.config.output_dir, logged_failures, reporter, &ctx.dir_cache);
                written.fetch_add(fin_stats.written, Ordering::Relaxed);
                skipped.fetch_add(fin_stats.skipped, Ordering::Relaxed);
                if fin_stats.failed > 0 {
                    error!(
                        "Archive {} finalization had {} failures",
                        prepared.archive_name, fin_stats.failed
                    );
                }
                failed.fetch_add(fin_stats.failed, Ordering::Relaxed);
                let fin_elapsed = phase_fin_start.elapsed();

                // Record per-phase metrics
                let archive_bytes = std::fs::metadata(&prepared.archive_path)
                    .map(|m| m.len())
                    .unwrap_or(0);
                let is_direct = fin_stats.written == 0 && fin_stats.failed == 0
                    && fin_stats.skipped == 0;
                metrics.record(
                    &archive_type,
                    archive_bytes,
                    extract_elapsed.as_millis() as u64,
                    dds_elapsed.as_millis() as u64,
                    if is_direct { 0 } else { fin_elapsed.as_millis() as u64 },
                );
                // Determine tier based on directive types (not finalize outcome)
                let has_patched = prepared.resolved.iter().any(|d| matches!(d, super::streaming::ArchiveDirective::Patched { .. }));
                let has_nested = prepared.resolved.iter().any(|d| matches!(d, super::streaming::ArchiveDirective::FromArchive { file_in_bsa: Some(_), .. }));
                let tier = if has_patched { 2u8 } else if has_nested { 1 } else { 0 };
                metrics.record_tier(tier, archive_bytes);
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

    #[cfg(target_os = "linux")]
    unsafe { libc::malloc_trim(0); }

    // Update extraction status counter
    let done = extract_counter.fetch_add(1, Ordering::Relaxed) + 1;
    if let Some(ref s) = *extract_status.lock().expect("extract_status lock") {
        s.set_count(done, total_archives);
    }
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
    reporter: &Arc<dyn super::progress::ProgressReporter>,
) -> Result<StreamingStats> {
    // Clean up leftover temp dirs from previous interrupted runs
    cleanup_temp_dirs(&ctx.config.downloads_dir, reporter);

    let rss_start = crate::installer::current_rss_kb().unwrap_or(0);
    reporter.log(&format!(
        "[RSS-PIPELINE] processing loop start: {}MB",
        rss_start / 1024
    ));

    // Stats
    let extracted = AtomicUsize::new(0);
    let written = AtomicUsize::new(0);
    let skipped = AtomicUsize::new(0);
    let failed = AtomicUsize::new(0);
    let logged_failures = Arc::new(AtomicUsize::new(0));
    let extraction_metrics = Arc::new(ExtractionMetrics::new());

    // Pre-compute counts for status counters (created later, after download scan)
    let total_archives: usize = {
        let mut all_hashes = std::collections::HashSet::new();
        all_hashes.extend(grouped.from_archive.keys());
        all_hashes.extend(grouped.patched.keys());
        all_hashes.extend(grouped.textures.keys());
        all_hashes.len()
    };
    let texture_count: usize = grouped.textures.values().map(|v| v.len()).sum();

    // Initialize GPU once for inline DDS processing (BC7 textures)
    if texture_count > 0 {
        use crate::textures::init_gpu;
        let _ = init_gpu();
    }

    // Pre-register GameFileSource archive paths so whole-file directives
    // that reference game files can find them before downloads start.
    {
        let archives = db.get_all_archives().unwrap_or_default();
        for archive in &archives {
            if archive.state_json.contains("GameFileSourceDownloader") {
                if let Ok(crate::modlist::DownloadState::GameFileSource(gf)) =
                    serde_json::from_str::<crate::modlist::DownloadState>(&archive.state_json)
                {
                    let game_file = &gf.game_file;
                    if let Some(resolved) = crate::paths::resolve_case_insensitive(
                        &ctx.config.game_dir,
                        game_file,
                    ) {
                        ctx.register_archive_path(archive.hash.clone(), resolved);
                    } else if let Some(resolved) = crate::paths::resolve_case_insensitive(
                        &ctx.config.game_dir,
                        &format!("Data/{}", game_file),
                    ) {
                        ctx.register_archive_path(archive.hash.clone(), resolved);
                    }
                }
            }
        }
    }

    // Process whole-file directives first (simple copy, no archive extraction needed)
    if !grouped.whole_file.is_empty() {
        reporter.log(&format!(
            "Copying {} whole-file directives...",
            grouped.whole_file.len()
        ));
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

    // Initialize the global thread budget for 7z processes
    super::thread_budget::init(extract_workers, max_concurrent);
    let active_count = std::sync::Mutex::new(0usize);
    let active_cvar = std::sync::Condvar::new();

    // Completion channel: extraction threads signal when done (hash of completed archive)
    let (done_tx, done_rx) = std::sync::mpsc::channel::<String>();

    // Status counters — created lazily on first extraction to avoid ghost bars
    // during the download scanning phase.
    let extract_status: Mutex<Option<Arc<dyn super::progress::ProgressHandle>>> = Mutex::new(None);
    let extract_counter = AtomicUsize::new(0);
    let dds_status: Mutex<Option<Arc<dyn super::progress::ProgressHandle>>> = Mutex::new(None);
    let dds_counter = AtomicUsize::new(0);
    let bsa_status: Mutex<Option<Arc<dyn super::progress::ProgressHandle>>> = Mutex::new(None);
    let bsa_built = AtomicUsize::new(0);
    let status_bars_created = AtomicBool::new(false);

    extraction_metrics.start_wall_clock(max_concurrent);

    std::thread::scope(|thread_scope| {
        let mut archives_processed = 0usize;
        let mut archives_failed = 0usize;

        // Rebind shared state as references so `move` closures copy the references
        let extracted = &extracted;
        let written = &written;
        let skipped = &skipped;
        let failed = &failed;
        let logged_failures = &logged_failures;
        let reporter = reporter;
        let active_count = &active_count;
        let active_cvar = &active_cvar;
        let extract_status = &extract_status;
        let extract_counter = &extract_counter;
        let dds_status = &dds_status;
        let dds_counter = &dds_counter;
        let bsa_status = &bsa_status;
        let bsa_built = &bsa_built;
        let status_bars_created = &status_bars_created;

        // BSA readiness tracker — builds BSAs as soon as all their source archives are processed
        let mut bsa_tracker = BsaReadinessTracker::new(db, ctx, grouped)
            .unwrap_or_else(|e| {
                warn!("Failed to initialize BSA tracker: {}", e);
                reporter.log(&format!(
                    "WARNING: BSA tracker init failed: {} — BSAs will be built at end instead of incrementally",
                    e
                ));
                BsaReadinessTracker {
                    bsa_directives: HashMap::new(),
                    pending_sources: HashMap::new(),
                    built_count: 0,
                }
            });

        let total_bsa_tracked = bsa_tracker.pending_sources.len();

        // Helper: create status bars on first use
        let ensure_status_bars = || {
            if !status_bars_created.swap(true, Ordering::Relaxed) {
                let mut es = extract_status.lock().expect("extract_status lock");
                if es.is_none() {
                    let s = reporter.begin_status("Extracted");
                    s.set_count(0, total_archives);
                    *es = Some(s);
                }
                if texture_count > 0 {
                    let mut ds = dds_status.lock().expect("dds_status lock");
                    if ds.is_none() {
                        let s = reporter.begin_status("DDS");
                        s.set_count(0, texture_count);
                        *ds = Some(s);
                    }
                }
                let mut bs = bsa_status.lock().expect("bsa_status lock");
                if bs.is_none() {
                    let s = reporter.begin_status("BSA");
                    let total = total_bsa_tracked;
                    if total > 0 { s.set_count(0, total); }
                    *bs = Some(s);
                }
            }
        };

        // Update progress bar for extraction phase
        reporter.overall_set_total(total_archives as u64);
        reporter.overall_set_message("Extracting archives...");

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

                        // Build BSA on a separate thread
                        thread_scope.spawn(move || {
                            match handle_create_bsa(ctx, &directive) {
                                Ok(()) => {
                                    let done = bsa_built.fetch_add(1, Ordering::Relaxed) + 1;
                                    if let Some(ref s) = *bsa_status.lock().expect("bsa lock") {
                                        s.set_count(done, total_bsa_tracked);
                                    }
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
                    // Create status bars on first archive (avoids ghost bars during scan)
                    ensure_status_bars();
                    // Register the archive path for extraction
                    ctx.register_archive_path(hash.clone(), path.clone());

                    // Phase 1: Prepare (DB work — needs ModlistDb, runs on main thread)
                    let prepared = prepare_archive(
                        db, ctx, &hash, &name, &path, grouped,
                    );

                    if let Some(prepared) = prepared {
                        // Throttle if RSS exceeds 90% of total RAM
                        memory_pressure_gate(90);

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

                        let metrics = extraction_metrics.clone();
                        thread_scope.spawn(move || {
                            extract_prepared_archive(
                                prepared,
                                ctx,
                                extracted,
                                written,
                                skipped,
                                failed,
                                logged_failures,
                                reporter,
                                extract_status,
                                extract_counter,
                                total_archives,
                                dds_status,
                                dds_counter,
                                texture_count,
                                &metrics,
                                None, // overlapped path: process DDS inline
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
                    reporter.overall_set_message(&format!(
                        "Extracting {} ({}/{})",
                        name, archives_processed, total_archives,
                    ));
                    reporter.overall_inc();
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

                    match handle_create_bsa(ctx, &directive) {
                        Ok(()) => {
                            let done = bsa_built.fetch_add(1, Ordering::Relaxed) + 1;
                            if let Some(ref s) = *bsa_status.lock().expect("bsa lock") {
                                s.set_count(done, total_bsa_tracked);
                            }
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
            reporter.log(&format!(
                "Pipeline complete: {} archives processed, {} failed, {} BSAs built early",
                archives_processed, archives_failed, bsa_tracker.built_count
            ));
        } else {
            reporter.log(&format!(
                "Pipeline complete: {} archives processed, {} failed",
                archives_processed, archives_failed
            ));
        }
    });

    extraction_metrics.stop_wall_clock();

    // Finish status counters
    if let Some(ref s) = *extract_status.lock().expect("lock") { s.finish(); }
    if let Some(ref s) = *bsa_status.lock().expect("lock") { s.finish(); }
    if let Some(ref s) = *dds_status.lock().expect("lock") { s.finish(); }

    extraction_metrics.log_summary(reporter);

    Ok(StreamingStats {
        extracted: extracted.load(Ordering::Relaxed),
        written: written.load(Ordering::Relaxed),
        skipped: skipped.load(Ordering::Relaxed) + grouped.pre_skipped,
        failed: failed.load(Ordering::Relaxed),
        failed_archive_hashes: Vec::new(),
    })
}

/// Phased processing loop for re-installs where all archives are already downloaded.
///
/// Instead of overlapping extraction/DDS/BSA, runs sequential phases where each
/// gets full CPU:
///
/// Phase 1: Complex extraction (patched, BSA-feeding, DDS, nested) — full cores
/// Phase 2: DDS processing safety net (near-empty, textures done inline)
/// Phase 3: BSA building — full cores for compression
/// Phase 4: Simple extraction (direct FromArchive only) — full cores
pub(crate) fn run_processing_loop_phased(
    db: &ModlistDb,
    ctx: &ProcessContext,
    rx: &std::sync::mpsc::Receiver<ArchiveEvent>,
    grouped: &GroupedDirectives,
    config: StreamingConfig,
    reporter: &Arc<dyn super::progress::ProgressReporter>,
) -> Result<StreamingStats> {
    cleanup_temp_dirs(&ctx.config.downloads_dir, reporter);

    let rss_start = crate::installer::current_rss_kb().unwrap_or(0);
    reporter.log(&format!("[RSS-PHASED] start: {}MB", rss_start / 1024));

    // Stats (Arc-wrapped for compatibility with streaming helpers)
    let extracted = Arc::new(AtomicUsize::new(0));
    let written = Arc::new(AtomicUsize::new(0));
    let skipped = Arc::new(AtomicUsize::new(0));
    let failed = Arc::new(AtomicUsize::new(0));
    let logged_failures = Arc::new(AtomicUsize::new(0));
    let extraction_metrics = Arc::new(ExtractionMetrics::new());

    let total_archives: usize = {
        let mut all_hashes = HashSet::new();
        all_hashes.extend(grouped.from_archive.keys());
        all_hashes.extend(grouped.patched.keys());
        all_hashes.extend(grouped.textures.keys());
        all_hashes.len()
    };
    let texture_count: usize = grouped.textures.values().map(|v| v.len()).sum();

    let extract_workers = config.max_extract_workers.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|n| n.get().max(2))
            .unwrap_or(4)
    });

    // Process whole-file directives first
    if !grouped.whole_file.is_empty() {
        reporter.log(&format!(
            "Copying {} whole-file directives...", grouped.whole_file.len()
        ));
        process_whole_file_directives(&grouped.whole_file, ctx, &extracted, &skipped, &failed);
    }

    // === Drain all archive events from channel ===
    reporter.log("Draining archive events...");
    let mut all_events: Vec<(String, String, PathBuf)> = Vec::new();
    while let Ok(event) = rx.recv() {
        match event {
            ArchiveEvent::Ready { hash, name, path } => {
                ctx.register_archive_path(hash.clone(), path.clone());
                all_events.push((hash, name, path));
            }
            ArchiveEvent::Failed { name, error, .. } => {
                error!("Archive download failed: {}: {}", name, error);
            }
            ArchiveEvent::Manual { name, .. } => {
                warn!("Manual download needed: {}", name);
            }
        }
    }

    // === Prepare all archives (DB work on main thread) ===
    reporter.log(&format!("Preparing {} archives...", all_events.len()));
    let mut all_prepared: Vec<PreparedArchive> = Vec::new();
    for (hash, name, path) in &all_events {
        if let Some(prepared) = prepare_archive(db, ctx, hash, name, path, grouped) {
            all_prepared.push(prepared);
        }
    }

    // === Classify into complex vs simple ===
    let (complex, simple): (Vec<_>, Vec<_>) = all_prepared.into_iter().partition(|p| {
        let has_patched = p.resolved.iter().any(|d| matches!(d, ArchiveDirective::Patched { .. }));
        let has_nested = p.resolved.iter().any(|d| {
            matches!(d, ArchiveDirective::FromArchive { file_in_bsa: Some(_), .. })
        });
        let has_dds = !p.tex_d2.is_empty() || !p.tex_d3.is_empty();
        let feeds_bsa = p.resolved.iter().any(|d| {
            let to_path = match d {
                ArchiveDirective::FromArchive { directive, .. } => &directive.to,
                ArchiveDirective::Patched { directive, .. } => &directive.to,
            };
            to_path.contains("TEMP_BSA_FILES")
        });
        has_patched || has_nested || has_dds || feeds_bsa
    });

    reporter.log(&format!(
        "  Phased extraction: {} complex + {} simple archives ({} cores)",
        complex.len(), simple.len(), extract_workers,
    ));

    // Status bars
    let extract_status: Arc<dyn super::progress::ProgressHandle> =
        reporter.begin_status("Extracted");
    extract_status.set_count(0, total_archives);
    let extract_counter = AtomicUsize::new(0);

    let dds_status: Option<Arc<dyn super::progress::ProgressHandle>> = if texture_count > 0 {
        let s = reporter.begin_status("DDS");
        s.set_count(0, texture_count);
        Some(s)
    } else {
        None
    };
    let dds_counter = AtomicUsize::new(0);

    reporter.overall_set_total(total_archives as u64);
    reporter.overall_set_message("Phase 1: Complex extraction...");

    extraction_metrics.start_wall_clock(extract_workers);

    // DDS spill: extraction threads send raw texture data through this channel.
    // A collector thread writes them to temp files on disk. Phase 2 processes them all at once.
    let dds_spill_dir = ctx.config.output_dir.join(".clf3_dds_spill");
    if dds_spill_dir.exists() {
        let _ = std::fs::remove_dir_all(&dds_spill_dir);
    }
    let _ = std::fs::create_dir_all(&dds_spill_dir);
    let collected_dds_jobs: Arc<Mutex<Vec<super::streaming::SpilledDdsJob>>> =
        Arc::new(Mutex::new(Vec::new()));

    let (dds_tx, dds_rx) = std::sync::mpsc::sync_channel::<super::streaming::DdsJob>(32);

    // === Phase 1: Complex Extraction (full CPU, DDS spilled) ===
    let phase1_start = std::time::Instant::now();
    if !complex.is_empty() {
        reporter.log(&format!("  Phase 1: {} complex archives...", complex.len()));
        super::thread_budget::init(extract_workers, extract_workers.min(4).max(2));

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(extract_workers)
            .thread_name(|i| format!("phase1-{}", i))
            .build()
            .expect("Failed to build phase 1 pool");

        // Collector thread: spills DDS data to disk as extraction runs
        let collected_jobs = collected_dds_jobs.clone();
        let spill_dir_clone = dds_spill_dir.clone();
        let collector_handle = std::thread::spawn(move || {
            let mut jobs = Vec::new();
            let mut idx = 0u64;
            while let Ok(job) = dds_rx.recv() {
                let data_path = spill_dir_clone.join(format!("dds_{}.tmp", idx));
                if let Err(e) = std::fs::write(&data_path, &job.data) {
                    warn!("DDS spill write failed: {}", e);
                    continue;
                }
                jobs.push(super::streaming::SpilledDdsJob {
                    id: job.id,
                    directive: job.directive,
                    data_path,
                });
                idx += 1;
            }
            let count = jobs.len();
            collected_jobs.lock().unwrap_or_else(|e| e.into_inner()).extend(jobs);
            count
        });

        let cursor = AtomicUsize::new(0);
        pool.scope(|s| {
            for _ in 0..extract_workers {
                s.spawn(|_| {
                    loop {
                        let idx = cursor.fetch_add(1, Ordering::Relaxed);
                        if idx >= complex.len() { break; }

                        // Throttle if RSS exceeds 95% of total RAM
                        memory_pressure_gate(95);

                        let prepared = &complex[idx];
                        let owned = PreparedArchive {
                            archive_hash: prepared.archive_hash.clone(),
                            archive_name: prepared.archive_name.clone(),
                            archive_path: prepared.archive_path.clone(),
                            resolved: prepared.resolved.clone(),
                            tex_d2: prepared.tex_d2.clone(),
                            tex_d3: prepared.tex_d3.clone(),
                            extra_paths: prepared.extra_paths.clone(),
                        };
                        let wrap_status = Mutex::new(Some(extract_status.clone()));
                        let wrap_dds = Mutex::new(dds_status.clone());
                        extract_prepared_archive(
                            owned, ctx,
                            &extracted, &written, &skipped, &failed,
                            &logged_failures, reporter,
                            &wrap_status, &extract_counter, total_archives,
                            &wrap_dds, &dds_counter, texture_count,
                            &extraction_metrics,
                            Some(&dds_tx), // spill DDS to channel
                        );
                        reporter.overall_inc();
                    }
                });
            }
        });

        // Drop sender so collector thread finishes
        drop(dds_tx);
        let spilled_count = collector_handle.join().unwrap_or(0);
        if spilled_count > 0 {
            reporter.log(&format!("  Collected {} DDS textures for Phase 2", spilled_count));
        }
    } else {
        drop(dds_tx);
    }
    reporter.log(&format!(
        "  Phase 1 complete in {:.1}s ({} complex archives)",
        phase1_start.elapsed().as_secs_f64(), complex.len()
    ));

    // === Phase 2: DDS Processing (full CPU + GPU) ===
    let phase2_start = std::time::Instant::now();
    reporter.overall_set_message("Phase 2: DDS processing...");
    {
        let dds_jobs = std::mem::take(
            &mut *collected_dds_jobs.lock().unwrap_or_else(|e| e.into_inner()),
        );
        if !dds_jobs.is_empty() {
            let dds_total = dds_jobs.len();
            reporter.log(&format!("  Phase 2: Processing {} DDS textures...", dds_total));
            if let Some(ref s) = dds_status {
                s.set_count(0, dds_total);
            }
            let dds_status_ref = &dds_status;
            super::streaming::process_spilled_dds_jobs_with_progress(
                dds_jobs,
                ctx,
                Some(&|done, total| {
                    if let Some(ref s) = *dds_status_ref {
                        s.set_count(done, total);
                    }
                }),
                Some(&written),
            );
            reporter.log(&format!(
                "  Phase 2 complete in {:.1}s",
                phase2_start.elapsed().as_secs_f64()
            ));
        }
    }
    // Clean up spill directory
    if dds_spill_dir.exists() {
        let _ = std::fs::remove_dir_all(&dds_spill_dir);
    }

    // === Phase 3: BSA Building (full CPU for compression) ===
    let phase3_start = std::time::Instant::now();
    reporter.overall_set_message("Phase 3: BSA building...");
    {
        let bsa_directives: Vec<(i64, CreateBSADirective)> = {
            let raw = db.get_all_pending_directives_of_type("CreateBSA")
                .unwrap_or_default();
            raw.into_iter()
                .filter_map(|(id, json)| {
                    match serde_json::from_str::<crate::modlist::Directive>(&json) {
                        Ok(crate::modlist::Directive::CreateBSA(d)) => {
                            if !output_bsa_valid(ctx, &d) {
                                Some((id, d))
                            } else {
                                None
                            }
                        }
                        _ => None,
                    }
                })
                .collect()
        };

        if !bsa_directives.is_empty() {
            let bsa_total = bsa_directives.len();
            reporter.log(&format!("  Phase 3: Building {} BSA archives (2 concurrent)...", bsa_total));
            let bsa_status = reporter.begin_status("BSA");
            bsa_status.set_count(0, bsa_total);
            let bsa_built = AtomicUsize::new(0);

            // Process 2 BSAs concurrently: while one compresses (CPU), the next
            // reads its staged files from disk (I/O). This eliminates the gap
            // between sequential BSA builds.
            let bsa_cursor = AtomicUsize::new(0);
            std::thread::scope(|s| {
                for _ in 0..2 {
                    s.spawn(|| {
                        loop {
                            let idx = bsa_cursor.fetch_add(1, Ordering::Relaxed);
                            if idx >= bsa_total { break; }
                            let (_id, directive) = &bsa_directives[idx];
                            let bsa_name = std::path::Path::new(&directive.to)
                                .file_name()
                                .map(|n| n.to_string_lossy().to_string())
                                .unwrap_or_else(|| directive.to.clone());
                            match handle_create_bsa(ctx, directive) {
                                Ok(()) => {
                                    let done = bsa_built.fetch_add(1, Ordering::Relaxed) + 1;
                                    bsa_status.set_count(done, bsa_total);
                                }
                                Err(e) => {
                                    error!("Failed to build BSA {}: {:#}", bsa_name, e);
                                    failed.fetch_add(1, Ordering::Relaxed);
                                    bsa_built.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    });
                }
            });
            bsa_status.finish();
        }
    }
    reporter.log(&format!(
        "  Phase 3 complete in {:.1}s",
        phase3_start.elapsed().as_secs_f64()
    ));

    // === Phase 4: Simple Extraction (full CPU) ===
    let phase4_start = std::time::Instant::now();
    reporter.overall_set_message("Phase 4: Simple extraction...");
    if !simple.is_empty() {
        reporter.log(&format!("  Phase 4: {} simple archives...", simple.len()));
        super::thread_budget::init(extract_workers, extract_workers);

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(extract_workers)
            .thread_name(|i| format!("phase4-{}", i))
            .build()
            .expect("Failed to build phase 4 pool");

        let cursor = AtomicUsize::new(0);
        pool.scope(|s| {
            for _ in 0..extract_workers {
                s.spawn(|_| {
                    loop {
                        let idx = cursor.fetch_add(1, Ordering::Relaxed);
                        if idx >= simple.len() { break; }

                        // Throttle if RSS exceeds 90% of total RAM
                        memory_pressure_gate(90);

                        let prepared = &simple[idx];
                        let owned = PreparedArchive {
                            archive_hash: prepared.archive_hash.clone(),
                            archive_name: prepared.archive_name.clone(),
                            archive_path: prepared.archive_path.clone(),
                            resolved: prepared.resolved.clone(),
                            tex_d2: prepared.tex_d2.clone(),
                            tex_d3: prepared.tex_d3.clone(),
                            extra_paths: prepared.extra_paths.clone(),
                        };
                        let wrap_status = Mutex::new(Some(extract_status.clone()));
                        let wrap_dds = Mutex::new(dds_status.clone());
                        extract_prepared_archive(
                            owned, ctx,
                            &extracted, &written, &skipped, &failed,
                            &logged_failures, reporter,
                            &wrap_status, &extract_counter, total_archives,
                            &wrap_dds, &dds_counter, texture_count,
                            &extraction_metrics,
                            None, // simple archives have no DDS
                        );
                        reporter.overall_inc();
                    }
                });
            }
        });
    }
    reporter.log(&format!(
        "  Phase 4 complete in {:.1}s ({} simple archives)",
        phase4_start.elapsed().as_secs_f64(), simple.len()
    ));

    extraction_metrics.stop_wall_clock();
    extract_status.finish();
    if let Some(ref s) = dds_status { s.finish(); }

    extraction_metrics.log_summary(reporter);

    reporter.log(&format!(
        "Phased pipeline complete: {} complex + {} simple archives",
        complex.len(), simple.len()
    ));

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
