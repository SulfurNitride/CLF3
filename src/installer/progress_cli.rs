//! CLI progress reporter using indicatif + console
//!
//! Owns the `MultiProgress` display and provides a `MakeWriter` for tracing integration.

use super::progress::{
    NullHandle, Phase, ProgressEvent, ProgressHandle, ProgressLogLevel, ProgressMode,
    ProgressReporter, ProgressSnapshot, ProgressUnit, TaskId, TaskKind, TaskOutcome, TaskSnapshot,
    TaskStage, TaskStarted, TaskUpdate,
};
use console::style;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// CLI reporter backed by indicatif `MultiProgress`.
pub struct CliReporter {
    mode: ProgressMode,
    mp: MultiProgress,
    overall: Mutex<Option<ProgressBar>>,
    active_header: ProgressBar,
    active: Mutex<HashMap<TaskId, TaskSnapshot>>,
    last_snapshot: Mutex<Option<Instant>>,
    plain_last_update: Mutex<HashMap<TaskId, Instant>>,
    snapshot_shutdown: AtomicBool,
    next_task_id: AtomicU64,
    /// Fixed pool of reusable bars for concurrent items (downloads, archive extractions).
    pool: Vec<ProgressBar>,
    pool_available: Vec<Mutex<bool>>,
}

impl CliReporter {
    /// Create a new CLI reporter with a bar pool of the given size.
    pub fn new(pool_size: usize, mode: ProgressMode) -> Arc<Self> {
        let mp = MultiProgress::new();
        let idle_style = ProgressStyle::default_bar().template("").unwrap();

        let active_header = mp.add(ProgressBar::new_spinner());
        active_header.set_style(
            ProgressStyle::default_spinner()
                .template("{msg}")
                .unwrap(),
        );
        if mode.is_interactive() {
            active_header.set_style(idle_style.clone());
        } else {
            active_header.set_style(idle_style.clone());
        }

        let mut pool = Vec::with_capacity(pool_size);
        let mut pool_available = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            let bar = mp.add(ProgressBar::new(0));
            bar.set_style(idle_style.clone());
            pool.push(bar);
            pool_available.push(Mutex::new(true));
        }

        let reporter = Arc::new(Self {
            mode,
            mp,
            overall: Mutex::new(None),
            active_header,
            active: Mutex::new(HashMap::new()),
            last_snapshot: Mutex::new(None),
            plain_last_update: Mutex::new(HashMap::new()),
            snapshot_shutdown: AtomicBool::new(false),
            next_task_id: AtomicU64::new(1),
            pool,
            pool_available,
        });

        if mode == ProgressMode::Snapshot {
            Self::spawn_snapshot_ticker(&reporter);
        }

        reporter
    }

    /// Get a `MakeWriter` for the tracing subscriber that routes through this reporter's
    /// `MultiProgress`. Call this before initializing the tracing subscriber.
    pub fn make_writer_factory(self: &Arc<Self>) -> CliWriterFactory {
        CliWriterFactory {
            mp: self.mp.clone(),
            use_multi_progress: self.mode.is_interactive(),
        }
    }

    fn claim_pool_bar(&self) -> Option<usize> {
        for (i, available) in self.pool_available.iter().enumerate() {
            let mut guard = available.lock().expect("pool lock");
            if *guard {
                *guard = false;
                self.show_active_header();
                return Some(i);
            }
        }
        None
    }

    fn release_pool_bar(&self, index: usize) {
        let idle_style = ProgressStyle::default_bar().template("").unwrap();
        self.pool[index].set_style(idle_style);
        self.pool[index].set_prefix("");
        self.pool[index].set_message("");
        self.pool[index].set_position(0);
        self.pool[index].set_length(0);
        self.pool[index].disable_steady_tick();
        *self.pool_available[index].lock().expect("pool lock") = true;
        self.hide_active_header_if_idle();
    }

    fn show_active_header(&self) {
        if !self.mode.is_interactive() {
            return;
        }
        self.active_header.set_style(
            ProgressStyle::default_spinner()
                .template("{msg}")
                .unwrap(),
        );
        self.active_header.set_message("Active workers");
    }

    fn hide_active_header_if_idle(&self) {
        if !self.mode.is_interactive() {
            return;
        }
        let any_busy = self
            .pool_available
            .iter()
            .any(|available| !*available.lock().expect("pool lock"));
        if !any_busy {
            self.active_header
                .set_style(ProgressStyle::default_bar().template("").unwrap());
            self.active_header.set_message("");
        }
    }

    fn ensure_overall(&self) -> ProgressBar {
        let mut guard = self.overall.lock().expect("overall lock");
        if let Some(pb) = guard.as_ref() {
            return pb.clone();
        }
        let pb = self.mp.add(ProgressBar::new(0));
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) | {msg}",
                )
                .unwrap()
                .progress_chars("=>-"),
        );
        pb.enable_steady_tick(Duration::from_millis(100));
        *guard = Some(pb.clone());
        pb
    }

    fn spawn_snapshot_ticker(this: &Arc<Self>) {
        let reporter = Arc::downgrade(this);
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_millis(250));
            let Some(reporter) = reporter.upgrade() else {
                break;
            };
            if reporter.snapshot_shutdown.load(Ordering::Relaxed) {
                break;
            }
            reporter.emit_snapshot();
        });
    }

    fn emit_snapshot(&self) {
        let active = self.active.lock().expect("active lock");
        let snapshot = ProgressEvent::Snapshot(ProgressSnapshot::from_active(&active));
        if let Ok(line) = serde_json::to_string(&snapshot) {
            println!("{}", line);
        }
    }

    fn update_active_state(&self, event: &ProgressEvent) {
        match event {
            ProgressEvent::TaskStarted(task) => {
                self.active
                    .lock()
                    .expect("active lock")
                    .insert(task.id.clone(), task.clone().into());
            }
            ProgressEvent::TaskUpdate(update) => {
                if let Some(task) = self.active.lock().expect("active lock").get_mut(&update.id) {
                    task.apply_update(update);
                }
            }
            ProgressEvent::TaskFinished { id, outcome, .. } => {
                {
                    if let Some(task) = self.active.lock().expect("active lock").get_mut(id) {
                        task.stage = match outcome {
                            TaskOutcome::Success | TaskOutcome::Skipped => {
                                super::progress::TaskStage::Complete
                            }
                            TaskOutcome::Failed { .. } => super::progress::TaskStage::Failed,
                            TaskOutcome::Manual => super::progress::TaskStage::Manual,
                        };
                    }
                }
                self.active.lock().expect("active lock").remove(id);
                self.plain_last_update
                    .lock()
                    .expect("plain update lock")
                    .remove(id);
            }
            _ => {}
        }
    }

    fn emit_machine_event(&self, event: &ProgressEvent) {
        match self.mode {
            ProgressMode::Json => {
                if let Ok(line) = serde_json::to_string(event) {
                    println!("{}", line);
                }
            }
            ProgressMode::Snapshot => {
                let should_emit = {
                    let mut last = self.last_snapshot.lock().expect("snapshot lock");
                    match *last {
                        Some(t) if t.elapsed() < Duration::from_millis(250) => false,
                        _ => {
                            *last = Some(Instant::now());
                            true
                        }
                    }
                };
                if should_emit {
                    self.emit_snapshot();
                }
            }
            _ => {}
        }
    }

    fn render_plain_event(&self, event: &ProgressEvent) {
        match event {
            ProgressEvent::PhaseStarted { phase, .. } => {
                eprintln!("=== {} ===", phase);
            }
            ProgressEvent::TaskStarted(task) => {
                eprintln!(
                    "{} started: {}",
                    task_kind_label(task.kind),
                    task.label
                );
            }
            ProgressEvent::TaskUpdate(update) => {
                let should_emit = update.stage.is_some()
                    || update.message.is_some()
                    || {
                        let mut last = self.plain_last_update.lock().expect("plain update lock");
                        match last.get(&update.id) {
                            Some(t) if t.elapsed() < Duration::from_secs(1) => false,
                            _ => {
                                last.insert(update.id.clone(), Instant::now());
                                true
                            }
                        }
                    };
                if !should_emit {
                    return;
                }
                if let Some(task) = self
                    .active
                    .lock()
                    .expect("active lock")
                    .get(&update.id)
                    .cloned()
                {
                    eprintln!(
                        "{} progress: {}{}",
                        task_kind_label(task.kind),
                        task.label,
                        format_task_suffix(&task)
                    );
                }
            }
            ProgressEvent::TaskFinished { id, outcome, .. } => {
                match outcome {
                    TaskOutcome::Success => eprintln!("task finished: {}", id),
                    TaskOutcome::Skipped => eprintln!("task skipped: {}", id),
                    TaskOutcome::Manual => eprintln!("task needs manual action: {}", id),
                    TaskOutcome::Failed { error } => {
                        eprintln!("task failed: {}: {}", id, error);
                    }
                }
            }
            ProgressEvent::Log { level, message, .. } => {
                eprintln!("{}: {}", log_level_label(*level), message);
            }
            ProgressEvent::Snapshot(_) => {}
        }
    }

    fn next_task_id(&self, kind: TaskKind) -> TaskId {
        let prefix = match kind {
            TaskKind::Download => "download",
            TaskKind::Verify => "verify",
            TaskKind::Extract => "extract",
            TaskKind::Finalize => "finalize",
            TaskKind::Dds => "dds",
            TaskKind::Bsa => "bsa",
            TaskKind::Cleanup => "cleanup",
        };
        let id = self.next_task_id.fetch_add(1, Ordering::Relaxed);
        TaskId::new(format!("{}:{}", prefix, id))
    }
}

impl ProgressReporter for CliReporter {
    fn emit(&self, event: ProgressEvent) {
        self.update_active_state(&event);
        if self.mode.is_machine_readable() {
            self.emit_machine_event(&event);
        } else if !self.mode.is_interactive() {
            self.render_plain_event(&event);
        }
    }

    fn mode(&self) -> ProgressMode {
        self.mode
    }

    fn phase_start(&self, phase: Phase) {
        self.emit(ProgressEvent::PhaseStarted {
            phase,
            timestamp_ms: super::progress::now_ms(),
        });
        if self.mode.is_machine_readable() {
            return;
        }
        if !self.mode.is_interactive() {
            return;
        }
        // Finish any leftover overall bar from the previous phase
        {
            let mut guard = self.overall.lock().expect("overall lock");
            if let Some(pb) = guard.take() {
                pb.finish_and_clear();
            }
        }
        let header = format!("=== {} ===", phase);
        let _ = self.mp.println(format!("{}", style(header).bold().cyan()));
    }

    fn overall_set_total(&self, total: u64) {
        if !self.mode.is_interactive() {
            return;
        }
        let pb = self.ensure_overall();
        pb.set_length(total);
        pb.set_position(0);
    }

    fn overall_inc(&self) {
        if !self.mode.is_interactive() {
            return;
        }
        let pb = self.ensure_overall();
        pb.inc(1);
    }

    fn overall_set_message(&self, msg: &str) {
        if !self.mode.is_interactive() {
            return;
        }
        let pb = self.ensure_overall();
        pb.set_message(msg.to_string());
    }

    fn overall_finish(&self) {
        if !self.mode.is_interactive() {
            return;
        }
        let mut guard = self.overall.lock().expect("overall lock");
        if let Some(pb) = guard.take() {
            pb.finish_and_clear();
        }
    }

    fn begin_item(&self, name: &str, total_bytes: Option<u64>) -> Arc<dyn ProgressHandle> {
        if !self.mode.is_interactive() {
            let (kind, stage, unit) = if total_bytes.is_some() {
                (
                    TaskKind::Download,
                    TaskStage::Downloading,
                    ProgressUnit::Bytes,
                )
            } else {
                (
                    TaskKind::Extract,
                    TaskStage::Extracting,
                    ProgressUnit::Items,
                )
            };
            let id = self.next_task_id(kind);
            self.task_start(TaskStarted::new(
                id.clone(),
                kind,
                stage,
                name.to_string(),
                unit,
                total_bytes,
            ));
            return Arc::new(EventHandle {
                reporter: self as *const CliReporter,
                id,
                finished: AtomicBool::new(false),
            });
        }
        if let Some(index) = self.claim_pool_bar() {
            let bar = &self.pool[index];
            bar.reset();

            if let Some(total) = total_bytes {
                // Byte-progress style (downloads)
                bar.set_style(
                    ProgressStyle::default_bar()
                        .template(
                            "  [{prefix:.cyan}] {msg:56} [{bar:28.white/dim}] {bytes}/{total_bytes} {bytes_per_sec}",
                        )
                        .unwrap()
                        .progress_chars("=>-"),
                );
                bar.set_length(total);
                bar.set_prefix(format!("{:02} download", index + 1));
            } else {
                // Spinner style (archive extractions)
                bar.set_style(
                    ProgressStyle::default_bar()
                        .template("  [{prefix:.cyan}] {msg:56} {pos}/{len}")
                        .unwrap()
                        .progress_chars("=>-"),
                );
                bar.set_prefix(format!("{:02} extract ", index + 1));
            }

            bar.set_message(truncate_progress_label(name, 56));
            bar.enable_steady_tick(Duration::from_millis(500));

            Arc::new(CliHandle {
                reporter: self as *const CliReporter,
                bar_index: index,
                kind: if total_bytes.is_some() {
                    TaskKind::Download
                } else {
                    TaskKind::Extract
                },
                stage: Mutex::new(if total_bytes.is_some() {
                    TaskStage::Downloading
                } else {
                    TaskStage::Extracting
                }),
                finished: AtomicBool::new(false),
            })
        } else {
            // Pool exhausted — return a null handle
            Arc::new(NullHandle)
        }
    }

    fn begin_status(&self, label: &str) -> Arc<dyn ProgressHandle> {
        if self.mode.is_machine_readable() {
            let (id, kind, stage, unit) = status_task_metadata(label);
            self.task_start(TaskStarted::new(
                id.clone(),
                kind,
                stage,
                label.to_string(),
                unit,
                None,
            ));
            return Arc::new(EventHandle {
                reporter: self as *const CliReporter,
                id,
                finished: AtomicBool::new(false),
            });
        }
        if !self.mode.is_interactive() {
            eprintln!("{}: started", label);
            return Arc::new(PlainStatusHandle {
                label: label.to_string(),
                last_update: Mutex::new(None),
                finished: AtomicBool::new(false),
            });
        }
        let bar = self.mp.add(ProgressBar::new(0));
        bar.set_style(
            ProgressStyle::default_bar()
                .template("  {prefix:.magenta} {wide_msg}")
                .unwrap(),
        );
        bar.set_prefix(status_prefix(label));
        bar.set_message(format!("{}: starting", label));
        bar.enable_steady_tick(Duration::from_millis(200));
        Arc::new(StatusHandle {
            bar,
            label: label.to_string(),
            finished: AtomicBool::new(false),
        })
    }

    fn log(&self, msg: &str) {
        self.emit(ProgressEvent::Log {
            level: ProgressLogLevel::Info,
            message: msg.to_string(),
            timestamp_ms: super::progress::now_ms(),
        });
        if self.mode.is_machine_readable() {
            eprintln!("{}", msg);
        } else if self.mode.is_interactive() {
            let _ = self.mp.println(msg);
        }
    }

    fn status(&self, msg: &str) {
        if !self.mode.is_interactive() {
            return;
        }
        let pb = self.ensure_overall();
        pb.set_message(msg.to_string());
    }
}

fn task_kind_label(kind: TaskKind) -> &'static str {
    match kind {
        TaskKind::Download => "download",
        TaskKind::Verify => "verify",
        TaskKind::Extract => "extract",
        TaskKind::Finalize => "finalize",
        TaskKind::Dds => "dds",
        TaskKind::Bsa => "bsa",
        TaskKind::Cleanup => "cleanup",
    }
}

fn task_stage_label(stage: TaskStage) -> &'static str {
    match stage {
        TaskStage::Queued => "queued",
        TaskStage::Resolving => "resolving",
        TaskStage::Downloading => "downloading",
        TaskStage::Verifying => "verifying",
        TaskStage::Extracting => "extracting",
        TaskStage::Finalizing => "finalizing",
        TaskStage::Transforming => "transforming",
        TaskStage::Building => "building",
        TaskStage::Waiting => "waiting",
        TaskStage::Complete => "complete",
        TaskStage::Failed => "failed",
        TaskStage::Manual => "manual",
    }
}

fn log_level_label(level: ProgressLogLevel) -> &'static str {
    match level {
        ProgressLogLevel::Info => "info",
        ProgressLogLevel::Warn => "warn",
        ProgressLogLevel::Error => "error",
    }
}

fn format_task_suffix(task: &TaskSnapshot) -> String {
    let mut parts = Vec::new();
    parts.push(task_stage_label(task.stage).to_string());
    if let (Some(position), Some(total)) = (task.position, task.total) {
        parts.push(format!("{}/{}", position, total));
    }
    if let Some(speed) = task.bytes_per_sec {
        parts.push(format!("{}/s", indicatif::HumanBytes(speed.max(0.0) as u64)));
    }
    if let Some(message) = &task.message {
        parts.push(message.clone());
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!(" ({})", parts.join(", "))
    }
}

fn truncate_progress_label(value: &str, max_chars: usize) -> String {
    let mut out = String::with_capacity(max_chars);
    for (idx, ch) in value.chars().enumerate() {
        if idx >= max_chars.saturating_sub(1) {
            out.push_str("...");
            return out;
        }
        out.push(ch);
    }
    out
}

fn status_prefix(label: &str) -> &'static str {
    match label.trim().to_ascii_lowercase().as_str() {
        "download" | "downloads" | "downloaded" => "Download",
        "extract" | "extracted" => "Extract ",
        "dds" | "textures" => "DDS     ",
        "bsa" | "bsas" => "BSA     ",
        "verify" | "verified" | "validating" => "Verify  ",
        _ => "Status  ",
    }
}

fn status_unit(label: &str) -> &'static str {
    match label.trim().to_ascii_lowercase().as_str() {
        "dds" | "textures" => "textures",
        "bsa" | "bsas" | "extract" | "extracted" | "download" | "downloads" | "downloaded"
        | "verify" | "verified" | "validating" => "archives",
        _ => "items",
    }
}

fn status_task_metadata(label: &str) -> (TaskId, TaskKind, TaskStage, ProgressUnit) {
    let normalized = label.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "download" | "downloads" | "downloaded" => (
            TaskId::new("status:download"),
            TaskKind::Download,
            TaskStage::Downloading,
            ProgressUnit::Archives,
        ),
        "extract" | "extracted" => (
            TaskId::new("status:extract"),
            TaskKind::Extract,
            TaskStage::Extracting,
            ProgressUnit::Archives,
        ),
        "dds" | "textures" => (
            TaskId::new("status:dds"),
            TaskKind::Dds,
            TaskStage::Transforming,
            ProgressUnit::Textures,
        ),
        "bsa" | "bsas" => (
            TaskId::new("status:bsa"),
            TaskKind::Bsa,
            TaskStage::Building,
            ProgressUnit::Archives,
        ),
        "verify" | "verified" | "validating" => (
            TaskId::new("status:verify"),
            TaskKind::Verify,
            TaskStage::Verifying,
            ProgressUnit::Archives,
        ),
        _ => (
            TaskId::new(format!("status:{}", normalized.replace(' ', "_"))),
            TaskKind::Finalize,
            TaskStage::Waiting,
            ProgressUnit::Items,
        ),
    }
}

/// Handle to a pooled progress bar. Releases back to pool on finish/drop.
struct CliHandle {
    // Raw pointer because CliReporter is always behind Arc and outlives handles.
    // We can't store Arc<CliReporter> because begin_item takes &self.
    reporter: *const CliReporter,
    bar_index: usize,
    kind: TaskKind,
    stage: Mutex<TaskStage>,
    finished: AtomicBool,
}

// SAFETY: CliReporter is Send+Sync (behind Arc), and we only access it through shared refs.
unsafe impl Send for CliHandle {}
unsafe impl Sync for CliHandle {}

impl CliHandle {
    fn reporter(&self) -> &CliReporter {
        // SAFETY: CliReporter is always behind Arc and outlives all handles.
        unsafe { &*self.reporter }
    }

    fn bar(&self) -> &ProgressBar {
        &self.reporter().pool[self.bar_index]
    }
}

impl ProgressHandle for CliHandle {
    fn set_bytes(&self, downloaded: u64, total: u64, _speed: f64) {
        let bar = self.bar();
        bar.set_length(total);
        bar.set_position(downloaded);
    }

    fn set_message(&self, msg: &str) {
        self.bar().set_message(truncate_progress_label(msg, 56));
    }

    fn set_stage(&self, stage: TaskStage) {
        *self.stage.lock().expect("stage lock") = stage;
        let kind = match self.kind {
            TaskKind::Download => "download",
            TaskKind::Extract => task_stage_label(stage),
            _ => task_kind_label(self.kind),
        };
        self.bar()
            .set_prefix(format!("{:02} {:<8}", self.bar_index + 1, kind));
    }

    fn set_count(&self, done: usize, total: usize) {
        let bar = self.bar();
        bar.set_length(total as u64);
        bar.set_position(done as u64);
    }

    fn finish(&self) {
        if !self.finished.swap(true, Ordering::Relaxed) {
            self.bar()
                .set_style(ProgressStyle::default_bar().template("").unwrap());
            self.bar().finish_and_clear();
            self.reporter().release_pool_bar(self.bar_index);
        }
    }

    fn finish_with_error(&self, msg: &str) {
        if !self.finished.swap(true, Ordering::Relaxed) {
            let _ = self.reporter().mp.println(format!("{}", style(msg).red()));
            self.bar()
                .set_style(ProgressStyle::default_bar().template("").unwrap());
            self.bar().finish_and_clear();
            self.reporter().release_pool_bar(self.bar_index);
        }
    }
}

impl Drop for CliHandle {
    fn drop(&mut self) {
        // Auto-finish if caller forgot
        if !self.finished.load(Ordering::Relaxed) {
            self.finish();
        }
    }
}

/// Event-only handle used by machine-readable progress modes while call sites
/// are migrated from legacy handles to typed tasks.
struct EventHandle {
    reporter: *const CliReporter,
    id: TaskId,
    finished: AtomicBool,
}

unsafe impl Send for EventHandle {}
unsafe impl Sync for EventHandle {}

impl EventHandle {
    fn reporter(&self) -> &CliReporter {
        unsafe { &*self.reporter }
    }
}

impl ProgressHandle for EventHandle {
    fn set_bytes(&self, downloaded: u64, total: u64, speed: f64) {
        let mut update = TaskUpdate::new(self.id.clone());
        update.position = Some(downloaded);
        update.total = Some(total);
        update.bytes_per_sec = Some(speed);
        self.reporter().task_update(update);
    }

    fn set_message(&self, msg: &str) {
        let mut update = TaskUpdate::new(self.id.clone());
        update.message = Some(msg.to_string());
        self.reporter().task_update(update);
    }

    fn set_stage(&self, stage: TaskStage) {
        let mut update = TaskUpdate::new(self.id.clone());
        update.stage = Some(stage);
        self.reporter().task_update(update);
    }

    fn set_count(&self, done: usize, total: usize) {
        let mut update = TaskUpdate::new(self.id.clone());
        update.position = Some(done as u64);
        update.total = Some(total as u64);
        update.files_done = Some(done as u64);
        update.files_total = Some(total as u64);
        self.reporter().task_update(update);
    }

    fn finish(&self) {
        if !self.finished.swap(true, Ordering::Relaxed) {
            self.reporter()
                .task_finish(self.id.clone(), TaskOutcome::Success);
        }
    }

    fn finish_with_error(&self, msg: &str) {
        if !self.finished.swap(true, Ordering::Relaxed) {
            self.reporter().task_finish(
                self.id.clone(),
                TaskOutcome::Failed {
                    error: msg.to_string(),
                },
            );
        }
    }
}

impl Drop for EventHandle {
    fn drop(&mut self) {
        if !self.finished.load(Ordering::Relaxed) {
            self.finish();
        }
    }
}

/// Non-pooled persistent status bar (for Extract/DDS/BSA counters).
struct StatusHandle {
    bar: ProgressBar,
    label: String,
    finished: AtomicBool,
}

/// Line-oriented status counter for `--progress plain`.
struct PlainStatusHandle {
    label: String,
    last_update: Mutex<Option<Instant>>,
    finished: AtomicBool,
}

impl ProgressHandle for PlainStatusHandle {
    fn set_bytes(&self, _downloaded: u64, _total: u64, _speed: f64) {}

    fn set_message(&self, msg: &str) {
        eprintln!("{}: {}", self.label, msg);
    }

    fn set_stage(&self, _stage: TaskStage) {}

    fn set_count(&self, done: usize, total: usize) {
        let should_emit = {
            let mut last = self.last_update.lock().expect("plain status lock");
            match *last {
                Some(t) if t.elapsed() < Duration::from_secs(1) && done < total => false,
                _ => {
                    *last = Some(Instant::now());
                    true
                }
            }
        };
        if should_emit {
            eprintln!("{}: {}/{}", self.label, done, total);
        }
    }

    fn finish(&self) {
        if !self.finished.swap(true, Ordering::Relaxed) {
            eprintln!("{}: finished", self.label);
        }
    }

    fn finish_with_error(&self, msg: &str) {
        if !self.finished.swap(true, Ordering::Relaxed) {
            eprintln!("{}: {}", self.label, msg);
        }
    }
}

impl Drop for PlainStatusHandle {
    fn drop(&mut self) {
        if !self.finished.load(Ordering::Relaxed) {
            eprintln!("{}: finished", self.label);
        }
    }
}

impl ProgressHandle for StatusHandle {
    fn set_bytes(&self, _downloaded: u64, _total: u64, _speed: f64) {}

    fn set_message(&self, msg: &str) {
        self.bar
            .set_message(format!("{}: {}", self.label, truncate_progress_label(msg, 72)));
    }

    fn set_stage(&self, _stage: TaskStage) {}

    fn set_count(&self, done: usize, total: usize) {
        self.bar.set_message(format!(
            "{}: {}/{} {}",
            self.label,
            done,
            total,
            status_unit(&self.label)
        ));
    }

    fn finish(&self) {
        if !self.finished.swap(true, Ordering::Relaxed) {
            self.bar.finish_and_clear();
        }
    }

    fn finish_with_error(&self, msg: &str) {
        if !self.finished.swap(true, Ordering::Relaxed) {
            self.bar.set_message(format!("{}: {}", self.label, msg));
            self.bar.finish_and_clear();
        }
    }
}

impl Drop for StatusHandle {
    fn drop(&mut self) {
        if !self.finished.load(Ordering::Relaxed) {
            self.bar.finish_and_clear();
        }
    }
}

impl Drop for CliReporter {
    fn drop(&mut self) {
        self.snapshot_shutdown.store(true, Ordering::Relaxed);
    }
}

/// `MakeWriter` factory for tracing that routes through a `MultiProgress`.
#[derive(Clone)]
pub struct CliWriterFactory {
    mp: MultiProgress,
    use_multi_progress: bool,
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CliWriterFactory {
    type Writer = CliWriter;

    fn make_writer(&'a self) -> Self::Writer {
        CliWriter {
            mp: if self.use_multi_progress {
                Some(self.mp.clone())
            } else {
                None
            },
            buffer: Vec::new(),
        }
    }
}

/// Buffered writer that flushes complete lines through `MultiProgress::println`.
pub struct CliWriter {
    mp: Option<MultiProgress>,
    buffer: Vec<u8>,
}

impl std::io::Write for CliWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let msg = String::from_utf8_lossy(&self.buffer).trim_end().to_string();
        self.buffer.clear();
        if msg.is_empty() {
            return Ok(());
        }
        if let Some(mp) = &self.mp {
            let _ = mp.println(&msg);
        } else {
            let mut stderr = std::io::stderr().lock();
            writeln!(stderr, "{}", msg)?;
        }
        Ok(())
    }
}
