//! Unified progress reporting trait
//!
//! All engine code reports progress through `ProgressReporter`.
//! Implementations: `CliReporter` (indicatif + console), `GuiReporter` (mpsc → Slint).

use serde::Serialize;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// CLI/tool progress rendering mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressMode {
    Auto,
    Full,
    Plain,
    Json,
    Snapshot,
}

impl ProgressMode {
    pub fn is_machine_readable(self) -> bool {
        matches!(self, Self::Json | Self::Snapshot)
    }

    pub fn is_interactive(self) -> bool {
        matches!(self, Self::Full)
    }
}

/// Phases of installation, reported to the UI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    GameCheck,
    Downloading,
    Validating,
    Extracting,
    Installing,
    DdsTransform,
    BsaBuild,
    Cleanup,
}

/// Type of unit a task reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProgressUnit {
    Bytes,
    Files,
    Archives,
    Textures,
    Directives,
    Items,
}

/// Domain-specific kind of progress task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskKind {
    Download,
    Verify,
    Extract,
    Finalize,
    Dds,
    Bsa,
    Cleanup,
}

/// Current stage of a progress task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStage {
    Queued,
    Resolving,
    Downloading,
    Verifying,
    Extracting,
    Finalizing,
    Transforming,
    Building,
    Waiting,
    Complete,
    Failed,
    Manual,
}

/// Stable task identifier used by human and machine renderers.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct TaskId(pub String);

impl TaskId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Event emitted when a task starts.
#[derive(Debug, Clone, Serialize)]
pub struct TaskStarted {
    pub id: TaskId,
    pub parent: Option<TaskId>,
    pub kind: TaskKind,
    pub stage: TaskStage,
    pub label: String,
    pub unit: ProgressUnit,
    pub total: Option<u64>,
    pub timestamp_ms: u128,
}

impl TaskStarted {
    pub fn new(
        id: TaskId,
        kind: TaskKind,
        stage: TaskStage,
        label: impl Into<String>,
        unit: ProgressUnit,
        total: Option<u64>,
    ) -> Self {
        Self {
            id,
            parent: None,
            kind,
            stage,
            label: label.into(),
            unit,
            total,
            timestamp_ms: now_ms(),
        }
    }
}

/// Event emitted when a task updates.
#[derive(Debug, Clone, Serialize)]
pub struct TaskUpdate {
    pub id: TaskId,
    pub stage: Option<TaskStage>,
    pub position: Option<u64>,
    pub total: Option<u64>,
    pub bytes_per_sec: Option<f64>,
    pub files_done: Option<u64>,
    pub files_total: Option<u64>,
    pub message: Option<String>,
    pub timestamp_ms: u128,
}

impl TaskUpdate {
    pub fn new(id: TaskId) -> Self {
        Self {
            id,
            stage: None,
            position: None,
            total: None,
            bytes_per_sec: None,
            files_done: None,
            files_total: None,
            message: None,
            timestamp_ms: now_ms(),
        }
    }
}

/// Final result of a task.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskOutcome {
    Success,
    Failed { error: String },
    Manual,
    Skipped,
}

/// Serializable active task state for snapshot mode.
#[derive(Debug, Clone, Serialize)]
pub struct TaskSnapshot {
    pub id: TaskId,
    pub parent: Option<TaskId>,
    pub kind: TaskKind,
    pub stage: TaskStage,
    pub label: String,
    pub unit: ProgressUnit,
    pub position: Option<u64>,
    pub total: Option<u64>,
    pub bytes_per_sec: Option<f64>,
    pub files_done: Option<u64>,
    pub files_total: Option<u64>,
    pub message: Option<String>,
    pub started_at_ms: u128,
    pub updated_at_ms: u128,
}

impl From<TaskStarted> for TaskSnapshot {
    fn from(started: TaskStarted) -> Self {
        Self {
            id: started.id,
            parent: started.parent,
            kind: started.kind,
            stage: started.stage,
            label: started.label,
            unit: started.unit,
            position: None,
            total: started.total,
            bytes_per_sec: None,
            files_done: None,
            files_total: None,
            message: None,
            started_at_ms: started.timestamp_ms,
            updated_at_ms: started.timestamp_ms,
        }
    }
}

impl TaskSnapshot {
    pub fn apply_update(&mut self, update: &TaskUpdate) {
        if let Some(stage) = update.stage {
            self.stage = stage;
        }
        if update.position.is_some() {
            self.position = update.position;
        }
        if update.total.is_some() {
            self.total = update.total;
        }
        if update.bytes_per_sec.is_some() {
            self.bytes_per_sec = update.bytes_per_sec;
        }
        if update.files_done.is_some() {
            self.files_done = update.files_done;
        }
        if update.files_total.is_some() {
            self.files_total = update.files_total;
        }
        if update.message.is_some() {
            self.message = update.message.clone();
        }
        self.updated_at_ms = update.timestamp_ms;
    }
}

/// Full active progress state for snapshot consumers.
#[derive(Debug, Clone, Serialize)]
pub struct ProgressSnapshot {
    pub timestamp_ms: u128,
    pub active: Vec<TaskSnapshot>,
}

impl ProgressSnapshot {
    pub fn from_active(active: &HashMap<TaskId, TaskSnapshot>) -> Self {
        let mut active: Vec<_> = active.values().cloned().collect();
        active.sort_by(|a, b| {
            a.started_at_ms
                .cmp(&b.started_at_ms)
                .then(a.id.0.cmp(&b.id.0))
        });
        Self {
            timestamp_ms: now_ms(),
            active,
        }
    }
}

/// Log level carried by structured progress events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProgressLogLevel {
    Info,
    Warn,
    Error,
}

/// Structured progress event for renderers and external tools.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProgressEvent {
    PhaseStarted {
        phase: Phase,
        timestamp_ms: u128,
    },
    TaskStarted(TaskStarted),
    TaskUpdate(TaskUpdate),
    TaskFinished {
        id: TaskId,
        outcome: TaskOutcome,
        timestamp_ms: u128,
    },
    Snapshot(ProgressSnapshot),
    Log {
        level: ProgressLogLevel,
        message: String,
        timestamp_ms: u128,
    },
}

pub fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

impl fmt::Display for Phase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Phase::GameCheck => write!(f, "Game Check"),
            Phase::Downloading => write!(f, "Downloading"),
            Phase::Validating => write!(f, "Validating"),
            Phase::Extracting => write!(f, "Extracting"),
            Phase::Installing => write!(f, "Installing"),
            Phase::DdsTransform => write!(f, "DDS Transform"),
            Phase::BsaBuild => write!(f, "BSA Build"),
            Phase::Cleanup => write!(f, "Cleanup"),
        }
    }
}

/// Handle to a concurrent progress item (download, archive extraction).
/// Dropping the handle marks the item as finished.
pub trait ProgressHandle: Send + Sync {
    /// Update byte-level progress (e.g. download bytes).
    fn set_bytes(&self, downloaded: u64, total: u64, speed: f64);
    /// Update the display message.
    fn set_message(&self, msg: &str);
    /// Update the task stage.
    fn set_stage(&self, _stage: TaskStage) {}
    /// Update item-level count (e.g. files extracted within an archive).
    fn set_count(&self, done: usize, total: usize);
    /// Mark as finished (success).
    fn finish(&self);
    /// Mark as finished with an error message.
    fn finish_with_error(&self, msg: &str);
}

/// The single trait all engine code uses for progress reporting.
pub trait ProgressReporter: Send + Sync {
    /// Emit a structured progress event. New progress code should prefer this
    /// API; the legacy convenience methods below are kept while call sites are
    /// migrated to typed tasks.
    fn emit(&self, _event: ProgressEvent) {}

    /// Return the reporter mode.
    fn mode(&self) -> ProgressMode {
        ProgressMode::Auto
    }

    /// Start a typed task.
    fn task_start(&self, task: TaskStarted) {
        self.emit(ProgressEvent::TaskStarted(task));
    }

    /// Update a typed task.
    fn task_update(&self, update: TaskUpdate) {
        self.emit(ProgressEvent::TaskUpdate(update));
    }

    /// Finish a typed task.
    fn task_finish(&self, id: TaskId, outcome: TaskOutcome) {
        self.emit(ProgressEvent::TaskFinished {
            id,
            outcome,
            timestamp_ms: now_ms(),
        });
    }

    /// A top-level phase is starting.
    fn phase_start(&self, _phase: Phase) {}

    /// Set the total count for the current phase's overall bar.
    fn overall_set_total(&self, _total: u64) {}
    /// Increment the overall bar by 1.
    fn overall_inc(&self) {}
    /// Set the overall bar's message text.
    fn overall_set_message(&self, _msg: &str) {}
    /// Finish/clear the overall bar.
    fn overall_finish(&self) {}

    /// Begin tracking a concurrent item. Returns a handle for updates.
    fn begin_item(&self, _name: &str, _total_bytes: Option<u64>) -> Arc<dyn ProgressHandle> {
        Arc::new(NullHandle)
    }

    /// Create a persistent status counter (not pooled — lives until dropped).
    /// Used for dedicated phase counters (e.g. "Extracted: 45/120 archives").
    fn begin_status(&self, _label: &str) -> Arc<dyn ProgressHandle> {
        Arc::new(NullHandle)
    }

    /// Print a persistent log line (survives progress bar redraws).
    fn log(&self, _msg: &str) {}
    /// Transient status line.
    fn status(&self, _msg: &str) {}
}

/// No-op handle for NullReporter and overflow items.
pub struct NullHandle;

impl ProgressHandle for NullHandle {
    fn set_bytes(&self, _downloaded: u64, _total: u64, _speed: f64) {}
    fn set_message(&self, _msg: &str) {}
    fn set_stage(&self, _stage: TaskStage) {}
    fn set_count(&self, _done: usize, _total: usize) {}
    fn finish(&self) {}
    fn finish_with_error(&self, _msg: &str) {}
}

/// No-op reporter for tests and headless operation.
pub struct NullReporter;

impl ProgressReporter for NullReporter {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_snapshot_applies_partial_updates() {
        let id = TaskId::new("download:test");
        let started = TaskStarted::new(
            id.clone(),
            TaskKind::Download,
            TaskStage::Downloading,
            "test archive",
            ProgressUnit::Bytes,
            Some(100),
        );
        let mut snapshot = TaskSnapshot::from(started);

        let mut update = TaskUpdate::new(id);
        update.position = Some(40);
        update.bytes_per_sec = Some(12.5);
        update.message = Some("half-ish".to_string());
        snapshot.apply_update(&update);

        assert_eq!(snapshot.stage, TaskStage::Downloading);
        assert_eq!(snapshot.position, Some(40));
        assert_eq!(snapshot.total, Some(100));
        assert_eq!(snapshot.bytes_per_sec, Some(12.5));
        assert_eq!(snapshot.message.as_deref(), Some("half-ish"));
    }

    #[test]
    fn progress_snapshot_sorts_active_tasks_by_start_time_then_id() {
        let first = TaskStarted::new(
            TaskId::new("extract:b"),
            TaskKind::Extract,
            TaskStage::Extracting,
            "b",
            ProgressUnit::Items,
            None,
        );
        let mut second = TaskStarted::new(
            TaskId::new("extract:a"),
            TaskKind::Extract,
            TaskStage::Extracting,
            "a",
            ProgressUnit::Items,
            None,
        );
        second.timestamp_ms = first.timestamp_ms;

        let mut active = HashMap::new();
        active.insert(first.id.clone(), TaskSnapshot::from(first));
        active.insert(second.id.clone(), TaskSnapshot::from(second));

        let snapshot = ProgressSnapshot::from_active(&active);
        let ids: Vec<_> = snapshot.active.iter().map(|task| task.id.0.as_str()).collect();

        assert_eq!(ids, vec!["extract:a", "extract:b"]);
    }

    #[test]
    fn progress_event_serializes_with_stable_type_names() {
        let event = ProgressEvent::TaskStarted(TaskStarted::new(
            TaskId::new("download:1"),
            TaskKind::Download,
            TaskStage::Downloading,
            "archive.7z",
            ProgressUnit::Bytes,
            Some(1024),
        ));

        let json = serde_json::to_value(event).expect("serialize progress event");

        assert_eq!(json["type"], "task_started");
        assert_eq!(json["id"], "download:1");
        assert_eq!(json["kind"], "download");
        assert_eq!(json["stage"], "downloading");
        assert_eq!(json["unit"], "bytes");
        assert_eq!(json["total"], 1024);
    }
}
