//! Unified progress reporting trait
//!
//! All engine code reports progress through `ProgressReporter`.
//! Implementations: `CliReporter` (indicatif + console), `GuiReporter` (mpsc → Slint).

use std::fmt;
use std::sync::Arc;

/// CLI progress rendering mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressMode {
    /// Use interactive progress on terminals and plain lines otherwise.
    Auto,
    /// Interactive progress bars and worker slots.
    Full,
    /// Line-oriented human-readable progress.
    Plain,
}

/// Phases of installation, reported to the UI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    /// Update item-level count (e.g. files extracted within an archive).
    fn set_count(&self, done: usize, total: usize);
    /// Mark as finished (success).
    fn finish(&self);
    /// Mark as finished with an error message.
    fn finish_with_error(&self, msg: &str);
}

/// The single trait all engine code uses for progress reporting.
pub trait ProgressReporter: Send + Sync {
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
    fn set_count(&self, _done: usize, _total: usize) {}
    fn finish(&self) {}
    fn finish_with_error(&self, _msg: &str) {}
}

/// No-op reporter for tests and headless operation.
pub struct NullReporter;

impl ProgressReporter for NullReporter {}
