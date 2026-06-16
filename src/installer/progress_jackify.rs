//! JackifyReporter: ProgressReporter implementation for --jackify mode.
//!
//! Emits structured JSON ProgressEvents to stdout for Jackify's parser,
//! plus plain-text lines for Show Details visibility.

use std::io::Write;
use std::sync::{Arc, Mutex};

use super::config::{ProgressCallback, ProgressEvent};
use super::progress::{NullHandle, Phase, ProgressHandle, ProgressReporter};

struct State {
    total: usize,
    count: usize,
    current_phase: Option<Phase>,
}

pub struct JackifyReporter {
    callback: ProgressCallback,
    state: Mutex<State>,
}

impl JackifyReporter {
    pub fn new(callback: ProgressCallback) -> Arc<Self> {
        Arc::new(Self {
            callback,
            state: Mutex::new(State {
                total: 0,
                count: 0,
                current_phase: None,
            }),
        })
    }

    fn emit(&self, event: ProgressEvent) {
        (self.callback)(event);
        let _ = std::io::stdout().flush();
    }

    fn print_text(&self, msg: &str) {
        println!("{}", msg);
        let _ = std::io::stdout().flush();
    }

    fn print_progress(&self, msg: &str) {
        println!("{}", msg);
        let _ = std::io::stdout().flush();
    }
}

impl ProgressReporter for JackifyReporter {
    fn phase_start(&self, phase: Phase) {
        {
            let mut s = self.state.lock().unwrap();
            s.current_phase = Some(phase);
            s.count = 0;
            s.total = 0;
        }
        self.emit(ProgressEvent::PhaseChange {
            phase: phase.to_string(),
        });
        self.print_text(&format!("=== {} ===", phase));
    }

    fn overall_set_total(&self, total: u64) {
        let mut s = self.state.lock().unwrap();
        s.total = total as usize;
        s.count = 0;
    }

    fn overall_inc(&self) {
        let (count, total, phase) = {
            let mut s = self.state.lock().unwrap();
            s.count += 1;
            (s.count, s.total, s.current_phase)
        };
        // Print a text line every 10 items so Show Details stays readable
        if total > 0 && (count == 1 || count % 10 == 0 || count == total) {
            let label = match phase {
                Some(Phase::Extracting) => "Extracting",
                Some(Phase::BsaBuild) => "Building BSA",
                Some(Phase::DdsTransform) => "DDS Transform",
                Some(Phase::Downloading) => "Downloading",
                _ => "Processing",
            };
            self.print_progress(&format!("{}: {}/{}", label, count, total));
        }
        match phase {
            Some(Phase::Extracting) | Some(Phase::BsaBuild) | Some(Phase::DdsTransform) => {
                self.emit(ProgressEvent::ArchiveComplete { index: count, total });
            }
            _ => {
                self.emit(ProgressEvent::DirectiveComplete { index: count, total });
            }
        }
    }

    fn overall_set_message(&self, msg: &str) {
        self.emit(ProgressEvent::Status {
            message: msg.to_string(),
        });
    }

    fn overall_finish(&self) {}

    fn begin_item(&self, name: &str, total_bytes: Option<u64>) -> Arc<dyn ProgressHandle> {
        if total_bytes.is_some() {
            // Downloads are tracked via config.progress_callback in downloader.rs;
            // emit a text line so the item is visible in Show Details.
            self.print_text(&format!("  Downloading: {}", name));
        }
        self.emit(ProgressEvent::Status {
            message: name.to_string(),
        });
        Arc::new(NullHandle)
    }

    fn begin_status(&self, _label: &str) -> Arc<dyn ProgressHandle> {
        Arc::new(NullHandle)
    }

    fn log(&self, msg: &str) {
        // Route log messages to Show Details (non-JSON stdout).
        self.print_text(msg);
    }

    fn status(&self, msg: &str) {
        self.emit(ProgressEvent::Status {
            message: msg.to_string(),
        });
    }
}
