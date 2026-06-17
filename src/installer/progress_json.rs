//! Newline-delimited JSON progress reporter for external drivers.

use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use super::config::{ProgressCallback, ProgressEvent};
use super::progress::{NullHandle, Phase, ProgressHandle, ProgressReporter};

#[derive(Clone)]
pub struct JsonEventWriter {
    stdout: Arc<Mutex<io::Stdout>>,
}

impl JsonEventWriter {
    pub fn stdout() -> Self {
        Self {
            stdout: Arc::new(Mutex::new(io::stdout())),
        }
    }

    pub fn emit(&self, event: ProgressEvent) {
        let mut stdout = self.stdout.lock().expect("stdout lock");
        if serde_json::to_writer(&mut *stdout, &event).is_ok() {
            let _ = stdout.write_all(b"\n");
        }
        let _ = stdout.flush();
    }
}

struct State {
    total: usize,
    count: usize,
    current_phase: Option<Phase>,
}

pub struct JsonReporter {
    writer: JsonEventWriter,
    state: Mutex<State>,
}

impl JsonReporter {
    pub fn new(writer: JsonEventWriter) -> Arc<Self> {
        Arc::new(Self {
            writer,
            state: Mutex::new(State {
                total: 0,
                count: 0,
                current_phase: None,
            }),
        })
    }

    pub fn download_skipped_callback(writer: JsonEventWriter) -> ProgressCallback {
        Arc::new(move |event| match event {
            event @ ProgressEvent::DownloadSkipped { .. } => writer.emit(event),
            _ => {}
        })
    }

    fn emit(&self, event: ProgressEvent) {
        self.writer.emit(event);
    }

    fn write_detail(&self, msg: &str) {
        let mut stderr = io::stderr().lock();
        let _ = writeln!(stderr, "{}", msg);
        let _ = stderr.flush();
    }
}

impl ProgressReporter for JsonReporter {
    fn phase_start(&self, phase: Phase) {
        {
            let mut state = self.state.lock().expect("json reporter state lock");
            state.current_phase = Some(phase);
            state.count = 0;
            state.total = 0;
        }
        self.emit(ProgressEvent::PhaseChange {
            phase: phase.to_string(),
        });
        self.write_detail(&format!("=== {} ===", phase));
    }

    fn overall_set_total(&self, total: u64) {
        let mut state = self.state.lock().expect("json reporter state lock");
        state.total = total as usize;
        state.count = 0;
    }

    fn overall_inc(&self) {
        let (index, total, phase) = {
            let mut state = self.state.lock().expect("json reporter state lock");
            state.count += 1;
            (state.count, state.total, state.current_phase)
        };

        match phase {
            Some(Phase::Downloading) | Some(Phase::Validating) | Some(Phase::Extracting) => {
                self.emit(ProgressEvent::ArchiveComplete { index, total });
            }
            _ => {
                self.emit(ProgressEvent::DirectiveComplete { index, total });
            }
        }
    }

    fn overall_set_message(&self, msg: &str) {
        self.emit(ProgressEvent::Status {
            message: msg.to_string(),
        });
    }

    fn begin_item(&self, name: &str, total_bytes: Option<u64>) -> Arc<dyn ProgressHandle> {
        self.emit(ProgressEvent::Status {
            message: name.to_string(),
        });
        Arc::new(JsonProgressHandle {
            writer: self.writer.clone(),
            name: name.to_string(),
            total_bytes,
            finished: AtomicBool::new(false),
        })
    }

    fn begin_status(&self, _label: &str) -> Arc<dyn ProgressHandle> {
        Arc::new(NullHandle)
    }

    fn log(&self, msg: &str) {
        self.write_detail(msg);
    }

    fn status(&self, msg: &str) {
        self.emit(ProgressEvent::Status {
            message: msg.to_string(),
        });
    }
}

struct JsonProgressHandle {
    writer: JsonEventWriter,
    name: String,
    total_bytes: Option<u64>,
    finished: AtomicBool,
}

impl ProgressHandle for JsonProgressHandle {
    fn set_bytes(&self, downloaded: u64, total: u64, speed: f64) {
        self.writer.emit(ProgressEvent::DownloadProgress {
            name: self.name.clone(),
            downloaded,
            total: if total > 0 {
                total
            } else {
                self.total_bytes.unwrap_or(0)
            },
            speed,
        });
    }

    fn set_message(&self, msg: &str) {
        self.writer.emit(ProgressEvent::Status {
            message: format!("{}: {}", self.name, msg),
        });
    }

    fn set_count(&self, _done: usize, _total: usize) {}

    fn finish(&self) {
        if !self.finished.swap(true, Ordering::Relaxed) {
            if self.total_bytes.is_some() {
                self.writer.emit(ProgressEvent::DownloadComplete {
                    name: self.name.clone(),
                });
            }
        }
    }

    fn finish_with_error(&self, msg: &str) {
        if !self.finished.swap(true, Ordering::Relaxed) {
            let mut stderr = io::stderr().lock();
            let _ = writeln!(stderr, "{}", msg);
            let _ = stderr.flush();
        }
    }
}

impl Drop for JsonProgressHandle {
    fn drop(&mut self) {
        if !self.finished.load(Ordering::Relaxed) {
            self.finish();
        }
    }
}
