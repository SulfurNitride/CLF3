//! Newline-delimited JSON progress reporter for external drivers.

use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use super::config::{ProgressCallback, ProgressEvent};
use super::progress::{Phase, ProgressHandle, ProgressReporter};

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
        self.emit_value(&event);
    }

    pub fn emit_value(&self, event: &impl serde::Serialize) {
        let mut stdout = self.stdout.lock().expect("stdout lock");
        if serde_json::to_writer(&mut *stdout, event).is_ok() {
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
    next_item_id: AtomicU64,
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
            next_item_id: AtomicU64::new(1),
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

    fn start_item(
        &self,
        name: &str,
        total_bytes: Option<u64>,
        stage: &str,
        display_name: &str,
        subtitle: &str,
        image_url: Option<&str>,
    ) -> Arc<dyn ProgressHandle> {
        let item_id = format!("item-{}", self.next_item_id.fetch_add(1, Ordering::Relaxed));
        self.writer.emit_value(&serde_json::json!({
            "type": "item_started",
            "item_id": item_id,
            "name": name,
            "display_name": display_name,
            "subtitle": subtitle,
            "stage": stage,
            "image_url": image_url,
            "total": total_bytes.unwrap_or(0),
            "unit": if total_bytes.is_some() { "bytes" } else { "items" },
        }));
        Arc::new(JsonProgressHandle {
            writer: self.writer.clone(),
            item_id,
            name: name.to_string(),
            total_bytes,
            finished: AtomicBool::new(false),
        })
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
        let (display_name, stage) = if let Some(archive) = name.strip_prefix("extracting ") {
            (archive, "Extracting")
        } else {
            let stage = self
                .state
                .lock()
                .expect("json reporter state lock")
                .current_phase
                .map(|phase| phase.to_string())
                .unwrap_or_else(|| "Working".to_string());
            return self.start_item(name, total_bytes, &stage, name, "", None);
        };
        self.start_item(display_name, total_bytes, stage, display_name, "", None)
    }

    fn begin_item_with_metadata(
        &self,
        name: &str,
        total_bytes: Option<u64>,
        stage: &str,
        display_name: &str,
        subtitle: &str,
        image_url: Option<&str>,
    ) -> Arc<dyn ProgressHandle> {
        self.start_item(name, total_bytes, stage, display_name, subtitle, image_url)
    }

    fn begin_status(&self, label: &str) -> Arc<dyn ProgressHandle> {
        self.start_item(label, None, "Pipeline", label, "", None)
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
    item_id: String,
    name: String,
    total_bytes: Option<u64>,
    finished: AtomicBool,
}

impl ProgressHandle for JsonProgressHandle {
    fn set_bytes(&self, downloaded: u64, total: u64, speed: f64) {
        let total = if total > 0 {
            total
        } else {
            self.total_bytes.unwrap_or(0)
        };
        self.writer.emit(ProgressEvent::DownloadProgress {
            name: self.name.clone(),
            downloaded,
            total,
            speed,
        });
        self.writer.emit_value(&serde_json::json!({
            "type": "item_progress",
            "item_id": self.item_id,
            "completed": downloaded,
            "total": total,
            "speed": speed,
            "unit": "bytes",
        }));
    }

    fn set_message(&self, msg: &str) {
        self.writer.emit_value(&serde_json::json!({
            "type": "item_message",
            "item_id": self.item_id,
            "message": msg,
        }));
    }

    fn set_count(&self, done: usize, total: usize) {
        self.writer.emit_value(&serde_json::json!({
            "type": "item_progress",
            "item_id": self.item_id,
            "completed": done,
            "total": total,
            "speed": 0,
            "unit": "items",
        }));
    }

    fn finish(&self) {
        if !self.finished.swap(true, Ordering::Relaxed) {
            self.writer.emit_value(&serde_json::json!({
                "type": "item_completed",
                "item_id": self.item_id,
            }));
            if self.total_bytes.is_some() {
                self.writer.emit(ProgressEvent::DownloadComplete {
                    name: self.name.clone(),
                });
            }
        }
    }

    fn finish_with_error(&self, msg: &str) {
        if !self.finished.swap(true, Ordering::Relaxed) {
            self.writer.emit_value(&serde_json::json!({
                "type": "item_failed",
                "item_id": self.item_id,
                "message": msg,
            }));
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
