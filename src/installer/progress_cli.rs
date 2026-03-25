//! CLI progress reporter using indicatif + console
//!
//! Owns the `MultiProgress` display and provides a `MakeWriter` for tracing integration.

use super::progress::{NullHandle, Phase, ProgressHandle, ProgressReporter};
use console::style;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// CLI reporter backed by indicatif `MultiProgress`.
pub struct CliReporter {
    mp: MultiProgress,
    overall: Mutex<Option<ProgressBar>>,
    /// Fixed pool of reusable bars for concurrent items (downloads, archive extractions).
    pool: Vec<ProgressBar>,
    pool_available: Vec<Mutex<bool>>,
}

impl CliReporter {
    /// Create a new CLI reporter with a bar pool of the given size.
    pub fn new(pool_size: usize) -> Arc<Self> {
        let mp = MultiProgress::new();
        let idle_style = ProgressStyle::default_bar().template("").unwrap();

        let mut pool = Vec::with_capacity(pool_size);
        let mut pool_available = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            let bar = mp.add(ProgressBar::new(0));
            bar.set_style(idle_style.clone());
            pool.push(bar);
            pool_available.push(Mutex::new(true));
        }

        Arc::new(Self {
            mp,
            overall: Mutex::new(None),
            pool,
            pool_available,
        })
    }

    /// Get a `MakeWriter` for the tracing subscriber that routes through this reporter's
    /// `MultiProgress`. Call this before initializing the tracing subscriber.
    pub fn make_writer_factory(self: &Arc<Self>) -> CliWriterFactory {
        CliWriterFactory {
            mp: self.mp.clone(),
        }
    }

    fn claim_pool_bar(&self) -> Option<usize> {
        for (i, available) in self.pool_available.iter().enumerate() {
            let mut guard = available.lock().expect("pool lock");
            if *guard {
                *guard = false;
                return Some(i);
            }
        }
        None
    }

    fn release_pool_bar(&self, index: usize) {
        let idle_style = ProgressStyle::default_bar().template("").unwrap();
        self.pool[index].set_style(idle_style);
        self.pool[index].set_message("");
        self.pool[index].set_position(0);
        self.pool[index].set_length(0);
        self.pool[index].disable_steady_tick();
        *self.pool_available[index].lock().expect("pool lock") = true;
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
}

impl ProgressReporter for CliReporter {
    fn phase_start(&self, phase: Phase) {
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
        let pb = self.ensure_overall();
        pb.set_length(total);
        pb.set_position(0);
    }

    fn overall_inc(&self) {
        let pb = self.ensure_overall();
        pb.inc(1);
    }

    fn overall_set_message(&self, msg: &str) {
        let pb = self.ensure_overall();
        pb.set_message(msg.to_string());
    }

    fn overall_finish(&self) {
        let mut guard = self.overall.lock().expect("overall lock");
        if let Some(pb) = guard.take() {
            pb.finish_and_clear();
        }
    }

    fn begin_item(&self, name: &str, total_bytes: Option<u64>) -> Arc<dyn ProgressHandle> {
        if let Some(index) = self.claim_pool_bar() {
            let bar = &self.pool[index];
            bar.reset();

            if let Some(total) = total_bytes {
                // Byte-progress style (downloads)
                bar.set_style(
                    ProgressStyle::default_bar()
                        .template(
                            "  {spinner:.blue} {wide_msg} [{bar:30.white/dim}] {bytes}/{total_bytes} {bytes_per_sec}",
                        )
                        .unwrap()
                        .progress_chars("=>-"),
                );
                bar.set_length(total);
            } else {
                // Spinner style (archive extractions)
                bar.set_style(
                    ProgressStyle::default_spinner()
                        .template("  {spinner:.blue} {wide_msg}")
                        .unwrap(),
                );
            }

            bar.set_message(name.to_string());
            bar.enable_steady_tick(Duration::from_millis(500));

            Arc::new(CliHandle {
                reporter: self as *const CliReporter,
                bar_index: index,
                finished: AtomicBool::new(false),
            })
        } else {
            // Pool exhausted — return a null handle
            Arc::new(NullHandle)
        }
    }

    fn begin_status(&self, label: &str) -> Arc<dyn ProgressHandle> {
        let bar = self.mp.add(ProgressBar::new(0));
        bar.set_style(
            ProgressStyle::default_bar()
                .template("  {spinner:.magenta} {wide_msg}")
                .unwrap(),
        );
        bar.set_message(label.to_string());
        bar.enable_steady_tick(Duration::from_millis(200));
        Arc::new(StatusHandle {
            bar,
            label: label.to_string(),
            finished: AtomicBool::new(false),
        })
    }

    fn log(&self, msg: &str) {
        let _ = self.mp.println(msg);
    }

    fn status(&self, msg: &str) {
        let pb = self.ensure_overall();
        pb.set_message(msg.to_string());
    }
}

/// Handle to a pooled progress bar. Releases back to pool on finish/drop.
struct CliHandle {
    // Raw pointer because CliReporter is always behind Arc and outlives handles.
    // We can't store Arc<CliReporter> because begin_item takes &self.
    reporter: *const CliReporter,
    bar_index: usize,
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
        self.bar().set_message(msg.to_string());
    }

    fn set_count(&self, done: usize, total: usize) {
        let bar = self.bar();
        bar.set_length(total as u64);
        bar.set_position(done as u64);
    }

    fn finish(&self) {
        if !self.finished.swap(true, Ordering::Relaxed) {
            self.bar().set_style(ProgressStyle::default_bar().template("").unwrap());
            self.bar().finish_and_clear();
            self.reporter().release_pool_bar(self.bar_index);
        }
    }

    fn finish_with_error(&self, msg: &str) {
        if !self.finished.swap(true, Ordering::Relaxed) {
            let _ = self.reporter().mp.println(format!("{}", style(msg).red()));
            self.bar().set_style(ProgressStyle::default_bar().template("").unwrap());
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

/// Non-pooled persistent status bar (for Extract/DDS/BSA counters).
struct StatusHandle {
    bar: ProgressBar,
    label: String,
    finished: AtomicBool,
}

impl ProgressHandle for StatusHandle {
    fn set_bytes(&self, _downloaded: u64, _total: u64, _speed: f64) {}

    fn set_message(&self, msg: &str) {
        self.bar.set_message(msg.to_string());
    }

    fn set_count(&self, done: usize, total: usize) {
        self.bar.set_message(format!("{}: {}/{}", self.label, done, total));
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

/// `MakeWriter` factory for tracing that routes through a `MultiProgress`.
#[derive(Clone)]
pub struct CliWriterFactory {
    mp: MultiProgress,
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CliWriterFactory {
    type Writer = CliWriter;

    fn make_writer(&'a self) -> Self::Writer {
        CliWriter {
            mp: self.mp.clone(),
            buffer: Vec::new(),
        }
    }
}

/// Buffered writer that flushes complete lines through `MultiProgress::println`.
pub struct CliWriter {
    mp: MultiProgress,
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
        let _ = self.mp.println(&msg);
        Ok(())
    }
}
