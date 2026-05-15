//! CLI progress reporter using indicatif + console
//!
//! Owns the `MultiProgress` display and provides a `MakeWriter` for tracing integration.

use super::progress::{NullHandle, Phase, ProgressHandle, ProgressMode, ProgressReporter};
use console::style;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// CLI reporter backed by indicatif `MultiProgress`.
pub struct CliReporter {
    mp: MultiProgress,
    mode: ProgressMode,
    overall: Mutex<Option<ProgressBar>>,
    system_status: ProgressBar,
    active_header: ProgressBar,
    /// Fixed pool of reusable bars for concurrent items (downloads, archive extractions).
    pool: Vec<ProgressBar>,
    pool_available: Vec<Mutex<bool>>,
    /// Per-slot current download speed in bytes/sec (0 when idle or extracting).
    bar_speeds: Arc<Vec<AtomicU64>>,
    ticker_shutdown: Arc<AtomicBool>,
}

impl CliReporter {
    /// Create a new CLI reporter with a bar pool of the given size.
    pub fn new(pool_size: usize, mode: ProgressMode) -> Arc<Self> {
        let mp = MultiProgress::new();
        let blank_style = blank_style();
        let idle_style = idle_slot_style();

        let system_status = mp.add(ProgressBar::new_spinner());
        if mode == ProgressMode::Full {
            system_status.set_style(ProgressStyle::default_spinner().template("{msg}").unwrap());
            system_status.set_message("System: starting...");
        } else {
            system_status.set_style(blank_style.clone());
        }

        let active_header = mp.add(ProgressBar::new_spinner());
        if mode == ProgressMode::Full {
            active_header.set_style(ProgressStyle::default_bar().template("{msg}").unwrap());
            active_header.set_message(format!("{}", style("Workers").bold().cyan()));
        } else {
            active_header.set_style(blank_style.clone());
        }

        let mut pool = Vec::with_capacity(pool_size);
        let mut pool_available = Vec::with_capacity(pool_size);
        let mut bar_speeds = Vec::with_capacity(pool_size);
        for index in 0..pool_size {
            let bar = mp.add(ProgressBar::new(0));
            if mode == ProgressMode::Full {
                bar.set_style(idle_style.clone());
                bar.set_prefix(worker_slot_prefix(index));
            } else {
                bar.set_style(blank_style.clone());
            }
            pool.push(bar);
            pool_available.push(Mutex::new(true));
            bar_speeds.push(AtomicU64::new(0));
        }

        let reporter = Arc::new(Self {
            mp,
            mode,
            overall: Mutex::new(None),
            system_status,
            active_header,
            pool,
            pool_available,
            bar_speeds: Arc::new(bar_speeds),
            ticker_shutdown: Arc::new(AtomicBool::new(false)),
        });

        if mode == ProgressMode::Full {
            Self::spawn_system_ticker(&reporter);
        }
        reporter
    }

    /// Get a `MakeWriter` for the tracing subscriber that routes through this reporter's
    /// `MultiProgress`. Call this before initializing the tracing subscriber.
    pub fn make_writer_factory(self: &Arc<Self>) -> CliWriterFactory {
        CliWriterFactory {
            mp: self.mp.clone(),
        }
    }

    fn claim_pool_bar(&self) -> Option<usize> {
        if self.mode != ProgressMode::Full {
            return None;
        }
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
        self.pool[index].set_style(idle_slot_style());
        self.pool[index].set_prefix(worker_slot_prefix(index));
        self.pool[index].set_message("");
        self.pool[index].set_position(0);
        self.pool[index].set_length(0);
        self.pool[index].disable_steady_tick();
        self.bar_speeds[index].store(0, Ordering::Relaxed);
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

    fn spawn_system_ticker(this: &Arc<Self>) {
        let system_status = this.system_status.clone();
        let bar_speeds = Arc::clone(&this.bar_speeds);
        let shutdown = Arc::clone(&this.ticker_shutdown);
        let total_mem_bytes = read_total_memory_bytes();

        std::thread::spawn(move || {
            let mut prev_disk_write = read_disk_write_bytes();
            let mut prev_time = Instant::now();
            let total_mem_label = total_mem_bytes
                .map(|t| format!(" / {}", format_bytes(t)))
                .unwrap_or_default();

            while !shutdown.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(500));

                let now = Instant::now();
                let elapsed = now
                    .saturating_duration_since(prev_time)
                    .as_secs_f64()
                    .max(0.001);
                prev_time = now;

                let net_bps: u64 = bar_speeds.iter().map(|a| a.load(Ordering::Relaxed)).sum();

                let cur_disk_write = read_disk_write_bytes();
                let disk_bps = match (cur_disk_write, prev_disk_write) {
                    (Some(cur), Some(prev)) => ((cur.saturating_sub(prev)) as f64 / elapsed) as u64,
                    _ => 0,
                };
                prev_disk_write = cur_disk_write;

                let rss_bytes = super::current_rss_kb().unwrap_or(0).saturating_mul(1024);
                system_status.set_message(format!(
                    "Net: {:>10}/s   Disk: {:>10}/s   RAM: {}{}",
                    format_bytes(net_bps),
                    format_bytes(disk_bps),
                    format_bytes(rss_bytes),
                    total_mem_label,
                ));
            }
            system_status.finish_and_clear();
        });
    }
}

impl Drop for CliReporter {
    fn drop(&mut self) {
        self.ticker_shutdown.store(true, Ordering::Relaxed);
    }
}

#[cfg(target_os = "linux")]
fn read_disk_write_bytes() -> Option<u64> {
    let s = std::fs::read_to_string("/proc/self/io").ok()?;
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("write_bytes:") {
            return rest.trim().parse().ok();
        }
    }
    None
}

#[cfg(not(target_os = "linux"))]
fn read_disk_write_bytes() -> Option<u64> {
    None
}

#[cfg(target_os = "linux")]
fn read_total_memory_bytes() -> Option<u64> {
    let s = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            let kb = rest.split_whitespace().next()?.parse::<u64>().ok()?;
            return Some(kb.saturating_mul(1024));
        }
    }
    None
}

#[cfg(not(target_os = "linux"))]
fn read_total_memory_bytes() -> Option<u64> {
    None
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut idx = 0;
    while value >= 1024.0 && idx + 1 < UNITS.len() {
        value /= 1024.0;
        idx += 1;
    }
    if idx == 0 {
        format!("{} {}", bytes, UNITS[idx])
    } else {
        format!("{:.1} {}", value, UNITS[idx])
    }
}

fn blank_style() -> ProgressStyle {
    ProgressStyle::default_bar().template("").unwrap()
}

fn idle_slot_style() -> ProgressStyle {
    ProgressStyle::default_bar()
        .template("  [{prefix:.dim}] idle")
        .unwrap()
}

fn worker_slot_prefix(index: usize) -> String {
    format!("worker {:02}", index + 1)
}

fn active_label(name: &str) -> String {
    const MAX_CHARS: usize = 52;
    let mut label: String = name.chars().take(MAX_CHARS).collect();
    if name.chars().count() > MAX_CHARS {
        label.push('~');
    }
    format!("{:<53}", label)
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
        if self.mode == ProgressMode::Plain {
            let _ = self.mp.println(format!("Total: {}", total));
            return;
        }
        let pb = self.ensure_overall();
        pb.set_length(total);
        pb.set_position(0);
    }

    fn overall_inc(&self) {
        if self.mode == ProgressMode::Plain {
            return;
        }
        let pb = self.ensure_overall();
        pb.inc(1);
    }

    fn overall_set_message(&self, msg: &str) {
        if self.mode == ProgressMode::Plain {
            let _ = self.mp.println(msg);
            return;
        }
        let pb = self.ensure_overall();
        pb.set_message(msg.to_string());
    }

    fn overall_finish(&self) {
        if self.mode == ProgressMode::Plain {
            return;
        }
        let mut guard = self.overall.lock().expect("overall lock");
        if let Some(pb) = guard.take() {
            pb.finish_and_clear();
        }
    }

    fn begin_item(&self, name: &str, total_bytes: Option<u64>) -> Arc<dyn ProgressHandle> {
        if self.mode == ProgressMode::Plain {
            let _ = self.mp.println(format!("START {}", name));
            return Arc::new(PlainHandle {
                mp: self.mp.clone(),
                name: name.to_string(),
                finished: AtomicBool::new(false),
            });
        }

        if let Some(index) = self.claim_pool_bar() {
            let bar = &self.pool[index];
            bar.reset();
            bar.set_prefix(worker_slot_prefix(index));

            if let Some(total) = total_bytes {
                // Byte-progress style (downloads)
                bar.set_style(
                    ProgressStyle::default_bar()
                        .template(
                            "  [{prefix:.cyan}] {msg} [{bar:24.white/dim}] {bytes}/{total_bytes} {bytes_per_sec}",
                        )
                        .unwrap()
                        .progress_chars("=>-"),
                );
                bar.set_length(total);
            } else {
                // Spinner style (archive extractions)
                bar.set_style(
                    ProgressStyle::default_spinner()
                        .template("  [{prefix:.cyan}] {msg}")
                        .unwrap(),
                );
            }

            bar.set_message(active_label(name));
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
        if self.mode == ProgressMode::Plain {
            return Arc::new(PlainStatusHandle {
                mp: self.mp.clone(),
                label: label.to_string(),
                finished: AtomicBool::new(false),
            });
        }

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
        if self.mode == ProgressMode::Plain {
            let _ = self.mp.println(msg);
            return;
        }
        let pb = self.ensure_overall();
        pb.set_message(msg.to_string());
    }
}

struct PlainHandle {
    mp: MultiProgress,
    name: String,
    finished: AtomicBool,
}

impl ProgressHandle for PlainHandle {
    fn set_bytes(&self, _downloaded: u64, _total: u64, _speed: f64) {}

    fn set_message(&self, msg: &str) {
        let _ = self.mp.println(format!("{}: {}", self.name, msg));
    }

    fn set_count(&self, _done: usize, _total: usize) {}

    fn finish(&self) {
        if !self.finished.swap(true, Ordering::Relaxed) {
            let _ = self.mp.println(format!("DONE {}", self.name));
        }
    }

    fn finish_with_error(&self, msg: &str) {
        if !self.finished.swap(true, Ordering::Relaxed) {
            let _ = self.mp.println(format!("ERROR {}: {}", self.name, msg));
        }
    }
}

impl Drop for PlainHandle {
    fn drop(&mut self) {
        if !self.finished.load(Ordering::Relaxed) {
            self.finish();
        }
    }
}

struct PlainStatusHandle {
    mp: MultiProgress,
    label: String,
    finished: AtomicBool,
}

impl ProgressHandle for PlainStatusHandle {
    fn set_bytes(&self, _downloaded: u64, _total: u64, _speed: f64) {}

    fn set_message(&self, msg: &str) {
        let _ = self.mp.println(format!("{}: {}", self.label, msg));
    }

    fn set_count(&self, _done: usize, _total: usize) {}

    fn finish(&self) {
        self.finished.store(true, Ordering::Relaxed);
    }

    fn finish_with_error(&self, msg: &str) {
        if !self.finished.swap(true, Ordering::Relaxed) {
            let _ = self.mp.println(format!("ERROR {}: {}", self.label, msg));
        }
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
    fn set_bytes(&self, downloaded: u64, total: u64, speed: f64) {
        let bar = self.bar();
        bar.set_length(total);
        bar.set_position(downloaded);
        self.reporter().bar_speeds[self.bar_index].store(speed.max(0.0) as u64, Ordering::Relaxed);
    }

    fn set_message(&self, msg: &str) {
        self.bar().set_message(active_label(msg));
    }

    fn set_count(&self, done: usize, total: usize) {
        let bar = self.bar();
        bar.set_length(total as u64);
        bar.set_position(done as u64);
    }

    fn finish(&self) {
        if !self.finished.swap(true, Ordering::Relaxed) {
            self.reporter().release_pool_bar(self.bar_index);
        }
    }

    fn finish_with_error(&self, msg: &str) {
        if !self.finished.swap(true, Ordering::Relaxed) {
            let _ = self.reporter().mp.println(format!("{}", style(msg).red()));
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
        self.bar
            .set_message(format!("{}: {}/{}", self.label, done, total));
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
