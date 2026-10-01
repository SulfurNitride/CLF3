//! CLI progress reporter using indicatif + console
//!
//! Two renderers sit behind [`CliReporter`]:
//!
//! - **Full** (interactive terminal): a single dashboard block (phase, overall
//!   bar, phase counters, system stats) followed by one row per active item.
//!   Item rows are created when work starts and removed when it ends; items
//!   beyond what fits on screen are tracked off-screen and promoted as rows
//!   free up, so nothing is silently dropped.
//! - **Plain** (pipes, CI, log capture): line-oriented output written straight
//!   to stderr, with throttled heartbeat lines instead of per-update spam.
//!
//! Both renderers share a [`LineSink`] that tracing output is routed through,
//! so log lines never tear the live display.

use super::progress::{Phase, ProgressHandle, ProgressMode, ProgressReporter};
use console::style;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use std::io::{IsTerminal, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Once, Weak};
use std::time::{Duration, Instant};

/// Redraw cadence for spinners / elapsed clocks.
const TICK: Duration = Duration::from_millis(200);
/// System stats (net/disk/RAM) refresh cadence, in ticks.
const SYSTEM_EVERY_TICKS: u32 = 5;
/// Minimum spacing between Plain-mode heartbeat lines triggered by progress.
const PLAIN_HEARTBEAT: Duration = Duration::from_secs(5);
/// Plain mode prints a keep-alive line at least this often while work is active,
/// so CI runners with "no output" timeouts don't kill long downloads.
const PLAIN_KEEPALIVE: Duration = Duration::from_secs(30);
/// Rows reserved for the dashboard, prompt and a little scrollback.
const RESERVED_ROWS: usize = 8;

// ---------------------------------------------------------------------------
// Line output
// ---------------------------------------------------------------------------

/// Writes whole lines without tearing the live display.
///
/// `MultiProgress::println` is a no-op when the draw target is hidden (e.g.
/// stderr is not a terminal), so hidden/absent displays write to stderr
/// directly instead.
struct LineSink {
    mp: Option<MultiProgress>,
}

impl LineSink {
    fn line(&self, msg: &str) {
        match &self.mp {
            Some(mp) if !mp.is_hidden() => {
                let _ = mp.println(msg);
            }
            Some(mp) => mp.suspend(|| write_stderr(msg)),
            None => write_stderr(msg),
        }
    }
}

fn write_stderr(msg: &str) {
    let mut err = std::io::stderr().lock();
    let _ = writeln!(err, "{}", msg);
    let _ = err.flush();
}

// ---------------------------------------------------------------------------
// Reporter
// ---------------------------------------------------------------------------

/// CLI reporter: interactive dashboard on terminals, plain lines otherwise.
pub struct CliReporter {
    sink: Arc<LineSink>,
    renderer: Renderer,
}

enum Renderer {
    Full(Arc<Live>),
    Plain(Arc<Plain>),
}

impl CliReporter {
    /// Create a CLI reporter. `max_rows` caps the number of item rows shown at
    /// once (it is further capped by terminal height). `ProgressMode::Auto`
    /// picks Full when both stdout and stderr are terminals.
    pub fn new(max_rows: usize, mode: ProgressMode) -> Arc<Self> {
        let mode = match mode {
            ProgressMode::Auto
                if std::io::stdout().is_terminal() && std::io::stderr().is_terminal() =>
            {
                ProgressMode::Full
            }
            ProgressMode::Auto => ProgressMode::Plain,
            mode => mode,
        };

        let (sink, renderer) = if mode == ProgressMode::Full {
            let mp = MultiProgress::new();
            let sink = Arc::new(LineSink {
                mp: Some(mp.clone()),
            });
            let live = Live::new(mp, max_rows.max(1));
            (sink, Renderer::Full(live))
        } else {
            let sink = Arc::new(LineSink { mp: None });
            (sink.clone(), Renderer::Plain(Plain::new(sink)))
        };
        Arc::new(Self { sink, renderer })
    }

    /// `MakeWriter` for the tracing subscriber that routes through this
    /// reporter so log lines print above the live display.
    pub fn make_writer_factory(self: &Arc<Self>) -> CliWriterFactory {
        CliWriterFactory {
            sink: Arc::clone(&self.sink),
        }
    }
}

impl ProgressReporter for CliReporter {
    fn phase_start(&self, phase: Phase) {
        match &self.renderer {
            Renderer::Full(live) => live.phase_start(phase),
            Renderer::Plain(plain) => plain.phase_start(phase),
        }
    }

    fn overall_set_total(&self, total: u64) {
        match &self.renderer {
            Renderer::Full(live) => live.overall_set_total(total),
            Renderer::Plain(plain) => plain.overall_set_total(total),
        }
    }

    fn overall_inc(&self) {
        match &self.renderer {
            Renderer::Full(live) => live.dash.inc(1),
            Renderer::Plain(plain) => plain.overall_inc(),
        }
    }

    fn overall_set_message(&self, msg: &str) {
        self.status(msg);
    }

    fn overall_finish(&self) {
        match &self.renderer {
            Renderer::Full(live) => live.overall_finish(),
            Renderer::Plain(plain) => plain.overall_finish(),
        }
    }

    fn begin_item(&self, name: &str, total_bytes: Option<u64>) -> Arc<dyn ProgressHandle> {
        // The pipeline names extraction items "extracting <archive>"; the row
        // glyph already says that, so show just the archive name.
        let label = name.strip_prefix("extracting ").unwrap_or(name);
        match &self.renderer {
            Renderer::Full(live) => live.begin_item(label, total_bytes),
            Renderer::Plain(plain) => plain.begin_item(label, total_bytes),
        }
    }

    fn begin_status(&self, label: &str) -> Arc<dyn ProgressHandle> {
        let counters = match &self.renderer {
            Renderer::Full(live) => &live.counters,
            Renderer::Plain(plain) => &plain.counters,
        };
        Arc::new(CounterHandle {
            counters: Arc::clone(counters),
            id: counters.add(label),
            sink: Arc::clone(&self.sink),
            label: label.to_string(),
            finished: AtomicBool::new(false),
        })
    }

    fn log(&self, msg: &str) {
        self.sink.line(msg);
    }

    fn status(&self, msg: &str) {
        match &self.renderer {
            Renderer::Full(live) => live.set_status(msg),
            Renderer::Plain(plain) => plain.set_status(msg),
        }
    }
}

// ---------------------------------------------------------------------------
// Shared phase counters ("Extracted 40/300 · DDS 100/2000")
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Counters {
    inner: Mutex<CountersInner>,
}

#[derive(Default)]
struct CountersInner {
    next_id: u64,
    entries: Vec<(u64, String, usize, usize)>,
}

impl Counters {
    fn add(&self, label: &str) -> u64 {
        let mut inner = self.inner.lock().expect("counters lock");
        inner.next_id += 1;
        let id = inner.next_id;
        inner.entries.push((id, label.to_string(), 0, 0));
        id
    }

    fn set(&self, id: u64, done: usize, total: usize) {
        let mut inner = self.inner.lock().expect("counters lock");
        if let Some(entry) = inner.entries.iter_mut().find(|e| e.0 == id) {
            entry.2 = done;
            entry.3 = total;
        }
    }

    fn remove(&self, id: u64) {
        self.inner
            .lock()
            .expect("counters lock")
            .entries
            .retain(|e| e.0 != id);
    }

    /// `None` when there are no live counters.
    fn render(&self) -> Option<String> {
        let inner = self.inner.lock().expect("counters lock");
        if inner.entries.is_empty() {
            return None;
        }
        Some(
            inner
                .entries
                .iter()
                .map(|(_, label, done, total)| format!("{} {}/{}", label, done, total))
                .collect::<Vec<_>>()
                .join(" · "),
        )
    }
}

struct CounterHandle {
    counters: Arc<Counters>,
    id: u64,
    sink: Arc<LineSink>,
    label: String,
    finished: AtomicBool,
}

impl ProgressHandle for CounterHandle {
    fn set_bytes(&self, _downloaded: u64, _total: u64, _speed: f64) {}

    fn set_message(&self, msg: &str) {
        self.sink.line(&format!("{}: {}", self.label, msg));
    }

    fn set_count(&self, done: usize, total: usize) {
        self.counters.set(self.id, done, total);
    }

    fn finish(&self) {
        if !self.finished.swap(true, Ordering::Relaxed) {
            self.counters.remove(self.id);
        }
    }

    fn finish_with_error(&self, msg: &str) {
        if !self.finished.swap(true, Ordering::Relaxed) {
            self.counters.remove(self.id);
            self.sink
                .line(&format!("{} {}: {}", style("✗").red(), self.label, msg));
        }
    }
}

impl Drop for CounterHandle {
    fn drop(&mut self) {
        self.finish();
    }
}

// ---------------------------------------------------------------------------
// Full (interactive) renderer
// ---------------------------------------------------------------------------

struct Live {
    mp: MultiProgress,
    out: LineSink,
    max_rows: usize,
    /// Dashboard block. Lives for the whole run; inserted into / removed from
    /// `mp` as it becomes relevant, so overall updates are always lock-free.
    dash: ProgressBar,
    counters: Arc<Counters>,
    state: Mutex<LiveState>,
    /// Latest "Net · Disk · RAM" text, refreshed by the ticker.
    system_line: Mutex<String>,
    run_start: Instant,
}

struct LiveState {
    phase: Option<Phase>,
    phase_active: bool,
    has_total: bool,
    has_status: bool,
    dash_shown: bool,
    dash_has_extra: bool,
    /// Active items, in start order.
    items: Vec<Arc<LiveItem>>,
}

struct LiveItem {
    bar: ProgressBar,
    shown: AtomicBool,
    is_bytes: bool,
    /// Last reported speed (bytes/sec); summed into the Net figure.
    speed: AtomicU64,
    first_bytes: AtomicBool,
    counting: AtomicBool,
}

impl Live {
    fn new(mp: MultiProgress, max_rows: usize) -> Arc<Self> {
        let dash = ProgressBar::with_draw_target(Some(0), ProgressDrawTarget::hidden());
        let live = Arc::new(Self {
            out: LineSink {
                mp: Some(mp.clone()),
            },
            mp,
            max_rows,
            dash,
            counters: Arc::new(Counters::default()),
            state: Mutex::new(LiveState {
                phase: None,
                phase_active: false,
                has_total: false,
                has_status: false,
                dash_shown: false,
                dash_has_extra: false,
                items: Vec::new(),
            }),
            system_line: Mutex::new(String::new()),
            run_start: Instant::now(),
        });
        live.restyle_dash(&live.state.lock().expect("live state"));
        Self::spawn_ticker(&live);
        live
    }

    /// Phases with network/disk activity get the dashboard + system line.
    fn phase_is_active(phase: Phase) -> bool {
        matches!(
            phase,
            Phase::Downloading
                | Phase::Validating
                | Phase::Extracting
                | Phase::Installing
                | Phase::DdsTransform
                | Phase::BsaBuild
        )
    }

    fn visible_rows(&self) -> usize {
        let (rows, _cols) = console::Term::stderr().size();
        (rows as usize)
            .saturating_sub(RESERVED_ROWS)
            .clamp(1, self.max_rows)
    }

    fn label_width(&self) -> usize {
        let (_rows, cols) = console::Term::stderr().size();
        (cols as usize).saturating_sub(58).clamp(16, 60)
    }

    fn phase_start(&self, phase: Phase) {
        let elapsed = format_duration(self.run_start.elapsed());
        self.out.line(&format!(
            "{} {} {}",
            style("==>").bold().cyan(),
            style(phase).bold(),
            style(format!("[{}]", elapsed)).dim()
        ));

        let mut st = self.state.lock().expect("live state");
        st.phase = Some(phase);
        st.phase_active = Self::phase_is_active(phase);
        st.has_total = false;
        st.has_status = false;
        self.dash.reset();
        self.dash.set_length(0);
        self.dash.set_prefix("");
        self.restyle_dash(&st);
        self.sync_dash_visibility(&mut st);
    }

    fn overall_set_total(&self, total: u64) {
        let mut st = self.state.lock().expect("live state");
        st.has_total = true;
        self.dash.set_length(total);
        self.dash.set_position(0);
        self.dash.reset_eta();
        self.restyle_dash(&st);
        self.sync_dash_visibility(&mut st);
    }

    fn overall_finish(&self) {
        let mut st = self.state.lock().expect("live state");
        st.has_total = false;
        st.has_status = false;
        self.dash.set_prefix("");
        self.restyle_dash(&st);
        self.sync_dash_visibility(&mut st);
    }

    fn set_status(&self, msg: &str) {
        self.dash.set_prefix(msg.to_string());
        let mut st = self.state.lock().expect("live state");
        if !st.has_status {
            st.has_status = true;
            self.sync_dash_visibility(&mut st);
        }
    }

    fn sync_dash_visibility(&self, st: &mut LiveState) {
        let want = st.phase_active || st.has_total || st.has_status;
        if want && !st.dash_shown {
            self.mp.insert(0, self.dash.clone());
            st.dash_shown = true;
        } else if !want && st.dash_shown {
            self.mp.remove(&self.dash);
            st.dash_shown = false;
        }
    }

    fn restyle_dash(&self, st: &LiveState) {
        let phase = st
            .phase
            .map(|p| p.to_string())
            .unwrap_or_else(|| "Working".to_string());
        let phase = format!("{}", style(format!("{:<13}", phase)).bold().cyan());
        let extra = if st.dash_has_extra { "\n{msg}" } else { "" };
        let template = if st.has_total {
            format!(
                " {{spinner:.cyan}} {phase} {{bar:30.cyan/blue}} {{pos:>5}}/{{len:<5}} \
                 {{percent:>3}}%  {{elapsed_precise}}  eta {{eta:<4}}  {{prefix:.dim}}{extra}"
            )
        } else {
            format!(" {{spinner:.cyan}} {phase} {{elapsed_precise}}  {{prefix:.dim}}{extra}")
        };
        self.dash.set_style(
            ProgressStyle::with_template(&template)
                .unwrap_or_else(|_| ProgressStyle::default_spinner())
                .progress_chars("━╸─"),
        );
    }

    fn begin_item(self: &Arc<Self>, label: &str, total_bytes: Option<u64>) -> Arc<dyn ProgressHandle> {
        let bar = ProgressBar::with_draw_target(total_bytes, ProgressDrawTarget::hidden());
        let width = self.label_width();
        if let Some(total) = total_bytes {
            bar.set_style(
                ProgressStyle::with_template(&format!(
                    "   {{prefix:.green}} {{msg:<{width}}} {{bar:20.green/blue}} \
                     {{bytes:>10}} / {{total_bytes:<10}} {{bytes_per_sec:>12}}"
                ))
                .expect("item template")
                .progress_chars("━╸─"),
            );
            bar.set_prefix("↓");
            bar.set_length(total);
        } else {
            bar.set_style(work_style(width, false));
        }
        bar.set_message(truncate_label(label, width));

        let item = Arc::new(LiveItem {
            bar,
            shown: AtomicBool::new(false),
            is_bytes: total_bytes.is_some(),
            speed: AtomicU64::new(0),
            first_bytes: AtomicBool::new(true),
            counting: AtomicBool::new(false),
        });

        {
            let mut st = self.state.lock().expect("live state");
            let shown = st.items.iter().filter(|i| i.shown.load(Ordering::Relaxed)).count();
            if shown < self.visible_rows() {
                self.show_item(&item);
            }
            st.items.push(Arc::clone(&item));
        }

        Arc::new(LiveHandle {
            live: Arc::clone(self),
            item,
            label: label.to_string(),
            finished: AtomicBool::new(false),
        })
    }

    fn show_item(&self, item: &LiveItem) {
        self.mp.add(item.bar.clone());
        item.shown.store(true, Ordering::Relaxed);
    }

    fn end_item(&self, item: &Arc<LiveItem>) {
        let mut st = self.state.lock().expect("live state");
        st.items.retain(|i| !Arc::ptr_eq(i, item));
        if item.shown.swap(false, Ordering::Relaxed) {
            item.bar.finish_and_clear();
            self.mp.remove(&item.bar);
        }
        // Promote off-screen items into any free rows.
        let limit = self.visible_rows();
        let mut shown = st.items.iter().filter(|i| i.shown.load(Ordering::Relaxed)).count();
        for candidate in &st.items {
            if shown >= limit {
                break;
            }
            if !candidate.shown.load(Ordering::Relaxed) {
                self.show_item(candidate);
                shown += 1;
            }
        }
    }

    fn spawn_ticker(this: &Arc<Self>) {
        let weak: Weak<Self> = Arc::downgrade(this);
        let total_mem_label = read_total_memory_bytes()
            .map(|t| format!(" / {}", format_bytes(t)))
            .unwrap_or_default();

        std::thread::Builder::new()
            .name("clf3-progress".into())
            .spawn(move || {
                let mut prev_disk = read_disk_write_bytes();
                let mut prev_time = Instant::now();
                let mut tick: u32 = 0;
                loop {
                    std::thread::sleep(TICK);
                    let Some(live) = weak.upgrade() else { break };
                    tick = tick.wrapping_add(1);

                    if tick % SYSTEM_EVERY_TICKS == 0 {
                        let now = Instant::now();
                        let secs = now.duration_since(prev_time).as_secs_f64().max(0.001);
                        prev_time = now;
                        let cur_disk = read_disk_write_bytes();
                        let disk_bps = match (cur_disk, prev_disk) {
                            (Some(c), Some(p)) => (c.saturating_sub(p) as f64 / secs) as u64,
                            _ => 0,
                        };
                        prev_disk = cur_disk;
                        let rss = super::current_rss_kb().unwrap_or(0).saturating_mul(1024);
                        *live.system_line.lock().expect("system line") = format!(
                            "Disk {}/s · RAM {}{}",
                            format_bytes(disk_bps),
                            format_bytes(rss),
                            total_mem_label
                        );
                    }

                    live.refresh(tick);
                }
            })
            .expect("spawn progress ticker");
    }

    /// Recompose the dashboard's extra lines and tick spinners.
    fn refresh(&self, tick: u32) {
        let mut st = self.state.lock().expect("live state");
        if !st.dash_shown && st.items.is_empty() {
            return;
        }

        let mut lines = Vec::new();
        if let Some(counters) = self.counters.render() {
            lines.push(format!("   {}", counters));
        }
        if st.phase_active {
            let mut downloading = false;
            let mut net: u64 = 0;
            for item in st.items.iter().filter(|i| i.is_bytes) {
                downloading = true;
                net += item.speed.load(Ordering::Relaxed);
            }
            let active = st.items.len();
            let shown = st.items.iter().filter(|i| i.shown.load(Ordering::Relaxed)).count();
            let mut parts = Vec::new();
            if downloading {
                parts.push(format!("Net {}/s", format_bytes(net)));
            }
            let system = self.system_line.lock().expect("system line").clone();
            if !system.is_empty() {
                parts.push(system);
            }
            let mut line = format!("   {}", parts.join(" · "));
            if active > 0 {
                if !parts.is_empty() {
                    line.push_str(" ·");
                }
                line.push_str(&format!(" {} active", active));
                if shown < active {
                    line.push_str(&format!(" ({} more off-screen)", active - shown));
                }
            }
            lines.push(format!("{}", style(line).dim()));
        }

        let has_extra = !lines.is_empty();
        if has_extra != st.dash_has_extra {
            st.dash_has_extra = has_extra;
            self.restyle_dash(&st);
        }
        if has_extra {
            self.dash.set_message(lines.join("\n"));
        }
        if st.dash_shown {
            self.dash.tick();
        }
        // Spinners on work rows; byte rows redraw on their own updates.
        if tick % 2 == 0 {
            for item in st.items.iter().filter(|i| !i.is_bytes) {
                if item.shown.load(Ordering::Relaxed) {
                    item.bar.tick();
                }
            }
        }
    }
}

fn work_style(width: usize, counting: bool) -> ProgressStyle {
    let template = if counting {
        format!("   {{spinner:.yellow}} {{msg:<{width}}} {{pos}}/{{len}} files")
    } else {
        format!("   {{spinner:.yellow}} {{msg:<{width}}}")
    };
    ProgressStyle::with_template(&template).expect("work template")
}

struct LiveHandle {
    live: Arc<Live>,
    item: Arc<LiveItem>,
    label: String,
    finished: AtomicBool,
}

impl ProgressHandle for LiveHandle {
    fn set_bytes(&self, downloaded: u64, total: u64, speed: f64) {
        let bar = &self.item.bar;
        if total > 0 {
            bar.set_length(total);
        }
        bar.set_position(downloaded);
        // A resumed download jumps straight to its offset; don't let that
        // jump inflate indicatif's rate estimate.
        if self.item.first_bytes.swap(false, Ordering::Relaxed) && downloaded > 0 {
            bar.reset_eta();
        }
        self.item
            .speed
            .store(speed.max(0.0) as u64, Ordering::Relaxed);
    }

    fn set_message(&self, msg: &str) {
        self.item
            .bar
            .set_message(truncate_label(msg, self.live.label_width()));
    }

    fn set_count(&self, done: usize, total: usize) {
        let bar = &self.item.bar;
        if !self.item.is_bytes && !self.item.counting.swap(true, Ordering::Relaxed) {
            bar.set_style(work_style(self.live.label_width(), true));
        }
        bar.set_length(total as u64);
        bar.set_position(done as u64);
    }

    fn finish(&self) {
        if !self.finished.swap(true, Ordering::Relaxed) {
            self.live.end_item(&self.item);
        }
    }

    fn finish_with_error(&self, msg: &str) {
        if !self.finished.swap(true, Ordering::Relaxed) {
            self.live.end_item(&self.item);
            let line = format!("{} {}: {}", style("✗").red().bold(), self.label, msg);
            self.live.out.line(&line);
        }
    }
}

impl Drop for LiveHandle {
    fn drop(&mut self) {
        self.finish();
    }
}

// ---------------------------------------------------------------------------
// Plain (line-oriented) renderer
// ---------------------------------------------------------------------------

struct Plain {
    sink: Arc<LineSink>,
    counters: Arc<Counters>,
    state: Mutex<PlainState>,
    ticker: Once,
}

struct PlainState {
    phase: Option<Phase>,
    total: u64,
    done: u64,
    status: String,
    last_emit: Instant,
    last_line: String,
    items: Vec<Arc<PlainItem>>,
}

struct PlainItem {
    pos: AtomicU64,
    total: AtomicU64,
    speed: AtomicU64,
}

impl Plain {
    fn new(sink: Arc<LineSink>) -> Arc<Self> {
        Arc::new(Self {
            sink,
            counters: Arc::new(Counters::default()),
            state: Mutex::new(PlainState {
                phase: None,
                total: 0,
                done: 0,
                status: String::new(),
                last_emit: Instant::now(),
                last_line: String::new(),
                items: Vec::new(),
            }),
            ticker: Once::new(),
        })
    }

    fn ensure_ticker(self: &Arc<Self>) {
        self.ticker.call_once(|| {
            let weak = Arc::downgrade(self);
            let _ = std::thread::Builder::new()
                .name("clf3-progress".into())
                .spawn(move || loop {
                    std::thread::sleep(Duration::from_secs(1));
                    let Some(plain) = weak.upgrade() else { break };
                    let mut st = plain.state.lock().expect("plain state");
                    if st.last_emit.elapsed() >= PLAIN_KEEPALIVE
                        && (st.phase.is_some() || !st.items.is_empty())
                    {
                        plain.emit(&mut st, true, true);
                    }
                });
        });
    }

    fn phase_start(self: &Arc<Self>, phase: Phase) {
        self.ensure_ticker();
        let mut st = self.state.lock().expect("plain state");
        st.phase = Some(phase);
        st.total = 0;
        st.done = 0;
        st.status.clear();
        st.last_line.clear();
        st.last_emit = Instant::now();
        self.sink.line(&format!("==> {}", phase));
    }

    fn overall_set_total(&self, total: u64) {
        let mut st = self.state.lock().expect("plain state");
        st.total = total;
        st.done = 0;
        self.emit(&mut st, true, false);
    }

    fn overall_inc(&self) {
        let mut st = self.state.lock().expect("plain state");
        st.done += 1;
        let complete = st.total > 0 && st.done >= st.total;
        self.emit(&mut st, complete, false);
    }

    fn overall_finish(&self) {
        let mut st = self.state.lock().expect("plain state");
        if st.total > 0 {
            self.emit(&mut st, true, false);
        }
        st.total = 0;
        st.done = 0;
        st.status.clear();
    }

    fn set_status(&self, msg: &str) {
        let mut st = self.state.lock().expect("plain state");
        if st.status != msg {
            st.status = msg.to_string();
            self.emit(&mut st, false, false);
        }
    }

    /// Print a heartbeat line if forced or enough time has passed, skipping
    /// exact repeats unless `keepalive` is set.
    fn emit(&self, st: &mut PlainState, force: bool, keepalive: bool) {
        if !force && st.last_emit.elapsed() < PLAIN_HEARTBEAT {
            return;
        }
        let mut parts = Vec::new();
        let phase = st.phase.map(|p| p.to_string()).unwrap_or_else(|| "Progress".into());
        if st.total > 0 {
            let pct = st.done.saturating_mul(100) / st.total.max(1);
            parts.push(format!("[{} {}/{} {}%]", phase, st.done, st.total, pct));
        } else {
            parts.push(format!("[{}]", phase));
        }
        if !st.status.is_empty() {
            parts.push(st.status.clone());
        }
        if let Some(counters) = self.counters.render() {
            parts.push(counters);
        }
        let byte_items: Vec<_> = st
            .items
            .iter()
            .filter(|i| i.total.load(Ordering::Relaxed) > 0)
            .collect();
        if keepalive && !byte_items.is_empty() {
            let pos: u64 = byte_items.iter().map(|i| i.pos.load(Ordering::Relaxed)).sum();
            let total: u64 = byte_items.iter().map(|i| i.total.load(Ordering::Relaxed)).sum();
            let speed: u64 = byte_items.iter().map(|i| i.speed.load(Ordering::Relaxed)).sum();
            parts.push(format!(
                "{} active, {} / {} at {}/s",
                byte_items.len(),
                format_bytes(pos),
                format_bytes(total),
                format_bytes(speed)
            ));
        }
        let line = parts.join(" · ");
        if !keepalive && line == st.last_line {
            return;
        }
        self.sink.line(&line);
        st.last_line = line;
        st.last_emit = Instant::now();
    }

    fn begin_item(self: &Arc<Self>, label: &str, total_bytes: Option<u64>) -> Arc<dyn ProgressHandle> {
        self.ensure_ticker();
        let item = Arc::new(PlainItem {
            pos: AtomicU64::new(0),
            total: AtomicU64::new(total_bytes.unwrap_or(0)),
            speed: AtomicU64::new(0),
        });
        self.state
            .lock()
            .expect("plain state")
            .items
            .push(Arc::clone(&item));
        Arc::new(PlainHandle {
            plain: Arc::clone(self),
            item,
            label: label.to_string(),
            is_bytes: total_bytes.is_some(),
            started: Instant::now(),
            finished: AtomicBool::new(false),
        })
    }

    fn end_item(&self, item: &Arc<PlainItem>) {
        self.state
            .lock()
            .expect("plain state")
            .items
            .retain(|i| !Arc::ptr_eq(i, item));
    }
}

struct PlainHandle {
    plain: Arc<Plain>,
    item: Arc<PlainItem>,
    label: String,
    is_bytes: bool,
    started: Instant,
    finished: AtomicBool,
}

impl ProgressHandle for PlainHandle {
    fn set_bytes(&self, downloaded: u64, total: u64, speed: f64) {
        self.item.pos.store(downloaded, Ordering::Relaxed);
        if total > 0 {
            self.item.total.store(total, Ordering::Relaxed);
        }
        self.item.speed.store(speed.max(0.0) as u64, Ordering::Relaxed);
    }

    // Item messages are display relabels ("Mega: foo", "foo (mirror)");
    // anything noteworthy is also logged, so Plain mode skips them.
    fn set_message(&self, _msg: &str) {}

    fn set_count(&self, _done: usize, _total: usize) {}

    fn finish(&self) {
        if !self.finished.swap(true, Ordering::Relaxed) {
            self.plain.end_item(&self.item);
            if self.is_bytes {
                let size = self.item.total.load(Ordering::Relaxed);
                self.plain.sink.line(&format!(
                    "  done  {} ({}, {})",
                    self.label,
                    format_bytes(size),
                    format_duration(self.started.elapsed())
                ));
            }
        }
    }

    fn finish_with_error(&self, msg: &str) {
        if !self.finished.swap(true, Ordering::Relaxed) {
            self.plain.end_item(&self.item);
            self.plain
                .sink
                .line(&format!("  FAIL  {}: {}", self.label, msg));
        }
    }
}

impl Drop for PlainHandle {
    fn drop(&mut self) {
        self.finish();
    }
}

// ---------------------------------------------------------------------------
// Tracing integration
// ---------------------------------------------------------------------------

/// `MakeWriter` factory for tracing that routes through the reporter's
/// [`LineSink`].
#[derive(Clone)]
pub struct CliWriterFactory {
    sink: Arc<LineSink>,
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CliWriterFactory {
    type Writer = CliWriter;

    fn make_writer(&'a self) -> Self::Writer {
        CliWriter {
            sink: Arc::clone(&self.sink),
            buffer: Vec::new(),
        }
    }
}

/// Buffers one formatted event and emits it as whole lines.
///
/// tracing-subscriber creates a writer per event and calls `write_all` but
/// never `flush`, so the buffered event is emitted on drop.
pub struct CliWriter {
    sink: Arc<LineSink>,
    buffer: Vec<u8>,
}

impl CliWriter {
    fn emit(&mut self) {
        if self.buffer.is_empty() {
            return;
        }
        let msg = String::from_utf8_lossy(&self.buffer).trim_end().to_string();
        self.buffer.clear();
        if !msg.is_empty() {
            self.sink.line(&msg);
        }
    }
}

impl std::io::Write for CliWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.emit();
        Ok(())
    }
}

impl Drop for CliWriter {
    fn drop(&mut self) {
        self.emit();
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
fn read_disk_write_bytes() -> Option<u64> {
    let s = std::fs::read_to_string("/proc/self/io").ok()?;
    s.lines()
        .find_map(|line| line.strip_prefix("write_bytes:"))
        .and_then(|rest| rest.trim().parse().ok())
}

#[cfg(not(target_os = "linux"))]
fn read_disk_write_bytes() -> Option<u64> {
    None
}

#[cfg(target_os = "linux")]
fn read_total_memory_bytes() -> Option<u64> {
    let s = std::fs::read_to_string("/proc/meminfo").ok()?;
    let rest = s.lines().find_map(|line| line.strip_prefix("MemTotal:"))?;
    let kb = rest.split_whitespace().next()?.parse::<u64>().ok()?;
    Some(kb.saturating_mul(1024))
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

fn format_duration(d: Duration) -> String {
    let secs = d.as_secs();
    let (h, m, s) = (secs / 3600, (secs / 60) % 60, secs % 60);
    if h > 0 {
        format!("{}h{:02}m{:02}s", h, m, s)
    } else if m > 0 {
        format!("{}m{:02}s", m, s)
    } else {
        format!("{}s", s)
    }
}

/// Truncate on char boundaries, marking the cut with `…`.
fn truncate_label(name: &str, max_chars: usize) -> String {
    if name.chars().count() <= max_chars {
        return name.to_string();
    }
    let mut label: String = name.chars().take(max_chars.saturating_sub(1)).collect();
    label.push('…');
    label
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_label_respects_char_boundaries() {
        assert_eq!(truncate_label("short", 10), "short");
        assert_eq!(truncate_label("Ünïcödé-ärchïvé.7z", 8), "Ünïcödé…");
        assert_eq!(truncate_label("Ünïcödé-ärchïvé.7z", 8).chars().count(), 8);
    }

    #[test]
    fn counters_render_joined_and_removed() {
        let counters = Counters::default();
        assert!(counters.render().is_none());
        let a = counters.add("Extracted");
        let b = counters.add("DDS");
        counters.set(a, 4, 10);
        counters.set(b, 1, 3);
        assert_eq!(counters.render().as_deref(), Some("Extracted 4/10 · DDS 1/3"));
        counters.remove(a);
        assert_eq!(counters.render().as_deref(), Some("DDS 1/3"));
    }

    #[test]
    fn format_duration_is_compact() {
        assert_eq!(format_duration(Duration::from_secs(5)), "5s");
        assert_eq!(format_duration(Duration::from_secs(125)), "2m05s");
        assert_eq!(format_duration(Duration::from_secs(3723)), "1h02m03s");
    }

    #[test]
    fn plain_items_are_tracked_and_released() {
        let reporter = CliReporter::new(4, ProgressMode::Plain);
        let Renderer::Plain(plain) = &reporter.renderer else {
            panic!("expected plain renderer");
        };
        let handle = reporter.begin_item("archive.7z", Some(100));
        handle.set_bytes(50, 100, 10.0);
        assert_eq!(plain.state.lock().unwrap().items.len(), 1);
        drop(handle);
        assert!(plain.state.lock().unwrap().items.is_empty());
    }
}
