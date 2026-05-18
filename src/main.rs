//! CLF3 - Wabbajack Modlist Installer
//!
//! Named after Chlorine Trifluoride - burns through modlists
//! like CLF3 burns through concrete.

// Use mimalloc — returns freed pages to OS aggressively, preventing RSS bloat
// during extraction where hundreds of large buffers are allocated and freed.
// glibc malloc holds onto freed arena pages; mimalloc uses OS-native page return.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod archive;
mod browser_gui;
mod bsa;
mod downloaders;
mod fluorine;
mod game_finder;
mod hash;
mod installer;
mod modlist;
mod nxm_handler;
mod octodiff;
mod paths;
mod self_update;
mod settings;
mod textures;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use installer::{
    CliReporter, ExtractStrategy, InstallConfig, Installer, ProgressMode, ProgressReporter,
};

/// CLI-facing enum for the `--extract` flag. Maps to the internal
/// `installer::ExtractStrategy` used by the install pipeline.
#[derive(Copy, Clone, Debug, ValueEnum)]
enum ExtractStrategyArg {
    /// Extract archives as they finish downloading. Network + CPU overlap.
    Streaming,
    /// Download everything first, then run 4 sequential extract phases.
    Phased,
}

impl From<ExtractStrategyArg> for ExtractStrategy {
    fn from(arg: ExtractStrategyArg) -> Self {
        match arg {
            ExtractStrategyArg::Streaming => ExtractStrategy::Streaming,
            ExtractStrategyArg::Phased => ExtractStrategy::Phased,
        }
    }
}

/// CLI-facing progress rendering mode.
#[derive(Copy, Clone, Debug, ValueEnum)]
enum ProgressModeArg {
    /// Use full progress on terminals and plain lines otherwise.
    Auto,
    /// Interactive progress with system status and active worker slots.
    Full,
    /// Line-oriented human-readable progress.
    Plain,
}

impl From<ProgressModeArg> for ProgressMode {
    fn from(arg: ProgressModeArg) -> Self {
        match arg {
            ProgressModeArg::Auto => ProgressMode::Auto,
            ProgressModeArg::Full => ProgressMode::Full,
            ProgressModeArg::Plain => ProgressMode::Plain,
        }
    }
}
use std::io::IsTerminal;
use std::path::PathBuf;
use std::sync::Arc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

#[derive(Parser)]
#[command(name = "clf3")]
#[command(author = "CLF3 Team")]
#[command(version)]
#[command(
    about = "Wabbajack modlist installer - burns through modlists like CLF3 burns through concrete"
)]
struct Cli {
    /// When omitted (e.g. double-click from a file manager), the browser
    /// GUI opens. Pass `clf3 help` to see all subcommands.
    #[command(subcommand)]
    command: Option<Commands>,

    /// Enable verbose logging (use RUST_LOG=debug for more detail)
    #[arg(short, long, global = true)]
    verbose: bool,
}

#[derive(Subcommand)]
#[allow(clippy::large_enum_variant)]
enum Commands {
    /// Browse available Wabbajack modlists
    Browser,

    /// Install a Wabbajack modlist
    Install {
        /// Path or URL to the .wabbajack file
        wabbajack_file: String,

        /// Directory for downloaded archives
        downloads: PathBuf,

        /// Installation target directory (where mods will be installed)
        output: PathBuf,

        /// Game installation directory (overrides auto-detection)
        #[arg(short, long)]
        game: Option<PathBuf>,

        /// Nexus Mods API key (overrides saved setting)
        #[arg(long, env = "NEXUS_API_KEY")]
        nexus_key: Option<String>,

        /// Maximum concurrent downloads (defaults to CPU thread count)
        #[arg(short, long)]
        concurrent: Option<usize>,

        /// Maximum parallel workers for extraction/install phase
        #[arg(long)]
        install_workers: Option<usize>,

        /// Maximum number of BSA/BA2 archives processed concurrently (default: 1)
        #[arg(long)]
        bsa_workers: Option<usize>,

        /// Maximum number of 7z archives processed concurrently (default: 1)
        #[arg(long)]
        sevenzip_workers: Option<usize>,

        /// Use NXM browser mode instead of direct API (avoids rate limits)
        #[arg(long)]
        nxm_mode: bool,

        /// Browser command to open Nexus pages (default: xdg-open)
        #[arg(long, default_value = "xdg-open")]
        browser: String,

        /// LoversLab email (overrides saved setting)
        #[arg(long, env = "LOVERSLAB_EMAIL")]
        ll_email: Option<String>,

        /// LoversLab password (overrides saved setting)
        #[arg(long, env = "LOVERSLAB_PASSWORD")]
        ll_password: Option<String>,

        /// Extraction strategy.
        ///
        /// - `streaming` (default): extract archives incrementally as they
        ///   finish downloading — best for large (>50 GB) modlists where the
        ///   download dominates wall-clock time. CPU and network run fully in
        ///   parallel.
        /// - `phased`: wait for all downloads to finish, then run 4 sequential
        ///   phases at full CPU. Better for small modlists where download is
        ///   short and CPU-heavy work (DDS, BSA) dominates.
        #[arg(long, value_enum, default_value_t = ExtractStrategyArg::Streaming)]
        extract: ExtractStrategyArg,

        /// Progress output mode.
        #[arg(long, value_enum, default_value_t = ProgressModeArg::Auto)]
        progress: ProgressModeArg,

        /// Override the modlist gallery `machine_name` recorded with this
        /// install. Defaults to a lookup against
        /// `Settings::browser_list_paths` keyed by the output directory.
        #[arg(long)]
        machine_name: Option<String>,

        /// Write the final InstallStats (downloads, directives, failures,
        /// manual-download list) as JSON to this path. Useful for external
        /// tooling driving CLF3.
        #[arg(long)]
        report_json: Option<PathBuf>,
    },

    /// Modlist update management (check for / apply gallery updates).
    Modlist {
        #[command(subcommand)]
        action: ModlistAction,
    },

    /// Set and verify your Nexus Mods API key
    SetApiKey {
        /// Your Nexus Mods API key
        key: String,
    },

    /// Set and verify your LoversLab login credentials
    SetLlLogin {
        /// LoversLab email or username
        email: String,
        /// LoversLab password
        password: String,
    },

    /// List available GPUs for texture encoding
    ListGpu {
        /// Emit the GPU list as JSON instead of the human-readable table.
        #[arg(long)]
        json: bool,
    },

    /// Select a GPU for texture encoding (use list-gpu to see indices)
    SelectGpu {
        /// GPU index from list-gpu (or "auto" for automatic selection)
        index: String,
    },

    /// Show current saved settings
    Config,

    /// Show information about a Wabbajack modlist
    Info {
        /// Path to the .wabbajack file
        wabbajack_file: PathBuf,
    },

    /// Fluorine Manager integration (auto-register finished installs).
    Fluorine {
        #[command(subcommand)]
        action: FluorineAction,
    },

    /// Self-update: check GitHub for a newer clf3 release and replace the
    /// running binary in place. Atomic on Linux — the kernel keeps the old
    /// inode mapped so the running invocation isn't disrupted.
    SelfUpdate {
        /// Only check; don't download or replace anything. Exits 0 if up to
        /// date, 1 if an update is available.
        #[arg(long)]
        check: bool,

        /// Skip the version-diff confirmation and apply.
        #[arg(long, short)]
        yes: bool,

        /// Re-download and replace even when versions match (or are
        /// unparseable). Useful for recovering from a broken install.
        #[arg(long)]
        force: bool,
    },
}

#[derive(Subcommand)]
enum FluorineAction {
    /// Show whether Fluorine is detected and where.
    Status,

    /// Download the latest Fluorine release and extract it to
    /// ~/.local/share/fluorine-manager (or `--dest` if given).
    Install {
        /// Override the install destination directory.
        #[arg(long)]
        dest: Option<PathBuf>,
    },

    /// Register an existing install directory as a Fluorine portable instance.
    Register {
        /// Path to the modlist install directory (must contain ModOrganizer.ini).
        install_dir: PathBuf,

        /// Also set this as the current instance Fluorine opens by default.
        #[arg(long)]
        make_current: bool,
    },

    /// Toggle whether successful installs are auto-registered with Fluorine.
    Enable {
        /// `true` or `false`. Persisted in settings.
        value: bool,
    },

    /// Make sure Fluorine is available on disk (downloading the latest
    /// release if not), and print where it lives. No registration step is
    /// performed — useful as a "primer" for launcher tooling.
    Ensure {
        /// Emit `{"binary": "...", "source": "..."}` as JSON instead of the
        /// human-readable line.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
enum ModlistAction {
    /// Check installed modlists against the gallery for newer versions.
    ///
    /// With no name, scans every install CLF3 knows about. With `--name`,
    /// reports only that modlist. The name accepts gallery `machine_name`
    /// values or partial-name queries (resolved like the GUI search bar).
    Check {
        /// Only check this modlist. Accepts a full or partial name.
        #[arg(long)]
        name: Option<String>,

        /// Emit the report as JSON instead of a human-readable table.
        #[arg(long)]
        json: bool,
    },

    /// Re-install a modlist against the latest gallery version. Reuses the
    /// install + downloads directories recorded in `.clf3-install.json`.
    Update {
        /// Full or partial modlist name. Partial names use the same
        /// matching rules as the gallery search bar.
        name: String,

        /// Skip the version-diff confirmation prompt.
        #[arg(long)]
        yes: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Set up file logging (always enabled) next to the executable
    let log_dir = std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|p| p.to_path_buf()))
        .unwrap_or_else(|| PathBuf::from("."));

    // Create timestamped log filename
    let timestamp = chrono::Local::now().format("%Y-%m-%d_%H-%M-%S");
    let log_filename = format!("clf3-{}.log", timestamp);

    // Set up file appender
    let file_appender = tracing_appender::rolling::never(&log_dir, &log_filename);
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    // Create filter - file gets info level, console follows user preference
    let file_filter = EnvFilter::new("clf3=info,warn");
    let console_filter = EnvFilter::from_default_env().add_directive(if cli.verbose {
        "clf3=debug".parse()?
    } else {
        "clf3=warn".parse()?
    });

    // File layer (always enabled)
    let file_layer = tracing_subscriber::fmt::layer()
        .with_writer(non_blocking)
        .with_ansi(false)
        .with_filter(file_filter);

    // Default to opening the browser when invoked with no subcommand — makes
    // double-click from a desktop / file manager Just Work.
    let command = cli.command.unwrap_or(Commands::Browser);

    let requested_progress_mode = match &command {
        Commands::Install { progress, .. } => (*progress).into(),
        // Other commands (Browser, SetApiKey, ListGpu, Config, Info, Fluorine…)
        // never spawn the worker pool — keep the CLI reporter silent so the
        // "System: starting…" / "Workers" UI doesn't render on top of them.
        _ => ProgressMode::Plain,
    };
    let progress_mode = match requested_progress_mode {
        ProgressMode::Auto
            if std::io::stdout().is_terminal() && std::io::stderr().is_terminal() =>
        {
            ProgressMode::Full
        }
        ProgressMode::Auto => ProgressMode::Plain,
        mode => mode,
    };

    // Create CLI reporter for progress bars — must exist before tracing init
    // so its writer factory can route tracing output through MultiProgress.
    let cli_reporter = CliReporter::new(16, progress_mode);

    // Console layer — routes through CliReporter's MultiProgress
    let console_layer = tracing_subscriber::fmt::layer()
        .with_writer(cli_reporter.make_writer_factory())
        .with_filter(console_filter);

    tracing_subscriber::registry()
        .with(file_layer)
        .with(console_layer)
        .init();

    let log_path = log_dir.join(&log_filename);
    tracing::info!("CLF3 started, logging to {}", log_path.display());

    // Panic hook: log panics from ANY thread to the log file before aborting.
    let panic_log = log_path.clone();
    std::panic::set_hook(Box::new(move |info| {
        let thread = std::thread::current();
        let thread_name = thread.name().unwrap_or("<unnamed>");
        let location = info
            .location()
            .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
            .unwrap_or_else(|| "unknown".into());
        let payload = if let Some(s) = info.payload().downcast_ref::<&str>() {
            s.to_string()
        } else if let Some(s) = info.payload().downcast_ref::<String>() {
            s.clone()
        } else {
            "Box<dyn Any>".into()
        };
        let msg = format!(
            "PANIC on thread '{}' at {}:\n  {}\n  backtrace: {:?}",
            thread_name,
            location,
            payload,
            std::backtrace::Backtrace::force_capture()
        );
        tracing::error!("{}", msg);
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&panic_log)
        {
            use std::io::Write;
            let _ = writeln!(f, "\n[PANIC] {}", msg);
        }
        eprintln!("PANIC: {}", msg);
    }));

    // Signal handler: log SIGTERM/SIGHUP so we know what killed the process.
    {
        let signal_log = log_path.clone();
        std::thread::spawn(move || {
            use std::sync::atomic::{AtomicBool, Ordering};
            static TERM: AtomicBool = AtomicBool::new(false);
            static HUP: AtomicBool = AtomicBool::new(false);

            unsafe {
                libc::signal(
                    libc::SIGTERM,
                    sigterm_handler as *const () as libc::sighandler_t,
                );
                libc::signal(
                    libc::SIGHUP,
                    sighup_handler as *const () as libc::sighandler_t,
                );
            }

            extern "C" fn sigterm_handler(_: libc::c_int) {
                TERM.store(true, Ordering::SeqCst);
            }
            extern "C" fn sighup_handler(_: libc::c_int) {
                HUP.store(true, Ordering::SeqCst);
            }

            loop {
                std::thread::sleep(std::time::Duration::from_millis(100));
                if TERM.load(Ordering::SeqCst) {
                    let rss = installer::current_rss_kb().unwrap_or(0);
                    let msg = format!(
                        "Received SIGTERM — process being killed externally. RSS: {}MB",
                        rss / 1024
                    );
                    tracing::error!("{}", msg);
                    if let Ok(mut f) = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&signal_log)
                    {
                        use std::io::Write;
                        let _ = writeln!(f, "\n[SIGNAL] {}", msg);
                    }
                    eprintln!("{}", msg);
                    std::process::exit(143);
                }
                if HUP.load(Ordering::SeqCst) {
                    let msg = "Received SIGHUP — terminal closed or session ended";
                    tracing::error!("{}", msg);
                    if let Ok(mut f) = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&signal_log)
                    {
                        use std::io::Write;
                        let _ = writeln!(f, "\n[SIGNAL] {}", msg);
                    }
                    eprintln!("{}", msg);
                    std::process::exit(129);
                }
            }
        });
    }

    // Best-effort startup check for a newer clf3 release. Skips itself for
    // the `SelfUpdate` subcommand (to avoid double-querying GitHub) and runs
    // with a short timeout so the network never blocks normal commands.
    // Result is cached in `~/.config/clf3/update_cache.json` for 24h.
    if !matches!(command, Commands::SelfUpdate { .. }) {
        // Don't `await` directly — race it against a short timeout so an
        // unreachable api.github.com doesn't visibly delay every invocation.
        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            self_update::startup_check_with_notice(),
        )
        .await;
    }

    match command {
        Commands::Browser => {
            if let Err(e) = browser_gui::launch_browser() {
                eprintln!("Browser GUI error: {}", e);
                std::process::exit(1);
            }
        }

        Commands::SetApiKey { key } => {
            println!("Verifying Nexus API key...");
            let nexus = downloaders::NexusDownloader::new(&key)?;
            match nexus.validate().await {
                Ok(user_info) => {
                    let mut settings = settings::Settings::load();
                    settings.nexus_api_key = key;
                    settings.save()?;
                    println!(
                        "API key verified! Logged in as: {} (Premium: {})",
                        user_info.name,
                        if user_info.is_premium { "Yes" } else { "No" }
                    );
                }
                Err(e) => {
                    eprintln!("API key verification failed: {}", e);
                    std::process::exit(1);
                }
            }
        }

        Commands::SetLlLogin { email, password } => {
            println!("Verifying LoversLab login...");
            match downloaders::LoversLabDownloader::login(&email, &password).await {
                Ok(_) => {
                    let mut settings = settings::Settings::load();
                    settings.loverslab_email = email;
                    settings.loverslab_password = password;
                    settings.save()?;
                    println!("LoversLab login verified and saved!");
                }
                Err(e) => {
                    eprintln!("LoversLab login failed: {}", e);
                    std::process::exit(1);
                }
            }
        }

        Commands::ListGpu { json } => {
            let gpus = textures::list_gpus();
            if json {
                println!("{}", serde_json::to_string_pretty(&gpus)?);
            } else if gpus.is_empty() {
                println!("No GPUs detected.");
            } else {
                println!("Available GPUs:");
                for gpu in &gpus {
                    println!(
                        "  [{}] {} ({}, {})",
                        gpu.adapter_index, gpu.name, gpu.backend, gpu.device_type
                    );
                }
                let settings = settings::Settings::load();
                match settings.gpu_index {
                    Some(idx) => println!("\nCurrently selected: [{}] {}", idx, settings.gpu_name),
                    None => println!("\nCurrently selected: auto"),
                }
            }
        }

        Commands::SelectGpu { index } => {
            if index == "auto" {
                let mut settings = settings::Settings::load();
                settings.gpu_index = None;
                settings.gpu_name = String::new();
                settings.save()?;
                println!("GPU selection set to: auto (recommended)");
            } else {
                let idx: usize = index.parse().map_err(|_| {
                    anyhow::anyhow!(
                        "Invalid GPU index '{}'. Use a number from list-gpu or 'auto'.",
                        index
                    )
                })?;
                let gpus = textures::list_gpus();
                let gpu = gpus
                    .iter()
                    .find(|g| g.adapter_index == idx)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "GPU index {} not found. Run 'clf3 list-gpu' to see available GPUs.",
                            idx
                        )
                    })?;
                let mut settings = settings::Settings::load();
                settings.gpu_index = Some(idx);
                settings.gpu_name = gpu.name.clone();
                settings.save()?;
                println!(
                    "GPU selected: [{}] {} ({}, {})",
                    idx, gpu.name, gpu.backend, gpu.device_type
                );
            }
        }

        Commands::Config => {
            let settings = settings::Settings::load();
            let path = settings::Settings::settings_path()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|_| "unknown".into());
            println!("Settings file: {}", path);
            println!();
            println!(
                "Nexus API key:    {}",
                if settings.nexus_api_key.is_empty() {
                    "(not set)"
                } else {
                    "(set)"
                }
            );
            println!(
                "LoversLab login:  {}",
                if settings.loverslab_email.is_empty() {
                    "(not set)"
                } else {
                    settings.loverslab_email.as_str()
                }
            );
            println!(
                "GPU:              {}",
                match settings.gpu_index {
                    Some(idx) => format!("[{}] {}", idx, settings.gpu_name),
                    None => "auto".into(),
                }
            );
            if !settings.patch_cache_dir.is_empty() {
                println!("Patch cache:      {}", settings.patch_cache_dir);
            }
        }

        Commands::Install {
            wabbajack_file,
            downloads,
            output,
            game,
            nexus_key,
            concurrent,
            install_workers,
            bsa_workers,
            sevenzip_workers,
            nxm_mode,
            browser,
            ll_email,
            ll_password,
            extract,
            progress: _,
            machine_name,
            report_json,
        } => {
            // Remember the original CLI argument as a URL if it was one — it
            // ends up recorded in `.clf3-install.json` so `modlist update`
            // can fall back to it later.
            let original_wabbajack_url = if wabbajack_file.starts_with("http://")
                || wabbajack_file.starts_with("https://")
            {
                Some(wabbajack_file.clone())
            } else {
                None
            };

            // If wabbajack_file is a URL, download it first.
            let wabbajack_file = if original_wabbajack_url.is_some() {
                fetch_wabbajack_from_url(&wabbajack_file).await?
            } else {
                PathBuf::from(&wabbajack_file)
            };

            let settings = settings::Settings::load();

            // Resolve API key: CLI arg > env var > saved settings
            let nexus_key = nexus_key
                .or_else(|| {
                    if settings.nexus_api_key.is_empty() {
                        None
                    } else {
                        Some(settings.nexus_api_key.clone())
                    }
                })
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Nexus API key required. Set it with: clf3 set-api-key YOUR_KEY"
                    )
                })?;

            // Resolve LL credentials: CLI arg > env var > saved settings
            let ll_email = ll_email.unwrap_or_else(|| settings.loverslab_email.clone());
            let ll_password = ll_password.unwrap_or_else(|| settings.loverslab_password.clone());

            // Game dir: CLI arg > auto-detect from modlist
            let game_dir = match game {
                Some(g) => g,
                None => {
                    // Try to auto-detect game path from the modlist's game type,
                    // preferring installs whose game files actually match the
                    // modlist's expected hashes (Steam first, then Heroic/GOG).
                    match auto_detect_game_dir(&wabbajack_file) {
                        Some((p, store)) => {
                            println!("Auto-detected game directory: {} ({})", p.display(), store);
                            p
                        }
                        None => {
                            anyhow::bail!(
                                "Could not auto-detect a game directory with matching files. \
                                 Specify one with --game PATH"
                            );
                        }
                    }
                }
            };

            // Default to CPU thread count
            let thread_count = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4);
            let concurrent = concurrent.unwrap_or(thread_count).max(1);
            let install_workers = install_workers.unwrap_or(thread_count).max(1);
            let bsa_workers = bsa_workers.unwrap_or(1).max(1);
            let sevenzip_workers = sevenzip_workers.unwrap_or(thread_count).max(1);

            println!("CLF3 - Wabbajack Modlist Installer");
            println!("Concurrent downloads: {}", concurrent);
            println!(
                "Install workers: {} (BSA archives in parallel: {})",
                install_workers, bsa_workers
            );
            println!("7z archives in parallel: {}", sevenzip_workers);
            if nxm_mode {
                println!("NXM Mode: enabled (browser-based downloads)");
            }
            println!();

            let patch_cache_dir = if settings.patch_cache_dir.is_empty() {
                None
            } else {
                Some(PathBuf::from(&settings.patch_cache_dir))
            };

            // Best-effort machine_name resolution. Explicit `--machine-name`
            // wins. Otherwise fall back to a unique entry in
            // `Settings::browser_list_paths` whose install_dir matches
            // `output` — the GUI populates this when the user picks paths
            // from the gallery, so most "from gallery" installs land here.
            let resolved_machine_name = machine_name
                .clone()
                .filter(|s| !s.is_empty())
                .or_else(|| derive_machine_name_from_settings(&settings, &output));

            let install_dir_for_fluorine = output.clone();

            let config = InstallConfig {
                wabbajack_path: wabbajack_file,
                output_dir: output,
                downloads_dir: downloads,
                game_dir,
                nexus_api_key: nexus_key,
                max_concurrent_downloads: concurrent,
                max_install_workers: install_workers,
                max_parallel_bsa_archives: bsa_workers,
                max_parallel_7z_archives: sevenzip_workers,
                nxm_mode,
                browser,
                patch_cache_dir,
                progress_callback: None,
                reporter: cli_reporter.clone() as Arc<dyn ProgressReporter>,
                loverslab_email: ll_email,
                loverslab_password: ll_password,
                extract_strategy: extract.into(),
                machine_name: resolved_machine_name,
                wabbajack_url: original_wabbajack_url,
            };

            let mut installer = Installer::new(config)?;
            let stats = installer.run_pipelined().await?;

            let reporter = &cli_reporter;
            let total_processed =
                stats.directives_completed + stats.directives_skipped + stats.directives_failed;

            reporter.log("\n=== Installation Summary ===");
            reporter.log(&format!(
                "Downloads:  {} downloaded, {} skipped, {} manual, {} failed",
                stats.archives_downloaded,
                stats.archives_skipped,
                stats.archives_manual,
                stats.archives_failed
            ));
            reporter.log(&format!(
                "Directives: {} new, {} existing, {} failed ({} total)",
                stats.directives_completed,
                stats.directives_skipped,
                stats.directives_failed,
                total_processed
            ));

            if !stats.manual_downloads.is_empty() {
                reporter.log(&format!(
                    "\n=== Manual Downloads Needed ({}) ===",
                    stats.manual_downloads.len()
                ));
                for (i, md) in stats.manual_downloads.iter().enumerate() {
                    reporter.log(&format!("{}. {}", i + 1, md.name));
                    reporter.log(&format!("   URL: {}", md.url));
                    reporter.log(&format!("   Size: {} bytes", md.expected_size));
                    if let Some(ref prompt) = md.prompt {
                        reporter.log(&format!("   Note: {}", prompt));
                    }
                }
            }

            if !stats.failed_downloads.is_empty() {
                reporter.log(&format!(
                    "\n=== Failed Downloads ({}) ===",
                    stats.failed_downloads.len()
                ));
                for (i, fd) in stats.failed_downloads.iter().enumerate() {
                    reporter.log(&format!("{}. {}", i + 1, fd.name));
                    reporter.log(&format!("   URL: {}", fd.url));
                    reporter.log(&format!("   Error: {}", fd.error));
                }
            }

            let installation_succeeded = stats.archives_manual == 0
                && stats.archives_failed == 0
                && stats.directives_failed == 0;

            if stats.archives_manual > 0 || stats.archives_failed > 0 {
                reporter.log("\nSome archives need manual download. Fix issues and run again.");
            } else if stats.directives_failed > 0 {
                reporter.log("\nSome directives failed. Check the log file for details.");
            } else {
                reporter.log("\nInstallation complete!");
            }

            // Fluorine auto-registration. Only runs on a clean install so we
            // don't add half-broken instances to the user's Fluorine sidebar.
            if installation_succeeded && settings.add_to_fluorine {
                if let Err(e) = ensure_fluorine_and_register(&settings, &install_dir_for_fluorine).await {
                    reporter.log(&format!("\nFluorine integration failed: {}", e));
                } else {
                    reporter.log(&format!(
                        "\nRegistered '{}' as a Fluorine portable instance.",
                        install_dir_for_fluorine.display()
                    ));
                }
            }

            // Optional structured report for external tooling.
            if let Some(report_path) = report_json {
                let content = serde_json::to_string_pretty(&stats)
                    .context("Failed to serialize InstallStats")?;
                std::fs::write(&report_path, content)
                    .with_context(|| format!("Failed to write report to {}", report_path.display()))?;
                reporter.log(&format!("Wrote install report to {}", report_path.display()));
            }
        }

        Commands::Modlist { action } => {
            run_modlist_action(action).await?;
        }

        Commands::Info { wabbajack_file } => {
            println!("Parsing {}...\n", wabbajack_file.display());

            let modlist = modlist::parse_wabbajack_file(&wabbajack_file)?;

            println!("=== Modlist Information ===");
            println!("Name:              {}", modlist.name);
            println!("Author:            {}", modlist.author);
            println!("Version:           {}", modlist.version);
            println!("Game:              {}", modlist.game_type);
            println!("Wabbajack Version: {}", modlist.wabbajack_version);
            println!(
                "NSFW:              {}",
                if modlist.is_nsfw { "Yes" } else { "No" }
            );
            println!();
            println!("Archives:          {}", modlist.archives.len());
            println!("Directives:        {}", modlist.directives.len());

            let mut type_counts = std::collections::HashMap::with_capacity(10);
            for directive in &modlist.directives {
                *type_counts.entry(directive.directive_type()).or_insert(0) += 1;
            }

            println!("\n=== Directives by Type ===");
            let mut counts: Vec<_> = type_counts.into_iter().collect();
            counts.sort_by(|a, b| b.1.cmp(&a.1));
            for (dtype, count) in counts {
                println!("{:>8}  {}", count, dtype);
            }

            let mut source_counts = std::collections::HashMap::with_capacity(10);
            for archive in &modlist.archives {
                let source_type = match &archive.state {
                    modlist::DownloadState::Nexus(_) => "Nexus",
                    modlist::DownloadState::Http(_) => "HTTP",
                    modlist::DownloadState::WabbajackCDN(_) => "WabbajackCDN",
                    modlist::DownloadState::GoogleDrive(_) => "GoogleDrive",
                    modlist::DownloadState::MediaFire(_) => "MediaFire",
                    modlist::DownloadState::Manual(_) => "Manual",
                    modlist::DownloadState::Mega(_) => "Mega",
                    modlist::DownloadState::GameFileSource(_) => "GameFile",
                };
                *source_counts.entry(source_type).or_insert(0) += 1;
            }

            println!("\n=== Download Sources ===");
            let mut counts: Vec<_> = source_counts.into_iter().collect();
            counts.sort_by(|a, b| b.1.cmp(&a.1));
            for (source, count) in counts {
                println!("{:>8}  {}", count, source);
            }
        }

        Commands::Fluorine { action } => {
            run_fluorine_action(action).await?;
        }

        Commands::SelfUpdate { check, yes, force } => {
            run_self_update(check, yes, force).await?;
        }
    }

    Ok(())
}

/// `clf3 self-update` handler. Implements the three modes — check-only,
/// interactive (default), and `--yes`/`--force` non-interactive apply.
async fn run_self_update(check: bool, yes: bool, force: bool) -> Result<()> {
    let release = self_update::fetch_latest_release()
        .await
        .context("Failed to query GitHub for the latest CLF3 release")?;
    let remote_version = release.version().to_string();
    let verdict = self_update::compare_to_running(&remote_version);

    let local = self_update::current_version();
    println!("Running version: {}", local);
    println!("Latest release:  {} ({})", remote_version, release.name);
    println!("Verdict:         {:?}", verdict);

    if check {
        if verdict.update_available() {
            println!(
                "\nUpdate available. Run `clf3 self-update --yes` to apply."
            );
            std::process::exit(1);
        } else {
            println!("\nUp to date.");
            return Ok(());
        }
    }

    let should_apply = force
        || matches!(verdict, self_update::UpdateVerdict::Newer)
        || (yes
            && matches!(
                verdict,
                self_update::UpdateVerdict::Equal | self_update::UpdateVerdict::Ahead
            ));

    if !should_apply {
        match verdict {
            self_update::UpdateVerdict::Equal | self_update::UpdateVerdict::Ahead => {
                println!("\nNothing to do. Pass --force to re-download anyway.");
                return Ok(());
            }
            self_update::UpdateVerdict::Unknown => {
                anyhow::bail!(
                    "Remote tag '{}' isn't comparable to running version '{}'. \
                     Pass --force to overwrite anyway.",
                    remote_version,
                    local
                );
            }
            self_update::UpdateVerdict::Newer => unreachable!(),
        }
    }

    if !yes && !force {
        println!(
            "\nReady to download and replace the running clf3 binary. \
             Re-run with --yes to apply."
        );
        anyhow::bail!("Self-update aborted (confirmation required — pass --yes to proceed)");
    }

    let outcome = self_update::run_update(force).await?;
    if outcome.replaced {
        println!("\nclf3 updated to {}.", outcome.version);
        println!("Restart any running clf3 invocations to pick up the new binary.");
    } else {
        println!("\nNo replacement needed — already on {}.", outcome.version);
    }
    Ok(())
}

/// Make sure a Fluorine install is available, downloading the latest release
/// if not, then register `install_dir` as a portable instance.
async fn ensure_fluorine_and_register(
    settings: &settings::Settings,
    install_dir: &std::path::Path,
) -> Result<()> {
    let override_path = if settings.fluorine_path.is_empty() {
        None
    } else {
        Some(settings.fluorine_path.as_str())
    };

    if fluorine::detect(override_path).is_none() {
        tracing::info!("Fluorine not detected — downloading the latest release");
        fluorine::download_latest(None).await?;
    }

    fluorine::register_portable_instance(install_dir, false)?;
    Ok(())
}

async fn run_fluorine_action(action: FluorineAction) -> Result<()> {
    match action {
        FluorineAction::Status => {
            let settings = settings::Settings::load();
            let override_path = if settings.fluorine_path.is_empty() {
                None
            } else {
                Some(settings.fluorine_path.as_str())
            };

            println!(
                "Auto-register installs: {}",
                if settings.add_to_fluorine {
                    "enabled"
                } else {
                    "disabled"
                }
            );
            match fluorine::detect(override_path) {
                Some(install) => println!(
                    "Detected:               {} (via {})",
                    install.binary.display(),
                    install.source
                ),
                None => println!(
                    "Detected:               (none — run `clf3 fluorine install` to fetch \
                     the latest release)"
                ),
            }
        }

        FluorineAction::Install { dest } => {
            println!("Downloading latest Fluorine release...");
            let path = fluorine::download_latest(dest).await?;
            println!("Installed Fluorine to: {}", path.display());
        }

        FluorineAction::Register {
            install_dir,
            make_current,
        } => {
            if !install_dir.exists() {
                anyhow::bail!("Install directory does not exist: {}", install_dir.display());
            }
            let ini = install_dir.join("ModOrganizer.ini");
            if !ini.exists() {
                eprintln!(
                    "Warning: {} has no ModOrganizer.ini — Fluorine may not show it",
                    install_dir.display()
                );
            }
            fluorine::register_portable_instance(&install_dir, make_current)?;
            println!(
                "Registered {} as a Fluorine portable instance{}",
                install_dir.display(),
                if make_current { " (current)" } else { "" }
            );
        }

        FluorineAction::Enable { value } => {
            let mut settings = settings::Settings::load();
            settings.add_to_fluorine = value;
            settings.save()?;
            println!(
                "Fluorine auto-register: {}",
                if value { "enabled" } else { "disabled" }
            );
        }

        FluorineAction::Ensure { json } => {
            let install = ensure_fluorine_available().await?;
            if json {
                let payload = serde_json::json!({
                    "binary": install.binary.display().to_string(),
                    "source": install.source,
                });
                println!("{}", serde_json::to_string_pretty(&payload)?);
            } else {
                println!(
                    "Fluorine ready: {} (via {})",
                    install.binary.display(),
                    install.source
                );
            }
        }
    }
    Ok(())
}

/// Detect Fluorine; if missing, download the latest release. Returns the
/// resolved install. Unlike `ensure_fluorine_and_register`, this does NOT
/// touch Fluorine's QSettings file — useful as a "primer" called by external
/// launchers that just want the binary path.
async fn ensure_fluorine_available() -> Result<fluorine::FluorineInstall> {
    let settings = settings::Settings::load();
    let override_path = if settings.fluorine_path.is_empty() {
        None
    } else {
        Some(settings.fluorine_path.as_str())
    };

    if let Some(install) = fluorine::detect(override_path) {
        return Ok(install);
    }
    tracing::info!("Fluorine not detected — downloading the latest release");
    fluorine::download_latest(None).await?;
    fluorine::detect(override_path)
        .ok_or_else(|| anyhow::anyhow!("Downloaded Fluorine but could not detect the binary"))
}

/// Download a .wabbajack file from a URL into the CLF3 cache, returning the
/// resolved local path. Reuses the existing cache file if present.
async fn fetch_wabbajack_from_url(url: &str) -> Result<PathBuf> {
    let cache_dir = dirs::cache_dir()
        .unwrap_or_else(|| PathBuf::from("/tmp"))
        .join("clf3")
        .join("modlists");
    std::fs::create_dir_all(&cache_dir)?;

    // Derive filename from URL path or use a default. Wabbajack authored_files
    // URLs look like ".../Name.wabbajack_<uuid>" — trim the _UUID suffix.
    let filename = url
        .rsplit('/')
        .next()
        .filter(|s| !s.is_empty())
        .map(|s| {
            let decoded = urlencoded_decode(s);
            if let Some(idx) = decoded.find(".wabbajack_") {
                decoded[..idx + ".wabbajack".len()].to_string()
            } else if decoded.ends_with(".wabbajack") {
                decoded
            } else {
                format!("{}.wabbajack", decoded)
            }
        })
        .unwrap_or_else(|| "download.wabbajack".into());
    let dest = cache_dir.join(&filename);

    let cached = std::fs::metadata(&dest).ok().filter(|m| m.len() > 0);
    if let Some(meta) = cached {
        println!(
            "Using cached .wabbajack file: {} ({} MiB)",
            dest.display(),
            meta.len() / (1024 * 1024)
        );
    } else {
        println!("Downloading .wabbajack file from URL...");
        let cdn = downloaders::wabbajack_cdn::WabbajackCdnDownloader::new()?;
        let pb = indicatif::ProgressBar::new(0);
        pb.set_style(
            indicatif::ProgressStyle::default_bar()
                .template("{msg} [{bar:40}] {bytes}/{total_bytes}")
                .expect("valid template")
                .progress_chars("=> "),
        );
        pb.set_message("Downloading");

        let pb_clone = pb.clone();
        cdn.download_with_progress(url, &dest, 0, move |downloaded, total| {
            if pb_clone.length() == Some(0) && total > 0 {
                pb_clone.set_length(total);
            }
            pb_clone.set_position(downloaded);
        })
        .await?;
        pb.finish_with_message("Downloaded");

        println!("Saved to: {}", dest.display());
    }

    Ok(dest)
}

/// Best-effort lookup: did the user pick this `output` directory from the
/// gallery? If exactly one `browser_list_paths` entry has the same
/// canonicalized install_dir, return its machine_name.
fn derive_machine_name_from_settings(
    settings: &settings::Settings,
    output: &std::path::Path,
) -> Option<String> {
    let canonical = output
        .canonicalize()
        .unwrap_or_else(|_| output.to_path_buf());
    let mut matches: Vec<&String> = Vec::new();
    for (machine_name, paths) in &settings.browser_list_paths {
        if paths.install_dir.is_empty() {
            continue;
        }
        let candidate = std::path::Path::new(&paths.install_dir);
        let canon_candidate = candidate
            .canonicalize()
            .unwrap_or_else(|_| candidate.to_path_buf());
        if canon_candidate == canonical {
            matches.push(machine_name);
        }
    }
    if matches.len() == 1 {
        Some(matches[0].clone())
    } else {
        None
    }
}

/// Top-level dispatcher for `clf3 modlist <action>`.
async fn run_modlist_action(action: ModlistAction) -> Result<()> {
    match action {
        ModlistAction::Check { name, json } => run_modlist_check(name, json).await,
        ModlistAction::Update { name, yes } => run_modlist_update(name, yes).await,
    }
}

/// `clf3 modlist check`: report which installs have a newer gallery version.
async fn run_modlist_check(name: Option<String>, json: bool) -> Result<()> {
    let settings = settings::Settings::load();
    let installs = modlist::update::discover_installs(&settings);

    let mut browser = modlist::ModlistBrowser::new()?;
    browser
        .fetch_modlists()
        .await
        .context("Failed to fetch modlist gallery")?;
    let gallery = browser.modlists();

    let filtered: Vec<modlist::update::InstallRecord> = match &name {
        Some(q) => {
            let machine_name = modlist::update::resolve_query(q, gallery, &installs)?;
            installs
                .iter()
                .filter(|i| i.machine_name == machine_name)
                .cloned()
                .collect()
        }
        None => installs.clone(),
    };

    if filtered.is_empty() {
        if let Some(q) = &name {
            anyhow::bail!(
                "No tracked install for '{}'. Try `clf3 install <wabbajack> ...` first.",
                q
            );
        }
    }

    let reports = modlist::update::build_update_reports(&filtered, gallery);

    if json {
        println!("{}", serde_json::to_string_pretty(&reports)?);
    } else {
        print!("{}", modlist::update::format_table(&reports));
    }
    Ok(())
}

/// `clf3 modlist update <name>`: re-install the modlist against the latest
/// gallery version, reusing the original install and downloads directories.
async fn run_modlist_update(name: String, yes: bool) -> Result<()> {
    let settings = settings::Settings::load();
    let installs = modlist::update::discover_installs(&settings);

    let mut browser = modlist::ModlistBrowser::new()?;
    browser
        .fetch_modlists()
        .await
        .context("Failed to fetch modlist gallery")?;
    let gallery = browser.modlists();

    let machine_name = modlist::update::resolve_query(&name, gallery, &installs)?;
    let record = installs
        .iter()
        .find(|r| r.machine_name == machine_name)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Modlist '{}' resolves to '{}' but is not tracked by CLF3. \
                 Run `clf3 install` once to register it.",
                name,
                machine_name
            )
        })?;
    let metadata = gallery
        .iter()
        .find(|m| m.machine_name == machine_name)
        .ok_or_else(|| anyhow::anyhow!("Modlist '{}' is not in the gallery", machine_name))?;

    let install_dir = record
        .install_dir
        .clone()
        .ok_or_else(|| anyhow::anyhow!("No install directory recorded for '{}'", machine_name))?;
    let downloads_dir = record
        .downloads_dir
        .clone()
        .ok_or_else(|| anyhow::anyhow!("No downloads directory recorded for '{}'", machine_name))?;

    if !install_dir.exists() {
        anyhow::bail!(
            "Install directory does not exist: {} (was the drive unmounted?)",
            install_dir.display()
        );
    }

    let cmp = modlist::install_manifest::compare_versions(&record.installed_version, &metadata.version);
    println!(
        "Modlist:  {} ({})",
        if metadata.title.is_empty() {
            machine_name.as_str()
        } else {
            metadata.title.as_str()
        },
        machine_name
    );
    println!(
        "Installed: {}",
        if record.installed_version.is_empty() {
            "<unknown>"
        } else {
            record.installed_version.as_str()
        }
    );
    println!("Latest:    {}", metadata.version);
    println!("Verdict:   {:?}", cmp);
    println!("Install dir:    {}", install_dir.display());
    println!("Downloads dir:  {}", downloads_dir.display());
    println!();

    if matches!(cmp, modlist::VersionCmp::Equal | modlist::VersionCmp::Older) {
        println!(
            "Nothing to do — installed version {} is at or ahead of gallery {}.",
            record.installed_version, metadata.version
        );
        if !yes {
            return Ok(());
        }
        println!("(--yes given — re-running anyway.)");
    }

    if !yes {
        println!(
            "Proceed to re-run the installer against {}? Re-run with --yes to skip this prompt.",
            install_dir.display()
        );
        anyhow::bail!("Update aborted (confirmation required — pass --yes to proceed)");
    }

    let download_url = metadata
        .download_url()
        .ok_or_else(|| anyhow::anyhow!("Gallery entry for '{}' has no download URL", machine_name))?
        .to_string();
    println!("Fetching latest .wabbajack from {} ...", download_url);
    let wabbajack_path = fetch_wabbajack_from_url(&download_url).await?;

    // Resolve API keys + game dir like the normal install path.
    let nexus_key = if settings.nexus_api_key.is_empty() {
        std::env::var("NEXUS_API_KEY").ok()
    } else {
        Some(settings.nexus_api_key.clone())
    }
    .ok_or_else(|| {
        anyhow::anyhow!("Nexus API key required. Set it with: clf3 set-api-key YOUR_KEY")
    })?;

    let game_dir = match auto_detect_game_dir(&wabbajack_path) {
        Some((p, store)) => {
            println!("Auto-detected game directory: {} ({})", p.display(), store);
            p
        }
        None => anyhow::bail!(
            "Could not auto-detect a game directory with matching files. \
             Run `clf3 install <wabbajack> ...` directly with --game to override."
        ),
    };

    let thread_count = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);

    let cli_reporter = CliReporter::new(16, ProgressMode::Plain);

    let patch_cache_dir = if settings.patch_cache_dir.is_empty() {
        None
    } else {
        Some(PathBuf::from(&settings.patch_cache_dir))
    };

    let config = InstallConfig {
        wabbajack_path,
        output_dir: install_dir.clone(),
        downloads_dir: downloads_dir.clone(),
        game_dir,
        nexus_api_key: nexus_key,
        max_concurrent_downloads: thread_count,
        max_install_workers: thread_count,
        max_parallel_bsa_archives: 1,
        max_parallel_7z_archives: thread_count,
        nxm_mode: false,
        browser: "xdg-open".into(),
        patch_cache_dir,
        progress_callback: None,
        reporter: cli_reporter.clone() as Arc<dyn ProgressReporter>,
        loverslab_email: settings.loverslab_email.clone(),
        loverslab_password: settings.loverslab_password.clone(),
        extract_strategy: installer::ExtractStrategy::Streaming,
        machine_name: Some(machine_name.clone()),
        wabbajack_url: Some(download_url),
    };

    let mut installer = Installer::new(config)?;
    let stats = installer.run_pipelined().await?;

    let installation_succeeded = stats.archives_manual == 0
        && stats.archives_failed == 0
        && stats.directives_failed == 0;

    // Mirror the fresh-install summary so the terminal — not just the log
    // file — shows *why* an update failed (which archive, which URL, which
    // error). Without this, the bail! below only carries counts.
    if !stats.manual_downloads.is_empty() {
        println!(
            "\n=== Manual Downloads Needed ({}) ===",
            stats.manual_downloads.len()
        );
        for (i, md) in stats.manual_downloads.iter().enumerate() {
            println!("{}. {}", i + 1, md.name);
            println!("   URL: {}", md.url);
            println!("   Size: {} bytes", md.expected_size);
            if let Some(ref prompt) = md.prompt {
                println!("   Note: {}", prompt);
            }
        }
    }

    if !stats.failed_downloads.is_empty() {
        println!(
            "\n=== Failed Downloads ({}) ===",
            stats.failed_downloads.len()
        );
        for (i, fd) in stats.failed_downloads.iter().enumerate() {
            println!("{}. {}", i + 1, fd.name);
            println!("   URL: {}", fd.url);
            println!("   Error: {}", fd.error);
        }
    }

    if installation_succeeded {
        println!(
            "\nUpdate complete: '{}' is now at version {}.",
            machine_name, metadata.version
        );
    } else {
        anyhow::bail!(
            "Update incomplete ({} manual, {} failed, {} directive failures). \
             Existing manifest left untouched — re-run after fixing issues.",
            stats.archives_manual,
            stats.archives_failed,
            stats.directives_failed
        );
    }
    Ok(())
}

/// Try to auto-detect the game installation directory from the modlist's game type.
///
/// Strategy:
/// 1. Parse modlist header to get `game_type`.
/// 2. Look up that game_type in `KNOWN_GAMES` to get Steam + (optional) GOG IDs.
/// 3. Try Steam install first. If game files hash-match → return (path, "Steam").
/// 4. Fall back to Heroic/GOG if available. Hash-match → return (path, "Heroic/GOG").
/// 5. If any install exists but hashes mismatch, log the diagnostic and keep
///    trying the next candidate.
/// 6. Last resort: return the first install that *exists* even if hashes can't
///    be checked (e.g. modlist has no GameFileSource entries at all — common
///    for Cyberpunk/Witcher3/BG3). Prefer Steam in this case.
///
/// Returns `(install_path, store_label)` for display.
fn auto_detect_game_dir(wabbajack_path: &std::path::Path) -> Option<(PathBuf, &'static str)> {
    use installer::game_preflight::check_game_files_from_modlist;

    let modlist = modlist::parse_wabbajack_file(wabbajack_path).ok()?;

    // Collect every KnownGame entry that maps to this Wabbajack game_type —
    // canonical + store variants (e.g. Fallout 3 + Fallout 3 GOTY share the
    // Bethesda registry path but have distinct Steam app IDs).
    let variants = game_finder::variants_for_wabbajack_type(&modlist.game_type);
    if variants.is_empty() {
        tracing::warn!(
            "Unknown Wabbajack game_type='{}'; can't auto-detect a game directory.",
            modlist.game_type
        );
        return None;
    }

    // Build an ordered candidate list: every Steam variant first (canonical
    // app ID before GOTY/store reissues), then every Heroic/GOG variant.
    let mut candidates: Vec<(PathBuf, &'static str)> = Vec::new();
    let mut tried_steam_ids: Vec<&'static str> = Vec::new();
    let mut tried_gog_ids: Vec<&'static str> = Vec::new();

    for g in &variants {
        tried_steam_ids.push(g.steam_app_id);
        if let Some(p) = game_finder::find_game_install_path(g.steam_app_id) {
            // Distinguish the canonical Steam entry from a store variant in
            // the log so the user can tell which appmanifest matched.
            let label: &'static str = if g.wabbajack_type.is_some() {
                "Steam"
            } else {
                "Steam (variant)"
            };
            candidates.push((p, label));
        }
    }

    let heroic_games = game_finder::detect_heroic_games();
    for g in &variants {
        if let Some(gog_app_id) = g.gog_app_id {
            tried_gog_ids.push(gog_app_id);
            for hg in &heroic_games {
                if hg.app_id == gog_app_id {
                    candidates.push((hg.install_path.clone(), "Heroic/GOG"));
                }
            }
        }
    }

    if candidates.is_empty() {
        tracing::warn!(
            "No installed game directory found for game_type='{}' \
             (tried Steam IDs {:?}, GOG IDs {:?}). \
             Install the game via Steam/Heroic or pass --game PATH.",
            modlist.game_type,
            tried_steam_ids,
            tried_gog_ids
        );
        return None;
    }

    // Hash-verify each candidate against the modlist's GameFileSource entries.
    let mut first_fallback: Option<(PathBuf, &'static str)> = None;

    for (path, store) in &candidates {
        let report = check_game_files_from_modlist(&modlist, path);

        if report.total == 0 {
            // Modlist has no pinned game files — any install for the right
            // game works. Remember the first such candidate (Steam wins due
            // to ordering) and keep scanning in case a later candidate also
            // has no pins; the first one still wins.
            if first_fallback.is_none() {
                first_fallback = Some((path.clone(), store));
            }
            continue;
        }

        if report.all_ok() {
            tracing::info!(
                "Game file preflight passed on {} candidate: {}",
                store,
                path.display()
            );
            println!(
                "Game file preflight passed on {} candidate: {}",
                store,
                path.display()
            );
            return Some((path.clone(), store));
        }

        // Preflight failed on this candidate. Log the per-file detail so the
        // log file shows what's actually wrong (missing files vs. hash drift
        // from a game update) — counts alone aren't actionable.
        tracing::warn!(
            "Skipping {} install at {} — preflight failed ({} missing, {} mismatch of {})",
            store,
            path.display(),
            report.missing().len(),
            report.mismatched().len(),
            report.total
        );
        for line in report.format_summary().lines() {
            tracing::warn!("{}", line);
        }
        eprintln!(
            "Skipping {} install at {}: {} file(s) missing, {} hash mismatch (of {}). \
             Per-file detail logged. Trying next candidate...",
            store,
            path.display(),
            report.missing().len(),
            report.mismatched().len(),
            report.total
        );
    }

    if first_fallback.is_none() {
        tracing::warn!(
            "All candidate game directories failed the game-file preflight for \
             modlist '{}' (game_type='{}'). The game is likely on the wrong version \
             or has files removed — restore vanilla and re-run, or pass --game PATH.",
            modlist.name,
            modlist.game_type
        );
    }

    // All hash-gated candidates failed. Fall back to the first install that
    // had no game files to pin (if any).
    first_fallback
}

/// Simple percent-decoding for URL filenames (e.g. %20 -> space).
fn urlencoded_decode(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.bytes();
    while let Some(b) = chars.next() {
        if b == b'%' {
            let hi = chars.next().and_then(|c| (c as char).to_digit(16));
            let lo = chars.next().and_then(|c| (c as char).to_digit(16));
            if let (Some(h), Some(l)) = (hi, lo) {
                result.push((h * 16 + l) as u8 as char);
            }
        } else {
            result.push(b as char);
        }
    }
    result
}
