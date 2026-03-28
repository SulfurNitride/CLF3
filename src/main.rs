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
mod bsa;
mod downloaders;
mod hash;
mod installer;
mod modlist;
mod nxm_handler;
mod octodiff;
mod paths;
mod textures;

use anyhow::Result;
use clap::{Parser, Subcommand};
use installer::{CliReporter, InstallConfig, Installer, ProgressReporter};
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
    #[command(subcommand)]
    command: Option<Commands>,

    /// Enable verbose logging (use RUST_LOG=debug for more detail)
    #[arg(short, long, global = true)]
    verbose: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Install a Wabbajack modlist
    Install {
        /// Path to the .wabbajack file
        wabbajack_file: PathBuf,

        /// Installation target directory (where mods will be installed)
        #[arg(short, long)]
        output: PathBuf,

        /// Directory for downloaded archives
        #[arg(short, long)]
        downloads: PathBuf,

        /// Game installation directory (for GameFileSource archives)
        #[arg(short, long)]
        game: PathBuf,

        /// Nexus Mods API key (required for download links)
        #[arg(long, env = "NEXUS_API_KEY")]
        nexus_key: String,

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

        /// LoversLab email for automated downloads
        #[arg(long, env = "LOVERSLAB_EMAIL")]
        ll_email: Option<String>,

        /// LoversLab password for automated downloads
        #[arg(long, env = "LOVERSLAB_PASSWORD")]
        ll_password: Option<String>,
    },

    /// Show information about a Wabbajack modlist
    Info {
        /// Path to the .wabbajack file
        wabbajack_file: PathBuf,
    },

    /// Register CLF3 as the system handler for nxm:// links
    NxmRegister,

    /// Handle an nxm:// link (called by the system)
    NxmHandle {
        /// The nxm:// URL to handle
        url: String,
    },

    /// List files inside a BSA archive
    ListBsa {
        /// Path to the BSA file
        bsa_file: PathBuf,
    },

    /// Extract a single file from a BSA archive
    ExtractBsa {
        /// Path to the BSA file
        bsa_file: PathBuf,
        /// Internal path of file to extract
        file_path: String,
        /// Output file path
        output: PathBuf,
    },

    /// List files inside a BA2 archive (Fallout 4/Starfield)
    ListBa2 {
        /// Path to the BA2 file
        ba2_file: PathBuf,
    },

    /// Extract a single file from a BA2 archive (Fallout 4/Starfield)
    ExtractBa2 {
        /// Path to the BA2 file
        ba2_file: PathBuf,
        /// Internal path of file to extract
        file_path: String,
        /// Output file path
        output: PathBuf,
    },

    /// Launch the graphical interface
    Gui {
        /// Nexus Mods API key
        #[arg(long, env = "NEXUS_API_KEY")]
        nexus_key: Option<String>,
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

    // Create CLI reporter for progress bars — must exist before tracing init
    // so its writer factory can route tracing output through MultiProgress.
    // Pool size is generous; unused bars stay hidden.
    let cli_reporter = CliReporter::new(16);

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
    // Without this, panics on worker threads silently kill the process.
    let panic_log = log_path.clone();
    std::panic::set_hook(Box::new(move |info| {
        let thread = std::thread::current();
        let thread_name = thread.name().unwrap_or("<unnamed>");
        let location = info.location().map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
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
            thread_name, location, payload, std::backtrace::Backtrace::force_capture()
        );
        tracing::error!("{}", msg);
        // Also append directly to log file in case tracing is broken
        if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(&panic_log) {
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

            // Register signal handlers via simple flag-setting
            unsafe {
                libc::signal(libc::SIGTERM, sigterm_handler as *const () as libc::sighandler_t);
                libc::signal(libc::SIGHUP, sighup_handler as *const () as libc::sighandler_t);
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
                    if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(&signal_log) {
                        use std::io::Write;
                        let _ = writeln!(f, "\n[SIGNAL] {}", msg);
                    }
                    eprintln!("{}", msg);
                    std::process::exit(143);
                }
                if HUP.load(Ordering::SeqCst) {
                    let msg = "Received SIGHUP — terminal closed or session ended";
                    tracing::error!("{}", msg);
                    if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(&signal_log) {
                        use std::io::Write;
                        let _ = writeln!(f, "\n[SIGNAL] {}", msg);
                    }
                    eprintln!("{}", msg);
                    std::process::exit(129);
                }
            }
        });
    }

    match cli.command {
        None => {
            // No command = launch GUI (double-click behavior)
            println!("Launching CLF3...");
            clf3::gui::run()?;
        }
        Some(Commands::Install {
            wabbajack_file,
            output,
            downloads,
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
        }) => {
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

            let config = InstallConfig {
                wabbajack_path: wabbajack_file,
                output_dir: output,
                downloads_dir: downloads,
                game_dir: game,
                nexus_api_key: nexus_key,
                max_concurrent_downloads: concurrent,
                max_install_workers: install_workers,
                max_parallel_bsa_archives: bsa_workers,
                max_parallel_7z_archives: sevenzip_workers,
                nxm_mode,
                browser,
                patch_cache_dir: None,
                progress_callback: None,
                reporter: cli_reporter.clone() as Arc<dyn ProgressReporter>,
                loverslab_email: ll_email.unwrap_or_default(),
                loverslab_password: ll_password.unwrap_or_default(),
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

            // Show manual download details
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

            // Show failed download details
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

            if stats.archives_manual > 0 || stats.archives_failed > 0 {
                reporter.log("\nSome archives need manual download. Fix issues and run again.");
            } else if stats.directives_failed > 0 {
                reporter.log("\nSome directives failed. Check the log file for details.");
            } else {
                reporter.log("\nInstallation complete!");
            }
        }

        Some(Commands::NxmRegister) => {
            let exe = std::env::current_exe()?.to_string_lossy().to_string();
            nxm_handler::register_handler(&exe)?;
        }

        Some(Commands::NxmHandle { url }) => {
            nxm_handler::send_to_socket(&url)?;
        }

        Some(Commands::ListBsa { bsa_file }) => {
            let files = bsa::list_files(&bsa_file)?;
            for f in &files {
                println!("{}", f.path.to_lowercase());
            }
            eprintln!("\nTotal: {} files", files.len());
        }

        Some(Commands::ExtractBsa {
            bsa_file,
            file_path,
            output,
        }) => {
            let data = bsa::extract_file(&bsa_file, &file_path)?;
            std::fs::write(&output, &data)?;
            println!("Extracted {} bytes to {}", data.len(), output.display());
        }

        Some(Commands::ListBa2 { ba2_file }) => {
            let files = bsa::list_ba2_files(&ba2_file)?;
            for f in &files {
                let tex_marker = if f.is_texture { " [DX10]" } else { "" };
                println!("{}{}", f.path, tex_marker);
            }
            eprintln!("\nTotal: {} files", files.len());
        }

        Some(Commands::ExtractBa2 {
            ba2_file,
            file_path,
            output,
        }) => {
            let data = bsa::extract_ba2_file(&ba2_file, &file_path)?;
            std::fs::write(&output, &data)?;
            println!("Extracted {} bytes to {}", data.len(), output.display());
        }

        Some(Commands::Info { wabbajack_file }) => {
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

            // Count directives by type
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

            // Count download sources
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

        Some(Commands::Gui { nexus_key: _ }) => {
            println!("Launching CLF3 GUI...");
            clf3::gui::run()?;
        }
    }

    Ok(())
}
