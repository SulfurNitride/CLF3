//! CLF3 - Wabbajack Modlist Installer
//!
//! Named after Chlorine Trifluoride - burns through modlists
//! like CLF3 burns through concrete.

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
use installer::{InstallConfig, Installer};
use std::path::PathBuf;
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

        /// Use NXM browser mode instead of direct API (avoids rate limits)
        #[arg(long)]
        nxm_mode: bool,

        /// Browser command to open Nexus pages (default: xdg-open)
        #[arg(long, default_value = "xdg-open")]
        browser: String,
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

    // Console layer (only if verbose or RUST_LOG is set)
    if cli.verbose || std::env::var("RUST_LOG").is_ok() {
        let console_layer = tracing_subscriber::fmt::layer().with_filter(console_filter);

        tracing_subscriber::registry()
            .with(file_layer)
            .with(console_layer)
            .init();
    } else {
        tracing_subscriber::registry().with(file_layer).init();
    }

    tracing::info!(
        "CLF3 started, logging to {}",
        log_dir.join(&log_filename).display()
    );

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
            nxm_mode,
            browser,
        }) => {
            // Default to CPU thread count
            let thread_count = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4);
            let concurrent = concurrent.unwrap_or(thread_count);

            println!("CLF3 - Wabbajack Modlist Installer");
            println!("Concurrent downloads: {}", concurrent);
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
                nxm_mode,
                browser,
                progress_callback: None, // CLI doesn't need progress callback
            };

            let mut installer = Installer::new(config)?;
            // Use streaming pipeline with 8 extraction + 8 mover workers
            // Processes archives in batches: ZIP (fastest) -> RAR -> 7z (slowest)
            let stats = installer.run_streaming(8, 8).await?;

            let total_processed =
                stats.directives_completed + stats.directives_skipped + stats.directives_failed;
            println!("\n=== Installation Summary ===");
            println!(
                "Downloads:  {} downloaded, {} skipped, {} manual, {} failed",
                stats.archives_downloaded,
                stats.archives_skipped,
                stats.archives_manual,
                stats.archives_failed
            );
            println!(
                "Directives: {} new, {} existing, {} failed ({} total)",
                stats.directives_completed,
                stats.directives_skipped,
                stats.directives_failed,
                total_processed
            );

            if stats.archives_manual > 0 || stats.archives_failed > 0 {
                println!("\nSome archives need manual download. Fix issues and run again.");
            } else if stats.directives_failed > 0 {
                println!("\nSome directives failed. Check logs and run again.");
            } else {
                println!("\nInstallation complete!");
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
