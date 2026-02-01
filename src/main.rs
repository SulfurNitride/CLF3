//! CLF3 - Wabbajack Modlist Installer
//!
//! Named after Chlorine Trifluoride - burns through modlists
//! like CLF3 burns through concrete.

mod archive;
mod bsa;
mod downloaders;
mod file_router;
mod games;
mod installer;
mod loot;
mod mo2;
mod modlist;
mod nxm_handler;
mod octodiff;
mod paths;
mod textures;

use anyhow::Result;
use clap::{Parser, Subcommand};
use installer::{InstallConfig, Installer};
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "clf3")]
#[command(author = "CLF3 Team")]
#[command(version)]
#[command(about = "Wabbajack modlist installer - burns through modlists like CLF3 burns through concrete")]
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

        /// Port for NXM handler server (default: 8007)
        #[arg(long, default_value = "8007")]
        nxm_port: u16,

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
    NxmRegister {
        /// Port for NXM handler server
        #[arg(long, default_value = "8007")]
        port: u16,
    },

    /// Handle an nxm:// link (called by the system)
    NxmHandle {
        /// The nxm:// URL to handle
        url: String,

        /// Port to send the link to
        #[arg(long, default_value = "8007")]
        port: u16,
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

    // Only initialize logging if verbose or RUST_LOG is set
    if cli.verbose || std::env::var("RUST_LOG").is_ok() {
        tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::from_default_env()
                    .add_directive(if cli.verbose { "clf3=debug".parse()? } else { "clf3=warn".parse()? }),
            )
            .init();
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
            nxm_mode,
            nxm_port,
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
                nxm_port,
                browser,
                progress_callback: None, // CLI doesn't need progress callback
            };

            let mut installer = Installer::new(config)?;
            // Use streaming pipeline with 8 extraction + 8 mover workers
            // Processes archives in batches: ZIP (fastest) -> RAR -> 7z (slowest)
            let stats = installer.run_streaming(8, 8).await?;

            println!("\n=== Installation Summary ===");
            println!("Downloads:  {} downloaded, {} skipped, {} manual, {} failed",
                stats.archives_downloaded, stats.archives_skipped, stats.archives_manual, stats.archives_failed);
            println!("Directives: {} completed, {} failed",
                stats.directives_completed, stats.directives_failed);

            if stats.archives_manual > 0 || stats.archives_failed > 0 {
                println!("\nSome archives need manual download. Fix issues and run again.");
            } else if stats.directives_failed > 0 {
                println!("\nSome directives failed. Check logs and run again.");
            } else {
                println!("\nInstallation complete!");
            }
        }

        Some(Commands::NxmRegister { port }) => {
            let exe = std::env::current_exe()?.to_string_lossy().to_string();
            nxm_handler::register_handler(&exe, port)?;
        }

        Some(Commands::NxmHandle { url, port }) => {
            nxm_handler::send_to_server(port, &url).await?;
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

        Some(Commands::Info { wabbajack_file }) => {
            println!("Parsing {}...\n", wabbajack_file.display());

            let modlist = modlist::parse_wabbajack_file(&wabbajack_file)?;

            println!("=== Modlist Information ===");
            println!("Name:              {}", modlist.name);
            println!("Author:            {}", modlist.author);
            println!("Version:           {}", modlist.version);
            println!("Game:              {}", modlist.game_type);
            println!("Wabbajack Version: {}", modlist.wabbajack_version);
            println!("NSFW:              {}", if modlist.is_nsfw { "Yes" } else { "No" });
            println!();
            println!("Archives:          {}", modlist.archives.len());
            println!("Directives:        {}", modlist.directives.len());

            // Count directives by type
            let mut type_counts = std::collections::HashMap::new();
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
            let mut source_counts = std::collections::HashMap::new();
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
