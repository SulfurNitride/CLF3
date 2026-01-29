//! CLF3 - Wabbajack Modlist Installer
//!
//! Named after Chlorine Trifluoride - burns through modlists
//! like CLF3 burns through concrete.

mod bsa;
mod collection;
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
    command: Commands,

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

    /// Install a Nexus Collection
    Collection {
        /// Collection URL or path to collection.json file
        /// URL format: https://www.nexusmods.com/skyrimspecialedition/collections/slug
        collection: String,

        /// Installation target directory (where MO2 instance will be created)
        #[arg(short, long)]
        output: PathBuf,

        /// Game installation directory
        #[arg(short, long)]
        game: PathBuf,

        /// Nexus Mods API key (required for download links)
        #[arg(long, env = "NEXUS_API_KEY")]
        nexus_key: String,
    },

    /// Show information about a Nexus Collection
    CollectionInfo {
        /// Collection URL or path to collection.json file
        collection: String,

        /// Nexus Mods API key (required if using URL)
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
        Commands::Install {
            wabbajack_file,
            output,
            downloads,
            game,
            nexus_key,
            concurrent,
            nxm_mode,
            nxm_port,
            browser,
        } => {
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
            };

            let mut installer = Installer::new(config)?;
            let stats = installer.run().await?;

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

        Commands::NxmRegister { port } => {
            let exe = std::env::current_exe()?.to_string_lossy().to_string();
            nxm_handler::register_handler(&exe, port)?;
        }

        Commands::NxmHandle { url, port } => {
            nxm_handler::send_to_server(port, &url).await?;
        }

        Commands::ListBsa { bsa_file } => {
            let files = bsa::list_files(&bsa_file)?;
            for f in &files {
                println!("{}", f.path.to_lowercase());
            }
            eprintln!("\nTotal: {} files", files.len());
        }

        Commands::ExtractBsa {
            bsa_file,
            file_path,
            output,
        } => {
            let data = bsa::extract_file(&bsa_file, &file_path)?;
            std::fs::write(&output, &data)?;
            println!("Extracted {} bytes to {}", data.len(), output.display());
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

        Commands::Collection {
            collection,
            output,
            game,
            nexus_key,
        } => {
            // Use CPU thread count for concurrency
            let thread_count = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4);

            println!("CLF3 - Nexus Collection Installer");
            println!("Threads:    {}", thread_count);

            // Resolve collection path - either from URL or local file
            let collection_path = if collection::is_url(&collection) {
                // Parse URL and fetch collection from Nexus
                let url_info = collection::parse_collection_url(&collection)
                    .ok_or_else(|| anyhow::anyhow!("Invalid collection URL format"))?;

                println!("Fetching collection from Nexus...");
                println!("  Game: {}", url_info.game);
                println!("  Slug: {}", url_info.slug);

                // Ensure output dir exists for temp files
                std::fs::create_dir_all(&output)?;

                collection::fetch_collection(&url_info, &nexus_key, &output).await?
            } else {
                PathBuf::from(&collection)
            };

            println!("Collection: {}", collection_path.display());
            println!("Output:     {}", output.display());
            println!("Game:       {}", game.display());
            println!();

            // Downloads always go to output/downloads (protected across installs)
            let downloads_dir = output.join("downloads");

            // Check for existing installation
            if output.exists() {
                let mo2_exe = output.join("ModOrganizer.exe");
                if mo2_exe.exists() {
                    println!("WARNING: Existing MO2 installation detected at {}", output.display());
                    println!("         Downloads folder will be preserved.");
                    println!("         All other files will be overwritten.");
                    println!();
                    print!("Continue? [y/N] ");
                    use std::io::Write;
                    std::io::stdout().flush()?;

                    let mut input = String::new();
                    std::io::stdin().read_line(&mut input)?;
                    if !input.trim().eq_ignore_ascii_case("y") {
                        println!("Aborted.");
                        return Ok(());
                    }

                    // Clean everything except downloads and .collection_temp
                    println!("Cleaning existing installation (preserving downloads)...");
                    for entry in std::fs::read_dir(&output)? {
                        let entry = entry?;
                        let path = entry.path();
                        let name = path.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_default();
                        if name == "downloads" || name == ".collection_temp" {
                            continue; // Protect downloads and fetched collection
                        }
                        if path.is_dir() {
                            std::fs::remove_dir_all(&path)?;
                        } else {
                            std::fs::remove_file(&path)?;
                        }
                    }
                }
            }

            // Create database path (inside output, gets cleaned with reinstall)
            let db_path = output.join("clf3_collection.db");

            // Setup config
            let config = collection::InstallerConfig {
                collection_path,
                output_dir: output.clone(),
                game_path: game,
                game_type: None, // Auto-detect from collection
                nexus_api_key: nexus_key,
                concurrent_downloads: thread_count,
                downloads_dir: Some(downloads_dir),
            };

            // Create installer
            let mut installer = collection::CollectionInstaller::new(config, &db_path)?;

            // Run installation
            installer.install().await?;

            println!("\nCollection installation complete!");
            println!("MO2 instance created at: {}", output.display());
        }

        Commands::CollectionInfo { collection, nexus_key } => {
            // Resolve collection path - either from URL or local file
            let collection_path = if collection::is_url(&collection) {
                // Parse URL and fetch collection from Nexus
                let url_info = collection::parse_collection_url(&collection)
                    .ok_or_else(|| anyhow::anyhow!("Invalid collection URL format"))?;

                let api_key = nexus_key.ok_or_else(|| {
                    anyhow::anyhow!("Nexus API key required when using URL (--nexus-key or NEXUS_API_KEY)")
                })?;

                println!("Fetching collection from Nexus...");
                println!("  Game: {}", url_info.game);
                println!("  Slug: {}", url_info.slug);

                // Use temp dir for fetched collection
                let temp_dir = std::env::temp_dir().join("clf3_collection_info");
                std::fs::create_dir_all(&temp_dir)?;

                collection::fetch_collection(&url_info, &api_key, &temp_dir).await?
            } else {
                PathBuf::from(&collection)
            };

            println!("Parsing {}...\n", collection_path.display());

            let coll = collection::load_collection(&collection_path)?;

            println!("=== Collection Information ===");
            println!("Name:        {}", coll.get_name());
            println!("Author:      {}", coll.get_author());
            println!("Domain:      {}", coll.get_domain_name());
            println!("Version:     {}", coll.version);
            println!();
            println!("Mods:        {}", coll.mods.len());
            println!("Mod Rules:   {}", coll.mod_rules.len());
            println!("Plugins:     {}", coll.plugins.len());

            // Count mods with FOMOD choices
            let fomod_count = coll.mods.iter().filter(|m| m.choices.is_some()).count();
            println!("FOMODs:      {}", fomod_count);

            // Count by phase
            let mut phase_counts: std::collections::HashMap<i32, usize> = std::collections::HashMap::new();
            for m in &coll.mods {
                *phase_counts.entry(m.phase).or_insert(0) += 1;
            }

            if phase_counts.len() > 1 {
                println!("\n=== Mods by Phase ===");
                let mut phases: Vec<_> = phase_counts.into_iter().collect();
                phases.sort_by_key(|(phase, _)| *phase);
                for (phase, count) in phases {
                    println!("  Phase {}: {} mods", phase, count);
                }
            }

            // Count optional mods
            let optional_count = coll.mods.iter().filter(|m| m.optional).count();
            if optional_count > 0 {
                println!("\nOptional:    {} mods", optional_count);
            }

            // Total download size
            let total_size: i64 = coll.mods.iter().map(|m| m.source.file_size).sum();
            println!("\nTotal size:  {:.2} GB", total_size as f64 / 1024.0 / 1024.0 / 1024.0);
        }

    }

    Ok(())
}
