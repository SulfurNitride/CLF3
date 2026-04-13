//! egui-based modlist browser GUI.
//!
//! Launched via `clf3 browser`. Shows a scrollable grid of modlist cards with
//! lazy-loaded thumbnail images, search/filter controls, and generates a CLI
//! install command for the user to copy-paste.

use crate::game_finder::{detect_all_games, find_by_gog_id, find_by_steam_id, Launcher};
use crate::modlist::browser::{ModlistBrowser, ModlistMetadata};
use eframe::egui;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// How many images to fetch concurrently during background loading.
const IMAGE_BATCH_SIZE: usize = 12;

/// List row thumbnail size.
const THUMB_WIDTH: f32 = 200.0;
const THUMB_HEIGHT: f32 = 113.0; // ~16:9
const ROW_HEIGHT: f32 = 120.0;

/// Launch the browser GUI window. Blocks until the window is closed.
pub fn launch_browser() -> Result<(), eframe::Error> {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_title("CLF3 — Modlist Browser")
            .with_inner_size([1280.0, 800.0])
            .with_min_inner_size([800.0, 600.0]),
        ..Default::default()
    };

    eframe::run_native(
        "clf3_browser",
        options,
        Box::new(|cc| {
            // Install image loaders so egui_extras can decode PNG/JPEG/etc.
            egui_extras::install_image_loaders(&cc.egui_ctx);
            Ok(Box::new(BrowserApp::new(cc)))
        }),
    )
}

/// Image loading state for a single modlist.
enum ImageState {
    /// Download in flight.
    Loading,
    /// Raw image bytes ready to be turned into a texture.
    Downloaded(Vec<u8>),
    /// Texture uploaded to GPU.
    Texture(egui::TextureHandle),
    /// Failed to load.
    Failed,
}

/// State shared between the main thread and the background image loader.
struct SharedState {
    modlists: Vec<ModlistMetadata>,
    games: Vec<String>,
    /// Keyed by machine_name.
    images: HashMap<String, ImageState>,
    fetch_done: bool,
    fetch_error: Option<String>,
}

struct BrowserApp {
    shared: Arc<Mutex<SharedState>>,
    /// Search query string.
    search: String,
    /// Selected game filter (empty = all).
    game_filter: String,
    /// Show NSFW lists.
    show_nsfw: bool,
    /// Show unavailable lists.
    show_unavailable: bool,
    /// Show only modlists for games the user has installed (Steam or Heroic/GOG).
    show_installed_only: bool,
    /// Set of canonical Wabbajack `GameType` strings the user has installed.
    /// Populated once at startup via `detect_all_games()`. Case-insensitive
    /// match to `ModlistMetadata.game` in `filtered_modlists`.
    installed_game_types: HashSet<String>,
    /// Count of launcher installs detected, shown next to the checkbox.
    installed_game_count: usize,
    /// Currently selected modlist machine_name (if any).
    selected: Option<String>,
    /// A pre-downloaded `.wabbajack` file the user browsed to. When set, the
    /// bottom panel switches into "local file" mode — no modlist metadata,
    /// just directory inputs and an install-command builder pointing at the
    /// on-disk path.
    local_wabbajack: Option<PathBuf>,
    /// User-entered downloads directory.
    downloads_dir: String,
    /// User-entered output/install directory.
    install_dir: String,
    /// The generated command string (shown after selection).
    generated_command: Option<String>,
    /// Whether we've kicked off the initial fetch.
    fetch_started: bool,
    /// tokio runtime for async operations. Held as `Option` so `Drop` can
    /// take ownership and call `shutdown_background()` — otherwise dropping
    /// the runtime while reqwest's connection pool is still tearing down
    /// panics with "Cannot drop a runtime in a context where blocking is
    /// not allowed".
    rt: Option<tokio::runtime::Runtime>,
    /// Image cache directory.
    image_cache_dir: PathBuf,
    /// Whether background image loading has been kicked off.
    image_load_started: bool,
}

impl Drop for BrowserApp {
    fn drop(&mut self) {
        // Hand the runtime off to a background shutdown so in-flight
        // reqwest connections can close cleanly without blocking the
        // main thread's async context.
        if let Some(rt) = self.rt.take() {
            rt.shutdown_background();
        }
    }
}

impl BrowserApp {
    fn new(_cc: &eframe::CreationContext<'_>) -> Self {
        let image_cache_dir = dirs::cache_dir()
            .unwrap_or_else(|| PathBuf::from("/tmp"))
            .join("clf3")
            .join("images");
        let _ = std::fs::create_dir_all(&image_cache_dir);

        // Detect all installed games (Steam, Heroic/GOG, Epic) once at startup.
        // Map each detected Game's app_id back to its canonical Wabbajack
        // `GameType` string via KNOWN_GAMES, so we can filter modlists by
        // "do you own this game in any launcher".
        let detected = detect_all_games();
        let installed_game_count = detected.games.len();
        let mut installed_game_types: HashSet<String> = HashSet::new();
        for game in &detected.games {
            let known = match game.launcher {
                Launcher::Steam { .. } => find_by_steam_id(&game.app_id),
                Launcher::Heroic { .. } => find_by_gog_id(&game.app_id),
            };
            if let Some(k) = known {
                if let Some(wj) = k.wabbajack_type {
                    installed_game_types.insert(wj.to_lowercase());
                }
            }
        }

        Self {
            shared: Arc::new(Mutex::new(SharedState {
                modlists: Vec::new(),
                games: Vec::new(),
                images: HashMap::new(),
                fetch_done: false,
                fetch_error: None,
            })),
            search: String::new(),
            game_filter: String::new(),
            show_nsfw: false,
            show_unavailable: false,
            show_installed_only: false,
            installed_game_types,
            installed_game_count,
            selected: None,
            local_wabbajack: None,
            downloads_dir: String::new(),
            install_dir: String::new(),
            generated_command: None,
            fetch_started: false,
            rt: Some(
                tokio::runtime::Runtime::new().expect("Failed to create tokio runtime"),
            ),
            image_cache_dir,
            image_load_started: false,
        }
    }

    /// Reference to the tokio runtime. Always present while the app is alive;
    /// only taken in `Drop`.
    fn rt(&self) -> &tokio::runtime::Runtime {
        self.rt.as_ref().expect("runtime taken outside Drop")
    }

    /// Kick off async modlist fetch in the background.
    fn start_fetch(&mut self, ctx: &egui::Context) {
        if self.fetch_started {
            return;
        }
        self.fetch_started = true;

        let shared = Arc::clone(&self.shared);
        let ctx = ctx.clone();

        self.rt().spawn(async move {
            let mut browser = match ModlistBrowser::new() {
                Ok(b) => b,
                Err(e) => {
                    let mut state = shared.lock().expect("lock shared state");
                    state.fetch_error = Some(format!("Failed to create browser: {}", e));
                    state.fetch_done = true;
                    ctx.request_repaint();
                    return;
                }
            };

            // Try cache first
            if ModlistBrowser::has_recent_cache() {
                if let Ok(true) = browser.load_cache() {
                    let mut state = shared.lock().expect("lock shared state");
                    state.games = browser.games().into_iter().map(String::from).collect();
                    state.modlists = browser.modlists().to_vec();
                    state.fetch_done = true;
                    ctx.request_repaint();
                    return;
                }
            }

            // Fetch from network
            match browser.fetch_modlists().await {
                Ok(_) => {
                    let _ = browser.save_cache();
                    let mut state = shared.lock().expect("lock shared state");
                    state.games = browser.games().into_iter().map(String::from).collect();
                    state.modlists = browser.modlists().to_vec();
                    state.fetch_done = true;
                }
                Err(e) => {
                    let mut state = shared.lock().expect("lock shared state");
                    state.fetch_error = Some(format!("Failed to fetch modlists: {}", e));
                    state.fetch_done = true;
                }
            }
            ctx.request_repaint();
        });
    }

    /// Kick off background image downloads for all modlists that have image URLs.
    fn start_image_loading(&mut self, ctx: &egui::Context) {
        if self.image_load_started {
            return;
        }
        self.image_load_started = true;

        let shared = Arc::clone(&self.shared);
        let ctx = ctx.clone();
        let cache_dir = self.image_cache_dir.clone();

        // Collect what needs loading.
        let to_load: Vec<(String, String)> = {
            let mut state = shared.lock().expect("lock shared state");
            let items: Vec<(String, String)> = state
                .modlists
                .iter()
                .filter_map(|m| {
                    let key = m.machine_name.clone();
                    if key.is_empty() {
                        return None;
                    }
                    let url = m.image_url()?.to_string();
                    if url.is_empty() {
                        return None;
                    }
                    Some((key, url))
                })
                .collect();
            for (key, _) in &items {
                state.images.insert(key.clone(), ImageState::Loading);
            }
            items
        };

        self.rt().spawn(async move {
            let client = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(15))
                .build()
                .unwrap();

            // Process in batches.
            for batch in to_load.chunks(IMAGE_BATCH_SIZE) {
                let mut handles = Vec::with_capacity(batch.len());

                for (key, url) in batch {
                    let client = client.clone();
                    let key = key.clone();
                    let url = url.clone();
                    let cache_dir = cache_dir.clone();

                    handles.push(tokio::spawn(async move {
                        // Check disk cache first.
                        let cached_path = cache_dir.join(&key);
                        if cached_path.exists() {
                            if let Ok(bytes) = std::fs::read(&cached_path) {
                                if !bytes.is_empty() {
                                    return (key, Ok(bytes));
                                }
                            }
                        }

                        // Download.
                        match client.get(&url).send().await {
                            Ok(resp) if resp.status().is_success() => {
                                match resp.bytes().await {
                                    Ok(bytes) if !bytes.is_empty() => {
                                        let _ = std::fs::write(&cached_path, &bytes);
                                        (key, Ok(bytes.to_vec()))
                                    }
                                    Ok(_) => (key, Err("Empty response".to_string())),
                                    Err(e) => (key, Err(e.to_string())),
                                }
                            }
                            Ok(resp) => (key, Err(format!("HTTP {}", resp.status()))),
                            Err(e) => (key, Err(e.to_string())),
                        }
                    }));
                }

                // Collect batch results.
                for handle in handles {
                    if let Ok((key, result)) = handle.await {
                        let mut state = shared.lock().expect("lock shared state");
                        match result {
                            Ok(bytes) => {
                                state.images.insert(key, ImageState::Downloaded(bytes));
                            }
                            Err(_) => {
                                state.images.insert(key, ImageState::Failed);
                            }
                        }
                    }
                }

                ctx.request_repaint();
            }
        });
    }

    /// Build the filtered list of modlists to display.
    fn filtered_modlists(&self) -> Vec<ModlistMetadata> {
        let state = self.shared.lock().expect("lock shared state");
        state
            .modlists
            .iter()
            .filter(|m| {
                if !self.show_unavailable && !m.is_available() {
                    return false;
                }
                if !self.show_nsfw && m.nsfw {
                    return false;
                }
                if self.show_installed_only
                    && !self.installed_game_types.contains(&m.game.to_lowercase())
                {
                    return false;
                }
                if !self.game_filter.is_empty() {
                    // Compare by formatted name so different raw casings match.
                    let selected = Self::format_game_name(&self.game_filter);
                    let this_game = Self::format_game_name(&m.game);
                    if selected != this_game {
                        return false;
                    }
                }
                if !self.search.is_empty() && !m.matches_query(&self.search) {
                    return false;
                }
                true
            })
            .cloned()
            .collect()
    }

    /// Format bytes into a human-readable string.
    fn format_size(bytes: u64) -> String {
        if bytes == 0 {
            return "—".into();
        }
        const GB: u64 = 1_073_741_824;
        const MB: u64 = 1_048_576;
        if bytes >= GB {
            format!("{:.1} GB", bytes as f64 / GB as f64)
        } else {
            format!("{:.0} MB", bytes as f64 / MB as f64)
        }
    }

    /// Format game name for display.
    fn format_game_name(game: &str) -> String {
        match game.to_lowercase().as_str() {
            "skyrimspecialedition" => "Skyrim SE".into(),
            "skyrim" => "Skyrim LE".into(),
            "skyrimvr" => "Skyrim VR".into(),
            "fallout4" => "Fallout 4".into(),
            "falloutnewvegas" => "Fallout NV".into(),
            "fallout3" => "Fallout 3".into(),
            "oblivion" => "Oblivion".into(),
            "oblivionremastered" => "Oblivion Remastered".into(),
            "morrowind" => "Morrowind".into(),
            "enderal" | "enderalspecialedition" => "Enderal".into(),
            "starfield" => "Starfield".into(),
            "nierautomata" => "NieR: Automata".into(),
            "mountandblade2bannerlord" => "Mount & Blade II".into(),
            "nomanssky" => "No Man's Sky".into(),
            "sevendaystodie" => "7 Days to Die".into(),
            "stardewvalley" => "Stardew Valley".into(),
            "cyberpunk2077" => "Cyberpunk 2077".into(),
            "dragonage" | "dragonageorigins" => "Dragon Age".into(),
            "baldursgate3" => "Baldur's Gate 3".into(),
            "dishonored" => "Dishonored".into(),
            "dragonsdogma" | "dragonsdogma2" => "Dragon's Dogma".into(),
            "fallout4vr" => "Fallout 4 VR".into(),
            "vtmb" => "Vampire: The Masquerade".into(),
            "witcher3" => "The Witcher 3".into(),
            _ => game.to_string(),
        }
    }

    /// Generate the CLI command for the selected modlist.
    fn generate_command(&self, modlist: &ModlistMetadata) -> Option<String> {
        let url = modlist.download_url()?;
        self.build_install_command(url)
    }

    /// Generate the CLI command for a local .wabbajack path.
    fn generate_command_for_local(&self, path: &Path) -> Option<String> {
        self.build_install_command(&path.display().to_string())
    }

    /// Shared command builder — both modlist-URL and local-path callers
    /// produce the same `clf3 install <src> <downloads> <install>` shape.
    fn build_install_command(&self, source: &str) -> Option<String> {
        if self.downloads_dir.is_empty() || self.install_dir.is_empty() {
            return None;
        }
        let exe = std::env::current_exe()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|_| "clf3".into());
        Some(format!(
            "\"{}\" install \"{}\" \"{}\" \"{}\"",
            exe, source, self.downloads_dir, self.install_dir
        ))
    }
}

impl eframe::App for BrowserApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // Kick off fetch on first frame.
        self.start_fetch(ctx);

        // Once modlists are loaded, start image loading.
        {
            let state = self.shared.lock().expect("lock shared state");
            if state.fetch_done && !state.modlists.is_empty() && !self.image_load_started {
                drop(state);
                self.start_image_loading(ctx);
            }
        }

        // Top panel: search and filters.
        egui::TopBottomPanel::top("top_panel").show(ctx, |ui| {
            ui.add_space(8.0);
            ui.horizontal(|ui| {
                ui.label("Search:");
                ui.add(egui::TextEdit::singleline(&mut self.search).desired_width(300.0));

                ui.separator();

                ui.label("Game:");
                let mut games: Vec<(String, String)> = {
                    let state = self.shared.lock().expect("lock shared state");
                    state
                        .games
                        .iter()
                        .filter(|g| {
                            // When the installed-only filter is on, hide games
                            // from the dropdown that the user doesn't actually
                            // own — dropdown and list view stay in sync.
                            if self.show_installed_only {
                                self.installed_game_types.contains(&g.to_lowercase())
                            } else {
                                true
                            }
                        })
                        .map(|g| (g.clone(), Self::format_game_name(g)))
                        .collect()
                };
                games.sort_by(|a, b| a.1.cmp(&b.1));
                games.dedup_by(|a, b| a.1 == b.1);

                // If the current selection is filtered out, reset it so the
                // combobox doesn't display a stale label.
                if !self.game_filter.is_empty()
                    && !games.iter().any(|(raw, _)| raw == &self.game_filter)
                {
                    self.game_filter.clear();
                }

                let selected_text = if self.game_filter.is_empty() {
                    "All Games".to_string()
                } else {
                    Self::format_game_name(&self.game_filter)
                };
                egui::ComboBox::from_id_salt("game_filter")
                    .selected_text(selected_text)
                    .show_ui(ui, |ui| {
                        ui.selectable_value(&mut self.game_filter, String::new(), "All Games");
                        for (raw, display) in &games {
                            ui.selectable_value(
                                &mut self.game_filter,
                                raw.clone(),
                                display.as_str(),
                            );
                        }
                    });

                ui.separator();
                ui.checkbox(&mut self.show_nsfw, "NSFW");
                ui.checkbox(&mut self.show_unavailable, "Unavailable");

                // "Installed only" is disabled when we didn't detect any
                // supported launcher installs — keeps the hint visible to the
                // user but prevents toggling into an empty list.
                let installed_label = if self.installed_game_count > 0 {
                    format!("Installed only ({} games)", self.installed_game_types.len())
                } else {
                    "Installed only (no games detected)".to_string()
                };
                ui.add_enabled(
                    !self.installed_game_types.is_empty(),
                    egui::Checkbox::new(&mut self.show_installed_only, installed_label),
                );

                ui.separator();

                // Let the user skip the browser entirely and point at an
                // already-downloaded `.wabbajack` file on disk.
                if ui.button("Open .wabbajack file...").clicked() {
                    if let Some(path) = rfd::FileDialog::new()
                        .add_filter("Wabbajack modlist", &["wabbajack"])
                        .pick_file()
                    {
                        self.local_wabbajack = Some(path);
                        // Picking a local file clears any network selection so
                        // the bottom panel doesn't show two competing titles.
                        self.selected = None;
                        self.generated_command = None;
                    }
                }
            });
            ui.add_space(4.0);
        });

        // Bottom panel: selected modlist details + command generation.
        // Shows for either a modlist picked from the grid OR a local
        // .wabbajack file opened via the top-panel button.
        if self.selected.is_some() || self.local_wabbajack.is_some() {
            egui::TopBottomPanel::bottom("bottom_panel")
                .min_height(120.0)
                .show(ctx, |ui| {
                    ui.add_space(8.0);

                    // Resolve the selected modlist once (needed if we're in
                    // modlist-selection mode).
                    let selected_modlist = self.selected.as_ref().and_then(|name| {
                        let state = self.shared.lock().expect("lock shared state");
                        state
                            .modlists
                            .iter()
                            .find(|m| &m.machine_name == name)
                            .cloned()
                    });

                    // Header row — title/author/game or local filename
                    ui.horizontal(|ui| {
                        if let Some(modlist) = &selected_modlist {
                            ui.heading(&modlist.title);
                            ui.label(format!("by {}", modlist.author));
                            ui.label(format!("({})", Self::format_game_name(&modlist.game)));
                        } else if let Some(path) = &self.local_wabbajack {
                            let filename = path
                                .file_name()
                                .map(|n| n.to_string_lossy().into_owned())
                                .unwrap_or_else(|| path.display().to_string());
                            ui.heading(filename);
                            ui.label("Local .wabbajack file");
                        }

                        if ui.button("X Close").clicked() {
                            self.selected = None;
                            self.local_wabbajack = None;
                            self.generated_command = None;
                        }
                    });

                    if let Some(path) = &self.local_wabbajack {
                        ui.label(
                            egui::RichText::new(path.display().to_string())
                                .size(11.0)
                                .color(egui::Color32::from_gray(160)),
                        );
                    }

                    ui.add_space(4.0);

                    ui.horizontal(|ui| {
                        ui.label("Downloads dir:");
                        ui.add(
                            egui::TextEdit::singleline(&mut self.downloads_dir)
                                .desired_width(300.0),
                        );
                        if ui.button("Browse...").clicked() {
                            if let Some(path) = rfd::FileDialog::new().pick_folder() {
                                self.downloads_dir = path.display().to_string();
                            }
                        }

                        ui.separator();

                        ui.label("Install dir:");
                        ui.add(
                            egui::TextEdit::singleline(&mut self.install_dir)
                                .desired_width(300.0),
                        );
                        if ui.button("Browse...").clicked() {
                            if let Some(path) = rfd::FileDialog::new().pick_folder() {
                                self.install_dir = path.display().to_string();
                            }
                        }
                    });

                    ui.add_space(4.0);

                    // Build the install command from whichever source is set.
                    // Local file takes precedence over an online selection
                    // because picking a file clears `selected`.
                    let generated = if let Some(path) = self.local_wabbajack.clone() {
                        self.generate_command_for_local(&path)
                    } else if let Some(modlist) = &selected_modlist {
                        self.generate_command(modlist)
                    } else {
                        None
                    };
                    if let Some(cmd) = generated {
                        self.generated_command = Some(cmd);
                    }

                    if let Some(ref cmd) = self.generated_command {
                        ui.horizontal(|ui| {
                            ui.label("Command:");
                            let response = ui.add(
                                egui::TextEdit::singleline(&mut cmd.clone())
                                    .desired_width(ui.available_width() - 80.0)
                                    .font(egui::TextStyle::Monospace),
                            );
                            if ui.button("Copy").clicked() {
                                ui.ctx().copy_text(cmd.clone());
                            }
                            // Select all on click for easy manual copy.
                            if response.clicked() {
                                ui.ctx().copy_text(cmd.clone());
                            }
                        });
                    } else if !self.downloads_dir.is_empty() || !self.install_dir.is_empty() {
                        ui.label("Fill in both directories to generate the install command.");
                    } else {
                        ui.label(
                            "Enter your downloads and install directories to generate the command.",
                        );
                    }

                    ui.add_space(4.0);
                });
        }

        // Convert any Downloaded images to GPU textures.
        {
            let mut state = self.shared.lock().expect("lock shared state");
            let keys_to_convert: Vec<String> = state
                .images
                .iter()
                .filter_map(|(k, v)| {
                    if matches!(v, ImageState::Downloaded(_)) {
                        Some(k.clone())
                    } else {
                        None
                    }
                })
                .collect();

            for key in keys_to_convert {
                if let Some(ImageState::Downloaded(bytes)) = state.images.remove(&key) {
                    match image::load_from_memory(&bytes) {
                        Ok(img) => {
                            let rgba = img.to_rgba8();
                            let size = [rgba.width() as usize, rgba.height() as usize];
                            let pixels = rgba.into_raw();
                            let color_image = egui::ColorImage::from_rgba_unmultiplied(size, &pixels);
                            let texture = ctx.load_texture(
                                &key,
                                color_image,
                                egui::TextureOptions::LINEAR,
                            );
                            state.images.insert(key, ImageState::Texture(texture));
                        }
                        Err(_) => {
                            state.images.insert(key, ImageState::Failed);
                        }
                    }
                }
            }
        }

        // Central panel: modlist grid.
        egui::CentralPanel::default().show(ctx, |ui| {
            let fetch_done = {
                let state = self.shared.lock().expect("lock shared state");
                if let Some(ref err) = state.fetch_error {
                    ui.colored_label(egui::Color32::RED, format!("Error: {}", err));
                    return;
                }
                state.fetch_done
            };

            if !fetch_done {
                ui.centered_and_justified(|ui| {
                    ui.spinner();
                });
                return;
            }

            let filtered = self.filtered_modlists();
            ui.label(format!("{} modlists", filtered.len()));
            ui.add_space(4.0);

            egui::ScrollArea::vertical()
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    for modlist in &filtered {
                        self.render_row(ui, modlist);
                        ui.add_space(4.0);
                    }
                });
        });

        // Check for pending card selection (set by render_card via ctx memory).
        let pending: Option<String> = ctx.memory_mut(|mem| {
            mem.data.get_temp(egui::Id::new("pending_selection"))
        });
        if let Some(name) = pending {
            ctx.memory_mut(|mem| {
                mem.data
                    .remove::<String>(egui::Id::new("pending_selection"));
            });
            self.selected = Some(name);
            self.generated_command = None;
        }
    }
}

impl BrowserApp {
    fn render_row(&self, ui: &mut egui::Ui, modlist: &ModlistMetadata) {
        let is_selected = self
            .selected
            .as_ref()
            .map(|s| s == &modlist.machine_name)
            .unwrap_or(false);

        let stroke = if is_selected {
            egui::Stroke::new(2.0, egui::Color32::from_rgb(100, 149, 237))
        } else {
            egui::Stroke::new(1.0, egui::Color32::from_gray(60))
        };

        egui::Frame::NONE
            .fill(egui::Color32::from_gray(30))
            .stroke(stroke)
            .corner_radius(6)
            .inner_margin(8)
            .show(ui, |ui| {
                ui.set_width(ui.available_width());
                ui.set_min_height(ROW_HEIGHT);

                ui.horizontal(|ui| {
                    // Thumbnail on the left.
                    let texture: Option<egui::TextureHandle> = {
                        let state = self.shared.lock().expect("lock shared state");
                        match state.images.get(&modlist.machine_name) {
                            Some(ImageState::Texture(tex)) => Some(tex.clone()),
                            _ => None,
                        }
                    };
                    let is_loading = {
                        let state = self.shared.lock().expect("lock shared state");
                        matches!(
                            state.images.get(&modlist.machine_name),
                            Some(ImageState::Loading) | Some(ImageState::Downloaded(_)) | None
                        )
                    };

                    let (_, thumb_rect) =
                        ui.allocate_space(egui::vec2(THUMB_WIDTH, THUMB_HEIGHT));

                    if let Some(tex) = texture {
                        let img = egui::Image::new(&tex)
                            .fit_to_exact_size(egui::vec2(THUMB_WIDTH, THUMB_HEIGHT))
                            .corner_radius(4);
                        ui.put(thumb_rect, img);
                    } else if is_loading {
                        ui.painter().rect_filled(
                            thumb_rect,
                            4,
                            egui::Color32::from_gray(45),
                        );
                        ui.put(thumb_rect, egui::Spinner::new());
                    } else {
                        // Failed.
                        ui.painter().rect_filled(
                            thumb_rect,
                            4,
                            egui::Color32::from_gray(35),
                        );
                        ui.painter().text(
                            thumb_rect.center(),
                            egui::Align2::CENTER_CENTER,
                            "No Image",
                            egui::FontId::proportional(12.0),
                            egui::Color32::from_gray(100),
                        );
                    }

                    ui.add_space(12.0);

                    // Text content on the right.
                    ui.vertical(|ui| {
                        // Title row with badges.
                        ui.horizontal(|ui| {
                            ui.label(
                                egui::RichText::new(&modlist.title)
                                    .strong()
                                    .size(16.0),
                            );
                            if modlist.nsfw {
                                ui.label(
                                    egui::RichText::new("NSFW")
                                        .size(11.0)
                                        .color(egui::Color32::from_rgb(220, 50, 50)),
                                );
                            }
                            if modlist.official {
                                ui.label(
                                    egui::RichText::new("Official")
                                        .size(11.0)
                                        .color(egui::Color32::from_rgb(50, 180, 50)),
                                );
                            }
                        });

                        // Author | Game.
                        ui.label(
                            egui::RichText::new(format!(
                                "{} | {}",
                                modlist.author,
                                Self::format_game_name(&modlist.game)
                            ))
                            .size(12.0)
                            .color(egui::Color32::from_gray(160)),
                        );

                        ui.add_space(2.0);

                        // Description.
                        let desc = if modlist.description.chars().count() > 200 {
                            let truncated: String =
                                modlist.description.chars().take(200).collect();
                            format!("{}...", truncated)
                        } else {
                            modlist.description.clone()
                        };
                        ui.label(
                            egui::RichText::new(desc)
                                .size(12.0)
                                .color(egui::Color32::from_gray(180)),
                        );

                        ui.add_space(2.0);

                        // Sizes + select button.
                        ui.horizontal(|ui| {
                            ui.label(
                                egui::RichText::new(format!(
                                    "Download: {}  |  Install: {}",
                                    Self::format_size(modlist.download_size()),
                                    Self::format_size(modlist.installed_size()),
                                ))
                                .size(11.0)
                                .color(egui::Color32::from_gray(120)),
                            );

                            ui.add_space(16.0);

                            if modlist.is_available()
                                && ui
                                    .button(if is_selected {
                                        "Selected"
                                    } else {
                                        "Select"
                                    })
                                    .clicked()
                            {
                                ui.ctx().memory_mut(|mem| {
                                    mem.data.insert_temp(
                                        egui::Id::new("pending_selection"),
                                        modlist.machine_name.clone(),
                                    );
                                });
                            }
                        });
                    });
                });
            });
    }
}
