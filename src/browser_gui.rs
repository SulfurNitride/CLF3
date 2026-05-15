//! egui-based modlist browser GUI.
//!
//! Launched via `clf3 browser`. Shows a scrollable grid of modlist cards with
//! lazy-loaded thumbnail images, search/filter controls, and generates a CLI
//! install command for the user to copy-paste.

use crate::collection::gallery::{self, CollectionListing, SortBy as CollectionSortBy};
use crate::game_finder::{detect_all_games, find_by_gog_id, find_by_steam_id, Game, Launcher};
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
            install_emoji_font(&cc.egui_ctx);
            Ok(Box::new(BrowserApp::new(cc)))
        }),
    )
}

/// Bundle OpenMoji-Black (monochrome, CC-BY-SA 4.0) as a fallback so glyphs
/// the default font lacks render instead of showing tofu boxes. OpenMoji
/// covers Unicode 15 (newer than Noto Emoji's discontinued mono variant).
fn install_emoji_font(ctx: &egui::Context) {
    const OPENMOJI: &[u8] = include_bytes!("../assets/OpenMoji-Black.ttf");

    let mut fonts = egui::FontDefinitions::default();
    fonts.font_data.insert(
        "openmoji_black".to_owned(),
        std::sync::Arc::new(egui::FontData::from_static(OPENMOJI)),
    );
    fonts
        .families
        .entry(egui::FontFamily::Proportional)
        .or_default()
        .push("openmoji_black".to_owned());
    fonts
        .families
        .entry(egui::FontFamily::Monospace)
        .or_default()
        .push("openmoji_black".to_owned());
    ctx.set_fonts(fonts);
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

/// State for the Collections gallery (parallel to SharedState for the
/// Wabbajack tab). Shared between the main thread and async fetch tasks.
struct CollectionsState {
    listings: Vec<CollectionListing>,
    /// Keyed by `slug` — image cache for tile thumbnails.
    images: HashMap<String, ImageState>,
    fetch_done: bool,
    fetch_error: Option<String>,
}

/// Top-level browser tabs.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum BrowserTab {
    Wabbajack,
    Collections,
}

struct BrowserApp {
    /// Active top-level tab.
    tab: BrowserTab,
    /// Collection input fields (URL/slug, downloads, output, game).
    collection_url: String,
    collection_downloads: String,
    collection_output: String,
    collection_game: String,
    /// Last generated `clf3 install-collection` command, if any.
    collection_generated_command: Option<String>,

    /// Collections gallery shared state.
    collections_shared: Arc<Mutex<CollectionsState>>,
    /// Game-domain filter for the Collections tab (Nexus URL slug, e.g.
    /// `skyrimspecialedition`). Empty = use the first known game.
    collections_game_filter: String,
    /// Sort selection.
    collections_sort: CollectionSortBy,
    /// Whether we've kicked off the initial gallery fetch.
    collections_fetch_started: bool,
    /// Whether background image loading has been kicked off.
    collections_image_load_started: bool,
    /// Slug of the currently-selected collection (drives "Use this" → form
    /// fields below).
    collections_selected_slug: Option<String>,
    /// All detected games (Steam, Heroic/GOG, etc.) — used for auto-detecting
    /// the game-dir field on the Collections install form.
    detected_games: Vec<Game>,

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
            tab: BrowserTab::Wabbajack,
            collection_url: String::new(),
            collection_downloads: String::new(),
            collection_output: String::new(),
            collection_game: String::new(),
            collection_generated_command: None,
            collections_shared: Arc::new(Mutex::new(CollectionsState {
                listings: Vec::new(),
                images: HashMap::new(),
                fetch_done: false,
                fetch_error: None,
            })),
            collections_game_filter: "skyrimspecialedition".to_string(),
            collections_sort: CollectionSortBy::Endorsements,
            collections_fetch_started: false,
            collections_image_load_started: false,
            collections_selected_slug: None,
            detected_games: detected.games.clone(),
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
            show_unavailable: true,
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
        // Kick off fetch on first frame (Wabbajack tab only — Collection tab
        // is form-driven, no background fetch yet).
        if self.tab == BrowserTab::Wabbajack {
            self.start_fetch(ctx);

            // Once modlists are loaded, start image loading.
            let state = self.shared.lock().expect("lock shared state");
            if state.fetch_done && !state.modlists.is_empty() && !self.image_load_started {
                drop(state);
                self.start_image_loading(ctx);
            }
        }

        // Top tab bar.
        egui::TopBottomPanel::top("tab_bar").show(ctx, |ui| {
            ui.add_space(4.0);
            ui.horizontal(|ui| {
                ui.selectable_value(&mut self.tab, BrowserTab::Wabbajack, "Wabbajack");
                ui.selectable_value(&mut self.tab, BrowserTab::Collections, "Collections");
            });
            ui.add_space(2.0);
        });

        // Collections tab: short-circuit, render its own panel and return.
        if self.tab == BrowserTab::Collections {
            self.render_collections_tab(ctx);
            return;
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

                    if let Some(modlist) = &selected_modlist {
                        if modlist.force_down {
                            let msg = if modlist.download_url().is_some() {
                                "Author marked this modlist DOWN. Some sources may \
                                 fail; install may still succeed if archives are \
                                 already in your downloads folder."
                            } else {
                                "Author marked this modlist DOWN and no download \
                                 link is published. Use the local .wabbajack \
                                 button above if you already have the file."
                            };
                            ui.label(
                                egui::RichText::new(msg)
                                    .size(11.0)
                                    .color(egui::Color32::from_rgb(220, 140, 50)),
                            );
                        }
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

        // Convert Downloaded → GPU textures, but cap per frame so paint
        // stays responsive when 100+ images land at once. PNG decode +
        // GPU upload are both UI-thread-bound and surprisingly expensive.
        const MAX_TEXTURE_UPLOADS_PER_FRAME: usize = 4;
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
                .take(MAX_TEXTURE_UPLOADS_PER_FRAME)
                .collect();
            let still_pending = !keys_to_convert.is_empty();

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
            // Keep painting next frame so remaining Downloaded entries get
            // converted without waiting for an unrelated input event.
            if still_pending {
                ctx.request_repaint();
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
                            if modlist.force_down {
                                ui.label(
                                    egui::RichText::new("DOWN")
                                        .size(11.0)
                                        .color(egui::Color32::from_rgb(220, 140, 50)),
                                )
                                .on_hover_text(
                                    "Author flagged this list as unavailable. \
                                     Install may still work if you already have \
                                     the .wabbajack file or all required archives \
                                     in your downloads folder.",
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

                            if modlist.download_url().is_some()
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

    /// Collections tab — gallery view of Nexus collections, filterable by
    /// game, with lazy-loaded thumbnails. Selecting a card prefills the URL
    /// in the install form below.
    fn render_collections_tab(&mut self, ctx: &egui::Context) {
        // Trigger initial fetch + image loading on first render of this tab.
        self.start_collections_fetch(ctx);
        if self
            .collections_shared
            .lock()
            .map(|s| s.fetch_done && !s.listings.is_empty())
            .unwrap_or(false)
            && !self.collections_image_load_started
        {
            self.start_collections_image_loading(ctx);
        }

        // Convert Downloaded → GPU textures, throttled per frame (see Wabbajack
        // tab). 4/frame keeps the paint loop responsive while 100+ images land.
        {
            const MAX: usize = 4;
            let mut state = self
                .collections_shared
                .lock()
                .expect("collections lock");
            let keys: Vec<String> = state
                .images
                .iter()
                .filter_map(|(k, v)| match v {
                    ImageState::Downloaded(_) => Some(k.clone()),
                    _ => None,
                })
                .take(MAX)
                .collect();
            let still_pending = !keys.is_empty();
            for key in keys {
                if let Some(ImageState::Downloaded(bytes)) = state.images.remove(&key) {
                    match image::load_from_memory(&bytes) {
                        Ok(img) => {
                            let rgba = img.to_rgba8();
                            let size =
                                [rgba.width() as usize, rgba.height() as usize];
                            let pixels = rgba.into_raw();
                            let color_image =
                                egui::ColorImage::from_rgba_unmultiplied(size, &pixels);
                            let texture = ctx.load_texture(
                                &format!("collection_{key}"),
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
            if still_pending {
                ctx.request_repaint();
            }
        }

        // Filter bar.
        egui::TopBottomPanel::top("collections_top").show(ctx, |ui| {
            ui.add_space(6.0);
            ui.horizontal(|ui| {
                ui.label("Game:");
                let games: &[(&str, &str)] = &[
                    ("skyrimspecialedition", "Skyrim Special Edition"),
                    ("skyrim", "Skyrim Legendary Edition"),
                    ("fallout4", "Fallout 4"),
                    ("falloutnewvegas", "Fallout: New Vegas"),
                    ("fallout3", "Fallout 3"),
                    ("oblivion", "Oblivion"),
                    ("starfield", "Starfield"),
                    ("baldursgate3", "Baldur's Gate 3"),
                    ("cyberpunk2077", "Cyberpunk 2077"),
                    ("stardewvalley", "Stardew Valley"),
                ];
                let current_label = games
                    .iter()
                    .find(|(slug, _)| *slug == self.collections_game_filter)
                    .map(|(_, name)| *name)
                    .unwrap_or("Custom");
                let mut new_filter = self.collections_game_filter.clone();
                egui::ComboBox::from_id_salt("collections_game")
                    .selected_text(current_label)
                    .show_ui(ui, |ui| {
                        for (slug, name) in games {
                            ui.selectable_value(&mut new_filter, slug.to_string(), *name);
                        }
                    });
                if new_filter != self.collections_game_filter {
                    self.collections_game_filter = new_filter;
                    self.reset_collections_fetch();
                }

                ui.separator();
                ui.label("Sort:");
                let mut new_sort = self.collections_sort;
                egui::ComboBox::from_id_salt("collections_sort")
                    .selected_text(match self.collections_sort {
                        CollectionSortBy::Endorsements => "Endorsements",
                        CollectionSortBy::Downloads => "Downloads",
                        CollectionSortBy::Recent => "Recent",
                    })
                    .show_ui(ui, |ui| {
                        ui.selectable_value(
                            &mut new_sort,
                            CollectionSortBy::Endorsements,
                            "Endorsements",
                        );
                        ui.selectable_value(
                            &mut new_sort,
                            CollectionSortBy::Downloads,
                            "Downloads",
                        );
                        ui.selectable_value(&mut new_sort, CollectionSortBy::Recent, "Recent");
                    });
                if !sort_eq(new_sort, self.collections_sort) {
                    self.collections_sort = new_sort;
                    self.reset_collections_fetch();
                }

                ui.separator();
                if ui.button("Refresh").clicked() {
                    self.reset_collections_fetch();
                }
            });
            ui.add_space(4.0);
        });

        // Bottom panel: install form (mirrors Wabbajack tab UX).
        egui::TopBottomPanel::bottom("collections_form")
            .min_height(160.0)
            .show(ctx, |ui| {
                ui.add_space(6.0);
                ui.heading("Install");
                ui.add_space(4.0);

                if let Some(slug) = &self.collections_selected_slug {
                    ui.label(
                        egui::RichText::new(format!("Selected: {slug}"))
                            .color(egui::Color32::from_rgb(120, 200, 255)),
                    );
                }

                egui::Grid::new("collection_inputs")
                    .num_columns(2)
                    .spacing([8.0, 6.0])
                    .show(ui, |ui| {
                        ui.label("URL / slug / path:");
                        ui.add(
                            egui::TextEdit::singleline(&mut self.collection_url)
                                .desired_width(640.0)
                                .hint_text("Click a card above, or paste a URL"),
                        );
                        ui.end_row();

                        ui.label("Downloads dir:");
                        ui.horizontal(|ui| {
                            ui.add(
                                egui::TextEdit::singleline(&mut self.collection_downloads)
                                    .desired_width(540.0),
                            );
                            if ui.button("Browse...").clicked() {
                                if let Some(p) = rfd::FileDialog::new().pick_folder() {
                                    self.collection_downloads = p.display().to_string();
                                }
                            }
                        });
                        ui.end_row();

                        ui.label("Output dir:");
                        ui.horizontal(|ui| {
                            ui.add(
                                egui::TextEdit::singleline(&mut self.collection_output)
                                    .desired_width(540.0),
                            );
                            if ui.button("Browse...").clicked() {
                                if let Some(p) = rfd::FileDialog::new().pick_folder() {
                                    self.collection_output = p.display().to_string();
                                }
                            }
                        });
                        ui.end_row();
                    });

                if !self.collection_game.trim().is_empty() {
                    ui.add_space(4.0);
                    ui.label(
                        egui::RichText::new(format!(
                            "Auto-detected game dir for LOOT: {}",
                            self.collection_game
                        ))
                        .size(11.0)
                        .color(egui::Color32::from_gray(150)),
                    );
                }

                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    let can_generate = !self.collection_url.trim().is_empty()
                        && !self.collection_downloads.trim().is_empty()
                        && !self.collection_output.trim().is_empty();
                    if ui
                        .add_enabled(can_generate, egui::Button::new("Generate command"))
                        .clicked()
                    {
                        self.collection_generated_command =
                            Some(self.build_collection_command());
                    }
                    if self.collection_generated_command.is_some()
                        && ui.button("Clear").clicked()
                    {
                        self.collection_generated_command = None;
                    }
                });

                if let Some(cmd) = self.collection_generated_command.clone() {
                    ui.add_space(6.0);
                    ui.label("Run this in your terminal:");
                    let mut display = cmd.clone();
                    ui.add(
                        egui::TextEdit::multiline(&mut display)
                            .desired_rows(2)
                            .desired_width(f32::INFINITY)
                            .font(egui::TextStyle::Monospace),
                    );
                    if ui.button("Copy").clicked() {
                        ctx.copy_text(cmd);
                    }
                }
                ui.add_space(6.0);
            });

        // Central: gallery grid.
        egui::CentralPanel::default().show(ctx, |ui| {
            let (fetch_done, error, count) = {
                let state = self.collections_shared.lock().expect("collections lock");
                (
                    state.fetch_done,
                    state.fetch_error.clone(),
                    state.listings.len(),
                )
            };

            if let Some(err) = error {
                ui.colored_label(egui::Color32::RED, format!("Error: {err}"));
                return;
            }
            if !fetch_done {
                ui.centered_and_justified(|ui| ui.spinner());
                return;
            }
            if count == 0 {
                ui.label("No collections returned for this game.");
                return;
            }

            ui.label(format!("{count} collections"));
            ui.add_space(4.0);

            // Snapshot listings to render without holding the lock during draw.
            let snapshot: Vec<CollectionListing> = self
                .collections_shared
                .lock()
                .map(|s| s.listings.clone())
                .unwrap_or_default();

            egui::ScrollArea::vertical()
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    for c in &snapshot {
                        self.render_collection_row(ui, c);
                        ui.add_space(4.0);
                    }
                });
        });
    }

    fn render_collection_row(&mut self, ui: &mut egui::Ui, c: &CollectionListing) {
        let is_selected = self
            .collections_selected_slug
            .as_deref()
            .map(|s| s == c.slug)
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
                    // Thumbnail.
                    let texture: Option<egui::TextureHandle> = self
                        .collections_shared
                        .lock()
                        .ok()
                        .and_then(|s| match s.images.get(&c.slug) {
                            Some(ImageState::Texture(tex)) => Some(tex.clone()),
                            _ => None,
                        });
                    match texture {
                        Some(tex) => {
                            ui.add(
                                egui::Image::from_texture(&tex)
                                    .fit_to_exact_size(egui::vec2(THUMB_WIDTH, THUMB_HEIGHT)),
                            );
                        }
                        None => {
                            let (rect, _) = ui.allocate_exact_size(
                                egui::vec2(THUMB_WIDTH, THUMB_HEIGHT),
                                egui::Sense::hover(),
                            );
                            ui.painter().rect_filled(
                                rect,
                                4.0,
                                egui::Color32::from_gray(50),
                            );
                            ui.painter().text(
                                rect.center(),
                                egui::Align2::CENTER_CENTER,
                                "(loading)",
                                egui::FontId::proportional(11.0),
                                egui::Color32::from_gray(150),
                            );
                        }
                    }

                    ui.add_space(12.0);

                    // Stack everything in one vertical column to mirror the
                    // Wabbajack tab card layout: title → author|game → summary
                    // → stats row + action button.
                    ui.vertical(|ui| {
                        ui.label(
                            egui::RichText::new(strip_format_chars(&c.name))
                                .strong()
                                .size(15.0),
                        );
                        let subtitle = if c.game_name.is_empty() {
                            format!("by {}", strip_format_chars(&c.author))
                        } else {
                            format!(
                                "by {} | {}",
                                strip_format_chars(&c.author),
                                strip_format_chars(&c.game_name)
                            )
                        };
                        ui.label(
                            egui::RichText::new(subtitle)
                                .size(12.0)
                                .color(egui::Color32::from_gray(170)),
                        );
                        ui.add_space(4.0);
                        let summary = truncate_chars(&strip_format_chars(&c.summary), 240);
                        ui.label(
                            egui::RichText::new(summary)
                                .size(11.0)
                                .color(egui::Color32::from_gray(190)),
                        );
                        ui.add_space(4.0);

                        ui.horizontal(|ui| {
                            let mut parts = vec![format!("rev {}", c.latest_revision)];
                            if c.mod_count > 0 {
                                parts.push(format!("{} mods", c.mod_count));
                            }
                            if c.total_size_bytes > 0 {
                                parts.push(format!("Download: {}", fmt_size(c.total_size_bytes)));
                            }
                            parts.push(format!("{} downloads", fmt_count(c.total_downloads)));
                            parts.push(format!("{} endorsements", fmt_count(c.endorsements)));
                            ui.label(
                                egui::RichText::new(parts.join("  |  "))
                                    .size(11.0)
                                    .color(egui::Color32::from_gray(120)),
                            );
                            ui.add_space(16.0);
                            if ui
                                .button(if is_selected { "Selected" } else { "Use" })
                                .clicked()
                            {
                                self.collections_selected_slug = Some(c.slug.clone());
                                self.collection_url = c.nexus_url();
                                // Auto-fill game dir from detected installs if we
                                // can match the collection's game.
                                if let Some(path) = self
                                    .detect_game_path_for_domain(&c.game_domain)
                                {
                                    self.collection_game = path;
                                }
                            }
                        });
                    });
                });
            });
    }

    /// Look up an install path for a Nexus domain (`skyrimspecialedition`)
    /// among detected Steam/Heroic/etc games, via KnownGames' wabbajack_type
    /// alias. Returns `None` if no match is detected.
    fn detect_game_path_for_domain(&self, domain: &str) -> Option<String> {
        use crate::game_finder::known_games;
        // Map Nexus domain → wabbajack_type by case-insensitive scan of the
        // KNOWN_GAMES table.
        let target_type = known_games::KNOWN_GAMES
            .iter()
            .find(|g| {
                g.wabbajack_type
                    .map(|wj| wj.eq_ignore_ascii_case(&domain.replace(['_', ' '], "")))
                    .unwrap_or(false)
            })
            .or_else(|| {
                // Fall back: the Nexus domain `skyrimspecialedition` collapses
                // to `SkyrimSpecialEdition` after capitalization removal —
                // direct compare against the lowercased wabbajack_type.
                known_games::KNOWN_GAMES.iter().find(|g| {
                    g.wabbajack_type
                        .map(|wj| wj.to_lowercase() == domain.to_lowercase())
                        .unwrap_or(false)
                })
            })?;
        let wj = target_type.wabbajack_type?;
        // Find a detected game with the matching wabbajack_type via
        // find_by_steam_id / find_by_gog_id.
        for g in &self.detected_games {
            let known = match g.launcher {
                Launcher::Steam { .. } => find_by_steam_id(&g.app_id),
                Launcher::Heroic { .. } => find_by_gog_id(&g.app_id),
            };
            if known.and_then(|k| k.wabbajack_type) == Some(wj) {
                return Some(g.install_path.display().to_string());
            }
        }
        None
    }

    /// Reset gallery state and refetch (after game/sort change or refresh).
    fn reset_collections_fetch(&mut self) {
        if let Ok(mut s) = self.collections_shared.lock() {
            s.listings.clear();
            s.images.clear();
            s.fetch_done = false;
            s.fetch_error = None;
        }
        self.collections_fetch_started = false;
        self.collections_image_load_started = false;
    }

    fn start_collections_fetch(&mut self, ctx: &egui::Context) {
        if self.collections_fetch_started {
            return;
        }
        self.collections_fetch_started = true;

        let api_key = crate::settings::Settings::load().nexus_api_key;
        if api_key.is_empty() {
            if let Ok(mut s) = self.collections_shared.lock() {
                s.fetch_error = Some(
                    "No Nexus API key saved. Run `clf3 set-api-key <KEY>` and reopen the browser."
                        .into(),
                );
                s.fetch_done = true;
            }
            return;
        }

        let game = self.collections_game_filter.clone();
        let sort = self.collections_sort;
        let shared = Arc::clone(&self.collections_shared);
        let ctx = ctx.clone();

        self.rt().spawn(async move {
            let game_ref = if game.is_empty() { None } else { Some(game.as_str()) };
            match gallery::fetch_page(&api_key, game_ref, sort, 0, None).await {
                Ok(listings) => {
                    let mut state = shared.lock().expect("collections lock");
                    state.listings = listings;
                    state.fetch_done = true;
                }
                Err(e) => {
                    let mut state = shared.lock().expect("collections lock");
                    state.fetch_error = Some(format!("{e:#}"));
                    state.fetch_done = true;
                }
            }
            ctx.request_repaint();
        });
    }

    fn start_collections_image_loading(&mut self, ctx: &egui::Context) {
        if self.collections_image_load_started {
            return;
        }
        self.collections_image_load_started = true;

        let shared = Arc::clone(&self.collections_shared);
        let ctx = ctx.clone();
        let cache_dir = self.image_cache_dir.join("collections");
        let _ = std::fs::create_dir_all(&cache_dir);

        let to_load: Vec<(String, String)> = {
            let mut state = shared.lock().expect("collections lock");
            let items: Vec<(String, String)> = state
                .listings
                .iter()
                .filter_map(|c| {
                    let url = c.image_url.clone()?;
                    if url.is_empty() {
                        return None;
                    }
                    Some((c.slug.clone(), url))
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

            for batch in to_load.chunks(IMAGE_BATCH_SIZE) {
                let mut handles = Vec::with_capacity(batch.len());
                for (key, url) in batch {
                    let client = client.clone();
                    let key = key.clone();
                    let url = url.clone();
                    let cache_dir = cache_dir.clone();
                    handles.push(tokio::spawn(async move {
                        // Disk cache.
                        let cached = cache_dir.join(&key);
                        if cached.exists() {
                            if let Ok(bytes) = std::fs::read(&cached) {
                                if !bytes.is_empty() {
                                    return (key, Ok(bytes));
                                }
                            }
                        }
                        let result: anyhow::Result<Vec<u8>> = async {
                            let resp = client.get(&url).send().await?;
                            let bytes = resp.bytes().await?.to_vec();
                            let _ = std::fs::write(&cached, &bytes);
                            Ok(bytes)
                        }
                        .await;
                        (key, result)
                    }));
                }
                for h in handles {
                    let Ok((key, result)) = h.await else { continue };
                    let mut state = shared.lock().expect("collections lock");
                    match result {
                        Ok(bytes) => {
                            state.images.insert(key, ImageState::Downloaded(bytes));
                        }
                        Err(_) => {
                            state.images.insert(key, ImageState::Failed);
                        }
                    }
                }
                ctx.request_repaint();
            }
        });
    }

    fn build_collection_command(&self) -> String {
        // Quote any path containing whitespace.
        let q = |s: &str| -> String {
            if s.chars().any(|c| c.is_whitespace()) {
                format!("\"{s}\"")
            } else {
                s.to_string()
            }
        };
        // Use the running binary's absolute path so the snippet is paste-ready
        // even when `clf3` isn't on $PATH (e.g. running from target/release).
        let binary = std::env::current_exe()
            .ok()
            .map(|p| q(&p.to_string_lossy()))
            .unwrap_or_else(|| "clf3".to_string());
        let mut cmd = format!(
            "{} install-collection {} {} {}",
            binary,
            q(self.collection_url.trim()),
            q(self.collection_downloads.trim()),
            q(self.collection_output.trim()),
        );
        if !self.collection_game.trim().is_empty() {
            cmd.push_str(&format!(" --game {}", q(self.collection_game.trim())));
        }
        cmd
    }
}

fn sort_eq(a: CollectionSortBy, b: CollectionSortBy) -> bool {
    a == b
}

/// Truncate a string to at most `max_chars` characters (not bytes), appending
/// an ellipsis if any characters were dropped. Char-boundary safe — the byte
/// slice form panics on multi-byte glyphs (emoji).
fn truncate_chars(s: &str, max_chars: usize) -> String {
    if s.chars().count() <= max_chars {
        return s.to_string();
    }
    let mut out: String = s.chars().take(max_chars).collect();
    out.push('…');
    out
}

/// Strip Unicode format characters that egui renders as tofu boxes — chiefly
/// the emoji/text Variation Selectors (U+FE0E, U+FE0F) which appear after
/// most modern emoji and have no glyph in our bundled fonts.
fn strip_format_chars(s: &str) -> String {
    s.chars()
        .filter(|c| !matches!(*c, '\u{FE0E}' | '\u{FE0F}'))
        .collect()
}

fn fmt_count(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

/// Format a byte count as human-friendly size (matches the Wabbajack tab).
fn fmt_size(bytes: u64) -> String {
    const GB: f64 = 1024.0 * 1024.0 * 1024.0;
    const MB: f64 = 1024.0 * 1024.0;
    let b = bytes as f64;
    if b >= GB {
        format!("{:.1} GB", b / GB)
    } else if b >= MB {
        format!("{:.0} MB", b / MB)
    } else {
        format!("{} B", bytes)
    }
}
