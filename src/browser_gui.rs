//! egui-based modlist browser GUI.
//!
//! Launched via `clf3 browser`. Shows a scrollable list of modlist cards with
//! lazy-loaded thumbnail images, search/filter controls, and a Settings tab
//! for managing API keys, GPU selection, and default directories.

use crate::downloaders::{LoversLabDownloader, NexusDownloader};
use crate::game_finder::{detect_all_games, find_by_gog_id, find_by_steam_id, Launcher};
use crate::modlist::browser::{ModlistBrowser, ModlistMetadata};
use crate::settings::{BrowserListPaths, Settings};
use crate::textures::{list_gpus, GpuInfo};
use eframe::egui;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};

/// How many images to fetch concurrently during background loading.
const IMAGE_BATCH_SIZE: usize = 12;

/// How many downloaded images to decode/upload on the UI thread per frame.
const IMAGE_CONVERSIONS_PER_FRAME: usize = 2;

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

/// Top-level navigation between the modlist browser and the settings editor.
#[derive(Default, Clone, Copy, PartialEq, Eq)]
enum Tab {
    #[default]
    Browser,
    Settings,
}

/// Async credential-validation status for the Settings tab.
#[derive(Clone)]
enum ValidationStatus {
    Idle,
    InProgress,
    Ok(String),
    Err(String),
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

    // --- Tab + Settings state ---
    /// Which top-level tab is showing.
    current_tab: Tab,
    /// Editable settings, loaded from disk at startup.
    settings: Settings,
    /// Async status for "Save & Verify Nexus key".
    nexus_status: Arc<Mutex<ValidationStatus>>,
    /// Async status for "Save & Verify LoversLab login".
    ll_status: Arc<Mutex<ValidationStatus>>,
    /// Whether to render the Nexus key in plain text.
    show_nexus_key: bool,
    /// Whether to render the LL email + password in plain text.
    show_ll_credentials: bool,
    /// Lazily populated on first Settings tab visit — wgpu enumeration is slow.
    available_gpus: Option<Vec<GpuInfo>>,
    /// One-shot message shown after saving (ok flag, text).
    settings_save_message: Option<(bool, String)>,
    /// Status message shown next to the Run button after spawning a terminal.
    run_status: Option<(bool, String)>,
    /// Whether we've tried to restore the last browser selection after metadata loaded.
    selection_restore_attempted: bool,
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

        // Load saved settings and pre-fill the install panel's directory
        // fields with the user's defaults (if any).
        let settings = Settings::load();
        let downloads_dir = settings.default_downloads_dir.clone();
        let install_dir = settings.default_install_dir.clone();

        Self {
            shared: Arc::new(Mutex::new(SharedState {
                modlists: Vec::new(),
                games: Vec::new(),
                images: HashMap::new(),
                fetch_done: false,
                fetch_error: None,
            })),
            search: String::new(),
            game_filter: settings.browser_game_filter.clone(),
            show_nsfw: settings.browser_show_nsfw,
            show_unavailable: settings.browser_show_unavailable,
            show_installed_only: settings.browser_show_installed_only,
            installed_game_types,
            installed_game_count,
            selected: None,
            local_wabbajack: None,
            downloads_dir,
            install_dir,
            generated_command: None,
            fetch_started: false,
            rt: Some(tokio::runtime::Runtime::new().expect("Failed to create tokio runtime")),
            image_cache_dir,
            image_load_started: false,
            current_tab: Tab::Browser,
            settings,
            nexus_status: Arc::new(Mutex::new(ValidationStatus::Idle)),
            ll_status: Arc::new(Mutex::new(ValidationStatus::Idle)),
            show_nexus_key: false,
            show_ll_credentials: false,
            available_gpus: None,
            settings_save_message: None,
            run_status: None,
            selection_restore_attempted: false,
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
                            Ok(resp) if resp.status().is_success() => match resp.bytes().await {
                                Ok(bytes) if !bytes.is_empty() => {
                                    let _ = std::fs::write(&cached_path, &bytes);
                                    (key, Ok(bytes.to_vec()))
                                }
                                Ok(_) => (key, Err("Empty response".to_string())),
                                Err(e) => (key, Err(e.to_string())),
                            },
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

    fn selected_list_key(&self) -> Option<String> {
        self.selected.clone()
    }

    fn apply_paths_for_list(&mut self, key: &str) {
        if let Some(paths) = self.settings.browser_list_paths.get(key) {
            self.downloads_dir = paths.downloads_dir.clone();
            self.install_dir = paths.install_dir.clone();
        } else {
            self.downloads_dir = self.settings.default_downloads_dir.clone();
            self.install_dir = self.settings.default_install_dir.clone();
        }
    }

    fn remember_current_list_paths(&mut self) {
        let Some(key) = self.selected_list_key() else {
            return;
        };

        self.settings.browser_list_paths.insert(
            key,
            BrowserListPaths {
                downloads_dir: self.downloads_dir.clone(),
                install_dir: self.install_dir.clone(),
            },
        );
        let _ = self.settings.save();
    }

    fn remember_browser_state(&mut self) {
        self.settings.browser_game_filter = self.game_filter.clone();
        self.settings.browser_show_nsfw = self.show_nsfw;
        self.settings.browser_show_unavailable = self.show_unavailable;
        self.settings.browser_show_installed_only = self.show_installed_only;
        self.settings.browser_last_selected_modlist = self.selected.clone();
        let _ = self.settings.save();
    }

    fn select_modlist(&mut self, name: String) {
        self.remember_current_list_paths();
        self.selected = Some(name.clone());
        self.local_wabbajack = None;
        self.generated_command = None;
        self.run_status = None;
        self.apply_paths_for_list(&name);
        self.settings.browser_last_selected_modlist = Some(name);
        let _ = self.settings.save();
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
        let mut parts = vec![
            shell_quote(&exe),
            "install".to_string(),
            shell_quote(source),
            shell_quote(&self.downloads_dir),
            shell_quote(&self.install_dir),
        ];
        parts.extend(self.manual_browser_display_args());
        Some(parts.join(" "))
    }

    /// Build the install command as a list of args (exe + args) for direct
    /// spawning, alongside the display string. Returns `None` when the user
    /// hasn't supplied both directories yet.
    fn build_install_args(&self, source: &str) -> Option<(PathBuf, Vec<String>)> {
        if self.downloads_dir.is_empty() || self.install_dir.is_empty() {
            return None;
        }
        let exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("clf3"));
        let mut args = vec![
            "install".into(),
            source.into(),
            self.downloads_dir.clone(),
            self.install_dir.clone(),
        ];
        args.extend(self.manual_browser_spawn_args());
        Some((exe, args))
    }

    fn manual_browser_spawn_args(&self) -> Vec<String> {
        if !self.settings.browser_manual_browser_mode {
            return Vec::new();
        }
        let mut args = vec!["--manual-browser-mode".to_string()];
        if !self.settings.browser_manual_watch_dir.trim().is_empty() {
            args.push("--manual-watch-dir".to_string());
            args.push(self.settings.browser_manual_watch_dir.clone());
        }
        args.push("--manual-max-active".to_string());
        args.push(self.manual_browser_max_active().to_string());
        args
    }

    fn manual_browser_max_active(&self) -> usize {
        if self.settings.browser_manual_max_active >= 8 {
            8
        } else {
            4
        }
    }

    fn manual_browser_display_args(&self) -> Vec<String> {
        self.manual_browser_spawn_args()
            .into_iter()
            .map(|arg| {
                if arg.starts_with("--") {
                    arg
                } else {
                    shell_quote(&arg)
                }
            })
            .collect()
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

        // Convert a small number of downloaded images to GPU textures each
        // frame. Decoding every ready image in one pass makes the browser feel
        // frozen on launch when many thumbnails arrive together.
        let images_to_convert: Vec<(String, Vec<u8>)> = {
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
                .take(IMAGE_CONVERSIONS_PER_FRAME)
                .collect();

            keys_to_convert
                .into_iter()
                .filter_map(|key| match state.images.remove(&key) {
                    Some(ImageState::Downloaded(bytes)) => Some((key, bytes)),
                    other => {
                        if let Some(state_value) = other {
                            state.images.insert(key, state_value);
                        }
                        None
                    }
                })
                .collect()
        };

        for (key, bytes) in images_to_convert {
            let converted = match image::load_from_memory(&bytes) {
                Ok(img) => {
                    let rgba = img.to_rgba8();
                    let size = [rgba.width() as usize, rgba.height() as usize];
                    let pixels = rgba.into_raw();
                    let color_image = egui::ColorImage::from_rgba_unmultiplied(size, &pixels);
                    ImageState::Texture(ctx.load_texture(
                        &key,
                        color_image,
                        egui::TextureOptions::LINEAR,
                    ))
                }
                Err(_) => ImageState::Failed,
            };

            let mut state = self.shared.lock().expect("lock shared state");
            state.images.insert(key, converted);
            if state
                .images
                .values()
                .any(|v| matches!(v, ImageState::Downloaded(_)))
            {
                ctx.request_repaint();
            }
        }

        // Top tab bar — always visible.
        egui::TopBottomPanel::top("tab_bar").show(ctx, |ui| {
            ui.add_space(4.0);
            ui.horizontal(|ui| {
                ui.selectable_value(&mut self.current_tab, Tab::Browser, "Modlist Browser");
                ui.selectable_value(&mut self.current_tab, Tab::Settings, "Settings");
            });
            ui.add_space(2.0);
        });

        match self.current_tab {
            Tab::Browser => self.render_browser_tab(ctx),
            Tab::Settings => self.render_settings_tab(ctx),
        }

        if !self.selection_restore_attempted {
            let last = self.settings.browser_last_selected_modlist.clone();
            if let Some(name) = last {
                let exists = {
                    let state = self.shared.lock().expect("lock shared state");
                    state.fetch_done && state.modlists.iter().any(|m| m.machine_name == name)
                };
                if exists {
                    self.select_modlist(name);
                    self.selection_restore_attempted = true;
                } else {
                    let fetch_done = {
                        let state = self.shared.lock().expect("lock shared state");
                        state.fetch_done
                    };
                    if fetch_done {
                        self.selection_restore_attempted = true;
                    }
                }
            } else {
                self.selection_restore_attempted = true;
            }
        }

        // Check for pending card selection (set by render_card via ctx memory).
        let pending: Option<String> =
            ctx.memory_mut(|mem| mem.data.get_temp(egui::Id::new("pending_selection")));
        if let Some(name) = pending {
            ctx.memory_mut(|mem| {
                mem.data
                    .remove::<String>(egui::Id::new("pending_selection"));
            });
            self.select_modlist(name);
        }
    }
}

impl BrowserApp {
    /// Top panel: search bar + filters + open-file button.
    fn render_browser_filters(&mut self, ctx: &egui::Context) {
        egui::TopBottomPanel::top("browser_filters").show(ctx, |ui| {
            let old_game_filter = self.game_filter.clone();
            let old_show_nsfw = self.show_nsfw;
            let old_show_unavailable = self.show_unavailable;
            let old_show_installed_only = self.show_installed_only;

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
                if self.installed_game_types.is_empty() {
                    self.show_installed_only = false;
                }

                ui.separator();

                // Let the user skip the browser entirely and point at an
                // already-downloaded `.wabbajack` file on disk.
                if ui.button("Open .wabbajack file...").clicked() {
                    if let Some(path) = rfd::FileDialog::new()
                        .add_filter("Wabbajack modlist", &["wabbajack"])
                        .pick_file()
                    {
                        self.remember_current_list_paths();
                        self.local_wabbajack = Some(path);
                        // Picking a local file clears any network selection so
                        // the bottom panel doesn't show two competing titles.
                        self.selected = None;
                        self.generated_command = None;
                        self.settings.browser_last_selected_modlist = None;
                        let _ = self.settings.save();
                    }
                }
            });
            ui.add_space(4.0);

            if self.game_filter != old_game_filter
                || self.show_nsfw != old_show_nsfw
                || self.show_unavailable != old_show_unavailable
                || self.show_installed_only != old_show_installed_only
            {
                self.remember_browser_state();
            }
        });
    }

    /// Bottom panel: directory inputs + generated command + Run/Copy buttons.
    /// Only rendered when a modlist or local file is selected.
    fn render_install_panel(&mut self, ctx: &egui::Context) {
        if self.selected.is_none() && self.local_wabbajack.is_none() {
            return;
        }

        egui::TopBottomPanel::bottom("install_panel")
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
                        self.remember_current_list_paths();
                        self.selected = None;
                        self.local_wabbajack = None;
                        self.generated_command = None;
                        self.run_status = None;
                        self.settings.browser_last_selected_modlist = None;
                        let _ = self.settings.save();
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
                    let downloads_response = ui.add(
                        egui::TextEdit::singleline(&mut self.downloads_dir).desired_width(300.0),
                    );
                    let mut paths_changed = downloads_response.changed();
                    if ui.button("Browse...").clicked() {
                        if let Some(path) = rfd::FileDialog::new().pick_folder() {
                            self.downloads_dir = path.display().to_string();
                            paths_changed = true;
                        }
                    }

                    ui.separator();

                    ui.label("Install dir:");
                    let install_response = ui.add(
                        egui::TextEdit::singleline(&mut self.install_dir).desired_width(300.0),
                    );
                    paths_changed |= install_response.changed();
                    if ui.button("Browse...").clicked() {
                        if let Some(path) = rfd::FileDialog::new().pick_folder() {
                            self.install_dir = path.display().to_string();
                            paths_changed = true;
                        }
                    }

                    if paths_changed {
                        self.generated_command = None;
                        self.remember_current_list_paths();
                    }
                });

                ui.add_space(4.0);

                // Build the install command from whichever source is set.
                // Local file takes precedence over an online selection
                // because picking a file clears `selected`.
                let (display_cmd, spawn_args) = if let Some(path) = self.local_wabbajack.clone() {
                    let source = path.display().to_string();
                    (
                        self.generate_command_for_local(&path),
                        self.build_install_args(&source),
                    )
                } else if let Some(modlist) = &selected_modlist {
                    let source_owned = modlist.download_url().map(|s| s.to_string());
                    let display = source_owned
                        .as_ref()
                        .and_then(|_| self.generate_command(modlist));
                    let args = source_owned
                        .as_ref()
                        .and_then(|s| self.build_install_args(s));
                    (display, args)
                } else {
                    (None, None)
                };

                if let Some(cmd) = display_cmd {
                    self.generated_command = Some(cmd);
                }

                if let Some(ref cmd) = self.generated_command.clone() {
                    ui.horizontal(|ui| {
                        ui.label("Command:");
                        ui.add(
                            egui::TextEdit::singleline(&mut cmd.clone())
                                .desired_width(ui.available_width() - 220.0)
                                .font(egui::TextStyle::Monospace),
                        );

                        let run_clicked = ui
                            .add_enabled(spawn_args.is_some(), egui::Button::new("Run"))
                            .on_hover_text(
                                "Launches the install in a new terminal window and closes \
                                 the browser. Falls back to the parent terminal if no \
                                 graphical terminal emulator is found.",
                            )
                            .clicked();

                        if ui.button("Copy").clicked() {
                            ui.ctx().copy_text(cmd.clone());
                            self.run_status = Some((true, "Copied to clipboard.".into()));
                        }

                        if run_clicked {
                            if let Some((exe, args)) = spawn_args {
                                match launch_install(&exe, &args) {
                                    Ok(LaunchOutcome::Terminal(name)) => {
                                        // Window will be closed once we exit
                                        // this UI scope; tell egui to do so.
                                        ctx.send_viewport_cmd(egui::ViewportCommand::Close);
                                        self.run_status = Some((
                                            true,
                                            format!("Launched in {} — closing browser.", name),
                                        ));
                                    }
                                    Ok(LaunchOutcome::Inline) => {
                                        ctx.send_viewport_cmd(egui::ViewportCommand::Close);
                                        self.run_status = Some((
                                            true,
                                            "No graphical terminal found — install spawned \
                                             inline; output goes to the terminal that \
                                             launched the browser."
                                                .into(),
                                        ));
                                    }
                                    Err(e) => {
                                        self.run_status =
                                            Some((false, format!("Run failed: {}", e)));
                                    }
                                }
                            }
                        }
                    });

                    if let Some((ok, ref msg)) = self.run_status {
                        let color = if ok {
                            egui::Color32::from_rgb(50, 180, 50)
                        } else {
                            egui::Color32::RED
                        };
                        ui.colored_label(color, msg);
                    }
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

    /// Browser tab: filters + install panel + modlist grid.
    fn render_browser_tab(&mut self, ctx: &egui::Context) {
        self.render_browser_filters(ctx);
        self.render_install_panel(ctx);

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
    }

    /// Settings tab: editable form for credentials, GPU, default directories.
    fn render_settings_tab(&mut self, ctx: &egui::Context) {
        // Lazy-load GPU list on first visit — wgpu enumeration can take 1-2s.
        if self.available_gpus.is_none() {
            self.available_gpus = Some(list_gpus());
        }

        egui::CentralPanel::default().show(ctx, |ui| {
            egui::ScrollArea::vertical().show(ui, |ui| {
                ui.heading("Settings");
                ui.label(
                    egui::RichText::new(
                        Settings::settings_path()
                            .map(|p| format!("Stored in: {}", p.display()))
                            .unwrap_or_else(|_| "Stored in: <unknown>".into()),
                    )
                    .size(11.0)
                    .color(egui::Color32::from_gray(140)),
                );
                ui.add_space(12.0);

                // --- Fluorine integration (single toggle at the top) ---
                let prev_add_to_fluorine = self.settings.add_to_fluorine;
                let cb = ui.checkbox(
                    &mut self.settings.add_to_fluorine,
                    "Add finished installs to Fluorine",
                );
                cb.on_hover_text(
                    "After a successful install, register the install directory as a \
                     portable instance in Fluorine Manager. If Fluorine isn't installed, \
                     CLF3 will download the latest release from GitHub automatically.",
                );
                if self.settings.add_to_fluorine != prev_add_to_fluorine {
                    let _ = self.settings.save();
                }

                ui.add_space(12.0);

                // --- Default Directories ---
                ui.group(|ui| {
                    ui.heading("Default Directories");
                    ui.label(
                        egui::RichText::new(
                            "Pre-fill the install panel each time you launch the browser.",
                        )
                        .size(11.0)
                        .color(egui::Color32::from_gray(160)),
                    );
                    ui.add_space(4.0);

                    ui.horizontal(|ui| {
                        ui.label("Downloads:");
                        ui.add(
                            egui::TextEdit::singleline(&mut self.settings.default_downloads_dir)
                                .desired_width(400.0),
                        );
                        if ui.button("Browse...").clicked() {
                            if let Some(p) = rfd::FileDialog::new().pick_folder() {
                                self.settings.default_downloads_dir = p.display().to_string();
                            }
                        }
                    });
                    ui.horizontal(|ui| {
                        ui.label("Install:    ");
                        ui.add(
                            egui::TextEdit::singleline(&mut self.settings.default_install_dir)
                                .desired_width(400.0),
                        );
                        if ui.button("Browse...").clicked() {
                            if let Some(p) = rfd::FileDialog::new().pick_folder() {
                                self.settings.default_install_dir = p.display().to_string();
                            }
                        }
                    });

                    ui.add_space(4.0);
                    if ui.button("Save default directories").clicked() {
                        // Mirror into the live install panel so the user sees
                        // their saved defaults immediately on the Browser tab.
                        self.downloads_dir = self.settings.default_downloads_dir.clone();
                        self.install_dir = self.settings.default_install_dir.clone();
                        self.do_save_settings("Default directories saved.");
                    }
                });

                ui.add_space(12.0);

                // --- Browser download mode ---
                ui.group(|ui| {
                    ui.heading("Manual Browser Downloads");
                    ui.label(
                        egui::RichText::new(
                            "Use the local controller for non-premium Nexus downloads.",
                        )
                        .size(11.0)
                        .color(egui::Color32::from_gray(160)),
                    );
                    ui.add_space(4.0);

                    let before_enabled = self.settings.browser_manual_browser_mode;
                    ui.checkbox(
                        &mut self.settings.browser_manual_browser_mode,
                        "Use manual browser mode by default",
                    );
                    if self.settings.browser_manual_browser_mode != before_enabled {
                        self.generated_command = None;
                        let _ = self.settings.save();
                    }

                    ui.horizontal(|ui| {
                        ui.label("Watch folder:");
                        let response = ui.add(
                            egui::TextEdit::singleline(&mut self.settings.browser_manual_watch_dir)
                                .desired_width(400.0),
                        );
                        if response.changed() {
                            self.generated_command = None;
                        }
                        if ui.button("Browse...").clicked() {
                            if let Some(p) = rfd::FileDialog::new().pick_folder() {
                                self.settings.browser_manual_watch_dir = p.display().to_string();
                                self.generated_command = None;
                            }
                        }
                    });

                    let mut use_eight_downloads = self.manual_browser_max_active() == 8;
                    let response = ui.checkbox(&mut use_eight_downloads, "Use 8 active downloads");
                    if response.changed() {
                        self.settings.browser_manual_max_active =
                            if use_eight_downloads { 8 } else { 4 };
                        self.generated_command = None;
                        let _ = self.settings.save();
                    }

                    ui.add_space(4.0);
                    if ui.button("Save manual browser settings").clicked() {
                        self.do_save_settings("Manual browser settings saved.");
                    }
                });

                ui.add_space(12.0);

                // --- Nexus Mods API Key ---
                ui.group(|ui| {
                    ui.heading("Nexus Mods");
                    ui.label(
                        egui::RichText::new(
                            "API key from https://www.nexusmods.com/users/myaccount?tab=api",
                        )
                        .size(11.0)
                        .color(egui::Color32::from_gray(160)),
                    );
                    ui.add_space(4.0);

                    ui.horizontal(|ui| {
                        ui.label("API key:");
                        ui.add(
                            egui::TextEdit::singleline(&mut self.settings.nexus_api_key)
                                .password(!self.show_nexus_key)
                                .desired_width(400.0),
                        );
                        ui.checkbox(&mut self.show_nexus_key, "Show");
                    });

                    ui.add_space(4.0);
                    ui.horizontal(|ui| {
                        if ui.button("Save & Verify").clicked() {
                            self.verify_and_save_nexus(ctx);
                        }
                        render_validation_status(ui, &self.nexus_status);
                    });
                });

                ui.add_space(12.0);

                // --- LoversLab login ---
                ui.group(|ui| {
                    ui.heading("LoversLab");
                    ui.label(
                        egui::RichText::new(
                            "Used to log in for automated downloads of LL-hosted files.",
                        )
                        .size(11.0)
                        .color(egui::Color32::from_gray(160)),
                    );
                    ui.add_space(4.0);

                    ui.horizontal(|ui| {
                        ui.label("Email/username:");
                        ui.add(
                            egui::TextEdit::singleline(&mut self.settings.loverslab_email)
                                .password(!self.show_ll_credentials)
                                .desired_width(360.0),
                        );
                    });
                    ui.horizontal(|ui| {
                        ui.label("Password:      ");
                        ui.add(
                            egui::TextEdit::singleline(&mut self.settings.loverslab_password)
                                .password(!self.show_ll_credentials)
                                .desired_width(360.0),
                        );
                        ui.checkbox(&mut self.show_ll_credentials, "Show");
                    });

                    ui.add_space(4.0);
                    ui.horizontal(|ui| {
                        if ui.button("Save & Verify").clicked() {
                            self.verify_and_save_ll(ctx);
                        }
                        render_validation_status(ui, &self.ll_status);
                    });
                });

                ui.add_space(12.0);

                // --- GPU selection ---
                ui.group(|ui| {
                    ui.heading("GPU (texture encoding)");
                    ui.label(
                        egui::RichText::new(
                            "Used for BC7/BC6H DDS encoding during install. Auto picks \
                             the first discrete GPU.",
                        )
                        .size(11.0)
                        .color(egui::Color32::from_gray(160)),
                    );
                    ui.add_space(4.0);

                    let gpus = self.available_gpus.clone().unwrap_or_default();
                    let current_text = match self.settings.gpu_index {
                        None => "auto (recommended)".to_string(),
                        Some(idx) => format!(
                            "[{}] {}",
                            idx,
                            if self.settings.gpu_name.is_empty() {
                                "<unknown>"
                            } else {
                                self.settings.gpu_name.as_str()
                            }
                        ),
                    };
                    egui::ComboBox::from_id_salt("gpu_select")
                        .selected_text(current_text)
                        .width(500.0)
                        .show_ui(ui, |ui| {
                            if ui
                                .selectable_label(
                                    self.settings.gpu_index.is_none(),
                                    "auto (recommended)",
                                )
                                .clicked()
                            {
                                self.settings.gpu_index = None;
                                self.settings.gpu_name = String::new();
                            }
                            for gpu in &gpus {
                                let label = format!(
                                    "[{}] {} ({}, {})",
                                    gpu.adapter_index, gpu.name, gpu.backend, gpu.device_type
                                );
                                let selected = self.settings.gpu_index == Some(gpu.adapter_index);
                                if ui.selectable_label(selected, &label).clicked() {
                                    self.settings.gpu_index = Some(gpu.adapter_index);
                                    self.settings.gpu_name = gpu.name.clone();
                                }
                            }
                        });

                    ui.add_space(4.0);
                    if ui.button("Save GPU selection").clicked() {
                        self.do_save_settings("GPU selection saved.");
                    }
                });

                ui.add_space(12.0);

                // --- Patch cache (optional) ---
                ui.group(|ui| {
                    ui.heading("Patch cache (optional)");
                    ui.label(
                        egui::RichText::new(
                            "Persistent directory for octodiff patched files \
                             across runs. Leave blank to disable.",
                        )
                        .size(11.0)
                        .color(egui::Color32::from_gray(160)),
                    );
                    ui.add_space(4.0);
                    ui.horizontal(|ui| {
                        ui.add(
                            egui::TextEdit::singleline(&mut self.settings.patch_cache_dir)
                                .desired_width(400.0),
                        );
                        if ui.button("Browse...").clicked() {
                            if let Some(p) = rfd::FileDialog::new().pick_folder() {
                                self.settings.patch_cache_dir = p.display().to_string();
                            }
                        }
                    });
                    ui.add_space(4.0);
                    if ui.button("Save patch cache dir").clicked() {
                        self.do_save_settings("Patch cache directory saved.");
                    }
                });

                ui.add_space(16.0);
                ui.separator();
                ui.add_space(4.0);
                ui.horizontal(|ui| {
                    if ui.button("Save all settings").clicked() {
                        self.do_save_settings("All settings saved.");
                    }
                    if let Some((ok, ref msg)) = self.settings_save_message {
                        let color = if ok {
                            egui::Color32::from_rgb(50, 180, 50)
                        } else {
                            egui::Color32::RED
                        };
                        ui.colored_label(color, msg);
                    }
                });
                ui.add_space(12.0);
            });
        });
    }

    /// Persist the current `self.settings` to disk and record a status message.
    fn do_save_settings(&mut self, success_msg: &str) {
        match self.settings.save() {
            Ok(_) => self.settings_save_message = Some((true, success_msg.into())),
            Err(e) => self.settings_save_message = Some((false, format!("Save failed: {}", e))),
        }
    }

    /// Validate the Nexus API key against the live API, then save settings on
    /// success. The status field is updated from the background task and
    /// triggers a repaint when done.
    fn verify_and_save_nexus(&mut self, ctx: &egui::Context) {
        let key = self.settings.nexus_api_key.clone();
        if key.is_empty() {
            *self.nexus_status.lock().unwrap() = ValidationStatus::Err("API key is empty".into());
            return;
        }
        *self.nexus_status.lock().unwrap() = ValidationStatus::InProgress;

        let status = Arc::clone(&self.nexus_status);
        let settings_snapshot = self.settings.clone();
        let ctx = ctx.clone();
        self.rt().spawn(async move {
            let result = match NexusDownloader::new(&key) {
                Ok(nx) => nx.validate().await,
                Err(e) => Err(e),
            };
            let mut s = status.lock().unwrap();
            match result {
                Ok(info) => match settings_snapshot.save() {
                    Ok(_) => {
                        *s = ValidationStatus::Ok(format!(
                            "Verified as {} (Premium: {})",
                            info.name,
                            if info.is_premium { "yes" } else { "no" }
                        ));
                    }
                    Err(e) => {
                        *s = ValidationStatus::Err(format!("Verified but save failed: {}", e));
                    }
                },
                Err(e) => *s = ValidationStatus::Err(format!("{}", e)),
            }
            drop(s);
            ctx.request_repaint();
        });
    }

    /// Validate LoversLab credentials by attempting a login, then save settings
    /// on success.
    fn verify_and_save_ll(&mut self, ctx: &egui::Context) {
        let email = self.settings.loverslab_email.clone();
        let password = self.settings.loverslab_password.clone();
        if email.is_empty() || password.is_empty() {
            *self.ll_status.lock().unwrap() =
                ValidationStatus::Err("Email and password are required".into());
            return;
        }
        *self.ll_status.lock().unwrap() = ValidationStatus::InProgress;

        let status = Arc::clone(&self.ll_status);
        let settings_snapshot = self.settings.clone();
        let ctx = ctx.clone();
        self.rt().spawn(async move {
            let result = LoversLabDownloader::login(&email, &password).await;
            let mut s = status.lock().unwrap();
            match result {
                Ok(_) => match settings_snapshot.save() {
                    Ok(_) => *s = ValidationStatus::Ok("Verified and saved.".into()),
                    Err(e) => {
                        *s = ValidationStatus::Err(format!("Verified but save failed: {}", e));
                    }
                },
                Err(e) => *s = ValidationStatus::Err(format!("{}", e)),
            }
            drop(s);
            ctx.request_repaint();
        });
    }
}

fn shell_quote(value: &str) -> String {
    if value.is_empty() {
        return "''".to_string();
    }
    if value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '.' | '-' | '_' | ':' | '='))
    {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}

/// Outcome of trying to run the install command.
enum LaunchOutcome {
    /// Spawned into a graphical terminal emulator (binary name).
    Terminal(&'static str),
    /// No graphical terminal found — spawned inline with inherited stdio.
    Inline,
}

/// Linux terminal emulators we know how to invoke, in preference order.
/// Each entry is `(binary, args_before_command)`.
const TERMINAL_CANDIDATES: &[(&str, &[&str])] = &[
    ("kitty", &[]),
    ("konsole", &["-e"]),
    ("alacritty", &["-e"]),
    ("wezterm", &["start", "--"]),
    ("gnome-terminal", &["--"]),
    ("foot", &[]),
    ("tilix", &["-e"]),
    ("xfce4-terminal", &["-e"]),
    ("xterm", &["-e"]),
];

/// Spawn the install command in a new terminal window (preferred) or inline
/// (fallback). Returns which path was taken.
fn launch_install(exe: &Path, args: &[String]) -> Result<LaunchOutcome, String> {
    // Build a single bash invocation that runs the install and pauses for
    // input afterwards so the user can read the final output.
    let mut quoted = shell_single_quote(&exe.display().to_string());
    for a in args {
        quoted.push(' ');
        quoted.push_str(&shell_single_quote(a));
    }
    let bash_script = format!(
        "{} ; echo ; echo \"Installation finished. Press Enter to close.\" ; read",
        quoted
    );

    for (binary, prefix) in TERMINAL_CANDIDATES {
        if which::which(binary).is_err() {
            continue;
        }
        let mut cmd = Command::new(binary);
        cmd.args(*prefix);
        cmd.args(["bash", "-c", &bash_script]);
        // Detach stdio so the new window owns its own TTY.
        cmd.stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        match cmd.spawn() {
            Ok(_) => return Ok(LaunchOutcome::Terminal(binary)),
            Err(_) => continue,
        }
    }

    // Inline fallback — spawn the install directly with inherited stdio so
    // its output goes to whatever terminal launched the browser.
    Command::new(exe)
        .args(args)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .map(|_| LaunchOutcome::Inline)
        .map_err(|e| format!("spawning {}: {}", exe.display(), e))
}

/// Wrap a string in single quotes for safe shell embedding.
fn shell_single_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        if c == '\'' {
            // Close quote, escape the apostrophe, reopen.
            out.push_str("'\\''");
        } else {
            out.push(c);
        }
    }
    out.push('\'');
    out
}

/// Render an inline indicator showing the current state of an async
/// validation task.
fn render_validation_status(ui: &mut egui::Ui, status: &Arc<Mutex<ValidationStatus>>) {
    let snapshot = status.lock().unwrap().clone();
    match snapshot {
        ValidationStatus::Idle => {}
        ValidationStatus::InProgress => {
            ui.spinner();
            ui.label("Verifying...");
        }
        ValidationStatus::Ok(msg) => {
            ui.colored_label(egui::Color32::from_rgb(50, 180, 50), msg);
        }
        ValidationStatus::Err(msg) => {
            ui.colored_label(egui::Color32::RED, msg);
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

                    let (_, thumb_rect) = ui.allocate_space(egui::vec2(THUMB_WIDTH, THUMB_HEIGHT));

                    if let Some(tex) = texture {
                        let img = egui::Image::new(&tex)
                            .fit_to_exact_size(egui::vec2(THUMB_WIDTH, THUMB_HEIGHT))
                            .corner_radius(4);
                        ui.put(thumb_rect, img);
                    } else if is_loading {
                        ui.painter()
                            .rect_filled(thumb_rect, 4, egui::Color32::from_gray(45));
                        ui.put(thumb_rect, egui::Spinner::new());
                    } else {
                        // Failed.
                        ui.painter()
                            .rect_filled(thumb_rect, 4, egui::Color32::from_gray(35));
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
                            ui.label(egui::RichText::new(&modlist.title).strong().size(16.0));
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
                            let truncated: String = modlist.description.chars().take(200).collect();
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
                                    .button(if is_selected { "Selected" } else { "Select" })
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
