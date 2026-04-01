//! egui-based modlist browser GUI.
//!
//! Launched via `clf3 browser`. Shows a scrollable grid of modlist cards with
//! lazy-loaded thumbnail images, search/filter controls, and generates a CLI
//! install command for the user to copy-paste.

use crate::modlist::browser::{ModlistBrowser, ModlistMetadata};
use eframe::egui;
use std::collections::HashMap;
use std::path::PathBuf;
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
    /// Not yet requested.
    Pending,
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
    /// Currently selected modlist machine_name (if any).
    selected: Option<String>,
    /// User-entered downloads directory.
    downloads_dir: String,
    /// User-entered output/install directory.
    install_dir: String,
    /// The generated command string (shown after selection).
    generated_command: Option<String>,
    /// Whether we've kicked off the initial fetch.
    fetch_started: bool,
    /// tokio runtime for async operations.
    rt: tokio::runtime::Runtime,
    /// Image cache directory.
    image_cache_dir: PathBuf,
    /// Whether background image loading has been kicked off.
    image_load_started: bool,
}

impl BrowserApp {
    fn new(_cc: &eframe::CreationContext<'_>) -> Self {
        let image_cache_dir = dirs::cache_dir()
            .unwrap_or_else(|| PathBuf::from("/tmp"))
            .join("clf3")
            .join("images");
        let _ = std::fs::create_dir_all(&image_cache_dir);

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
            selected: None,
            downloads_dir: String::new(),
            install_dir: String::new(),
            generated_command: None,
            fetch_started: false,
            rt: tokio::runtime::Runtime::new().expect("Failed to create tokio runtime"),
            image_cache_dir,
            image_load_started: false,
        }
    }

    /// Kick off async modlist fetch in the background.
    fn start_fetch(&mut self, ctx: &egui::Context) {
        if self.fetch_started {
            return;
        }
        self.fetch_started = true;

        let shared = Arc::clone(&self.shared);
        let ctx = ctx.clone();

        self.rt.spawn(async move {
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

        self.rt.spawn(async move {
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
        if self.downloads_dir.is_empty() || self.install_dir.is_empty() {
            return None;
        }
        // Use the current executable path so the command works from anywhere.
        let exe = std::env::current_exe()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|_| "clf3".into());
        Some(format!(
            "\"{}\" install \"{}\" \"{}\" \"{}\"",
            exe, url, self.downloads_dir, self.install_dir
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
                        .map(|g| (g.clone(), Self::format_game_name(g)))
                        .collect()
                };
                games.sort_by(|a, b| a.1.cmp(&b.1));
                games.dedup_by(|a, b| a.1 == b.1);

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
            });
            ui.add_space(4.0);
        });

        // Bottom panel: selected modlist details + command generation.
        if self.selected.is_some() {
            egui::TopBottomPanel::bottom("bottom_panel")
                .min_height(120.0)
                .show(ctx, |ui| {
                    ui.add_space(8.0);

                    let selected_modlist = {
                        let state = self.shared.lock().expect("lock shared state");
                        let name = self.selected.as_ref().unwrap();
                        state
                            .modlists
                            .iter()
                            .find(|m| &m.machine_name == name)
                            .cloned()
                    };

                    if let Some(modlist) = selected_modlist {
                        ui.horizontal(|ui| {
                            ui.heading(&modlist.title);
                            ui.label(format!("by {}", modlist.author));
                            ui.label(format!("({})", Self::format_game_name(&modlist.game)));

                            if ui.button("X Close").clicked() {
                                self.selected = None;
                                self.generated_command = None;
                            }
                        });

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

                        // Generate command button.
                        if let Some(cmd) = self.generate_command(&modlist) {
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
                            Some(ImageState::Loading) | Some(ImageState::Pending) | Some(ImageState::Downloaded(_)) | None
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
