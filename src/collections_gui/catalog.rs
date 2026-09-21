use clf3::collection::progress::safe_message;
use clf3::collection_app::catalog::{Catalog, Entry, Game, Page, Search, Sort, PAGE_SIZE};
use eframe::egui;
use rayon::prelude::*;
use std::{
    collections::HashMap,
    sync::mpsc::Receiver,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

enum Event {
    Results(Option<Vec<Game>>, Page),
    Thumbnail(String, egui::ColorImage),
    Error(String),
    Done,
}

#[derive(Default)]
pub(super) struct CatalogView {
    search: Search,
    game_search: String,
    games: Vec<Game>,
    page: Option<Page>,
    receiver: Option<Receiver<Event>>,
    cancellation: CancellationToken,
    thumbnails: HashMap<String, egui::TextureHandle>,
    loading: bool,
    started: bool,
    error: Option<String>,
    search_due: Option<Instant>,
}

impl Drop for CatalogView {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

impl CatalogView {
    fn fetch(&mut self, ctx: &egui::Context) {
        self.cancellation.cancel();
        self.cancellation = CancellationToken::new();
        let token = self.cancellation.clone();
        let search = self.search.clone();
        let load_games = self.games.is_empty();
        let (sender, receiver) = std::sync::mpsc::sync_channel(26);
        self.receiver = Some(receiver);
        self.page = None;
        self.thumbnails.clear();
        self.loading = true;
        self.started = true;
        self.error = None;
        self.search_due = None;
        let ctx = ctx.clone();
        std::thread::spawn(move || {
            let result = (|| -> anyhow::Result<()> {
                let catalog = Catalog::new()?;
                let games = if load_games {
                    Some(catalog.games(&token)?)
                } else {
                    None
                };
                let page = catalog.search(&search, &token)?;
                let images: Vec<_> = page
                    .nodes
                    .iter()
                    .filter_map(|e| {
                        e.tile_image
                            .as_ref()
                            .map(|t| (e.url().unwrap_or_default(), t.thumbnail_url.clone()))
                    })
                    .collect();
                if sender.send(Event::Results(games, page)).is_err() {
                    return Ok(());
                }
                ctx.request_repaint();
                // Four small image transfers/decodes at a time. No account key
                // is available to this thread or its image client.
                let pool = rayon::ThreadPoolBuilder::new().num_threads(4).build()?;
                pool.install(|| {
                    images.par_iter().for_each(|(id, url)| {
                        if token.is_cancelled() {
                            return;
                        }
                        if let Ok(rgba) = catalog.thumbnail(url, &token) {
                            if token.is_cancelled() {
                                return;
                            }
                            let image = egui::ColorImage::from_rgba_unmultiplied(
                                [rgba.width() as usize, rgba.height() as usize],
                                &rgba,
                            );
                            let _ = sender.send(Event::Thumbnail(id.clone(), image));
                            ctx.request_repaint();
                        }
                    })
                });
                Ok(())
            })();
            if let Err(error) = result {
                let _ = sender.send(Event::Error(safe_message(&format!("{error:#}"))));
            }
            let _ = sender.send(Event::Done);
            ctx.request_repaint();
        });
    }

    pub(super) fn poll(&mut self, ctx: &egui::Context) {
        let events: Vec<_> = self
            .receiver
            .as_ref()
            .map(|r| r.try_iter().take(8).collect())
            .unwrap_or_default();
        for event in events {
            match event {
                Event::Results(games, page) => {
                    if let Some(games) = games {
                        self.games = games;
                    }
                    self.page = Some(page);
                    self.loading = false;
                }
                Event::Thumbnail(id, image) => {
                    let texture = ctx.load_texture(
                        format!("collection:{id}"),
                        image,
                        egui::TextureOptions::LINEAR,
                    );
                    self.thumbnails.insert(id, texture);
                }
                Event::Error(error) => {
                    self.error = Some(error);
                    self.loading = false;
                }
                Event::Done => {
                    self.receiver = None;
                    self.loading = false;
                }
            }
        }
        if self.receiver.is_some() {
            ctx.request_repaint_after(Duration::from_millis(100));
        }
    }

    /// Selection returns a pinned revision URL; the installer performs its own
    /// complete compatibility review before enabling installation.
    pub(super) fn render(
        &mut self,
        ui: &mut egui::Ui,
        ctx: &egui::Context,
        installing: bool,
    ) -> Option<String> {
        let mut selected = None;
        let mut refresh = !self.started;
        ui.label("Discover Nexus collections across all games.");
        ui.small("Install Skyrim SE collections, or try the experimental Fallout 4, New Vegas, Fallout 3, Oblivion and Skyrim adapters. All games remain browsable.");
        ui.add_space(8.0);
        ui.horizontal(|ui| {
            let response = ui.add(
                egui::TextEdit::singleline(&mut self.search.text)
                    .hint_text("Search collection names…")
                    .char_limit(200)
                    .desired_width((ui.available_width() - 175.0).max(200.0)),
            );
            if response.changed() {
                self.search.page = 0;
                self.search_due = Some(Instant::now() + Duration::from_millis(450));
            }
            if ui.button("Search").clicked()
                || (response.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)))
            {
                self.search.page = 0;
                refresh = true;
            }
            if ui.button("Refresh").clicked() {
                refresh = true;
            }
        });
        ui.horizontal_wrapped(|ui| {
            ui.label("Game:");
            let game_name = self
                .games
                .iter()
                .find(|g| g.domain_name == self.search.game)
                .map(|g| g.name.as_str())
                .unwrap_or("All games");
            egui::ComboBox::from_id_salt("collection_game")
                .width(240.0)
                .close_behavior(egui::PopupCloseBehavior::CloseOnClickOutside)
                .selected_text(game_name)
                .show_ui(ui, |ui| {
                    ui.add(
                        egui::TextEdit::singleline(&mut self.game_search)
                            .hint_text("Find a game…")
                            .desired_width(235.0),
                    );
                    if ui
                        .selectable_value(&mut self.search.game, String::new(), "All games")
                        .clicked()
                    {
                        ui.memory_mut(|m| m.close_popup());
                        self.search.page = 0;
                        refresh = true;
                    }
                    let filter = self.game_search.to_lowercase();
                    for game in self
                        .games
                        .iter()
                        .filter(|g| g.name.to_lowercase().contains(&filter))
                    {
                        if ui
                            .selectable_value(
                                &mut self.search.game,
                                game.domain_name.clone(),
                                &game.name,
                            )
                            .clicked()
                        {
                            ui.memory_mut(|m| m.close_popup());
                            self.search.page = 0;
                            refresh = true;
                        }
                    }
                });
            ui.label("Sort:");
            egui::ComboBox::from_id_salt("collection_sort")
                .width(145.0)
                .selected_text(self.search.sort.label())
                .show_ui(ui, |ui| {
                    for sort in [Sort::Downloads, Sort::Updated, Sort::Newest] {
                        if ui
                            .selectable_value(&mut self.search.sort, sort, sort.label())
                            .changed()
                        {
                            self.search.page = 0;
                            refresh = true;
                        }
                    }
                });
            if ui
                .checkbox(&mut self.search.hide_adult, "Hide adult content")
                .changed()
            {
                self.search.page = 0;
                refresh = true;
            }
        });
        if self.search_due.is_some_and(|due| Instant::now() >= due) {
            refresh = true;
        }
        if self.search_due.is_some() {
            ctx.request_repaint_after(Duration::from_millis(100));
        }
        if refresh {
            self.fetch(ctx);
        }
        ui.add_space(8.0);
        if self.loading {
            ui.horizontal(|ui| {
                ui.spinner();
                ui.label("Loading collections…");
            });
        }
        if let Some(error) = &self.error {
            ui.colored_label(egui::Color32::LIGHT_RED, error);
            if ui.button("Retry catalog").clicked() {
                self.fetch(ctx);
            }
        }
        let mut next_page = None;
        if let Some(page) = &self.page {
            ui.horizontal(|ui| {
                let page_count = page.total_count.div_ceil(PAGE_SIZE).max(1);
                ui.label(format!(
                    "{} collections · Page {} of {}",
                    page.total_count,
                    self.search.page + 1,
                    page_count
                ));
                if ui
                    .add_enabled(self.search.page > 0, egui::Button::new("Previous"))
                    .clicked()
                {
                    next_page = self.search.page.checked_sub(1);
                }
                if ui
                    .add_enabled(self.search.page + 1 < page_count, egui::Button::new("Next"))
                    .clicked()
                {
                    next_page = Some(self.search.page + 1);
                }
            });
            ui.separator();
            if page.nodes.is_empty() {
                ui.label("No collections match this search. Try another name or game.");
            }
            egui::ScrollArea::vertical().id_salt("collection_catalog_results").auto_shrink([false, false]).show_rows(ui, 166.0, page.nodes.len(), |ui, rows| {
                for entry in &page.nodes[rows] {
                    let url = entry.url().unwrap_or_default();
                    ui.push_id(&url, |ui| {
                        ui.group(|ui| {
                            ui.set_min_height(146.0);
                            ui.set_width(ui.available_width());
                            ui.horizontal(|ui| {
                                ui.allocate_ui_with_layout(egui::vec2(192.0, 135.0), egui::Layout::top_down(egui::Align::Center), |ui| {
                                    if let Some(texture) = self.thumbnails.get(&url) {
                                        ui.add(egui::Image::new(texture).fit_to_exact_size(egui::vec2(192.0, 108.0)));
                                    } else {
                                        let (rect, _) = ui.allocate_exact_size(egui::vec2(192.0, 108.0), egui::Sense::hover());
                                        ui.painter().rect_filled(rect, 4.0, ui.visuals().faint_bg_color);
                                        ui.painter().text(rect.center(), egui::Align2::CENTER_CENTER, "Nexus Collection", egui::FontId::proportional(15.0), ui.visuals().weak_text_color());
                                    }
                                    ui.small(format!("{} downloads", entry.total_downloads));
                                });
                                ui.vertical(|ui| {
                                    ui.set_width(ui.available_width());
                                    ui.add(egui::Label::new(egui::RichText::new(safe_message(&entry.name)).size(18.0).strong()).truncate()).on_hover_text(&entry.name);
                                    ui.label(format!("{} · By {}", safe_message(&entry.game.name), safe_message(&entry.user.name)));
                                    if let Some(revision) = &entry.latest_published_revision {
                                        ui.small(format!("{} mods · Revision {} · Updated {}{}", revision.mod_count, revision.revision_number, entry.updated_at.get(..10).unwrap_or(&entry.updated_at), if revision.adult_content { " · Adult content" } else { "" }));
                                    }
                                    ui.add(egui::Label::new(safe_message(&entry.summary)).truncate()).on_hover_text(safe_message(&entry.summary));
                                    ui.add_space(5.0);
                                    ui.horizontal_wrapped(|ui| {
                                        ui.hyperlink_to("View on Nexus", &url);
                                        if entry.can_review_installation() {
                                            if clf3::collection::games::profile(&entry.game.domain_name).is_some_and(|g| g.experimental) { ui.small("Experimental support"); }
                                            if ui.add_enabled(!installing, egui::Button::new("Review installation")).on_disabled_hover_text("Wait for the current collection job to finish or stop it from Install.").clicked() { selected = Some(url.clone()); }
                                        } else {
                                            ui.small(installation_label(entry)).on_hover_text(clf3::collection::games::unsupported_reason(&entry.game.domain_name));
                                        }
                                    });
                                });
                            });
                        });
                    });
                }
            });
        }
        if let Some(page) = next_page {
            self.search.page = page;
            self.fetch(ctx);
        }
        selected
    }
}

fn installation_label(entry: &Entry) -> &'static str {
    if clf3::collection::games::profile(&entry.game.domain_name).is_none() {
        "Browsing only · Installation for this game is not supported yet"
    } else {
        "Browsing only · This revision is not supported for installation"
    }
}
