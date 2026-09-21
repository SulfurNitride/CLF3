//! Standalone Collections view. Long-running I/O stays in application services.
use clf3::collection::{
    progress::{safe_message, WorkerEvent},
    publish::PublicationReport,
    worker::read_json,
};
use clf3::collection_app::{self as app, Event, Inputs, MissingArtifact, Prepared};
use eframe::egui;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::mpsc::{Receiver, SyncSender},
};
use tokio_util::sync::CancellationToken;
mod catalog;

pub struct CollectionsView {
    selected_game: String,
    detected_games: BTreeMap<String, (PathBuf, PathBuf)>,
    catalog: catalog::CatalogView,
    browse: bool,
    inputs: Inputs,
    prepared: Option<Prepared>,
    receiver: Option<Receiver<Event>>,
    cancellation: CancellationToken,
    busy: bool,
    dirty: bool,
    phase: String,
    item: String,
    progress: Option<f32>,
    counters: String,
    error: Option<String>,
    missing: Vec<MissingArtifact>,
    completed: Option<PublicationReport>,
    show_report: bool,
    filter: String,
    recent: Option<PathBuf>,
    executable: Option<PathBuf>,
}

impl Drop for CollectionsView {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

fn size(bytes: u64) -> String {
    format!("{:.1} GiB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
}

impl CollectionsView {
    pub fn new(
        detected_games: BTreeMap<String, (PathBuf, PathBuf)>,
        downloads: &str,
        install: &str,
    ) -> Self {
        let (game, profile_ini) = detected_games
            .get("skyrimspecialedition")
            .cloned()
            .unwrap_or_default();
        Self {
            selected_game: "skyrimspecialedition".into(),
            detected_games,
            catalog: Default::default(),
            browse: true,
            inputs: Inputs {
                game,
                profile_ini,
                output: PathBuf::from(install),
                cache: if downloads.is_empty() {
                    app::cache_root().join("downloads")
                } else {
                    PathBuf::from(downloads)
                },
                ..Default::default()
            },
            prepared: None,
            receiver: None,
            cancellation: CancellationToken::new(),
            busy: false,
            dirty: false,
            phase: "Load a collection to begin".into(),
            item: String::new(),
            progress: None,
            counters: String::new(),
            error: None,
            missing: Vec::new(),
            completed: None,
            show_report: false,
            filter: String::new(),
            recent: read_json(&app::recent_path(), 8192).ok(),
            executable: std::env::current_exe().ok(),
        }
    }

    pub fn is_busy(&self) -> bool {
        self.busy
    }

    fn task(
        &mut self,
        phase: &str,
        ctx: &egui::Context,
        work: impl FnOnce(&CancellationToken, &SyncSender<Event>) -> anyhow::Result<()> + Send + 'static,
    ) {
        if self.busy {
            return;
        }
        let (sender, receiver) = std::sync::mpsc::sync_channel(64);
        self.receiver = Some(receiver);
        self.cancellation = CancellationToken::new();
        let token = self.cancellation.clone();
        let ctx = ctx.clone();
        self.busy = true;
        self.phase = phase.into();
        self.item.clear();
        self.counters.clear();
        self.progress = None;
        self.error = None;
        std::thread::spawn(move || {
            match work(&token, &sender) {
                Ok(()) => {}
                Err(_) if token.is_cancelled() => {
                    let _ = sender.send(Event::Cancelled);
                }
                Err(error) => {
                    let _ = sender.send(Event::Failed(safe_message(&format!("{error:#}"))));
                }
            }
            ctx.request_repaint();
        });
    }

    pub fn poll(&mut self, ctx: &egui::Context) {
        self.catalog.poll(ctx);
        let incoming: Vec<_> = self
            .receiver
            .as_ref()
            .map(|r| r.try_iter().take(128).collect())
            .unwrap_or_default();
        for event in incoming {
            match event {
                Event::Prepared(prepared) => {
                    self.selected_game = prepared.plan.domain.clone();
                    self.inputs = prepared.inputs.clone();
                    self.recent = Some(prepared.job.clone());
                    self.completed = read_json::<PublicationReport>(
                        &prepared.job.join("completed.json"),
                        1024 * 1024,
                    )
                    .ok()
                    .filter(|r| {
                        r.package_sha256 == prepared.plan.package_sha256
                            && r.output == prepared.inputs.output
                            && r.output.join(".collection/report.json").is_file()
                    });
                    self.prepared = Some(*prepared);
                    self.dirty = false;
                    self.busy = false;
                    self.missing.clear();
                    self.phase = if self.completed.is_some() {
                        "Installation complete"
                    } else {
                        "Review the collection"
                    }
                    .into();
                }
                Event::Worker(WorkerEvent::Progress {
                    phase,
                    completed,
                    total,
                    item,
                }) => {
                    self.phase = phase;
                    let label = self
                        .prepared
                        .as_ref()
                        .and_then(|p| p.plan.members.iter().find(|m| m.id == item))
                        .map(|m| m.name.as_str())
                        .unwrap_or(&item);
                    self.item = safe_message(label);
                    self.progress = (total > 0).then(|| (completed as f32 / total as f32).min(1.0));
                    self.counters = if total > 0 {
                        format!("{completed} / {total}")
                    } else {
                        String::new()
                    };
                }
                Event::Downloads {
                    verified,
                    total,
                    bytes,
                    total_bytes,
                    item,
                } => {
                    self.phase = "Downloading and checking exact archives".into();
                    self.item = safe_message(&item);
                    self.progress = Some(if total > 0 {
                        verified as f32 / total as f32
                    } else {
                        1.0
                    });
                    self.counters = format!(
                        "{verified} / {total} archives verified · {} / {}",
                        size(bytes),
                        size(total_bytes)
                    );
                }
                Event::NeedsFiles(missing) => {
                    self.missing = missing;
                    self.busy = false;
                    self.phase = "Some exact files need attention".into();
                    self.item.clear();
                }
                Event::Finished(report) => {
                    self.completed = Some(*report);
                    self.busy = false;
                    self.phase = "Installation complete".into();
                    self.progress = Some(1.0);
                    self.item.clear();
                    self.missing.clear();
                }
                Event::Failed(error) => {
                    self.error = Some(error);
                    self.busy = false;
                    self.phase = "Installation needs attention".into();
                }
                Event::Cancelled => {
                    self.busy = false;
                    self.phase = "Stopped — verified work is saved".into();
                    self.item.clear();
                }
                _ => {}
            }
        }
        if self.busy {
            ctx.request_repaint_after(std::time::Duration::from_millis(100));
        }
    }

    fn select_game(&mut self, domain: &str) {
        if self.selected_game == domain {
            return;
        }
        self.selected_game = domain.into();
        let (game, ini) = self.detected_games.get(domain).cloned().unwrap_or_default();
        self.inputs.game = game;
        self.inputs.profile_ini = ini;
        self.inputs.masterlist.clear();
        self.inputs.output.clear();
        self.inputs.selected_optional.clear();
        self.inputs.all_optional = false;
        self.dirty = true;
    }

    fn load(&mut self, ctx: &egui::Context, key: &str) {
        if let Ok(locator) = clf3::collection::url::parse_collection_url(self.inputs.source.trim())
        {
            self.select_game(&locator.domain);
        }
        let mut inputs = self.inputs.clone();
        let previous = self.prepared.clone();
        if previous
            .as_ref()
            .is_some_and(|p| p.inputs.source != inputs.source)
        {
            inputs.selected_optional.clear();
            inputs.all_optional = false;
        }
        let key = key.to_owned();
        self.completed = None;
        self.task(
            "Loading and planning collection",
            ctx,
            move |token, events| {
                let prepared = app::prepare(inputs, previous.as_ref(), &key, token)?;
                if token.is_cancelled() {
                    anyhow::bail!("Cancelled");
                }
                let _ = events.send(Event::Prepared(Box::new(prepared)));
                Ok(())
            },
        );
    }

    fn install(&mut self, ctx: &egui::Context, key: &str) {
        let Some(prepared) = self.prepared.clone() else {
            return;
        };
        if self.dirty || self.inputs != prepared.inputs {
            self.error = Some("Review the changed options before installing".into());
            return;
        }
        let key = key.to_owned();
        self.missing.clear();
        let executable = self.executable.clone();
        self.task("Checking installation paths", ctx, move |token, events| {
            let executable = executable
                .ok_or_else(|| anyhow::anyhow!("Could not locate the CLF3 executable"))?;
            app::install(&prepared, &key, &executable, token, events)
        });
    }

    /// Returns true when the user requests the existing account Settings tab.
    pub fn render(&mut self, ctx: &egui::Context, key: &str) -> bool {
        let mut settings = false;
        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("Collections");
            ui.horizontal(|ui| {
                ui.selectable_value(&mut self.browse, true, "Browse collections");
                ui.selectable_value(&mut self.browse, false, "Install");
                if self.busy { ui.separator(); ui.spinner(); ui.label(&self.phase); }
            });
            ui.separator();
            if self.browse {
                if let Some(url) = self.catalog.render(ui, ctx, self.busy) {
                    self.inputs.source = url;
                    self.dirty = true;
                    self.browse = false;
                    self.load(ctx, key);
                }
                return;
            }
            egui::ScrollArea::vertical().id_salt("collections_page").show(ui, |ui| {
                ui.heading("Install collection");
                ui.label("Install a pinned Nexus collection into its own folder.");
                ui.horizontal(|ui| {
                    let game = clf3::collection::games::profile(&self.selected_game);
                    ui.small(game.map_or("Unsupported game", |g| g.name));
                    ui.separator();
                    ui.small(if key.is_empty() { "Nexus account not configured" } else { "Saved Nexus key available" });
                    if ui.link("Account settings").clicked() { settings=true; }
                });
                ui.add_space(10.0);
                ui.add_enabled_ui(!self.busy, |ui| {
                    let mut selected_game = self.selected_game.clone();
                    egui::ComboBox::from_id_salt("collection_install_game").selected_text(clf3::collection::games::profile(&selected_game).map_or("Choose game", |g| g.name)).show_ui(ui, |ui| {
                        for profile in clf3::collection::games::PROFILES {
                            ui.selectable_value(&mut selected_game, profile.domain.into(), format!("{}{}", profile.name, if profile.experimental { " (experimental)" } else { "" }));
                        }
                    });
                    self.select_game(&selected_game);
                    ui.horizontal(|ui| {
                        let changed=ui.add(egui::TextEdit::singleline(&mut self.inputs.source).hint_text("Paste a Nexus collection URL or choose a local package").desired_width((ui.available_width()-230.0).max(200.0))).changed();
                        self.dirty |= changed;
                        if ui.button("Open package…").clicked() {
                            if let Some(path)=rfd::FileDialog::new().add_filter("Collection package", &["7z","zip","rar"]).pick_file() { self.inputs.source=path.to_string_lossy().into_owned(); self.dirty=true; }
                        }
                        if ui.add_enabled(!self.inputs.source.trim().is_empty(),egui::Button::new(if self.dirty && self.prepared.is_some() { "Review changes" } else { "Load collection" })).clicked() { self.load(ctx,key); }
                    });
                    if let Some(job)=self.recent.clone() {
                        ui.horizontal(|ui| {
                            if ui.button("Restore last job").clicked() {
                                self.task("Restoring saved job",ctx,move |_,events| { let _=events.send(Event::Prepared(Box::new(Prepared::restore(&job)?))); Ok(()) });
                            }
                            ui.small("Restore pinned choices and resume verified work.");
                        });
                    }
                });
                ui.add_space(10.0);
                egui::CollapsingHeader::new("Installation locations").default_open(true).show(ui,|ui| {
                    ui.add_enabled_ui(!self.busy,|ui| {
                        self.dirty |= path_row(ui,"Source game",&mut self.inputs.game,true);
                        self.dirty |= path_row(ui,"Download cache",&mut self.inputs.cache,true);
                        self.dirty |= path_row(ui,"Install folder",&mut self.inputs.output,true);
                        ui.small("Choose a new install folder. Collection Mods and Mod Additions headings preserve the collection's order.");
                        egui::CollapsingHeader::new("Advanced paths").show(ui,|ui| {
                            self.dirty |= path_row(ui,"Profile INI source",&mut self.inputs.profile_ini,true);
                            self.dirty |= path_row(ui,"LOOT masterlist",&mut self.inputs.masterlist,false);
                            ui.small("Blank masterlist uses pinned rules for the collection's game. INI edits are applied to private profile copies.");
                        });
                    });
                });
                if let Some(prepared)=self.prepared.as_ref() {
                    ui.separator();
                    ui.heading(&prepared.plan.name);
                    if !prepared.author.is_empty() { ui.small(format!("By {}", prepared.author)); }
                    let count=prepared.plan.members.iter().filter(|m| m.selected).count();
                    let total_bytes: u64=prepared.plan.artifacts.iter().filter(|a| prepared.plan.members.iter().any(|m| m.selected && m.artifact_id==a.id)).filter_map(|a| a.expected_size).sum();
                    let revision=prepared.plan.locator.as_ref().and_then(|l| l.revision).map(|n|format!("Revision {n}")).unwrap_or_else(|| "Local package".into());
                    ui.label(format!("{revision} · {count} selected mods · {} archives · Source runtime {}",size(total_bytes),prepared.plan.game_version.as_deref().unwrap_or("unknown")));
                    for blocker in &prepared.plan.blockers {
                        let name = blocker.member_id.as_ref().and_then(|id| prepared.plan.members.iter().find(|m| &m.id == id)).map(|m| m.name.as_str());
                        let message = name.map_or_else(|| blocker.message.clone(), |name| format!("{name}: {}", blocker.message));
                        ui.colored_label(egui::Color32::LIGHT_RED,safe_message(&message));
                    }
                    if !prepared.plan.warnings.is_empty() {
                        egui::CollapsingHeader::new(format!("{} planning notes",prepared.plan.warnings.len())).show(ui,|ui| { for warning in &prepared.plan.warnings { ui.label(safe_message(&warning.message)); } });
                    }
                    let options:Vec<_>=prepared.plan.members.iter().filter(|m| m.optional).map(|m|(m.id.clone(),m.name.clone())).collect();
                    let members:Vec<_>=prepared.plan.members.iter().filter(|m| self.filter.is_empty() || m.name.to_lowercase().contains(&self.filter.to_lowercase())).map(|m|(m.name.clone(),m.version.clone(),m.optional,m.selected)).collect();
                    if !options.is_empty() {
                        egui::CollapsingHeader::new(format!("Optional mods ({})",options.len())).show(ui,|ui| {
                            ui.add_enabled_ui(!self.busy,|ui| {
                                self.dirty |= ui.checkbox(&mut self.inputs.all_optional,"Include all optional mods").changed();
                                egui::ScrollArea::vertical().id_salt("collection_optionals").max_height(190.0).show_rows(ui,24.0,options.len(),|ui,range| {
                                    for (id,name) in &options[range] {
                                        let mut selected=self.inputs.all_optional || self.inputs.selected_optional.contains(id);
                                        if ui.checkbox(&mut selected,name).changed() {
                                            if self.inputs.all_optional { self.inputs.all_optional=false; self.inputs.selected_optional=options.iter().map(|(id,_)|id.clone()).collect(); }
                                            if selected { self.inputs.selected_optional.insert(id.clone()); } else { self.inputs.selected_optional.remove(id); }
                                            self.dirty=true;
                                        }
                                    }
                                });
                            });
                        });
                    }
                    egui::CollapsingHeader::new("Collection contents").show(ui,|ui| {
                        ui.add(egui::TextEdit::singleline(&mut self.filter).hint_text("Filter mods"));
                        egui::ScrollArea::vertical().id_salt("collection_members").max_height(220.0).show_rows(ui,23.0,members.len(),|ui,range| {
                            for (name,version,optional,selected) in &members[range] { ui.label(format!("{} {}  {}{}",if *selected { "✓" } else { "○" },name,version,if *optional { " · optional" } else { "" })); }
                        });
                    });
                }
                ui.separator();
                if self.dirty && self.prepared.is_some() { ui.colored_label(egui::Color32::YELLOW,"Options changed. Review changes before installing."); }
                ui.horizontal(|ui| {
                    let ready=self.prepared.as_ref().is_some_and(|p| p.plan.blockers.is_empty()) && !self.dirty && !self.busy && self.completed.is_none();
                    if ui.add_enabled(ready,egui::Button::new("Install / resume").min_size(egui::vec2(150.0,32.0))).clicked() { self.install(ctx,key); }
                    if self.busy && ui.add_enabled(!self.cancellation.is_cancelled(),egui::Button::new("Cancel")).clicked() { self.cancellation.cancel(); }
                    ui.strong(if self.busy && self.cancellation.is_cancelled() { "Stopping safely…" } else { &self.phase });
                    if self.busy { ui.spinner(); }
                });
                if self.busy || self.progress.is_some() {
                    if let Some(fraction)=self.progress { ui.add(egui::ProgressBar::new(fraction).text(&self.counters)); }
                    if !self.item.is_empty() { ui.label(&self.item); }
                }
                if let Some(error)=&self.error { ui.colored_label(egui::Color32::LIGHT_RED,error); }
                if !self.missing.is_empty() {
                    ui.label("Choose the requested archives below, then Install / resume. Each file must match the collection's exact size and hash.");
                    egui::ScrollArea::vertical().id_salt("collection_missing").max_height(300.0).show_rows(ui,85.0,self.missing.len(),|ui,range| {
                        for missing in &self.missing[range] {
                            ui.group(|ui| {
                                ui.strong(&missing.name); ui.small(&missing.reason);
                                ui.horizontal(|ui| {
                                    if let Some(page)=&missing.page { if ui.link("Download page").clicked() { open_path(Path::new(page)); } }
                                    if ui.button("Select archive…").clicked() {
                                        if let Some(path)=rfd::FileDialog::new().pick_file() {
                                            if let Some(prepared)=&mut self.prepared {
                                                prepared.manual_archives.insert(missing.id.clone(),path);
                                                if let Err(e)=prepared.save() { self.error=Some(e.to_string()); }
                                            }
                                        }
                                    }
                                    if let Some(path)=self.prepared.as_ref().and_then(|p|p.manual_archives.get(&missing.id)) { ui.small(format!("Selected: {}",path.file_name().unwrap_or_default().to_string_lossy())); }
                                });
                            });
                        }
                    });
                }
                if let Some(report)=&self.completed {
                    ui.add_space(8.0);
                    ui.colored_label(egui::Color32::LIGHT_GREEN,format!("{} mods installed · {} files verified · {} enabled plugins",report.installed_members,report.verified_mod_files,report.enabled_plugins));
                    ui.label(report.output.to_string_lossy());
                    ui.small("Game launch has not been tested.");
                    ui.horizontal(|ui| {
                        if ui.button("Open Folder").clicked() { open_path(&report.output); }
                        ui.toggle_value(&mut self.show_report,"Installation report");
                    });
                    if self.show_report { ui.monospace(serde_json::to_string_pretty(report).unwrap_or_default()); }
                }
            });
        });
        settings
    }
}

fn path_row(ui: &mut egui::Ui, label: &str, path: &mut PathBuf, directory: bool) -> bool {
    let mut changed = false;
    ui.horizontal(|ui| {
        ui.add_sized([130.0, 22.0], egui::Label::new(label));
        let mut value = path.to_string_lossy().into_owned();
        if ui
            .add(
                egui::TextEdit::singleline(&mut value)
                    .desired_width((ui.available_width() - 50.0).max(100.0)),
            )
            .changed()
        {
            *path = PathBuf::from(value);
            changed = true;
        }
        if ui.button("…").clicked() {
            let mut dialog = rfd::FileDialog::new();
            if path.is_dir() {
                dialog = dialog.set_directory(&*path);
            }
            let selected = if directory {
                dialog.pick_folder()
            } else {
                dialog.pick_file()
            };
            if let Some(selected) = selected {
                *path = selected;
                changed = true;
            }
        }
    });
    changed
}

fn open_path(path: &Path) {
    let _ = std::process::Command::new("xdg-open")
        .arg(path)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();
}
