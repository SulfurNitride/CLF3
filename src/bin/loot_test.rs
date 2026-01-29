//! Quick LOOT test binary - tests with ALL plugins
use std::path::Path;
use std::collections::HashSet;
use anyhow::Result;
use clf3::loot::PluginSorter;
use clf3::games::GameType;
use walkdir::WalkDir;

fn main() -> Result<()> {
    let game_path = Path::new("/home/luke/Games/MO2Debugging/Stock Game");
    let mods_dir = Path::new("/home/luke/Games/MO2Debugging/mods");
    let masterlist = Path::new("/home/luke/.cache/clf3/loot/skyrimse/masterlist.yaml");

    // Discover all plugins in mods folder
    println!("Discovering plugins in mods folder...");
    let mut plugins: HashSet<String> = HashSet::new();
    for entry in WalkDir::new(mods_dir).min_depth(2).max_depth(2) {
        let entry = entry?;
        if entry.file_type().is_file() {
            if let Some(ext) = entry.path().extension() {
                let ext_lower = ext.to_string_lossy().to_lowercase();
                if ext_lower == "esp" || ext_lower == "esm" || ext_lower == "esl" {
                    if let Some(name) = entry.path().file_name() {
                        plugins.insert(name.to_string_lossy().to_string());
                    }
                }
            }
        }
    }

    let mut plugin_list: Vec<String> = plugins.into_iter().collect();
    plugin_list.sort(); // Sort alphabetically first
    println!("Found {} plugins", plugin_list.len());

    println!("\nCreating LOOT sorter...");
    let mut sorter = PluginSorter::new(GameType::SkyrimSE, game_path, mods_dir)?;

    println!("Loading masterlist from: {}", masterlist.display());
    sorter.load_masterlist(masterlist)?;

    println!("\nFirst 10 plugins BEFORE LOOT sort (alphabetical):");
    for (i, p) in plugin_list.iter().take(10).enumerate() {
        println!("  {}: {}", i, p);
    }

    // Find RaceMenu position before
    let racemenu_before = plugin_list.iter().position(|p| p.to_lowercase() == "racemenu.esp");
    println!("\nRaceMenu.esp position BEFORE: {:?}", racemenu_before);

    println!("\nSetting mod paths and loading plugin headers...");
    sorter.set_mod_paths()?;
    let loaded = sorter.load_plugins(&plugin_list)?;
    println!("Loaded {} of {} plugins", loaded.len(), plugin_list.len());

    println!("\nSorting with LOOT...");
    let sorted = sorter.sort_plugins(&loaded)?;

    println!("\nFirst 10 plugins AFTER LOOT sort:");
    for (i, p) in sorted.iter().take(10).enumerate() {
        println!("  {}: {}", i, p);
    }

    // Find RaceMenu position after
    let racemenu_after = sorted.iter().position(|p| p.to_lowercase() == "racemenu.esp");
    println!("\nRaceMenu.esp position AFTER: {:?}", racemenu_after);

    // Find Realistic Natural Skin Tones
    let skin_after = sorted.iter().position(|p| p.to_lowercase() == "realistic natural skin tones.esp");
    println!("Realistic Natural Skin Tones.esp position AFTER: {:?}", skin_after);

    Ok(())
}
