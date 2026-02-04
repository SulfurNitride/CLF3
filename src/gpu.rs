//! GPU detection and INI fixing
//!
//! Detects the system GPU using Vulkan and fixes game INI files
//! that have incorrect `sD3DDevice` settings.

use anyhow::{Context, Result};
use std::path::Path;
use tracing::{info, warn};
use walkdir::WalkDir;

/// Get the primary GPU device name as seen by Vulkan/DXVK
///
/// Returns the device name in the format games expect, e.g.:
/// "AMD Radeon RX 9070 XT (RADV GFX1201)"
/// "NVIDIA GeForce RTX 4080"
pub fn get_gpu_device_name() -> Result<String> {
    // Use wgpu to get the Vulkan adapter name
    let instance = wgpu::Instance::new(&wgpu::InstanceDescriptor {
        backends: wgpu::Backends::VULKAN,
        ..Default::default()
    });

    let adapter = pollster::block_on(instance.request_adapter(&wgpu::RequestAdapterOptions {
        power_preference: wgpu::PowerPreference::HighPerformance,
        compatible_surface: None,
        force_fallback_adapter: false,
    }))
    .context("No Vulkan adapter found")?;

    let info = adapter.get_info();

    info!("Detected GPU: {} ({})", info.name, info.backend.to_str());

    Ok(info.name)
}

/// Get all available GPU device names
pub fn get_all_gpu_names() -> Vec<String> {
    let instance = wgpu::Instance::new(&wgpu::InstanceDescriptor {
        backends: wgpu::Backends::VULKAN,
        ..Default::default()
    });

    let adapters: Vec<wgpu::Adapter> = pollster::block_on(instance.enumerate_adapters(wgpu::Backends::VULKAN));

    adapters
        .into_iter()
        .map(|a| {
            let info = a.get_info();
            info.name
        })
        .collect()
}

/// Fix sD3DDevice in all INI files under the given directory
///
/// Searches for INI files containing `sD3DDevice=` and updates
/// them to use the correct GPU name.
///
/// Returns the number of files fixed.
pub fn fix_ini_gpu_settings(install_dir: &Path) -> Result<usize> {
    let gpu_name = get_gpu_device_name()?;
    fix_ini_gpu_settings_with_name(install_dir, &gpu_name)
}

/// Fix sD3DDevice in all INI files using a specific GPU name
pub fn fix_ini_gpu_settings_with_name(install_dir: &Path, gpu_name: &str) -> Result<usize> {
    info!("Fixing sD3DDevice in INI files to: {}", gpu_name);

    let mut fixed_count = 0;

    // Search for INI files in common locations
    let search_paths = [
        install_dir.join("profiles"),
        install_dir.join("mods"),
        install_dir.join("Stock Game"),
        install_dir.join("Game Root"),
    ];

    for search_path in &search_paths {
        if !search_path.exists() {
            continue;
        }

        for entry in WalkDir::new(search_path)
            .follow_links(true)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            let path = entry.path();

            // Only process .ini files
            if path.extension().map(|e| e.to_ascii_lowercase()) != Some("ini".into()) {
                continue;
            }

            // Check if this INI has sD3DDevice
            if let Ok(fixed) = fix_single_ini_file(path, gpu_name) {
                if fixed {
                    fixed_count += 1;
                }
            }
        }
    }

    info!("Fixed sD3DDevice in {} INI files", fixed_count);
    Ok(fixed_count)
}

/// Fix sD3DDevice in a single INI file
///
/// Returns true if the file was modified, false otherwise.
fn fix_single_ini_file(path: &Path, gpu_name: &str) -> Result<bool> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read: {}", path.display()))?;

    // Check if file contains sD3DDevice
    if !content.to_lowercase().contains("sd3ddevice") {
        return Ok(false);
    }

    let mut modified = false;
    let mut new_lines: Vec<String> = Vec::new();

    for line in content.lines() {
        let trimmed = line.trim();

        // Check for sD3DDevice= (case insensitive)
        if trimmed.to_lowercase().starts_with("sd3ddevice=") {
            // Extract current value
            let current_value = trimmed
                .splitn(2, '=')
                .nth(1)
                .unwrap_or("")
                .trim()
                .trim_matches('"');

            // Check if it needs updating
            if current_value != gpu_name {
                // Preserve the original key casing
                let key = trimmed.splitn(2, '=').next().unwrap_or("sD3DDevice");
                let new_line = format!("{}={}", key, gpu_name);

                info!("  {} : {} -> {}", path.display(), current_value, gpu_name);
                new_lines.push(new_line);
                modified = true;
            } else {
                new_lines.push(line.to_string());
            }
        } else {
            new_lines.push(line.to_string());
        }
    }

    if modified {
        let new_content = new_lines.join("\n");
        // Preserve original line ending style
        let new_content = if content.contains("\r\n") {
            new_content.replace('\n', "\r\n")
        } else {
            new_content
        };

        std::fs::write(path, new_content)
            .with_context(|| format!("Failed to write: {}", path.display()))?;
    }

    Ok(modified)
}

/// Common INI files that may contain sD3DDevice
pub const COMMON_INI_FILES: &[&str] = &[
    "Fallout.ini",
    "FalloutPrefs.ini",
    "Fallout4.ini",
    "Fallout4Prefs.ini",
    "FalloutCustom.ini",
    "Skyrim.ini",
    "SkyrimPrefs.ini",
    "SkyrimCustom.ini",
    "Oblivion.ini",
];

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    #[test]
    fn test_fix_ini_with_sd3ddevice() {
        let temp = TempDir::new().unwrap();
        let profiles = temp.path().join("profiles").join("Default");
        std::fs::create_dir_all(&profiles).unwrap();

        let ini_path = profiles.join("Fallout.ini");
        let mut f = std::fs::File::create(&ini_path).unwrap();
        writeln!(f, "[Display]").unwrap();
        writeln!(f, "sD3DDevice=NVIDIA GeForce RTX 4080").unwrap();
        writeln!(f, "iSize H=1080").unwrap();

        let result = fix_ini_gpu_settings_with_name(temp.path(), "AMD Radeon RX 9070 XT").unwrap();
        assert_eq!(result, 1);

        let content = std::fs::read_to_string(&ini_path).unwrap();
        assert!(content.contains("sD3DDevice=AMD Radeon RX 9070 XT"));
        assert!(!content.contains("NVIDIA"));
    }

    #[test]
    fn test_no_change_when_correct() {
        let temp = TempDir::new().unwrap();
        let profiles = temp.path().join("profiles").join("Default");
        std::fs::create_dir_all(&profiles).unwrap();

        let ini_path = profiles.join("Fallout.ini");
        let mut f = std::fs::File::create(&ini_path).unwrap();
        writeln!(f, "[Display]").unwrap();
        writeln!(f, "sD3DDevice=AMD Radeon RX 9070 XT").unwrap();

        let result = fix_ini_gpu_settings_with_name(temp.path(), "AMD Radeon RX 9070 XT").unwrap();
        assert_eq!(result, 0);
    }

    #[test]
    fn test_skip_files_without_sd3ddevice() {
        let temp = TempDir::new().unwrap();
        let profiles = temp.path().join("profiles").join("Default");
        std::fs::create_dir_all(&profiles).unwrap();

        let ini_path = profiles.join("SomeOther.ini");
        let mut f = std::fs::File::create(&ini_path).unwrap();
        writeln!(f, "[General]").unwrap();
        writeln!(f, "sLanguage=ENGLISH").unwrap();

        let result = fix_ini_gpu_settings_with_name(temp.path(), "AMD Radeon").unwrap();
        assert_eq!(result, 0);
    }
}
