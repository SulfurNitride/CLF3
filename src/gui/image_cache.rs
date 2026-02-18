//! Image cache for modlist thumbnails
//!
//! Caches modlist images to ~/.cache/clf3/images/ with a manifest
//! for tracking URL changes and cleanup of stale images.

use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tracing::{debug, info, warn};

/// Image cache manifest - tracks machine_name -> image_url mappings
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ImageManifest {
    /// Map of machine_name -> image_url
    pub images: HashMap<String, String>,
}

impl ImageManifest {
    /// Load manifest from disk
    pub fn load(path: &Path) -> Self {
        match std::fs::read_to_string(path) {
            Ok(content) => serde_json::from_str(&content).unwrap_or_default(),
            Err(_) => Self::default(),
        }
    }

    /// Save manifest to disk
    pub fn save(&self, path: &Path) -> Result<()> {
        let content = serde_json::to_string_pretty(self)?;
        std::fs::write(path, content)?;
        Ok(())
    }
}

/// Image cache manager
pub struct ImageCache {
    cache_dir: PathBuf,
    manifest_path: PathBuf,
    manifest: ImageManifest,
    client: Client,
}

impl ImageCache {
    /// Create a new image cache
    pub fn new() -> Result<Self> {
        let cache_dir = dirs::cache_dir()
            .context("Could not determine cache directory")?
            .join("clf3")
            .join("images");

        std::fs::create_dir_all(&cache_dir)?;

        let manifest_path = cache_dir.join("manifest.json");
        let manifest = ImageManifest::load(&manifest_path);

        let client = Client::builder()
            .user_agent(concat!("clf3/", env!("CARGO_PKG_VERSION")))
            .timeout(std::time::Duration::from_secs(30))
            .build()?;

        Ok(Self {
            cache_dir,
            manifest_path,
            manifest,
            client,
        })
    }

    /// Get the cache directory path
    pub fn cache_dir(&self) -> &Path {
        &self.cache_dir
    }

    /// Detect image format from magic bytes
    fn detect_image_format(bytes: &[u8]) -> &'static str {
        if bytes.len() < 12 {
            return "bin";
        }

        // PNG: 89 50 4E 47 0D 0A 1A 0A
        if bytes.starts_with(&[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]) {
            return "png";
        }

        // JPEG: FF D8 FF
        if bytes.starts_with(&[0xFF, 0xD8, 0xFF]) {
            return "jpg";
        }

        // WebP: RIFF....WEBP
        if bytes.starts_with(b"RIFF") && bytes.len() >= 12 && &bytes[8..12] == b"WEBP" {
            return "webp";
        }

        // GIF: GIF87a or GIF89a
        if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
            return "gif";
        }

        "bin"
    }

    /// Get the cached image path for a modlist (checks all supported extensions)
    pub fn get_cached_path(&self, machine_name: &str) -> Option<PathBuf> {
        // Check for any supported image extension
        for ext in &["png", "jpg", "webp", "gif"] {
            let path = self.cache_dir.join(format!("{}.{}", machine_name, ext));
            if path.exists() {
                return Some(path);
            }
        }
        None
    }

    /// Check if an image needs to be downloaded/updated
    pub fn needs_download(&self, machine_name: &str, current_url: &str) -> bool {
        // Check if any cached file exists
        if self.get_cached_path(machine_name).is_none() {
            return true;
        }

        // Check if URL has changed
        match self.manifest.images.get(machine_name) {
            Some(cached_url) => cached_url != current_url,
            None => true,
        }
    }

    /// Remove old cached images for a machine name (all extensions)
    fn remove_old_cached(&self, machine_name: &str) {
        for ext in &["png", "jpg", "webp", "gif", "bin"] {
            let path = self.cache_dir.join(format!("{}.{}", machine_name, ext));
            if path.exists() {
                let _ = std::fs::remove_file(&path);
            }
        }
    }

    /// Download an image and cache it with the correct extension
    pub async fn download_image(&mut self, machine_name: &str, url: &str) -> Result<PathBuf> {
        debug!("Downloading image for {} from {}", machine_name, url);

        let response = self
            .client
            .get(url)
            .send()
            .await
            .with_context(|| format!("Failed to fetch image: {}", url))?;

        if !response.status().is_success() {
            anyhow::bail!("Failed to download image: HTTP {}", response.status());
        }

        let bytes = response.bytes().await?;

        // Detect actual format from magic bytes
        let ext = Self::detect_image_format(&bytes);

        // Remove any old cached versions with different extensions
        self.remove_old_cached(machine_name);

        let path = self.cache_dir.join(format!("{}.{}", machine_name, ext));

        let mut file = fs::File::create(&path).await?;
        file.write_all(&bytes).await?;
        file.flush().await?;

        // Update manifest
        self.manifest
            .images
            .insert(machine_name.to_string(), url.to_string());

        debug!(
            "Cached image for {} ({} bytes, format: {})",
            machine_name,
            bytes.len(),
            ext
        );

        Ok(path)
    }

    /// Remove stale images that are no longer in the modlist
    pub fn cleanup_stale(&mut self, current_machine_names: &HashSet<String>) -> Result<usize> {
        let mut removed = 0;

        // Find images in manifest that are no longer in current modlists
        let stale_names: Vec<String> = self
            .manifest
            .images
            .keys()
            .filter(|name| !current_machine_names.contains(*name))
            .cloned()
            .collect();

        for name in stale_names {
            // Remove all possible extensions for this machine name
            for ext in &["png", "jpg", "webp", "gif", "bin"] {
                let path = self.cache_dir.join(format!("{}.{}", name, ext));
                if path.exists() {
                    if let Err(e) = std::fs::remove_file(&path) {
                        warn!("Failed to remove stale image {}: {}", name, e);
                    } else {
                        debug!("Removed stale image: {}.{}", name, ext);
                        removed += 1;
                    }
                }
            }
            self.manifest.images.remove(&name);
        }

        if removed > 0 {
            info!("Cleaned up {} stale images", removed);
        }

        Ok(removed)
    }

    /// Save the manifest to disk
    pub fn save_manifest(&self) -> Result<()> {
        self.manifest.save(&self.manifest_path)
    }

    /// Prepare sync - cleanup stale images and return list of images to download
    /// Call this while holding the lock, then release lock before downloading
    pub fn prepare_sync(
        &mut self,
        modlists: &[(String, String)],
    ) -> Result<(Vec<(String, String)>, usize, usize)> {
        // Build set of current machine names for cleanup
        let current_names: HashSet<String> = modlists.iter().map(|(n, _)| n.clone()).collect();

        // Clean up stale images first
        let removed = self.cleanup_stale(&current_names)?;

        // Filter to only images that need downloading
        let to_download: Vec<(String, String)> = modlists
            .iter()
            .filter(|(name, url)| !url.is_empty() && self.needs_download(name, url))
            .cloned()
            .collect();

        let skipped = modlists.len() - to_download.len();

        Ok((to_download, skipped, removed))
    }

    /// Update manifest with download results
    pub fn finish_sync(&mut self, results: &[(String, String)]) -> Result<()> {
        for (name, url) in results {
            self.manifest.images.insert(name.clone(), url.clone());
        }
        self.save_manifest()
    }

    /// Get client and cache_dir for standalone downloads
    pub fn get_download_context(&self) -> (Client, PathBuf) {
        (self.client.clone(), self.cache_dir.clone())
    }
}

/// Detect image format from magic bytes (standalone for parallel use)
fn detect_format(bytes: &[u8]) -> &'static str {
    if bytes.len() < 12 {
        return "bin";
    }
    if bytes.starts_with(&[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]) {
        return "png";
    }
    if bytes.starts_with(&[0xFF, 0xD8, 0xFF]) {
        return "jpg";
    }
    if bytes.starts_with(b"RIFF") && bytes.len() >= 12 && &bytes[8..12] == b"WEBP" {
        return "webp";
    }
    if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        return "gif";
    }
    "bin"
}

/// Download a single image (standalone for parallel use)
async fn download_single_image(
    client: &Client,
    cache_dir: &Path,
    machine_name: &str,
    url: &str,
) -> Result<PathBuf> {
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("Failed to fetch image: {}", url))?;

    if !response.status().is_success() {
        anyhow::bail!("Failed to download image: HTTP {}", response.status());
    }

    let bytes = response.bytes().await?;
    let ext = detect_format(&bytes);

    // Remove old versions with different extensions
    for old_ext in &["png", "jpg", "webp", "gif", "bin"] {
        let old_path = cache_dir.join(format!("{}.{}", machine_name, old_ext));
        let _ = std::fs::remove_file(&old_path);
    }

    let path = cache_dir.join(format!("{}.{}", machine_name, ext));
    let mut file = fs::File::create(&path).await?;
    file.write_all(&bytes).await?;
    file.flush().await?;

    debug!(
        "Downloaded {} ({} bytes, {})",
        machine_name,
        bytes.len(),
        ext
    );
    Ok(path)
}

/// Download images in parallel (standalone, doesn't need cache lock)
/// Returns list of successfully downloaded (machine_name, url) pairs
pub async fn download_images_parallel(
    client: &Client,
    cache_dir: &Path,
    to_download: Vec<(String, String)>,
) -> (Vec<(String, String)>, usize) {
    use futures::stream::{self, StreamExt};

    if to_download.is_empty() {
        return (Vec::new(), 0);
    }

    // Use system thread count for concurrency
    let concurrency = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);

    info!(
        "Downloading {} images with {} parallel connections...",
        to_download.len(),
        concurrency
    );

    let results: Vec<Result<(String, String), (String, String)>> = stream::iter(to_download)
        .map(|(name, url)| {
            let client = client.clone();
            let cache_dir = cache_dir.to_path_buf();
            async move {
                match download_single_image(&client, &cache_dir, &name, &url).await {
                    Ok(_) => Ok((name, url)),
                    Err(e) => {
                        warn!("Failed to download image for {}: {}", name, e);
                        Err((name, url))
                    }
                }
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;

    let mut succeeded = Vec::new();
    let mut failed = 0;

    for result in results {
        match result {
            Ok(pair) => succeeded.push(pair),
            Err(_) => failed += 1,
        }
    }

    (succeeded, failed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_roundtrip() {
        let mut manifest = ImageManifest::default();
        manifest.images.insert(
            "test_modlist".into(),
            "https://example.com/image.webp".into(),
        );

        let json = serde_json::to_string(&manifest).unwrap();
        let loaded: ImageManifest = serde_json::from_str(&json).unwrap();

        assert_eq!(
            loaded.images.get("test_modlist"),
            Some(&"https://example.com/image.webp".into())
        );
    }

    #[test]
    fn test_detect_format() {
        // PNG magic bytes
        let png = [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0, 0, 0, 0];
        assert_eq!(ImageCache::detect_image_format(&png), "png");

        // JPEG magic bytes
        let jpg = [0xFF, 0xD8, 0xFF, 0xE0, 0, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(ImageCache::detect_image_format(&jpg), "jpg");

        // WebP magic bytes
        let webp = b"RIFF\x00\x00\x00\x00WEBP";
        assert_eq!(ImageCache::detect_image_format(webp), "webp");
    }
}
