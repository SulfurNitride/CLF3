//! Modlist browser for fetching and displaying available modlists from Wabbajack repositories.
#![allow(dead_code)] // Used by lib crate (GUI), not by binary crate

use crate::downloaders::wabbajack_cdn::WabbajackCdnDownloader;
use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use tracing::{debug, info, warn};

const REPOSITORIES_URL: &str =
    "https://raw.githubusercontent.com/wabbajack-tools/mod-lists/master/repositories.json";
const FEATURED_URL: &str =
    "https://raw.githubusercontent.com/wabbajack-tools/mod-lists/master/featured_lists.json";

/// Download metadata for a modlist
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DownloadMetadata {
    #[serde(rename = "Hash", default)]
    pub hash: String,
    #[serde(rename = "Size", default)]
    pub size: u64,
    #[serde(rename = "NumberOfArchives", default)]
    pub number_of_archives: u32,
    #[serde(rename = "SizeOfArchives", default)]
    pub size_of_archives: u64,
    #[serde(rename = "NumberOfInstalledFiles", default)]
    pub number_of_installed_files: u32,
    #[serde(rename = "SizeOfInstalledFiles", default)]
    pub size_of_installed_files: u64,
    #[serde(rename = "TotalSize", default)]
    pub total_size: u64,
}

/// Links associated with a modlist
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ModlistLinks {
    #[serde(default)]
    pub image: String,
    #[serde(default)]
    pub readme: String,
    #[serde(default)]
    pub download: String,
    #[serde(rename = "machineURL", default)]
    pub machine_url: String,
    #[serde(rename = "discordURL", default)]
    pub discord_url: String,
    #[serde(rename = "websiteURL", default)]
    pub website_url: String,
}

/// Modlist metadata from the Wabbajack API
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ModlistMetadata {
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub author: String,
    #[serde(default)]
    pub game: String,
    #[serde(default)]
    pub official: bool,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub nsfw: bool,
    #[serde(default)]
    pub utility_list: bool,
    #[serde(default)]
    pub image_contains_title: bool,
    #[serde(default)]
    pub force_down: bool,
    #[serde(default)]
    pub links: Option<ModlistLinks>,
    #[serde(rename = "download_metadata", alias = "DownloadMetadata", default)]
    pub download_metadata: Option<DownloadMetadata>,
    #[serde(default)]
    pub version: String,
    // Fields we populate ourselves (included in cache for quick loading)
    #[serde(default)]
    pub repository_name: String,
    #[serde(default)]
    pub machine_name: String,
}

impl ModlistMetadata {
    /// Get the download URL for this modlist
    pub fn download_url(&self) -> Option<&str> {
        self.links
            .as_ref()
            .map(|l| l.download.as_str())
            .filter(|s| !s.is_empty())
    }

    pub fn image_url(&self) -> Option<&str> {
        self.links
            .as_ref()
            .map(|l| l.image.as_str())
            .filter(|s| !s.is_empty())
    }

    pub fn readme_url(&self) -> Option<&str> {
        self.links
            .as_ref()
            .map(|l| l.readme.as_str())
            .filter(|s| !s.is_empty())
    }

    pub fn download_size(&self) -> u64 {
        self.download_metadata
            .as_ref()
            .map(|d| d.size_of_archives)
            .unwrap_or(0)
    }

    pub fn installed_size(&self) -> u64 {
        self.download_metadata
            .as_ref()
            .map(|d| d.size_of_installed_files)
            .unwrap_or(0)
    }

    pub fn matches_game(&self, game_filter: &str) -> bool {
        game_filter.is_empty() || self.game.eq_ignore_ascii_case(game_filter)
    }

    pub fn matches_query(&self, query: &str) -> bool {
        if query.is_empty() {
            return true;
        }
        let q = query.to_lowercase();
        self.title.to_lowercase().contains(&q)
            || self.author.to_lowercase().contains(&q)
            || self.description.to_lowercase().contains(&q)
            || self.game.to_lowercase().contains(&q)
    }

    pub fn is_available(&self) -> bool {
        !self.force_down && self.download_url().is_some()
    }
}

/// Browser for fetching and searching modlists
pub struct ModlistBrowser {
    client: Client,
    modlists: Vec<ModlistMetadata>,
    featured_names: Vec<String>,
}

impl ModlistBrowser {
    /// Create a new modlist browser
    pub fn new() -> Result<Self> {
        let client = Client::builder()
            .user_agent(concat!("clf3/", env!("CARGO_PKG_VERSION")))
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .context("Failed to create HTTP client")?;

        Ok(Self {
            client,
            modlists: Vec::new(),
            featured_names: Vec::new(),
        })
    }

    /// Fetch all modlists from the Wabbajack repositories
    pub async fn fetch_modlists(&mut self) -> Result<&[ModlistMetadata]> {
        info!("Fetching modlist repositories...");

        // Fetch the repositories index
        let response = self
            .client
            .get(REPOSITORIES_URL)
            .send()
            .await
            .context("Failed to fetch repositories.json")?;

        let repos: HashMap<String, String> = response
            .json()
            .await
            .context("Failed to parse repositories.json")?;

        info!("Found {} repositories", repos.len());

        // Fetch modlists from each repository
        let mut all_modlists = Vec::new();

        for (repo_name, repo_url) in repos {
            debug!("Fetching modlists from repository: {}", repo_name);

            let repo_name_clone = repo_name.clone();
            match self.client.get(&repo_url).send().await {
                Ok(response) if response.status().is_success() => {
                    // Each repository URL returns an ARRAY of modlists
                    match response.json::<Vec<ModlistMetadata>>().await {
                        Ok(mut modlists) => {
                            // Set repository name and machine_name for each modlist
                            for modlist in &mut modlists {
                                modlist.repository_name = repo_name_clone.clone();
                                if let Some(links) = &modlist.links {
                                    modlist.machine_name = links.machine_url.clone();
                                }
                            }
                            debug!("  {} modlists from {}", modlists.len(), repo_name_clone);
                            all_modlists.extend(modlists);
                        }
                        Err(e) => {
                            warn!("Failed to parse modlists from {}: {}", repo_name_clone, e);
                        }
                    }
                }
                Ok(response) => {
                    warn!(
                        "Failed to fetch {}: HTTP {}",
                        repo_name_clone,
                        response.status()
                    );
                }
                Err(e) => {
                    warn!("Failed to fetch {}: {}", repo_name_clone, e);
                }
            }
        }

        // Count available vs unavailable for logging (but keep all in list)
        let available_count = all_modlists.iter().filter(|m| m.is_available()).count();
        let unavailable_count = all_modlists.len() - available_count;
        info!(
            "Total modlists: {} ({} available, {} unavailable)",
            all_modlists.len(),
            available_count,
            unavailable_count
        );
        self.modlists = all_modlists;

        // Fetch featured list
        if let Ok(featured) = self.fetch_featured_names().await {
            self.featured_names = featured;
        }

        Ok(&self.modlists)
    }

    async fn fetch_featured_names(&self) -> Result<Vec<String>> {
        let response = self
            .client
            .get(FEATURED_URL)
            .send()
            .await
            .context("Failed to fetch featured_lists.json")?;

        let featured: Vec<String> = response
            .json()
            .await
            .context("Failed to parse featured_lists.json")?;

        Ok(featured)
    }

    /// Search modlists by query and optional game filter
    pub fn search<'a>(
        &'a self,
        query: &str,
        game_filter: Option<&str>,
    ) -> impl Iterator<Item = &'a ModlistMetadata> {
        let query = query.to_lowercase();
        let game = game_filter.unwrap_or("").to_string();

        self.modlists
            .iter()
            .filter(move |m| m.is_available() && m.matches_query(&query) && m.matches_game(&game))
    }

    pub fn modlists(&self) -> &[ModlistMetadata] {
        &self.modlists
    }

    pub fn games(&self) -> Vec<&str> {
        let mut games: Vec<&str> = self
            .modlists
            .iter()
            .map(|m| m.game.as_str())
            .filter(|g| !g.is_empty())
            .collect();
        games.sort_unstable();
        games.dedup();
        games
    }

    /// Download a modlist .wabbajack file using chunked CDN downloads
    pub async fn download_modlist(
        &self,
        metadata: &ModlistMetadata,
        output_dir: &std::path::Path,
    ) -> Result<PathBuf> {
        self.download_modlist_with_progress(metadata, output_dir, |_, _| {})
            .await
    }

    /// Download a modlist .wabbajack file with progress callback
    /// Progress callback receives (bytes_downloaded, total_bytes)
    pub async fn download_modlist_with_progress<F>(
        &self,
        metadata: &ModlistMetadata,
        output_dir: &std::path::Path,
        progress_callback: F,
    ) -> Result<PathBuf>
    where
        F: Fn(u64, u64) + Send + Sync + 'static,
    {
        let download_url = metadata
            .download_url()
            .context("Modlist has no download URL")?;

        let filename = if !metadata.machine_name.is_empty() {
            format!("{}.wabbajack", metadata.machine_name)
        } else {
            format!("{}.wabbajack", metadata.title.replace(' ', "_"))
        };

        let output_path = output_dir.join(&filename);

        // Get expected size from metadata (0 if unknown)
        let expected_size = metadata
            .download_metadata
            .as_ref()
            .map(|d| d.size)
            .unwrap_or(0);

        // Check if file already exists with correct size (cache check)
        if output_path.exists() && expected_size > 0 {
            if let Ok(file_meta) = std::fs::metadata(&output_path) {
                if file_meta.len() == expected_size {
                    info!(
                        "Using cached file: {} (size matches: {} bytes)",
                        output_path.display(),
                        expected_size
                    );
                    return Ok(output_path);
                } else {
                    debug!(
                        "Cached file size mismatch: expected {} got {}, re-downloading",
                        expected_size,
                        file_meta.len()
                    );
                }
            }
        }

        info!("Downloading {} to {:?}", metadata.title, output_path);

        // Use the CDN downloader for chunked parallel downloads
        let cdn_downloader = WabbajackCdnDownloader::new()?;
        let bytes_downloaded = cdn_downloader
            .download_with_progress(download_url, &output_path, expected_size, progress_callback)
            .await?;

        info!("Downloaded {} ({} bytes)", metadata.title, bytes_downloaded);

        Ok(output_path)
    }

    pub fn find_by_name(&self, name: &str) -> Option<&ModlistMetadata> {
        self.modlists
            .iter()
            .find(|m| m.machine_name.eq_ignore_ascii_case(name))
    }

    pub fn find_by_title(&self, title: &str) -> Option<&ModlistMetadata> {
        self.modlists
            .iter()
            .find(|m| m.title.eq_ignore_ascii_case(title))
    }

    /// Get the cache directory for modlist metadata
    fn cache_dir() -> Result<PathBuf> {
        let cache_dir = dirs::cache_dir()
            .context("Could not determine cache directory")?
            .join("clf3")
            .join("modlists");
        std::fs::create_dir_all(&cache_dir)?;
        Ok(cache_dir)
    }

    /// Save current modlists to cache
    pub fn save_cache(&self) -> Result<()> {
        let cache_path = Self::cache_dir()?.join("modlists.json");
        let json = serde_json::to_string(&self.modlists)?;
        std::fs::write(&cache_path, json)?;
        info!("Saved {} modlists to cache", self.modlists.len());
        Ok(())
    }

    /// Load modlists from cache (returns empty vec if no cache exists)
    pub fn load_cache(&mut self) -> Result<bool> {
        let cache_path = Self::cache_dir()?.join("modlists.json");
        if !cache_path.exists() {
            return Ok(false);
        }

        let json = std::fs::read_to_string(&cache_path)?;
        self.modlists = serde_json::from_str(&json)?;
        info!("Loaded {} modlists from cache", self.modlists.len());
        Ok(true)
    }

    /// Check if cache exists and is recent (less than 1 hour old)
    pub fn has_recent_cache() -> bool {
        if let Ok(cache_dir) = Self::cache_dir() {
            let cache_path = cache_dir.join("modlists.json");
            if let Ok(metadata) = std::fs::metadata(&cache_path) {
                if let Ok(modified) = metadata.modified() {
                    let age = std::time::SystemTime::now()
                        .duration_since(modified)
                        .unwrap_or_default();
                    return age.as_secs() < 3600; // 1 hour
                }
            }
        }
        false
    }

    /// Get cache age in seconds (or None if no cache)
    pub fn cache_age_secs() -> Option<u64> {
        let cache_dir = Self::cache_dir().ok()?;
        let cache_path = cache_dir.join("modlists.json");
        let metadata = std::fs::metadata(&cache_path).ok()?;
        let modified = metadata.modified().ok()?;
        let age = std::time::SystemTime::now().duration_since(modified).ok()?;
        Some(age.as_secs())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore] // Requires network
    async fn test_fetch_modlists() {
        let mut browser = ModlistBrowser::new().unwrap();
        let modlists = browser.fetch_modlists().await.unwrap();

        println!("Fetched {} modlists", modlists.len());
        assert!(!modlists.is_empty(), "Should fetch some modlists");

        // Check a few modlists have expected fields
        for ml in modlists.iter().take(5) {
            println!(
                "  {} by {} ({}) - {}",
                ml.title, ml.author, ml.game, ml.machine_name
            );
            assert!(!ml.title.is_empty());
            assert!(!ml.game.is_empty());
        }
    }
}
