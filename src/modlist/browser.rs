//! Modlist browser for fetching and displaying available modlists from Wabbajack repositories.

use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::io::AsyncWriteExt;
use tracing::{debug, info, warn};

const REPOSITORIES_URL: &str = "https://raw.githubusercontent.com/wabbajack-tools/mod-lists/master/repositories.json";
const FEATURED_URL: &str = "https://raw.githubusercontent.com/wabbajack-tools/mod-lists/master/featured_lists.json";
const REPO_BASE_URL: &str = "https://raw.githubusercontent.com/wabbajack-tools/mod-lists/master";

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
    // Fields we populate ourselves
    #[serde(skip)]
    pub repository_name: String,
    #[serde(skip)]
    pub machine_name: String,
}

impl ModlistMetadata {
    /// Get the download URL for this modlist
    pub fn download_url(&self) -> Option<&str> {
        self.links.as_ref().map(|l| l.download.as_str()).filter(|s| !s.is_empty())
    }

    pub fn image_url(&self) -> Option<&str> {
        self.links.as_ref().map(|l| l.image.as_str()).filter(|s| !s.is_empty())
    }

    pub fn readme_url(&self) -> Option<&str> {
        self.links.as_ref().map(|l| l.readme.as_str()).filter(|s| !s.is_empty())
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
                            debug!(
                                "  {} modlists from {}",
                                modlists.len(),
                                repo_name_clone
                            );
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

        // Filter out force_down modlists
        all_modlists.retain(|m| !m.force_down);

        info!("Total modlists available: {}", all_modlists.len());
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

        self.modlists.iter().filter(move |m| {
            m.is_available() && m.matches_query(&query) && m.matches_game(&game)
        })
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

    /// Download a modlist .wabbajack file
    pub async fn download_modlist(
        &self,
        metadata: &ModlistMetadata,
        output_dir: &std::path::Path,
    ) -> Result<PathBuf> {
        let download_url = metadata
            .download_url()
            .context("Modlist has no download URL")?;

        let filename = if !metadata.machine_name.is_empty() {
            format!("{}.wabbajack", metadata.machine_name)
        } else {
            format!("{}.wabbajack", metadata.title.replace(' ', "_"))
        };

        let output_path = output_dir.join(&filename);

        info!("Downloading {} to {:?}", metadata.title, output_path);

        let response = self
            .client
            .get(download_url)
            .send()
            .await
            .with_context(|| format!("Failed to download modlist from {}", download_url))?;

        if !response.status().is_success() {
            anyhow::bail!(
                "Failed to download modlist: HTTP {}",
                response.status()
            );
        }

        let bytes = response.bytes().await?;

        // Write to file
        let mut file = tokio::fs::File::create(&output_path).await?;
        file.write_all(&bytes).await?;

        info!(
            "Downloaded {} ({} bytes)",
            metadata.title,
            bytes.len()
        );

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
