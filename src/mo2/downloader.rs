//! MO2 download from GitHub releases.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::Path;

/// GitHub release information.
#[derive(Debug, Clone, Deserialize)]
pub struct GithubRelease {
    /// Release tag (e.g., "v2.5.2").
    pub tag_name: String,
    /// Release assets (downloadable files).
    pub assets: Vec<GithubAsset>,
}

/// GitHub release asset.
#[derive(Debug, Clone, Deserialize)]
pub struct GithubAsset {
    /// Filename of the asset.
    pub name: String,
    /// Direct download URL.
    pub browser_download_url: String,
    /// File size in bytes.
    pub size: u64,
}

/// Fetches the latest MO2 release information from GitHub.
pub async fn fetch_latest_mo2_release() -> Result<GithubRelease> {
    let url = "https://api.github.com/repos/ModOrganizer2/modorganizer/releases/latest";

    let client = reqwest::Client::new();
    let response = client
        .get(url)
        .header("User-Agent", "CLF3-Modlist-Installer")
        .send()
        .await
        .context("Failed to fetch MO2 release info from GitHub")?;

    if !response.status().is_success() {
        anyhow::bail!(
            "GitHub API returned status {}: {}",
            response.status(),
            response.text().await.unwrap_or_default()
        );
    }

    let release: GithubRelease = response
        .json()
        .await
        .context("Failed to parse GitHub release JSON")?;

    Ok(release)
}

/// Finds the correct MO2 Windows x64 archive from release assets.
///
/// Filters for:
/// - Starts with "Mod.Organizer-2"
/// - Ends with ".7z"
/// - Excludes: Linux, pdbs, src, uibase, commits
pub fn find_mo2_asset(release: &GithubRelease) -> Option<&GithubAsset> {
    const INVALID_TERMS: &[&str] = &["Linux", "pdbs", "src", "uibase", "commits"];

    release.assets.iter().find(|asset| {
        asset.name.starts_with("Mod.Organizer-2")
            && asset.name.ends_with(".7z")
            && !INVALID_TERMS.iter().any(|term| asset.name.contains(term))
    })
}

/// Downloads MO2 and extracts it to the specified directory.
///
/// # Arguments
/// * `output_dir` - Directory where MO2 will be extracted
/// * `progress_callback` - Optional callback for download progress (bytes_downloaded, total_bytes)
///
/// # Returns
/// The path to the extracted MO2 directory.
pub async fn download_mo2<F>(
    output_dir: &Path,
    progress_callback: Option<F>,
) -> Result<()>
where
    F: Fn(u64, u64) + Send + Sync,
{
    // Fetch release info
    let release = fetch_latest_mo2_release().await?;
    let asset = find_mo2_asset(&release)
        .ok_or_else(|| anyhow::anyhow!("No valid MO2 archive found in release {}", release.tag_name))?;

    tracing::info!("Downloading MO2 {} ({})", release.tag_name, asset.name);

    // Create temp file for download (in output dir, not /tmp)
    std::fs::create_dir_all(output_dir).context("Failed to create output directory")?;
    let temp_dir = tempfile::tempdir_in(output_dir).context("Failed to create temp directory")?;
    let archive_path = temp_dir.path().join(&asset.name);

    // Download the archive
    download_file(&asset.browser_download_url, &archive_path, asset.size, progress_callback).await?;

    // Extract to output directory
    tracing::info!("Extracting MO2 to {}", output_dir.display());
    std::fs::create_dir_all(output_dir).context("Failed to create output directory")?;

    crate::archive::sevenzip::extract_all(&archive_path, output_dir)
        .context("Failed to extract MO2 archive")?;

    // Verify extraction
    let exe_path = output_dir.join("ModOrganizer.exe");
    if !exe_path.exists() {
        anyhow::bail!(
            "ModOrganizer.exe not found after extraction. Expected at: {}",
            exe_path.display()
        );
    }

    tracing::info!("MO2 {} extracted successfully", release.tag_name);
    Ok(())
}

/// Downloads a file with optional progress reporting.
async fn download_file<F>(
    url: &str,
    output_path: &Path,
    total_size: u64,
    progress_callback: Option<F>,
) -> Result<()>
where
    F: Fn(u64, u64) + Send + Sync,
{
    use futures::StreamExt;
    use std::io::Write;

    let client = reqwest::Client::new();
    let response = client
        .get(url)
        .header("User-Agent", "CLF3-Modlist-Installer")
        .send()
        .await
        .context("Failed to start download")?;

    if !response.status().is_success() {
        anyhow::bail!("Download failed with status {}", response.status());
    }

    let mut file = std::fs::File::create(output_path)
        .context("Failed to create output file")?;

    let mut downloaded: u64 = 0;
    let mut stream = response.bytes_stream();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("Error reading download stream")?;
        file.write_all(&chunk).context("Error writing to file")?;
        downloaded += chunk.len() as u64;

        if let Some(ref callback) = progress_callback {
            callback(downloaded, total_size);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_mo2_asset() {
        let release = GithubRelease {
            tag_name: "v2.5.2".to_string(),
            assets: vec![
                GithubAsset {
                    name: "Mod.Organizer-2.5.2.7z".to_string(),
                    browser_download_url: "https://example.com/mo2.7z".to_string(),
                    size: 1000000,
                },
                GithubAsset {
                    name: "Mod.Organizer-2.5.2-Linux.7z".to_string(),
                    browser_download_url: "https://example.com/mo2-linux.7z".to_string(),
                    size: 1000000,
                },
                GithubAsset {
                    name: "Mod.Organizer-2.5.2-pdbs.7z".to_string(),
                    browser_download_url: "https://example.com/mo2-pdbs.7z".to_string(),
                    size: 500000,
                },
            ],
        };

        let asset = find_mo2_asset(&release);
        assert!(asset.is_some());
        assert_eq!(asset.unwrap().name, "Mod.Organizer-2.5.2.7z");
    }

    #[test]
    fn test_find_mo2_asset_none() {
        let release = GithubRelease {
            tag_name: "v2.5.2".to_string(),
            assets: vec![
                GithubAsset {
                    name: "Mod.Organizer-2.5.2-Linux.7z".to_string(),
                    browser_download_url: "https://example.com/mo2-linux.7z".to_string(),
                    size: 1000000,
                },
            ],
        };

        let asset = find_mo2_asset(&release);
        assert!(asset.is_none());
    }
}
