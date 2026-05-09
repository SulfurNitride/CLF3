//! Fetch collections from Nexus Mods URLs.
//!
//! Supports fetching collection.json from:
//! - Direct URL: https://www.nexusmods.com/skyrimspecialedition/collections/slug
//! - Short URL: https://next.nexusmods.com/skyrimspecialedition/collections/slug

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use regex::Regex;
use serde::Deserialize;
use tracing::info;

/// Parsed collection URL info
#[derive(Debug)]
pub struct CollectionUrlInfo {
    pub game: String,
    pub slug: String,
    /// Optional revision number from `?revision=N` or path suffix
    /// `/revisions/N`. None → fetch the latest published revision.
    pub revision: Option<i32>,
}

/// Parse a Nexus collection URL into game and slug components.
///
/// Supports URLs like:
/// - https://www.nexusmods.com/skyrimspecialedition/collections/vith5v
/// - https://next.nexusmods.com/skyrimspecialedition/collections/vith5v
/// - https://next.nexusmods.com/skyrimspecialedition/collections/vith5v/revisions/3
/// - https://...?revision=3
pub fn parse_collection_url(input: &str) -> Option<CollectionUrlInfo> {
    // Check if it looks like a URL
    if !input.contains("nexusmods.com") && !input.starts_with("http") {
        return None;
    }

    // Regex to match: nexusmods.com/[games/]<game>/collections/<slug>
    let re = Regex::new(r"nexusmods\.com/(?:games/)?([^/]+)/collections/([^/?#]+)").ok()?;

    let caps = re.captures(input)?;
    let game = caps.get(1)?.as_str().to_string();
    let slug = caps.get(2)?.as_str().to_string();

    // Path suffix `/revisions/N`.
    let path_rev = Regex::new(r"/revisions/(\d+)")
        .ok()
        .and_then(|r| r.captures(input))
        .and_then(|c| c.get(1))
        .and_then(|m| m.as_str().parse::<i32>().ok());

    // Query param `?revision=N` or `&revision=N`.
    let query_rev = Regex::new(r"[?&]revision=(\d+)")
        .ok()
        .and_then(|r| r.captures(input))
        .and_then(|c| c.get(1))
        .and_then(|m| m.as_str().parse::<i32>().ok());

    Some(CollectionUrlInfo {
        game,
        slug,
        revision: path_rev.or(query_rev),
    })
}

/// Check if input looks like a URL (vs a file path)
pub fn is_url(input: &str) -> bool {
    input.starts_with("http://") || input.starts_with("https://") || input.contains("nexusmods.com")
}

/// GraphQL response types
#[derive(Debug, Deserialize)]
struct GraphQLResponse {
    data: Option<GraphQLData>,
    errors: Option<Vec<serde_json::Value>>,
}

#[derive(Debug, Deserialize)]
struct GraphQLData {
    #[serde(rename = "collectionRevision")]
    collection_revision: Option<CollectionRevision>,
}

#[derive(Debug, Deserialize)]
struct CollectionRevision {
    #[serde(rename = "revisionNumber")]
    revision_number: i32,
    #[serde(rename = "downloadLink")]
    download_link: Option<String>,
    collection: Option<CollectionInfo>,
}

#[derive(Debug, Deserialize)]
struct CollectionInfo {
    name: String,
}

#[derive(Debug, Deserialize)]
struct DownloadLinksResponse {
    download_links: Vec<DownloadLink>,
}

#[derive(Debug, Deserialize)]
struct DownloadLink {
    #[serde(rename = "URI")]
    uri: String,
}

/// Fetch a collection from Nexus and return the path to collection.json
///
/// This will:
/// 1. Query the GraphQL API for the collection revision
/// 2. Get the download link for the .7z archive
/// 3. Download and extract it to a temp directory
/// 4. Return the path to collection.json
pub async fn fetch_collection(
    url_info: &CollectionUrlInfo,
    api_key: &str,
    output_dir: &Path,
) -> Result<PathBuf> {
    info!("Fetching collection from Nexus...");
    info!("  Game: {}", url_info.game);
    info!("  Slug: {}", url_info.slug);

    let client = reqwest::Client::new();

    // Step 1: Query GraphQL for collection revision. When the URL pinned a
    // specific revision, ask for that one; otherwise let Nexus return the
    // latest published.
    let graphql_url = "https://api.nexusmods.com/v2/graphql";
    let query = if let Some(rev) = url_info.revision {
        serde_json::json!({
            "query": r#"
                query GetCollectionRevision($slug: String!, $revisionNumber: Int!) {
                    collectionRevision(slug: $slug, revisionNumber: $revisionNumber) {
                        revisionNumber
                        downloadLink
                        collection { name }
                    }
                }
            "#,
            "variables": { "slug": url_info.slug, "revisionNumber": rev }
        })
    } else {
        serde_json::json!({
            "query": r#"
                query GetCollection($slug: String!) {
                    collectionRevision(slug: $slug) {
                        revisionNumber
                        downloadLink
                        collection { name }
                    }
                }
            "#,
            "variables": { "slug": url_info.slug }
        })
    };

    let response = client
        .post(graphql_url)
        .header("Content-Type", "application/json")
        .header("apikey", api_key)
        .json(&query)
        .send()
        .await
        .context("Failed to query Nexus GraphQL API")?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        bail!("GraphQL API error {}: {}", status, body);
    }

    let graphql_response: GraphQLResponse = response
        .json()
        .await
        .context("Failed to parse GraphQL response")?;

    // Check for errors
    if let Some(errors) = graphql_response.errors {
        bail!("GraphQL errors: {:?}", errors);
    }

    let revision = graphql_response
        .data
        .and_then(|d| d.collection_revision)
        .context("No collection revision in response (may be adult content blocked)")?;

    let collection_name = revision
        .collection
        .map(|c| c.name)
        .unwrap_or_else(|| url_info.slug.clone());

    info!("  Collection: {}", collection_name);
    info!("  Revision: {}", revision.revision_number);

    let download_link = revision
        .download_link
        .context("No download link (may require premium or adult content setting)")?;

    // Step 2: Get CDN download link
    let full_download_url = if download_link.starts_with('/') {
        format!("https://api.nexusmods.com{}", download_link)
    } else {
        download_link
    };

    info!("  Getting CDN download link...");

    let links_response = client
        .get(&full_download_url)
        .header("apikey", api_key)
        .send()
        .await
        .context("Failed to get download links")?;

    if !links_response.status().is_success() {
        let status = links_response.status();
        let body = links_response.text().await.unwrap_or_default();
        bail!("Download links API error {}: {}", status, body);
    }

    let links: DownloadLinksResponse = links_response
        .json()
        .await
        .context("Failed to parse download links")?;

    let cdn_url = links
        .download_links
        .first()
        .map(|l| &l.uri)
        .context("No download links in response")?;

    // Step 3: Download the .7z archive
    info!("  Downloading collection archive...");

    // Create a temp directory for the collection
    let collection_dir = output_dir.join(".collection_temp");
    std::fs::create_dir_all(&collection_dir)?;

    let archive_path = collection_dir.join(format!("{}.7z", url_info.slug));

    let mut archive_response = client
        .get(cdn_url)
        .send()
        .await
        .context("Failed to download collection archive")?;

    if !archive_response.status().is_success() {
        bail!("Archive download failed: {}", archive_response.status());
    }

    // Stream the archive to disk in chunks so a multi-MB collection doesn't
    // sit in process RAM until we're done.
    let mut out = tokio::fs::File::create(&archive_path)
        .await
        .with_context(|| format!("Failed to create {}", archive_path.display()))?;
    let mut total_bytes: u64 = 0;
    while let Some(chunk) = archive_response
        .chunk()
        .await
        .context("Failed to read archive chunk")?
    {
        tokio::io::AsyncWriteExt::write_all(&mut out, &chunk)
            .await
            .context("Failed to write archive chunk")?;
        total_bytes += chunk.len() as u64;
    }
    tokio::io::AsyncWriteExt::flush(&mut out)
        .await
        .context("Failed to flush archive")?;
    drop(out);

    info!("  Downloaded {} bytes", total_bytes);

    // Step 4: Extract collection.json from the .7z archive
    info!("  Extracting collection.json...");

    let extract_dir = collection_dir.join("extracted");
    std::fs::create_dir_all(&extract_dir)?;

    // Extract the entire collection archive: collection.json + patches/ subtree
    // (bsdiff blobs) + any bundled assets. Collection archives are typically
    // a few hundred KB so the cost is negligible.
    let seven_z = crate::archive::sevenzip::get_7z_path()?;

    let output = std::process::Command::new(&seven_z)
        .args([
            "x",
            "-y",
            &format!("-o{}", extract_dir.display()),
            archive_path.to_str().unwrap(),
        ])
        .output()
        .context("Failed to run 7z")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("7z extraction failed: {}", stderr);
    }

    // Find collection.json
    let collection_json = extract_dir.join("collection.json");
    if !collection_json.exists() {
        bail!("collection.json not found in archive");
    }

    info!("  Extracted collection.json");
    if extract_dir.join("patches").exists() {
        info!("  Extracted patches/ subtree");
    }

    // Clean up archive (keep extracted json + patches dir)
    let _ = std::fs::remove_file(&archive_path);

    Ok(collection_json)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_collection_url() {
        // Standard URL
        let url = "https://www.nexusmods.com/skyrimspecialedition/collections/vith5v";
        let info = parse_collection_url(url).unwrap();
        assert_eq!(info.game, "skyrimspecialedition");
        assert_eq!(info.slug, "vith5v");

        // Next URL
        let url = "https://next.nexusmods.com/skyrimspecialedition/collections/vith5v";
        let info = parse_collection_url(url).unwrap();
        assert_eq!(info.game, "skyrimspecialedition");
        assert_eq!(info.slug, "vith5v");

        // With query params
        let url = "https://www.nexusmods.com/skyrimspecialedition/collections/vith5v?tab=mods";
        let info = parse_collection_url(url).unwrap();
        assert_eq!(info.slug, "vith5v");

        // Not a URL
        assert!(parse_collection_url("/path/to/collection.json").is_none());
        assert!(parse_collection_url("collection.json").is_none());
    }

    #[test]
    fn test_is_url() {
        assert!(is_url("https://www.nexusmods.com/skyrimspecialedition/collections/vith5v"));
        assert!(is_url("http://nexusmods.com/foo"));
        assert!(is_url("nexusmods.com/foo"));
        assert!(!is_url("/path/to/collection.json"));
        assert!(!is_url("collection.json"));
        assert!(!is_url("./collection.json"));
    }
}
