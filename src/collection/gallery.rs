//! Nexus Collection gallery — list collections via GraphQL.
//!
//! Schema reference: `Nexus-Mods/node-nexus-api` (`src/Nexus.ts`,
//! `src/types.ts`). The list query is `collectionsV2(...)` on the v2 endpoint
//! at `api.nexusmods.com/v2/graphql`. Filter shape uses operator-tagged
//! predicates: `{ op: "AND", gameDomain: [{ op: "EQUALS", value: "..." }] }`.

use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;

const GRAPHQL_URL: &str = "https://api.nexusmods.com/v2/graphql";
const DEFAULT_PAGE_SIZE: i32 = 30;

/// One collection card as returned by the gallery query.
#[derive(Debug, Clone)]
pub struct CollectionListing {
    pub slug: String,
    pub name: String,
    pub summary: String,
    pub author: String,
    pub game_domain: String,
    pub game_name: String,
    pub image_url: Option<String>,
    pub endorsements: u64,
    pub total_downloads: u64,
    pub latest_revision: u64,
    /// Total download size in bytes (sum of all archives).
    pub total_size_bytes: u64,
    /// Number of mods in the latest published revision.
    pub mod_count: u64,
}

impl CollectionListing {
    pub fn nexus_url(&self) -> String {
        format!(
            "https://next.nexusmods.com/{}/collections/{}",
            self.game_domain, self.slug
        )
    }
}

/// Sort field — direction is always DESC, schema accepts ASC|DESC if needed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortBy {
    Endorsements,
    Downloads,
    Recent,
}

impl SortBy {
    fn graphql_field(self) -> &'static str {
        match self {
            SortBy::Endorsements => "endorsements",
            SortBy::Downloads => "downloads",
            SortBy::Recent => "createdAt",
        }
    }
}

/// Fetch a page of collections.
///
/// `game_domain` is the Nexus URL slug (e.g. `skyrimspecialedition`). Pass
/// `None` to query all games.
pub async fn fetch_page(
    api_key: &str,
    game_domain: Option<&str>,
    sort: SortBy,
    offset: i32,
    count: Option<i32>,
) -> Result<Vec<CollectionListing>> {
    if api_key.is_empty() {
        bail!("Nexus API key required to fetch the collection gallery");
    }

    let count = count.unwrap_or(DEFAULT_PAGE_SIZE);

    let query = r#"
        query CollectionsV2(
            $count: Int,
            $offset: Int,
            $filter: CollectionsSearchFilter,
            $sort: [CollectionsSearchSort!]
        ) {
            collectionsV2(count: $count, offset: $offset, filter: $filter, sort: $sort) {
                totalCount
                nodes {
                    slug
                    name
                    summary
                    endorsements
                    totalDownloads
                    tileImage { url }
                    user { name }
                    game { domainName name }
                    latestPublishedRevision { revisionNumber totalSize modCount }
                }
            }
        }
    "#;

    // Filter: only published+listed collections, optionally pinned to a game.
    let mut filter = serde_json::json!({
        "op": "AND",
        "collectionStatus": [
            { "op": "EQUALS", "value": "listed" },
            { "op": "EQUALS", "value": "published" }
        ],
    });
    if let Some(domain) = game_domain {
        filter["gameDomain"] = serde_json::json!([
            { "op": "EQUALS", "value": domain }
        ]);
    }

    let body = serde_json::json!({
        "query": query,
        "variables": {
            "count": count,
            "offset": offset,
            "filter": filter,
            "sort": { sort.graphql_field(): { "direction": "DESC" } },
        },
    });

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(20))
        .build()?;

    let response = client
        .post(GRAPHQL_URL)
        .header("Content-Type", "application/json")
        .header("Application-Name", "clf3")
        .header("Application-Version", env!("CARGO_PKG_VERSION"))
        .header("Protocol-Version", "1.0.0")
        .header("APIKEY", api_key)
        .json(&body)
        .send()
        .await
        .context("Failed to query Nexus collectionsV2")?;

    if !response.status().is_success() {
        let status = response.status();
        let body_text = response.text().await.unwrap_or_default();
        bail!("Nexus GraphQL HTTP {}: {}", status, body_text);
    }

    let parsed: GraphQLEnvelope = response
        .json()
        .await
        .context("Failed to parse GraphQL response (schema may have changed)")?;

    if let Some(errs) = parsed.errors {
        if !errs.is_empty() {
            return Err(anyhow!(
                "GraphQL errors: {}",
                serde_json::to_string(&errs).unwrap_or_default()
            ));
        }
    }

    let data = parsed
        .data
        .and_then(|d| d.collections_v2)
        .context("GraphQL response missing `collectionsV2` payload")?;

    let nodes = data.nodes.unwrap_or_default();
    let listings = nodes
        .into_iter()
        .map(|n| CollectionListing {
            slug: n.slug.unwrap_or_default(),
            name: n.name.unwrap_or_default(),
            summary: n.summary.unwrap_or_default(),
            author: n.user.and_then(|u| u.name).unwrap_or_default(),
            game_domain: n
                .game
                .as_ref()
                .and_then(|g| g.domain_name.clone())
                .unwrap_or_default(),
            game_name: n.game.and_then(|g| g.name).unwrap_or_default(),
            image_url: n.tile_image.and_then(|i| i.thumbnail_url.or(i.url)),
            endorsements: n.endorsements.unwrap_or(0),
            total_downloads: n.total_downloads.unwrap_or(0),
            latest_revision: n
                .latest_published_revision
                .as_ref()
                .and_then(|r| r.revision_number)
                .unwrap_or(0),
            total_size_bytes: n
                .latest_published_revision
                .as_ref()
                .and_then(|r| r.total_size.as_deref())
                .and_then(parse_size)
                .unwrap_or(0),
            mod_count: n
                .latest_published_revision
                .and_then(|r| r.mod_count)
                .unwrap_or(0),
        })
        .collect();

    Ok(listings)
}

// ============================================================================
// GraphQL response shapes — every field optional so a future schema rename
// drops fields rather than blowing up the whole gallery.
// ============================================================================

#[derive(Debug, Deserialize)]
struct GraphQLEnvelope {
    data: Option<EnvelopeData>,
    errors: Option<Vec<serde_json::Value>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EnvelopeData {
    collections_v2: Option<CollectionsPayload>,
}

#[derive(Debug, Deserialize)]
struct CollectionsPayload {
    nodes: Option<Vec<CollectionNode>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CollectionNode {
    slug: Option<String>,
    name: Option<String>,
    summary: Option<String>,
    endorsements: Option<u64>,
    total_downloads: Option<u64>,
    user: Option<UserNode>,
    game: Option<GameNode>,
    tile_image: Option<TileImage>,
    latest_published_revision: Option<RevisionNode>,
}

#[derive(Debug, Deserialize)]
struct UserNode {
    name: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GameNode {
    domain_name: Option<String>,
    name: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TileImage {
    url: Option<String>,
    thumbnail_url: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RevisionNode {
    revision_number: Option<u64>,
    /// Nexus returns size as a stringified u64 (BigInt scalar). Parsed via
    /// `parse_size`.
    total_size: Option<String>,
    mod_count: Option<u64>,
}

/// Nexus' GraphQL `BigInt` scalar comes through as a JSON string.
fn parse_size(s: &str) -> Option<u64> {
    s.parse().ok()
}
