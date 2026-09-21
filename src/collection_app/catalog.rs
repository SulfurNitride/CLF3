//! Public Nexus discovery. This client never receives account credentials.
use super::acquire::{check, Nexus};
use crate::collection::url::valid_id;
use anyhow::{bail, Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{io::Read, time::Duration};
use tokio_util::sync::CancellationToken;

pub const PAGE_SIZE: u32 = 24;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Game {
    pub name: String,
    pub domain_name: String,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Sort {
    #[default]
    Downloads,
    Updated,
    Newest,
}

impl Sort {
    pub fn label(self) -> &'static str {
        match self {
            Self::Downloads => "Most downloaded",
            Self::Updated => "Recently updated",
            Self::Newest => "Newest",
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Search {
    pub text: String,
    /// Empty means every game, independently of installation support.
    pub game: String,
    pub sort: Sort,
    pub hide_adult: bool,
    pub page: u32,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Author {
    pub name: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Tile {
    pub thumbnail_url: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Revision {
    pub revision_number: u32,
    pub mod_count: u32,
    pub collection_schema_id: u32,
    pub adult_content: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Entry {
    pub slug: String,
    pub name: String,
    pub summary: String,
    pub game: Game,
    pub user: Author,
    pub total_downloads: u64,
    pub updated_at: String,
    pub tile_image: Option<Tile>,
    pub latest_published_revision: Option<Revision>,
}

impl Entry {
    pub fn url(&self) -> Option<String> {
        if !valid_id(&self.slug) || !valid_id(&self.game.domain_name) {
            return None;
        }
        let base = format!(
            "https://www.nexusmods.com/games/{}/collections/{}",
            self.game.domain_name, self.slug
        );
        Some(match &self.latest_published_revision {
            Some(r) if r.revision_number > 0 => format!("{base}/revisions/{}", r.revision_number),
            _ => base,
        })
    }

    pub fn can_review_installation(&self) -> bool {
        crate::collection::games::profile(&self.game.domain_name).is_some()
            && self
                .latest_published_revision
                .as_ref()
                .is_some_and(|r| r.revision_number > 0 && r.collection_schema_id == 1)
            && self.url().is_some()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Page {
    pub total_count: u32,
    pub nodes: Vec<Entry>,
}

fn search_body(search: &Search) -> Result<Value> {
    if !search.game.is_empty() && !valid_id(&search.game) {
        bail!("Invalid game domain");
    }
    if search.text.len() > 200 {
        bail!("Keep the collection search under 200 bytes");
    }
    let offset = search
        .page
        .checked_mul(PAGE_SIZE)
        .filter(|n| *n <= i32::MAX as u32)
        .context("Search page is too large")?;
    let mut filter = json!({
        "collectionStatus":[{"value":"listed","op":"EQUALS"}],
        "hasPublishedRevision":[{"value":true,"op":"EQUALS"}]
    });
    if !search.game.is_empty() {
        filter["gameDomain"] = json!([{"value":search.game,"op":"EQUALS"}]);
    }
    if !search.text.trim().is_empty() {
        filter["name"] = json!([{"value":search.text.trim(),"op":"WILDCARD"}]);
    }
    if search.hide_adult {
        filter["adultContent"] = json!([{"value":false,"op":"EQUALS"}]);
    }
    let sort = match search.sort {
        Sort::Downloads => json!({"downloads":{"direction":"DESC"}}),
        Sort::Updated => json!({"updatedAt":{"direction":"DESC"}}),
        Sort::Newest => json!({"createdAt":{"direction":"DESC"}}),
    };
    Ok(json!({
        "query":"query($filter:CollectionsSearchFilter,$sort:[CollectionsSearchSort!],$offset:Int!,$count:Int!){collectionsV2(filter:$filter,sort:$sort,offset:$offset,count:$count){totalCount nodes{slug name summary totalDownloads updatedAt game{name domainName} user{name} tileImage{thumbnailUrl(size:med)} latestPublishedRevision{revisionNumber modCount collectionSchemaId adultContent}}}}",
        "variables":{"filter":filter,"sort":[sort],"offset":offset,"count":PAGE_SIZE}
    }))
}

fn data(response: Value) -> Result<Value> {
    // Do not display server error bodies or reflected request values.
    if response
        .get("errors")
        .is_some_and(|e| !e.is_null() && e.as_array().is_none_or(|a| !a.is_empty()))
    {
        bail!("Nexus could not load the collection catalog. Try again shortly.");
    }
    response
        .get("data")
        .cloned()
        .context("Nexus returned no catalog data")
}

pub struct Catalog {
    api: Nexus,
    images: reqwest::blocking::Client,
}

fn image_url_allowed(url: &reqwest::Url) -> bool {
    super::acquire::valid_archive_url(url)
        && url.port_or_known_default() == Some(443)
        && url
            .host_str()
            .is_some_and(|host| host.ends_with(".nexusmods.com"))
}

impl Catalog {
    pub fn new() -> Result<Self> {
        Ok(Self {
            api: Nexus::new("")?,
            images: reqwest::blocking::Client::builder()
                .user_agent("CLF3/collections")
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(25))
                .redirect(reqwest::redirect::Policy::custom(|attempt| {
                    if attempt.previous().len() >= 3 || !image_url_allowed(attempt.url()) {
                        attempt.error("Invalid thumbnail redirect")
                    } else {
                        attempt.follow()
                    }
                }))
                .build()?,
        })
    }

    pub fn games(&self, token: &CancellationToken) -> Result<Vec<Game>> {
        check(token)?;
        let value = data(self.api.request(
            "/v2/graphql",
            Some(json!({"query":"{collectionGames{name domainName}}"})),
        )?)?;
        check(token)?;
        let mut games: Vec<Game> = serde_json::from_value(value["collectionGames"].clone())
            .context("Invalid collection game list")?;
        games.retain(|g| valid_id(&g.domain_name));
        games.sort_by_cached_key(|g| g.name.to_lowercase());
        games.dedup_by(|a, b| a.domain_name == b.domain_name);
        Ok(games)
    }

    pub fn search(&self, search: &Search, token: &CancellationToken) -> Result<Page> {
        check(token)?;
        let value = data(
            self.api
                .request("/v2/graphql", Some(search_body(search)?))?,
        )?;
        check(token)?;
        let page: Page = serde_json::from_value(value["collectionsV2"].clone())
            .context("Invalid collection search results")?;
        if page.nodes.len() > PAGE_SIZE as usize || page.nodes.iter().any(|e| e.url().is_none()) {
            bail!("Nexus returned invalid collection entries");
        }
        Ok(page)
    }

    pub fn thumbnail(&self, url: &str, token: &CancellationToken) -> Result<image::RgbaImage> {
        check(token)?;
        if !image_url_allowed(&reqwest::Url::parse(url)?) {
            bail!("Invalid thumbnail URL");
        }
        let response = self
            .images
            .get(url)
            .send()
            .map_err(|_| anyhow::anyhow!("Thumbnail unavailable"))?;
        if !response.status().is_success() {
            bail!("Thumbnail unavailable");
        }
        let mut bytes = Vec::new();
        response.take(8 * 1024 * 1024 + 1).read_to_end(&mut bytes)?;
        check(token)?;
        if bytes.len() > 8 * 1024 * 1024 {
            bail!("Thumbnail too large");
        }
        let mut reader =
            image::ImageReader::new(std::io::Cursor::new(bytes)).with_guessed_format()?;
        let mut limits = image::Limits::default();
        limits.max_image_width = Some(8192);
        limits.max_image_height = Some(8192);
        limits.max_alloc = Some(256 * 1024 * 1024);
        reader.limits(limits);
        Ok(reader.decode()?.thumbnail(384, 216).to_rgba8())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn search_filters_do_not_limit_catalog_to_installer_games() {
        let all = search_body(&Search::default()).unwrap();
        let filter = &all["variables"]["filter"];
        assert!(filter.get("gameDomain").is_none());
        assert!(filter.get("adultContent").is_none());
        let selected = search_body(&Search {
            game: "fallout4".into(),
            text: "\"test\"\\query".into(),
            page: 2,
            sort: Sort::Updated,
            hide_adult: true,
        })
        .unwrap();
        assert_eq!(
            selected["variables"]["filter"]["gameDomain"][0]["value"],
            "fallout4"
        );
        assert_eq!(
            selected["variables"]["filter"]["name"][0]["value"],
            "\"test\"\\query"
        );
        assert_eq!(selected["variables"]["offset"], 48);
        assert_eq!(
            selected["variables"]["sort"][0]["updatedAt"]["direction"],
            "DESC"
        );
        assert!(search_body(&Search {
            page: u32::MAX,
            ..Default::default()
        })
        .is_err());
        assert!(search_body(&Search {
            game: "../skyrim".into(),
            ..Default::default()
        })
        .is_err());
    }

    #[test]
    fn discovery_links_pin_revision_and_gate_installation() {
        let mut e: Entry = serde_json::from_value(json!({"slug":"qdurkx","name":"GTS","summary":"","game":{"name":"Skyrim","domainName":"skyrimspecialedition"},"user":{"name":"author"},"totalDownloads":1,"updatedAt":"","tileImage":null,"latestPublishedRevision":{"revisionNumber":117,"modCount":1966,"collectionSchemaId":1,"adultContent":true}})).unwrap();
        assert!(e.can_review_installation());
        assert!(e.url().unwrap().ends_with("/qdurkx/revisions/117"));
        e.game.domain_name = "fallout4".into();
        assert!(e.url().is_some());
        assert!(e.can_review_installation());
        e.game.domain_name = "cyberpunk2077".into();
        assert!(!e.can_review_installation());
        e.game.domain_name = "skyrimspecialedition".into();
        e.latest_published_revision
            .as_mut()
            .unwrap()
            .collection_schema_id = 2;
        assert!(!e.can_review_installation());
        e.slug = "bad?redirect=x".into();
        assert!(e.url().is_none());
    }

    #[test]
    fn thumbnail_origins_and_partial_graphql_errors_are_rejected() {
        for url in [
            "http://media.nexusmods.com/a",
            "https://nexusmods.com.evil.test/a",
            "https://user:pass@media.nexusmods.com/a",
            "https://127.0.0.1/a",
        ] {
            assert!(!image_url_allowed(&reqwest::Url::parse(url).unwrap()));
        }
        assert!(image_url_allowed(
            &reqwest::Url::parse("https://media.nexusmods.com/a.webp").unwrap()
        ));
        assert!(data(
            json!({"data":{"collectionsV2":{}},"errors":[{"message":"private reflected value"}]})
        )
        .is_err());
    }
}
