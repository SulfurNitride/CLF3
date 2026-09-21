use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CollectionLocator {
    pub domain: String,
    pub slug: String,
    pub revision: Option<u32>,
}

pub fn parse_collection_url(input: &str) -> Result<CollectionLocator> {
    let url = reqwest::Url::parse(input).context("Expected a Nexus collection URL")?;
    if url.scheme() != "https"
        || !matches!(
            url.host_str(),
            Some("www.nexusmods.com" | "next.nexusmods.com" | "nexusmods.com")
        )
        || !url.username().is_empty()
        || url.password().is_some()
        || url.port().is_some()
    {
        bail!("Expected an HTTPS Nexus collection URL without credentials");
    }
    let parts: Vec<_> = url
        .path()
        .trim_end_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();
    let parts = if parts.first() == Some(&"games") {
        &parts[1..]
    } else {
        &parts[..]
    };
    if parts.len() < 3 || parts[1] != "collections" || !valid_id(parts[0]) || !valid_id(parts[2]) {
        bail!("Expected a game domain and collection slug");
    }
    let mut revision = match &parts[3..] {
        [] => None,
        ["revisions", number] => Some(parse_revision(number)?),
        _ => bail!("Unexpected collection URL path"),
    };
    for (key, value) in url.query_pairs() {
        if key == "revision" {
            let number = parse_revision(&value)?;
            if revision.is_some_and(|existing| existing != number) {
                bail!("Conflicting revision numbers");
            }
            revision = Some(number);
        }
    }
    Ok(CollectionLocator {
        domain: parts[0].into(),
        slug: parts[2].into(),
        revision,
    })
}

fn parse_revision(value: &str) -> Result<u32> {
    let number: u32 = value.parse().context("Invalid revision number")?;
    if number == 0 {
        bail!("Revision numbers start at 1");
    }
    Ok(number)
}

pub fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}
