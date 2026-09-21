//! Account-bearing frontend I/O. Never imported by the credential-free worker.
use crate::collection::{plan::ArtifactPlan, url::CollectionLocator};
use anyhow::{bail, Context, Result};
use reqwest::{
    blocking::{Client, Response},
    header::HeaderValue,
    redirect::Policy,
    Url,
};
use serde_json::{json, Value};
use std::{
    fs::File,
    io::{Read, Write},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

pub struct Nexus {
    api: Client,
    archives: Client,
    key: HeaderValue,
}

pub fn check(token: &CancellationToken) -> Result<()> {
    if token.is_cancelled() {
        bail!("Cancelled; verified files are retained for resume");
    }
    Ok(())
}

fn archive_client() -> Result<Client> {
    Ok(Client::builder()
        .user_agent("CLF3/collections")
        .connect_timeout(Duration::from_secs(20))
        .timeout(Duration::from_secs(120))
        .redirect(Policy::custom(|attempt| {
            if attempt.previous().len() >= 5 || !valid_archive_url(attempt.url()) {
                attempt.error("Invalid archive redirect")
            } else {
                attempt.follow()
            }
        }))
        .build()?)
}

pub fn valid_archive_url(url: &Url) -> bool {
    url.scheme() == "https"
        && url.host_str().is_some()
        && url.username().is_empty()
        && url.password().is_none()
}

pub fn api_url(path: &str) -> Result<Url> {
    // The key is added only after this check. No absolute/network-path URLs,
    // query strings or redirects can change the authenticated origin.
    let parts: Vec<_> = path.split('/').collect();
    let collection_download = matches!(parts.as_slice(), ["", "v2", "collections", collection, "revisions", revision, "download_link"]
        if !collection.is_empty() && collection.bytes().all(|b| b.is_ascii_digit()) && !revision.is_empty() && revision.bytes().all(|b| b.is_ascii_digit()));
    if !(path.starts_with("/v1/") || path == "/v2/graphql" || collection_download)
        || path.contains(['?', '#', '\\'])
        || path.contains("..")
    {
        bail!("Invalid Nexus API path");
    }
    let url = Url::parse(&format!("https://api.nexusmods.com{path}"))?;
    if url.host_str() != Some("api.nexusmods.com") || url.port().is_some() {
        bail!("Invalid Nexus API origin");
    }
    Ok(url)
}

fn json_response(mut response: Response) -> Result<Value> {
    if !response.status().is_success() {
        bail!("Nexus API returned HTTP {}", response.status().as_u16());
    }
    let mut bytes = Vec::new();
    (&mut response)
        .take(4 * 1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| anyhow::anyhow!("Nexus response could not be read"))?;
    if bytes.len() > 4 * 1024 * 1024 {
        bail!("Nexus response exceeded its size limit");
    }
    serde_json::from_slice(&bytes).map_err(|_| anyhow::anyhow!("Nexus returned invalid JSON"))
}

impl Nexus {
    pub fn new(key: &str) -> Result<Self> {
        let mut header = HeaderValue::from_str(key)
            .map_err(|_| anyhow::anyhow!("Invalid saved Nexus key; check Settings"))?;
        header.set_sensitive(true);
        Ok(Self {
            api: Client::builder()
                .user_agent("CLF3/collections")
                .redirect(Policy::none())
                .connect_timeout(Duration::from_secs(20))
                .timeout(Duration::from_secs(45))
                .build()?,
            archives: archive_client()?,
            key: header,
        })
    }

    pub(crate) fn request(&self, path: &str, body: Option<Value>) -> Result<Value> {
        let url = api_url(path)?;
        let builder = if let Some(body) = body {
            self.api.post(url).json(&body)
        } else {
            self.api.get(url)
        };
        let builder = if self.key.is_empty() {
            builder
        } else {
            builder.header("apikey", self.key.clone())
        };
        let response = builder.send().map_err(|_| {
            anyhow::anyhow!("Nexus request failed; check the connection and saved account")
        })?;
        json_response(response)
    }

    pub fn premium(&self) -> Result<bool> {
        if self.key.is_empty() {
            return Ok(false);
        }
        Ok(self.request("/v1/users/validate.json", None)?["is_premium"]
            .as_bool()
            .unwrap_or(false))
    }

    pub fn package(
        &self,
        locator: &CollectionLocator,
        directory: &Path,
        token: &CancellationToken,
    ) -> Result<(PathBuf, CollectionLocator, u32)> {
        check(token)?;
        let fields = "revisionNumber collectionSchemaId downloadLink";
        let (query, variables, pointer) = if let Some(revision) = locator.revision {
            (format!("query($slug:String!,$domain:String!,$revision:Int!) {{ collectionRevision(slug:$slug,domainName:$domain,revision:$revision) {{ {fields} }} }}"),
             json!({"slug":locator.slug,"domain":locator.domain,"revision":revision}), "/data/collectionRevision")
        } else {
            (format!("query($slug:String!,$domain:String!) {{ collection(slug:$slug,domainName:$domain) {{ latestPublishedRevision {{ {fields} }} }} }}"),
             json!({"slug":locator.slug,"domain":locator.domain}), "/data/collection/latestPublishedRevision")
        };
        let metadata = self.request(
            "/v2/graphql",
            Some(json!({"query":query,"variables":variables})),
        )?;
        if metadata.get("errors").is_some() {
            bail!("Nexus could not resolve this collection revision");
        }
        let revision = metadata
            .pointer(pointer)
            .context("No published collection revision was returned")?;
        let number = revision["revisionNumber"]
            .as_u64()
            .and_then(|n| u32::try_from(n).ok())
            .filter(|n| *n > 0)
            .context("Invalid revision number")?;
        if locator.revision.is_some_and(|r| r != number) {
            bail!("Nexus returned a different collection revision");
        }
        let schema = revision["collectionSchemaId"]
            .as_u64()
            .context("Collection schema is missing")?;
        if schema != 1 {
            bail!("Only Collection schema 1 is currently supported");
        }
        check(token)?;
        let links = self.request(
            revision["downloadLink"]
                .as_str()
                .context("No collection package is available")?,
            None,
        )?;
        let mirrors = links["download_links"]
            .as_array()
            .context("No package mirrors were returned")?;
        std::fs::create_dir_all(directory)?;
        let target = directory.join("collection.7z");
        let mut success = false;
        for mirror in mirrors {
            if let Some(url) = mirror["URI"].as_str() {
                if transfer(
                    &self.archives,
                    url,
                    &target,
                    None,
                    None,
                    4 * 1024 * 1024 * 1024,
                    token,
                    &|_| {},
                )
                .is_ok()
                {
                    success = true;
                    break;
                }
            }
            check(token)?;
        }
        if !success {
            bail!("The pinned collection package could not be downloaded");
        }
        Ok((
            target,
            CollectionLocator {
                revision: Some(number),
                ..locator.clone()
            },
            1,
        ))
    }

    pub fn artifact(
        &self,
        artifact: &ArtifactPlan,
        direct: Option<&str>,
        target: &Path,
        token: &CancellationToken,
        progress: &(dyn Fn(u64) + Sync),
    ) -> Result<()> {
        let mut last = String::from("No exact download is available");
        for attempt in 0..3 {
            check(token)?;
            let result = (|| {
                let urls = if artifact.source_type == "direct" {
                    vec![direct
                        .context("The pinned direct source is unavailable")?
                        .to_owned()]
                } else {
                    if !crate::collection::url::valid_id(&artifact.domain)
                        || artifact.mod_id == 0
                        || artifact.file_id == 0
                    {
                        bail!("Invalid exact Nexus artifact");
                    }
                    let links = self.request(
                        &format!(
                            "/v1/games/{}/mods/{}/files/{}/download_link.json",
                            artifact.domain, artifact.mod_id, artifact.file_id
                        ),
                        None,
                    )?;
                    links
                        .as_array()
                        .context("No exact download mirrors")?
                        .iter()
                        .filter_map(|l| l["URI"].as_str().map(str::to_owned))
                        .collect()
                };
                let url = urls
                    .get(attempt % urls.len().max(1))
                    .context("No exact download mirrors")?;
                transfer(
                    &self.archives,
                    url,
                    target,
                    artifact.expected_size,
                    Some(&artifact.expected_md5),
                    128 * 1024 * 1024 * 1024,
                    token,
                    progress,
                )
            })();
            match result {
                Ok(()) => return Ok(()),
                Err(e) => last = e.to_string(),
            }
            if attempt < 2 {
                for _ in 0..10 * (attempt + 1) {
                    check(token)?;
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        }
        bail!("{last}")
    }
}

/// Independent unauthenticated HTTP client, even when following CDN redirects.
#[allow(clippy::too_many_arguments)] // Explicit transfer limits and integrity requirements.
pub fn transfer(
    client: &Client,
    source: &str,
    target: &Path,
    expected_size: Option<u64>,
    md5: Option<&str>,
    limit: u64,
    token: &CancellationToken,
    progress: &(dyn Fn(u64) + Sync),
) -> Result<()> {
    let url = Url::parse(source).map_err(|_| anyhow::anyhow!("Invalid download URL"))?;
    if !valid_archive_url(&url) {
        bail!("An HTTPS download without URL credentials is required");
    }
    check(token)?;
    let mut response = client
        .get(url)
        .send()
        .map_err(|_| anyhow::anyhow!("Archive transfer failed"))?;
    if !response.status().is_success() {
        bail!(
            "Archive server returned HTTP {}",
            response.status().as_u16()
        );
    }
    let parent = target.parent().context("Missing download directory")?;
    std::fs::create_dir_all(parent)?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    let mut hash = md5::Context::new();
    let mut count = 0u64;
    let mut buffer = vec![0u8; 1024 * 1024];
    let mut last_event = std::time::Instant::now();
    loop {
        check(token)?;
        let n = response
            .read(&mut buffer)
            .map_err(|_| anyhow::anyhow!("Archive transfer interrupted"))?;
        if n == 0 {
            break;
        }
        count += n as u64;
        if count > expected_size.unwrap_or(limit).min(limit) {
            bail!("Archive exceeded the expected size");
        }
        temporary.write_all(&buffer[..n])?;
        hash.consume(&buffer[..n]);
        if last_event.elapsed() >= Duration::from_millis(200) {
            progress(count);
            last_event = std::time::Instant::now();
        }
    }
    if expected_size.is_some_and(|size| size != count)
        || md5
            .is_some_and(|expected| !format!("{:x}", hash.compute()).eq_ignore_ascii_case(expected))
    {
        bail!("Downloaded archive does not match the requested size/hash");
    }
    temporary.as_file().sync_all()?;
    temporary.persist(target).map_err(|e| e.error)?;
    progress(count);
    Ok(())
}

pub fn verify_archive(
    path: &Path,
    artifact: &ArtifactPlan,
    token: &CancellationToken,
) -> Result<()> {
    check(token)?;
    if artifact.expected_md5.len() != 32
        || !artifact.expected_md5.bytes().all(|b| b.is_ascii_hexdigit())
    {
        bail!("Artifact has no valid exact hash");
    }
    if !path.symlink_metadata()?.is_file() {
        bail!("Archive must be a regular file");
    }
    let mut file = File::open(path)?;
    if artifact
        .expected_size
        .is_some_and(|size| file.metadata().map(|m| m.len() != size).unwrap_or(true))
    {
        bail!("Archive size differs from the pinned file");
    }
    let mut hash = md5::Context::new();
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        check(token)?;
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.consume(&buffer[..n]);
    }
    if !format!("{:x}", hash.compute()).eq_ignore_ascii_case(&artifact.expected_md5) {
        bail!("Archive hash differs from the pinned file");
    }
    Ok(())
}

pub const MASTERLIST_SHA256: &str =
    "95caf8492923b77386fc725150d38c635a581a16bf0e546c3a44eec85afbe484";
pub const MASTERLIST_URL: &str = "https://raw.githubusercontent.com/loot/skyrimse/e3c591ba9c041f23f407a0a0f87f72cc6325aa43/masterlist.yaml";

pub fn masterlist(target: &Path, token: &CancellationToken) -> Result<()> {
    masterlist_for("skyrimspecialedition", target, token)
}

pub fn masterlist_for(domain: &str, target: &Path, token: &CancellationToken) -> Result<()> {
    let pin = &crate::collection::games::require(domain)?.masterlist;
    if target.is_file() && crate::collection::package::digest_file(target)? == pin.sha256 {
        return Ok(());
    }
    transfer(
        &archive_client()?,
        &pin.url(),
        target,
        None,
        None,
        16 * 1024 * 1024,
        token,
        &|_| {},
    )?;
    if crate::collection::package::digest_file(target)? != pin.sha256 {
        bail!("LOOT masterlist did not match the pinned digest");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn authenticated_client_stops_at_redirect_and_archive_client_has_no_key() {
        use std::net::TcpListener;
        let server = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = server.local_addr().unwrap();
        let observed = std::thread::spawn(move || {
            let mut requests = Vec::new();
            for index in 0..2 {
                let (mut stream, _) = server.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut request = String::new();
                let mut reader = std::io::BufReader::new(stream.try_clone().unwrap());
                loop {
                    use std::io::BufRead;
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" || line.is_empty() {
                        break;
                    }
                    request.push_str(&line);
                }
                requests.push(request);
                if index == 0 {
                    write!(stream,"HTTP/1.1 302 Found\r\nLocation: http://{address}/redirect\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
                } else {
                    stream
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                        )
                        .unwrap();
                }
            }
            requests
        });
        let nexus = Nexus::new("test-only-key").unwrap();
        // Test the production client configurations against a local HTTP server;
        // production URL validation independently restricts API/archive origins.
        let response = nexus
            .api
            .get(format!("http://{address}/api"))
            .header("apikey", nexus.key.clone())
            .send()
            .unwrap();
        assert_eq!(response.status().as_u16(), 302);
        nexus
            .archives
            .get(format!("http://{address}/archive"))
            .send()
            .unwrap();
        let requests = observed.join().unwrap();
        assert!(requests[0].contains("test-only-key"));
        assert!(requests[1].starts_with("GET /archive "));
        assert!(!requests[1].contains("test-only-key"));
        assert!(!requests[1].to_ascii_lowercase().contains("apikey:"));
    }
    #[test]
    fn account_origin_and_download_validation() {
        assert!(api_url("/v2/collections/85713/revisions/775970/download_link").is_ok());
        for path in [
            "https://evil.example/v1/x",
            "//evil.example/v1/x",
            "/v1/x?apikey=secret",
            "/v1/../other",
            "/v1/x#secret",
            "/v1/\\evil",
        ] {
            assert!(api_url(path).is_err(), "{path}");
        }
        assert_eq!(
            api_url("/v1/users/validate.json").unwrap().host_str(),
            Some("api.nexusmods.com")
        );
        for source in [
            "http://example.com/file",
            "https://key:secret@example.com/file",
        ] {
            assert!(!valid_archive_url(&Url::parse(source).unwrap()));
        }
        assert!(valid_archive_url(
            &Url::parse("https://cdn.example/file?token=private").unwrap()
        ));
    }
    #[test]
    fn manual_archive_requires_exact_bytes_and_rejects_links() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("a.zip");
        std::fs::write(&path, b"correct").unwrap();
        let artifact = ArtifactPlan {
            id: "a".into(),
            source_type: "nexus".into(),
            domain: "skyrimspecialedition".into(),
            mod_id: 1,
            file_id: 2,
            expected_md5: format!("{:x}", md5::compute(b"correct")),
            expected_size: Some(7),
            update_policy: "exact".into(),
        };
        assert!(verify_archive(&path, &artifact, &Default::default()).is_ok());
        std::fs::write(&path, b"changed").unwrap();
        assert!(verify_archive(&path, &artifact, &Default::default()).is_err());
        #[cfg(unix)]
        {
            let link = temp.path().join("link");
            std::os::unix::fs::symlink(&path, &link).unwrap();
            assert!(verify_archive(&link, &artifact, &Default::default()).is_err());
        }
    }
}
