//! Nexus Mods downloader with rate limiting and Premium support

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::{Client, Response};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::RwLock;
use tracing::{debug, info, warn};

const API_BASE_URL: &str = "https://api.nexusmods.com";
const AUTH_HEADER: &str = "apikey";

/// Nexus API rate limits (tracked from response headers)
#[derive(Debug, Clone)]
pub struct NexusRateLimits {
    pub hourly_limit: usize,
    pub hourly_remaining: usize,
    pub hourly_reset: Option<DateTime<Utc>>,
    pub daily_limit: usize,
    pub daily_remaining: usize,
    pub daily_reset: Option<DateTime<Utc>>,
}

impl Default for NexusRateLimits {
    fn default() -> Self {
        Self {
            hourly_limit: 100,
            hourly_remaining: 100,
            hourly_reset: None,
            // Premium users have 20,000 daily limit
            daily_limit: 20000,
            daily_remaining: 20000,
            daily_reset: None,
        }
    }
}

/// User info from Nexus API validation
#[derive(Debug, Clone, Deserialize)]
pub struct NexusUserInfo {
    pub name: String,
    #[serde(rename = "is_premium?")]
    pub is_premium: bool,
    #[serde(rename = "is_supporter?")]
    pub is_supporter: bool,
    pub user_id: u64,
}

impl NexusRateLimits {
    /// Parse rate limits from response headers
    fn from_response(response: &Response) -> Option<Self> {
        let headers = response.headers();

        fn get_header<T: std::str::FromStr>(headers: &HeaderMap, name: &str) -> Option<T> {
            headers.get(name)?.to_str().ok()?.parse().ok()
        }

        Some(Self {
            hourly_limit: get_header(headers, "X-RL-Hourly-Limit")?,
            hourly_remaining: get_header(headers, "X-RL-Hourly-Remaining")?,
            hourly_reset: get_header(headers, "X-RL-Hourly-Reset"),
            daily_limit: get_header(headers, "X-RL-Daily-Limit")?,
            daily_remaining: get_header(headers, "X-RL-Daily-Remaining")?,
            daily_reset: get_header(headers, "X-RL-Daily-Reset"),
        })
    }

    /// Check if we're approaching rate limits
    pub fn is_low(&self) -> bool {
        self.hourly_remaining < 10 || self.daily_remaining < 50
    }

    /// Check if we've hit rate limits
    pub fn is_exhausted(&self) -> bool {
        self.hourly_remaining == 0 || self.daily_remaining == 0
    }
}

/// Nexus Mods API client
pub struct NexusDownloader {
    client: Client,
    /// Current rate limits (updated after each request)
    rate_limits: RwLock<NexusRateLimits>,
    /// Total requests made this session
    request_count: AtomicUsize,
    /// Whether user has Premium status (enables direct API downloads)
    is_premium: AtomicBool,
    /// Whether we've validated the API key
    validated: AtomicBool,
    /// Override to force non-premium mode even if account is premium
    premium_override: AtomicBool,
}

impl NexusDownloader {
    /// Create a new Nexus downloader with API key
    pub fn new(api_key: &str) -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTH_HEADER,
            HeaderValue::from_str(api_key).context("Invalid API key format")?,
        );

        let client = Client::builder()
            .default_headers(headers)
            .user_agent(concat!("clf3/", env!("CARGO_PKG_VERSION")))
            .build()
            .context("Failed to create HTTP client")?;

        Ok(Self {
            client,
            rate_limits: RwLock::new(NexusRateLimits::default()),
            request_count: AtomicUsize::new(0),
            is_premium: AtomicBool::new(false),
            validated: AtomicBool::new(false),
            premium_override: AtomicBool::new(false),
        })
    }

    /// Validate the API key and get user info (including Premium status)
    ///
    /// This should be called once at startup to verify credentials and check
    /// if the user has Premium (which enables direct API downloads without rate limits).
    pub async fn validate(&self) -> Result<NexusUserInfo> {
        let url = format!("{}/v1/users/validate.json", API_BASE_URL);

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .context("Failed to validate API key")?;

        // Update rate limits from response
        if let Some(limits) = NexusRateLimits::from_response(&response) {
            *self.rate_limits.write().unwrap() = limits;
        }

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            bail!("Nexus API key validation failed ({}): {}", status, body);
        }

        let user_info: NexusUserInfo =
            response.json().await.context("Failed to parse user info")?;

        // Store Premium status
        self.is_premium
            .store(user_info.is_premium, Ordering::Relaxed);
        self.validated.store(true, Ordering::Relaxed);

        info!(
            "Nexus user '{}' validated (Premium: {})",
            user_info.name, user_info.is_premium
        );

        Ok(user_info)
    }

    /// Check if user has Premium status
    ///
    /// Premium users can use direct API downloads with 20,000 daily limit.
    /// Non-premium users are limited and may need NXM browser mode.
    ///
    /// Note: If premium override is active, this returns false even if the
    /// account is actually premium, forcing NXM browser mode.
    pub fn is_premium(&self) -> bool {
        if self.premium_override.load(Ordering::Relaxed) {
            return false;
        }
        self.is_premium.load(Ordering::Relaxed)
    }

    /// Set premium override to force non-premium mode
    ///
    /// When `force_non_premium` is true, `is_premium()` will return false
    /// regardless of actual account status, forcing NXM browser mode.
    /// This is useful for users who want to use browser-based downloads
    /// even if they have a premium account.
    pub fn set_premium_override(&self, force_non_premium: bool) {
        self.premium_override
            .store(force_non_premium, Ordering::Relaxed);
        if force_non_premium {
            info!("Premium override enabled - forcing non-premium (NXM browser) mode");
        } else {
            debug!("Premium override disabled - using actual account status");
        }
    }

    /// Check if premium override is currently active
    pub fn is_premium_override_active(&self) -> bool {
        self.premium_override.load(Ordering::Relaxed)
    }

    /// Check if API key has been validated
    pub fn is_validated(&self) -> bool {
        self.validated.load(Ordering::Relaxed)
    }

    /// Get current rate limits
    pub fn rate_limits(&self) -> NexusRateLimits {
        self.rate_limits.read().unwrap().clone()
    }

    /// Get download URL for a file (direct API mode for Premium users)
    ///
    /// Premium users can call this directly with 20,000 daily limit.
    /// Non-premium users may hit rate limits and should use NXM mode.
    pub async fn get_download_link(
        &self,
        game_domain: &str,
        mod_id: u64,
        file_id: u64,
    ) -> Result<String> {
        self.get_download_link_internal(game_domain, mod_id, file_id, None, None)
            .await
    }

    /// Get download URL for a file with NXM key/expires (bypasses hourly limit, uses daily limit)
    ///
    /// The `key` and `expires` parameters come from NXM links generated when the user
    /// clicks "Download with Manager" on the Nexus website. When these params are provided,
    /// the request bypasses the hourly API limit and instead uses the daily download limit.
    pub async fn get_download_link_with_nxm_key(
        &self,
        game_domain: &str,
        mod_id: u64,
        file_id: u64,
        key: &str,
        expires: u64,
    ) -> Result<String> {
        self.get_download_link_internal(game_domain, mod_id, file_id, Some(key), Some(expires))
            .await
    }

    /// Internal method for getting download links with optional NXM key
    async fn get_download_link_internal(
        &self,
        game_domain: &str,
        mod_id: u64,
        file_id: u64,
        nxm_key: Option<&str>,
        nxm_expires: Option<u64>,
    ) -> Result<String> {
        // Log current rate limit state (but don't pre-emptively fail - limits may have reset)
        {
            let limits = self.rate_limits.read().unwrap();
            if limits.is_low() {
                debug!(
                    "Rate limits status: Hourly: {}/{}, Daily: {}/{}",
                    limits.hourly_remaining,
                    limits.hourly_limit,
                    limits.daily_remaining,
                    limits.daily_limit
                );
            }
        }

        // Build URL with optional NXM key/expires params
        let url = if let (Some(key), Some(expires)) = (nxm_key, nxm_expires) {
            // NXM mode: add key/expires to bypass hourly limit
            format!(
                "{}/v1/games/{}/mods/{}/files/{}/download_link.json?key={}&expires={}",
                API_BASE_URL, game_domain, mod_id, file_id, key, expires
            )
        } else {
            // Standard API mode
            format!(
                "{}/v1/games/{}/mods/{}/files/{}/download_link.json",
                API_BASE_URL, game_domain, mod_id, file_id
            )
        };

        debug!("Fetching download link from: {}", url);

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .with_context(|| format!("Failed to fetch download link from {}", url))?;

        // Update rate limits from response
        if let Some(limits) = NexusRateLimits::from_response(&response) {
            debug!(
                "Rate limits: {}/{} hourly, {}/{} daily",
                limits.hourly_remaining,
                limits.hourly_limit,
                limits.daily_remaining,
                limits.daily_limit
            );
            *self.rate_limits.write().unwrap() = limits;
        }

        self.request_count.fetch_add(1, Ordering::Relaxed);

        // Handle errors
        if !response.status().is_success() {
            let status = response.status();

            // Check for rate limit error (429)
            if status.as_u16() == 429 {
                let limits = self.rate_limits.read().unwrap();
                bail!(
                    "Nexus API rate limit hit (429). Hourly: {}/{}, Daily: {}/{}. Wait for reset or use NXM mode.",
                    limits.hourly_remaining,
                    limits.hourly_limit,
                    limits.daily_remaining,
                    limits.daily_limit
                );
            }

            // Check for forbidden (403) - usually means non-Premium trying direct API
            if status.as_u16() == 403 {
                let is_premium = self.is_premium.load(Ordering::Relaxed);
                if !is_premium {
                    bail!(
                        "Nexus API forbidden (403). Your account is not Premium. \
                        Free users need to use NXM browser mode for downloads. \
                        Re-run with --nxm flag or get Nexus Premium for direct downloads."
                    );
                } else {
                    // Premium user got 403 - might be deleted mod or permissions issue
                    bail!(
                        "Nexus API forbidden (403). The mod may be hidden, deleted, or requires special permissions."
                    );
                }
            }

            let body = response.text().await.unwrap_or_default();
            bail!("Nexus API error {}: {}", status, body);
        }

        // Parse response
        let body = response
            .text()
            .await
            .context("Failed to read response body")?;
        let links: Vec<DownloadLink> =
            serde_json::from_str(&body).context("Failed to parse download links response")?;

        links
            .into_iter()
            .next()
            .map(|link| link.uri)
            .context("No download links returned by Nexus API")
    }

    /// Get the mod page URL for manual fallback
    pub fn get_mod_page_url(game_domain: &str, mod_id: u64, file_id: u64) -> String {
        format!(
            "https://www.nexusmods.com/{}/mods/{}?tab=files&file_id={}&nmm=1",
            game_domain.to_lowercase(),
            mod_id,
            file_id
        )
    }

    /// Map game name to Nexus domain name
    pub fn game_domain(game_name: &str) -> &str {
        match game_name.to_lowercase().as_str() {
            "falloutnewvegas" | "fallout new vegas" | "fnv" => "newvegas",
            "fallout3" | "fallout 3" | "fo3" => "fallout3",
            "fallout4" | "fallout 4" | "fo4" => "fallout4",
            "skyrim" => "skyrim",
            "skyrimspecialedition" | "skyrim special edition" | "sse" => "skyrimspecialedition",
            "skyrimvr" => "skyrimspecialedition", // Uses same domain
            "oblivion" => "oblivion",
            "morrowind" => "morrowind",
            "starfield" => "starfield",
            "enderal" => "enderal",
            "enderalspecialedition" => "enderalspecialedition",
            "cyberpunk2077" | "cyberpunk 2077" => "cyberpunk2077",
            "baldursgate3" | "baldur's gate 3" | "bg3" => "baldursgate3",
            "vtmb"
            | "vampirethemasqueradebloodlines"
            | "vampire the masquerade bloodlines"
            | "vampire: the masquerade - bloodlines" => "vampirebloodlines",
            "site" | "moddingtools" => "site", // Modding tools
            _ => game_name,                    // Pass through unknown
        }
    }

    /// Get total requests made this session
    pub fn request_count(&self) -> usize {
        self.request_count.load(Ordering::Relaxed)
    }
}

/// Nexus download link response
#[derive(Debug, Deserialize, Serialize)]
struct DownloadLink {
    #[serde(rename = "URI")]
    uri: String,
    name: String,
    short_name: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_game_domain_mapping() {
        assert_eq!(NexusDownloader::game_domain("falloutnewvegas"), "newvegas");
        assert_eq!(NexusDownloader::game_domain("FalloutNewVegas"), "newvegas");
        assert_eq!(
            NexusDownloader::game_domain("skyrimspecialedition"),
            "skyrimspecialedition"
        );
        assert_eq!(NexusDownloader::game_domain("VtMB"), "vampirebloodlines");
        assert_eq!(
            NexusDownloader::game_domain("VampireTheMasqueradeBloodlines"),
            "vampirebloodlines"
        );
        assert_eq!(NexusDownloader::game_domain("unknowngame"), "unknowngame");
    }

    #[test]
    fn test_rate_limits_default() {
        let limits = NexusRateLimits::default();
        assert!(!limits.is_low());
        assert!(!limits.is_exhausted());
    }

    #[test]
    fn test_rate_limits_low() {
        let limits = NexusRateLimits {
            hourly_remaining: 5,
            ..Default::default()
        };
        assert!(limits.is_low());
        assert!(!limits.is_exhausted());
    }

    #[test]
    fn test_rate_limits_exhausted() {
        let limits = NexusRateLimits {
            hourly_remaining: 0,
            ..Default::default()
        };
        assert!(limits.is_exhausted());
    }

    #[test]
    fn test_premium_override() {
        let downloader = NexusDownloader::new("test_api_key").unwrap();

        // Initially, premium should be false (not validated yet)
        assert!(!downloader.is_premium());
        assert!(!downloader.is_premium_override_active());

        // Manually set premium to true (simulating validated premium account)
        downloader.is_premium.store(true, Ordering::Relaxed);
        assert!(downloader.is_premium());

        // Enable premium override - should force non-premium mode
        downloader.set_premium_override(true);
        assert!(downloader.is_premium_override_active());
        assert!(!downloader.is_premium()); // Should return false despite account being premium

        // Disable override - should return to actual premium status
        downloader.set_premium_override(false);
        assert!(!downloader.is_premium_override_active());
        assert!(downloader.is_premium()); // Should return true again
    }
}
