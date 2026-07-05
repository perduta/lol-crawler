//! Thin Riot API client: rate-limited GETs with retry/backoff per class of error.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail};
use serde::Deserialize;

use crate::config::{self, Region};
use crate::ratelimit::{LimiterRegistry, RateLimiter};

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct LeagueEntry {
    pub puuid: String,
    pub queue_type: String,
    pub tier: String,
    pub rank: String,
    pub league_points: i32,
    pub wins: i32,
    pub losses: i32,
}

/// Apex league list (challenger/grandmaster/master): the whole league in
/// one response.
#[derive(Debug, Deserialize)]
pub struct LeagueList {
    #[serde(default)]
    pub entries: Vec<LeagueListEntry>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct LeagueListEntry {
    #[serde(default)]
    pub puuid: String,
    #[serde(default)]
    pub league_points: i32,
    #[serde(default)]
    pub wins: i32,
    #[serde(default)]
    pub losses: i32,
}

pub struct RiotClient {
    http: reqwest::Client,
    api_key: String,
    limiters: Arc<LimiterRegistry>,
}

/// Outcome of a GET: body, definitively-missing, or error (after retries).
enum GetResult {
    Body(String),
    NotFound,
}

impl RiotClient {
    pub fn new(api_key: String, limiters: Arc<LimiterRegistry>) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("http client");
        Self { http, api_key, limiters }
    }

    fn limiter(&self, host: &str) -> Arc<RateLimiter> {
        self.limiters.get(host)
    }

    async fn get(&self, host: &str, path_and_query: &str) -> Result<GetResult> {
        let url = format!("https://{host}.api.riotgames.com{path_and_query}");
        let limiter = self.limiter(host);
        let mut attempts = 0u32;
        loop {
            attempts += 1;
            limiter.acquire().await;
            let resp = self
                .http
                .get(&url)
                .header("X-Riot-Token", &self.api_key)
                .send()
                .await;
            match resp {
                Ok(r) => {
                    let status = r.status();
                    if status.is_success() {
                        return Ok(GetResult::Body(r.text().await?));
                    }
                    match status.as_u16() {
                        404 => return Ok(GetResult::NotFound),
                        429 => {
                            let retry_after = r
                                .headers()
                                .get("Retry-After")
                                .and_then(|v| v.to_str().ok())
                                .and_then(|v| v.parse::<u64>().ok())
                                .unwrap_or(10);
                            limiter.cooldown(retry_after).await;
                            // 429 doesn't count against attempts; the budget is the fix.
                            attempts -= 1;
                        }
                        401 | 403 => {
                            tracing::error!(
                                url,
                                status = status.as_u16(),
                                "API key rejected — likely expired dev key. \
                                 Update api_key.env and restart."
                            );
                            tokio::time::sleep(Duration::from_secs(60)).await;
                            if attempts >= 3 {
                                bail!("API key rejected ({status})");
                            }
                        }
                        500..=599 => {
                            tracing::warn!(url, status = status.as_u16(), "server error, retrying");
                            tokio::time::sleep(Duration::from_secs(5 * attempts as u64)).await;
                            if attempts >= 4 {
                                bail!("giving up after {attempts} attempts: {status} {url}");
                            }
                        }
                        _ => bail!("unexpected status {status} for {url}"),
                    }
                }
                Err(e) => {
                    tracing::warn!(url, error = %e, "request error, retrying");
                    tokio::time::sleep(Duration::from_secs(5 * attempts as u64)).await;
                    if attempts >= 4 {
                        return Err(e.into());
                    }
                }
            }
        }
    }

    async fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        host: &str,
        path: &str,
    ) -> Result<Option<T>> {
        match self.get(host, path).await? {
            GetResult::Body(body) => Ok(Some(serde_json::from_str(&body)?)),
            GetResult::NotFound => Ok(None),
        }
    }

    /// Full apex league (`challengerleagues` | `grandmasterleagues` |
    /// `masterleagues`), platform host, one request.
    pub async fn apex_league(&self, region: Region, league: &str) -> Result<LeagueList> {
        let path = format!("/lol/league/v4/{league}/by-queue/{}", config::RANKED_QUEUE_TYPE);
        Ok(self
            .get_json(region.platform_host, &path)
            .await?
            .unwrap_or(LeagueList { entries: Vec::new() }))
    }

    /// One page of a tier/division ladder (platform host).
    pub async fn league_entries_page(
        &self,
        region: Region,
        tier: &str,
        division: &str,
        page: u32,
    ) -> Result<Vec<LeagueEntry>> {
        let path = format!(
            "/lol/league/v4/entries/{}/{tier}/{division}?page={page}",
            config::RANKED_QUEUE_TYPE
        );
        Ok(self
            .get_json(region.platform_host, &path)
            .await?
            .unwrap_or_default())
    }

    /// All league entries for one player (platform host).
    pub async fn league_entries_by_puuid(
        &self,
        region: Region,
        puuid: &str,
    ) -> Result<Vec<LeagueEntry>> {
        let path = format!("/lol/league/v4/entries/by-puuid/{puuid}");
        Ok(self
            .get_json(region.platform_host, &path)
            .await?
            .unwrap_or_default())
    }

    /// Ranked matchlist for a player (regional host), newest first.
    pub async fn match_ids(&self, region: Region, puuid: &str, start_time_s: i64) -> Result<Vec<String>> {
        let path = format!(
            "/lol/match/v5/matches/by-puuid/{puuid}/ids?queue={}&start=0&count=100&startTime={start_time_s}",
            config::QUEUE_ID
        );
        Ok(self
            .get_json(region.regional_host, &path)
            .await?
            .unwrap_or_default())
    }

    /// Full match JSON (regional host). None if the API 404s.
    pub async fn match_raw(&self, region: Region, match_id: &str) -> Result<Option<String>> {
        let path = format!("/lol/match/v5/matches/{match_id}");
        match self.get(region.regional_host, &path).await? {
            GetResult::Body(b) => Ok(Some(b)),
            GetResult::NotFound => Ok(None),
        }
    }

    /// Full timeline JSON (regional host). None if the API 404s.
    pub async fn timeline_raw(&self, region: Region, match_id: &str) -> Result<Option<String>> {
        let path = format!("/lol/match/v5/matches/{match_id}/timeline");
        match self.get(region.regional_host, &path).await? {
            GetResult::Body(b) => Ok(Some(b)),
            GetResult::NotFound => Ok(None),
        }
    }
}
