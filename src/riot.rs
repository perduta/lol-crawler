//! Thin Riot API client: rate-limited GETs with retry/backoff per class of error.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail};
use serde::Deserialize;

use crate::config::{self, Region};
use crate::ratelimit::{LimiterRegistry, parse_limit_header};

fn header<'a>(r: &'a reqwest::Response, name: &str) -> Option<&'a str> {
    r.headers().get(name).and_then(|v| v.to_str().ok())
}

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

    async fn get(&self, host: &str, method: &'static str, path_and_query: &str) -> Result<GetResult> {
        let url = format!("https://{host}.api.riotgames.com{path_and_query}");
        // Method limiter first: waiting on a tight method budget must not
        // consume app-budget slots that other endpoints could use.
        let method_limiter = self.limiters.method(host, method);
        let app_limiter = self.limiters.app(host);
        let mut attempts = 0u32;
        loop {
            attempts += 1;
            method_limiter.acquire().await;
            app_limiter.acquire().await;
            let resp = self
                .http
                .get(&url)
                .header("X-Riot-Token", &self.api_key)
                .send()
                .await;
            match resp {
                Ok(r) => {
                    // Adopt the server-declared limit windows (a production
                    // key's budgets apply with no config change).
                    if let Some(h) = header(&r, "X-App-Rate-Limit") {
                        app_limiter.update_specs(parse_limit_header(h)).await;
                    }
                    if let Some(h) = header(&r, "X-Method-Rate-Limit") {
                        method_limiter.update_specs(parse_limit_header(h)).await;
                    }
                    let status = r.status();
                    if status.is_success() {
                        return Ok(GetResult::Body(r.text().await?));
                    }
                    match status.as_u16() {
                        404 => return Ok(GetResult::NotFound),
                        429 => {
                            let retry_after = header(&r, "Retry-After")
                                .and_then(|v| v.parse::<u64>().ok())
                                .unwrap_or(10);
                            // Scope the cooldown to the layer that tripped.
                            if header(&r, "X-Rate-Limit-Type") == Some("method") {
                                method_limiter.cooldown(retry_after).await;
                            } else {
                                app_limiter.cooldown(retry_after).await;
                            }
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
        method: &'static str,
        path: &str,
    ) -> Result<Option<T>> {
        match self.get(host, method, path).await? {
            GetResult::Body(body) => Ok(Some(serde_json::from_str(&body)?)),
            GetResult::NotFound => Ok(None),
        }
    }

    /// Full apex league (`challengerleagues` | `grandmasterleagues` |
    /// `masterleagues`), platform host, one request.
    pub async fn apex_league(&self, region: Region, league: &str) -> Result<LeagueList> {
        let path = format!("/lol/league/v4/{league}/by-queue/{}", config::RANKED_QUEUE_TYPE);
        Ok(self
            .get_json(region.platform_host, "league-v4.apex", &path)
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
            .get_json(region.platform_host, "league-v4.entries", &path)
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
            .get_json(region.platform_host, "league-v4.by-puuid", &path)
            .await?
            .unwrap_or_default())
    }

    /// One page (up to 100 ids) of a player's ranked matchlist (regional
    /// host), newest first. `start_time_s: None` = no age cutoff (deep
    /// backfill walks the full history the API still has).
    pub async fn match_ids_page(
        &self,
        region: Region,
        puuid: &str,
        start_time_s: Option<i64>,
        start: u32,
    ) -> Result<Vec<String>> {
        let mut path = format!(
            "/lol/match/v5/matches/by-puuid/{puuid}/ids?queue={}&start={start}&count=100",
            config::QUEUE_ID
        );
        if let Some(t) = start_time_s {
            path.push_str(&format!("&startTime={t}"));
        }
        Ok(self
            .get_json(region.regional_host, "match-v5.ids", &path)
            .await?
            .unwrap_or_default())
    }

    /// Full match JSON (regional host). None if the API 404s.
    pub async fn match_raw(&self, region: Region, match_id: &str) -> Result<Option<String>> {
        let path = format!("/lol/match/v5/matches/{match_id}");
        match self.get(region.regional_host, "match-v5.match", &path).await? {
            GetResult::Body(b) => Ok(Some(b)),
            GetResult::NotFound => Ok(None),
        }
    }

    /// Full timeline JSON (regional host). None if the API 404s.
    pub async fn timeline_raw(&self, region: Region, match_id: &str) -> Result<Option<String>> {
        let path = format!("/lol/match/v5/matches/{match_id}/timeline");
        match self.get(region.regional_host, "match-v5.timeline", &path).await? {
            GetResult::Body(b) => Ok(Some(b)),
            GetResult::NotFound => Ok(None),
        }
    }
}
