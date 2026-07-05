//! Typed Riot API surface for the crawl logic — but the server never talks
//! to Riot itself: every call becomes an opaque GET job handed to a crawler
//! node via the [`crate::broker::Broker`], and the JSON that comes back is
//! parsed here. Same method names and semantics the crawl loops always had.

use std::sync::Arc;

use anyhow::Result;
use serde::Deserialize;

use crate::broker::Broker;
use crate::config::{self, Region};

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
    broker: Arc<Broker>,
}

impl RiotClient {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }

    async fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        host: &str,
        method: &'static str,
        path: String,
    ) -> Result<Option<T>> {
        match self.broker.fetch(host, method, path).await? {
            Some(body) => Ok(Some(serde_json::from_str(&body)?)),
            None => Ok(None),
        }
    }

    /// Full apex league (`challengerleagues` | `grandmasterleagues` |
    /// `masterleagues`), platform host, one request.
    pub async fn apex_league(&self, region: Region, league: &str) -> Result<LeagueList> {
        let path = format!("/lol/league/v4/{league}/by-queue/{}", config::RANKED_QUEUE_TYPE);
        Ok(self
            .get_json(region.platform_host, "league-v4.apex", path)
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
            .get_json(region.platform_host, "league-v4.entries", path)
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
            .get_json(region.platform_host, "league-v4.by-puuid", path)
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
            .get_json(region.regional_host, "match-v5.ids", path)
            .await?
            .unwrap_or_default())
    }

    /// Full match JSON (regional host). None if the API 404s.
    pub async fn match_raw(&self, region: Region, match_id: &str) -> Result<Option<String>> {
        let path = format!("/lol/match/v5/matches/{match_id}");
        self.broker.fetch(region.regional_host, "match-v5.match", path).await
    }

    /// Full timeline JSON (regional host). None if the API 404s.
    pub async fn timeline_raw(&self, region: Region, match_id: &str) -> Result<Option<String>> {
        let path = format!("/lol/match/v5/matches/{match_id}/timeline");
        self.broker.fetch(region.regional_host, "match-v5.timeline", path).await
    }
}
