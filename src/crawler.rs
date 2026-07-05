//! Per-region crawl loop — apex-cohort strategy.
//!
//! Goal: maximize full-history training samples. We maintain a *cohort*: a
//! player set whose every ranked game we fetch. Seeded from the apex leagues
//! (challenger/GM/master — the most closed matchmaking pool), grown two ways:
//! leak-driven adoption (outsiders seen in >= ADOPTION_THRESHOLD stored
//! matches join, patching closure holes where they occur) and ladder-band
//! expansion (Diamond I downward) only when the frontier is idle and the
//! budget brake is off. Non-cohort players popped from the frontier (legacy
//! entries) are dropped without spending requests.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use futures::StreamExt;
use tokio::sync::{Mutex, mpsc, watch};

use crate::config::{self, Region};
use crate::metrics::Metrics;
use crate::record;
use crate::riot::RiotClient;
use crate::storage::{
    BUCKET_PRIORITY, COHORT_SRC_APEX, COHORT_SRC_LADDER, FrontierTask, RankSnap, Store,
};

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

pub struct RegionCrawler {
    region: Region,
    client: Arc<RiotClient>,
    store: Arc<Mutex<Store>>,
    stop: watch::Receiver<bool>,
    metrics: Arc<Metrics>,
    /// Freshly adopted players, handed to the adoption worker for a rank
    /// snapshot + frontier enqueue (platform-host budget).
    adopt_tx: mpsc::UnboundedSender<(u32, String)>,
}

impl RegionCrawler {
    pub fn spawn(
        region: Region,
        client: Arc<RiotClient>,
        store: Arc<Mutex<Store>>,
        stop: watch::Receiver<bool>,
        metrics: Arc<Metrics>,
    ) -> Vec<tokio::task::JoinHandle<()>> {
        let (adopt_tx, adopt_rx) = mpsc::unbounded_channel();
        let crawler = RegionCrawler {
            region,
            client: client.clone(),
            store: store.clone(),
            stop: stop.clone(),
            metrics,
            adopt_tx,
        };
        let adoption_worker = AdoptionWorker {
            region,
            client,
            store,
            stop,
            rx: adopt_rx,
        };
        vec![
            tokio::spawn(async move {
                if let Err(e) = crawler.run().await {
                    tracing::error!(platform = region.platform, error = %e, "crawler died");
                }
            }),
            tokio::spawn(async move {
                adoption_worker.run().await;
            }),
        ]
    }

    fn stopped(&self) -> bool {
        *self.stop.borrow()
    }

    async fn run(&self) -> Result<()> {
        tracing::info!(platform = self.region.platform, "crawler started");
        let mut last_commit = std::time::Instant::now();
        let mut matches_stored = 0u64;
        let mut braked = false;

        while !self.stopped() {
            self.maybe_seed(&mut braked).await?;

            let task = {
                let mut store = self.store.lock().await;
                store.frontier_pop_due(self.region.platform, now_ms())?
            };

            match task {
                Some((_bucket, pid, task)) => {
                    // Legacy/non-cohort entries are dropped without spending
                    // any requests; the cohort defines who we crawl.
                    let in_cohort = {
                        let store = self.store.lock().await;
                        store.is_cohort(pid)
                    };
                    if !in_cohort {
                        tracing::debug!(platform = self.region.platform, pid, "dropped non-cohort task");
                        continue;
                    }
                    match self.process_player(pid, &task).await {
                        Ok(n) => matches_stored += n,
                        Err(e) => {
                            tracing::warn!(platform = self.region.platform, error = %e,
                                "process_player failed; player rescheduled");
                            // Put the player back a bit later rather than losing them.
                            let mut store = self.store.lock().await;
                            store.frontier_push(
                                self.region.platform,
                                BUCKET_PRIORITY,
                                now_ms() + 3600 * 1000,
                                pid,
                                &task,
                            )?;
                        }
                    }
                }
                None => {
                    let expanded = self.maybe_expand(&mut braked).await?;
                    if !expanded {
                        let next_due = {
                            let store = self.store.lock().await;
                            store.frontier_next_due(self.region.platform)?
                        };
                        let sleep_ms = next_due
                            .map(|due| due.saturating_sub(now_ms()).clamp(1_000, 30_000))
                            .unwrap_or(30_000);
                        tracing::debug!(platform = self.region.platform, sleep_ms, "frontier idle");
                        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                    }
                }
            }

            if last_commit.elapsed().as_secs() >= config::FLUSH_INTERVAL_SECS || {
                let store = self.store.lock().await;
                store.should_commit()
            } {
                let mut store = self.store.lock().await;
                store.commit()?;
                last_commit = std::time::Instant::now();
                tracing::info!(
                    platform = self.region.platform,
                    matches_stored,
                    "committed"
                );
            }
        }

        self.store.lock().await.commit()?;
        tracing::info!(platform = self.region.platform, matches_stored, "crawler stopped");
        Ok(())
    }

    /// Recomputes the budget brake with hysteresis: on while the frontier
    /// backlog shows we can't even keep up with the current cohort.
    async fn update_brake(&self, braked: &mut bool) -> Result<()> {
        let overdue = {
            let store = self.store.lock().await;
            store.frontier_overdue_count(
                self.region.platform,
                now_ms(),
                config::BRAKE_OVERDUE_GRACE_MS,
                config::BRAKE_ON_COUNT * 2,
            )?
        };
        if !*braked && overdue >= config::BRAKE_ON_COUNT {
            *braked = true;
            tracing::info!(platform = self.region.platform, overdue, "budget brake ON — cohort growth paused");
        } else if *braked && overdue <= config::BRAKE_OFF_COUNT {
            *braked = false;
            tracing::info!(platform = self.region.platform, overdue, "budget brake OFF — cohort growth resumed");
        }
        Ok(())
    }

    /// Registers a batch of league entries as cohort members: player ids,
    /// rank snapshot, frontier task (dedup keeps existing schedules).
    async fn enroll(
        &self,
        entries: Vec<(String, RankSnap)>,
        source: u8,
    ) -> Result<usize> {
        let ts = now_ms();
        let mut store = self.store.lock().await;
        let puuids: Vec<String> = entries.iter().map(|(p, _)| p.clone()).collect();
        let pids = store.assign_player_ids(&puuids)?;
        let newly = store.cohort_add_batch(&pids, source, ts)?;
        for ((puuid, snap), pid) in entries.iter().zip(&pids) {
            store.add_rank_snapshot(*pid, ts, snap)?;
            let _ = puuid;
        }
        let items: Vec<(u32, FrontierTask)> = pids
            .iter()
            .zip(&puuids)
            .map(|(pid, puuid)| {
                (*pid, FrontierTask { puuid: puuid.clone(), last_visit_ms: 0 })
            })
            .collect();
        store.frontier_push_batch(self.region.platform, BUCKET_PRIORITY, ts, &items)?;
        Ok(newly.len())
    }

    /// Re-fetches the apex leagues on a cadence: 1 request per league,
    /// snapshots everyone's LP, enrolls new arrivals. Lower leagues are only
    /// pulled while the brake is off, so a saturated crawler stops widening.
    async fn maybe_seed(&self, braked: &mut bool) -> Result<()> {
        // Key is versioned per strategy so stale timestamps from an older
        // seeding scheme can't suppress the first apex seed.
        let meta_key_ts = format!("seed_apex_{}", self.region.platform);
        let now_s = (now_ms() / 1000) as u32;
        {
            let store = self.store.lock().await;
            if let Some(last) = store.meta_get_u32(&meta_key_ts)? {
                if (now_s.saturating_sub(last) as u64) < config::SEED_INTERVAL_SECS {
                    return Ok(());
                }
            }
        }

        for (i, (league, tier)) in config::APEX_LEAGUES.iter().enumerate() {
            if self.stopped() {
                return Ok(());
            }
            self.update_brake(braked).await?;
            if i > 0 && *braked {
                tracing::info!(platform = self.region.platform, league, "seed skipped (braked)");
                break;
            }
            let list = self.client.apex_league(self.region, league).await?;
            let entries: Vec<(String, RankSnap)> = list
                .entries
                .into_iter()
                .filter(|e| !e.puuid.is_empty())
                .map(|e| {
                    (
                        e.puuid,
                        RankSnap {
                            tier: tier.to_string(),
                            division: "I".to_string(),
                            lp: e.league_points,
                            wins: e.wins,
                            losses: e.losses,
                        },
                    )
                })
                .collect();
            let total = entries.len();
            let newly = self.enroll(entries, COHORT_SRC_APEX).await?;
            tracing::info!(platform = self.region.platform, league, total, newly, "apex league seeded");
        }

        self.store.lock().await.meta_set_u32(&meta_key_ts, now_s)?;
        Ok(())
    }

    /// Ladder-band fallback expansion: one page per idle tick, only while
    /// the brake is off. Returns true if it enrolled anyone.
    async fn maybe_expand(&self, braked: &mut bool) -> Result<bool> {
        self.update_brake(braked).await?;
        if *braked {
            return Ok(false);
        }
        let cursor_key = format!("expand_cursor_{}", self.region.platform);
        let cursor = {
            let store = self.store.lock().await;
            store.meta_get_u32(&cursor_key)?.unwrap_or(0)
        };
        let band_idx = (cursor / 10_000) as usize;
        let page = (cursor % 10_000).max(1);
        let Some((tier, division)) = config::EXPANSION_BANDS.get(band_idx) else {
            return Ok(false); // ladder exhausted; deepening coverage is all that's left
        };

        let entries = self
            .client
            .league_entries_page(self.region, tier, division, page)
            .await?;
        let next_cursor = if entries.is_empty() {
            (band_idx as u32 + 1) * 10_000 + 1 // band done, move down
        } else {
            band_idx as u32 * 10_000 + page + 1
        };
        let batch: Vec<(String, RankSnap)> = entries
            .into_iter()
            .map(|e| {
                (
                    e.puuid,
                    RankSnap {
                        tier: e.tier,
                        division: e.rank,
                        lp: e.league_points,
                        wins: e.wins,
                        losses: e.losses,
                    },
                )
            })
            .collect();
        let total = batch.len();
        let newly = if total > 0 { self.enroll(batch, COHORT_SRC_LADDER).await? } else { 0 };
        self.store.lock().await.meta_set_u32(&cursor_key, next_cursor)?;
        if newly > 0 {
            tracing::info!(platform = self.region.platform, tier, division, page, newly, "ladder expansion");
        }
        Ok(newly > 0)
    }

    /// Fetches a player's matchlist and downloads every unseen match.
    /// Returns the number of matches stored.
    async fn process_player(&self, pid: u32, task: &FrontierTask) -> Result<u64> {
        let start_time_s =
            (now_ms() / 1000) as i64 - config::MAX_MATCH_AGE_DAYS * 24 * 3600;

        // Matchlist (regional host) and rank refresh (platform host) hit
        // different budgets — fetch them concurrently. The snapshot feeds
        // as-of-time elo joins at materialization.
        let (ids, rank_result) = tokio::join!(
            self.client.match_ids(self.region, &task.puuid, start_time_s),
            self.client.league_entries_by_puuid(self.region, &task.puuid),
        );
        let ids = ids?;

        match rank_result {
            Ok(entries) => {
                if let Some(e) = entries
                    .iter()
                    .find(|e| e.queue_type == config::RANKED_QUEUE_TYPE)
                {
                    let mut store = self.store.lock().await;
                    store.add_rank_snapshot(
                        pid,
                        now_ms(),
                        &RankSnap {
                            tier: e.tier.clone(),
                            division: e.rank.clone(),
                            lp: e.league_points,
                            wins: e.wins,
                            losses: e.losses,
                        },
                    )?;
                }
            }
            Err(e) => tracing::warn!(error = %e, "rank refresh failed"),
        }

        let mut new_ids = Vec::new();
        {
            let store = self.store.lock().await;
            for id in &ids {
                let Ok((platform, num)) = record::split_match_id(id) else {
                    continue;
                };
                // Matchlists can contain games played on other shards; those
                // are that shard's job (only relevant with several enabled).
                if platform != self.region.platform {
                    continue;
                }
                if !store.is_seen(platform, num) {
                    new_ids.push(id.clone());
                }
            }
        }

        // Pipeline match downloads so in-flight latency never idles the
        // rate budget; the limiter alone decides the pace. (Futures are
        // collected eagerly to sidestep a rustc HRTB inference limitation.)
        let fetches: Vec<_> = new_ids
            .iter()
            .map(|match_id| self.fetch_counted(match_id, pid))
            .collect();
        let stored = futures::stream::iter(fetches)
            .buffer_unordered(config::MATCH_FETCH_CONCURRENCY)
            .fold(0u64, |acc, n| async move { acc + n })
            .await;

        // Reschedule by activity (port of the old ComputeExpiracyDays).
        let now = now_ms();
        let days_passed = if task.last_visit_ms == 0 {
            14.0
        } else {
            ((now - task.last_visit_ms) as f64 / 86_400_000.0).max(0.1)
        };
        let matches_per_day = ((3.0 + new_ids.len() as f64) / (7.0 + days_passed)).max(0.1);
        let revisit_days = (config::REVISIT_AFTER_MATCHES / matches_per_day)
            .clamp(config::MIN_REVISIT_DAYS, config::MAX_REVISIT_DAYS);
        let due = now + (revisit_days * 86_400_000.0) as u64;

        let mut store = self.store.lock().await;
        store.frontier_push(
            self.region.platform,
            BUCKET_PRIORITY,
            due,
            pid,
            &FrontierTask { puuid: task.puuid.clone(), last_visit_ms: now },
        )?;
        Ok(stored)
    }

    /// fetch_and_store_match wrapper for the pipeline: 1 if stored, else 0.
    async fn fetch_counted(&self, match_id: &str, pid: u32) -> u64 {
        if self.stopped() {
            return 0;
        }
        match self.fetch_and_store_match(match_id, pid).await {
            Ok(true) => 1,
            Ok(false) => 0,
            Err(e) => {
                tracing::warn!(match_id, error = %e, "match fetch failed; will retry later");
                0
            }
        }
    }

    /// Returns Ok(true) if a record was stored, Ok(false) if the match was
    /// skipped for a permanent reason (marked seen either way).
    async fn fetch_and_store_match(&self, match_id: &str, _current_pid: u32) -> Result<bool> {
        let (platform, game_id) = record::split_match_id(match_id)?;

        // Match and timeline concurrently: the matchlist already filtered on
        // queue, so a wasted timeline for a stray non-420 match is rare and
        // cheaper than serializing every pair on latency.
        let (match_json, timeline_json) = tokio::join!(
            self.client.match_raw(self.region, match_id),
            self.client.timeline_raw(self.region, match_id),
        );
        let (Some(match_json), timeline_json) = (match_json?, timeline_json?) else {
            self.store.lock().await.mark_seen(platform, game_id);
            return Ok(false);
        };

        let parsed = match record::parse_match(&match_json, timeline_json.as_deref()) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(match_id, error = %e, "unparseable match, skipping");
                self.store.lock().await.mark_seen(platform, game_id);
                return Ok(false);
            }
        };
        if parsed.record.queue_id != config::QUEUE_ID
            || parsed.record.platform != self.region.platform
        {
            self.store.lock().await.mark_seen(platform, game_id);
            return Ok(false);
        }
        let mut rec = parsed.record;

        let mut store = self.store.lock().await;
        let ts_ms = rec.game_start_ms.max(0) as u64;
        let outcome = store.store_match(&mut rec, &parsed.puuids, ts_ms)?;

        if game_id % 1000 < config::RAW_SAMPLE_PERMILLE {
            store.save_raw_sample(platform, match_id, &match_json, timeline_json.as_deref())?;
        }
        self.metrics.inc_games(platform);

        // Newly adopted leak-players get a rank snapshot + frontier entry.
        for (pid, puuid) in outcome.adopted {
            let _ = self.adopt_tx.send((pid, puuid));
        }
        Ok(true)
    }
}

/// Consumes freshly adopted players: snapshots their rank (platform-host
/// budget, doesn't compete with match fetching) and enqueues their first
/// crawl visit.
struct AdoptionWorker {
    region: Region,
    client: Arc<RiotClient>,
    store: Arc<Mutex<Store>>,
    stop: watch::Receiver<bool>,
    rx: mpsc::UnboundedReceiver<(u32, String)>,
}

impl AdoptionWorker {
    async fn run(mut self) {
        while !*self.stop.borrow() {
            let msg = tokio::select! {
                m = self.rx.recv() => m,
                _ = tokio::time::sleep(Duration::from_secs(5)) => continue,
            };
            let Some((pid, puuid)) = msg else { break };
            if let Err(e) = self.handle(pid, &puuid).await {
                tracing::warn!(error = %e, "adoption worker error");
            }
        }
    }

    async fn handle(&self, pid: u32, puuid: &str) -> Result<()> {
        let entries = self.client.league_entries_by_puuid(self.region, puuid).await?;
        let solo = entries
            .iter()
            .find(|e| e.queue_type == config::RANKED_QUEUE_TYPE);

        let mut store = self.store.lock().await;
        let ts = now_ms();
        if let Some(e) = solo {
            store.add_rank_snapshot(
                pid,
                ts,
                &RankSnap {
                    tier: e.tier.clone(),
                    division: e.rank.clone(),
                    lp: e.league_points,
                    wins: e.wins,
                    losses: e.losses,
                },
            )?;
        }
        store.frontier_push(
            self.region.platform,
            BUCKET_PRIORITY,
            ts,
            pid,
            &FrontierTask { puuid: puuid.to_string(), last_visit_ms: 0 },
        )?;
        Ok(())
    }
}
