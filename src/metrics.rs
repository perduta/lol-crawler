//! Per-region crawl counters + the 60 s throughput reporter.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::config::Region;
use crate::ratelimit::LimiterRegistry;

#[derive(Default)]
pub struct Metrics {
    /// platform -> matches stored since start
    games: HashMap<&'static str, AtomicU64>,
}

impl Metrics {
    pub fn new(regions: &[Region]) -> Self {
        let mut games = HashMap::new();
        for r in regions {
            games.insert(r.platform, AtomicU64::new(0));
        }
        Self { games }
    }

    pub fn inc_games(&self, platform: &str) {
        if let Some(c) = self.games.get(platform) {
            c.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn totals(&self) -> Vec<(&'static str, u64)> {
        let mut v: Vec<_> = self
            .games
            .iter()
            .map(|(p, c)| (*p, c.load(Ordering::Relaxed)))
            .collect();
        v.sort();
        v
    }
}

/// Prints, every 60 s: games stored per region in that window (+ running
/// totals), full-history sample counts, and requests sent per routing host.
pub async fn reporter(
    metrics: Arc<Metrics>,
    limiters: Arc<LimiterRegistry>,
    store: Arc<tokio::sync::Mutex<crate::storage::Store>>,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    let mut prev_games: HashMap<&'static str, u64> = HashMap::new();
    let mut prev_valid: HashMap<String, u64> = HashMap::new();
    let mut prev_reqs: HashMap<String, u64> = HashMap::new();
    let mut window = 0u32;
    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(60)) => {}
            _ = stop.changed() => return,
        }
        window += 1;

        let games_line = metrics
            .totals()
            .into_iter()
            .map(|(p, total)| {
                let prev = prev_games.insert(p, total).unwrap_or(0);
                format!("{p}=+{} (total {total})", total - prev)
            })
            .collect::<Vec<_>>()
            .join("  ");

        let valid_line = {
            let store = store.lock().await;
            store
                .valid_sample_totals()
                .into_iter()
                .map(|(p, total)| {
                    let prev = prev_valid.insert(p.clone(), total).unwrap_or(0);
                    format!("{p}=+{} (total {total})", total - prev)
                })
                .collect::<Vec<_>>()
                .join("  ")
        };

        let reqs_line = limiters
            .sent_totals()
            .into_iter()
            .map(|(h, total)| {
                let prev = prev_reqs.insert(h.clone(), total).unwrap_or(0);
                format!("{h}=+{}", total - prev)
            })
            .collect::<Vec<_>>()
            .join("  ");

        tracing::info!(
            "[window {window:02}] games/60s: {games_line} | valid samples: {valid_line} | req/60s: {reqs_line}"
        );
    }
}
