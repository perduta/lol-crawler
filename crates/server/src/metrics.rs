//! Per-region crawl counters + the 60 s throughput reporter.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::broker::Broker;
use crate::config::Region;
use crate::registry::Registry;

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
/// totals), full-history sample counts, per-node request counts / audit
/// verdicts / key state, and broker queue depth.
pub async fn reporter(
    metrics: Arc<Metrics>,
    registry: Arc<Registry>,
    broker: Arc<Broker>,
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

        let nodes_line = {
            let reports = registry.report();
            if reports.is_empty() {
                "none enrolled".to_string()
            } else {
                reports
                    .into_iter()
                    .map(|r| {
                        let prev = prev_reqs.insert(r.name.clone(), r.completed).unwrap_or(0);
                        let mut s = format!("{}=+{}", r.name, r.completed - prev);
                        if r.audits_pass + r.audits_soft_fail + r.audits_hard_fail > 0 {
                            s.push_str(&format!(
                                " audits:{}ok/{}soft/{}FAIL",
                                r.audits_pass, r.audits_soft_fail, r.audits_hard_fail
                            ));
                        }
                        if r.key_bad {
                            s.push_str(" KEY-BAD");
                        }
                        match r.idle_secs {
                            Some(i) if i > 120 => s.push_str(&format!(" idle:{}m", i / 60)),
                            None => s.push_str(" offline"),
                            _ => {}
                        }
                        s
                    })
                    .collect::<Vec<_>>()
                    .join("  ")
            }
        };
        let (queued, leased) = broker.depth();

        tracing::info!(
            "[window {window:02}] games/60s: {games_line} | valid samples: {valid_line} | nodes: {nodes_line} | jobs: {queued} queued, {leased} leased"
        );
    }
}
