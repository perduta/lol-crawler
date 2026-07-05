//! Sliding-window rate limiter, one instance per routing host.
//!
//! Enforces both dev-key windows (20 req / 1 s and 100 req / 2 min) and a
//! shared cooldown that 429 responses (Retry-After) push out.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

use crate::config;

struct Windows {
    /// Send times of recent requests, pruned past the sustained window.
    sent: VecDeque<Instant>,
    /// Do not send anything before this time (set on 429).
    cooldown_until: Option<Instant>,
}

pub struct RateLimiter {
    inner: Mutex<Windows>,
    host: String,
    sent_total: std::sync::atomic::AtomicU64,
}

impl RateLimiter {
    pub fn new(host: &str) -> Self {
        Self {
            inner: Mutex::new(Windows { sent: VecDeque::new(), cooldown_until: None }),
            host: host.to_string(),
            sent_total: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Requests sent since process start.
    pub fn sent_total(&self) -> u64 {
        self.sent_total.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Waits until a request may be sent, then reserves the slot.
    pub async fn acquire(&self) {
        loop {
            let wait = {
                let mut w = self.inner.lock().await;
                let now = Instant::now();

                if let Some(until) = w.cooldown_until {
                    if until > now {
                        Some(until - now)
                    } else {
                        w.cooldown_until = None;
                        None
                    }
                } else {
                    None
                }
                .or_else(|| {
                    let sustained = Duration::from_millis(config::RL_SUSTAINED_WINDOW_MS);
                    let burst = Duration::from_millis(config::RL_BURST_WINDOW_MS);
                    while let Some(&front) = w.sent.front() {
                        if now.duration_since(front) >= sustained {
                            w.sent.pop_front();
                        } else {
                            break;
                        }
                    }
                    if w.sent.len() >= config::RL_SUSTAINED as usize {
                        // Oldest entry leaving the 2-min window frees a slot.
                        return Some(sustained - now.duration_since(*w.sent.front().unwrap()));
                    }
                    let in_burst =
                        w.sent.iter().rev().take_while(|t| now.duration_since(**t) < burst).count();
                    if in_burst >= config::RL_BURST as usize {
                        let nth_newest = w.sent[w.sent.len() - config::RL_BURST as usize];
                        return Some(burst - now.duration_since(nth_newest));
                    }
                    w.sent.push_back(now);
                    None
                })
            };
            match wait {
                None => {
                    self.sent_total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return;
                }
                Some(d) => tokio::time::sleep(d + Duration::from_millis(5)).await,
            }
        }
    }

    /// Called on 429: block the whole host for `secs`.
    pub async fn cooldown(&self, secs: u64) {
        let mut w = self.inner.lock().await;
        let until = Instant::now() + Duration::from_secs(secs);
        if w.cooldown_until.is_none_or(|u| u < until) {
            w.cooldown_until = Some(until);
        }
        tracing::warn!(host = %self.host, secs, "rate limit cooldown");
    }
}

/// Registry handing out one limiter per host, shared across regions.
#[derive(Default)]
pub struct LimiterRegistry {
    limiters: std::sync::Mutex<HashMap<String, Arc<RateLimiter>>>,
}

impl LimiterRegistry {
    pub fn get(&self, host: &str) -> Arc<RateLimiter> {
        let mut map = self.limiters.lock().unwrap();
        map.entry(host.to_string())
            .or_insert_with(|| Arc::new(RateLimiter::new(host)))
            .clone()
    }

    /// (host, requests sent since start), sorted by host.
    pub fn sent_totals(&self) -> Vec<(String, u64)> {
        let map = self.limiters.lock().unwrap();
        let mut v: Vec<_> =
            map.iter().map(|(h, l)| (h.clone(), l.sent_total())).collect();
        v.sort();
        v
    }
}
