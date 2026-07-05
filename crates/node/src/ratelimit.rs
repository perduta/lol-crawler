//! Sliding-window rate limiters, mirroring Riot's two enforcement layers:
//!
//!  - one *app* limiter per routing host (all endpoints share its budget);
//!  - one *method* limiter per (host, endpoint method).
//!
//! Both start from the conservative dev-key defaults in `config` and adopt
//! the server-declared windows from the `X-App-Rate-Limit` /
//! `X-Method-Rate-Limit` response headers as soon as one is seen, so a
//! production key needs no config change. 429 cooldowns are scoped by
//! `X-Rate-Limit-Type` to the offending layer.
//!
//! App limiters additionally *pace*: instead of bursting a whole window's
//! budget at once and then starving (100 sends in ~5 s, then ~115 s of
//! silence on a dev key), sends are spread at the sustained rate with
//! randomized gaps. The average gap equals `window / limit` for the
//! tightest window, so utilization is still ~100% of budget — the stream
//! is just continuous (and the desktop visualization flows instead of
//! pulsing). A side bonus: no 100-request burst right after a restart, so
//! the leftover spend in Riot's server-side window no longer triggers a
//! 429 storm. The sliding windows stay on as the hard cap beneath the
//! pacer; method limiters are left bursty on purpose (their budgets are
//! rarely binding and pacing them would only delay league seeds).

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

/// Initial app-limit windows: the dev-key defaults (20 req/1s +
/// 100 req/2min). The limiter adopts the live values from
/// `X-App-Rate-Limit` / `X-Method-Rate-Limit` response headers after the
/// first response on each host, so a production key needs no edit here.
const RL_DEFAULT_WINDOWS: &[(u32, u64)] = &[(20, 1_000), (100, 120_000)];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WindowSpec {
    pub limit: u32,
    pub window_ms: u64,
}

fn default_specs() -> Vec<WindowSpec> {
    RL_DEFAULT_WINDOWS
        .iter()
        .map(|&(limit, window_ms)| WindowSpec { limit, window_ms })
        .collect()
}

/// Parses a Riot rate-limit header, e.g. "20:1,100:120" = 20/1s + 100/120s.
pub fn parse_limit_header(s: &str) -> Vec<WindowSpec> {
    s.split(',')
        .filter_map(|part| {
            let (limit, secs) = part.trim().split_once(':')?;
            Some(WindowSpec {
                limit: limit.trim().parse().ok()?,
                window_ms: secs.trim().parse::<u64>().ok()?.checked_mul(1000)?,
            })
        })
        .collect()
}

struct Windows {
    specs: Vec<WindowSpec>,
    /// Send times of recent requests, pruned past the largest window.
    sent: VecDeque<Instant>,
    /// Do not send anything before this time (set on 429).
    cooldown_until: Option<Instant>,
    /// Pacing: earliest time the next send may go out (paced limiters).
    next_slot: Option<Instant>,
}

pub struct RateLimiter {
    inner: Mutex<Windows>,
    key: String,
    /// Spread sends at the sustained rate with random gaps instead of
    /// bursting the window budget (used for app limiters).
    pace: bool,
    sent_total: std::sync::atomic::AtomicU64,
}

impl RateLimiter {
    pub fn new(key: &str, specs: Vec<WindowSpec>, pace: bool) -> Self {
        Self {
            inner: Mutex::new(Windows {
                specs,
                sent: VecDeque::new(),
                cooldown_until: None,
                next_slot: None,
            }),
            key: key.to_string(),
            pace,
            sent_total: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Requests sent since process start.
    pub fn sent_total(&self) -> u64 {
        self.sent_total.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Replaces the limit windows with server-declared ones (no-op if equal).
    pub async fn update_specs(&self, specs: Vec<WindowSpec>) {
        if specs.is_empty() {
            return;
        }
        let mut w = self.inner.lock().await;
        if w.specs != specs {
            tracing::info!(key = %self.key, ?specs, "rate limit windows updated from headers");
            w.specs = specs;
        }
    }

    /// Waits until a request may be sent, then reserves the slot.
    pub async fn acquire(&self) {
        loop {
            let wait = {
                let mut w = self.inner.lock().await;
                let now = Instant::now();
                let mut wait: Option<Duration> = None;

                if let Some(until) = w.cooldown_until {
                    if until > now {
                        wait = Some(until - now);
                    } else {
                        w.cooldown_until = None;
                    }
                }

                // Pacing gate: contenders woken together re-race; the winner
                // advances `next_slot`, the rest sleep again.
                if wait.is_none() && self.pace {
                    if let Some(ns) = w.next_slot {
                        if ns > now {
                            wait = Some(ns - now);
                        }
                    }
                }

                if wait.is_none() {
                    let max_window = Duration::from_millis(
                        w.specs.iter().map(|s| s.window_ms).max().unwrap_or(0),
                    );
                    while let Some(&front) = w.sent.front() {
                        if now.duration_since(front) >= max_window {
                            w.sent.pop_front();
                        } else {
                            break;
                        }
                    }
                    for spec in &w.specs {
                        let win = Duration::from_millis(spec.window_ms);
                        let in_window = w
                            .sent
                            .iter()
                            .rev()
                            .take_while(|t| now.duration_since(**t) < win)
                            .count();
                        if in_window >= spec.limit as usize {
                            // The limit-th newest send leaving the window frees a slot.
                            let nth_newest = w.sent[w.sent.len() - spec.limit as usize];
                            let d = win.saturating_sub(now.duration_since(nth_newest));
                            wait = Some(wait.map_or(d, |cur| cur.max(d)));
                        }
                    }
                    if wait.is_none() {
                        w.sent.push_back(now);
                        if self.pace {
                            // Mean gap = window/limit of the tightest window
                            // (jitter is uniform on 0.35..1.65, mean 1.0), so
                            // the long-run rate is exactly the budget — an
                            // idle spell just resolves to "send immediately",
                            // it never accumulates a burst allowance.
                            let base_ms = w
                                .specs
                                .iter()
                                .map(|s| s.window_ms as f64 / s.limit.max(1) as f64)
                                .fold(0.0, f64::max);
                            let jitter = 0.35 + rand::random::<f64>() * 1.3;
                            w.next_slot =
                                Some(now + Duration::from_millis((base_ms * jitter) as u64));
                        }
                    }
                }
                wait
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

    /// Called on 429: block this limiter for `secs`.
    pub async fn cooldown(&self, secs: u64) {
        let mut w = self.inner.lock().await;
        let until = Instant::now() + Duration::from_secs(secs);
        if w.cooldown_until.is_none_or(|u| u < until) {
            w.cooldown_until = Some(until);
        }
        tracing::warn!(key = %self.key, secs, "rate limit cooldown");
    }
}

/// Registry handing out app limiters (per host) and method limiters
/// (per host+method), shared across regions.
///
/// Method limiters start at the app defaults — never binding before the app
/// limiter is — and tighten to the real per-method windows once the first
/// `X-Method-Rate-Limit` header for that method arrives.
#[derive(Default)]
pub struct LimiterRegistry {
    app: std::sync::Mutex<HashMap<String, Arc<RateLimiter>>>,
    method: std::sync::Mutex<HashMap<(String, String), Arc<RateLimiter>>>,
}

impl LimiterRegistry {
    pub fn app(&self, host: &str) -> Arc<RateLimiter> {
        let mut map = self.app.lock().unwrap();
        map.entry(host.to_string())
            .or_insert_with(|| Arc::new(RateLimiter::new(host, default_specs(), true)))
            .clone()
    }

    pub fn method(&self, host: &str, method: &str) -> Arc<RateLimiter> {
        let mut map = self.method.lock().unwrap();
        map.entry((host.to_string(), method.to_string()))
            .or_insert_with(|| {
                Arc::new(RateLimiter::new(&format!("{host}/{method}"), default_specs(), false))
            })
            .clone()
    }

    /// (host, requests sent since start) from the app limiters, sorted by host.
    pub fn sent_totals(&self) -> Vec<(String, u64)> {
        let map = self.app.lock().unwrap();
        let mut v: Vec<_> = map.iter().map(|(h, l)| (h.clone(), l.sent_total())).collect();
        v.sort();
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_limit_headers() {
        assert_eq!(
            parse_limit_header("20:1,100:120"),
            vec![
                WindowSpec { limit: 20, window_ms: 1_000 },
                WindowSpec { limit: 100, window_ms: 120_000 },
            ]
        );
        assert_eq!(parse_limit_header("2000:10"), vec![WindowSpec { limit: 2000, window_ms: 10_000 }]);
        assert!(parse_limit_header("garbage").is_empty());
    }
}
