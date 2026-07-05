//! Per-node contribution counters for the desktop leaderboard: a minute
//! ring for the last hour, hour buckets for a week, and all-time totals.
//!
//! Counters accumulate in RAM and the dirty hour buckets flush to redb
//! about once a minute (fast-path txn — durable at the next checkpoint).
//! All-time totals are the sum of every hour bucket ever written, so
//! nothing is pruned on disk; a restart only loses minute-level detail
//! for the current hour, which the 60 min tab shrugs off.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

pub const MINUTE_MS: u64 = 60_000;
pub const HOUR_MS: u64 = 3_600_000;
/// Hour buckets kept in RAM: the 7 d window plus the partial current hour.
const RETAIN_HOURS: u64 = 169;

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct Bucket {
    pub requests: u64,
    pub matches: u64,
}

impl Bucket {
    fn add(&mut self, o: Bucket) {
        self.requests += o.requests;
        self.matches += o.matches;
    }
}

struct NodeStats {
    total: Bucket,
    /// Last 60 minutes; slot = absolute minute index % 60.
    minutes: [Bucket; 60],
    /// Absolute minute index the ring head sits at.
    head_minute: u64,
    /// hour_start_ms -> counts, last [`RETAIN_HOURS`] hours only.
    hours: BTreeMap<u64, Bucket>,
    /// Hour keys touched since the last flush.
    dirty: BTreeSet<u64>,
}

impl Default for NodeStats {
    fn default() -> Self {
        Self {
            total: Bucket::default(),
            minutes: [Bucket::default(); 60],
            head_minute: 0,
            hours: BTreeMap::new(),
            dirty: BTreeSet::new(),
        }
    }
}

impl NodeStats {
    /// Moves the ring head to `minute`, zeroing every slot skipped over.
    fn advance(&mut self, minute: u64) {
        if minute <= self.head_minute {
            return;
        }
        let steps = (minute - self.head_minute).min(60);
        for i in 1..=steps {
            self.minutes[((self.head_minute + i) % 60) as usize] = Bucket::default();
        }
        self.head_minute = minute;
    }

    fn window_m60(&self) -> Bucket {
        let mut b = Bucket::default();
        for slot in &self.minutes {
            b.add(*slot);
        }
        b
    }

    /// Sum of the current partial hour bucket plus the `n - 1` before it.
    fn window_hours(&self, now_ms: u64, n: u64) -> Bucket {
        let cutoff = (now_ms / HOUR_MS).saturating_sub(n - 1) * HOUR_MS;
        let mut b = Bucket::default();
        for (_, v) in self.hours.range(cutoff..) {
            b.add(*v);
        }
        b
    }
}

/// Windowed counters for one node, as the API serializes them.
pub struct NodeWindows {
    pub node_id: u32,
    pub m60: Bucket,
    pub h24: Bucket,
    pub d7: Bucket,
    pub all: Bucket,
}

#[derive(Default)]
pub struct Stats {
    inner: Mutex<HashMap<u32, NodeStats>>,
}

impl Stats {
    /// Rebuilds from every `(node, hour, bucket)` row on disk: totals from
    /// all of history, in-RAM hour buckets from the recent slice only.
    pub fn load(rows: Vec<(u32, u64, Bucket)>, now_ms: u64) -> Self {
        let cutoff = (now_ms / HOUR_MS).saturating_sub(RETAIN_HOURS) * HOUR_MS;
        let mut map: HashMap<u32, NodeStats> = HashMap::new();
        for (node, hour, bucket) in rows {
            let ns = map.entry(node).or_default();
            ns.total.add(bucket);
            if hour >= cutoff {
                ns.hours.insert(hour, bucket);
            }
            ns.head_minute = now_ms / MINUTE_MS;
        }
        Self { inner: Mutex::new(map) }
    }

    /// One completed Riot request by `node_id`; `is_match` marks a stored
    /// full match body (the feel-good counter).
    pub fn record(&self, node_id: u32, is_match: bool, now_ms: u64) {
        let b = Bucket { requests: 1, matches: is_match as u64 };
        let mut inner = self.inner.lock().unwrap();
        let ns = inner.entry(node_id).or_default();
        ns.advance(now_ms / MINUTE_MS);
        ns.minutes[(ns.head_minute % 60) as usize].add(b);
        let hour = now_ms / HOUR_MS * HOUR_MS;
        ns.hours.entry(hour).or_default().add(b);
        ns.dirty.insert(hour);
        ns.total.add(b);
        if ns.hours.len() as u64 > RETAIN_HOURS {
            let cutoff = hour.saturating_sub(RETAIN_HOURS * HOUR_MS);
            ns.hours = ns.hours.split_off(&cutoff);
        }
    }

    pub fn snapshot(&self, now_ms: u64) -> Vec<NodeWindows> {
        let mut inner = self.inner.lock().unwrap();
        inner
            .iter_mut()
            .map(|(id, ns)| {
                ns.advance(now_ms / MINUTE_MS);
                NodeWindows {
                    node_id: *id,
                    m60: ns.window_m60(),
                    h24: ns.window_hours(now_ms, 24),
                    d7: ns.window_hours(now_ms, 168),
                    all: ns.total,
                }
            })
            .collect()
    }

    /// Drains dirty hour buckets as absolute values ready to upsert.
    pub fn take_dirty(&self) -> Vec<(u32, u64, Bucket)> {
        let mut inner = self.inner.lock().unwrap();
        let mut out = Vec::new();
        for (id, ns) in inner.iter_mut() {
            for hour in std::mem::take(&mut ns.dirty) {
                if let Some(b) = ns.hours.get(&hour) {
                    out.push((*id, hour, *b));
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows_roll_and_totals_persist() {
        let stats = Stats::default();
        let t0 = 1_000_000 * HOUR_MS; // some hour boundary
        stats.record(1, true, t0);
        stats.record(1, false, t0 + MINUTE_MS);

        let snap = stats.snapshot(t0 + 2 * MINUTE_MS);
        let n = &snap[0];
        assert_eq!(n.m60.requests, 2);
        assert_eq!(n.m60.matches, 1);
        assert_eq!(n.h24.requests, 2);
        assert_eq!(n.all.requests, 2);

        // 61 minutes later the minute ring is empty, hours still count.
        let snap = stats.snapshot(t0 + 62 * MINUTE_MS);
        let n = &snap[0];
        assert_eq!(n.m60.requests, 0);
        assert_eq!(n.h24.requests, 2);

        // 25 hours later the 24 h window is empty, 7 d and all-time not.
        let snap = stats.snapshot(t0 + 25 * HOUR_MS);
        let n = &snap[0];
        assert_eq!(n.h24.requests, 0);
        assert_eq!(n.d7.requests, 2);
        assert_eq!(n.all.requests, 2);
    }

    #[test]
    fn dirty_flush_roundtrips_through_load() {
        let stats = Stats::default();
        let t0 = 500_000 * HOUR_MS;
        stats.record(7, true, t0);
        stats.record(7, true, t0 + HOUR_MS); // second hour bucket
        let dirty = stats.take_dirty();
        assert_eq!(dirty.len(), 2);
        assert!(stats.take_dirty().is_empty(), "drained");

        // Reload far in the future: totals survive, windows are empty.
        let reloaded = Stats::load(dirty, t0 + 400 * HOUR_MS);
        let snap = reloaded.snapshot(t0 + 400 * HOUR_MS);
        assert_eq!(snap[0].all.requests, 2);
        assert_eq!(snap[0].all.matches, 2);
        assert_eq!(snap[0].d7.requests, 0);
    }
}
