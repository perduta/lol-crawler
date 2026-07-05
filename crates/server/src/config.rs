//! Region table and crawler tuning knobs.
//!
//! All 15 platforms are declared; flip entries in [`ENABLED_REGIONS`] to
//! enable/disable crawling per region. Rate limiters are shared per routing
//! host, so enabling e.g. EUW1 + EUN1 correctly splits the `europe` budget.

/// A Riot platform (shard) and its routing hosts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Region {
    /// Platform id, also the match-id prefix, e.g. "EUW1".
    pub platform: &'static str,
    /// Host for league-v4 / summoner-v4 (`{platform}.api.riotgames.com`), lowercase.
    pub platform_host: &'static str,
    /// Regional routing for match-v5, e.g. "europe".
    pub regional_host: &'static str,
}

pub const ALL_REGIONS: &[Region] = &[
    Region { platform: "BR1",  platform_host: "br1",  regional_host: "americas" },
    Region { platform: "EUN1", platform_host: "eun1", regional_host: "europe" },
    Region { platform: "EUW1", platform_host: "euw1", regional_host: "europe" },
    Region { platform: "JP1",  platform_host: "jp1",  regional_host: "asia" },
    Region { platform: "KR",   platform_host: "kr",   regional_host: "asia" },
    Region { platform: "LA1",  platform_host: "la1",  regional_host: "americas" },
    Region { platform: "LA2",  platform_host: "la2",  regional_host: "americas" },
    Region { platform: "ME1",  platform_host: "me1",  regional_host: "europe" },
    Region { platform: "NA1",  platform_host: "na1",  regional_host: "americas" },
    Region { platform: "OC1",  platform_host: "oc1",  regional_host: "sea" },
    Region { platform: "RU",   platform_host: "ru",   regional_host: "europe" },
    Region { platform: "SG2",  platform_host: "sg2",  regional_host: "sea" },
    Region { platform: "TR1",  platform_host: "tr1",  regional_host: "europe" },
    Region { platform: "TW2",  platform_host: "tw2",  regional_host: "sea" },
    Region { platform: "VN2",  platform_host: "vn2",  regional_host: "sea" },
];

/// Platforms to actually crawl. Add entries to scale out.
/// Rate budgets are per ROUTING HOST, so one platform per host maximizes
/// total throughput; a second platform on the same host (e.g. EUN1 next to
/// EUW1) only splits that host's budget.
pub const ENABLED_REGIONS: &[&str] = &["EUW1", "NA1", "KR", "VN2"];

/// Matches fetched concurrently per player visit. This bounds how deep the
/// broker's job queue gets per region, so it must cover the whole fleet:
/// with N nodes each holding up to `broker::TARGET_INFLIGHT_PER_HOST` jobs
/// per host, keep this >= N * that target or nodes idle. Node-side rate
/// limiters still enforce every key's real budget.
pub const MATCH_FETCH_CONCURRENCY: usize = 32;

pub fn enabled_regions() -> Vec<Region> {
    ALL_REGIONS
        .iter()
        .copied()
        .filter(|r| ENABLED_REGIONS.contains(&r.platform))
        .collect()
}

// ---- Node fleet ----
/// A job handed to a node must be answered within this window or it is
/// re-issued to another node (hard requirement: 2.5 min max).
pub const LEASE_MS: u64 = 150_000;

/// Fraction of dispatched jobs (percent) duplicated to a second node to
/// cross-check results, catching nodes that return wrong data. Override
/// with env `CRAWLER_AUDIT_DUP_PERCENT`.
pub const AUDIT_DUP_PERCENT_DEFAULT: f64 = 1.0;

pub fn audit_dup_percent() -> f64 {
    std::env::var("CRAWLER_AUDIT_DUP_PERCENT")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|v| (0.0..=100.0).contains(v))
        .unwrap_or(AUDIT_DUP_PERCENT_DEFAULT)
}

/// HTTP bind address for the node API; override with env `CRAWLER_BIND`.
pub fn bind_addr() -> String {
    std::env::var("CRAWLER_BIND").unwrap_or_else(|_| "0.0.0.0:8420".to_string())
}

// ---- Queues ----
/// Solo queue only for the MVP; matchlist requests filter on this.
pub const QUEUE_ID: u32 = 420;
pub const RANKED_QUEUE_TYPE: &str = "RANKED_SOLO_5x5";

/// Also fetch the match timeline (a second regional request per match).
/// The pre-game winner model doesn't use timeline data, and skipping it
/// roughly doubles matches/day on the same budget.
pub const FETCH_TIMELINES: bool = false;

/// Matches shorter than this are remakes: archived in the segment log but
/// they earn no history credit, no adoption credit, and never count as
/// training samples.
pub const REMAKE_MAX_DURATION_S: u32 = 300;

// ---- Apex cohort strategy ----
// Goal: maximize full-history training samples (all 10 participants with
// their 20 preceding games stored). We crawl a *closed pool*: seed from the
// apex leagues (smallest, most self-contained crowd), fetch every game the
// cohort plays, and grow the cohort along observed matchmaking edges.

/// Apex league endpoints seeded every cycle, in priority order.
pub const APEX_LEAGUES: &[(&str, &str)] = &[
    ("challengerleagues", "CHALLENGER"),
    ("grandmasterleagues", "GRANDMASTER"),
    ("masterleagues", "MASTER"),
];

/// Re-fetch apex leagues this often (also snapshots everyone's LP).
pub const SEED_INTERVAL_SECS: u64 = 6 * 3600;

/// A non-cohort player appearing in this many stored matches gets adopted
/// (leak-driven expansion: patches closure holes where they actually occur).
pub const ADOPTION_THRESHOLD: u32 = 2;

/// Ladder bands for fallback expansion when the pool is fully covered and
/// budget is idle, walked in order, one page at a time.
pub const EXPANSION_BANDS: &[(&str, &str)] = &[
    ("DIAMOND", "I"), ("DIAMOND", "II"), ("DIAMOND", "III"), ("DIAMOND", "IV"),
    ("EMERALD", "I"), ("EMERALD", "II"), ("EMERALD", "III"), ("EMERALD", "IV"),
];

/// Budget brake: stop widening the cohort while the frontier has this many
/// tasks overdue by more than the grace period (we can't keep up as is);
/// resume below the off threshold (hysteresis).
pub const BRAKE_OVERDUE_GRACE_MS: u64 = 2 * 3600 * 1000;
pub const BRAKE_ON_COUNT: u64 = 500;
pub const BRAKE_OFF_COUNT: u64 = 100;

/// History depth a participant needs (stored earlier games) for a stored
/// match to count as a full-history training sample.
pub const HISTORY_REQUIRED: u8 = 20;

// ---- Frontier scheduling (activity-based revisit heuristic) ----
pub const MIN_REVISIT_DAYS: f64 = 7.0;
pub const MAX_REVISIT_DAYS: f64 = 60.0;
pub const REVISIT_AFTER_MATCHES: f64 = 4.0;

// ---- Storage ----
pub const DATA_DIR: &str = "data";
/// Flush segment block + commit derived state at least this often.
pub const FLUSH_INTERVAL_SECS: u64 = 60;
/// ...or when the uncompressed block buffer exceeds this size.
pub const BLOCK_TARGET_BYTES: usize = 4 * 1024 * 1024;
pub const ZSTD_LEVEL: i32 = 7;
/// Keep raw API JSON (zstd) for this permille of matches, for regression tests.
pub const RAW_SAMPLE_PERMILLE: u64 = 10; // 1%

/// Ignore matches older than this when walking a player's matchlist.
/// (Deep backfill visits ignore this and walk the full history.)
pub const MAX_MATCH_AGE_DAYS: i64 = 130;

/// Hard cap on matchlist paging depth (ids per player per visit), a safety
/// bound for deep backfill walks against API pathologies.
pub const MATCHLIST_MAX_DEPTH: u32 = 10_000;
