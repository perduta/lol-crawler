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

/// Matches fetched concurrently per player visit (each = 2 regional-host
/// requests in flight). Keeps the 20 req/1s burst window busy instead of
/// serializing on network latency; the limiter still enforces the budget.
pub const MATCH_FETCH_CONCURRENCY: usize = 8;

pub fn enabled_regions() -> Vec<Region> {
    ALL_REGIONS
        .iter()
        .copied()
        .filter(|r| ENABLED_REGIONS.contains(&r.platform))
        .collect()
}

// ---- Rate limits (per routing host, dev key) ----
pub const RL_BURST: u32 = 20; // requests
pub const RL_BURST_WINDOW_MS: u64 = 1_000;
pub const RL_SUSTAINED: u32 = 100; // requests
pub const RL_SUSTAINED_WINDOW_MS: u64 = 120_000;

// ---- Queues ----
/// Solo queue only for the MVP; matchlist requests filter on this.
pub const QUEUE_ID: u32 = 420;
pub const RANKED_QUEUE_TYPE: &str = "RANKED_SOLO_5x5";

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

// ---- Frontier scheduling (mirrors the old ComputeExpiracyDays heuristic) ----
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
pub const MAX_MATCH_AGE_DAYS: i64 = 130;
