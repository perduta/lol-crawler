# lol-crawler

Rust rewrite of the WRBoost Riot API crawler, implementing the append-only-log
design from `../crawler_idea.md`. Solo queue (420), dev-key rate limits,
**apex-cohort strategy**: the crawl is optimized to maximize *full-history
training samples* — stored matches where all 10 participants also have their
20 preceding games stored. (Emerald-first crawling was the original idea and
was replaced by this; the data exists only to make training samples, and the
apex ladder yields far more of them per request.)

## Run

```sh
cargo run --release
```

The API key is read from `$API_KEY` or the nearest `api_key.env`
(`API_KEY="RGAPI-..."`) walking up from the working directory. Data lands in
`./data` (override with `CRAWLER_DATA_DIR`). Ctrl-C drains and flushes.
`RUST_LOG=debug` for verbose logging.

## Enabling more regions

Edit `ENABLED_REGIONS` in `src/config.rs` — all 15 platforms are already
declared in `ALL_REGIONS`. Rate limiters are keyed by routing host, so
platforms sharing a host (EUW1+EUN1 → `europe`) automatically share that
host's budget when both are enabled.

## How it works — apex cohort strategy

The economics: a full-history sample naively costs ~200 history matches
(~410 requests), but every fetched match fills up to 10 history slots at
once, so inside a *closed player pool* (players who mostly play each other)
the marginal cost approaches ~2 requests per sample. Matchmaking is closest
to closed at the top of the ladder — so that's where the cohort starts.

- **Cohort**: the set of players whose every ranked game we fetch. A stored
  match is a **valid sample** when all 10 participants have
  `HISTORY_REQUIRED` (20) stored earlier games.
- **Seeding**: every 6 h, fetch the apex leagues (challenger/GM/master — one
  request each returns the entire league), snapshot everyone's LP, enroll
  new arrivals. Lower apex leagues are skipped while the brake is on, so a
  saturated crawler doesn't widen.
- **Leak-driven adoption**: a non-cohort player appearing in
  `ADOPTION_THRESHOLD` (2) stored matches is adopted automatically. This
  patches pool-closure holes exactly where matchmaking creates them
  (lower-elo participants in apex games), using data already paid for.
- **Ladder expansion**: only when the frontier is idle *and* the brake is
  off, walk Diamond I → Emerald IV pages one page per idle tick.
- **Budget brake**: while >500 frontier tasks are overdue by >2 h, all
  cohort growth pauses (resumes below 100, hysteresis). Going deeper in
  time beats going wider when the budget is saturated.
- **Per player visit**: fetch ranked matchlist (regional host) + refresh rank
  snapshot (platform host, separate budget), download every unseen match +
  timeline, reschedule by activity (4 matches' worth, clamped to 7–60 days —
  the old ComputeExpiracyDays). Non-cohort players popped from the frontier
  (legacy entries) are dropped without spending requests.
- **Sample-progress tracking**: every stored match keeps a per-participant
  count of stored predecessor games, updated incrementally (including
  retroactively when backfill lands an older game). The 60 s report and
  `inspect` show live valid-sample counts per region.
- **Dedup**: per-platform roaring bitmap of seen game ids. 404s /
  wrong-queue matches are marked seen so they're never re-fetched.

## Assumptions & known limitations (recorded on purpose)

1. **Poll cadence is inherited from the Emerald-era design** (revisit =
   ~4 matches' worth, clamped to **7–60 days**) and was deliberately kept for
   now. Consequence: an apex player who plays more than ~100 games between
   visits (≈14+/day at the 7-day minimum) overflows the 100-id matchlist
   window → coverage holes → those matches never become valid samples. KR/VN2
   grinders can realistically hit this. Fix when it matters: lower
   `MIN_REVISIT_DAYS` for cohort members (hours-scale) or page matchlists.
2. **"Valid sample" counting is an approximation**: it counts *any* 20 stored
   earlier games per participant, not exactly "the 20 most recent per the
   matchlist". The materializer must do the exact check (and should use the
   old system's "20 of the last 25" flex). The counter is a crawl-progress
   metric, not ground truth.
3. Matches stored before this strategy landed (the Emerald-era ~1.5 K) have
   no progress records; retro-updates skip them silently.
4. The cohort/outsider/valid caches live in RAM (linear in players seen) —
   fine at dev-key scale, revisit alongside the production-key changes.
5. The brake thresholds (500/100 overdue, 2 h grace) are heuristics, not
   tuned values.
6. Corpus elo composition: on big servers the cohort will stay Master+ at
   dev-key rates (expansion rarely unbrakes); reaching Emerald requires
   small servers or a production key. Accepted: samples > elo purity.

## Storage layout (`data/`)

```
state.redb                     players (puuid→u32), rank snapshots, player
                               timelines, frontier, seen-bitmaps, cursors
matches/EUW1/YYYY-MM-DD.seg    zstd blocks of length-prefixed MatchRecord protobufs
matches/EUW1/YYYY-MM-DD.idx    (game_id u64, block_off u64, rec_off u32) LE per record
raw/EUW1/<id>.*.json.zst       1% raw API JSON sample for regression tests
```

Segment block: `magic u32 | crc32(compressed) u32 | compressed_len u32 |
uncompressed_len u32 | zstd bytes`; inside, each record is `len u32 | protobuf`.
Torn tails are truncated on restart. Wire schema: `matchrecord.proto`
(hand-mirrored by `src/record.rs`; participant stats are a varint array in
`STAT_FIELDS_V1` order — append-only, bump `SCHEMA_VERSION` when adding).

Durability ordering: player-id assignments commit before any segment write;
segment blocks fsync before bitmaps/frontier commit. A crash re-fetches a
little work; it can never mis-map a player id.

## Rate limiting

Sliding windows per routing host: 20 req/1 s and 100 req/2 min, plus a
host-wide cooldown honoring `Retry-After` on 429. Sustained ceiling is
~0.83 req/s per host → about 1,200–1,400 matches/day-equivalent per hour...
i.e. ~1,400 matches/hr are impossible: expect **~20–25 matches/hr stored**
with timelines on a dev key (2 regional requests per match + matchlist +
occasional league calls). Upgrade to a production key and only
`src/config.rs` needs new numbers.

## Not in the MVP (by design)

Mastery snapshots, the Parquet materializer/training-set builder, raw
timeline retention beyond the 1% sample, multi-process sharding.
