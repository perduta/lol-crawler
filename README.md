# lol-crawler

Rust rewrite of the WRBoost Riot API crawler, implementing the append-only-log
design from `../crawler_idea.md`. Solo queue (420),
**apex-cohort strategy**: the crawl is optimized to maximize *full-history
training samples* — stored matches where all 10 participants also have their
20 preceding games stored. (Emerald-first crawling was the original idea and
was replaced by this; the data exists only to make training samples, and the
apex ladder yields far more of them per request.)

## Architecture: server + node fleet

The crawler is split into two programs (workspace crates):

- **`crawler-server`** (`crates/server`) — runs on one host, owns *all*
  crawl logic and *all* data, and makes **zero** Riot API requests. Every
  Riot fetch becomes an opaque job `{host, method, path}` handed to a node.
- **`crawler-node`** (`crates/node`) — the node core **library** plus a
  small CLI for power users. Each node enrolls once with an invite code,
  then pulls jobs, executes them with *its operator's own Riot API key* at
  full rate-limit speed (the same two-layer sliding-window limiters +
  header adoption the original crawler used), and uploads the raw bodies.
  Nodes know nothing about the crawl strategy, so the server can evolve
  freely without breaking deployed nodes.
- **`crawler-desktop`** (`crates/desktop`) — **Crawl Crew**, the friendly
  Tauri GUI over the same node library (identical crawl loop). This is
  what you actually send to friends; see below.
- **`crawler-proto`** (`crates/proto`) — the tiny JSON wire protocol
  (additive-changes-only; version header, 426 on real breaks).

Scheduling is **pull-based**: a node reports how many jobs it holds per
routing host and the server tops each host up to a small target (8), so
node-side limiters alone set the pace — a production key applies with no
change anywhere. Jobs are leased for **2.5 minutes**; unanswered leases
re-queue for other nodes, and duplicate/stale results are ignored (safe:
`store_match` re-stores are guarded). The job queue is RAM-only, derived
from the frontier — a server restart just re-derives work; the seen-bitmap
prevents refetching.

**Duplicate audits**: a configurable percentage of dispatched jobs
(`CRAWLER_AUDIT_DUP_PERCENT`, default **1%**) is cloned to a *different*
node and the two bodies are compared, to catch a node that starts lying or
serving corrupted data. Immutable endpoints (match, timeline) must match
exactly ("AUDIT MISMATCH" warning + per-node counters shown in the 60 s
report); volatile endpoints (matchlists, leagues) only count as soft
mismatches since ground truth moves between the two fetches. Both nodes of
a failed pair are flagged — the liar is the one accumulating fails across
many partners.

### Run the server

```sh
cargo run --release -p crawler-server
```

Data lands in `./data` (override with `CRAWLER_DATA_DIR`). The node API
binds `0.0.0.0:8420` (override with `CRAWLER_BIND`; put TLS in front with
a reverse proxy if you expose it publicly). Ctrl-C drains and flushes.
`RUST_LOG=debug` for verbose logging.

```sh
cargo run --release -p crawler-server -- invite alice
```

mints a one-time invite code (stored in `data/invites.txt` until used).
Send the code + your server URL to a friend.

### Run a node (what you send to friends)

```sh
cargo build --release -p crawler-node   # ship target/release/crawler-node
crawler-node --server http://your-host:8420
```

First run asks for the invite code, a node name, and their Riot API key
(from <https://developer.riotgames.com>), then saves
`~/.config/crawler-node/config.json` (token + key, chmod 600). After that,
plain `crawler-node` resumes. Dev keys expire daily: on a 401/403 the node
pauses and `crawler-node set-key` (or editing the config) resumes it
within ~15 s. The key never leaves their machine — the server only ever
sees fetched bodies.

### Crawl Crew — the desktop node

```sh
cargo build --release -p crawler-desktop   # needs webkit2gtk-4.1/gtk3 dev libs on Linux
```

Same node core as the CLI, wrapped in a warm little app your friends will
actually enjoy leaving open:

- **Left panel** — live visualization of jobs flowing: orbs drop in as the
  server hands out work (colored per routing host), bob while the Riot
  request runs, then swoosh into the "delivered" vault; rotating thank-you
  messages, lifetime counters, and milestone confetti (100 / 1k / 10k /
  ... matches).
- **Right panel** — the crew leaderboard with **60 min / 24 h / 7 d /
  all-time** tabs, powered by the server's `/v1/stats` endpoint (per-node
  minute ring + hourly buckets persisted in redb, so nobody's all-time
  glory is lost to a restart).
- **Key expiry QoL** — when Riot rejects the key, a native notification
  fires and an in-app banner takes the new key; crawling resumes in
  seconds. First run shows an enrollment form (server, name, invite code,
  key) instead of CLI prompts.
- **Resource discipline** — closing the window *destroys* the webview
  (frees its RAM; ~15 MB Rust core keeps crawling) and the tray icon
  brings it back. The canvas only animates while visible, the leaderboard
  polls every 45 s, and quitting from the tray drains result uploads.

The stats flow adds two things server-side: completions are bucketed per
node (`stats.rs`, flushed to the `node_stats` redb table ~1/min) and
`POST /v1/stats` returns the windowed leaderboard. Both are additive —
protocol still v1, old CLI nodes unaffected.

Windows/macOS builds must currently be produced on that OS (or CI):
`cargo tauri build` there yields a signed-nothing, single-file installer;
WebView2 is preinstalled on Windows 10/11.

### Backfill

```sh
cargo run --release -p crawler-server -- backfill
```

`backfill` reschedules every queued cohort member to *now* as a **deep
visit**: their full matchlist is walked (paged, no age cutoff — as far back
as the Riot API still has) and every unseen match is fetched, before normal
scheduling resumes per player. Deep visits survive restarts: an interrupted
one keeps its marker and resumes on the next run. Use after enabling
history-hungry changes (e.g. after turning timelines off doubled the
budget) or to heal coverage holes from the pre-paging era.

## Enabling more regions

Edit `ENABLED_REGIONS` in `crates/server/src/config.rs` — all 15 platforms
are already declared in `ALL_REGIONS`. Each node's rate limiters are keyed
by routing host, so platforms sharing a host (EUW1+EUN1 → `europe`) split
that host's budget, while **every additional node multiplies the budget of
every host** — with several nodes it pays to enable more regions.

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
  cohort growth pauses (resumes below 100, hysteresis) — seeding of lower
  apex leagues, ladder expansion, *and* leak-driven adoption. Outsider
  sightings still accrue while braked; anyone past the threshold converts
  on their next sighting after the brake lifts. Going deeper in time beats
  going wider when the budget is saturated.
- **Per player visit**: fetch ranked matchlist (regional host, paged past
  the 100-id window while a full page has no already-seen id, so heavy
  grinders don't silently lose matches) + refresh rank snapshot (platform
  host, separate budget), download every unseen match (+ timeline only if
  `FETCH_TIMELINES` — off by default, since the pre-game model doesn't use
  timelines and skipping them doubles match throughput), reschedule by
  activity (4 matches' worth, clamped to 7–60 days — the old
  ComputeExpiracyDays). Non-cohort players popped from the frontier (legacy
  entries) are dropped without spending requests.
- **Remakes**: matches shorter than `REMAKE_MAX_DURATION_S` are archived in
  the segment log but earn no history credit, no adoption credit, and never
  count as valid samples.
- **Sample-progress tracking**: every stored match keeps a per-participant
  count of stored predecessor games, updated incrementally (including
  retroactively when backfill lands an older game). The 60 s report and
  `inspect` show live valid-sample counts per region.
- **Dedup**: per-platform roaring bitmap of seen game ids. 404s /
  wrong-queue matches are marked seen so they're never re-fetched.

## Assumptions & known limitations (recorded on purpose)

1. **Poll cadence is inherited from the Emerald-era design** (revisit =
   ~4 matches' worth, clamped to **7–60 days**). The old consequence —
   >100 games between visits overflowing the matchlist window — is fixed:
   visits now page past 100 while a full page has no already-seen id.
   Residual caveat: normal-visit paging stops at the first seen id, so it
   catches overflow *since the last visit* but does not heal pre-existing
   holes; run `backfill` for that.
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
7. **Rank snapshots are crawl-time, not game-time** (known, deliberately
   deferred). Snapshots are taken at seeding (apex: whole league every 6 h,
   so ≤6 h stale) and at each visit — but visits are 7–60 days apart, so
   for adopted/ladder players the "as-of-game elo" join can be days-to-weeks
   stale, at the elo band where LP moves fastest. Planned fix, when it
   matters: (a) materializer-side — bracket each game between the two
   nearest snapshots and interpolate LP using the wins+losses delta both
   snapshots carry (retroactively improves all data already collected);
   (b) crawl-side — a low-priority freshness worker on the mostly-idle
   platform-host budget that re-snapshots any stored-match participant
   whose latest snapshot is older than ~2 days. Do (a) first; add (b) only
   if interpolation residuals hurt the model.

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
Torn tails are truncated on restart. Wire schema:
`crates/server/matchrecord.proto` (hand-mirrored by
`crates/server/src/record.rs`; participant stats are a varint array in
`STAT_FIELDS_V1` order — append-only, bump `SCHEMA_VERSION` when adding).

Durability: hot-path redb transactions (per-match state, frontier ops, id
assignment) commit fast (atomic, not fsynced); the periodic commit runs a
durable checkpoint, *then* fsyncs segment blocks, *then* commits
seen-bitmaps. A crash re-fetches a little work — re-stores are guarded by
the progress record so nothing is double-counted — and can never mis-map a
player id or leave a bitmap claiming unflushed bytes. Stranded cohort
members (an adoption whose enqueue never landed) are re-queued by a startup
reconciliation pass.

## Rate limiting

Rate limiting lives entirely **node-side** (`crates/node/src/ratelimit.rs`),
since limits are per API key. Two limiter layers, mirroring Riot's
enforcement: an *app* limiter per routing host and a *method* limiter per
(host, endpoint). Both start from the dev-key defaults (20 req/1 s +
100 req/2 min) and adopt the live windows from `X-App-Rate-Limit` /
`X-Method-Rate-Limit` response headers, so a production key applies with
no config change. 429 cooldowns honor `Retry-After` and are scoped by
`X-Rate-Limit-Type` to the offending layer. App limiters additionally
*pace*: sends are spread at the sustained rate with randomized gaps
(mean gap = tightest `window / limit`, so utilization stays ~100% of
budget) instead of bursting a whole window and starving — the stream is
continuous, restarts no longer trigger a 429 storm, and the Crawl Crew
visualization flows instead of pulsing. Dev-key sustained ceiling is
~0.83 req/s per host *per node*: expect **~40–50 matches/hr stored per
node** without timelines (half that with `FETCH_TIMELINES` on).

## Not in the MVP (by design)

Mastery snapshots, the Parquet materializer/training-set builder, raw
timeline retention beyond the 1% sample, multi-process sharding.
