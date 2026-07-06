### Disclaimer
Entire project is vibe coded and **SHOULD NOT BE CONSIDERED PRODUCTION READY**, but I've decided to push it anyway, maybe someone will find any use to it.

# lol-crawler

A distributed crawler for the Riot Games API that collects League of Legends
ranked solo queue (queue 420) matches as training data for win prediction.
The crawl runs an **apex-cohort strategy**, optimized to maximize
*full-history training samples*: stored matches where all 10 participants
also have their 20 preceding games stored — the shape a pre-game
win-prediction model trains on. See
[docs/crawl-strategy.md](docs/crawl-strategy.md) for the how and why.

One server owns the crawl; friends run lightweight nodes (a CLI, or the
**Crawl Crew** desktop app) that execute Riot API requests with their own
keys and stream the results back. Every node multiplies the crawl budget.

## Architecture: server + node fleet

The crawler is split into two programs (workspace crates):

- **`crawler-server`** (`crates/server`) — runs on one host, owns *all*
  crawl logic and *all* data, and makes **zero** Riot API requests. Every
  Riot fetch becomes an opaque job `{host, method, path}` handed to a node.
- **`crawler-node`** (`crates/node`) — the node core **library** plus a
  small CLI for power users. Each node enrolls once with an invite code,
  then pulls jobs, executes them with *its operator's own Riot API key* at
  full rate-limit speed (two-layer sliding-window limiters with live
  header adoption — see [docs/rate-limiting.md](docs/rate-limiting.md)),
  and uploads the raw bodies. Nodes know nothing about the crawl strategy,
  so the server can evolve freely without breaking deployed nodes.
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

To catch a node that starts lying or serving corrupted data, a
configurable percentage of dispatched jobs (`CRAWLER_AUDIT_DUP_PERCENT`,
default **1%**) is cloned to a *different* node and the two bodies are
compared: immutable endpoints (match, timeline) must match exactly
("AUDIT MISMATCH" warning + per-node counters in the 60 s report), while
volatile endpoints (matchlists, leagues) only count as soft mismatches.
Both nodes of a failed pair are flagged — the liar is the one accumulating
fails across many partners.

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

Working on the frontend? `cargo run -p crawler-desktop -- --mock` (or env
`CRAWL_CREW_MOCK=1`) fakes both the server and Riot: the enrollment form
accepts anything, fake jobs stream through the visualization, the
leaderboard is fabricated, and the on-disk node config is never touched.
The script also plays a disconnect blip every ~90 s and a one-time key
expiry at ~150 s so every UI state is reachable.

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
budget) or to heal older coverage holes.

## Enabling more regions

Edit `ENABLED_REGIONS` in `crates/server/src/config.rs` — all 15 platforms
are already declared in `ALL_REGIONS`. Each node's rate limiters are keyed
by routing host, so platforms sharing a host (EUW1+EUN1 → `europe`) split
that host's budget, while **every additional node multiplies the budget of
every host** — with several nodes it pays to enable more regions.

## Expected throughput

On a dev key, expect **~40–50 matches/hr stored per node** without
timelines (half that with `FETCH_TIMELINES` on). Details of the limiter
design and pacing are in [docs/rate-limiting.md](docs/rate-limiting.md).

## Documentation

- [docs/crawl-strategy.md](docs/crawl-strategy.md) — the apex-cohort
  strategy: sample economics, cohort seeding, leak-driven adoption, the
  budget brake, and recorded assumptions & known limitations.
- [docs/storage.md](docs/storage.md) — on-disk layout, segment/record
  format, and the durability model.
- [docs/rate-limiting.md](docs/rate-limiting.md) — the node-side two-layer
  limiters, header adoption, 429 handling, and paced sending.

## Not in the MVP (by design)

Mastery snapshots, the Parquet materializer/training-set builder, raw
timeline retention beyond the 1% sample, multi-process sharding.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in this work by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.

## Legal

lol-crawler isn't endorsed by Riot Games and doesn't reflect the views or
opinions of Riot Games or anyone officially involved in producing or
managing Riot Games properties. Riot Games, and all associated properties
are trademarks or registered trademarks of Riot Games, Inc.

Each node operator uses their **own** Riot API key and is responsible for
complying with the [Riot Developer Terms of
Service](https://developer.riotgames.com/policies/general).
