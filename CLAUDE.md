# lol-crawler

Distributed Riot API crawler: one server owns all crawl logic and storage;
nodes (CLI or the Crawl Crew Tauri app) execute Riot requests with their
operators' own API keys. See README.md for the overview and `docs/` for
design details (crawl strategy, storage format, rate limiting).

## Workspace

- `crates/server` — crawl strategy, storage (redb + zstd segment logs), node API on :8420
- `crates/node` — node core library + CLI; all rate limiting lives here
- `crates/desktop` — Crawl Crew Tauri GUI over the node library (needs webkit2gtk-4.1/gtk3 on Linux)
- `crates/proto` — JSON wire protocol; additive changes only, bump version header on breaks

## Commands

- Build/test: `cargo build --workspace`, `cargo test --workspace` (desktop crate needs GTK dev libs; exclude it if they're missing)
- Run server: `cargo run --release -p crawler-server` (data in `./data`, override `CRAWLER_DATA_DIR`)

## Invariants to preserve

- `crates/server/matchrecord.proto` is hand-mirrored by `crates/server/src/record.rs`; `STAT_FIELDS_V1` is append-only — bump `SCHEMA_VERSION` when adding fields.
- Durability ordering in storage: redb checkpoint → fsync segment blocks → commit seen-bitmaps. Don't reorder.
- Nodes must stay strategy-agnostic: crawl logic changes go in the server only.
- A development server instance may be running locally on port 8420 — never kill crawler processes by name.
