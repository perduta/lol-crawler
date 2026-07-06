# Storage layout (`data/`)

```
state.redb                     players (puuid→u32), rank snapshots, player
                               timelines, frontier, seen-bitmaps, cursors
matches/EUW1/YYYY-MM-DD.seg    zstd blocks of length-prefixed MatchRecord protobufs
matches/EUW1/YYYY-MM-DD.idx    (game_id u64, block_off u64, rec_off u32) LE per record
raw/EUW1/<id>.*.json.zst       1% raw API JSON sample for regression tests
```

## Segment format

Segment block: `magic u32 | crc32(compressed) u32 | compressed_len u32 |
uncompressed_len u32 | zstd bytes`. Row blocks contain `len u32 | protobuf`
records. Closed days are recompacted into columnar blocks (`1COL` container
magic, `LCOL` payload); readers dispatch per block, so mixed row/columnar
segments are valid. `.idx` stays `(game_id, block_off, rec_off)`; `rec_off`
is a byte offset for row blocks and a record ordinal for columnar blocks.
Torn tails are truncated on restart. Wire schema:
`crates/server/matchrecord.proto` (hand-mirrored by
`crates/server/src/record.rs`; participant stats are a varint array in
`STAT_FIELDS_V1` order — append-only, bump `SCHEMA_VERSION` when adding).
Raw samples are first written as plain zstd files and later dict-recompressed
at rest at zstd-19 with `raw/match-v1.dict`; the read path handles both
encodings.

## Durability

Hot-path redb transactions (per-match state, frontier ops, id assignment)
commit fast (atomic, not fsynced); the periodic commit runs a durable
checkpoint, *then* fsyncs segment blocks, *then* commits seen-bitmaps. A
crash re-fetches a little work — re-stores are guarded by the progress
record so nothing is double-counted — and can never mis-map a player id or
leave a bitmap claiming unflushed bytes. Stranded cohort members (an
adoption whose enqueue never landed) are re-queued by a startup
reconciliation pass.

Recompaction is outside the hot path: it writes `.tmp` segment/index files,
verifies byte-identical record reconstruction, fsyncs, atomically renames,
and skips dates still held by open segment writers.
