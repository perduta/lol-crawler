# Columnar recompaction (design note, 2026-07-05)

Status: **implemented as of 2026-07-06.** All measured numbers below are the
historical design measurements from real segments (EUW1/KR/VN2 2026-07-05,
~32k matches / ~106 MB uncompressed each). Reference experiment code:
`docs/experiments/columnar/`.

## Decision

1. **Hot path: unchanged.** Row protobuf, zstd-7, flush-on-commit blocks.
   No dictionary, no level change (both measured, both ≤ 2.4% — not worth it).
2. **Archived-day recompaction job (server-side):** startup + hourly cycle;
   once a platform-day is eligible, rewrite `matches/{platform}/{date}.seg`
   as **columnar blocks** at **zstd-19**, chunked at **6,000 records/block**,
   rebuilding the `.idx`. New block magic; record schema untouched.
3. **Raw JSON samples (`raw/`):** recompress per-file at zstd-19 with a
   **trained ≤128 KB dictionary** (`raw/match-v1.dict`; train from 500–4,000
   eligible older plain samples). Keeps per-file access for regression tests.

Net effect on archival data (segments + raw, ~335 MiB at time of writing):
**≈ −40 %**, fully lossless (byte-identical records after roundtrip).

## Implementation

- Codec: `crates/server/src/columnar.rs`. Payload magic is `LCOL`, version 1,
  then a varint manifest: record count, stat-column count, rune-column count,
  varint-encoded byte length for each stream, then streams in deterministic order
  (50 fixed streams, dynamic stat streams, dynamic rune streams).
  `encode_block` decodes its own output and byte-compares every record.
- Segment container/read path: `crates/server/src/storage.rs`. Container block
  header stays `magic u32 | crc32(compressed) u32 | compressed_len u32 |
  uncompressed_len u32 | zstd bytes`; row blocks use `SEG_MAGIC =
  0x4753_4C31` (`1LSG` on disk), columnar blocks use `COL_MAGIC =
  0x4C4F_4331` (`1COL` on disk), and the decompressed columnar payload starts
  with `LCOL`. Readers dispatch per block. `.idx` stays
  `(game_id, block_off, rec_off)`; `rec_off` is a byte offset for row blocks
  and a record ordinal for columnar blocks. `read_record_at` handles both.
- Recompaction: `recompact_segment` reads the segment strictly, chunks by
  `config::COLUMNAR_RECOMPACT_RECORDS_PER_BLOCK = 6_000`, compresses at
  `config::COLUMNAR_RECOMPACT_ZSTD_LEVEL = 19`, writes `.tmp` segment and
  `.idx` files, verifies the temp segment byte-identical before replacing,
  then renames and fsyncs parent directories. Already-columnar segments are
  left in place; a stale/missing `.idx` is rebuilt from decoded records.
- Trigger: `spawn_recompactor` runs once shortly after startup (30 s delay)
  and then hourly. A segment is eligible only when its date is before today
  in UTC, its mtime is at least one hour old, and its UTC date is not held by
  an open `SegmentWriter`.
- Raw samples: each recompaction cycle also calls `recompact_raw_samples`.
  The dict lives at `raw/match-v1.dict`; training skips recent files and
  already-dict frames, requires at least 500 samples, caps training at 4,000
  samples and 128 KiB, then recompresses eligible files at zstd-19 with a
  frame dict id. `read_raw_sample` handles both plain and dict-compressed
  files and caches the dict.
- Real-data verification: EUW1 2026-07-04, 2,033 records, 10.18 MB →
  5.76 MB (ratio 0.566), byte-identical after readback. Re-run against real
  segments with the ignored tests `real_segment_roundtrip`
  (`COLUMNAR_SEG_PATH`) and `real_segment_recompact` (`COLUMNAR_SEG_PATH`,
  `COLUMNAR_IDX_PATH`).

## Measured results (per EUW1 day, 67.9 MB stored today)

| Scheme                                    | Size    | vs today |
|-------------------------------------------|---------|----------|
| current (row, zstd-7, ~67 KB blocks)       | 67.9 MB | —        |
| row + zstd-19                              | 58.4 MB | −14 %    |
| row + xz -9e (row ceiling)                 | 53.1 MB | −22 %    |
| **columnar + zstd-19 (chosen)**            | 40.5 MB | **−40 %**|
| columnar + xz -9e (ceiling)                | 37.6 MB | −45 %    |

KR: 39.7 MB, VN2: 40.5 MB — the ~2.6× ratio generalizes across platforms.
Raw JSON (38 MiB stored): zstd-19+dict → 22 MiB (−43 %); solid tar → 19 MiB
(only if per-file access is ever dropped).

## Columnar block format

Lossless, reversible transform of the existing `MatchRecord` protobuf
(schema unchanged). Prototype spec: `docs/experiments/columnar/roundtrip.py`
(encoder + decoder, **verified byte-identical on all 96,368 records** of
EUW1+KR+VN2 2026-07-05). Production codec:
`crates/server/src/columnar.rs`; experiment bench:
`docs/experiments/columnar/transpose_bench.rs` (287 MB/s single-thread).

Per block of N records, every field becomes its own contiguous column
stream; block payload = concatenation of columns in a fixed order + a small
manifest of column lengths (needed to locate streams; a few hundred bytes).
Varint/zigzag encoding throughout:

- scalars (queue, duration, patch, …): plain varint columns, record order;
  `game_id` and `game_start_ms` delta vs previous record (zigzag);
- participants: one column per subfield; **one column per rune slot** and
  **one column per `STAT_FIELDS_V1` index** (stat columns keep the wire's
  zigzag varints — pure byte redistribution);
- timeline minute series (gold/xp/cs/dmg): **delta along minutes per
  participant slot** (zigzag) — this is the single biggest win; the
  transposed blob is smaller than the input *before* compression
  (106.7 → 81.5 MB);
- kill/objective/ward events: column per subfield, timestamps
  delta-encoded within the record;
- counts (participants, runes, stats, frames, series lengths, events) are
  explicit columns so variable shapes reconstruct exactly;
- **timeline presence bit** per record: distinguishes absent vs
  present-but-empty `TimelineLite` (subtle but required for byte-identical
  reconstruction — proto3 default-omission is otherwise ambiguous).

Reconstruction rebuilds prost's canonical encoding (fields in tag order,
defaults omitted); safe because all records are produced by this server.

### Container / migration

- New segment block magic `COL_MAGIC = 0x4C4F_4331` (`1COL`) alongside row
  `SEG_MAGIC = 0x4753_4C31` (`1LSG`); readers dispatch per block, so a `.seg`
  may mix row blocks (today's tail) and columnar blocks (compacted). The
  columnar payload itself starts with `LCOL`.
- `.idx` format unchanged: `(game_id, block_off, rec_off)`; for columnar
  blocks `rec_off` is the record's ordinal in the block, not a byte offset.
- Record `SCHEMA_VERSION` does **not** change — records reconstruct
  byte-identically, `STAT_FIELDS_V1` untouched.
- Durability invariant untouched: hot-path redb → segment-fsync → bitmap
  ordering is unchanged. Recompaction writes `.tmp` segment + idx files,
  fsyncs them, verifies the temp segment byte-identical, atomically renames,
  then fsyncs parent directories.
- Nodes are unaffected (strategy- and storage-agnostic; server-only change).

### Chunk size: 6,000 records

Measured ratio-vs-chunk curve (zstd-19, per-chunk columnarization):
250 → +9.1 %, 500 → +6.9 %, 1k → +5.1 %, 2k → +3.8 %, 4k → +2.7 %,
8k → +1.4 %, whole day = floor. Each doubling buys ~1–1.5 %. The real trade
is read amplification: a point lookup decompresses + re-rowifies the whole
block (~50 ms at 8k records, ~7 ms at 1k). Reads today are bulk
replay/backfill, so the implementation uses 6k; the constant is not encoded
in the format and can change per compaction run.

## Rejected (all measured, keep for posterity)

| Idea | Result | Verdict |
|---|---|---|
| Just raise segment zstd level 7→19 | −14 % | superseded by columnar |
| Fix small hot-path blocks (~67 KB avg vs 4 MiB target; early flush by durability commit) | −6 % | not worth touching hot path; recompaction absorbs it |
| Dict for hot-path row blocks (trained day-1 → applied day-2) | −2.4 % | blocks already too big for dicts |
| Dict for **columnar** chunks | neutral to negative at every chunk size | columns self-prime; chunks 0.6–5 MB ≫ dict-useful sizes |
| Sort records by (queue, patch, game_id) pre-encode | −0.7 % | not worth losing append order |
| Second-order deltas on minute series | **worse** (+4 %) | per-minute income too noisy |
| xz instead of zstd | −7 % more | not worth decode speed/dependency |

## Cost (measured)

Per platform-day (~32k matches): decompress 0.16 s + transpose 0.37 s (Rust,
1 thread) + zstd-19 ≈ 26 s CPU ≈ **0.8 ms CPU per match**; zstd-19 is >95 %
of the cost. Today's 4 platforms ≈ 105 s CPU/day; blocks compress
independently so wall time parallelizes freely. Fallback if CPU ever
matters: columnar + zstd-7 is only +4 % size for ~10× less compression CPU.

## Open items

- Re-train `raw/match-v1.dict` occasionally as Riot JSON shape shifts with
  patches; the implemented reader/writer uses the existing dict until it is
  replaced.
