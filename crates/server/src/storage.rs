//! Storage: append-only zstd segment log for MatchRecords + redb for the
//! small derived/mutable state (player dictionary, rank snapshots, player
//! timelines, crawl frontier, seen-bitmaps).
//!
//! Layout under DATA_DIR:
//!   state.redb                        all KV state
//!   matches/{platform}/{date}.seg     zstd row blocks; closed days may be columnar
//!   matches/{platform}/{date}.idx     (game_id u64, block_off u64, rec_off u32) LE per record
//!   raw/{platform}/{match_id}.*.zst   1% raw JSON sample
//!
//! Durability: hot-path redb transactions (per-match derived state,
//! frontier ops, id assignment) commit *fast* (atomic, not fsynced) and are
//! made durable by the periodic checkpoint in [`Store::commit`], which runs
//! before any segment fsync. Ordering per commit: durable checkpoint, then
//! segment flush+fsync, then seen-bitmaps. A crash can only lose work that
//! will be re-fetched (re-stores are guarded by the progress record), never
//! corrupt the player mapping or leave a bitmap claiming unflushed bytes.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result, anyhow, bail, ensure};
use prost::Message;
use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition};
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};

use crate::config;
use crate::record::MatchRecord;

const T_PLAYERS: TableDefinition<&str, u32> = TableDefinition::new("players");
const T_META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
/// (player_id, ts_ms) -> postcard RankSnap
const T_RANKS: TableDefinition<(u32, u64), &[u8]> = TableDefinition::new("rank_snapshots");
/// (player_id, ts_ms) -> postcard (game_id, position)
const T_PLAYER_TL: TableDefinition<(u32, u64), &[u8]> = TableDefinition::new("player_timeline");
/// (platform, bucket, due_ms, player_id) -> postcard FrontierTask
const T_FRONTIER: TableDefinition<(&str, u8, u64, u32), &[u8]> =
    TableDefinition::new("frontier");
/// player_id -> current frontier key (for dedup), postcard
const T_FRONTIER_IDX: TableDefinition<u32, &[u8]> = TableDefinition::new("frontier_index");
/// player_id -> postcard (joined_ms, source)
const T_COHORT: TableDefinition<u32, &[u8]> = TableDefinition::new("cohort");
/// non-cohort player_id -> times seen in stored matches
const T_OUTSIDER: TableDefinition<u32, u32> = TableDefinition::new("outsider_seen");
/// (platform, game_id) -> 11 bytes: per-participant stored-predecessor count
/// (capped at HISTORY_REQUIRED) x10 + valid flag
const T_PROGRESS: TableDefinition<(&str, u64), &[u8]> = TableDefinition::new("sample_progress");
/// node_id -> postcard NodeRec (enrolled crawler nodes; tokens stored hashed)
const T_NODES: TableDefinition<u32, &[u8]> = TableDefinition::new("nodes");
/// Per-node hourly contribution buckets `(node_id, hour_start_ms)`,
/// upserted by the stats flusher; never pruned (all-time totals are the
/// sum of all rows).
const T_NODE_STATS: TableDefinition<(u32, u64), &[u8]> = TableDefinition::new("node_stats");

pub const BUCKET_PRIORITY: u8 = 0; // cohort members
pub const BUCKET_OTHER: u8 = 1; // legacy; drained and dropped

pub const COHORT_SRC_APEX: u8 = 0;
pub const COHORT_SRC_ADOPTED: u8 = 1;
pub const COHORT_SRC_LADDER: u8 = 2;

/// An enrolled crawler node. The bearer token itself is never stored —
/// only its sha256, so a leaked state.redb can't impersonate nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRec {
    pub name: String,
    pub token_sha256_hex: String,
    pub created_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RankSnap {
    pub tier: String,
    pub division: String,
    pub lp: i32,
    pub wins: i32,
    pub losses: i32,
}

/// Sentinel `last_visit_ms` value: this task is a *deep* visit — walk the
/// player's entire matchlist (no age cutoff, full paging). Set by the
/// `backfill` startup mode; survives restarts since it lives in the task.
pub const DEEP_VISIT_MS: u64 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrontierTask {
    pub puuid: String,
    /// Last time we walked this player's matchlist
    /// (0 = never, [`DEEP_VISIT_MS`] = deep full-history visit requested).
    pub last_visit_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FrontierKey {
    platform: String,
    bucket: u8,
    due_ms: u64,
}

const SEG_MAGIC: u32 = 0x4753_4C31; // "1LSG"
const COL_MAGIC: u32 = 0x4C4F_4331; // "1COL"
const BLOCK_HEADER_LEN: u64 = 16;
const IDX_ENTRY_LEN: usize = 20;
const MAX_SEGMENT_BLOCK_BYTES: usize = 256 * 1024 * 1024;

const RAW_DICT_NAME: &str = "match-v1.dict";
const RAW_DICT_MAX_BYTES: usize = 128 * 1024;
const RAW_DICT_MIN_SAMPLES: usize = 500;
const RAW_DICT_MAX_TRAINING_SAMPLES: usize = 4_000;
const RAW_RECOMPACT_ZSTD_LEVEL: i32 = 19;
const RAW_RECOMPACT_MIN_AGE: Duration = Duration::from_secs(3600);

static RAW_DICT_CACHE: OnceLock<Mutex<HashMap<PathBuf, Arc<[u8]>>>> = OnceLock::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SegmentBlockKind {
    Row,
    Columnar,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IdxEntry {
    game_id: u64,
    block_off: u64,
    rec_off: u32,
}

struct SegmentBlock {
    kind: SegmentBlockKind,
    block_off: u64,
    next_off: u64,
    records: Vec<Vec<u8>>,
    rec_offsets: Vec<u32>,
}

struct SegmentLayout {
    records: Vec<Vec<u8>>,
    idx_entries: Vec<IdxEntry>,
    has_row_blocks: bool,
    torn_tail: Option<SegmentTornTail>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SegmentTornTail {
    valid_len: u64,
    dropped_bytes: u64,
}

struct SegmentWriter {
    dir: PathBuf,
    date: String,
    seg: File,
    idx: File,
    /// Uncompressed, length-prefixed records waiting for the next block.
    buf: Vec<u8>,
    /// (game_id, offset in `buf`) for pending records.
    pending: Vec<(u64, u32)>,
}

impl SegmentWriter {
    fn open(dir: PathBuf) -> Result<Self> {
        fs::create_dir_all(&dir)?;
        let date = chrono::Utc::now().format("%Y-%m-%d").to_string();
        let (seg, idx) = Self::open_files(&dir, &date)?;
        Ok(Self { dir, date, seg, idx, buf: Vec::new(), pending: Vec::new() })
    }

    fn open_files(dir: &Path, date: &str) -> Result<(File, File)> {
        let seg_path = dir.join(format!("{date}.seg"));
        let mut seg = OpenOptions::new().create(true).read(true).append(true).open(&seg_path)?;
        let valid = validate_segment(&mut seg)?;
        let len = seg.metadata()?.len();
        if valid < len {
            tracing::warn!(?seg_path, valid, len, "truncating torn segment tail");
            seg.set_len(valid)?;
            seg.seek(SeekFrom::End(0))?;
        }
        let idx = OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join(format!("{date}.idx")))?;
        Ok((seg, idx))
    }

    /// True when the UTC date changed since this writer's files were opened.
    /// The caller must then run a durable redb checkpoint and call [`roll`]:
    /// rolling flushes buffered bytes, which may not hit disk before the
    /// player ids they reference are durable.
    fn needs_roll(&self) -> bool {
        chrono::Utc::now().format("%Y-%m-%d").to_string() != self.date
    }

    /// Flushes the old day's buffer and switches to today's files.
    fn roll(&mut self) -> Result<()> {
        self.flush()?;
        let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
        let (seg, idx) = Self::open_files(&self.dir, &today)?;
        self.seg = seg;
        self.idx = idx;
        self.date = today;
        Ok(())
    }

    fn append(&mut self, rec: &MatchRecord) -> Result<()> {
        let off = self.buf.len() as u32;
        let bytes = rec.encode_to_vec();
        self.buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        self.buf.extend_from_slice(&bytes);
        self.pending.push((rec.game_id, off));
        Ok(())
    }

    fn should_flush(&self) -> bool {
        self.buf.len() >= config::BLOCK_TARGET_BYTES
    }

    /// Compress + append the pending block, fsync seg and idx.
    fn flush(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let block_off = self.seg.metadata()?.len();
        let compressed = zstd::bulk::compress(&self.buf, config::ZSTD_LEVEL)?;
        let crc = crc32fast::hash(&compressed);

        let mut header = Vec::with_capacity(16);
        header.extend_from_slice(&SEG_MAGIC.to_le_bytes());
        header.extend_from_slice(&crc.to_le_bytes());
        header.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
        header.extend_from_slice(&(self.buf.len() as u32).to_le_bytes());
        self.seg.write_all(&header)?;
        self.seg.write_all(&compressed)?;
        self.seg.sync_data()?;

        let mut idx_buf = Vec::with_capacity(self.pending.len() * 20);
        for (game_id, rec_off) in &self.pending {
            idx_buf.extend_from_slice(&game_id.to_le_bytes());
            idx_buf.extend_from_slice(&block_off.to_le_bytes());
            idx_buf.extend_from_slice(&rec_off.to_le_bytes());
        }
        self.idx.write_all(&idx_buf)?;
        self.idx.sync_data()?;

        tracing::debug!(
            records = self.pending.len(),
            raw = self.buf.len(),
            compressed = compressed.len(),
            "segment block flushed"
        );
        self.buf.clear();
        self.pending.clear();
        Ok(())
    }
}

fn known_segment_magic(magic: u32) -> bool {
    matches!(magic, SEG_MAGIC | COL_MAGIC)
}

/// Returns the byte offset of the end of the last intact block.
fn validate_segment(seg: &mut File) -> Result<u64> {
    let len = seg.metadata()?.len();
    let mut pos = 0u64;
    seg.seek(SeekFrom::Start(0))?;
    let mut header = [0u8; 16];
    loop {
        if pos + BLOCK_HEADER_LEN > len {
            return Ok(pos);
        }
        seg.seek(SeekFrom::Start(pos))?;
        seg.read_exact(&mut header)?;
        let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
        let clen = u32::from_le_bytes(header[8..12].try_into().unwrap()) as u64;
        if !known_segment_magic(magic) || pos + BLOCK_HEADER_LEN + clen > len {
            return Ok(pos);
        }
        pos += BLOCK_HEADER_LEN + clen;
    }
}

/// Reads every record (raw protobuf bytes) from a segment file.
/// Tolerates a torn tail: stops at the first invalid block.
pub fn read_segment_records(path: &Path) -> Result<Vec<Vec<u8>>> {
    let mut f = File::open(path)?;
    let len = f.metadata()?.len();
    let mut records = Vec::new();
    let mut pos = 0u64;
    while pos < len {
        let Some(block) = read_segment_block(&mut f, pos, len, false)? else {
            break;
        };
        pos = block.next_off;
        records.extend(block.records);
    }
    Ok(records)
}

/// Reads one record by the unchanged `.idx` locator tuple. Row blocks use a
/// byte offset into the uncompressed row block; columnar blocks use the
/// record ordinal within that block.
pub fn read_record_at(seg_path: &Path, block_off: u64, rec_off: u32) -> Result<Vec<u8>> {
    let mut f = File::open(seg_path)?;
    let len = f.metadata()?.len();
    let block = read_segment_block(&mut f, block_off, len, true)?
        .with_context(|| format!("no block at offset {block_off}"))?;
    match block.kind {
        SegmentBlockKind::Row => block
            .rec_offsets
            .iter()
            .position(|off| *off == rec_off)
            .map(|idx| block.records[idx].clone())
            .with_context(|| format!("row record offset {rec_off} not found in block {block_off}")),
        SegmentBlockKind::Columnar => {
            block
                .records
                .get(rec_off as usize)
                .cloned()
                .with_context(|| {
                    format!("columnar record ordinal {rec_off} not found in block {block_off}")
                })
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecompactOutcome {
    pub already_compacted: bool,
    pub rebuilt_idx: bool,
    pub dropped_tail_bytes: u64,
    pub before_seg_bytes: u64,
    pub after_seg_bytes: u64,
    pub before_idx_bytes: u64,
    pub after_idx_bytes: u64,
    pub record_count: usize,
}

pub fn recompact_segment(seg: &Path, idx: &Path) -> Result<RecompactOutcome> {
    match recompact_segment_inner(seg, idx) {
        Ok(outcome) => Ok(outcome),
        Err(err) => {
            let _ = fs::remove_file(tmp_path(seg));
            let _ = fs::remove_file(tmp_path(idx));
            Err(err)
        }
    }
}

fn recompact_segment_inner(seg: &Path, idx: &Path) -> Result<RecompactOutcome> {
    let seg_tmp = tmp_path(seg);
    let idx_tmp = tmp_path(idx);
    remove_file_if_exists(&seg_tmp)?;
    remove_file_if_exists(&idx_tmp)?;

    let before_seg_bytes = fs::metadata(seg)
        .with_context(|| format!("stat segment {}", seg.display()))?
        .len();
    let before_idx_bytes = file_len_or_zero(idx)?;
    let layout = read_segment_layout(seg)?;
    let record_count = layout.records.len();
    let dropped_tail_bytes = layout.torn_tail.map_or(0, |tail| tail.dropped_bytes);
    if let Some(tail) = layout.torn_tail {
        ensure_torn_tail_unindexed(idx, tail)?;
        tracing::warn!(
            path = %seg.display(),
            valid_len = tail.valid_len,
            dropped_bytes = tail.dropped_bytes,
            "dropping unindexed torn segment tail during recompaction"
        );
    }

    if !layout.has_row_blocks {
        if let Some(tail) = layout.torn_tail {
            truncate_segment_tail(seg, tail.valid_len)?;
        }
        let rebuilt_idx = ensure_idx_consistent_or_rebuild(idx, &layout.idx_entries)?;
        return Ok(RecompactOutcome {
            already_compacted: true,
            rebuilt_idx,
            dropped_tail_bytes,
            before_seg_bytes,
            after_seg_bytes: fs::metadata(seg)?.len(),
            before_idx_bytes,
            after_idx_bytes: file_len_or_zero(idx)?,
            record_count,
        });
    }

    let mut new_idx = Vec::with_capacity(record_count);
    {
        let mut out =
            File::create(&seg_tmp).with_context(|| format!("create {}", seg_tmp.display()))?;
        for chunk in layout
            .records
            .chunks(config::COLUMNAR_RECOMPACT_RECORDS_PER_BLOCK)
        {
            let block_off =
                write_columnar_block(&mut out, chunk, config::COLUMNAR_RECOMPACT_ZSTD_LEVEL)?;
            for (ord, raw) in chunk.iter().enumerate() {
                new_idx.push(IdxEntry {
                    game_id: decode_game_id(raw)?,
                    block_off,
                    rec_off: u32::try_from(ord).context("columnar ordinal exceeds u32")?,
                });
            }
        }
        out.sync_all()
            .with_context(|| format!("fsync {}", seg_tmp.display()))?;
    }
    write_idx_tmp(&idx_tmp, &new_idx)?;

    let rewritten = read_segment_records_strict(&seg_tmp)?;
    verify_rewrite(&layout.records, &rewritten).inspect_err(|_| {
        let _ = fs::remove_file(&seg_tmp);
        let _ = fs::remove_file(&idx_tmp);
    })?;

    fs::rename(&seg_tmp, seg)
        .with_context(|| format!("rename {} over {}", seg_tmp.display(), seg.display()))?;
    fs::rename(&idx_tmp, idx)
        .with_context(|| format!("rename {} over {}", idx_tmp.display(), idx.display()))?;
    sync_parent_dir(seg)?;
    sync_parent_dir(idx)?;

    Ok(RecompactOutcome {
        already_compacted: false,
        rebuilt_idx: true,
        dropped_tail_bytes,
        before_seg_bytes,
        after_seg_bytes: fs::metadata(seg)?.len(),
        before_idx_bytes,
        after_idx_bytes: file_len_or_zero(idx)?,
        record_count,
    })
}

fn read_segment_records_strict(path: &Path) -> Result<Vec<Vec<u8>>> {
    let layout = read_segment_layout(path)?;
    ensure!(
        layout.torn_tail.is_none(),
        "rewritten segment {} ended with an invalid/truncated tail",
        path.display()
    );
    Ok(layout.records)
}

fn read_segment_layout(path: &Path) -> Result<SegmentLayout> {
    let mut f = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let len = f.metadata()?.len();
    let mut records = Vec::new();
    let mut idx_entries = Vec::new();
    let mut has_row_blocks = false;
    let mut torn_tail = None;
    let mut pos = 0u64;

    while pos < len {
        let block = match read_segment_block(&mut f, pos, len, true) {
            Ok(Some(block)) => block,
            Ok(None) => {
                torn_tail = Some(SegmentTornTail {
                    valid_len: pos,
                    dropped_bytes: len - pos,
                });
                break;
            }
            Err(_) => {
                torn_tail = Some(SegmentTornTail {
                    valid_len: pos,
                    dropped_bytes: len - pos,
                });
                break;
            }
        };
        has_row_blocks |= block.kind == SegmentBlockKind::Row;
        ensure!(
            block.records.len() == block.rec_offsets.len(),
            "record/offset count mismatch at block {}",
            block.block_off
        );
        for (raw, rec_off) in block.records.into_iter().zip(block.rec_offsets.into_iter()) {
            let game_id = decode_game_id(&raw).with_context(|| {
                format!(
                    "decode game_id at block {} rec_off {}",
                    block.block_off, rec_off
                )
            })?;
            idx_entries.push(IdxEntry {
                game_id,
                block_off: block.block_off,
                rec_off,
            });
            records.push(raw);
        }
        pos = block.next_off;
    }

    Ok(SegmentLayout {
        records,
        idx_entries,
        has_row_blocks,
        torn_tail,
    })
}

fn read_segment_block(
    f: &mut File,
    pos: u64,
    file_len: u64,
    strict: bool,
) -> Result<Option<SegmentBlock>> {
    if pos + BLOCK_HEADER_LEN > file_len {
        return invalid_block(strict, format!("truncated segment header at {pos}"));
    }

    let mut header = [0u8; 16];
    f.seek(SeekFrom::Start(pos))?;
    f.read_exact(&mut header)?;
    let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
    let crc = u32::from_le_bytes(header[4..8].try_into().unwrap());
    let clen = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
    let ulen = u32::from_le_bytes(header[12..16].try_into().unwrap()) as usize;
    let next_off = pos
        .checked_add(BLOCK_HEADER_LEN)
        .and_then(|p| p.checked_add(clen as u64))
        .ok_or_else(|| anyhow!("segment block length overflow at {pos}"))?;
    if !known_segment_magic(magic) {
        return invalid_block(
            strict,
            format!("unknown segment block magic {magic:#x} at {pos}"),
        );
    }
    if next_off > file_len {
        return invalid_block(strict, format!("truncated segment block at {pos}"));
    }
    if clen > MAX_SEGMENT_BLOCK_BYTES {
        return invalid_block(
            strict,
            format!("compressed segment block too large at {pos}: {clen} bytes"),
        );
    }
    if ulen > MAX_SEGMENT_BLOCK_BYTES {
        return invalid_block(
            strict,
            format!("uncompressed segment block too large at {pos}: {ulen} bytes"),
        );
    }

    let mut compressed = vec![0u8; clen];
    f.read_exact(&mut compressed)?;
    if crc32fast::hash(&compressed) != crc {
        return invalid_block(strict, format!("crc mismatch at segment block {pos}"));
    }
    let block = match zstd::bulk::decompress(&compressed, ulen) {
        Ok(block) => block,
        Err(err) => {
            return invalid_block(strict, format!("zstd decompress failed at {pos}: {err}"));
        }
    };
    if block.len() != ulen {
        return invalid_block(
            strict,
            format!(
                "uncompressed length mismatch at {pos}: got {} want {ulen}",
                block.len()
            ),
        );
    }

    let kind = match magic {
        SEG_MAGIC => SegmentBlockKind::Row,
        COL_MAGIC => SegmentBlockKind::Columnar,
        _ => unreachable!(),
    };
    let (records, rec_offsets) = match kind {
        SegmentBlockKind::Row => decode_row_block(&block, strict)?,
        SegmentBlockKind::Columnar => match crate::columnar::decode_block(&block) {
            Ok(records) => {
                let offsets = (0..records.len())
                    .map(|idx| u32::try_from(idx).context("columnar block has too many records"))
                    .collect::<Result<Vec<_>>>()?;
                (records, offsets)
            }
            Err(err) => {
                return invalid_block(strict, format!("columnar decode failed at {pos}: {err}"));
            }
        },
    };

    Ok(Some(SegmentBlock {
        kind,
        block_off: pos,
        next_off,
        records,
        rec_offsets,
    }))
}

fn decode_row_block(block: &[u8], strict: bool) -> Result<(Vec<Vec<u8>>, Vec<u32>)> {
    let mut records = Vec::new();
    let mut rec_offsets = Vec::new();
    let mut off = 0usize;
    while off < block.len() {
        if off + 4 > block.len() {
            if strict {
                bail!("truncated row record length at byte offset {off}");
            }
            break;
        }
        let rec_off = u32::try_from(off).context("row record offset exceeds u32")?;
        let rlen = u32::from_le_bytes(block[off..off + 4].try_into().unwrap()) as usize;
        off += 4;
        if off + rlen > block.len() {
            if strict {
                bail!("truncated row record body at byte offset {rec_off}");
            }
            break;
        }
        rec_offsets.push(rec_off);
        records.push(block[off..off + rlen].to_vec());
        off += rlen;
    }
    Ok((records, rec_offsets))
}

fn invalid_block<T>(strict: bool, msg: String) -> Result<Option<T>> {
    if strict { bail!(msg) } else { Ok(None) }
}

fn write_columnar_block(seg: &mut File, records: &[Vec<u8>], zstd_level: i32) -> Result<u64> {
    let block_off = seg.seek(SeekFrom::End(0))?;
    let payload = crate::columnar::encode_block(records)?;
    let compressed = zstd::bulk::compress(&payload, zstd_level)?;
    write_container_block(seg, COL_MAGIC, &payload, &compressed)?;
    Ok(block_off)
}

fn write_container_block(
    out: &mut File,
    magic: u32,
    uncompressed: &[u8],
    compressed: &[u8],
) -> Result<()> {
    let clen = u32::try_from(compressed.len()).context("compressed block exceeds u32")?;
    let ulen = u32::try_from(uncompressed.len()).context("uncompressed block exceeds u32")?;
    let crc = crc32fast::hash(compressed);
    let mut header = Vec::with_capacity(BLOCK_HEADER_LEN as usize);
    header.extend_from_slice(&magic.to_le_bytes());
    header.extend_from_slice(&crc.to_le_bytes());
    header.extend_from_slice(&clen.to_le_bytes());
    header.extend_from_slice(&ulen.to_le_bytes());
    out.write_all(&header)?;
    out.write_all(compressed)?;
    Ok(())
}

fn decode_game_id(raw: &[u8]) -> Result<u64> {
    Ok(MatchRecord::decode(raw)?.game_id)
}

fn verify_rewrite(want: &[Vec<u8>], got: &[Vec<u8>]) -> Result<()> {
    ensure!(
        want.len() == got.len(),
        "recompact verify record count mismatch: want {} got {}",
        want.len(),
        got.len()
    );
    for (idx, (want, got)) in want.iter().zip(got.iter()).enumerate() {
        ensure!(
            want == got,
            "recompact verify byte mismatch at record {idx}: want {} bytes got {} bytes",
            want.len(),
            got.len()
        );
    }
    Ok(())
}

fn ensure_torn_tail_unindexed(idx: &Path, tail: SegmentTornTail) -> Result<()> {
    let bytes = match fs::read(idx) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err).with_context(|| format!("read {}", idx.display())),
    };
    ensure!(
        bytes.len() % IDX_ENTRY_LEN == 0,
        "refusing to drop torn segment tail at byte {} ({} bytes): idx {} is malformed",
        tail.valid_len,
        tail.dropped_bytes,
        idx.display()
    );
    let entries = parse_idx_entries(&bytes).expect("idx length already checked");
    if let Some(entry) = entries.iter().find(|entry| entry.block_off >= tail.valid_len) {
        // idx entries are written only after the seg block is fsynced, so a
        // genuine crash tail has no idx entries at/behind it. If entries do
        // point there, the file is mid-file-corrupt (for example bitrot);
        // dropping would lose indexed records, so refuse and let the hourly
        // warning stand.
        bail!(
            "refusing to drop torn segment tail at byte {} ({} bytes): idx entry game_id={} references block_off={} at/behind the tail",
            tail.valid_len,
            tail.dropped_bytes,
            entry.game_id,
            entry.block_off
        );
    }
    Ok(())
}

fn truncate_segment_tail(seg: &Path, valid_len: u64) -> Result<()> {
    let f = OpenOptions::new()
        .write(true)
        .open(seg)
        .with_context(|| format!("open {} for tail truncation", seg.display()))?;
    f.set_len(valid_len)
        .with_context(|| format!("truncate {} to {}", seg.display(), valid_len))?;
    f.sync_all()
        .with_context(|| format!("fsync truncated {}", seg.display()))?;
    Ok(())
}

fn ensure_idx_consistent_or_rebuild(idx: &Path, expected: &[IdxEntry]) -> Result<bool> {
    if idx_matches(idx, expected)? {
        return Ok(false);
    }
    replace_idx(idx, expected)?;
    Ok(true)
}

fn idx_matches(idx: &Path, expected: &[IdxEntry]) -> Result<bool> {
    let Some(entries) = read_idx_entries(idx)? else {
        return Ok(false);
    };
    Ok(entries == expected)
}

fn read_idx_entries(idx: &Path) -> Result<Option<Vec<IdxEntry>>> {
    let bytes = match fs::read(idx) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err).with_context(|| format!("read {}", idx.display())),
    };
    Ok(parse_idx_entries(&bytes))
}

fn parse_idx_entries(bytes: &[u8]) -> Option<Vec<IdxEntry>> {
    if bytes.len() % IDX_ENTRY_LEN != 0 {
        return None;
    }
    let mut entries = Vec::with_capacity(bytes.len() / IDX_ENTRY_LEN);
    for chunk in bytes.chunks_exact(IDX_ENTRY_LEN) {
        entries.push(IdxEntry {
            game_id: u64::from_le_bytes(chunk[0..8].try_into().unwrap()),
            block_off: u64::from_le_bytes(chunk[8..16].try_into().unwrap()),
            rec_off: u32::from_le_bytes(chunk[16..20].try_into().unwrap()),
        });
    }
    Some(entries)
}

fn replace_idx(idx: &Path, entries: &[IdxEntry]) -> Result<()> {
    let idx_tmp = tmp_path(idx);
    remove_file_if_exists(&idx_tmp)?;
    write_idx_tmp(&idx_tmp, entries)?;
    fs::rename(&idx_tmp, idx)
        .with_context(|| format!("rename {} over {}", idx_tmp.display(), idx.display()))?;
    sync_parent_dir(idx)?;
    Ok(())
}

fn write_idx_tmp(path: &Path, entries: &[IdxEntry]) -> Result<()> {
    let mut out = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let mut buf = Vec::with_capacity(entries.len() * IDX_ENTRY_LEN);
    for entry in entries {
        buf.extend_from_slice(&entry.game_id.to_le_bytes());
        buf.extend_from_slice(&entry.block_off.to_le_bytes());
        buf.extend_from_slice(&entry.rec_off.to_le_bytes());
    }
    out.write_all(&buf)?;
    out.sync_all()
        .with_context(|| format!("fsync {}", path.display()))?;
    Ok(())
}

fn tmp_path(path: &Path) -> PathBuf {
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".tmp");
    PathBuf::from(tmp)
}

fn file_len_or_zero(path: &Path) -> Result<u64> {
    match fs::metadata(path) {
        Ok(meta) => Ok(meta.len()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(err) => Err(err).with_context(|| format!("stat {}", path.display())),
    }
}

fn remove_file_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("remove {}", path.display())),
    }
}

fn sync_parent_dir(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    File::open(parent)
        .with_context(|| format!("open directory {}", parent.display()))?
        .sync_all()
        .with_context(|| format!("fsync directory {}", parent.display()))?;
    Ok(())
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RawSampleRecompactOutcome {
    pub dict_created: bool,
    pub dict_bytes: u64,
    pub training_samples: usize,
    pub files_seen: usize,
    pub files_upgraded: usize,
    pub files_already_dict: usize,
    pub files_skipped_recent: usize,
    pub files_skipped_changed: usize,
    pub files_failed: usize,
    pub before_bytes: u64,
    pub after_bytes: u64,
}

pub fn recompact_raw_samples(data_dir: &Path) -> Result<RawSampleRecompactOutcome> {
    recompact_raw_samples_at(data_dir, SystemTime::now())
}

fn recompact_raw_samples_at(data_dir: &Path, now: SystemTime) -> Result<RawSampleRecompactOutcome> {
    let raw_dir = data_dir.join("raw");
    let mut outcome = RawSampleRecompactOutcome::default();
    let paths = collect_raw_sample_paths(&raw_dir)?;
    outcome.files_seen = paths.len();
    if paths.is_empty() {
        return Ok(outcome);
    }

    let dict_path = raw_dir.join(RAW_DICT_NAME);
    let dict = match fs::read(&dict_path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            let (maybe_dict, training_samples) = train_raw_sample_dict(&paths, now)?;
            outcome.training_samples = training_samples;
            let Some(dict) = maybe_dict else {
                return Ok(outcome);
            };
            write_raw_dict_atomically(&dict_path, &dict)?;
            outcome.dict_created = true;
            dict
        }
        Err(err) => return Err(err).with_context(|| format!("read {}", dict_path.display())),
    };
    outcome.dict_bytes = dict.len() as u64;
    let dict_id = raw_dict_id(&dict).with_context(|| format!("inspect {}", dict_path.display()))?;

    let encoder_dict = zstd::dict::EncoderDictionary::copy(&dict, RAW_RECOMPACT_ZSTD_LEVEL);
    let decoder_dict = zstd::dict::DecoderDictionary::copy(&dict);
    let mut compressor = zstd::bulk::Compressor::with_prepared_dictionary(&encoder_dict)?;
    compressor.set_parameter(zstd::zstd_safe::CParameter::DictIdFlag(true))?;
    let mut decompressor = zstd::bulk::Decompressor::with_prepared_dictionary(&decoder_dict)?;

    for path in paths {
        if !file_mtime_older_than(&path, RAW_RECOMPACT_MIN_AGE, now) {
            outcome.files_skipped_recent += 1;
            continue;
        }
        match recompact_raw_sample_path(&path, &mut compressor, &mut decompressor, dict_id) {
            Ok(RawSampleUpgrade::Upgraded {
                before_bytes,
                after_bytes,
            }) => {
                outcome.files_upgraded += 1;
                outcome.before_bytes += before_bytes;
                outcome.after_bytes += after_bytes;
            }
            Ok(RawSampleUpgrade::AlreadyDict) => outcome.files_already_dict += 1,
            Ok(RawSampleUpgrade::Changed) => outcome.files_skipped_changed += 1,
            Err(err) => {
                outcome.files_failed += 1;
                tracing::warn!(
                    path = %path.display(),
                    error = %err,
                    "raw sample recompression failed; leaving original file in place"
                );
            }
        }
    }

    Ok(outcome)
}

pub fn read_raw_sample(path: &Path) -> Result<String> {
    read_raw_sample_file(path)
}

enum RawSampleUpgrade {
    Upgraded { before_bytes: u64, after_bytes: u64 },
    AlreadyDict,
    Changed,
}

fn collect_raw_sample_paths(raw_dir: &Path) -> Result<Vec<PathBuf>> {
    let entries = match fs::read_dir(raw_dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err).with_context(|| format!("read_dir {}", raw_dir.display())),
    };

    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry.with_context(|| format!("read_dir entry {}", raw_dir.display()))?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let platform_dir = entry.path();
        for sample in fs::read_dir(&platform_dir)
            .with_context(|| format!("read_dir {}", platform_dir.display()))?
        {
            let sample =
                sample.with_context(|| format!("read_dir entry {}", platform_dir.display()))?;
            if sample.file_type()?.is_file() && is_raw_json_sample_path(&sample.path()) {
                paths.push(sample.path());
            }
        }
    }
    paths.sort();
    Ok(paths)
}

fn is_raw_json_sample_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            name.ends_with(".match.json.zst") || name.ends_with(".timeline.json.zst")
        })
}

fn write_raw_plain_sample_create_once(path: &Path, json: &str) -> Result<()> {
    let compressed = zstd::bulk::compress(json.as_bytes(), 3)
        .with_context(|| format!("compress raw sample {}", path.display()))?;
    write_file_create_once_atomically(path, &compressed)
}

fn write_file_create_once_atomically(path: &Path, bytes: &[u8]) -> Result<()> {
    match fs::metadata(path) {
        Ok(_) => return Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(err).with_context(|| format!("stat {}", path.display())),
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create_dir_all {}", parent.display()))?;
    }

    let tmp = raw_write_tmp_path(path);
    remove_file_if_exists(&tmp)?;
    {
        let mut out = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)
            .with_context(|| format!("create {}", tmp.display()))?;
        out.write_all(bytes)?;
        out.sync_all()
            .with_context(|| format!("fsync {}", tmp.display()))?;
    }

    match fs::hard_link(&tmp, path) {
        Ok(()) => {
            remove_file_if_exists(&tmp)?;
            sync_parent_dir(path)?;
            Ok(())
        }
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
            remove_file_if_exists(&tmp)?;
            Ok(())
        }
        Err(err) => {
            let _ = fs::remove_file(&tmp);
            Err(err).with_context(|| {
                format!("link {} into {}", tmp.display(), path.display())
            })
        }
    }
}

fn raw_write_tmp_path(path: &Path) -> PathBuf {
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".write.tmp");
    PathBuf::from(tmp)
}

fn train_raw_sample_dict(paths: &[PathBuf], now: SystemTime) -> Result<(Option<Vec<u8>>, usize)> {
    let mut samples = Vec::new();
    for path in paths {
        if samples.len() >= RAW_DICT_MAX_TRAINING_SAMPLES {
            break;
        }
        if !file_mtime_older_than(path, RAW_RECOMPACT_MIN_AGE, now) {
            continue;
        }
        let compressed = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => {
                tracing::debug!(path = %path.display(), error = %err, "skipping unreadable raw sample while training dictionary");
                continue;
            }
        };
        if raw_frame_dict_id(&compressed) != 0 {
            continue;
        }
        match decompress_raw_plain_bytes(&compressed) {
            Ok(sample) => samples.push(sample),
            Err(err) => tracing::debug!(
                path = %path.display(),
                error = %err,
                "skipping undecodable raw sample while training dictionary"
            ),
        }
    }

    let sample_count = samples.len();
    if sample_count < RAW_DICT_MIN_SAMPLES {
        return Ok((None, sample_count));
    }

    let dict = zstd::dict::from_samples(&samples, RAW_DICT_MAX_BYTES)
        .context("train raw sample zstd dictionary")?;
    ensure!(
        dict.len() <= RAW_DICT_MAX_BYTES,
        "trained raw sample dictionary is {} bytes, max {}",
        dict.len(),
        RAW_DICT_MAX_BYTES
    );
    let dict_id = raw_dict_id(&dict).context("trained raw sample dictionary has no id")?;
    tracing::info!(
        samples = sample_count,
        dict_bytes = dict.len(),
        dict_id,
        "trained raw sample dictionary"
    );
    Ok((Some(dict), sample_count))
}

fn write_raw_dict_atomically(path: &Path, dict: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create_dir_all {}", parent.display()))?;
    }
    let tmp = tmp_path(path);
    remove_file_if_exists(&tmp)?;
    {
        let mut out = File::create(&tmp).with_context(|| format!("create {}", tmp.display()))?;
        out.write_all(dict)?;
        out.sync_all()
            .with_context(|| format!("fsync {}", tmp.display()))?;
    }
    fs::rename(&tmp, path)
        .with_context(|| format!("rename {} over {}", tmp.display(), path.display()))?;
    sync_parent_dir(path)?;
    Ok(())
}

fn recompact_raw_sample_path(
    path: &Path,
    compressor: &mut zstd::bulk::Compressor<'_>,
    decompressor: &mut zstd::bulk::Decompressor<'_>,
    dict_id: u32,
) -> Result<RawSampleUpgrade> {
    let before = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    if raw_frame_dict_id(&before) != 0 {
        return Ok(RawSampleUpgrade::AlreadyDict);
    }

    let plain = decompress_raw_plain_bytes(&before)
        .with_context(|| format!("decompress plain raw sample {}", path.display()))?;
    let compressed = compressor
        .compress(&plain)
        .with_context(|| format!("compress raw sample {} with dictionary", path.display()))?;
    let frame_dict_id = raw_frame_dict_id(&compressed);
    ensure!(
        frame_dict_id == dict_id,
        "recompressed raw sample {} has dict id {}, expected {}",
        path.display(),
        frame_dict_id,
        dict_id
    );
    let roundtrip = decompressor
        .decompress(&compressed, plain.len())
        .with_context(|| format!("verify raw sample {} with dictionary", path.display()))?;
    ensure!(
        roundtrip == plain,
        "raw sample dictionary roundtrip mismatch for {}",
        path.display()
    );

    let tmp = tmp_path(path);
    remove_file_if_exists(&tmp)?;
    {
        let mut out = File::create(&tmp).with_context(|| format!("create {}", tmp.display()))?;
        out.write_all(&compressed)?;
        out.sync_all()
            .with_context(|| format!("fsync {}", tmp.display()))?;
    }

    match fs::read(path) {
        Ok(current) if current == before => {}
        Ok(_) => {
            remove_file_if_exists(&tmp)?;
            return Ok(RawSampleUpgrade::Changed);
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            remove_file_if_exists(&tmp)?;
            return Ok(RawSampleUpgrade::Changed);
        }
        Err(err) => return Err(err).with_context(|| format!("reread {}", path.display())),
    }

    fs::rename(&tmp, path)
        .with_context(|| format!("rename {} over {}", tmp.display(), path.display()))?;
    sync_parent_dir(path)?;
    Ok(RawSampleUpgrade::Upgraded {
        before_bytes: before.len() as u64,
        after_bytes: compressed.len() as u64,
    })
}

fn read_raw_sample_file(path: &Path) -> Result<String> {
    let compressed = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let json = match raw_frame_dict_id(&compressed) {
        0 => decompress_raw_plain_bytes(&compressed)
            .with_context(|| format!("decompress plain raw sample {}", path.display()))?,
        frame_dict_id => {
            let dict = load_cached_raw_dict_for_sample(path)?;
            let dict_id = raw_dict_id(dict.as_ref())?;
            ensure!(
                frame_dict_id == dict_id,
                "raw sample {} uses dict id {}, but {} has id {}",
                path.display(),
                frame_dict_id,
                RAW_DICT_NAME,
                dict_id
            );
            decompress_raw_with_dict_bytes(&compressed, dict.as_ref())
                .with_context(|| format!("decompress dict raw sample {}", path.display()))?
        }
    };
    String::from_utf8(json).with_context(|| format!("raw sample {} is not utf-8", path.display()))
}

fn load_cached_raw_dict_for_sample(path: &Path) -> Result<Arc<[u8]>> {
    let raw_dir = raw_dir_for_sample(path)?;
    let dict_path = raw_dir.join(RAW_DICT_NAME);
    let cache = RAW_DICT_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(dict) = cache
        .lock()
        .expect("raw dictionary cache poisoned")
        .get(&dict_path)
        .cloned()
    {
        return Ok(dict);
    }

    let bytes = fs::read(&dict_path).with_context(|| format!("read {}", dict_path.display()))?;
    raw_dict_id(&bytes).with_context(|| format!("inspect {}", dict_path.display()))?;
    let dict: Arc<[u8]> = Arc::from(bytes.into_boxed_slice());
    let mut guard = cache.lock().expect("raw dictionary cache poisoned");
    Ok(guard
        .entry(dict_path)
        .or_insert_with(|| dict.clone())
        .clone())
}

fn raw_dir_for_sample(path: &Path) -> Result<PathBuf> {
    let platform_dir = path
        .parent()
        .with_context(|| format!("raw sample {} has no platform directory", path.display()))?;
    platform_dir
        .parent()
        .map(Path::to_path_buf)
        .with_context(|| format!("raw sample {} has no raw directory", path.display()))
}

fn decompress_raw_plain_bytes(compressed: &[u8]) -> Result<Vec<u8>> {
    zstd::stream::decode_all(compressed).context("zstd decompress")
}

fn decompress_raw_with_dict_bytes(compressed: &[u8], dict: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = zstd::stream::Decoder::with_dictionary(compressed, dict)?;
    let mut out = Vec::new();
    decoder.read_to_end(&mut out)?;
    Ok(out)
}

fn raw_dict_id(dict: &[u8]) -> Result<u32> {
    zstd::zstd_safe::get_dict_id_from_dict(dict)
        .map(|id| id.get())
        .with_context(|| "zstd dictionary has no id")
}

fn raw_frame_dict_id(compressed: &[u8]) -> u32 {
    zstd::zstd_safe::get_dict_id_from_frame(compressed)
        .map(|id| id.get())
        .unwrap_or(0)
}

fn file_mtime_older_than(path: &Path, age: Duration, now: SystemTime) -> bool {
    fs::metadata(path)
        .and_then(|meta| meta.modified())
        .ok()
        .and_then(|mtime| now.duration_since(mtime).ok())
        .is_some_and(|elapsed| elapsed >= age)
}

pub struct StoreMatchOutcome {
    /// Players that just crossed the adoption threshold (pid, puuid); the
    /// caller should snapshot their rank and enqueue them.
    pub adopted: Vec<(u32, String)>,
}

#[derive(Debug, Default)]
pub struct StoreStats {
    pub players: u64,
    pub rank_snapshots: u64,
    pub player_timeline_entries: u64,
    /// (platform, bucket) -> queued tasks
    pub frontier: Vec<(String, u8, u64)>,
    /// platform -> seen match count
    pub seen: Vec<(String, u64)>,
    pub cohort: u64,
    pub outsiders_tracked: u64,
    /// platform -> full-history samples
    pub valid_samples: Vec<(String, u64)>,
    /// histogram over stored matches: index = number of participants with
    /// full history (0..=10)
    pub readiness_histogram: [u64; 11],
}

pub struct Store {
    db: Database,
    data_dir: PathBuf,
    /// Full puuid -> id cache (write-through).
    players: HashMap<String, u32>,
    next_player_id: u32,
    /// Per-platform seen-game bitmaps (persisted in META at commit).
    seen: HashMap<String, RoaringTreemap>,
    writers: HashMap<String, SegmentWriter>,
    /// Buffered rank snapshots, committed after segment flush.
    pending_ranks: Vec<(u32, u64, Vec<u8>)>,
    dirty_bitmaps: bool,
    /// Cohort membership cache (write-through to T_COHORT).
    cohort: std::collections::HashSet<u32>,
    /// Times each non-cohort player was seen in stored matches.
    outsider_seen: HashMap<u32, u32>,
    /// platform -> full-history sample count (write-through to META).
    valid_samples: HashMap<String, u64>,
}

impl Store {
    pub fn open(data_dir: &str) -> Result<Self> {
        let data_dir = PathBuf::from(data_dir);
        fs::create_dir_all(&data_dir)?;
        let db = Database::create(data_dir.join("state.redb"))?;

        // Ensure all tables exist, then load caches.
        let txn = db.begin_write()?;
        {
            txn.open_table(T_PLAYERS)?;
            txn.open_table(T_META)?;
            txn.open_table(T_RANKS)?;
            txn.open_table(T_PLAYER_TL)?;
            txn.open_table(T_FRONTIER)?;
            txn.open_table(T_FRONTIER_IDX)?;
            txn.open_table(T_COHORT)?;
            txn.open_table(T_OUTSIDER)?;
            txn.open_table(T_PROGRESS)?;
            txn.open_table(T_NODES)?;
            txn.open_table(T_NODE_STATS)?;
        }
        txn.commit()?;

        let rtxn = db.begin_read()?;
        let mut players = HashMap::new();
        let mut next_player_id = 1u32;
        {
            let t = rtxn.open_table(T_PLAYERS)?;
            for kv in t.iter()? {
                let (k, v) = kv?;
                let id = v.value();
                next_player_id = next_player_id.max(id + 1);
                players.insert(k.value().to_string(), id);
            }
        }
        let mut seen = HashMap::new();
        let mut valid_samples = HashMap::new();
        {
            let t = rtxn.open_table(T_META)?;
            for region in config::enabled_regions() {
                let key = format!("bitmap_{}", region.platform);
                let bm = match t.get(key.as_str())? {
                    Some(v) => RoaringTreemap::deserialize_from(v.value())?,
                    None => RoaringTreemap::new(),
                };
                seen.insert(region.platform.to_string(), bm);

                let vkey = format!("valid_samples_{}", region.platform);
                let v = t
                    .get(vkey.as_str())?
                    .map(|g| u64::from_le_bytes(g.value().try_into().unwrap_or([0; 8])))
                    .unwrap_or(0);
                valid_samples.insert(region.platform.to_string(), v);
            }
        }
        let mut cohort = std::collections::HashSet::new();
        {
            let t = rtxn.open_table(T_COHORT)?;
            for kv in t.iter()? {
                cohort.insert(kv?.0.value());
            }
        }
        let mut outsider_seen = HashMap::new();
        {
            let t = rtxn.open_table(T_OUTSIDER)?;
            for kv in t.iter()? {
                let (k, v) = kv?;
                outsider_seen.insert(k.value(), v.value());
            }
        }
        drop(rtxn);

        tracing::info!(
            players = players.len(),
            seen_matches = seen.values().map(|b| b.len()).sum::<u64>(),
            cohort = cohort.len(),
            valid_samples = valid_samples.values().sum::<u64>(),
            "store opened"
        );
        Ok(Self {
            db,
            data_dir,
            players,
            next_player_id,
            seen,
            writers: HashMap::new(),
            pending_ranks: Vec::new(),
            dirty_bitmaps: false,
            cohort,
            outsider_seen,
            valid_samples,
        })
    }

    /// UTC dates currently held by open segment writers. Recompaction uses
    /// this as a short-lived snapshot so it never renames a file that an
    /// append fd may still target.
    pub fn open_writer_dates(&self) -> Vec<String> {
        let mut dates: Vec<_> = self.writers.values().map(|w| w.date.clone()).collect();
        dates.sort();
        dates.dedup();
        dates
    }

    // ---- transactions ----

    /// Fast-path write transaction: atomic and ordered, but not fsynced.
    /// Everything committed this way becomes durable at the next
    /// [`Self::durable_checkpoint`]. A crash loses fast-path state and the
    /// in-memory segment buffer *together*, which keeps them consistent:
    /// lost matches are simply re-fetched.
    fn begin_fast(&self) -> Result<redb::WriteTransaction> {
        let mut txn = self.db.begin_write()?;
        txn.set_durability(redb::Durability::None);
        Ok(txn)
    }

    /// Durable (fsynced) redb commit: persists every fast-path transaction
    /// since the last checkpoint, plus buffered rank snapshots. Must run
    /// before any segment bytes are fsynced, so segments never reference
    /// player ids a crash could take back.
    fn durable_checkpoint(&mut self) -> Result<()> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let txn = self.db.begin_write()?; // Durability::Immediate (default)
        {
            // Always write something so the commit can't be elided.
            let mut t_meta = txn.open_table(T_META)?;
            t_meta.insert("checkpoint_ms", now_ms.to_le_bytes().as_slice())?;
            let mut t_ranks = txn.open_table(T_RANKS)?;
            for (pid, ts, val) in self.pending_ranks.drain(..) {
                t_ranks.insert((pid, ts), val.as_slice())?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    // ---- players ----

    /// Assigns ids to unknown puuids (committed fast-path; durable by the
    /// time any segment referencing them is flushed).
    pub fn assign_player_ids(&mut self, puuids: &[String]) -> Result<Vec<u32>> {
        let mut new: Vec<(String, u32)> = Vec::new();
        let mut out = Vec::with_capacity(puuids.len());
        for p in puuids {
            let id = match self.players.get(p) {
                Some(&id) => id,
                None => {
                    let id = self.next_player_id;
                    self.next_player_id += 1;
                    self.players.insert(p.clone(), id);
                    new.push((p.clone(), id));
                    id
                }
            };
            out.push(id);
        }
        if !new.is_empty() {
            let txn = self.begin_fast()?;
            {
                let mut t = txn.open_table(T_PLAYERS)?;
                for (p, id) in &new {
                    t.insert(p.as_str(), *id)?;
                }
            }
            txn.commit()?;
        }
        Ok(out)
    }

    // ---- dedup ----

    pub fn is_seen(&self, platform: &str, game_id: u64) -> bool {
        self.seen.get(platform).is_some_and(|b| b.contains(game_id))
    }

    pub fn mark_seen(&mut self, platform: &str, game_id: u64) {
        self.seen.entry(platform.to_string()).or_default().insert(game_id);
        self.dirty_bitmaps = true;
    }

    // ---- match log ----

    /// Stores a match end-to-end: assigns player ids (filling them into
    /// `rec`), counts/adopts outsiders, writes player-timeline entries,
    /// maintains the sample-progress tracker, then appends to the segment.
    /// One fast-path redb transaction per match, committed *before* the
    /// record enters the segment buffer and made durable before any segment
    /// flush, so flushed segments never reference unknown ids.
    ///
    /// Progress semantics: a participant slot counts *stored earlier games of
    /// that player* (capped at HISTORY_REQUIRED); "valid sample" = all 10
    /// slots full. This is a crawl-progress metric — the materializer does
    /// the exact last-20 check against real matchlist order.
    /// `growth_paused` (the budget brake): outsider sightings still accrue,
    /// but nobody crosses into the cohort while set — they convert on their
    /// next sighting after the brake lifts.
    pub fn store_match(
        &mut self,
        rec: &mut MatchRecord,
        puuids: &[String],
        ts_ms: u64,
        growth_paused: bool,
    ) -> Result<StoreMatchOutcome> {
        let platform = rec.platform.clone();
        let game_id = rec.game_id;
        let required = config::HISTORY_REQUIRED;
        let mut adopted: Vec<(u32, String)> = Vec::new();
        let mut newly_valid = 0u64;

        // Re-store guard: a progress record (even an empty remake marker)
        // means this match's derived state is already committed — a crash
        // can lose the seen-bitmap and cause a re-fetch. Redo only the
        // segment append (a possible duplicate record beats a hole; the
        // materializer dedups by game id); never re-count history,
        // adoption, or validity.
        let already_stored = {
            let rtxn = self.db.begin_read()?;
            let t = rtxn.open_table(T_PROGRESS)?;
            t.get((platform.as_str(), game_id))?.is_some()
        };
        // Remakes are archived but sample-irrelevant: no history credit, no
        // adoption credit, never a valid sample. Their empty progress value
        // doubles as the "stored" marker for the guard above.
        let is_remake = rec.duration_s < config::REMAKE_MAX_DURATION_S;

        // ids from cache first; new ones get created inside the txn below.
        let txn = self.begin_fast()?;
        {
            let mut t_players = txn.open_table(T_PLAYERS)?;
            let mut player_ids = Vec::with_capacity(10);
            for p in puuids {
                let id = match self.players.get(p) {
                    Some(&id) => id,
                    None => {
                        let id = self.next_player_id;
                        self.next_player_id += 1;
                        self.players.insert(p.clone(), id);
                        t_players.insert(p.as_str(), id)?;
                        id
                    }
                };
                player_ids.push(id);
            }
            for (part, pid) in rec.participants.iter_mut().zip(&player_ids) {
                part.player_id = *pid;
            }

            if is_remake && !already_stored {
                // Empty progress value = "stored, not sample-relevant".
                let mut t_prog = txn.open_table(T_PROGRESS)?;
                t_prog.insert((platform.as_str(), game_id), [0u8; 0].as_slice())?;
            } else if !already_stored {
                // Leak-driven adoption: outsiders seen often enough join the cohort.
                let mut t_out = txn.open_table(T_OUTSIDER)?;
                let mut t_coh = txn.open_table(T_COHORT)?;
                for (pid, puuid) in player_ids.iter().zip(puuids) {
                    if self.cohort.contains(pid) {
                        continue;
                    }
                    let count = self.outsider_seen.get(pid).copied().unwrap_or(0) + 1;
                    if !growth_paused && count >= config::ADOPTION_THRESHOLD {
                        self.outsider_seen.remove(pid);
                        t_out.remove(*pid)?;
                        self.cohort.insert(*pid);
                        let val = postcard::to_allocvec(&(ts_ms, COHORT_SRC_ADOPTED))?;
                        t_coh.insert(*pid, val.as_slice())?;
                        adopted.push((*pid, puuid.clone()));
                    } else {
                        self.outsider_seen.insert(*pid, count);
                        t_out.insert(*pid, count)?;
                    }
                }

                let mut t_tl = txn.open_table(T_PLAYER_TL)?;
                let mut t_prog = txn.open_table(T_PROGRESS)?;

                // Predecessor counts (strictly before ts) for this match.
                let mut progress = [0u8; 11];
                for (i, pid) in player_ids.iter().enumerate() {
                    let mut c = 0u8;
                    for kv in t_tl.range((*pid, 0u64)..(*pid, ts_ms))?.rev() {
                        kv?;
                        c += 1;
                        if c >= required {
                            break;
                        }
                    }
                    progress[i] = c;
                }
                if progress[..10].iter().all(|c| *c >= required) {
                    progress[10] = 1;
                    newly_valid += 1;
                }
                t_prog.insert((platform.as_str(), game_id), progress.as_slice())?;

                for (i, pid) in player_ids.iter().enumerate() {
                    let val = postcard::to_allocvec(&(game_id, i as u8))?;
                    t_tl.insert((*pid, ts_ms), val.as_slice())?;
                }

                // Retroactive: this match is now a stored predecessor for the
                // player's later stored games. Counts are monotone in game time,
                // so stop at the first later game already at the cap.
                for pid in &player_ids {
                    for kv in t_tl.range((*pid, ts_ms + 1)..=(*pid, u64::MAX))? {
                        let (_, v) = kv?;
                        let (gid2, pos2): (u64, u8) = postcard::from_bytes(v.value())?;
                        let key = (platform.as_str(), gid2);
                        let Some(mut pr) = t_prog.get(key)?.and_then(|g| {
                            <[u8; 11]>::try_from(g.value()).ok()
                        }) else {
                            continue; // legacy/remake match without slot counts
                        };
                        let slot = pos2 as usize;
                        if pr[slot] >= required {
                            break;
                        }
                        pr[slot] += 1;
                        if pr[10] == 0 && pr[..10].iter().all(|c| *c >= required) {
                            pr[10] = 1;
                            newly_valid += 1;
                        }
                        t_prog.insert(key, pr.as_slice())?;
                    }
                }

                if newly_valid > 0 {
                    let total = self
                        .valid_samples
                        .entry(platform.clone())
                        .and_modify(|v| *v += newly_valid)
                        .or_insert(newly_valid);
                    let mut t_meta = txn.open_table(T_META)?;
                    t_meta.insert(
                        format!("valid_samples_{platform}").as_str(),
                        total.to_le_bytes().as_slice(),
                    )?;
                }
            }
        }
        txn.commit()?;

        // A date roll flushes the old day's buffer to disk; checkpoint first
        // so those bytes never reference ids only fast-path txns know about.
        if self.writers.get(&platform).is_some_and(|w| w.needs_roll()) {
            self.durable_checkpoint()?;
            self.writers.get_mut(&platform).unwrap().roll()?;
        }
        let writer = match self.writers.entry(platform.clone()) {
            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::hash_map::Entry::Vacant(e) => e.insert(SegmentWriter::open(
                self.data_dir.join("matches").join(&platform),
            )?),
        };
        writer.append(rec)?;
        self.mark_seen(&platform, game_id);

        Ok(StoreMatchOutcome { adopted })
    }

    pub fn read_raw_sample(path: &Path) -> Result<String> {
        read_raw_sample_file(path)
    }

    pub fn save_raw_sample(
        &self,
        platform: &str,
        match_id: &str,
        match_json: &str,
        timeline_json: Option<&str>,
    ) -> Result<()> {
        let dir = self.data_dir.join("raw").join(platform);
        fs::create_dir_all(&dir)?;
        write_raw_plain_sample_create_once(
            &dir.join(format!("{match_id}.match.json.zst")),
            match_json,
        )?;
        if let Some(tj) = timeline_json {
            write_raw_plain_sample_create_once(
                &dir.join(format!("{match_id}.timeline.json.zst")),
                tj,
            )?;
        }
        Ok(())
    }

    // ---- snapshots ----

    pub fn add_rank_snapshot(&mut self, player_id: u32, ts_ms: u64, snap: &RankSnap) -> Result<()> {
        self.pending_ranks.push((player_id, ts_ms, postcard::to_allocvec(snap)?));
        Ok(())
    }

    // ---- cohort ----

    pub fn is_cohort(&self, player_id: u32) -> bool {
        self.cohort.contains(&player_id)
    }

    /// Adds players to the cohort (skipping existing members) in one txn.
    /// Returns the ids that were newly added.
    pub fn cohort_add_batch(
        &mut self,
        players: &[u32],
        source: u8,
        now_ms: u64,
    ) -> Result<Vec<u32>> {
        let new: Vec<u32> = players
            .iter()
            .copied()
            .filter(|pid| !self.cohort.contains(pid))
            .collect();
        if new.is_empty() {
            return Ok(new);
        }
        let txn = self.begin_fast()?;
        {
            let mut t_coh = txn.open_table(T_COHORT)?;
            let mut t_out = txn.open_table(T_OUTSIDER)?;
            let val = postcard::to_allocvec(&(now_ms, source))?;
            for pid in &new {
                t_coh.insert(*pid, val.as_slice())?;
                t_out.remove(*pid)?;
            }
        }
        txn.commit()?;
        for pid in &new {
            self.cohort.insert(*pid);
            self.outsider_seen.remove(pid);
        }
        Ok(new)
    }

    pub fn valid_sample_totals(&self) -> Vec<(String, u64)> {
        let mut v: Vec<_> = self
            .valid_samples
            .iter()
            .map(|(p, n)| (p.clone(), *n))
            .collect();
        v.sort();
        v
    }

    // ---- crawler nodes ----

    /// All enrolled nodes, for loading the runtime registry at startup.
    pub fn nodes_all(&self) -> Result<Vec<(u32, NodeRec)>> {
        let txn = self.db.begin_read()?;
        let t = txn.open_table(T_NODES)?;
        let mut out = Vec::new();
        for kv in t.iter()? {
            let (k, v) = kv?;
            out.push((k.value(), postcard::from_bytes(v.value())?));
        }
        Ok(out)
    }

    /// Persists a freshly enrolled node (durable immediately — enrollment
    /// is rare and losing a friend's token to a crash would be rude).
    pub fn node_add(&mut self, name: &str, token_sha256_hex: &str, now_ms: u64) -> Result<u32> {
        let id = self.meta_get_u32("next_node_id")?.unwrap_or(1);
        let rec = NodeRec {
            name: name.to_string(),
            token_sha256_hex: token_sha256_hex.to_string(),
            created_ms: now_ms,
        };
        let txn = self.db.begin_write()?; // Durability::Immediate
        {
            let mut t = txn.open_table(T_NODES)?;
            t.insert(id, postcard::to_allocvec(&rec)?.as_slice())?;
            let mut t_meta = txn.open_table(T_META)?;
            t_meta.insert("next_node_id", (id + 1).to_le_bytes().as_slice())?;
        }
        txn.commit()?;
        Ok(id)
    }

    /// All persisted stats buckets, for [`crate::stats::Stats::load`].
    pub fn node_stats_all(&self) -> Result<Vec<(u32, u64, crate::stats::Bucket)>> {
        let txn = self.db.begin_read()?;
        let t = txn.open_table(T_NODE_STATS)?;
        let mut out = Vec::new();
        for kv in t.iter()? {
            let (k, v) = kv?;
            let (node, hour) = k.value();
            out.push((node, hour, postcard::from_bytes(v.value())?));
        }
        Ok(out)
    }

    /// Upserts dirty hour buckets (fast-path: durable at next checkpoint —
    /// a crash costs at most a minute of leaderboard credit).
    pub fn node_stats_upsert(&mut self, rows: &[(u32, u64, crate::stats::Bucket)]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let txn = self.begin_fast()?;
        {
            let mut t = txn.open_table(T_NODE_STATS)?;
            for (node, hour, bucket) in rows {
                t.insert((*node, *hour), postcard::to_allocvec(bucket)?.as_slice())?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    // ---- frontier ----

    /// Adds/keeps a crawl task. If the player is already queued, the entry is
    /// kept unless the new one has a strictly better (lower) bucket.
    pub fn frontier_push(
        &mut self,
        platform: &str,
        bucket: u8,
        due_ms: u64,
        player_id: u32,
        task: &FrontierTask,
    ) -> Result<()> {
        let txn = self.begin_fast()?;
        {
            let mut t_idx = txn.open_table(T_FRONTIER_IDX)?;
            let mut t_f = txn.open_table(T_FRONTIER)?;
            let existing: Option<FrontierKey> = match t_idx.get(player_id)? {
                Some(v) => Some(postcard::from_bytes(v.value())?),
                None => None,
            };
            if let Some(ex) = existing {
                if ex.bucket <= bucket {
                    return Ok(()); // already queued at same/better priority
                }
                t_f.remove((ex.platform.as_str(), ex.bucket, ex.due_ms, player_id))?;
            }
            t_f.insert(
                (platform, bucket, due_ms, player_id),
                postcard::to_allocvec(task)?.as_slice(),
            )?;
            let key = FrontierKey { platform: platform.to_string(), bucket, due_ms };
            t_idx.insert(player_id, postcard::to_allocvec(&key)?.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Pops the highest-priority due task for a platform. Priority bucket 0
    /// first; within a bucket, earliest due first.
    pub fn frontier_pop_due(
        &mut self,
        platform: &str,
        now_ms: u64,
    ) -> Result<Option<(u8, u32, FrontierTask)>> {
        let txn = self.begin_fast()?;
        let mut popped: Option<(u8, u32, FrontierTask)> = None;
        {
            let mut t_f = txn.open_table(T_FRONTIER)?;
            let mut t_idx = txn.open_table(T_FRONTIER_IDX)?;
            for bucket in [BUCKET_PRIORITY, BUCKET_OTHER] {
                let start = (platform, bucket, 0u64, 0u32);
                let end = (platform, bucket, now_ms, u32::MAX);
                let found = {
                    let mut range = t_f.range(start..=end)?;
                    match range.next() {
                        Some(kv) => {
                            let (k, v) = kv?;
                            let (_, b, due, pid) = k.value();
                            Some((b, due, pid, postcard::from_bytes::<FrontierTask>(v.value())?))
                        }
                        None => None,
                    }
                };
                if let Some((b, due, pid, task)) = found {
                    t_f.remove((platform, b, due, pid))?;
                    t_idx.remove(pid)?;
                    popped = Some((b, pid, task));
                    break;
                }
            }
        }
        txn.commit()?;
        Ok(popped)
    }

    /// Batch frontier push (one txn), same dedup semantics as frontier_push.
    pub fn frontier_push_batch(
        &mut self,
        platform: &str,
        bucket: u8,
        due_ms: u64,
        items: &[(u32, FrontierTask)],
    ) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        let txn = self.begin_fast()?;
        {
            let mut t_idx = txn.open_table(T_FRONTIER_IDX)?;
            let mut t_f = txn.open_table(T_FRONTIER)?;
            for (player_id, task) in items {
                let existing: Option<FrontierKey> = match t_idx.get(*player_id)? {
                    Some(v) => Some(postcard::from_bytes(v.value())?),
                    None => None,
                };
                if let Some(ex) = existing {
                    if ex.bucket <= bucket {
                        continue;
                    }
                    t_f.remove((ex.platform.as_str(), ex.bucket, ex.due_ms, *player_id))?;
                }
                t_f.insert(
                    (platform, bucket, due_ms, *player_id),
                    postcard::to_allocvec(task)?.as_slice(),
                )?;
                let key = FrontierKey { platform: platform.to_string(), bucket, due_ms };
                t_idx.insert(*player_id, postcard::to_allocvec(&key)?.as_slice())?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    /// Tasks in the priority bucket overdue by more than `grace_ms`,
    /// counted up to `cap` (budget-brake signal).
    pub fn frontier_overdue_count(&self, platform: &str, now_ms: u64, grace_ms: u64, cap: u64) -> Result<u64> {
        let cutoff = now_ms.saturating_sub(grace_ms);
        let txn = self.db.begin_read()?;
        let t = txn.open_table(T_FRONTIER)?;
        let start = (platform, BUCKET_PRIORITY, 0u64, 0u32);
        let end = (platform, BUCKET_PRIORITY, cutoff, u32::MAX);
        let mut n = 0u64;
        for kv in t.range(start..=end)? {
            kv?;
            n += 1;
            if n >= cap {
                break;
            }
        }
        Ok(n)
    }

    /// Earliest due time across buckets for a platform (for idle sleeping).
    pub fn frontier_next_due(&self, platform: &str) -> Result<Option<u64>> {
        let txn = self.db.begin_read()?;
        let t_f = txn.open_table(T_FRONTIER)?;
        let mut best: Option<u64> = None;
        for bucket in [BUCKET_PRIORITY, BUCKET_OTHER] {
            let start = (platform, bucket, 0u64, 0u32);
            let end = (platform, bucket, u64::MAX, u32::MAX);
            if let Some(kv) = t_f.range(start..=end)?.next() {
                let (k, _) = kv?;
                let due = k.value().2;
                best = Some(best.map_or(due, |b: u64| b.min(due)));
            }
        }
        Ok(best)
    }

    /// Re-enqueues cohort members that have no frontier entry (e.g. an
    /// adoption whose enqueue never landed, or a pop lost to a crash).
    /// Platform is inferred by probing the progress table for the player's
    /// most recent stored game; members with no stored games are left alone
    /// (apex/ladder members re-enroll at the next seed anyway). Run once at
    /// startup. Returns how many players were re-queued.
    pub fn frontier_reconcile(&mut self, now_ms: u64) -> Result<usize> {
        let stranded: Vec<u32> = {
            let txn = self.db.begin_read()?;
            let t_idx = txn.open_table(T_FRONTIER_IDX)?;
            let t_coh = txn.open_table(T_COHORT)?;
            let mut v = Vec::new();
            for kv in t_coh.iter()? {
                let pid = kv?.0.value();
                if t_idx.get(pid)?.is_none() {
                    v.push(pid);
                }
            }
            v
        };
        if stranded.is_empty() {
            return Ok(0);
        }
        let puuid_of: HashMap<u32, String> =
            self.players.iter().map(|(p, id)| (*id, p.clone())).collect();
        let platforms: Vec<&'static str> =
            config::enabled_regions().iter().map(|r| r.platform).collect();

        let mut items: HashMap<&'static str, Vec<(u32, FrontierTask)>> = HashMap::new();
        {
            let txn = self.db.begin_read()?;
            let t_tl = txn.open_table(T_PLAYER_TL)?;
            let t_prog = txn.open_table(T_PROGRESS)?;
            for pid in stranded {
                let Some(puuid) = puuid_of.get(&pid) else {
                    tracing::warn!(pid, "cohort member without puuid mapping, skipped");
                    continue;
                };
                let last_game =
                    match t_tl.range((pid, 0u64)..=(pid, u64::MAX))?.next_back() {
                        Some(kv) => {
                            let (_, v) = kv?;
                            let (gid, _): (u64, u8) = postcard::from_bytes(v.value())?;
                            Some(gid)
                        }
                        None => None,
                    };
                let Some(gid) = last_game else {
                    continue; // no stored games yet; seeding will re-enqueue
                };
                let Some(platform) =
                    platforms.iter().find(|p| {
                        t_prog.get((**p, gid)).is_ok_and(|v| v.is_some())
                    })
                else {
                    tracing::warn!(pid, gid, "stranded cohort member on unknown platform, skipped");
                    continue;
                };
                items.entry(platform).or_default().push((
                    pid,
                    FrontierTask { puuid: puuid.clone(), last_visit_ms: 0 },
                ));
            }
        }
        let mut queued = 0;
        for (platform, batch) in items {
            queued += batch.len();
            tracing::info!(platform, n = batch.len(), "re-queued stranded cohort members");
            self.frontier_push_batch(platform, BUCKET_PRIORITY, now_ms, &batch)?;
        }
        Ok(queued)
    }

    /// `backfill` startup mode: reschedules every queued cohort member to
    /// *now* as a deep visit (full-history matchlist walk, no age cutoff).
    /// Normal scheduling resumes per player as their deep visit completes.
    /// Returns how many tasks were converted.
    pub fn frontier_backfill_reset(&mut self, now_ms: u64) -> Result<u64> {
        let entries: Vec<(String, u64, u32, FrontierTask)> = {
            let txn = self.db.begin_read()?;
            let t_f = txn.open_table(T_FRONTIER)?;
            let mut v = Vec::new();
            for kv in t_f.iter()? {
                let (k, val) = kv?;
                let (platform, bucket, due, pid) = k.value();
                if bucket != BUCKET_PRIORITY || !self.cohort.contains(&pid) {
                    continue;
                }
                v.push((platform.to_string(), due, pid, postcard::from_bytes(val.value())?));
            }
            v
        };
        let n = entries.len() as u64;
        let txn = self.begin_fast()?;
        {
            let mut t_f = txn.open_table(T_FRONTIER)?;
            let mut t_idx = txn.open_table(T_FRONTIER_IDX)?;
            for (platform, due, pid, task) in entries {
                t_f.remove((platform.as_str(), BUCKET_PRIORITY, due, pid))?;
                let deep = FrontierTask { puuid: task.puuid, last_visit_ms: DEEP_VISIT_MS };
                t_f.insert(
                    (platform.as_str(), BUCKET_PRIORITY, now_ms, pid),
                    postcard::to_allocvec(&deep)?.as_slice(),
                )?;
                let key =
                    FrontierKey { platform, bucket: BUCKET_PRIORITY, due_ms: now_ms };
                t_idx.insert(pid, postcard::to_allocvec(&key)?.as_slice())?;
            }
        }
        txn.commit()?;
        Ok(n)
    }

    // ---- meta (seed cursors etc.) ----

    pub fn meta_get_u32(&self, key: &str) -> Result<Option<u32>> {
        let txn = self.db.begin_read()?;
        let t = txn.open_table(T_META)?;
        Ok(t.get(key)?
            .map(|v| u32::from_le_bytes(v.value().try_into().unwrap_or([0; 4]))))
    }

    pub fn meta_set_u32(&self, key: &str, val: u32) -> Result<()> {
        let txn = self.begin_fast()?;
        {
            let mut t = txn.open_table(T_META)?;
            t.insert(key, val.to_le_bytes().as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    pub fn debug_stats(&self) -> Result<StoreStats> {
        let txn = self.db.begin_read()?;
        let mut stats = StoreStats {
            players: self.players.len() as u64,
            ..Default::default()
        };
        stats.rank_snapshots = txn.open_table(T_RANKS)?.len()?;
        stats.player_timeline_entries = txn.open_table(T_PLAYER_TL)?.len()?;
        {
            let t = txn.open_table(T_FRONTIER)?;
            let mut counts: HashMap<(String, u8), u64> = HashMap::new();
            for kv in t.iter()? {
                let (k, _) = kv?;
                let (platform, bucket, _, _) = k.value();
                *counts.entry((platform.to_string(), bucket)).or_default() += 1;
            }
            let mut v: Vec<_> =
                counts.into_iter().map(|((p, b), n)| (p, b, n)).collect();
            v.sort();
            stats.frontier = v;
        }
        for (platform, bm) in &self.seen {
            stats.seen.push((platform.clone(), bm.len()));
        }
        stats.seen.sort();
        stats.cohort = self.cohort.len() as u64;
        stats.outsiders_tracked = self.outsider_seen.len() as u64;
        stats.valid_samples = self.valid_sample_totals();
        {
            let t = txn.open_table(T_PROGRESS)?;
            for kv in t.iter()? {
                let (_, v) = kv?;
                let pr = v.value();
                if pr.len() == 11 {
                    let ready = pr[..10]
                        .iter()
                        .filter(|c| **c >= config::HISTORY_REQUIRED)
                        .count();
                    stats.readiness_histogram[ready] += 1;
                }
            }
        }
        Ok(stats)
    }

    // ---- commit ----

    /// Full durable commit, in the only safe order:
    /// 1. durable redb checkpoint — persists every fast-path txn (player
    ///    ids, progress, frontier, cohort) + buffered rank snapshots;
    /// 2. segment blocks flush + fsync (their ids are now durable);
    /// 3. seen-bitmaps commit last, so a bitmap can never claim a match
    ///    whose segment bytes weren't flushed.
    pub fn commit(&mut self) -> Result<()> {
        self.durable_checkpoint()?;
        for w in self.writers.values_mut() {
            w.flush()?;
        }
        if !self.dirty_bitmaps {
            return Ok(());
        }
        let txn = self.db.begin_write()?;
        {
            let mut t_meta = txn.open_table(T_META)?;
            for (platform, bm) in &self.seen {
                let mut buf = Vec::with_capacity(bm.serialized_size());
                bm.serialize_into(&mut buf)?;
                t_meta.insert(format!("bitmap_{platform}").as_str(), buf.as_slice())?;
            }
        }
        txn.commit()?;
        self.dirty_bitmaps = false;
        Ok(())
    }

    /// True when enough is buffered that a commit is worthwhile.
    pub fn should_commit(&self) -> bool {
        self.writers.values().any(|w| w.should_flush()) || self.pending_ranks.len() > 5_000
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        if let Err(e) = self.commit() {
            tracing::error!(error = %e, "final commit failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap as StdHashMap;
    use std::time::{Duration, SystemTime};

    use crate::record::Participant;
    use serde_json::{Value, json};

    fn test_store(name: &str) -> (PathBuf, Store) {
        let dir =
            std::env::temp_dir().join(format!("lolcrawler-test-{}-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let store = Store::open(dir.to_str().unwrap()).unwrap();
        (dir, store)
    }

    fn mk_match(platform: &str, game_id: u64, duration_s: u32) -> MatchRecord {
        MatchRecord {
            schema_version: 1,
            game_id,
            platform: platform.to_string(),
            queue_id: 420,
            game_start_ms: game_id as i64,
            duration_s,
            blue_won: true,
            game_version: String::new(),
            patch_major: 16,
            patch_minor: 1,
            participants: (0..10)
                .map(|i| Participant {
                    player_id: 0,
                    champion_id: 1,
                    position: (i % 5) as u32,
                    spell1: 0,
                    spell2: 0,
                    runes: vec![],
                    stats: vec![],
                })
                .collect(),
            timeline: None,
            bans: vec![],
        }
    }

    fn progress_of(store: &Store, platform: &str, game_id: u64) -> Vec<u8> {
        let txn = store.db.begin_read().unwrap();
        let t = txn.open_table(T_PROGRESS).unwrap();
        t.get((platform, game_id))
            .unwrap()
            .unwrap()
            .value()
            .to_vec()
    }

    fn segment_paths(dir: &Path, platform: &str) -> (PathBuf, PathBuf) {
        let match_dir = dir.join("matches").join(platform);
        let mut segs: Vec<_> = fs::read_dir(&match_dir)
            .unwrap()
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .filter(|path| path.extension().is_some_and(|ext| ext == "seg"))
            .collect();
        segs.sort();
        assert_eq!(
            segs.len(),
            1,
            "expected one segment in {}",
            match_dir.display()
        );
        let seg = segs.remove(0);
        let idx = seg.with_extension("idx");
        (seg, idx)
    }

    fn store_matches(
        store: &mut Store,
        platform: &str,
        first_game_id: u64,
        n: usize,
        puuid_prefix: &str,
    ) -> Vec<Vec<u8>> {
        let puuids: Vec<String> = (0..10)
            .map(|slot| format!("{puuid_prefix}-{slot:02}"))
            .collect();
        let mut out = Vec::new();
        for i in 0..n {
            let game_id = first_game_id + i as u64;
            let mut rec = mk_match(platform, game_id, 1800);
            store
                .store_match(&mut rec, &puuids, game_id, false)
                .unwrap();
            out.push(rec.encode_to_vec());
        }
        out
    }

    fn idx_entries(idx: &Path) -> Vec<IdxEntry> {
        read_idx_entries(idx).unwrap().unwrap()
    }

    fn raw_test_now() -> SystemTime {
        SystemTime::now() + RAW_RECOMPACT_MIN_AGE + Duration::from_secs(60)
    }

    fn raw_sample_path(dir: &Path, platform: &str, match_id: &str, kind: &str) -> PathBuf {
        dir.join("raw")
            .join(platform)
            .join(format!("{match_id}.{kind}.json.zst"))
    }

    fn raw_template_participants() -> Vec<String> {
        let template: Value = serde_json::from_str(include_str!("../testdata/match.json")).unwrap();
        template["metadata"]["participants"]
            .as_array()
            .unwrap()
            .iter()
            .take(10)
            .map(|value| value.as_str().unwrap().to_string())
            .collect()
    }

    fn raw_fixture_pair(platform: &str, idx: usize, participants: &[String]) -> (String, String) {
        let match_id = format!("{platform}_raw_{idx:06}");
        let game_id = 9_000_000_000u64 + idx as u64;
        let positions = ["TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"];
        let participant_json: Vec<Value> = participants
            .iter()
            .enumerate()
            .map(|(slot, puuid)| {
                let team_position = positions[slot % positions.len()];
                json!({
                    "puuid": puuid,
                    "participantId": slot + 1,
                    "teamId": if slot < 5 { 100 } else { 200 },
                    "championId": 11 + ((idx + slot) % 160),
                    "championName": format!("Champion{}", (idx + slot) % 32),
                    "teamPosition": team_position,
                    "summoner1Id": 4,
                    "summoner2Id": 14,
                    "kills": (idx + slot) % 19,
                    "deaths": (idx + slot * 2) % 11,
                    "assists": (idx + slot * 3) % 27,
                    "goldEarned": 9_000 + ((idx * 37 + slot * 101) % 11_000),
                    "totalDamageDealtToChampions": 12_000 + ((idx * 113 + slot * 257) % 45_000),
                    "wardsPlaced": (idx + slot) % 32,
                    "win": idx % 2 == slot % 2,
                    "perks": {
                        "statPerks": {"defense": 5002, "flex": 5008, "offense": 5005},
                        "styles": [
                            {"description": "primaryStyle", "style": 8000, "selections": [
                                {"perk": 8010, "var1": idx + slot, "var2": slot, "var3": idx % 7},
                                {"perk": 9111, "var1": idx % 13, "var2": slot * 2, "var3": 0}
                            ]},
                            {"description": "subStyle", "style": 8100, "selections": [
                                {"perk": 8139, "var1": idx % 17, "var2": slot, "var3": 0},
                                {"perk": 8135, "var1": idx % 19, "var2": slot + 3, "var3": 0}
                            ]}
                        ]
                    }
                })
            })
            .collect();

        let match_json = json!({
            "metadata": {
                "dataVersion": "2",
                "matchId": match_id,
                "participants": participants,
            },
            "info": {
                "gameCreation": 1_704_000_000_000u64 + idx as u64 * 1_000,
                "gameDuration": 1_500 + (idx % 900),
                "gameEndTimestamp": 1_704_000_001_500u64 + idx as u64 * 1_000,
                "gameId": game_id,
                "gameMode": "CLASSIC",
                "gameName": format!("teambuilder-match-{game_id}"),
                "gameStartTimestamp": 1_704_000_000_000u64 + idx as u64 * 1_000,
                "gameType": "MATCHED_GAME",
                "gameVersion": "16.1.1.1234",
                "mapId": 11,
                "participants": participant_json,
                "platformId": platform,
                "queueId": 420,
                "teams": [
                    {"teamId": 100, "win": idx % 2 == 0, "objectives": {"baron": {"first": true, "kills": idx % 3}, "dragon": {"first": false, "kills": idx % 5}}},
                    {"teamId": 200, "win": idx % 2 == 1, "objectives": {"baron": {"first": false, "kills": (idx + 1) % 3}, "dragon": {"first": true, "kills": (idx + 2) % 5}}}
                ]
            }
        })
        .to_string();

        let frames: Vec<Value> = (0..8)
            .map(|minute| {
                let mut participant_frames = serde_json::Map::new();
                for slot in 1..=10 {
                    participant_frames.insert(
                        slot.to_string(),
                        json!({
                            "participantId": slot,
                            "currentGold": 300 + minute * 70 + slot + idx % 23,
                            "totalGold": 500 + minute * 440 + slot * 13 + idx % 101,
                            "level": 1 + minute / 2,
                            "xp": minute * 310 + slot * 17 + idx % 97,
                            "minionsKilled": minute * 6 + slot + idx % 5,
                            "jungleMinionsKilled": if slot == 2 || slot == 7 { minute * 4 + idx % 3 } else { 0 },
                            "damageStats": {
                                "totalDamageDoneToChampions": minute * 250 + slot * 31 + idx % 211,
                                "magicDamageDoneToChampions": minute * 90 + slot * 7,
                                "physicalDamageDoneToChampions": minute * 140 + slot * 11,
                                "trueDamageDoneToChampions": minute * 12 + slot,
                            },
                            "position": {"x": 500 + minute * 70 + slot * 31, "y": 800 + minute * 45 + slot * 29}
                        }),
                    );
                }
                json!({
                    "timestamp": minute * 60_000,
                    "participantFrames": participant_frames,
                    "events": [
                        {"type": "SKILL_LEVEL_UP", "timestamp": minute * 60_000 + 2_500, "participantId": minute % 10 + 1, "skillSlot": minute % 4 + 1},
                        {"type": "WARD_PLACED", "timestamp": minute * 60_000 + 8_500, "creatorId": minute % 10 + 1, "wardType": "YELLOW_TRINKET"}
                    ]
                })
            })
            .collect();
        let timeline_json = json!({
            "metadata": {
                "dataVersion": "2",
                "matchId": match_id,
                "participants": participants,
            },
            "info": {
                "frameInterval": 60_000,
                "gameId": game_id,
                "frames": frames,
                "participants": participants.iter().enumerate().map(|(slot, puuid)| {
                    json!({"participantId": slot + 1, "puuid": puuid})
                }).collect::<Vec<_>>()
            }
        })
        .to_string();

        (match_json, timeline_json)
    }

    fn write_raw_fixtures(
        store: &Store,
        dir: &Path,
        platform: &str,
        first_idx: usize,
        pairs: usize,
    ) -> StdHashMap<PathBuf, String> {
        let participants = raw_template_participants();
        let mut originals = StdHashMap::new();
        for offset in 0..pairs {
            let idx = first_idx + offset;
            let match_id = format!("{platform}_raw_{idx:06}");
            let (match_json, timeline_json) = raw_fixture_pair(platform, idx, &participants);
            store
                .save_raw_sample(platform, &match_id, &match_json, Some(&timeline_json))
                .unwrap();
            originals.insert(
                raw_sample_path(dir, platform, &match_id, "match"),
                match_json,
            );
            originals.insert(
                raw_sample_path(dir, platform, &match_id, "timeline"),
                timeline_json,
            );
        }
        originals
    }

    fn raw_state_snapshot(dir: &Path) -> StdHashMap<PathBuf, (Vec<u8>, SystemTime)> {
        let mut paths = collect_raw_sample_paths(&dir.join("raw")).unwrap();
        let dict = dir.join("raw").join(RAW_DICT_NAME);
        if dict.exists() {
            paths.push(dict);
        }
        paths.sort();
        paths
            .into_iter()
            .map(|path| {
                let bytes = fs::read(&path).unwrap();
                let modified = fs::metadata(&path).unwrap().modified().unwrap();
                (path, (bytes, modified))
            })
            .collect()
    }

    fn raw_total_size(dir: &Path) -> u64 {
        let mut paths = collect_raw_sample_paths(&dir.join("raw")).unwrap();
        let dict = dir.join("raw").join(RAW_DICT_NAME);
        if dict.exists() {
            paths.push(dict);
        }
        paths
            .iter()
            .map(|path| fs::metadata(path).unwrap().len())
            .sum()
    }

    fn assert_raw_samples_upgraded(originals: &StdHashMap<PathBuf, String>) {
        for path in originals.keys() {
            let bytes = fs::read(path).unwrap();
            assert_ne!(raw_frame_dict_id(&bytes), 0, "{}", path.display());
        }
    }

    fn assert_idx_resolves(seg: &Path, idx: &Path, records: &[Vec<u8>]) {
        let entries = idx_entries(idx);
        assert_eq!(entries.len(), records.len());
        for (entry, raw) in entries.iter().zip(records) {
            let rec = MatchRecord::decode(raw.as_slice()).unwrap();
            assert_eq!(entry.game_id, rec.game_id);
            let got = read_record_at(seg, entry.block_off, entry.rec_off).unwrap();
            assert_eq!(got.as_slice(), raw.as_slice());
        }
    }

    fn append_segment_tail(seg: &Path, bytes: &[u8]) -> u64 {
        let mut f = OpenOptions::new().append(true).open(seg).unwrap();
        let off = f.metadata().unwrap().len();
        f.write_all(bytes).unwrap();
        f.sync_all().unwrap();
        off
    }

    fn append_idx_entry(idx: &Path, entry: IdxEntry) {
        let mut f = OpenOptions::new().append(true).open(idx).unwrap();
        f.write_all(&entry.game_id.to_le_bytes()).unwrap();
        f.write_all(&entry.block_off.to_le_bytes()).unwrap();
        f.write_all(&entry.rec_off.to_le_bytes()).unwrap();
        f.sync_all().unwrap();
    }

    #[test]
    fn raw_samples_train_dict_and_upgrade_end_to_end() {
        let platform = config::enabled_regions()[0].platform;
        let (dir, store) = test_store("raw-e2e");
        let originals = write_raw_fixtures(&store, &dir, platform, 0, RAW_DICT_MIN_SAMPLES / 2 + 8);
        let before_size = raw_total_size(&dir);

        let outcome = recompact_raw_samples_at(&dir, raw_test_now()).unwrap();

        let dict = dir.join("raw").join(RAW_DICT_NAME);
        assert!(outcome.dict_created);
        assert!(dict.exists());
        assert!(fs::metadata(&dict).unwrap().len() <= RAW_DICT_MAX_BYTES as u64);
        assert_eq!(outcome.files_seen, originals.len());
        assert_eq!(outcome.files_upgraded, originals.len());
        assert_eq!(outcome.files_failed, 0);
        assert_raw_samples_upgraded(&originals);
        let after_size = raw_total_size(&dir);
        assert!(
            after_size < before_size,
            "raw samples did not shrink: before={before_size} after={after_size}"
        );

        for (path, want) in &originals {
            assert_eq!(Store::read_raw_sample(path).unwrap(), *want);
            assert_eq!(read_raw_sample(path).unwrap(), *want);
        }
        drop(store);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn raw_sample_recompact_second_run_is_noop() {
        let platform = config::enabled_regions()[0].platform;
        let (dir, store) = test_store("raw-idem");
        let originals =
            write_raw_fixtures(&store, &dir, platform, 1_000, RAW_DICT_MIN_SAMPLES / 2 + 8);
        recompact_raw_samples_at(&dir, raw_test_now()).unwrap();
        let snapshot = raw_state_snapshot(&dir);

        let outcome =
            recompact_raw_samples_at(&dir, raw_test_now() + Duration::from_secs(7_200)).unwrap();

        assert!(!outcome.dict_created);
        assert_eq!(outcome.files_upgraded, 0);
        assert_eq!(outcome.files_already_dict, originals.len());
        assert_eq!(raw_state_snapshot(&dir), snapshot);
        drop(store);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn raw_sample_recompact_handles_mixed_plain_and_dict_state() {
        let platform = config::enabled_regions()[0].platform;
        let (dir, store) = test_store("raw-mixed");
        let originals =
            write_raw_fixtures(&store, &dir, platform, 2_000, RAW_DICT_MIN_SAMPLES / 2 + 8);
        recompact_raw_samples_at(&dir, raw_test_now()).unwrap();
        let snapshot = raw_state_snapshot(&dir);

        let new_originals = write_raw_fixtures(&store, &dir, platform, 9_000, 1);
        for path in new_originals.keys() {
            assert_eq!(raw_frame_dict_id(&fs::read(path).unwrap()), 0);
        }

        let outcome =
            recompact_raw_samples_at(&dir, raw_test_now() + Duration::from_secs(7_200)).unwrap();

        assert_eq!(outcome.files_upgraded, new_originals.len());
        assert_eq!(outcome.files_failed, 0);
        for (path, (bytes, modified)) in snapshot {
            assert_eq!(fs::read(&path).unwrap(), bytes, "{}", path.display());
            assert_eq!(fs::metadata(&path).unwrap().modified().unwrap(), modified);
        }
        assert_raw_samples_upgraded(&originals);
        assert_raw_samples_upgraded(&new_originals);
        for (path, want) in &new_originals {
            assert_eq!(Store::read_raw_sample(path).unwrap(), *want);
        }
        drop(store);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn read_raw_sample_handles_plain_file_without_dict() {
        let platform = config::enabled_regions()[0].platform;
        let (dir, store) = test_store("raw-plain-read");
        let json = json!({"metadata": {"matchId": "plain"}, "info": {"queueId": 420}}).to_string();
        store
            .save_raw_sample(platform, "plain", &json, None)
            .unwrap();
        let path = raw_sample_path(&dir, platform, "plain", "match");

        assert!(!dir.join("raw").join(RAW_DICT_NAME).exists());
        assert_eq!(raw_frame_dict_id(&fs::read(&path).unwrap()), 0);
        assert_eq!(Store::read_raw_sample(&path).unwrap(), json);
        drop(store);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn raw_sample_save_does_not_overwrite_existing_sample() {
        let platform = config::enabled_regions()[0].platform;
        let (dir, store) = test_store("raw-create-once");
        let first = json!({"metadata": {"matchId": "same"}, "info": {"queueId": 420}}).to_string();
        let second = json!({"metadata": {"matchId": "same"}, "info": {"queueId": 999}}).to_string();
        store.save_raw_sample(platform, "same", &first, None).unwrap();
        let path = raw_sample_path(&dir, platform, "same", "match");
        let first_bytes = fs::read(&path).unwrap();

        store.save_raw_sample(platform, "same", &second, None).unwrap();

        assert_eq!(fs::read(&path).unwrap(), first_bytes);
        assert_eq!(Store::read_raw_sample(&path).unwrap(), first);
        drop(store);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn raw_sample_recompact_skips_training_when_too_few_samples() {
        let platform = config::enabled_regions()[0].platform;
        let (dir, store) = test_store("raw-too-few");
        let originals = write_raw_fixtures(&store, &dir, platform, 3_000, 8);

        let outcome = recompact_raw_samples_at(&dir, raw_test_now()).unwrap();

        assert!(!outcome.dict_created);
        assert!(!dir.join("raw").join(RAW_DICT_NAME).exists());
        assert_eq!(outcome.training_samples, originals.len());
        assert_eq!(outcome.files_upgraded, 0);
        for path in originals.keys() {
            assert_eq!(raw_frame_dict_id(&fs::read(path).unwrap()), 0);
        }
        drop(store);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn read_record_at_rejects_oversized_segment_block_header() {
        let dir = std::env::temp_dir().join(format!(
            "lolcrawler-test-{}-oversized-block",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let seg = dir.join("bad.seg");
        let mut header = Vec::new();
        header.extend_from_slice(&SEG_MAGIC.to_le_bytes());
        header.extend_from_slice(&crc32fast::hash(&[]).to_le_bytes());
        header.extend_from_slice(&0u32.to_le_bytes());
        header.extend_from_slice(&u32::MAX.to_le_bytes());
        fs::write(&seg, header).unwrap();

        let err = read_record_at(&seg, 0, 0).unwrap_err().to_string();

        assert!(err.contains("uncompressed segment block too large"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn mixed_block_segment_reads_in_order() {
        let platform = config::enabled_regions()[0].platform;
        let (dir, mut store) = test_store("mixed-blocks");
        let mut want = store_matches(&mut store, platform, 10_000, 3, "mix");
        store.commit().unwrap();
        drop(store);
        let (seg, _) = segment_paths(&dir, platform);

        let columnar_records: Vec<Vec<u8>> = (20_000..20_003)
            .map(|game_id| mk_match(platform, game_id, 1800).encode_to_vec())
            .collect();
        let mut seg_file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&seg)
            .unwrap();
        write_columnar_block(
            &mut seg_file,
            &columnar_records,
            config::COLUMNAR_RECOMPACT_ZSTD_LEVEL,
        )
        .unwrap();
        seg_file.sync_all().unwrap();
        want.extend(columnar_records);

        assert_eq!(read_segment_records(&seg).unwrap(), want);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn recompact_segment_end_to_end_is_idempotent() {
        let platform = config::enabled_regions()[0].platform;
        let (dir, mut store) = test_store("recompact-e2e");
        let records = store_matches(&mut store, platform, 30_000, 24, "compact");
        store.commit().unwrap();
        drop(store);
        let (seg, idx) = segment_paths(&dir, platform);

        let outcome = recompact_segment(&seg, &idx).unwrap();
        assert!(!outcome.already_compacted);
        assert!(outcome.rebuilt_idx);
        assert_eq!(outcome.record_count, records.len());
        assert_eq!(read_segment_records(&seg).unwrap(), records);
        assert_idx_resolves(&seg, &idx, &records);

        let seg_bytes = fs::read(&seg).unwrap();
        let idx_bytes = fs::read(&idx).unwrap();
        let second = recompact_segment(&seg, &idx).unwrap();
        assert!(second.already_compacted);
        assert!(!second.rebuilt_idx);
        assert_eq!(second.record_count, records.len());
        assert_eq!(fs::read(&seg).unwrap(), seg_bytes);
        assert_eq!(fs::read(&idx).unwrap(), idx_bytes);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn recompact_cleans_tmps_and_heals_stale_idx() {
        let platform = config::enabled_regions()[0].platform;
        let (dir, mut store) = test_store("recompact-crash");
        let records = store_matches(&mut store, platform, 40_000, 12, "crash");
        store.commit().unwrap();
        drop(store);
        let (seg, idx) = segment_paths(&dir, platform);
        let seg_tmp = tmp_path(&seg);
        let idx_tmp = tmp_path(&idx);
        fs::write(&seg_tmp, b"stale segment tmp").unwrap();
        fs::write(&idx_tmp, b"stale idx tmp").unwrap();

        let outcome = recompact_segment(&seg, &idx).unwrap();
        assert!(!outcome.already_compacted);
        assert!(!seg_tmp.exists());
        assert!(!idx_tmp.exists());
        assert_eq!(read_segment_records(&seg).unwrap(), records);

        let compacted_seg = fs::read(&seg).unwrap();
        fs::write(&idx, b"stale").unwrap();
        let healed = recompact_segment(&seg, &idx).unwrap();
        assert!(healed.already_compacted);
        assert!(healed.rebuilt_idx);
        assert_eq!(fs::read(&seg).unwrap(), compacted_seg);
        assert_idx_resolves(&seg, &idx, &records);
        assert!(!seg_tmp.exists());
        assert!(!idx_tmp.exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn recompact_segment_drops_unindexed_garbage_tail() {
        let platform = config::enabled_regions()[0].platform;
        let (dir, mut store) = test_store("recompact-tail-garbage");
        let records = store_matches(&mut store, platform, 60_000, 8, "tail-garbage");
        store.commit().unwrap();
        drop(store);
        let (seg, idx) = segment_paths(&dir, platform);

        append_segment_tail(&seg, b"garbage tail bytes");

        let outcome = recompact_segment(&seg, &idx).unwrap();
        assert_eq!(outcome.dropped_tail_bytes, b"garbage tail bytes".len() as u64);
        assert_eq!(outcome.record_count, records.len());
        assert_eq!(read_segment_records(&seg).unwrap(), records);
        assert_idx_resolves(&seg, &idx, &records);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn recompact_segment_refuses_tail_with_idx_entry_inside_it() {
        let platform = config::enabled_regions()[0].platform;
        let (dir, mut store) = test_store("recompact-tail-indexed");
        let records = store_matches(&mut store, platform, 70_000, 8, "tail-indexed");
        store.commit().unwrap();
        drop(store);
        let (seg, idx) = segment_paths(&dir, platform);

        let tail_off = append_segment_tail(&seg, b"garbage tail bytes");
        append_idx_entry(&idx, IdxEntry {
            game_id: 999_999,
            block_off: tail_off,
            rec_off: 0,
        });
        let seg_before = fs::read(&seg).unwrap();
        let idx_before = fs::read(&idx).unwrap();

        let err = recompact_segment(&seg, &idx).unwrap_err().to_string();

        assert!(err.contains("refusing to drop torn segment tail"));
        assert_eq!(fs::read(&seg).unwrap(), seg_before);
        assert_eq!(fs::read(&idx).unwrap(), idx_before);
        assert_eq!(read_segment_records(&seg).unwrap(), records);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn recompact_segment_drops_unindexed_truncated_header_tail() {
        let platform = config::enabled_regions()[0].platform;
        let (dir, mut store) = test_store("recompact-tail-header");
        let records = store_matches(&mut store, platform, 80_000, 8, "tail-header");
        store.commit().unwrap();
        drop(store);
        let (seg, idx) = segment_paths(&dir, platform);

        append_segment_tail(&seg, &SEG_MAGIC.to_le_bytes()[..3]);

        let outcome = recompact_segment(&seg, &idx).unwrap();
        assert_eq!(outcome.dropped_tail_bytes, 3);
        assert_eq!(read_segment_records(&seg).unwrap(), records);
        assert_idx_resolves(&seg, &idx, &records);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn read_record_at_handles_row_and_columnar_blocks() {
        let platform = config::enabled_regions()[0].platform;
        let (dir, mut store) = test_store("point-lookup");
        let records = store_matches(&mut store, platform, 50_000, 5, "point");
        store.commit().unwrap();
        drop(store);
        let (seg, idx) = segment_paths(&dir, platform);

        let row_entries = idx_entries(&idx);
        let row_got =
            read_record_at(&seg, row_entries[2].block_off, row_entries[2].rec_off).unwrap();
        assert_eq!(row_got, records[2]);

        recompact_segment(&seg, &idx).unwrap();
        let col_entries = idx_entries(&idx);
        assert_eq!(col_entries[2].rec_off, 2);
        let col_got =
            read_record_at(&seg, col_entries[2].block_off, col_entries[2].rec_off).unwrap();
        assert_eq!(col_got, records[2]);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    #[ignore]
    fn real_segment_recompact() -> anyhow::Result<()> {
        let Ok(src_seg) = std::env::var("COLUMNAR_SEG_PATH") else {
            eprintln!("COLUMNAR_SEG_PATH unset; skipping real_segment_recompact");
            return Ok(());
        };
        let Ok(src_idx) = std::env::var("COLUMNAR_IDX_PATH") else {
            eprintln!("COLUMNAR_IDX_PATH unset; skipping real_segment_recompact");
            return Ok(());
        };

        let dir = std::env::temp_dir().join(format!(
            "lolcrawler-test-{}-real-recompact",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir)?;
        let seg = dir.join("real.seg");
        let idx = dir.join("real.idx");
        fs::copy(&src_seg, &seg)?;
        fs::copy(&src_idx, &idx)?;

        let before_size = fs::metadata(&seg)?.len();
        let before = read_segment_records(&seg)?;
        let outcome = recompact_segment(&seg, &idx)?;
        let after = read_segment_records(&seg)?;
        let after_size = fs::metadata(&seg)?.len();
        assert_eq!(after, before);
        println!(
            "real_segment_recompact records={} before={} after={} ratio={:.3}",
            outcome.record_count,
            before_size,
            after_size,
            after_size as f64 / before_size.max(1) as f64
        );
        let _ = fs::remove_dir_all(&dir);
        Ok(())
    }

    #[test]
    fn restore_is_idempotent_and_remakes_earn_nothing() {
        let platform = config::enabled_regions()[0].platform;
        let (dir, mut store) = test_store("idem");
        let puuids: Vec<String> = (0..10).map(|i| format!("p{i:02}")).collect();

        let mut rec = mk_match(platform, 100, 1800);
        store.store_match(&mut rec, &puuids, 100, false).unwrap();
        assert_eq!(store.outsider_seen.values().sum::<u32>(), 10);
        assert_eq!(progress_of(&store, platform, 100), vec![0u8; 11]);

        // Re-store (crash re-fetch): no double adoption or history credit.
        let mut again = mk_match(platform, 100, 1800);
        store.store_match(&mut again, &puuids, 100, false).unwrap();
        assert_eq!(store.outsider_seen.values().sum::<u32>(), 10);

        // Remake: archived with an empty progress marker, credits nothing.
        let mut remake = mk_match(platform, 200, 120);
        store.store_match(&mut remake, &puuids, 200, false).unwrap();
        assert_eq!(store.outsider_seen.values().sum::<u32>(), 10);
        assert!(progress_of(&store, platform, 200).is_empty());

        // Game 300 sees exactly 1 stored predecessor per player (game 100;
        // the remake earns no history credit, the re-store didn't double).
        let mut later = mk_match(platform, 300, 1800);
        store.store_match(&mut later, &puuids, 300, false).unwrap();
        let mut want = vec![1u8; 10];
        want.push(0);
        assert_eq!(progress_of(&store, platform, 300), want);

        // Backfill (game 50) retro-bumps both later games exactly once.
        let mut backfill = mk_match(platform, 50, 1800);
        store.store_match(&mut backfill, &puuids, 50, false).unwrap();
        let mut want100 = vec![1u8; 10];
        want100.push(0);
        let mut want300 = vec![2u8; 10];
        want300.push(0);
        assert_eq!(progress_of(&store, platform, 100), want100);
        assert_eq!(progress_of(&store, platform, 300), want300);

        drop(store);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn brake_defers_adoption_until_lifted() {
        let platform = config::enabled_regions()[0].platform;
        let (dir, mut store) = test_store("brake");
        let puuids: Vec<String> = (0..10).map(|i| format!("b{i:02}")).collect();

        // Two sightings while braked: counts accrue past the threshold (2)
        // but nobody joins the cohort.
        let mut a = mk_match(platform, 100, 1800);
        store.store_match(&mut a, &puuids, 100, true).unwrap();
        let mut b = mk_match(platform, 200, 1800);
        store.store_match(&mut b, &puuids, 200, true).unwrap();
        assert_eq!(store.cohort.len(), 0);
        assert!(store.outsider_seen.values().all(|c| *c == 2));

        // First sighting after the brake lifts converts everyone.
        let mut c = mk_match(platform, 300, 1800);
        let outcome = store.store_match(&mut c, &puuids, 300, false).unwrap();
        assert_eq!(outcome.adopted.len(), 10);
        assert_eq!(store.cohort.len(), 10);
        assert!(store.outsider_seen.is_empty());

        drop(store);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn backfill_reset_converts_queued_cohort_to_deep_visits() {
        let platform = config::enabled_regions()[0].platform;
        let (dir, mut store) = test_store("backfill");
        let puuids: Vec<String> = (0..3).map(|i| format!("d{i:02}")).collect();
        let pids = store.assign_player_ids(&puuids).unwrap();
        store.cohort_add_batch(&pids, COHORT_SRC_APEX, 1).unwrap();
        // Queued far in the future, as after a normal visit.
        let far = 9_000_000_000_000u64;
        for (pid, puuid) in pids.iter().zip(&puuids) {
            let task = FrontierTask { puuid: puuid.clone(), last_visit_ms: 500 };
            store.frontier_push(platform, BUCKET_PRIORITY, far, *pid, &task).unwrap();
        }
        // A non-cohort legacy entry must be left alone.
        let outsider = store.assign_player_ids(&["legacy".to_string()]).unwrap()[0];
        let legacy = FrontierTask { puuid: "legacy".into(), last_visit_ms: 500 };
        store.frontier_push(platform, BUCKET_PRIORITY, far, outsider, &legacy).unwrap();

        assert_eq!(store.frontier_backfill_reset(1_000).unwrap(), 3);
        // All cohort members are now due immediately, flagged deep.
        for _ in 0..3 {
            let (_, pid, task) = store.frontier_pop_due(platform, 2_000).unwrap().unwrap();
            assert!(pids.contains(&pid));
            assert_eq!(task.last_visit_ms, DEEP_VISIT_MS);
        }
        // The legacy entry is untouched at its original due time.
        assert!(store.frontier_pop_due(platform, 2_000).unwrap().is_none());
        let (_, pid, task) = store.frontier_pop_due(platform, u64::MAX).unwrap().unwrap();
        assert_eq!(pid, outsider);
        assert_eq!(task.last_visit_ms, 500);

        drop(store);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn reconcile_requeues_stranded_cohort_members() {
        let platform = config::enabled_regions()[0].platform;
        let (dir, mut store) = test_store("reconcile");
        let puuids: Vec<String> = (0..10).map(|i| format!("q{i:02}")).collect();
        let pids = store.assign_player_ids(&puuids).unwrap();
        store.cohort_add_batch(&pids, COHORT_SRC_ADOPTED, 1).unwrap();
        let mut rec = mk_match(platform, 100, 1800);
        store.store_match(&mut rec, &puuids, 100, false).unwrap();

        // The frontier enqueue "never landed" — all 10 are stranded.
        assert!(store.frontier_pop_due(platform, u64::MAX).unwrap().is_none());
        assert_eq!(store.frontier_reconcile(5_000).unwrap(), 10);
        // Idempotent: nothing left to reconcile once everyone is queued.
        assert_eq!(store.frontier_reconcile(5_000).unwrap(), 0);
        let (_, pid, task) = store.frontier_pop_due(platform, u64::MAX).unwrap().unwrap();
        assert!(pids.contains(&pid));
        assert!(task.puuid.starts_with('q'));

        drop(store);
        let _ = fs::remove_dir_all(&dir);
    }
}
