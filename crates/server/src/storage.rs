//! Storage: append-only zstd segment log for MatchRecords + redb for the
//! small derived/mutable state (player dictionary, rank snapshots, player
//! timelines, crawl frontier, seen-bitmaps).
//!
//! Layout under DATA_DIR:
//!   state.redb                        all KV state
//!   matches/{platform}/{date}.seg     zstd blocks of length-prefixed MatchRecords
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

use anyhow::Result;
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

/// Returns the byte offset of the end of the last intact block.
fn validate_segment(seg: &mut File) -> Result<u64> {
    let len = seg.metadata()?.len();
    let mut pos = 0u64;
    seg.seek(SeekFrom::Start(0))?;
    let mut header = [0u8; 16];
    loop {
        if pos + 16 > len {
            return Ok(pos);
        }
        seg.seek(SeekFrom::Start(pos))?;
        seg.read_exact(&mut header)?;
        let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
        let clen = u32::from_le_bytes(header[8..12].try_into().unwrap()) as u64;
        if magic != SEG_MAGIC || pos + 16 + clen > len {
            return Ok(pos);
        }
        pos += 16 + clen;
    }
}

/// Reads every record (raw protobuf bytes) from a segment file.
/// Tolerates a torn tail: stops at the first invalid block.
pub fn read_segment_records(path: &Path) -> Result<Vec<Vec<u8>>> {
    let mut f = File::open(path)?;
    let len = f.metadata()?.len();
    let mut records = Vec::new();
    let mut pos = 0u64;
    let mut header = [0u8; 16];
    while pos + 16 <= len {
        f.seek(SeekFrom::Start(pos))?;
        f.read_exact(&mut header)?;
        let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
        let crc = u32::from_le_bytes(header[4..8].try_into().unwrap());
        let clen = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
        let ulen = u32::from_le_bytes(header[12..16].try_into().unwrap()) as usize;
        if magic != SEG_MAGIC || pos + 16 + clen as u64 > len {
            break;
        }
        let mut compressed = vec![0u8; clen];
        f.read_exact(&mut compressed)?;
        if crc32fast::hash(&compressed) != crc {
            break;
        }
        let block = zstd::bulk::decompress(&compressed, ulen)?;
        let mut off = 0usize;
        while off + 4 <= block.len() {
            let rlen = u32::from_le_bytes(block[off..off + 4].try_into().unwrap()) as usize;
            off += 4;
            if off + rlen > block.len() {
                break;
            }
            records.push(block[off..off + rlen].to_vec());
            off += rlen;
        }
        pos += 16 + clen as u64;
    }
    Ok(records)
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

    pub fn save_raw_sample(
        &self,
        platform: &str,
        match_id: &str,
        match_json: &str,
        timeline_json: Option<&str>,
    ) -> Result<()> {
        let dir = self.data_dir.join("raw").join(platform);
        fs::create_dir_all(&dir)?;
        fs::write(
            dir.join(format!("{match_id}.match.json.zst")),
            zstd::bulk::compress(match_json.as_bytes(), 3)?,
        )?;
        if let Some(tj) = timeline_json {
            fs::write(
                dir.join(format!("{match_id}.timeline.json.zst")),
                zstd::bulk::compress(tj.as_bytes(), 3)?,
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
    use crate::record::Participant;

    fn test_store(name: &str) -> (PathBuf, Store) {
        let dir = std::env::temp_dir()
            .join(format!("lolcrawler-test-{}-{name}", std::process::id()));
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
        t.get((platform, game_id)).unwrap().unwrap().value().to_vec()
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
