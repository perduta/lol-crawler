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
//! Durability ordering per commit: new players are committed to redb the
//! moment ids are assigned (so segment records never reference unknown ids);
//! then segment blocks are flushed+fsynced; then bitmaps/frontier/snapshots
//! commit. A crash can only lose work that will be re-fetched, never corrupt
//! the player mapping.

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

pub const BUCKET_PRIORITY: u8 = 0; // cohort members
pub const BUCKET_OTHER: u8 = 1; // legacy; drained and dropped

pub const COHORT_SRC_APEX: u8 = 0;
pub const COHORT_SRC_ADOPTED: u8 = 1;
pub const COHORT_SRC_LADDER: u8 = 2;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RankSnap {
    pub tier: String,
    pub division: String,
    pub lp: i32,
    pub wins: i32,
    pub losses: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrontierTask {
    pub puuid: String,
    /// Last time we walked this player's matchlist (0 = never).
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

    fn append(&mut self, rec: &MatchRecord) -> Result<()> {
        // Roll the file on date change so segments stay time-ordered.
        let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
        if today != self.date {
            self.flush()?;
            let (seg, idx) = Self::open_files(&self.dir, &today)?;
            self.seg = seg;
            self.idx = idx;
            self.date = today;
        }
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

    // ---- players ----

    /// Assigns ids to unknown puuids and durably commits them immediately.
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
            let txn = self.db.begin_write()?;
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
    /// One durable redb transaction per match, committed *before* the record
    /// enters the segment buffer, so segments never reference unknown ids.
    ///
    /// Progress semantics: a participant slot counts *stored earlier games of
    /// that player* (capped at HISTORY_REQUIRED); "valid sample" = all 10
    /// slots full. This is a crawl-progress metric — the materializer does
    /// the exact last-20 check against real matchlist order.
    pub fn store_match(
        &mut self,
        rec: &mut MatchRecord,
        puuids: &[String],
        ts_ms: u64,
    ) -> Result<StoreMatchOutcome> {
        let platform = rec.platform.clone();
        let game_id = rec.game_id;
        let required = config::HISTORY_REQUIRED;
        let mut adopted: Vec<(u32, String)> = Vec::new();
        let mut newly_valid = 0u64;

        // ids from cache first; new ones get created inside the txn below.
        let txn = self.db.begin_write()?;
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

            // Leak-driven adoption: outsiders seen often enough join the cohort.
            let mut t_out = txn.open_table(T_OUTSIDER)?;
            let mut t_coh = txn.open_table(T_COHORT)?;
            for (pid, puuid) in player_ids.iter().zip(puuids) {
                if self.cohort.contains(pid) {
                    continue;
                }
                let count = self.outsider_seen.get(pid).copied().unwrap_or(0) + 1;
                if count >= config::ADOPTION_THRESHOLD {
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
                        continue; // legacy match without a progress record
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
        txn.commit()?;

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
        let txn = self.db.begin_write()?;
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
        let txn = self.db.begin_write()?;
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
        let txn = self.db.begin_write()?;
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
        let txn = self.db.begin_write()?;
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

    // ---- meta (seed cursors etc.) ----

    pub fn meta_get_u32(&self, key: &str) -> Result<Option<u32>> {
        let txn = self.db.begin_read()?;
        let t = txn.open_table(T_META)?;
        Ok(t.get(key)?
            .map(|v| u32::from_le_bytes(v.value().try_into().unwrap_or([0; 4]))))
    }

    pub fn meta_set_u32(&self, key: &str, val: u32) -> Result<()> {
        let txn = self.db.begin_write()?;
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

    /// Flushes segment blocks (fsync), then commits bitmaps + buffered rank
    /// snapshots in one transaction.
    pub fn commit(&mut self) -> Result<()> {
        for w in self.writers.values_mut() {
            w.flush()?;
        }
        if !self.dirty_bitmaps && self.pending_ranks.is_empty() {
            return Ok(());
        }
        let txn = self.db.begin_write()?;
        {
            let mut t_meta = txn.open_table(T_META)?;
            if self.dirty_bitmaps {
                for (platform, bm) in &self.seen {
                    let mut buf = Vec::with_capacity(bm.serialized_size());
                    bm.serialize_into(&mut buf)?;
                    t_meta.insert(format!("bitmap_{platform}").as_str(), buf.as_slice())?;
                }
            }
            let mut t_ranks = txn.open_table(T_RANKS)?;
            for (pid, ts, val) in self.pending_ranks.drain(..) {
                t_ranks.insert((pid, ts), val.as_slice())?;
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
