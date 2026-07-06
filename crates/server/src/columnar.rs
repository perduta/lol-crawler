//! Columnar block transform for archived `MatchRecord` protobuf bytes.
//!
//! The payload produced here is intentionally uncompressed; callers own the
//! outer zstd block. Layout:
//!
//! ```text
//! magic      4 bytes: b"LCOL"
//! version    1 byte:  1
//! records    varint:  number of records in the block
//! stats      varint:  number of dynamic stat-index columns present
//! runes      varint:  number of dynamic rune-slot columns present
//! lengths    varint[] byte length for each stream, in stream order
//! streams    bytes:   stream payloads concatenated in the same order
//! ```
//!
//! Stream order is deterministic: the 50 fixed streams listed in
//! `FIXED_NAMES`, then `stat_000..stat_{N-1}`, then `rune_00..rune_{N-1}`.
//! Dynamic counts are the maximum stat/rune vector lengths observed in the
//! block, so blocks with narrower records do not carry trailing empty dynamic
//! columns. All delta state is block-local and starts from zero.

#![allow(dead_code)] // Runtime callers are added by the recompaction/read-path step.

use anyhow::{Context, Result, anyhow, bail, ensure};
use prost::Message;

use crate::record::{MatchRecord, TimelineLite};

const MAGIC: &[u8; 4] = b"LCOL";
const VERSION: u8 = 1;

const SCHEMA: usize = 0;
const GID_D: usize = 1;
const PLAT_LEN: usize = 2;
const PLAT: usize = 3;
const QUEUE: usize = 4;
const START_D: usize = 5;
const DUR: usize = 6;
const BLUE: usize = 7;
const GV_LEN: usize = 8;
const GV: usize = 9;
const PMAJ: usize = 10;
const PMIN: usize = 11;
const BANS_N: usize = 12;
const BANS: usize = 13;
const NPARTS: usize = 14;
const P_PLAYER: usize = 15;
const P_CHAMP: usize = 16;
const P_POS: usize = 17;
const P_SP1: usize = 18;
const P_SP2: usize = 19;
const P_RUNES_N: usize = 20;
const P_STATS_N: usize = 21;
const TL_PRESENT: usize = 22;
const NFRAMES: usize = 23;
const F_MINUTE: usize = 24;
const F_GOLD_N: usize = 25;
const F_GOLD: usize = 26;
const F_XP_N: usize = 27;
const F_XP: usize = 28;
const F_CS_N: usize = 29;
const F_CS: usize = 30;
const F_DMG_N: usize = 31;
const F_DMG: usize = 32;
const NKILLS: usize = 33;
const K_T: usize = 34;
const K_KILLER: usize = 35;
const K_VICTIM: usize = 36;
const K_MASK: usize = 37;
const K_X: usize = 38;
const K_Y: usize = 39;
const NOBJS: usize = 40;
const O_T: usize = 41;
const O_KIND: usize = 42;
const O_KILLER: usize = 43;
const O_TEAM: usize = 44;
const O_LANE: usize = 45;
const NWARDS: usize = 46;
const W_T: usize = 47;
const W_KIND: usize = 48;
const W_PART: usize = 49;

const NMISC: usize = 50;
const FIXED_NAMES: [&str; NMISC] = [
    "schema",
    "game_id_d",
    "plat_len",
    "plat",
    "queue",
    "start_d",
    "dur",
    "blue_won",
    "gv_len",
    "gv",
    "patch_maj",
    "patch_min",
    "bans_n",
    "bans",
    "n_parts",
    "p_player",
    "p_champ",
    "p_pos",
    "p_sp1",
    "p_sp2",
    "p_runes_n",
    "p_stats_n",
    "tl_present",
    "n_frames",
    "f_minute",
    "f_gold_n",
    "f_gold",
    "f_xp_n",
    "f_xp",
    "f_cs_n",
    "f_cs",
    "f_dmg_n",
    "f_dmg",
    "n_kills",
    "k_t",
    "k_killer",
    "k_victim",
    "k_mask",
    "k_x",
    "k_y",
    "n_objs",
    "o_t",
    "o_kind",
    "o_killer",
    "o_team",
    "o_lane",
    "n_wards",
    "w_t",
    "w_kind",
    "w_part",
];

/// records = raw MatchRecord protobuf bytes (as stored in row blocks, without
/// the u32 length prefix).
pub fn encode_block(records: &[Vec<u8>]) -> Result<Vec<u8>> {
    let mut cols = Cols::new();
    let mut state = BlockState::default();

    for (idx, raw) in records.iter().enumerate() {
        let record = MatchRecord::decode(raw.as_slice())
            .with_context(|| format!("decode input record {idx}"))?;
        encode_record(&record, &mut cols, &mut state)
            .with_context(|| format!("encode input record {idx}"))?;
    }

    let payload = encode_payload(records.len(), &cols)?;
    let rebuilt = decode_block(&payload).context("self-check decode failed")?;
    ensure!(
        rebuilt.len() == records.len(),
        "self-check record count mismatch: input {} decoded {}",
        records.len(),
        rebuilt.len()
    );
    for (idx, (want, got)) in records.iter().zip(rebuilt.iter()).enumerate() {
        if want != got {
            bail!("{}", mismatch_error(idx, want, got));
        }
    }

    Ok(payload)
}

/// Inverse of [`encode_block`]: returns the exact original protobuf bytes for
/// every record represented by the columnar payload.
pub fn decode_block(payload: &[u8]) -> Result<Vec<Vec<u8>>> {
    let manifest = Manifest::parse(payload)?;
    let mut cols = ColReaders::new(&manifest.columns, manifest.stat_cols, manifest.rune_cols)?;
    let mut state = BlockState::default();
    let mut records = Vec::new();

    for idx in 0..manifest.records {
        records.push(
            decode_record(&mut cols, &mut state).with_context(|| format!("decode record {idx}"))?,
        );
    }
    cols.ensure_done()?;
    Ok(records)
}

#[derive(Default)]
struct BlockState {
    game_id: u64,
    game_start_ms: i64,
}

struct Cols {
    fixed: Vec<Vec<u8>>,
    stat: Vec<Vec<u8>>,
    rune: Vec<Vec<u8>>,
}

impl Cols {
    fn new() -> Self {
        Self {
            fixed: (0..NMISC).map(|_| Vec::new()).collect(),
            stat: Vec::new(),
            rune: Vec::new(),
        }
    }

    fn wv(&mut self, col: usize, v: u64) {
        write_varint(&mut self.fixed[col], v);
    }

    fn wz(&mut self, col: usize, v: i64) {
        self.wv(col, zigzag_i64(v));
    }

    fn wbytes(&mut self, col: usize, bytes: &[u8]) {
        self.fixed[col].extend_from_slice(bytes);
    }

    fn rune(&mut self, idx: usize, v: u64) {
        while self.rune.len() <= idx {
            self.rune.push(Vec::new());
        }
        write_varint(&mut self.rune[idx], v);
    }

    fn stat_z(&mut self, idx: usize, v: i64) {
        while self.stat.len() <= idx {
            self.stat.push(Vec::new());
        }
        write_varint(&mut self.stat[idx], zigzag_i64(v));
    }
}

fn encode_record(record: &MatchRecord, cols: &mut Cols, state: &mut BlockState) -> Result<()> {
    cols.wv(SCHEMA, record.schema_version as u64);

    let gid_d = delta_u64(record.game_id, state.game_id, "game_id")?;
    cols.wz(GID_D, gid_d);
    state.game_id = record.game_id;

    cols.wv(PLAT_LEN, record.platform.len() as u64);
    cols.wbytes(PLAT, record.platform.as_bytes());
    cols.wv(QUEUE, record.queue_id as u64);

    let start_d = delta_i64(record.game_start_ms, state.game_start_ms, "game_start_ms")?;
    cols.wz(START_D, start_d);
    state.game_start_ms = record.game_start_ms;

    cols.wv(DUR, record.duration_s as u64);
    cols.wv(BLUE, u64::from(record.blue_won));
    cols.wv(GV_LEN, record.game_version.len() as u64);
    cols.wbytes(GV, record.game_version.as_bytes());
    cols.wv(PMAJ, record.patch_major as u64);
    cols.wv(PMIN, record.patch_minor as u64);

    cols.wv(BANS_N, record.bans.len() as u64);
    for &ban in &record.bans {
        cols.wv(BANS, ban as u64);
    }

    cols.wv(NPARTS, record.participants.len() as u64);
    for participant in &record.participants {
        cols.wv(P_PLAYER, participant.player_id as u64);
        cols.wv(P_CHAMP, participant.champion_id as u64);
        cols.wv(P_POS, participant.position as u64);
        cols.wv(P_SP1, participant.spell1 as u64);
        cols.wv(P_SP2, participant.spell2 as u64);
        cols.wv(P_RUNES_N, participant.runes.len() as u64);
        for (idx, &rune) in participant.runes.iter().enumerate() {
            cols.rune(idx, rune as u64);
        }
        cols.wv(P_STATS_N, participant.stats.len() as u64);
        for (idx, &stat) in participant.stats.iter().enumerate() {
            cols.stat_z(idx, stat);
        }
    }

    cols.wv(TL_PRESENT, u64::from(record.timeline.is_some()));
    if let Some(timeline) = &record.timeline {
        encode_timeline(timeline, cols)?;
    }

    Ok(())
}

fn encode_timeline(timeline: &TimelineLite, cols: &mut Cols) -> Result<()> {
    cols.wv(NFRAMES, timeline.frames.len() as u64);
    let mut prev_series: [Vec<u32>; 4] = std::array::from_fn(|_| Vec::new());
    for frame in &timeline.frames {
        cols.wv(F_MINUTE, frame.minute as u64);
        let series = [
            (F_GOLD_N, F_GOLD, frame.total_gold.as_slice()),
            (F_XP_N, F_XP, frame.xp.as_slice()),
            (F_CS_N, F_CS, frame.cs.as_slice()),
            (F_DMG_N, F_DMG, frame.dmg_to_champs.as_slice()),
        ];
        for (idx, (n_col, v_col, values)) in series.into_iter().enumerate() {
            cols.wv(n_col, values.len() as u64);
            for (slot, &value) in values.iter().enumerate() {
                let prev = prev_series[idx].get(slot).copied().unwrap_or(0);
                cols.wz(v_col, value as i64 - prev as i64);
            }
            prev_series[idx] = values.to_vec();
        }
    }

    cols.wv(NKILLS, timeline.kills.len() as u64);
    let mut prev_t = 0u32;
    for kill in &timeline.kills {
        cols.wz(K_T, kill.t_s as i64 - prev_t as i64);
        prev_t = kill.t_s;
        cols.wv(K_KILLER, kill.killer as u64);
        cols.wv(K_VICTIM, kill.victim as u64);
        cols.wv(K_MASK, kill.assist_mask as u64);
        cols.wv(K_X, kill.x as u64);
        cols.wv(K_Y, kill.y as u64);
    }

    cols.wv(NOBJS, timeline.objectives.len() as u64);
    prev_t = 0;
    for obj in &timeline.objectives {
        cols.wz(O_T, obj.t_s as i64 - prev_t as i64);
        prev_t = obj.t_s;
        cols.wv(O_KIND, obj.kind as u64);
        cols.wv(O_KILLER, obj.killer as u64);
        cols.wv(O_TEAM, obj.losing_team as u64);
        cols.wv(O_LANE, obj.lane as u64);
    }

    cols.wv(NWARDS, timeline.wards.len() as u64);
    prev_t = 0;
    for ward in &timeline.wards {
        cols.wz(W_T, ward.t_s as i64 - prev_t as i64);
        prev_t = ward.t_s;
        cols.wv(W_KIND, ward.kind as u64);
        cols.wv(W_PART, ward.participant as u64);
    }

    Ok(())
}

fn encode_payload(records: usize, cols: &Cols) -> Result<Vec<u8>> {
    let total_cols = NMISC
        .checked_add(cols.stat.len())
        .and_then(|n| n.checked_add(cols.rune.len()))
        .context("too many column streams")?;
    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    out.push(VERSION);
    write_varint(&mut out, records as u64);
    write_varint(&mut out, cols.stat.len() as u64);
    write_varint(&mut out, cols.rune.len() as u64);

    for col in cols
        .fixed
        .iter()
        .chain(cols.stat.iter())
        .chain(cols.rune.iter())
    {
        write_varint(&mut out, col.len() as u64);
    }

    let data_len = cols
        .fixed
        .iter()
        .chain(cols.stat.iter())
        .chain(cols.rune.iter())
        .try_fold(0usize, |acc, col| acc.checked_add(col.len()))
        .context("column data too large")?;
    out.reserve(total_cols.saturating_add(data_len));
    for col in cols
        .fixed
        .iter()
        .chain(cols.stat.iter())
        .chain(cols.rune.iter())
    {
        out.extend_from_slice(col);
    }
    Ok(out)
}

struct Manifest<'a> {
    records: usize,
    stat_cols: usize,
    rune_cols: usize,
    columns: Vec<&'a [u8]>,
}

impl<'a> Manifest<'a> {
    fn parse(payload: &'a [u8]) -> Result<Self> {
        ensure!(
            payload.len() >= MAGIC.len() + 1,
            "truncated columnar header"
        );
        ensure!(&payload[..MAGIC.len()] == MAGIC, "bad columnar magic");
        ensure!(
            payload[MAGIC.len()] == VERSION,
            "unsupported columnar version"
        );

        let mut pos = MAGIC.len() + 1;
        let records = to_usize(
            read_varint_at(payload, &mut pos, "record count")?,
            "records",
        )?;
        let stat_cols = to_usize(
            read_varint_at(payload, &mut pos, "stat column count")?,
            "stat columns",
        )?;
        let rune_cols = to_usize(
            read_varint_at(payload, &mut pos, "rune column count")?,
            "rune columns",
        )?;
        let dynamic_cols = stat_cols
            .checked_add(rune_cols)
            .context("too many dynamic column streams")?;
        let total_cols = NMISC
            .checked_add(dynamic_cols)
            .context("too many column streams")?;
        ensure!(
            total_cols <= payload.len().saturating_sub(pos),
            "truncated column manifest"
        );

        let mut lengths = Vec::with_capacity(total_cols);
        for idx in 0..total_cols {
            let len = to_usize(
                read_varint_at(payload, &mut pos, "column length")?,
                "column length",
            )
            .with_context(|| format!("column {idx} length"))?;
            lengths.push(len);
        }

        let data_start = pos;
        let data_len = lengths
            .iter()
            .try_fold(0usize, |acc, len| acc.checked_add(*len))
            .context("column lengths overflow")?;
        let data_end = data_start
            .checked_add(data_len)
            .context("column lengths overflow")?;
        ensure!(data_end <= payload.len(), "truncated column data");
        ensure!(
            data_end == payload.len(),
            "trailing bytes after column data"
        );

        let mut columns = Vec::with_capacity(total_cols);
        let mut off = data_start;
        for len in lengths {
            let end = off + len;
            columns.push(&payload[off..end]);
            off = end;
        }

        Ok(Self {
            records,
            stat_cols,
            rune_cols,
            columns,
        })
    }
}

struct ColReader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> ColReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn varint(&mut self, name: &str) -> Result<u64> {
        read_varint_at(self.bytes, &mut self.pos, name)
    }

    fn bytes(&mut self, len: usize, name: &str) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(len)
            .with_context(|| format!("byte length overflow in {name}"))?;
        ensure!(end <= self.bytes.len(), "truncated bytes in {name}");
        let out = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(out)
    }
}

struct ColReaders<'a> {
    fixed: Vec<ColReader<'a>>,
    stat: Vec<ColReader<'a>>,
    rune: Vec<ColReader<'a>>,
}

impl<'a> ColReaders<'a> {
    fn new(columns: &[&'a [u8]], stat_cols: usize, rune_cols: usize) -> Result<Self> {
        ensure!(
            columns.len() == NMISC + stat_cols + rune_cols,
            "column manifest/count mismatch"
        );
        let mut fixed = Vec::with_capacity(NMISC);
        for col in &columns[..NMISC] {
            fixed.push(ColReader::new(col));
        }
        let stat_start = NMISC;
        let rune_start = stat_start + stat_cols;
        let stat = columns[stat_start..rune_start]
            .iter()
            .map(|col| ColReader::new(col))
            .collect();
        let rune = columns[rune_start..]
            .iter()
            .map(|col| ColReader::new(col))
            .collect();
        Ok(Self { fixed, stat, rune })
    }

    fn rv(&mut self, col: usize) -> Result<u64> {
        self.fixed[col].varint(FIXED_NAMES[col])
    }

    fn rz(&mut self, col: usize) -> Result<i64> {
        Ok(unzigzag_i64(self.rv(col)?))
    }

    fn rbytes(&mut self, col: usize, len: usize) -> Result<&'a [u8]> {
        self.fixed[col].bytes(len, FIXED_NAMES[col])
    }

    fn stat_z(&mut self, idx: usize) -> Result<i64> {
        let col = self
            .stat
            .get_mut(idx)
            .with_context(|| format!("missing stat column {idx}"))?;
        Ok(unzigzag_i64(col.varint("stat")?))
    }

    fn rune_v(&mut self, idx: usize) -> Result<u64> {
        self.rune
            .get_mut(idx)
            .with_context(|| format!("missing rune column {idx}"))?
            .varint("rune")
    }

    fn ensure_done(&self) -> Result<()> {
        for (idx, col) in self.fixed.iter().enumerate() {
            ensure!(
                col.pos == col.bytes.len(),
                "unconsumed bytes in column {}",
                FIXED_NAMES[idx]
            );
        }
        for (idx, col) in self.stat.iter().enumerate() {
            ensure!(
                col.pos == col.bytes.len(),
                "unconsumed bytes in stat column {idx}"
            );
        }
        for (idx, col) in self.rune.iter().enumerate() {
            ensure!(
                col.pos == col.bytes.len(),
                "unconsumed bytes in rune column {idx}"
            );
        }
        Ok(())
    }
}

fn decode_record(cols: &mut ColReaders<'_>, state: &mut BlockState) -> Result<Vec<u8>> {
    let mut out = Vec::new();

    put_field_varint(
        &mut out,
        1,
        read_u32(cols.rv(SCHEMA)?, "schema_version")? as u64,
    );

    let game_id = add_delta_u64(state.game_id, cols.rz(GID_D)?, "game_id")?;
    state.game_id = game_id;
    put_field_varint(&mut out, 2, game_id);

    let platform_len = to_usize(cols.rv(PLAT_LEN)?, "platform length")?;
    let platform = cols.rbytes(PLAT, platform_len)?;
    put_field_bytes(&mut out, 3, platform);

    put_field_varint(&mut out, 4, read_u32(cols.rv(QUEUE)?, "queue_id")? as u64);

    let game_start_ms = add_delta_i64(state.game_start_ms, cols.rz(START_D)?, "game_start_ms")?;
    state.game_start_ms = game_start_ms;
    put_field_int64(&mut out, 5, game_start_ms);

    put_field_varint(&mut out, 6, read_u32(cols.rv(DUR)?, "duration_s")? as u64);

    let blue_won = cols.rv(BLUE)?;
    ensure!(blue_won <= 1, "blue_won must be 0 or 1");
    put_field_varint(&mut out, 7, blue_won);

    let game_version_len = to_usize(cols.rv(GV_LEN)?, "game_version length")?;
    let game_version = cols.rbytes(GV, game_version_len)?;
    put_field_bytes(&mut out, 8, game_version);

    put_field_varint(&mut out, 9, read_u32(cols.rv(PMAJ)?, "patch_major")? as u64);
    put_field_varint(
        &mut out,
        10,
        read_u32(cols.rv(PMIN)?, "patch_minor")? as u64,
    );

    let n_parts = to_usize(cols.rv(NPARTS)?, "participant count")?;
    for _ in 0..n_parts {
        let mut participant = Vec::new();
        put_field_varint(
            &mut participant,
            1,
            read_u32(cols.rv(P_PLAYER)?, "player_id")? as u64,
        );
        put_field_varint(
            &mut participant,
            2,
            read_u32(cols.rv(P_CHAMP)?, "champion_id")? as u64,
        );
        put_field_varint(
            &mut participant,
            3,
            read_u32(cols.rv(P_POS)?, "position")? as u64,
        );
        put_field_varint(
            &mut participant,
            4,
            read_u32(cols.rv(P_SP1)?, "spell1")? as u64,
        );
        put_field_varint(
            &mut participant,
            5,
            read_u32(cols.rv(P_SP2)?, "spell2")? as u64,
        );

        let n_runes = to_usize(cols.rv(P_RUNES_N)?, "rune count")?;
        let mut packed = Vec::new();
        for idx in 0..n_runes {
            write_varint(&mut packed, read_u32(cols.rune_v(idx)?, "rune")? as u64);
        }
        put_field_packed_body(&mut participant, 6, &packed);

        let n_stats = to_usize(cols.rv(P_STATS_N)?, "stat count")?;
        packed.clear();
        for idx in 0..n_stats {
            write_varint(&mut packed, zigzag_i64(cols.stat_z(idx)?));
        }
        put_field_packed_body(&mut participant, 7, &packed);

        put_field_message(&mut out, 11, &participant);
    }

    let tl_present = cols.rv(TL_PRESENT)?;
    ensure!(tl_present <= 1, "tl_present must be 0 or 1");
    if tl_present == 1 {
        let timeline = decode_timeline(cols)?;
        put_field_message(&mut out, 12, &timeline);
    }

    let n_bans = to_usize(cols.rv(BANS_N)?, "ban count")?;
    let mut bans = Vec::new();
    for _ in 0..n_bans {
        write_varint(&mut bans, read_u32(cols.rv(BANS)?, "ban")? as u64);
    }
    // Prost writes field 13 after the optional timeline, and the Python
    // executable spec matches that ordering.
    put_field_packed_body(&mut out, 13, &bans);

    Ok(out)
}

fn decode_timeline(cols: &mut ColReaders<'_>) -> Result<Vec<u8>> {
    let mut timeline = Vec::new();
    let mut prev_series: [Vec<u32>; 4] = std::array::from_fn(|_| Vec::new());

    let n_frames = to_usize(cols.rv(NFRAMES)?, "frame count")?;
    for _ in 0..n_frames {
        let mut frame = Vec::new();
        put_field_varint(
            &mut frame,
            1,
            read_u32(cols.rv(F_MINUTE)?, "minute")? as u64,
        );

        let series = [
            (2u32, F_GOLD_N, F_GOLD, "gold"),
            (3, F_XP_N, F_XP, "xp"),
            (4, F_CS_N, F_CS, "cs"),
            (5, F_DMG_N, F_DMG, "damage"),
        ];
        for (idx, (field, n_col, v_col, name)) in series.into_iter().enumerate() {
            let n_values = to_usize(cols.rv(n_col)?, name)?;
            let mut values = Vec::new();
            let mut packed = Vec::new();
            for slot in 0..n_values {
                let prev = prev_series[idx].get(slot).copied().unwrap_or(0);
                let value = add_delta_u32(prev, cols.rz(v_col)?, name)?;
                write_varint(&mut packed, value as u64);
                values.push(value);
            }
            prev_series[idx] = values;
            put_field_packed_body(&mut frame, field, &packed);
        }

        put_field_message(&mut timeline, 1, &frame);
    }

    let n_kills = to_usize(cols.rv(NKILLS)?, "kill count")?;
    let mut prev_t = 0u32;
    for _ in 0..n_kills {
        let mut kill = Vec::new();
        prev_t = add_delta_u32(prev_t, cols.rz(K_T)?, "kill timestamp")?;
        put_field_varint(&mut kill, 1, prev_t as u64);
        put_field_varint(&mut kill, 2, read_u32(cols.rv(K_KILLER)?, "killer")? as u64);
        put_field_varint(&mut kill, 3, read_u32(cols.rv(K_VICTIM)?, "victim")? as u64);
        put_field_varint(
            &mut kill,
            4,
            read_u32(cols.rv(K_MASK)?, "assist_mask")? as u64,
        );
        put_field_varint(&mut kill, 5, read_u32(cols.rv(K_X)?, "kill x")? as u64);
        put_field_varint(&mut kill, 6, read_u32(cols.rv(K_Y)?, "kill y")? as u64);
        put_field_message(&mut timeline, 2, &kill);
    }

    let n_objs = to_usize(cols.rv(NOBJS)?, "objective count")?;
    prev_t = 0;
    for _ in 0..n_objs {
        let mut obj = Vec::new();
        prev_t = add_delta_u32(prev_t, cols.rz(O_T)?, "objective timestamp")?;
        put_field_varint(&mut obj, 1, prev_t as u64);
        put_field_varint(
            &mut obj,
            2,
            read_u32(cols.rv(O_KIND)?, "objective kind")? as u64,
        );
        put_field_varint(
            &mut obj,
            3,
            read_u32(cols.rv(O_KILLER)?, "objective killer")? as u64,
        );
        put_field_varint(
            &mut obj,
            4,
            read_u32(cols.rv(O_TEAM)?, "objective team")? as u64,
        );
        put_field_varint(
            &mut obj,
            5,
            read_u32(cols.rv(O_LANE)?, "objective lane")? as u64,
        );
        put_field_message(&mut timeline, 3, &obj);
    }

    let n_wards = to_usize(cols.rv(NWARDS)?, "ward count")?;
    prev_t = 0;
    for _ in 0..n_wards {
        let mut ward = Vec::new();
        prev_t = add_delta_u32(prev_t, cols.rz(W_T)?, "ward timestamp")?;
        put_field_varint(&mut ward, 1, prev_t as u64);
        put_field_varint(
            &mut ward,
            2,
            read_u32(cols.rv(W_KIND)?, "ward kind")? as u64,
        );
        put_field_varint(
            &mut ward,
            3,
            read_u32(cols.rv(W_PART)?, "ward participant")? as u64,
        );
        put_field_message(&mut timeline, 4, &ward);
    }

    Ok(timeline)
}

fn put_field_varint(out: &mut Vec<u8>, field: u32, value: u64) {
    if value == 0 {
        return;
    }
    write_varint(out, (field as u64) << 3);
    write_varint(out, value);
}

fn put_field_int64(out: &mut Vec<u8>, field: u32, value: i64) {
    if value == 0 {
        return;
    }
    write_varint(out, (field as u64) << 3);
    write_varint(out, value as u64);
}

fn put_field_bytes(out: &mut Vec<u8>, field: u32, bytes: &[u8]) {
    if bytes.is_empty() {
        return;
    }
    write_varint(out, ((field as u64) << 3) | 2);
    write_varint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

fn put_field_message(out: &mut Vec<u8>, field: u32, bytes: &[u8]) {
    write_varint(out, ((field as u64) << 3) | 2);
    write_varint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

fn put_field_packed_body(out: &mut Vec<u8>, field: u32, body: &[u8]) {
    put_field_bytes(out, field, body);
}

fn write_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn read_varint_at(bytes: &[u8], pos: &mut usize, name: &str) -> Result<u64> {
    let mut out = 0u64;
    for idx in 0..10 {
        ensure!(*pos < bytes.len(), "truncated varint in {name}");
        let byte = bytes[*pos];
        *pos += 1;
        if idx == 9 && (byte & 0xfe) != 0 {
            bail!("varint overflow in {name}");
        }
        out |= ((byte & 0x7f) as u64) << (idx * 7);
        if byte & 0x80 == 0 {
            return Ok(out);
        }
    }
    bail!("varint too long in {name}");
}

fn zigzag_i64(value: i64) -> u64 {
    ((value as u64) << 1) ^ ((value >> 63) as u64)
}

fn unzigzag_i64(value: u64) -> i64 {
    ((value >> 1) as i64) ^ -((value & 1) as i64)
}

fn to_usize(value: u64, name: &str) -> Result<usize> {
    usize::try_from(value).map_err(|_| anyhow!("{name} does not fit in usize"))
}

fn read_u32(value: u64, name: &str) -> Result<u32> {
    u32::try_from(value).map_err(|_| anyhow!("{name} exceeds u32 range"))
}

fn delta_u64(value: u64, prev: u64, name: &str) -> Result<i64> {
    let delta = value as i128 - prev as i128;
    i64::try_from(delta).map_err(|_| anyhow!("{name} delta exceeds i64 range"))
}

fn add_delta_u64(prev: u64, delta: i64, name: &str) -> Result<u64> {
    let value = prev as i128 + delta as i128;
    ensure!(
        (0..=u64::MAX as i128).contains(&value),
        "{name} delta reconstructs outside u64 range"
    );
    Ok(value as u64)
}

fn delta_i64(value: i64, prev: i64, name: &str) -> Result<i64> {
    let delta = value as i128 - prev as i128;
    i64::try_from(delta).map_err(|_| anyhow!("{name} delta exceeds i64 range"))
}

fn add_delta_i64(prev: i64, delta: i64, name: &str) -> Result<i64> {
    let value = prev as i128 + delta as i128;
    ensure!(
        (i64::MIN as i128..=i64::MAX as i128).contains(&value),
        "{name} delta reconstructs outside i64 range"
    );
    Ok(value as i64)
}

fn add_delta_u32(prev: u32, delta: i64, name: &str) -> Result<u32> {
    let value = prev as i128 + delta as i128;
    ensure!(
        (0..=u32::MAX as i128).contains(&value),
        "{name} delta reconstructs outside u32 range"
    );
    Ok(value as u32)
}

fn mismatch_error(idx: usize, want: &[u8], got: &[u8]) -> String {
    let first_diff = want
        .iter()
        .zip(got.iter())
        .position(|(a, b)| a != b)
        .or_else(|| (want.len() != got.len()).then_some(want.len().min(got.len())));
    match first_diff {
        Some(pos) => format!(
            "columnar self-check mismatch at record {idx}: input {} bytes decoded {} bytes first_diff={pos}",
            want.len(),
            got.len()
        ),
        None => format!("columnar self-check mismatch at record {idx}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{
        KillEvent, MinuteFrame, ObjectiveEvent, Participant, TimelineLite, WardEvent, parse_match,
    };

    fn fixture(name: &str) -> String {
        std::fs::read_to_string(format!("testdata/{name}")).unwrap()
    }

    fn encode_records(records: &[MatchRecord]) -> Vec<Vec<u8>> {
        records.iter().map(Message::encode_to_vec).collect()
    }

    fn assert_block_roundtrip(records: &[Vec<u8>]) {
        let payload = encode_block(records).unwrap();
        let decoded = decode_block(&payload).unwrap();
        assert_eq!(decoded, records);
    }

    #[test]
    fn real_record_variations_roundtrip() {
        let match_json = fixture("match.json");
        let timeline_json = fixture("timeline.json");
        let base = parse_match(&match_json, Some(&timeline_json))
            .unwrap()
            .record;

        let mut lower_ids = base.clone();
        lower_ids.game_id -= 10;
        lower_ids.game_start_ms -= 60_000;

        let mut no_timeline = base.clone();
        no_timeline.timeline = None;

        let mut empty_timeline = base.clone();
        empty_timeline.timeline = Some(TimelineLite::default());

        let mut empty_strings = base.clone();
        empty_strings.platform.clear();
        empty_strings.game_version.clear();

        let records =
            encode_records(&[base, lower_ids, no_timeline, empty_timeline, empty_strings]);
        assert_block_roundtrip(&records);
    }

    #[test]
    fn edge_cases_roundtrip() {
        assert_block_roundtrip(&[]);

        let mut none_timeline = MatchRecord {
            schema_version: 1,
            game_id: 100,
            platform: String::new(),
            queue_id: 0,
            game_start_ms: 1_000,
            duration_s: 0,
            blue_won: false,
            game_version: String::new(),
            patch_major: 0,
            patch_minor: 0,
            participants: vec![
                Participant::default(),
                Participant {
                    player_id: 7,
                    champion_id: 266,
                    position: 1,
                    spell1: 4,
                    spell2: 12,
                    runes: Vec::new(),
                    stats: Vec::new(),
                },
            ],
            timeline: None,
            bans: Vec::new(),
        };
        let mut present_empty_timeline = none_timeline.clone();
        present_empty_timeline.timeline = Some(TimelineLite::default());
        assert_ne!(
            none_timeline.encode_to_vec(),
            present_empty_timeline.encode_to_vec()
        );

        let timeline = TimelineLite {
            frames: vec![
                MinuteFrame {
                    minute: 0,
                    total_gold: vec![500],
                    xp: Vec::new(),
                    cs: vec![3, 2],
                    dmg_to_champs: vec![0],
                },
                MinuteFrame {
                    minute: 2,
                    total_gold: vec![450, 600],
                    xp: vec![12],
                    cs: Vec::new(),
                    dmg_to_champs: vec![5, 1, 9],
                },
            ],
            kills: vec![
                KillEvent {
                    t_s: 10,
                    killer: 1,
                    victim: 0,
                    assist_mask: 3,
                    x: 100,
                    y: 200,
                },
                KillEvent {
                    t_s: 8,
                    killer: 0,
                    victim: 1,
                    assist_mask: 0,
                    x: 50,
                    y: 75,
                },
            ],
            objectives: vec![
                ObjectiveEvent {
                    t_s: 20,
                    kind: 7,
                    killer: 255,
                    losing_team: 200,
                    lane: 2,
                },
                ObjectiveEvent {
                    t_s: 15,
                    kind: 1,
                    killer: 1,
                    losing_team: 0,
                    lane: 0,
                },
            ],
            wards: vec![
                WardEvent {
                    t_s: 3,
                    kind: 1,
                    participant: 0,
                },
                WardEvent {
                    t_s: 1,
                    kind: 2,
                    participant: 1,
                },
            ],
        };

        let mut rich = none_timeline.clone();
        rich.game_id = 90;
        rich.game_start_ms = 900;
        rich.blue_won = true;
        rich.participants[0].runes = vec![8005, 9111, 9104];
        rich.participants[0].stats = vec![-5, 0, 17];
        rich.participants[1].runes = vec![8112];
        rich.participants[1].stats = vec![9];
        rich.timeline = Some(timeline);

        none_timeline.game_id = 110;
        let records = encode_records(&[none_timeline, present_empty_timeline, rich.clone(), rich]);
        assert_block_roundtrip(&records[..1]);
        assert_block_roundtrip(&records);
    }

    #[test]
    fn malformed_payloads_error() {
        assert!(decode_block(&[]).is_err());
        assert!(decode_block(b"LCOL").is_err());

        let empty = encode_block(&[]).unwrap();
        assert!(decode_block(&empty[..empty.len() - 1]).is_err());

        let one = encode_block(&[MatchRecord::default().encode_to_vec()]).unwrap();
        assert!(decode_block(&one[..one.len() - 1]).is_err());

        let mut huge_len = Vec::new();
        huge_len.extend_from_slice(MAGIC);
        huge_len.push(VERSION);
        write_varint(&mut huge_len, 0);
        write_varint(&mut huge_len, 0);
        write_varint(&mut huge_len, 0);
        write_varint(&mut huge_len, u64::MAX);
        for _ in 1..NMISC {
            write_varint(&mut huge_len, 0);
        }
        assert!(decode_block(&huge_len).is_err());

        let mut huge_columns = Vec::new();
        huge_columns.extend_from_slice(MAGIC);
        huge_columns.push(VERSION);
        write_varint(&mut huge_columns, 0);
        write_varint(&mut huge_columns, u64::MAX);
        write_varint(&mut huge_columns, 0);
        assert!(decode_block(&huge_columns).is_err());
    }

    #[test]
    fn encode_self_check_rejects_unknown_fields() {
        let mut raw = MatchRecord::default().encode_to_vec();
        write_varint(&mut raw, 99 << 3);
        write_varint(&mut raw, 1);
        assert!(encode_block(&[raw]).is_err());
    }

    #[test]
    #[ignore]
    fn real_segment_roundtrip() -> Result<()> {
        let Ok(path) = std::env::var("COLUMNAR_SEG_PATH") else {
            eprintln!("COLUMNAR_SEG_PATH unset; skipping real_segment_roundtrip");
            return Ok(());
        };

        let records = crate::storage::read_segment_records(std::path::Path::new(&path))?;
        for (idx, chunk) in records.chunks(4096).enumerate() {
            let input: Vec<Vec<u8>> = chunk.to_vec();
            let payload = encode_block(&input).with_context(|| format!("encode chunk {idx}"))?;
            let decoded = decode_block(&payload).with_context(|| format!("decode chunk {idx}"))?;
            assert_eq!(decoded, input, "chunk {idx}");
        }
        Ok(())
    }
}
