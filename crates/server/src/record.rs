//! MatchRecord: the lossless, compact, protobuf-encoded match representation.
//!
//! Wire format is plain protobuf (hand-derived via prost, mirrored for
//! reference in `matchrecord.proto`). Participant stats are stored as a
//! varint array whose field order is pinned by `STAT_FIELDS_V1` and
//! `schema_version` — append-only, never reorder.

use anyhow::{Context, Result, bail};
use serde_json::Value;

pub const SCHEMA_VERSION: u32 = 1;

/// Participant stat fields, in stat-array order. APPEND ONLY — never reorder
/// or remove; bump SCHEMA_VERSION when appending. Booleans stored as 0/1,
/// missing fields as 0.
pub const STAT_FIELDS_V1: &[&str] = &[
    "assists",
    "baronKills",
    "bountyLevel",
    "champExperience",
    "champLevel",
    "championTransform",
    "consumablesPurchased",
    "damageDealtToBuildings",
    "damageDealtToObjectives",
    "damageDealtToTurrets",
    "damageSelfMitigated",
    "deaths",
    "detectorWardsPlaced",
    "doubleKills",
    "dragonKills",
    "firstBloodAssist",
    "firstBloodKill",
    "firstTowerAssist",
    "firstTowerKill",
    "gameEndedInEarlySurrender",
    "gameEndedInSurrender",
    "goldEarned",
    "goldSpent",
    "inhibitorKills",
    "inhibitorTakedowns",
    "inhibitorsLost",
    "item0",
    "item1",
    "item2",
    "item3",
    "item4",
    "item5",
    "item6",
    "itemsPurchased",
    "killingSprees",
    "kills",
    "largestCriticalStrike",
    "largestKillingSpree",
    "largestMultiKill",
    "longestTimeSpentLiving",
    "magicDamageDealt",
    "magicDamageDealtToChampions",
    "magicDamageTaken",
    "neutralMinionsKilled",
    "nexusKills",
    "nexusLost",
    "nexusTakedowns",
    "objectivesStolen",
    "objectivesStolenAssists",
    "pentaKills",
    "physicalDamageDealt",
    "physicalDamageDealtToChampions",
    "physicalDamageTaken",
    "quadraKills",
    "sightWardsBoughtInGame",
    "spell1Casts",
    "spell2Casts",
    "spell3Casts",
    "spell4Casts",
    "summoner1Casts",
    "summoner2Casts",
    "summonerLevel",
    "teamEarlySurrendered",
    "timeCCingOthers",
    "timePlayed",
    "totalAllyJungleMinionsKilled",
    "totalDamageDealt",
    "totalDamageDealtToChampions",
    "totalDamageShieldedOnTeammates",
    "totalDamageTaken",
    "totalEnemyJungleMinionsKilled",
    "totalHeal",
    "totalHealsOnTeammates",
    "totalMinionsKilled",
    "totalTimeCCDealt",
    "totalTimeSpentDead",
    "totalUnitsHealed",
    "tripleKills",
    "trueDamageDealt",
    "trueDamageDealtToChampions",
    "trueDamageTaken",
    "turretKills",
    "turretTakedowns",
    "turretsLost",
    "unrealKills",
    "visionScore",
    "visionWardsBoughtInGame",
    "wardsKilled",
    "wardsPlaced",
    "win",
];

pub const POSITIONS: &[&str] = &["TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"];

#[derive(Clone, PartialEq, prost::Message)]
pub struct MatchRecord {
    #[prost(uint32, tag = "1")]
    pub schema_version: u32,
    /// Numeric part of the match id; the platform is the shard.
    #[prost(uint64, tag = "2")]
    pub game_id: u64,
    #[prost(string, tag = "3")]
    pub platform: String,
    #[prost(uint32, tag = "4")]
    pub queue_id: u32,
    #[prost(int64, tag = "5")]
    pub game_start_ms: i64,
    #[prost(uint32, tag = "6")]
    pub duration_s: u32,
    #[prost(bool, tag = "7")]
    pub blue_won: bool,
    #[prost(string, tag = "8")]
    pub game_version: String,
    #[prost(uint32, tag = "9")]
    pub patch_major: u32,
    #[prost(uint32, tag = "10")]
    pub patch_minor: u32,
    /// Exactly 10: blue team (positions TOP..UTILITY) then red team.
    #[prost(message, repeated, tag = "11")]
    pub participants: Vec<Participant>,
    #[prost(message, optional, tag = "12")]
    pub timeline: Option<TimelineLite>,
    /// 10 banned champion ids (0 = no ban), pick-turn order.
    #[prost(uint32, repeated, tag = "13")]
    pub bans: Vec<u32>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Participant {
    /// Dictionary-encoded puuid (players table).
    #[prost(uint32, tag = "1")]
    pub player_id: u32,
    #[prost(uint32, tag = "2")]
    pub champion_id: u32,
    /// 0..4 = TOP..UTILITY (index into POSITIONS).
    #[prost(uint32, tag = "3")]
    pub position: u32,
    #[prost(uint32, tag = "4")]
    pub spell1: u32,
    #[prost(uint32, tag = "5")]
    pub spell2: u32,
    /// [primaryStyle, subStyle, perk x6, statPerk x3]
    #[prost(uint32, repeated, tag = "6")]
    pub runes: Vec<u32>,
    /// Values for STAT_FIELDS of this record's schema_version.
    #[prost(sint64, repeated, tag = "7")]
    pub stats: Vec<i64>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct TimelineLite {
    #[prost(message, repeated, tag = "1")]
    pub frames: Vec<MinuteFrame>,
    #[prost(message, repeated, tag = "2")]
    pub kills: Vec<KillEvent>,
    #[prost(message, repeated, tag = "3")]
    pub objectives: Vec<ObjectiveEvent>,
    #[prost(message, repeated, tag = "4")]
    pub wards: Vec<WardEvent>,
}

/// Per-minute snapshot; each vec has exactly 10 entries (participant order).
#[derive(Clone, PartialEq, prost::Message)]
pub struct MinuteFrame {
    #[prost(uint32, tag = "1")]
    pub minute: u32,
    #[prost(uint32, repeated, tag = "2")]
    pub total_gold: Vec<u32>,
    #[prost(uint32, repeated, tag = "3")]
    pub xp: Vec<u32>,
    /// minionsKilled + jungleMinionsKilled
    #[prost(uint32, repeated, tag = "4")]
    pub cs: Vec<u32>,
    #[prost(uint32, repeated, tag = "5")]
    pub dmg_to_champs: Vec<u32>,
}

/// Participant indices are 0..9; 255 = none (e.g. executed by minions).
#[derive(Clone, PartialEq, prost::Message)]
pub struct KillEvent {
    #[prost(uint32, tag = "1")]
    pub t_s: u32,
    #[prost(uint32, tag = "2")]
    pub killer: u32,
    #[prost(uint32, tag = "3")]
    pub victim: u32,
    /// Bit i set = participant i assisted.
    #[prost(uint32, tag = "4")]
    pub assist_mask: u32,
    #[prost(uint32, tag = "5")]
    pub x: u32,
    #[prost(uint32, tag = "6")]
    pub y: u32,
}

pub mod objective_kind {
    pub const DRAGON: u32 = 1;
    pub const RIFT_HERALD: u32 = 2;
    pub const BARON: u32 = 3;
    pub const HORDE: u32 = 4; // void grubs
    pub const ELDER_DRAGON: u32 = 5;
    pub const ATAKHAN: u32 = 6;
    pub const TOWER: u32 = 7;
    pub const INHIBITOR: u32 = 8;
    pub const PLATE: u32 = 9;
    pub const OTHER_MONSTER: u32 = 20;
    pub const OTHER_BUILDING: u32 = 21;
}

pub mod lane {
    pub const NONE: u32 = 0;
    pub const TOP: u32 = 1;
    pub const MID: u32 = 2;
    pub const BOT: u32 = 3;
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ObjectiveEvent {
    #[prost(uint32, tag = "1")]
    pub t_s: u32,
    /// See [`objective_kind`].
    #[prost(uint32, tag = "2")]
    pub kind: u32,
    /// Killer participant 0..9, 255 = none/unknown.
    #[prost(uint32, tag = "3")]
    pub killer: u32,
    /// For buildings/plates: the team that LOST it (100/200). 0 otherwise.
    #[prost(uint32, tag = "4")]
    pub losing_team: u32,
    /// See [`lane`]; only for buildings/plates.
    #[prost(uint32, tag = "5")]
    pub lane: u32,
}

pub mod ward_kind {
    pub const PLACED: u32 = 1;
    pub const KILLED: u32 = 2;
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct WardEvent {
    #[prost(uint32, tag = "1")]
    pub t_s: u32,
    /// See [`ward_kind`].
    #[prost(uint32, tag = "2")]
    pub kind: u32,
    /// Creator (PLACED) or killer (KILLED), 0..9; 255 = unknown.
    #[prost(uint32, tag = "3")]
    pub participant: u32,
}

pub struct ParsedMatch {
    pub record: MatchRecord,
    /// Puuids in participant order (blue TOP..red UTILITY), for player-id assignment.
    pub puuids: Vec<String>,
}

/// Splits "EUW1_7819712430" into (platform, numeric id).
pub fn split_match_id(match_id: &str) -> Result<(&str, u64)> {
    let (platform, num) = match_id
        .split_once('_')
        .with_context(|| format!("bad match id {match_id}"))?;
    Ok((platform, num.parse()?))
}

fn i(v: &Value, key: &str) -> i64 {
    match v.get(key) {
        Some(Value::Number(n)) => n.as_i64().unwrap_or(0),
        Some(Value::Bool(b)) => *b as i64,
        _ => 0,
    }
}

fn s<'a>(v: &'a Value, key: &str) -> &'a str {
    v.get(key).and_then(Value::as_str).unwrap_or("")
}

/// Parses match + timeline API JSON into a MatchRecord.
/// `player_ids` are filled in later by the caller (storage assigns them);
/// here participants carry index-order and the puuids are returned alongside.
pub fn parse_match(match_json: &str, timeline_json: Option<&str>) -> Result<ParsedMatch> {
    let m: Value = serde_json::from_str(match_json)?;
    let info = m.get("info").context("no info")?;
    let metadata = m.get("metadata").context("no metadata")?;

    let match_id = s(metadata, "matchId");
    let (platform, game_id) = split_match_id(match_id)?;

    let participants_json = info
        .get("participants")
        .and_then(Value::as_array)
        .context("no participants")?;
    if participants_json.len() != 10 {
        bail!("{} participants, want 10", participants_json.len());
    }

    // gameDuration is seconds in modern payloads; ms in some old ones.
    let mut duration = i(info, "gameDuration");
    if duration > 30_000 {
        duration /= 1000;
    }
    let game_start_ms = {
        let ts = i(info, "gameStartTimestamp");
        if ts > 0 { ts } else { i(info, "gameCreation") }
    };
    let game_version = s(info, "gameVersion").to_string();
    let mut ver_parts = game_version.split('.');
    let patch_major: u32 = ver_parts.next().unwrap_or("0").parse().unwrap_or(0);
    let patch_minor: u32 = ver_parts.next().unwrap_or("0").parse().unwrap_or(0);

    let teams = info.get("teams").and_then(Value::as_array);
    let blue_won = teams
        .and_then(|ts| ts.iter().find(|t| i(t, "teamId") == 100))
        .map(|t| t.get("win").and_then(Value::as_bool).unwrap_or(false))
        .unwrap_or(false);
    let bans: Vec<u32> = teams
        .map(|ts| {
            ts.iter()
                .flat_map(|t| {
                    t.get("bans")
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default()
                })
                .map(|b| i(&b, "championId").max(0) as u32)
                .collect()
        })
        .unwrap_or_default();

    // Order participants blue-then-red, TOP..UTILITY within each team,
    // repairing missing teamPosition into the unused slot.
    let mut ordered: Vec<(usize, &Value)> = Vec::with_capacity(10);
    for team in 0..2 {
        let members = &participants_json[team * 5..(team + 1) * 5];
        for p in members {
            if i(p, "teamId") != if team == 0 { 100 } else { 200 } {
                bail!("participants not grouped by team");
            }
        }
        let mut slot_of: Vec<Option<usize>> = vec![None; 5];
        let mut unplaced: Vec<usize> = Vec::new();
        for (idx, p) in members.iter().enumerate() {
            match POSITIONS.iter().position(|pos| *pos == s(p, "teamPosition")) {
                Some(slot) if slot_of[slot].is_none() => slot_of[slot] = Some(idx),
                _ => unplaced.push(idx),
            }
        }
        for slot in 0..5 {
            if slot_of[slot].is_none() {
                slot_of[slot] = unplaced.pop();
            }
        }
        for slot in 0..5 {
            let idx = slot_of[slot].context("position repair failed")?;
            ordered.push((slot, &members[idx]));
        }
    }

    let mut participants = Vec::with_capacity(10);
    let mut puuids = Vec::with_capacity(10);
    for (slot, p) in &ordered {
        puuids.push(s(p, "puuid").to_string());

        let mut runes: Vec<u32> = Vec::with_capacity(11);
        if let Some(styles) = p
            .get("perks")
            .and_then(|pk| pk.get("styles"))
            .and_then(Value::as_array)
        {
            for style in styles {
                runes.push(i(style, "style").max(0) as u32);
            }
            for style in styles {
                if let Some(sels) = style.get("selections").and_then(Value::as_array) {
                    for sel in sels {
                        runes.push(i(sel, "perk").max(0) as u32);
                    }
                }
            }
        }
        if let Some(stat_perks) = p.get("perks").and_then(|pk| pk.get("statPerks")) {
            for k in ["offense", "flex", "defense"] {
                runes.push(i(stat_perks, k).max(0) as u32);
            }
        }

        participants.push(Participant {
            player_id: 0, // assigned by storage
            champion_id: i(p, "championId").max(0) as u32,
            position: *slot as u32,
            spell1: i(p, "summoner1Id").max(0) as u32,
            spell2: i(p, "summoner2Id").max(0) as u32,
            runes,
            stats: STAT_FIELDS_V1.iter().map(|f| i(p, f)).collect(),
        });
    }

    // The timeline (and its events) index participants 1..10 in ORIGINAL
    // order; map original index -> our position-sorted index.
    let mut orig_to_sorted = [0usize; 10];
    for (sorted_idx, (_, p)) in ordered.iter().enumerate() {
        let orig = participants_json
            .iter()
            .position(|q| std::ptr::eq(*p, q))
            .unwrap();
        orig_to_sorted[orig] = sorted_idx;
    }

    let timeline = match timeline_json {
        Some(tj) => Some(parse_timeline(tj, &orig_to_sorted)?),
        None => None,
    };

    Ok(ParsedMatch {
        record: MatchRecord {
            schema_version: SCHEMA_VERSION,
            game_id,
            platform: platform.to_string(),
            queue_id: i(info, "queueId").max(0) as u32,
            game_start_ms,
            duration_s: duration.max(0) as u32,
            blue_won,
            game_version,
            patch_major,
            patch_minor,
            participants,
            timeline,
            bans,
        },
        puuids,
    })
}

/// `pid` in events is 1-based original participant id; returns sorted 0..9 or 255.
fn map_pid(orig_to_sorted: &[usize; 10], pid: i64) -> u32 {
    if (1..=10).contains(&pid) {
        orig_to_sorted[(pid - 1) as usize] as u32
    } else {
        255
    }
}

fn parse_timeline(timeline_json: &str, orig_to_sorted: &[usize; 10]) -> Result<TimelineLite> {
    let t: Value = serde_json::from_str(timeline_json)?;
    let frames_json = t
        .get("info")
        .and_then(|x| x.get("frames"))
        .and_then(Value::as_array)
        .context("no timeline frames")?;

    let mut tl = TimelineLite::default();

    for (minute, frame) in frames_json.iter().enumerate() {
        let mut mf = MinuteFrame {
            minute: minute as u32,
            total_gold: vec![0; 10],
            xp: vec![0; 10],
            cs: vec![0; 10],
            dmg_to_champs: vec![0; 10],
        };
        if let Some(pframes) = frame.get("participantFrames").and_then(Value::as_object) {
            for (key, pf) in pframes {
                let orig: i64 = key.parse().unwrap_or(0);
                let idx = map_pid(orig_to_sorted, orig);
                if idx == 255 {
                    continue;
                }
                let idx = idx as usize;
                mf.total_gold[idx] = i(pf, "totalGold").max(0) as u32;
                mf.xp[idx] = i(pf, "xp").max(0) as u32;
                mf.cs[idx] =
                    (i(pf, "minionsKilled") + i(pf, "jungleMinionsKilled")).max(0) as u32;
                mf.dmg_to_champs[idx] = pf
                    .get("damageStats")
                    .map(|d| i(d, "totalDamageDoneToChampions"))
                    .unwrap_or(0)
                    .max(0) as u32;
            }
        }
        tl.frames.push(mf);

        let Some(events) = frame.get("events").and_then(Value::as_array) else {
            continue;
        };
        for e in events {
            let t_s = (i(e, "timestamp") / 1000).max(0) as u32;
            match s(e, "type") {
                "CHAMPION_KILL" => {
                    let mut assist_mask = 0u32;
                    if let Some(assists) =
                        e.get("assistingParticipantIds").and_then(Value::as_array)
                    {
                        for a in assists {
                            let idx = map_pid(orig_to_sorted, a.as_i64().unwrap_or(0));
                            if idx < 10 {
                                assist_mask |= 1 << idx;
                            }
                        }
                    }
                    let pos = e.get("position");
                    tl.kills.push(KillEvent {
                        t_s,
                        killer: map_pid(orig_to_sorted, i(e, "killerId")),
                        victim: map_pid(orig_to_sorted, i(e, "victimId")),
                        assist_mask,
                        x: pos.map(|p| i(p, "x")).unwrap_or(0).max(0) as u32,
                        y: pos.map(|p| i(p, "y")).unwrap_or(0).max(0) as u32,
                    });
                }
                "ELITE_MONSTER_KILL" => {
                    let kind = match s(e, "monsterType") {
                        "DRAGON" => {
                            if s(e, "monsterSubType") == "ELDER_DRAGON" {
                                objective_kind::ELDER_DRAGON
                            } else {
                                objective_kind::DRAGON
                            }
                        }
                        "RIFTHERALD" => objective_kind::RIFT_HERALD,
                        "BARON_NASHOR" => objective_kind::BARON,
                        "HORDE" => objective_kind::HORDE,
                        "ATAKHAN" => objective_kind::ATAKHAN,
                        _ => objective_kind::OTHER_MONSTER,
                    };
                    tl.objectives.push(ObjectiveEvent {
                        t_s,
                        kind,
                        killer: map_pid(orig_to_sorted, i(e, "killerId")),
                        losing_team: 0,
                        lane: lane::NONE,
                    });
                }
                "BUILDING_KILL" => {
                    let kind = match s(e, "buildingType") {
                        "TOWER_BUILDING" => objective_kind::TOWER,
                        "INHIBITOR_BUILDING" => objective_kind::INHIBITOR,
                        _ => objective_kind::OTHER_BUILDING,
                    };
                    tl.objectives.push(ObjectiveEvent {
                        t_s,
                        kind,
                        killer: map_pid(orig_to_sorted, i(e, "killerId")),
                        losing_team: i(e, "teamId").max(0) as u32,
                        lane: parse_lane(s(e, "laneType")),
                    });
                }
                "TURRET_PLATE_DESTROYED" => {
                    tl.objectives.push(ObjectiveEvent {
                        t_s,
                        kind: objective_kind::PLATE,
                        killer: 255,
                        losing_team: i(e, "teamId").max(0) as u32,
                        lane: parse_lane(s(e, "laneType")),
                    });
                }
                "WARD_PLACED" => {
                    tl.wards.push(WardEvent {
                        t_s,
                        kind: ward_kind::PLACED,
                        participant: map_pid(orig_to_sorted, i(e, "creatorId")),
                    });
                }
                "WARD_KILL" => {
                    tl.wards.push(WardEvent {
                        t_s,
                        kind: ward_kind::KILLED,
                        participant: map_pid(orig_to_sorted, i(e, "killerId")),
                    });
                }
                _ => {}
            }
        }
    }

    Ok(tl)
}

fn parse_lane(lane_type: &str) -> u32 {
    match lane_type {
        "TOP_LANE" => lane::TOP,
        "MID_LANE" => lane::MID,
        "BOT_LANE" => lane::BOT,
        _ => lane::NONE,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    fn fixture(name: &str) -> String {
        std::fs::read_to_string(format!("testdata/{name}")).unwrap()
    }

    #[test]
    fn parses_real_match() {
        let match_json = fixture("match.json");
        let timeline_json = fixture("timeline.json");
        let parsed = parse_match(&match_json, Some(&timeline_json)).unwrap();
        let r = &parsed.record;

        assert_eq!(r.schema_version, SCHEMA_VERSION);
        assert_eq!(r.game_id, 7819712430);
        assert_eq!(r.platform, "EUW1");
        assert_eq!(r.queue_id, 420);
        assert_eq!(r.duration_s, 2130);
        assert_eq!(r.patch_major, 16);
        assert_eq!(r.patch_minor, 7);
        assert_eq!(r.participants.len(), 10);
        assert_eq!(parsed.puuids.len(), 10);
        assert!(parsed.puuids.iter().all(|p| p.len() > 70));
        // Positions: TOP..UTILITY per team.
        for (i, p) in r.participants.iter().enumerate() {
            assert_eq!(p.position, (i % 5) as u32, "participant {i}");
            assert_eq!(p.stats.len(), STAT_FIELDS_V1.len());
            assert!(p.champion_id > 0);
        }
        // Stats round-trip: sum of "kills" (index in schema) is plausible.
        let kills_idx = STAT_FIELDS_V1.iter().position(|f| *f == "kills").unwrap();
        let total_kills: i64 = r.participants.iter().map(|p| p.stats[kills_idx]).sum();
        assert!(total_kills > 0 && total_kills < 200, "total kills {total_kills}");
        // win flag consistent with blue_won
        let win_idx = STAT_FIELDS_V1.iter().position(|f| *f == "win").unwrap();
        assert_eq!(r.participants[0].stats[win_idx] == 1, r.blue_won);

        let tl = r.timeline.as_ref().unwrap();
        assert_eq!(tl.frames.len(), 37);
        assert!(tl.frames.iter().all(|f| f.total_gold.len() == 10));
        // Gold monotonically grows for participant 0.
        let g0: Vec<u32> = tl.frames.iter().map(|f| f.total_gold[0]).collect();
        assert!(g0.windows(2).all(|w| w[0] <= w[1]));
        assert!(!tl.kills.is_empty());
        assert!(tl.kills.iter().all(|k| k.victim < 10));
        assert!(!tl.objectives.is_empty());
        assert!(!tl.wards.is_empty());

        // Encoded size sanity: compact and re-decodable.
        let bytes = r.encode_to_vec();
        assert!(bytes.len() < 100_000, "record too big: {}", bytes.len());
        let back = MatchRecord::decode(bytes.as_slice()).unwrap();
        assert_eq!(&back, r);
    }

    #[test]
    fn match_only_without_timeline() {
        let match_json = fixture("match.json");
        let parsed = parse_match(&match_json, None).unwrap();
        assert!(parsed.record.timeline.is_none());
        assert_eq!(parsed.record.participants.len(), 10);
    }

    #[test]
    fn split_ids() {
        assert_eq!(split_match_id("EUW1_7819712430").unwrap(), ("EUW1", 7819712430));
        assert!(split_match_id("garbage").is_err());
    }
}
