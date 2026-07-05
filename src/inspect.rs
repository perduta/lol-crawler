//! `lol-crawler inspect` — decode and sanity-check everything on disk.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::Result;
use prost::Message;

use crate::record::MatchRecord;
use crate::storage::{self, Store};

pub fn run(data_dir: &str) -> Result<()> {
    let store = Store::open(data_dir)?;
    let stats = store.debug_stats()?;
    println!("== store ==");
    println!("players:                  {}", stats.players);
    println!("rank snapshots:           {}", stats.rank_snapshots);
    println!("player timeline entries:  {}", stats.player_timeline_entries);
    for (platform, n) in &stats.seen {
        println!("seen matches {platform}:       {n}");
    }
    for (platform, bucket, n) in &stats.frontier {
        let label = if *bucket == 0 { "priority" } else { "other" };
        println!("frontier {platform} {label}:   {n}");
    }
    println!("cohort members:           {}", stats.cohort);
    println!("outsiders tracked:        {}", stats.outsiders_tracked);
    for (platform, n) in &stats.valid_samples {
        println!("valid samples {platform}:      {n}");
    }
    println!(
        "readiness histogram (matches by #participants with full history):"
    );
    for (ready, n) in stats.readiness_histogram.iter().enumerate() {
        if *n > 0 {
            println!("  {ready:>2}/10 ready: {n}");
        }
    }
    drop(store);

    let matches_dir = Path::new(data_dir).join("matches");
    if !matches_dir.exists() {
        return Ok(());
    }
    for platform_dir in std::fs::read_dir(&matches_dir)? {
        let platform_dir = platform_dir?.path();
        let mut seg_files: Vec<_> = std::fs::read_dir(&platform_dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|e| e == "seg"))
            .collect();
        seg_files.sort();

        for seg in seg_files {
            let file_bytes = std::fs::metadata(&seg)?.len();
            let raw = storage::read_segment_records(&seg)?;
            let mut decoded = 0u64;
            let mut decode_errors = 0u64;
            let mut raw_bytes = 0u64;
            let mut with_timeline = 0u64;
            let mut by_patch: BTreeMap<String, u64> = BTreeMap::new();
            let mut sample: Option<MatchRecord> = None;
            for bytes in &raw {
                raw_bytes += bytes.len() as u64;
                match MatchRecord::decode(bytes.as_slice()) {
                    Ok(rec) => {
                        decoded += 1;
                        if rec.timeline.is_some() {
                            with_timeline += 1;
                        }
                        *by_patch
                            .entry(format!("{}.{}", rec.patch_major, rec.patch_minor))
                            .or_default() += 1;
                        if sample.is_none() {
                            sample = Some(rec);
                        }
                    }
                    Err(_) => decode_errors += 1,
                }
            }
            println!("\n== {} ==", seg.display());
            println!("records decoded:   {decoded} (errors: {decode_errors})");
            println!("with timeline:     {with_timeline}");
            println!("file size:         {file_bytes} B");
            if decoded > 0 {
                println!(
                    "bytes/match:       {} on disk, {} uncompressed",
                    file_bytes / decoded,
                    raw_bytes / decoded
                );
            }
            println!("patches:           {by_patch:?}");
            if let Some(r) = sample {
                let tl = r.timeline.as_ref();
                println!(
                    "first record:      {}_{} queue={} dur={}s blue_won={} \
                     participants={} frames={} kills={} objectives={} wards={}",
                    r.platform,
                    r.game_id,
                    r.queue_id,
                    r.duration_s,
                    r.blue_won,
                    r.participants.len(),
                    tl.map_or(0, |t| t.frames.len()),
                    tl.map_or(0, |t| t.kills.len()),
                    tl.map_or(0, |t| t.objectives.len()),
                    tl.map_or(0, |t| t.wards.len()),
                );
            }
        }
    }
    Ok(())
}
