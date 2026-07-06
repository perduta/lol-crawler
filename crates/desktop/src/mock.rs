//! `--mock` mode: fakes both the crawler-server and the Riot API so the
//! frontend can be exercised with nothing else running (`crawler-desktop
//! --mock`, or env `CRAWL_CREW_MOCK=1`).
//!
//! The script hits every UI surface:
//!   - starts unenrolled (the enrollment form accepts anything), "connects"
//!     ~1 s after the worker starts, then streams fake jobs whose lifecycle
//!     events drive the orb visualization and counters;
//!   - a short disconnect/reconnect blip every ~90 s (status pill);
//!   - a one-time "key expired" pause ~150 s in — paste anything into the
//!     key banner to resume;
//!   - [`stats`] fabricates a leaderboard whose "you" row grows with the
//!     session so the lifetime counters keep moving.
//!
//! Mock mode never touches the network or the real node config file.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use crawler_node::events::{NodeEvent, NodeHandle};
use crawler_proto as proto;
use tokio::sync::watch;

/// (routing host, method bucket, weight) — plausible traffic mix.
const TRAFFIC: &[(&str, &str, u64)] = &[
    ("europe", "match-v5.match", 30),
    ("americas", "match-v5.match", 22),
    ("asia", "match-v5.match", 16),
    ("sea", "match-v5.match", 8),
    ("europe", "match-v5.matchlist", 6),
    ("americas", "match-v5.matchlist", 4),
    ("euw1", "league-v4.entries", 5),
    ("na1", "league-v4.entries", 4),
    ("kr", "league-v4.entries", 3),
    ("euw1", "summoner-v4.by-puuid", 2),
];

/// xorshift64* — no need to pull in `rand` for scenery.
struct Rng(u64);

impl Rng {
    fn seeded() -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos() as u64)
            .unwrap_or(0);
        Self(nanos | 1)
    }

    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn ms(&mut self, lo: u64, hi: u64) -> Duration {
        Duration::from_millis(lo + self.below(hi - lo))
    }
}

fn pick_traffic(rng: &mut Rng) -> (&'static str, &'static str) {
    let total: u64 = TRAFFIC.iter().map(|t| t.2).sum();
    let mut roll = rng.below(total);
    for &(host, method, w) in TRAFFIC {
        if roll < w {
            return (host, method);
        }
        roll -= w;
    }
    unreachable!("weights sum mismatch")
}

/// One fake job through the full real lifecycle:
/// started → active → done → uploaded, with human-scale delays.
async fn fake_job(handle: Arc<NodeHandle>, id: u64, seed: u64) {
    let mut rng = Rng(seed | 1);
    let (host, method) = pick_traffic(&mut rng);
    handle.emit(NodeEvent::JobStarted {
        id,
        host: host.to_string(),
        method: method.to_string(),
    });
    // queued on the (fictional) local rate limiter
    tokio::time::sleep(rng.ms(300, 2600)).await;
    handle.emit(NodeEvent::JobActive { id });
    // "Riot" answering
    let ms = rng.ms(120, 900);
    tokio::time::sleep(ms).await;
    let roll = rng.below(100);
    let outcome = if roll < 91 {
        "ok"
    } else if roll < 97 {
        "not_found"
    } else {
        "failed"
    };
    let is_match = method == "match-v5.match" && outcome == "ok";
    handle.job_finished(host, is_match);
    handle.emit(NodeEvent::JobDone {
        id,
        host: host.to_string(),
        method: method.to_string(),
        outcome: outcome.to_string(),
        ms: ms.as_millis() as u64,
    });
    // result "uploading" to the server
    tokio::time::sleep(rng.ms(150, 700)).await;
    handle.emit(NodeEvent::JobUploaded { id });
}

/// True means "stop now".
async fn sleep_or_stop(stop: &mut watch::Receiver<bool>, d: Duration) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(d) => *stop.borrow(),
        _ = stop.changed() => true,
    }
}

/// Mock counterpart of `crawler_node::worker::run`: same [`NodeHandle`]
/// surface, no network. Runs until `stop` flips.
pub async fn run(handle: Arc<NodeHandle>, mut stop: watch::Receiver<bool>) {
    tracing::info!(
        "MOCK node started — no server, no Riot requests. \
         Script: disconnect blip every ~90 s, key-expiry demo at ~150 s."
    );
    let mut rng = Rng::seeded();

    // Let the "connecting…" pill state be visible for a beat.
    if sleep_or_stop(&mut stop, Duration::from_millis(1200)).await {
        return finish(&handle);
    }
    handle.connected.store(true, Ordering::Relaxed);
    handle.emit(NodeEvent::Connected);

    let started = Instant::now();
    let mut next_id: u64 = 1;
    let mut next_blip = Duration::from_secs(90);
    let mut key_demo_done = false;

    loop {
        if sleep_or_stop(&mut stop, rng.ms(350, 1100)).await {
            break;
        }

        // One-time key-expiry demo: pause exactly like the real loop and
        // resume when the frontend saves any key (set_key nudges key_update).
        if !key_demo_done && started.elapsed() >= Duration::from_secs(150) {
            key_demo_done = true;
            handle.key_bad.store(true, Ordering::Relaxed);
            handle.emit(NodeEvent::KeyBad);
            tokio::select! {
                _ = handle.key_update.notified() => {}
                _ = stop.changed() => break,
            }
            handle.key_bad.store(false, Ordering::Relaxed);
            handle.emit(NodeEvent::KeyOk);
            continue;
        }

        // Periodic outage blip for the status pill.
        if started.elapsed() >= next_blip {
            next_blip = started.elapsed() + Duration::from_secs(90);
            handle.connected.store(false, Ordering::Relaxed);
            handle.emit(NodeEvent::Disconnected);
            if sleep_or_stop(&mut stop, Duration::from_secs(4)).await {
                break;
            }
            handle.connected.store(true, Ordering::Relaxed);
            handle.emit(NodeEvent::Connected);
        }

        // A small batch of jobs per tick; each lives its own life.
        for _ in 0..(1 + rng.below(3)) {
            let id = next_id;
            next_id += 1;
            tauri::async_runtime::spawn(fake_job(handle.clone(), id, rng.next()));
        }
    }
    finish(&handle)
}

fn finish(handle: &NodeHandle) {
    handle.connected.store(false, Ordering::Relaxed);
    handle.emit(NodeEvent::Stopped);
    tracing::info!("mock node stopped");
}

/// Windows for a node that's been crawling at `rpm` requests/minute for a
/// while; `extra` is added everywhere so live session counts only ever
/// push the numbers up.
fn windows(rpm: u64, extra: u64) -> proto::WindowCounts {
    proto::WindowCounts {
        m60: rpm * 60 + extra,
        h24: rpm * 60 * 20 + extra,
        d7: rpm * 60 * 20 * 6 + extra,
        all: rpm * 60 * 20 * 25 + extra,
    }
}

/// Fabricated `/v1/stats` leaderboard. The fictional crew is static; the
/// "you" row folds in the live session counters from `handle`.
pub fn stats(you: &str, handle: &NodeHandle) -> proto::StatsResponse {
    const CREW: &[(&str, u64, bool)] = &[
        ("poro-feeder", 480, true),
        ("baron-stealer", 350, true),
        ("minion-wave", 220, true),
        ("afk-ward", 90, false),
    ];
    let mut nodes: Vec<proto::NodeStatsEntry> = CREW
        .iter()
        .map(|&(name, rpm, online)| proto::NodeStatsEntry {
            name: name.to_string(),
            online,
            requests: windows(rpm, 0),
            matches: windows(rpm / 3, 0),
        })
        .collect();
    nodes.push(proto::NodeStatsEntry {
        name: you.to_string(),
        online: true,
        requests: windows(300, handle.completed.load(Ordering::Relaxed)),
        matches: windows(100, handle.matches.load(Ordering::Relaxed)),
    });
    proto::StatsResponse {
        you: you.to_string(),
        nodes,
        generated_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0),
    }
}
