//! crawler-server: owns all crawl logic and all data, makes **zero** Riot
//! API requests itself. Riot fetches are handed as opaque jobs to enrolled
//! crawler-node instances (friends running their own API keys), pulled over
//! HTTP, cross-checked by random duplicate audits.
//!
//! Commands:
//!   (none)         run the server
//!   backfill       run, first rescheduling the cohort for deep visits
//!   inspect        decode and sanity-check everything on disk
//!   invite <name>  mint a one-time enrollment code for a new node

mod api;
mod broker;
mod columnar;
mod config;
mod crawler;
mod inspect;
mod invites;
mod metrics;
mod record;
mod registry;
mod riot;
mod stats;
mod storage;

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use anyhow::{Context, Result};
use sha2::Digest;
use tokio::sync::{Mutex, watch};

use crawler::RegionCrawler;
use riot::RiotClient;
use storage::Store;

/// 32 random bytes, hex — the node bearer token.
pub fn generate_token() -> String {
    let bytes: [u8; 32] = rand::random();
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn token_hash_hex(token: &str) -> String {
    let d = sha2::Sha256::digest(token.as_bytes());
    d.iter().map(|b| format!("{b:02x}")).collect()
}

fn spawn_recompactor(
    data_dir: String,
    store: Arc<Mutex<Store>>,
    mut stop: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut delay = Duration::from_secs(30);
        loop {
            if *stop.borrow() {
                break;
            }
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = stop.changed() => break,
            }
            run_recompaction_cycle(&data_dir, &store).await;
            delay = Duration::from_secs(3600);
        }
    })
}

async fn run_recompaction_cycle(data_dir: &str, store: &Arc<Mutex<Store>>) {
    let open_dates: HashSet<String> = store.lock().await.open_writer_dates().into_iter().collect();
    let today = chrono::Utc::now().date_naive();
    let now = SystemTime::now();

    for region in config::enabled_regions() {
        let platform = region.platform;
        let dir = Path::new(data_dir).join("matches").join(platform);
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };

        let mut segs: Vec<PathBuf> = entries
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .filter(|path| path.extension().is_some_and(|ext| ext == "seg"))
            .collect();
        segs.sort();

        for seg in segs {
            let Some((date_s, date)) = segment_date(&seg) else {
                continue;
            };
            if date >= today {
                continue;
            }
            // After midnight the writer can still flush buffered records for
            // the old day for a short while; renaming a file out from under
            // its open append fd would silently lose those bytes.
            if open_dates.contains(&date_s) {
                continue;
            }
            if !mtime_older_than(&seg, Duration::from_secs(3600), now) {
                continue;
            }

            let idx = seg.with_extension("idx");
            let seg_for_log = seg.clone();
            let start = Instant::now();
            let result =
                tokio::task::spawn_blocking(move || storage::recompact_segment(&seg, &idx)).await;
            let duration = start.elapsed();
            match result {
                Ok(Ok(outcome)) => {
                    tracing::info!(
                        platform,
                        date = %date_s,
                        already_compacted = outcome.already_compacted,
                        rebuilt_idx = outcome.rebuilt_idx,
                        before_seg_bytes = outcome.before_seg_bytes,
                        after_seg_bytes = outcome.after_seg_bytes,
                        before_idx_bytes = outcome.before_idx_bytes,
                        after_idx_bytes = outcome.after_idx_bytes,
                        records = outcome.record_count,
                        duration_ms = duration.as_millis() as u64,
                        "segment recompaction finished"
                    );
                }
                Ok(Err(err)) => {
                    tracing::warn!(
                        platform,
                        date = %date_s,
                        path = %seg_for_log.display(),
                        error = %err,
                        "segment recompaction failed; leaving day uncompacted"
                    );
                }
                Err(err) => {
                    tracing::warn!(
                        platform,
                        date = %date_s,
                        path = %seg_for_log.display(),
                        error = %err,
                        "segment recompaction task failed; leaving day uncompacted"
                    );
                }
            }
        }
    }

    let raw_data_dir = PathBuf::from(data_dir);
    let raw_start = Instant::now();
    match tokio::task::spawn_blocking(move || storage::recompact_raw_samples(&raw_data_dir)).await {
        Ok(Ok(outcome)) => {
            if outcome.dict_created || outcome.files_upgraded > 0 || outcome.files_failed > 0 {
                tracing::info!(
                    dict_created = outcome.dict_created,
                    dict_bytes = outcome.dict_bytes,
                    training_samples = outcome.training_samples,
                    files_seen = outcome.files_seen,
                    files_upgraded = outcome.files_upgraded,
                    files_already_dict = outcome.files_already_dict,
                    files_skipped_recent = outcome.files_skipped_recent,
                    files_skipped_changed = outcome.files_skipped_changed,
                    files_failed = outcome.files_failed,
                    before_bytes = outcome.before_bytes,
                    after_bytes = outcome.after_bytes,
                    duration_ms = raw_start.elapsed().as_millis() as u64,
                    "raw sample recompression finished"
                );
            }
        }
        Ok(Err(err)) => {
            tracing::warn!(
                error = %err,
                "raw sample recompression failed; leaving raw samples unchanged"
            );
        }
        Err(err) => {
            tracing::warn!(
                error = %err,
                "raw sample recompression task failed; leaving raw samples unchanged"
            );
        }
    }
}

fn segment_date(path: &Path) -> Option<(String, chrono::NaiveDate)> {
    let date_s = path.file_stem()?.to_str()?.to_string();
    let date = chrono::NaiveDate::parse_from_str(&date_s, "%Y-%m-%d").ok()?;
    Some((date_s, date))
}

fn mtime_older_than(path: &Path, age: Duration, now: SystemTime) -> bool {
    std::fs::metadata(path)
        .and_then(|meta| meta.modified())
        .ok()
        .and_then(|mtime| now.duration_since(mtime).ok())
        .is_some_and(|elapsed| elapsed >= age)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let data_dir =
        std::env::var("CRAWLER_DATA_DIR").unwrap_or_else(|_| config::DATA_DIR.to_string());

    let mut backfill = false;
    match std::env::args().nth(1).as_deref() {
        Some("inspect") => return inspect::run(&data_dir),
        Some("backfill") => backfill = true,
        Some("invite") => {
            let label = std::env::args().nth(2).unwrap_or_else(|| "friend".to_string());
            let code = invites::create(&data_dir, &label)?;
            println!("invite code for {label}: {code}");
            println!("they enroll with: crawler-node --server http://<this-host>:8420");
            return Ok(());
        }
        Some(other) => {
            anyhow::bail!("unknown command {other:?} (try: inspect | backfill | invite <name>)")
        }
        None => {}
    }

    let mut store = Store::open(&data_dir)?;
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as u64;
    store.frontier_reconcile(now_ms)?;
    if backfill {
        let n = store.frontier_backfill_reset(now_ms)?;
        tracing::info!(n, "backfill: cohort queued for deep full-history visits");
    }

    // Node registry: persistent records live in redb, runtime view in RAM.
    let registry = Arc::new(registry::Registry::new());
    for (id, rec) in store.nodes_all()? {
        registry.enroll_runtime(id, rec.name, rec.token_sha256_hex);
    }
    // Leaderboard stats: hour buckets reload from redb, minute detail is
    // fresh each run.
    let stats = Arc::new(stats::Stats::load(store.node_stats_all()?, now_ms));
    let store = Arc::new(Mutex::new(store));

    let audit_percent = config::audit_dup_percent();
    let broker = Arc::new(broker::Broker::new(registry.clone(), stats.clone(), audit_percent));
    let client = Arc::new(RiotClient::new(broker.clone()));
    let (stop_tx, stop_rx) = watch::channel(false);

    let regions = config::enabled_regions();
    anyhow::ensure!(!regions.is_empty(), "no regions enabled in config::ENABLED_REGIONS");
    tracing::info!(
        regions = ?regions.iter().map(|r| r.platform).collect::<Vec<_>>(),
        data_dir,
        audit_percent,
        nodes = registry.report().len(),
        "starting crawler server"
    );

    // Node-facing HTTP API.
    let bind = config::bind_addr();
    let app = api::router(api::AppState {
        broker: broker.clone(),
        registry: registry.clone(),
        stats: stats.clone(),
        store: store.clone(),
        data_dir: data_dir.clone(),
    });
    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .with_context(|| format!("binding {bind}"))?;
    tracing::info!(bind, "node API listening");
    let mut api_stop = stop_rx.clone();
    let api_handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = api_stop.changed().await;
            })
            .await;
    });

    let metrics = Arc::new(metrics::Metrics::new(&regions));
    let mut handles = Vec::new();
    for region in &regions {
        handles.extend(RegionCrawler::spawn(
            *region,
            client.clone(),
            store.clone(),
            stop_rx.clone(),
            metrics.clone(),
        ));
    }
    handles.push(broker::spawn_sweeper(broker.clone(), stop_rx.clone()));
    // Stats flusher: dirty leaderboard hour buckets -> redb, ~1/min.
    {
        let stats = stats.clone();
        let store = store.clone();
        let mut stop = stop_rx.clone();
        handles.push(tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => {}
                    _ = stop.changed() => break,
                }
                let dirty = stats.take_dirty();
                if let Err(e) = store.lock().await.node_stats_upsert(&dirty) {
                    tracing::warn!(error = %e, "stats flush failed");
                }
            }
            let dirty = stats.take_dirty();
            if let Err(e) = store.lock().await.node_stats_upsert(&dirty) {
                tracing::warn!(error = %e, "final stats flush failed");
            }
        }));
    }
    handles.push(spawn_recompactor(
        data_dir.clone(),
        store.clone(),
        stop_rx.clone(),
    ));
    handles.push(tokio::spawn(metrics::reporter(
        metrics.clone(),
        registry.clone(),
        broker.clone(),
        store.clone(),
        stop_rx.clone(),
    )));

    tokio::signal::ctrl_c().await?;
    tracing::info!("shutdown requested, draining...");
    stop_tx.send(true)?;
    // Resolve every in-flight fetch so region crawlers can't hang waiting
    // for jobs no node will serve; long-polls wake and return empty.
    broker.shutdown();
    for h in handles {
        let _ = h.await;
    }
    let _ = api_handle.await;
    store.lock().await.commit()?;
    tracing::info!("bye");
    Ok(())
}
