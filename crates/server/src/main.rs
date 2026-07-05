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

use std::sync::Arc;

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
