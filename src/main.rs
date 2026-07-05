mod config;
mod crawler;
mod inspect;
mod metrics;
mod ratelimit;
mod record;
mod riot;
mod storage;

use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::sync::{Mutex, watch};

use crawler::RegionCrawler;
use ratelimit::LimiterRegistry;
use riot::RiotClient;
use storage::Store;

/// API key: env var API_KEY, else api_key.env in cwd or parent dirs
/// (format: API_KEY="RGAPI-...").
fn load_api_key() -> Result<String> {
    if let Ok(k) = std::env::var("API_KEY") {
        return Ok(k.trim().trim_matches('"').to_string());
    }
    let mut dir = std::env::current_dir()?;
    loop {
        let candidate = dir.join("api_key.env");
        if candidate.exists() {
            let content = std::fs::read_to_string(&candidate)?;
            for line in content.lines() {
                if let Some(v) = line.trim().strip_prefix("API_KEY=") {
                    return Ok(v.trim().trim_matches('"').to_string());
                }
            }
        }
        if !dir.pop() {
            anyhow::bail!("API_KEY not set and no api_key.env found in cwd or parents");
        }
    }
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

    if std::env::args().nth(1).as_deref() == Some("inspect") {
        return inspect::run(&data_dir);
    }

    let api_key = load_api_key().context("loading API key")?;

    let store = Arc::new(Mutex::new(Store::open(&data_dir)?));
    let limiters = Arc::new(LimiterRegistry::default());
    let client = Arc::new(RiotClient::new(api_key, limiters.clone()));
    let (stop_tx, stop_rx) = watch::channel(false);

    let regions = config::enabled_regions();
    anyhow::ensure!(!regions.is_empty(), "no regions enabled in config::ENABLED_REGIONS");
    tracing::info!(
        regions = ?regions.iter().map(|r| r.platform).collect::<Vec<_>>(),
        data_dir,
        "starting crawler"
    );

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
    handles.push(tokio::spawn(metrics::reporter(
        metrics.clone(),
        limiters.clone(),
        store.clone(),
        stop_rx.clone(),
    )));

    tokio::signal::ctrl_c().await?;
    tracing::info!("shutdown requested, draining...");
    stop_tx.send(true)?;
    for h in handles {
        let _ = h.await;
    }
    store.lock().await.commit()?;
    tracing::info!("bye");
    Ok(())
}
