//! The node work loop: pull jobs from the server (long-poll), execute them
//! against the Riot API at full local rate-limit speed, upload results.
//!
//! The node reports how many jobs it already holds per routing host
//! (`pending`); the server tops each host up to its per-node target, so
//! saturation is driven purely by the node's own limiters — exactly how
//! the original single-process crawler maximized a key.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Result, bail};
use crawler_proto as proto;
use tokio::sync::{Notify, mpsc};

use crate::config::NodeConfig;
use crate::executor::Executor;

/// Long-poll budget when idle; short pace while jobs are flowing.
const IDLE_WAIT_MS: u64 = 25_000;
const BUSY_WAIT_MS: u64 = 1_500;

pub struct ServerClient {
    http: reqwest::Client,
    pub server: String,
    token: String,
}

impl ServerClient {
    pub fn new(server: &str, token: &str) -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(60))
                .build()
                .expect("http client"),
            server: server.trim_end_matches('/').to_string(),
            token: token.to_string(),
        }
    }

    pub async fn post_json<B: serde::Serialize, T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T> {
        let resp = self
            .http
            .post(format!("{}{}", self.server, path))
            .header(proto::PROTO_HEADER, proto::PROTOCOL_VERSION.to_string())
            .bearer_auth(&self.token)
            .json(body)
            .send()
            .await?;
        let status = resp.status();
        if status.is_success() {
            return Ok(resp.json().await?);
        }
        let msg = resp
            .json::<proto::ErrorResponse>()
            .await
            .map(|e| e.message)
            .unwrap_or_else(|_| status.to_string());
        if status == reqwest::StatusCode::UPGRADE_REQUIRED {
            eprintln!("\nserver says: {msg}\n");
            std::process::exit(2);
        }
        bail!("server {path}: {status}: {msg}");
    }
}

struct Shared {
    executor: Executor,
    /// Jobs held (queued + running) per routing host, reported as
    /// `pending` so the server knows how much to top up.
    inflight: std::sync::Mutex<HashMap<String, u32>>,
    /// Woken on job completion so the poll loop refills promptly.
    poll_nudge: Notify,
    completed: AtomicU64,
}

pub async fn run(cfg: NodeConfig, config_path: std::path::PathBuf) -> Result<()> {
    let client = Arc::new(ServerClient::new(&cfg.server, &cfg.token));
    let shared = Arc::new(Shared {
        executor: Executor::new(cfg.riot_api_key.clone()),
        inflight: std::sync::Mutex::new(HashMap::new()),
        poll_nudge: Notify::new(),
        completed: AtomicU64::new(0),
    });
    let stop = Arc::new(AtomicBool::new(false));

    // Uploader: results must reach the server; retry with backoff, and
    // only give up when the lease is long dead anyway (the server will
    // have re-issued the job).
    let (result_tx, mut result_rx) = mpsc::unbounded_channel::<proto::JobResult>();
    let up_client = client.clone();
    let uploader = tokio::spawn(async move {
        while let Some(res) = result_rx.recv().await {
            let mut delay = 1u64;
            let mut waited = 0u64;
            loop {
                match up_client.post_json::<_, serde_json::Value>("/v1/result", &res).await {
                    Ok(_) => break,
                    Err(e) => {
                        if waited >= 300 {
                            tracing::warn!(job = res.id, error = %e, "dropping result (lease expired anyway)");
                            break;
                        }
                        tracing::debug!(job = res.id, error = %e, "result upload failed, retrying");
                        tokio::time::sleep(Duration::from_secs(delay)).await;
                        waited += delay;
                        delay = (delay * 2).min(30);
                    }
                }
            }
        }
    });

    // Status line.
    {
        let shared = shared.clone();
        let stop = stop.clone();
        tokio::spawn(async move {
            let mut prev = 0u64;
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                let total = shared.completed.load(Ordering::Relaxed);
                let hosts = shared
                    .executor
                    .limiters
                    .sent_totals()
                    .into_iter()
                    .map(|(h, n)| format!("{h}={n}"))
                    .collect::<Vec<_>>()
                    .join(" ");
                let paused = shared.executor.key_bad.load(Ordering::Relaxed);
                tracing::info!(
                    "jobs done: +{} (total {total}) | requests sent: {hosts}{}",
                    total - prev,
                    if paused { " | PAUSED: key rejected" } else { "" }
                );
                prev = total;
            }
        });
    }

    let mut announced_pause = false;
    let mut key_mtime = crate::config::mtime(&config_path);
    tracing::info!(server = %client.server, name = %cfg.name, "node started");

    loop {
        if stop.load(Ordering::Relaxed) {
            break;
        }

        // Key rejected: stop pulling work, watch the config file for a new
        // key (edit it or run `crawler-node set-key`).
        if shared.executor.key_bad.load(Ordering::Relaxed) {
            if !announced_pause {
                tracing::error!(
                    "Riot rejected your API key (dev keys expire daily). \
                     Update it with: crawler-node set-key   — then work resumes automatically."
                );
                announced_pause = true;
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(15)) => {}
                _ = tokio::signal::ctrl_c() => break,
            }
            let m = crate::config::mtime(&config_path);
            if m != key_mtime {
                key_mtime = m;
                if let Ok(Some(fresh)) = crate::config::load(&config_path) {
                    shared.executor.set_api_key(fresh.riot_api_key);
                    announced_pause = false;
                    tracing::info!("API key updated, resuming");
                }
            }
            continue;
        }

        let pending: HashMap<String, u32> = {
            let inf = shared.inflight.lock().unwrap();
            inf.iter().filter(|(_, n)| **n > 0).map(|(h, n)| (h.clone(), *n)).collect()
        };
        let idle = pending.is_empty();
        let req = proto::WorkRequest {
            pending,
            wait_ms: if idle { IDLE_WAIT_MS } else { BUSY_WAIT_MS },
            client_version: env!("CARGO_PKG_VERSION").to_string(),
        };

        let work = tokio::select! {
            w = client.post_json::<_, proto::WorkResponse>("/v1/work", &req) => w,
            _ = tokio::signal::ctrl_c() => break,
        };
        match work {
            Ok(resp) => {
                for job in resp.jobs {
                    *shared.inflight.lock().unwrap().entry(job.host.clone()).or_default() += 1;
                    let shared = shared.clone();
                    let tx = result_tx.clone();
                    tokio::spawn(async move {
                        let res = shared.executor.execute(&job).await;
                        {
                            let mut inf = shared.inflight.lock().unwrap();
                            if let Some(n) = inf.get_mut(&job.host) {
                                *n = n.saturating_sub(1);
                            }
                        }
                        shared.completed.fetch_add(1, Ordering::Relaxed);
                        let _ = tx.send(res);
                        shared.poll_nudge.notify_waiters();
                    });
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "work poll failed (server unreachable?), retrying in 5s");
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                    _ = tokio::signal::ctrl_c() => break,
                }
                continue;
            }
        }

        // Pace the next poll: immediately after a completion nudge, else
        // a short sleep so a fully-loaded node doesn't hammer the server.
        tokio::select! {
            _ = shared.poll_nudge.notified() => {}
            _ = tokio::time::sleep(Duration::from_millis(750)) => {}
            _ = tokio::signal::ctrl_c() => break,
        }
    }

    stop.store(true, Ordering::Relaxed);
    tracing::info!("draining result uploads...");
    drop(result_tx);
    let _ = tokio::time::timeout(Duration::from_secs(15), uploader).await;
    tracing::info!("bye");
    Ok(())
}
