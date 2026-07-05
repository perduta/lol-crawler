//! The node work loop: pull jobs from the server (long-poll), execute them
//! against the Riot API at full local rate-limit speed, upload results.
//!
//! The node reports how many jobs it already holds per routing host
//! (`pending`); the server tops each host up to its per-node target, so
//! saturation is driven purely by the node's own limiters — exactly how
//! the original single-process crawler maximized a key.
//!
//! This module never prints to the user or exits the process: frontends
//! observe progress through [`NodeHandle`] events and stop the loop via
//! the watch channel.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use crawler_proto as proto;
use tokio::sync::{mpsc, watch};

use crate::config::NodeConfig;
use crate::events::{NodeEvent, NodeHandle};
use crate::executor::Executor;

/// Long-poll budget when idle; short pace while jobs are flowing.
const IDLE_WAIT_MS: u64 = 25_000;
const BUSY_WAIT_MS: u64 = 1_500;

/// The server refused our protocol version (426): this build is obsolete.
/// Carried inside anyhow errors; frontends downcast to detect it.
#[derive(Debug)]
pub struct ProtocolMismatch(pub String);

impl std::fmt::Display for ProtocolMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for ProtocolMismatch {}

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
            return Err(anyhow::Error::new(ProtocolMismatch(msg)));
        }
        bail!("server {path}: {status}: {msg}");
    }

    /// Leaderboard fetch, for GUI frontends.
    pub async fn stats(&self) -> Result<proto::StatsResponse> {
        self.post_json("/v1/stats", &serde_json::json!({})).await
    }
}

fn outcome_str(o: proto::JobOutcome) -> &'static str {
    match o {
        proto::JobOutcome::Ok => "ok",
        proto::JobOutcome::NotFound => "not_found",
        proto::JobOutcome::KeyRejected => "key_rejected",
        proto::JobOutcome::Failed => "failed",
    }
}

struct Shared {
    executor: Executor,
    /// Jobs held (queued + running) per routing host, reported as
    /// `pending` so the server knows how much to top up.
    inflight: std::sync::Mutex<HashMap<String, u32>>,
    /// Woken on job completion so the poll loop refills promptly.
    poll_nudge: tokio::sync::Notify,
    handle: Arc<NodeHandle>,
}

/// Runs the node until `stop` flips to true. Returns Err only for
/// unrecoverable states (currently: protocol mismatch).
pub async fn run(
    cfg: NodeConfig,
    config_path: std::path::PathBuf,
    handle: Arc<NodeHandle>,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    let client = Arc::new(ServerClient::new(&cfg.server, &cfg.token));
    let shared = Arc::new(Shared {
        executor: Executor::new(cfg.riot_api_key.clone()),
        inflight: std::sync::Mutex::new(HashMap::new()),
        poll_nudge: tokio::sync::Notify::new(),
        handle: handle.clone(),
    });

    // Uploader: results must reach the server; retry with backoff, and
    // only give up when the lease is long dead anyway (the server will
    // have re-issued the job).
    let (result_tx, mut result_rx) = mpsc::unbounded_channel::<proto::JobResult>();
    let up_client = client.clone();
    let up_handle = handle.clone();
    let uploader = tokio::spawn(async move {
        while let Some(res) = result_rx.recv().await {
            let mut delay = 1u64;
            let mut waited = 0u64;
            loop {
                match up_client.post_json::<_, serde_json::Value>("/v1/result", &res).await {
                    Ok(_) => break,
                    Err(e) => {
                        if e.downcast_ref::<ProtocolMismatch>().is_some() {
                            // The main loop will hit the same wall and stop.
                            break;
                        }
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
            // Uploaded or given up: either way the job is off this node.
            up_handle.emit(NodeEvent::JobUploaded { id: res.id });
        }
    });

    // Status line.
    {
        let shared = shared.clone();
        let mut stop = stop.clone();
        tokio::spawn(async move {
            let mut prev = 0u64;
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(60)) => {}
                    _ = stop.changed() => return,
                }
                let total = shared.handle.completed.load(Ordering::Relaxed);
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
    let mut fatal: Option<anyhow::Error> = None;
    tracing::info!(server = %client.server, name = %cfg.name, "node started");

    loop {
        if *stop.borrow() {
            break;
        }

        // Key rejected: stop pulling work, wait for a new key — either the
        // config file changes (CLI set-key / manual edit) or a frontend
        // pokes `key_update` after saving one.
        if shared.executor.key_bad.load(Ordering::Relaxed) {
            if !announced_pause {
                tracing::error!(
                    "Riot rejected your API key (dev keys expire daily). \
                     Update it with: crawler-node set-key   — then work resumes automatically."
                );
                handle.key_bad.store(true, Ordering::Relaxed);
                handle.emit(NodeEvent::KeyBad);
                announced_pause = true;
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(15)) => {}
                _ = handle.key_update.notified() => {}
                _ = stop.changed() => break,
            }
            let m = crate::config::mtime(&config_path);
            if m != key_mtime {
                key_mtime = m;
                if let Ok(Some(fresh)) = crate::config::load(&config_path) {
                    shared.executor.set_api_key(fresh.riot_api_key);
                    announced_pause = false;
                    handle.key_bad.store(false, Ordering::Relaxed);
                    handle.emit(NodeEvent::KeyOk);
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
            _ = stop.changed() => break,
        };
        match work {
            Ok(resp) => {
                if !handle.connected.swap(true, Ordering::Relaxed) {
                    handle.emit(NodeEvent::Connected);
                }
                for job in resp.jobs {
                    *shared.inflight.lock().unwrap().entry(job.host.clone()).or_default() += 1;
                    let shared = shared.clone();
                    let tx = result_tx.clone();
                    tokio::spawn(async move {
                        shared.handle.emit(NodeEvent::JobStarted {
                            id: job.id,
                            host: job.host.clone(),
                            method: job.method.clone(),
                        });
                        let started = Instant::now();
                        let res = shared
                            .executor
                            .execute(&job, || {
                                shared.handle.emit(NodeEvent::JobActive { id: job.id });
                            })
                            .await;
                        {
                            let mut inf = shared.inflight.lock().unwrap();
                            if let Some(n) = inf.get_mut(&job.host) {
                                *n = n.saturating_sub(1);
                            }
                        }
                        let is_match =
                            job.method == "match-v5.match" && res.outcome == proto::JobOutcome::Ok;
                        shared.handle.job_finished(&job.host, is_match);
                        shared.handle.emit(NodeEvent::JobDone {
                            id: job.id,
                            host: job.host.clone(),
                            method: job.method.clone(),
                            outcome: outcome_str(res.outcome).to_string(),
                            ms: started.elapsed().as_millis() as u64,
                        });
                        let _ = tx.send(res);
                        shared.poll_nudge.notify_waiters();
                    });
                }
            }
            Err(e) => {
                if e.downcast_ref::<ProtocolMismatch>().is_some() {
                    handle.emit(NodeEvent::ProtocolMismatch { message: e.to_string() });
                    fatal = Some(e);
                    break;
                }
                if handle.connected.swap(false, Ordering::Relaxed) {
                    handle.emit(NodeEvent::Disconnected);
                }
                tracing::warn!(error = %e, "work poll failed (server unreachable?), retrying in 5s");
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                    _ = stop.changed() => break,
                }
                continue;
            }
        }

        // Pace the next poll: immediately after a completion nudge, else
        // a short sleep so a fully-loaded node doesn't hammer the server.
        tokio::select! {
            _ = shared.poll_nudge.notified() => {}
            _ = tokio::time::sleep(Duration::from_millis(750)) => {}
            _ = stop.changed() => break,
        }
    }

    tracing::info!("draining result uploads...");
    drop(result_tx);
    let _ = tokio::time::timeout(Duration::from_secs(15), uploader).await;
    handle.connected.store(false, Ordering::Relaxed);
    handle.emit(NodeEvent::Stopped);
    tracing::info!("bye");
    match fatal {
        Some(e) => Err(e),
        None => Ok(()),
    }
}
