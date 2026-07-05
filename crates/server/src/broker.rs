//! Fetch broker: bridges the crawl logic (which awaits Riot API bodies) to
//! the node fleet (which pulls opaque GET jobs and uploads results).
//!
//! - Jobs are ephemeral (RAM only, derived from the frontier): a server
//!   restart re-derives work; the seen-bitmap prevents refetching.
//! - Nodes *pull*: rate limiting lives entirely node-side, the broker just
//!   tops each node up to a per-host inflight target.
//! - Leases: a job handed out is re-queued if no result arrives within
//!   [`config::LEASE_MS`] (2.5 min).
//! - Audits: a configurable fraction of dispatched jobs (default 1%) is
//!   cloned to a *different* node; the two bodies are compared to catch
//!   nodes returning wrong data. Immutable endpoints (match, timeline) must
//!   match exactly; volatile ones (matchlists, leagues) only count as
//!   "soft" mismatches since the ground truth moves between the two fetches.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use tokio::sync::{Notify, oneshot};

use crate::config;
use crate::registry::Registry;

/// Per-node, per-host inflight target: how many jobs a node may hold for
/// one routing host. Big enough to hide poll latency, small enough that a
/// dying node doesn't strand much work behind its lease.
pub const TARGET_INFLIGHT_PER_HOST: u32 = 8;
/// A job is abandoned (fetch resolves Err) after this many failed
/// attempts across nodes (node-reported failures + lease expiries).
const MAX_ATTEMPTS: u32 = 6;
/// Unserved audit twins older than this are dropped (e.g. the only other
/// node went offline).
const TWIN_TTL: Duration = Duration::from_secs(600);

/// Endpoints whose responses are immutable, so audit bodies must match
/// exactly. Everything else changes over time and only soft-mismatches.
const IMMUTABLE_METHODS: &[&str] = &["match-v5.match", "match-v5.timeline"];

type FetchResult = Result<Option<String>>;

struct JobState {
    host: String,
    method: String,
    path: String,
    /// Resolves the crawl-side future. None for audit twins.
    responder: Option<oneshot::Sender<FetchResult>>,
    lease: Option<(u32, Instant)>,
    attempts: u32,
    /// Audit twins must not run on the node that ran the primary.
    exclude_node: Option<u32>,
    /// Primary <-> twin cross-link (audit pairs).
    twin_id: Option<u64>,
    is_twin: bool,
    created: Instant,
}

/// A completed fetch body as the audit comparator sees it.
#[derive(Debug, Clone, PartialEq)]
enum AuditBody {
    Found(String),
    NotFound,
}

/// Keyed by twin job id in `Inner::audits`.
struct AuditPair {
    method: String,
    path: String,
    primary_node: u32,
    twin_node: Option<u32>,
    primary: Option<AuditBody>,
    twin: Option<AuditBody>,
}

#[derive(Default)]
struct Inner {
    next_id: u64,
    jobs: HashMap<u64, JobState>,
    /// Unleased job ids per routing host, with their exclude_node denorm'd
    /// so dispatch doesn't need a second lookup. Invariant: every entry is
    /// unleased and present in `jobs` (entries whose job vanished are
    /// skipped lazily).
    queues: HashMap<String, VecDeque<(u64, Option<u32>)>>,
    audits: HashMap<u64, AuditPair>,
}

pub struct Broker {
    inner: Mutex<Inner>,
    /// Woken whenever new work lands or shutdown starts (long-poll wakeup).
    pub notify: Notify,
    registry: std::sync::Arc<Registry>,
    audit_percent: f64,
    shutdown: AtomicBool,
}

impl Broker {
    pub fn new(registry: std::sync::Arc<Registry>, audit_percent: f64) -> Self {
        Self {
            inner: Mutex::new(Inner { next_id: 1, ..Default::default() }),
            notify: Notify::new(),
            registry,
            audit_percent,
            shutdown: AtomicBool::new(false),
        }
    }

    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
    }

    /// Crawl-side entry point: enqueue one GET and await its body.
    /// Ok(None) = definitive 404. Err = abandoned (attempts exhausted or
    /// shutdown); callers treat it like today's exhausted-retries error.
    pub async fn fetch(&self, host: &str, method: &str, path: String) -> FetchResult {
        if self.is_shutdown() {
            bail!("broker shut down");
        }
        let (tx, rx) = oneshot::channel();
        {
            let mut inner = self.inner.lock().unwrap();
            let id = inner.next_id;
            inner.next_id += 1;
            inner.jobs.insert(id, JobState {
                host: host.to_string(),
                method: method.to_string(),
                path,
                responder: Some(tx),
                lease: None,
                attempts: 0,
                exclude_node: None,
                twin_id: None,
                is_twin: false,
                created: Instant::now(),
            });
            inner.queues.entry(host.to_string()).or_default().push_back((id, None));
        }
        self.notify.notify_waiters();
        match rx.await {
            Ok(res) => res,
            Err(_) => bail!("job dropped (server shutting down)"),
        }
    }

    /// Node-side entry point: lease jobs for a node, topping each host up
    /// to [`TARGET_INFLIGHT_PER_HOST`] minus what the node already holds.
    /// Rolls the audit dice on every primary dispatched.
    pub fn take_jobs(&self, node_id: u32, pending: &HashMap<String, u32>) -> Vec<crawler_proto::Job> {
        let now = Instant::now();
        let mut out = Vec::new();
        let audit_possible = self.registry.active_node_other_than(node_id);
        let mut inner = self.inner.lock().unwrap();
        let hosts: Vec<String> = inner.queues.keys().cloned().collect();
        let mut new_twins: Vec<(u64, String, String, String)> = Vec::new();

        for host in hosts {
            let held = pending.get(&host).copied().unwrap_or(0);
            let mut cap = TARGET_INFLIGHT_PER_HOST.saturating_sub(held);
            let mut picked = Vec::new();
            {
                let q = inner.queues.get_mut(&host).unwrap();
                let mut i = 0;
                while cap > 0 && i < q.len() {
                    if q[i].1 == Some(node_id) {
                        i += 1; // audit twin excluded from this node
                        continue;
                    }
                    let (id, _) = q.remove(i).unwrap();
                    picked.push(id);
                    cap -= 1;
                }
            }
            for id in picked {
                // Entries can outlive their job (dropped twins); skip those.
                let Some(job) = inner.jobs.get_mut(&id) else { continue };
                job.lease = Some((node_id, now + Duration::from_millis(config::LEASE_MS)));
                out.push(crawler_proto::Job {
                    id,
                    host: job.host.clone(),
                    method: job.method.clone(),
                    path: job.path.clone(),
                    lease_ms: config::LEASE_MS,
                });
                if !job.is_twin
                    && job.twin_id.is_none()
                    && audit_possible
                    && rand::random::<f64>() * 100.0 < self.audit_percent
                {
                    new_twins.push((id, job.host.clone(), job.method.clone(), job.path.clone()));
                }
            }
        }

        for (primary_id, host, method, path) in new_twins {
            let twin_id = inner.next_id;
            inner.next_id += 1;
            inner.jobs.insert(twin_id, JobState {
                host: host.clone(),
                method: method.clone(),
                path: path.clone(),
                responder: None,
                lease: None,
                attempts: 0,
                exclude_node: Some(node_id),
                twin_id: Some(primary_id),
                is_twin: true,
                created: now,
            });
            if let Some(job) = inner.jobs.get_mut(&primary_id) {
                job.twin_id = Some(twin_id);
            }
            inner.queues.entry(host).or_default().push_back((twin_id, Some(node_id)));
            inner.audits.insert(twin_id, AuditPair {
                method,
                path,
                primary_node: node_id,
                twin_node: None,
                primary: None,
                twin: None,
            });
        }
        out
    }

    /// Node-side entry point: a result came back. Stale results (unknown
    /// id, or job re-leased to a different node) are ignored — first
    /// answer wins, duplicates are harmless.
    pub fn complete(&self, node_id: u32, res: crawler_proto::JobResult) {
        let mut requeued = false;
        {
            let mut inner = self.inner.lock().unwrap();
            let Some(job) = inner.jobs.get(&res.id) else { return };
            match job.lease {
                Some((n, _)) if n == node_id => {}
                None => {} // lease expired but the job wasn't re-leased yet: accept
                _ => return,
            }

            match res.outcome {
                crawler_proto::JobOutcome::Ok | crawler_proto::JobOutcome::NotFound => {
                    let job = inner.jobs.remove(&res.id).unwrap();
                    let body = if res.outcome == crawler_proto::JobOutcome::Ok {
                        AuditBody::Found(res.body.unwrap_or_default())
                    } else {
                        AuditBody::NotFound
                    };
                    if let Some(tx) = job.responder {
                        let out = match &body {
                            AuditBody::Found(b) => Ok(Some(b.clone())),
                            AuditBody::NotFound => Ok(None),
                        };
                        let _ = tx.send(out);
                    }
                    self.registry.job_completed(node_id);
                    if job.is_twin {
                        if let Some(pair) = inner.audits.get_mut(&res.id) {
                            pair.twin_node = Some(node_id);
                            pair.twin = Some(body);
                        }
                        self.finish_audit_if_ready(&mut inner, res.id);
                    } else if let Some(twin_id) = job.twin_id {
                        if let Some(pair) = inner.audits.get_mut(&twin_id) {
                            pair.primary = Some(body);
                        }
                        self.finish_audit_if_ready(&mut inner, twin_id);
                    }
                }
                crawler_proto::JobOutcome::KeyRejected => {
                    self.registry.set_key_bad(node_id, true);
                    Self::release(&mut inner, res.id, false);
                    requeued = true;
                }
                crawler_proto::JobOutcome::Failed => {
                    let job = inner.jobs.get_mut(&res.id).unwrap();
                    job.attempts += 1;
                    tracing::warn!(
                        job = res.id, node = node_id, attempts = job.attempts,
                        error = res.error.as_deref().unwrap_or("?"),
                        path = %job.path, "node reported job failure"
                    );
                    if job.attempts >= MAX_ATTEMPTS {
                        Self::abandon(&mut inner, res.id, res.error.as_deref().unwrap_or("failed"));
                    } else {
                        Self::release(&mut inner, res.id, false);
                        requeued = true;
                    }
                }
            }
        }
        if requeued {
            self.notify.notify_waiters();
        }
    }

    /// Puts a job back on its host queue. `count_attempt` for lease expiry.
    fn release(inner: &mut Inner, id: u64, count_attempt: bool) {
        let Some(job) = inner.jobs.get_mut(&id) else { return };
        job.lease = None;
        if count_attempt {
            job.attempts += 1;
            if job.attempts >= MAX_ATTEMPTS {
                Self::abandon(inner, id, "lease expired too many times");
                return;
            }
        }
        let (host, entry) = {
            let job = inner.jobs.get(&id).unwrap();
            (job.host.clone(), (id, job.exclude_node))
        };
        inner.queues.entry(host).or_default().push_back(entry);
    }

    /// Removes a job for good, resolving its future with Err and tearing
    /// down any audit pairing it was part of.
    fn abandon(inner: &mut Inner, id: u64, why: &str) {
        let Some(job) = inner.jobs.remove(&id) else { return };
        if let Some(tx) = job.responder {
            let _ = tx.send(Err(anyhow::anyhow!("job abandoned: {why} ({})", job.path)));
        }
        if job.is_twin {
            inner.audits.remove(&id);
        } else if let Some(twin_id) = job.twin_id {
            // Primary died: the audit has nothing to compare against.
            inner.audits.remove(&twin_id);
            inner.jobs.remove(&twin_id);
        }
    }

    fn finish_audit_if_ready(&self, inner: &mut Inner, twin_id: u64) {
        let ready = inner
            .audits
            .get(&twin_id)
            .is_some_and(|p| p.primary.is_some() && p.twin.is_some());
        if !ready {
            return;
        }
        let pair = inner.audits.remove(&twin_id).unwrap();
        let (a, b) = (pair.primary.as_ref().unwrap(), pair.twin.as_ref().unwrap());
        let equal = match (a, b) {
            (AuditBody::NotFound, AuditBody::NotFound) => true,
            (AuditBody::Found(x), AuditBody::Found(y)) => json_equal(x, y),
            _ => false,
        };
        let twin_node = pair.twin_node.unwrap_or(0);
        let hard = IMMUTABLE_METHODS.contains(&pair.method.as_str());
        if equal {
            self.registry.audit_result(pair.primary_node, twin_node, crate::registry::AuditVerdict::Pass);
        } else if hard {
            tracing::warn!(
                method = %pair.method, path = %pair.path,
                node_a = %self.registry.name_of(pair.primary_node),
                node_b = %self.registry.name_of(twin_node),
                "AUDIT MISMATCH on immutable endpoint — one of these nodes returned wrong data"
            );
            self.registry.audit_result(pair.primary_node, twin_node, crate::registry::AuditVerdict::HardFail);
        } else {
            tracing::debug!(
                method = %pair.method, path = %pair.path,
                "audit soft mismatch (volatile endpoint)"
            );
            self.registry.audit_result(pair.primary_node, twin_node, crate::registry::AuditVerdict::SoftFail);
        }
    }

    /// Periodic maintenance: re-queue expired leases, expire stale twins.
    /// Takes `now` so tests can fast-forward time.
    pub fn sweep(&self, now: Instant) {
        let mut woke = false;
        {
            let mut inner = self.inner.lock().unwrap();
            let expired: Vec<u64> = inner
                .jobs
                .iter()
                .filter(|(_, j)| j.lease.is_some_and(|(_, exp)| exp <= now))
                .map(|(id, _)| *id)
                .collect();
            for id in expired {
                tracing::debug!(job = id, "lease expired, re-queueing");
                Self::release(&mut inner, id, true);
                woke = true;
            }
            let stale_twins: Vec<u64> = inner
                .jobs
                .iter()
                .filter(|(_, j)| {
                    j.is_twin && j.lease.is_none() && now.duration_since(j.created) > TWIN_TTL
                })
                .map(|(id, _)| *id)
                .collect();
            for id in stale_twins {
                inner.jobs.remove(&id);
                inner.audits.remove(&id);
            }
        }
        if woke {
            self.notify.notify_waiters();
        }
    }

    /// Resolves every pending fetch with Err and stops accepting new ones.
    /// Called on ctrl-c so region crawlers can drain instead of hanging on
    /// fetches no node will ever serve.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        let mut inner = self.inner.lock().unwrap();
        for (_, job) in inner.jobs.drain() {
            if let Some(tx) = job.responder {
                let _ = tx.send(Err(anyhow::anyhow!("server shutting down")));
            }
        }
        inner.queues.clear();
        inner.audits.clear();
        drop(inner);
        self.notify.notify_waiters();
    }

    /// (queued, leased) job counts, for the status report.
    pub fn depth(&self) -> (usize, usize) {
        let inner = self.inner.lock().unwrap();
        let leased = inner.jobs.values().filter(|j| j.lease.is_some()).count();
        (inner.jobs.len() - leased, leased)
    }
}

/// Structural JSON comparison so formatting differences can't false-alarm;
/// falls back to byte equality for non-JSON bodies.
fn json_equal(a: &str, b: &str) -> bool {
    match (
        serde_json::from_str::<serde_json::Value>(a),
        serde_json::from_str::<serde_json::Value>(b),
    ) {
        (Ok(x), Ok(y)) => x == y,
        _ => a == b,
    }
}

/// Spawns the lease sweeper; returns its handle.
pub fn spawn_sweeper(
    broker: std::sync::Arc<Broker>,
    mut stop: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(15)) => {}
                _ = stop.changed() => return,
            }
            broker.sweep(Instant::now());
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::Registry;
    use std::sync::Arc;

    fn setup(audit_percent: f64) -> (Arc<Registry>, Arc<Broker>) {
        let registry = Arc::new(Registry::new());
        registry.enroll_runtime(1, "alice".into(), "h1".into());
        registry.enroll_runtime(2, "bob".into(), "h2".into());
        registry.touch(1);
        registry.touch(2);
        (registry.clone(), Arc::new(Broker::new(registry, audit_percent)))
    }

    fn ok_result(id: u64, body: &str) -> crawler_proto::JobResult {
        crawler_proto::JobResult {
            id,
            outcome: crawler_proto::JobOutcome::Ok,
            body: Some(body.to_string()),
            error: None,
        }
    }

    #[tokio::test]
    async fn fetch_roundtrip() {
        let (_reg, broker) = setup(0.0);
        let b2 = broker.clone();
        let fut = tokio::spawn(async move { b2.fetch("europe", "match-v5.match", "/x".into()).await });
        tokio::task::yield_now().await;
        let jobs = broker.take_jobs(1, &HashMap::new());
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].host, "europe");
        broker.complete(1, ok_result(jobs[0].id, "{\"a\":1}"));
        let got = fut.await.unwrap().unwrap();
        assert_eq!(got.as_deref(), Some("{\"a\":1}"));
    }

    #[tokio::test]
    async fn audit_twin_goes_to_other_node_and_mismatch_is_counted() {
        let (reg, broker) = setup(100.0); // every job audited
        let b2 = broker.clone();
        let fut =
            tokio::spawn(async move { b2.fetch("europe", "match-v5.match", "/m/1".into()).await });
        tokio::task::yield_now().await;

        let jobs1 = broker.take_jobs(1, &HashMap::new());
        assert_eq!(jobs1.len(), 1, "primary only; twin created after dispatch");
        // Twin must not be dispatched to node 1.
        assert!(broker.take_jobs(1, &HashMap::new()).is_empty());
        let jobs2 = broker.take_jobs(2, &HashMap::new());
        assert_eq!(jobs2.len(), 1, "twin goes to the other node");
        assert_eq!(jobs2[0].path, "/m/1");

        broker.complete(1, ok_result(jobs1[0].id, "{\"winner\":\"blue\"}"));
        assert_eq!(fut.await.unwrap().unwrap().as_deref(), Some("{\"winner\":\"blue\"}"));
        broker.complete(2, ok_result(jobs2[0].id, "{\"winner\":\"red\"}"));

        let reports = reg.report();
        for r in reports {
            if r.name == "alice" || r.name == "bob" {
                assert_eq!(r.audits_hard_fail, 1, "{} should be flagged", r.name);
            }
        }
    }

    #[tokio::test]
    async fn expired_lease_requeues_for_other_nodes() {
        let (_reg, broker) = setup(0.0);
        let b2 = broker.clone();
        let fut = tokio::spawn(async move { b2.fetch("americas", "match-v5.ids", "/l".into()).await });
        tokio::task::yield_now().await;
        let jobs = broker.take_jobs(1, &HashMap::new());
        assert_eq!(jobs.len(), 1);
        // Nothing else to hand out while leased.
        assert!(broker.take_jobs(2, &HashMap::new()).is_empty());
        broker.sweep(Instant::now() + Duration::from_millis(config::LEASE_MS + 1000));
        let jobs2 = broker.take_jobs(2, &HashMap::new());
        assert_eq!(jobs2.len(), 1);
        assert_eq!(jobs2[0].id, jobs[0].id);
        broker.complete(2, ok_result(jobs2[0].id, "[]"));
        assert_eq!(fut.await.unwrap().unwrap().as_deref(), Some("[]"));
    }

    #[tokio::test]
    async fn shutdown_resolves_pending_fetches() {
        let (_reg, broker) = setup(0.0);
        let b2 = broker.clone();
        let fut = tokio::spawn(async move { b2.fetch("asia", "match-v5.match", "/x".into()).await });
        tokio::task::yield_now().await;
        broker.shutdown();
        assert!(fut.await.unwrap().is_err());
    }
}
