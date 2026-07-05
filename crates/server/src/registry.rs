//! Runtime node registry: auth (token-hash -> node), liveness, per-node
//! counters (completed jobs, audit verdicts, key state). Persistent node
//! records live in redb ([`crate::storage`]); this is the in-RAM view the
//! API and broker touch on every request.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

/// A node counts as "active" (eligible as an audit counterpart) if it
/// polled within this window.
const ACTIVE_WINDOW: Duration = Duration::from_secs(600);

pub enum AuditVerdict {
    Pass,
    /// Immutable endpoint, bodies differ: someone returned wrong data.
    HardFail,
    /// Volatile endpoint, bodies differ: expected occasionally.
    SoftFail,
}

#[derive(Default)]
struct NodeRt {
    name: String,
    last_seen: Option<Instant>,
    completed: u64,
    audits_pass: u64,
    audits_hard_fail: u64,
    audits_soft_fail: u64,
    key_bad: bool,
}

#[derive(Clone, Debug)]
pub struct NodeReport {
    pub name: String,
    pub completed: u64,
    pub audits_pass: u64,
    pub audits_hard_fail: u64,
    pub audits_soft_fail: u64,
    pub key_bad: bool,
    /// Seconds since last poll; None = never seen this run.
    pub idle_secs: Option<u64>,
}

#[derive(Default)]
struct Inner {
    by_token_hash: HashMap<String, u32>,
    nodes: HashMap<u32, NodeRt>,
}

#[derive(Default)]
pub struct Registry {
    inner: RwLock<Inner>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a node in the runtime view (startup load + fresh enrolls).
    pub fn enroll_runtime(&self, id: u32, name: String, token_hash_hex: String) {
        let mut inner = self.inner.write().unwrap();
        inner.by_token_hash.insert(token_hash_hex, id);
        inner.nodes.insert(id, NodeRt { name, ..Default::default() });
    }

    pub fn name_taken(&self, name: &str) -> bool {
        let inner = self.inner.read().unwrap();
        inner.nodes.values().any(|n| n.name == name)
    }

    /// Bearer-token auth: returns the node id for a valid token.
    pub fn auth(&self, token: &str) -> Option<u32> {
        let hash = crate::token_hash_hex(token);
        self.inner.read().unwrap().by_token_hash.get(&hash).copied()
    }

    pub fn touch(&self, id: u32) {
        if let Some(n) = self.inner.write().unwrap().nodes.get_mut(&id) {
            n.last_seen = Some(Instant::now());
        }
    }

    pub fn job_completed(&self, id: u32) {
        let mut inner = self.inner.write().unwrap();
        if let Some(n) = inner.nodes.get_mut(&id) {
            n.completed += 1;
            // A completed Riot request proves the key works again.
            n.key_bad = false;
        }
    }

    pub fn set_key_bad(&self, id: u32, bad: bool) {
        if let Some(n) = self.inner.write().unwrap().nodes.get_mut(&id) {
            n.key_bad = bad;
        }
    }

    /// Both participants of an audit get the verdict: on a mismatch we
    /// can't know who lied, so both counters rise and the operator looks
    /// for the node that accumulates fails across many partners.
    pub fn audit_result(&self, a: u32, b: u32, verdict: AuditVerdict) {
        let mut inner = self.inner.write().unwrap();
        for id in [a, b] {
            if let Some(n) = inner.nodes.get_mut(&id) {
                match verdict {
                    AuditVerdict::Pass => n.audits_pass += 1,
                    AuditVerdict::HardFail => n.audits_hard_fail += 1,
                    AuditVerdict::SoftFail => n.audits_soft_fail += 1,
                }
            }
        }
    }

    /// Is any *other* node active? (Precondition for creating audit twins:
    /// with one node there is nobody to cross-check against.)
    pub fn active_node_other_than(&self, id: u32) -> bool {
        let inner = self.inner.read().unwrap();
        let now = Instant::now();
        inner.nodes.iter().any(|(nid, n)| {
            *nid != id && n.last_seen.is_some_and(|t| now.duration_since(t) < ACTIVE_WINDOW)
        })
    }

    pub fn name_of(&self, id: u32) -> String {
        self.inner
            .read()
            .unwrap()
            .nodes
            .get(&id)
            .map(|n| n.name.clone())
            .unwrap_or_else(|| format!("node#{id}"))
    }

    pub fn report(&self) -> Vec<NodeReport> {
        let inner = self.inner.read().unwrap();
        let now = Instant::now();
        let mut v: Vec<NodeReport> = inner
            .nodes
            .values()
            .map(|n| NodeReport {
                name: n.name.clone(),
                completed: n.completed,
                audits_pass: n.audits_pass,
                audits_hard_fail: n.audits_hard_fail,
                audits_soft_fail: n.audits_soft_fail,
                key_bad: n.key_bad,
                idle_secs: n.last_seen.map(|t| now.duration_since(t).as_secs()),
            })
            .collect();
        v.sort_by(|a, b| a.name.cmp(&b.name));
        v
    }
}
