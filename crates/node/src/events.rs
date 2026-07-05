//! Observability surface of a running node, shared by every frontend:
//! the CLI ignores most of it, the desktop app turns it into the live
//! visualization and its counters survive webview teardown/recreate.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde::Serialize;
use tokio::sync::{Notify, broadcast};

/// Everything interesting a node does, in the order it happens. Cheap to
/// clone; receivers that lag simply miss frames (fine for a UI).
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NodeEvent {
    /// First successful poll (or first after an outage).
    Connected,
    /// A poll failed; the node keeps retrying by itself.
    Disconnected,
    /// Pulled from the server; waiting on the local rate limiter.
    JobStarted { id: u64, host: String, method: String },
    /// Limiter permits acquired — the Riot request is going out now.
    JobActive { id: u64 },
    /// Riot answered (the result still has to reach the server).
    JobDone { id: u64, host: String, method: String, outcome: String, ms: u64 },
    /// Result delivered to (or given up on) the server.
    JobUploaded { id: u64 },
    /// Riot rejected the API key; the node paused.
    KeyBad,
    /// A fresh key worked (or was accepted); the node resumed.
    KeyOk,
    /// Server speaks a newer protocol; this build must be updated.
    ProtocolMismatch { message: String },
    Stopped,
}

/// Counters a frontend can read at any moment (e.g. to repopulate a
/// freshly recreated window without replaying events).
#[derive(Debug, Default, Clone, Serialize)]
pub struct Snapshot {
    pub completed: u64,
    pub matches: u64,
    /// Jobs finished per routing host.
    pub per_host: HashMap<String, u64>,
    pub key_bad: bool,
    pub connected: bool,
}

/// Shared handle between a running [`crate::worker::run`] and its frontend.
pub struct NodeHandle {
    pub events: broadcast::Sender<NodeEvent>,
    /// Signal that the config file holds a new API key (skips the mtime
    /// polling delay while paused).
    pub key_update: Notify,
    pub completed: AtomicU64,
    pub matches: AtomicU64,
    per_host: Mutex<HashMap<String, u64>>,
    pub key_bad: AtomicBool,
    pub connected: AtomicBool,
}

impl NodeHandle {
    pub fn new() -> Arc<Self> {
        let (events, _) = broadcast::channel(512);
        Arc::new(Self {
            events,
            key_update: Notify::new(),
            completed: AtomicU64::new(0),
            matches: AtomicU64::new(0),
            per_host: Mutex::new(HashMap::new()),
            key_bad: AtomicBool::new(false),
            connected: AtomicBool::new(false),
        })
    }

    /// Send-and-forget: an event with no listeners is not an error.
    pub fn emit(&self, ev: NodeEvent) {
        let _ = self.events.send(ev);
    }

    pub fn job_finished(&self, host: &str, is_match: bool) {
        self.completed.fetch_add(1, Ordering::Relaxed);
        if is_match {
            self.matches.fetch_add(1, Ordering::Relaxed);
        }
        *self.per_host.lock().unwrap().entry(host.to_string()).or_default() += 1;
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            completed: self.completed.load(Ordering::Relaxed),
            matches: self.matches.load(Ordering::Relaxed),
            per_host: self.per_host.lock().unwrap().clone(),
            key_bad: self.key_bad.load(Ordering::Relaxed),
            connected: self.connected.load(Ordering::Relaxed),
        }
    }
}
