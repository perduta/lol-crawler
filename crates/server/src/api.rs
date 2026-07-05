//! HTTP API for crawler nodes: enroll, poll work (long-poll), upload
//! results. Everything else — parsing, storing, scheduling — is server
//! internals the nodes never see.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Json;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use crawler_proto as proto;
use tokio::sync::Mutex;

use crate::broker::Broker;
use crate::registry::Registry;
use crate::storage::Store;

/// Long-poll ceiling; nodes may ask for less.
const MAX_WAIT_MS: u64 = 30_000;
/// Timeline bodies run to a few MB of JSON.
const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone)]
pub struct AppState {
    pub broker: Arc<Broker>,
    pub registry: Arc<Registry>,
    pub stats: Arc<crate::stats::Stats>,
    pub store: Arc<Mutex<Store>>,
    pub data_dir: String,
}

pub fn router(state: AppState) -> axum::Router {
    axum::Router::new()
        .route("/v1/enroll", post(enroll))
        .route("/v1/work", post(work))
        .route("/v1/result", post(result))
        .route("/v1/stats", post(stats))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(state)
}

fn err(status: StatusCode, message: &str) -> Response {
    (status, Json(proto::ErrorResponse { message: message.to_string() })).into_response()
}

/// Additive protocol changes don't bump the version; a real break makes
/// old nodes print this message and exit instead of misbehaving.
fn check_proto(headers: &HeaderMap) -> Result<(), Response> {
    let v = headers
        .get(proto::PROTO_HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u32>().ok());
    if v == Some(proto::PROTOCOL_VERSION) {
        Ok(())
    } else {
        Err(err(
            StatusCode::UPGRADE_REQUIRED,
            &format!(
                "protocol version mismatch (server speaks v{}); please update crawler-node",
                proto::PROTOCOL_VERSION
            ),
        ))
    }
}

fn auth(headers: &HeaderMap, registry: &Registry) -> Result<u32, Response> {
    let token = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    match token.and_then(|t| registry.auth(t)) {
        Some(id) => Ok(id),
        None => Err(err(StatusCode::UNAUTHORIZED, "invalid or unknown token")),
    }
}

fn valid_name(name: &str) -> bool {
    (1..=32).contains(&name.len())
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
}

async fn enroll(
    State(st): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<proto::EnrollRequest>,
) -> Response {
    if let Err(r) = check_proto(&headers) {
        return r;
    }
    if !valid_name(&req.name) {
        return err(StatusCode::BAD_REQUEST, "name must be [a-z0-9_-]{1,32}");
    }
    if st.registry.name_taken(&req.name) {
        return err(StatusCode::CONFLICT, "node name already taken on this server");
    }
    match crate::invites::consume(&st.data_dir, &req.invite_code) {
        Ok(true) => {}
        Ok(false) => return err(StatusCode::FORBIDDEN, "invalid or already-used invite code"),
        Err(e) => {
            tracing::error!(error = %e, "invite check failed");
            return err(StatusCode::INTERNAL_SERVER_ERROR, "invite check failed");
        }
    }

    let token = crate::generate_token();
    let hash = crate::token_hash_hex(&token);
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let id = {
        let mut store = st.store.lock().await;
        match store.node_add(&req.name, &hash, now_ms) {
            Ok(id) => id,
            Err(e) => {
                tracing::error!(error = %e, "node persist failed");
                return err(StatusCode::INTERNAL_SERVER_ERROR, "could not persist node");
            }
        }
    };
    st.registry.enroll_runtime(id, req.name.clone(), hash);
    tracing::info!(node = %req.name, id, version = %req.client_version, "node enrolled");
    Json(proto::EnrollResponse { token, name: req.name }).into_response()
}

async fn work(
    State(st): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<proto::WorkRequest>,
) -> Response {
    if let Err(r) = check_proto(&headers) {
        return r;
    }
    let node_id = match auth(&headers, &st.registry) {
        Ok(id) => id,
        Err(r) => return r,
    };
    st.registry.touch(node_id);

    let pending: HashMap<String, u32> = req.pending;
    let deadline = Instant::now() + Duration::from_millis(req.wait_ms.min(MAX_WAIT_MS));
    loop {
        if st.broker.is_shutdown() {
            return Json(proto::WorkResponse { jobs: vec![] }).into_response();
        }
        let jobs = st.broker.take_jobs(node_id, &pending);
        if !jobs.is_empty() || Instant::now() >= deadline {
            return Json(proto::WorkResponse { jobs }).into_response();
        }
        tokio::select! {
            _ = st.broker.notify.notified() => {}
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {}
        }
    }
}

async fn result(
    State(st): State<AppState>,
    headers: HeaderMap,
    Json(res): Json<proto::JobResult>,
) -> Response {
    if let Err(r) = check_proto(&headers) {
        return r;
    }
    let node_id = match auth(&headers, &st.registry) {
        Ok(id) => id,
        Err(r) => return r,
    };
    st.registry.touch(node_id);
    st.broker.complete(node_id, res);
    // A JSON body, not a bare 200: the node parses the response.
    Json(serde_json::json!({"ok": true})).into_response()
}

/// Leaderboard for GUI nodes. A node polling counts as "online" if it hit
/// any endpoint within two long-poll cycles.
async fn stats(State(st): State<AppState>, headers: HeaderMap) -> Response {
    if let Err(r) = check_proto(&headers) {
        return r;
    }
    let node_id = match auth(&headers, &st.registry) {
        Ok(id) => id,
        Err(r) => return r,
    };

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let windows: HashMap<u32, crate::stats::NodeWindows> = st
        .stats
        .snapshot(now_ms)
        .into_iter()
        .map(|w| (w.node_id, w))
        .collect();

    let counts = |b: &crate::stats::NodeWindows| {
        let req = proto::WindowCounts {
            m60: b.m60.requests,
            h24: b.h24.requests,
            d7: b.d7.requests,
            all: b.all.requests,
        };
        let mat = proto::WindowCounts {
            m60: b.m60.matches,
            h24: b.h24.matches,
            d7: b.d7.matches,
            all: b.all.matches,
        };
        (req, mat)
    };

    let mut nodes: Vec<proto::NodeStatsEntry> = st
        .registry
        .report()
        .into_iter()
        .map(|r| {
            let (requests, matches) = windows
                .get(&r.id)
                .map(counts)
                .unwrap_or_default();
            proto::NodeStatsEntry {
                name: r.name,
                online: r.idle_secs.is_some_and(|s| s < 120),
                requests,
                matches,
            }
        })
        .collect();
    nodes.sort_by(|a, b| b.requests.all.cmp(&a.requests.all).then(a.name.cmp(&b.name)));

    Json(proto::StatsResponse {
        you: st.registry.name_of(node_id),
        nodes,
        generated_ms: now_ms,
    })
    .into_response()
}
