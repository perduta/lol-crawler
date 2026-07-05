//! crawler-node core: a dumb, rate-limit-maximizing Riot API fetcher that
//! serves opaque jobs pulled from a crawler-server.
//!
//! This library is frontend-agnostic: the CLI binary in this crate and the
//! desktop app both drive [`worker::run`], observing it through
//! [`events::NodeHandle`]. Nothing in here prints, prompts, or exits.

pub mod config;
pub mod events;
pub mod executor;
pub mod ratelimit;
pub mod worker;

use anyhow::{Context, Result, bail};
use crawler_proto as proto;

/// One-shot enrollment call: trades an invite code for a bearer token.
/// The caller assembles and saves the [`config::NodeConfig`].
pub async fn enroll_request(
    server: &str,
    name: &str,
    invite_code: &str,
) -> Result<proto::EnrollResponse> {
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;
    let resp = http
        .post(format!("{}/v1/enroll", server.trim_end_matches('/')))
        .header(proto::PROTO_HEADER, proto::PROTOCOL_VERSION.to_string())
        .json(&proto::EnrollRequest {
            invite_code: invite_code.to_string(),
            name: name.to_string(),
            client_version: env!("CARGO_PKG_VERSION").to_string(),
        })
        .send()
        .await
        .context("connecting to server")?;
    let status = resp.status();
    if !status.is_success() {
        let msg = resp
            .json::<proto::ErrorResponse>()
            .await
            .map(|e| e.message)
            .unwrap_or_else(|_| status.to_string());
        bail!("enrollment failed: {msg}");
    }
    Ok(resp.json().await?)
}
