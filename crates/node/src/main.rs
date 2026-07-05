//! crawler-node: a dumb, rate-limit-maximizing Riot API fetcher.
//!
//! Connects to a crawler-server, enrolls once with an invite code (the
//! server issues a token saved locally), then pulls opaque GET jobs,
//! executes them with the operator's own Riot API key at full budget, and
//! uploads the bodies. It knows nothing about the crawl strategy, so
//! server-side changes never require a node update.
//!
//! Usage:
//!   crawler-node --server http://host:8420    first run (enrollment)
//!   crawler-node                              subsequent runs
//!   crawler-node set-key                      update the Riot API key

mod config;
mod executor;
mod ratelimit;
mod worker;

use std::io::Write;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use crawler_proto as proto;

fn prompt(question: &str) -> Result<String> {
    print!("{question}: ");
    std::io::stdout().flush()?;
    let mut line = String::new();
    std::io::stdin().read_line(&mut line)?;
    Ok(line.trim().to_string())
}

struct Args {
    server: Option<String>,
    config_path: PathBuf,
    set_key: bool,
}

fn parse_args() -> Result<Args> {
    let mut args = Args {
        server: None,
        config_path: config::default_path(),
        set_key: false,
    };
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--server" => args.server = Some(it.next().context("--server needs a URL")?),
            "--config" => {
                args.config_path = PathBuf::from(it.next().context("--config needs a path")?)
            }
            "set-key" => args.set_key = true,
            "--help" | "-h" => {
                println!(
                    "crawler-node [--server URL] [--config PATH] [set-key]\n\
                     First run needs --server and an invite code from the server operator."
                );
                std::process::exit(0);
            }
            other => bail!("unknown argument {other:?} (try --help)"),
        }
    }
    Ok(args)
}

async fn enroll(server: &str, config_path: &PathBuf) -> Result<config::NodeConfig> {
    println!("First run — enrolling with {server}");
    let default_name = std::env::var("USER").unwrap_or_else(|_| "node".into());
    let mut name = prompt(&format!("node name [{default_name}]"))?;
    if name.is_empty() {
        name = default_name;
    }
    let invite_code = prompt("invite code (ask the server operator)")?;
    let riot_api_key = prompt("your Riot API key (RGAPI-..., from developer.riotgames.com)")?;

    let http = reqwest::Client::new();
    let resp = http
        .post(format!("{}/v1/enroll", server.trim_end_matches('/')))
        .header(proto::PROTO_HEADER, proto::PROTOCOL_VERSION.to_string())
        .json(&proto::EnrollRequest {
            invite_code,
            name: name.clone(),
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
    let er: proto::EnrollResponse = resp.json().await?;

    let cfg = config::NodeConfig {
        server: server.trim_end_matches('/').to_string(),
        name: er.name,
        token: er.token,
        riot_api_key,
    };
    config::save(config_path, &cfg)?;
    println!("enrolled as '{}'; config saved to {}", cfg.name, config_path.display());
    Ok(cfg)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = parse_args()?;

    if args.set_key {
        let mut cfg = config::load(&args.config_path)?
            .context("no config yet — run with --server first to enroll")?;
        cfg.riot_api_key = prompt("new Riot API key (RGAPI-...)")?;
        config::save(&args.config_path, &cfg)?;
        println!("key updated; a running node picks it up within ~15s.");
        return Ok(());
    }

    let cfg = match config::load(&args.config_path)? {
        Some(mut cfg) => {
            // Allow pointing an existing enrollment at a moved server.
            if let Some(s) = args.server {
                cfg.server = s.trim_end_matches('/').to_string();
                config::save(&args.config_path, &cfg)?;
            }
            cfg
        }
        None => {
            let server = args.server.clone().context(
                "no config found — first run needs --server http://host:8420 \
                 (and an invite code from the server operator)",
            )?;
            enroll(&server, &args.config_path).await?
        }
    };

    worker::run(cfg, args.config_path).await
}
