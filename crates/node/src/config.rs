//! Node-local config: server address, node name, bearer token (issued by
//! the server at enrollment), and the operator's own Riot API key. Lives at
//! `~/.config/crawler-node/config.json` unless `--config` says otherwise.

use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    pub server: String,
    pub name: String,
    pub token: String,
    pub riot_api_key: String,
}

pub fn default_path() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("crawler-node")
        .join("config.json")
}

pub fn load(path: &PathBuf) -> Result<Option<NodeConfig>> {
    if !path.exists() {
        return Ok(None);
    }
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("reading {}", path.display()))?;
    Ok(Some(serde_json::from_str(&content).with_context(|| {
        format!("parsing {} (delete it to re-enroll)", path.display())
    })?))
}

pub fn save(path: &PathBuf, cfg: &NodeConfig) -> Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::write(path, serde_json::to_string_pretty(cfg)?)?;
    // Token + API key inside: keep it private on unix.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

/// File modification time, for detecting an out-of-band key update while
/// the node is paused on a rejected key.
pub fn mtime(path: &PathBuf) -> Option<std::time::SystemTime> {
    std::fs::metadata(path).and_then(|m| m.modified()).ok()
}
