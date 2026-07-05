//! One-time invite codes for node enrollment, stored as plain lines
//! (`CODE<TAB>label`) in `{data_dir}/invites.txt` so `crawler-server
//! invite` works from the CLI whether or not the server is running (redb
//! is single-process; a text file isn't).

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Result;
use rand::Rng;

fn path(data_dir: &str) -> PathBuf {
    Path::new(data_dir).join("invites.txt")
}

/// Unambiguous alphabet (no 0/O, 1/I/L).
const ALPHABET: &[u8] = b"ABCDEFGHJKMNPQRSTUVWXYZ23456789";

pub fn create(data_dir: &str, label: &str) -> Result<String> {
    fs::create_dir_all(data_dir)?;
    let mut rng = rand::rng();
    let code: String = (0..10)
        .map(|_| ALPHABET[rng.random_range(0..ALPHABET.len())] as char)
        .collect();
    let mut content = fs::read_to_string(path(data_dir)).unwrap_or_default();
    if !content.is_empty() && !content.ends_with('\n') {
        content.push('\n');
    }
    content.push_str(&format!("{code}\t{label}\n"));
    fs::write(path(data_dir), content)?;
    Ok(code)
}

/// Consumes `code` if present: removes it from the file and returns true.
pub fn consume(data_dir: &str, code: &str) -> Result<bool> {
    let p = path(data_dir);
    let content = match fs::read_to_string(&p) {
        Ok(c) => c,
        Err(_) => return Ok(false),
    };
    let mut found = false;
    let kept: Vec<&str> = content
        .lines()
        .filter(|line| {
            let matches = line.split('\t').next() == Some(code.trim());
            if matches {
                found = true;
            }
            !matches
        })
        .collect();
    if found {
        let mut out = kept.join("\n");
        if !out.is_empty() {
            out.push('\n');
        }
        fs::write(&p, out)?;
    }
    Ok(found)
}
