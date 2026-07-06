# Releasing the desktop app (Crawl Crew)

Windows installers are published on GitHub Releases and installed apps
self-update from there: on launch (and every six hours in the tray) the app
fetches `releases/latest/download/latest.json`, and if it lists a newer
version, downloads the NSIS installer, verifies its updater signature, and
reinstalls in passive mode.

## Cutting a release

1. `sh scripts/release.sh 0.2.0` — sets `version` in
   `crates/desktop/Cargo.toml` (the only place — `tauri.conf.json`
   deliberately has no version field), syncs `Cargo.lock`, commits, and
   tags `v0.2.0`. It never pushes.
2. Release it: `git push origin main v0.2.0`.
3. The `release` workflow (`.github/workflows/release.yml`) builds the NSIS
   installer on `windows-latest`, signs the update artifact, and publishes
   the release with `latest.json`. Fleet nodes pick it up within six hours.

The tag name is cosmetic (release title); the updater compares the version
from `Cargo.toml` baked into `latest.json`, so keep them matching to avoid
confusion.

## Keys and secrets

- **Updater signing key** (minisign, not Authenticode): private key lives in
  the `TAURI_SIGNING_PRIVATE_KEY` repo secret, public key is baked into
  `tauri.conf.json` (`plugins.updater.pubkey`). Losing the private key means
  shipped apps reject all future updates — keep the offline backup. The key
  has no password (`TAURI_SIGNING_PRIVATE_KEY_PASSWORD` is empty in the
  workflow).
- **Windows code signing (Authenticode)** is not set up yet, so first-time
  installs still hit SmartScreen; self-updates are unaffected. When a cert
  exists, wire it into `bundle.windows.signCommand` in `tauri.conf.json`.

## Caveats

- Only desktop releases may use `v*` tags: the updater endpoint points at
  the repo's *latest* release, so a `v*`-tagged release without updater
  artifacts would break update checks until the next desktop release.
- Dev builds (`cargo run`, debug or `--mock`) never self-update.
