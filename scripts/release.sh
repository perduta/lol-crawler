#!/usr/bin/env bash
# Cut a Crawl Crew desktop release: set the version in
# crates/desktop/Cargo.toml, sync Cargo.lock, commit, and tag vX.Y.Z.
# Never pushes — release when ready with:  git push origin main vX.Y.Z
#
# If <version> equals the current version (first release, or the bump
# was already committed), it just creates the tag.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

ver="${1:-}"
if ! [[ "$ver" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "usage: scripts/release.sh <version>    e.g. scripts/release.sh 0.2.0" >&2
  exit 1
fi

if git rev-parse -q --verify "refs/tags/v$ver" >/dev/null; then
  echo "error: tag v$ver already exists" >&2
  exit 1
fi

toml=crates/desktop/Cargo.toml
cur="$(sed -n 's/^version = "\(.*\)"$/\1/p' "$toml" | head -1)"

if [[ "$cur" != "$ver" ]]; then
  if [[ -n "$(git status --porcelain)" ]]; then
    echo "error: working tree not clean — commit or stash first" >&2
    exit 1
  fi
  sed -i "0,/^version = \".*\"\$/s//version = \"$ver\"/" "$toml"
  cargo update --workspace --quiet
  git commit -q -m "Release Crawl Crew v$ver" "$toml" Cargo.lock
  echo "Bumped $cur -> $ver and committed."
fi

git tag "v$ver"
echo "Tagged v$ver. Release it with:  git push origin main v$ver"
