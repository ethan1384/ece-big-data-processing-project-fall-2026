#!/usr/bin/env bash
# Pull the seven public IMDb dumps. Non-commercial use only.
# https://datasets.imdbws.com/
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
mkdir -p "$root/data/raw"

base="https://datasets.imdbws.com"
files=(
  name.basics.tsv.gz
  title.akas.tsv.gz
  title.basics.tsv.gz
  title.crew.tsv.gz
  title.episode.tsv.gz
  title.principals.tsv.gz
  title.ratings.tsv.gz
)

for f in "${files[@]}"; do
  dest="$root/data/raw/$f"
  if [[ -s "$dest" ]]; then
    echo "skip $f"
    continue
  fi
  echo "get $f"
  curl -fL --retry 3 --retry-delay 2 -o "$dest.partial" "$base/$f"
  mv "$dest.partial" "$dest"
done

echo "done"
ls -lh "$root/data/raw"
