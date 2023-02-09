#!/usr/bin/env bash
# usage: poetry run -- ./run.bash scraper.env ARGS...
set -euo pipefail
set -x

source "$1"
shift

self="${BASH_SOURCE[0]}"
dir="$(dirname "$self")"
scraper="$dir/scraper.py"
exec python "$scraper" "$@" 1>run.log 2>&1
