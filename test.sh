#!/bin/sh

cargo neon endpoint stop ep-main 2>/dev/null
kill $(pgrep compute_ctl)
cargo neon stop 2>/dev/null
rm -rf .neon
cargo neon init
cargo neon start
cargo neon tenant create --pg-version 15 --set-default
cargo neon endpoint create --pg-version 15 --upgrade-only
cargo neon endpoint start ep-main
curl -i -X POST http://localhost:55433/upgrade -d '{"pg_version": "16"}'
