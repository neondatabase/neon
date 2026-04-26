#!/usr/bin/env bash
# Sweep pgbench across 3 configs (direct, proxy+pool, proxy no pool),
# 2 workloads (tpcb, readonly), and 6 concurrencies, 3 reps each.
#
# Prereqs:
#   - compute-A docker postgres on 127.0.0.1:5433 with pgbench scale=10 initialized
#   - auth Postgres on 127.0.0.1:5432 with proxytest user / SCRAM verifier
#   - /Users/mel/neon/target/debug/proxy built (with the testing feature)
#
# Output:
#   benchmarks/results.csv          one row per (config,workload,concurrency,rep)
#   benchmarks/mem_<config>.txt     proxy ps snapshot at peak load (C=100)
#   benchmarks/logs/                pgbench -l logs and proxy stdout/stderr
#
# Override DURATION=30 with env var if needed.

set -euo pipefail

BENCH_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULTS="$BENCH_DIR/results.csv"
LOGDIR="$BENCH_DIR/logs"
PARSE="$BENCH_DIR/parse_pcts.py"
PROGRESS="$BENCH_DIR/progress.txt"
PROXY_BIN=/Users/mel/neon/target/debug/proxy

mkdir -p "$LOGDIR"

# By default the script truncates results.csv and progress.txt on startup so
# a sweep gives a clean output. APPEND_RESULTS=1 keeps the existing rows —
# useful when running a partial sweep on top of an earlier one.
APPEND_RESULTS="${APPEND_RESULTS:-0}"
if [[ "$APPEND_RESULTS" != "1" ]]; then
    : > "$PROGRESS"
fi

DURATION="${DURATION:-30}"
USER=proxytest
DB=proxytest_db
PASS=testpw

# config_name|host|port|extra_url
CONFIGS=(
    "direct|127.0.0.1|5433|"
    "proxy_pool|127.0.0.1|4432|&options=endpoint%3Dep-test-123"
    "proxy_nopool|127.0.0.1|4432|&options=endpoint%3Dep-test-123"
    "proxy_txn|127.0.0.1|4432|&options=endpoint%3Dep-test-123"
)

# workload_name|pgbench_flag
# - tpcb_steady:        default tpc-b mix, persistent sessions
# - readonly_steady:    SELECT-only, persistent sessions
# - readonly_short:     SELECT-only with -C (reconnect per transaction).
#                       This is the case where the connection pool can win.
WORKLOADS=(
    "tpcb_steady|"
    "readonly_steady|-S"
    "readonly_short|-S -C"
)

CONCURRENCIES=(1 5 10 25 50 100)
REPS="${REPS:-3}"

# Allow override via env: CONCURRENCIES_OVERRIDE="1 10" REPS=1 DURATION=3
if [[ -n "${CONCURRENCIES_OVERRIDE:-}" ]]; then
    read -r -a CONCURRENCIES <<< "$CONCURRENCIES_OVERRIDE"
fi

# CONFIGS_OVERRIDE="proxy_pool proxy_nopool" picks a subset of CONFIGS by name.
if [[ -n "${CONFIGS_OVERRIDE:-}" ]]; then
    selected=()
    for want in $CONFIGS_OVERRIDE; do
        for entry in "${CONFIGS[@]}"; do
            if [[ "${entry%%|*}" == "$want" ]]; then
                selected+=("$entry")
            fi
        done
    done
    CONFIGS=("${selected[@]}")
fi

if [[ "$APPEND_RESULTS" != "1" || ! -s "$RESULTS" ]]; then
    echo "config,workload,concurrency,rep,tps,p50_ms,p95_ms,p99_ms,latency_avg_ms,init_conn_ms" > "$RESULTS"
fi

start_proxy() {
    local enabled=$1
    local mode="${2:-session}"
    pkill -f "target/debug/proxy" 2>/dev/null || true
    sleep 0.5
    while lsof -iTCP:4432 -sTCP:LISTEN >/dev/null 2>&1; do sleep 0.2; done

    RUST_LOG="proxy=warn" PGPASSWORD=$PASS "$PROXY_BIN" \
        --auth-backend=postgres \
        --auth-endpoint='postgresql://proxytest@localhost:5432/proxytest_db' \
        --compute-endpoint='postgresql://localhost:5433/proxytest_db?sslmode=disable' \
        --proxy=127.0.0.1:4432 --mgmt=127.0.0.1:7000 --http=127.0.0.1:7001 --wss=127.0.0.1:7002 \
        --tcp-pool-enabled=$enabled \
        --tcp-pool-mode=$mode \
        --tcp-pool-max-conns-per-key=200 \
        --tcp-pool-max-total-conns=200 \
        --tcp-pool-fallback-direct-connect=false \
        --endpoint-rps-limit "100000@1s" --endpoint-rps-limit "100000@60s" --endpoint-rps-limit "100000@600s" \
        --wake-compute-limit "100000@1s" --wake-compute-limit "100000@60s" --wake-compute-limit "100000@600s" \
        > "$LOGDIR/proxy_${enabled}_${mode}.log" 2>&1 &
    PROXY_PID=$!

    until lsof -iTCP:4432 -sTCP:LISTEN >/dev/null 2>&1; do sleep 0.2; done
}

stop_proxy() {
    pkill -f "target/debug/proxy" 2>/dev/null || true
    while lsof -iTCP:4432 -sTCP:LISTEN >/dev/null 2>&1; do sleep 0.2; done
    PROXY_PID=""
}

snap_mem() {
    local cfg=$1
    if [[ -z "${PROXY_PID:-}" ]]; then return; fi
    {
        echo "config: $cfg  ts: $(date -Iseconds)"
        ps -o pid,rss,vsz,%cpu,etime,command -p "$PROXY_PID" 2>&1 || true
    } > "$BENCH_DIR/mem_${cfg}.txt"
}

# Asynchronously sample auth-Postgres pg_stat_activity during a run, to detect
# whether auth (port 5432) becomes the bottleneck under -C workloads.
auth_pg_watch_start() {
    local out=$1 dur=$2
    {
        echo "ts,active,idle,total"
        local end=$(($(date +%s) + dur))
        while (( $(date +%s) < end )); do
            local ts
            ts=$(date +%H:%M:%S)
            local stats
            stats=$(psql -h localhost -p 5432 -U "$(whoami)" -d postgres -tA -F, -c \
                "SELECT count(*) FILTER (WHERE state='active'), count(*) FILTER (WHERE state='idle'), count(*) FROM pg_stat_activity WHERE usename='proxytest'" \
                2>/dev/null || echo ",,")
            echo "${ts},${stats}"
            sleep 2
        done
    } > "$out" &
    AUTH_WATCHER_PID=$!
}

auth_pg_watch_stop() {
    if [[ -n "${AUTH_WATCHER_PID:-}" ]]; then
        wait "$AUTH_WATCHER_PID" 2>/dev/null || true
        AUTH_WATCHER_PID=""
    fi
}

run_one() {
    local cfg=$1 conn=$2 wl=$3 wl_flag=$4 c=$5 rep=$6
    local prefix="$LOGDIR/${cfg}_${wl}_c${c}_r${rep}"
    rm -f "${prefix}".*

    # For the short-session (-C) workload at proxy configs and meaningful
    # concurrency, sample auth-Postgres concurrently to detect bottlenecking.
    if [[ "$wl" == "readonly_short" && "$cfg" != "direct" && "$c" -ge 25 ]]; then
        auth_pg_watch_start \
            "$BENCH_DIR/auth_${cfg}_${wl}_c${c}_r${rep}.csv" \
            "$DURATION"
    fi

    local out
    # -j capped at concurrency.
    out=$(PGPASSWORD=$PASS pgbench "$conn" \
            $wl_flag \
            -c "$c" -j "$c" -T "$DURATION" \
            -l --log-prefix="$prefix" \
            2>&1 || true)

    auth_pg_watch_stop

    local tps lat_avg init_conn
    # awk doesn't return non-zero on no-match (unlike grep), so missing fields
    # produce empty strings rather than aborting the script under `set -eo pipefail`.
    # `-C` mode prints "average connection time" instead of "initial connection time";
    # we capture either as init_conn for a single column.
    tps=$(echo "$out" | awk '/^tps = / { print $3; exit }')
    lat_avg=$(echo "$out" | awk '/^latency average/ { print $4; exit }')
    init_conn=$(echo "$out" | awk '/^initial connection time|^average connection time/ { print $5; exit }')

    # `local` always returns 0, so a failed pipe inside `$(...)` won't trip
    # `set -e` here. If pgbench crashed and wrote no log files, we want to
    # record empty percentiles and keep going.
    local pcts
    pcts=$(cat "${prefix}".* 2>/dev/null | python3 "$PARSE" 2>/dev/null || echo ",,")

    echo "${cfg},${wl},${c},${rep},${tps:-},${pcts:-,,},${lat_avg:-},${init_conn:-}" >> "$RESULTS"
    echo "$(date +%H:%M:%S) ${cfg} ${wl} c=${c} rep=${rep} tps=${tps:-?} pcts=${pcts:-?}" >> "$PROGRESS"

    # Trim large logs to save disk; keep only summaries.
    rm -f "${prefix}".*
}

for cfg_entry in "${CONFIGS[@]}"; do
    IFS='|' read -r cfg host port extra <<< "$cfg_entry"
    case "$cfg" in
      direct) ;;
      proxy_pool) start_proxy true session ;;
      proxy_nopool) start_proxy false session ;;
      proxy_txn) start_proxy true transaction ;;
    esac

    conn="postgresql://${USER}@${host}:${port}/${DB}?sslmode=disable${extra}"

    for wl_entry in "${WORKLOADS[@]}"; do
        IFS='|' read -r wl wl_flag <<< "$wl_entry"

        for c in "${CONCURRENCIES[@]}"; do
            for rep in $(seq 1 $REPS); do
                run_one "$cfg" "$conn" "$wl" "$wl_flag" "$c" "$rep"

                # Snapshot proxy memory at peak concurrency once per proxy config.
                if [[ "$cfg" != "direct" && "$c" == "100" && "$rep" == "1" && "$wl" == "tpcb_steady" ]]; then
                    snap_mem "$cfg"
                fi

                sleep 1
            done
        done
    done
done

stop_proxy
echo "DONE $(date -Iseconds)" >> "$PROGRESS"
echo "Done. Results in $RESULTS"
