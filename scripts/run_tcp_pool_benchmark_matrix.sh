#!/usr/bin/env bash
set -euo pipefail

# External orchestration wrapper for scripts/tcp_pool_benchmark.py.
#
# The Python harness measures one or more pgbench cells. This wrapper owns
# environment reset between cells, so it can run from a different host than the
# proxy/PgCat processes. Configure RESET_*_CMD with local, ssh, or SSM commands.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

export PGSSLROOTCERT="${PGSSLROOTCERT:-./server.crt}"
export TCP_POOL_BENCH_DOCKER_CMD="${TCP_POOL_BENCH_DOCKER_CMD:-sudo -n docker}"

PROXY_POOL_URL="${PROXY_POOL_URL:-postgresql://postgres:password@comp1-pooler.local.neon.build:4432/postgres?sslmode=verify-full}"
PROXY_PGCAT_URL="${PROXY_PGCAT_URL:-postgresql://postgres:password@pgcat.local.neon.build:4432/postgres?sslmode=verify-full}"
PROXY_PASSTHROUGH_URL="${PROXY_PASSTHROUGH_URL:-postgresql://postgres:password@comp1.local.neon.build:4432/postgres?sslmode=verify-full}"
BACKEND_URL="${BACKEND_URL:-$PROXY_PASSTHROUGH_URL}"

RUN_ID="${RUN_ID:-$(date +%Y%m%d-%H%M%S)}"
OUT_DIR="${OUT_DIR:-benchmark-results/tcp-pool-sanity-$RUN_ID}"
TARGETS="${TARGETS:-proxy_pool proxy_pgcat proxy_passthrough}"
WORKLOADS="${WORKLOADS:-readonly_steady readonly_connect}"
STEADY_CONCURRENCIES="${STEADY_CONCURRENCIES:-10 50 100 250}"
CONNECT_CONCURRENCIES="${CONNECT_CONCURRENCIES:-1 5 10 25 50 100}"
STEADY_DURATION="${STEADY_DURATION:-60}"
CONNECT_DURATION="${CONNECT_DURATION:-30}"
REPS="${REPS:-3}"
IDLE_WAITS="${IDLE_WAITS:-5,15,30,45,60,75}"
MAX_JOBS="${MAX_JOBS:-64}"
SAMPLE_INTERVAL="${SAMPLE_INTERVAL:-1.0}"
RESET_WAIT_SECONDS="${RESET_WAIT_SECONDS:-5}"
CLEAN_IDLE_PGBENCH="${CLEAN_IDLE_PGBENCH:-1}"
PRINT_BACKEND_BASELINE="${PRINT_BACKEND_BASELINE:-1}"
WAIT_TARGET_SELECT="${WAIT_TARGET_SELECT:-0}"

# Optional reset commands. Examples:
#   RESET_PROXY_POOL_CMD="ssh ec2-proxy sudo systemctl restart neon-proxy"
#   RESET_PROXY_PGCAT_CMD="ssh ec2-proxy sudo systemctl restart neon-proxy && ssh ec2-pgcat sudo systemctl restart pgcat"
#   RESET_PROXY_PASSTHROUGH_CMD="ssh ec2-proxy sudo systemctl restart neon-proxy-passthrough"
RESET_PROXY_POOL_CMD="${RESET_PROXY_POOL_CMD:-}"
RESET_PROXY_PGCAT_CMD="${RESET_PROXY_PGCAT_CMD:-}"
RESET_PROXY_PASSTHROUGH_CMD="${RESET_PROXY_PASSTHROUGH_CMD:-}"

# Extra args are parsed as shell words. By default, sample this local benchmark
# setup: proxy and PgCat processes plus the Dockerized compute node.
EXTRA_BENCH_ARGS="${EXTRA_BENCH_ARGS:---resource 'proxy=pgrep:target/debug/proxy --auth-backend cplane-v1' --resource 'pgcat=pgrep:/home/charles/db_final_project/pgcat/target/debug/pgcat' --resource compute=docker:compute-node-1}"

target_url() {
    case "$1" in
        proxy_pool) printf '%s\n' "$PROXY_POOL_URL" ;;
        proxy_pgcat) printf '%s\n' "$PROXY_PGCAT_URL" ;;
        proxy_passthrough) printf '%s\n' "$PROXY_PASSTHROUGH_URL" ;;
        *) echo "unknown target: $1" >&2; return 1 ;;
    esac
}

target_reset_cmd() {
    case "$1" in
        proxy_pool) printf '%s\n' "$RESET_PROXY_POOL_CMD" ;;
        proxy_pgcat) printf '%s\n' "$RESET_PROXY_PGCAT_CMD" ;;
        proxy_passthrough) printf '%s\n' "$RESET_PROXY_PASSTHROUGH_CMD" ;;
        *) echo "unknown target: $1" >&2; return 1 ;;
    esac
}

workload_concurrencies() {
    case "$1" in
        readonly_connect) printf '%s\n' "$CONNECT_CONCURRENCIES" ;;
        *) printf '%s\n' "$STEADY_CONCURRENCIES" ;;
    esac
}

workload_duration() {
    case "$1" in
        readonly_connect) printf '%s\n' "$CONNECT_DURATION" ;;
        *) printf '%s\n' "$STEADY_DURATION" ;;
    esac
}

run_reset() {
    local target="$1"
    local cmd
    cmd="$(target_reset_cmd "$target")"
    if [[ -n "$cmd" ]]; then
        echo "$(date -Is) reset target=$target"
        bash -lc "$cmd"
    fi
    if [[ "$RESET_WAIT_SECONDS" != "0" ]]; then
        sleep "$RESET_WAIT_SECONDS"
    fi
}

wait_target_select() {
    local url="$1"
    if [[ "$WAIT_TARGET_SELECT" != "1" ]]; then
        return
    fi
    local attempt
    for attempt in {1..60}; do
        if psql "$url" -X -q -At -c "select 1" >/dev/null 2>&1; then
            return
        fi
        sleep 1
    done
    echo "target did not pass select 1 readiness: $url" >&2
    return 1
}

clean_backend_idle_pgbench() {
    if [[ "$CLEAN_IDLE_PGBENCH" != "1" ]]; then
        return
    fi
    psql "$BACKEND_URL" -X -q -At -c "
        select count(pg_terminate_backend(pid))
        from pg_stat_activity
        where datname = current_database()
          and pid <> pg_backend_pid()
          and application_name = 'pgbench'
          and state = 'idle';
    " >/dev/null
}

print_backend_baseline() {
    if [[ "$PRINT_BACKEND_BASELINE" != "1" ]]; then
        return
    fi
    echo "$(date -Is) backend baseline"
    psql "$BACKEND_URL" -X -q -P pager=off -c "
        select state, application_name, client_addr, count(*)
        from pg_stat_activity
        where datname = current_database()
          and pid <> pg_backend_pid()
        group by 1,2,3
        order by count(*) desc, 1,2,3;
    "
}

run_cell() {
    local target="$1"
    local workload="$2"
    local concurrency="$3"
    local rep="$4"
    local url duration
    url="$(target_url "$target")"
    duration="$(workload_duration "$workload")"

    run_reset "$target"
    wait_target_select "$url"
    clean_backend_idle_pgbench
    print_backend_baseline

    echo "$(date -Is) benchmark target=$target workload=$workload concurrency=$concurrency rep=$rep"

    local -a extra_args=()
    if [[ -n "$EXTRA_BENCH_ARGS" ]]; then
        eval "extra_args=( $EXTRA_BENCH_ARGS )"
    fi

    python3 scripts/tcp_pool_benchmark.py \
        --target "$target=$url" \
        --backend-url "$BACKEND_URL" \
        --workload "$workload" \
        --concurrency "$concurrency" \
        --duration "$duration" \
        --reps 1 \
        --rep-start "$rep" \
        --idle-waits "$IDLE_WAITS" \
        --max-jobs "$MAX_JOBS" \
        --sample-interval "$SAMPLE_INTERVAL" \
        --out-dir "$OUT_DIR" \
        "${extra_args[@]}"
}

for target in $TARGETS; do
    for workload in $WORKLOADS; do
        for concurrency in $(workload_concurrencies "$workload"); do
            for rep in $(seq 1 "$REPS"); do
                run_cell "$target" "$workload" "$concurrency" "$rep"
            done
        done
    done
done

echo "wrote $OUT_DIR/summary.csv"
