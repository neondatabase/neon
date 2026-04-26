# proxy connection-pool benchmarks

Sweep pgbench across three configurations to characterise both the steady-state
overhead of the proxy and the throughput benefit of the embedded TCP connection
pool under short-session workloads.

## Configurations

| name           | client target              | what it isolates                       |
| -------------- | -------------------------- | -------------------------------------- |
| `direct`       | docker postgres 127.0.0.1:5433 | baseline floor                     |
| `proxy_pool`   | proxy 127.0.0.1:4432, `--tcp-pool-enabled=true`  | proxy + pool benefit |
| `proxy_nopool` | proxy 127.0.0.1:4432, `--tcp-pool-enabled=false` | proxy overhead, no pool |

## Workloads

| name                  | pgbench flags | what it stresses                              |
| --------------------- | ------------- | --------------------------------------------- |
| `tpcb_steady`         | (default)     | mixed read/write, persistent sessions         |
| `readonly_steady`     | `-S`          | SELECT-only, persistent sessions              |
| `readonly_short`      | `-S -C`       | SELECT-only, **reconnect per transaction**    |

`-C` is the case where pooling can win: every transaction pays a fresh
client→proxy connection cost, and the pool determines whether the proxy
also pays a fresh proxy→compute cost or reuses one.

## Sweep

- concurrency: 1, 5, 10, 25, 50, 100
- 3 repetitions per cell
- 30 s per run
- 3 configs × 3 workloads × 6 concurrencies × 3 reps = **162 runs**, ~80 min wall

## Reproduce

Prereqs:

```bash
# auth Postgres on 127.0.0.1:5432 (Homebrew or similar) with user proxytest
# compute-A docker on 127.0.0.1:5433
docker run --name proxytest-compute -e POSTGRES_PASSWORD=testpw \
    -e POSTGRES_USER=proxytest -e POSTGRES_DB=proxytest_db \
    -p 5433:5432 -d postgres:16

# copy SCRAM verifier from auth pg to compute (so SCRAM passthrough works)
VERIFIER=$(psql -h localhost -p 5432 -U $(whoami) -d postgres -At -c \
  "SELECT rolpassword FROM pg_authid WHERE rolname='proxytest'")
docker exec proxytest-compute psql -U proxytest -d proxytest_db -c \
  "ALTER USER proxytest WITH PASSWORD '$VERIFIER';"

# build the proxy
cargo build --features testing --bin proxy

# initialise pgbench (scale 10 ~= 150 MB)
PGPASSWORD=testpw pgbench -i -s 10 -h 127.0.0.1 -p 5433 -U proxytest proxytest_db
```

Run the sweep:

```bash
cd benchmarks
./run_bench.sh                   # ~80 minutes
./summarize.py                   # aggregates results.csv -> summary.csv
```

## Outputs

- `results.csv` — one row per (config, workload, concurrency, rep)
- `summary.csv` — aggregated mean ± std of TPS, mean of p50/p95/p99, plus
  bottleneck `notes` populated from auth-Postgres watcher samples
- `mem_<config>.txt` — proxy `ps -o pid,rss,vsz,%cpu` snapshot at peak load
- `auth_<config>_<workload>_c<C>_r<rep>.csv` — auth-Postgres pg_stat_activity
  samples taken concurrently with the run (only collected for the
  `readonly_short` workload at C ≥ 25 on proxy configs)
- `logs/proxy_*.log` — proxy stdout/stderr for each run
- `progress.txt` — live progress log

## Watch out for

The `readonly_short` workload puts the auth Postgres on the critical path:
every fresh client connection through the proxy queries `pg_authid` once
to get the SCRAM verifier. At high concurrency, auth Postgres can become
the bottleneck instead of the pool being the differentiator. The
`auth_*.csv` snapshots and the `notes` column in `summary.csv` flag this
when it happens.

## Override knobs

```bash
DURATION=60 ./run_bench.sh                     # longer per-cell duration
CONCURRENCIES_OVERRIDE="10 100" ./run_bench.sh # test only two concurrencies
```
