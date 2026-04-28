# 03 — Benchmarks

Commits `9ce39d144` and `ef3db262b`. The first added the harness and
the initial three configurations; the second added a fourth
(`proxy_txn`) and the transaction-mode discussion in §5 of FINDINGS.

The headline numbers are in `benchmarks/FINDINGS.md` — that's the
write-up I'd put in the report verbatim. This file is about
methodology and the things the numbers actually mean.

## Methodology

3 configurations × 3 workloads × 6 concurrencies × 2 reps × 10s
= 108 cells, ~22 minutes wall time on macOS / Apple Silicon, all
co-located on one host (no network).

Configurations:

| name           | what it isolates                              |
| -------------- | --------------------------------------------- |
| `direct`       | floor: client → docker Postgres on 5433       |
| `proxy_pool`   | full proxy path with TCP pool enabled         |
| `proxy_nopool` | proxy path without pool — proxy overhead only |
| `proxy_txn`    | proxy path with `--tcp-pool-mode=transaction` |

Workloads:

| name                | pgbench flags  | what it stresses                 |
| ------------------- | -------------- | -------------------------------- |
| `tpcb_steady`       | (default)      | mixed read/write, persistent     |
| `readonly_steady`   | `-S`           | SELECT-only, persistent          |
| `readonly_short`    | `-S -C`        | SELECT-only, **reconnect per tx**|

The `-C` workload is what matters — that's where pool reuse pays off,
because every transaction in pgbench `-C` mode goes through a fresh
client → proxy connect.

## What the numbers say (vs. what they look like at first glance)

### Steady-state proxy overhead is small

`tpcb_steady` C=100: direct 4440 / pool 3778 / nopool 3960. Proxy
adds ~13 %. Pool ≈ no-pool because persistent sessions never exercise
the pool — each pgbench client holds one compute conn for the full
30 s, so the pool only saves at most one handshake per client.
**This is the right answer for this workload**, not a problem.

### Short-session pool benefit is real

`readonly_short` (proxy_pool TPS / proxy_nopool TPS):

| C   | pool | nopool | ratio |
| --- | ---: | -----: | ----: |
| 1   |  114 |     61 | 1.86× |
| 5   |  499 |    336 | 1.49× |
| 10  |  621 |    357 | 1.74× |
| 25  |  649 |    511 | 1.27× |
| 50  |  588 |    249 | 2.36× |
| 100 |  633 |    384 | 1.65× |

1.3×–2.4× pool benefit on the short-session workload.

### The remaining gap to direct is auth-Postgres, not the pool

`proxy_pool` short-session TPS plateaus around 600–650 even at high
concurrency, well below `direct` (~800–860). The bottleneck isn't
the pool — the pool is doing exactly what it should (compute
handshake skipped). The bottleneck is per-client-connection auth
lookups: every fresh client connection through the proxy queries
`pg_authid` on the auth Postgres. With C=100 -C that's hundreds
of fresh connect-cycles per second, all hitting auth-pg.

Direct evidence in the proxy log under `-C` C=100:
```
19  Can't assign requested address (os error 49)
```

That's macOS ephemeral-port exhaustion on the proxy → auth-pg socket
(macOS recycles TIME_WAIT slowly). Real evidence for "auth-pg is on
the critical path" — fixing it requires either (a) auth-info caching,
or (b) transaction-level pooling that lets one client connection share
both compute and auth across many short sessions.

### Memory at peak load

`proxy_pool` 81 MB RSS, `proxy_nopool` 78 MB. The pool adds ~3 MB to
hold up to ~100 idle compute conns — about 30 KB per pooled session.
Total proxy footprint stays under 100 MB serving 100 concurrent
clients.

### Transaction mode doesn't help on pgbench

`readonly_short` proxy_txn ≈ proxy_pool (within noise). Two
structural reasons (covered in `04-transaction-mode.md` and
FINDINGS §5):

1. `pgbench -C` is one transaction per client session, so each
   pgbench session only goes through one txn-boundary release/
   re-acquire cycle. No more multiplex than session mode does.
2. `pgbench` without `-C` keeps every client persistently busy, no
   idle gap to multiplex into.

Transaction mode wins on **idle think time between transactions**.
pgbench doesn't produce that profile.

A small per-boundary release/re-acquire overhead is visible at low
concurrency on persistent-session workloads (`tpcb_steady` at C=1:
920 → 765, ~17 %). Converges to ~equal at C=100.

## Files

- `benchmarks/run_bench.sh` — orchestrator (~250 lines)
- `benchmarks/parse_pcts.py` — percentile post-processor
- `benchmarks/summarize.py` — aggregator (mean ± std)
- `benchmarks/results.csv` — 144 raw rows
- `benchmarks/summary.csv` — 72 aggregated rows
- `benchmarks/FINDINGS.md` — the write-up
- `benchmarks/auth_*.csv` — auth-pg pg_stat_activity samples taken
  concurrently with the runs
- `benchmarks/mem_*.txt` — proxy ps snapshots at peak load

## Read in this order

1. **`benchmarks/FINDINGS.md`** — sections 1-5. Read end-to-end.
2. **`benchmarks/summary.csv`** — open in your IDE; sort by
   workload+concurrency to compare the four configs side by side.
3. **`benchmarks/run_bench.sh`** — to understand exactly how each
   number was generated. The interesting bits are `start_proxy`,
   `auth_pg_watch_start`, and the per-cell `run_one`.
4. **`benchmarks/auth_proxy_pool_readonly_short_c25_r1.csv`** (and
   siblings) — what auth-pg looked like during a `-C` run.

## Things I'd challenge in review

- **`-C` runs at C=100 hit ephemeral-port exhaustion on macOS.** That's
  a kernel artefact, not a real bottleneck on Linux. The numbers at
  C=100 -C are noisier than the others (proxy_pool stddev 82, nopool
  186) because of this. On a Linux host you'd see different numbers.
- **REPS=2 is statistical-stress floor.** With 2 reps, stddev is
  population stddev of 2 points — basically range/2. The 30s × 3-rep
  full sweep would have been more rigorous; we ran the 10s × 2-rep
  variant for time. The mean column is more trustworthy than the
  stddev column.
- **Memory snapshot is one moment in time.** RSS during a 100-client
  load. Doesn't tell you whether the proxy memory grows over time
  with churn. Worth running a longer soak to confirm.
- **DURATION=10 might be short.** Below the 30s floor I originally
  recommended. pgbench amortises connection-setup over the duration,
  so short duration over-emphasises the connect cost. For workloads
  where the connect cost is the main story (`-C`), 10s is fine.
- **`auth_*.csv` polling cadence is 2 seconds.** Coarse. It catches
  steady-state contention (≥ 2 active conns concurrently) but not
  microsecond bursts. The proxy log is the more reliable signal for
  fast contention.
- **No comparison against PgBouncer.** Deliberate per the brief —
  apples-to-apples requires transaction pooling, which we now have
  but haven't benchmarked against PgBouncer yet.
