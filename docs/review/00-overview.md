# Review pack — `mel/pool-fixes-and-benchmarks`

You're reviewing five commits sitting on top of Charles's session-pool
WIP commit (`6e871ba05`). Three pieces of work, in dependency order:

1. **Fix the session-pool half-close bug** so reused connections actually
   work (`6a361a996`).
2. **Multi-endpoint routing** so the mock control plane can return
   different compute addresses for different endpoint IDs
   (`fa654b133`).
3. **Benchmark sweep** comparing direct / proxy+pool / proxy without pool,
   plus FINDINGS.md (`9ce39d144`).
4. **Transaction-mode pool multiplexing** — release on every
   `ReadyForQuery 'I'`, re-acquire from the pool for the next
   transaction (`29f0bc6d2`).
5. **Benchmark update** adding a `proxy_txn` configuration and section 5
   in FINDINGS.md (`ef3db262b`).

## Reading order

1. **`01-session-pool-fix.md`** — start here. Smallest commit, cleanest
   bug story. The fix is ~30 lines but the chain of reasoning that got
   there is half the value.
2. **`02-multi-endpoint.md`** — quick read; mostly CLI plumbing and a
   lookup-priority decision in `mock.rs`.
3. **`03-benchmarks.md`** — methodology, findings, and the things the
   numbers actually say (vs. what they superficially look like). Read
   `benchmarks/FINDINGS.md` alongside.
4. **`04-transaction-mode.md`** — the bigger architectural change. Read
   the `ReadyForQueryWatcher` and `proxy_pass_transaction_mode`
   sections; skim the rest. Compare against `pgcat/src/client.rs`'s
   two-loop structure if you want the canonical reference.
5. **`05-open-questions.md`** — the weak points and decisions I'd
   challenge if I were reviewing me. Read this last — it'll change
   how you read everything else.

## High-level commit map

```
ef3db262b benchmarks: add proxy_txn config and update FINDINGS
29f0bc6d2 transaction-mode pool multiplexing
9ce39d144 benchmarks: pool/no-pool/direct sweep with FINDINGS
fa654b133 multi-endpoint routing via --compute-endpoint-map
6a361a996 session pooling works: don't poll_shutdown(compute) on client Terminate
6e871ba05 basic bb8 pooling   <- Charles's last commit
```

`git log --stat --reverse 6e871ba05..HEAD` gives the per-commit diff
sizes. Five commits, ~1200 lines added, ~50 removed.

## What's in scope vs out of scope

| In scope                                       | Out of scope                       |
| ---------------------------------------------- | ---------------------------------- |
| Session-pool correctness (the half-close bug)  | Pool eviction / health checks      |
| Per-endpoint routing in the mock control plane | Real cplane integration            |
| Transaction-mode multiplexing                  | `SET` / `LISTEN` detection         |
| pgbench sweep + FINDINGS                       | Cancellation across multiplexed conns |
| Memory measurement at peak                     | Auth caching                       |

The five "out of scope" items are all real follow-ups; some are noted in
`05-open-questions.md`.

## How to run things locally

Already documented in `benchmarks/README.md`. The shortest reproducer:

```bash
# Build
cargo build --features testing --bin proxy

# Smoke session pool
RUST_LOG="proxy::tcp_pool=info" PGPASSWORD=testpw target/debug/proxy \
    --auth-backend=postgres \
    --auth-endpoint='postgresql://proxytest@localhost:5432/proxytest_db' \
    --compute-endpoint='postgresql://localhost:5433/proxytest_db?sslmode=disable' \
    --proxy=127.0.0.1:4432 --mgmt=127.0.0.1:7000 --http=127.0.0.1:7001 --wss=127.0.0.1:7002 \
    --tcp-pool-enabled=true --tcp-pool-max-conns-per-key=5 --tcp-pool-fallback-direct-connect=true

# Run 5 sessions and check the pid is stable (session pooling works)
for i in 1 2 3 4 5; do
    PGPASSWORD=testpw psql "postgresql://proxytest@127.0.0.1:4432/proxytest_db?sslmode=disable&options=endpoint%3Dep-test-123" \
        -c "SELECT $i, pg_backend_pid()"
done

# Add --tcp-pool-mode=transaction to see multiplex behaviour
```
