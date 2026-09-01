# 05 — Open questions and weak points

The honest list of things I'd push back on if I were reviewing me.
Read this last; it'll change how you read everything else.

Organised by area.

## Session-pool fix (`6a361a996`)

- **The `_no_shutdown` variant of `transfer_one_direction` has a dead
  `ShuttingDown` arm.** It exists to keep the state-machine match
  exhaustive. An alternative is two separate state enums; the cost is
  more types for very little code-clarity gain. I went with the dead
  arm. A reasonable reviewer could push the other way.

- **`TerminateFilter` returning EOF (zero-fill `Ready`) is the actual
  shape of the protocol violation.** A cleaner design: have the
  filter return `Pending` with a self-wake when it sees Terminate,
  and have the pump check `saw_terminate` at the top of each
  iteration before doing anything. Strictly cleaner; downside is
  self-wake plumbing that has to be careful not to busy-loop. I went
  with the no-shutdown variant because it's local to the bug. The
  filter-returns-Pending variant would probably be worth migrating
  to in a future cleanup pass.

## Multi-endpoint (`fa654b133`)

- **Compute-endpoint-map is parsed at startup and never refreshed.**
  Real cplane would react to endpoint moves. Not a problem for our
  testing use-case but worth flagging.

- **No tests for the parser.** Should add at least three: well-formed
  multi-entry input, trailing comma, missing `=`, duplicate id.
  Trivially small to add.

## Benchmarks (`9ce39d144` + `ef3db262b`)

- **macOS ephemeral-port exhaustion contaminates `-C` runs at
  C ≥ 50.** The 19 transient `EADDRNOTAVAIL` errors at C=100 -C are
  a kernel artefact. A Linux re-run would give cleaner numbers.

- **`REPS=2` is the bare minimum for a stddev.** Population stddev
  on two points is just |a-b|/2. Means are trustworthy; stddev
  columns are not very informative. The original `REPS=3 DURATION=30s`
  sweep would have been more rigorous; we ran the short variant for
  time.

- **No PgBouncer comparison.** Per the brief, deliberately deferred
  until transaction pooling. Now that we have transaction pooling,
  the comparison is unblocked.

- **Memory snapshot is one moment.** Doesn't characterise growth
  under sustained churn. Soak test would be more informative.

- **The benchmark only stresses pgbench scenarios.** No web-app-like
  workload (think-time between transactions). That's the workload
  where transaction-mode pool benefit would actually surface, and
  we don't have data for it.

## Transaction mode (`29f0bc6d2`) — the longest list

- **No client-side peek means we hold compute during client idle.**
  The single biggest design limitation. Without a `peek_byte()` on
  the client stream, the multiplex loop always has compute held while
  waiting for the client's next message. That window between "we
  released compute on `'Z' I`" and "client sends next byte" is
  microseconds, not milliseconds. **This is why the pgbench
  benchmark doesn't show a transaction-mode TPS win.**
  pgcat's design is fundamentally cleaner because it uses
  message-framed reads on the client (`read_message` blocks for a
  full message), which acts as the peek-then-acquire primitive
  naturally. Migrating to that model is the big follow-up.

- **`try_acquire_idle` returning `None` cleanly closes the client.**
  Real pooler would either wait briefly for an idle conn (with
  a tokio Notify) or fall back to opening a new one. We don't have
  the connect closure plumbed through the multiplex loop, so we
  can't open new. Fine for our pool-size=200 benchmark scenario
  but a real deployment would want graceful contention handling.

- **`last_known_status` initialised to `'I'` is a load-bearing
  assumption.** It assumes that by the time `proxy_pass_transaction_mode`
  runs, compute has emitted `'Z' I` and we've consumed it via
  `forward_compute_params_to_client` in handle_client. If that
  invariant ever changes (e.g., a code path that enters proxy_pass
  without forwarding params), the assumption breaks silently and we
  could pool a mid-tx conn. Worth either asserting or initialising
  to `Some(b'I')` only after explicit ack.

- **Standard PgBouncer caveats apply but aren't enforced.** `SET`
  (without `LOCAL`), `LISTEN`, simple `PREPARE`, temp tables,
  advisory locks. We could detect these by parsing the client's
  Query text — pgcat does, in `QueryRouter::parse`. We don't. The
  user has to know not to use them.

- **Cancellation correctness.** `pg_cancel_backend(pid)` takes the
  synthesized pid the client received in `BackendKeyData`. In
  transaction mode that pid doesn't correspond to any single
  backend (multiplexed). pgcat handles this with a separate
  cancel-routing map keyed on `(process_id, secret_key)` →
  `(real_backend_pid, real_secret_key, server)`. We don't have this
  yet. Also affects session mode if pool reuse changes the actual
  backend pid out from under the client.

- **The `cancel_user_info` / cancel_token path in `proxy/mod.rs` was
  designed assuming one compute conn per session.** In transaction
  mode, the conn rotates. Currently the cancel_token captures the
  *initial* compute's `process_id` and `secret_key`. After rotation
  these are stale. Not just a "no support" issue — it's an actively
  wrong reference. Worth checking whether anything reads them later.

- **Extended-query protocol is not specially handled.** Parse / Bind /
  Describe / Execute / Close / Sync messages just flow through as
  bytes; we wait for `'Z'` after Sync. Should "just work" because
  Postgres only emits `'Z'` after Sync (or implicit Sync). But: I
  haven't tested this end-to-end with a client that uses extended
  query. pgbench uses simple-Q by default, which is what we tested.
  An asyncpg / pgx-style client would exercise extended query and is
  worth smoke-testing.

- **No tests.** Already noted in `04-`. The watcher's state machine
  is small but has tricky edge cases (header split across two
  poll_reads, body with `'Z'` followed immediately by another
  message). Worth at least one unit test.

- **The `Compute is done, terminate client` info-log fires too
  often.** Inherited from `copy_bidirectional_client_compute_pooled`.
  In transaction mode it can fire on every iteration. Probably
  should be debug.

- **`Compute closed connection` on `BoundaryReason::ComputeClosed`
  uses `io::Error::other`.** Slightly opaque. A typed variant of
  `ErrorSource` would be cleaner.

## Cross-cutting

- **CLAUDE.md or repo docs**. There's no top-level documentation of
  what was changed and how to use it. The benchmarks/README.md is
  partial. A short summary in a top-level README of the
  `mel/pool-fixes-and-benchmarks` branch's contributions would help
  a real reviewer get oriented faster than reading commits.

- **Telemetry is light.** We have `info!` logs on pool open / reuse
  and warn on discard, but no Prometheus metrics for "pool hit rate",
  "transaction boundaries per second", "auth-pg query latency", etc.
  Real production deployment would want these.

- **Memory leak risk on long-running sessions in transaction mode.**
  Each release/re-acquire creates a new `MeasuredStream`. If those
  hold any state internally (a closure capture growing? the recorded
  byte counts?), they accumulate. I don't think they do, but worth
  spot-checking.

## What I'd do next if I had another day

1. Add the client-side peek primitive and restructure the multiplex
   loop to release-then-wait-then-acquire. This is the
   transaction-mode win we don't currently get.
2. Write a custom pgbench script (`pgbench -f` with `pg_sleep`) that
   simulates think-time between transactions. Re-run the proxy_txn
   sweep on it. Numbers should move.
3. Auth-info caching. The `pg_authid` lookup per fresh client connection
   is the actual bottleneck on `-C`-style workloads. A
   `(endpoint, role) → AuthInfo` LRU with a short TTL would cut a
   huge chunk of the proxy → auth-pg traffic.
4. Cancellation routing: a `(synthesized_pid, secret_key) →
   real_backend` map, keyed at session start, used by the cancel
   path. PgBouncer-style.
5. Tests. Specifically: watcher state-machine, the multiplex loop's
   release-on-`'Z' I` decision, the multi-endpoint parser.
