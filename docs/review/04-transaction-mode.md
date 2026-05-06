# 04 — Transaction-mode pool multiplexing

Commit `29f0bc6d2`. Seven files, ~470 lines added.

This is the bigger change. The reference architecture is
`pgcat/src/client.rs` (in `~/mel/pgcat`) — its two-loop structure
("client idle" outer loop / "transaction in progress" inner loop) is
the model. Read `pgcat/src/client.rs:891-1380` if you want the
canonical PgBouncer-style implementation.

## TL;DR

Add `--tcp-pool-mode=session|transaction` (default `session`). In
transaction mode, the proxy releases the compute connection back to
the pool at every `'Z' I` ReadyForQuery boundary and re-acquires for
the next transaction, allowing one compute conn to serve many client
transactions over its lifetime — the PgBouncer transaction-mode
semantic.

## What the wire-protocol level looks like

Postgres sends a `'Z'` (ReadyForQuery) message to the client at every
"ready to accept new query" point. The body is one byte indicating
transaction status:

- `'I'` — idle, not in a transaction. **Safe to release the conn.**
- `'T'` — in a transaction. Conn is mid-`BEGIN`, must be held.
- `'E'` — in a failed transaction. Conn is mid-`BEGIN` after error,
  must be held until `ROLLBACK`.

So the multiplex rule is: at every `'Z'`, look at the status byte. If
`'I'`, return to pool and acquire a (potentially different) conn for
the next transaction. Otherwise, keep the same conn held.

## Implementation pieces

Four new pieces, plus wiring:

### 1. `ReadyForQueryWatcher<R>`

`proxy/src/pglb/copy_bidirectional.rs`. Mirror of `TerminateFilter` on
the read side. Wraps an `AsyncRead + AsyncWrite`, transparently
forwards bytes, parses Postgres BE protocol message headers as they
flow through, and tracks `last_status: Option<u8>` from each `'Z'`
message body byte.

State machine same shape as `TerminateFilter`:

```
AwaitingHeader { header: [u8; 5], pos } → InBody { tag, remaining, body_seen }
```

Difference: doesn't drop messages, just observes. For `tag == b'Z'`
messages the first body byte is the status; we record it and let it
flow through to the client.

```rust
if *tag == b'Z' && *body_seen == 0 {
    let status = limited.filled()[0];
    this.last_status = Some(status);
    this.saw_ready_for_query = true;
}
```

`take_ready_for_query()` returns `true` if a `'Z'` was observed since
the last call (and clears the flag). The boundary pump uses this as
the "transaction over" signal.

### 2. `copy_bidirectional_until_boundary`

Same file. Variant of `copy_bidirectional_client_compute_pooled` that
returns when **either** boundary is reached:

```rust
pub enum BoundaryReason {
    ReadyForQuery(u8),    // status byte
    ClientTerminated,
    ComputeClosed,
}
```

Uses `TerminateFilter` on client and `ReadyForQueryWatcher` on
compute, never calls `poll_shutdown` on compute. The poll_fn body:

```rust
if filtered_client.saw_terminate() { return ... ClientTerminated; }
let _ = transfer_one_direction_no_shutdown(client→compute, ...);
if filtered_client.saw_terminate() { return ... ClientTerminated; }
let _ = transfer_one_direction_no_shutdown(compute→client, ...);
if watched_compute.take_ready_for_query() {
    return ... ReadyForQuery(watched_compute.last_status());
}
```

### 3. `TcpPoolManager::try_acquire_idle`

`proxy/src/tcp_pool.rs`. Non-blocking pool re-acquire — pops one idle
conn for the given key if any. Returns `None` if the per-key pool is
empty. No connect closure required.

This is what the multiplex loop calls between transactions, instead
of the regular `acquire_or_connect`. We don't have a connect closure
plumbed all the way through; if the pool is empty, the simplest MVP
behaviour is to disconnect the client cleanly.

### 4. `proxy_pass_transaction_mode`

`proxy/src/pglb/passthrough.rs`. The outer multiplex loop:

```
loop {
    boundary = copy_bidirectional_until_boundary(client, compute)

    match boundary {
        ReadyForQuery('I'):
            release compute → pool
            try_acquire_idle next compute
            if None: close client cleanly
        ReadyForQuery('T'|'E'):
            keep compute held, continue inner pump
        ClientTerminated:
            release if last_status was 'I' (clean idle)
            discard if mid-transaction (open BEGIN block)
            exit loop
        ComputeClosed:
            error path
    }
}
```

The `last_known_status` variable tracks tx status across iterations
so the `ClientTerminated` branch knows whether the conn is safe to
pool. Initialised to `'I'` because compute's first `'Z'` (sent during
`forward_compute_params_to_client` in handle_client) has already
landed.

## How dispatch works

`ProxyPassthrough::proxy_pass` branches three ways:

```rust
if transaction_mode {
    return proxy_pass_transaction_mode(self).await;
}
// otherwise, the existing session-pooled or direct path
```

`transaction_mode` is `tcp_pool_checkout.is_some() && config.mode ==
TcpPoolMode::Transaction`. Default mode is `Session`, so existing
deployments aren't affected.

## Empirical multiplexing

Verification that the loop actually multiplexes: 10 concurrent psql
sessions, each running 3 queries, in transaction mode. The pool
opened **5 distinct compute backends** for 30 queries; multiple
client sessions were served by the same backend across transactions.
`pg_stat_activity` showed 5 idle backends pooled afterward.

```
c3, c4, c5, c8 → pid 49787   (4 sessions sharing one backend)
c2, c7, c9     → pid 49789   (3 sessions sharing one backend)
c1             → pid 49792
c6             → pid 49791
c10            → pid 49790
```

That's the pgbouncer-style multiplex. Working correctly.

## Files

- `proxy/src/pglb/copy_bidirectional.rs` — `ReadyForQueryWatcher`,
  `BoundaryReason`, `copy_bidirectional_until_boundary`
- `proxy/src/pglb/passthrough.rs` — `proxy_pass_transaction_mode`,
  dispatch in `ProxyPassthrough::proxy_pass`
- `proxy/src/tcp_pool.rs` — `try_acquire_idle`, `key()` accessor on
  `TcpPoolCheckout`
- `proxy/src/config.rs` — `TcpPoolMode` enum, `mode` field on
  `TcpPoolConfig`
- `proxy/src/binary/proxy.rs` — `--tcp-pool-mode` clap arg,
  `TcpPoolModeArg`
- `proxy/src/binary/local_proxy.rs` — fill in the new field
- `proxy/src/compute/mod.rs` — `#[derive(Clone)]` on `AuthInfo`
  (prep for a reusable connect closure that I ended up not using;
  could be reverted)

## Read in this order

1. **`pgcat/src/client.rs:891-1295`** if available locally — the
   reference. The two-loop structure (`loop` at line 891 outer,
   `loop` at line 1172 inner with `if !server.in_transaction()
   break`) is the canonical PgBouncer-style implementation.
2. **`proxy/src/pglb/copy_bidirectional.rs::ReadyForQueryWatcher`** —
   the protocol-watching primitive.
3. **`proxy/src/pglb/copy_bidirectional.rs::copy_bidirectional_until_boundary`** —
   the inner pump.
4. **`proxy/src/pglb/passthrough.rs::proxy_pass_transaction_mode`** —
   the outer multiplex loop.
5. **`proxy/src/tcp_pool.rs::try_acquire_idle`** — non-blocking
   re-acquire.

## Things I'd challenge in review

- **There is no `peek-then-acquire` on the client side.** This is the
  big one. The current implementation always holds a compute conn
  during the bidir pump's `Pending` state — including while the
  client is between queries. Releasing on idle would require waiting
  for the client to send the next byte *before* acquiring, which
  needs a peek-without-consume on the client stream. We don't have
  that primitive in our wrappers (`MeasuredStream`, `Stream<S>`,
  `PqStream`). pgcat sidesteps this because it reads message-framed
  on the client side — `read_message` blocks until a complete
  message arrives, *then* it acquires.
  Without peek, transaction mode's only release window is the
  microseconds between "compute sent `'Z' I`" and "client sends
  next byte". That's what limits the throughput win on benchmarks.

- **Pool-empty-on-reacquire just disconnects the client.** If
  transaction-mode is on and `max_conns_per_key` is sized too small
  for concurrent demand, clients can be cleanly closed mid-session.
  Real pooler would either wait or open a new conn. MVP punt.

- **`AuthInfo` was made `Clone`.** I did this to support a reusable
  connect closure for the multiplex loop, then ended up not using it.
  Could be reverted, but it's also harmless. Worth considering for a
  future iteration that wants `acquire_or_connect` instead of
  `try_acquire_idle`.

- **Standard PgBouncer transaction-mode caveats apply.** They're noted
  in the doc-comment on `proxy_pass_transaction_mode`. Specifically:
  `SET` (without `LOCAL`), `LISTEN`, simple `PREPARE`, temp tables,
  advisory locks won't survive across transactions. We don't detect
  these — the user is on their own.

- **Cancellation across multiplexed conns is broken.** `pg_cancel_backend(pid)`
  takes a pid that's been synthesized by the proxy (in the
  was-reused / synthesized BackendKeyData branch) and doesn't
  correspond to any single backend. PgBouncer has its own cancel
  routing for this. We don't.

- **Why expose `key()` on `TcpPoolCheckout`?** The multiplex loop
  needs `pool_key` to call `try_acquire_idle` after releasing. The
  cleanest factoring would be `try_acquire_idle_for(checkout)` that
  takes the checkout — but that's awkward because the checkout has
  already been consumed by the prior `release()`. The `key()`
  accessor lets us snapshot the key before consuming. Alternative:
  store the key separately in the multiplex loop's state (it's
  cheap, smol_strs).

- **The `Compute is done, terminate client` info-log fires once per
  pooled session even on clean session-mode exit.** Look at
  `copy_bidirectional_client_compute_pooled`. Probably should be
  silenced or moved to debug.

- **No tests.** The change is 470 lines and there's no unit test
  beyond the existing ones. Worth adding at least one for the
  watcher's `take_ready_for_query` semantics with the various
  status bytes, and one integration-style test that verifies the
  multiplex pid-sharing in a smoke harness.
