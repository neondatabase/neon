# 01 — Session-pool half-close bug

Commit `6a361a996` "session pooling works: don't poll_shutdown(compute)
on client Terminate". Three files, ~190 lines net.

## TL;DR

When a client cleanly disconnected (Postgres `'X'` Terminate), the
existing `TerminateFilter` correctly **didn't forward** the Terminate
to compute, but the surrounding bidirectional-copy state machine
**still ran `poll_shutdown` on the compute writer** as part of its
"reader hit EOF, shut down the writer" cleanup path. That's
`shutdown(SHUT_WR)` on the live compute socket. Postgres saw the FIN,
closed its side, and the next pool reuse hit `ENOTCONN`.

Fix: a parallel `transfer_one_direction_no_shutdown` that goes
`Running → Done` directly, used for the client→compute direction in
the pooled bidir copy. Compute's writer is never half-closed.

## The chain of reasoning that got here

This took several iterations to localize. The diagnostic timeline
(captured by adding probes that I later removed) was:

```
20.247745  tcp pool: opened new connection
20.249371  TerminateFilter: intercepted Postgres Terminate
20.249403  transfer_one_direction: entering ShuttingDown    ← BUG
20.249433  poll_shutdown(writer) returned Ready(Ok)         ← FIN sent
20.249507  poll_write(empty) error: Broken pipe             ← write half dead
20.249559  tcp pool: returning connection                    ← broken conn pooled
[iter 2, 243ms later]
20.493396  peer_addr error: Invalid argument                ← peer gone
20.493689  per-client task IO error: Socket is not connected
```

The bug is that `TerminateFilter::poll_read` returns `Ready(Ok(()))`
with **zero bytes filled** when it intercepts `'X'`. `CopyBuffer`
treats zero-fill as EOF (`me.read_done = me.cap == filled_len`), and
`poll_copy` returns `Ready(Ok(amt))`. `transfer_one_direction` then
transitions:

```
Running → ShuttingDown → poll_shutdown(writer) → Done
```

That `poll_shutdown(writer)` is on the compute side. It's the bug.

The early `saw_terminate()` check the existing pooled bidir copy
**did** have was checked *after* `transfer_one_direction` returned —
too late, the shutdown had already happened inside the call.

## The fix

`transfer_one_direction_no_shutdown` is the same state machine minus
the `ShuttingDown` step:

```rust
TransferState::Running(buf) => {
    let count = ready!(buf.poll_copy(cx, r.as_mut(), w.as_mut()))?;
    *state = TransferState::Done(count);   // skip ShuttingDown
}
TransferState::ShuttingDown(count) => {
    *state = TransferState::Done(*count);  // dead branch, but keep
}
TransferState::Done(count) => return Poll::Ready(Ok(*count)),
```

In `copy_bidirectional_client_compute_pooled`, swap the
client→compute call to the no-shutdown variant:

```rust
let client_to_compute_result = transfer_one_direction_no_shutdown(
    cx, &mut client_to_compute, &mut filtered_client, compute,
).map_err(ErrorSource::from_client)?;
```

The compute→client direction keeps using the regular variant — if
compute really closes, shutting down the client side is correct.

## Files

- `proxy/src/pglb/copy_bidirectional.rs` — the fix
- `proxy/src/pglb/passthrough.rs` — unchanged (probes removed)
- `proxy/src/proxy/mod.rs`, `proxy/src/tcp_pool.rs` — small unrelated
  prep work in this commit (was-reused signal, pool key on checkout
  via `Option<...>` tuple)

## Read in this order

1. `proxy/src/pglb/copy_bidirectional.rs:43-95` — read both
   `transfer_one_direction` and `transfer_one_direction_no_shutdown`
   side by side. The diff is exactly the deletion of the
   `poll_shutdown` call.
2. `proxy/src/pglb/copy_bidirectional.rs:120-235` — read
   `copy_bidirectional_client_compute_pooled` and find the call site
   that uses the no-shutdown variant. The CRITICAL comment is a
   load-bearing invariant.
3. `proxy/src/pglb/copy_bidirectional.rs::TerminateFilter` (around
   line 460) — to understand what the filter actually emits when it
   intercepts `'X'`. That's the input the bidir copy is responding
   to.

## Things I'd challenge in review

- **The `ShuttingDown` arm in `_no_shutdown` is dead code.** It exists
  to keep the state machine total. Pragmatic but a bit ugly. An
  alternative is a separate state enum.
- **Why not just have `TerminateFilter` return `Pending` instead of
  EOF on intercept?** I considered this in passing — it would avoid
  the EOF-cascade entirely. The downside is the filter would need to
  self-wake or we'd hang. Strictly cleaner but adds wake-up plumbing.
  Worth challenging.
- **`compute→client` still uses the shutdown-variant.** That's fine
  *today* because compute closing means the conn is dead anyway. But
  it does mean a slow-loris compute (sends a few bytes, then EOFs)
  half-closes the client. In our case the client is being torn down
  anyway. Edge-case unlikely to matter in practice.
- **The probes I used to find this bug were temporary** (zero-byte
  `poll_write` and `peer_addr` checks at release/reuse, plus a
  `entering ShuttingDown` log line). I stripped them before the
  commit. If you want to see them, look at the diff history for the
  fix on `mel/pool-fixes-and-benchmarks` — it's in the conversational
  history but was never committed.
