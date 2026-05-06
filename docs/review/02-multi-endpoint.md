# 02 — Multi-endpoint routing

Commit `fa654b133` "multi-endpoint routing via --compute-endpoint-map".
Two files, ~57 lines added.

## TL;DR

Charles's earlier `--compute-endpoint` flag was a global override:
every `wake_compute` call used the same compute address regardless of
the endpoint ID in the connection string. This commit adds
`--compute-endpoint-map` to the mock control plane, threads the
`EndpointId` from `ComputeUserInfo` into `do_wake_compute`, and uses
a three-tier lookup so the existing single-tenant test keeps working.

## Lookup priority (in `do_wake_compute`)

```
self.compute_endpoint_map.get(endpoint)   // per-endpoint map (new)
    .or(self.compute_endpoint.as_ref())   // global override (existing)
    .unwrap_or(&self.auth_endpoint)       // legacy fallback (existing)
```

This means:
- Endpoint IDs in the map → routed to their mapped address
- Endpoint IDs not in the map → fall through to `--compute-endpoint`
  if set
- Otherwise → `--auth-endpoint` (the original behaviour)

So adding a map doesn't change behaviour for IDs that aren't in it.
That kept Charles's single-tenant flow untouched.

## Flag format

```
--compute-endpoint-map='ep-A=postgresql://localhost:5433/db?sslmode=disable,ep-B=postgresql://localhost:5434/db?sslmode=disable'
```

Comma-separated entries; first `=` separates endpoint id from URL.
URLs reuse the same `parse_compute_endpoint` validator (must include
hostname, sslmode if present must be `disable` or `require`).

## Files

- `proxy/src/control_plane/client/mock.rs` — `compute_endpoint_map:
  HashMap<EndpointId, ApiUrl>` field on `MockControlPlane`,
  `do_wake_compute(&EndpointId)` lookup, `wake_compute` plumb-through.
- `proxy/src/binary/proxy.rs` — `--compute-endpoint-map` clap arg,
  `parse_compute_endpoint_map` parser.

## Read in this order

1. `proxy/src/control_plane/client/mock.rs::do_wake_compute` —
   the lookup logic (the chained `or` / `unwrap_or` is the whole
   feature).
2. `proxy/src/control_plane/client/mock.rs::wake_compute` (in the
   `ControlPlaneApi` impl) — confirms `user_info.endpoint` is what
   gets passed in.
3. `proxy/src/binary/proxy.rs::parse_compute_endpoint_map` — the
   parser. ~20 lines, validates duplicates and missing separators.

## How it was verified

Two docker Postgres instances (`compute-A` on 5433, `compute-B` on
5434), each with a `marker` table containing `'compute-A'` /
`'compute-B'`. Same SCRAM verifier copied from auth Postgres so
SCRAM-passthrough works on both. Then:

```bash
# Through the proxy with the map:
psql ".../?options=endpoint%3Dep-A" -c "SELECT val FROM marker"
# returns: compute-A

psql ".../?options=endpoint%3Dep-B" -c "SELECT val FROM marker"
# returns: compute-B
```

Plus 5x `pg_backend_pid()` per endpoint to confirm pool isolation:
each endpoint's pool is keyed independently by `(endpoint, db, role)`,
so within `ep-A` the pid is stable (pool reuse) and across A/B the pids
differ (different computes).

## Things I'd challenge in review

- **Why not a database-backed mapping?** The brief noted that real
  Neon uses `neon_control_plane.endpoints`. I picked the CLI-flag
  approach because it's faster to iterate on and doesn't require
  schema changes; the brief explicitly suggested this trade-off.
  A real cplane would query the db.
- **`HashMap` lookup is sync inside `do_wake_compute`'s async fn.**
  Fine — the map is read-only after startup. If it ever became
  mutable, would need a `RwLock`.
- **No support for SSL-mode beyond `disable`/`require`.** Inherited
  from Charles's `parse_compute_endpoint`. Anything unrecognised
  errors at parse time.
- **Endpoint id in the auth context is actually `EndpointId` (a
  smol_str wrapper),** so the HashMap key works without allocations.
  Look at `proxy/src/types.rs::smol_str_wrapper!` to confirm.
- **Cancellation correctness**: I haven't audited whether
  cross-endpoint cancellation routes correctly. Probably fine because
  `BackendKeyData` is per-session and the cancel path doesn't go
  through `wake_compute`. But worth confirming if you're paranoid.
