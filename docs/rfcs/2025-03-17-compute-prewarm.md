# Compute rolling restart with prewarm

Created on 2025-03-17
Implemented on _TBD_

## Summary

This RFC describes an approach to reduce performance degradation due to missing caches after compute node restart, i.e.:

1. Rolling restart of the running instance via 'warm' replica.
2. Auto-prewarm compute caches after unplanned restart or scale-to-zero.

## Motivation

Neon currently implements several features that guarantee high uptime of compute nodes:

1. Storage high-availability (HA), i.e. each tenant shard has a secondary pageserver location, so we can quickly switch over compute to it in case of primary pageserver failure.
2. Fast compute provisioning, i.e. we have a fleet of pre-created empty computes, that are ready to serve workload, so restarting unresponsive compute is very fast.
3. Preemptive NeonVM compute provisioning in case of k8s node unavailability.

This helps us to be well-within the uptime SLO of 99.95% most of the time. Problems begin when we go up to multi-TB workloads and 32-64 CU computes.
During restart, compute looses all caches: LFC, shared buffers, file system cache. Depending on the workload, it can take a lot of time to warm up the caches,
so that performance could be degraded and might be even unacceptable for certain workloads. The latter means that although current approach works well for small to
medium workloads, we still have to do some additional work to avoid performance degradation after restart of large instances.

## Non Goals

- Details of the persistence storage for prewarm data are out of scope, there is a separate RFC for that: <https://github.com/neondatabase/neon/pull/9661>.
- Complete compute/Postgres HA setup and flow. Although it was originally in scope of this RFC, during preliminary research it appeared to be a rabbit hole, so it's worth of a separate RFC.
- Low-level implementation details for Postgres replica-to-primary promotion. There are a lot of things to think and care about: how to start walproposer, [logical replication failover](https://www.postgresql.org/docs/current/logical-replication-failover.html), and so on, but it's worth of at least a separate one-pager design document if not RFC.

## Impacted components

Postgres, compute_ctl, Control plane, S3 proxy for unlogged storage of compute files.

## Proposed implementation

### compute_ctl API

1. `POST /store_lfc_state` - dump LFC state using Postgres SQL interface and store result in S3.

2. `GET /dump_lfc_state` - dump LFC state using Postgres SQL interface and return it as is
    in text format suitable for the future restore/prewarm. This API is not strictly needed at
    the end state, but could be useful for a faster prototyping of a complete rolling restart flow
    with prewarm, as it doesn't require persistent for LFC state storage.

3. `POST /restore_lfc_state` - restore/prewarm LFC state with request

    ```yaml
    RestoreLFCStateRequest:
      oneOf:
        - type: object
          required:
            - lfc_state
          properties:
            lfc_state:
              type: string
              description: Raw LFC content dumped with GET `/dump_lfc_state`
        - type: object
          required:
            - lfc_cache_key
          properties:
            lfc_cache_key:
              type: string
              description: |
                endpoint_id of the source endpoint on the same branch
                to use as a 'donor' for LFC content. Compute will look up
                LFC content dump in S3 using this key and do prewarm.
    ```

    where `lfc_state` and `lfc_cache_key` are mutually exclusive.

    The actual prewarming will happen asynchronously, so the caller need to check the
    prewarm status using the compute's standard `GET /status` API.

4. `GET /status` - extend existing API with following attributes

    ```rust
    struct ComputeStatusResponse {
        // [All existing attributes]
        ...
        pub prewarm_state: PrewarmState
    }

    // Compute prewarm state. Will be stored in the shared Compute state
    // in compute_ctl
    struct PrewarmState {
        pub status: PrewarmStatus
        // Prewarm progress in the range [0, 1]
        pub progress: f32
        // Optional prewarm error
        pub error: Option<String>
    }

    pub enum PrewarmStatus {
        // Prewarming was never requested on this compute
        Off,
        // Prewarming was requested, but not started yet
        Pending,
        // Prewarming is in progress. The caller should follow
        // `PrewarmState::progress`.
        InProgress,
        // Prewarming has been successfully completed
        Completed,
        // Prewarming failed. The caller should look at
        // `PrewarmState::error` for the reason.
        Failed,
    }
    ```

5. `POST /promote` - this is a **blocking** API call to promote compute replica into primary.
    - If promotion is done successfully, it will return `200 OK`.
    - If compute is already primary, the call will be no-op and `compute_ctl`
      will return `412 Precondition Failed`.
    - If, for some reason, second request reaches compute that is in progress of promotion,
      it will respond with `409 Conflict`.
    - If compute hit any permanent failure during promotion `500 Internal Server Error`
      will be returned.

### Control plane operations

### Complete rolling restart flow

### Auto-prewarm

### Reliability, failure modes and corner cases

We consider following failures while implementing this RFC:

1. Compute got interrupted/crashed/restarted during prewarm. The caller -- control plane -- should
    detect that and start prewarm from the beginning.

2. Control plane promotion request timed out or hit network issues. If it never reached the
    compute, control plane should just repeat it. If it did reach the compute, then during
    retry control plane can hit `409` as previous request triggered the promotion already.
    In this case, control plane need to retry until either `200` or
    permanent error `500` is returned.

### Interaction/Sequence diagram (if relevant)

### Scalability (if relevant)

### Security implications (if relevant)

### Unresolved questions (if relevant)

## Alternative implementation (if relevant)

## Pros/cons of proposed approaches (if relevant)

## Definition of Done (if relevant)
