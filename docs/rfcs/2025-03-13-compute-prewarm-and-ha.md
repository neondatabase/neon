# Compute rolling restart with prewarm and HA

Created on 2025-03-13
Implemented on _TBD_

## Summary

This RFC describes a more _traditional_ approach to compute/Postgres high availability (HA):

1. Rolling restart of the running instance via 'warm' replica.
2. Hot standby replica, which is always ready to take over in case of primary failure.
3. Auto-prewarm compute caches after restart.

Yet, taking into account Neon's specifics.

## Motivation

Neon currently implements several features that guarantee high uptime of compute nodes:

1. Storage HA, i.e. each tenant shard has a secondary pageserver location, so we can quickly switch over compute to it in case of primary pageserver failure.
2. Fast compute provisioning, i.e. we have a fleet of pre-created empty computes, that are ready to serve workload, so restarting unresponsive compute is very fast.
3. Preemptive NeonVM compute provisioning in case of k8s node unavailability.

This helps us to be well-within the uptime SLO of 99.95 most of the time. Problems begin when we go up to multi-TB workloads and 32-64 CU computes.
During restart, compute looses all caches: LFC, shared buffers, file system cache. Depending on the workload, it can take a lot of time to warm up the caches,
so that performance could be degraded and might be even unacceptable for certain workloads. The latter means that although current approach works well for small to
medium workloads, we still need to implement a more traditional approach to Postgres HA for compute nodes.

## Non Goals

Details of the persistance storage for prewarm data are out of scope, there is a separate RFC for that: https://github.com/neondatabase/neon/pull/9661.

## Impacted components (e.g. pageserver, safekeeper, console, etc)

## Proposed implementation

### Reliability, failure modes and corner cases (if relevant)

### Interaction/Sequence diagram (if relevant)

### Scalability (if relevant)

### Security implications (if relevant)

### Unresolved questions (if relevant)

## Alternative implementation (if relevant)

## Pros/cons of proposed approaches (if relevant)

## Definition of Done (if relevant)
