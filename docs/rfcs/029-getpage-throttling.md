# Per-Tenant GetPage@LSN Throttling

Author: Christian Schwarz
Date: Oct 24, 2023

## Summary

This RFC proposes per-tenant throttling of GetPage@LSN requests inside Pageserver
and the interactions with its client, i.e., the neon_smgr component in Compute.

The result of implementing & executing this RFC will be a fleet-wide upper limit for
**"the highest GetPage/second that Pageserver can support for a single tenant/shard"**.

## Background

### GetPage@LSN Request Flow

Pageserver exposes its `page_service.rs` as a libpq listener.
The Computes' `neon_smgr` module connects to that libpq listener.
Once a connection is established, the protocol allows Compute to request page images at a given LSN.
We call these requests GetPage@LSN requests, or GetPage requests for short.
Other request types can be sent, but these are low traffic compared to GetPage requests
and are not the concern of this RFC.

Pageserver associates one libpq connection with one tokio task.

Per connection/task, the pq protocol is handled by the common `postgres_backend` crate.
Its `run_message_loop` function invokes the `page_service` specific `impl<IO> postgres_backend::Handler<IO> for PageServerHandler`.
Requests are processed in the order in which they arrive via the TCP-based pq protocol.
So, there is no concurrent request processing within one connection/task.

There is a degree of natural pipelining:
Compute can "fill the pipe" by sending more than one GetPage request into the libpq TCP stream.
And Pageserver can fill the pipe with responses in the other direction.
Both directions are subject to the limit of tx/rx buffers, nodelay, TCP flow control, etc.

### GetPage@LSN Access Pattern

The Compute has its own hierarchy of caches, specifically `shared_buffers` and the `local file cache` (LFC).
Compute only issues GetPage requests to Pageserver if it encounters a miss in these caches.

If the working set stops fitting into Compute's caches, requests to Pageserver increase sharply -- the Compute starts *thrashing*.

## Motivation

In INC-69, a tenant issued 155k GetPage/second for a period of 10 minutes and 60k GetPage/second for a period of 3h,
then dropping to ca 18k GetPage/second for a period of 9h.

We noticed this because of an internal GetPage latency SLO burn rate alert, i.e.,
the request latency profile during this period significantly exceeded what was acceptable according to the internal SLO.

Sadly, we do not have the observability data to determine the impact of this tenant on other tenants on the same tenants.

However, here are some illustrative data points for the 155k period:
The tenant was responsible for >= 99% of the GetPage traffic and, frankly, the overall activity on this Pageserver instance.
We were serving pages at 10 Gb/s (`155k x 8 kbyte (PAGE_SZ) per second is 1.12GiB/s = 9.4Gb/s.`)
The CPU utilization of the instance was 75% user+system.
Pageserver page cache served 1.75M accesses/second at a hit rate of ca 90%.
The hit rate for materialized pages was ca. 40%.
Curiously, IOPS to the Instance Store NVMe were very low, rarely exceeding 100.

The fact that the IOPS were so low / the materialized page cache hit rate was so high suggests that **this tenant's compute's caches were thrashing**.
The compute was of type `k8s-pod`; hence, auto-scaling could/would not have helped remediate the thrashing by provisioning more RAM.
The consequence was that the **thrashing translated into excessive GetPage requests against Pageserver**.

My claim is that it was **unhealthy to serve this workload at the pace we did**:
* it is likely that other tenants were/would have experienced high latencies (again, we sadly don't have per-tenant latency data to confirm this)
* more importantly, it was **unsustainable** to serve traffic at this pace for multiple reasons:
    * **predictability of performance**: when the working set grows, the pageserver materialized page cache hit rate drops.
      At some point, we're bound by the EC2 Instance Store NVMe drive's IOPS limit.
      The result is an **uneven** performance profile from the Compute perspective.

    * **economics**: Neon currently does not charge for IOPS, only capacity.
      **We cannot afford to undercut the market in IOPS/$ this drastically; it leads to adverse selection and perverse incentives.**
      For example, the 155k IOPS, which we served for 10min, would cost ca. 6.5k$/month when provisioned as an io2 EBS volume.
      Even the 18k IOPS, which we served for 9h, would cost ca. 1.1k$/month when provisioned as an io2 EBS volume.
      We charge 0$.
      It could be economically advantageous to keep using a low-DRAM compute because Pageserver IOPS are fast enough and free.


Note: It is helpful to think of Pageserver as a disk, because it's precisely where `neon_smgr` sits:
vanilla Postgres gets its pages from disk, Neon Postgres gets them from Pageserver.
So, regarding the above performance & economic arguments, it is fair to say that we currently provide an "as-fast-as-possible-IOPS" disk that we charge for only by capacity.

## Solution: Throttling GetPage Requests

**The consequence of the above analysis must be that Pageserver throttles GetPage@LSN requests**.
That is, unless we want to start charging for provisioned GetPage@LSN/second.
Throttling sets the correct incentive for a thrashing Compute to scale up its DRAM to the working set size.
Neon Autoscaling will make this easy, [eventually](https://github.com/neondatabase/neon/pull/3913).

## The Design Space

What that remains is the question about *policy* and *mechanism*:

**Policy** concerns itself with the question of what limit applies to a given connection|timeline|tenant.
Candidates are:

* hard limit, same limit value per connection|timeline|tenant
    * Per-tenant will provide an upper bound for the impact of a tenant on a given Pageserver instance.
      This is a major operational pain point / risk right now.
* hard limit, configurable per connection|timeline|tenant
    * This outsources policy to console/control plane, with obvious advantages for flexible structuring of what service we offer to customers.
    * Note that this is not a mechanism to guarantee a minium provisioned rate, i.e., this is not a mechanism to guarantee a certain QoS for a tenant.
* fair share among active connections|timelines|tenants per instance
    * example: each connection|timeline|tenant gets a fair fraction of the machine's GetPage/second capacity
    * NB: needs definition of "active", and knowledge of available GetPage/second capacity in advance
* ...


Regarding **mechanism**, it's clear that **backpressure** is the way to go.
However, we must choose between
* **implicit** backpressure through pq/TCP and
* **explicit** rejection of requests + retries with exponential backoff

Further, there is the question of how throttling GetPage@LSN will affect the **internal GetPage latency SLO**:
where do we measure the SLI for Pageserver's internal getpage latency SLO? Before or after the throttling?

And when we eventually move the measurement point into the Computes (to avoid coordinated omission),
how do we avoid counting throttling-induced latency toward the internal getpage latency SLI/SLO?

## Scope Of This RFC

**This RFC proposes introducing a hard GetPage@LSN/second limit per tenant, with the same value applying to each tenant on a Pageserver**.

This proposal is easy to implement and significantly de-risks operating large Pageservers,
based on the assumption that extremely-high-GetPage-rate-episodes like the one from the "Motivation" section are uncorrelated between tenants.

For example, suppose we pick a limit that allows up to 10 tenants to go at limit rate.
Suppose our Pageserver can serve 100k GetPage/second total at a 100% page cache miss rate.
If each tenant gets a hard limit of 10k GetPage/second, we can serve up to 10 tenants at limit speed without latency degradation.

The mechanism for backpressure will be TCP-based implicit backpressure.
The compute team isn't concerned about prefetch queue depth.
Pageserver will implement it by delaying the reading of requests from the libpq connection(s).

The rate limit will be implemented using a per-tenant token bucket.
The bucket will be be shared among all connections to the tenant.
The bucket implementation supports starvation-preventing `await`ing.
The current candidate for the implementation is [`leaky_bucket`](https://docs.rs/leaky-bucket/).
The getpage@lsn benchmark that's being added in https://github.com/neondatabase/neon/issues/5771
can be used to evaluate the overhead of sharing the bucket among connections of a tenant.
A possible technique to mitigate the impact of sharing the bucket would be to maintain a buffer of a few tokens per connection handler.

Regarding metrics / the internal GetPage latency SLO:
we will measure the GetPage latency SLO _after_ the throttler and introduce a new metric to measure the amount of throttling, quantified by:
- histogram that records the tenants' observations of queue depth before they start waiting (one such histogram per pageserver)
- histogram that records the tenants' observations of time spent waiting (one such histogram per pageserver)

Further observability measures:
- an INFO log message at frequency 1/min if the tenant/timeline/connection was throttled in that last minute.
  The message will identify the tenant/timeline/connection to allow correlation with compute logs/stats.

Rollout will happen as follows:
- deploy 1: implementation + config: disabled by default, ability to enable it per tenant through tenant_conf
- experimentation in staging and later production to study impact & interaction with auto-scaling
- determination of a sensible global default value
  - the value will be chosen as high as possible ...
  - ... but low enough to work towards this RFC's goal that one tenant should not be able to dominate a pageserver instance.
- deploy 2: implementation fixes if any + config: enabled by default with the aforementioned global default
- reset of the experimental per-tenant overrides
- gain experience & lower the limit over time
  - we stop lowering the limit as soon as this RFC's goal is achieved, i.e.,
    once we decide that in practice the chosen value sufficiently de-risks operating large pageservers

The per-tenant override will remain for emergencies and testing.
But since Console doesn't preserve it during tenant migrations, it isn't durably configurable for the tenant.

Toward the upper layers of the Neon stack, the resulting limit will be
**"the highest GetPage/second that Pageserver can support for a single tenant"**.

### Rationale

We decided against error + retry because of worries about starvation.

## Future Work

Enable per-tenant emergency override of the limit via Console.
Should be part of a more general framework to specify tenant config overrides.
**NB:** this is **not** the right mechanism to _sell_ different max GetPage/second levels to users,
or _auto-scale_ the GetPage/second levels. Such functionality will require a separate RFC that
concerns itself with GetPage/second capacity planning.

Compute-side metrics for GetPage latency.

Back-channel to inform Compute/Autoscaling/ControlPlane that the project is being throttled.

Compute-side neon_smgr improvements to avoid sending the same GetPage request multiple times if multiple backends experience a cache miss.

Dealing with read-only endpoints: users use read-only endpoints to scale reads for a single tenant.
Possibly there are also assumptions around read-only endpoints not affecting the primary read-write endpoint's performance.
With per-tenant rate limiting, we will not meet that expectation.
However, we can currently only scale per tenant.
Soon, we will have sharding (#5505), which will apply the throttling on a per-shard basis.
But, that's orthogonal to scaling reads: if many endpoints hit one shard, they share the same throttling limit.
To solve this properly, I think we'll need replicas for tenants / shard.
To performance-isolate a tenant's endpoints from each other, we'd then route them to different replicas.
