# GetPage@LSN Rate Limit

Author: Christian Schwarz
Date: Oct 24, 2023

## Summary

This RFC proposes basic rate-limiting GetPage@LSN requests inside Pageserver
and the interactions with its client, i.e., the neon_smgr component in Compute.

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
We were serving pages at 10 GiB/s (`155k x 8 kbyte (PAGE_SZ) per second is 1.12GiB/s = 9.4Gb/s.`)
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

## Solution: Limiting IOPS

**The consequence of the above analysis must be that Pageserver limits IOPS; unless we want to start charging for IOPS**.
Limiting IOPS sets the correct incentive to scale up computes (=> DRAM) to an adequate level for the given workload.
Neon makes this scaling Compute up and down exceptionally easy through Neon Autoscaling.

## The Design Space

What that remains is the question about *policy* and *mechanism*:

**Policy** concerns itself with the question of what limit applies to a given connection|timeline|tenant.
Candidates are:

* hard limit, same limit value per connection|timeline|tenant
    * Per-tenant will provide an upper bound for the impact of a tenant on a given Pageserver instance.
      This is a major operational pain point / risk right now.
* hard limit, configurable per connection|timeline|tenant
    * This outsources policy to console/control plane, with obvious advantages for flexible structuring of what service we offer to customers.
    * Note that this is not a mechanism to guarantee a *lower* bound of IOPS, i.e., to guarantee a certain QoS.
* fair share among active connections|timelines|tenants per instance
    * example: divide available IOPS evenly among the tenants that were active in the last 5min
    * needs definition of "active", and knowledge of available IOPS in advance
* ...


Regarding **mechanism**, it's clear that **backpressure** is the way to go.
However, we must choose between
* **implicit** backpressure through pq/TCP and
* **explicit** rejection of requests + retries with exponential backoff

Further, there is the question of how limiting IOPS will affect the **internal GetPage latency SLO**:
where do we measure the SLI for Pageserver's internal getpage latency SLO? Before or after the rate limiter?

And when we eventually move the measurement point into the Computes (to avoid coordinated omission),
how do we avoid counting rate-limiting induced latency toward the internal getpage latency SLI/SLO?

## Scope Of This RFC

**This RFC proposes introducing a hard IOPS limit per tenant, with the same value applying to each tenant on a Pageserver**.

This proposal is easy to implement and significantly de-risks operating large Pageservers,
based on the assumption that extremely-high-IOPS-episodes like the one from the "Motivation" section are uncorrelated between tenants.

For example, suppose we pick a limit that allows up to 10 tenants to go at limit IOPS, and our
For example, suppose our Pageserver can serve 100k IOPS total at a 100% page cache miss rate.
If each tenant gets a hard limit of 10k IOPS, we can serve up to 10 tenants at limit speed without latency degradation.

The mechanism for backpressure will be TCP-based implicit backpressure.
The compute team isn't concerned about prefetch queue depth.
Pageserver will implement it by delaying the reading of requests from the libpq connection(s).
Pageserver will implement starvation prevention, but not guaranteed fairness, among connections by using a fair semaphore.

Regarding metrics / the internal GetPage latency SLO:
we will measure the GetPage latency SLO _after_ the rate limiter and introduce a new metric to measure the amount of rate limiting, quantified by:
- histogram that records the tenants' observations of queue depth before they start waiting (one such histogram per pageserver)
- histogram that records the tenants' observations of time spent waiting (one such histogram per pageserver)

Further observability measures:
- a WARN log message at frequency 1/min if the tenant/timeline/connection spent any time waiting for the rate limit;
  the message will identify the tenant/timeline/connection to allow correlation with compute logs/stats

Rollout will happen as follows:
- deploy 1: implementation + config: disabled by default, ability to enable it per tenant through tenant_conf
- experimentation in staging and later production to study impact & interaction with auto-scaling
- determination of a sensible global default value
- deploy 2: implementation fixes if any + config: enabled by default with the aforementioned global default
- reset of the experimental per-tenant overrides

The per-tenant override will remain for emergencies and testing.
But since Console doesn't preserve it during tenant migrations, it isn't durably configurable for the tenant.

### Rationale

We decided against error + retry because of worries about starvation.

## Future Work

Enable per-tenant override via Console.
Should be part of a more general framework to specify tenant config overrides.
A simple JSON field in Admin UI would do.

Compute-side metrics for GetPage latency.

Back-channel to inform Compute that it's being rate-limited.

Compute-side neon_smgr improvements to avoid sending the same GetPage request multiple times if multiple backends experience a cache miss.

