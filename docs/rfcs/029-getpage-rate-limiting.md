# Throttling GetPage@LSN

## Summary

This RFC proposes basic rate-limiting GetPage@LSN requests inside Pageserver
and the interactions with its client, i.e., the neon_smgr component in Compute.

## Background

### GetPage@LSN Request Flow

Pageserver exposes its `page_service.rs` as a libpq listener.
The Computes' `neon_smgr` module connects to that libpq listener.
Once a connection is established, the protocol allows compute to request page images at a given LSN.
We call these requests GetPage@LSN requests, or GetPage requests for short.
There are other request types that can be sent, but these are low traffic compared to GetPage requests
and not the concern of this RFC.

Pageserver associates one libpq connection with one tokio task.

Per connection/task, the pq protocol is handled by the common `postgres_backend` crate.
Its `run_message_loop` function invokes the `page_service` specific `impl<IO> postgres_backend::Handler<IO> for PageServerHandler`.
Requests are processed in the order in which they are arrive via the TCP-based pq protocol.
So, there is no concurrent request processing within one connection/task.

There is natural pipelining if page requests are independent:
Compute can send more than one GetPage request into the libpq TCP stream,
and Pageserver can send multiple responses; both subject to the limit of tx/rx buffers, TCP flow control, etc.
NB: we have not disabled buffering / TCP_NODELAY on the pageserver side.

### GetPage@LSN Access Pattern

The compute has its own hierarchy of caches, specifically, shared_buffers and the local file cache plugin.
Compute only issues GetPage requests to Pageserver if it encounters a miss in these caches.

If the working set stops fitting into Compute's caches, requests to Pageserver increase sharply -- the Compute starts thrashing.

## Motivation

In INC-69 a tenant issued peak 155k GetPage/second for a period of 10 minutes and 60k GetPage/second for a period of 3h, then dropping to ca 18k GetPage/second for a period of 9h.

We noticed this because of internal SLO burn rate alerts, i.e., the request latency profile during this period significantly exceeded what was acceptable according to the internal SLO.

Sadly, we do not have the observability data to determine the impact of this tenant on other tenants on the same tenants.

However, here are some illustrative data points for the 155k period:
The tenant was >= 99% of the GetPage traffic (and frankly: overall activity) on its instance.
We were serving pages at 10 GiB/s (`155k x 8 kbyte (PAGE_SZ) per second is 1.12GiB/s = 9.4Gb/s.`)
The CPU utilization of the instance was 75% user+system.
Pageserver page cache served 1.75M accesses/second at a hit rate of ca 90%.
The hit rate for materialized pages was ca 40%.
Curiously, IOPS to NVMe were very low, rarey exceeding 100.

The fact that the IOPS were so low / the materialized page cache hit rate was so high suggests that **this tenant's compute's caches were thrashing**.
The compute was of type `k8s-pod` and hence auto-scaling could/would not have helped remediate the thrasing by provisioning more RAM.
The consequence was that the **thrashing translated into excessive GetPage requests against Pageserver**.

My claim is that it was **unhealthy to serve this workload at the pace we did**:
* it is likely that other tenants were/would have experienced high latencies (again, we sadly don't have per-tenant latency data to confirm this)
* more importantly, it was **unsustainable** to serve traffic at this pace, for multiple reasons:
    * **predictability of performance**: when the working set grows further, the pageserver materialized page cache hit rate eventually drops.
      At that point we're bound by the EC2 Instance Store NVMe drive's IOPS limit.
      The result is an **uneven** performance profile from the Compute perspective.

    * **business**: Neon currently does not charge for IOPS, only for capacity.
      **We cannot afford to undercut the market in IOPS/$ this drastically; it leads to adverse selection and perverts incentives.**
      For example, the 155k IOPS which we served for 10min would cost ca 6.5k$/month when provisioned as an io2 EBS volume.
      Even the 18k IOPS which we served for 9h would cost ca 1.1k$/month when provisoined as an io2 EBS volume.
      We charge 0$.

Note: It is helpful to think of Pageserver as a disk, because it's exactly where neon_smgr sits: vanilla Postgres gets its pages from disk, Neon Postgres gets them from Pageserver.
So, regarding above performance & economical arguments, it is fair to say that we currently provide an "as-fast-as-possible-IOPS" disk that we charge for only by capacity.

## Solution: Limiting IOPS

**The consequence of above analysis must be that Pageserver limits IOPS, unless we want to start charging for IOPS**.
This sets the correct incentive to scale up computes (=> DRAM) to a level that's adequate for the given workload.
Neon makes this exceptionally easy through Neon Autoscaling.

## The Design Space

What that remains is the question about *policy* and *mechanism*:

**Policy** concerns itself with the question of what limit applies to a given connection|timeline|tenant.
Candidates are:

* hard limit, same limit value per connection|timeline|tenant
    * Per-tenant will provide an upper bound for the impact of a tenant on a given Pageserver instance.
      This is a major operational pain point / risk right now.
* hard limit, configurable per connection|timeline|tenant
    * This outsources policy to console/control plane, with obvious advantages for flexible structing of what service we offer to customers.
    * However, without also doing capacity planning in control plane, we cannot guarantee any lower bounds.
* fair share among active connections|timelines|tenants per instance
    * example: divide available IOPS evenly among the tenants that were active in the last 5min
    * needs definition of "active", and knowledge of available IOPS in advance
* ...


Regarding **mechanism**, it's clear that **backpressure** is the way to go.
However, we must choose between
* **implicit** backpressure through pq/TCP and
* **explicit** rejection of requests + retries with exponential backoff

Further, there is the question of how limiting IOPS will affect the **internal getpage latency SLO**:
where do we measure the SLI for Pageserver's internal getpage latency SLO? Before or after the rate limiter?

And when we eventually move the measurement point into the Computes (to avoid coordinated omission),
how do we avoid counting rate-limiting induced latency toward the internal getpage latency SLI/SLO?

## Scope Of This RFC

This RFC proposes the introduction of a hard IOPS limit per tenant, with the same value applying to each tenant on a Pageserver.

This is extremely easy to implement and de-risks operating large Pageservers under the assumption that extremely-high-IOPS-episodes like the one from the "Motivation" section are uncorrelated between tenants.

For example, if we pick a limit that allows up to 10 tenants to go at limit IOPS, and our page server can serve 100k IOPS total at a 100% page cache miss rate, we can pick a per-tenant hard limit of 10k IOPS and serve up to 10 tenants that experience a high-IOPS-episode.

We will measure the getpage latency SLO _after_ the rate limiter and introduce a new metric for the number of  tenants and amount of time that was spent waiting for the rate limiter.
This new metric will not be an SLO, but will be used to quickly rule out pageserver slowness concerns during performance investigations.

The mechanism for backpressure is still subject to debate.
