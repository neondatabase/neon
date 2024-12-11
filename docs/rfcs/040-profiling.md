# CPU and Memory Profiling

Created 2024-12-11 by Erik Grinaker.

See also [internal user guide](https://www.notion.so/neondatabase/Storage-CPU-Memory-Profiling-14bf189e004780228ec7d04442742324?pvs=4).

## Summary

This document proposes a standard cross-team pattern for CPU and memory profiling across
applications and languages, using the [pprof](https://github.com/google/pprof) profile format.

It enables both ad hoc profiles via HTTP endpoints, and continuous profiling via [Grafana Cloud
Profiles](https://grafana.com/docs/grafana-cloud/monitor-applications/profiles/) across the fleet.

## Motivation

CPU and memory profiles are crucial observability tools for understanding performance issues,
resource exhaustion, and resource costs. They allow answering questions like:

* Why is this process using 100% CPU?
* How do I make this go faster?
* Why did this process run out of memory?
* Why are we paying for all these CPU cores and memory chips?

Go has [first-class support](https://pkg.go.dev/net/http/pprof) for profiling included in its
standard library, using the [pprof](https://github.com/google/pprof) profile format and associated
tooling.

This is not the case for Rust and C, where obtaining profiles is currently rather cumbersome. It
requires installing and running additional tools like `perf` as root on production nodes, with
analysis tools that can be hard to use and often don't give good results. This is not only annoying,
but can also significantly affect the resolution time of production incidents.

This proposal aims to:

* Provide CPU and heap profiles in pprof format via HTTP API.
* Record continuous profiles in Grafana for aggregate historical analysis.
* Make it easy for anyone to see a flamegraph in less than one minute.
* Be reasonably consistent across teams and services (Rust, Go, C).

## Non Goals

These are out of scope here, but may be considered later.

* [Additional profile types](https://grafana.com/docs/pyroscope/next/configure-client/profile-types/)
  like mutexes, locks, goroutines, etc.
* [Runtime trace integration](https://grafana.com/docs/pyroscope/next/configure-client/trace-span-profiles/).
* [Profile-guided optimization](https://en.wikipedia.org/wiki/Profile-guided_optimization).

## Using Profiles

Ready-to-use profiles will be obtainable using e.g. `curl`:

```
$ curl localhost:7676/profile/cpu >profile.pb
```

pprof profiles can be explored using the [`pprof`](https://github.com/google/pprof) web UI, which
provides flamegraphs, call graphs, plain text listings, and more:

```
$ pprof -http :6060 <profile>
```

Some endpoints (e.g. Rust-based ones) may also be able to generate e.g. flamegraph SVGs directly:

```
$ curl localhost:7676/profile/cpu?format=svg >profile.svg
$ open profile.svg
```

Continuous profiles will be available in Grafana under Explore → Profiles → Explore Profiles
(currently only in [staging](https://neonstaging.grafana.net/a/grafana-pyroscope-app/profiles-explorer)).

## CPU Profiling

Requirements:

* HTTP endpoint that takes a CPU profile over the request time interval.
* Returns profile in pprof format, with symbols.
* `seconds` query parameter specifying the profile duration.
* Default sample frequency should not impact service (at most 5% CPU).
* Unauthenticated. Do not expose user data or risk denial-of-service. TODO: reconsider.
* Linux-compatibile.

Optional:

* `frequency` query parameter specifying the sample frequency in Hertz.
* Emit human-readable profile formats (e.g. SVG flamegraph or plain text).
* macOS-compatibile.

### Rust CPU Profiling

Use [pprof-rs](https://github.com/tikv/pprof-rs) via
[`profile_cpu_handler`](https://github.com/neondatabase/neon/blob/dcb24ce170573a2ae6ed29467669d03c73b589e6/libs/utils/src/http/endpoint.rs#L336)
from `lib/utils/src/http/endpoint.rs`. Expose it unauthenticated at `/profile/cpu`.

Parameters:

* `format`: profile output format (`pprof` or `svg`; default `pprof`).
* `seconds`: duration to collect profile over, in seconds (default `5`).
* `frequency`: how often to sample thread stacks, in Hz (default `99`).

Works on Linux and macOS.

### Go CPU Profiling

Use [net/http/pprof](https://pkg.go.dev/net/http/pprof). Expose it unauthenticated at
`/debug/pprof/profile`.

Parameters:

* `debug`: profile output format (`0` is pprof, `1` or above is plaintext; default `0`).
* `seconds`: duration to collect profile over, in seconds (default `30`).

Does not support a frequency parameter (see [#57488](https://github.com/golang/go/issues/57488));
uses 100 Hz by default. TODO: consider hardcoding a lower frequency if needed, via 
`SetCPUProfileRate()`.

Works on Linux and macOS.

### C CPU Profiling

Unclear if there is a C library for in-process pprof profiling.

Alternatively, can use [eBPF profiling](https://pyroscope.io/blog/ebpf-profiling-pros-cons/),
potentially together with [pprof-ebpf](https://github.com/cpg1111/pprof-ebpf) to convert them
to pprof with a sidecar service for HTTP access. This would be Linux-only.

TODO: explore options.

## Memory Profiling

Requirements:

* HTTP endpoint that returns a profile of the current heap allocations by size.
* Returns profile in pprof format, with symbols.
* Sample frequency should not impact service (at most 5% CPU).
* Unauthenticated. Do not expose user data or risk denial-of-service.
* Linux compatibility.

Optional:

* Return a profile with historical allocations by count and size.
* Emit human-readable profile formats (e.g. SVG flamegraph or plain text).
* macOS compatibility.

### Rust Memory Profiling

Use the jemalloc allocator via [tikv-jemallocator](https://github.com/tikv/jemallocator),
and enable profiling with samples every 1 MB allocated:

```rust
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:20\0";
```

Use [`profile_heap_handler`](https://github.com/neondatabase/neon/blob/dcb24ce170573a2ae6ed29467669d03c73b589e6/libs/utils/src/http/endpoint.rs#L414)
from `lib/utils/src/http/endpoint.rs`. Expose it unauthenticated at `/profile/heap`.

Parameters:

* `format`: profile output format (`pprof` or `jemalloc`; default `pprof`).

TODO: the profile is [not yet symbolized](https://github.com/neondatabase/neon/issues/9964).

Works on Linux only.

### Go Memory Profiling

Use [net/http/pprof](https://pkg.go.dev/net/http/pprof). Expose it unauthenticated at
`/debug/pprof/heap`.

Parameters:

* `seconds`: duration to collect profile over, in seconds (default `30`).

Does not support a frequency parameter (see [#57488](https://github.com/golang/go/issues/57488));
always uses 100 Hz.

Works on Linux and macOS.

### C Memory Profiling

Unclear if there is a C library for in-process pprof heap profiling.

TODO: explore options.

## Grafana Continuous Profiling

[Grafana Alloy](https://grafana.com/docs/alloy/latest/) will continually scrape CPU and memory
profiles across the fleet, and archive them as time series. This can be used to analyze resource
usage over time, either in aggregate or zoomed in to specific events and nodes.

It is currently enabled in [staging](https://neonstaging.grafana.net/a/grafana-pyroscope-app/profiles-explorer),
with CPU profiles for Safekeeper and Pageserver.

TODO: decide on retention period.

### Scrape Frequency

There are two main approaches that can be taken for CPU profiles:

* Continuous low-frequency profiles (e.g. 11 Hz for 20 seconds every 20 seconds).
* Occasional high-frequency profiles (e.g. 99 Hz for 5 seconds every 60 seconds).

We prefer continuous low-frequency profiles where possible. This has a fixed low overhead, instead
of a spiky high overhead. It likely also gives a more representative view of resource usage.
However, a 11 Hz rate will consider all samples to take a minimum of 90.9 ms CPU time, which may be
much larger than the actual runtime.

Note that Go does not support a frequency parameter, so we must set a fixed frequency for all
profiles via `SetCPUProfileRate()` (default 100 Hz).

Typically, only 1 CPU profile can be taken at a time. This means that we can't take ad hoc CPU
profiles while a continuous profile is running. If necessary, we either have to stop Alloy, or
[add an override parameter](https://github.com/neondatabase/neon/issues/10072).

TODO: decide on an optimal sample frequency. Ideally below 1% CPU usage.

### Unresolved Questions

* Should we standardize on pprof?
* Should we use Grafana Cloud Profiles?
* Which sample frequency should we use for continuous profiles?
* How long should we retain continuous profiles for?
* Should we use authentication for profile endpoints?
* Should we add profiling for C (i.e. PostgreSQL)?

## Alternatives Considered

* eBPF profiles instead of pprof.
  * Don't require instrumenting the binary.
  * Use less resources.
  * Can profile in kernel space too.
  * Supported by Grafana.
  * Less information about stack frames and spans.
  * Limited tooling for local analysis.
  * Does not work on macOS.

* [Polar Signals](https://www.polarsignals.com) instead of Grafana.
  * We already use Grafana for everything else. Appears good enough.
