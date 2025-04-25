# CPU and Memory Profiling

Created 2025-01-12 by Erik Grinaker.

See also [internal user guide](https://www.notion.so/neondatabase/Storage-CPU-Memory-Profiling-14bf189e004780228ec7d04442742324?pvs=4).

## Summary

This document proposes a standard cross-team pattern for CPU and memory profiling across
applications and languages, using the [pprof](https://github.com/google/pprof) profile format.

It enables both ad hoc profiles via HTTP endpoints, and continuous profiling across the fleet via
[Grafana Cloud Profiles](https://grafana.com/docs/grafana-cloud/monitor-applications/profiles/).
Continuous profiling incurs an overhead of about 0.1% CPU usage and 3% slower heap allocations.

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

This is not the case for Rust and C, where obtaining profiles can be rather cumbersome. It requires
installing and running additional tools like `perf` as root on production nodes, with analysis tools
that can be hard to use and often don't give good results. This is not only annoying, but can also
significantly affect the resolution time of production incidents.

This proposal will:

* Provide CPU and heap profiles in pprof format via HTTP API.
* Record continuous profiles in Grafana for aggregate historical analysis.
* Make it easy for anyone to see a flamegraph in less than one minute.
* Be reasonably consistent across teams and services (Rust, Go, C).

## Non Goals (For Now)

* [Additional profile types](https://grafana.com/docs/pyroscope/next/configure-client/profile-types/)
  like mutexes, locks, goroutines, etc.
* [Runtime trace integration](https://grafana.com/docs/pyroscope/next/configure-client/trace-span-profiles/).
* [Profile-guided optimization](https://en.wikipedia.org/wiki/Profile-guided_optimization).

## Using Profiles

Ready-to-use profiles can be obtained using e.g. `curl`. For Rust services:

```
$ curl localhost:9898/profile/cpu >profile.pb.gz
```

pprof profiles can be explored using the [`pprof`](https://github.com/google/pprof) web UI, which
provides flamegraphs, call graphs, plain text listings, and more:

```
$ pprof -http :6060 <profile>
```

Some endpoints (e.g. Rust-based ones) can also generate flamegraph SVGs directly:

```
$ curl localhost:9898/profile/cpu?format=svg >profile.svg
$ open profile.svg
```

Continuous profiles are available in Grafana under Explore → Profiles → Explore Profiles
(currently only in [staging](https://neonstaging.grafana.net/a/grafana-pyroscope-app/profiles-explorer)).

## API Requirements

* HTTP endpoints that return a profile in pprof format (with symbols).
  * CPU: records a profile over the request time interval (`seconds` query parameter).
  * Memory: returns the current in-use heap allocations.
* Unauthenticated, as it should not expose user data or pose a denial-of-service risk.
* Default sample frequency should not impact service (maximum 5% CPU overhead).
* Linux-compatibility.

Nice to have:

* Return flamegraph SVG directly from the HTTP endpoint if requested.
* Configurable sample frequency for CPU profiles.
* Historical heap allocations, by count and bytes.
* macOS-compatiblity.

## Rust Profiling

[`libs/utils/src/http/endpoint.rs`](https://github.com/neondatabase/neon/blob/8327f68043e692c77f70d6a6dafa463636c01578/libs/utils/src/http/endpoint.rs)
contains ready-to-use HTTP endpoints for CPU and memory profiling:
[`profile_cpu_handler`](https://github.com/neondatabase/neon/blob/8327f68043e692c77f70d6a6dafa463636c01578/libs/utils/src/http/endpoint.rs#L338) and [`profile_heap_handler`](https://github.com/neondatabase/neon/blob/8327f68043e692c77f70d6a6dafa463636c01578/libs/utils/src/http/endpoint.rs#L416).

### CPU

CPU profiles are provided by [pprof-rs](https://github.com/tikv/pprof-rs) via
[`profile_cpu_handler`](https://github.com/neondatabase/neon/blob/8327f68043e692c77f70d6a6dafa463636c01578/libs/utils/src/http/endpoint.rs#L338).
Expose it unauthenticated at `/profile/cpu`.

Parameters:

* `format`: profile output format (`pprof` or `svg`; default `pprof`).
* `seconds`: duration to collect profile over, in seconds (default `5`).
* `frequency`: how often to sample thread stacks, in Hz (default `99`).
* `force`: if `true`, cancel a running profile and start a new one (default `false`).

Works on Linux and macOS.

### Memory

Use the jemalloc allocator via [`tikv-jemallocator`](https://github.com/tikv/jemallocator),
and enable profiling with samples every 2 MB allocated:

```rust
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:21\0";
```

pprof profiles are generated by
[`jemalloc-pprof`](https://github.com/polarsignals/rust-jemalloc-pprof) via
[`profile_heap_handler`](https://github.com/neondatabase/neon/blob/8327f68043e692c77f70d6a6dafa463636c01578/libs/utils/src/http/endpoint.rs#L416).
Expose it unauthenticated at `/profile/heap`.

Parameters:

* `format`: profile output format (`pprof`, `svg`, or `jemalloc`; default `pprof`).

Works on Linux only, due to [jemalloc limitations](https://github.com/jemalloc/jemalloc/issues/26).

## Go Profiling

The Go standard library includes pprof profiling via HTTP API in
[`net/http/pprof`](https://pkg.go.dev/net/http/pprof). Expose it unauthenticated at
`/debug/pprof`.

Works on Linux and macOS.

### CPU 

Via `/debug/pprof/profile`. Parameters:

* `debug`: profile output format (`0` is pprof, `1` or above is plaintext; default `0`).
* `seconds`: duration to collect profile over, in seconds (default `30`).

Does not support a frequency parameter (see [#57488](https://github.com/golang/go/issues/57488)),
and defaults to 100 Hz. A lower frequency can be hardcoded via `SetCPUProfileRate`, but the default
is likely ok (estimated 1% overhead).

### Memory

Via `/debug/pprof/heap`. Parameters:

* `seconds`: take a delta profile over the given duration, in seconds (default `0`).
* `gc`: if `1`, garbage collect before taking profile.

## C Profiling

[gperftools](https://github.com/gperftools/gperftools) provides in-process CPU and heap profiling
with pprof output.

However, continuous profiling of PostgreSQL is expensive (many computes), and has limited value
since we don't own the internals anyway.

Ad hoc profiling might still be useful, but the compute team considers existing tooling sufficient,
so this is not a priority at the moment.

## Grafana Continuous Profiling

[Grafana Alloy](https://grafana.com/docs/alloy/latest/) continually scrapes CPU and memory profiles
across the fleet, and archives them as time series. This can be used to analyze resource usage over
time, either in aggregate or zoomed in to specific events and nodes.

Profiles are retained for 30 days. Profile ingestion volume for CPU+heap at 60-second intervals
is about 0.5 GB/node/day, or about $0.25/node/day = $7.5/node/month ($0.50/GB).

It is currently enabled in [staging](https://neonstaging.grafana.net/a/grafana-pyroscope-app/profiles-explorer)
for Pageserver and Safekeeper.

### Scraping

* CPU profiling: 59 seconds at 19 Hz every 60 seconds.
* Heap profiling: heap snapshot with 2 MB frequency every 60 seconds.

There are two main approaches that can be taken for CPU profiles:

* Continuous low-frequency profiles (e.g. 19 Hz for 60 seconds every 60 seconds).
* Occasional high-frequency profiles (e.g. 99 Hz for 5 seconds every 60 seconds).

We choose continuous low-frequency profiles where possible. This has a fixed low overhead, instead
of a spiky high overhead. It likely also gives a more representative view of resource usage.
However, a 19 Hz rate gives a minimum resolution of 52.6 ms per sample, which may be larger than the
actual runtime of small functions. Note that Go does not support a frequency parameter, so we must
use a fixed frequency for all profiles via `SetCPUProfileRate()` (default 100 Hz).

Only one CPU profile can be taken at a time. With continuous profiling, one will always be running.
To allow also taking an ad hoc CPU profile, the Rust endpoint supports a `force` query parameter to
cancel a running profile and start a new one.

### Overhead

With Rust:

* CPU profiles at 19 Hz frequency: 0.1% overhead.
* Heap profiles at 2 MB frequency: 3% allocation overhead.
* Profile call/encoding/symbolization: 20 ms every 60 seconds, or 0.03% of 1 CPU (for Pageserver).
* Profile symbolization caches: 125 MB memory, or 0.4% of 32 GB (for Pageserver).

Benchmarks with pprof-rs showed that the CPU time for taking a stack trace of a 40-frame stack was
11 µs using the `frame-pointer` feature, and 1.4 µs using `libunwind` with DWARF. `libunwind` saw
frequent seg faults, so we use `frame-pointer` and build binaries with frame pointers (negligible
overhead).

CPU profiles work by installing an `ITIMER_PROF` for the process, which triggers a `SIGPROF` signal
after a given amount of cumulative CPU time across all CPUs. The signal handler will run for one
of the currently executing threads and take a stack trace. Thus, a 19 Hz profile will take 1 stack
trace every 52.6 ms CPU time -- assuming 11 µs for a stack trace, this is 0.02% overhead, but
likely 0.1% in practice (given e.g. context switches).

Heap profiles work by probabilistically taking a stack trace on allocations, adjusted for the
allocation size. A 1 MB allocation takes about 15 µs in benchmarks, and a stack trace about 1 µs,
so we can estimate that a 2 MB sampling frequency has about 3% allocation overhead -- this is 
consistent with benchmarks. This is significantly larger than CPU profiles, but mitigated by the
fact that performance-sensitive code will avoid allocations as far as possible.

Profile symbolization uses in-memory caches for symbol lookups. These take about 125 MB for
Pageserver.

## Alternatives Considered

* eBPF profiles.
  * Don't require instrumenting the binary.
  * Use less resources.
  * Can profile in kernel space too.
  * Supported by Grafana.
  * Less information about stack frames and spans.
  * Limited tooling for local analysis.
  * Does not support heap profiles.
  * Does not work on macOS.

* [Polar Signals](https://www.polarsignals.com) instead of Grafana.
  * We already use Grafana for everything else. Appears good enough.
