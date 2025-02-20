use std::time::Duration;

use criterion::{criterion_group, criterion_main, Bencher, Criterion};
use pprof::criterion::{Output, PProfProfiler};
use utils::id;
use utils::logging::warn_slow;

// Register benchmarks with Criterion.
criterion_group!(
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_id_stringify,
    bench_warn_slow,
);
criterion_main!(benches);

pub fn bench_id_stringify(c: &mut Criterion) {
    // Can only use public methods.
    let ttid = id::TenantTimelineId::generate();

    c.bench_function("id.to_string", |b| {
        b.iter(|| {
            // FIXME measurement overhead?
            //for _ in 0..1000 {
            //    ttid.tenant_id.to_string();
            //}
            ttid.tenant_id.to_string();
        })
    });
}

pub fn bench_warn_slow(c: &mut Criterion) {
    for enabled in [false, true] {
        c.bench_function(&format!("warn_slow/enabled={enabled}"), |b| {
            run_bench(b, enabled).unwrap()
        });
    }

    // The actual benchmark.
    fn run_bench(b: &mut Bencher, enabled: bool) -> anyhow::Result<()> {
        const THRESHOLD: Duration = Duration::from_secs(1);

        // Use a multi-threaded runtime to avoid thread parking overhead when yielding.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;

        // Test both with and without warn_slow, since we're essentially measuring Tokio scheduling
        // performance too. Use a simple noop future that yields once, to avoid any scheduler fast
        // paths for a ready future.
        if enabled {
            b.iter(|| {
                runtime.block_on(warn_slow("ready", THRESHOLD, async {
                    tokio::task::yield_now().await;
                }))
            });
        } else {
            b.iter(|| {
                runtime.block_on(async {
                    tokio::task::yield_now().await;
                })
            });
        }

        Ok(())
    }
}
