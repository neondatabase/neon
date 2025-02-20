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

        let runtime = tokio::runtime::Builder::new_current_thread() // single is fine, sync IO only
            .enable_all()
            .build()?;

        // Test both with and without warn_slow, since we're essentially measuring Tokio scheduling
        // performance too.
        if enabled {
            b.iter(|| runtime.block_on(warn_slow("ready", THRESHOLD, std::future::ready(()))));
        } else {
            b.iter(|| runtime.block_on(std::future::ready(())));
        }

        Ok(())
    }
}
