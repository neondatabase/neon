use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use utils::id::{TenantId, TimelineId};

//
// Demonstrates that repeat label values lookup is a multicore scalability bottleneck
// that is worth avoiding.
//
criterion_group!(
    label_values,
    label_values::bench_naive_usage,
    label_values::bench_cache_label_values_lookup
);
mod label_values {
    use super::*;

    pub fn bench_naive_usage(c: &mut Criterion) {
        let mut g = c.benchmark_group("label_values__naive_usage");

        for ntimelines in [1, 4, 8] {
            g.bench_with_input(
                BenchmarkId::new("ntimelines", ntimelines),
                &ntimelines,
                |b, ntimelines| {
                    b.iter_custom(|iters| {
                        let barrier = std::sync::Barrier::new(*ntimelines + 1);

                        let timelines = (0..*ntimelines)
                            .map(|_| {
                                (
                                    TenantId::generate().to_string(),
                                    "0000".to_string(),
                                    TimelineId::generate().to_string(),
                                )
                            })
                            .collect::<Vec<_>>();

                        let metric_vec = metrics::UIntGaugeVec::new(
                            metrics::opts!("testmetric", "testhelp"),
                            &["tenant_id", "shard_id", "timeline_id"],
                        )
                        .unwrap();

                        std::thread::scope(|s| {
                            for (tenant_id, shard_id, timeline_id) in &timelines {
                                s.spawn(|| {
                                    barrier.wait();
                                    for _ in 0..iters {
                                        metric_vec
                                            .with_label_values(&[tenant_id, shard_id, timeline_id])
                                            .inc();
                                    }
                                    barrier.wait();
                                });
                            }
                            barrier.wait();
                            let start = std::time::Instant::now();
                            barrier.wait();
                            start.elapsed()
                        })
                    })
                },
            );
        }
        g.finish();
    }

    pub fn bench_cache_label_values_lookup(c: &mut Criterion) {
        let mut g = c.benchmark_group("label_values__cache_label_values_lookup");

        for ntimelines in [1, 4, 8] {
            g.bench_with_input(
                BenchmarkId::new("ntimelines", ntimelines),
                &ntimelines,
                |b, ntimelines| {
                    b.iter_custom(|iters| {
                        let barrier = std::sync::Barrier::new(*ntimelines + 1);

                        let timelines = (0..*ntimelines)
                            .map(|_| {
                                (
                                    TenantId::generate().to_string(),
                                    "0000".to_string(),
                                    TimelineId::generate().to_string(),
                                )
                            })
                            .collect::<Vec<_>>();

                        let metric_vec = metrics::UIntGaugeVec::new(
                            metrics::opts!("testmetric", "testhelp"),
                            &["tenant_id", "shard_id", "timeline_id"],
                        )
                        .unwrap();

                        std::thread::scope(|s| {
                            for (tenant_id, shard_id, timeline_id) in &timelines {
                                s.spawn(|| {
                                    let metric = metric_vec.with_label_values(&[
                                        tenant_id,
                                        shard_id,
                                        timeline_id,
                                    ]);
                                    barrier.wait();
                                    for _ in 0..iters {
                                        metric.inc();
                                    }
                                    barrier.wait();
                                });
                            }
                            barrier.wait();
                            let start = std::time::Instant::now();
                            barrier.wait();
                            start.elapsed()
                        })
                    })
                },
            );
        }
        g.finish();
    }
}

//
// Demonstrates that even a single metric can be a scalability bottleneck
// if multiple threads in it concurrently but there's nothing we can do
// about it without changing the metrics framework to use e.g. sharded counte atomics.
//
criterion_group!(
    single_metric_multicore_scalability,
    single_metric_multicore_scalability::bench,
);
mod single_metric_multicore_scalability {
    use super::*;

    pub fn bench(c: &mut Criterion) {
        let mut g = c.benchmark_group("single_metric_multicore_scalability");

        for nthreads in [1, 4, 8] {
            g.bench_with_input(
                BenchmarkId::new("nthreads", nthreads),
                &nthreads,
                |b, nthreads| {
                    b.iter_custom(|iters| {
                        let barrier = std::sync::Barrier::new(*nthreads + 1);

                        let metric = metrics::UIntGauge::new("testmetric", "testhelp").unwrap();

                        std::thread::scope(|s| {
                            for _ in 0..*nthreads {
                                s.spawn(|| {
                                    barrier.wait();
                                    for _ in 0..iters {
                                        metric.inc();
                                    }
                                    barrier.wait();
                                });
                            }
                            barrier.wait();
                            let start = std::time::Instant::now();
                            barrier.wait();
                            start.elapsed()
                        })
                    })
                },
            );
        }
        g.finish();
    }
}

//
// Demonstrates that even if we cache label value, the propagation of such a cached metric value
// by Clone'ing it is a scalability bottleneck.
// The reason is that it's an Arc internally and thus there's contention on the reference count atomics.
//
// We can avoid that by having long-lived references per thread (= indirection).
//
criterion_group!(
    propagation_of_cached_label_value,
    propagation_of_cached_label_value::bench_naive,
    propagation_of_cached_label_value::bench_long_lived_reference_per_thread,
);
mod propagation_of_cached_label_value {
    use std::sync::Arc;

    use super::*;

    pub fn bench_naive(c: &mut Criterion) {
        let mut g = c.benchmark_group("propagation_of_cached_label_value__naive");

        for nthreads in [1, 4, 8] {
            g.bench_with_input(
                BenchmarkId::new("nthreads", nthreads),
                &nthreads,
                |b, nthreads| {
                    b.iter_custom(|iters| {
                        let barrier = std::sync::Barrier::new(*nthreads + 1);

                        let metric = metrics::UIntGauge::new("testmetric", "testhelp").unwrap();

                        std::thread::scope(|s| {
                            for _ in 0..*nthreads {
                                s.spawn(|| {
                                    barrier.wait();
                                    for _ in 0..iters {
                                        // propagating the metric means we'd clone it into the child RequestContext
                                        let propagated = metric.clone();
                                        // simulate some work
                                        criterion::black_box(propagated);
                                    }
                                    barrier.wait();
                                });
                            }
                            barrier.wait();
                            let start = std::time::Instant::now();
                            barrier.wait();
                            start.elapsed()
                        })
                    })
                },
            );
        }
        g.finish();
    }

    pub fn bench_long_lived_reference_per_thread(c: &mut Criterion) {
        let mut g =
            c.benchmark_group("propagation_of_cached_label_value__long_lived_reference_per_thread");

        for nthreads in [1, 4, 8] {
            g.bench_with_input(
                BenchmarkId::new("nthreads", nthreads),
                &nthreads,
                |b, nthreads| {
                    b.iter_custom(|iters| {
                        let barrier = std::sync::Barrier::new(*nthreads + 1);

                        let metric = metrics::UIntGauge::new("testmetric", "testhelp").unwrap();

                        std::thread::scope(|s| {
                            for _ in 0..*nthreads {
                                s.spawn(|| {
                                    // This is the technique.
                                    let this_threads_metric_reference = Arc::new(metric.clone());

                                    barrier.wait();
                                    for _ in 0..iters {
                                        // propagating the metric means we'd clone it into the child RequestContext
                                        let propagated = Arc::clone(&this_threads_metric_reference);
                                        // simulate some work (include the pointer chase!)
                                        criterion::black_box(&*propagated);
                                    }
                                    barrier.wait();
                                });
                            }
                            barrier.wait();
                            let start = std::time::Instant::now();
                            barrier.wait();
                            start.elapsed()
                        })
                    })
                },
            );
        }
    }
}

criterion_main!(
    label_values,
    single_metric_multicore_scalability,
    propagation_of_cached_label_value
);
