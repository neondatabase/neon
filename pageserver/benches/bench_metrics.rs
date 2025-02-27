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

/*
RUST_BACKTRACE=full cargo bench --bench bench_metrics --  --discard-baseline --noplot

Results on an im4gn.2xlarge instance

label_values__naive_usage/ntimelines/1 time:   [178.71 ns 178.74 ns 178.76 ns]
label_values__naive_usage/ntimelines/4 time:   [532.94 ns 539.59 ns 546.31 ns]
label_values__naive_usage/ntimelines/8 time:   [1.1082 µs 1.1109 µs 1.1135 µs]
label_values__cache_label_values_lookup/ntimelines/1 time:   [6.4116 ns 6.4119 ns 6.4123 ns]
label_values__cache_label_values_lookup/ntimelines/4 time:   [6.3482 ns 6.3819 ns 6.4079 ns]
label_values__cache_label_values_lookup/ntimelines/8 time:   [6.4213 ns 6.5279 ns 6.6293 ns]
single_metric_multicore_scalability/nthreads/1 time:   [6.0102 ns 6.0104 ns 6.0106 ns]
single_metric_multicore_scalability/nthreads/4 time:   [38.127 ns 38.275 ns 38.416 ns]
single_metric_multicore_scalability/nthreads/8 time:   [73.698 ns 74.882 ns 75.864 ns]
propagation_of_cached_label_value__naive/nthreads/1 time:   [14.424 ns 14.425 ns 14.426 ns]
propagation_of_cached_label_value__naive/nthreads/4 time:   [100.71 ns 102.53 ns 104.35 ns]
propagation_of_cached_label_value__naive/nthreads/8 time:   [211.50 ns 214.44 ns 216.87 ns]
propagation_of_cached_label_value__long_lived_reference_per_thread/nthreads/1 time:   [14.135 ns 14.147 ns 14.160 ns]
propagation_of_cached_label_value__long_lived_reference_per_thread/nthreads/4 time:   [14.243 ns 14.255 ns 14.268 ns]
propagation_of_cached_label_value__long_lived_reference_per_thread/nthreads/8 time:   [14.470 ns 14.682 ns 14.895 ns]

Results on an M4 MAX MacBook Pro   Total Number of Cores:	14 (10 performance and 4 efficiency)

label_values__naive_usage/ntimelines/1      time:   [52.711 ns 53.026 ns 53.381 ns]
label_values__naive_usage/ntimelines/4      time:   [323.99 ns 330.40 ns 337.53 ns]
label_values__naive_usage/ntimelines/8      time:   [1.1615 µs 1.1998 µs 1.2399 µs]
label_values__cache_label_values_lookup/ntimelines/1      time:   [1.6635 ns 1.6715 ns 1.6809 ns]
label_values__cache_label_values_lookup/ntimelines/4      time:   [1.7786 ns 1.7876 ns 1.8028 ns]
label_values__cache_label_values_lookup/ntimelines/8      time:   [1.8195 ns 1.8371 ns 1.8665 ns]
single_metric_multicore_scalability/nthreads/1      time:   [1.7764 ns 1.7909 ns 1.8079 ns]
single_metric_multicore_scalability/nthreads/4      time:   [33.875 ns 34.868 ns 35.923 ns]
single_metric_multicore_scalability/nthreads/8      time:   [226.85 ns 235.30 ns 244.18 ns]
propagation_of_cached_label_value__naive/nthreads/1     time:   [3.4337 ns 3.4491 ns 3.4660 ns]
propagation_of_cached_label_value__naive/nthreads/4     time:   [69.486 ns 71.937 ns 74.472 ns]
propagation_of_cached_label_value__naive/nthreads/8     time:   [434.87 ns 456.47 ns 477.84 ns]
propagation_of_cached_label_value__long_lived_reference_per_thread/nthreads/1     time:   [3.3767 ns 3.3974 ns 3.4220 ns]
propagation_of_cached_label_value__long_lived_reference_per_thread/nthreads/4     time:   [3.6105 ns 4.2355 ns 5.1463 ns]
propagation_of_cached_label_value__long_lived_reference_per_thread/nthreads/8     time:   [4.0889 ns 4.9714 ns 6.0779 ns]
*/
