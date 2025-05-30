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

criterion_group!(histograms, histograms::bench_bucket_scalability);
mod histograms {
    use std::time::Instant;

    use criterion::{BenchmarkId, Criterion};
    use metrics::core::Collector;

    pub fn bench_bucket_scalability(c: &mut Criterion) {
        let mut g = c.benchmark_group("bucket_scalability");

        for n in [1, 4, 8, 16, 32, 64, 128, 256] {
            g.bench_with_input(BenchmarkId::new("nbuckets", n), &n, |b, n| {
                b.iter_custom(|iters| {
                    let buckets: Vec<f64> = (0..*n).map(|i| i as f64 * 100.0).collect();
                    let histo = metrics::Histogram::with_opts(
                        metrics::prometheus::HistogramOpts::new("name", "help")
                            .buckets(buckets.clone()),
                    )
                    .unwrap();
                    let start = Instant::now();
                    for i in 0..usize::try_from(iters).unwrap() {
                        histo.observe(buckets[i % buckets.len()]);
                    }
                    let elapsed = start.elapsed();
                    // self-test
                    let mfs = histo.collect();
                    assert_eq!(mfs.len(), 1);
                    let metrics = mfs[0].get_metric();
                    assert_eq!(metrics.len(), 1);
                    let histo = metrics[0].get_histogram();
                    let buckets = histo.get_bucket();
                    assert!(
                        buckets
                            .iter()
                            .enumerate()
                            .all(|(i, b)| b.get_cumulative_count()
                                >= i as u64 * (iters / buckets.len() as u64))
                    );
                    elapsed
                })
            });
        }
    }
}

criterion_main!(
    label_values,
    single_metric_multicore_scalability,
    propagation_of_cached_label_value,
    histograms,
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
bucket_scalability/nbuckets/1     time:   [30.352 ns 30.353 ns 30.354 ns]
bucket_scalability/nbuckets/4     time:   [30.464 ns 30.465 ns 30.467 ns]
bucket_scalability/nbuckets/8     time:   [30.569 ns 30.575 ns 30.584 ns]
bucket_scalability/nbuckets/16      time:   [30.961 ns 30.965 ns 30.969 ns]
bucket_scalability/nbuckets/32      time:   [35.691 ns 35.707 ns 35.722 ns]
bucket_scalability/nbuckets/64      time:   [47.829 ns 47.898 ns 47.974 ns]
bucket_scalability/nbuckets/128     time:   [73.479 ns 73.512 ns 73.545 ns]
bucket_scalability/nbuckets/256     time:   [127.92 ns 127.94 ns 127.96 ns]

Results on an i3en.3xlarge instance

label_values__naive_usage/ntimelines/1      time:   [117.32 ns 117.53 ns 117.74 ns]
label_values__naive_usage/ntimelines/4      time:   [736.58 ns 741.12 ns 745.61 ns]
label_values__naive_usage/ntimelines/8      time:   [1.4513 µs 1.4596 µs 1.4665 µs]
label_values__cache_label_values_lookup/ntimelines/1      time:   [8.0964 ns 8.0979 ns 8.0995 ns]
label_values__cache_label_values_lookup/ntimelines/4      time:   [8.1620 ns 8.2912 ns 8.4491 ns]
label_values__cache_label_values_lookup/ntimelines/8      time:   [14.148 ns 14.237 ns 14.324 ns]
single_metric_multicore_scalability/nthreads/1      time:   [8.0993 ns 8.1013 ns 8.1046 ns]
single_metric_multicore_scalability/nthreads/4      time:   [80.039 ns 80.672 ns 81.297 ns]
single_metric_multicore_scalability/nthreads/8      time:   [153.58 ns 154.23 ns 154.90 ns]
propagation_of_cached_label_value__naive/nthreads/1     time:   [13.924 ns 13.926 ns 13.928 ns]
propagation_of_cached_label_value__naive/nthreads/4     time:   [143.66 ns 145.27 ns 146.59 ns]
propagation_of_cached_label_value__naive/nthreads/8     time:   [296.51 ns 297.90 ns 299.30 ns]
propagation_of_cached_label_value__long_lived_reference_per_thread/nthreads/1     time:   [14.013 ns 14.149 ns 14.308 ns]
propagation_of_cached_label_value__long_lived_reference_per_thread/nthreads/4     time:   [14.311 ns 14.625 ns 14.984 ns]
propagation_of_cached_label_value__long_lived_reference_per_thread/nthreads/8     time:   [25.981 ns 26.227 ns 26.476 ns]

Results on an Standard L16s v3 (16 vcpus, 128 GiB memory)  Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz

label_values__naive_usage/ntimelines/1      time:   [101.63 ns 101.84 ns 102.06 ns]
label_values__naive_usage/ntimelines/4      time:   [417.55 ns 424.73 ns 432.63 ns]
label_values__naive_usage/ntimelines/8      time:   [874.91 ns 889.51 ns 904.25 ns]
label_values__cache_label_values_lookup/ntimelines/1      time:   [5.7724 ns 5.7760 ns 5.7804 ns]
label_values__cache_label_values_lookup/ntimelines/4      time:   [7.8878 ns 7.9401 ns 8.0034 ns]
label_values__cache_label_values_lookup/ntimelines/8      time:   [7.2621 ns 7.6354 ns 8.0337 ns]
single_metric_multicore_scalability/nthreads/1      time:   [5.7710 ns 5.7744 ns 5.7785 ns]
single_metric_multicore_scalability/nthreads/4      time:   [66.629 ns 66.994 ns 67.336 ns]
single_metric_multicore_scalability/nthreads/8      time:   [130.85 ns 131.98 ns 132.91 ns]
propagation_of_cached_label_value__naive/nthreads/1     time:   [11.540 ns 11.546 ns 11.553 ns]
propagation_of_cached_label_value__naive/nthreads/4     time:   [131.22 ns 131.90 ns 132.56 ns]
propagation_of_cached_label_value__naive/nthreads/8     time:   [260.99 ns 262.75 ns 264.26 ns]
propagation_of_cached_label_value__long_lived_reference_per_thread/nthreads/1     time:   [11.544 ns 11.550 ns 11.557 ns]
propagation_of_cached_label_value__long_lived_reference_per_thread/nthreads/4     time:   [11.568 ns 11.642 ns 11.763 ns]
propagation_of_cached_label_value__long_lived_reference_per_thread/nthreads/8     time:   [13.416 ns 14.121 ns 14.886 ns

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
bucket_scalability/nbuckets/1     time:   [4.8455 ns 4.8542 ns 4.8646 ns]
bucket_scalability/nbuckets/4     time:   [4.5663 ns 4.5722 ns 4.5787 ns]
bucket_scalability/nbuckets/8     time:   [4.5531 ns 4.5670 ns 4.5842 ns]
bucket_scalability/nbuckets/16      time:   [4.6392 ns 4.6524 ns 4.6685 ns]
bucket_scalability/nbuckets/32      time:   [6.0302 ns 6.0439 ns 6.0589 ns]
bucket_scalability/nbuckets/64      time:   [10.608 ns 10.644 ns 10.691 ns]
bucket_scalability/nbuckets/128     time:   [22.178 ns 22.316 ns 22.483 ns]
bucket_scalability/nbuckets/256     time:   [42.190 ns 42.328 ns 42.492 ns]

Results on a Hetzner AX102 AMD Ryzen 9 7950X3D 16-Core Processor

label_values__naive_usage/ntimelines/1      time:   [64.510 ns 64.559 ns 64.610 ns]
label_values__naive_usage/ntimelines/4      time:   [309.71 ns 326.09 ns 342.32 ns]
label_values__naive_usage/ntimelines/8      time:   [776.92 ns 819.35 ns 856.93 ns]
label_values__cache_label_values_lookup/ntimelines/1      time:   [1.2855 ns 1.2943 ns 1.3021 ns]
label_values__cache_label_values_lookup/ntimelines/4      time:   [1.3865 ns 1.4139 ns 1.4441 ns]
label_values__cache_label_values_lookup/ntimelines/8      time:   [1.5311 ns 1.5669 ns 1.6046 ns]
single_metric_multicore_scalability/nthreads/1      time:   [1.1927 ns 1.1981 ns 1.2049 ns]
single_metric_multicore_scalability/nthreads/4      time:   [24.346 ns 25.439 ns 26.634 ns]
single_metric_multicore_scalability/nthreads/8      time:   [58.666 ns 60.137 ns 61.486 ns]
propagation_of_cached_label_value__naive/nthreads/1     time:   [2.7067 ns 2.7238 ns 2.7402 ns]
propagation_of_cached_label_value__naive/nthreads/4     time:   [62.723 ns 66.214 ns 69.787 ns]
propagation_of_cached_label_value__naive/nthreads/8     time:   [164.24 ns 170.10 ns 175.68 ns]
propagation_of_cached_label_value__long_lived_reference_per_thread/nthreads/1     time:   [2.2915 ns 2.2960 ns 2.3012 ns]
propagation_of_cached_label_value__long_lived_reference_per_thread/nthreads/4     time:   [2.5726 ns 2.6158 ns 2.6624 ns]
propagation_of_cached_label_value__long_lived_reference_per_thread/nthreads/8     time:   [2.7068 ns 2.8243 ns 2.9824 ns]
bucket_scalability/nbuckets/1     time:   [6.3998 ns 6.4288 ns 6.4684 ns]
bucket_scalability/nbuckets/4     time:   [6.3603 ns 6.3620 ns 6.3637 ns]
bucket_scalability/nbuckets/8     time:   [6.1646 ns 6.1654 ns 6.1667 ns]
bucket_scalability/nbuckets/16      time:   [6.1341 ns 6.1391 ns 6.1454 ns]
bucket_scalability/nbuckets/32      time:   [8.2206 ns 8.2254 ns 8.2301 ns]
bucket_scalability/nbuckets/64      time:   [13.988 ns 13.994 ns 14.000 ns]
bucket_scalability/nbuckets/128     time:   [28.180 ns 28.216 ns 28.251 ns]
bucket_scalability/nbuckets/256     time:   [54.914 ns 54.931 ns 54.951 ns]

*/
