use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn bench_writes(c: &mut Criterion) {
    // TODO setup

    let mut group = c.benchmark_group("g1");
    group.bench_function("f1", |b| {
        b.iter(|| {
            // TODO
        });
    });
    group.bench_function("f2", |b| {
        b.iter(|| {
            // TODO
        });
    });
    group.finish();
}


criterion_group!(group_1, bench_writes);
criterion_main!(group_1);
