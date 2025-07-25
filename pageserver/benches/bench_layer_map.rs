use std::cmp::{max, min};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Instant;

use criterion::measurement::WallTime;
use criterion::{BenchmarkGroup, Criterion, black_box, criterion_group, criterion_main};
use pageserver::tenant::layer_map::LayerMap;
use pageserver::tenant::storage_layer::{LayerName, PersistentLayerDesc};
use pageserver_api::key::Key;
use pageserver_api::shard::TenantShardId;
use rand::prelude::{SeedableRng, StdRng};
use rand::seq::IndexedRandom;
use utils::id::{TenantId, TimelineId};
use utils::lsn::Lsn;

fn fixture_path(relative: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative)
}

fn build_layer_map(filename_dump: PathBuf) -> LayerMap {
    let mut layer_map = LayerMap::default();

    let mut min_lsn = Lsn(u64::MAX);
    let mut max_lsn = Lsn(0);

    let filenames = BufReader::new(File::open(filename_dump).unwrap()).lines();

    let mut updates = layer_map.batch_update();
    for fname in filenames {
        let fname = fname.unwrap();
        let fname = LayerName::from_str(&fname).unwrap();
        let layer = PersistentLayerDesc::from(fname);

        let lsn_range = layer.get_lsn_range();
        min_lsn = min(min_lsn, lsn_range.start);
        max_lsn = max(max_lsn, Lsn(lsn_range.end.0 - 1));

        updates.insert_historic(layer);
    }

    println!("min: {min_lsn}, max: {max_lsn}");

    updates.flush();
    layer_map
}

/// Construct a layer map query pattern for benchmarks
fn uniform_query_pattern(layer_map: &LayerMap) -> Vec<(Key, Lsn)> {
    // For each image layer we query one of the pages contained, at LSN right
    // before the image layer was created. This gives us a somewhat uniform
    // coverage of both the lsn and key space because image layers have
    // approximately equal sizes and cover approximately equal WAL since
    // last image.
    layer_map
        .iter_historic_layers()
        .filter_map(|l| {
            if l.is_incremental() {
                None
            } else {
                let kr = l.get_key_range();
                let lr = l.get_lsn_range();

                let key_inside = kr.start.next();
                let lsn_before = Lsn(lr.start.0 - 1);

                Some((key_inside, lsn_before))
            }
        })
        .collect()
}

// Benchmark using metadata extracted from our performance test environment, from
// a project where we have run pgbench many timmes. The pgbench database was initialized
// between each test run.
fn bench_from_captest_env(c: &mut Criterion) {
    // TODO consider compressing this file
    let layer_map = build_layer_map(fixture_path("benches/odd-brook-layernames.txt"));
    let queries: Vec<(Key, Lsn)> = uniform_query_pattern(&layer_map);

    // Test with uniform query pattern
    c.bench_function("captest_uniform_queries", |b| {
        b.iter(|| {
            for q in queries.clone().into_iter() {
                black_box(layer_map.search(q.0, q.1));
            }
        });
    });

    // test with a key that corresponds to the RelDir entry. See pgdatadir_mapping.rs.
    c.bench_function("captest_rel_dir_query", |b| {
        b.iter(|| {
            let result = black_box(layer_map.search(
                Key::from_hex("000000067F00008000000000000000000001").unwrap(),
                // This LSN is higher than any of the LSNs in the tree
                Lsn::from_str("D0/80208AE1").unwrap(),
            ));
            result.unwrap();
        });
    });
}

// Benchmark using metadata extracted from a real project that was taknig
// too long processing layer map queries.
fn bench_from_real_project(c: &mut Criterion) {
    // Init layer map
    let now = Instant::now();
    let layer_map = build_layer_map(fixture_path("benches/odd-brook-layernames.txt"));
    println!("Finished layer map init in {:?}", now.elapsed());

    // Choose uniformly distributed queries
    let queries: Vec<(Key, Lsn)> = uniform_query_pattern(&layer_map);

    // Define and name the benchmark function
    let mut group = c.benchmark_group("real_map");
    group.bench_function("uniform_queries", |b| {
        b.iter(|| {
            for q in queries.clone().into_iter() {
                black_box(layer_map.search(q.0, q.1));
            }
        });
    });
    group.finish();
}

// Benchmark using synthetic data. Arrange image layers on stacked diagonal lines.
fn bench_sequential(c: &mut Criterion) {
    // Init layer map. Create 100_000 layers arranged in 1000 diagonal lines.
    //
    // TODO This code is pretty slow and runs even if we're only running other
    //      benchmarks. It needs to be somewhere else, but it's not clear where.
    //      Putting it inside the `bench_function` closure is not a solution
    //      because then it runs multiple times during warmup.
    let now = Instant::now();
    let mut layer_map = LayerMap::default();
    let mut updates = layer_map.batch_update();
    for i in 0..100_000 {
        let i32 = (i as u32) % 100;
        let zero = Key::from_hex("000000000000000000000000000000000000").unwrap();
        let layer = PersistentLayerDesc::new_img(
            TenantShardId::unsharded(TenantId::generate()),
            TimelineId::generate(),
            zero.add(10 * i32)..zero.add(10 * i32 + 1),
            Lsn(i),
            0,
        );
        updates.insert_historic(layer);
    }
    updates.flush();
    println!("Finished layer map init in {:?}", now.elapsed());

    // Choose 100 uniformly random queries
    let rng = &mut StdRng::seed_from_u64(1);
    let queries: Vec<(Key, Lsn)> = uniform_query_pattern(&layer_map)
        .choose_multiple(rng, 100)
        .copied()
        .collect();

    // Define and name the benchmark function
    let mut group = c.benchmark_group("sequential");
    group.bench_function("uniform_queries", |b| {
        b.iter(|| {
            for q in queries.clone().into_iter() {
                black_box(layer_map.search(q.0, q.1));
            }
        });
    });
    group.finish();
}

fn bench_visibility_with_map(
    group: &mut BenchmarkGroup<WallTime>,
    layer_map: LayerMap,
    read_points: Vec<Lsn>,
    bench_name: &str,
) {
    group.bench_function(bench_name, |b| {
        b.iter(|| black_box(layer_map.get_visibility(read_points.clone())));
    });
}

// Benchmark using synthetic data. Arrange image layers on stacked diagonal lines.
fn bench_visibility(c: &mut Criterion) {
    let mut group = c.benchmark_group("visibility");
    {
        // Init layer map. Create 100_000 layers arranged in 1000 diagonal lines.
        let now = Instant::now();
        let mut layer_map = LayerMap::default();
        let mut updates = layer_map.batch_update();
        for i in 0..100_000 {
            let i32 = (i as u32) % 100;
            let zero = Key::from_hex("000000000000000000000000000000000000").unwrap();
            let layer = PersistentLayerDesc::new_img(
                TenantShardId::unsharded(TenantId::generate()),
                TimelineId::generate(),
                zero.add(10 * i32)..zero.add(10 * i32 + 1),
                Lsn(i),
                0,
            );
            updates.insert_historic(layer);
        }
        updates.flush();
        println!("Finished layer map init in {:?}", now.elapsed());

        let mut read_points = Vec::new();
        for i in (0..100_000).step_by(1000) {
            read_points.push(Lsn(i));
        }

        bench_visibility_with_map(&mut group, layer_map, read_points, "sequential");
    }

    {
        let layer_map = build_layer_map(fixture_path("benches/odd-brook-layernames.txt"));
        let read_points = vec![Lsn(0x1C760FA190)];
        bench_visibility_with_map(&mut group, layer_map, read_points, "real_map");

        let layer_map = build_layer_map(fixture_path("benches/odd-brook-layernames.txt"));
        let read_points = vec![
            Lsn(0x1C760FA190),
            Lsn(0x000000931BEAD539),
            Lsn(0x000000931BF63011),
            Lsn(0x000000931B33AE68),
            Lsn(0x00000038E67ABFA0),
            Lsn(0x000000931B33AE68),
            Lsn(0x000000914E3F38F0),
            Lsn(0x000000931B33AE68),
        ];
        bench_visibility_with_map(&mut group, layer_map, read_points, "real_map_many_branches");
    }

    group.finish();
}

criterion_group!(group_1, bench_from_captest_env);
criterion_group!(group_2, bench_from_real_project);
criterion_group!(group_3, bench_sequential);
criterion_group!(group_4, bench_visibility);
criterion_main!(group_1, group_2, group_3, group_4);
