use anyhow::Result;
use num_traits::ToPrimitive;
use pageserver::repository::{Key, Value};
use pageserver::tenant::bst_layer_map::RetroactiveLayerMap;
use pageserver::tenant::filename::{DeltaFileName, ImageFileName};
use pageserver::tenant::layer_map::LayerMap;
use pageserver::tenant::storage_layer::Layer;
use pageserver::tenant::storage_layer::ValueReconstructResult;
use pageserver::tenant::storage_layer::ValueReconstructState;
use rand::prelude::{SeedableRng, SliceRandom, StdRng};
use std::cmp::{max, min};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::ops::Range;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use utils::id::{TenantId, TimelineId};
use utils::lsn::Lsn;

use criterion::{criterion_group, criterion_main, Criterion};

struct DummyDelta {
    key_range: Range<Key>,
    lsn_range: Range<Lsn>,
}

impl Layer for DummyDelta {
    fn get_tenant_id(&self) -> TenantId {
        TenantId::from_str("00000000000000000000000000000000").unwrap()
    }

    fn get_timeline_id(&self) -> TimelineId {
        TimelineId::from_str("00000000000000000000000000000000").unwrap()
    }

    fn get_key_range(&self) -> Range<Key> {
        self.key_range.clone()
    }

    fn get_lsn_range(&self) -> Range<Lsn> {
        self.lsn_range.clone()
    }

    fn filename(&self) -> PathBuf {
        todo!()
    }

    fn local_path(&self) -> Option<PathBuf> {
        todo!()
    }

    fn get_value_reconstruct_data(
        &self,
        _key: Key,
        _lsn_range: Range<Lsn>,
        _reconstruct_data: &mut ValueReconstructState,
    ) -> Result<ValueReconstructResult> {
        panic!()
    }

    fn is_incremental(&self) -> bool {
        true
    }

    fn is_in_memory(&self) -> bool {
        false
    }

    fn iter(&self) -> Box<dyn Iterator<Item = Result<(Key, Lsn, Value)>> + '_> {
        panic!()
    }

    fn key_iter(&self) -> Box<dyn Iterator<Item = (Key, Lsn, u64)> + '_> {
        panic!("Not implemented")
    }

    fn delete(&self) -> Result<()> {
        panic!()
    }

    fn dump(&self, _verbose: bool) -> Result<()> {
        todo!()
    }
}

struct DummyImage {
    key_range: Range<Key>,
    lsn: Lsn,
}

impl Layer for DummyImage {
    fn get_tenant_id(&self) -> TenantId {
        TenantId::from_str("00000000000000000000000000000000").unwrap()
    }

    fn get_timeline_id(&self) -> TimelineId {
        TimelineId::from_str("00000000000000000000000000000000").unwrap()
    }

    fn get_key_range(&self) -> Range<Key> {
        self.key_range.clone()
    }

    fn get_lsn_range(&self) -> Range<Lsn> {
        // End-bound is exclusive
        self.lsn..(self.lsn + 1)
    }

    fn filename(&self) -> PathBuf {
        todo!()
    }

    fn local_path(&self) -> Option<PathBuf> {
        todo!()
    }

    fn get_value_reconstruct_data(
        &self,
        _key: Key,
        _lsn_range: Range<Lsn>,
        _reconstruct_data: &mut ValueReconstructState,
    ) -> Result<ValueReconstructResult> {
        panic!()
    }

    fn is_incremental(&self) -> bool {
        false
    }

    fn is_in_memory(&self) -> bool {
        false
    }

    fn iter(&self) -> Box<dyn Iterator<Item = Result<(Key, Lsn, Value)>> + '_> {
        panic!()
    }

    fn key_iter(&self) -> Box<dyn Iterator<Item = (Key, Lsn, u64)> + '_> {
        panic!("Not implemented")
    }

    fn delete(&self) -> Result<()> {
        panic!()
    }

    fn dump(&self, _verbose: bool) -> Result<()> {
        todo!()
    }
}

fn build_layer_map(filename_dump: PathBuf) -> LayerMap {
    let mut layer_map = LayerMap::default();

    let mut min_lsn = Lsn(u64::MAX);
    let mut max_lsn = Lsn(0);

    let filenames = BufReader::new(File::open(filename_dump).unwrap()).lines();

    for fname in filenames {
        let fname = &fname.unwrap();
        if let Some(imgfilename) = ImageFileName::parse_str(fname) {
            let layer = DummyImage {
                key_range: imgfilename.key_range,
                lsn: imgfilename.lsn,
            };
            layer_map.insert_historic(Arc::new(layer));
            min_lsn = min(min_lsn, imgfilename.lsn);
            max_lsn = max(max_lsn, imgfilename.lsn);
        } else if let Some(deltafilename) = DeltaFileName::parse_str(fname) {
            let layer = DummyDelta {
                key_range: deltafilename.key_range,
                lsn_range: deltafilename.lsn_range.clone(),
            };
            layer_map.insert_historic(Arc::new(layer));
            min_lsn = min(min_lsn, deltafilename.lsn_range.start);
            max_lsn = max(max_lsn, deltafilename.lsn_range.end);
        } else {
            panic!("unexpected filename {fname}");
        }
    }
    layer_map.rebuild_index();

    println!("min: {min_lsn}, max: {max_lsn}");

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
    let layer_map = build_layer_map(PathBuf::from("benches/odd-brook-layernames.txt"));
    let queries: Vec<(Key, Lsn)> = uniform_query_pattern(&layer_map);

    // Test with uniform query pattern
    c.bench_function("captest_uniform_queries", |b| {
        b.iter(|| {
            for q in queries.clone().into_iter() {
                layer_map.search(q.0, q.1).unwrap();
            }
        });
    });

    // test with a key that corresponds to the RelDir entry. See pgdatadir_mapping.rs.
    c.bench_function("captest_rel_dir_query", |b| {
        b.iter(|| {
            let result = layer_map.search(
                Key::from_hex("000000067F00008000000000000000000001").unwrap(),
                // This LSN is higher than any of the LSNs in the tree
                Lsn::from_str("D0/80208AE1").unwrap(),
            );
            result.unwrap();
        });
    });
}

// Benchmark using metadata extracted from a real project that was taknig
// too long processing layer map queries.
fn bench_from_real_project(c: &mut Criterion) {
    // TODO consider compressing this file

    // Init layer map
    let now = Instant::now();
    let layer_map = build_layer_map(PathBuf::from("benches/odd-brook-layernames.txt"));
    println!("Finished layer map init in {:?}", now.elapsed());

    // Init bst layer map with the same layers
    let now = Instant::now();
    let mut bstlm = RetroactiveLayerMap::new();
    for layer in layer_map.iter_historic_layers() {
        let kr = layer.get_key_range();
        let lr = layer.get_lsn_range();

        bstlm.insert(
            kr.start.to_i128()..kr.end.to_i128(),
            lr.start.0..lr.end.0,
            format!("Layer {}", lr.start.0),
        );
    }
    bstlm.rebuild();
    println!("Finished bst init in {:?}", now.elapsed());

    // Choose uniformly distributed queries
    let queries: Vec<(Key, Lsn)> = uniform_query_pattern(&layer_map);

    // Define and name the benchmark function
    let mut group = c.benchmark_group("real_map_uniform_queries");
    group.bench_function("current_code", |b| {
        b.iter(|| {
            for q in queries.clone().into_iter() {
                layer_map.search(q.0, q.1).unwrap();
            }
        });
    });
    group.bench_function("persistent_bst", |b| {
        b.iter(|| {
            for q in queries.clone().into_iter() {
                bstlm.query(q.0.to_i128(), q.1 .0);
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
    for i in 0..100_000 {
        let i32 = (i as u32) % 100;
        let zero = Key::from_hex("000000000000000000000000000000000000").unwrap();
        let layer = DummyImage {
            key_range: zero.add(10 * i32)..zero.add(10 * i32 + 1),
            lsn: Lsn(i),
        };
        layer_map.insert_historic(Arc::new(layer));
    }
    layer_map.rebuild_index();
    println!("Finished layer map init in {:?}", now.elapsed());

    // Init bst layer map with the same layers
    let now = Instant::now();
    let mut bstlm = RetroactiveLayerMap::new();
    for layer in layer_map.iter_historic_layers() {
        let kr = layer.get_key_range();
        let lr = layer.get_lsn_range();

        bstlm.insert(
            kr.start.to_i128()..kr.end.to_i128(),
            lr.start.0..lr.end.0,
            format!("Layer {}", lr.start.0),
        );
    }
    bstlm.rebuild();
    println!("Finished bst init in {:?}", now.elapsed());

    // Choose 100 uniformly random queries
    let rng = &mut StdRng::seed_from_u64(1);
    let queries: Vec<(Key, Lsn)> = uniform_query_pattern(&layer_map)
        .choose_multiple(rng, 100)
        .copied()
        .collect();

    // Define and name the benchmark function
    let mut group = c.benchmark_group("sequential_uniform_queries");
    group.bench_function("current_code", |b| {
        b.iter(|| {
            for q in queries.clone().into_iter() {
                layer_map.search(q.0, q.1).unwrap();
            }
        });
    });
    group.bench_function("persistent_bst", |b| {
        b.iter(|| {
            for q in queries.clone().into_iter() {
                bstlm.query(q.0.to_i128(), q.1 .0);
            }
        });
    });
    group.finish();
}

criterion_group!(group_1, bench_from_captest_env);
criterion_group!(group_2, bench_from_real_project);
criterion_group!(group_3, bench_sequential);
criterion_main!(group_1, group_2, group_3);
