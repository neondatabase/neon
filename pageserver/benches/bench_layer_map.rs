use anyhow::Result;
use pageserver::repository::{Key, Value};
use pageserver::tenant::filename::{DeltaFileName, ImageFileName};
use pageserver::tenant::layer_map::LayerMap;
use pageserver::tenant::storage_layer::Layer;
use pageserver::tenant::storage_layer::ValueReconstructResult;
use pageserver::tenant::storage_layer::ValueReconstructState;
use std::cmp::{max, min};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::ops::Range;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
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

    println!("min: {min_lsn}, max: {max_lsn}");

    layer_map
}

fn large_layer_map(c: &mut Criterion) {
    // A list of layer filenames, extracted from our performance test environment, from
    // a project where we have run pgbench many timmes. The pgbench database was initialized
    // between each test run.
    let layer_map = build_layer_map(PathBuf::from("benches/odd-brook-layernames.txt"));

    c.bench_function("search", |b| {
        b.iter(|| {
            let result = layer_map.search(
                // Just an arbitrary point
                //
                // TODO do better.
                Key::from_hex("000000067F000080000009E014000001B011").unwrap(),
                // This LSN is higher than any of the LSNs in the tree
                Lsn::from_str("D0/80208AE1").unwrap(),
            );
            result.unwrap();
        });
    });

    // test with a key that corresponds to the RelDir entry. See pgdatadir_mapping.rs.
    c.bench_function("search_rel_dir", |b| {
        b.iter(|| {
            let result = layer_map.search(
                Key::from_hex("000000067F00008000000000000000000001").unwrap(),
                // This LSN is higher than any of the LSNs in the tree
                Lsn::from_str("D0/80208AE1").unwrap(),
            );
            result.unwrap();
        });
    });

    // TODO I don't think I'm using criterion right. Should I keep adding to the
    //      same function?

    // A list of layer filenames, extracted from a real project that was taknig
    // too long processing layer map queries.
    //
    // TODO consider compressing these files
    let layer_map = build_layer_map(PathBuf::from("benches/odd-brook-layernames.txt"));

    c.bench_function("search_real_map", |b| {
        b.iter(|| {
            let result = layer_map.search(
                // Just an arbitrary point
                //
                // TODO Instead, query right before the midpoint of each image layer.
                //      It's the simplest way to query the entire keyspace uniformly.
                Key::from_hex("000000067F000080000009E014000001B011").unwrap(),
                // This LSN is higher than any of the LSNs in the tree
                Lsn::from_str("D0/80208AE1").unwrap(),
            );
            result.unwrap();
        });
    });
}

criterion_group!(benches, large_layer_map);
criterion_main!(benches);
